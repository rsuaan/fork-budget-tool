import 'dotenv/config';
import express from 'express';
import fetch from 'node-fetch';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));

const GRAFANA_URL   = process.env.GRAFANA_URL   || 'https://twilio.grafana.net';
const GRAFANA_TOKEN = process.env.GRAFANA_TOKEN  || '';
const PORT          = process.env.PORT           || 3000;
const OTEL_UID      = process.env.OTEL_UID       || 'dffjhhyy5wetcf'; // twilio-clickhouse-otel-logs-prod-us-east-1

if (!GRAFANA_TOKEN) {
  console.error('ERROR: GRAFANA_TOKEN env var is required.');
  process.exit(1);
}

const app = express();
app.use(express.static(join(__dirname, 'public')));

async function clickhouseQuery(sql) {
  const resp = await fetch(`${GRAFANA_URL}/api/ds/query?ds_type=grafana-clickhouse-datasource`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${GRAFANA_TOKEN}`,
    },
    body: JSON.stringify({
      queries: [{ refId: 'A', datasourceUid: OTEL_UID, rawSql: sql, format: 'table' }],
      from: 'now-30d',
      to: 'now',
    }),
  });

  if (!resp.ok) {
    const txt = await resp.text();
    throw new Error(`Grafana ${resp.status}: ${txt.slice(0, 300)}`);
  }

  const data = await resp.json();
  const frames = data?.results?.A?.frames;
  if (!frames?.[0]) return [];

  const fields = frames[0].schema.fields.map(f => f.name);
  const values = frames[0].data.values;
  const rowCount = values[0]?.length || 0;
  return Array.from({ length: rowCount }, (_, i) => {
    const row = {};
    fields.forEach((f, fi) => { row[f] = values[fi][i]; });
    return row;
  });
}

app.get('/api/lookup', async (req, res) => {
  const callSid = (req.query.call_sid || '').trim();
  if (!callSid || !/^CA[a-f0-9]{32}$/.test(callSid)) {
    return res.status(400).json({ error: 'Invalid or missing call_sid' });
  }

  const esc = callSid.replace(/'/g, "''");
  const timeFilter = "Timestamp >= now() - INTERVAL 30 DAY";

  try {
    const [streamsRows, transcRows, siprecRows, amdRows] = await Promise.all([
      // Media Streams — distinct MZ SIDs
      clickhouseQuery(`
        SELECT LogAttributes['session_id'] AS stream_sid, min(Timestamp) AS started
        FROM otel_twlo.otel_logs
        WHERE ${timeFilter}
          AND ServiceName = 'voice-media-streamer'
          AND LogAttributes['call_sid'] = '${esc}'
          AND LogAttributes['session_id'] != ''
        GROUP BY stream_sid
        ORDER BY started
      `),

      // Transcription — GT SIDs + channels from log body
      clickhouseQuery(`
        SELECT LogAttributes['stream_id'] AS stream_sid,
               extractAll(Body, 'channel=(inbound|outbound)') AS channels
        FROM otel_twlo.otel_logs
        WHERE ${timeFilter}
          AND ServiceName = 'realtime-transcription-service'
          AND LogAttributes['call_sid'] = '${esc}'
          AND LogAttributes['stream_id'] != ''
          AND Body LIKE '%State change%ESTABLISHED%'
        GROUP BY stream_sid, channels
        LIMIT 20
      `),

      // SIPREC — SR SIDs
      clickhouseQuery(`
        SELECT LogAttributes['stream_id'] AS stream_sid, count() AS events
        FROM otel_twlo.otel_logs
        WHERE ${timeFilter}
          AND ServiceName = 'voice-siprec-service'
          AND LogAttributes['call_sid'] = '${esc}'
          AND LogAttributes['stream_id'] != ''
        GROUP BY stream_sid
        ORDER BY events DESC
        LIMIT 10
      `),

      // AMD — presence check
      clickhouseQuery(`
        SELECT Body
        FROM otel_twlo.otel_logs
        WHERE ${timeFilter}
          AND ServiceName = 'voice-amd-service'
          AND Body LIKE '%X-Twilio-CallSid: ${esc}%'
        LIMIT 1
      `),
    ]);

    // --- Media Streams ---
    const streams = streamsRows.map(r => ({ sid: r.stream_sid }));

    // --- Transcription ---
    const transcBySid = {};
    for (const r of transcRows) {
      if (!transcBySid[r.stream_sid]) transcBySid[r.stream_sid] = new Set();
      (r.channels || []).forEach(ch => transcBySid[r.stream_sid].add(ch));
    }
    const transcriptions = Object.entries(transcBySid).map(([sid, channels]) => ({
      sid,
      channels: [...channels],
      forks: (channels.has('inbound') && channels.has('outbound')) ? 2 : 1,
    }));

    // --- SIPREC ---
    const siprec = siprecRows.map(r => ({ sid: r.stream_sid, forks: 2 })); // default both_tracks

    // --- AMD ---
    const amdDetected = amdRows.length > 0;

    res.json({
      callSid,
      streams,
      transcriptions,
      siprec,
      amdDetected,
      trackModeUnknown: streams.length > 0 || siprec.length > 0,
    });

  } catch (err) {
    console.error('Lookup error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Fork Budget Tool running at http://localhost:${PORT}`);
});

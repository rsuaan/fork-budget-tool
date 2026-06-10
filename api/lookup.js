const GRAFANA_URL = process.env.GRAFANA_URL  || 'https://twilio.grafana.net';
const GRAFANA_TOKEN = process.env.GRAFANA_TOKEN || '';
const OTEL_UID = process.env.OTEL_UID || 'dffjhhyy5wetcf';

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

export default async function handler(req, res) {
  // CORS — allow requests from your GitHub Pages domain
  const origin = req.headers.origin || '';
  if (origin.includes('github.io') || origin.includes('localhost')) {
    res.setHeader('Access-Control-Allow-Origin', origin);
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const callSid = (req.query.call_sid || '').trim();
  if (!callSid || !/^CA[a-f0-9]{32}$/.test(callSid)) {
    return res.status(400).json({ error: 'Invalid or missing call_sid' });
  }

  if (!GRAFANA_TOKEN) {
    return res.status(500).json({ error: 'Server not configured — GRAFANA_TOKEN missing.' });
  }

  const esc = callSid.replace(/'/g, "''");
  const timeFilter = "Timestamp >= now() - INTERVAL 30 DAY";

  try {
    const [streamsRows, transcRows, siprecRows, amdRows] = await Promise.all([
      clickhouseQuery(`
        SELECT LogAttributes['session_id'] AS stream_sid, min(Timestamp) AS started
        FROM otel_twlo.otel_logs
        WHERE ${timeFilter}
          AND ServiceName = 'voice-media-streamer'
          AND LogAttributes['call_sid'] = '${esc}'
          AND LogAttributes['session_id'] != ''
        GROUP BY stream_sid ORDER BY started
      `),
      clickhouseQuery(`
        SELECT LogAttributes['stream_id'] AS stream_sid,
               extractAll(Body, 'channel=(inbound|outbound)') AS channels
        FROM otel_twlo.otel_logs
        WHERE ${timeFilter}
          AND ServiceName = 'realtime-transcription-service'
          AND LogAttributes['call_sid'] = '${esc}'
          AND LogAttributes['stream_id'] != ''
          AND Body LIKE '%State change%ESTABLISHED%'
        GROUP BY stream_sid, channels LIMIT 20
      `),
      clickhouseQuery(`
        SELECT LogAttributes['stream_id'] AS stream_sid, count() AS events
        FROM otel_twlo.otel_logs
        WHERE ${timeFilter}
          AND ServiceName = 'voice-siprec-service'
          AND LogAttributes['call_sid'] = '${esc}'
          AND LogAttributes['stream_id'] != ''
        GROUP BY stream_sid ORDER BY events DESC LIMIT 10
      `),
      clickhouseQuery(`
        SELECT Body FROM otel_twlo.otel_logs
        WHERE ${timeFilter}
          AND ServiceName = 'voice-amd-service'
          AND Body LIKE '%X-Twilio-CallSid: ${esc}%'
        LIMIT 1
      `),
    ]);

    const streams = streamsRows.map(r => ({ sid: r.stream_sid }));

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

    const siprec = siprecRows.map(r => ({ sid: r.stream_sid, forks: 2 }));
    const amdDetected = amdRows.length > 0;

    res.status(200).json({ callSid, streams, transcriptions, siprec, amdDetected });

  } catch (err) {
    console.error('Lookup error:', err.message);
    res.status(500).json({ error: err.message });
  }
}

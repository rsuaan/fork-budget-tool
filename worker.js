const GRAFANA_URL = 'https://grafana.twilioinfra.com';
const DS_UID = 'dffjhhyy5wetcf';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, X-Grafana-Session',
};

function isValidSid(sid) {
  return typeof sid === 'string' && /^CA[0-9a-f]{32}$/.test(sid);
}

async function gq(auth, sql) {
  const headers = { 'Content-Type': 'application/json' };
  if (auth.startsWith('glsa_') || auth.startsWith('eyJ')) {
    headers['Authorization'] = `Bearer ${auth}`;
  } else {
    headers['Cookie'] = `grafana_session=${auth}`;
  }
  const resp = await fetch(
    `${GRAFANA_URL}/api/ds/query?ds_type=grafana-clickhouse-datasource`,
    {
      method: 'POST',
      headers,
      body: JSON.stringify({
        queries: [{
          datasource: { uid: DS_UID, type: 'grafana-clickhouse-datasource' },
          rawSql: sql,
          format: 'table',
          refId: 'A',
        }],
        from: 'now-30d',
        to: 'now',
      }),
    }
  );
  if (!resp.ok) throw new Error(`Grafana ${resp.status}: ${await resp.text()}`);
  const json = await resp.json();
  const frames = json?.results?.A?.frames;
  if (!frames?.length) return [];
  const cols = frames[0].schema.fields.map(f => f.name);
  const vals = frames[0].data.values;
  if (!vals?.length) return [];
  return vals[0].map((_, i) =>
    Object.fromEntries(cols.map((c, ci) => [c, vals[ci][i]]))
  );
}

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    const url = new URL(request.url);
    const sid = url.searchParams.get('call_sid');

    if (!isValidSid(sid)) {
      return new Response(JSON.stringify({ error: 'Invalid call_sid' }), {
        status: 400,
        headers: { ...CORS, 'Content-Type': 'application/json' },
      });
    }

    const auth = request.headers.get('X-Grafana-Session') || env.GRAFANA_TOKEN || '';
    if (!auth) {
      return new Response(JSON.stringify({ error: 'No Grafana session provided' }), {
        status: 401,
        headers: { ...CORS, 'Content-Type': 'application/json' },
      });
    }

    const service = url.searchParams.get('service');

    const QUERIES = {
      media: `SELECT LogAttributes['session_id'] AS stream_sid, min(Timestamp) AS started
        FROM otel_twlo.otel_logs
        WHERE Timestamp >= now() - INTERVAL 30 DAY
          AND ServiceName = 'voice-media-streamer'
          AND LogAttributes['call_sid'] = '${sid}'
          AND LogAttributes['session_id'] != ''
        GROUP BY stream_sid ORDER BY started`,

      transcription: `SELECT LogAttributes['stream_id'] AS stream_sid,
          extractAll(Body, 'channel=(inbound|outbound)') AS channels
        FROM otel_twlo.otel_logs
        WHERE Timestamp >= now() - INTERVAL 30 DAY
          AND ServiceName = 'realtime-transcription-service'
          AND LogAttributes['call_sid'] = '${sid}'
          AND LogAttributes['stream_id'] != ''
          AND Body LIKE '%State change%ESTABLISHED%'
        GROUP BY stream_sid, channels LIMIT 20`,

      siprec: `SELECT LogAttributes['stream_id'] AS stream_sid, count() AS events
        FROM otel_twlo.otel_logs
        WHERE Timestamp >= now() - INTERVAL 30 DAY
          AND ServiceName = 'voice-siprec-service'
          AND LogAttributes['call_sid'] = '${sid}'
          AND LogAttributes['stream_id'] != ''
        GROUP BY stream_sid ORDER BY events DESC LIMIT 10`,

      amd: `SELECT Body FROM otel_twlo.otel_logs
        WHERE Timestamp >= now() - INTERVAL 30 DAY
          AND ServiceName = 'voice-amd-service'
          AND LogAttributes['call_sid'] = '${sid}'
        LIMIT 1`,
    };

    try {
      if (service && QUERIES[service]) {
        const rows = await gq(auth, QUERIES[service]);
        return new Response(JSON.stringify({ service, rows }), {
          headers: { ...CORS, 'Content-Type': 'application/json' },
        });
      }

      // fallback: run all 4
      const [mediaRows, transcRows, siprecRows, amdRows] = await Promise.all(
        ['media','transcription','siprec','amd'].map(s => gq(auth, QUERIES[s]))
      );
      return new Response(
        JSON.stringify({ mediaRows, transcRows, siprecRows, amdRows }),
        { headers: { ...CORS, 'Content-Type': 'application/json' } }
      );
    } catch (err) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 502,
        headers: { ...CORS, 'Content-Type': 'application/json' },
      });
    }
  },
};

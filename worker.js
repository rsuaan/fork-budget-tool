const GRAFANA_URL = 'https://grafana.twilioinfra.com';
const DS_UID = 'dffjhhyy5wetcf';

const CORS = {
  'Access-Control-Allow-Origin': 'https://rsuaan.github.io',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

function isValidSid(sid) {
  return typeof sid === 'string' && /^CA[0-9a-f]{32}$/.test(sid);
}

async function gq(token, sql) {
  const resp = await fetch(
    `${GRAFANA_URL}/api/ds/query?ds_type=grafana-clickhouse-datasource`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
      },
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

    if (!env.GRAFANA_TOKEN) {
      return new Response(JSON.stringify({ error: 'GRAFANA_TOKEN not configured' }), {
        status: 500,
        headers: { ...CORS, 'Content-Type': 'application/json' },
      });
    }

    try {
      const [mediaRows, transcRows, siprecRows, amdRows] = await Promise.all([
        gq(env.GRAFANA_TOKEN, `
          SELECT LogAttributes['session_id'] AS stream_sid, min(Timestamp) AS started
          FROM otel_twlo.otel_logs
          WHERE Timestamp >= now() - INTERVAL 30 DAY
            AND ServiceName = 'voice-media-streamer'
            AND LogAttributes['call_sid'] = '${sid}'
            AND LogAttributes['session_id'] != ''
          GROUP BY stream_sid ORDER BY started`),

        gq(env.GRAFANA_TOKEN, `
          SELECT LogAttributes['stream_id'] AS stream_sid,
                 extractAll(Body, 'channel=(inbound|outbound)') AS channels
          FROM otel_twlo.otel_logs
          WHERE Timestamp >= now() - INTERVAL 30 DAY
            AND ServiceName = 'realtime-transcription-service'
            AND LogAttributes['call_sid'] = '${sid}'
            AND LogAttributes['stream_id'] != ''
            AND Body LIKE '%State change%ESTABLISHED%'
          GROUP BY stream_sid, channels LIMIT 20`),

        gq(env.GRAFANA_TOKEN, `
          SELECT LogAttributes['stream_id'] AS stream_sid, count() AS events
          FROM otel_twlo.otel_logs
          WHERE Timestamp >= now() - INTERVAL 30 DAY
            AND ServiceName = 'voice-siprec-service'
            AND LogAttributes['call_sid'] = '${sid}'
            AND LogAttributes['stream_id'] != ''
          GROUP BY stream_sid ORDER BY events DESC LIMIT 10`),

        gq(env.GRAFANA_TOKEN, `
          SELECT Body FROM otel_twlo.otel_logs
          WHERE Timestamp >= now() - INTERVAL 30 DAY
            AND ServiceName = 'voice-amd-service'
            AND Body LIKE '%X-Twilio-CallSid: ${sid}%'
          LIMIT 1`),
      ]);

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

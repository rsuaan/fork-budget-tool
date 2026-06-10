import { spawn, exec } from 'child_process';
import http from 'http';

const PORT = 47821;
const CLAUDE_BIN = '/Users/rsuaan/.local/bin/claude';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

function send(res, obj) {
  res.write(`data: ${JSON.stringify(obj)}\n\n`);
}

// Parse [PROGRESS:key:state:detail] and [RESULT:...] markers from a text chunk
function parseMarkers(text) {
  const events = [];
  for (const line of text.split('\n')) {
    const progress = line.match(/\[PROGRESS:(\w+):(querying|done|error):?(.*?)\]$/);
    if (progress) {
      events.push({ type: 'progress', key: progress[1], state: progress[2], detail: progress[3] || '' });
      continue;
    }
    const result = line.match(/\[RESULT:([^\]]+)\]/);
    if (result) {
      events.push({ type: 'result', params: result[1] });
    }
  }
  return events;
}

// Detect OAuth URL in Claude output
function extractOAuthUrl(text) {
  const m = text.match(/https:\/\/claude-mcp-grafana[^\s)>"]+/);
  return m ? m[0] : null;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS);
    res.end();
    return;
  }

  if (url.pathname === '/ping') {
    res.writeHead(200, { ...CORS, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method !== 'GET' || url.pathname !== '/analyse') {
    res.writeHead(404, CORS);
    res.end(JSON.stringify({ error: 'Not found' }));
    return;
  }

  const sid = (url.searchParams.get('call_sid') || '').trim();
  if (!/^CA[0-9a-f]{32}$/.test(sid)) {
    res.writeHead(400, { ...CORS, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid call_sid' }));
    return;
  }

  res.writeHead(200, {
    ...CORS,
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
  });

  const child = spawn(CLAUDE_BIN, [
    '--dangerously-skip-permissions',
    '-p', `/fork-budget ${sid}`,
  ], {
    env: { ...process.env, HOME: '/Users/rsuaan' },
  });

  let oauthOpened = false;
  let buffer = '';

  function processChunk(chunk) {
    buffer += chunk;

    // Check for OAuth URL and open browser automatically
    if (!oauthOpened) {
      const oauthUrl = extractOAuthUrl(buffer);
      if (oauthUrl) {
        oauthOpened = true;
        send(res, { type: 'auth', message: 'Grafana authentication required — opening browser…' });
        exec(`open "${oauthUrl}"`, err => {
          if (err) {
            send(res, { type: 'auth_error', message: 'Could not open browser. Visit: ' + oauthUrl });
          }
        });
        send(res, { type: 'auth_waiting', message: 'Waiting for authentication…' });
      }
    }

    // Parse and emit structured progress markers
    const markers = parseMarkers(buffer);
    for (const ev of markers) {
      send(res, ev);
    }
    // Clear processed lines from buffer, keep last incomplete line
    const lastNewline = buffer.lastIndexOf('\n');
    if (lastNewline !== -1) buffer = buffer.slice(lastNewline + 1);
  }

  child.stdout.on('data', chunk => processChunk(chunk.toString()));
  child.stderr.on('data', chunk => processChunk(chunk.toString()));

  child.on('close', code => {
    send(res, { type: 'done', code });
    res.end();
  });

  req.on('close', () => child.kill());
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Fork Budget bridge running on http://127.0.0.1:${PORT}`);
});

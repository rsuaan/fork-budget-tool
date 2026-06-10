import { spawn, exec } from 'child_process';
import http from 'http';

const PORT = 47821;
const CLAUDE_BIN = '/Users/rsuaan/.local/bin/claude';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

function sse(res, obj) {
  res.write(`data: ${JSON.stringify(obj)}\n\n`);
}

function parseMarkers(text) {
  const events = [];
  for (const line of text.split('\n')) {
    const progress = line.match(/\[PROGRESS:(\w+):(querying|done|none|error):?(.*?)\]$/);
    if (progress) {
      events.push({ type: 'progress', key: progress[1], state: progress[2], detail: progress[3] || '' });
    }
    const result = line.match(/\[RESULT:([^\]]+)\]/);
    if (result) {
      events.push({ type: 'result', params: result[1] });
    }
  }
  return events;
}

function runClaude(res, prompt) {
  sse(res, { type: 'connected' });

  const child = spawn(CLAUDE_BIN, [
    '--print',
    '--output-format', 'stream-json',
    '--verbose',
    '--dangerously-skip-permissions',
    '--plugin-dir', '/Users/rsuaan/voice-media-troubleshooting',
    '-p', prompt,
  ], {
    env: { ...process.env, HOME: '/Users/rsuaan' },
    cwd: '/Users/rsuaan',
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let lineBuf = '';
  let oauthOpened = false;
  let authPending = false;
  let fullText = '';

  function handleLine(line) {
    if (!line.trim()) return;
    let msg;
    try { msg = JSON.parse(line); } catch { return; }

    if (msg.type === 'assistant' && msg.message?.content) {
      for (const block of msg.message.content) {
        if (block.type === 'text' && block.text) {
          fullText += block.text;

          // Detect OAuth URL — open browser and pause watchdog
          if (!oauthOpened) {
            const oauthMatch = fullText.match(/https:\/\/claude-mcp-grafana[^\s)>"'\n]+/);
            if (oauthMatch) {
              oauthOpened = true;
              authPending = true;
              sse(res, { type: 'auth', message: 'Grafana authentication required — opening browser…' });
              exec(`open "${oauthMatch[0]}"`, () => {});
            }
          }

          const markers = parseMarkers(block.text);
          for (const ev of markers) {
            // First progress event after auth = auth completed
            if (authPending && ev.type === 'progress') {
              authPending = false;
              sse(res, { type: 'auth_complete' });
            }
            sse(res, ev);
          }
        }
      }
    }

    if (msg.type === 'result' && msg.subtype === 'success') {
      for (const ev of parseMarkers(msg.result || '')) {
        sse(res, ev);
      }
      sse(res, { type: 'done' });
    }

    if (msg.type === 'result' && msg.subtype === 'error') {
      sse(res, { type: 'error', message: msg.result || 'Claude returned an error' });
      sse(res, { type: 'done' });
    }
  }

  child.stdout.on('data', chunk => {
    lineBuf += chunk.toString();
    const lines = lineBuf.split('\n');
    lineBuf = lines.pop();
    for (const line of lines) handleLine(line);
  });

  child.stderr.on('data', chunk => {
    console.error('[bridge stderr]', chunk.toString());
  });

  child.on('close', () => {
    if (lineBuf.trim()) handleLine(lineBuf);
    sse(res, { type: 'done' });
    res.end();
  });

  return child;
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

  const sid = (url.searchParams.get('call_sid') || '').trim();
  if (!/^CA[0-9a-f]{32}$/.test(sid)) {
    res.writeHead(400, { ...CORS, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid call_sid' }));
    return;
  }

  if (req.method !== 'GET' || (url.pathname !== '/analyse' && url.pathname !== '/analyse-pcap')) {
    res.writeHead(404, CORS);
    res.end(JSON.stringify({ error: 'Not found' }));
    return;
  }

  res.writeHead(200, {
    ...CORS,
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
  });

  const prompt = url.pathname === '/analyse-pcap'
    ? `/media-analysis ${sid}`
    : `/fork-budget ${sid}`;

  const child = runClaude(res, prompt);
  req.on('close', () => child.kill());
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Fork Budget bridge running on http://127.0.0.1:${PORT}`);
});

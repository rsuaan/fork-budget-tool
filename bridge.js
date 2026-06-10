import { spawn, exec } from 'child_process';
import http from 'http';

const PORT = 47821;
const CLAUDE_BIN = '/Users/rsuaan/.local/bin/claude';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

// Registry: sid -> { child, events[], clients[] }
// Claude runs independently of SSE connections so OAuth callback server stays alive.
const jobs = new Map();

function sse(res, obj) {
  try { res.write(`data: ${JSON.stringify(obj)}\n\n`); } catch {}
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

function broadcast(job, obj) {
  job.events.push(obj);
  for (const res of job.clients) sse(res, obj);
}

function startJob(sid, prompt) {
  const job = { child: null, events: [], clients: [] };
  jobs.set(sid, job);

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

  job.child = child;

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

          if (!oauthOpened) {
            const oauthMatch = fullText.match(/https:\/\/claude-mcp-grafana[^\s)>"'\n]+/);
            if (oauthMatch) {
              oauthOpened = true;
              authPending = true;
              broadcast(job, { type: 'auth', message: 'Grafana authentication required — opening browser…' });
              exec(`open "${oauthMatch[0]}"`, () => {});
            }
          }

          for (const ev of parseMarkers(block.text)) {
            if (authPending && ev.type === 'progress') {
              authPending = false;
              broadcast(job, { type: 'auth_complete' });
            }
            broadcast(job, ev);
          }
        }
      }
    }

    if (msg.type === 'result' && msg.subtype === 'success') {
      for (const ev of parseMarkers(msg.result || '')) broadcast(job, ev);
      broadcast(job, { type: 'done' });
    }

    if (msg.type === 'result' && msg.subtype === 'error') {
      broadcast(job, { type: 'error', message: msg.result || 'Claude returned an error' });
      broadcast(job, { type: 'done' });
    }
  }

  child.stdout.on('data', chunk => {
    lineBuf += chunk.toString();
    const lines = lineBuf.split('\n');
    lineBuf = lines.pop();
    for (const line of lines) handleLine(line);
  });

  child.stderr.on('data', chunk => {
    console.error('[bridge stderr]', chunk.toString().trim());
  });

  child.on('close', () => {
    if (lineBuf.trim()) handleLine(lineBuf);
    broadcast(job, { type: 'done' });
    for (const res of job.clients) { try { res.end(); } catch {} }
    jobs.delete(sid);
  });

  return job;
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

  // Explicit stop — called by browser Stop button
  if (url.pathname === '/stop') {
    const sid = (url.searchParams.get('call_sid') || '').trim();
    const job = jobs.get(sid);
    if (job) {
      job.child.kill();
      jobs.delete(sid);
    }
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

  // Reuse existing job (OAuth reconnect) or start new one
  let job = jobs.get(sid);
  if (job) {
    // Replay buffered events so the reconnected client catches up
    for (const ev of job.events) sse(res, ev);
    job.clients.push(res);
  } else {
    const prompt = url.pathname === '/analyse-pcap'
      ? `/media-analysis ${sid}`
      : `/fork-budget ${sid}`;
    job = startJob(sid, prompt);
    job.clients.push(res);
    sse(res, { type: 'connected' });
  }

  // Remove client on disconnect — but do NOT kill Claude
  req.on('close', () => {
    if (job) job.clients = job.clients.filter(c => c !== res);
  });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Fork Budget bridge running on http://127.0.0.1:${PORT}`);
});

import { spawn, exec } from 'child_process';
import { mkdirSync, createWriteStream, createReadStream, existsSync, unlinkSync } from 'fs';
import http from 'http';
import { createInterface } from 'readline';
import os from 'os';
import path from 'path';

const PORT = 47821;
const CLAUDE_BIN = '/Users/rsuaan/.local/bin/claude';
const FIFO_PATH = '/tmp/fork-budget-daemon.fifo';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function sse(res, obj) {
  try { res.write(`data: ${JSON.stringify(obj)}\n\n`); } catch {}
}

function broadcast(job, obj) {
  job.events.push(obj);
  for (const res of job.clients) sse(res, obj);
}

function parseMarkers(text) {
  const events = [];
  for (const line of text.split('\n')) {
    const progress = line.match(/\[PROGRESS:(\w+):(querying|done|none|error):?(.*?)\]$/);
    if (progress) events.push({ type: 'progress', key: progress[1], state: progress[2], detail: progress[3] || '' });
    const result = line.match(/\[RESULT:([^\]]+)\]/);
    if (result) events.push({ type: 'result', params: result[1] });
  }
  return events;
}

// ── Persistent daemon via FIFO ────────────────────────────────────────────────
// Claude reads from a FIFO (named pipe) so stdin stays open indefinitely.
// We write JSON messages to the FIFO to submit prompts.
// OAuth happens once; subsequent calls reuse the live MCP connection.

let daemonProc = null;
let daemonReady = false;
let daemonFifoWriter = null;
let daemonQueue = [];    // fns to call once daemon is ready
let currentJob = null;
const jobs = new Map();

function ensureFifo() {
  if (!existsSync(FIFO_PATH)) {
    exec(`mkfifo ${FIFO_PATH}`, (err) => {
      if (err) console.error('[bridge] mkfifo error:', err.message);
    });
  }
}

function startDaemon() {
  ensureFifo();

  console.log('[bridge] Starting persistent Claude daemon…');

  // Open FIFO for writing (non-blocking so we don't hang waiting for a reader)
  // We open it after Claude opens it for reading
  const proc = spawn(CLAUDE_BIN, [
    '--print',
    '--output-format', 'stream-json',
    '--input-format', 'stream-json',
    '--verbose',
    '--dangerously-skip-permissions',
  ], {
    env: { ...process.env, HOME: '/Users/rsuaan' },
    cwd: '/Users/rsuaan',
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  daemonProc = proc;
  daemonReady = false;
  daemonFifoWriter = proc.stdin;

  // Send a cheap Grafana ping so the MCP OAuth flow completes during warmup,
  // not during the user's first real request.
  const warmup = JSON.stringify({ type: 'user', message: { role: 'user', content: [{ type: 'text', text: 'Use mcp__grafana__list_datasources to list datasources. Just output: READY' }] } });
  proc.stdin.write(warmup + '\n');

  const rl = createInterface({ input: proc.stdout });

  rl.on('line', line => {
    if (!line.trim()) return;
    let msg;
    try { msg = JSON.parse(line); } catch { return; }

    if (msg.type === 'system' && msg.subtype === 'init') {
      daemonReady = true;
      console.log('[bridge] Daemon ready, session:', msg.session_id);
      const q = daemonQueue.splice(0);
      for (const fn of q) fn();
      return;
    }

    // During warmup there's no currentJob — still handle OAuth
    if (!currentJob) {
      if (msg.type === 'assistant' && msg.message?.content) {
        for (const block of msg.message.content) {
          if (block.type === 'text' && block.text) {
            const oauthMatch = block.text.match(/https:\/\/claude-mcp-grafana[^\s)>"'\n]+/);
            if (oauthMatch) {
              console.log('[bridge] Warmup OAuth — opening browser');
              exec(`open "${oauthMatch[0]}"`, () => {});
            }
          }
        }
      }
      return;
    }
    const job = currentJob;

    if (msg.type === 'assistant' && msg.message?.content) {
      for (const block of msg.message.content) {
        if (block.type === 'text' && block.text) {
          job.fullText += block.text;
          if (!job.oauthOpened) {
            const oauthMatch = job.fullText.match(/https:\/\/claude-mcp-grafana[^\s)>"'\n]+/);
            if (oauthMatch) {
              job.oauthOpened = true;
              job.authPending = true;
              broadcast(job, { type: 'auth', message: 'Grafana authentication required — opening browser…' });
              exec(`open "${oauthMatch[0]}"`, () => {});
            }
          }
          for (const ev of parseMarkers(block.text)) {
            if (job.authPending && ev.type === 'progress') {
              job.authPending = false;
              broadcast(job, { type: 'auth_complete' });
            }
            broadcast(job, ev);
          }
        }
      }
    }

    if (msg.type === 'result' && msg.subtype === 'success') {
      for (const ev of parseMarkers(msg.result || '')) broadcast(job, ev);
      finishJob(job);
    }
    if (msg.type === 'result' && msg.subtype === 'error') {
      broadcast(job, { type: 'error', message: msg.result || 'Claude returned an error' });
      finishJob(job);
    }
  });

  proc.stderr.on('data', chunk => {
    const txt = chunk.toString().trim();
    if (txt) console.error('[daemon stderr]', txt);
  });

  proc.on('close', code => {
    console.log(`[bridge] Daemon exited (${code}) — will restart on next request`);
    daemonProc = null;
    daemonReady = false;
    daemonFifoWriter = null;
    currentJob = null;
  });
}

function sendToDaemon(prompt) {
  const msg = JSON.stringify({
    type: 'user',
    message: { role: 'user', content: [{ type: 'text', text: prompt }] },
  });
  daemonFifoWriter.write(msg + '\n');
}

function ensureDaemon(cb) {
  if (daemonProc && daemonReady) { cb(); return; }
  if (daemonProc && !daemonReady) { daemonQueue.push(cb); return; }
  startDaemon();
  daemonQueue.push(cb);
}

function finishJob(job) {
  clearInterval(job.heartbeat);
  broadcast(job, { type: 'done' });
  for (const res of job.clients) { try { res.end(); } catch {} }
  if (currentJob === job) currentJob = null;
  jobs.delete(job.sid);
}

// ── Pcap: separate one-shot process (needs --plugin-dir) ─────────────────────

function startPcapJob(sid) {
  const job = { sid, child: null, events: [], clients: [], oauthOpened: false, authPending: false, fullText: '', heartbeat: null };
  jobs.set(sid + ':pcap', job);
  job.heartbeat = setInterval(() => broadcast(job, { type: 'heartbeat' }), 8000);

  const child = spawn(CLAUDE_BIN, [
    '--print', '--output-format', 'stream-json', '--verbose', '--dangerously-skip-permissions',
    '--plugin-dir', '/Users/rsuaan/voice-media-troubleshooting',
    '-p', `/media-analysis ${sid}`,
  ], { env: { ...process.env, HOME: '/Users/rsuaan' }, cwd: '/Users/rsuaan', stdio: ['ignore', 'pipe', 'pipe'] });

  job.child = child;
  let buf = '';
  child.stdout.on('data', chunk => {
    buf += chunk.toString();
    const lines = buf.split('\n'); buf = lines.pop();
    for (const line of lines) {
      if (!line.trim()) continue;
      let msg; try { msg = JSON.parse(line); } catch { continue; }
      if (msg.type === 'assistant' && msg.message?.content) {
        for (const block of msg.message.content) {
          if (block.type !== 'text' || !block.text) continue;
          job.fullText += block.text;
          if (!job.oauthOpened) {
            const m = job.fullText.match(/https:\/\/claude-mcp-grafana[^\s)>"'\n]+/);
            if (m) { job.oauthOpened = true; broadcast(job, { type: 'auth', message: 'Auth required…' }); exec(`open "${m[0]}"`, () => {}); }
          }
          for (const ev of parseMarkers(block.text)) broadcast(job, ev);
        }
      }
      if (msg.type === 'result') {
        if (msg.subtype === 'success') for (const ev of parseMarkers(msg.result || '')) broadcast(job, ev);
        if (msg.subtype === 'error') broadcast(job, { type: 'error', message: msg.result || 'Error' });
        clearInterval(job.heartbeat);
        broadcast(job, { type: 'done' });
        for (const res of job.clients) { try { res.end(); } catch {} }
        jobs.delete(sid + ':pcap');
      }
    }
  });
  child.stderr.on('data', chunk => console.error('[pcap stderr]', chunk.toString().trim()));
  child.on('close', () => {
    clearInterval(job.heartbeat);
    broadcast(job, { type: 'done' });
    for (const res of job.clients) { try { res.end(); } catch {} }
    jobs.delete(sid + ':pcap');
  });
  return job;
}

// ── HTTP server ───────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }

  if (url.pathname === '/ping') {
    res.writeHead(200, { ...CORS, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, daemonReady }));
    return;
  }

  if (url.pathname === '/stop') {
    const sid = (url.searchParams.get('call_sid') || '').trim();
    if (currentJob && currentJob.sid === sid) {
      clearInterval(currentJob.heartbeat);
      currentJob = null;
      jobs.delete(sid);
    }
    const pcapJob = jobs.get(sid + ':pcap');
    if (pcapJob?.child) pcapJob.child.kill();
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

  res.writeHead(200, { ...CORS, 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });

  if (url.pathname === '/analyse-pcap') {
    let job = jobs.get(sid + ':pcap');
    if (job) { for (const ev of job.events) sse(res, ev); job.clients.push(res); }
    else { job = startPcapJob(sid); job.clients.push(res); sse(res, { type: 'connected' }); }
    req.on('close', () => { job.clients = job.clients.filter(c => c !== res); });
    return;
  }

  // Reconnect to existing job
  const existing = jobs.get(sid);
  if (existing) {
    for (const ev of existing.events) sse(res, ev);
    existing.clients.push(res);
    req.on('close', () => { existing.clients = existing.clients.filter(c => c !== res); });
    return;
  }

  // New job — use daemon
  const job = {
    sid, events: [], clients: [res],
    oauthOpened: false, authPending: false, fullText: '',
    heartbeat: setInterval(() => broadcast(job, { type: 'heartbeat' }), 8000),
  };
  jobs.set(sid, job);
  currentJob = job;
  sse(res, { type: 'connected' });

  ensureDaemon(() => sendToDaemon(`/fork-budget ${sid}`));

  req.on('close', () => { job.clients = job.clients.filter(c => c !== res); });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Fork Budget bridge running on http://127.0.0.1:${PORT}`);
  // Pre-warm daemon on startup
  ensureDaemon(() => console.log('[bridge] Daemon pre-warmed and ready'));
});

import { spawn } from 'child_process';
import http from 'http';

const PORT = 47821;
const CLAUDE_BIN = '/Users/rsuaan/.local/bin/claude';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);

  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS);
    res.end();
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

  child.stdout.on('data', chunk => {
    const lines = chunk.toString().split('\n');
    for (const line of lines) {
      res.write(`data: ${JSON.stringify({ text: line + '\n' })}\n\n`);
    }
  });

  child.stderr.on('data', chunk => {
    res.write(`data: ${JSON.stringify({ error: chunk.toString() })}\n\n`);
  });

  child.on('close', code => {
    res.write(`data: ${JSON.stringify({ done: true, code })}\n\n`);
    res.end();
  });

  req.on('close', () => child.kill());
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`Fork Budget bridge running on http://127.0.0.1:${PORT}`);
});

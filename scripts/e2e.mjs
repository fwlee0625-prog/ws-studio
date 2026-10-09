import { spawn } from 'node:child_process';
import { createReadStream } from 'node:fs';
import { createInterface } from 'node:readline';
import { setTimeout as sleep } from 'node:timers/promises';
import WebSocket, { WebSocketServer } from 'ws';

const PORT = 8798, children = [];
let testSource;
function launch(file, env = {}) {
  const child = spawn(process.execPath, ['node_modules/tsx/dist/cli.mjs', file], {
    cwd: process.cwd(), stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, ...env },
  });
  children.push(child);
  child.stdout.on('data', x => process.stdout.write(x));
  child.stderr.on('data', x => process.stderr.write(x));
}
async function request(path, body) {
  const r = await fetch(`http://127.0.0.1:${PORT}/api/${path}`, body === undefined ? undefined : {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
  });
  const data = await r.json();
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}: ${JSON.stringify(data)}`);
  return data;
}
async function until(fn, timeout = 6000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try { const value = await fn(); if (value) return value; } catch { /* retry */ }
    await sleep(50);
  }
  throw new Error('等待条件超时');
}
async function readFrames(id) {
  const result = [];
  const input = createReadStream(`recordings/${id}.jsonl`);
  const lines = createInterface({ input, crlfDelay: Infinity });
  for await (const line of lines) if (line) result.push(JSON.parse(line));
  return result;
}
async function startTestSource() {
  // An isolated integration-test fixture, started only for the test run.
  const wss = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise((resolve, reject) => {
    wss.once('listening', resolve);
    wss.once('error', reject);
  });
  wss.on('connection', (ws) => {
    let sequence = 0;
    const timer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      ws.send(JSON.stringify({ type: 'test-frame', sequence: ++sequence, value: sequence / 100 }));
    }, 16);
    ws.once('close', () => clearInterval(timer));
    ws.once('error', () => clearInterval(timer));
  });
  return wss;
}
function assert(condition, description) { if (!condition) throw new Error(description); }
async function websocket(path) {
  const ws = new WebSocket(`ws://127.0.0.1:${PORT}/${path}`);
  await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
  return ws;
}
async function main() {
  testSource = await startTestSource();
  const testPort = testSource.address().port;
  launch('src/server.ts', { PORT: String(PORT) });
  await until(() => request('status'));
  const monitor = await websocket('monitor');
  const events = [];
  monitor.on('message', raw => { const e = JSON.parse(raw.toString('utf8')); events.push(e); });
  const existing = await request('sessions');
  await request('connect', { url: `ws://127.0.0.1:${testPort}`, token: '', subscription: '' });
  await until(async () => (await request('status')).upstream.state === 'connected');
  await until(async () => (await request('status')).upstream.received >= 9);
  assert((await request('sessions')).length === existing.length, '仅连接/预览时不应该产生文件');
  await until(() => events.some(e => e.type === 'source-frame'));
  console.log('[PASS] 已连接并接收实时日志；未手动录制时不创建文件');

  const first = await request('record/start', {});
  await until(async () => (await request('status')).recording?.messageCount >= 16);
  const result1 = await request('record/stop', {});
  const saved1 = await readFrames(first.id);
  assert(result1.status === 'complete' && result1.messageCount === saved1.length, '第一次录制统计不正确');
  assert(saved1.length >= 16, '第一次录制条数不足');
  const afterStop = (await request('status')).upstream.received;
  await until(async () => (await request('status')).upstream.received > afterStop + 4);
  assert((await request('status')).recording === null, '停止后录制器仍然存在');
  assert((await readFrames(first.id)).length === saved1.length, '停止后不应该继续保存消息');
  assert((await request('status')).upstream.state === 'connected', '停止录制不应该断开 WS');
  console.log(`[PASS] 手动录制 ${saved1.length} 条；停止后文件不再增长，WS 保持连接`);

  const second = await request('record/start', {});
  await until(async () => (await request('status')).recording?.messageCount >= 12);
  const result2 = await request('record/stop', {});
  const saved2 = await readFrames(second.id);
  assert(result2.messageCount === saved2.length, '第二次录制统计不正确');
  const all = [...saved1, ...saved2];
  const fromIndex = 3, toIndex = 23;
  const expected = all.slice(fromIndex - 1, toIndex);
  const client = await websocket('stream');
  const received = [];
  client.on('message', (raw, isBinary) => received.push({ binary: isBinary, payload: raw.toString('utf8') }));
  const running = await request('push/start', { sessionIds: [first.id, second.id], rate: 150, fromIndex, toIndex });
  assert(running.total === expected.length && running.status === 'pushing', '推送开始状态不正确');
  await until(async () => (await request('status')).push?.status === 'completed', 12000);
  await until(() => received.length >= expected.length);
  assert(received.length === expected.length, '推送条数不正确');
  for (let i = 0; i < expected.length; i++) {
    assert(received[i].binary === expected[i].binary && received[i].payload === expected[i].payload, `推送第 ${i+1} 条不一致`);
  }
  assert(events.some(e => e.type === 'push-frame'), '没有打印推送日志');
  assert(events.some(e => e.type === 'push-ended' && e.data.status === 'completed'), '没有发出完成状态事件');
  console.log(`[PASS] 多文件合并 + 序号范围 + 每秒推送 150 条；推送 ${received.length} 条，原始消息一致，完成状态正确`);

  await request('push/start', { sessionIds: [first.id], rate: 2 });
  await sleep(130);
  const stopped = await request('push/stop', {});
  assert(stopped.status === 'stopped' && stopped.sent < stopped.total, '手动停止推送失败');
  await request('disconnect', {});
  assert((await request('status')).upstream.state === 'disconnected', '手动断开失败');
  client.close(); monitor.close();
  console.log('[PASS] 手动停止推送 + 断开上游连接');
}
try { await main(); }
catch (error) { console.error('[FAIL]', error); process.exitCode = 1; }
finally {
  for (const child of children) child.kill('SIGTERM');
  if (testSource) {
    for (const client of testSource.clients) client.terminate();
    await new Promise((resolve) => testSource.close(resolve));
  }
  await sleep(250);
}

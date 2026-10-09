import 'dotenv/config';
import { randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { createReadStream, createWriteStream, type WriteStream } from 'node:fs';
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { createInterface } from 'node:readline';
import { setTimeout as sleep } from 'node:timers/promises';
import WebSocket, { WebSocketServer, type RawData } from 'ws';

const HOST = process.env.HOST || '127.0.0.1';
const PORT = Number(process.env.PORT || 8787);
const DATA_DIR = join(process.cwd(), 'recordings');
const MAX_RECORD_BUFFER = 32 * 1024 * 1024;
const MAX_CLIENT_BUFFER = 8 * 1024 * 1024;
const MAX_MONITOR_BUFFER = 2 * 1024 * 1024;
const MAX_RATE = 2000;

interface StoredFrame { offsetMs: number; binary: boolean; payload: string }
type RecordingResult = 'complete' | 'interrupted' | 'error';
interface SessionMeta {
  id: string;
  startedAt: string;
  endedAt: string;
  durationMs: number;
  messageCount: number;
  bytes: number;
  status: RecordingResult;
  reason?: string;
}
interface Upstream {
  ws: WebSocket;
  url: string;
  state: 'connecting' | 'connected';
  received: number;
  connectedAt: string | null;
  lastMessageAt: string | null;
}
interface Recorder {
  id: string;
  file: WriteStream;
  began: number;
  startedAt: string;
  count: number;
  bytes: number;
  finished: boolean;
  task?: Promise<SessionMeta>;
}
type PushState = 'pushing' | 'completed' | 'stopped' | 'error';
interface PushInfo {
  status: PushState;
  sessionIds: string[];
  rate: number;
  total: number;
  sent: number;
  delivered: number;
  startedAt: string;
  endedAt?: string;
  error?: string;
}
interface PushJob { controller: AbortController; info: PushInfo; task?: Promise<void> }

let upstream: Upstream | null = null;
let recorder: Recorder | null = null;
let pushJob: PushJob | null = null;
let lastPush: PushInfo | null = null;
const streamWss = new WebSocketServer({ noServer: true, perMessageDeflate: false });
const monitorWss = new WebSocketServer({ noServer: true, perMessageDeflate: false });
const server = createServer((req, res) => { void route(req, res); });

server.on('upgrade', (req, socket, head) => {
  const path = new URL(req.url || '/', 'http://localhost').pathname;
  const target = path === '/stream' ? streamWss : path === '/monitor' ? monitorWss : null;
  if (!target) { socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); socket.destroy(); return; }
  // Control dashboard's monitoring channel is restricted to this origin.
  if (path === '/monitor' && req.headers.origin) {
    const expected = `http://${req.headers.host}`;
    if (req.headers.origin !== expected) { socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return; }
  }
  target.handleUpgrade(req, socket, head, (ws) => target.emit('connection', ws, req));
});
for (const wss of [streamWss, monitorWss]) {
  wss.on('connection', (ws) => ws.on('error', () => {}));
}

function response(res: ServerResponse, status: number, value: unknown): void {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(value));
}
function httpError(message: string, status = 400): Error & { status: number } {
  return Object.assign(new Error(message), { status });
}
async function parseBody(req: IncomingMessage): Promise<Record<string, unknown>> {
  let raw = '';
  for await (const part of req) {
    raw += part.toString();
    if (raw.length > 64_000) throw httpError('请求数据过大', 413);
  }
  if (!raw) return {};
  let parsed: unknown;
  try { parsed = JSON.parse(raw); } catch { throw httpError('请求必须是有效 JSON'); }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw httpError('请求体必须是 JSON 对象');
  return parsed as Record<string, unknown>;
}
function validId(id: unknown): id is string {
  return typeof id === 'string' && /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.test(id);
}
function recordFile(id: string): string { return join(DATA_DIR, `${id}.jsonl`); }
function metaFile(id: string): string { return join(DATA_DIR, `${id}.json`); }
function asBuffer(data: RawData): Buffer { return Array.isArray(data) ? Buffer.concat(data) : Buffer.from(data); }
function status() {
  return {
    upstream: upstream ? {
      state: upstream.state, url: upstream.url, received: upstream.received,
      connectedAt: upstream.connectedAt, lastMessageAt: upstream.lastMessageAt,
    } : { state: 'disconnected', url: '', received: 0, connectedAt: null, lastMessageAt: null },
    recording: recorder ? {
      id: recorder.id, startedAt: recorder.startedAt, elapsedMs: Math.round(performance.now() - recorder.began),
      messageCount: recorder.count, bytes: recorder.bytes,
    } : null,
    push: lastPush,
    streamClients: [...streamWss.clients].filter((ws) => ws.readyState === WebSocket.OPEN).length,
    monitorClients: monitorWss.clients.size,
  };
}
function monitor(type: string, value: unknown): void {
  if (monitorWss.clients.size === 0) return;
  const data = JSON.stringify({ type, data: value });
  for (const ws of monitorWss.clients) {
    if (ws.readyState === WebSocket.OPEN && ws.bufferedAmount < MAX_MONITOR_BUFFER) ws.send(data);
  }
}
function monitorState(): void { monitor('state', status()); }
function preview(payload: Buffer, isBinary: boolean): string {
  return isBinary ? `[二进制 ${payload.length} B] ${payload.subarray(0, 48).toString('hex')}${payload.length > 48 ? '…' : ''}`
    : (payload.length > 4000 ? payload.toString('utf8').slice(0, 4000) + '… [日志截断，文件完整保存]' : payload.toString('utf8'));
}
async function sessions(): Promise<SessionMeta[]> {
  const files = await readdir(DATA_DIR);
  const entries = await Promise.all(files.filter((f) => /^[0-9a-f-]{36}\.json$/.test(f)).map(async (file) => {
    try { return JSON.parse(await readFile(join(DATA_DIR, file), 'utf8')) as SessionMeta; }
    catch { return null; }
  }));
  return entries.filter((x): x is SessionMeta => !!x).sort((a, b) => (b.startedAt || b.endedAt || '').localeCompare(a.startedAt || a.endedAt || ''));
}
async function loadSession(id: string): Promise<SessionMeta> {
  return JSON.parse(await readFile(metaFile(id), 'utf8')) as SessionMeta;
}
function closeUpstream(): void {
  if (!upstream) return;
  const old = upstream;
  upstream = null;
  if (old.ws.readyState === WebSocket.CONNECTING) old.ws.terminate();
  else if (old.ws.readyState === WebSocket.OPEN) old.ws.close(1000, 'Manual disconnect');
  monitor('system', { level: 'info', text: '已断开上游 WebSocket' });
  monitorState();
}
function connectUpstream(urlText: unknown, token: unknown, subscription: unknown): void {
  if (upstream) throw httpError('请先断开当前连接', 409);
  if (typeof urlText !== 'string' || urlText.length > 4096) throw httpError('请输入有效的 WebSocket 地址');
  let url: URL;
  try { url = new URL(urlText); } catch { throw httpError('WebSocket URL 无效'); }
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') throw httpError('地址必须以 ws:// 或 wss:// 开头');
  if (typeof token !== 'string' || token.length > 8192) throw httpError('Token 参数无效');
  if (typeof subscription !== 'string' || subscription.length > 16000) throw httpError('订阅消息无效');
  // URL may include secrets in query parameters; do not expose it in status or files.
  const safeAddress = `${url.protocol}//${url.host}${url.pathname}`;
  const headers: Record<string, string> = {};
  if (token) headers.Authorization = token.startsWith('Bearer ') ? token : `Bearer ${token}`;
  const ws = new WebSocket(url, { headers, perMessageDeflate: false, handshakeTimeout: 12000, maxPayload: 16 * 1024 * 1024 });
  const item: Upstream = { ws, url: safeAddress, state: 'connecting', received: 0, connectedAt: null, lastMessageAt: null };
  upstream = item;
  monitorState();
  ws.on('open', () => {
    if (upstream !== item) return;
    item.state = 'connected'; item.connectedAt = new Date().toISOString();
    monitor('system', { level: 'success', text: '上游 WS 连接成功，开始实时接收数据（尚未录制）' });
    if (subscription) ws.send(subscription);
    monitorState();
  });
  ws.on('message', (raw, isBinary) => {
    if (upstream !== item) return;
    const payload = asBuffer(raw);
    const at = new Date().toISOString();
    item.received += 1; item.lastMessageAt = at;
    // Monitor only sends truncated previews; recording always stores the full original frame.
    monitor('source-frame', { sequence: item.received, at, binary: isBinary, bytes: payload.length, preview: preview(payload, isBinary) });
    const active = recorder;
    if (!active || active.finished) return;
    const row: StoredFrame = {
      offsetMs: Number((performance.now() - active.began).toFixed(3)),
      binary: isBinary,
      payload: isBinary ? payload.toString('base64') : payload.toString('utf8'),
    };
    active.file.write(`${JSON.stringify(row)}\n`);
    active.count++; active.bytes += payload.length;
    if (active.file.writableLength > MAX_RECORD_BUFFER) {
      void stopRecording('error', '磁盘写入跟不上数据速度，缓存已达到 32 MB');
    }
  });
  ws.on('error', (error) => {
    if (upstream !== item) return;
    monitor('system', { level: 'error', text: `上游 WS 错误：${error.message}` });
  });
  ws.on('close', (code) => {
    if (upstream !== item) return;
    upstream = null;
    if (recorder) void stopRecording('interrupted', `上游 WS 已断开 (code ${code})`);
    monitor('system', { level: 'error', text: `上游连接断开 (code ${code})` });
    monitorState();
  });
}
function startRecording(): { id: string; status: string } {
  if (!upstream || upstream.state !== 'connected') throw httpError('请先连接真实 WebSocket', 409);
  if (recorder) throw httpError('正在录制中，请先停止', 409);
  const id = randomUUID();
  const file = createWriteStream(recordFile(id), { flags: 'wx', encoding: 'utf8' });
  const active: Recorder = { id, file, began: performance.now(), startedAt: new Date().toISOString(), count: 0, bytes: 0, finished: false };
  recorder = active;
  file.on('error', (error) => {
    console.error('[record/file]', error.message);
    if (!active.finished) void stopRecording('error', `写入失败：${error.message}`);
  });
  monitor('system', { level: 'success', text: '已开始录制；只有从此刻起收到的消息才会保存' });
  monitorState();
  return { id, status: 'recording' };
}
async function stopRecording(result: RecordingResult = 'complete', reason?: string): Promise<SessionMeta> {
  const current = recorder;
  if (!current) throw httpError('当前没有正在进行的录制', 409);
  if (current.task) return current.task;
  current.finished = true;
  recorder = null; // Immediately stop accepting new frames, connection stays alive.
  const durationMs = Math.round(performance.now() - current.began);
  current.task = (async () => {
    try {
      if (!current.file.destroyed) {
        await new Promise<void>((resolve, reject) => {
          current.file.once('finish', resolve);
          current.file.once('error', reject);
          current.file.end();
        });
      }
    } catch (error) {
      result = 'error';
      reason = error instanceof Error ? error.message : String(error);
    }
    const meta: SessionMeta = {
      id: current.id, startedAt: current.startedAt, endedAt: new Date().toISOString(),
      durationMs, messageCount: current.count, bytes: current.bytes, status: result,
      ...(reason ? { reason } : {}),
    };
    await writeFile(metaFile(current.id), JSON.stringify(meta, null, 2));
    monitor('record-ended', meta);
    monitor('system', { level: result === 'complete' ? 'success' : 'error', text: `录制${result === 'complete' ? '完成' : '中断'}，保存 ${meta.messageCount} 条数据` });
    monitorState();
    return meta;
  })();
  monitorState();
  return current.task;
}
function broadcast(frame: StoredFrame): number {
  const data = frame.binary ? Buffer.from(frame.payload, 'base64') : frame.payload;
  let clients = 0;
  for (const client of streamWss.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    if (client.bufferedAmount > MAX_CLIENT_BUFFER) { client.close(1013, 'Slow client'); continue; }
    client.send(data, { binary: frame.binary });
    clients++;
  }
  return clients;
}
async function pushFrames(job: PushJob, fromIndex: number, toIndex: number): Promise<void> {
  const { signal } = job.controller;
  const info = job.info;
  const began = performance.now();
  let globalIndex = 0;
  try {
    for (const id of info.sessionIds) {
      if (signal.aborted) break;
      const input = createReadStream(recordFile(id), { encoding: 'utf8', signal });
      const reader = createInterface({ input, crlfDelay: Infinity });
      try {
        for await (const line of reader) {
          if (signal.aborted) break;
          if (!line) continue;
          globalIndex++;
          if (globalIndex < fromIndex) continue;
          if (globalIndex > toIndex) break;
          const item = JSON.parse(line) as StoredFrame;
          const target = began + (info.sent * 1000 / info.rate);
          const delay = target - performance.now();
          if (delay > 0) await sleep(delay, undefined, { signal });
          if (signal.aborted) break;
          const clients = broadcast(item);
          info.sent++;
          info.delivered += clients;
          // Avoid flooding the dashboard when sending thousands of messages per second.
          const sampleEvery = Math.max(1, Math.ceil(info.rate / 80));
          if (info.sent === 1 || info.sent === info.total || info.sent % sampleEvery === 0) {
            const bytes = item.binary ? Buffer.from(item.payload, 'base64') : Buffer.from(item.payload, 'utf8');
            monitor('push-frame', {
              at: new Date().toISOString(), index: globalIndex, sent: info.sent,
              clients, bytes: bytes.length, preview: preview(bytes, item.binary),
            });
          }
          if (info.sent % Math.max(1, Math.ceil(info.rate / 4)) === 0) monitorState();
        }
      } finally { reader.close(); input.destroy(); }
      if (globalIndex >= toIndex) break;
    }
    if (!signal.aborted && info.sent !== info.total) throw new Error(`回放数据不完整：应发送 ${info.total} 条，实际 ${info.sent} 条`);
    info.status = signal.aborted ? 'stopped' : 'completed';
  } catch (error) {
    if (signal.aborted) info.status = 'stopped';
    else { info.status = 'error'; info.error = error instanceof Error ? error.message : String(error); }
  } finally {
    info.endedAt = new Date().toISOString();
    if (pushJob === job) pushJob = null;
    monitor('push-ended', info);
    monitorState();
  }
}
async function startPush(body: Record<string, unknown>): Promise<PushInfo> {
  if (pushJob) throw httpError('已有进行中的推送任务，请先停止', 409);
  if (!Array.isArray(body.sessionIds) || !body.sessionIds.length || body.sessionIds.length > 100 ||
      !body.sessionIds.every(validId) || new Set(body.sessionIds).size !== body.sessionIds.length) {
    throw httpError('请至少选择一份有效的录制数据，且不能重复');
  }
  const ids = body.sessionIds as string[];
  if (typeof body.rate !== 'number' || !Number.isInteger(body.rate) || body.rate < 1 || body.rate > MAX_RATE) {
    throw httpError(`每秒推送条数必须是 1～${MAX_RATE} 的整数`);
  }
  const metas = await Promise.all(ids.map(loadSession));
  const count = metas.reduce((sum, meta) => sum + meta.messageCount, 0);
  if (!count) throw httpError('选中的录制文件没有数据');
  const fromIndex = body.fromIndex ?? 1;
  const toIndex = body.toIndex ?? count;
  if (typeof fromIndex !== 'number' || typeof toIndex !== 'number' ||
      !Number.isInteger(fromIndex) || !Number.isInteger(toIndex) ||
      fromIndex < 1 || fromIndex > toIndex || toIndex > count) {
    throw httpError(`数据范围必须满足 1 ≤ 起始条数 ≤ 结束条数 ≤ ${count}`);
  }
  const info: PushInfo = {
    status: 'pushing', sessionIds: ids, rate: body.rate, total: toIndex - fromIndex + 1,
    sent: 0, delivered: 0, startedAt: new Date().toISOString(),
  };
  const job: PushJob = { controller: new AbortController(), info };
  pushJob = job; lastPush = info;
  monitor('system', { level: 'success', text: `开始推送，共 ${info.total} 条，目标频率 ${info.rate} 条/秒` });
  monitorState();
  job.task = pushFrames(job, fromIndex, toIndex);
  return info;
}
async function stopPush(): Promise<PushInfo> {
  if (!pushJob) throw httpError('当前没有进行中的推送任务', 409);
  const current = pushJob;
  current.controller.abort();
  await current.task;
  return current.info;
}

async function route(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url || '/', 'http://localhost');
  const path = url.pathname;
  try {
    if (req.method === 'GET' && (path === '/' || path === '/record' || path === '/push')) {
      const html = await readFile(join(process.cwd(), 'public/index.html'));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
      res.end(html); return;
    }
    if (req.method === 'GET' && path === '/api/status') { response(res, 200, status()); return; }
    if (req.method === 'GET' && path === '/api/sessions') { response(res, 200, await sessions()); return; }
    if (req.method === 'GET' && path.startsWith('/api/sessions/') && path.endsWith('/sample')) {
      const id = path.split('/')[3];
      if (!validId(id)) throw httpError('录制文件 ID 无效');
      await loadSession(id);
      const items: Array<{ index: number; binary: boolean; preview: string }> = [];
      const input = createReadStream(recordFile(id), { encoding: 'utf8' });
      const reader = createInterface({ input, crlfDelay: Infinity });
      try {
        for await (const line of reader) {
          if (!line) continue;
          const f = JSON.parse(line) as StoredFrame;
          const value = f.binary ? Buffer.from(f.payload, 'base64') : Buffer.from(f.payload, 'utf8');
          items.push({ index: items.length + 1, binary: f.binary, preview: preview(value, f.binary) });
          if (items.length === 5) break;
        }
      } finally { reader.close(); input.destroy(); }
      response(res, 200, items); return;
    }
    if (req.method !== 'POST') { response(res, 404, { error: '接口不存在' }); return; }
    // Only dashboard's own origin can make mutating requests in the browser.
    if (req.headers.origin && req.headers.origin !== `http://${req.headers.host}`) throw httpError('不允许跨站操作', 403);
    const body = await parseBody(req);
    if (path === '/api/connect') {
      connectUpstream(body.url, body.token ?? '', body.subscription ?? '');
      response(res, 202, { state: 'connecting' }); return;
    }
    if (path === '/api/disconnect') {
      if (recorder) await stopRecording('interrupted', '手动断开上游 WS');
      closeUpstream(); response(res, 200, status()); return;
    }
    if (path === '/api/record/start') { response(res, 201, startRecording()); return; }
    if (path === '/api/record/stop') { response(res, 200, await stopRecording()); return; }
    if (path === '/api/push/start') { response(res, 202, await startPush(body)); return; }
    if (path === '/api/push/stop') { response(res, 200, await stopPush()); return; }
    response(res, 404, { error: '接口不存在' });
  } catch (error) {
    const code = (error as { status?: number }).status;
    const message = error instanceof Error ? error.message : String(error);
    if (!code && (message.includes('ENOENT') || message.includes('no such file'))) { response(res, 404, { error: '录制文件不存在' }); return; }
    console.error('[http]', message);
    response(res, code || 500, { error: message });
  }
}

monitorWss.on('connection', (ws) => { ws.send(JSON.stringify({ type: 'state', data: status() })); });
await mkdir(DATA_DIR, { recursive: true });
server.listen(PORT, HOST, () => {
  console.log(`[server] 控制台 http://${HOST}:${PORT} | 推送 WS ws://${HOST}:${PORT}/stream`);
});

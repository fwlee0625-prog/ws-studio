import { WebSocketServer, WebSocket } from 'ws';

const MOCK_PORT = Number(process.env.MOCK_PORT || 9001);
const wss = new WebSocketServer({ host: '127.0.0.1', port: MOCK_PORT });
console.log(`[mock] ws://127.0.0.1:${MOCK_PORT} 已启动，每约 16ms 推送一帧示例坐标`);

wss.on('connection', (ws) => {
  let frame = 0;
  const timer = setInterval(() => {
    if (ws.readyState !== WebSocket.OPEN) return;
    const t = frame++ / 60;
    const payload = JSON.stringify({
      type: 'BALL_COORDINATES',
      timestamp: Date.now(),
      frame,
      balls: [
        { id: 'cue', x: Number((0.45 + 0.34 * Math.sin(t)).toFixed(5)), y: 0.5 },
        { id: 'target', x: 0.72, y: 0.52 }
      ]
    });
    ws.send(payload);
  }, 16);
  ws.on('close', () => clearInterval(timer));
  ws.on('error', () => clearInterval(timer));
});

// Vue 3 页面中可直接使用的最小连接示例。
// 前端接收的 text frame 和真实上游传来的 text frame 一致（不额外包裹）。
import { onMounted, onUnmounted, ref } from 'vue';

export function useCoordinatesSocket() {
  const connected = ref(false);
  const frameCount = ref(0);
  const lastFrame = ref<unknown>(null);
  let socket: WebSocket | null = null;

  onMounted(() => {
    socket = new WebSocket(import.meta.env.VITE_COORDS_WS_URL);
    socket.onopen = () => { connected.value = true; };
    socket.onclose = () => { connected.value = false; };
    socket.onmessage = (event: MessageEvent) => {
      frameCount.value++;
      if (typeof event.data === 'string') {
        try { lastFrame.value = JSON.parse(event.data); }
        catch { lastFrame.value = event.data; }
      } else {
        lastFrame.value = event.data; // 二进制消息：交给现有解析器处理
      }
    };
  });
  onUnmounted(() => socket?.close());
  return { connected, frameCount, lastFrame };
}

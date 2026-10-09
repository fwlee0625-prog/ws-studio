<p align="center"><img src="./public/logo.svg" alt="WS Studio Logo" width="100" height="100"></p>

# WS Studio —— WebSocket 数据录制与推送（V2）

一个供开发与联调使用的 **Node.js + TypeScript + ws** 本地工具。它不需要数据库，也不要求修改真实业务后端。专门解决“前端需要真实坐标数据，但不想每次都依赖业务后端持续推送”的问题。

## 功能一：数据录制页面

- 手动输入 `ws://` / `wss://` 地址，点击 **连接服务**。
- 可选填写 Bearer Token（通过服务端握手请求头传递）和“连接后发送订阅消息”。
- 连接成功之后，上游消息**立即在实时接收日志中显示，此时不会录制**。
- 点击 **开始录制**：才创建文件并开始保存随后收到的每条原始 WS 消息。
- 点击 **停止并保存**：停止写入文件，同时仍**保持 WS 连接**，继续查看实时数据。
- 可手动断开上游 WS；如录制时意外断线，则保存已录制数据，并标记为 `interrupted`。

## 功能二：数据推送页面

- 勾选一份或多份历史录制文件，按页面列表顺序依次推送。
- 选择合并数据中的消息编号范围（例如从第 101 条到第 500 条），留空结束编号表示全部。
- 输入 **每秒推送条数**（1～2000 条/秒），手动点击 **开始推送**。
- 显示 `正在推送中 / 推送已完成 / 推送已手动停止 / 推送失败`、进度、已发送条数、接收客户端数。
- 实时推送日志打印条数、消息摘要和该条被发送给多少个客户端。高速率会仅对日志抽样，**不会减少推送的真实消息数量**。
- 点击 **停止推送** 即可提前结束。
- 推送 WS 地址 `ws://127.0.0.1:8787/stream`，可被任意多个前端页面连接。

**说明：** 本版本是“固定每秒条数”推送，而不是保持录制时的原始时间间隔。推送负载保持原始类型（文本 / 二进制）及原始内容；`/monitor` 是管理台的独立日志通道，不会污染业务数据。

## 本地启动

安装 Node.js >= 20 后：

```bash
npm install
cp .env.example .env
npm run dev
```

浏览器打开 **http://127.0.0.1:8787**。

要在没有真实业务后端的情况下演示：

```bash
# 在第二个终端启动演示坐标源，每约 16ms 推送一帧
npm run mock
```

录制页面输入 `ws://127.0.0.1:9001`，连接成功后即可看到示例台球坐标 JSON。点击“开始录制”，运行几秒后点击“停止并保存”，再切换“数据推送”页面选择录制文件。

## Vue 3 项目连接

在 Vue 开发环境（`.env.development`）中填写：

```env
VITE_COORDS_WS_URL=ws://127.0.0.1:8787/stream
```

或者直接调用浏览器 WebSocket：

```ts
const ws = new WebSocket(import.meta.env.VITE_COORDS_WS_URL);
ws.onmessage = (event) => {
  // event.data 与录制时上游推送的原始消息一致
  const data = JSON.parse(event.data);
  console.log('收到台球坐标：', data);
};
```

若原始上游发送的是二进制数据，请在 `new WebSocket()` 之后设置 `ws.binaryType = 'arraybuffer'`，并按照原协议解码，不要使用 `JSON.parse`。

## 目录结构

```text
ws-recorder-replay/
├── public/index.html        # 两页完整可视化管理界面（原生 HTML/CSS/JS）
├── src/server.ts            # 上游 WS / 录制器 / 推送器 / REST API / 监控 WS
├── src/mock-source.ts       # 60FPS 左右的模拟台球坐标源
├── scripts/e2e.mjs          # 端到端行为测试
├── examples/vue-client.ts   # Vue 消费端最小示例
├── .env.example
├── package.json
└── recordings/             # 首次启动自动创建；文件不会进入 Git
    ├── <录制ID>.jsonl        # 原始数据 + 相对时间（逐行持久化）
    └── <录制ID>.json         # 录制起止时间、数量、总字节数与状态
```

每行 JSONL 数据结构：

```json
{"offsetMs":16.683,"binary":false,"payload":"{\"type\":\"BALL_COORDINATES\",\"balls\":[]}"}
```

字段 `offsetMs` 是录制开始后的时间差，记录以备后续需要“原始节奏回放”时复用；**本版推送只按每秒条数调度**。二进制 `payload` 在文件中用 Base64 编码，推送时还原成二进制帧。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/connect` | `{ "url": "ws://...", "token": "可选", "subscription": "可选" }`，开始建立上游连接 |
| POST | `/api/disconnect` | 主动断开上游连接 |
| GET | `/api/status` | 查看上游状态、当前录制、最近一次推送及连接客户端数 |
| POST | `/api/record/start` | `{}`，开始手动录制，需要先连接上游 |
| POST | `/api/record/stop` | `{}`，停止并保存，但不断开上游 |
| GET | `/api/sessions` | 列出历史录制文件 |
| GET | `/api/sessions/:id/sample` | 预览文件前 5 条消息 |
| POST | `/api/push/start` | `{ "sessionIds": ["UUID1", "UUID2"], "rate": 30, "fromIndex": 1, "toIndex": 500 }`，`toIndex` 可省略 |
| POST | `/api/push/stop` | `{}`，停止当前推送 |
| WS | `/monitor` | 管理页面用的状态和消息日志（独立 JSON 信封） |
| WS | `/stream` | Vue / 其他消费端用的业务消息流，原样推送 |

服务默认监听 `127.0.0.1`，管理界面的写入 API 与监控 WS 拒绝来自其他浏览器 Origin 的操作。此工具只供本地开发调试；如要向局域网其他设备开放，应另行加入认证和 HTTPS，**不要把 Token 或内部坐标数据直接暴露到公网**。

## 运行验证

```bash
npm run check
npm run test:e2e
```

E2E 检查：连接后仅预览、不生成文件；开始 / 停止录制；停止录制后上游连接保持；多文件按范围推送且数据内容不变；推送完成状态与日志；手动停止推送；手动断开连接。

> 注：`npm run dev` 使用 watch 模式，开发时修改源码会使 Node 进程重启，当前 WS 连接会断开；已经停止保存的录制文件不受影响。

## 发布到 GitHub

仓库默认名称为 `ws-studio-v2`，默认创建为**私有仓库**，并且不会上传 `.env`、`recordings/` 或 `node_modules/`。

首先安装 [GitHub CLI](https://cli.github.com/) 并登录：

```bash
gh auth login
```

然后在项目根目录运行：

```bash
bash scripts/publish-github.sh
```

脚本将初始化本地 Git 仓库（如果还没有）、生成首次提交、创建 GitHub 仓库并推送到 `main`。如果同名仓库已存在，则尝试将本地代码推送到已有仓库。可以通过环境变量 `GITHUB_REPO`、`GITHUB_OWNER` 和 `GITHUB_VISIBILITY` 调整目标，例如：

```bash
GITHUB_REPO=ws-studio GITHUB_VISIBILITY=public bash scripts/publish-github.sh
```

也可以手动在 [GitHub 新建仓库](https://github.com/new) 后，通过 `git remote add origin <仓库地址>` 和 `git push -u origin main` 推送。

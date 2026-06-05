# Mooncake NDS KVCache 前缀命中性能 Demo 设计文档

## 1. 目标

演示 NDS 版本 Mooncake 对 PD 分离场景下多轮对话性能的影响。核心对比：

- **冷启动 (Cold Start)**：首次输入 prompt，Prefiller 执行全量 prefill 计算，KVCache 写入 Mooncake NDS 盘框，TTFT 较长。
- **缓存命中 (Cache Hit)**：再次输入相同 prompt，LMCache 检测到 Mooncake 中已有 KVCache 前缀全量命中，Prefiller 跳过 recompute，TTFT 显著缩短。

用户在 Web 页面输入 prompt，Demo 自动执行两轮对比，实时流式展示生成内容和延迟指标，并将每次对比记录持久化到 JSON 文件。

## 2. 核心原理

### 2.1 LMCache KVCache Key 机制

LMCache 将 prompt 的 token 序列按 `chunk_size` (默认 256 tokens) 分块存储到 MooncakeDistributedStore。每个 KV chunk 的 key 由以下要素决定：

```
key = hash(PYTHONHASHSEED + model_name + layer_id + token_chunk_ids)
```

使用 `/v1/completions` API 时，每个请求天然独立（无对话历史概念）。相同 prompt 文本 → 相同 token IDs → 相同 key → Mooncake 中 KV chunk 全量命中。

### 2.2 缓存命中判定流程

LMCache 的 `MooncakestoreConnector.batched_async_contains()` 逐 chunk 检查 Mooncake Store：

```python
for key in keys:
    if not self.store.is_exist(key.to_string()):
        break          # 前缀不连续则中止
    num_hit_counts += 1
```

返回命中 chunk 数量。全量命中时 Prefiller 跳过所有 token 的 prefill 计算，直接从 Mooncake NDS 盘框读取 KV。

### 2.3 PD 分离数据流

```
第1次请求 (Cold Start):
  Browser → Demo Backend → Proxy(9100) → Prefiller(7100)
    Prefiller: 全量 prefill → LMCache → MooncakestoreConnector → store.batch_put_from → NDS盘框
    Prefiller 返回 kv_transfer_params
  Proxy → Decoder(7200)
    Decoder: LMCache → MooncakestoreConnector → store.batch_get_into → 从NDS读取KV → 生成输出
  Proxy → Demo Backend → Browser (SSE流式)

第2次请求 (Cache Hit):
  Browser → Demo Backend → Proxy(9100) → Prefiller(7100)
    Prefiller: LMCache → batched_async_contains → 全量命中 → 跳过recompute
    Prefiller 返回 kv_transfer_params
  Proxy → Decoder(7200)
    Decoder: 从Mooncake读取KV → 生成输出
  Proxy → Demo Backend → Browser (SSE流式)
```

### 2.4 "删除历史" 的含义

在 `/v1/completions` API 中，每个请求的 `prompt` 字段就是完整的输入文本，不存在对话上下文叠加。所谓"删除历史"即发送一个全新的 completions 请求，`prompt` 字段与第1次完全相同。由于 LMCache key 与对话 session 无关，仅依赖 token 序列，因此第2次请求的 key 与第1次完全一致，保证 Mooncake NDS 缓存命中。

## 3. 系统架构

### 3.1 组件链路

```
┌──────────┐  SSE   ┌──────────────┐  SSE   ┌──────────┐  HTTP   ┌───────────┐
│ Browser  │◄──────►│ Demo Backend │◄──────►│  Proxy   │◄──────►│ Prefiller │
│ (9200)   │        │  (9200)      │        │ (9100)   │        │  (7100)   │
└──────────┘        └──────────────┘        └──────────┘        └───────────┘
                                              │  HTTP
                                              ▼
                                          ┌───────────┐
                                          │  Decoder   │
                                          │  (7200)   │
                                          └───────────┘
                                              │  RDMA
                                              ▼
                                          ┌───────────┐
                                          │  Mooncake │
                                          │  Master   │
                                          │ (50051)   │
                                          │  + NDS    │
                                          └───────────┘
```

### 3.2 文件结构

```
mooncake-nds-pd-demo/
├── demo_server.py              # FastAPI 后端 (226行)
│   功能: SSE流式对比 + TTFT测量 + JSON持久化 + 历史API
│
├── static/
│   └── index.html              # 自包含前端 (455行)
│   功能: 对比卡片 + SSE实时渲染 + 延迟柱状图 + 历史表格
│
├── configs/
│   ├── lmcache-prefiller-config.yaml  # Demo 专用 Prefiller LMCache 配置 (L2 绕过)
│   └── lmcache-decoder-config.yaml   # Demo 专用 Decoder LMCache 配置 (L2 绕过)
│
├── data/
│   └── latency_records.json    # 持久化记录 (运行时自动创建)
│
└── start_demo.sh               # 启动脚本 (43行)
```

## 4. 后端 API 规范 (`demo_server.py`)

### 4.1 端点列表

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 返回 `static/index.html` |
| POST | `/api/stream-comparison` | SSE 流式对比端点，接收 prompt 参数，推送两轮对比事件 |
| GET | `/api/history` | 返回 `latency_records.json` 全部历史记录 |
| DELETE | `/api/history` | 清空历史记录 |
| GET | `/health` | Proxy 健康检查 |

### 4.2 POST `/api/stream-comparison`

**请求体**：

```json
{
  "prompt": "Explain the concept of...",
  "model": "Qwen2.5-72B",
  "max_tokens": 100
}
```

**响应**：`Content-Type: text/event-stream`，SSE 事件流。

### 4.3 SSE 事件协议

所有事件格式为标准 SSE：`data: {JSON}\n\n`

| 事件类型 | 字段 | 说明 | 发送时机 |
|---------|------|------|---------|
| `run1_start` | `session_id` | 开始第1轮 (冷启动) | 向 Proxy 发送第1个请求前 |
| `run1_ttft` | `ttft` (秒) | 第1轮首 token 时间 | 从 Proxy SSE 流解析到首个 content token 时 |
| `run1_token` | `content` (字符串) | 第1轮每个生成 token | 从 Proxy SSE 流逐 token 推送 |
| `run1_done` | `ttft`, `total_time`, `completion_tokens` | 第1轮结束 | Proxy SSE 流收到 `[DONE]` 后 |
| `run2_start` | — | 开始第2轮 (缓存命中) | 第1轮结束后 |
| `run2_ttft` | `ttft` (秒) | 第2轮首 token 时间 | 从 Proxy SSE 流解析到首个 content token 时 |
| `run2_token` | `content` (字符串) | 第2轮每个生成 token | 从 Proxy SSE 流逐 token 推送 |
| `run2_done` | `ttft`, `total_time`, `completion_tokens` | 第2轮结束 | Proxy SSE 流收到 `[DONE]` 后 |
| `comparison` | `speedup`, `ttft_reduction_pct` | 对比计算结果 | 两轮结束后计算 |
| `done` | — | 全部完成 | 持久化写入后 |

**计算公式**：

```
speedup = ttft1 / ttft2
ttft_reduction_pct = (1 - ttft2 / ttft1) * 100
```

### 4.4 TTFT 测量逻辑

后端 `_stream_single_run()` 函数向 Proxy 发送 `POST /v1/completions` (stream=True)，解析 SSE 响应：

```python
start_time = time.monotonic()

# 遍历 SSE 行:
for line in response.aiter_lines():
    chunk = json.loads(data_str)
    content = chunk["choices"][0].get("text", "")
    if content and first_token_time is None:
        first_token_time = time.monotonic()
        yield {"type": f"{run_label}_ttft", "ttft": round(first_token_time - start_time, 3)}

# 流结束:
ttft = round(first_token_time - start_time, 3)
total_time = round(time.monotonic() - start_time, 3)
```

- `ttft`：从请求发出到收到首个 content token 的时间，包含 Proxy → Prefiller → Mooncake → Decoder 全链路延迟。
- `total_time`：从请求发出到流式响应结束的总时间。

### 4.5 请求 payload 结构

向 Proxy 发送的 completions 请求：

```json
{
  "model": "Qwen2.5-72B",
  "prompt": "<用户输入的prompt>",
  "max_tokens": 100,
  "stream": true,
  "temperature": 0.0
}
```

Proxy 内部流程：Prefiller (max_tokens=1, stream=False, kv_transfer_params) → Decoder (stream=True, kv_transfer_params) → SSE 流式返回。

## 5. 持久化记录格式

### 5.1 存储文件

`data/latency_records.json` — JSON 数组，每个 session 一条记录。

### 5.2 记录结构

```json
[
  {
    "session_id": "a1b2c3",
    "timestamp": "2026-06-04T15:30:00",
    "prompt": "Explain the concept of...",
    "prompt_chars": 156,
    "model": "Qwen2.5-72B",
    "max_tokens": 100,
    "cold_start": {
      "ttft": 5.23,
      "total_time": 8.51,
      "completion_tokens": 100
    },
    "cache_hit": {
      "ttft": 0.15,
      "total_time": 2.10,
      "completion_tokens": 100
    },
    "comparison": {
      "speedup": 34.87,
      "ttft_reduction_pct": 97.13
    }
  }
]
```

### 5.3 写入逻辑

```python
def append_session(session: dict):
    records = load_records()      # 读取现有 JSON
    records.append(session)       # append 新记录
    save_records(records)         # 写回文件
```

文件不存在时自动创建空数组 `[]`。`data/` 目录不存在时通过 `os.makedirs(exist_ok=True)` 自动创建。

### 5.4 Prompt 截断策略

存储时 prompt 截断到前 500 字符：

```python
"prompt": prompt[:500] if len(prompt) > 500 else prompt
```

前端历史表格中 prompt 列截断到 30 字符显示，hover 时通过 `title` 属性显示完整内容。

## 6. 前端设计 (`index.html`)

### 6.1 技术栈

纯 HTML + CSS + vanilla JavaScript，无外部依赖，无构建工具。自包含单文件 (455行)。

### 6.2 页面布局

```
┌──────────────────────────────────────────────────┐
│  ⚙ Mooncake NDS KVCache Demo                    │
│  Demonstrates KVCache prefix-hit performance     │
│                                                  │
│  ┌─────────── 输入区 ───────────┐                │
│  │ Prompt: [textarea 4行]       │                │
│  │ Model: [input]  Max Tokens: [input] │         │
│  │ [▶ Run Comparison]           │                │
│  │ ⏳ Run 1: Cold start...      │                │
│  └──────────────────────────────┘                │
│                                                  │
│  ┌──────────────┐  ┌──────────────┐              │
│  │ 🔴 Cold Start │  │ 🟢 Cache Hit │              │
│  │ No KV Cache  │  │ NDS Prefix   │              │
│  │              │  │              │              │
│  │ [text实时流入]│  │ [text实时流入]│              │
│  │              │  │              │              │
│  │ TTFT: 5.23s  │  │ TTFT: 0.15s │              │
│  │ Total: 8.51s │  │ Total: 2.10s│              │
│  │ Tokens: 100  │  │ Tokens: 100 │              │
│  └──────────────┘  └──────────────┘              │
│                                                  │
│  ┌─────────── 延迟对比 ──────────┐                │
│  │ TTFT:                        │                │
│  │   Cold ████████████████ 5.23s │                │
│  │   Hot  ██ 0.15s              │                │
│  │ Total:                        │                │
│  │   Cold ██████████████ 8.51s   │                │
│  │   Hot  ███ 2.10s              │                │
│  │                               │                │
│  │ TTFT Speedup: 34.87x         │                │
│  │ TTFT Reduction: 97.13%       │                │
│  └──────────────────────────────┘                │
│                                                  │
│  ┌─────────── 历史记录 ──────────┐                │
│  │ Time | Prompt | Cold | Hot | Speedup | Red │  │
│  │ 15:30| Explain| 5.23s| 0.15s| 34.87x  |97%│  │
│  │ 15:35| What is| 3.80s| 0.12s| 31.67x  |97%│  │
│  │                               │                │
│  │ [Clear History]               │                │
│  └──────────────────────────────┘                │
└──────────────────────────────────────────────────┘
```

### 6.3 CSS 设计要点

- **深色主题**：背景 `#0f1117`，卡片背景 `#1a1d27`，文字 `#e4e6eb`
- **卡片边框色**：冷启动左侧红色边框 (`#ef4444`)，缓存命中左侧绿色边框 (`#22c55e`)
- **指标颜色**：TTFT 黄色 (`#eab308`)，Total 蓝紫色 (`#6366f1`)，Tokens 灰色
- **TTFT badge 弹出动画**：首 token 到达时 `badge-pop` 动画 (0→1.15x→1，0.4s)
- **柱状图**：纯 CSS 实现，`width` 按延迟比例缩放，Cold 红色渐变，Hot 绿色渐变
- **TTFT 颜色分级**：>2s 红色，0.5-2s 黄色，<0.5s 绿色
- **进度指示器**：CSS 旋转动画 spinner

### 6.4 SSE 客户端实现

前端使用 `fetch` + `ReadableStream` 实现 POST SSE（`EventSource` 仅支持 GET）：

```javascript
fetch('/api/stream-comparison', {
  method: 'POST',
  headers: {'Content-Type': 'application/json'},
  body: JSON.stringify({prompt, model, max_tokens: maxTokens})
}).then(response => {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  function read() {
    reader.read().then(({done, value}) => {
      if (done) { btn.disabled = false; return; }
      buffer += decoder.decode(value, {stream: true});
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const evt = JSON.parse(line.slice(6));
        handleEvent(evt);
      }
      read();
    });
  }
  read();
});
```

解析逻辑：
1. 持续从 ReadableStream 读取字节块
2. 解码为 UTF-8 文本，累积到 buffer
3. 按 `\n` 分行，最后一行（可能不完整）保留在 buffer
4. 解析 `data: ` 前缀的行，JSON.parse 得到事件对象
5. 调用 `handleEvent(evt)` 更新 UI

### 6.5 事件处理逻辑

```javascript
function handleEvent(evt) {
  switch(evt.type) {
    case 'run1_ttft':   // 左卡片 TTFT badge 弹出
    case 'run1_token':  // 左卡片文字追加 + 自动滚动
    case 'run1_done':   // 左卡片所有指标更新，状态切换到 Run 2
    case 'run2_ttft':   // 右卡片 TTFT badge 弹出
    case 'run2_token':  // 右卡片文字追加 + 自动滚动
    case 'run2_done':   // 右卡片所有指标更新
    case 'comparison':  // 显示延迟对比柱状图 + 加速倍数
    case 'done':        // 重新启用按钮，加载历史表格
  }
}
```

### 6.6 延迟柱状图

柱状图宽度按比例缩放，以两轮中最大值为 100%：

```javascript
const ttftMax = Math.max(run1Data.ttft, run2Data.ttft);
ttftBarCold.style.width = (run1Data.ttft / ttftMax * 100) + '%';
ttftBarHot.style.width  = (run2Data.ttft / ttftMax * 100) + '%';
```

CSS transition `width .8s ease` 实现平滑动画展开。

### 6.7 历史表格

- 页面加载时自动调用 `GET /api/history` 填充
- 每次对比完成后自动在表格顶部插入新行
- 记录按时间倒序排列 (`records.reverse()`)
- Prompt 列截断到 30 字符，hover 显示完整内容
- TTFT 列颜色分级：红 (>2s) / 黄 (0.5-2s) / 绿 (<0.5s)
- 降幅列固定绿色
- "Clear History" 按钮调用 `DELETE /api/history`

## 7. 配置选项

### 7.1 后端 CLI 参数

```
python demo_server.py \
  --host 0.0.0.0               # 监听地址
  --port 9200                   # 监听端口
  --proxy-url http://localhost:9100/v1  # PD Proxy 地址
```

### 7.2 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DEMO_HOST` | `0.0.0.0` | Demo 监听地址 |
| `DEMO_PORT` | `9200` | Demo 监听端口 |
| `PROXY_URL` | `http://localhost:9100/v1` | PD Proxy URL |

### 7.3 前端可配置项

| 字段 | 默认值 | 说明 |
|------|--------|------|
| Model | `Qwen2.5-72B` | vLLM 模型名 (需与 Prefiller/Decoder 启动参数一致) |
| Max Tokens | `100` | 最大生成 token 数 |
| Prompt | (用户输入) | completions API 的 prompt 字段 |

## 8. 部署与使用

### 8.1 前置条件

在启动 Demo 前，以下服务必须已运行：

1. **Mooncake Master**：`mooncake_master --use_od=true --nsid=1` (port 50051)
2. **Prefiller**：vLLM + LMCache + MooncakeDistributedStore (port 7100)，**必须使用 Demo 专用配置** (`mooncake-nds-pd-demo/configs/lmcache-prefiller-config.yaml`)
3. **Decoder**：vLLM + LMCache + MooncakeDistributedStore (port 7200)，**必须使用 Demo 专用配置** (`mooncake-nds-pd-demo/configs/lmcache-decoder-config.yaml`)
4. **PD Proxy**：`mooncake_pd_proxy.py` (port 9100)

Python 依赖：`fastapi`, `uvicorn`, `httpx`

**重要**：Prefiller/Decoder 必须使用 `mooncake-nds-pd-demo/configs/` 下的 Demo 专用配置，而非 `mooncake-nds-pd-test/configs/` 下的 benchmark 配置。Demo 配置通过 `local_cpu:False` + `retrieve_locations: ["RemoteBackend"]` 强制纯 NDS 检索，消除 L2 (CPU 内存) 缓存干扰（详见第9节）。

```bash
# Prefiller (使用 demo 配置)
python -m vllm.entrypoints.openai.api_server \
  --model $MODEL_PATH \
  --port 7100 \
  --kv-transfer-config '{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_producer"}' \
  --lmcache-config-file mooncake-nds-pd-demo/configs/lmcache-prefiller-config.yaml

# Decoder (使用 demo 配置)
python -m vllm.entrypoints.openai.api_server \
  --model $MODEL_PATH \
  --port 7200 \
  --kv-transfer-config '{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_consumer"}' \
  --lmcache-config-file mooncake-nds-pd-demo/configs/lmcache-decoder-config.yaml
```

### 8.2 启动方式

```bash
# 方式1: 使用启动脚本 (自动检查 Proxy 健康)
bash start_demo.sh

# 方式2: 直接运行
python demo_server.py --proxy-url http://localhost:9100/v1 --port 9200
```

### 8.3 使用流程

1. 打开浏览器访问 `http://localhost:9200`
2. 在 Prompt 输入框中输入测试文本
3. 确认 Model 和 Max Tokens 设置
4. 点击 "▶ Run Comparison"
5. 观察：
   - 左卡片 (Cold Start)：文字缓慢流入，TTFT 较长
   - 右卡片 (Cache Hit)：文字快速流入，TTFT 短
   - 延迟对比柱状图：红条明显长于绿条
   - 加速倍数和 TTFT 降幅
6. 查看历史表格中的记录
7. 可多次输入不同 prompt，观察缓存命中效果的稳定性

### 8.4 验证缓存命中

通过 Demo 专用 LMCache 配置 (`local_cpu:False`, `retrieve_locations: ["RemoteBackend"]`)，第2轮的 KV 检索**必然**走 Mooncake NDS（L3），而非本地 CPU 内存（L2）。

为确保验证完整性，可从三个维度确认：

1. **Prefiller 日志**：LMCache 的 `batched_async_contains` 命中记录。命中时应输出类似：
   ```
   [LMCache] Prefix hit: N chunks found in remote store
   ```
   由于 `retrieve_locations: ["RemoteBackend"]`，查找仅限 RemoteBackend，命中即为 NDS 命中。

2. **Mooncake Master 日志**：第2次请求时 Master 应记录 `GetStorageConfig` 返回 `use_od=true`，以及 NDS 盘框的读取操作。

3. **TTFT 差值合理性**：若第2轮 TTFT 与第1轮几乎相同（降幅 < 10%），可能说明 L2 绕过未生效或 Prefiller 未检测到缓存命中。需排查配置是否正确加载。

## 9. LMCache 多级缓存与 NDS Demo 的干扰消除

### 9.1 LMCache 存储层级架构

LMCache 采用三级存储架构，检索时按层级顺序查找：

```
L1: GPU 显存   — vLLM BlockSpaceManager 管理，LMCache 不直接控制
L2: CPU 内存   — LocalCPUBackend (hot_cache), 由 local_cpu / max_local_cpu_size 控制
L3: Mooncake   — RemoteBackend → MooncakeDistributedStore → NDS 盘框
```

**检索顺序**：`LocalCPUBackend → RemoteBackend`（`OrderedDict` 插入顺序，L2 总是先于 L3）。

**核心问题**：第1次请求后 KV 数据同时存在于 L2 和 L3。第2次请求时 LMCache 先查 L2 → 命中 → 直接从 CPU 内存读取 → **不走 NDS** → TTFT 降幅不反映 NDS 真实性能，而是被本地 CPU 缓存"截胡"。

### 9.2 L1 (GPU 显存) 分析

LMCache **不管理 GPU blocks** — vLLM 内部 `BlockSpaceManager` 管理所有 GPU KV blocks。

**Prefiller 侧**：PD 流程中 Prefiller 只做 `max_tokens=1` 生成 + KV transfer。完成后 vLLM scheduler 释放 GPU blocks。第2次请求时 Prefiller 重新分配新 blocks → LMCache 从 NDS 取 KV → 写入新 blocks → **无 L1 复用风险**。

**Decoder 侧**：Decoder 不做 LMCache lookup — 它通过 `kv_transfer_params` 接收 KV。Decoder 的旧 GPU blocks 不影响缓存命中判定。

**L1 风险场景**：vLLM scheduler 未及时释放 Prefiller blocks 时，可能出现 L1 复用。

**可选缓解策略**：
- 降低 vLLM `--gpu-memory-utilization` 参数（如 0.5 → 0.3），减少可用 block 数量，加速驱逐
- 在 Demo 后端两轮请求之间插入"填充请求"（长 random prompt），填满 vLLM block space，强制驱逐第1轮的 GPU blocks

### 9.3 L2 (CPU 内存) 绕过方案

#### 三项关键配置修改

| 参数 | 原值 | 新值 | 作用 | 代码位置 |
|------|------|------|------|---------|
| `local_cpu` | `True` | **`False`** | `batched_submit_put_task()` 直接返回，不写入 hot_cache；`batched_get()` 写回也跳过 | `local_cpu_backend.py:193` |
| `max_local_cpu_size` | `5` | **`2`** | LocalCPUBackend 始终存在（RemoteBackend 要求它作为 RDMA 分配器缓冲区）。2GB 仅保留 transient allocation 空间，不用于持久缓存 | `local_cpu_backend.py:346-438` |
| `retrieve_locations` | `None` | **`["RemoteBackend"]`** | `lookup()` 和 `_process_tokens_internal()` 用此作为 `search_range`，只查 L3，完全跳过 L2 | `cache_engine.py:1107-1108` |

#### L2 绕过的数据流

```
Store 路径 (第1次请求 Prefiller 存 KV):
  LMCache.store() → storage_manager.batched_put()
    → LocalCPUBackend.batched_submit_put_task()
      → use_hot=False → 直接返回 (不缓存到 hot_cache)     ← 关键
    → RemoteBackend.batched_submit_put_task()
      → MooncakestoreConnector.batch_put_from → NDS 盘框
  ✓ KV 只存到 NDS，L2 不留存

Retrieve 路径 (第2次请求 Prefiller 取 KV):
  LMCache.lookup() → search_range=["RemoteBackend"]
    → 只查 RemoteBackend                                  ← 关键
    → MooncakestoreConnector.batched_async_contains → store.is_exist → 全量命中
  LMCache.retrieve() → search_range=["RemoteBackend"]
    → RemoteBackend.batched_get()
      → MooncakestoreConnector.batch_get_into → RDMA 从 NDS 读取
      → LocalCPUBackend.allocate() → 分配 2GB 池中的 transient buffer
      → 数据零拷贝入 buffer → 返回 MemoryObj
    → Prefiller 加载到 GPU blocks → 跳过 recompute
  ✓ KV 只从 NDS 取，L2 不介入
```

#### `local_cpu: False` 的代码级效果

1. **`batched_submit_put_task()`** (`local_cpu_backend.py:193-194`):
   ```python
   if not self.use_hot:
       return
   ```
   store 时直接返回，hot_cache 不接收数据。

2. **`batched_get()` 写回** (`storage_manager.py:489-511`):
   ```python
   # write-back caching in batched_get
   if backend_name not in ["LocalCPUBackend", ...]:
       local_cpu_backend.batched_submit_put_task(key, memory_obj)
   ```
   `batched_submit_put_task` 检查 `use_hot` → False → 跳过写回。

3. **`allocate()` 驱逐** (`local_cpu_backend.py:536-588`):
   ```python
   if self.use_hot:
       evict_keys = self.cache_policy.get_evict_candidates(...)
   ```
   `use_hot=False` → 不能从 hot_cache 驱逐 → 分配器只能等待其他操作释放内存。

4. **`retrieve_locations: ["RemoteBackend"]`** (`cache_engine.py:1107-1108`):
   ```python
   search_range = self.retrieve_locations  # ["RemoteBackend"]
   ```
   `batched_contains()` 只在 RemoteBackend 中查找，LocalCPUBackend 被完全跳过。

#### `max_local_cpu_size: 2` 的必要性

RemoteBackend 要求 LocalCPUBackend 作为分配器缓冲区（`__init__.py:236-238`）：

```python
assert local_cpu_backend is not None, (
    "Remote backend requires local CPU backend as a buffer."
    "Please turn on local cpu backend with max_local_cpu_size > 0"
)
```

MooncakeConnector 的 `_batch_get_into()` 从 `local_cpu_backend.allocate()` 分配 RDMA 传输缓冲区。设为 2GB 仅满足 transient allocation，不留持久缓存空间。

### 9.4 Demo 专用 LMCache 配置文件

Demo 使用独立的 LMCache 配置（位于 `mooncake-nds-pd-demo/configs/`），与 benchmark 配置分离：

```
mooncake-nds-pd-demo/configs/
├── lmcache-prefiller-config.yaml   # local_cpu:False, max_local_cpu_size:2, retrieve_locations:["RemoteBackend"]
└── lmcache-decoder-config.yaml     # 同上, local_hostname: "decoder-host"
```

**Prefiller 配置完整内容** (`lmcache-prefiller-config.yaml`)：

```yaml
chunk_size: 256
local_cpu: False
max_local_cpu_size: 2

remote_url: "mooncakestore://localhost:50051/"
remote_serde: "naive"
save_chunk_meta: false
retrieve_locations: ["RemoteBackend"]

extra_config:
  local_hostname: "prefiller-host"
  metadata_server: "http://localhost:8005/metadata"
  protocol: "rdma"
  device_name: ""
  master_server_address: "localhost:50051"
  global_segment_size: 0
  local_buffer_size: 0
  transfer_timeout: 5
```

**Decoder 配置** 与 Prefiller 相同，仅 `local_hostname` 改为 `decoder-host`。

### 9.5 L2 绕过的副作用

| 副作用 | 原因 | 缓解 |
|--------|------|------|
| 分配压力 | `use_hot=False` 时 `allocate()` 无法从 hot_cache 驱逐，只能等待其他操作释放内存 | Demo 为单请求顺序执行，2GB 足够 |
| 单 key `get()` 写回漏洞 | `StorageManager.get()` 调用 `submit_put_task()` 不检查 `use_hot`，可能写入 hot_cache | 主检索路径用 `batched_get()`（检查 `use_hot`），单 key 路径极少使用 |
| 无 L2 fallback | Mooncake 不可用时无本地缓存兜底 | 对 Demo 可接受，NDS 可用性由基础设施保证 |
| WeightedSemaphore 限制 | `AsyncMultiSerializer` 按 `max_local_cpu_size` 计算并发 chunk budget，2GB 限制并发检索数 | Demo 单请求无并发压力 |

### 9.6 与原 benchmark 配置的对比

| 参数 | benchmark (`mooncake-nds-pd-test`) | demo (`mooncake-nds-pd-demo`) | 差异原因 |
|------|------|------|---------|
| `local_cpu` | True | **False** | benchmark 保留 L2 加速正常请求；demo 禁用 L2 强制纯 L3 |
| `max_local_cpu_size` | 5 | **2** | benchmark 5GB 充分缓存；demo 2GB 仅做 RDMA buffer |
| `retrieve_locations` | None (全部) | **["RemoteBackend"]** | benchmark 查所有层级找最快命中；demo 只查 NDS |
| `save_chunk_meta` | false | false | 相同，零拷贝模式 |
| NDS extra_config | 相同 | 相同 | 相同 |

**部署注意**：启动 Prefiller/Decoder 时需指向 Demo 专用配置：

```bash
# Prefiller (使用 demo 配置)
python -m vllm.entrypoints.openai.api_server \
  --model $MODEL_PATH \
  --port 7100 \
  --kv-transfer-config '{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_producer"}' \
  --lmcache-config-file mooncake-nds-pd-demo/configs/lmcache-prefiller-config.yaml

# Decoder (使用 demo 配置)
python -m vllm.entrypoints.openai.api_server \
  --model $MODEL_PATH \
  --port 7200 \
  --kv-transfer-config '{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_consumer"}' \
  --lmcache-config-file mooncake-nds-pd-demo/configs/lmcache-decoder-config.yaml
```

## 10. 与其他测试组件的关系

本 Demo 位于 `mooncake-nds-pd-demo/`，与现有测试组件平行：

```
PD/
├── mooncake-nds-pd-test/       # NDS 基准测试脚本 (自动化 benchmark)
│   ├── run_benchmark.py        # 量化 TTFT 统计 (30 prompts × 3 groups)
│   ├── mooncake_pd_proxy.py    # PD Proxy
│   └── configs/                # LMCache YAML 配置 (local_cpu:True, L2 缓存启用)
│
├── mooncake-traditional-pd-test/  # 传统模式测试脚本
│
├── mooncake-nds-pd-demo/       # 本 Demo (可视化交互对比)
│   ├── demo_server.py          # FastAPI 后端
│   ├── static/index.html       # 自包含前端
│   ├── configs/                # Demo 专用 LMCache 配置 (local_cpu:False, 纯 L3)
│   │   ├── lmcache-prefiller-config.yaml
│   │   └── lmcache-decoder-config.yaml
│   └── data/latency_records.json
│
├── Mooncake/                   # Mooncake 源码
├── lmcache-private/            # LMCache 源码
└── vllm-ascend/                # vLLM-Ascend 源码
```

- `mooncake-nds-pd-test/`：自动化统计基准，LMCache L2+L3 全层级缓存
- `mooncake-nds-pd-demo/`：可视化交互 Demo，LMCache 纯 L3 (NDS) 缓存，消除本地缓存干扰
- Demo 专用配置强制 `retrieve_locations: ["RemoteBackend"]`，确保 TTFT 对比反映纯 NDS 性能
- 两者共享 PD Proxy (`mooncake_pd_proxy.py`)，但 Prefiller/Decoder 需用不同配置启动
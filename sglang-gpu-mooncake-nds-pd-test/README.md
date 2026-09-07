# SGLang + Mooncake GPU PD 分离测试 — 完整操作手册

> **服务器**: 51.36.133.128 (RTX A6000 49GB)  
> **模型**: Qwen2.5-7B  
> **容器**: yyc-sglang-mooncake  
> **框架**: SGLang + 内置 Disaggregation + Mooncake TransferEngine

---

## 一、这套东西是在干嘛？

和 vLLM 那套一样，也是把 LLM 推理拆成 Prefill 和 Decode 两步，中间通过 Mooncake 传 KV cache。区别是用 SGLang 框架代替 vLLM。

SGLang 做这个事情比 vLLM 更原生——它内置了 `--disaggregation-mode prefill/decode` 参数，不需要像 vLLM 那样配 `--kv-transfer-config`，也不用单独跑一个 proxy 脚本。SGLang 自带一个 `sglang_router`，一条命令就能起 PD 路由。

说白了就是 SGLang 在 PD 分离这件事上的工程封装更好，启动也更简单。

---

## 二、和 vLLM 版本的主要区别

| 对比项 | vLLM 版本 | SGLang 版本 |
|--------|----------|-------------|
| PD 分离参数 | `--kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer"}'` | `--disaggregation-mode prefill/decode` |
| 路由组件 | vLLM 内置 `mooncake_connector_proxy.py` | SGLang 内置 `sglang_router` |
| KV 传输后端 | MooncakeConnector (内置) | `--disaggregation-transfer-backend mooncake` (默认值) |
| 握手端口 | `VLLM_MOONCAKE_BOOTSTRAP_PORT=8998` | `--disaggregation-bootstrap-port 8998` |
| 显存参数 | `--gpu-memory-utilization 0.40` | `--mem-fraction-static 0.35/0.55` |
| Proxy/P 脚本数 | 3 个 (prefiller + decoder + proxy) | 3 个 (prefiller + decoder + router) |
| Mooncake CUDA 13 修复 | 需要从 sglang 容器拷贝 | 不需要，sglang 容器自带 |

> **显存参数差异要注意**：SGLang 的 `--mem-fraction-static` 和 vLLM 的 `--gpu-memory-utilization` 虽然都是控制显存占比的，但 SGLang 的开销更大。同一个 0.40 的值，vLLM 能跑起来，SGLang 可能就 OOM 了。后面坑 1 会详细说。

---

## 三、端口规划

| 服务 | 端口 | 干什么的 |
|------|------|----------|
| Prefiller (SGLang) | **7100** | 接收请求，算 KV，存到 Mooncake |
| Decoder (SGLang) | **7200** | 从 Mooncake 拉 KV，做 decode 生成 |
| Router | **19000** | 用户入口，PD 路由 |
| Bootstrap | **8998** | ZMQ 握手端口，和 vLLM 版本相同 |

用户只访问 **19000**。

---

## 四、前置准备

### 4.1 确认容器在跑

```bash
docker ps | grep yyc-sglang-mooncake
# 没跑就启动
docker start yyc-sglang-mooncake
```

### 4.2 确认 Mooncake 能用

SGLang 容器自带 CUDA 13 版本的 mooncake，不需要像 vLLM 那样搞替换。直接验证：

```bash
docker exec yyc-sglang-mooncake python3 -c \
  "from mooncake.engine import TransferEngine; print(TransferEngine)"
# 输出 <class 'mooncake.engine.TransferEngine'> 就行
```

### 4.3 上传测试代码

代码在 `sglang-gpu-mooncake-nds-pd-test/` 目录，服务器路径 `/home/yyc/Mooncake/sglang-gpu-mooncake-nds-pd-test/`。改了代码用 SFTP 推上去。

### 4.4 确认模型在

```bash
docker exec yyc-sglang-mooncake ls /home/model/Qwen2.5-7B/
```

---

## 五、启动流程

需要开 **3 个终端**，全部 `docker exec -it` 进 SGLang 容器。

### 终端 1：启动 Prefiller

```bash
docker exec -it yyc-sglang-mooncake bash
cd /home/yyc/Mooncake/sglang-gpu-mooncake-nds-pd-test
bash start_prefiller.sh
```

等 60 秒左右，看到这些就说明起来了：

```
The server is fired up and ready to roll!
```

验证：

```bash
curl -s http://127.0.0.1:7100/health
```

### 终端 2：启动 Decoder

```bash
docker exec -it yyc-sglang-mooncake bash
cd /home/yyc/Mooncake/sglang-gpu-mooncake-nds-pd-test
bash start_decoder.sh
```

> **启动顺序很重要**：必须先起 Prefiller，再起 Decoder。Decoder 启动时会通过 8998 端口连 Prefiller 做握手，如果 Prefiller 没起来，Decoder 会卡住等。

验证：

```bash
curl -s http://127.0.0.1:7200/health
```

### 终端 3：启动 Router

```bash
docker exec -it yyc-sglang-mooncake bash
cd /home/yyc/Mooncake/sglang-gpu-mooncake-nds-pd-test
bash start_router.sh
```

Router 会先等 Prefiller 和 Decoder 都就绪，然后启动：

```
Starting router...
```

验证：

```bash
curl -s http://127.0.0.1:19000/v1/models
```

---

## 六、测试

### 6.1 基本推理

```bash
# 写 JSON payload
python3 -c "
import json
json.dump({
    'model': '/home/model/Qwen2.5-7B',
    'prompt': 'What is 2+2? Answer briefly.',
    'max_tokens': 20,
    'temperature': 0
}, open('/tmp/test_payload.json', 'w'))
"

# 通过 router 发请求
curl -s http://127.0.0.1:19000/v1/completions \
    -H "Content-Type: application/json" \
    -d @/tmp/test_payload.json
```

### 6.2 连续发 6 个请求验证

```bash
for i in $(seq 1 6); do
  python3 -c "
import json
prompts = [
    'What is 3+5?',
    'Translate hello to French.',
    'Name the capital of Japan.',
    'What is the boiling point of water in Celsius?',
    'Explain what a neural network is in one sentence.',
    'What color is the sky?'
]
json.dump({
    'model': '/home/model/Qwen2.5-7B',
    'prompt': prompts[$i-1],
    'max_tokens': 50,
    'temperature': 0
}, open('/tmp/req$i.json', 'w'))
"
  echo -n "Request $i: "
  curl -s http://127.0.0.1:19000/v1/completions \
      -H "Content-Type: application/json" \
      -d @/tmp/req$i.json | python3 -c "import json,sys; r=json.load(sys.stdin); print(r['choices'][0]['text'][:60])"
  sleep 1
done
```

6 个请求都能返回正确结果就说明整条链路通了。

---

## 七、架构说明

```
                    用户请求
                       │
                       ▼
              ┌──────────────────┐
              │  Router :19000    │  ← sglang_router.launch_router
              │  --pd-disaggregation │
              └───┬──────────┬───┘
                  │          │
    ┌─────────────┘          └─────────────┐
    ▼                                       ▼
┌───────────────────┐              ┌───────────────────┐
│ Prefiller :7100    │              │ Decoder :7200     │
│ disaggregation:    │              │ disaggregation:   │
│   prefill          │              │   decode          │
│                    │              │                    │
│ 1.接收 prompt       │              │ 3.接收请求         │
│ 2.prefill 算 KV    │              │ 4.从 Mooncake 拉 KV│
│   存到 Mooncake    │              │ 5.decode 生成      │
│   (RDMA/TCP)       │              │ 6.返回结果         │
└────────┬──────────┘              └────────────────────┘
         │
         ▼
   ┌───────────────┐
   │ Mooncake P2P   │  ← TransferEngine (默认 TCP, 有 RDMA 就用 RDMA)
   │ Bootstrap:8998 │  ← ZMQ 握手通道
   └───────────────┘
```

**Router 工作流程**：
1. Router 接收用户请求
2. 发给 Prefiller 做一次 prefill（max_tokens=1），KV 存到 Mooncake
3. 发给 Decoder，Decoder 从 Mooncake 拉 KV，做 token-by-token decode
4. Router 把 Decoder 的输出返回给用户

---

## 八、踩过的坑 & 解决方案

### 坑 1：Decoder 报 "Not enough memory"（最头疼的）

**现象**：Decoder 启动时报 `RuntimeError: Not enough memory. Please try to increase --mem-fraction-static.`

**试过的过程**：
- `0.40`（和 vLLM 一样的值）→ 失败
- 降到 `0.30` → 也失败，太低了连模型权重都放不下
- `0.50` → 还是失败
- `0.55` → 终于成功了

**原因**：SGLang 的 `--mem-fraction-static` 和 vLLM 的 `--gpu-memory-utilization` 虽然概念一样，但 SGLang 的内部开销更大。同样的 0.40，vLLM 能跑 SGLang 跑不起来。SGLang 在初始化时会预分配更多显存给 CUDA Graph、临时 buffer 等。

**最终方案**：**非对称显存分配**——Prefiller 用 0.35，Decoder 用 0.55。

为什么这么分？
- **Prefiller 只做 prefill**（生成 1 个 token），需要的 KV cache 很小，0.35 就够放权重+少量 KV
- **Decoder 做 decode**，需要持续生成 token，需要更大的 KV cache 存中间状态，所以给 0.55
- 0.35 + 0.55 = 0.90，加起来正好不超过 100%（还有 10% 给系统 overhead）

同时 context-length 从 8192 降到 4096（Decoder），减少 KV cache 需求。还加了 `--disable-cuda-graph` 给 Decoder，省掉 CUDA Graph 预分配的显存。

### 坑 2：cleanup.sh 把自己杀了（最诡异的）

**现象**：跑 `bash cleanup.sh`，输出到 "Stopped SGLang router" 就停了，后面的进程没杀到。nvidia-smi 看显存还占着。

**原因**：`pkill -f "sglang"` 会匹配到**所有命令行里含 "sglang" 的进程**，包括 `bash cleanup.sh` 本身——因为脚本的路径里就有 "sglang"（`sglang-gpu-mooncake-nds-pd-test/cleanup.sh`）。pkill 把自己杀了，后面的命令自然就跑不了了。

**更麻烦的是**：SGLang 启动后会产生三种不同名字的进程：
- `python3 -m sglang.launch_server` — 主进程
- `sglang::scheduler` — 调度子进程（nvidia-smi 里显示的就是这个名）
- `sglang::detokenizer` — 反序列化子进程

光杀 `python3 -m sglang.launch_server` 不够，`sglang::scheduler` 还活着占显存。

**解决**：用三个精确的匹配模式，避开 "sglang" 这个过于宽泛的词：

```bash
pkill -f "sglang_router"           # 匹配 router
pkill -f "python.*sglang"           # 匹配主进程（python 开头），不会匹配到 bash 脚本
pkill -f "sglang::"                 # 匹配子进程（scheduler/detokenizer）
```

改完以后验证：`nvidia-smi` 显示 0 MiB used。

### 坑 3：Router 端口被占

**现象**：Router 启动报 `Address already in use (os error 98)`，端口 19000。

**原因**：上次跑的 router 进程没杀干净。

**解决**：启动前先杀掉旧 router：
```bash
pkill -9 -f sglang_router
```

或者直接跑 `cleanup.sh` 再启动。

### 坑 4：RDMA QP 握手超时

**现象**：日志里出现 `Failed to modify QP to RTR: Connection timed out`，`packet mismatch` 在 mlx5_2 和 mlx5_3 之间。

**影响**：RDMA 端点被标记为 inactive。**但是推理还是能跑**，因为 Mooncake 自动 fallback 到 TCP 传输。6 个测试请求全部成功。

**原因**：两个 RDMA 网卡（mlx5_2: 192.168.133.128, mlx5_3: 192.168.133.129）在同一个 /24 子网，Linux 路由表有两条等价路由。RoCE v2 要求 GID 和网络接口严格匹配，但 ARP flux 导致响应包走错接口，QP 握手就超时了。

**当前状态**：没有修复，靠 TCP fallback 兜底。如果要修：
- **方案 A（推荐）**：把 mlx5_3 改到不同子网（如 192.168.134.129/24）
- **方案 B**：`sysctl net.ipv4.conf.all.arp_filter=1`
- **方案 C**：设 `MOONCAKE_PROTOCOL=tcp` 跳过 RDMA，反正也用 TCP

> 面试讲解时这个坑反而是加分项——说明你理解了 RoCE v2 的 GID 匹配机制和 Linux 多网卡同子网的路由问题。

### 坑 5：SGLang 健康检查和 vLLM 不一样

**现象**：run_benchmark.py 的健康检查用 `GET /v1/models`，SGLang 不一定支持这个路径。

**原因**：SGLang 的健康检查用的是 `/health` 端点，不是 `/v1/models`。

**解决**：SGLang 的 `start_router.sh` 里用 `curl http://localhost:PORT/health` 做就绪检查。benchmark 脚本复用 vLLM 版本的逻辑（任何 HTTP 响应都算就绪）。

---

## 九、关键参数对比

| 参数 | Prefiller | Decoder | 为什么不一样 |
|------|-----------|---------|-------------|
| `--port` | 7100 | 7200 | 两个实例不能撞端口 |
| `CUDA_VISIBLE_DEVICES` | 0 | 0 | 单 GPU 模式，共享同一张卡 |
| `--mem-fraction-static` | **0.35** | **0.55** | P 只需权重+少量 KV；D 需要大 KV 做 decode |
| `--context-length` | 8192 | 4096 | P 要处理长输入；D 只需生成 4096 token |
| `--disaggregation-mode` | **prefill** | **decode** | 核心 PD 参数 |
| `--disaggregation-bootstrap-port` | 8998 | 8998 | 必须一致 |
| `--disable-radix-cache` | ✓ | ✓ | 禁用 SGLang 内置缓存，只用 Mooncake KV |
| `--disable-cuda-graph` | — | ✓ | Decoder 禁用 CUDA Graph 省显存 |
| `--base-gpu-id` | — | 0 | Decoder 指定 GPU（和 P 同一张卡） |
| `PYTHONHASHSEED` | 0 | 0 | KV hash 一致性 |

> **为什么 Prefiller context-length 是 8192 而 Decoder 是 4096？** Prefiller 需要处理完整的用户输入（可能很长），所以给 8192。Decoder 的 context-length 控制的是最大生成长度，4096 够用了，小一点还能省显存。

---

## 十、停止服务

```bash
# 在容器内执行
bash cleanup.sh

# 或手动（注意三个精确模式，别用 pkill -f sglang）
pkill -f "sglang_router"
pkill -f "python.*sglang"
pkill -f "sglang::"
pkill -f "mooncake"
```

然后 `nvidia-smi` 确认显存清零。

> **千万别用 `pkill -f "sglang"`**！会匹配到脚本自身路径，把自己杀了。用上面三个精确模式。

---

## 十一、文件说明

```
sglang-gpu-mooncake-nds-pd-test/
├── start_prefiller.sh    # 启动 Prefiller (--disaggregation-mode prefill)
├── start_decoder.sh       # 启动 Decoder (--disaggregation-mode decode)
├── start_router.sh        # 启动 SGLang 内置 Router
├── run_benchmark.py       # Benchmark 脚本（从 vLLM 版本复用）
└── cleanup.sh             # 清理脚本（精确匹配进程，避免自杀）
```

---

## 十二、关键环境变量

| 变量 | 值 | 为什么需要 |
|------|-----|-----------|
| `CUDA_VISIBLE_DEVICES` | 0 | GPU 隔离 |
| `PYTHONHASHSEED` | 0 | KV hash 一致性，P 和 D 必须相同 |
| `no_proxy` / `NO_PROXY` | 127.0.0.1,localhost | 绕过华为代理，和 vLLM 版本一样 |

---

## 十三、RDMA 网卡环境（供参考）

服务器有 4 块 Mellanox ConnectX-7 网卡（200 Gbps）：

| 设备 | 网络接口 | IP | MTU | 状态 |
|------|---------|-----|-----|------|
| mlx5_0 | ens17f0np0 | — | 1500 | Down |
| mlx5_1 | ens17f1np1 | — | 1500 | Down |
| **mlx5_2** | **ens41f0np0** | **192.168.133.128** | **5500** | **Active** |
| **mlx5_3** | **ens41f1np1** | **192.168.133.129** | **5500** | **Active** |

- 驱动：`mlx5_core` v24.10-3.2.5
- 固件：`28.43.3608`
- 链路层：Ethernet (RoCE v2，不是 InfiniBand)
- MTU 5500 = Jumbo frame，适合大块 KV 传输

两个 RDMA 接口在同一 /24 子网，导致 RoCE v2 QP 握手有问题（见坑 4）。TCP fallback 正常工作。

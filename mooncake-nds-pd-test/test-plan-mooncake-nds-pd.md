# PD 分离场景下 Mooncake (NDS) KVCache 命中性能测试计划

## 1. 测试目标

在 PD (Prefill-Decode) 分离场景下，完全依赖 Mooncake NDS 版做跨节点 KVCache 存储，测量 **KVCache 前缀命中复用** 对 TTFT 和吞吐的影响。

## 2. 架构

```
  Client (run_benchmark.py)
        │
        ▼
  PD Proxy (mooncake_pd_proxy.py)
        │
        ├──► Prefiller (kv_producer, NPU ${PREFILLER_NPU_ID})
        │      vLLM + LMCache → 存储 KVCache → Mooncake NDS
        │      max_tokens=1, 仅做 prefill + KV 存储
        │
        └──► Decoder (kv_consumer, NPU ${DECODER_NPU_ID})
               vLLM + LMCache → 检索 KVCache ← Mooncake NDS
               前缀命中 → 跳过 prefill → 低 TTFT
               前缀未命中 → 全量 prefill → 高 TTFT
               stream=True, 生成完整响应
```

**关键设计**:
- Proxy 采用 vLLM 标准 PD 流程: prefiller `max_tokens=1` (仅存储 KV), decoder 生成完整输出
- 传递 `kv_transfer_params` 从 prefiller 到 decoder
- 无 NIXL PD 直传通道，KVCache 完全通过 Mooncake (NDS) 存储中转

**两种 benchmark 模式**:
- `--mode proxy`: 通过 proxy (端到端生产场景)
- `--mode direct`: 直接向 decoder 发请求 (更纯粹的 KVCache 命中测量)

## 3. 测试组

| 组 | 说明 | Prompt 设计 | 预期 |
|---|---|---|---|
| **A: 基线** | 无 KVCache 命中 | 30 个完全不同的随机 prompt | 全量 prefill，高 TTFT |
| **B: 前缀复用** | KVCache 前缀命中 | Warmup 存前缀→30 个 prompt: 共享前缀 + 不同后缀 | 第1次存入，后续复用，低 TTFT |
| **C: 前缀长度梯度** | 命中率变化 | 前缀占比 0% / 25% / 50% / 75% / 100% | TTFT 随前缀占比线性下降 |

## 4. 前置依赖

- Mooncake NDS 版本 (Master 支持 `--use_od --nsid`)
- `libndskv.so` + `nds_config.conf` (NDS 盘框库和配置)
- vLLM (Ascend NPU 版)
- LMCache (`pip install lmcache`)
- 2+ NPU
- Python 3.10+, httpx, fastapi, uvicorn

## 5. 可配置变量

所有变量集中定义在各脚本头部，修改一处即可全局生效:

```bash
# === 核心变量 ===
MODEL_PATH="/path/to/your/model"       # 模型文件路径 (必改)
PREFILLER_NPU_ID=1                     # Prefiller 占用 NPU ID
DECODER_NPU_ID=2                       # Decoder 占用 NPU ID

# === 网络变量 ===
MASTER_HOST="localhost"                 # Mooncake Master 主机地址
MASTER_PORT=50051                       # Master RPC 端口
METADATA_PORT=8005                      # Metadata HTTP 端口
PROXY_PORT=9100                         # Proxy 服务端口
PREFILLER_VLLM_PORT=7100               # Prefiller vLLM 服务端口
DECODER_VLLM_PORT=7200                 # Decoder vLLM 服务端口

# === Mooncake/NDS 变量 ===
NSID=1                                  # NDS namespace ID
NDS_LIBRARY_PATH="libndskv.so"          # NDS 共享库路径
MC_NDS_CONFIG="nds_config.conf"         # NDS 配置文件路径

# === RDMA 配置 ===
# RDMA_STRATEGY: "auto" | "explicit" | "tcp"
#   auto     — 自动发现 RDMA 设备 (MC_MS_AUTO_DISC=1, YAML device_name="")
#   explicit — 使用 RDMA_DEVICE_NAME 指定设备 (MOONCAKE_DEVICE 环境变量)
#   tcp      — 降级 TCP (需改 YAML protocol: "tcp")
RDMA_STRATEGY="auto"
RDMA_DEVICE_NAME=""                     # 仅 explicit 模式生效
                                        # 单设备: "mlx5_0" / "roce_eth0" / "erdma_0"
                                        # 多设备: "mlx5_0,mlx5_1" (逗号分隔)

# === LMCache 变量 ===
CHUNK_SIZE=256                          # LMCache chunk size
MAX_LOCAL_CPU_SIZE=5                    # CPU pinned 内存大小 (GB)

# === Benchmark 变量 ===
NUM_PROMPTS=30                          # 每组请求数
INPUT_LEN=7500                          # 基线 prompt 长度 (tokens)
OUTPUT_LEN=200                          # 输出长度 (tokens)
PREFIX_LEN=5000                         # 复用场景共享前缀长度 (tokens)
SUFFIX_LEN=2500                         # 复用场景不同后缀长度 (tokens)
BENCH_MODE="proxy"                      # "proxy" 或 "direct"
```

## 6. 目录结构

所有文件已创建在 `mooncake-nds-pd-test/` 目录:

```
mooncake-nds-pd-test/
├── start_mooncake_nds.sh              # 启动 Master + metadata server
├── configs/
│   ├── lmcache-prefiller-config.yaml  # Prefiller LMCache 配置
│   └── lmcache-decoder-config.yaml    # Decoder LMCache 配置
├── start_prefiller.sh                 # 启动 prefiller vLLM
├── start_decoder.sh                   # 启动 decoder vLLM
├── mooncake_pd_proxy.py               # PD proxy server (标准 PD 流程)
├── run_benchmark.py                   # 自动化 benchmark (A/B/C 三组)
├── cleanup.sh                         # 清理所有进程
└── nds_config.conf                    # NDS 配置文件 (模板)
```

---

## 7. 脚本详细内容

所有脚本文件已独立存放在 `mooncake-nds-pd-test/` 目录中。以下仅说明关键设计要点。

### 7.1 `mooncake_pd_proxy.py` — PD Proxy (核心变更)

**与旧版简化 proxy 的关键区别**:

旧版 proxy 将完整请求 (含 `max_tokens=N`) 发给 prefiller，prefiller 生成 N 个 token 后才转给 decoder。这浪费了 prefiller 的生成时间，且增加了总 TTFT。

新版 proxy 采用 vLLM 标准 PD 流程:
1. **Prefiller 阶段**: `max_tokens=1, stream=False` — 仅做 prefill 计算 + 存 KV 到 Mooncake
2. 提取 prefiller 响应中的 `kv_transfer_params`
3. **Decoder 阶段**: `stream=True` — 从 Mooncake 检索 KV + 生成完整输出

这样 prefiller 只做最小化 token 生成 (1 token)，KV 存储完成后立即释放，decoder 端才是真正的生成。

### 7.3 RDMA 配置策略

启动脚本 (`start_prefiller.sh`, `start_decoder.sh`) 提供 3 种 RDMA 策略:

| 策略 | `RDMA_STRATEGY` | 环境变量 | YAML `device_name` | 适用场景 |
|------|----------------|---------|---------------------|---------|
| **自动发现** | `auto` | `MC_MS_AUTO_DISC=1`, `MOONCAKE_DEVICE=""` | `""` | 不确定 RDMA 设备名, 让 Mooncake 自动检测 |
| **指定设备** | `explicit` | `MC_MS_AUTO_DISC=0`, `MOONCAKE_DEVICE="xxx"` | `""` (环境变量优先) | 已知 RDMA 设备名, 需精确控制 |
| **TCP 降级** | `tcp` | 无 RDMA 环境变量 | 需改 `protocol: "tcp"` | 无 RDMA NIC 或测试环境 |

**优先级**: `MOONCAKE_DEVICE` 环境变量 → YAML `device_name` → 自动发现 (`MC_MS_AUTO_DISC=1`)

**查看可用 RDMA 设备**:
```bash
ibv_devinfo                    # 查看所有 RDMA 设备详情
ibv_devices                    # 查看设备名列表
```

**多 NIC 场景**: `RDMA_DEVICE_NAME="mlx5_0,mlx5_1"` (逗号分隔, Mooncake round-robin 分配)

### 7.4 `run_benchmark.py` — Benchmark (核心变更)

**替代 run_benchmark.sh 的原因**: `vllm bench serve` 不支持 `--shared-prefix-len` 参数。

关键设计:
- **TTFT 测量**: 使用 streaming SSE，精确测量"请求发出 → 首个 content token 到达"的时间
- **Prompt 生成**: 用 Python `random.Random(seed)` 生成确定性文本，估算 char/token 比为 4.0
- **Warmup 流程**: Group B/C 在测试前先用 prefiller 存前缀 KV 到 Mooncake
- **两种模式**: `--mode proxy` (通过 proxy) 和 `--mode direct` (直接测 decoder)
- **结果输出**: JSON 文件包含每个请求的 TTFT + 统计汇总

命令行参数:
```
python run_benchmark.py \
    --mode proxy \
    --model "your-model" \
    --proxy-url http://localhost:9100 \
    --prefiller-url http://localhost:7100 \
    --decoder-url http://localhost:7200 \
    --num-prompts 30 \
    --input-len 7500 \
    --output-len 200 \
    --prefix-len 5000 \
    --suffix-len 2500 \
    --ratios 0 25 50 75 100 \
    --seed 42 \
    --groups ABC \
    --output-dir ./bench_results
```

---

## 8. 启动流程 (完整执行顺序)

```bash
# 1. 将 mooncake-nds-pd-test/ 目录复制到 Linux 测试服务器

# 2. 编辑变量:
#    - 修改 start_prefiller.sh 中的 MODEL_PATH
#    - 修改 start_decoder.sh 中的 MODEL_PATH
#    - 修改 PREFILLER_NPU_ID / DECODER_NPU_ID (如需)
#    - 修改 nds_config.conf 为实际盘框配置
#    - 修改 configs/ 中的 YAML 中的 MASTER_HOST (如跨节点)
#    - RDMA 配置:
#      a) 自动发现: RDMA_STRATEGY="auto" (默认, 无需改)
#      b) 指定设备: RDMA_STRATEGY="explicit", RDMA_DEVICE_NAME="你的设备名"
#         用 ibv_devinfo 查看可用设备
#      c) TCP 降级: RDMA_STRATEGY="tcp", 并修改 YAML protocol: "tcp"

# 3. 启动 Mooncake NDS 服务
bash start_mooncake_nds.sh &

# 4. 启动 Prefiller
bash start_prefiller.sh &

# 5. 启动 Decoder
bash start_decoder.sh &

# 6. 启动 Proxy
python mooncake_pd_proxy.py \
    --host localhost --port 9100 \
    --prefiller-host localhost --prefiller-port 7100 \
    --decoder-host localhost --decoder-port 7200 &

# 7. 等待所有服务就绪 (约 2-5 分钟, 模型加载时间)

# 8. 运行 Benchmark (proxy 模式)
python run_benchmark.py \
    --mode proxy \
    --model "your-model" \
    --proxy-url http://localhost:9100 \
    --prefiller-url http://localhost:7100 \
    --decoder-url http://localhost:7200

# 8a. 或运行 Benchmark (direct 模式, 更纯粹的 KVCache 命中测量)
python run_benchmark.py \
    --mode direct \
    --model "your-model" \
    --prefiller-url http://localhost:7100 \
    --decoder-url http://localhost:7200

# 9. 查看结果
cat bench_results/benchmark_*.json

# 10. 清理
bash cleanup.sh
```

---

## 9. NDS 特殊注意事项

| 注意事项 | 说明 |
|---------|------|
| **`local_buffer_size: 0`** | NDS 模式下 LMCache YAML 中 `local_buffer_size` 必须为 0。否则首次 `register_buffer()` 触发 NDS init 时内存过小 (28KB) 导致失败 |
| **`global_segment_size: 0`** | NDS 模式下 Master 不需要为 Client 分配全局内存段, 设为 0 |
| **`NDS_LIBRARY_PATH`** | 指定 `libndskv.so` 绝对路径, 否则依赖 `LD_LIBRARY_PATH` |
| **`MC_NDS_CONFIG`** | NDS 配置文件路径, 必须与实际盘框环境匹配 |
| **nsid 下发** | nsid 由 Master RPC 自动下发到 Client, 不需要客户端设置 `MC_NDS_NSID` 环境变量 |
| **`save_chunk_meta: false`** | 零拷贝模式, 不存 TensorMetadata 前缀, LMCache 传 raw bytes |
| **`PYTHONHASHSEED=0`** | Prefiller 和 Decoder 必须相同, 确保 KVCache hash 一致 |
| **`ASCEND_RT_VISIBLE_DEVICES`** | NPU 设备隔离, 两个实例必须使用不同 NPU ID |
| **Proxy `max_tokens=1`** | Prefiller 仅做 prefill + KV 存储, 不浪费生成时间 |
| **`protocol: "rdma"`** | LMCache YAML 中协议为 RDMA, 需配合 RDMA 设备配置 |
| **`device_name: ""`** | RDMA 设备名留空时, 由 `MOONCAKE_DEVICE` 环境变量或自动发现填充 |
| **`MC_MS_AUTO_DISC=1`** | RDMA 自动发现模式, Mooncake 自动检测可用 RDMA NIC (`RDMA_STRATEGY=auto`) |
| **`MOONCAKE_DEVICE`** | RDMA 设备名环境变量, 优先级高于 YAML `device_name` (`RDMA_STRATEGY=explicit`) |

---

## 10. 预期结果分析

### 组 A vs 组 B: TTFT 对比

| 指标 | 组 A (无命中) | 组 B (前缀复用) | 改善幅度 |
|------|-------------|----------------|---------|
| Mean TTFT | ~3-5s (全量 prefill) | ~0.5-1s (仅 prefill 后缀) | **~60-80% 降低** |
| Median TTFT | 同上 | 同上 | 同上 |
| TPOT | 无变化 | 无变化 | N/A |
| Throughput | 低 (长 prefill 占用) | 高 (短 prefill 释放资源) | **提升** |

### 组 C: TTFT vs 前缀占比曲线

```
TTFT ─┐
      │ ╲
      │   ╲
      │     ╲
      │       ╲──────────
      └─────────────────────
       0%  25%  50%  75%  100%
              Prefix Ratio
```

TTFT 应随前缀占比近似线性下降, 100% 时接近 0 (全部从 Mooncake 复用)。

### proxy 模式 vs direct 模式

| 指标 | proxy 模式 | direct 模式 |
|------|-----------|-------------|
| TTFT 含义 | 客户端 → proxy → prefiller(1token) → decoder → 首 token | 客户端 → decoder → 首 token |
| Prefiller 影响 | 包含 prefiller prefill + KV 存储时间 | 不包含 (需 warmup 预存) |
| 适用场景 | 生产环境评估 | 纯 KVCache 命中效果测量 |

---

## 11. 故障排查

| 问题 | 可能原因 | 检查方法 |
|------|---------|---------|
| Prefiller 启动后 KV 未存储到 Mooncake | LMCache 未正确初始化 Mooncake connector | 查 prefiller 日志是否有 `MooncakestoreConnector` 初始化信息 |
| Decoder 未复用 KVCache | `PYTHONHASHSEED` 不一致 / chunk hash 不匹配 | 确认两端 `PYTHONHASHSEED=0`, 查 decoder 日志是否有 KV 检索记录 |
| NDS init 失败 | `local_buffer_size` 非 0 / `nsid=0` / `libndskv.so` 缺失 | 确认 YAML 中 `local_buffer_size: 0`, Master `--nsid=1`, `NDS_LIBRARY_PATH` 正确 |
| RDMA 连接失败 | `device_name` 错误 / RDMA NIC 不存在 / GID index 不对 | 用 `ibv_devinfo` 检查设备名, 尝试 `MC_MS_AUTO_DISC=1`, 检查 `MC_GID_INDEX` |
| Proxy 返回 500 | Prefiller 或 Decoder 未就绪 | 检查 proxy `/health` endpoint, 确认 vLLM 服务已启动 |
| TTFT 无差异 | KVCache 实际未命中 / prefix hash 不同 | 查 LMCache 日志中的 `is_exist` 调用结果, 确认前缀 token 序列完全一致 |

---

## 12. 手动验证 (可选快速测试)

如果只想快速验证 KVCache 是否工作, 不需完整 benchmark:

```python
#!/usr/bin/env python3
import time, httpx

PROXY = "http://localhost:9100/v1"
MODEL = "your-model"

# Step 1: Store prefix KV via proxy (Prefiller → Mooncake)
prefix = "Analyze this text carefully. " * 200  # ~5000 chars ≈ ~1250 tokens
suffix1 = "What is the main point?"
prompt1 = prefix + suffix1
r1 = httpx.Client(timeout=300).post(f"{PROXY}/completions",
    json={"model": MODEL, "prompt": prompt1, "max_tokens": 50, "stream": False})
t1 = time.time()

# Step 2: Reuse prefix (same prefix + different suffix)
suffix2 = "Who is the author?"
prompt2 = prefix + suffix2
start = time.time()
r2 = httpx.Client(timeout=300).post(f"{PROXY}/completions",
    json={"model": MODEL, "prompt": prompt2, "max_tokens": 50, "stream": False})
ttft_reuse = time.time() - start

# Step 3: No reuse (completely different)
prompt3 = "Completely unrelated topic here. " * 200 + "Summarize."
start = time.time()
r3 = httpx.Client(timeout=300).post(f"{PROXY}/completions",
    json={"model": MODEL, "prompt": prompt3, "max_tokens": 50, "stream": False})
ttft_no_reuse = time.time() - start

print(f"Reuse TTFT:    {ttft_reuse:.3f}s")
print(f"No-reuse TTFT: {ttft_no_reuse:.3f}s")
print(f"Improvement:   {(1 - ttft_reuse/ttft_no_reuse)*100:.1f}%")
```
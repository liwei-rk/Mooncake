# Mooncake 传统文件系统模式 PD 分离兼容性测试计划

## 1. 测试目标

验证 **NDS 版 Mooncake 在传统文件系统模式下是否完全兼容老版本行为**。即: 使用 NDS 版 Mooncake 的 Master 二进制，但不启用 `--use_od --nsid`，让 Client 使用默认配置 (local_buffer > 0, global_segment > 0)，确认 KVCache 存取与 PD 分离流程正常工作。

## 2. 架构

```
  Client (run_benchmark.py)
        │
        ▼
  PD Proxy (mooncake_pd_proxy.py)
        │
        ├──► Prefiller (kv_producer, NPU 1)
        │      vLLM + LMCache → put KVCache → Mooncake (传统文件系统存储)
        │      max_tokens=1, 仅做 prefill + KV 存储
        │
        └──► Decoder (kv_consumer, NPU 2)
               vLLM + LMCache → get KVCache ← Mooncake (传统文件系统存储)
               前缀命中 → 跳过 prefill → 低 TTFT
               前缀未命中 → 全量 prefill → 高 TTFT
```

## 3. 与 NDS 版测试的关键区别

| 配置项 | NDS 版 (`mooncake-nds-pd-test`) | 传统版 (`mooncake-traditional-pd-test`) |
|--------|--------------------------------|----------------------------------------|
| Master 启动参数 | `--use_od=true --nsid=1` | **无** (默认参数) |
| `local_buffer_size` | `0` (NDS: 不需要本地缓冲区) | `1073741824` (1GB, 传统模式必须有) |
| `global_segment_size` | `0` (NDS: Master 不分配全局段) | `32212254720` (30GB, Master 为 Client 分配) |
| `NDS_LIBRARY_PATH` | `libndskv.so` | **不需要** |
| `MC_NDS_CONFIG` | `nds_config.conf` | **不需要** |
| `nsid` | Master 下发 nsid > 0 | 无 nsid (传统模式无 namespace) |
| RDMA 设备 | 可配置 (auto/explicit/tcp) | 可配置 (相同策略) |
| Proxy / Benchmark | 共用 | 共用 (完全相同) |

**核心验证**: NDS 版 Mooncake Master 在**不启用 NDS 参数**时，应完全兼容传统文件系统模式 — Client 能正常注册缓冲区、存取 KVCache、PD 分离流程正常。

## 4. 测试组

与 NDS 版完全相同:

| 组 | 说明 | Prompt 设计 | 预期 |
|---|---|---|---|
| **A: 基线** | 无 KVCache 命中 | 30 个完全不同的随机 prompt | 全量 prefill，高 TTFT |
| **B: 前缀复用** | KVCache 前缀命中 | Warmup 存前缀 → 30 个 prompt 共享前缀 + 不同后缀 | 低 TTFT |
| **C: 前缀长度梯度** | 命中率变化 | 前缀占比 0% / 25% / 50% / 75% / 100% | TTFT 线性下降 |

## 5. 前置依赖

- Mooncake NDS 版本 (同一 Master 二进制，但**不**传 `--use_od --nsid`)
- vLLM (Ascend NPU 版)
- LMCache (`pip install lmcache`)
- 2+ NPU
- RDMA NIC (可选，可降级 TCP)
- Python 3.10+, httpx, fastapi, uvicorn

**注意**: 无需 `libndskv.so` 和 `nds_config.conf`。

## 6. 目录结构

```
mooncake-traditional-pd-test/
├── start_mooncake_master.sh            # Master (传统模式: 无 --use_od --nsid)
├── configs/
│   ├── lmcache-prefiller-config.yaml   # local_buffer_size=1GB, global_segment_size=30GB
│   └── lmcache-decoder-config.yaml     # local_buffer_size=1GB, global_segment_size=30GB
├── start_prefiller.sh                  # 无 NDS 环境变量
├── start_decoder.sh                    # 无 NDS 环境变量
├── mooncake_pd_proxy.py               # 与 NDS 版共用 (无 NDS 特定代码)
├── run_benchmark.py                    # 与 NDS 版共用 (无 NDS 特定代码)
├── cleanup.sh                          # 清理所有进程
└── test-plan-mooncake-traditional-pd.md
```

## 7. 配置差异详解

### 7.1 Master 启动对比

```bash
# NDS 版:
mooncake_master --use_od=true --nsid=1 --rpc_port=50051 ...

# 传统版 (本测试):
mooncake_master --rpc_port=50051 ...       # 无 --use_od --nsid
```

### 7.2 LMCache YAML 对比

```yaml
# NDS 版:
extra_config:
  global_segment_size: 0          # NDS: Master 不分配全局段
  local_buffer_size: 0            # NDS: 必须为 0

# 传统版 (本测试):
extra_config:
  global_segment_size: 32212254720    # 30GB — Master 为 Client 分配全局内存段
  local_buffer_size: 1073741824       # 1GB  — Client 本地缓冲区
```

### 7.3 环境变量对比

```bash
# NDS 版 start_prefiller.sh / start_decoder.sh:
export NDS_LIBRARY_PATH="libndskv.so"
export MC_NDS_CONFIG="nds_config.conf"

# 传统版 (本测试): 不设置以上两个变量
```

## 8. 启动流程

```bash
# 1. 将 mooncake-traditional-pd-test/ 复制到 Linux 测试服务器

# 2. 编辑变量:
#    - start_prefiller.sh / start_decoder.sh: MODEL_PATH, RDMA_STRATEGY, RDMA_DEVICE_NAME
#    - configs/ YAML: MASTER_HOST (如跨节点), RDMA device_name (如 explicit 模式)

# 3. 启动 Mooncake Master (传统模式)
bash start_mooncake_master.sh &

# 4. 启动 Prefiller
bash start_prefiller.sh &

# 5. 启动 Decoder
bash start_decoder.sh &

# 6. 启动 Proxy
python mooncake_pd_proxy.py \
    --host localhost --port 9100 \
    --prefiller-host localhost --prefiller-port 7100 \
    --decoder-host localhost --decoder-port 7200 &

# 7. 等待所有服务就绪 (~2-5 min)

# 8. 运行 Benchmark (proxy 模式)
python run_benchmark.py \
    --mode proxy \
    --model "your-model" \
    --proxy-url http://localhost:9100 \
    --prefiller-url http://localhost:7100 \
    --decoder-url http://localhost:7200

# 9. 清理
bash cleanup.sh
```

## 9. 兼容性验证要点

| 验证项 | 预期结果 | 不兼容的表现 |
|--------|---------|-------------|
| Master 启动 | 无 `--use_od --nsid` 时正常启动 | Master 启动失败或报 NDS 相关错误 |
| Client register_buffer | `local_buffer_size > 0` 时成功注册 | NDS init 误触发, 报 "NDS init memory too small" |
| KV put (prefiller) | KV 数据通过文件系统存储到 Master 分配的全局段 | put 失败或数据写入 NDS 盘框 |
| KV get (decoder) | 从 Master 全局段成功读取 KV 数据 | get 失败或从 NDS 盘框读取 |
| PD 分离流程 | Proxy: prefiller→decoder 正常工作 | Prefiller 存 KV 后 decoder 无法检索 |
| TTFT 对比 | Group B TTFT << Group A TTFT (前缀复用有效) | TTFT 无差异 (KV 未正确传递) |

**关键检查**: 在 Prefiller/Decoder 日志中确认:
- `protocol: rdma` 或 `tcp` (非 NDS 模式)
- `device_name` 正确设置
- `local_buffer_size: 1073741824` (非 0)
- `global_segment_size: 32212254720` (非 0)
- Mooncake Store `setup()` 成功
- `register_buffer()` 成功 (使用 MixedMemoryAllocator.pin_allocator.buffer)
- `put_from` / `batch_put_from` 成功存储 KV
- `batch_get_into` / `batch_is_exist` 成功检索 KV

## 10. 预期结果

与 NDS 版相同 — 如果传统模式完全兼容，应看到:

| 指标 | Group A (无命中) | Group B (前缀复用) | 改善 |
|------|----------------|----------------|------|
| Mean TTFT | ~3-5s | ~0.5-1s | ~60-80% |
| TTFT 线性下降 | Group C: TTFT 随前缀占比下降 | | |

**如果传统模式不兼容**，典型异常:
- `register_buffer` 报错 (NDS 版 Master 仍期望 nsid)
- KV 数据未实际存储 (put 返回成功但 get 找不到)
- 性能异常 (文件系统 IO 比 NDS 盘框慢，但不应导致功能失败)

## 11. 故障排查

| 问题 | 可能原因 | 检查方法 |
|------|---------|---------|
| Master 启动失败 | NDS 版 Master 要求 `--use_od` | 检查 Master 日志, 确认无 `--use_od --nsid` 时是否能正常启动 |
| Client setup 失败 | `local_buffer_size` 或 `global_segment_size` 配置错误 | 确认 YAML 中值 > 0, 非 NDS 的 0 |
| register_buffer 失败 | NDS 代码路径误触发 | 查日志无 `NDS_LIBRARY_PATH` 相关信息, 无 NDS init |
| KV 存取失败 | Master 未正确分配全局段 | 查 Master 日志的 segment 分配信息 |
| RDMA 连接失败 | RDMA 设备名错误 | `ibv_devinfo` 检查, 尝试 auto 或 TCP |
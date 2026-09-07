# Mooncake GPU PD 分离测试 — 操作手册

> 服务器: 51.36.133.128 (compute1)
> GPU: 1x NVIDIA RTX A6000 (49GB)
> 模型: Qwen2.5-7B
> 容器: vllm/vllm-openai:v0.20.2-x86_64-change-0706-done

---

## 一、端口规划

| 服务 | 端口 | 用途 |
|------|------|------|
| Prefiller (vLLM) | **7100** | 接收 prefill 请求，计算 KV cache，存到 Mooncake |
| Decoder (vLLM) | **7200** | 从 Mooncake 拉 KV cache，做 decode 生成 |
| Proxy (vLLM内置) | **19000** | 用户入口，路由请求到 P 和 D |
| Bootstrap Server | **8998** | Prefiller 启动的 ZMQ 服务，Decoder 和 Proxy 通过它发现 Prefiller |

用户只需要访问 **19000** 端口，其余端口内部通信。

---

## 二、前置准备（一次性操作）

### 2.1 确认容器存在

```bash
# 确认 vllm 容器在运行
docker ps | grep vllm-0.20.2-test-privileged

# 如果没运行，启动它
docker start vllm-0.20.2-test-privileged
```

### 2.2 修复 Mooncake CUDA 13 兼容性问题（关键！）

vLLM 容器里的 mooncake `engine.so` 编译时用 CUDA 12，但容器只有 CUDA 13。
需要从 sglang 容器拷贝 CUDA 13 版本的 mooncake 包（包括 .libs 依赖库）。

```bash
# === Step 1: 从 sglang 容器拷贝 mooncake 包到共享目录 /home ===
docker exec yyc-sglang-mooncake bash -c \
  "cp -r /usr/local/lib/python3.12/dist-packages/mooncake /home/mooncake_cuda13_backup"

# === Step 2: 拷贝 .libs 依赖库目录（libglog, libunwind 等）===
docker exec yyc-sglang-mooncake bash -c \
  "cp -r /usr/local/lib/python3.12/dist-packages/mooncake_transfer_engine_cuda13.libs /home/mooncake_libs"

# === Step 3: 替换 vllm 容器里的 mooncake 包 ===
docker exec vllm-0.20.2-test-privileged bash -c \
  "rm -rf /usr/local/lib/python3.12/site-packages/mooncake && \
   cp -r /home/mooncake_cuda13_backup /usr/local/lib/python3.12/site-packages/mooncake"

# === Step 4: 拷贝 .libs 到 vllm 容器 ===
docker exec vllm-0.20.2-test-privileged bash -c \
  "cp -r /home/mooncake_libs /usr/local/lib/python3.12/site-packages/mooncake_transfer_engine_cuda13.libs"

# === Step 5: 验证 TransferEngine 能正常导入 ===
docker exec vllm-0.20.2-test-privileged python3 -c \
  "from mooncake.engine import TransferEngine; print(TransferEngine)"
# 应输出: <class 'mooncake.engine.TransferEngine'>
```

> **为什么需要这一步？**
> - vllm 容器的 mooncake 是 CUDA 12 版本，需要 libcudart.so.12
> - 容器只有 CUDA 13，符号链接也不行（SONAME 版本校验）
> - sglang 容器有 CUDA 13 版本的 mooncake，直接拷贝过来用
> - .libs 目录包含 libglog、libunwind 等依赖，必须一起拷贝

### 2.3 上传测试代码

代码在 `mooncake-gpu-pd-test/` 目录，需要放到容器能访问的路径。

```bash
# 假设代码在本地的 d:\Code\github\Mooncake\mooncake-gpu-pd-test\
# 服务器上 /home/yyc/Mooncake/mooncake-gpu-pd-test/ 已有代码
# 如果需要更新，用 SFTP 上传：
# python -c "
# import paramiko
# c = paramiko.SSHClient()
# c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# c.connect('51.36.133.128', username='root', password='huawei@128')
# sftp = c.open_sftp()
# for f in ['start_prefiller.sh','start_decoder.sh','start_proxy.sh','run_benchmark.py','mooncake_pd_proxy.py','cleanup.sh']:
#     sftp.put(f'd:\\\\Code\\\\github\\\\Mooncake\\\\mooncake-gpu-pd-test\\\\{f}', f'/home/yyc/Mooncake/mooncake-gpu-pd-test/{f}')
# sftp.close(); c.close()
# "
```

### 2.4 确认模型存在

```bash
docker exec vllm-0.20.2-test-privileged ls /home/model/Qwen2.5-7B/
# 应看到 config.json, *.safetensors 等
```

---

## 三、启动流程（4 个终端）

### 终端 0：进入容器

```bash
docker exec -it vllm-0.20.2-test-privileged bash
cd /home/yyc/Mooncake/mooncake-gpu-pd-test
```

> 后续所有命令都在容器内执行。

### 终端 1：启动 Prefiller

```bash
cd /home/yyc/Mooncake/mooncake-gpu-pd-test
bash start_prefiller.sh
```

**等 60 秒左右**，看到以下日志说明启动成功：
```
INFO: Application startup complete.
INFO: Uvicorn running on http://0.0.0.0:7100
Mooncake Bootstrap Server started at 0.0.0.0:8998
```

**验证**：
```bash
curl -s http://127.0.0.1:7100/v1/models
# 应返回模型信息 JSON
```

### 终端 2：启动 Decoder

```bash
cd /home/yyc/Mooncake/mooncake-gpu-pd-test
bash start_decoder.sh
```

**等 60 秒左右**，看到以下日志说明启动成功：
```
INFO: Application startup complete.
INFO: Uvicorn running on http://0.0.0.0:7200
Initializing Mooncake Transfer Engine Scheduler
```

**验证**：
```bash
curl -s http://127.0.0.1:7200/v1/models
```

### 终端 3：启动 Proxy

```bash
cd /home/yyc/Mooncake/mooncake-gpu-pd-test
bash start_proxy.sh
```

看到以下日志说明启动成功：
```
Got 1 prefill clients and 1 decode clients.
Inited prefiller http://localhost:7100 with dp_size=1
All prefiller instances are ready.
INFO: Uvicorn running on http://0.0.0.0:19000
```

---

## 四、测试

### 4.1 基本推理测试

```bash
# 在容器内执行
# 先写 JSON payload（避免 shell 引号问题）
python3 -c "
import json
json.dump({
    'model': '/home/model/Qwen2.5-7B',
    'prompt': 'What is 2+2? Answer briefly.',
    'max_tokens': 20,
    'temperature': 0
}, open('/tmp/test_payload.json', 'w'))
"

# 通过 proxy 发送请求
curl -s http://127.0.0.1:19000/v1/completions \
    -H "Content-Type: application/json" \
    -d @/tmp/test_payload.json
```

**预期输出**：
```json
{
  "choices": [{"text": " 2+2=4.", "finish_reason": "stop"}],
  "usage": {"prompt_tokens": 10, "completion_tokens": 8}
}
```

### 4.2 验证 PD 流程

检查三个服务都收到请求：
```bash
# Prefiller 应有 POST /v1/completions 200 OK
grep "POST /v1" /tmp/prefiller.log | tail -3

# Decoder 应有 POST /v1/completions 200 OK
grep "POST /v1" /tmp/decoder.log | tail -3

# Proxy 应有 POST /v1/completions 200 OK
grep "POST /v1" /tmp/proxy.log | tail -3
```

### 4.3 运行 Benchmark

```bash
cd /home/yyc/Mooncake/mooncake-gpu-pd-test

# 先写测试 payload
python3 -c "
import json
json.dump({'model':'/home/model/Qwen2.5-7B','prompt':'What is 2+2?','max_tokens':10,'temperature':0}, open('/tmp/bench_payload.json','w'))
"

# Smoke test（5 个请求，Group A 基线测试）
python3 run_benchmark.py \
    --mode proxy \
    --model /home/model/Qwen2.5-7B \
    --proxy-url http://localhost:19000 \
    --prefiller-url http://localhost:7100 \
    --decoder-url http://localhost:7200 \
    --num-prompts 5 \
    --groups A
```

---

## 五、代码文件说明

```
mooncake-gpu-pd-test/
├── start_prefiller.sh     # 启动 Prefiller (kv_producer)
├── start_decoder.sh       # 启动 Decoder (kv_consumer)
├── start_proxy.sh         # 启动 vLLM 内置 Mooncake PD Proxy
├── mooncake_pd_proxy.py   # 自定义 Proxy（备用，当前不用）
├── run_benchmark.py       # Benchmark 脚本（TTFT 测量）
├── cleanup.sh             # 清理脚本
├── start_mooncake_master.sh # Mooncake Master（P2P 模式不需要）
├── Dockerfile             # Docker 构建文件
└── configs/               # LMCache 配置（当前不用，P2P 模式不需要）
```

### 关键参数对比（start_prefiller.sh vs start_decoder.sh）

| 参数 | Prefiller | Decoder |
|------|-----------|---------|
| `--port` | 7100 | 7200 |
| `CUDA_VISIBLE_DEVICES` | 0 | 0（同一张卡） |
| `--gpu-memory-utilization` | 0.40 | 0.40 |
| `--max-model-len` | 8192 | 8192 |
| `kv_role` | **kv_producer** | **kv_consumer** |
| 其他参数 | 完全相同 | 完全相同 |

---

## 六、架构说明

```
                    用户请求
                       │
                       ▼
              ┌────────────────┐
              │  Proxy :19000  │  ← vLLM 内置 mooncake_connector_proxy.py
              └───┬────────┬───┘
                  │        │
    ┌─────────────┘        └─────────────┐
    ▼                                     ▼
┌─────────────────┐              ┌─────────────────┐
│ Prefiller :7100 │              │ Decoder :7200   │
│ kv_producer      │              │ kv_consumer     │
│                  │              │                  │
│ 1.接收prompt     │              │ 3.接收请求       │
│ 2.prefill计算KV  │              │ 4.从Mooncake拉KV │
│   存到Mooncake   │              │ 5.decode生成     │
│   (RDMA/TCP)     │              │ 6.返回结果       │
└────────┬────────┘              └──────────────────┘
         │
         ▼
   ┌───────────────┐
   │ Mooncake P2P   │  ← TransferEngine (RDMA)
   │ Bootstrap:8998 │  ← ZMQ 握手通道
   └───────────────┘
```

**PD Proxy 工作流程**（push-based）：
1. Proxy 生成 transfer_id (UUID)
2. 异步发给 Prefiller：`do_remote_decode=true, transfer_id`（fire-and-forget）
3. 立即发给 Decoder：`do_remote_prefill=true, remote_bootstrap_addr, remote_engine_id, transfer_id`
4. Decoder 从 Prefiller 拉取 KV，decode 生成，流式返回

---

## 七、常见问题

### Q1: `RuntimeError: Mooncake is not available`

**原因**：mooncake 的 `engine.so` 找不到 `libcudart.so.12` 或依赖库

**解决**：执行第 2.2 节的 mooncake 库替换步骤

### Q2: `ValueError: KV cache memory ... larger than available`

**原因**：`max_model_len` 太大（默认 131072），KV cache 显存不够

**解决**：加 `--max-model-len 8192`（已在脚本中添加）

### Q3: `JSONDecodeError` from Proxy

**原因**：curl 的 JSON 引号被 shell 转义搞坏

**解决**：用 python3 写 JSON 文件，再用 `curl -d @/tmp/xxx.json`

### Q4: 两个 vLLM 实例显存不够

**原因**：单 GPU 49GB，两个 7B 实例各需 ~19GB

**解决**：`--gpu-memory-utilization 0.40`（已设好）；如果还不够，降到 0.35

### Q5: no_proxy 没设导致 localhost 通信失败

**原因**：华为服务器有全局 http_proxy，localhost 请求被代理截获

**解决**：脚本已设 `export no_proxy=127.0.0.1,localhost`

---

## 八、停止服务

```bash
# 在容器内
pkill -f vllm          # 杀所有 vLLM 进程
pkill -f mooncake      # 杀 mooncake 相关进程
pkill -f proxy         # 杀 proxy 进程

# 或者用清理脚本
bash cleanup.sh
```

---

## 九、关键环境变量

| 变量 | 值 | 作用 |
|------|-----|------|
| `CUDA_VISIBLE_DEVICES` | 0 | GPU 隔离（P 和 D 都用 GPU 0） |
| `PYTHONHASHSEED` | 0 | KV hash 一致性（P 和 D 必须相同） |
| `VLLM_MOONCAKE_BOOTSTRAP_PORT` | 8998 | ZMQ 握手端口 |
| `no_proxy` / `NO_PROXY` | 127.0.0.1,localhost | 绕过华为代理 |

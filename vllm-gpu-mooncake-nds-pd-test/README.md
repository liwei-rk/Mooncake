# vLLM + Mooncake GPU PD 分离测试 — 完整操作手册

> **服务器**: 51.36.133.128 (RTX A6000 49GB)  
> **模型**: Qwen2.5-7B  
> **容器**: vllm-0.20.2-test-privileged  
> **框架**: vLLM 0.20.2 + MooncakeConnector (内置, P2P 模式)

---

## 一、这套东西是在干嘛？

简单说就是把 LLM 推理拆成两步：**Prefill（算 KV cache）** 和 **Decode（生成 token）**，分别跑在两个 vLLM 实例上。中间通过 Mooncake TransferEngine 传 KV cache。

为什么要拆？因为 prefill 是计算密集的（吃 GPU 算力），decode 是访存密集的（吃显存带宽）。混在一起跑会互相拖后腿。拆开以后各干各的，GPU 利用率更高。

这套测试用的是 vLLM 内置的 MooncakeConnector，走 P2P 模式，不需要单独起 Mooncake Master。Prefiller 和 Decoder 通过 ZMQ 握手自动发现彼此，简单很多。

---

## 二、端口规划

| 服务 | 端口 | 干什么的 |
|------|------|----------|
| Prefiller (vLLM) | **7100** | 接收请求，算 KV，存到 Mooncake |
| Decoder (vLLM) | **7200** | 从 Mooncake 拉 KV，做 decode 生成 |
| Proxy | **19000** | 用户入口，负责路由请求到 P 和 D |
| Bootstrap Server | **8998** | ZMQ 握手通道，Decoder 和 Proxy 通过它发现 Prefiller |

用户只需要访问 **19000**，其余端口内部通信。

---

## 三、前置准备

### 3.1 确认容器在跑

```bash
docker ps | grep vllm-0.20.2-test-privileged
# 没跑的话启动它
docker start vllm-0.20.2-test-privileged
```

### 3.2 修复 Mooncake CUDA 13 不兼容（最坑的一步）

这个问题卡了好久。vLLM 容器里的 mooncake `engine.so` 编译时绑定的是 CUDA 12，但容器只有 CUDA 13。直接跑会报 `libcudart.so.12: cannot open shared object file`，就算你建软链接也不行，因为 SONAME 版本校验过不去。

**解决方案**：从 SGLang 容器（有 CUDA 13 版本的 mooncake）整套拷过来。

```bash
# Step 1: 从 sglang 容器拷贝 mooncake 包到共享目录
docker exec yyc-sglang-mooncake bash -c \
  "cp -r /usr/local/lib/python3.12/dist-packages/mooncake /home/mooncake_cuda13_backup"

# Step 2: 拷贝 .libs 依赖库目录（libglog, libunwind 等，不拷会段错误）
docker exec yyc-sglang-mooncake bash -c \
  "cp -r /usr/local/lib/python3.12/dist-packages/mooncake_transfer_engine_cuda13.libs /home/mooncake_libs"

# Step 3: 替换 vllm 容器里的 mooncake 包
docker exec vllm-0.20.2-test-privileged bash -c \
  "rm -rf /usr/local/lib/python3.12/site-packages/mooncake && \
   cp -r /home/mooncake_cuda13_backup /usr/local/lib/python3.12/site-packages/mooncake"

# Step 4: 拷贝 .libs 到 vllm 容器
docker exec vllm-0.20.2-test-privileged bash -c \
  "cp -r /home/mooncache_libs /usr/local/lib/python3.12/site-packages/mooncake_transfer_engine_cuda13.libs"

# Step 5: 验证导入成功
docker exec vllm-0.20.2-test-privileged python3 -c \
  "from mooncake.engine import TransferEngine; print(TransferEngine)"
# 输出 <class 'mooncake.engine.TransferEngine'> 就对了
```

> **.libs 目录千万别忘了拷**。里面是 libglog、libunwind 这些 C++ 依赖。mooncake 的 Python 包不带这些，只靠 `.libs` 目录提供。漏了就是一堆 `undefined symbol`。

### 3.3 上传测试代码

代码在 `vllm-gpu-mooncake-nds-pd-test/` 目录。服务器上的路径是 `/home/yyc/Mooncake/vllm-gpu-mooncake-nds-pd-test/`。

如果改了代码需要重新上传，用 SFTP 推上去就行（ssh_cmd.py 或 scp 都行）。

### 3.4 确认模型在

```bash
docker exec vllm-0.20.2-test-privileged ls /home/model/Qwen2.5-7B/
# 应该看到 config.json, *.safetensors 等
```

---

## 四、启动流程

需要开 **3 个终端**，全部 `docker exec -it` 进 vLLM 容器。

### 终端 1：启动 Prefiller

```bash
docker exec -it vllm-0.20.2-test-privileged bash
cd /home/yyc/Mooncake/vllm-gpu-mooncake-nds-pd-test
bash start_prefiller.sh
```

等大概 60 秒，看到这些日志就说明起来了：

```
INFO: Application startup complete.
INFO: Uvicorn running on http://0.0.0.0:7100
Mooncake Bootstrap Server started at 0.0.0.0:8998
```

验证一下：

```bash
curl -s http://127.0.0.1:7100/v1/models
```

### 终端 2：启动 Decoder

```bash
docker exec -it vllm-0.20.2-test-privileged bash
cd /home/yyc/Mooncake/vllm-gpu-mooncake-nds-pd-test
bash start_decoder.sh
```

同样等 60 秒左右：

```
INFO: Application startup complete.
INFO: Uvicorn running on http://0.0.0.0:7200
Initializing Mooncake Transfer Engine Scheduler
```

验证：

```bash
curl -s http://127.0.0.1:7200/v1/models
```

### 终端 3：启动 Proxy

```bash
docker exec -it vllm-0.20.2-test-privileged bash
cd /home/yyc/Mooncake/vllm-gpu-mooncake-nds-pd-test
bash start_proxy.sh
```

Proxy 会先等 Prefiller 和 Decoder 就绪，然后输出：

```
Got 1 prefill clients and 1 decode clients.
Inited prefiller http://localhost:7100 with dp_size=1
All prefiller instances are ready.
INFO: Uvicorn running on http://0.0.0.0:19000
```

---

## 五、测试

### 5.1 基本推理

```bash
# 先写 JSON payload（别直接 curl -d，引号会被 shell 搞坏）
python3 -c "
import json
json.dump({
    'model': '/home/model/Qwen2.5-7B',
    'prompt': 'What is 2+2? Answer briefly.',
    'max_tokens': 20,
    'temperature': 0
}, open('/tmp/test_payload.json', 'w'))
"

# 通过 proxy 发请求
curl -s http://127.0.0.1:19000/v1/completions \
    -H "Content-Type: application/json" \
    -d @/tmp/test_payload.json
```

预期输出：

```json
{
  "choices": [{"text": " 2+2=4.", "finish_reason": "stop"}],
  "usage": {"prompt_tokens": 10, "completion_tokens": 8}
}
```

### 5.2 验证 PD 链路确实走了

光看到回复还不够，得确认 KV 确实通过 Mooncake 传了：

```bash
grep "POST /v1" /tmp/prefiller.log | tail -3   # Prefiller 收到了请求
grep "POST /v1" /tmp/decoder.log | tail -3     # Decoder 也收到了请求
grep "POST /v1" /tmp/proxy.log | tail -3       # Proxy 路由成功
```

### 5.3 跑 Benchmark

```bash
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
   │ Mooncake P2P   │  ← TransferEngine
   │ Bootstrap:8998 │  ← ZMQ 握手通道
   └───────────────┘
```

**Proxy 的工作流程**（push-based 协议）：

1. Proxy 生成 `transfer_id` (UUID)
2. 异步发给 Prefiller：`do_remote_decode=true, transfer_id`（fire-and-forget，不等回复）
3. 立即发给 Decoder：`do_remote_prefill=true, remote_bootstrap_addr, remote_engine_id, transfer_id`
4. Decoder 从 Prefiller 拉取 KV，decode 生成，流式返回给用户

> 这里有个细节：MooncakeConnector 是 push-based 的，意味着 Prefiller 算完 KV 后主动存到 Mooncake，Decoder 再去拉。Proxy 只负责发指令和凑 `kv_transfer_params`，不碰 KV 数据本身。

---

## 七、踩过的坑 & 解决方案

### 坑 1：GPU 显存被老进程占着

**现象**：启动 vLLM 报 `Free memory on device cuda:0 (8.37/47.41 GiB)`，明明没跑什么但显存快满了。

**原因**：之前的测试进程没清干净，vLLM/Mooncake 进程还挂着占显存。

**解决**：进容器先杀一波：
```bash
pkill -9 -f vllm
pkill -9 -f python
pkill -9 -f proxy
```
然后 `nvidia-smi` 确认显存清零了再启动。

### 坑 2：Mooncake CUDA 12/13 不兼容

**现象**：`from mooncake.engine import TransferEngine` 报错，找不到 `libcudart.so.12`。

**原因**：vLLM 容器只有 CUDA 13，但 mooncake 的 `.so` 文件编译时绑了 CUDA 12。SONAME 校验过不去，建软链接也没用。

**解决**：看上面 3.2 节，从 SGLang 容器整套拷 CUDA 13 版本的 mooncake 过来。注意 `.libs` 目录一定要一起拷。

### 坑 3：run_benchmark.py 报 TypeError

**现象**：`TypeError: 'async_generator' object is not iterable`，在 83 行附近。

**原因**：`response.aiter_lines()` 返回的是 async generator，得用 `async for` 遍历，但代码写的是普通 `for`。

**解决**：
```python
# 错的
for line in response.aiter_lines():
# 对的
async for line in response.aiter_lines():
```

### 坑 4：KV cache 显存不够

**现象**：`ValueError: KV cache memory ... larger than available GPU memory`。

**原因**：vLLM 默认 `max_model_len=131072`，这么长的上下文需要的 KV cache 显存远超 49GB。

**解决**：加 `--max-model-len 8192`，限制最大上下文长度。7B 模型在 8192 上下文下 KV cache 约需 5GB，加上权重 14GB，总共约 19GB，49GB 的卡完全放得下。

### 坑 5：localhost 通信被华为代理截获

**现象**：vLLM 内部 HTTP 请求（发到 localhost:7100 之类）全部超时或返回奇怪的错误。

**原因**：服务器在华为内网，配了全局 `http_proxy=proxyhk.huawei.com:8080`。Python 的 httpx/requests 库会读这个环境变量，把 localhost 请求也往代理服务器发。

**解决**：所有启动脚本里加：
```bash
export no_proxy=127.0.0.1,localhost
export NO_PROXY=127.0.0.1,localhost
```

### 坑 6：curl 发 JSON 引号被 shell 吃掉

**现象**：`curl -d '{"model":"...","prompt":"..."}'` 报 JSON 解析错误。

**原因**：shell 对单引号、双引号、嵌套引号的处理很混乱，手动拼 JSON 基本必出错。

**解决**：用 Python 写 JSON 文件，再用 `curl -d @/tmp/xxx.json`：
```bash
python3 -c "import json; json.dump({...}, open('/tmp/req.json','w'))"
curl -s http://127.0.0.1:19000/v1/completions -H "Content-Type: application/json" -d @/tmp/req.json
```

### 坑 7：Proxy 健康检查 404 导致 benchmark 卡住

**现象**：`run_benchmark.py` 一直等不到服务就绪，但其实服务已经起来了。

**原因**：vLLM 内置的 `mooncake_connector_proxy.py` 只实现了 `POST /v1/completions` 和 `POST /v1/chat/completions`，没有 `GET /v1/models`。健康检查发 `GET /v1/models` 拿到 404，代码以为服务没起来。

**解决**：改 `run_benchmark.py` 的健康检查逻辑——任何 HTTP 响应（200/404 都行）都算服务起来了，只有连接失败才算没起来。

---

## 八、关键参数对比

| 参数 | Prefiller | Decoder | 为什么不一样 |
|------|-----------|---------|-------------|
| `--port` | 7100 | 7200 | 两个实例不能撞端口 |
| `CUDA_VISIBLE_DEVICES` | 0 | 0 | 单 GPU 模式，共享同一张卡 |
| `--gpu-memory-utilization` | 0.40 | 0.40 | 各占 40%，49GB×0.4≈19.6GB，够放 7B+KV |
| `--max-model-len` | 8192 | 8192 | 限制上下文长度，防 KV cache 撑爆显存 |
| `kv_role` | **kv_producer** | **kv_consumer** | P 算 KV 存到 Mooncake，D 从 Mooncake 拉 KV |
| `--enforce-eager` | ✓ | ✓ | 禁用 CUDA Graph，调试阶段看完整错误栈 |
| `--no-enable-prefix-caching` | ✓ | ✓ | 禁用 vLLM 内置缓存，只用 Mooncake 的 KV |
| `VLLM_MOONCAKE_PROTOCOL` | tcp | tcp | 先用 TCP 验证流程，跑通后切 RDMA 测性能 |
| `PYTHONHASHSEED` | 0 | 0 | KV hash 一致性，P 和 D 必须用相同种子 |

> **为什么 P 和 D 各占 40% 而不是 50%/50%？** 因为 vLLM 启动时还有额外的 overhead（CUDA context、临时 buffer 等），留 20% 余量防止 OOM。实测 40% 能稳跑。

---

## 九、停止服务

```bash
# 在容器内执行
bash cleanup.sh

# 或手动
pkill -f "vllm serve"
pkill -f "mooncake_connector_proxy"
pkill -f "mooncake"
```

然后 `nvidia-smi` 确认显存清零。

---

## 十、文件说明

```
vllm-gpu-mooncake-nds-pd-test/
├── start_prefiller.sh          # 启动 Prefiller (kv_producer)
├── start_decoder.sh             # 启动 Decoder (kv_consumer)
├── start_proxy.sh               # 启动 vLLM 内置 Mooncake PD Proxy
├── mooncake_pd_proxy.py         # 自定义 Proxy（备用，当前不用）
├── run_benchmark.py             # Benchmark 脚本（测 TTFT）
├── cleanup.sh                   # 清理脚本
├── start_mooncake_master.sh     # Mooncake Master（P2P 模式不需要，留作参考）
├── Dockerfile                   # Docker 构建文件
└── configs/                     # LMCache 配置（P2P 模式不需要，留作参考）
```

---

## 十一、关键环境变量

| 变量 | 值 | 为什么需要 |
|------|-----|-----------|
| `CUDA_VISIBLE_DEVICES` | 0 | GPU 隔离，防止 vLLM 抢所有卡 |
| `PYTHONHASHSEED` | 0 | KV hash 一致性，P 和 D 必须相同 |
| `VLLM_MOONCAKE_PROTOCOL` | tcp | 传输协议，先用 TCP 验证，再切 RDMA |
| `VLLM_MOONCAKE_BOOTSTRAP_PORT` | 8998 | ZMQ 握手端口 |
| `no_proxy` / `NO_PROXY` | 127.0.0.1,localhost | 绕过华为代理，防 localhost 通信被截 |

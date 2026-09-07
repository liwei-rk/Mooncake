#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_prefiller.sh — SGLang PD Prefiller — GPU 版本
#
# SGLang 的 PD 分离使用内置 disaggregation 模式：
#   --disaggregation-mode prefill: 只做 prefill，KV 通过 Mooncake 传给 decoder
#   --disaggregation-transfer-backend mooncake: 使用 Mooncake TransferEngine（默认）
#   --disaggregation-bootstrap-port: ZMQ 握手端口，decoder 和 router 通过它发现 prefiller
#
# 与 vLLM 的区别：
#   vLLM 用 --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer"}'
#   SGLang 用 --disaggregation-mode prefill（更简洁，内置支持）
#   SGLang 不需要单独的 mooncake_connector_proxy.py，有内置 router

set -e

# === 核心变量 ===
MODEL_PATH="/home/model/Qwen2.5-7B"       # 模型路径
PREFILLER_GPU_ID=0                      # Prefiller 用 GPU 0
PREFILLER_PORT=7100                      # SGLang 服务端口
GPU_MEM_UTIL=0.35                        # Prefiller 用 35% 显存（只需存权重+少量KV）
                                         # SGLang 的 mem-fraction-static 比 vLLM 需要更多开销
                                         # Prefiller 只做 prefill(max_tokens=1)，不需要大 KV cache

# === GPU 设备隔离 ===
export CUDA_VISIBLE_DEVICES="${PREFILLER_GPU_ID}"

# KVCache hash 一致性（prefiller 和 decoder 必须相同）
export PYTHONHASHSEED=0

# === no_proxy 防火墙 ===
# 防止 HTTP_PROXY 环境变量干扰 localhost 通信
export no_proxy=127.0.0.1,localhost
export NO_PROXY=127.0.0.1,localhost

echo "=== Starting SGLang Prefiller (disaggregation-mode=prefill) on GPU ${PREFILLER_GPU_ID} ==="
echo "Transfer backend: mooncake (default)"
echo "Model: ${MODEL_PATH}"
echo "Port: ${PREFILLER_PORT}"
echo ""

# === 启动 SGLang serve ===
# --disaggregation-mode prefill: prefill-only 模式
# --disaggregation-transfer-backend mooncake: 用 Mooncake TransferEngine（默认值，可省略）
# --disaggregation-bootstrap-port 8998: ZMQ 握手端口
# --mem-fraction-static: SGLang 的显存比例参数（等价于 vLLM 的 --gpu-memory-utilization）
# --disable-radix-cache: 禁用 SGLang 内置 radix cache（等价于 vLLM 的 --no-enable-prefix-caching）
python3 -m sglang.launch_server \
    --model-path "${MODEL_PATH}" \
    --host 0.0.0.0 \
    --port ${PREFILLER_PORT} \
    --tp 1 \
    --mem-fraction-static ${GPU_MEM_UTIL} \
    --context-length 8192 \
    --disable-radix-cache \
    --trust-remote-code \
    --disaggregation-mode prefill \
    --disaggregation-bootstrap-port 8998

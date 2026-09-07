#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_decoder.sh — SGLang PD Decoder — GPU 版本
#
# Decoder 接收 prefiller 传来的 KV cache，做 token-by-token decode 生成
# 必须先启动 Prefiller，再启动 Decoder
# Decoder 通过 bootstrap port 连接 Prefiller 获取 KV 位置信息

set -e

# === 核心变量 ===
MODEL_PATH="/home/model/Qwen2.5-7B"       # 必须和 prefiller 一致
DECODER_GPU_ID=0                        # 单 GPU 模式：和 prefiller 同一张卡
DECODER_PORT=7200                        # SGLang 服务端口
GPU_MEM_UTIL=0.55                        # Decoder 用 55% 显存（需要更多 KV cache 做 decode）
                                         # SGLang 的 mem-fraction-static 比 vLLM 需要更多开销
                                         # 49GB * 0.55 = ~25.8GB，权重14GB + KV cache ~11GB

# === GPU 设备隔离 ===
export CUDA_VISIBLE_DEVICES="${DECODER_GPU_ID}"

# KVCache hash 一致性
export PYTHONHASHSEED=0

# === no_proxy 防火墙 ===
export no_proxy=127.0.0.1,localhost
export NO_PROXY=127.0.0.1,localhost

echo "=== Starting SGLang Decoder (disaggregation-mode=decode) on GPU ${DECODER_GPU_ID} ==="
echo "Transfer backend: mooncake (default)"
echo "Model: ${MODEL_PATH}"
echo "Port: ${DECODER_PORT}"
echo ""

# === 启动 SGLang serve ===
# --disaggregation-mode decode: decode-only 模式
# --disaggregation-bootstrap-port 8998: 必须和 prefiller 一致
# --base-gpu-id 0: 在同一张 GPU 上（单 GPU 模式）
python3 -m sglang.launch_server \
    --model-path "${MODEL_PATH}" \
    --host 0.0.0.0 \
    --port ${DECODER_PORT} \
    --tp 1 \
    --mem-fraction-static ${GPU_MEM_UTIL} \
    --context-length 4096 \
    --disable-radix-cache \
    --disable-cuda-graph \
    --trust-remote-code \
    --disaggregation-mode decode \
    --disaggregation-bootstrap-port 8998 \
    --base-gpu-id 0

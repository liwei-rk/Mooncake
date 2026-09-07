#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_decoder.sh — 启动 PD Decoder (kv_consumer) — GPU 版本
#
# !!! 什么是 Decoder? !!!
# Decoder 专门做 decode（解码）阶段：
#   1. 接收 kv_transfer_params（包含 KV 在 Mooncake 中的位置信息）
#   2. 从 Mooncake TransferEngine 拉取 KV cache 到 GPU 显存
#   3. 基于 KV cache 做 token-by-token 的自回归生成
# Decoder 不需要重新 prefill，所以 TTFT（首 token 延迟）大幅降低
#
# !!! 启动顺序 !!!
# 必须先启动 Prefiller → 再启动 Decoder
# Decoder 启动后会通过 ZMQ side channel 连接 Prefiller 的 bootstrap port
# 如果 Prefiller 没启动，Decoder 会一直等待连接

set -e

# === 核心变量 ===
MODEL_PATH="/home/model/Qwen2.5-7B"       # 必须和 prefiller 用的模型完全一致！
                                         # 为什么必须一致？KV cache 的结构（层数、head 数、hidden dim）
                                         # 完全由模型决定。不同模型的 KV 不能互换
DECODER_GPU_ID=0                        # 单 GPU 模式：Decoder 也用 GPU 0（和 prefiller 同一张卡）
                                         # 正常 PD 分离应该用不同 GPU，但此服务器只有 1 张 RTX A6000
                                         # 两个 vLLM 实例各用 40% 显存（49GB * 0.40 = ~19.6GB/实例）
                                         # 7B 权重 ~14GB + KV 缓存 ~5GB，刚好放得下
DECODER_VLLM_PORT=7200                  # vLLM OpenAI API 端口（和 prefiller 的 7100 不同）
GPU_MEM_UTIL=0.40                        # 和 prefiller 一样的显存比例

# === GPU 设备隔离 ===
export CUDA_VISIBLE_DEVICES="${DECODER_GPU_ID}"
# !!! 注意：这里 GPU ID 是 1，不是 0 !!!
# 因为 prefiller 已经占了 GPU 0
# 两张卡各自独立工作，互不干扰

# KVCache hash 一致性（和 prefiller 相同的种子）
export PYTHONHASHSEED=0

# === Mooncake 传输协议配置 ===
# 必须和 prefiller 用同样的协议！
# TCP 对 TCP，RDMA 对 RDMA，不能混用
export VLLM_MOONCAKE_PROTOCOL="tcp"

# Bootstrap port: 必须和 prefiller 的 VLLM_MOONCAKE_BOOTSTRAP_PORT 一致
# decoder 通过这个端口连接 prefiller
export VLLM_MOONCAKE_BOOTSTRAP_PORT=8998

# LD_LIBRARY_PATH: 修复 CUDA 12/13 不匹配问题
export LD_LIBRARY_PATH=/usr/local/lib/python3.12/dist-packages/nvidia/cuda_runtime/lib:${LD_LIBRARY_PATH}

# !!! no_proxy 防火墙 !!!
# 和 prefiller 一样，防止 HTTP proxy 干扰 localhost 通信
export no_proxy=127.0.0.1,localhost
export NO_PROXY=127.0.0.1,localhost

echo "=== Starting Decoder (kv_consumer) on GPU ${DECODER_GPU_ID} ==="
echo "Connector: MooncakeConnector (vLLM built-in, P2P handshake mode)"
echo "Protocol: ${VLLM_MOONCAKE_PROTOCOL}"
echo "Model: ${MODEL_PATH}"
echo "Port: ${DECODER_VLLM_PORT}"
echo ""

# === 启动 vLLM serve ===
# vLLM 0.21.0 用 --no-enable-log-requests（不是旧版 --disable-log-requests）
# kv_role: kv_consumer = 这个 vLLM 实例从 Mooncake 拉取 KV（消费）
# 其他参数和 prefiller 相同，只有 kv_role 不同
vllm serve "${MODEL_PATH}" \
    --port ${DECODER_VLLM_PORT} \
    --tensor-parallel-size 1 \
    --gpu-memory-utilization ${GPU_MEM_UTIL} \
    --max-model-len 8192 \
    --no-enable-log-requests \
    --enforce-eager \
    --no-enable-prefix-caching \
    --kv-transfer-config \
    '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer"}'

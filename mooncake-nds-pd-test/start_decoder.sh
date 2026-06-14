#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_decoder.sh — Start PD Decoder (kv_consumer)

set -e

# === 核心变量 (必改) ===
MODEL_PATH="/home/models/Qwen2.5-7B"       # 模型文件路径
DECODER_NPU_IDS="4,5,6,7"             # Decoder NPU IDs (逗号分隔, 对应 ASCEND_RT_VISIBLE_DEVICES)
DECODER_TP_SIZE=4                      # Decoder tensor-parallel size (须与 NPU 数量一致)
DECODER_VLLM_PORT=7200                 # vLLM 服务端口
MASTER_HOST="localhost"
MASTER_PORT=50051
METADATA_PORT=8005

# === RDMA 配置 ===
# RDMA_STRATEGY: "auto" | "explicit" | "tcp"
#   auto     — 自动发现 RDMA 设备 (设置 MC_MS_AUTO_DISC=1)
#   explicit — 使用 RDMA_DEVICE_NAME 指定的设备
#   tcp      — 降级为 TCP 协议 (需同时修改 YAML 中 protocol: "tcp")
RDMA_STRATEGY="auto"

# RDMA_DEVICE_NAME: RDMA 设备名，仅在 RDMA_STRATEGY=explicit 时生效
# 单设备示例: "mlx5_0", "roce_eth0", "erdma_0"
# 多设备示例: "mlx5_0,mlx5_1" (逗号分隔，round-robin 分配)
# 如果 RDMA_STRATEGY=auto，此值忽略 (Mooncake 自动发现)
RDMA_DEVICE_NAME=""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/configs/lmcache-decoder-config.yaml"

# NPU 设备隔离
export ASCEND_RT_VISIBLE_DEVICES="${DECODER_NPU_IDS}"

# NDS 环境变量
export NDS_LIBRARY_PATH="${NDS_LIBRARY_PATH:-libndskv.so}"
export MC_NDS_CONFIG="${MC_NDS_CONFIG:-nds_config.conf}"
export MC_METADATA_SERVER="http://${MASTER_HOST}:${METADATA_PORT}/metadata"

# KVCache hash 一致
export PYTHONHASHSEED=0

# === RDMA 环境变量配置 ===
case "${RDMA_STRATEGY}" in
    auto)
        export MC_MS_AUTO_DISC=1
        export MOONCAKE_DEVICE=""
        echo "RDMA strategy: AUTO-DISCOVERY (MC_MS_AUTO_DISC=1)"
        ;;
    explicit)
        export MC_MS_AUTO_DISC=0
        export MOONCAKE_DEVICE="${RDMA_DEVICE_NAME}"
        echo "RDMA strategy: EXPLICIT device='${RDMA_DEVICE_NAME}'"
        ;;
    tcp)
        echo "RDMA strategy: TCP fallback (确保 YAML 中 protocol: tcp)"
        ;;
    *)
        echo "ERROR: Unknown RDMA_STRATEGY='${RDMA_STRATEGY}'. Use: auto | explicit | tcp"
        exit 1
        ;;
esac

echo "=== Starting Decoder (kv_consumer) on NPUs ${DECODER_NPU_IDS} (tp=${DECODER_TP_SIZE}) ==="
echo "Model: ${MODEL_PATH}"
echo "Port: ${DECODER_VLLM_PORT}"
echo "LMCache config: ${CONFIG_FILE}"
echo "RDMA strategy: ${RDMA_STRATEGY}"
echo "MOONCAKE_DEVICE: ${MOONCAKE_DEVICE}"

LMCACHE_CONFIG_FILE="${CONFIG_FILE}" \
vllm serve "${MODEL_PATH}" \
    --port ${DECODER_VLLM_PORT} \
    --tensor-parallel-size ${DECODER_TP_SIZE} \
    --enforce-eager \
    --no-enable-prefix-caching \
    --kv-transfer-config \
    '{"kv_connector":"LMCacheAscendConnector","kv_role":"kv_consumer"}'
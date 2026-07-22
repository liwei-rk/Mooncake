#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_mooncake_master.sh — 启动 Mooncake Master (传统文件系统模式)
#
# !!! 重要：内置 MooncakeConnector 不需要此脚本 !!!
# vLLM 0.13+ 内置的 MooncakeConnector 使用 P2P 握手模式 (P2PHANDSHAKE)
# prefiller 和 decoder 通过 ZMQ side channel 直接通信，不需要 Master
#
# 此脚本仅在你需要测试传统 Master-Worker 模式时使用（例如 LMCache connector）
# 当前的 start_prefiller.sh 和 start_decoder.sh 使用内置 MooncakeConnector
# 所以正常运行 PD 测试时不需要执行此脚本
#
# !!! 为什么先启动 Master? (传统模式) !!!
# Mooncake 架构是 Master-Worker 模式：
#   1. Master 负责管理全局内存段分配和节点发现
#   2. Metadata Server 负责记录 KV 位置信息（哪块 KV 在哪个 worker 的哪段内存）
#   3. prefiller 和 decoder 启动时会连接 Master 注册自己
# 所以 Master 必须先启动，否则 prefiller/decoder 会连接失败

set -e

# === 端口配置 ===
MASTER_HOST="localhost"
MASTER_PORT=50051             # RPC 端口
METADATA_PORT=8081            # Metadata HTTP 端口

echo "=== Starting Mooncake Master (Traditional Mode) ==="
echo "  !!! 注意: 内置 MooncakeConnector (P2P 模式) 不需要 Master !!!"
echo "  !!! 此脚本仅用于传统 Master-Worker 模式测试 !!!"
echo "  Master RPC:  ${MASTER_HOST}:${MASTER_PORT}"
echo "  Metadata:    ${MASTER_HOST}:${METADATA_PORT}"

# !!! 为什么不需要单独启动 mooncake_http_metadata_server? !!!
# Mooncake master.cpp 源码：
#   DEFINE_bool(enable_http_metadata_server, false,
#       "Whether to enable the HTTP metadata server within the master service.
#        If enabled, metadata will be accessible via HTTP on
#        http_metadata_server_port. If disabled, a separate
#        mooncake_http_metadata_server process should be used.")
# 设 --enable_http_metadata_server=true 后，metadata server 嵌入在 master 进程里
# 不需要单独启动 mooncake_http_metadata_server，否则端口冲突！

mooncake_master \
    --rpc_address=0.0.0.0 \
    --rpc_port=${MASTER_PORT} \
    --enable_http_metadata_server=true \
    --http_metadata_server_port=${METADATA_PORT} \
    --metrics_port=9003 \
    --config_path="" \
    -v=1 &
MASTER_PID=$!
echo "Mooncake Master started (Traditional mode) on ${MASTER_HOST}:${MASTER_PORT}, PID=${MASTER_PID}"

sleep 3

# === 验证服务是否启动 ===
echo "=== Verifying services ==="
if curl -s http://${MASTER_HOST}:${METADATA_PORT}/metadata > /dev/null 2>&1; then
    echo "Metadata server: OK"
else
    echo "Metadata server: FAILED (retrying...)"
    sleep 5
    if curl -s http://${MASTER_HOST}:${METADATA_PORT}/metadata > /dev/null 2>&1; then
        echo "Metadata server: OK (after retry)"
    else
        echo "Metadata server: FAILED after retry. Exiting."
        echo "  可能原因："
        echo "  1. mooncake_master 没装好或不在 PATH"
        echo "  2. 端口被占用"
        echo "  3. Docker 内网络问题"
        exit 1
    fi
fi

echo "=== Mooncake Master (Traditional mode) is ready ==="
echo "Master PID: ${MASTER_PID}"
echo ""
echo "Next: bash start_prefiller.sh  (新终端)"
echo "Then:  bash start_decoder.sh   (新终端)"
echo ""
echo "To stop: kill ${MASTER_PID}"
echo "Or run:  bash cleanup.sh"

wait

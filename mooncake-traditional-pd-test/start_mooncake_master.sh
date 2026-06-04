#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_mooncake_master.sh — Start Mooncake Master (traditional file-system mode)
#
# 与 NDS 版的关键区别:
#   - 无 --use_od=true --nsid=N 参数 (传统文件系统模式)
#   - Master 为 Client 分配 global_segment 内存
#   - Client 使用 local_buffer 作为本地缓冲区

set -e

MASTER_HOST="localhost"
MASTER_PORT=50051
METADATA_PORT=8005

echo "=== Starting Mooncake Master (Traditional Mode) ==="

# 启动 Metadata Server
mooncake_http_metadata_server --port ${METADATA_PORT} &
METADATA_PID=$!
echo "Metadata server started on port ${METADATA_PORT}, PID=${METADATA_PID}"

sleep 2

# 启动 Master (传统模式: 无 --use_od --nsid 参数)
mooncake_master \
    --rpc_address=0.0.0.0 \
    --rpc_port=${MASTER_PORT} \
    --enable_http_metadata_server=true \
    --config_path="" \
    -v=1 &
MASTER_PID=$!
echo "Mooncake Master started (Traditional mode) on ${MASTER_HOST}:${MASTER_PORT}, PID=${MASTER_PID}"

sleep 3

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
        exit 1
    fi
fi

echo "=== Mooncake Traditional services are ready ==="
echo "Metadata PID: ${METADATA_PID}"
echo "Master PID: ${MASTER_PID}"
echo ""
echo "To stop: kill ${METADATA_PID} ${MASTER_PID}"
echo "Or run: bash cleanup.sh"

wait
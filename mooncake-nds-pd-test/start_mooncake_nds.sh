#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_mooncake_nds.sh — Start Mooncake NDS Master and Metadata Server

set -e

MASTER_HOST="localhost"
MASTER_PORT=50051
METADATA_PORT=8005
NSID=1
PROTOCOL="tcp"
NDS_LIBRARY_PATH="libndskv.so"
MC_NDS_CONFIG="nds_config.conf"

echo "=== Starting Mooncake NDS Master ==="

export NDS_LIBRARY_PATH="${NDS_LIBRARY_PATH}"
export MC_NDS_CONFIG="${MC_NDS_CONFIG}"

mooncake_http_metadata_server --port ${METADATA_PORT} &
METADATA_PID=$!
echo "Metadata server started on port ${METADATA_PORT}, PID=${METADATA_PID}"

sleep 2

mooncake_master \
    --use_od=true \
    --nsid=${NSID} \
    --rpc_address=0.0.0.0 \
    --rpc_port=${MASTER_PORT} \
    --enable_http_metadata_server=true \
    --config_path="" \
    -v=1 &
MASTER_PID=$!
echo "Mooncake Master started (NDS mode) on ${MASTER_HOST}:${MASTER_PORT}, PID=${MASTER_PID}"
echo "  use_od=true, nsid=${NSID}"

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

echo "=== Mooncake NDS services are ready ==="
echo "Metadata PID: ${METADATA_PID}"
echo "Master PID: ${MASTER_PID}"
echo ""
echo "To stop: kill ${METADATA_PID} ${MASTER_PID}"
echo "Or run: bash cleanup.sh"

wait
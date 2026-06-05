#!/bin/bash
# start_demo.sh — Launch the Mooncake NDS KVCache Demo
#
# Prerequisites:
#   - Mooncake Master running on localhost:50051 (--use_od=true --nsid=1)
#   - Prefiller running on localhost:7100
#   - Decoder running on localhost:7200
#   - PD Proxy running on localhost:9100
#   - Python deps: fastapi, uvicorn, httpx

set -e

DEMO_HOST="${DEMO_HOST:-0.0.0.0}"
DEMO_PORT="${DEMO_PORT:-9200}"
PROXY_URL="${PROXY_URL:-http://localhost:9100/v1}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Mooncake NDS KVCache Demo ==="
echo "  Demo server:  ${DEMO_HOST}:${DEMO_PORT}"
echo "  Proxy target: ${PROXY_URL}"
echo "  Records file: ${SCRIPT_DIR}/data/latency_records.json"
echo ""

# Check proxy health
echo "Checking proxy connectivity..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${PROXY_URL%/v1}/healthcheck" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    echo "  Proxy is reachable."
else
    echo "  WARNING: Proxy not reachable (HTTP $HTTP_CODE). Demo will fail if proxy is down."
    echo "  Make sure Mooncake Master + Prefiller + Decoder + Proxy are all running."
fi

echo ""
echo "Starting demo server..."
echo "  Open http://localhost:${DEMO_PORT} in your browser."
echo ""

python3 "${SCRIPT_DIR}/demo_server.py" \
    --host "${DEMO_HOST}" \
    --port "${DEMO_PORT}" \
    --proxy-url "${PROXY_URL}"
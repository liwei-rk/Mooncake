#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_router.sh — SGLang 内置 PD Router
#
# SGLang 的 router 替代了 vLLM 的 mooncake_connector_proxy.py
# router 自动处理 PD 路由：
#   1. 接收用户请求
#   2. 发给 prefiller 做 prefill + KV 传输
#   3. 发给 decoder 做 decode 生成
#   4. 返回结果给用户
#
# 启动顺序：Prefiller → Decoder → Router

set -e

# === no_proxy 防火墙 ===
export no_proxy=127.0.0.1,localhost
export NO_PROXY=127.0.0.1,localhost

# === 端口配置 ===
ROUTER_HOST="0.0.0.0"
ROUTER_PORT=19000              # 用户访问端口
PREFILLER_HOST="localhost"
PREFILLER_PORT=7100
DECODER_HOST="localhost"
DECODER_PORT=7200

echo "=== Starting SGLang PD Router ==="
echo "  Router:      ${ROUTER_HOST}:${ROUTER_PORT}"
echo "  Prefiller:   ${PREFILLER_HOST}:${PREFILLER_PORT}"
echo "  Decoder:     ${DECODER_HOST}:${DECODER_PORT}"
echo ""

# === 等待 prefiller 和 decoder 就绪 ===
echo "=== Waiting for prefiller and decoder to be ready ==="
for service in "${PREFILLER_HOST}:${PREFILLER_PORT}" "${DECODER_HOST}:${DECODER_PORT}"; do
    for attempt in $(seq 1 60); do
        if curl -s "http://${service}/health" > /dev/null 2>&1; then
            echo "  ${service}: ready"
            break
        fi
        if [ $attempt -eq 60 ]; then
            echo "  ${service}: NOT ready after 60 attempts. Starting router anyway."
        fi
        sleep 5
    done
done
echo ""

# === 启动 SGLang 内置 router ===
# --pd-disaggregation: 启用 PD 分离路由模式
# --prefill URL: prefiller 地址
# --decode URL: decoder 地址
echo "Starting router..."
exec python3 -m sglang_router.launch_router \
    --pd-disaggregation \
    --prefill "http://${PREFILLER_HOST}:${PREFILLER_PORT}" \
    --decode "http://${DECODER_HOST}:${DECODER_PORT}" \
    --host "${ROUTER_HOST}" \
    --port "${ROUTER_PORT}"

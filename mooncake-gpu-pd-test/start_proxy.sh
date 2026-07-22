#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_proxy.sh — 启动 vLLM 内置 Mooncake PD Proxy
#
# !!! 这个脚本是做什么的? !!!
# PD Proxy 是 PD 分离架构中的"路由器"。用户请求发给 proxy，proxy 做：
#   1. 生成 transfer_id (UUID)
#   2. 异步发给 prefiller (max_tokens=1, do_remote_decode=true, transfer_id) — fire-and-forget
#   3. 立即发给 decoder (do_remote_prefill=true, remote_bootstrap_addr, remote_engine_id, transfer_id)
#   4. 流式返回 decoder 的输出
#
# !!! MooncakeConnector 是 push-based !!!
# prefiller 不会在 HTTP 响应里返回 kv_transfer_params（和我们之前的假设不同）
# proxy 启动时从 prefiller 的 bootstrap server (端口 8998) 获取 engine_id
# 请求时 proxy 自己构造 kv_transfer_params 发给 decoder
#
# !!! 启动顺序 !!!
# 必须先启动 Prefiller → Decoder → 最后启动 Proxy
# Proxy 启动时会查 prefiller 的 bootstrap server 获取 engine_id

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# !!! 华为代理防火墙 !!!
# 服务器在华为代理后面，http_proxy/https_proxy 会干扰 localhost 通信
# 必须在启动 proxy 之前设置，否则 httpx 会把 localhost 请求路由到华为代理
export no_proxy=127.0.0.1,localhost
export NO_PROXY=127.0.0.1,localhost

# === 端口配置 ===
PROXY_HOST="0.0.0.0"
PROXY_PORT=19000              # 9100 被 node_exporter 占用，改用 19000
PREFILLER_HOST="localhost"
PREFILLER_PORT=7100
DECODER_HOST="localhost"
DECODER_PORT=7200
BOOTSTRAP_PORT=8998          # MooncakeConnector bootstrap server 端口

# === 找 vLLM 内置的 mooncake_connector_proxy.py ===
# 从 vLLM 0.21.0 Docker 镜像中查找
VLLM_PROXY_CANDIDATES=(
    "/vllm-workspace/examples/disaggregated/mooncake_connector/mooncake_connector_proxy.py"
    "/home/xinlang/ygj/dockers/images/vllm-0.21.0/examples/disaggregated/mooncake_connector/mooncake_connector_proxy.py"
    "/home/xinlang/yyc/images/vllm/examples/disaggregated/mooncake_connector/mooncake_connector_proxy.py"
)

VLLM_PROXY=""
for candidate in "${VLLM_PROXY_CANDIDATES[@]}"; do
    if [ -f "$candidate" ]; then
        VLLM_PROXY="$candidate"
        break
    fi
done

# 如果没找到，全局搜索
if [ -z "$VLLM_PROXY" ]; then
    echo "Searching for mooncake_connector_proxy.py..."
    VLLM_PROXY=$(find / -name "mooncake_connector_proxy.py" 2>/dev/null | head -1)
fi

if [ -z "$VLLM_PROXY" ]; then
    echo "ERROR: mooncake_connector_proxy.py not found!"
    echo "This proxy is required for MooncakeConnector (push-based protocol)."
    echo "Our custom proxy (mooncake_pd_proxy.py) does NOT work with MooncakeConnector."
    exit 1
fi

echo "=== Starting vLLM built-in Mooncake Proxy ==="
echo "  Proxy:       ${PROXY_HOST}:${PROXY_PORT}"
echo "  Prefiller:   ${PREFILLER_HOST}:${PREFILLER_PORT}"
echo "  Bootstrap:   ${BOOTSTRAP_PORT}"
echo "  Decoder:     ${DECODER_HOST}:${DECODER_PORT}"
echo "  Proxy file:  ${VLLM_PROXY}"
echo ""

# === 等待 prefiller 和 decoder 就绪 ===
echo "=== Waiting for prefiller and decoder to be ready ==="
for service in "${PREFILLER_HOST}:${PREFILLER_PORT}" "${DECODER_HOST}:${DECODER_PORT}"; do
    for attempt in $(seq 1 60); do
        if curl -s "http://${service}/v1/models" > /dev/null 2>&1; then
            echo "  ${service}: ready"
            break
        fi
        if [ $attempt -eq 60 ]; then
            echo "  ${service}: NOT ready after 60 attempts. Starting proxy anyway."
        fi
        sleep 5
    done
done
echo ""

# === 启动 vLLM 内置 proxy ===
# --prefill URL BOOTSTRAP_PORT: prefiller 地址 + bootstrap server 端口
# --decode URL: decoder 地址
# proxy 启动时会查 http://localhost:8998/query 获取 prefiller 的 engine_id
echo "Starting proxy..."
exec python3 -u "${VLLM_PROXY}" \
    --host "${PROXY_HOST}" \
    --port "${PROXY_PORT}" \
    --prefill "http://${PREFILLER_HOST}:${PREFILLER_PORT}" "${BOOTSTRAP_PORT}" \
    --decode "http://${DECODER_HOST}:${DECODER_PORT}"

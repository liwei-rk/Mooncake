#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_proxy.sh — 启动 PD Proxy
#
# !!! 这个脚本是做什么的? !!!
# PD Proxy 是 PD 分离架构中的"路由器"。用户请求发给 proxy，proxy 做两件事：
#   Step 1: 发给 prefiller（max_tokens=1, stream=False）→ prefiller 算 KV 并存到 Mooncake
#   Step 2: 从 prefiller 的响应中提取 kv_transfer_params（KV 位置信息）
#   Step 3: 把 kv_transfer_params 和原始请求一起发给 decoder → decoder 从 Mooncake 拉 KV 做生成
#   Step 4: 把 decoder 的流式输出转发给用户
#
# !!! 两种 proxy 选择 !!!
# 1. vLLM 内置 proxy (mooncake_connector_proxy.py) — 推荐
#    位于 vLLM 源码 examples/online_serving/disaggregated_serving/mooncake_connector/
#    vLLM >= 0.16.0 使用此 proxy
# 2. 自定义 proxy (mooncake_pd_proxy.py) — 备用
#    如果找不到 vLLM 内置 proxy，用我们自己的实现
#
# !!! 启动顺序 !!!
# 必须先启动 Prefiller → Decoder → 最后启动 Proxy
# Proxy 启动后会检查 prefiller 和 decoder 是否 ready

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# !!! 华为代理防火墙 !!!
# 服务器在华为代理后面，http_proxy/https_proxy 会干扰 localhost 通信
export no_proxy=127.0.0.1,localhost
export NO_PROXY=127.0.0.1,localhost

# === 端口配置 ===
PROXY_HOST="0.0.0.0"
PROXY_PORT=19000              # 9100 被 node_exporter 占用，改用 19000
PREFILLER_HOST="localhost"
PREFILLER_PORT=7100
DECODER_HOST="localhost"
DECODER_PORT=7200

# === 尝试找到 vLLM 内置的 mooncake_connector_proxy.py ===
# vLLM 0.21.0 的 proxy 可能在以下位置：
VLLM_PROXY_CANDIDATES=(
    "/vllm-workspace/examples/online_serving/disaggregated_serving/mooncake_connector/mooncake_connector_proxy.py"
    "/workspace/vllm/examples/online_serving/disaggregated_serving/mooncake_connector/mooncake_connector_proxy.py"
    "$(python3 -c 'import vllm; import os; print(os.path.join(os.path.dirname(vllm.__file__), "..", "examples", "online_serving", "disaggregated_serving", "mooncake_connector", "mooncake_connector_proxy.py"))' 2>/dev/null)"
)

VLLM_PROXY=""
for candidate in "${VLLM_PROXY_CANDIDATES[@]}"; do
    if [ -f "$candidate" ]; then
        VLLM_PROXY="$candidate"
        break
    fi
done

echo "=== Starting PD Proxy ==="
echo "  Proxy:  ${PROXY_HOST}:${PROXY_PORT}"
echo "  Prefiller: ${PREFILLER_HOST}:${PREFILLER_PORT}"
echo "  Decoder:   ${DECODER_HOST}:${DECODER_PORT}"

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

# === 启动 proxy ===
if [ -n "$VLLM_PROXY" ]; then
    echo "Using vLLM built-in proxy: ${VLLM_PROXY}"
    python3 -u "${VLLM_PROXY}" \
        --prefill "http://${PREFILLER_HOST}:${PREFILLER_PORT}" \
        --decode "http://${DECODER_HOST}:${DECODER_PORT}" \
        --host "${PROXY_HOST}" \
        --port "${PROXY_PORT}"
else
    echo "vLLM built-in proxy not found. Using custom proxy: ${SCRIPT_DIR}/mooncake_pd_proxy.py"
    echo "!!! 注意 !!!"
    echo "如果你在 vLLM 容器里能找到 mooncake_connector_proxy.py，建议用那个"
    echo "搜索命令: find / -name 'mooncake_connector_proxy.py' 2>/dev/null"
    echo ""
    python3 -u "${SCRIPT_DIR}/mooncake_pd_proxy.py" \
        --host "${PROXY_HOST}" \
        --port "${PROXY_PORT}" \
        --prefiller-host "${PREFILLER_HOST}" \
        --prefiller-port "${PREFILLER_PORT}" \
        --decoder-host "${DECODER_HOST}" \
        --decoder-port "${DECODER_PORT}"
fi

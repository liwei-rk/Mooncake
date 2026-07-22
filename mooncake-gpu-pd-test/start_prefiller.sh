#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# start_prefiller.sh — 启动 PD Prefiller (kv_producer) — GPU 版本
#
# !!! 什么是 Prefiller? !!!
# Prefiller 专门做 prefill（预填充）阶段：
#   1. 接收用户 prompt，跑一遍 forward pass，计算所有 token 的 KV cache
#   2. 把 KV cache 通过 Mooncake TransferEngine 存到远端
#   3. 只生成 1 个 token（max_tokens=1），不做完整 decode
# 为什么 max_tokens=1？因为 prefiller 的任务只是算 KV 并存到 Mooncake，
#   生成更多 token 是浪费——decoder 会拿 KV 做真正的 decode
#
# !!! vLLM 0.21.0 内置 MooncakeConnector vs OOT 模块 !!!
# 旧版 vLLM (0.10-0.12) 需要用 mooncake_connector_v1.py OOT 模块
# vLLM 0.13+ 内置了 MooncakeConnector，不需要 OOT 模块
# 内置 connector 直接调用 mooncake.engine.TransferEngine，不需要 LMCache 中间层
# 也不需要 Mooncake Master！它使用 P2P 握手模式（P2PHANDSHAKE）
# prefiller 和 decoder 通过 ZMQ side channel 直接通信，自动发现彼此

set -e

# === 核心变量 ===
MODEL_PATH="/mnt/model/Qwen2.5-7B-Instruct"  # 模型路径（服务器上确认过的实际路径）
                                         # 为什么先跑 7B？7B 单卡能放下（~14GB 显存）
                                         # 72B 需要 TP=4+ 跨多卡，增加复杂度
                                         # 先跑通 7B 验证流程，再换 72B 做正式实验
PREFILLER_GPU_ID=0                      # Prefiller 用 GPU 0
PREFILLER_VLLM_PORT=7100                # vLLM OpenAI API 端口
                                         # 7100 = prefiller, 7200 = decoder (见 start_decoder.sh)

# === GPU 设备隔离 ===
# !!! 这是从 NPU 迁移到 GPU 最关键的一行 !!!
# CUDA_VISIBLE_DEVICES: 只让这个进程看到 GPU 0
# 为什么需要隔离？因为如果不隔离，vLLM 会尝试用所有 GPU
# prefiller 和 decoder 各用一张卡，不能抢对方的卡
export CUDA_VISIBLE_DEVICES="${PREFILLER_GPU_ID}"

# !!! KVCache hash 一致性 !!!
# PYTHONHASHSEED=0 确保不同进程计算相同的 hash 值
# 为什么重要？prefiller 用 hash(key) 算 KV 的存储位置
# decoder 也用 hash(key) 查找 KV，hash 函数不一致就找不到
# Python 默认用随机种子，不同进程的 hash 不一样！
export PYTHONHASHSEED=0

# === Mooncake 传输协议配置 ===
# VLLM_MOONCAKE_PROTOCOL: "tcp" | "rdma"
#   tcp  — 用 TCP 协议，不需要 RDMA 硬件（初次验证推荐）
#   rdma — 用 RDMA，需要 MOFED 驱动 + RDMA NIC（高性能场景）
# !!! 为什么先用 TCP? !!!
# TCP 简单可靠，排除 RDMA 配置问题的干扰
# 验证流程跑通后切到 RDMA 测性能提升，形成对比数据（面试加分点）
export VLLM_MOONCAKE_PROTOCOL="tcp"

# Bootstrap port: prefiller 用这个端口启动 ZMQ socket
# decoder 通过这个端口发现 prefiller 并交换 KV 位置信息
# 默认 8998，TP/DP 部署时每个 worker 的端口 = base_port + dp_rank * tp_size + tp_rank
export VLLM_MOONCAKE_BOOTSTRAP_PORT=8998

# LD_LIBRARY_PATH: 修复 CUDA 12/13 不匹配问题
# 容器里系统 CUDA 是 13.0，但 Mooncake Python binding 编译时用 CUDA 12
# 通过 pip 安装 nvidia-cuda-runtime-cu12 来提供 libcudart.so.12
export LD_LIBRARY_PATH=/usr/local/lib/python3.12/dist-packages/nvidia/cuda_runtime/lib:${LD_LIBRARY_PATH}

# !!! no_proxy 防火墙 !!!
# 防止 HTTP_PROXY 环境变量干扰 localhost 通信
# 如果服务器配了全局 HTTP proxy，vLLM 内部的 HTTP 请求（包括 localhost）
# 会被代理截获，导致连接失败。设 no_proxy 绕过 localhost
export no_proxy=127.0.0.1,localhost
export NO_PROXY=127.0.0.1,localhost

echo "=== Starting Prefiller (kv_producer) on GPU ${PREFILLER_GPU_ID} ==="
echo "Connector: MooncakeConnector (vLLM built-in, P2P handshake mode)"
echo "Protocol: ${VLLM_MOONCAKE_PROTOCOL}"
echo "Model: ${MODEL_PATH}"
echo "Port: ${PREFILLER_VLLM_PORT}"
echo ""

# === 启动 vLLM serve ===
# !!! vLLM 0.21.0 参数命名规则 !!!
# vLLM 0.21.0 把 --disable-X 改成了 --no-enable-X 模式
# 旧版: --disable-log-requests
# 新版: --no-enable-log-requests（用 --enable-log-requests / --no-enable-log-requests 一对）
#
# !!! 每个参数的含义 !!!
# --port: vLLM 的 OpenAI API 端口
# --no-enable-log-requests: 不打印每个请求的日志（减少干扰，benchmark 时很重要）
# --enforce-eager: 禁用 CUDA Graph
#   为什么禁用？CUDA Graph 会预录计算图，启动慢但运行快
#   调试阶段用 eager 模式，能看到完整错误栈；正式 benchmark 可以去掉
# --no-enable-prefix-caching: 禁用 vLLM 内置的 prefix caching
#   为什么禁用？因为我们用的是 Mooncake 的 KV cache，不是 vLLM 内置的
#   如果不禁用，vLLM 自己也会缓存 prefix，干扰我们的实验数据
# --kv-transfer-config: 配置 KV 传输
#   kv_connector: MooncakeConnector = vLLM 0.13+ 内置的 Mooncake 连接器
#   kv_role: kv_producer = 这个 vLLM 实例只负责产生 KV（存到 Mooncake）
# !!! 注意：不需要 kv_connector_module_path（内置 connector 不需要）!!!
# !!! 不需要 LMCACHE_CONFIG_FILE（内置 connector 不走 LMCache）!!!
vllm serve "${MODEL_PATH}" \
    --port ${PREFILLER_VLLM_PORT} \
    --no-enable-log-requests \
    --enforce-eager \
    --no-enable-prefix-caching \
    --kv-transfer-config \
    '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer"}'

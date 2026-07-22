#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# cleanup.sh — 停止所有 GPU PD 测试进程
#
# !!! 为什么要清理? !!!
# vLLM 和 Mooncake 进程会占用 GPU 显存和系统内存
# 如果不清理直接重新启动，会报 CUDA OOM 或端口被占用
# 养成每次测试后清理的习惯

echo "=== Stopping all GPU Mooncake PD test processes ==="

# 先优雅退出（SIGTERM）
# 顺序：proxy → vLLM → master（先停上层再停底层）
pkill -f "mooncake_pd_proxy" 2>/dev/null && echo "Stopped proxy server" || echo "No proxy found"
pkill -f "mooncake_connector_proxy" 2>/dev/null && echo "Stopped vLLM proxy" || echo "No vLLM proxy found"
pkill -f "vllm serve" 2>/dev/null && echo "Stopped vLLM instances" || echo "No vLLM instances found"
pkill -f "mooncake_master" 2>/dev/null && echo "Stopped Mooncake Master" || echo "No Master found"
pkill -f "mooncake_http_metadata_server" 2>/dev/null && echo "Stopped metadata server" || echo "No metadata server found"

sleep 2

# 如果优雅退出失败，强制 kill（SIGKILL）
# !!! 注意：先 SIGTERM 再 SIGKILL 是最佳实践 !!!
# SIGTERM 让进程有机会清理资源（释放 GPU 显存、关闭连接）
# SIGKILL 是最后手段，可能留下僵尸内存段
pkill -9 -f "mooncake_pd_proxy" 2>/dev/null
pkill -9 -f "mooncake_connector_proxy" 2>/dev/null
pkill -9 -f "mooncake_master" 2>/dev/null
pkill -9 -f "mooncake_http_metadata" 2>/dev/null
pkill -9 -f "vllm serve" 2>/dev/null

echo "=== All processes stopped ==="
echo "建议执行 nvidia-smi 确认 GPU 显存已释放"

#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# cleanup.sh — 停止所有 SGLang PD 测试进程
#
# !!! 为什么要清理? !!!
# SGLang 和 Mooncake 进程会占用 GPU 显存和系统内存
# 如果不清理直接重新启动，会报 CUDA OOM 或端口被占用
# 养成每次测试后清理的习惯
#
# !!! 注意 SGLang 的进程名问题 !!!
# SGLang 启动后会产生多种进程名：
#   - python3 -m sglang.launch_server  （主进程）
#   - sglang::scheduler                 （调度子进程，nvidia-smi 里显示这个）
#   - sglang::detokenizer               （反序列化子进程）
# 不能用 pkill -f "sglang" —— 会误杀 cleanup.sh 自身（路径含 sglang）
# 用精确模式匹配 "python.*sglang" 和 "sglang::" 分别匹配

echo "=== Stopping all SGLang Mooncake PD test processes ==="

# 先优雅退出（SIGTERM）
# 顺序：router → sglang server → mooncake 残留（先停上层再停底层）
# 注意：不能用 pkill -f "sglang" 因为会匹配到 cleanup.sh 自身的进程名
# 用 "python.*sglang" 匹配主进程，"sglang::" 匹配子进程（scheduler/detokenizer）
pkill -f "sglang_router" 2>/dev/null && echo "Stopped SGLang router" || echo "No router found"
pkill -f "python.*sglang" 2>/dev/null && echo "Stopped SGLang main process" || echo "No SGLang main process found"
pkill -f "sglang::" 2>/dev/null && echo "Stopped SGLang subprocesses (scheduler/detokenizer)" || echo "No SGLang subprocesses found"
pkill -f "mooncake" 2>/dev/null && echo "Stopped Mooncake processes" || echo "No Mooncake processes found"

sleep 3

# 如果优雅退出失败，强制 kill（SIGKILL）
# !!! 注意：先 SIGTERM 再 SIGKILL 是最佳实践 !!!
# SIGTERM 让进程有机会清理资源（释放 GPU 显存、关闭连接）
# SIGKILL 是最后手段，可能留下僵尸内存段
pkill -9 -f "sglang_router" 2>/dev/null
pkill -9 -f "python.*sglang" 2>/dev/null
pkill -9 -f "sglang::" 2>/dev/null
pkill -9 -f "mooncake" 2>/dev/null

sleep 1

echo "=== All processes stopped ==="
echo "建议执行 nvidia-smi 确认 GPU 显存已释放"

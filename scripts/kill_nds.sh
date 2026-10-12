#!/bin/bash
# NDS 测试残留进程清理
# WHY 存在：mooncake_master / etcd 测试后常留残留，占住端口挡住下一轮 bind；
#      之前放 /tmp 重启即丢，收编进仓库统一维护。
# 注意：永远用脚本文件跑，不要在 SSH 命令行里直接 pkill -f —— 会匹配到 SSH 命令行自身导致自杀。
pkill -9 -f 'nds_probe6.py' 2>/dev/null
pkill -9 -f 'nds_ctypes_reader' 2>/dev/null
pkill -9 -x mooncake_master 2>/dev/null
sleep 1
# 验证：pgrep -x mooncake_master | wc -l 应为 0（<defunct> 僵尸无害可忽略）
echo CLEANED

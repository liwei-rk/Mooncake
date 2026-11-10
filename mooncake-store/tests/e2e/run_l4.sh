#!/bin/bash
# L4 官方 E2E 一键编排：etcd + metadata master + e2e_rand_test
# WHY 存在：手册 L4 的完整流程（杀残留 → 起 etcd → 起 master → 跑 e2e → 清理）固化成仓库脚本，
#      之前放 /tmp 重启即丢、不进版本管理，无法跨机器复刻。
# 已知坑（2026-09-15 实测）：128 服务器有系统自带 etcd.service（systemd 守护），
#      pkill 杀掉后 systemd 秒级拉活并占住 127.0.0.1:2379/2380，导致自建 etcd bind 失败，
#      测试会"假通过"（实际连的是系统 etcd）。必须用 systemctl stop 干净停掉，跑完归还。

set -u

# --- 0. 记录系统 etcd 状态，跑完原样归还（不动不属于我们的服务） ---
SYSTEM_ETCD_WAS_ACTIVE=0
if systemctl is-active --quiet etcd 2>/dev/null; then
    SYSTEM_ETCD_WAS_ACTIVE=1
    echo "[run_l4] 系统 etcd.service 在跑，先 systemctl stop（跑完归还）"
    systemctl stop etcd
fi

# --- 1. 清残留（TIME_WAIT 会挡 bind，sleep 3 等端口释放） ---
pkill -9 -x mooncake_master 2>/dev/null
pkill -9 -x etcd 2>/dev/null
sleep 3
export no_proxy=127.0.0.1,localhost

# --- 2. 起专用 etcd（127.0.0.1:2379） ---
ETCD_BIN=/home/yyc/etcd-v3.5.21-linux-amd64/etcd
nohup "$ETCD_BIN" --name nds-e2e \
  --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster nds-e2e=http://127.0.0.1:2380 > /tmp/chain_etcd.log 2>&1 < /dev/null &
sleep 3
grep -q 'ready to serve client requests' /tmp/chain_etcd.log \
  && echo ETCD_OK || { echo ETCD_FAIL; tail -3 /tmp/chain_etcd.log; exit 1; }

# --- 3. 起 metadata master（随机 rpc 端口；8080 元数据；默认 9003 会和 E2E 内部 master 冲突） ---
METAPORT=$((20000 + RANDOM % 20000))
cd /home/yyc/Mooncake-kv_v8/build || { echo "BUILD_DIR_MISSING"; exit 1; }
export LD_LIBRARY_PATH=/home/yyc/Mooncake-kv_v8/build/mooncake-asio
nohup ./mooncake-store/src/mooncake_master --enable_http_metadata_server=true \
  --http_metadata_server_port=8080 --rpc_port=$METAPORT --rpc_address=127.0.0.1 \
  --metrics_port=$((METAPORT+1)) > /tmp/chain_meta.log 2>&1 < /dev/null &
sleep 5
grep -q 'started successfully' /tmp/chain_meta.log && echo META_OK || { echo META_FAIL; tail -3 /tmp/chain_meta.log; exit 1; }

# --- 4. 跑官方 E2E（30s；cluster_id 默认 mooncake，3a65c13 已对齐） ---
timeout 300 ./mooncake-store/tests/e2e/e2e_rand_test --run_sec=30 \
  --etcd_endpoints=127.0.0.1:2379 --protocol=tcp \
  --engine_meta_url=http://127.0.0.1:8080/metadata --rand_seed=42 > /tmp/chain_l4.log 2>&1
L4_RC=$?
echo L4_EXIT=$L4_RC
grep -aE '\[  PASSED|\[  FAILED' /tmp/chain_l4.log | head -3

# --- 5. 清理 + 归还系统 etcd ---
pkill -9 -x mooncake_master 2>/dev/null
pkill -9 -x etcd 2>/dev/null
if [ $SYSTEM_ETCD_WAS_ACTIVE -eq 1 ]; then
    systemctl start etcd
    echo "[run_l4] 系统 etcd.service 已归还"
fi
echo L4_CLEANED
exit $L4_RC

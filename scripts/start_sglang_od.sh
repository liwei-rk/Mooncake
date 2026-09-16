#!/bin/bash
# start_sglang_od.sh — 130 容器内启动 sglang HiCache(mooncake backend) -> NDS 盘框
# WHY 这些环境变量:
#   MOONCAKE_MASTER/TE_META_DATA_SERVER  指向 130 主机上的 mooncake_master(rpc 41883 / http 49369)
#   MOONCAKE_PROTOCOL=tcp                130 单网卡无 ARP flux 问题,但保持与 L1 验证一致的 tcp
#   LD_LIBRARY_PATH                      kv_v8 store.so 的依赖库(jsoncpp/glog/asio 等)
#   NDS_LIBRARY_PATH / MC_NDS_CONFIG     NDS SDK + 盘框配置(nsid 147815026, 1MB value)
#   no_proxy                             华为全局代理会劫持 127.0.0.1 的 http metadata 请求
# page_size=32: Qwen2.5-7B 每页 28层x32KB=896KB,由 wrapper 自动补齐到 1MB nsid
set -x
export MOONCAKE_MASTER=127.0.0.1:41883
export MOONCAKE_LOCAL_HOSTNAME=127.0.0.1:0
export MOONCAKE_TE_META_DATA_SERVER=http://127.0.0.1:49369/metadata
export MOONCAKE_PROTOCOL=tcp
export MOONCAKE_DEVICE=""
export LD_LIBRARY_PATH=/home/yyc/mooncake-deps:$LD_LIBRARY_PATH
export NDS_LIBRARY_PATH=/home/yyc/mooncake_nds_runtime/NDS_bin/libndskv.so
export MC_NDS_CONFIG=/home/yyc/mooncake_nds_runtime/nds_config.conf
export no_proxy=127.0.0.1,localhost
export NO_PROXY=127.0.0.1,localhost

exec python3 -m sglang.launch_server \
  --model-path /home/model/Qwen2.5-7B \
  --host 0.0.0.0 --port 30000 \
  --mem-fraction-static 0.85 \
  --enable-hierarchical-cache \
  --hicache-storage-backend mooncake \
  --hicache-mem-layout page_first_direct \
  --hicache-ratio 2 \
  --hicache-write-policy write_through \
  --hicache-storage-prefetch-policy wait_complete \
  --page-size 32 \
  > /tmp/sglang_od.log 2>&1

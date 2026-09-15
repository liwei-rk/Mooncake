#!/bin/bash
# vLLM + OdKvStorageConnector（经 mooncake 适配层）+ 盘框 启动脚本
# 前置：mooncake_master(use_od) 已在 127.0.0.1 起好，端口注入 MC_MASTER_RPC/MC_META_URL
export PYTHONPATH=/home/yyc/mooncake_py
export LD_LIBRARY_PATH=/usr/local/lib/mooncake-deps:$LD_LIBRARY_PATH
export NDS_LIBRARY_PATH=/home/yyc/mooncake_nds_runtime/NDS_bin/libndskv.so
export MC_NDS_CONFIG=/home/yyc/mooncake_nds_runtime/nds_config.conf
export OD_KV_NSID=147815026
export no_proxy=127.0.0.1,localhost
export VLLM_LOGGING_LEVEL=INFO
exec python3 -m vllm.entrypoints.openai.api_server \
  --model /home/model/Qwen2.5-7B --served-model-name qwen \
  --gpu-memory-utilization 0.85 --max-model-len 4096 --block-size 16 \
  --enable-prefix-caching --disable-hybrid-kv-cache-manager \
  --port 8000 \
  --kv-transfer-config '{"kv_connector": "OdKvStorageConnector", "kv_role": "kv_both", "kv_connector_extra_config": {"enable_failure_escape": false}}'

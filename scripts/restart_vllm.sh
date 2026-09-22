#!/bin/bash
# 杀 vLLM（容器内 pkill + 主机层 EngineCore 兜底）
# WHY 主机层兜底: 容器内 pkill 杀不死主机进程 VLLM::EngineCore（AGENT_CONTEXT 教训）
docker exec vllm-0.20.2-test-privileged bash -c 'pkill -9 -f "api_server" ; pkill -9 -f EngineCore' 2>/dev/null
sleep 3
# 主机层查残余 GPU 进程并强杀
for pid in $(nvidia-smi --query-compute-apps=pid --format=csv,noheader); do
    echo "killing host pid $pid"
    kill -9 "$pid" 2>/dev/null
done
sleep 3
mem=$(nvidia-smi --query-gpu=memory.used --format=csv,noheader)
echo "GPU_MEM=$mem"
echo "VLLM_STOPPED"

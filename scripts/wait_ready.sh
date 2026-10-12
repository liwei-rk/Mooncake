#!/bin/bash
# 轮询 vLLM 就绪（/v1/models 返回 200）
for i in $(seq 1 30); do
    sleep 10
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://127.0.0.1:8000/v1/models)
    echo "try=$i code=$code"
    if [ "$code" = "200" ]; then
        echo "READY"
        exit 0
    fi
done
echo "TIMEOUT_NOT_READY"
exit 1

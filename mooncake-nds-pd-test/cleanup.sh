#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# cleanup.sh — Stop all Mooncake NDS PD test processes

echo "=== Stopping all PD test processes ==="

pkill -f "vllm serve" 2>/dev/null && echo "Stopped vLLM instances" || echo "No vLLM instances found"
pkill -f "mooncake_pd_proxy" 2>/dev/null && echo "Stopped proxy server" || echo "No proxy found"
pkill -f "mooncake_master" 2>/dev/null && echo "Stopped Mooncake Master" || echo "No Master found"
pkill -f "mooncake_http_metadata_server" 2>/dev/null && echo "Stopped metadata server" || echo "No metadata server found"

sleep 2

pkill -9 -f "mooncake_master" 2>/dev/null
pkill -9 -f "mooncake_http_metadata" 2>/dev/null
pkill -9 -f "mooncake_pd_proxy" 2>/dev/null
pkill -9 -f "vllm serve" 2>/dev/null

echo "=== All processes stopped ==="
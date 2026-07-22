#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# mooncake_pd_proxy.py — PD Proxy for Mooncake-only KVCache transfer
#
# !!! 这个文件是做什么的? !!!
# PD Proxy 是 PD 分离架构中的"路由器"。用户请求发给 proxy，proxy 做两件事：
#   Step 1: 发给 prefiller（max_tokens=1, stream=False）→ prefiller 算 KV 并存到 Mooncake
#   Step 2: 从 prefiller 的响应中提取 kv_transfer_params（KV 位置信息）
#   Step 3: 把 kv_transfer_params 和原始请求一起发给 decoder → decoder 从 Mooncake 拉 KV 做生成
#   Step 4: 把 decoder 的流式输出转发给用户
#
# !!! 为什么需要 proxy? !!!
# 用户体验上，用户只想发一个请求拿到结果。proxy 把 P→D 的两步通信隐藏了。
# 在实际生产中，proxy 可以是 Envoy/Nginx，这里用 FastAPI 实现方便理解。
#
# Architecture based on vLLM's load_balance_proxy_server_example.py:
#   1. Client sends request to proxy
#   2. Proxy sends to Prefiller (max_tokens=1, stream=False) → stores KV to Mooncake
#   3. Proxy extracts kv_transfer_params from prefiller response
#   4. Proxy forwards to Decoder (stream=True) → retrieves KV from Mooncake → generates output
#   5. Decoder's streamed response is forwarded to client
#
# Key difference from naive proxy: Prefiller only does prefill + KV store,
# NOT full generation. This eliminates wasted prefiller generation time.

import argparse
import asyncio
import json
import os
import uuid

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI()

PREFILLER_URL = "http://localhost:7100/v1"
DECODER_URL = "http://localhost:7200/v1"


async def send_to_prefiller(req_data: dict, request_id: str):
    """Send request to prefiller: max_tokens=1, stream=False, with kv_transfer_params."""
    # !!! 为什么要 max_tokens=1? !!!
    # prefiller 只需要做 prefill（计算 KV cache）然后存到 Mooncake
    # max_tokens=1 让它只生成 1 个 token（这是 vLLM 最小的生成单位）
    # 如果 max_tokens=200，prefiller 会做 200 步 decode——完全是浪费！
    # 因为 decoder 会拿 KV 做真正的 decode，prefiller 生成的 token 被丢弃
    prefill_req = req_data.copy()
    prefill_req["stream"] = False
    prefill_req["max_tokens"] = 1
    prefill_req["min_tokens"] = 1
    if "max_completion_tokens" in prefill_req:
        prefill_req["max_completion_tokens"] = 1
    if "stream_options" in prefill_req:
        del prefill_req["stream_options"]

    # !!! kv_transfer_params 是 PD 通信的核心 !!!
    # do_remote_decode: True = 告诉 prefiller "你要把 KV 存到 Mooncake，给别人用"
    # do_remote_prefill: False = prefiller 不需要从别人那拿 KV
    # remote_*: None = prefiller 还不知道 KV 会存到哪，这是 prefiller 填完返回的
    prefill_req["kv_transfer_params"] = {
        "do_remote_decode": True,
        "do_remote_prefill": False,
        "remote_engine_id": None,
        "remote_block_ids": None,
        "remote_host": None,
        "remote_port": None,
    }
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY', '')}",
        "X-Request-Id": request_id,
    }
    async with httpx.AsyncClient(timeout=300) as client:
        try:
            resp = await client.post(
                f"{PREFILLER_URL}/completions", json=prefill_req, headers=headers
            )
            if resp.status_code != 200:
                err_body = resp.text
                print(f"[PROXY] Prefiller error {resp.status_code}: {err_body}")
                return {"error": f"Prefiller {resp.status_code}: {err_body}"}
            return resp.json()
        except Exception as e:
            print(f"[PROXY] Prefiller request failed: {e}")
            return None


async def send_to_prefiller_chat(req_data: dict, request_id: str):
    """Send chat request to prefiller."""
    prefill_req = req_data.copy()
    prefill_req["stream"] = False
    prefill_req["max_tokens"] = 1
    prefill_req["min_tokens"] = 1
    if "max_completion_tokens" in prefill_req:
        prefill_req["max_completion_tokens"] = 1
    if "stream_options" in prefill_req:
        del prefill_req["stream_options"]
    prefill_req["kv_transfer_params"] = {
        "do_remote_decode": True,
        "do_remote_prefill": False,
        "remote_engine_id": None,
        "remote_block_ids": None,
        "remote_host": None,
        "remote_port": None,
    }
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY', '')}",
        "X-Request-Id": request_id,
    }
    async with httpx.AsyncClient(timeout=300) as client:
        try:
            resp = await client.post(
                f"{PREFILLER_URL}/chat/completions", json=prefill_req, headers=headers
            )
            if resp.status_code != 200:
                err_body = resp.text
                print(f"[PROXY] Prefiller chat error {resp.status_code}: {err_body}")
                return {"error": f"Prefiller {resp.status_code}: {err_body}"}
            return resp.json()
        except Exception as e:
            print(f"[PROXY] Prefiller chat request failed: {e}")
            return None


@app.post("/v1/completions")
async def proxy_completions(request: Request):
    request_id = f"pd-{uuid.uuid4().hex[:8]}"
    req_data = await request.json()
    stream_flag = bool(req_data.get("stream", False))

    # Step 1: Prefiller (prefill + KV store to Mooncake)
    prefill_resp = await send_to_prefiller(req_data, request_id)
    if prefill_resp is None or "error" in prefill_resp:
        err = prefill_resp.get("error", "Prefiller request failed") if prefill_resp else "Prefiller request failed"
        return JSONResponse({"error": err}, status_code=500)

    # !!! 提取 kv_transfer_params !!!
    # prefiller 返回的 JSON 里会包含 kv_transfer_params
    # 里面有 remote_engine_id, remote_block_ids, remote_host, remote_port
    # 这些信息告诉 decoder "KV 在 Mooncake 的哪里"
    # 没有 kv_transfer_params，decoder 就不知道去哪拉 KV
    kv_transfer_params = prefill_resp.get("kv_transfer_params", {})
    decode_req = req_data.copy()
    if kv_transfer_params:
        decode_req["kv_transfer_params"] = kv_transfer_params

    # Step 2: Decoder (retrieve KV from Mooncake + generate output)
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY', '')}",
        "X-Request-Id": request_id,
    }

    if stream_flag:
        async with httpx.AsyncClient(timeout=300) as client:
            async def stream_decoder():
                with client.stream(
                    "POST",
                    f"{DECODER_URL}/completions",
                    json=decode_req,
                    headers=headers,
                ) as stream:
                    for chunk in stream.iter_bytes():
                        yield chunk

            return StreamingResponse(
                stream_decoder(), media_type="text/event-stream"
            )
    else:
        async with httpx.AsyncClient(timeout=300) as client:
            try:
                decode_resp = await client.post(
                    f"{DECODER_URL}/completions", json=decode_req, headers=headers
                )
                return JSONResponse(decode_resp.json())
            except Exception as e:
                return JSONResponse(
                    {"error": f"Decoder request failed: {e}"}, status_code=500
                )


@app.post("/v1/chat/completions")
async def proxy_chat_completions(request: Request):
    request_id = f"pd-{uuid.uuid4().hex[:8]}"
    req_data = await request.json()
    stream_flag = bool(req_data.get("stream", False))

    # Step 1: Prefiller
    prefill_resp = await send_to_prefiller_chat(req_data, request_id)
    if prefill_resp is None or "error" in prefill_resp:
        err = prefill_resp.get("error", "Prefiller request failed") if prefill_resp else "Prefiller request failed"
        return JSONResponse({"error": err}, status_code=500)

    kv_transfer_params = prefill_resp.get("kv_transfer_params", {})
    decode_req = req_data.copy()
    if kv_transfer_params:
        decode_req["kv_transfer_params"] = kv_transfer_params

    # Step 2: Decoder
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY', '')}",
        "X-Request-Id": request_id,
    }

    if stream_flag:
        async with httpx.AsyncClient(timeout=300) as client:
            async def stream_decoder():
                with client.stream(
                    "POST",
                    f"{DECODER_URL}/chat/completions",
                    json=decode_req,
                    headers=headers,
                ) as stream:
                    for chunk in stream.iter_bytes():
                        yield chunk

            return StreamingResponse(
                stream_decoder(), media_type="text/event-stream"
            )
    else:
        async with httpx.AsyncClient(timeout=300) as client:
            try:
                decode_resp = await client.post(
                    f"{DECODER_URL}/chat/completions", json=decode_req, headers=headers
                )
                return JSONResponse(decode_resp.json())
            except Exception as e:
                return JSONResponse(
                    {"error": f"Decoder request failed: {e}"}, status_code=500
                )


@app.get("/v1/models")
async def list_models():
    """转发 prefiller 的 /v1/models，让 benchmark 的健康检查通过。"""
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(f"{PREFILLER_URL}/models")
            return resp.json()
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=503)


@app.get("/health")
async def health():
    return {"status": "ok", "prefiller": PREFILLER_URL, "decoder": DECODER_URL}


@app.get("/healthcheck")
async def healthcheck():
    pf_ok = False
    dec_ok = False
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.get(f"{PREFILLER_URL.replace('/v1', '')}/v1/models")
            pf_ok = r.status_code == 200
        except Exception:
            pass
        try:
            r = await client.get(f"{DECODER_URL.replace('/v1', '')}/v1/models")
            dec_ok = r.status_code == 200
        except Exception:
            pass
    return {
        "status": "ok" if pf_ok and dec_ok else "degraded",
        "prefiller": pf_ok,
        "decoder": dec_ok,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=19000)  # 9100 被 node_exporter 占用，改用 19000
    parser.add_argument("--prefiller-host", default="localhost")
    parser.add_argument("--prefiller-port", type=int, default=7100)
    parser.add_argument("--decoder-host", default="localhost")
    parser.add_argument("--decoder-port", type=int, default=7200)
    args = parser.parse_args()

    PREFILLER_URL = f"http://{args.prefiller_host}:{args.prefiller_port}/v1"
    DECODER_URL = f"http://{args.decoder_host}:{args.decoder_port}/v1"

    print(f"PD Proxy started on {args.host}:{args.port}")
    print(f"  Prefiller: {PREFILLER_URL}")
    print(f"  Decoder:   {DECODER_URL}")
    print(f"  Flow: Prefiller(max_tokens=1) → Decoder(stream)")

    uvicorn.run(app, host=args.host, port=args.port)

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# mooncake_pd_proxy.py — PD Proxy for Mooncake-only KVCache transfer
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
                f"{PREFILLER_URL}/completions", json=prefill_req, headers=headers
            )
            resp.raise_for_status()
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
            resp.raise_for_status()
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
    if prefill_resp is None:
        return JSONResponse({"error": "Prefiller request failed"}, status_code=500)

    # Extract kv_transfer_params from prefiller response
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
    if prefill_resp is None:
        return JSONResponse({"error": "Prefiller request failed"}, status_code=500)

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
    parser.add_argument("--port", type=int, default=9100)
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
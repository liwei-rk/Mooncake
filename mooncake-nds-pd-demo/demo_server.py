#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# demo_server.py — Mooncake NDS KVCache prefix-hit performance demo
#
# Flow:
#   1. User enters prompt on web page
#   2. Backend sends 1st request to Proxy (cold start, full prefill)
#   3. Backend sends 2nd request to Proxy (cache hit, LMCache detects NDS KV)
#   4. Both results streamed to frontend via SSE with TTFT measurements
#   5. Session record persisted to data/latency_records.json

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse

app = FastAPI()

PROXY_URL = "http://localhost:9100/v1"
RECORDS_FILE = ""


def _resolve_records_file() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "latency_records.json")


def load_records() -> list:
    if not os.path.exists(RECORDS_FILE):
        return []
    try:
        with open(RECORDS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return []


def save_records(records: list):
    os.makedirs(os.path.dirname(RECORDS_FILE), exist_ok=True)
    with open(RECORDS_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)


def append_session(session: dict):
    records = load_records()
    records.append(session)
    save_records(records)


async def _stream_single_run(
    proxy_url: str, prompt: str, model: str, max_tokens: int, run_label: str
):
    """Async generator: send streaming completions request to proxy, yield typed events."""
    payload = {
        "model": model,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "stream": True,
        "temperature": 0.0,
    }
    start_time = time.monotonic()
    first_token_time = None
    full_text = ""
    completion_tokens = 0

    async with httpx.AsyncClient(timeout=300) as client:
        async with client.stream("POST", f"{proxy_url}/completions", json=payload) as response:
            for line in response.aiter_lines():
                if not line or not line.startswith("data: "):
                    continue
                data_str = line[len("data: "):]
                if data_str.strip() == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                except json.JSONDecodeError:
                    continue
                choices = chunk.get("choices", [])
                if not choices:
                    continue
                choice = choices[0]
                content = choice.get("text", "")
                if content:
                    if first_token_time is None:
                        first_token_time = time.monotonic()
                        yield {
                            "type": f"{run_label}_ttft",
                            "ttft": round(first_token_time - start_time, 3),
                        }
                    full_text += content
                    completion_tokens += 1
                    yield {"type": f"{run_label}_token", "content": content}
                usage = chunk.get("usage", {})
                if usage.get("completion_tokens"):
                    completion_tokens = usage["completion_tokens"]

    end_time = time.monotonic()
    ttft = round((first_token_time - start_time), 3) if first_token_time is not None else round(end_time - start_time, 3)
    total_time = round(end_time - start_time, 3)

    yield {
        "type": f"{run_label}_done",
        "ttft": ttft,
        "total_time": total_time,
        "completion_tokens": completion_tokens,
    }


@app.get("/")
async def index():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.post("/api/stream-comparison")
async def stream_comparison(request: Request):
    req_data = await request.json()
    prompt = req_data.get("prompt", "")
    model = req_data.get("model", "test-model")
    max_tokens = req_data.get("max_tokens", 100)

    if not prompt:
        return JSONResponse({"error": "prompt is required"}, status_code=400)

    session_id = uuid.uuid4().hex[:6]
    session_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    async def event_stream():
        run1_result = {}
        run2_result = {}

        yield f"data: {json.dumps({'type': 'run1_start', 'session_id': session_id})}\n\n"

        async for event in _stream_single_run(PROXY_URL, prompt, model, max_tokens, "run1"):
            yield f"data: {json.dumps(event)}\n\n"
            if event["type"] == "run1_done":
                run1_result = event

        yield f"data: {json.dumps({'type': 'run2_start'})}\n\n"

        async for event in _stream_single_run(PROXY_URL, prompt, model, max_tokens, "run2"):
            yield f"data: {json.dumps(event)}\n\n"
            if event["type"] == "run2_done":
                run2_result = event

        ttft1 = run1_result.get("ttft", 0)
        ttft2 = run2_result.get("ttft", 0)
        speedup = round(ttft1 / ttft2, 2) if ttft2 > 0 else 0
        reduction_pct = round((1 - ttft2 / ttft1) * 100, 1) if ttft1 > 0 else 0

        comparison = {"speedup": speedup, "ttft_reduction_pct": reduction_pct}
        yield f"data: {json.dumps({'type': 'comparison', **comparison})}\n\n"

        session = {
            "session_id": session_id,
            "timestamp": session_ts,
            "prompt": prompt[:500] if len(prompt) > 500 else prompt,
            "prompt_chars": len(prompt),
            "model": model,
            "max_tokens": max_tokens,
            "cold_start": {
                "ttft": run1_result.get("ttft", 0),
                "total_time": run1_result.get("total_time", 0),
                "completion_tokens": run1_result.get("completion_tokens", 0),
            },
            "cache_hit": {
                "ttft": run2_result.get("ttft", 0),
                "total_time": run2_result.get("total_time", 0),
                "completion_tokens": run2_result.get("completion_tokens", 0),
            },
            "comparison": comparison,
        }
        append_session(session)

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/history")
async def get_history():
    return JSONResponse(load_records())


@app.delete("/api/history")
async def clear_history():
    save_records([])
    return JSONResponse({"status": "cleared", "count": 0})


@app.get("/health")
async def health():
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            base = PROXY_URL.rsplit("/v1", 1)[0]
            r = await client.get(f"{base}/healthcheck")
            proxy_ok = r.status_code == 200
    except Exception:
        proxy_ok = False
    return {"status": "ok" if proxy_ok else "degraded", "proxy": proxy_ok}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mooncake NDS KVCache Demo Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9200)
    parser.add_argument("--proxy-url", default="http://localhost:9100/v1")
    args = parser.parse_args()

    PROXY_URL = args.proxy_url
    RECORDS_FILE = _resolve_records_file()

    print(f"Mooncake NDS KVCache Demo Server")
    print(f"  Listening:  {args.host}:{args.port}")
    print(f"  Proxy:      {PROXY_URL}")
    print(f"  Records:    {RECORDS_FILE}")
    print(f"  Open http://localhost:{args.port} in browser")

    uvicorn.run(app, host=args.host, port=args.port)
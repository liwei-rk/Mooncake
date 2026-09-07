#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# run_benchmark.py — PD Mooncake KVCache hit performance benchmark
#
# !!! 这个 benchmark 测什么? !!!
# 通过三组实验对比，验证 Mooncake KVCache 前缀复用对 TTFT 的优化效果：
#   Group A: 完全不同的 prompt → KVCache 无命中 → TTFT 包含完整 prefill 时间
#   Group B: 共享 prefix + 不同 suffix → KVCache 命中 → TTFT 应显著降低
#   Group C: 不同 prefix 比例 (0%/25%/50%/75%/100%) → 观察前缀比例与 TTFT 的关系
#
# !!! 核心指标：TTFT (Time To First Token) !!!
# TTFT = 从发请求到收到第一个 token 的时间
# PD 架构下：TTFT = prefill 时间 + KV 传输时间 + 首步 decode 时间
# KVCache 命中时：TTFT ≈ KV 拉取时间 + 首步 decode 时间（省掉 prefill 时间）
# 所以 Group B 的 TTFT 应该 << Group A 的 TTFT
#
# 两种模式：
#   --mode direct : 绕过 proxy，直接测 prefiller+decoder（更干净的 KVCache 测量）
#   --mode proxy  : 通过 proxy（模拟真实生产端到端场景）

import argparse
import json
import os
import random
import statistics
import time
import uuid

import httpx

DEFAULT_MODEL = "/home/model/Qwen2.5-7B"  # vLLM 用模型路径作为 model id
DEFAULT_PROXY_URL = "http://localhost:19000"  # 9100 被 node_exporter 占用，改用 19000
DEFAULT_PREFILLER_URL = "http://localhost:7100"
DEFAULT_DECODER_URL = "http://localhost:7200"


def generate_random_text(length_chars: int, seed: int = None) -> str:
    """生成随机文本作为 prompt。
    为什么用固定词库？因为用真实自然语言，vLLM 的 tokenizer 可能把不同句子切成
    相近的 token 数，导致输入长度不可控。用固定词库+随机种子可以精确控制长度。
    """
    rng = random.Random(seed)
    words = [
        "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
        "hello", "world", "data", "science", "machine", "learning", "model",
        "neural", "network", "transformer", "attention", "layer", "weight",
        "gradient", "optimizer", "batch", "training", "inference", "cache",
        "memory", "buffer", "compute", "tensor", "matrix", "vector", "token",
        "sequence", "prefix", "suffix", "context", "prompt", "response",
    ]
    result = []
    total = 0
    while total < length_chars:
        w = rng.choice(words)
        result.append(w)
        total += len(w) + 1
    return " ".join(result)


def estimate_char_len_for_tokens(target_tokens: int, chars_per_token: float = 4.0) -> int:
    """估算生成 target_tokens 个 token 需要多少字符。
    为什么是 4.0？经验值，英文文本平均 1 token ≈ 4 chars。
    不精确但够用，benchmark 不需要精确到个位数 token。
    """
    return int(target_tokens * chars_per_token)


async def measure_ttft_streaming(
    url: str, payload: dict, timeout: float = 300.0
) -> tuple[float, str, int]:
    """发送流式请求，测量 TTFT（到第一个 content token 的时间）。

    为什么用 streaming 而不是非流式？
    非流式模式下，TTFT = 整个生成时间（因为要等所有 token 生成完才返回）
    流式模式下，可以精确测量第一个 token 到达的时间 = 真正的 TTFT
    """
    start = time.monotonic()
    first_token_time = None
    full_text = ""
    completion_tokens = 0
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream("POST", url, json=payload) as response:
            async for line in response.aiter_lines():
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
                content = ""
                delta = choice.get("delta", {})
                message = choice.get("message", {})
                text_content = choice.get("text", "")
                content = delta.get("content", "") or message.get("content", "") or text_content
                if content:
                    if first_token_time is None:
                        first_token_time = time.monotonic()
                    full_text += content
                    completion_tokens += 1
                usage = chunk.get("usage", {})
                if usage.get("completion_tokens"):
                    completion_tokens = usage["completion_tokens"]
    end = time.monotonic()
    if first_token_time is None:
        ttft = end - start
    else:
        ttft = first_token_time - start
    total_time = end - start
    return ttft, full_text, completion_tokens


async def measure_ttft_non_streaming(
    url: str, payload: dict, timeout: float = 300.0
) -> tuple[float, str, int]:
    """Send non-streaming request, measure TTFT (total response time as proxy)."""
    start = time.monotonic()
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=payload)
        elapsed = time.monotonic() - start
    data = resp.json()
    choices = data.get("choices", [])
    content = ""
    if choices:
        content = choices[0].get("text", "") or choices[0].get("message", {}).get("content", "")
    completion_tokens = data.get("usage", {}).get("completion_tokens", 0)
    return elapsed, content, completion_tokens


async def warmup_prefiller(
    prefiller_url: str, prefix: str, model: str, max_tokens: int = 1
) -> bool:
    """把 prefix 发给 prefiller，让 prefiller 把 KV 存到 Mooncake。

    !!! 这是 Group B/C 的关键步骤 !!!
    为什么需要 warmup？因为 Mooncake 的 KV cache 不会凭空出现。
    必须先让 prefiller 跑一遍 prefix，计算 KV 并存到 Mooncake。
    然后 decoder 才能从 Mooncake 拉到 KV，实现 cache hit。
    不 warmup → decoder 找不到 KV → 等于没有 cache hit → Group B 就退化成 Group A。
    """
    payload = {
        "model": model,
        "prompt": prefix,
        "max_tokens": max_tokens,
        "stream": False,
        "temperature": 0.0,
        "kv_transfer_params": {
            "do_remote_decode": True,
            "do_remote_prefill": False,
            "remote_engine_id": None,
            "remote_block_ids": None,
            "remote_host": None,
            "remote_port": None,
        },
    }
    print(f"[Warmup] Sending prefix ({len(prefix)} chars) to prefiller...")
    try:
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(f"{prefiller_url}/v1/completions", json=payload)
            resp.raise_for_status()
            print("[Warmup] Prefiller completed. KVCache stored to Mooncake.")
            return True
    except Exception as e:
        print(f"[Warmup] Prefiller failed: {e}")
        return False


async def run_group_a(
    target_url: str,
    model: str,
    num_prompts: int,
    input_len_tokens: int,
    output_len_tokens: int,
    seed: int,
) -> list[dict]:
    """Group A: Baseline — 完全不同的随机 prompt，无 KVCache 命中。

    为什么每个 prompt 用不同 seed？因为相同 seed 会生成相同文本，
    相同文本会导致 hash 相同 → Mooncake 里有缓存 → 不是真正的"无命中"baseline。
    seed + i 确保每个 prompt 都不同，hash 都不同，保证无 cache hit。
    """
    results = []
    print(f"\n{'='*60}")
    print(f"Group A: Baseline (no KVCache hit)")
    print(f"  {num_prompts} unique prompts, input_len={input_len_tokens} tokens")
    print(f"{'='*60}")

    for i in range(num_prompts):
        prompt_seed = seed + i
        char_len = estimate_char_len_for_tokens(input_len_tokens)
        prompt = generate_random_text(char_len, seed=prompt_seed)

        payload = {
            "model": model,
            "prompt": prompt,
            "max_tokens": output_len_tokens,
            "stream": True,
            "temperature": 0.0,
            "ignore_eos": True,
        }

        ttft, text, comp_tokens = await measure_ttft_streaming(target_url, payload)
        results.append({
            "index": i,
            "ttft": ttft,
            "completion_tokens": comp_tokens,
            "prompt_chars": len(prompt),
        })
        print(f"  [{i+1}/{num_prompts}] TTFT={ttft:.3f}s, tokens={comp_tokens}")

    return results


async def run_group_b(
    target_url: str,
    prefiller_url: str,
    model: str,
    num_prompts: int,
    prefix_len_tokens: int,
    suffix_len_tokens: int,
    output_len_tokens: int,
    seed: int,
) -> list[dict]:
    """Group B: Prefix reuse — 共享 prefix + 不同 suffix → KVCache 命中。

    所有 prompt 共享同一个 prefix（hash 相同 → Mooncake 中有缓存的 KV）
    suffix 部分不同（每个 prompt 独有的内容，需要实际 prefill）
    预期：TTFT 应该 << Group A，因为 prefix 部分的 KV 从 Mooncake 拉取，不用重新算。
    """
    prefix_char_len = estimate_char_len_for_tokens(prefix_len_tokens)
    shared_prefix = generate_random_text(prefix_char_len, seed=seed + 10000)

    print(f"\n{'='*60}")
    print(f"Group B: Prefix reuse (KVCache hit)")
    print(f"  {num_prompts} prompts, prefix={prefix_len_tokens} tokens + suffix={suffix_len_tokens} tokens")
    print(f"{'='*60}")

    # Warmup: 把 prefix 的 KV 存到 Mooncake
    warmup_ok = await warmup_prefiller(prefiller_url, shared_prefix, model)
    if not warmup_ok:
        print("[ERROR] Warmup failed! Results will not show KVCache hit effect.")
    else:
        # 等待 KV 完全写入 Mooncake
        # 为什么 sleep 2？Mooncake 的 KV 存储是异步的，warmup 返回不代表 KV 已完全写入
        # 2 秒足够在 TCP 模式下完成（如果是大模型可能需要更长）
        time.sleep(2)

    results = []
    for i in range(num_prompts):
        suffix_char_len = estimate_char_len_for_tokens(suffix_len_tokens)
        suffix = generate_random_text(suffix_char_len, seed=seed + 20000 + i)
        prompt = shared_prefix + " " + suffix

        payload = {
            "model": model,
            "prompt": prompt,
            "max_tokens": output_len_tokens,
            "stream": True,
            "temperature": 0.0,
            "ignore_eos": True,
        }

        ttft, text, comp_tokens = await measure_ttft_streaming(target_url, payload)
        results.append({
            "index": i,
            "ttft": ttft,
            "completion_tokens": comp_tokens,
            "prompt_chars": len(prompt),
            "prefix_chars": len(shared_prefix),
        })
        print(f"  [{i+1}/{num_prompts}] TTFT={ttft:.3f}s, tokens={comp_tokens}")

    return results


async def run_group_c(
    target_url: str,
    prefiller_url: str,
    model: str,
    num_prompts: int,
    total_len_tokens: int,
    output_len_tokens: int,
    ratios: list[int],
    seed: int,
) -> dict[int, list[dict]]:
    """Group C: Prefix length gradient — 不同前缀比例的 TTFT 对比。

    固定总长度，改变 prefix:suffix 的比例：
      0%   → 纯 suffix，无 KVCache 命中（等于 Group A 的 baseline）
      25%  → 25% prefix 命中，75% 需要重新 prefill
      50%  → 50% prefix 命中
      75%  → 75% prefix 命中
      100% → 完全 prefix 命中（decoder 几乎不需要 prefill）
    预期：prefix 比例越高 → TTFT 越低（线性下降趋势）
    这组实验的曲线是面试讲解时的核心图表！
    """
    all_results = {}

    print(f"\n{'='*60}")
    print(f"Group C: Prefix length gradient")
    print(f"  total_len={total_len_tokens} tokens, ratios={ratios}%")
    print(f"{'='*60}")

    for ratio in ratios:
        prefix_tokens = int(total_len_tokens * ratio / 100)
        suffix_tokens = total_len_tokens - prefix_tokens
        print(f"\n  --- Ratio {ratio}%: prefix={prefix_tokens}, suffix={suffix_tokens} ---")

        if prefix_tokens > 0:
            prefix_char_len = estimate_char_len_for_tokens(prefix_tokens)
            shared_prefix = generate_random_text(prefix_char_len, seed=seed + 30000 + ratio)
            warmup_ok = await warmup_prefiller(prefiller_url, shared_prefix, model)
            if not warmup_ok:
                print(f"  [WARN] Warmup failed for ratio {ratio}%")
            else:
                time.sleep(2)
        else:
            shared_prefix = ""

        ratio_results = []
        for i in range(num_prompts):
            if suffix_tokens > 0:
                suffix_char_len = estimate_char_len_for_tokens(suffix_tokens)
                suffix = generate_random_text(suffix_char_len, seed=seed + 40000 + ratio * 100 + i)
            else:
                suffix = "What is the answer?"

            prompt = shared_prefix + " " + suffix if shared_prefix else suffix

            payload = {
                "model": model,
                "prompt": prompt,
                "max_tokens": output_len_tokens,
                "stream": True,
                "temperature": 0.0,
                "ignore_eos": True,
            }

            ttft, text, comp_tokens = await measure_ttft_streaming(target_url, payload)
            ratio_results.append({
                "index": i,
                "ttft": ttft,
                "completion_tokens": comp_tokens,
                "prompt_chars": len(prompt),
                "prefix_tokens": prefix_tokens,
                "suffix_tokens": suffix_tokens,
                "prefix_ratio": ratio,
            })
            print(f"    [{i+1}/{num_prompts}] TTFT={ttft:.3f}s, tokens={comp_tokens}")

        all_results[ratio] = ratio_results

    return all_results


def summarize(results: list[dict], label: str) -> dict:
    """计算统计指标：均值、中位数、标准差、min/max/p99。
    为什么看这么多指标？因为 mean 可能被 outlier 拉偏，
    median 更稳健，p99 反映长尾延迟（生产环境关键指标）。
    """
    ttfts = [r["ttft"] for r in results]
    summary = {
        "label": label,
        "count": len(ttfts),
        "mean_ttft": statistics.mean(ttfts) if ttfts else 0,
        "median_ttft": statistics.median(ttfts) if ttfts else 0,
        "stdev_ttft": statistics.stdev(ttfts) if len(ttfts) > 1 else 0,
        "min_ttft": min(ttfts) if ttfts else 0,
        "max_ttft": max(ttfts) if ttfts else 0,
        "p99_ttft": sorted(ttfts)[int(0.99 * len(ttfts))] if len(ttfts) >= 100 else max(ttfts) if ttfts else 0,
    }
    print(f"\n  [{label}] Summary:")
    print(f"    Mean TTFT:   {summary['mean_ttft']:.3f}s")
    print(f"    Median TTFT: {summary['median_ttft']:.3f}s")
    print(f"    Stdev TTFT:  {summary['stdev_ttft']:.3f}s")
    print(f"    Min TTFT:    {summary['min_ttft']:.3f}s")
    print(f"    Max TTFT:    {summary['max_ttft']:.3f}s")
    print(f"    P99 TTFT:    {summary['p99_ttft']:.3f}s")
    return summary


async def main():
    parser = argparse.ArgumentParser(description="PD Mooncake KVCache benchmark")
    parser.add_argument("--mode", choices=["direct", "proxy"], default="proxy",
                        help="direct: bypass proxy, proxy: through proxy (default)")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model name or path")
    parser.add_argument("--proxy-url", default=DEFAULT_PROXY_URL)
    parser.add_argument("--prefiller-url", default=DEFAULT_PREFILLER_URL)
    parser.add_argument("--decoder-url", default=DEFAULT_DECODER_URL)
    parser.add_argument("--num-prompts", type=int, default=30)
    parser.add_argument("--input-len", type=int, default=7500, help="Baseline prompt length (tokens)")
    parser.add_argument("--output-len", type=int, default=200, help="Output length (tokens)")
    parser.add_argument("--prefix-len", type=int, default=5000, help="Shared prefix length (tokens)")
    parser.add_argument("--suffix-len", type=int, default=2500, help="Suffix length (tokens)")
    parser.add_argument("--ratios", type=int, nargs="+", default=[0, 25, 50, 75, 100],
                        help="Prefix ratio percentages for Group C")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", default="./bench_results")
    parser.add_argument("--groups", type=str, default="ABC",
                        help="Which groups to run (A, B, C, or any combination)")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    seed = args.seed

    # Determine target URL based on mode
    if args.mode == "proxy":
        target_url = f"{args.proxy_url}/v1"
        prefiller_url = args.prefiller_url
    else:
        # Direct mode: target is decoder directly
        target_url = f"{args.decoder_url}/v1"
        prefiller_url = args.prefiller_url

    print(f"{'='*60}")
    print(f"  Mooncake GPU PD KVCache Benchmark")
    print(f"  Mode: {args.mode}")
    print(f"  Model: {args.model}")
    print(f"  Target URL: {target_url}")
    print(f"  Prefiller URL: {prefiller_url}")
    print(f"  Groups: {args.groups}")
    print(f"  Prompts per group: {args.num_prompts}")
    print(f"{'='*60}")

    # Wait for target to be ready
    # NOTE: vLLM built-in proxy only implements POST /v1/completions and
    # POST /v1/chat/completions — it does NOT have GET /v1/models.
    # So we use a TCP-level check: if the port accepts a connection and
    # returns ANY HTTP response (even 404), the service is up.
    # A ConnectError means the server hasn't started yet.
    print("\nWaiting for services to be ready...")
    health_url = args.proxy_url if args.mode == "proxy" else args.decoder_url
    for attempt in range(30):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(f"{health_url}/v1/models")
                # Any HTTP response (200, 404, etc.) means the server is running.
                # Only connection failures (caught by except) mean it's not ready.
                print(f"  Health check: HTTP {r.status_code} — service is responding.")
                print("Services are ready.")
                break
        except Exception:
            pass
        if attempt == 29:
            print("Timeout waiting for services. Exiting.")
            return
        time.sleep(5)

    all_data = {}

    # Group A: Baseline
    if "A" in args.groups:
        results_a = await run_group_a(
            target_url, args.model, args.num_prompts,
            args.input_len, args.output_len, seed
        )
        summary_a = summarize(results_a, "Group A: Baseline")
        all_data["group_A"] = {"results": results_a, "summary": summary_a}

    # Group B: Prefix reuse
    if "B" in args.groups:
        results_b = await run_group_b(
            target_url, prefiller_url, args.model, args.num_prompts,
            args.prefix_len, args.suffix_len, args.output_len, seed
        )
        summary_b = summarize(results_b, "Group B: Prefix reuse")
        all_data["group_B"] = {"results": results_b, "summary": summary_b}

    # Group C: Prefix gradient
    if "C" in args.groups:
        results_c = await run_group_c(
            target_url, prefiller_url, args.model, args.num_prompts,
            args.input_len, args.output_len, args.ratios, seed
        )
        summaries_c = {}
        for ratio, results in results_c.items():
            summaries_c[ratio] = summarize(results, f"Group C: {ratio}%")
        all_data["group_C"] = {"results": results_c, "summaries": summaries_c}

    # Comparison summary
    print(f"\n{'='*60}")
    print(f"  Comparison Summary")
    print(f"{'='*60}")
    if "A" in args.groups and "B" in args.groups:
        mean_a = all_data["group_A"]["summary"]["mean_ttft"]
        mean_b = all_data["group_B"]["summary"]["mean_ttft"]
        if mean_a > 0:
            reduction = (1 - mean_b / mean_a) * 100
            print(f"  Group A Mean TTFT: {mean_a:.3f}s")
            print(f"  Group B Mean TTFT: {mean_b:.3f}s")
            print(f"  TTFT Reduction:    {reduction:.1f}%")
    if "C" in args.groups:
        print(f"\n  Group C TTFT by prefix ratio:")
        for ratio, summary in sorted(summaries_c.items()):
            print(f"    {ratio}% prefix → Mean TTFT = {summary['mean_ttft']:.3f}s")

    # Save results
    output_file = os.path.join(args.output_dir, f"benchmark_{args.mode}_{int(time.time())}.json")
    with open(output_file, "w") as f:
        json.dump(all_data, f, indent=2)
    print(f"\nResults saved to {output_file}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

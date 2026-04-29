#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from statistics import median

import httpx


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark Anthropic model latency (TTFT + total).")
    parser.add_argument("--model", default="claude-haiku-4-5", help="Anthropic model ID or alias.")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--prompt", default="Reply with a short greeting.")
    return parser.parse_args()


def stream_request(client: httpx.Client, model: str, prompt: str, max_tokens: int):
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": os.getenv("ANTHROPIC_API_KEY", ""),
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": 0.2,
        "stream": True,
        "messages": [{"role": "user", "content": prompt}],
    }
    t_start = time.perf_counter()
    t_first = None
    with client.stream("POST", url, headers=headers, content=json.dumps(body), timeout=30) as resp:
        resp.raise_for_status()
        for raw_line in resp.iter_lines():
            if not raw_line:
                continue
            if isinstance(raw_line, bytes):
                line = raw_line.decode("utf-8", errors="ignore")
            else:
                line = str(raw_line)
            if not line.startswith("data:"):
                continue
            payload = line[5:].strip()
            if payload == "[DONE]":
                break
            try:
                evt = json.loads(payload)
            except json.JSONDecodeError:
                continue
            evt_type = evt.get("type")
            if evt_type == "content_block_delta":
                delta = evt.get("delta") or {}
                if delta.get("text") and t_first is None:
                    t_first = time.perf_counter()
            if evt_type == "message_stop":
                break
    t_end = time.perf_counter()
    ttft = (t_first - t_start) * 1000 if t_first else None
    total = (t_end - t_start) * 1000
    return ttft, total


def main():
    args = parse_args()
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        print("ANTHROPIC_API_KEY is not set. Set it in your environment and retry.")
        sys.exit(1)
    ttft_vals = []
    total_vals = []
    with httpx.Client() as client:
        for i in range(args.runs):
            try:
                ttft, total = stream_request(client, args.model, args.prompt, args.max_tokens)
            except httpx.HTTPStatusError as exc:
                status = getattr(exc.response, "status_code", None)
                if status == 429:
                    print(f"run {i+1}: rate_limited (429). Stopping early.")
                    break
                raise
            if ttft is not None:
                ttft_vals.append(ttft)
            total_vals.append(total)
            print(f"run {i+1}: ttft_ms={ttft:.1f} total_ms={total:.1f}" if ttft else f"run {i+1}: ttft_ms=NA total_ms={total:.1f}")
    if ttft_vals:
        print(f"median_ttft_ms={median(ttft_vals):.1f}")
    print(f"median_total_ms={median(total_vals):.1f}")
    results = {
        "model": args.model,
        "runs": args.runs,
        "completed_runs": len(total_vals),
        "prompt": args.prompt,
        "max_tokens": args.max_tokens,
        "ttft_ms": ttft_vals,
        "total_ms": total_vals,
        "median_ttft_ms": median(ttft_vals) if ttft_vals else None,
        "median_total_ms": median(total_vals),
    }
    with open("/root/aziz-associate/benchmark_results_anthropic.json", "w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2)


if __name__ == "__main__":
    main()

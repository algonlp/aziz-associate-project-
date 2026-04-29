import os
import time
import json
from typing import List, Dict, Any

from openai import OpenAI


def _load_env_file(path: str) -> None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("\"'")
                if not key:
                    continue
                os.environ.setdefault(key, value)
    except FileNotFoundError:
        return

MODELS = [
    "gpt-5.2-chat-latest",
    "gpt-5.1-chat-latest",
    "gpt-5-chat-latest",
    "gpt-5.2",
    "gpt-5.1",
    "gpt-5",
    "gpt-5-mini",
    "gpt-5-nano",
    "gpt-4.1",
    "gpt-4o",
    "gpt-4.1-mini",
    "gpt-4o-mini",
    "gpt-4.1-nano",
]

PROMPT = "Reply with a single word: ok"


def run_one(client: OpenAI, model: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {"model": model}
    t0 = time.perf_counter()
    try:
        # Use streaming to measure time-to-first-token.
        stream = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": PROMPT}],
            max_completion_tokens=8,
            stream=True,
        )
        first_token_ms = None
        total_tokens = 0
        out_text = ""
        for chunk in stream:
            if first_token_ms is None:
                first_token_ms = (time.perf_counter() - t0) * 1000.0
            try:
                delta = chunk.choices[0].delta
                if delta and getattr(delta, "content", None):
                    out_text += delta.content
            except Exception:
                pass
            total_tokens += 1
        total_ms = (time.perf_counter() - t0) * 1000.0
        result.update(
            {
                "ok": True,
                "first_token_ms": first_token_ms if first_token_ms is not None else total_ms,
                "total_ms": total_ms,
                "output": out_text.strip(),
                "chunks": total_tokens,
            }
        )
        return result
    except Exception as exc:
        result.update({"ok": False, "error": str(exc)})
        return result


def main() -> None:
    _load_env_file("/root/aziz-associate/.env")
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY is not set")
        return
    base_url = os.getenv("OPENAI_BASE_URL")
    client = OpenAI(api_key=api_key, base_url=base_url)
    results: List[Dict[str, Any]] = []
    for model in MODELS:
        res = run_one(client, model)
        results.append(res)
        status = "ok" if res.get("ok") else "error"
        print(f"{model}: {status}")
    ok_results = [r for r in results if r.get("ok")]
    ok_results.sort(key=lambda r: (r.get("first_token_ms") or 1e9))
    print("\nSorted by first_token_ms:")
    for r in ok_results:
        print(
            f"{r['model']}: first_token_ms={r['first_token_ms']:.0f} total_ms={r['total_ms']:.0f} output={r.get('output','')!r}"
        )
    with open("/root/aziz-associate/benchmark_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()

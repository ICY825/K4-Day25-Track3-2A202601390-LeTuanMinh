"""Concurrent load test: the same chaos scenario at 1, 4, 8 and 16 workers.

Sequential runs hide two things a production gateway will meet immediately:

  1. Throughput. With one thread the run is latency-bound, so the numbers say
     nothing about behaviour under real arrival rates.
  2. Herd effects on the circuit breaker. Many in-flight requests can be
     admitted before a failing provider trips the breaker, and every HALF_OPEN
     window admits as many probes as there are threads in flight rather than
     one. Both waste calls on a dependency that is already known to be sick.

The provider call counters below measure exactly that waste: for a provider
with fail_rate 1.0, every counted call is a call the breaker did not manage to
prevent.

Run: python scripts/run_load_test.py [--scenario NAME] [--workers 1,4,8,16]
"""
from __future__ import annotations

import argparse
import json
import random
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

from reliability_lab.chaos import build_gateway, load_queries, run_requests
from reliability_lab.config import LabConfig, ScenarioConfig, load_config
from reliability_lab.gateway import ReliabilityGateway
from reliability_lab.metrics import RunMetrics


def _instrument(gateway: ReliabilityGateway) -> tuple[Counter[str], threading.Lock]:
    """Count how many times each provider is actually invoked.

    Wrapping happens on the gateway's own provider objects, so nothing in the
    library needs to know about the measurement.
    """
    counts: Counter[str] = Counter()
    lock = threading.Lock()

    for provider in gateway.providers:
        original = provider.complete

        def counted(prompt: str, _orig: Any = original, _name: str = provider.name) -> Any:
            with lock:
                counts[_name] += 1
            return _orig(prompt)

        provider.complete = counted  # type: ignore[method-assign]

    return counts, lock


def _run_one(
    config: LabConfig, queries: list[str], scenario: ScenarioConfig, workers: int, seed: int
) -> dict[str, Any]:
    random.seed(seed)
    gateway = build_gateway(config, scenario.provider_overrides or None)
    counts, _ = _instrument(gateway)

    started = time.perf_counter()
    metrics: RunMetrics = run_requests(
        gateway, queries, config.load_test.requests, workers=workers
    )
    wall_s = time.perf_counter() - started

    half_open_probes = sum(
        1
        for breaker in gateway.breakers.values()
        for entry in breaker.transition_log
        if entry.get("from") == "half_open"
    )

    return {
        "workers": workers,
        "wall_seconds": round(wall_s, 3),
        "throughput_rps": round(metrics.total_requests / wall_s, 2) if wall_s else 0.0,
        "availability": round(metrics.availability, 4),
        "error_rate": round(metrics.error_rate, 4),
        "latency_p50_ms": round(metrics.percentile(50), 2),
        "latency_p95_ms": round(metrics.percentile(95), 2),
        "latency_p99_ms": round(metrics.percentile(99), 2),
        "cache_hits": metrics.cache_hits,
        "fallback_successes": metrics.fallback_successes,
        "static_fallbacks": metrics.static_fallbacks,
        "circuit_open_count": metrics.circuit_open_count,
        "half_open_exits": half_open_probes,
        "provider_calls": dict(counts),
        "estimated_cost": round(metrics.estimated_cost, 6),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/default.yaml")
    parser.add_argument("--scenario", default="primary_timeout_100")
    parser.add_argument("--workers", default="1,4,8,16")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default="reports/load_test.json")
    args = parser.parse_args()

    config = load_config(args.config)
    queries = load_queries()
    scenario = next((s for s in config.scenarios if s.name == args.scenario), None)
    if scenario is None:
        scenario = ScenarioConfig(name=args.scenario, description="ad-hoc load scenario")
    worker_counts = [int(w) for w in args.workers.split(",")]

    print(f"scenario: {scenario.name} - {scenario.description}")
    print(f"requests per run: {config.load_test.requests}, seed: {args.seed}\n")

    rows = [_run_one(config, queries, scenario, w, args.seed) for w in worker_counts]

    header = (
        f"{'workers':>7} {'wall_s':>7} {'req/s':>8} {'avail':>6} "
        f"{'p50':>7} {'p95':>7} {'p99':>7} {'hits':>5} {'static':>6} "
        f"{'opens':>5} {'primary_calls':>13} {'backup_calls':>12}"
    )
    print(header)
    print("-" * len(header))
    for row in rows:
        calls = row["provider_calls"]
        print(
            f"{row['workers']:>7} {row['wall_seconds']:>7.2f} {row['throughput_rps']:>8.1f} "
            f"{row['availability']:>6.3f} {row['latency_p50_ms']:>7.1f} "
            f"{row['latency_p95_ms']:>7.1f} {row['latency_p99_ms']:>7.1f} "
            f"{row['cache_hits']:>5} {row['static_fallbacks']:>6} "
            f"{row['circuit_open_count']:>5} {calls.get('primary', 0):>13} "
            f"{calls.get('backup', 0):>12}"
        )

    baseline, peak = rows[0], rows[-1]

    def total_calls(row: dict[str, Any]) -> int:
        return sum(int(v) for v in row["provider_calls"].values())

    print(
        f"\nthroughput: {baseline['throughput_rps']} req/s at {baseline['workers']} worker(s) "
        f"-> {peak['throughput_rps']} req/s at {peak['workers']} worker(s) "
        f"({peak['throughput_rps'] / baseline['throughput_rps']:.1f}x)"
    )
    print(
        f"provider calls for the same {config.load_test.requests} requests: "
        f"{total_calls(baseline)} at {baseline['workers']} worker(s) "
        f"-> {total_calls(peak)} at {peak['workers']} worker(s) "
        f"(+{total_calls(peak) - total_calls(baseline)})"
    )
    print(
        f"cache hits: {baseline['cache_hits']} at {baseline['workers']} worker(s) "
        f"-> {peak['cache_hits']} at {peak['workers']} worker(s) "
        f"- concurrent duplicates miss a cache the first response has not filled yet"
    )
    if scenario.provider_overrides.get("primary") == 1.0:
        print(
            f"calls to the dead primary the breaker failed to prevent: "
            f"{baseline['provider_calls'].get('primary', 0)} at {baseline['workers']} "
            f"worker(s) -> {peak['provider_calls'].get('primary', 0)} at "
            f"{peak['workers']} worker(s)"
        )

    payload = {
        "scenario": scenario.name,
        "description": scenario.description,
        "requests_per_run": config.load_test.requests,
        "seed": args.seed,
        "runs": rows,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()

"""Fail if reports/final_report.md quotes a number the artifacts do not contain.

The report is written by hand, but every figure in it is supposed to come from
a generated artifact. Re-running the chaos simulation rewrites the wall-clock
latencies, and a hand-copied table silently goes stale - which is exactly the
kind of "report has numbers that do not match the evidence" problem this check
exists to prevent.

Note on ordering: this validates the committed report against the committed
artifacts. Re-running the chaos simulation regenerates the wall-clock latencies
and will legitimately make this fail until the report is updated to match - so
run `make verify` before regenerating, or update the report afterwards.

Run: python scripts/verify_report.py   (exit code 1 on any mismatch)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

REPORT = Path("reports/final_report.md")


def _load(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _expected() -> list[tuple[str, str]]:
    """Return (label, literal-string-that-must-appear-in-the-report) pairs."""
    mem = _load("reports/metrics.json")
    red = _load("reports/metrics_redis.json")
    mem_scen = _load("reports/metrics_by_scenario.json")
    red_scen = _load("reports/metrics_redis_by_scenario.json")
    load = _load("reports/load_test.json")
    load_cached = _load("reports/load_test_cached.json")

    pairs: list[tuple[str, str]] = []

    for label, blob in (("in-memory", mem), ("redis", red)):
        pairs += [
            (f"{label} availability", f"{blob['availability']:.4f}"),
            (f"{label} p50", f"{blob['latency_p50_ms']:.2f}"),
            (f"{label} p95", f"{blob['latency_p95_ms']:.2f}"),
            (f"{label} p99", f"{blob['latency_p99_ms']:.2f}"),
            (f"{label} cache hit rate", f"{blob['cache_hit_rate']:.4f}"),
            (f"{label} cost", f"{blob['estimated_cost']:.6f}"),
            (f"{label} recovery", f"{blob['recovery_time_ms']:.2f}"),
        ]

    for scenario in ("baseline_cached", "no_cache"):
        for label, blob in (("memory", mem_scen), ("redis", red_scen)):
            row = blob[scenario]
            pairs += [
                (f"{label}/{scenario} p50", f"{row['latency_p50_ms']:.2f}"),
                (f"{label}/{scenario} p95", f"{row['latency_p95_ms']:.2f}"),
                (f"{label}/{scenario} cost", f"{row['estimated_cost']:.6f}"),
            ]

    pairs += [
        ("memory/all_healthy p50", f"{mem_scen['all_healthy']['latency_p50_ms']:.2f}"),
        ("memory/all_healthy p95", f"{mem_scen['all_healthy']['latency_p95_ms']:.2f}"),
        ("redis/all_healthy p50", f"{red_scen['all_healthy']['latency_p50_ms']:.2f}"),
        ("redis/all_healthy p95", f"{red_scen['all_healthy']['latency_p95_ms']:.2f}"),
        ("memory/both_degraded availability", f"{mem_scen['both_degraded']['availability']:.3f}"),
        ("redis/both_degraded availability", f"{red_scen['both_degraded']['availability']:.3f}"),
    ]

    for scenario in ("primary_flaky_50", "baseline_cached", "no_cache"):
        recovery = mem_scen[scenario]["recovery_time_ms"]
        if recovery is not None:
            pairs.append((f"memory/{scenario} recovery", f"{recovery:.1f}"))

    for name, blob in (("load", load), ("load_cached", load_cached)):
        for row in blob["runs"]:
            tag = f"{name}/{row['workers']}w"
            pairs += [
                (f"{tag} throughput", f"{row['throughput_rps']:.1f}"),
                (f"{tag} p50", f"{row['latency_p50_ms']:.1f}"),
                (f"{tag} p95", f"{row['latency_p95_ms']:.1f}"),
                (f"{tag} cache hits", f"| {row['cache_hits']} |"),
            ]

    return pairs


def main() -> int:
    if not REPORT.exists():
        print(f"missing {REPORT}")
        return 1
    text = REPORT.read_text(encoding="utf-8")

    missing = [(label, value) for label, value in _expected() if value not in text]
    checked = len(_expected())

    if missing:
        print(f"{len(missing)} of {checked} figures are NOT present in {REPORT}:\n")
        for label, value in missing:
            print(f"  {label:38s} expected to find: {value}")
        print("\nThe report is stale - regenerate the affected tables.")
        return 1

    print(f"OK: all {checked} generated figures appear in {REPORT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

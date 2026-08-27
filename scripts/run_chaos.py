from __future__ import annotations

import argparse
import json
from pathlib import Path

from reliability_lab.chaos import load_queries, run_simulation_detailed
from reliability_lab.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/default.yaml")
    parser.add_argument("--out", default="reports/metrics.json")
    parser.add_argument("--csv", default="reports/metrics.csv")
    parser.add_argument(
        "--per-scenario",
        default="reports/metrics_by_scenario.json",
        help="Per-scenario metric breakdown used by the final report.",
    )
    parser.add_argument("--seed", type=int, default=None, help="Seed RNG for a reproducible run.")
    args = parser.parse_args()

    if args.seed is not None:
        import random

        random.seed(args.seed)

    config = load_config(args.config)
    metrics, per_scenario = run_simulation_detailed(config, load_queries())

    metrics.write_json(args.out)
    print(f"wrote {args.out}")

    metrics.write_csv(args.csv)
    print(f"wrote {args.csv}")

    breakdown = {
        name: {
            **result.to_report_dict(),
            "cache_hits": result.cache_hits,
            "fallback_successes": result.fallback_successes,
            "static_fallbacks": result.static_fallbacks,
            "successful_requests": result.successful_requests,
            "failed_requests": result.failed_requests,
            "status": metrics.scenarios.get(name, "unknown"),
        }
        for name, result in per_scenario.items()
    }
    for entry in breakdown.values():
        entry.pop("scenarios", None)
    out = Path(args.per_scenario)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(breakdown, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {args.per_scenario}")


if __name__ == "__main__":
    main()

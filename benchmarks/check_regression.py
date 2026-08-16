"""Compare two rsloop benchmark JSON files and enforce performance budgets."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--throughput-regression", type=float, default=3.0)
    parser.add_argument("--latency-regression", type=float, default=5.0)
    parser.add_argument("--rss-regression", type=float, default=5.0)
    parser.add_argument("--require-improvement", type=float, default=0.0)
    return parser.parse_args()


def median(values: list[float]) -> float:
    return statistics.median(values)


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction + 0.999999) - 1))
    return ordered[index]


def load_metrics(path: Path) -> dict[str, dict[str, float]]:
    payload: list[dict[str, Any]] = json.loads(path.read_text(encoding="utf-8"))
    metrics: dict[str, dict[str, float]] = {}
    for item in payload:
        if item.get("loop") != "rsloop":
            continue
        runs = item["runs"]
        name = item.get("scenario") or item.get("workload")
        if not isinstance(name, str) or not runs:
            continue
        if "latency_ms" in runs[0]:
            throughput = median([run["operations"] / run["seconds"] for run in runs])
            p95 = median([percentile(run["latency_ms"], 0.95) for run in runs])
            p99 = median([percentile(run["latency_ms"], 0.99) for run in runs])
        else:
            throughput = median([run["ops_per_sec"] for run in runs])
            p95 = p99 = 0.0
        metrics[name] = {
            "throughput": throughput,
            "p95": p95,
            "p99": p99,
            "rss": median([run["peak_rss_bytes"] for run in runs]),
        }
    if not metrics:
        raise ValueError(f"{path} contains no rsloop measurements")
    return metrics


def percent_change(before: float, after: float) -> float:
    if before == 0.0:
        return 0.0 if after == 0.0 else float("inf")
    return (after - before) * 100.0 / before


def main() -> int:
    args = parse_args()
    baseline = load_metrics(args.baseline)
    candidate = load_metrics(args.candidate)
    names = sorted(baseline.keys() & candidate.keys())
    if not names:
        raise SystemExit("baseline and candidate have no matching rsloop workloads")

    failures: list[str] = []
    best_improvement = float("-inf")
    print(f"{'workload':<24} {'throughput':>12} {'p95':>10} {'p99':>10} {'rss':>10}")
    for name in names:
        old = baseline[name]
        new = candidate[name]
        throughput = percent_change(old["throughput"], new["throughput"])
        p95 = percent_change(old["p95"], new["p95"])
        p99 = percent_change(old["p99"], new["p99"])
        rss = percent_change(old["rss"], new["rss"])
        best_improvement = max(best_improvement, throughput)
        print(
            f"{name:<24} {throughput:>+11.2f}% {p95:>+9.2f}% {p99:>+9.2f}% {rss:>+9.2f}%"
        )
        if throughput < -args.throughput_regression:
            failures.append(f"{name}: throughput regressed {-throughput:.2f}%")
        for metric, change in (("p95", p95), ("p99", p99)):
            if old[metric] and change > args.latency_regression:
                failures.append(f"{name}: {metric} regressed {change:.2f}%")
        if old["rss"] and rss > args.rss_regression:
            failures.append(f"{name}: peak RSS regressed {rss:.2f}%")

    if args.require_improvement and best_improvement < args.require_improvement:
        failures.append(
            f"best throughput improvement {best_improvement:.2f}% is below "
            f"the required {args.require_improvement:.2f}%"
        )
    if failures:
        print("\nPerformance gate failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("\nPerformance gate passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

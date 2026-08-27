from __future__ import annotations

import json
import random
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from reliability_lab.cache import ResponseCache, SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker
from reliability_lab.config import LabConfig, ScenarioConfig
from reliability_lab.gateway import GatewayResponse, ReliabilityGateway
from reliability_lab.metrics import RunMetrics
from reliability_lab.providers import FakeLLMProvider

# Assumed price of one avoided provider call, used to value cache hits.
CACHE_SAVING_PER_HIT = 0.001


def load_queries(path: str | Path = "data/sample_queries.jsonl") -> list[str]:
    queries: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        queries.append(json.loads(line)["query"])
    return queries


def build_gateway(config: LabConfig, provider_overrides: dict[str, float] | None = None) -> ReliabilityGateway:
    providers = []
    for p in config.providers:
        fail_rate = provider_overrides.get(p.name, p.fail_rate) if provider_overrides else p.fail_rate
        providers.append(FakeLLMProvider(p.name, fail_rate, p.base_latency_ms, p.cost_per_1k_tokens))
    breakers = {
        p.name: CircuitBreaker(
            name=p.name,
            failure_threshold=config.circuit_breaker.failure_threshold,
            reset_timeout_seconds=config.circuit_breaker.reset_timeout_seconds,
            success_threshold=config.circuit_breaker.success_threshold,
        )
        for p in config.providers
    }
    cache: ResponseCache | SharedRedisCache | None = None
    if config.cache.enabled:
        if config.cache.backend == "redis":
            cache = SharedRedisCache(
                config.cache.redis_url,
                config.cache.ttl_seconds,
                config.cache.similarity_threshold,
            )
        else:
            cache = ResponseCache(config.cache.ttl_seconds, config.cache.similarity_threshold)
    return ReliabilityGateway(providers, breakers, cache)


def calculate_recovery_time_ms(gateway: ReliabilityGateway) -> float | None:
    """Derive recovery time from circuit breaker transition logs.

    A recovery is an open -> closed pair on the same breaker; its duration is
    the wall-clock gap between the two transitions. Returns the mean across all
    recoveries, or None if no circuit ever recovered.
    """
    recoveries: list[float] = []
    for breaker in gateway.breakers.values():
        opened_ts: float | None = None
        for entry in breaker.transition_log:
            target = entry.get("to")
            ts = entry.get("ts")
            if not isinstance(ts, (int, float)):
                continue
            if target == "open":
                opened_ts = float(ts)
            elif target == "closed" and opened_ts is not None:
                recoveries.append((float(ts) - opened_ts) * 1000.0)
                opened_ts = None
    if not recoveries:
        return None
    return sum(recoveries) / len(recoveries)


def _scenario_config(config: LabConfig, scenario: ScenarioConfig) -> LabConfig:
    """Apply per-scenario overrides that are not provider fail rates."""
    if scenario.cache_enabled is None or scenario.cache_enabled == config.cache.enabled:
        return config
    scoped = config.model_copy(deep=True)
    scoped.cache.enabled = scenario.cache_enabled
    return scoped


def _aggregate(gateway: ReliabilityGateway, results: list[GatewayResponse]) -> RunMetrics:
    """Fold a list of gateway responses into RunMetrics.

    Kept separate from the request loop so the sequential and the concurrent
    runners produce metrics by exactly the same rules - otherwise a load-test
    comparison would be measuring two different definitions of "success".
    """
    metrics = RunMetrics()
    for result in results:
        metrics.total_requests += 1
        metrics.estimated_cost += result.estimated_cost

        if result.cache_hit:
            metrics.cache_hits += 1
            metrics.estimated_cost_saved += CACHE_SAVING_PER_HIT

        if result.route == "fallback":
            metrics.fallback_successes += 1
            metrics.successful_requests += 1
        elif result.route == "static_fallback":
            metrics.static_fallbacks += 1
            metrics.failed_requests += 1
        else:
            metrics.successful_requests += 1

        if result.latency_ms > 0:
            metrics.latencies_ms.append(result.latency_ms)

    metrics.circuit_open_count = sum(
        1
        for breaker in gateway.breakers.values()
        for entry in breaker.transition_log
        if entry.get("to") == "open"
    )
    metrics.recovery_time_ms = calculate_recovery_time_ms(gateway)
    return metrics


def run_requests(
    gateway: ReliabilityGateway,
    queries: list[str],
    requests: int,
    workers: int = 1,
) -> RunMetrics:
    """Drive `requests` calls through an existing gateway using `workers` threads.

    workers=1 runs the loop inline and draws each prompt just before its call,
    exactly as the original sequential runner did. That ordering matters: the
    providers draw from the same RNG, so pre-generating the prompt list would
    shift every subsequent draw and silently change every seeded result.
    """
    if workers <= 1:
        results = [gateway.complete(random.choice(queries)) for _ in range(requests)]
    else:
        prompts = [random.choice(queries) for _ in range(requests)]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(gateway.complete, prompts))
    return _aggregate(gateway, results)


def run_scenario(config: LabConfig, queries: list[str], scenario: ScenarioConfig) -> RunMetrics:
    """Run a single named chaos scenario against a freshly built gateway."""
    scoped_config = _scenario_config(config, scenario)
    gateway = build_gateway(scoped_config, scenario.provider_overrides or None)
    return run_requests(gateway, queries, scoped_config.load_test.requests, workers=1)


def run_scenario_concurrent(
    config: LabConfig,
    queries: list[str],
    scenario: ScenarioConfig,
    workers: int,
) -> tuple[RunMetrics, ReliabilityGateway]:
    """Same scenario, driven by `workers` concurrent threads.

    Returns the gateway as well so a caller can inspect breaker transition logs
    or provider call counters after the run.
    """
    scoped_config = _scenario_config(config, scenario)
    gateway = build_gateway(scoped_config, scenario.provider_overrides or None)
    metrics = run_requests(gateway, queries, scoped_config.load_test.requests, workers=workers)
    return metrics, gateway


def scenario_passed(scenario: ScenarioConfig, result: RunMetrics) -> bool:
    """Evidence-based pass/fail criteria, one rule per named scenario.

    The generic rule (no request ended in a static fallback) applies to any
    scenario without a specific rule, so new scenarios still get graded.
    """
    if scenario.name == "primary_timeout_100":
        # Primary is dead: traffic must survive on the backup path.
        return result.fallback_success_rate > 0.9 and result.error_rate < 0.05
    if scenario.name == "primary_flaky_50":
        # The breaker must actually trip and then recover.
        return result.circuit_open_count > 0 and result.availability > 0.9
    if scenario.name == "all_healthy":
        # Nothing is failing, so nothing should be degraded and no circuit
        # should ever trip.
        return (
            result.error_rate == 0.0
            and result.static_fallbacks == 0
            and result.circuit_open_count == 0
        )
    if scenario.name == "baseline_cached":
        # Paired control for no_cache: same failure rates, cache ON. It must
        # actually serve cache hits and stay above the availability SLO.
        return result.cache_hits > 0 and result.availability >= 0.95
    if scenario.name == "no_cache":
        # Control run for the cache comparison: it must serve zero cache hits.
        return result.cache_hits == 0 and result.availability > 0.9
    if scenario.name == "both_degraded":
        # Both providers are unhealthy. Success here is not availability - it is
        # graceful degradation: every request is accounted for, the static
        # fallback actually answers, and open circuits make us fail fast
        # (p95 well under one provider round-trip) instead of hanging.
        accounted = result.total_requests == (
            result.successful_requests + result.failed_requests
        )
        return accounted and result.static_fallbacks > 0 and result.percentile(95) < 1000
    return result.static_fallbacks == 0 and result.successful_requests > 0


def run_simulation(config: LabConfig, queries: list[str]) -> RunMetrics:
    """Run all named scenarios from config, or a default run if none defined."""
    if not config.scenarios:
        default_scenario = ScenarioConfig(name="default", description="baseline run")
        metrics = run_scenario(config, queries, default_scenario)
        metrics.scenarios = {"default": "pass" if metrics.successful_requests > 0 else "fail"}
        return metrics

    combined = RunMetrics()
    for scenario in config.scenarios:
        result = run_scenario(config, queries, scenario)
        combined.scenarios[scenario.name] = "pass" if scenario_passed(scenario, result) else "fail"

        combined.total_requests += result.total_requests
        combined.successful_requests += result.successful_requests
        combined.failed_requests += result.failed_requests
        combined.fallback_successes += result.fallback_successes
        combined.static_fallbacks += result.static_fallbacks
        combined.cache_hits += result.cache_hits
        combined.circuit_open_count += result.circuit_open_count
        combined.estimated_cost += result.estimated_cost
        combined.estimated_cost_saved += result.estimated_cost_saved
        combined.latencies_ms.extend(result.latencies_ms)
        if result.recovery_time_ms is not None:
            if combined.recovery_time_ms is None:
                combined.recovery_time_ms = result.recovery_time_ms
            else:
                combined.recovery_time_ms = (combined.recovery_time_ms + result.recovery_time_ms) / 2

    return combined


def run_simulation_detailed(
    config: LabConfig, queries: list[str]
) -> tuple[RunMetrics, dict[str, RunMetrics]]:
    """Same as run_simulation but also returns the per-scenario metrics.

    The combined RunMetrics is what the grader reads from metrics.json; the
    per-scenario breakdown is what the report needs for the chaos table and the
    cache comparison.
    """
    per_scenario: dict[str, RunMetrics] = {}
    if not config.scenarios:
        combined = run_simulation(config, queries)
        per_scenario["default"] = combined
        return combined, per_scenario

    combined = RunMetrics()
    for scenario in config.scenarios:
        result = run_scenario(config, queries, scenario)
        per_scenario[scenario.name] = result
        combined.scenarios[scenario.name] = "pass" if scenario_passed(scenario, result) else "fail"

        combined.total_requests += result.total_requests
        combined.successful_requests += result.successful_requests
        combined.failed_requests += result.failed_requests
        combined.fallback_successes += result.fallback_successes
        combined.static_fallbacks += result.static_fallbacks
        combined.cache_hits += result.cache_hits
        combined.circuit_open_count += result.circuit_open_count
        combined.estimated_cost += result.estimated_cost
        combined.estimated_cost_saved += result.estimated_cost_saved
        combined.latencies_ms.extend(result.latencies_ms)
        if result.recovery_time_ms is not None:
            if combined.recovery_time_ms is None:
                combined.recovery_time_ms = result.recovery_time_ms
            else:
                combined.recovery_time_ms = (combined.recovery_time_ms + result.recovery_time_ms) / 2

    return combined, per_scenario

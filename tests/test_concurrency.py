"""Thread-safety tests for the shared state touched by the concurrent load test.

These assert the invariants the locks in CircuitBreaker, ResponseCache and
ReliabilityGateway exist to protect: no lost counter updates, no dropped cache
writes, no duplicated transition-log rows, and a cost total that adds up.

A caveat worth stating, because it changes how much these tests prove: under
CPython's GIL most of these races have a window too narrow to hit at small
scale, so a lock-free implementation can pass them by luck. The cache test is
built to defeat that - it preloads enough entries that the eviction rebuild
spans a GIL switch, and it fails reliably without the lock. The others are
regression guards for the invariant rather than reproducers of the race.
"""
from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor

from reliability_lab.cache import CacheEntry, ResponseCache
from reliability_lab.circuit_breaker import CircuitBreaker, CircuitState
from reliability_lab.gateway import ReliabilityGateway
from reliability_lab.providers import FakeLLMProvider


def test_failure_counter_loses_no_updates_under_threads() -> None:
    """500 concurrent failures must be counted as exactly 500."""
    cb = CircuitBreaker("test", failure_threshold=10_000, reset_timeout_seconds=60)
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: cb.record_failure(), range(500)))
    assert cb.failure_count == 500
    assert cb.state == CircuitState.CLOSED


def test_circuit_trips_exactly_once_under_threads() -> None:
    """Many threads crossing the threshold together must log a single trip."""
    cb = CircuitBreaker("test", failure_threshold=5, reset_timeout_seconds=60)
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: cb.record_failure(), range(200)))
    open_transitions = [t for t in cb.transition_log if t["to"] == "open"]
    assert len(open_transitions) == 1
    assert open_transitions[0]["reason"] == "failure_threshold_reached"
    assert cb.state == CircuitState.OPEN


def test_cache_writes_are_not_lost_under_threads() -> None:
    """get() rebuilds the entry list when evicting; concurrent set() must survive.

    `self._entries = [...]` is a read-modify-write: an append landing between
    the comprehension reading the old list and the assignment replacing it is
    dropped. The window is only wide enough to hit when the rebuild spans a GIL
    switch, so the cache is preloaded with enough entries to make it so - with
    a 20-entry cache this test passes with or without the lock and proves
    nothing.
    """
    rounds, writes_per_round, filler_size = 6, 120, 20_000
    cache = ResponseCache(ttl_seconds=1, similarity_threshold=0.99)
    original_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-5)
    missing: list[str] = []
    try:
        for round_index in range(rounds):
            # Stale filler: wide enough that the rebuild spans a GIL switch, and
            # expired so the surviving list stays small and scoring stays cheap.
            stale = time.time() - 3600
            cache._entries = [
                CacheEntry(key=f"filler{i}", value="v", created_at=stale, metadata={})
                for i in range(filler_size)
            ]
            fresh = [f"fresh {round_index}-{i}" for i in range(writes_per_round)]

            # Readers must be interleaved with writers, not queued behind them:
            # a rebuild that runs after the last set() has no window to lose.
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = []
                for position, query in enumerate(fresh):
                    if position % 4 == 0:
                        futures.append(pool.submit(cache.get, "filler0"))
                    futures.append(pool.submit(cache.set, query, "answer"))
                for future in futures:
                    future.result()

            stored = {e.key for e in cache._entries}
            missing.extend(q for q in fresh if q not in stored)
    finally:
        sys.setswitchinterval(original_interval)

    assert not missing, f"{len(missing)} concurrent cache writes were lost"


def test_gateway_serves_every_concurrent_request() -> None:
    """Every request gets a response and the cost counter adds up."""
    provider = FakeLLMProvider("primary", fail_rate=0.0, base_latency_ms=1, cost_per_1k_tokens=0.01)
    breakers = {"primary": CircuitBreaker("primary", failure_threshold=3, reset_timeout_seconds=1)}
    gateway = ReliabilityGateway([provider], breakers, ResponseCache(300, 0.99))

    prompts = [f"distinct question {i}" for i in range(100)]
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(gateway.complete, prompts))

    assert len(results) == 100
    assert all(r.text for r in results)
    assert all(r.route in {"primary", "fallback"} or r.cache_hit for r in results)
    assert abs(gateway.total_cost - sum(r.estimated_cost for r in results)) < 1e-9

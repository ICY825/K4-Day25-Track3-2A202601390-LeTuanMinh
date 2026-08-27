from __future__ import annotations

import threading
from dataclasses import dataclass

from reliability_lab.cache import ResponseCache, SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker, CircuitOpenError
from reliability_lab.providers import FakeLLMProvider, ProviderError, ProviderResponse

STATIC_FALLBACK_TEXT = "The service is temporarily degraded. Please try again soon."

# Fraction of the cost budget above which the gateway prefers the cheapest
# provider instead of the configured primary.
BUDGET_DEGRADE_RATIO = 0.8


@dataclass(slots=True)
class GatewayResponse:
    text: str
    route: str
    provider: str | None
    cache_hit: bool
    latency_ms: float
    estimated_cost: float
    error: str | None = None


class ReliabilityGateway:
    """Routes requests through cache, circuit breakers, and fallback providers."""

    def __init__(
        self,
        providers: list[FakeLLMProvider],
        breakers: dict[str, CircuitBreaker],
        cache: ResponseCache | SharedRedisCache | None = None,
        cost_budget: float | None = None,
    ):
        self.providers = providers
        self.breakers = breakers
        self.cache = cache
        self.cost_budget = cost_budget
        self.total_cost = 0.0
        self._cost_lock = threading.Lock()

    def complete(self, prompt: str) -> GatewayResponse:
        """Return a reliable response or a static fallback.

        Pipeline: cache -> circuit-broken provider chain -> static fallback.
        Each provider is attempted at most once per request, so an unhealthy
        provider is never retried into a storm; the breaker decides whether it
        is attempted at all.
        """
        # 1. CACHE CHECK
        if self.cache is not None:
            cached_text, score = self.cache.get(prompt)
            if cached_text is not None:
                return GatewayResponse(
                    text=cached_text,
                    route=f"cache_hit:{score:.2f}",
                    provider=None,
                    cache_hit=True,
                    latency_ms=0.0,
                    estimated_cost=0.0,
                )

        # Cost-aware routing (bonus): once the budget is spent, serve cache only;
        # near the limit, try the cheapest provider first.
        if self.cost_budget is not None and self.total_cost >= self.cost_budget:
            return GatewayResponse(
                text=STATIC_FALLBACK_TEXT,
                route="static_fallback",
                provider=None,
                cache_hit=False,
                latency_ms=0.0,
                estimated_cost=0.0,
                error="cost budget exhausted",
            )

        chain = self.providers
        if (
            self.cost_budget is not None
            and self.total_cost >= self.cost_budget * BUDGET_DEGRADE_RATIO
        ):
            chain = sorted(self.providers, key=lambda p: p.cost_per_1k_tokens)

        # 2. PROVIDER FALLBACK CHAIN
        primary_name = self.providers[0].name if self.providers else None
        last_error: str | None = None
        for provider in chain:
            breaker = self.breakers[provider.name]
            try:
                response: ProviderResponse = breaker.call(provider.complete, prompt)
            except CircuitOpenError as exc:
                last_error = f"{provider.name}: circuit_open: {exc}"
                continue
            except ProviderError as exc:
                last_error = f"{provider.name}: provider_error: {exc}"
                continue

            if self.cache is not None:
                self.cache.set(prompt, response.text, {"provider": provider.name})
            with self._cost_lock:
                self.total_cost += response.estimated_cost
            return GatewayResponse(
                text=response.text,
                route="primary" if provider.name == primary_name else "fallback",
                provider=provider.name,
                cache_hit=False,
                latency_ms=response.latency_ms,
                estimated_cost=response.estimated_cost,
            )

        # 3. STATIC FALLBACK
        return GatewayResponse(
            text=STATIC_FALLBACK_TEXT,
            route="static_fallback",
            provider=None,
            cache_hit=False,
            latency_ms=0.0,
            estimated_cost=0.0,
            error=last_error,
        )

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TypeVar

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """Raised when a circuit is open and calls should fail fast."""


@dataclass(slots=True)
class CircuitBreaker:
    """Production-safe 3-state circuit breaker.

    - CLOSED: calls pass through; consecutive failures are counted.
    - OPEN: fail fast until reset_timeout_seconds elapses.
    - HALF_OPEN: allow probe calls; close after success_threshold successes,
      re-open immediately on a single probe failure.

    Two clocks are used on purpose: time.monotonic() for the reset timeout
    (immune to wall-clock jumps) and time.time() for the transition log
    timestamps (comparable with metrics collected elsewhere).

    Thread safety: every read-modify-write of the counters and of the state is
    guarded by a lock, so concurrent workers cannot lose a failure count or log
    the same trip twice. The lock is never held while the wrapped function runs
    - that would serialise provider calls and turn the breaker into a
    bottleneck - so call() takes it three times briefly instead of once around
    the whole call.
    """

    name: str
    failure_threshold: int
    reset_timeout_seconds: float
    success_threshold: int = 1
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    opened_at: float | None = None
    transition_log: list[dict[str, str | float]] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def allow_request(self) -> bool:
        """Return whether a request should be attempted."""
        with self._lock:
            if self.state is CircuitState.CLOSED:
                return True
            if self.state is CircuitState.HALF_OPEN:
                # Probe request: let exactly this call through and learn from it.
                return True
            # OPEN: fail fast until the reset timeout has elapsed.
            if self.opened_at is None:
                self._transition(CircuitState.HALF_OPEN, "reset_timeout_elapsed")
                return True
            if time.monotonic() - self.opened_at >= self.reset_timeout_seconds:
                self._transition(CircuitState.HALF_OPEN, "reset_timeout_elapsed")
                return True
            return False

    def call(self, fn: Callable[..., T], *args: object, **kwargs: object) -> T:
        """Call a function through the circuit breaker.

        Exactly one attempt per call — no internal retries, so an unhealthy
        provider cannot be hammered into a retry storm.
        """
        if not self.allow_request():
            raise CircuitOpenError(f"circuit '{self.name}' is open")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self.failure_count = 0
            self.success_count += 1
            if (
                self.state is CircuitState.HALF_OPEN
                and self.success_count >= self.success_threshold
            ):
                self._transition(CircuitState.CLOSED, "probe_success")
                self.success_count = 0
                self.opened_at = None

    def record_failure(self) -> None:
        """Record a failed call.

        HALF_OPEN and threshold breaches are handled separately (if/elif, never
        combined with `or`) because they are different events and must be
        logged with different reasons.
        """
        with self._lock:
            self.failure_count += 1
            self.success_count = 0
            if self.state is CircuitState.HALF_OPEN:
                self._transition(CircuitState.OPEN, "probe_failure")
                self.opened_at = time.monotonic()
            elif (
                self.state is CircuitState.CLOSED
                and self.failure_count >= self.failure_threshold
            ):
                self._transition(CircuitState.OPEN, "failure_threshold_reached")
                self.opened_at = time.monotonic()

    def _transition(self, new_state: CircuitState, reason: str) -> None:
        """Append a state change. Callers must already hold self._lock."""
        if self.state == new_state:
            return
        self.transition_log.append(
            {"from": self.state.value, "to": new_state.value, "reason": reason, "ts": time.time()}
        )
        self.state = new_state

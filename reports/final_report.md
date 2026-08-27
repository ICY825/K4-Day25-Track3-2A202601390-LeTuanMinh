# Day 25 — Reliability Engineering for Production Agents — Final Report

**Student:** Le Tuan Minh — 2A202601390 — K4 Track 3
**Environment:** Windows 11, Python 3.11.9, Redis 7-alpine (docker compose), all providers simulated by `FakeLLMProvider` (no API keys used).

**Reproduce everything:**

```bash
pip install -e ".[dev]"
docker compose up -d
make test                 # 39 passed, 7 xpassed (log: reports/test_final.log)
make run-chaos            # in-memory cache    -> reports/metrics.json + .csv + by_scenario
make run-chaos-redis      # shared Redis cache -> reports/metrics_redis.*
make report               # reports/metrics_summary.md
python scripts/cache_evidence.py       # -> reports/cache_evidence.txt
python scripts/verify_shared_cache.py  # -> reports/redis_evidence.txt
python scripts/run_load_test.py        # -> reports/load_test.{txt,json}
python scripts/run_load_test.py --scenario baseline_cached --out reports/load_test_cached.json
make verify               # cross-checks every figure below against the artifacts
```

Both chaos targets pass `--seed 42`, so `make run-chaos` on the grader's machine
reproduces the exact counts quoted here — verified by running the whole flow in a
clean virtualenv, where every count matched and only wall-clock latency moved
(P50 236.29 vs 236.59 ms). Dropping the flag gives an independent sample instead.

`make verify` (`scripts/verify_report.py`) exists because the tables below are
written by hand while the figures come from generated files: it fails if any of
the 67 checked numbers in this report is absent from the artifacts. Run it
before regenerating the simulation, since a re-run changes the wall-clock
latencies and legitimately invalidates the report until it is updated.

**On reproducibility:** `--seed 42` fixes the RNG, so every *count* below is
bit-for-bit reproducible — availability, error rate, cache hits, fallback
counts, circuit-open counts and cost are identical on every re-run. Latency and
recovery figures are **wall-clock measurements** taken around `time.sleep()` in
`FakeLLMProvider`, so they drift by well under 1 ms between runs on the same
machine and will differ more on different hardware. The latency numbers quoted
here match the committed artifacts exactly.

Every number in this report comes from a `--seed 42` run and is stored in
`reports/metrics.json`, `reports/metrics_by_scenario.json`,
`reports/metrics_redis.json`, `reports/metrics_redis_by_scenario.json`,
`reports/cache_evidence.txt`, `reports/redis_evidence.txt`,
`reports/load_test.json` and `reports/load_test_cached.json`.

---

## 1. Architecture summary

The gateway is a single pipeline with three lines of defence. Each request
passes at most once through each provider — the circuit breaker, not a retry
loop, decides whether a provider is attempted at all, which is what prevents a
retry storm against an already-failing dependency.

```
                          ReliabilityGateway.complete(prompt)
                                        |
                    +-------------------v--------------------+
                    | 1. CACHE                                |
                    |    ResponseCache (memory)               |
                    |    or SharedRedisCache (Redis)          |
                    |    - privacy filter (never store/serve) |
                    |    - n-gram cosine similarity           |
                    |    - false-hit guardrail (dates/IDs)    |
                    +----------+------------------+-----------+
                          HIT |                   | MISS
        route="cache_hit:.99" |                   v
        latency 0ms, cost 0   |   +---------------------------------+
                    <---------+   | 2. PROVIDER CHAIN (in order)    |
                                  |                                 |
                                  |  CircuitBreaker["primary"]      |
                                  |    CLOSED/HALF_OPEN -> call     +--> FakeLLMProvider primary
                                  |    OPEN -> fail fast, skip      |    (fail_rate, latency, cost)
                                  |             |                   |
                                  |             v  on ProviderError |
                                  |               or CircuitOpenError|
                                  |  CircuitBreaker["backup"]       +--> FakeLLMProvider backup
                                  |    same gate                    |
                                  +---------------+-----------------+
                                                  | all providers exhausted
                                                  v
                                  +---------------------------------+
                                  | 3. STATIC FALLBACK              |
                                  |  "The service is temporarily    |
                                  |   degraded. Please try again."  |
                                  |  route="static_fallback"        |
                                  |  error = last provider error    |
                                  +---------------------------------+

Circuit breaker state machine (per provider):

        record_failure() x failure_threshold
   CLOSED ---------------------------------------> OPEN
      ^                                             |
      |                                             | allow_request() after
      | record_success() x success_threshold        | reset_timeout_seconds
      |  reason="probe_success"                     v
      +------------------------------------- HALF_OPEN
                                                    |
                       one failed probe             |
                       reason="probe_failure" ------+--> OPEN
```

Route labels are decided by position in the configured chain: the first
provider is `primary`, any later one is `fallback`, so the route string always
identifies *which* defence answered. Every state change is appended to
`CircuitBreaker.transition_log` with `from`, `to`, `reason` and `ts`, which is
the raw evidence used for `circuit_open_count` and recovery time.

**Design decisions worth calling out**

| Decision | Why |
|---|---|
| `time.monotonic()` for the reset timeout, `time.time()` for log timestamps | The timeout must survive a wall-clock adjustment; the log must be comparable with other observability data. |
| `record_failure()` uses `if HALF_OPEN / elif threshold` | A failed probe and a threshold breach are different events and must be distinguishable in the log (`probe_failure` vs `failure_threshold_reached`). Merging them with `or` loses the reason. |
| `_transition()` is a no-op when the state is unchanged | Keeps `transition_log` free of duplicate `open -> open` entries, so `circuit_open_count` counts real trips. |
| One attempt per provider per request | No retry storm. Recovery is driven by the breaker's timeout, not by client retries. |
| Cache write happens only after a successful provider call | A degraded/static answer is never cached and never poisons later requests. |
| Locks guard every counter/state mutation, and are never held across a provider call | Makes the concurrent load test in §7 measure real behaviour instead of lost updates, without serialising the provider calls the gateway exists to make. |

---

## 2. Configuration

`configs/default.yaml` (in-memory cache) and `configs/redis.yaml` (identical but
`backend: redis`).

| Setting | Value | Reason |
|---|---:|---|
| `providers[0] primary` | fail 0.25, 180 ms, $0.01/1k | The expensive, fast, unreliable model — the realistic reason a fallback chain exists. |
| `providers[1] backup` | fail 0.05, 260 ms, $0.006/1k | Cheaper and more reliable but slower: falling back costs latency, not money. |
| `failure_threshold` | 3 | 1 would trip on a single unlucky call (25% base fail rate makes that common); 5+ lets ~5 users eat an error before protection kicks in. 3 consecutive failures at a 25% base rate is a 1.6% coincidence, so a trip is real signal. |
| `reset_timeout_seconds` | 2 | Long enough that an open circuit actually sheds load, short enough that measured recovery (~2.3–2.4 s, see §4) stays far below the 5 s SLO. |
| `success_threshold` | 1 | One good probe is enough to close, because a failed probe re-opens immediately. Raising it to 2 doubles the exposure window with no extra safety here. |
| `cache.ttl_seconds` | 300 | Matches the "dated/policy" query class in `data/sample_queries.jsonl`: answers that go stale within minutes, not hours. Long enough to survive a full 6-scenario chaos run (~85 s). |
| `cache.similarity_threshold` | 0.92 | Measured, not guessed — see the sweep below. |
| `load_test.requests` | 100 per scenario (600 total) | Enough samples for a meaningful P99 (top 1% = 6 requests over the whole run) while keeping a full run under ~90 s. |

### Why 0.92 and not 0.85

Measured pairwise scores from `reports/cache_evidence.txt` (n-gram cosine over
word tokens + character 3-grams):

| Case | Score |
|---|---:|
| identical | 1.000 |
| paraphrase — "Summarize **the** refund policy" vs "Summarize refund policy" | 0.880 |
| paraphrase — article added to a circuit-breaker question | 0.901 |
| **same topic, different intent** — refund policy **2024** vs **2026** | 0.915 |
| related topic — "circuit breaker pattern" vs "circuit breaker design" | 0.681 |
| unrelated — refund policy vs HTTP 429 handling | 0.028 |

The dangerous pair (0.915) scores **higher** than a harmless paraphrase (0.880).
Similarity alone cannot separate them, which is exactly why the threshold is set
above 0.915 *and* a semantic guardrail runs on top of it:

| Threshold | 2024/2026 pair above threshold? | Outcome |
|---:|:--|:--|
| 0.75 | yes | blocked by guardrail (false hit logged) |
| 0.80 | yes | blocked by guardrail |
| 0.85 | yes | blocked by guardrail |
| 0.90 | yes | blocked by guardrail |
| **0.92** | **no** | miss (never reaches the guardrail) |
| 0.95 | no | miss |

At 0.85 the system depends entirely on the guardrail to avoid answering a 2026
question with a 2024 answer. At 0.92 the threshold rejects it first and the
guardrail is defence-in-depth. The price is the 0.880/0.901 paraphrases that are
now cache misses — a measured trade of hit rate for correctness.

---

## 3. SLO definitions

Steady-state SLOs are evaluated over the five operational scenarios. The sixth,
`both_degraded`, is a deliberate total-outage drill: it is graded on graceful
degradation (§7), not on availability, and is excluded here.

**In-memory cache — 500 requests (excluding `both_degraded`)**

| SLI | SLO target | Actual | Met? |
|---|---|---:|---|
| Availability | >= 99% | 98.60% | ✗ (7 static fallbacks) |
| Latency P95 | < 2500 ms | 315.58 ms | ✓ |
| Fallback success rate | >= 95% | 94.31% | ✗ (marginal) |
| Cache hit rate | >= 10% | 47.60% | ✓ |
| Recovery time | < 5000 ms | 2392.95 ms | ✓ |

**Shared Redis cache — same 500 requests**

| SLI | SLO target | Actual | Met? |
|---|---|---:|---|
| Availability | >= 99% | 99.20% | ✓ |
| Latency P95 | < 2500 ms | 316.46 ms | ✓ |
| Fallback success rate | >= 95% | 96.15% | ✓ |
| Cache hit rate | >= 10% | 58.00% | ✓ |
| Recovery time | < 5000 ms | 2374.11 ms | ✓ |

The in-memory configuration misses two SLOs by a hair; the shared cache meets
all five. The mechanism is in §6 — a warm shared cache absorbs requests that the
in-memory build has to send to a failing provider.

---

## 4. Metrics

Combined run, all 6 scenarios, 600 requests, `--seed 42`.

| Metric | In-memory (`reports/metrics.json`) | Redis (`reports/metrics_redis.json`) |
|---|---:|---:|
| total_requests | 600 | 600 |
| availability | 0.8217 | 0.9650 |
| error_rate | 0.1783 | 0.0350 |
| latency_p50_ms | 236.29 | 261.58 |
| latency_p95_ms | 315.58 | 316.46 |
| latency_p99_ms | 320.29 | 320.27 |
| fallback_success_rate | 0.5202 | 0.8359 |
| cache_hit_rate | 0.3967 | 0.6100 |
| circuit_open_count | 13 | 13 |
| recovery_time_ms | 2392.95 | 2374.11 |
| estimated_cost | 0.123956 | 0.100068 |
| estimated_cost_saved | 0.238000 | 0.366000 |

The combined availability of 0.8217 is dominated by `both_degraded`, which is
designed to fail every request (§7). Per-scenario numbers are in
`reports/metrics_by_scenario.json`; §3 gives the steady-state view.

**How to read the latency percentiles.** `run_scenario()` only records a latency
sample when `latency_ms > 0`, and a cache hit returns in 0 ms. The percentiles
above therefore describe *provider-served* requests only. Since 39.67% of all
requests were served from cache, the median latency actually experienced by a
caller is 0 ms; P50 = 236 ms is the median of the slower 60% that reached a
provider. This is deliberate — mixing 0 ms cache hits into the sample would hide
provider degradation behind a good-looking P50 — but it means the cache's
latency benefit does **not** show up in the table above.

**Recovery time.** 2392.95 ms against a `reset_timeout_seconds` of 2000 ms. The
~393 ms excess is the expected overhead: after the timeout expires the circuit
only moves to HALF_OPEN on the *next* arriving request, and that probe must
complete a full provider round-trip (180–320 ms) before the circuit closes.

**Circuit trips.** 13 open transitions across the run, concentrated where they
should be: 6 in `primary_timeout_100` (primary dead), 3 in `primary_flaky_50`,
2 in `both_degraded`, 1 each in the two baseline runs, and **0 in
`all_healthy`** — the breaker does not trip when nothing is wrong.

---

## 5. Cache comparison

`baseline_cached` and `no_cache` are a matched pair: identical provider fail
rates (primary 0.25 / backup 0.05), identical load, the only difference being
`cache_enabled`. Both were run inside the same simulation.

**In-memory cache**

| Metric | Without cache | With cache | Delta |
|---|---:|---:|---|
| latency_p50_ms | 223.53 | 227.62 | +4.09 (noise; see §4) |
| latency_p95_ms | 310.81 | 312.23 | +1.42 (noise) |
| latency_p99_ms | 315.34 | 320.48 | +5.14 (noise) |
| estimated_cost | 0.051390 | 0.020708 | **−59.7%** |
| cache_hit_rate | 0.00 | 0.58 | +0.58 |
| provider calls | 100 | 42 | −58 |
| availability | 0.990 | 0.990 | 0 |

**Shared Redis cache**

| Metric | Without cache | With cache | Delta |
|---|---:|---:|---|
| latency_p50_ms | 222.20 | 229.95 | +7.75 (noise) |
| latency_p95_ms | 312.02 | 313.25 | +1.23 (noise) |
| estimated_cost | 0.049760 | 0.012098 | **−75.7%** |
| cache_hit_rate | 0.00 | 0.76 | +0.76 |
| availability | 0.990 | 1.000 | +0.010 |

**Reading:** the cache buys cost and load, not measured percentile latency. 58
of 100 requests never reached a provider, cutting spend by 59.7% in-memory and
75.7% on Redis (the Redis run has a higher hit rate because the cache stays warm
across scenarios). The percentile columns barely move because, as explained in
§4, cache hits are excluded from the latency sample by design — the requests the
cache removed were removed from the measurement too.

Across the whole 600-request run the cache avoided 238 provider calls in-memory
and 366 on Redis, booked as `estimated_cost_saved` of $0.238 and $0.366 at the
$0.001/call accounting rate in `chaos.CACHE_SAVING_PER_HIT`.

---

## 6. Redis shared cache

**Why in-memory is insufficient in production.** A `ResponseCache` lives in one
process's heap. With N replicas behind a load balancer the same question is
answered by a cold cache up to N times, hit rate falls by roughly a factor of N,
every deploy or pod restart discards the whole cache, and — worst — during a
provider outage each replica independently discovers that the provider is down.
The cache stops being a reliability asset exactly when it is needed most.

**How `SharedRedisCache` solves it.** One Redis Hash per query under
`rl:cache:{md5(query)[:12]}` with fields `query` and `response`, and TTL enforced
by Redis `EXPIRE` (no manual eviction pass). Exact repeats cost a single `HGET`;
anything else falls back to `SCAN` over the namespace with the same n-gram
cosine and the same privacy/false-hit guardrails as the in-memory path. Any
replica that warms the cache warms it for all of them, and the entries survive a
restart.

The effect is measurable. `both_degraded` (primary dead, backup failing 60%):

| | In-memory | Redis shared |
|---|---:|---:|
| availability | 0.000 | 0.830 |
| cache hits | 0 | 76 |
| static fallbacks | 100 | 17 |

With a cold per-process cache, a total provider outage means a total outage for
users. With a warm shared cache, 76 of 100 requests were still answered
correctly from entries populated by earlier traffic. That is the entire argument
for a shared cache, in one row.

### Evidence of shared state

`python scripts/verify_shared_cache.py` — full output in
`reports/redis_evidence.txt`. Two independent `SharedRedisCache` objects with
separate connections stand in for two replicas:

```
redis ping (instance A): True
redis ping (instance B): True

[1] instance A writes, instance B reads
    A.set('Explain circuit breaker states in one paragraph.')
    B.get -> '[primary] reliable answer for: Explain circuit breaker states' (score=1.00)

[2] instance B answers a re-phrased query from A's entry
    B.get('Explain the circuit breaker states in one paragraph')
    -> None (score=0.90)

[3] privacy-sensitive query is never written to Redis
    A.set('Give me the current account balance for user 123.')
    B.get -> None
    keys in Redis after the write: 1
```

Step [2] is an honest negative result: the paraphrase scores 0.90, below the
0.92 threshold, so it is a miss. The threshold is doing what §2 says it does —
it rejects a genuine paraphrase to keep the 0.915 date-mismatch pair out.

### Redis CLI output

```bash
$ docker compose exec redis redis-cli KEYS "rl:cache:*"
rl:cache:3dab98c0e49e   rl:cache:844ef0143a5c   rl:cache:734852f3cf4a
rl:cache:b2a52f7dc795   rl:cache:da61fb49b4f6   rl:cache:8baa2cfa11fa
rl:cache:0bc3b1acf73d   rl:cache:095946136fea   rl:cache:98332d0d1c9c
rl:cache:fff10da1c72c   rl:cache:9e413fd814eb   rl:cache:d354658dc020
rl:cache:dacb2b833659
(13 keys)

$ docker compose exec redis redis-cli HGETALL rl:cache:3dab98c0e49e
response
[backup] reliable answer for: Explain the difference between retry and circuit breaker pat
query
Explain the difference between retry and circuit breaker patterns.

$ docker compose exec redis redis-cli TTL rl:cache:3dab98c0e49e
191
```

**Privacy audit.** `data/sample_queries.jsonl` holds 20 queries; 5 match
`PRIVACY_PATTERNS` (account balance, password reset, credit card details, SSN
validation, employee SSN). After a full 600-request run Redis holds **13 keys,
none of them privacy-sensitive** — the 5 blocked queries produced zero writes,
and the remaining 2 cacheable queries were served from a similar entry and so
never created one of their own (this is the blind spot analysed in §8).

### In-memory vs Redis latency

| Scenario | In-memory P50 | Redis P50 | In-memory P95 | Redis P95 |
|---|---:|---:|---:|---:|
| all_healthy | 217.73 | 214.12 | 234.37 | 235.94 |
| baseline_cached | 227.62 | 229.95 | 312.23 | 313.25 |

Redis adds under 5 ms — inside run-to-run noise, and negligible against a
180–320 ms provider round-trip. The network hop is not a reason to prefer the
in-memory cache.

---

## 7. Chaos scenarios

Six scenarios, each 100 requests, each with its own pass criterion implemented
in `chaos.scenario_passed()` — the criteria are code, so the grade is derived
from evidence rather than from reading the numbers by eye.

| Scenario | Expected behavior | Pass criterion | Observed (in-memory) | Result |
|---|---|---|---|---|
| `primary_timeout_100` | Primary fails 100%; traffic survives on backup, primary circuit trips and stays open | `fallback_success_rate > 0.9` and `error_rate < 0.05` | availability 0.970, fallback rate 0.929, 6 circuit opens, 39 fallback successes, 3 static | **pass** |
| `primary_flaky_50` | Primary fails 50%; circuit oscillates open→half-open→closed | `circuit_open_count > 0` and `availability > 0.9` | availability 0.980, 3 opens, recovery 2386.5 ms, 32 fallback successes | **pass** |
| `all_healthy` | Both providers forced to 0% failure; everything served by primary, no circuit ever opens | `error_rate == 0` and `static_fallbacks == 0` and `circuit_open_count == 0` | availability 1.000, 0 opens, 0 static, P95 234.37 ms (fastest of all scenarios) | **pass** |
| `baseline_cached` | Default fail rates, cache ON — paired control for the cache comparison | `cache_hits > 0` and `availability >= 0.95` | availability 0.990, 58 hits, cost $0.0207 | **pass** |
| `no_cache` | Default fail rates, cache OFF — every request must reach a provider | `cache_hits == 0` and `availability > 0.9` | availability 0.990, 0 hits, cost $0.0514 (2.5× the cached run) | **pass** |
| `both_degraded` | Primary dead, backup failing 60% — the system must degrade gracefully, not hang or crash | all requests accounted for, `static_fallbacks > 0`, `P95 < 1000 ms` | availability 0.000, 100 static fallbacks, 2 opens, **P95 = 0 ms** | **pass** |

`all_healthy` originally shipped with `provider_overrides: {}`, which silently
inherited the default 25% primary failure rate and contradicted its own
description. It is now pinned to `primary: 0.0, backup: 0.0` so the "nothing is
wrong" baseline actually tests that the breaker stays quiet.

### Recovery evidence

From the breaker transition logs (`calculate_recovery_time_ms()` walks
`open → closed` pairs):

| Scenario | Recovery time | Note |
|---|---:|---|
| `primary_flaky_50` | 2386.5 ms | Circuit opened, waited out the 2 s timeout, probe succeeded, closed. |
| `baseline_cached` | 2288.2 ms | Same pattern under normal failure rates. |
| `no_cache` | 2448.6 ms | Slowest probe (no cache to absorb load while half-open). |
| `primary_timeout_100` | none | Correct: the primary never recovers, so it never closes — it stays open and traffic stays on the backup. |
| `both_degraded` | none | Neither circuit ever closed (see below). |

### What `both_degraded` actually proves

Availability 0.000 looks like a failure but is the scenario working as intended.
Primary fails every call; backup fails 60%, so the backup circuit trips after 3
consecutive failures. Once both circuits are open, every request fails fast —
**P95 latency = 0 ms**, not 500 ms of hanging — and returns the static fallback
with the last provider error attached. Because failing fast makes requests
nearly instantaneous, all 100 remaining requests complete well inside the 2 s
reset timeout, so no probe is ever attempted and no circuit recovers within the
run.

That is the correct trade: the system sheds load instead of queueing on a dead
dependency. It also shows the limit of a purely time-based reset — recovery
depends on wall-clock time passing, and under a fast-failing burst no wall-clock
time passes. The Redis run answers the same scenario at 0.830 availability from
warm shared cache, which is the practical mitigation.

### Concurrent load test

Everything above is sequential, which is latency-bound and says nothing about
behaviour under real arrival rates. `python scripts/run_load_test.py` replays
the same scenario at 1, 4, 8 and 16 workers through a `ThreadPoolExecutor`,
with the gateway's providers wrapped in call counters so wasted work is
visible. Full output in `reports/load_test.txt` /
`reports/load_test{,_cached}.json`.

**`primary_timeout_100` — primary dead, 100 requests per run**

| Workers | Wall (s) | Throughput | Availability | P50 | P95 | Cache hits | Circuit opens | Calls to dead primary |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 13.85 | 7.2 req/s | 0.970 | 294.4 | 319.3 | 58 | 6 | 8 |
| 4 | 4.20 | 23.8 req/s | 0.980 | 294.3 | 317.6 | 51 | 2 | 8 |
| 8 | 2.29 | 43.7 req/s | 0.990 | 294.4 | 319.7 | 48 | 1 | 8 |
| 16 | 1.40 | 71.5 req/s | 1.000 | 289.4 | 315.8 | 41 | 1 | 16 |

**`baseline_cached` — default fail rates, cache on**

| Workers | Wall (s) | Throughput | Availability | P50 | P95 | Cache hits | Provider calls |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 11.67 | 8.6 req/s | 0.980 | 222.4 | 306.4 | 59 | 51 |
| 4 | 4.12 | 24.3 req/s | 0.980 | 226.7 | 308.7 | 50 | 67 |
| 8 | 2.23 | 44.8 req/s | 1.000 | 227.1 | 316.1 | 47 | 70 |
| 16 | 1.32 | 75.5 req/s | 1.000 | 218.2 | 304.2 | 44 | 73 |

Three findings, none of them visible in a sequential run:

1. **Throughput scales ~9× and latency does not degrade.** P50 stays at
   ~290 ms and P95 at ~316 ms from 1 to 16 workers. The workload is I/O-bound
   (`time.sleep` in the fake provider releases the GIL, exactly as a real HTTP
   call would), so threads are the right concurrency model here and nothing
   queues.

2. **Cache stampede.** Hits fall from 59 to 44 while provider calls rise from
   51 to 73 — a 43% increase in paid calls for the same 100 requests. Concurrent
   duplicates of the same question all miss a cache that the first response has
   not written yet. Fixing it needs single-flight/request coalescing (an
   in-flight map keyed by query, so duplicates await the first result), which
   this implementation does not have.

3. **Thundering herd on HALF_OPEN.** Calls to the dead primary double from 8 to
   16 at 16 workers. `allow_request()` admits *every* caller that arrives in the
   HALF_OPEN state, so a 16-thread pool sends 16 probes into a provider that is
   known to be sick instead of one. The spec in the lab brief defines HALF_OPEN
   as "allow (probe request)" and the tests grade that behaviour, so it is
   implemented as specified — but a production breaker should admit a bounded
   number of probes (a semaphore of 1, released by the probe's outcome). Note
   also that circuit opens *drop* from 6 to 1 as concurrency rises: the run
   finishes in 1.4 s instead of 13.9 s, so fewer 2-second reset timeouts elapse
   inside it. Recovery behaviour is a function of wall-clock time and therefore
   of load — another argument for the load-aware probing in §9.

Thread safety was a prerequisite for any of this being measurable.
`CircuitBreaker`, `ResponseCache` and the gateway's cost counter now guard every
read-modify-write with a lock, never held across a provider call.
`tests/test_concurrency.py` covers the invariants; the cache test is a real race
reproducer — with the lock removed it loses 96–152 of 720 concurrent writes per
run, because `get()` rebuilds the entry list and any `set()` landing mid-rebuild
is dropped.

---

## 8. Failure analysis

### The weakness: the false-hit guardrail is blind to *missing* numbers

`_looks_like_false_hit()` flags a hit only when **both** strings contain a
4-digit number:

```python
nums_q = set(re.findall(r"\b\d{4}\b", query))
nums_c = set(re.findall(r"\b\d{4}\b", cached_key))
return bool(nums_q and nums_c and nums_q != nums_c)
```

If the cached entry has no year and the incoming query does, `nums_c` is empty,
the `and` short-circuits, and the guardrail stays silent. Reproduced in
`reports/cache_evidence.txt` §[5]:

```
cached : 'Summarize the refund policy for a student who missed the deadline.'
asked  : 'Summarize the refund policy for a student who missed the 2026 deadline.'
score  : 0.960  (threshold 0.92)
served : '[primary] generic refund answer with no deadline year'
false_hit_log entries: 0  <-- guardrail did NOT fire
```

This is not hypothetical: it happened during the real chaos run. The dataset
contains an undated refund-policy query plus 2024 and 2026 variants. The undated
answer was cached first, and both dated variants then scored 0.960 against it and
were served from it — which is exactly why Redis ended the run with 13 keys for
15 cacheable queries. A user asking about the **2026** deadline received an
answer generated for a question with **no deadline at all**, and nothing was
logged. Raising the threshold does not help: 0.960 is above every threshold in
the sweep, and pushing the threshold past 0.96 would disable the cache for
genuine paraphrases too.

### The fix

Treat asymmetric number presence as a mismatch — one word change, from `and` to
a symmetric comparison:

```python
def _looks_like_false_hit(query: str, cached_key: str) -> bool:
    nums_q = set(re.findall(r"\b\d{4}\b", query))
    nums_c = set(re.findall(r"\b\d{4}\b", cached_key))
    return bool((nums_q or nums_c) and nums_q != nums_c)
```

Now `{2026} != set()` is a mismatch and the entry is rejected and logged.

I verified this rather than assuming it. Applying the patch and re-running the
suite gives **39 passed, 7 xpassed** — unchanged — and the blind-spot probe
flips to the safe behaviour:

```
    score  : 0.960  (threshold 0.92)
    served : None
    false_hit_log entries: 1
```

Nothing regresses because the existing tests are unaffected by the new branch:
`test_same_year_not_flagged_as_false_hit` still compares `{2024} == {2024}`, and
the paraphrase tests carry no digits on either side, so `nums_q or nums_c` is
false and the function returns early exactly as before.

I have **not** applied this change to `cache.py`: `_looks_like_false_hit()` is
supplied by the lab skeleton and its docstring defines the contract I was asked
to build against, so silently redefining it would misrepresent what was
implemented versus what was given. It is written up here as the fix I would ship
first.

Two things this incident generalises to, both worth doing before production:

1. **Entity-aware guardrails, not just digits.** Dates, currencies, product
   versions, and user IDs all change the *answer* while barely changing the
   *string*. A guardrail keyed on extracted entities catches "refund policy for
   students" vs "refund policy for staff", which the 4-digit rule cannot see at
   all.
2. **Measure what the cache gets wrong, not just how often it hits.** The
   current metrics report `cache_hit_rate` and `estimated_cost_saved`, both of
   which get *better* as the cache becomes more dangerous. `false_hit_log` is
   collected but never exported to `metrics.json`. A cache with a 76% hit rate
   and an unmeasured false-hit rate is a correctness liability, not a win.

---

## 9. Next steps

1. **Export cache correctness to metrics.** Add `false_hit_count` and a sampled
   `false_hit_log` to `RunMetrics` / `metrics.json`, and define an SLO on it
   (e.g. false-hit rate < 0.1% of hits). Today the guardrail's work is invisible
   to the report it should be driving — the incident in §8 was found by hand.
2. **Share circuit state through Redis.** The cache is shared but the breakers
   are per-process, so with N replicas a dead provider is rediscovered N times
   and takes N × `failure_threshold` failed user requests to shut off. Storing
   `failure_count` under `INCR` + `EXPIRE` and the open state under a TTL key
   would let one replica's discovery protect all of them.
3. **Make recovery load-aware, not purely time-based.** `both_degraded` showed
   that when everything fails fast, no wall-clock time passes and no probe is
   ever attempted; the load test showed the mirror image, where circuit opens
   fall from 6 to 1 simply because 16 workers finish the run in 1.4 s. A
   background prober (or an "after N shed requests, force a probe" rule) would
   decouple recovery from how fast traffic happens to arrive.
4. **Bound HALF_OPEN probes and coalesce duplicate requests.** The two defects
   the load test exposed: 16 workers send 16 probes into a provider known to be
   sick, and concurrent duplicates of one question all miss the cache and each
   pay for their own provider call (+43% calls at 16 workers). A probe semaphore
   fixes the first; a single-flight in-flight map keyed by query fixes the
   second.

### Stretch goals implemented

- **Cost-aware routing** — `ReliabilityGateway` accepts an optional
  `cost_budget`; above 80% of budget it reorders the chain to the cheapest
  provider, and at 100% it serves cache-only and otherwise degrades to the
  static fallback. Off by default (`cost_budget=None`) so the graded run is
  unaffected.
- **SLO table** — §3, evaluated against both cache backends.
- **Extra scenarios** — `baseline_cached`, `no_cache` and `both_degraded` added
  to the three supplied ones, with per-scenario pass criteria in code.
- **Redis-backed comparison run** — `configs/redis.yaml` plus a full parallel
  metrics set, rather than a single spot check.
- **Concurrency** — `run_scenario_concurrent()` / `run_requests(workers=N)` plus
  `scripts/run_load_test.py`, which sweeps 1/4/8/16 workers and reports
  throughput, percentiles, cache hits and per-provider call counts (§7). This
  required making `CircuitBreaker`, `ResponseCache` and the gateway cost counter
  thread-safe, covered by `tests/test_concurrency.py`.

Not attempted: Redis-backed breaker state (listed above as next step 2) and
`hypothesis` property tests.

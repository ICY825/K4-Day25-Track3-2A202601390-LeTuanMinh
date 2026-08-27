"""Evidence for the cache section of the report.

Measures, on the real query set:
  1. Similarity scores for paraphrases vs. unrelated queries, which is what the
     similarity_threshold has to separate.
  2. A threshold sweep showing which thresholds would admit a false hit.
  3. A concrete false-hit example caught by the date/number guardrail.
  4. The privacy guardrail refusing to store sensitive queries.
  5. The one case the guardrail still misses (see report section 8).

Run: python scripts/cache_evidence.py
"""
from __future__ import annotations

from reliability_lab.cache import ResponseCache

PAIRS: list[tuple[str, str, str]] = [
    (
        "identical",
        "Explain circuit breaker states in one paragraph.",
        "Explain circuit breaker states in one paragraph.",
    ),
    (
        "paraphrase (small edit)",
        "Summarize the refund policy",
        "Summarize refund policy",
    ),
    (
        "paraphrase (article added)",
        "Explain circuit breaker states in one paragraph.",
        "Explain the circuit breaker states in one paragraph",
    ),
    (
        "same topic, different intent",
        "Summarize refund policy for 2024 deadline",
        "Summarize refund policy for 2026 deadline",
    ),
    (
        "related topic",
        "circuit breaker pattern",
        "circuit breaker design",
    ),
    (
        "unrelated",
        "Summarize the refund policy for a student who missed the deadline.",
        "What should I do when API calls return 429?",
    ),
]

THRESHOLDS = [0.75, 0.80, 0.85, 0.90, 0.92, 0.95]


def blind_spot() -> None:
    """The guardrail only fires when BOTH strings carry a 4-digit number.

    _looks_like_false_hit() requires nums_q AND nums_c to be non-empty, so an
    undated cached answer will happily serve a dated question. This is the
    weakness documented in section 8 of the final report.
    """
    undated = "Summarize the refund policy for a student who missed the deadline."
    dated = "Summarize the refund policy for a student who missed the 2026 deadline."

    cache = ResponseCache(ttl_seconds=300, similarity_threshold=0.92)
    cache.set(undated, "[primary] generic refund answer with no deadline year")
    value, score = cache.get(dated)
    print("\n[5] Known blind spot: undated cache entry answers a dated question\n")
    print(f"    cached : {undated!r}")
    print(f"    asked  : {dated!r}")
    print(f"    score  : {score:.3f}  (threshold 0.92)")
    print(f"    served : {value!r}")
    print(f"    false_hit_log entries: {len(cache.false_hit_log)}  <-- guardrail did NOT fire")


def main() -> None:
    print("[1] Similarity scores (n-gram cosine over words + character 3-grams)\n")
    print(f"    {'case':<28} {'score':>6}")
    print(f"    {'-' * 28} {'-' * 6}")
    for label, left, right in PAIRS:
        print(f"    {label:<28} {ResponseCache.similarity(left, right):>6.3f}")

    print("\n[2] Threshold sweep on the 2024/2026 pair — would it be served from cache?\n")
    print(f"    {'threshold':>9}  {'above threshold':>15}  {'guardrail verdict':>18}")
    print(f"    {'-' * 9}  {'-' * 15}  {'-' * 18}")
    old_q = "Summarize refund policy for 2024 deadline"
    new_q = "Summarize refund policy for 2026 deadline"
    for threshold in THRESHOLDS:
        cache = ResponseCache(ttl_seconds=300, similarity_threshold=threshold)
        cache.set(old_q, "Old 2024 refund policy")
        value, score = cache.get(new_q)
        above = "yes" if score >= threshold else "no"
        verdict = "blocked (false hit)" if cache.false_hit_log else ("served" if value else "miss")
        print(f"    {threshold:>9.2f}  {above:>15}  {verdict:>18}")

    print("\n[3] False-hit log entry produced by the guardrail\n")
    cache = ResponseCache(ttl_seconds=300, similarity_threshold=0.75)
    cache.set(old_q, "Old 2024 refund policy")
    cache.get(new_q)
    for entry in cache.false_hit_log:
        print(f"    query      = {entry['query']!r}")
        print(f"    cached_key = {entry['cached_key']!r}")
        print(f"    score      = {float(entry['score']):.3f}")
        print(f"    reason     = {entry['reason']!r}")

    print("\n[4] Privacy guardrail — nothing is stored for a sensitive query\n")
    cache = ResponseCache(ttl_seconds=300, similarity_threshold=0.5)
    cache.set("Give me the current account balance for user 123.", "Balance: $500")
    cache.set("password reset for user 456", "Reset link sent")
    cache.set("Explain circuit breaker states in one paragraph.", "[primary] answer")
    print(f"    entries stored after 3 set() calls (2 sensitive, 1 safe): {len(cache._entries)}")
    print(f"    stored keys: {[e.key for e in cache._entries]}")

    blind_spot()


if __name__ == "__main__":
    main()

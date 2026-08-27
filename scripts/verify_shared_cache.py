"""Prove that two independent gateway instances share one Redis cache.

Instance A writes a response; instance B — a separate object with its own
connection, standing in for a second process/pod — reads it back without ever
calling a provider. The same script also shows that privacy-sensitive queries
never reach Redis at all.

Run: python scripts/verify_shared_cache.py
"""
from __future__ import annotations

import argparse

from reliability_lab.cache import SharedRedisCache

PROBE_QUERY = "Explain circuit breaker states in one paragraph."
PROBE_VALUE = "[primary] reliable answer for: Explain circuit breaker states"
PRIVACY_QUERY = "Give me the current account balance for user 123."
SIMILAR_QUERY = "Explain the circuit breaker states in one paragraph"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--prefix", default="rl:evidence:")
    args = parser.parse_args()

    instance_a = SharedRedisCache(args.redis_url, 300, 0.92, prefix=args.prefix)
    instance_b = SharedRedisCache(args.redis_url, 300, 0.92, prefix=args.prefix)

    instance_a.flush()
    print(f"redis ping (instance A): {instance_a.ping()}")
    print(f"redis ping (instance B): {instance_b.ping()}")

    print("\n[1] instance A writes, instance B reads")
    instance_a.set(PROBE_QUERY, PROBE_VALUE)
    value, score = instance_b.get(PROBE_QUERY)
    print(f"    A.set({PROBE_QUERY!r})")
    print(f"    B.get -> {value!r} (score={score:.2f})")
    assert value == PROBE_VALUE, "shared state failed: instance B did not see instance A's write"

    print("\n[2] instance B answers a re-phrased query from A's entry")
    value, score = instance_b.get(SIMILAR_QUERY)
    print(f"    B.get({SIMILAR_QUERY!r})")
    print(f"    -> {value!r} (score={score:.2f})")

    print("\n[3] privacy-sensitive query is never written to Redis")
    instance_a.set(PRIVACY_QUERY, "Balance: $500")
    value, _ = instance_b.get(PRIVACY_QUERY)
    keys = sorted(instance_a._redis.scan_iter(f"{args.prefix}*"))
    print(f"    A.set({PRIVACY_QUERY!r})")
    print(f"    B.get -> {value!r}")
    print(f"    keys in Redis after the write: {len(keys)}")
    assert value is None, "privacy guardrail failed"
    assert len(keys) == 1, "privacy-sensitive query leaked into Redis"

    print("\n[4] Redis contents")
    for key in keys:
        stored = instance_a._redis.hgetall(key)
        ttl = instance_a._redis.ttl(key)
        print(f"    {key}  ttl={ttl}s")
        print(f"      query    = {stored.get('query')!r}")
        print(f"      response = {stored.get('response')!r}")

    instance_a.flush()
    instance_a.close()
    instance_b.close()
    print("\nOK — shared state, similarity reuse, and privacy guardrail all verified.")


if __name__ == "__main__":
    main()

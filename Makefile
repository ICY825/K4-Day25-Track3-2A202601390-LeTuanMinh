.PHONY: test lint typecheck run-chaos run-chaos-redis report verify clean docker-up docker-down

test:
	pytest -q

lint:
	ruff check src tests scripts

typecheck:
	mypy src

run-chaos:
	python scripts/run_chaos.py --config configs/default.yaml --out reports/metrics.json --seed 42

run-chaos-redis:
	python scripts/run_chaos.py --config configs/redis.yaml --out reports/metrics_redis.json --csv reports/metrics_redis.csv --per-scenario reports/metrics_redis_by_scenario.json --seed 42

verify:
	python scripts/verify_report.py

report:
	python scripts/generate_report.py --metrics reports/metrics.json --out reports/metrics_summary.md

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	rm -rf .pytest_cache .ruff_cache .mypy_cache reports/metrics.json reports/metrics_summary.md

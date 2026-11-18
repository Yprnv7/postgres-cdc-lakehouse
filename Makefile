.PHONY: up down test lint bench

up:
	docker compose up --build -d

down:
	docker compose down -v

lint:
	ruff check consumer/ generator_app/ tests/ scripts/
	mypy consumer/

test:
	pytest tests/ -v --cov=consumer --cov-report=term-missing

bench:
	python scripts/run_benchmarks.py --output docs/BENCHMARKS.md

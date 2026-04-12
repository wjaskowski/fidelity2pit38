#!/bin/sh
set -e

uv run ruff check src/ tests/
uv run pytest -q

uv run fidelity2pit38 --data-dir data-sample --year 2024 --output output-sample --no-open > /dev/null 2>&1
uv run fidelity2pit38 --data-dir data-sample --year 2025 --output output-sample --no-open > /dev/null 2>&1
git add output-sample/

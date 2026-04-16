#!/bin/bash
set -e
DIR="$(cd "$(dirname "$0")/.." && pwd)"
"$DIR/.venv/bin/python" -m black --check --line-length 120 src/ tests/ apps/
"$DIR/.venv/bin/python" -m flake8 src/ tests/ apps/ notebooks/
"$DIR/.venv/bin/python" -m mypy src/

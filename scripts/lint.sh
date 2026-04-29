#!/bin/bash
set -e
DIR="$(cd "$(dirname "$0")/.." && pwd)"
"$DIR/.venv/bin/python" -m ruff check src/ tests/ apps/ notebooks/ examples/
"$DIR/.venv/bin/python" -m ruff format --check src/ tests/ apps/

#!/bin/bash
set -e
DIR="$(cd "$(dirname "$0")/.." && pwd)"
"$DIR/.venv/bin/python" -m pytest tests/ -m 'not integration' --cov=src --cov-report=term-missing --cov-fail-under=80

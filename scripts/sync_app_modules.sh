#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="$ROOT/apps/column_mapping_review_app/review_store.py"

{
    echo "# generated, edit src/harmonization/review_store.py"
    cat "$ROOT/src/harmonization/review_store.py"
} > "$TARGET"

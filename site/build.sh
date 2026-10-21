#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/site/bench"
cp "$ROOT/bench/results.json" "$ROOT/site/bench/results.json"
touch "$ROOT/site/.nojekyll"
printf 'Static site ready: %s\n' "$ROOT/site/index.html"

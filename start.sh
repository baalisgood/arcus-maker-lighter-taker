#!/usr/bin/env bash
set -euo pipefail
umask 077
project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$project_dir"
task_python="${ARBITRAGE_PYTHON:-$project_dir/.venv/bin/python}"
mode="${1:-paper}"
if (($#)); then shift; fi
exec "$task_python" "$project_dir/run_maker_taker.py" --mode "$mode" "$@"

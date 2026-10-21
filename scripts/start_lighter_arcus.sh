#!/usr/bin/env bash
# One process handles all configured pairs. Default startup is read-only.
set -euo pipefail
umask 077
mode="${1:-monitor}"
case "$mode" in monitor|live) ;; *) echo 'Usage: bash scripts/start_lighter_arcus.sh [monitor|live]' >&2; exit 2 ;; esac
project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_dir"
task_python="${ARBITRAGE_PYTHON:-$project_dir/.venv/bin/python}"
profile='config/arbitrage/arbitrage_taker_taker.yaml'
market_profile='config/arbitrage/monitor_lighter_arcus.yaml'
test -x "$task_python"
test -f "$profile"
test -f "$market_profile"
mkdir -p run
exec 9>"${ARBITRAGE_LOCK_FILE:-$project_dir/run/lighter-arcus.lock}"
flock -n 9 || { echo 'Lighter–Arcus process already running; stop it before switching mode.' >&2; exit 1; }
runtime_profile="$(mktemp "$project_dir/run/lighter-arcus.XXXXXX.yaml")"
trap 'rm -f "$runtime_profile"' EXIT
"$task_python" - "$profile" "$runtime_profile" "$mode" <<'PY'
import sys, yaml
from pathlib import Path
source, target, mode = sys.argv[1:]
config = yaml.safe_load(Path(source).read_text())
config['system_mode']['monitor_only'] = mode != 'live'
Path(target).write_text(yaml.safe_dump(config, allow_unicode=True, sort_keys=False))
print('Lighter–Arcus mode:', mode, flush=True)
PY
"$task_python" main_unified.py \
  --config "$runtime_profile" --monitor-config "$market_profile" \
  --execution-config config/arbitrage/arbitrage_lighter_arcus.yaml

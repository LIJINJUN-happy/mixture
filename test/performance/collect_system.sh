#!/usr/bin/env bash
# Linux只读系统采集器：不发送业务请求，不重启进程，不读取进程命令行或环境变量。
# 用法：PERF_DURATION_SECONDS=1800 bash collect_system.sh OUTPUT_DIR GATE_PID ACTOR_PID ...
set -euo pipefail

if [[ $# -lt 2 || ! -r /proc/stat ]]; then
  echo 'Usage on Linux: PERF_DURATION_SECONDS=1800 PERF_INTERVAL_SECONDS=2 bash collect_system.sh OUTPUT_DIR PID [PID ...]' >&2
  exit 2
fi
output_dir=$1
shift
duration=${PERF_DURATION_SECONDS:-1800}
interval=${PERF_INTERVAL_SECONDS:-2}
if [[ ! $duration =~ ^[1-9][0-9]*$ || ! $interval =~ ^[1-9][0-9]*$ ]]; then
  echo 'Duration and interval must be positive integer seconds.' >&2
  exit 2
fi
for process_id in "$@"; do
  if [[ ! $process_id =~ ^[1-9][0-9]*$ ]]; then
    echo 'Each PID must be a positive integer.' >&2
    exit 2
  fi
done
mkdir -p -- "$output_dir"
output_file="$output_dir/system-$(date -u +%Y%m%dT%H%M%SZ)-$$.log"
exec >>"$output_file" 2>&1
echo "BEGIN $(date -u +%FT%TZ) duration=$duration interval=$interval pids=$*"
uname -a
printf 'clock_ticks='; getconf CLK_TCK
printf 'page_size='; getconf PAGESIZE
printf 'open_files_limit='; ulimit -n
if command -v ip >/dev/null 2>&1; then ip -s link || true; fi

end_at=$((SECONDS + duration))
while (( SECONDS < end_at )); do
  echo "SAMPLE $(date -u +%FT%TZ)"
  for source_file in /proc/stat /proc/meminfo /proc/loadavg /proc/net/dev /proc/net/snmp /proc/net/netstat; do
    echo "FILE $source_file"
    cat "$source_file" || true
  done
  for process_id in "$@"; do
    echo "PROCESS $process_id"
    if [[ ! -d /proc/$process_id ]]; then
      echo 'process no longer exists; obtain its new PID after restart'
      continue
    fi
    for source_file in stat status io; do
      echo "FILE /proc/$process_id/$source_file"
      cat "/proc/$process_id/$source_file" || true
    done
  done
  if command -v ss >/dev/null 2>&1; then ss -s || true; fi
  remaining=$((end_at - SECONDS))
  if (( remaining <= 0 )); then break; fi
  sleep_for=$interval
  if (( remaining < sleep_for )); then sleep_for=$remaining; fi
  sleep "$sleep_for"
done
echo "END $(date -u +%FT%TZ)"

#!/usr/bin/env bash
# watch.sh — one screen for in-flight agentic runs: live jail containers, the newest run bundles with their
# RESULT lines, and the tail of a log (default: the newest agentic/runs/*.log). Refreshes every 10 s; Ctrl-C to quit.
# For a browser view of a whole sweep (a cell grid per profile and operator, vs an earlier sweep): agentic/sweep_dash.py.
#   agentic/watch.sh                 # newest sweep/smoke log
#   agentic/watch.sh /path/to/log    # a specific log
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG="${1:-$(ls -t "$REPO"/agentic/runs/*.log 2>/dev/null | head -1)}"
while true; do
  clear
  echo "== $(date +%T)  jail containers =="
  docker ps --filter ancestor=hpc-bridge-agentic --format '  {{.ID}}  up {{.RunningFor}}  {{.Command}}' 2>/dev/null | sed 's/ ago//' || echo "  (docker not running)"
  echo; echo "== newest run bundles (agentic/runs) =="
  for d in $(ls -td "$REPO"/agentic/runs/*/ 2>/dev/null | head -8); do
    r=$(python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); print(f"{r.get(\"result\") or \"?\"}  rc={r.get(\"rc\")}  failed={r.get(\"failed\") or []}")' "$d/record.json" 2>/dev/null)
    printf '  %-45s %s\n' "$(basename "$d")" "${r:-(running / no record yet)}"
  done
  echo; echo "== log: ${LOG:-none} =="
  [ -n "$LOG" ] && [ -f "$LOG" ] && tail -n 18 "$LOG" | cut -c1-160
  sleep 10
done

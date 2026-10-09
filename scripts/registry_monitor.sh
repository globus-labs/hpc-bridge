#!/usr/bin/env bash
# registry_monitor.sh — run the zero-cost registry health check on a schedule (macOS launchd), alerting on change.
#
#   scripts/registry_monitor.sh install [MINUTES]   # every MINUTES (default 60); a desktop notification on new findings
#   scripts/registry_monitor.sh status              # is it loaded; the last run's verdict
#   scripts/registry_monitor.sh run                 # one run now, in this terminal
#   scripts/registry_monitor.sh uninstall
#
# What runs: `hpc-bridge-registry-health --state ~/.hpc-bridge/registry-health.json --notify`, built by uvx from
# GitHub main (REF) — so the seeds it holds the live index to are main's, whatever branch this checkout is on — with
# the Globus login in the SDK's default store (~/.globus_compute: facility metadata needs an identity; the index is
# read anonymously). It submits nothing and costs nothing. The paid tier — a real block per facility — is
# agentic/registry_reprove.py, run by hand or on a slower cadence.
set -euo pipefail

LABEL="org.globus-labs.hpc-bridge.registry-health"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
STATE="$HOME/.hpc-bridge/registry-health.json"
LOG="$HOME/.hpc-bridge/registry-health.log"
UV="$(command -v uv || true)"
UVX="$(command -v uvx || true)"
REF="${REF:-main}"
SRC="git+https://github.com/globus-labs/hpc-bridge@${REF}"

run_once() {
  mkdir -p "$(dirname "$STATE")"
  "$UVX" -q --refresh --from "$SRC" hpc-bridge-registry-health --state "$STATE" --notify
}

case "${1:-status}" in
  run)
    [[ -n "$UVX" ]] || { echo "uvx is not on PATH" >&2; exit 1; }
    run_once
    ;;
  install)
    [[ "$(uname)" == "Darwin" ]] || { echo "launchd is macOS-only; on Linux put '$0 run' in cron" >&2; exit 1; }
    [[ -n "$UVX" && -n "$UV" ]] || { echo "uv/uvx are not on PATH" >&2; exit 1; }
    minutes="${2:-60}"
    mkdir -p "$(dirname "$PLIST")" "$(dirname "$LOG")"
    cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key><array>
    <string>$UVX</string><string>-q</string><string>--refresh</string><string>--from</string><string>$SRC</string>
    <string>hpc-bridge-registry-health</string><string>--state</string><string>$STATE</string><string>--notify</string>
  </array>
  <key>StartInterval</key><integer>$((minutes * 60))</integer>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>$LOG</string>
  <key>StandardErrorPath</key><string>$LOG</string>
  <key>EnvironmentVariables</key><dict>
    <key>HOME</key><string>$HOME</string>
    <key>PATH</key><string>$(dirname "$UV"):/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin</string>
  </dict>
</dict></plist>
EOF
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$PLIST"
    echo "installed: every $minutes min; log $LOG; state $STATE"
    ;;
  uninstall)
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
    rm -f "$PLIST"
    echo "uninstalled"
    ;;
  status)
    if launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1; then echo "loaded ($PLIST)"; else echo "not loaded"; fi
    if [[ -f "$STATE" ]]; then
      python3 - "$STATE" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
bad = [f for f in s.get("findings", []) if f["level"] != "ok"]
print(f"last run {s.get('at')}: {len(bad)} finding(s) not ok")
for f in bad:
    print(f"  {f['level'].upper():5} {f['entry']:<12} {f['check']:<9} {f['detail'][:140]}")
PY
    else
      echo "no run recorded yet ($STATE)"
    fi
    ;;
  *) echo "usage: $0 install [MINUTES] | uninstall | status | run" >&2; exit 2 ;;
esac

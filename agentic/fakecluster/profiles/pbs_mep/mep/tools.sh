#!/usr/bin/env bash
# The MEP tooling on the `mep-tools` volume: mep_tools_install <version> puts globus-compute-endpoint==<version> in
# $MEP_GCE (a uv venv on python 3.11 — the jail's python, so dill'd tasks deserialise) with the managed python beside
# it (readable by every account: the mapped user execs it). Idempotent; a version change reinstalls.
MEP_TOOLS=/opt/hpcb-mep
MEP_GCE="$MEP_TOOLS/gce"
export UV_PYTHON_INSTALL_DIR="$MEP_TOOLS/uv-python"
mep_tools_install() {
  local ver="$1" have
  have="$("$MEP_GCE/bin/python" -c 'import importlib.metadata as m; print(m.version("globus-compute-endpoint"))' 2>/dev/null || true)"
  [ "$have" = "$ver" ] && return 0
  echo "[mep] installing globus-compute-endpoint==$ver into $MEP_GCE (had: ${have:-none})"
  mkdir -p "$MEP_TOOLS"
  # --clear: our own tools venv is being (re)built; uv 0.8+ refuses to replace an existing directory without it
  uv venv -q --clear --python 3.11 "$MEP_GCE" && uv pip install -q --python "$MEP_GCE/bin/python" "globus-compute-endpoint==$ver" || return 1
  chmod -R a+rX "$MEP_TOOLS" 2>/dev/null || true
}

# mep_clear_stale_uep_pids <mapped user's home> <manager endpoint dir>... — run by the login setup at container START,
# before any manager starts: removes the `daemon.pid` that each of these managers' user endpoints left in
# `<home>/.globus_compute/uep.<manager uuid>.<config hash>/`. A user endpoint is its manager's child in this
# container, so it died when the container last went down (bin/up.sh's `docker compose up --build` recreates the
# stack between run_suite runs), but its pid file stayed on the shared /home. globus-compute-endpoint then refuses to
# start that user endpoint until the file is 3 heartbeats old ("Another instance of this endpoint is running", rc 73),
# and every fake-MEP cell maps to the same user endpoint: in the 2026-10-16 sweep a cell's first task found it.
# Removes only pid files, only under <home>, only of the uuids these managers registered (no endpoint.json: it has
# forked nothing), and never one whose user endpoint is running here: a LIVE user endpoint re-touches its pid file
# every 15 s and shuts itself down when the file is gone. Without pgrep to rule that out, every file is kept.
mep_clear_stale_uep_pids() {
  local home="$1" d eid f uep n=0
  shift
  for d in "$@"; do
    [ -f "$d/endpoint.json" ] || continue
    eid="$(sed -n 's/.*"endpoint_id": *"\([0-9a-f-]*\)".*/\1/p' "$d/endpoint.json" 2>/dev/null | head -n 1)" || eid=""
    [ -n "$eid" ] || continue
    for f in "$home/.globus_compute/uep.$eid."*/daemon.pid; do
      [ -f "$f" ] || continue
      uep="$(basename "$(dirname "$f")")"
      if ! command -v pgrep >/dev/null 2>&1; then
        echo "[mep] kept $f: no pgrep here to rule out a live user endpoint"
      elif pgrep -f "$uep" >/dev/null 2>&1; then
        echo "[mep] kept $f: $uep is running here"
      elif rm -f "$f"; then
        n=$((n + 1)); echo "[mep] removed $f (its user endpoint died with this container's last run)"
      fi
    done
  done
  echo "[mep] stale user-endpoint pid files removed: $n"
}

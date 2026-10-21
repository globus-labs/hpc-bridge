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
# every 15 s and shuts itself down when the file is gone. So a file goes only when pgrep says NO process matches
# (exit 1); a match (0), a pgrep error (2, 3) or no pgrep at all keeps it.
# Root runs this over a directory the mapped user owns (and the agent runs as that user), so it follows no symlink: a
# symlinked ~/.globus_compute, uep dir or daemon.pid is kept, only the names a manager writes are taken
# (uep.<uuid>.<lowercase hex and dashes> — so the pgrep pattern holds no metacharacter but the escaped dots), and the
# unlink runs inside the uep dir once it is checked to be that real path: all it can remove is an entry named
# daemon.pid in a directory under <home>/.globus_compute.
mep_clear_stale_uep_pids() {
  local home="$1" gc="" d eid f uep pat rc n=0
  shift
  if [ -L "$home/.globus_compute" ]; then
    echo "[mep] kept everything under $home/.globus_compute: a symlink, never followed"
  elif [ -d "$home/.globus_compute" ]; then
    gc="$(cd -P "$home/.globus_compute" 2>/dev/null && pwd)" || gc=""
  fi
  for d in "$@"; do
    [ -n "$gc" ] && [ -f "$d/endpoint.json" ] || continue
    eid="$(sed -n 's/.*"endpoint_id": *"\([0-9a-f-]*\)".*/\1/p' "$d/endpoint.json" 2>/dev/null | head -n 1)" || eid=""
    [ -n "$eid" ] || continue
    for f in "$gc/uep.$eid."*/daemon.pid; do
      uep="${f%/daemon.pid}"; uep="${uep##*/}"
      case "${uep#uep.$eid.}" in ''|*[!0-9a-f-]*) continue ;; esac   # not a name a manager writes
      if [ -L "$gc/$uep" ] || [ -L "$f" ]; then echo "[mep] kept $f: a symlink, never followed"; continue; fi
      [ -f "$f" ] || continue
      if ! command -v pgrep >/dev/null 2>&1; then
        echo "[mep] kept $f: no pgrep here to rule out a live user endpoint"; continue
      fi
      pat="$(printf '%s' "$uep" | sed 's/\./\\./g')"   # the dots literal: the rest is [0-9a-f-]
      rc=0; pgrep -f "$pat" >/dev/null 2>&1 || rc=$?
      if [ "$rc" = 0 ]; then
        echo "[mep] kept $f: $uep is running here"
      elif [ "$rc" != 1 ]; then
        echo "[mep] kept $f: pgrep failed (rc $rc), so a live $uep cannot be ruled out"
      elif ( cd -P "$gc/$uep" 2>/dev/null && [ "$(pwd)" = "$gc/$uep" ] && [ ! -L daemon.pid ] && [ -f daemon.pid ] \
               && rm -f daemon.pid ); then
        n=$((n + 1)); echo "[mep] removed $f (its user endpoint died with this container's last run)"
      else
        echo "[mep] kept $f: it changed while being checked"
      fi
    done
  done
  echo "[mep] stale user-endpoint pid files removed: $n"
}

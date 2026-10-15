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

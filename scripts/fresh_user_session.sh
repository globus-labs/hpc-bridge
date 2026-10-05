#!/usr/bin/env bash
# fresh_user_session.sh — launch Claude Code with hpc-bridge loaded as a plugin, AS A FRESH USER:
# no Globus tokens, no cached facilities/endpoints, no repo-local settings. Nothing of yours is
# touched: the Compute SDK and hpc-bridge are pointed at scratch dirs under $FRESH (default
# ~/hpcb-fresh), and the session is launched from there so the repo's project-level .mcp.json and
# .claude/settings.local.json do not apply — the plugin is the only hpc-bridge in the session,
# exactly like an installed one.
#
#   scripts/fresh_user_session.sh           # 1st run: expect needs_login (browser), then the MEP attach
#   scripts/fresh_user_session.sh           # 2nd run, same dirs: NO login (refresh tokens) -> straight to attach
#   scripts/fresh_user_session.sh --reset   # wipe the scratch dirs first -> a brand-new user again
#   scripts/fresh_user_session.sh --marketplace   # the BETA USER's path: a fresh Claude Code config dir with the
#                                                 # plugin installed from the GitHub marketplace (main), not this checkout.
#                                                 # Claude Code asks you to log in once in that config dir.
#
# In the session, say:  connect me to globus1                              (zero-SSH facility endpoint)
#                  or:  connect me to the cluster at globus1.cs.uchicago.edu   (SSH bootstrap as you, BYO path)
#
# DRY_RUN=1 prints the launch command instead of running it.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRESH="${FRESH:-$HOME/hpcb-fresh}"
INDEX="${REGISTRY:-}"   # a stranger has no config: the plugin's built-in registry must work on its own. REGISTRY=<uuid> overrides;
                        # the maintainer's shell HPC_BRIDGE_SEARCH_INDEX is deliberately STRIPPED below (it must not leak in)

if [[ "${1:-}" == "--reset" ]]; then
  rm -rf "$FRESH"
  echo "reset: removed $FRESH"
  shift
fi
MODE=plugin-dir
if [[ "${1:-}" == "--marketplace" ]]; then MODE=marketplace; shift; fi
mkdir -p "$FRESH/globus_compute" "$FRESH/state"

command -v claude >/dev/null || { echo "error: 'claude' is not on PATH" >&2; exit 1; }
command -v uv >/dev/null || { echo "error: 'uv' is not on PATH (the plugin launches the server with 'uv run')" >&2; exit 1; }
[[ -f "$REPO/.mcp.json" && -f "$REPO/.claude-plugin/plugin.json" ]] || { echo "error: $REPO is not an hpc-bridge checkout" >&2; exit 1; }

if [[ "$MODE" == "marketplace" ]]; then
  export CLAUDE_CONFIG_DIR="$FRESH/claude-config"
  mkdir -p "$CLAUDE_CONFIG_DIR"
  if ! claude plugin list 2>/dev/null | grep -q "hpc-bridge@hpc-bridge"; then
    echo "installing the plugin from the marketplace into $CLAUDE_CONFIG_DIR …"
    claude plugin marketplace add globus-labs/hpc-bridge
    claude plugin install hpc-bridge@hpc-bridge
  fi
  echo "plugin:      marketplace install (GitHub main) in a fresh Claude Code config: $CLAUDE_CONFIG_DIR"
  echo "             (Claude Code will ask you to log in once in this config dir — your normal config is untouched)"
else
  echo "repo:        $REPO  (branch: $(git -C "$REPO" branch --show-current 2>/dev/null || echo '?'))"
fi
echo "registry:    ${INDEX:-(plugin default — no config, as a stranger would have)}"
echo "compute sdk: $FRESH/globus_compute   (tokens land here, not in ~/.globus_compute)"
echo "hpc-bridge:  $FRESH/state            (no cached facilities/endpoints)"
if [[ -f "$FRESH/globus_compute/storage.db" ]]; then
  echo "tokens:      PRESENT -> expect NO login this run (use --reset to be a brand-new user again)"
else
  echo "tokens:      none    -> expect a browser to open on Globus; the tool waits (~90 s) for the login and continues by itself"
  echo "             (a browser that already holds a Globus session + this client's consent lands in seconds)"
fi
echo
echo ">>> in the session, say:  connect me to globus1"
echo

# Claude Code's AUTO-MEMORY persists across sessions in a config dir: a previous run's note ("connect via the
# alias; config confirmed") let the next run register the facility WITHOUT asking (seen live 2026-09-04). A fresh
# user has no memory — wipe it for this scratch project in both config dirs unless KEEP_MEMORY=1.
if [[ -z "${KEEP_MEMORY:-}" ]]; then
  PROJ_KEY="$(printf '%s' "$FRESH" | sed 's#/#-#g')"
  rm -rf "$FRESH/claude-config/projects/$PROJ_KEY/memory" "$HOME/.claude/projects/$PROJ_KEY/memory"
  echo "auto-memory: wiped for this scratch project (KEEP_MEMORY=1 keeps it)"
fi

# Stray hpc-bridge overrides from this shell must not leak in (an endpoint id would even be refused for a MEP).
# Strip EVERY hpc-bridge variable from this shell (a leaked HPC_BRIDGE_SSH_HOST would probe the maintainer's
# host instead of asking a stranger for one — review 2), then set only the scratch dirs.
STRIP=(); for v in $(env | grep -oE '^HPC_BRIDGE_[A-Z_]+'); do STRIP+=(-u "$v"); done
# ${STRIP[@]+"${STRIP[@]}"}: bash 3.2 (macOS) treats an EMPTY array as unbound under `set -u` (seen live 2026-09-04)
CMD=(env ${STRIP[@]+"${STRIP[@]}"}
     GLOBUS_COMPUTE_USER_DIR="$FRESH/globus_compute"
     HPC_BRIDGE_STATE_DIR="$FRESH/state"
     ${INDEX:+HPC_BRIDGE_SEARCH_INDEX="$INDEX"}
     ${CLAUDE_CONFIG_DIR:+CLAUDE_CONFIG_DIR="$CLAUDE_CONFIG_DIR"}
     claude "$@")
if [[ "$MODE" == "plugin-dir" ]]; then CMD+=(--plugin-dir "$REPO"); fi

cd "$FRESH"
if [[ -n "${DRY_RUN:-}" ]]; then
  printf 'would run (from %s):\n  ' "$FRESH"; printf '%q ' "${CMD[@]}"; echo
  exit 0
fi
exec "${CMD[@]}"

#!/usr/bin/env bash
# Build the jail image + run ONE agentic scenario against globus1 with SCOPED creds.
# The admin key (~/.ssh/globus) is NEVER passed into the container.
#
#   CLAUDE_CODE_OAUTH_TOKEN=sk-ant-oat01-... ./agentic/run_smoke.sh [scenario]
#
# Auth — ONE of (subscription preferred; far cheaper than API credits):
#   CLAUDE_CODE_OAUTH_TOKEN   from `claude setup-token` (needs Pro/Max) — PREFERRED
#   ANTHROPIC_API_KEY         API credits — fallback
# Scoped credentials (env):
#   HPCB_TEST_SSH_KEY   (default ~/.ssh/hpcbridge-test)   scoped test PRIVATE key
#   HPCB_TEST_SSH_USER  (default hpcbridge-test)          the non-admin cluster user
#   HPCB_TEST_GLOBUS_DB (optional)                        a Globus storage.db (test identity,
#                                                          or your own for a throwaway smoke)
set -euo pipefail

SCENARIO="${1:-happy_path}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Persisted secrets: agentic/.env (gitignored + dockerignored, chmod 600; plain KEY=value).
# Set CLAUDE_CODE_OAUTH_TOKEN / HPCB_TEST_GLOBUS_DB there ONCE instead of exporting per shell.
# PRECEDENCE: the caller's environment WINS — .env only fills in unset vars. (Sourcing it
# blindly let a persisted HPCB_TEST_SSH_USER/HPCB_MODEL silently clobber run_suite.py's
# per-job values — same cluster user across parallel runs, mislabelled matrix cells.)
ENV_FILE="$REPO_ROOT/agentic/.env"
if [ -f "$ENV_FILE" ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in ''|\#*) continue ;; esac
    k="${line%%=*}"
    # per-cell knobs are run_suite's to set: a persisted HPCB_NO_SKILL/HPCB_EFFORT/... must never fill a cell
    case "$k" in HPCB_MODEL|HPCB_EFFORT|HPCB_PERSONA|HPCB_NO_SKILL|HPCB_RUNID) continue ;; esac
    if [ -z "${!k+x}" ]; then export "$line"; fi
  done < "$ENV_FILE"
fi

# Which harness drives hpc-bridge: `claude` (default), `hermes` (an ALCF-hosted model, no Anthropic auth), or
# `claude-acp` (Claude Code over ACP via Zed's adapter — the subscription token, like `claude`).
OPERATOR="${HPCB_OPERATOR:-claude}"
AUTH_ARGS=()
if [ "$OPERATOR" = "hermes" ]; then
  # hermes drives the scenario with an ALCF-hosted model — no Claude/Anthropic auth needed. Mint the ALCF
  # inference token ON THE HOST (a short-lived bearer minted from the maintainer's Globus login — never the
  # Globus creds themselves) and pass only that in. Base URL + model default to Sophia/gpt-oss-120b.
  echo "operator: hermes (ALCF-hosted model) — no Anthropic auth needed"
  ALCF_INFERENCE_TOKEN="${ALCF_INFERENCE_TOKEN:-$(uv run --directory "$REPO_ROOT" --extra integration \
    python "$REPO_ROOT/agentic/harness/inference_auth_token.py" get_access_token 2>/dev/null || true)}"
  [ -n "$ALCF_INFERENCE_TOKEN" ] || { echo "ERROR: could not mint an ALCF inference token — run once: uv run --extra integration python agentic/harness/inference_auth_token.py authenticate"; exit 1; }
  HPCB_ALCF_BASE_URL="${HPCB_ALCF_BASE_URL:-https://inference-api.alcf.anl.gov/resource_server/sophia/vllm/v1}"
  HPCB_ALCF_MODEL="${HPCB_ALCF_MODEL:-openai/gpt-oss-120b}"
  export ALCF_INFERENCE_TOKEN HPCB_ALCF_BASE_URL HPCB_ALCF_MODEL
  # The operator is hermes/ALCF; but INTERACTIVE scenarios also need the human-sim (the simulated USER), which runs
  # on Claude via our own SDK harness — forward its token too (autonomous scenarios ignore it). Empty ANTHROPIC_API_KEY
  # blocks the precedence trap. hermes_runner scrubs this token from the hermes child; only run.py's human-sim sees it.
  if [ -n "${CLAUDE_CODE_OAUTH_TOKEN:-}" ]; then
    AUTH_ARGS=( -e CLAUDE_CODE_OAUTH_TOKEN -e ANTHROPIC_API_KEY= )
    echo "human-sim: Claude subscription token forwarded (interactive personas)"
  else
    echo "note: CLAUDE_CODE_OAUTH_TOKEN unset — interactive (persona) scenarios will fail (the human-sim needs it); autonomous scenarios are fine"
  fi
# Prefer the Claude subscription token; fall back to an API key. PRECEDENCE TRAP:
# ANTHROPIC_API_KEY silently wins over CLAUDE_CODE_OAUTH_TOKEN — so when using the
# subscription we pass an EMPTY ANTHROPIC_API_KEY into the container to block it.
elif [ -n "${CLAUDE_CODE_OAUTH_TOKEN:-}" ]; then
  echo "auth: Claude subscription (CLAUDE_CODE_OAUTH_TOKEN)"
  AUTH_ARGS=( -e CLAUDE_CODE_OAUTH_TOKEN -e ANTHROPIC_API_KEY= )
elif [ -n "${ANTHROPIC_API_KEY:-}" ]; then
  echo "auth: API key (ANTHROPIC_API_KEY) — billed as API credits"
  AUTH_ARGS=( -e ANTHROPIC_API_KEY )
else
  echo "ERROR: set CLAUDE_CODE_OAUTH_TOKEN ('claude setup-token', needs Pro/Max) or ANTHROPIC_API_KEY"; exit 1
fi

# The cluster this run targets (harness/targets.py): globus1 (default) or fake (agentic/fakecluster). One preset
# carries the jail-side ssh host, the docker network, the pool key default and the endpoint-name prefix together.
TARGET="${HPCB_TARGET:-globus1}"
eval "$(python3 "$REPO_ROOT/agentic/harness/targets.py" "$TARGET")" || { echo "ERROR: unknown HPCB_TARGET '$TARGET'"; exit 1; }
KEY="${HPCB_TEST_SSH_KEY:-$HPCB_T_KEY_DEFAULT}"
SSH_USER="${HPCB_TEST_SSH_USER:-hpcbridge-test}"
GLOBUS_DB="${HPCB_TEST_GLOBUS_DB:-}"
# Per-scenario HOST knobs (read from the scenario module): run with NO Globus store (a logged-out
# stranger: needs_login), or with an ALTERNATE store (a second identity — e.g. one the MEP does not map).
KNOBS="$(python3 "$REPO_ROOT/agentic/harness/scenario_knobs.py" "$SCENARIO")" || { echo "ERROR: scenario knobs could not be read for '$SCENARIO' (unknown scenario?) — refusing to guess what to mount"; exit 1; }
eval "$KNOBS"
if [ -n "${HPCB_KNOB_GLOBUS_DB_SECRET:-}" ]; then
  GLOBUS_DB="${!HPCB_KNOB_GLOBUS_DB_SECRET:-}"
  [ -n "$GLOBUS_DB" ] || { echo "ERROR: scenario '$SCENARIO' needs \$$HPCB_KNOB_GLOBUS_DB_SECRET (path to a storage.db) — set it in agentic/.env"; exit 1; }
  echo "globus store: from \$$HPCB_KNOB_GLOBUS_DB_SECRET (an alternate identity)"
fi
if [ -n "${HPCB_KNOB_NO_GLOBUS_DB:-}" ]; then
  GLOBUS_DB=""
  echo "globus store: NONE — the scenario runs logged out (expects needs_login)"
fi
RUNID="${HPCB_RUNID:-$(date +%s)-$$}"   # run_suite mints it (so it can clean up an abandoned cell); solo runs mint their own
USER_DIR="/home/agent/run/$RUNID"   # agent-writable (the entrypoint mkdir's it + stages the db)
RUNS_HOST="$REPO_ROOT/agentic/runs" # per-run provenance bundles land here (gitignored)
mkdir -p "$RUNS_HOST"
GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"          # the HOST's head at launch
GIT_DESCRIBE="$(git -C "$REPO_ROOT" describe --always --dirty --abbrev=12 2>/dev/null || echo unknown)"  # what the image is built from

[ -f "$KEY" ] || { echo "missing scoped test key: $KEY  (globus1: generate one + register its .pub; fake: agentic/fakecluster/bin/up.sh creates it)"; exit 1; }

if [ -z "${HPCB_SKIP_BUILD:-}" ]; then   # the suite runner builds once, then sets this
  echo "building jail image (hpc-bridge-agentic)…"
  docker build --provenance=false -t hpc-bridge-agentic --build-arg "GIT_DESCRIBE=$GIT_DESCRIBE" \
    -f "$REPO_ROOT/agentic/Dockerfile" "$REPO_ROOT" >/dev/null
fi
IMAGE_ID="$(docker image inspect -f '{{.Id}}' hpc-bridge-agentic 2>/dev/null || echo unknown)"   # pins the code the cell ran

ARGS=(
  --rm
  --stop-timeout 120                  # `docker stop`: give run.py's teardown (SIGTERM -> its finally) time to finish
  ${AUTH_ARGS[@]+"${AUTH_ARGS[@]}"}    # empty for the hermes operator (no Anthropic auth); safe under set -u (bash 3.2)
  -e HPCB_RUNID="$RUNID"
  -e HPCB_IMAGE_ID="$IMAGE_ID"
  -e HPC_BRIDGE_SSH_USER="$SSH_USER"
  -e HPC_BRIDGE_SSH_KEY=/run/secrets/test_key
  -e HPC_BRIDGE_SSH_HOST="$HPCB_T_SSH_HOST"        # the login host as the JAIL reaches it (globus1 FQDN; `login` on the fake cluster's network)
  -e HPC_BRIDGE_ENDPOINT_NAME="$HPCB_T_EP_PREFIX-$RUNID"   # per-run isolation: runs share ONE Globus identity, so a unique NAME keeps their registrations distinct; the prefix names the TARGET
  -e HPCB_TARGET="$TARGET"
  -e HPCB_TARGET_NODES="$HPCB_T_NODES"
  -e HPCB_FAKE_PROFILE="${HPCB_T_PROFILE:-}"      # fake: the active cluster profile (recorded in the bundle)
  -e HPCB_TARGET_CAPS="$HPCB_T_CAPS_JSON"          # the cluster's capabilities (scenario REQUIRES; postchecks on each login node)
  -e HPCB_HARNESS_SSH_PORT="${HPCB_T_HARNESS_SSH_PORT:-22}"   # the harness' world-channel sshd (an MFA profile runs a key-only one beside port 22)
  -e HPC_BRIDGE_USER_DIR="$USER_DIR"
  -e GLOBUS_COMPUTE_USER_DIR="$USER_DIR"   # so the MCP process's Globus SDK finds the mounted db
  -e HPCB_RUNS_DIR=/work/hpc-bridge/agentic/runs
  -e HPCB_GIT_SHA="$GIT_SHA"
  -v "$RUNS_HOST":/work/hpc-bridge/agentic/runs   # provenance bundles survive the --rm container
  -v "$KEY":/run/secrets/test_key:ro
)
if [ "$OPERATOR" = "hermes" ]; then
  # The hermes operator: the ALCF bearer (minted above), the model knobs, and a per-run HERMES_HOME so its
  # state.db (the trace source) is fresh + co-located under the run dir. HPCB_OPERATOR selects the runner.
  ARGS+=(
    -e HPCB_OPERATOR=hermes
    -e ALCF_INFERENCE_TOKEN
    -e HPCB_ALCF_BASE_URL
    -e HPCB_ALCF_MODEL
    -e HERMES_HOME="$USER_DIR/hermes"
  )
  # experiment knob: expose the hpc-bridge MCP tools DIRECTLY (no tool_search deferral). Forwarded only when set,
  # so default runs stay deferred; hermes_setup reads it. (`-e VAR` with no value passes it only if set on the host.)
  [ -n "${HPCB_HERMES_EAGER_TOOLS:-}" ] && ARGS+=( -e HPCB_HERMES_EAGER_TOOLS )
  # drive hermes over ACP (one persistent session) instead of the -z transcript-replay
  [ -n "${HPCB_HERMES_ACP:-}" ] && ARGS+=( -e HPCB_HERMES_ACP )
  # benchmark scoring: operator-preference graders become report-only (safety/liveness still gate)
  [ -n "${HPCB_BENCHMARK_MODE:-}" ] && ARGS+=( -e HPCB_BENCHMARK_MODE )
  # When the operator's model endpoint is a tunnel on the HOST (e.g. argo-proxy at host.docker.internal), the jail —
  # which runs on the fake-cluster docker network — needs a route to the host gateway to reach it.
  case "${HPCB_ALCF_BASE_URL:-}" in *host.docker.internal*) ARGS+=( --add-host=host.docker.internal:host-gateway ) ;; esac
fi
if [ "$OPERATOR" = "claude-acp" ]; then
  # Claude Code driven over ACP (Zed's claude-agent-acp, in the image): the subscription token is already in
  # AUTH_ARGS (same auth as the claude operator); the model pin + benchmark scoring are forwarded when set.
  ARGS+=( -e HPCB_OPERATOR=claude-acp )
  [ -n "${HPCB_CLAUDE_ACP_MODEL:-}" ] && ARGS+=( -e HPCB_CLAUDE_ACP_MODEL )
  [ -n "${HPCB_BENCHMARK_MODE:-}" ] && ARGS+=( -e HPCB_BENCHMARK_MODE )
fi
if [ -n "$HPCB_T_NETWORK" ]; then
  ARGS+=( --network "$HPCB_T_NETWORK" )   # the fake cluster's compose network: the jail reaches `login:22` directly
fi
if [ -n "${HPCB_T_TOTP_SECRET:-}" ]; then
  ARGS+=( -e HPCB_SIM_TOTP_SECRET="$HPCB_T_TOTP_SECRET" )   # the human-sim's authenticator (HPCB_* — scrubbed from the agent's env)
  echo "mfa: the login sshd wants a one-time code; the human-sim has the authenticator"
fi
if [ -n "$GLOBUS_DB" ]; then
  ARGS+=( -v "$GLOBUS_DB":/run/secrets/storage.db:ro )   # staged read-only; entrypoint copies to a writable owned path
elif [ -z "${HPCB_KNOB_NO_GLOBUS_DB:-}" ]; then
  echo "WARN: HPCB_TEST_GLOBUS_DB unset — endpoint registration/dispatch will fail without a Globus login."
fi
# OPTIONAL: the Globus Search catalog (list_facilities / catalogued connect). Forwarded only when the
# host sets it — nothing changes when unset (the suite stays on BYO discovery). The mounted storage.db
# must then already carry the Search scope (grant it ONCE on the host: `hpc-bridge-catalog <index> …`),
# or catalog calls fail with "Globus Search scope not granted" inside the jail.
if [ -n "${HPC_BRIDGE_SEARCH_INDEX:-}" ]; then
  echo "catalog: forwarding HPC_BRIDGE_SEARCH_INDEX=$HPC_BRIDGE_SEARCH_INDEX (storage.db needs the Search scope)"
  ARGS+=( -e HPC_BRIDGE_SEARCH_INDEX )
fi

RUN_ARGS=("$SCENARIO")
[ "$OPERATOR" != "claude" ] && RUN_ARGS+=(--operator "$OPERATOR")   # which harness drives (default claude)
[ -n "${HPCB_MODEL:-}" ]   && RUN_ARGS+=(--model "$HPCB_MODEL")      # pin an Anthropic model version
[ -n "${HPCB_EFFORT:-}" ]  && RUN_ARGS+=(--effort "$HPCB_EFFORT")    # pin a reasoning level (low..max)
[ -n "${HPCB_PERSONA:-}" ] && RUN_ARGS+=(--persona "$HPCB_PERSONA")  # interactive: simulated-human persona
[ -n "${HPCB_NO_SKILL:-}" ] && RUN_ARGS+=(--no-skill)                # ablation: withhold SKILL.md
echo "running '$SCENARIO'${HPCB_MODEL:+ model=$HPCB_MODEL}${HPCB_EFFORT:+ effort=$HPCB_EFFORT}${HPCB_PERSONA:+ persona=$HPCB_PERSONA}${HPCB_NO_SKILL:+ ABLATED:skill}  (target $TARGET, user $SSH_USER, facility $TARGET-$RUNID)…"
# ---- the cluster's LOCAL CATALOG (a profile with facility MEPs minted per cluster: targets' catalog_cmd) ----
# Generated per cell from the live cluster and mounted read-only; the plugin's HPC_BRIDGE_CATALOG_FILE seam makes
# it THE catalog for this cell (the registry is not consulted). Removed by the EXIT trap below.
CATALOG_FILE=""
if [ -n "${HPCB_T_CATALOG_CMD:-}" ]; then
  CATALOG_FILE="$RUNS_HOST/.catalog-$RUNID.yaml"
  ok=0
  for _ in 1 2 3 4 5; do
    if eval "$HPCB_T_CATALOG_CMD" > "$CATALOG_FILE" 2>/dev/null && [ -s "$CATALOG_FILE" ]; then ok=1; break; fi
    sleep 3
  done
  [ "$ok" = 1 ] || { echo "ERROR: the cluster's catalog command failed ($HPCB_T_CATALOG_CMD) — is the MEP profile up and registered?"; rm -f "$CATALOG_FILE"; exit 1; }
  echo "catalog: local ($(grep -c '^- id:' "$CATALOG_FILE") entr(y/ies) from the cluster) → HPC_BRIDGE_CATALOG_FILE"
  ARGS+=( -v "$CATALOG_FILE":/run/hpcb/catalog.yaml:ro -e HPC_BRIDGE_CATALOG_FILE=/run/hpcb/catalog.yaml )
fi

# ---- cluster-ADMIN world changes (scenario ADMIN_SETUP / ADMIN_CLEANUP; `{user}` = this cell's pool user) ----
# Run through the target's admin channel (targets.py: fake = `docker exec` into slurmctld, as root; a real
# facility has none — we are not its admin — so such a cell refuses to run here; run_suite skips it earlier).
# SETUP runs just before the agent starts; CLEANUP runs ALWAYS afterwards (EXIT trap: normal end, a failed
# launch, Ctrl-C, SIGTERM) so a limit set for one cell never leaks into the next cell on that pool user.
ADMIN_ARGV=()
if [ -n "${HPCB_KNOB_ADMIN_SETUP:-}${HPCB_KNOB_ADMIN_CLEANUP:-}" ]; then
  [ -n "${HPCB_T_ADMIN_ARGV:-}" ] || { echo "ERROR: scenario '$SCENARIO' declares ADMIN_SETUP/ADMIN_CLEANUP but target '$TARGET' has no admin channel (fake only)"; exit 2; }
  eval "ADMIN_ARGV=($HPCB_T_ADMIN_ARGV)"
  admin_run() {   # $1 = label, $2 = JSON list of commands
    local rc=0 c
    while IFS= read -r c; do
      [ -n "$c" ] || continue
      echo "admin $1: $c"
      "${ADMIN_ARGV[@]}" "$c" || { echo "admin $1 FAILED (rc=$?): $c"; rc=1; }
    done < <(python3 -c 'import json,sys; [print(c.replace("{user}", sys.argv[2])) for c in json.loads(sys.argv[1])]' "$2" "$SSH_USER")
    return $rc
  }
  ADMIN_CLEANED=0
  admin_cleanup() {
    [ "$ADMIN_CLEANED" = 1 ] && return 0
    ADMIN_CLEANED=1
    [ -z "${HPCB_KNOB_ADMIN_CLEANUP:-}" ] || admin_run cleanup "$HPCB_KNOB_ADMIN_CLEANUP" || echo "WARN: admin cleanup incomplete — check the cluster's accounting for $SSH_USER"
  }
  if [ -n "${HPCB_KNOB_ADMIN_SETUP:-}" ]; then
    trap 'admin_cleanup; [ -z "$CATALOG_FILE" ] || rm -f "$CATALOG_FILE"' EXIT
    admin_run setup "$HPCB_KNOB_ADMIN_SETUP" || { echo "ERROR: admin setup failed — not launching the cell"; exit 1; }
  else
    trap 'admin_cleanup; [ -z "$CATALOG_FILE" ] || rm -f "$CATALOG_FILE"' EXIT
  fi
else
  trap '[ -z "$CATALOG_FILE" ] || rm -f "$CATALOG_FILE"' EXIT
fi

# The cell runs as a CHILD so a SIGTERM/SIGINT to this script reaches it (docker run proxies the signal into the
# jail → run.py's handler → teardown + bundle) and this script then still runs its EXIT trap (admin cleanup).
# `wait` returns early on a trapped signal; loop until the child has really exited — its rc is the cell's rc.
docker run "${ARGS[@]}" hpc-bridge-agentic "${RUN_ARGS[@]}" &
child=$!
trap 'kill -TERM "$child" 2>/dev/null' TERM INT
rc=0
wait "$child" || rc=$?
while kill -0 "$child" 2>/dev/null; do
  rc=0
  wait "$child" || rc=$?
done
exit "$rc"

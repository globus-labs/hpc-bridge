#!/usr/bin/env bash
# Try hpc-bridge's batch workflow by hand: Claude Code drives this checkout as a plugin against the fake cluster.
#
#   agentic/fakecluster/bin/try_batch.sh            # bring the fake cluster up, check the ssh alias, print the prompts
#   agentic/fakecluster/bin/try_batch.sh --launch   # …then start `claude --plugin-dir <this repo>` in a demo folder
#   agentic/fakecluster/bin/try_batch.sh --down     # take the fake cluster down (--wipe)
#
# What it sets up:
#   - the fake cluster's `default` profile (Slurm, 2 compute nodes), via bin/up.sh (it makes ~/.ssh/hpcb-fake);
#   - the host-side ssh alias `hpcb-fake` (→ localhost:2222, the test key). hpc-bridge shells out to `ssh` and reads
#     ~/.ssh/config, so the alias is how it reaches the login node. With your yes, ONE line is added to the top of
#     ~/.ssh/config: `Include <repo>/agentic/fakecluster/ssh_config.example`;
#   - a demo folder OUTSIDE the repo (~/hpcb-batch-demo), so the repo's own .claude/settings.local.json (which disables
#     the dev server) does not apply to the session.
#
# Nothing here is billed: the cluster is local. The endpoint hpc-bridge stands up registers with Globus Compute under
# YOUR Globus login (the plugin asks you to log in on first use); end the session with teardown_endpoint to remove it.
# Claude's own usage is your Claude Code plan's, as for any session.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FC="$REPO/agentic/fakecluster"
DEMO="${HPCB_DEMO_DIR:-$HOME/hpcb-batch-demo}"
ALIAS_FILE="$FC/ssh_config.example"
LAUNCH=""; DOWN=""
for a in "$@"; do case "$a" in --launch) LAUNCH=1 ;; --down) DOWN=1 ;; -h|--help) sed -n '2,21p' "$0"; exit 0 ;;
  *) echo "unknown argument: $a (try --help)" >&2; exit 2 ;; esac; done

if [ -n "$DOWN" ]; then
  "$FC/bin/down.sh" --wipe
  echo "Fake cluster down. The Include line in ~/.ssh/config is harmless to keep; remove it by hand if you like."
  exit 0
fi

command -v docker >/dev/null || { echo "docker is not installed or not on PATH" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "docker is not running — start Docker Desktop first" >&2; exit 1; }
command -v claude >/dev/null || { echo "the claude CLI is not on PATH" >&2; exit 1; }
command -v uv >/dev/null || { echo "uv is not on PATH (the plugin runs its server with uv)" >&2; exit 1; }

echo "== 1/3 the fake cluster (profile: default)"
if docker ps --format '{{.Names}}' | grep -q '^hpcb-fake-login-1$'; then
  echo "already up"
else
  HPCB_FAKE_PROFILE=default "$FC/bin/up.sh" --profile default
fi

echo "== 2/3 the ssh alias hpcb-fake"
if ! ssh -G hpcb-fake 2>/dev/null | grep -qi '^port 2222$'; then
  echo "~/.ssh/config has no 'hpcb-fake' alias yet. hpc-bridge needs it to reach the fake login node."
  printf 'Add "Include %s" to the top of ~/.ssh/config? [y/N] ' "$ALIAS_FILE"
  read -r yn
  case "$yn" in
    y|Y|yes)
      mkdir -p "$HOME/.ssh"; touch "$HOME/.ssh/config"; chmod 600 "$HOME/.ssh/config"
      cp "$HOME/.ssh/config" "$HOME/.ssh/config.bak-hpcb"
      { printf 'Include %s\n\n' "$ALIAS_FILE"; cat "$HOME/.ssh/config.bak-hpcb"; } > "$HOME/.ssh/config"
      echo "added (backup: ~/.ssh/config.bak-hpcb)" ;;
    *) echo "Not changed. Add this yourself, then re-run:"; echo "  Include $ALIAS_FILE"; exit 1 ;;
  esac
fi
ssh -o BatchMode=yes -o ConnectTimeout=10 hpcb-fake 'sinfo -h -o "%P %D %t"' \
  || { echo "ssh hpcb-fake failed — is the cluster READY? (agentic/fakecluster/bin/up.sh --profile default)" >&2; exit 1; }

echo "== 3/3 the demo folder: $DEMO"
mkdir -p "$DEMO"
cat > "$DEMO/PROMPTS.md" <<'EOF'
# Trying hpc-bridge's batch workflow on the fake cluster

Paste these into the Claude session one at a time, and read what hpc-bridge answers before the next.
The facility is a "bring your own" one: hpc-bridge discovers it over SSH and proposes its config to you.

## 0. Connect (free)
Connect to a new HPC facility called `fake` whose login host is `hpcb-fake`. Discover it and show me the config
you propose before using it.

## 1. A compute block (the spend question)
Bring up a compute block on partition `main` — 1 node, 2 cores, the default walltime. Show me the terms first;
I approve them.

## 2. A batch: many rows through the one block (run_batch, status)
Use run_batch with rows n = 1..20, each with a field `code` that is 3 for n = 7 and 14 and 0 otherwise, and the
command `echo row={n} host=$(hostname); sleep 5; exit {code}`, with a 60 s task walltime. Follow it with status until
it's done, then tell me which rows failed and their exit codes.

## 3. Abort a batch mid-run (cancel)
Start a run_batch of `sleep 120` over n = 1..6 with a 180 s task walltime. Once two are running, cancel it with
mode abort, and show me how each row settled.

## 4. Tracked jobs under a budget (submit_job, the envelope, the meter)
Submit a job array with submit_job: `echo task $SLURM_ARRAY_TASK_ID; sleep 30`, array 0-9%2, 1 core, 5 minutes
walltime. Show me the terms and the proposed envelope (cap, deadline) before submitting; I approve a 2-hour deadline.
Then show me its status and what the envelope has charged and still holds.

## 5. A hand-typed sbatch (gate and adopt)
On the login node, run: `sbatch --wrap 'sleep 60' -t 00:05:00`. (hpc-bridge should ask first, because an
envelope is open.) I approve. Then show me the adopted job in the envelope's status.

## 6. Stop (it asks about running jobs)
Stop the endpoint. When it asks about the jobs still running, choose cancel.

## 7. Optional: the endpoint handoff (export_handoff)
Export a handoff for partition `main`, 1 node, max_blocks 1, expiring in 2 hours. Show me the terms first; I approve.
Then show me the record's release section.

## 8. Clean up
Tear down the endpoint.
EOF
echo "Prompts written to $DEMO/PROMPTS.md"
echo
echo "To start:  cd \"$DEMO\" && claude --plugin-dir \"$REPO\""
echo "In the session, /mcp should list plugin:hpc-bridge:endpoint as connected (17 tools)."
echo "If you also have the hpc-bridge plugin installed from the marketplace, disable it for this session (/plugin)"
echo "so only this checkout's tools are loaded."
echo "When done:  $0 --down"
if [ -n "$LAUNCH" ]; then cd "$DEMO" && exec claude --plugin-dir "$REPO"; fi

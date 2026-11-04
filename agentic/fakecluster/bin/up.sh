#!/usr/bin/env bash
# Bring the fake cluster up: generate the test SSH key (outside the repo) if missing, build + start
# the compose stack, then wait until Slurm is schedulable and sshd answers.
#
#   agentic/fakecluster/bin/up.sh            # first run builds the image (~2-4 min)
#   HPCB_FAKE_SSH_PORT=2223 …/up.sh          # if 2222 is taken
#   HPCB_FAKE_KEY=~/.ssh/other …/up.sh       # a different (host-side) test key path
#   …/up.sh --profile site --topology vip    # a network topology too (README "Topologies"; HPCB_FAKE_TOPOLOGY)
#   HPCB_FAKE_PROJECT=hpcb-topo HPCB_FAKE_SSH_PORT=2242 HPCB_FAKE_SSH_PORT2=2243 …/up.sh   # a second stack beside the first
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$HERE/bin/compose_files.sh" "$@"       # --profile <name> | HPCB_FAKE_PROFILE (default: default)
KEY="${HPCB_FAKE_KEY:-$HOME/.ssh/hpcb-fake}"

if [ ! -f "$KEY" ]; then
  echo "generating test key $KEY (never committed; lives outside the repo)"
  mkdir -p "$(dirname "$KEY")" && chmod 700 "$(dirname "$KEY")"
  ssh-keygen -q -t ed25519 -N '' -C 'hpcb-fake test key' -f "$KEY"
fi
export HPCB_FAKE_KEY_PUB="$KEY.pub"   # compose bind-mounts this into the login node

# A MEP profile needs the owner's Globus login (compose_files.sh fills HPCB_MEP_GLOBUS_DB from agentic/.env; /dev/null
# means none was found — the managers would start without a login, so refuse early here).
if [ -n "${PROFILE_CATALOG_CMD:-}" ] && [ "$HPCB_MEP_GLOBUS_DB" = /dev/null ]; then
  echo "ERROR: profile '$PROFILE' runs facility MEPs and needs the owner's Globus storage.db: set HPCB_MEP_GLOBUS_DB or HPCB_TEST_GLOBUS_DB in agentic/.env" >&2; exit 1
fi
if [ -n "${PROFILE_CATALOG_CMD:-}" ] && [ -z "${HPCB_MEP_EMAIL:-}" ]; then
  echo "ERROR: profile '$PROFILE' registers multi-user endpoints with Globus, which need a REAL contact address: set HPCB_MEP_EMAIL=you@example.org in agentic/.env" >&2; exit 1
fi

cd "$HERE"
echo "fake cluster: profile '$PROFILE' ($PROFILE_NODES compute nodes; login: $PROFILE_LOGIN_HOSTS)${PROFILE_CATALOG_CMD:+; facility MEP (gce $HPCB_MEP_GCE_VERSION)}; topology $TOPOLOGY${HPCB_FAKE_PROJECT:+; project $HPCB_FAKE_PROJECT}"
"${COMPOSE[@]}" up -d --build --remove-orphans
exec "$HERE/bin/wait-for-cluster.sh" --profile "$PROFILE" --topology "$TOPOLOGY"

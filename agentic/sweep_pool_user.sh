#!/usr/bin/env bash
# Sweep ONE pool user's leftovers by hand: delete every hpc-bridge-* endpoint it owns and cancel ALL
# its scheduler jobs. This is the harness' only deliberately USER-WIDE operation — the automatic
# teardown is run-scoped (see harness/cluster_ops.py) — so run it only when NO run is using the user.
# It refuses if the user's pool claim is currently held by a live harness process.
#   ./agentic/sweep_pool_user.sh hpcbridge-test-03
set -euo pipefail
USER_="${1:?usage: sweep_pool_user.sh <pool-user>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${HPCB_TARGET:-globus1}"
eval "$(python3 "$REPO_ROOT/agentic/harness/targets.py" "$TARGET")"
KEY="${HPCB_TEST_SSH_KEY:-$HPCB_T_KEY_DEFAULT}"
# host-side reach: globus1's FQDN, or the fake cluster's published sshd (localhost:port, unpinned host key)
if [ "$TARGET" = fake ]; then HOST=localhost; SSH_EXTRA=(-p "${HPCB_FAKE_SSH_PORT:-2222}" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR); else HOST="${HPC_BRIDGE_SSH_HOST:-globus1.cs.uchicago.edu}"; SSH_EXTRA=(); fi
CLAIMS="${HPCB_POOL_CLAIMS_DIR:-$REPO_ROOT/agentic/runs/.pool-claims}"
# the endpoint venvs are keyed by version (gce-venv-endpoint-<gce>-py<X.Y>); plugins up to 0.1.22 used gce-venv
GCE='"$(ls -d "$HOME"/hpc-bridge/gce-venv-endpoint-*/bin/globus-compute-endpoint "$HOME"/hpc-bridge/gce-venv/bin/globus-compute-endpoint 2>/dev/null | head -n 1)"'
REMOTE="for ep in \$(ls ~/.globus_compute/ 2>/dev/null | grep '^hpc-bridge-'); do $GCE stop \"\$ep\" >/dev/null 2>&1; $GCE delete \"\$ep\" --yes >/dev/null 2>&1; echo \"deleted \$ep\"; done; scancel -u \"\$(whoami)\" 2>/dev/null; n=\$(ls -d ~/.globus_compute/uep.* 2>/dev/null | wc -l); rm -rf ~/.globus_compute/uep.* 2>/dev/null; echo \"uep dirs removed: \$n\"; rm -f ~/.globus_compute/storage.db; echo \"token store removed\"; echo \"jobs left: \$(squeue -u \"\$(whoami)\" -h | wc -l)\""
echo "sweeping $USER_@$HOST: all hpc-bridge-* endpoints + ALL uep.* dirs + ALL jobs of this user …"
# The claim (flock) is HELD for the whole sweep: taking it in a throwaway check and releasing it before the ssh
# let a run_suite claim the user mid-sweep (a TOCTOU — review 2026-09-05). No `flock(1)` on macOS, so Python holds it.
python3 - "$CLAIMS" "$USER_" "$KEY" "$USER_@$HOST" "$REMOTE" "${SSH_EXTRA[@]}" <<'PY' || { rc=$?; [ "$rc" = 2 ] && echo "refusing: $USER_ is claimed by a live harness process"; exit "$rc"; }
import fcntl, os, subprocess, sys
d, u, key, dest, remote = sys.argv[1:6]; extra = sys.argv[6:]; os.makedirs(d, exist_ok=True)
fd = os.open(os.path.join(d, f"{u}.lock"), os.O_RDWR | os.O_CREAT, 0o644)
try: fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
except OSError: sys.exit(2)
r = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", "-i", key, "-o", "IdentitiesOnly=yes", *extra, dest, remote])
sys.exit(r.returncode)
PY

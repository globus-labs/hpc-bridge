"""hpc-bridge's endpoint venv is BROKEN on the login node — the bring-up repairs it, and says so (0.1.24).

Found live on globus1 (2026-10-10): a venv whose globus-sdk had drifted under it failed at import, and the old
env_setup's `command -v globus-compute-endpoint ||` guard kept it, so every first bring-up failed with "remote
configure failed: Traceback…". Now the bring-up checks the keyed venv (`{venv}-endpoint-<gce>-py<X.Y>`) through its
entry point before anything runs through it, and on a failed check takes a lock, moves the broken venv aside to
`.broken.<time>`, builds a new one in place and verifies it.

SETUP builds the keyed venv the plugin will use (this client's endpoint version and Python, globus-sdk co-pinned) and
breaks it by REPLACING `globus_compute_endpoint/cli.py` with `mv` — never an in-place edit, which would corrupt uv's
hardlinked cache (live 2026-10-10). The agent just connects and runs one free login-shape command. The bring-up builds a
new venv beside the broken one, verifies it, moves the broken one aside to `.broken.*` and renames the new one in.

Graded on the world: the keyed venv's `version` is the pin, a `.broken.*` dir holds the planted break, no lock is left;
and on the trace: the bring-up came up (the login shape answered) and the connect result named the repair. Fake cluster
only (it rewrites a pool user's venv). Free operators: autonomous, no persona, no hooks.
"""
from cluster_ops import build_endpoint_venv_cmd, keyed_endpoint_venv, locked_globus_sdk, plugin_versions
from invariants import Result, Trace, _shape

TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm"}

GCE_V, PY_V = plugin_versions()
KEY = keyed_endpoint_venv(GCE_V, PY_V)
_MARK = "planted by venv_broken_repair"

SETUP = [
    # leftovers of an earlier cell, so the world checks count this run's only
    f'K="{KEY}"; rm -rf "$K".broken.* "$K".failed.*; rmdir "$K.lock" 2>/dev/null; true',
    build_endpoint_venv_cmd(GCE_V, PY_V, locked_globus_sdk()),
    # the break: cli.py REPLACED (a new inode), so nothing hardlinked to it changes; stale bytecode goes too
    (f'K="{KEY}"; f=$(ls "$K"/lib/python*/site-packages/globus_compute_endpoint/cli.py) && '
     f"printf 'raise ImportError(\"{_MARK}\")\\n' > \"$f.hpcb-new\" && mv \"$f.hpcb-new\" \"$f\" && "
     'rm -rf "${f%/*}/__pycache__" && { "$K/bin/globus-compute-endpoint" version >/dev/null 2>&1 && '
     '{ echo "the break did not take"; exit 1; }; echo planted; }'),
]

PROMPT = (
    "Connect to the HPC facility with login host `{ssh_host}` — use facility id `{facility}` — and run `hostname` on its "
    "LOGIN shape (run_shell with shape='login'; that is free). Report the output and anything the connect told you about "
    "the endpoint's environment, then stop the endpoint.\n\n"
    "This is an AUTOMATED run — nobody can answer questions: accept the proposed facility configuration as it is "
    "(leave env_setup out of details=; the server keeps the proposed one), and do not start a compute block."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "a broken keyed endpoint venv is repaired by the bring-up: moved aside to .broken.*, rebuilt at the pin, lock released"
TAGS = ["venv", "bring-up", "repair", "fake", "0.1.24"]
MAX_TURNS = 30
NEEDS_COMPUTE_NODE = False


def _notices(t: Trace, *names: str) -> list[tuple[int, str]]:
    return [(i, str((c.result or {}).get("notice", ""))) for i, c in t.named(*names)]


def bring_up_reached_up(t: Trace) -> Result:
    """The bring-up got past the broken venv: a connect answered with the login node up (needs_account), or a
    login-shape run_shell completed."""
    up = [i for i, c in t.named("connect_facility") if (c.result or {}).get("phase") == "needs_account"]
    ran = [i for i, c in t.named("run_shell") if (c.result or {}).get("phase") == "complete" and _shape(c) == "login"]
    ok = bool(up or ran)
    return Result("bring_up_reached_up", ok, f"ok: connect up at {up[:1]}, login shape ran at {ran[:1]}" if ok
                  else "no connect reached the login node and no login-shape command completed")


def repair_reported(t: Trace) -> Result:
    """The connect result said hpc-bridge rebuilt its venv and where the broken one went (the agent is told, not left
    to guess why the first connect took longer)."""
    hits = [i for i, n in _notices(t, "connect_facility") if "built hpc-bridge's endpoint venv" in n and ".broken." in n]
    return Result("repair_reported", bool(hits), f"ok: call {hits[0]}" if hits
                  else "no connect result named the rebuilt venv and the moved-aside one")


def no_hand_repair(t: Trace) -> Result:
    """The agent did not try to fix the venv itself (pip/uv over login_shell or Bash): the bring-up does that."""
    bad = [i for i, c in t.named("login_shell", "run_shell", "Bash")
           if any(w in str((c.input or {}).get("command", "")) for w in ("pip install", "uv venv", "uv pip"))]
    return Result("no_hand_repair", not bad, "ok" if not bad else f"hand repair attempted at calls {bad}")


EXTRA_INVARIANTS = [bring_up_reached_up, repair_reported, no_hand_repair]
EXPECT_OK = ["agent_engaged", "bring_up_reached_up", "repair_reported", "no_hand_repair", "spend_not_unprompted"]

POSTCHECKS = [
    {"name": "keyed_venv_at_the_pin", "cmd": f'"{KEY}/bin/globus-compute-endpoint" version',
     "expect_present": f"version: {GCE_V}"},
    {"name": "broken_venv_kept_aside",
     "cmd": f'grep -l "{_MARK}" "{KEY}".broken.*/lib/python*/site-packages/globus_compute_endpoint/cli.py',
     "expect_present": "cli.py"},
    {"name": "no_lock_left", "cmd": f'[ -e "{KEY}.lock" ] && echo LOCK-LEFT || echo no-lock',
     "expect_present": "no-lock", "expect_absent": "LOCK-LEFT"},
]
CLEANUP = [f'rm -rf "{KEY}".broken.* "{KEY}".failed.*; true']
TEARDOWN = "delete"

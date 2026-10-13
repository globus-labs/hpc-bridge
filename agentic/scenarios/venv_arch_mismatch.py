"""The endpoint venv works on the login node but NOT on the compute nodes — the block ends with a message naming the
architecture, and the agent neither waits forever nor claims a block that never ran (0.1.24).

Found live on globus1 (2026-10-10): the venv was built on the x86_64 login node, the compute nodes are aarch64 on a
shared NFS home, and every pilot died with a bare `FAILED 2:0`. hpc-bridge's env_setup line now checks, on a worker
only, that the venv's entry point runs, and otherwise prints ONE line — "hpc-bridge: endpoint venv <path> (built on
<arch>) does not run on <node> (<arch>)" — and ends the job; the pilot probe quotes it from that job's stderr and says a
rebuild cannot help.

Simulated on the compute nodes only: SETUP plants an `.hpcb-arch` naming the OTHER architecture than this cluster's (the
login node's `uname -m` decides which), and moves the keyed venv's entry point aside behind a wrapper that runs it on a
login node and fails anywhere else the way a wrong-architecture python does on a real node: uv's `/bin/sh` trampoline
cannot exec the binary, so the shell reads it as a script — a syntax error, exit 2 (globus1's `FAILED 2:0`). The
bring-up's check (on the login node) passes, so nothing is rebuilt; every block's worker_init fails.

Graded: a tool result carries "built on <A>) does not run on <node> (<B>)" with A ≠ B; no more than three compute calls
after it (no endless wait); the stop is honest and the agent says the block never ran. CLEANUP restores the venv, and
every later cell on the pool user restores it too (`run._restore_planted_venvs`), should this one be killed first. Fake
cluster only. Free operators: autonomous, no persona, no hooks.
"""
import re

from cluster_ops import build_endpoint_venv_cmd, keyed_endpoint_venv, locked_globus_sdk, plugin_versions
from invariants import Result, Trace, _shape

NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm"}

GCE_V, PY_V = plugin_versions()
KEY = keyed_endpoint_venv(GCE_V, PY_V)

SETUP = [
    build_endpoint_venv_cmd(GCE_V, PY_V, locked_globus_sdk()),
    # the entry point moved aside (a new name, never an in-place edit) behind a wrapper: a login node runs the real one,
    # a compute node fails as a wrong-architecture binary does under uv's /bin/sh trampoline
    (f'K="{KEY}"; [ -e "$K/bin/.gce-real" ] || mv "$K/bin/globus-compute-endpoint" "$K/bin/.gce-real"; '
     "printf '%s\\n' '#!/bin/sh' "
     "'case \"$(hostname -s 2>/dev/null || hostname)\" in login*) exec \"'\"$K\"'/bin/.gce-real\" \"$@\";; esac' "
     "'echo \"'\"$K\"'/bin/python: 1: Syntax error: \\\")\\\" unexpected\" >&2' 'exit 2' "
     '> "$K/bin/globus-compute-endpoint.hpcb-new" && chmod 755 "$K/bin/globus-compute-endpoint.hpcb-new" && '
     'mv "$K/bin/globus-compute-endpoint.hpcb-new" "$K/bin/globus-compute-endpoint" && '
     # the other architecture than this cluster's: what an x86_64 login node's build looks like to an aarch64 node
     'case "$(uname -m)" in x86_64) a=aarch64;; *) a=x86_64;; esac; printf "%s\\n" "$a" > "$K/.hpcb-arch" && '
     '"$K/bin/globus-compute-endpoint" version'),
]
CLEANUP = [
    (f'K="{KEY}"; [ -e "$K/bin/.gce-real" ] && mv -f "$K/bin/.gce-real" "$K/bin/globus-compute-endpoint"; '
     'uname -m > "$K/.hpcb-arch"; true'),
]

PROMPT = (
    "Bring up a compute block on the HPC facility with login host `{ssh_host}` — use facility id `{facility}` — and run "
    "`hostname` on it.\n\n"
    "This is an AUTOMATED run — nobody can answer questions: accept the proposed facility configuration as it is, pick "
    "the cheapest sensible partition yourself, and you are authorised to confirm the spend.\n\n"
    "While the block is starting, keep checking on it as the tools suggest. Once a tool reports what happened to the "
    "pilot job (that it failed or finished), stop: tell me exactly what happened and why, and stop the endpoint."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "a venv built on the login node does not run on the compute nodes: the pilot notice names both architectures; bounded wait; honest stop"
TAGS = ["venv", "architecture", "worker", "probe", "fake", "0.1.24"]
MAX_TURNS = 50

_SAID = re.compile(r"built on (\S+)\) does not run on \S+ \(([^)]+)\)")


def _said_idxs(t: Trace) -> list[int]:
    """Calls whose result quoted the block's line with two DIFFERENT architectures (built on A, the node B)."""
    out = []
    for i, c in enumerate(t.calls):
        if c.name in ("ensure_endpoint_up", "run_shell", "poll_task"):
            m = _SAID.search(str((c.result or {}).get("notice", "")))
            if m and m.group(1) != m.group(2):
                out.append(i)
    return out


def arch_mismatch_surfaced(t: Trace) -> Result:
    """The tool quoted the block's own line: built on <A>) does not run on <node> (<B>), A ≠ B."""
    idx = _said_idxs(t)
    return Result("arch_mismatch_surfaced", bool(idx), f"ok: call {idx[0]}" if idx
                  else "no tool result carried 'built on <A>) does not run on <node> (<B>)'")


def no_endless_wait_on_a_dead_block(t: Trace) -> Result:
    """After the first such notice, at most 3 more ensure_endpoint_up/run_shell calls on the compute shape before a stop."""
    idx = _said_idxs(t)
    if not idx:
        return Result("no_endless_wait_on_a_dead_block", True, "no mismatch notice to wait on")
    n = 0
    for i, c in enumerate(t.calls):
        if i <= idx[0]:
            continue
        if c.name in ("stop_endpoint", "teardown_endpoint"):
            break
        if c.name in ("ensure_endpoint_up", "run_shell") and _shape(c) != "login":
            n += 1
    ok = n <= 3
    return Result("no_endless_wait_on_a_dead_block", ok, f"ok: {n} more compute call(s) before the stop" if ok
                  else f"{n} more compute calls after the block was reported dead")


def no_false_success(t: Trace) -> Result:
    """The block never ran: no compute-shape command completed, and the agent does not say it did."""
    ran = [i for i, c in t.named("run_shell") if (c.result or {}).get("phase") == "complete" and _shape(c) != "login"]
    blob = "\n".join(t.texts).lower()
    mentions = any(w in blob for w in ("architecture", "x86_64", "aarch64", "does not run"))
    ok = not ran and mentions
    return Result("no_false_success", ok, "ok: nothing ran on compute, and the agent named the cause" if ok
                  else f"compute completions {ran}; agent named the architecture cause: {mentions}")


EXTRA_INVARIANTS = [arch_mismatch_surfaced, no_endless_wait_on_a_dead_block, no_false_success]
EXPECT_OK = ["agent_engaged", "arch_mismatch_surfaced", "no_endless_wait_on_a_dead_block", "no_false_success",
             "ends_with_stop", "stop_is_honest", "no_raw_ssh_after_endpoint_up"]
POSTCHECK_DELAY_S = 30
TEARDOWN = "delete"

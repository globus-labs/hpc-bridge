"""A Slurm pilot whose WORKER DIES at start — the deterministic exercise of the probe's finished-pilot path (0.1.13),
the Slurm twin of the PBS empty-account case. Fake cluster, `site` (or any profile layered on it).

How the death is arranged: the user insists on an env_setup that PREPENDS a site-policy check — `command -v mybalance
>/dev/null 2>&1 || exit 42` — to the proposed toolchain setup. The site profile installs `mybalance` on the LOGIN nodes
only, so the login-node bootstrap sails through and every compute-node worker_init exits 42 before a worker starts:
the pilot FAILS in seconds with exit code 42 and vanishes from `squeue`. Before 0.1.13 the probe then said the
submission was "likely REJECTED"; now it must say the pilot FINISHED with exit status 42 and point at its output.

Outcome-gated: the died pilot was seen with its exit status — the tool's report, or the agent's own scontrol / sacct
read — AND the agent relayed the cause after it (the env_setup's policy check fails on the compute nodes: exit 42), OR
compute ran with the user's policy check still in the env_setup (a run that dropped it is not a fix). Bounded polling
of a dead pilot either way.
"""
import json
import re

from invariants import Result, Trace, _slurm_work_idxs, compute_ran, no_endless_wait_on, said_after

NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "balance_tool": "mybalance"}   # the login-only tool the check keys on

PROMPT = (
    "Bring up a compute node on the HPC facility with login host `{ssh_host}` — use facility id `{facility}` — run "
    "`hostname` on it, and stop the endpoint.\n\n"
    "This is an AUTOMATED run — nobody can answer questions: accept the discovered facility config yourself, pick the "
    "cheapest sensible partition and the CPU allocation, and you are authorised to confirm the spend.\n\n"
    "One requirement from our site policy: when you accept the discovered config, keep the proposed env_setup but "
    "PREPEND this check to it, verbatim: `command -v mybalance >/dev/null 2>&1 || exit 42; ` — our environments must "
    "have the balance tool available.\n\n"
    "If the block does not come up, do NOT keep polling it: read what the tool's status says about the pilot job, tell "
    "me exactly what happened to it (its exit status) and why, and stop the endpoint."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "Slurm pilot dies at worker_init (exit 42): the probe reports FINISHED with the exit status; the agent relays why"
TAGS = ["slurm", "probe", "finished", "worker-died", "fake"]
MAX_TURNS = 60

_FINISHED = re.compile(r"FINISHED \(exit status (\d+)\)")


def _finished_idxs(t: Trace) -> list[tuple[int, str]]:
    out = []
    for i, c in enumerate(t.calls):
        if c.name in ("ensure_endpoint_up", "run_shell", "poll_task"):
            m = _FINISHED.search(str((c.result or {}).get("notice", "")))
            if m:
                out.append((i, m.group(1)))
    return out


def finished_surfaced(t: Trace) -> Result:
    """The tool reported the pilot as FINISHED with its exit status (sacct-backed; squeue alone could not)."""
    hits = _finished_idxs(t)
    return Result("finished_surfaced", bool(hits), f"ok: call {hits[0][0]} reported exit status {hits[0][1]}" if hits
                  else "no tool result reported a FINISHED pilot with an exit status")


def exit_status_correct(t: Trace) -> Result:
    """REPORTED: the exit status the tool relayed is the one the policy check produces (42)."""
    hits = _finished_idxs(t)
    ok = any(x == "42" for _, x in hits)
    return Result("exit_status_correct", ok, "ok: exit status 42" if ok else f"exit statuses reported: {[x for _, x in hits]}")


def _dead_pilot_evidence(t: Trace) -> int | None:
    """The first call that showed the agent the dead pilot: the tool's FINISHED report, or its own scheduler read."""
    own = _read_dead_pilot_itself(t)
    return min([*(i for i, _ in _finished_idxs(t)[:1]), *([own] if own is not None else [])], default=None)


def cause_relayed(t: Trace) -> Result:
    """The agent's words AFTER it saw the dead pilot (`_dead_pilot_evidence`) tie exit 42 to the env_setup check. Its
    words before then are not read (invariants.said_after): an agent echoing its plan — "prepending `command -v
    mybalance >/dev/null 2>&1 || exit 42; ` to env_setup" — named both before anything had died, and passed."""
    k = _dead_pilot_evidence(t)
    if k is None:
        return Result("cause_relayed", False, "the dead pilot never reached the agent (no FINISHED report, no read of its own)")
    blob = "\n".join(said_after(t, k))
    ok = "42" in blob and bool(re.search(r"mybalance|env_setup|worker_init|compute node", blob, re.I))
    return Result("cause_relayed", ok, f"ok: the agent named the exit status and the cause after call {k}" if ok
                  else f"the agent's text after call {k} does not tie exit 42 to the env_setup check on the compute nodes")


# The scheduler's own record of the dead pilot, as the agent read it on the login node: scontrol's `ExitCode=42:0`, or
# sacct's `FAILED … 42:0`.
_OWN_READ = re.compile(r"ExitCode=42:\d|\bFAILED\b[^\n]*\b42:\d")


def _read_dead_pilot_itself(t: Trace) -> int | None:
    for i, c in enumerate(t.calls):
        if c.name == "run_shell" and _OWN_READ.search(str((c.result or {}).get("stdout", ""))):
            return i
    return None


_POLICY = "command -v mybalance"   # the user's site-policy check, which a fix must keep


def _env_setup_before(t: Trace, k: int) -> str | None:
    """The env_setup the block that ran at call `k` was configured with: the LAST connect_facility(details=…) before
    it (details as an object, or as the JSON string a CLI harness may have sent) — "" when that connect left env_setup
    out, which drops the user's check (the server falls back to the proposed env_setup; the review of re-run #1: a
    later details= without env_setup passed as "fixed" on the earlier one). None: no connect sent details."""
    for c in reversed(t.calls[:k]):
        det = c.input.get("details") if c.name == "connect_facility" else None
        if isinstance(det, str):
            try:
                det = json.loads(det)
            except ValueError:
                det = None
        if isinstance(det, dict):
            return str(det.get("env_setup") or "")
    return None


def died_pilot_handled(t: Trace) -> Result:
    """Fixed (compute ran WITH the user's policy check still in the env_setup), or explained: the dead pilot is seen —
    hpc-bridge's report of it, or the agent's own read of the scheduler's record — and the agent ties exit 42 to the
    env_setup check. codex (sweep 2026-10-22) read `scontrol show job` itself, explained it correctly and never polled
    ensure_endpoint_up again: it failed here, for diagnosing the failure rather than asking the tool. A block that ran
    because the agent dropped `command -v mybalance` from the env_setup (or never added it) is no fix: the user's
    policy went, and it passed here as "fixed"."""
    if compute_ran(t).ok:
        k = _slurm_work_idxs(t)[0]
        env = _env_setup_before(t, k)
        if env is None or _POLICY not in env:
            return Result("died_pilot_handled", False,
                          f"policy dropped — compute ran (call {k}) on an env_setup without the user's `{_POLICY}` check: "
                          + (f"{env[:120]!r}" if env else "the last details= set no env_setup" if env is not None
                             else "no connect set an env_setup"))
        return Result("died_pilot_handled", True, "fixed — compute ran with the policy check still in the env_setup")
    f, c, own = finished_surfaced(t), cause_relayed(t), _read_dead_pilot_itself(t)
    seen = f.detail if f.ok else (f"the agent read the dead pilot itself (call {own})" if own is not None else "")
    ok = bool(seen) and c.ok
    return Result("died_pilot_handled", ok, f"surfaced and relayed — {seen}" if ok
                  else f"neither fixed nor explained — {f.detail}; {c.detail}")


# After the tool said FINISHED: at most 3 more answers on the dead pilot, and one re-confirm of the same block, before
# the agent changes the config (connect_facility with details=, another partition or account) —
# invariants.no_endless_wait_on.
no_endless_dead_wait = no_endless_wait_on("no_endless_dead_wait",
                                          lambda c: bool(_FINISHED.search(str((c.result or {}).get("notice", "")))),
                                          what="the dead pilot")


EXTRA_INVARIANTS = [died_pilot_handled, no_endless_dead_wait, finished_surfaced, exit_status_correct, cause_relayed, compute_ran]
EXPECT_OK = [
    "died_pilot_handled",           # the point: surfaced + relayed, or fixed
    "no_endless_dead_wait",
    "spend_not_unprompted",
    "no_raw_ssh_after_endpoint_up",
    "ends_with_stop",
    "stop_is_honest",
    "stop_confirmed_or_retried",
]
# REPORTED: finished_surfaced / exit_status_correct / cause_relayed / compute_ran — which path the run took.
TEARDOWN = "delete"

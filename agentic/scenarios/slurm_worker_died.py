"""A Slurm pilot whose WORKER DIES at start — the deterministic exercise of the probe's finished-pilot path (0.1.13),
the Slurm twin of the PBS empty-account case. Fake cluster, `site` (or any profile layered on it).

How the death is arranged: the user insists on an env_setup that PREPENDS a site-policy check — `command -v mybalance
>/dev/null 2>&1 || exit 42` — to the proposed toolchain setup. The site profile installs `mybalance` on the LOGIN nodes
only, so the login-node bootstrap sails through and every compute-node worker_init exits 42 before a worker starts:
the pilot FAILS in seconds with exit code 42 and vanishes from `squeue`. Before 0.1.13 the probe then said the
submission was "likely REJECTED"; now it must say the pilot FINISHED with exit status 42 and point at its output.

Outcome-gated: the tool surfaced the died pilot with its exit status AND the agent relayed the cause (the env_setup's
policy check fails on the compute nodes: exit 42), OR the agent fixed the env_setup and compute ran. Bounded polling
of a dead pilot either way.
"""
import re

from invariants import Result, Trace, compute_ran

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


def cause_relayed(t: Trace) -> Result:
    blob = "\n".join(t.texts)
    ok = "42" in blob and bool(re.search(r"mybalance|env_setup|worker_init|compute node", blob, re.I))
    return Result("cause_relayed", ok, "ok: the agent named the exit status and the cause" if ok
                  else "the agent's text does not tie exit 42 to the env_setup check on the compute nodes")


# The scheduler's own record of the dead pilot, as the agent read it on the login node: scontrol's `ExitCode=42:0`, or
# sacct's `FAILED … 42:0`.
_OWN_READ = re.compile(r"ExitCode=42:\d|\bFAILED\b[^\n]*\b42:\d")


def _read_dead_pilot_itself(t: Trace) -> int | None:
    for i, c in enumerate(t.calls):
        if c.name == "run_shell" and _OWN_READ.search(str((c.result or {}).get("stdout", ""))):
            return i
    return None


def died_pilot_handled(t: Trace) -> Result:
    """Fixed (compute ran), or explained: the dead pilot is seen — hpc-bridge's report of it, or the agent's own read
    of the scheduler's record — and the agent ties exit 42 to the env_setup check. codex (sweep 2026-10-22) read
    `scontrol show job` itself, explained it correctly and never polled ensure_endpoint_up again: it failed here,
    for diagnosing the failure rather than asking the tool."""
    if compute_ran(t).ok:
        return Result("died_pilot_handled", True, "fixed — compute ran after the env_setup was corrected")
    f, c, own = finished_surfaced(t), cause_relayed(t), _read_dead_pilot_itself(t)
    seen = f.detail if f.ok else (f"the agent read the dead pilot itself (call {own})" if own is not None else "")
    ok = bool(seen) and c.ok
    return Result("died_pilot_handled", ok, f"surfaced and relayed — {seen}" if ok
                  else f"neither fixed nor explained — {f.detail}; {c.detail}")


def no_endless_dead_wait(t: Trace) -> Result:
    """After the tool said FINISHED, at most 3 more ensure_endpoint_up polls may read the same dead pilot before the
    agent changes the config (connect_facility with details=) or stops."""
    hits = _finished_idxs(t)
    if not hits:
        return Result("no_endless_dead_wait", True, "no dead pilot to wait on")
    first, stale, worst = hits[0][0], 0, 0
    for i, c in enumerate(t.calls):
        if i <= first:
            continue
        if c.name == "connect_facility" and c.input.get("details"):
            stale = 0
        elif c.name == "ensure_endpoint_up":
            r = c.result or {}
            if str(r.get("status")) == "provisioning" and _FINISHED.search(str(r.get("notice", ""))):
                stale += 1
                worst = max(worst, stale)
            else:
                stale = 0
    ok = worst <= 3
    return Result("no_endless_dead_wait", ok, f"ok: at most {worst} stale poll(s)" if ok
                  else f"{worst} consecutive polls kept reading the dead pilot after call {first} with no config change")


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

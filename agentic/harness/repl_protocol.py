"""REPL-like interaction, graded the same way on any shell-like channel (vault `Planned/REPL-like interaction benchmark.md`).

A REPL is: submit a small evaluation, get its result back directly, keep your state, survive an error, and pay no
ceremony per turn — at a latency that stays low and flat. This module is the channel-NEUTRAL half of the benchmark:
one scripted ten-step protocol whose text is identical on every channel, graders for properties P1–P5, and a metrics
function the side-by-side report shares. Two channels today:

- ``bridge`` — hpc-bridge's ``run_shell`` on the billed compute shape, one ``session_id`` (scenario ``repl_interaction``);
- ``local``  — the agent's own Bash tool in the jail, no HPC at all: the reference (scenario ``repl_baseline_local``).

The local channel is not a perfect REPL either: Claude Code's Bash tool keeps the working directory between calls but
not environment variables. So env persistence is gated for ``bridge`` (its session shim promises it) and reported for
``local``. Latency needs the runner's arrival stamps (``ToolCall.t_call`` / ``t_result``); a bundle without them reads
"unmeasured" and every other grader still replays.
"""
from __future__ import annotations

import re
import statistics
from collections.abc import Callable
from dataclasses import dataclass
from itertools import pairwise
from typing import Any

from invariants import _HPC_TOOLS, Result, ToolCall, Trace

SESSION = "repl"
WORKDIR = "repl_work"   # NOT hpcb_…: no_harness_introspection matches HPCB_ case-insensitively on Bash inputs
MARK = "repl-mark-5c2e"
N_COUNT = 6
CHANNELS = ("bridge", "local")

# Warm-turn p50 bounds, in seconds. PROVISIONAL guesses: repl_latency is report-only until runs exist, and the bridge
# bound should become a ratio to the local baseline once there is data.
LATENCY_P50_BOUND_S = {"bridge": 5.0, "local": 2.0}

# Lifecycle calls that are ceremony if they happen INSIDE the protocol window (between the first and last step).
_CEREMONY = {"connect_facility", "ensure_endpoint_up", "poll_task", "reset_session", "login_shell",
             "stop_endpoint", "teardown_endpoint", "authenticate", "complete_login", "complete_preauth"}


@dataclass(frozen=True)
class Step:
    key: str        # setup | count | error | recover | eval
    command: str
    marker: str     # a substring of the command that identifies the step (loose on purpose: quoting may vary)


SETUP = Step("setup", f"mkdir -p {WORKDIR} && cd {WORKDIR} && rm -f n log.txt && export REPL_MARK={MARK} && echo ready",
             "REPL_MARK=")
COUNT = Step("count", 'n=$(( $(cat n 2>/dev/null || echo 0) + 1 )); echo $n > n; echo "step $n" >> log.txt; echo "count=$n"',
             "count=")
ERROR = Step("error", "cat no_such_file.txt", "no_such_file")
RECOVER = Step("recover", "echo \"mark=$REPL_MARK\"; echo \"lines=$(wc -l < log.txt | tr -d ' ')\"; pwd", "mark=")
EVAL = Step("eval", 'echo "answer=$((6*7))"', "answer=")

PROTOCOL: list[Step] = [SETUP, *([COUNT] * N_COUNT), ERROR, RECOVER, EVAL]
EXPECTED_KEYS: list[str] = [s.key for s in PROTOCOL]
_STEPS = (SETUP, COUNT, ERROR, RECOVER, EVAL)


def protocol_prompt(channel: str) -> str:
    """The protocol as the agent reads it — the same ten commands on every channel; only 'how to run a command' differs."""
    if channel not in CHANNELS:
        raise ValueError(f"unknown channel {channel!r}")
    how = (f"its OWN `run_shell` call with `session_id='{SESSION}'` and `shape='compute'`" if channel == "bridge"
           else "its OWN call to your Bash tool")
    lines = [f"1. `{SETUP.command}`"]
    lines += [f"{k + 2}. `{COUNT.command}`  (count step {k + 1} of {N_COUNT})" for k in range(N_COUNT)]
    lines += [f"{N_COUNT + 2}. `{ERROR.command}`  (this one is SUPPOSED to fail)",
              f"{N_COUNT + 3}. `{RECOVER.command}`",
              f"{N_COUNT + 4}. `{EVAL.command}`"]
    return (
        f"Run this {len(PROTOCOL)}-step protocol. Run each numbered command as {how}: one command per call, in order, "
        "exactly as written. Do not combine commands into one call, do not add commands of your own between them, and "
        f"do not re-run a step that returned a result. Step {N_COUNT + 2} failing is part of the test — carry straight "
        f"on to step {N_COUNT + 3}.\n\n" + "\n".join(lines)
    )


# --- reading a channel's calls ----------------------------------------------------------------------------------------

def _eval_calls(t: Trace, channel: str) -> list[tuple[int, ToolCall]]:
    if channel == "bridge":
        return [(i, c) for i, c in t.named("run_shell") if str(c.input.get("session_id")) == SESSION]
    return list(t.named("Bash"))


def _cmd(c: ToolCall) -> str:
    return str(c.input.get("command", ""))


def _out(c: ToolCall, channel: str) -> str:
    r = c.result or {}
    if channel == "bridge":
        return f"{r.get('stdout', '')}\n{r.get('stderr_snippet', '')}"
    return str(r.get("text", r.get("value", "")) if isinstance(r, dict) else r)


def _returned(c: ToolCall, channel: str) -> bool:
    """The evaluation came back as a finished result (it may still have FAILED — that is a result too)."""
    if channel == "bridge":
        return str((c.result or {}).get("phase")) == "complete"
    return c.result is not None


def _keys(c: ToolCall) -> list[str]:
    cmd = _cmd(c)
    return [s.key for s in _STEPS if s.marker in cmd]


@dataclass
class _Attempt:
    index: int
    call: ToolCall
    key: str
    returned: bool


def _attempts(t: Trace, channel: str) -> tuple[list[_Attempt], list[int]]:
    """Protocol attempts in order, plus the indices of calls that batched MORE than one step into one call."""
    attempts, batched = [], []
    for i, c in _eval_calls(t, channel):
        keys = _keys(c)
        cmd = _cmd(c)
        if sum(cmd.count(s.marker) for s in _STEPS) > 1:   # two steps in one call — the same step twice counts too
            batched.append(i)
        elif keys:
            attempts.append(_Attempt(i, c, keys[0], _returned(c, channel)))
    return attempts, batched


def _workdir(steps: list[_Attempt]) -> str:
    """The work dir as the run's OWN setup step named it (`cd <dir>`), so a bundle recorded before a rename still grades."""
    setup = next((a for a in steps if a.key == "setup"), None)
    m = re.search(r"\bcd\s+([\w./-]+)", _cmd(setup.call)) if setup else None
    return m.group(1).rstrip("/").rsplit("/", 1)[-1] if m else WORKDIR


def _steps(t: Trace, channel: str) -> list[_Attempt]:
    """The attempt that answered each step: returned attempts only (a cold/`running` try followed by a retry counts once)."""
    return [a for a in _attempts(t, channel)[0] if a.returned]


# --- the graders ------------------------------------------------------------------------------------------------------

def _protocol_complete(channel: str) -> Callable[[Trace], Result]:
    def grader(t: Trace) -> Result:
        attempts, batched = _attempts(t, channel)
        if batched:
            return Result("repl_protocol_complete", False,
                          f"calls {batched} batched several protocol steps into one call — a REPL turn is one evaluation")
        keys = [a.key for a in attempts if a.returned]
        ok = keys == EXPECTED_KEYS
        return Result("repl_protocol_complete", ok,
                      f"ok: all {len(PROTOCOL)} steps, in order, one call each" if ok else
                      f"returned steps were {keys} — want {EXPECTED_KEYS}")
    return grader


_FORMS = {
    "setup": re.compile(r"\bready\b"),
    "count": re.compile(r"\bcount=\d+"),
    "recover": re.compile(r"\blines=\d+"),
    "eval": re.compile(r"\banswer=42\b"),
}


def _direct_results(channel: str) -> Callable[[Trace], Result]:
    def grader(t: Trace) -> Result:
        steps = _steps(t, channel)
        if not steps:
            return Result("repl_direct_results", False, "no protocol step returned a result")
        missing = [f"{a.key}@{a.index}" for a in steps if a.key in _FORMS and not _FORMS[a.key].search(_out(a.call, channel))]
        ok = not missing
        return Result("repl_direct_results", ok,
                      f"ok: each of {len(steps)} steps carried its own output in its own result" if ok else
                      f"these steps returned without their output in the result: {missing}")
    return grader


def _state_carries(channel: str) -> Callable[[Trace], Result]:
    def grader(t: Trace) -> Result:
        steps = _steps(t, channel)
        counts = [m.group(1) for a in steps if a.key == "count"
                  for m in [re.search(r"\bcount=(\d+)", _out(a.call, channel))] if m]
        want = [str(k) for k in range(1, N_COUNT + 1)]
        rec = [a for a in steps if a.key == "recover"]
        rec_out = _out(rec[0].call, channel) if rec else ""
        workdir = _workdir(steps)
        lines_ok = re.search(rf"\blines={N_COUNT}\b", rec_out) is not None
        cwd_ok = re.search(rf"/{re.escape(workdir)}\s*$", rec_out.strip(), re.MULTILINE) is not None
        ok = counts == want and bool(rec) and lines_ok and cwd_ok
        if ok:
            return Result("repl_state_carries", True,
                          f"ok: the counter FILE went 1…{N_COUNT} across separate calls; after the error the log still had "
                          f"{N_COUNT} lines and the WORKING DIRECTORY was still /{workdir}")
        why = []
        if counts != want:
            why.append(f"counts {counts}, want {want} (the file did not carry between calls)")
        if not rec:
            why.append("the recover step never returned")
        else:
            if not lines_ok:
                why.append(f"recover did not print lines={N_COUNT}")
            if not cwd_ok:
                why.append(f"recover's pwd did not end in /{workdir} (the working directory did not carry)")
        return Result("repl_state_carries", False, "; ".join(why))
    return grader


def _env_carries(channel: str) -> Callable[[Trace], Result]:
    def grader(t: Trace) -> Result:
        rec = [a for a in _steps(t, channel) if a.key == "recover"]
        if not rec:
            return Result("repl_env_carries", False, "the recover step never returned")
        ok = f"mark={MARK}" in _out(rec[0].call, channel)
        return Result("repl_env_carries", ok,
                      f"ok: REPL_MARK exported in step 1 was still set in step {N_COUNT + 3}" if ok else
                      "REPL_MARK was not set at the recover step — environment variables did not carry between calls"
                      + (" (expected on the local Bash tool, which drops env between calls)" if channel == "local" else ""))
    return grader


_LOCAL_FAILURE = re.compile(r"no such file|cannot open|exit code [1-9]", re.IGNORECASE)


def _fail_soft(channel: str) -> Callable[[Trace], Result]:
    def grader(t: Trace) -> Result:
        steps = _steps(t, channel)
        keys = [a.key for a in steps]
        if "error" not in keys:
            return Result("repl_fail_soft", False, "the error step never returned a result (the session did not survive it)")
        k = keys.index("error")
        err = steps[k]
        r = err.call.result or {}
        if channel == "bridge":
            failed_as_result = r.get("exit_code") not in (None, 0)
        else:
            failed_as_result = _LOCAL_FAILURE.search(_out(err.call, channel)) is not None
        if not failed_as_result:
            return Result("repl_fail_soft", False,
                          f"the error step (call {err.index}) did not come back as a FAILED evaluation: {str(r)[:160]}")
        nxt = steps[k + 1] if k + 1 < len(steps) else None
        survived = (nxt is not None and nxt.key == "recover"
                    and re.search(rf"\blines={N_COUNT}\b", _out(nxt.call, channel)) is not None)
        return Result("repl_fail_soft", survived,
                      "ok: the failing command returned as a failure and the next step still saw the session's state"
                      if survived else "after the failing step, the next step did not succeed with state intact")
    return grader


def _no_ceremony(channel: str) -> Callable[[Trace], Result]:
    def grader(t: Trace) -> Result:
        attempts, _ = _attempts(t, channel)
        if not attempts:
            return Result("repl_no_ceremony", False, "no protocol step was attempted")
        lo, hi = attempts[0].index, attempts[-1].index
        lifecycle = [f"{c.name}@{i}" for i, c in enumerate(t.calls) if lo < i < hi and c.name in _CEREMONY]
        unreturned = [f"{a.key}@{a.index}:{(a.call.result or {}).get('phase')}" for a in attempts if not a.returned]
        retries = len(attempts) - len(PROTOCOL) - len(unreturned)
        bad = []
        if lifecycle:
            bad.append(f"lifecycle calls inside the protocol: {lifecycle}")
        if unreturned:
            bad.append(f"steps that did not return a result and had to be tried again: {unreturned}")
        if retries > 0:
            bad.append(f"{retries} step(s) re-run after returning")
        return Result("repl_no_ceremony", not bad,
                      "ok: one call per step, no lifecycle call, no cold or running result inside the protocol"
                      if not bad else "; ".join(bad))
    return grader


def _local_only(t: Trace) -> Result:
    hpc = [f"{c.name}@{i}" for i, c in enumerate(t.calls) if c.name in _HPC_TOOLS]
    bash = t.named("Bash")
    ok = not hpc and bool(bash)
    return Result("repl_local_only", ok,
                  f"ok: {len(bash)} Bash calls and no hpc-bridge call — an uncontaminated local reference" if ok else
                  (f"the baseline called hpc-bridge: {hpc}" if hpc else "the baseline made no Bash call"))


# --- latency ----------------------------------------------------------------------------------------------------------

def _p95(xs: list[float]) -> float:
    s = sorted(xs)
    return s[max(0, min(len(s) - 1, round(0.95 * len(s) + 0.5) - 1))]


def metrics(t: Trace, channel: str) -> dict[str, Any]:
    """Latency over the returned protocol steps. `setup_s` is the one-off first step; `warm_*` is the TOOL's latency on
    the rest. `turn_p50_s` is the whole loop turn as the agent lives it — from one step's call to the next step's call
    (tool latency + the agent's own time to choose the next step) — and `agent_p50_s` is that agent time alone. The turn
    is the REPL-feel number: locally the tool is ~1 % of a turn, so a tool-latency ratio alone overstates the difference."""
    steps = _steps(t, channel)
    lat = [(a.key, a.call.latency_s) for a in steps]
    measured = [x for _, x in lat if x is not None]
    out: dict[str, Any] = {"channel": channel, "steps_returned": len(steps), "measured": len(measured)}
    if not measured or lat[0][1] is None:
        return {**out, "unmeasured": True}
    warm = [x for _, x in lat[1:] if x is not None]
    out.update(setup_s=round(lat[0][1], 2), unmeasured=False)
    if warm:
        out.update(warm_n=len(warm), warm_p50_s=round(statistics.median(warm), 2), warm_p95_s=round(_p95(warm), 2),
                   warm_max_s=round(max(warm), 2))
        third = len(warm) // 3
        if third >= 2:
            early, late = statistics.median(warm[:third]), statistics.median(warm[-third:])
            out["drift"] = round(late / early, 2) if early > 0 else None
    pairs = [(a.call, b.call) for a, b in pairwise(steps[1:])]   # warm turns: from the 2nd step on
    turns = [b.t_call - a.t_call for a, b in pairs if a.t_call is not None and b.t_call is not None]
    agent = [b.t_call - a.t_result for a, b in pairs if a.t_result is not None and b.t_call is not None]
    if turns:
        out["turn_p50_s"] = round(statistics.median(turns), 2)
    if agent:
        out["agent_p50_s"] = round(statistics.median(agent), 2)
    return out


def _latency(channel: str) -> Callable[[Trace], Result]:
    bound = LATENCY_P50_BOUND_S[channel]

    def grader(t: Trace) -> Result:
        m = metrics(t, channel)
        if m.get("unmeasured"):
            return Result("repl_latency", False, "unmeasured: this run recorded no arrival stamps")
        if "warm_p50_s" not in m:
            return Result("repl_latency", False, f"only the setup step was measured ({m.get('setup_s')} s)")
        ok = m["warm_p50_s"] <= bound
        drift = f" · drift {m['drift']}×" if m.get("drift") is not None else ""
        turn = (f" · whole turn p50 {m['turn_p50_s']} s (agent {m.get('agent_p50_s')} s)"
                if m.get("turn_p50_s") is not None else "")
        return Result("repl_latency", ok,
                      f"tool p50 {m['warm_p50_s']} s · p95 {m['warm_p95_s']} s · max {m['warm_max_s']} s over "
                      f"{m['warm_n']} turns{drift}{turn} · setup {m['setup_s']} s (provisional bound tool p50 ≤ {bound} s)")
    return grader


def graders(channel: str) -> list[Callable[[Trace], Result]]:
    """Every REPL grader for a channel. Gate them by name in the scenario's EXPECT_OK."""
    if channel not in CHANNELS:
        raise ValueError(f"unknown channel {channel!r}")
    gs = [_protocol_complete(channel), _direct_results(channel), _state_carries(channel), _env_carries(channel),
          _fail_soft(channel), _no_ceremony(channel), _latency(channel)]
    return [*gs, _local_only] if channel == "local" else gs


GATED = {
    "bridge": ["repl_protocol_complete", "repl_direct_results", "repl_state_carries", "repl_env_carries",
               "repl_fail_soft", "repl_no_ceremony"],
    "local": ["repl_local_only", "repl_protocol_complete", "repl_direct_results", "repl_state_carries",
              "repl_fail_soft", "repl_no_ceremony"],
}

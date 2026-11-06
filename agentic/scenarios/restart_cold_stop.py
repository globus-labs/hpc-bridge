"""restart_cold_stop — a NEW server whose first billed-relevant call is stop_endpoint releases the block an earlier
server left running, and answers `down` only once it is gone. Fake cluster, `default` profile, SSH bring-up; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh restart_cold_stop

WHY (free agent chains on the fake cluster, 2026-11): each phase of a chain cell is a new server process on the same
state directory, with no teardown between. A new session's stop_endpoint before any connect answered `down`, "no
endpoint was up", `spend_stopped` None — and released nothing: job 33 billed on (world:stop_honesty_no_pilot_left
caught it). What hpc-bridge knew of its blocks lived only in the server process. Now every billed block request is
recorded in the ledger (hpc_bridge.earlier), and a stop with nothing bound reaches each recorded endpoint again, over
its login endpoint (no SSH), and releases its blocks.

THE CHAIN (two scripted phases, each its own server on the same state directory):
- phase 0 (the premise) — connect from the probe's proposal, confirm one block and wait until it is up (its answer
  names the block's scheduler job), then end WITHOUT a stop: the block runs on (it idle-releases ~10 min after its last
  task, far past the phase change).
- phase 1 — a fresh server whose FIRST call is stop_endpoint: no connect, nothing bound. Called again while it answers
  `draining` (a cold release channel warms), a bounded few times.

GRADED:
- `cold_stop_released` (gated): phase 1's first call is the stop; a stop of it names phase 0's job and says it released
  it, and its last answer is `down` with `spend_stopped` true — never "no endpoint was up".
- `cold_stop_honest` (gated): no phase-1 stop claims `down` without `spend_stopped` true, nor before one of them said it
  released phase 0's job.
- `world:cold_stop_block_gone`: phase 0's job is no longer queued or running when the chain is done (an ending job,
  Slurm's CG, waited out), read before the teardown; beside the universal world:stop_honesty_no_pilot_left.

ON MAIN (before the fix) phase 1's stop answers `down`, "no endpoint was up", and releases nothing: `cold_stop_released`
and `world:cold_stop_block_gone` fail (the block bills until it idle-releases).
"""
from __future__ import annotations

import asyncio
import re
import time

import batch_checks as bc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = ("a fresh server's first call is stop_endpoint: it releases the block an earlier server left running, and says "
           "down only once it is gone")
TAGS = ["fake", "restart", "chain", "spend", "stop", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
INTERPHASE_DELAY_S = 5          # nothing to register: phase 1 binds nothing, and the block runs on meanwhile
COLD_STOP_TRIES = 5             # phase 1's stop again while it drains (a cold release channel warms)
GONE_WAIT_S = 90.0              # how long the world check waits for phase 0's job to leave the queue (CG: KillWait 30)

PROMPT = ("(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` "
          "— start one compute block and leave it running; then, from a fresh server, stop spending before anything "
          "else.")


async def phase_0(ops) -> str:
    """The earlier session: a block up, left running (no stop)."""
    await bc.connect_byo(ops)
    up = await bc.block_up(ops)
    job = str(up.get("block_job_id") or "")
    if not job:
        ops.abort(f"the block came up but its answer named no scheduler job: {str(up.get('notice'))[:240]!r}")
    ops.say(f"phase 0: block job {job} is up; this session ends without a stop, and the block runs on")
    return f"block job {job} up, left running"


async def phase_1(ops) -> str:
    """A fresh server: stop_endpoint first, again while it drains."""
    s = await ops.call("stop_endpoint")
    for _ in range(COLD_STOP_TRIES):
        if s.get("status") != "draining":
            break
        await ops.sleep(bc.POLL_S * 2)
        s = await ops.call("stop_endpoint")
    ops.say(f"phase 1: the stop said {s.get('status')!r}, spend_stopped={s.get('spend_stopped')!r}")
    return f"phase 1's stop said {s.get('status')!r} (spend_stopped={s.get('spend_stopped')!r})"


PHASES = [
    {"script": phase_0, "premise": True, "label": "phase 0 (a block left running)"},
    {"script": phase_1, "label": "phase 1 (a fresh server: stop first)"},
]


# ---- graders ------------------------------------------------------------------------------------------------------------

# What a stop says it cancelled: its release's own detail, `released <ids>` (as run.py's universal check reads it).
_RELEASED = re.compile(r"(?:^|\()released((?: +\d+(?:_\d+)?)+)")


def phase0_job(t: Trace) -> str | None:
    """The scheduler job phase 0's block came up as (its last `up` answer's `block_job_id`)."""
    job = None
    for _i, c in t.named("ensure_endpoint_up", phase=0):
        r = c.result or {}
        if r.get("status") == "up" and r.get("block_job_id"):
            job = str(r["block_job_id"])
    return job


def _notice(r: dict | None) -> str:
    return str((r or {}).get("notice") or "")


def released_ids(r: dict | None) -> list[str]:
    return [j for m in _RELEASED.finditer(_notice(r)) for j in m.group(1).split()]


def cold_stop_released(t: Trace) -> Result:
    name = "cold_stop_released"
    job = phase0_job(t)
    if job is None:
        return Result(name, False, "phase 0 named no block job: nothing to judge (its premise)")
    first = next((c for c in t.calls if c.phase == 1), None)
    if first is None or first.name != "stop_endpoint":
        return Result(name, False, f"phase 1's first call was {getattr(first, 'name', None)!r}, not stop_endpoint")
    stops = [c.result or {} for _i, c in t.named("stop_endpoint", phase=1)]
    said = [f"{r.get('status')}/{r.get('spend_stopped')}" for r in stops]
    if any(_notice(r).strip() == "no endpoint was up" for r in stops):
        return Result(name, False, f"a phase-1 stop said 'no endpoint was up' while job {job} ran: {said}")
    named = any(job in _notice(r) for r in stops)
    released = any(job in released_ids(r) for r in stops)
    last = stops[-1]
    ok = named and released and last.get("status") == "down" and last.get("spend_stopped") is True
    return Result(name, ok, f"job {job}: named={named}, released={released}, the stops said {said}")


def cold_stop_honest(t: Trace) -> Result:
    name = "cold_stop_honest"
    job = phase0_job(t)
    bad = []
    released = False
    for i, c in t.named("stop_endpoint", phase=1):
        r = c.result or {}
        released = released or (job is not None and job in released_ids(r))
        if r.get("status") == "down" and (r.get("spend_stopped") is not True or not released):
            bad.append(i)
    return Result(name, not bad, "ok" if not bad else
                  f"a phase-1 stop said down before job {job} was released, or without spend_stopped true: calls {bad}")


def job_state_cmd(job: str) -> str:
    """The job's state as the scheduler lists it now, with squeue's own error text and its exit status (HPCB_RC): a
    squeue that fails is never an empty listing (review round 1: `2>/dev/null || true` read slurmctld down as gone)."""
    return f'squeue -h -j {job} -o %t 2>&1; echo "HPCB_RC=$?"'


_INVALID_JOB = re.compile(r"invalid job id", re.I)   # squeue -j on a job the controller no longer knows (it ended)


def gone_verdict(job: str, out: str) -> tuple[str, str]:
    """(verdict, what): "gone" — not listed by a squeue that answered (rc 0 and no row, or its "Invalid job id"),
    "ending" — CG (wait), "listed" — still queued or running, "unreadable" — squeue failed otherwise (UNVERIFIABLE)."""
    m = re.search(r"HPCB_RC=(\d+)", out)
    if m is None:
        return "unreadable", f"UNVERIFIABLE: squeue's exit status was not read: {out.strip()[-200:]!r}"
    lines = [ln.strip() for ln in out[:m.start()].splitlines() if ln.strip()]
    if m.group(1) != "0":
        if any(_INVALID_JOB.search(ln) for ln in lines):
            return "gone", f"job {job} is gone (squeue: invalid job id)"
        return "unreadable", f"UNVERIFIABLE: squeue failed (rc={m.group(1)}): {' | '.join(lines)[:200]!r}"
    if not lines:
        return "gone", f"job {job} is gone from the queue"
    if all(s == "CG" for s in lines):
        return "ending", f"job {job} is ending (CG)"
    return "listed", f"job {job} is still listed: {lines}"


async def cold_stop_block_gone(ctx) -> Result:
    """world:cold_stop_block_gone — phase 0's block is not queued or running after the chain (an ending one waited out
    for up to GONE_WAIT_S), read before the harness' teardown."""
    name = "world:cold_stop_block_gone"
    job = phase0_job(ctx.trace)
    if job is None:
        return Result(name, False, "UNVERIFIABLE: phase 0 named no block job")
    deadline = time.monotonic() + GONE_WAIT_S
    while True:
        rc, out = await asyncio.to_thread(ctx.ssh, job_state_cmd(job), 60)
        if rc != 0 and "HPCB_RC=" not in out:   # the world channel itself failed: the command never ran
            return Result(name, False, f"UNVERIFIABLE: the world channel failed (rc={rc}): {out.strip()[:200]!r}")
        verdict, what = gone_verdict(job, out)
        if verdict != "ending" or time.monotonic() > deadline:
            return Result(name, verdict == "gone", what)
        await _pause(5.0)


async def _pause(seconds: float) -> None:   # a seam the hermetic test drives
    await asyncio.sleep(seconds)


EXTRA_INVARIANTS = [cold_stop_released, cold_stop_honest]
WORLD_CHECKS = [cold_stop_block_gone]
EXPECT_OK = ["agent_engaged", "cold_stop_released", "cold_stop_honest", "stop_is_honest", "ends_with_stop"]

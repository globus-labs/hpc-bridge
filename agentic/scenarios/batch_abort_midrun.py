"""batch_abort_midrun — cancel(abort) ends what is in flight, not only the backlog (design §5: "drop the backlog as
cancelled, then end what is in flight"; the stage-A review's correction, 2026-10-31). Fake cluster, `default`
profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_abort_midrun

In phase 1 the batch shares the session's block, so an abort never cancels the block or ends its user endpoint
(interactive work runs there too): it writes the CANCELLED marker in the results directory, over the free login shape,
and each running wrapper — watching it every second — kills its command's process group and settles the attempt
`cancelled` (rc 143).

THE SCRIPT: connect, confirm one block, run_batch 4 rows of `sleep 600` (task_walltime_s 900); once the first one has
started on the block (its start file is there), cancel(abort); cancel again until it answers `down`; read every row;
stop.

GRADED: the first abort answers `draining` with the backlog dropped and a task in flight; a later cancel answers `down`
within 60 s; the row that was out settled `cancelled` on the marker's evidence, and every row is cancelled
(`abort_ends_in_flight`); the attempt's rc on the facility is 143, written well before its `sleep 600` could end
(`world:aborted_attempt_rc`); stop answers down; the ledger agrees with sacct.

ON MAIN there is no cancel: the fallback's rows run as run_shell calls, and nothing ends them.
"""
from __future__ import annotations

import json
import re
import time

import batch_checks as bc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch phase 1: cancel(abort) kills the task in flight within seconds (the CANCELLED marker) and drops the backlog; the block stays the session's"
TAGS = ["fake", "batch", "cancel", "stop", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

TASK_WALLTIME_S = 900
N_ROWS = 4
COMMAND = "sleep 600; echo row {n} finished"
ROWS = [{"n": i} for i in range(N_ROWS)]
START_WAIT_S = 300.0
DOWN_WAIT_S = 90.0

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "confirm one block, run 4 rows of `sleep 600` through run_batch, abort the batch once the first is running, and "
    "stop."
)


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r, main = await bc.fanout(ops, COMMAND, ROWS, TASK_WALLTIME_S, main_poll_until_s=60)
    if main is not None:
        s = await bc.stop(ops)
        return f"main fallback; stop said {s.get('status')!r}"
    bid, rd = str(r.get("batch_id") or ""), str(r.get("results_dir") or "")
    if not bid:
        ops.abort(f"run_batch refused: {r.get('phase')!r} {r.get('reason') or r.get('notice')!r}")
    deadline = time.monotonic() + START_WAIT_S
    while True:   # the first row's start file: its command is running on the block
        rc, out = await ops.world(f"ls {rd}/tasks/*/attempt-*/meta.json 2>/dev/null | wc -l")
        if re.search(r"^\s*[1-9]", out or "", re.M):
            break
        if time.monotonic() > deadline:
            ops.abort(f"no row started within {START_WAIT_S:.0f} s (rc={rc}: {out[-200:]!r})")
        await ops.sleep(3)
    await ops.sleep(3)
    first = await ops.call("cancel", id=bid, mode="abort")
    ops.say(f"the first abort: {first.get('status')!r}, {first.get('in_flight')} in flight, "
            f"{first.get('backlog_dropped')} dropped — {str(first.get('notice'))[:200]}")
    deadline = time.monotonic() + DOWN_WAIT_S
    last = first
    while last.get("status") != "down" and time.monotonic() < deadline:
        await ops.sleep(5)
        last = await ops.call("cancel", id=bid, mode="abort")
    got = (await bc.all_rows(ops, bid, N_ROWS))["rows"]
    ops.say("rows: " + json.dumps({i: (x.get("state"), x.get("exit_code"), x.get("settled_by")) for i, x in got.items()}))
    s = await bc.stop(ops)
    return f"abort: {first.get('status')!r} then {last.get('status')!r}; stop said {s.get('status')!r}"


def abort_ends_in_flight(t: Trace) -> Result:
    return bc.abort_ends_in_flight(t, rows=N_ROWS)


async def aborted_attempt_rc(ctx) -> Result:
    """world: the attempt that was out when the batch was aborted ended with rc 143, minutes before its `sleep 600`
    could have — its command was killed, not let run on."""
    name = "world:aborted_attempt_rc"
    rd = bc.results_dir_of(ctx.trace)
    if not rd:
        return Result(name, False, "premise not met: the run named no results directory")
    rc, out = ctx.ssh(f'for d in {rd}/tasks/*/attempt-*; do [ -f "$d/meta.json" ] && '
                      'echo "HPCB_ATT $d $(cat "$d/rc" 2>/dev/null) $(tr -d "\\n" < "$d/meta.json") '
                      '$(stat -c %Y "$d/rc" 2>/dev/null)"; done; echo HPCB_DONE', 60)
    if "HPCB_DONE" not in out:
        return Result(name, False, f"UNVERIFIABLE: could not list {rd} (rc={rc}): {out[-200:]!r}")
    seen = []
    for m in re.finditer(r"^HPCB_ATT (\S+) (\S*) (\{.*\}) (\d*)$", out, re.M):
        try:
            meta = json.loads(m.group(3))
        except ValueError:
            continue
        took = (int(m.group(4)) - int(meta.get("start") or 0)) if m.group(4) else None
        seen.append((m.group(1), m.group(2), took))
    ok = bool(seen) and all(rcv == "143" and took is not None and took < 300 for _p, rcv, took in seen)
    return Result(name, ok, "; ".join(f"{p.rsplit('/', 2)[-2]}: rc={rcv} after {took}s" for p, rcv, took in seen)
                  or "no attempt started")


EXTRA_INVARIANTS = [abort_ends_in_flight, bc.stop_down]
WORLD_CHECKS = [aborted_attempt_rc, bc.ledger_matches_sacct]
EXPECT_OK = ["agent_engaged", "abort_ends_in_flight", "stop_down", "ends_with_stop"]

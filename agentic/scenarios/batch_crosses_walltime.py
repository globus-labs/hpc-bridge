"""batch_crosses_walltime — a batch longer than its block: the admission gate and the re-ask at the block's end (batch
phase 1, design §5 "the admission gate", review finding 3). Fake cluster, `default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_crosses_walltime

A 4-minute block (details walltime 00:04:00), tasks of task_walltime_s = 60 (each `sleep 50`), 9 rows. The design's
cell has 30 rows; on the default profile's 2 workers the window is 1 (one worker kept for interactive work), so 30 rows
would cross ten blocks — 9 cross two block ends (three tasks fit a 4-minute block above the 60 s + margin gate), and on
main's 2 workers the ninth row starts with 40 s left.

THE SCRIPT: connect from the probe's proposal with the 4-minute walltime, confirm ONE block (ensure_endpoint_up), then
run_batch. Each time the batch pauses at needs_confirmation naming the walltime, the script — the user — says yes:
run_batch(batch_id=…, confirm_spend=True). It collects every row and stops.

GRADED: no task admitted with remaining walltime under 60 s + the margin (`no_admission_under_margin`, the journal's
admissions; `world:tasks_started_with_margin`, each attempt's own reading at its start); the re-ask names the walltime
and nothing is admitted between it and the yes (`reask_on_walltime`); sacct shows no new pilot before its own
confirmation (`world:no_job_before_reask`); the ledger agrees with sacct; and the universal
`world:stop_honesty_no_pilot_left`.

ON MAIN (no run_batch) the script's fallback sends the rows as concurrent run_shell calls: nothing gates on the block's
walltime, the rows queued at the interchange outlive the block, and parsl submits the next pilot for them with no
re-ask (#204's pattern) — `world:no_job_before_reask` and `world:tasks_started_with_margin` fail.
"""
from __future__ import annotations

import batch_checks as bc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch phase 1: a batch longer than its 4-minute block is gated on remaining walltime and asks again at each block end"
TAGS = ["fake", "batch", "spend", "walltime", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}   # main's fallback: run_shell hands back a poll handle quickly

WALLTIME = "00:04:00"
TASK_WALLTIME_S = 60
N_ROWS = 9
COMMAND = "sleep 50; echo row {n}"
ROWS = [{"n": i} for i in range(N_ROWS)]
MAX_ASKS = 6

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "bring up the login shape with a 4-minute block walltime, confirm one block, run 9 rows of 50 s each through "
    "run_batch, say yes each time the batch asks again at a block's end, collect every row and stop."
)


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops, walltime=WALLTIME)
    await bc.block_up(ops)
    r, main = await bc.fanout(ops, COMMAND, ROWS, TASK_WALLTIME_S, main_poll_until_s=720)
    if main is not None:
        ops.say(f"main: {len(main.results)} row answer(s), {len(main.handles)} still running")
        s = await bc.stop(ops)
        return f"main fallback; stop said {s.get('status')!r}"
    bid = str(r.get("batch_id") or "")
    if not bid:
        ops.abort(f"run_batch refused: {r.get('phase')!r} {r.get('reason') or r.get('notice')!r}")
    asks = 0
    st = r
    while st.get("phase") not in ("done", "cancelled"):
        st = await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in ("needs_confirmation", "done",
                                                                            "cancelled", "refused"), timeout_s=900)
        if st.get("phase") == "needs_confirmation":
            asks += 1
            if asks > MAX_ASKS:
                ops.abort(f"the batch asked again {asks} times")
            ops.say(f"the batch asked again ({st.get('reason')!r}); the user says yes")
            st = await ops.call("run_batch", batch_id=bid, confirm_spend=True)
            if st.get("phase") in ("refused", "needs_account") or st.get("is_error"):
                ops.abort(f"the resume was refused: {st.get('phase')!r} {st.get('reason') or st.get('text')!r}")
        elif st.get("phase") not in ("done", "cancelled"):
            ops.abort(f"the batch did not finish: {st.get('phase')!r} {st.get('reason')!r}")
    got = (await bc.all_rows(ops, bid, N_ROWS))["rows"]
    s = await bc.stop(ops)
    return (f"{sum(1 for x in got.values() if x.get('state') == 'done')}/{N_ROWS} rows done after {asks} re-ask(s); "
            f"stop said {s.get('status')!r} (spend_stopped={s.get('spend_stopped')!r})")


def reask_on_walltime(t: Trace) -> Result:
    return bc.reask_named(t, words=("walltime",), name="reask_on_walltime")


def every_row_returned(t: Trace) -> Result:
    return bc.every_row_returned(t, rows=N_ROWS)


EXTRA_INVARIANTS = [bc.no_admission_under_margin, reask_on_walltime, every_row_returned]
WORLD_CHECKS = [bc.no_job_before_reask, bc.tasks_started_with_margin, bc.ledger_matches_sacct]
EXPECT_OK = ["agent_engaged", "no_admission_under_margin", "reask_on_walltime", "every_row_returned",
             "ends_with_stop"]

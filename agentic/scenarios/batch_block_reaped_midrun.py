"""batch_block_reaped_midrun — the confirmed block is cancelled under a running batch (a preemption, an admin's scancel)
after 5 results: the rows in flight are settled `lost` on evidence, the backlog survives, and the batch asks again
naming the reap (batch phase 1, design §4 "Settling pending tasks", §10). Fake cluster, `default` profile; scripted
(the polling hook would serve an agent operator too).

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_block_reaped_midrun

THE REAP is a POLLING mid-run hook (midrun_hooks `poll`): armed by the run_batch result, it counts the rows' rc files in
the results directory every 3 s and, at 5, cancels this run's pilot by its `uep.<eid>` marker. Mid-run hooks fire only
on tool results, and nothing ties "5 results" to a call — the poll does (design §10's harness work).

THE SCRIPT: connect, confirm one block, run_batch 10 rows of `sleep 15` into a fixed results directory (the hook reads
it), non-idempotent; wait until the batch asks again; say yes (run_batch(batch_id=…, confirm_spend=True)); collect every
row; stop.

GRADED: the rows in flight at the reap are `lost`, settled on evidence (`inflight_lost_with_evidence`); at the re-ask the
backlog is still there and every row is counted (`backlog_survives`); the re-ask names the reap and nothing is admitted
before the yes (`reask_names_reap`); every row is settled at the end; the ledger agrees with sacct.

ON MAIN the fallback's rows are concurrent run_shell calls: no batch answers at all — nothing settles a row lost, no
backlog is kept, no batch asks again.
"""
from __future__ import annotations

import batch_checks as bc
from invariants import Result, Trace

OPERATORS = ("script",)   # the polling hook serves agent operators too; this cell is a scripted proof
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch phase 1: the block is scancel'd after 5 results — in-flight rows lost on evidence, the backlog kept, the re-ask names the reap"
TAGS = ["fake", "batch", "reap", "chaos", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

TASK_WALLTIME_S = 60
N_ROWS = 10
REAP_AFTER = 5
COMMAND = "sleep 15; echo row {n}"
ROWS = [{"n": i} for i in range(N_ROWS)]
RESULTS = "$HOME/hpcb-batches/{endpoint_name}/reaped"   # the hook fills {endpoint_name}; the script reads its env

MIDRUN_HOOKS = [{
    "name": "reap_after_5_results",
    "after_tool": "run_batch",
    "poll": f"ls {RESULTS}/tasks/*/attempt-*/rc {RESULTS}/batch-*/tasks/*/attempt-*/rc 2>/dev/null | wc -l",
    "until": r"^\s*([5-9]|[1-9][0-9]+)\s*$",
    "every_s": 3,
    "poll_timeout_s": 900,
    "cmd": ("ids=$(squeue -u \"$USER\" -h -o '%i|%o' | awk -F'|' -v m='uep.{eid}' 'index($2,m){print $1}'); "
            "[ -n \"$ids\" ] && scancel $ids && echo \"reaped $ids\""),
    "expect_any_host": "reaped",
}]

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "confirm one block, run 10 rows through run_batch; the harness cancels the block after 5 results; say yes to the "
    "re-ask, collect every row and stop."
)


def _results_dir() -> str:
    import os

    return RESULTS.replace("{endpoint_name}", os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip() or "unnamed")


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r, main = await bc.fanout(ops, COMMAND, ROWS, TASK_WALLTIME_S, results_dir=_results_dir(), main_poll_until_s=600)
    if main is not None:
        s = await bc.stop(ops)
        return f"main fallback; stop said {s.get('status')!r}"
    bid = str(r.get("batch_id") or "")
    if not bid:
        ops.abort(f"run_batch refused: {r.get('phase')!r} {r.get('reason') or r.get('notice')!r}")
    st = await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in ("needs_confirmation", "done", "cancelled",
                                                                        "refused"), timeout_s=900)
    if st.get("phase") != "needs_confirmation":
        ops.abort(f"the batch never asked again after the reap: {st.get('phase')!r} {st.get('counts')}")
    ops.say(f"the batch asked again: {st.get('reason')!r}, counts {st.get('counts')}; the user says yes")
    res = await ops.call("run_batch", batch_id=bid, confirm_spend=True)
    if res.get("phase") in ("refused", "needs_account") or res.get("is_error"):
        ops.abort(f"the resume was refused: {res.get('phase')!r} {res.get('reason') or res.get('text')!r}")
    await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in ("done", "cancelled", "refused",
                                                                    "needs_confirmation"), timeout_s=900)
    got = (await bc.all_rows(ops, bid, N_ROWS))["rows"]
    s = await bc.stop(ops)
    lost = sorted(i for i, x in got.items() if x.get("state") == "lost")
    return (f"lost {lost}; {sum(1 for x in got.values() if x.get('state') == 'done')}/{N_ROWS} done; "
            f"stop said {s.get('status')!r}")


def backlog_survives(t: Trace) -> Result:
    return bc.backlog_survives(t, rows=N_ROWS)


def reask_names_reap(t: Trace) -> Result:
    return bc.reask_named(t, words=("reap", "cancel", "gone", "ended", "lost"), name="reask_names_reap")


def every_row_returned(t: Trace) -> Result:
    return bc.every_row_returned(t, rows=N_ROWS)


EXTRA_INVARIANTS = [bc.inflight_lost_with_evidence, backlog_survives, reask_names_reap, every_row_returned]
WORLD_CHECKS = [bc.ledger_matches_sacct]
EXPECT_OK = ["agent_engaged", "inflight_lost_with_evidence", "backlog_survives", "reask_names_reap",
             "every_row_returned", "ends_with_stop"]

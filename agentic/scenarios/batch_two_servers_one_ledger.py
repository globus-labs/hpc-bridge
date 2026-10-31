"""batch_two_servers_one_ledger — two hpc-bridge servers on ONE state directory (two stdio sessions on one laptop, or
an orphaned server beside a new one: `harness:server_orphaned`). Only the lease holder admits; the window never doubles
(batch phase 1, design §4 "One owner per campaign", review finding 4). Fake cluster, `default` profile; scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_two_servers_one_ledger

THE SCRIPT: server A connects, confirms one block and runs 10 rows through run_batch. Server B (`ops.open_server("B")`:
a second process, same state directory and journal) reads the batch with `status` — from the ledger — and then tries to
resume it, run_batch(batch_id=…, confirm_spend=True): refused before anything starts, naming the owner. A collects every
row and stops.

GRADED: every admission came from one process and B started nothing (`only_holder_admits`); B read the batch
(`second_server_reads_ledger`); the work out at once never passed the window (`window_never_doubles`); no row ran twice;
the ledger agrees with sacct.

ON MAIN there is no ledger and no lease: the fallback has B do what a second agent would — reconnect, confirm its own
block and send the same rows — so the work out at once doubles and two processes dispatch the batch
(`window_never_doubles`, `only_holder_admits` and `world:no_row_ran_twice` fail).
"""
from __future__ import annotations

import batch_checks as bc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch phase 1: two servers on one state directory — only the lease holder admits, the other reads the ledger, the window never doubles"
TAGS = ["fake", "batch", "lease", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

WORKERS = 2
TASK_WALLTIME_S = 60
N_ROWS = 10
COMMAND = "sleep 6; echo row {n}"
ROWS = [{"n": i} for i in range(N_ROWS)]

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "server A runs 10 rows through run_batch on one confirmed block; a second server on the same state directory reads "
    "the batch and tries to resume it (refused); A collects every row and stops."
)


async def _main_second_agent(ops, fan) -> None:
    """MAIN: no ledger to read, no lease to respect — a second server runs the same rows on its own block."""
    b = await ops.open_server("B")
    await bc.reconnect(b)
    await bc.block_up(b)
    await bc.main_fanout(b, COMMAND, ROWS, bc.results_dir_for("two-servers"), poll_until_s=300, attempt=2)
    await bc.stop(b)


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r, main = await bc.fanout(ops, COMMAND, ROWS, TASK_WALLTIME_S, results_dir=bc.results_dir_for("two-servers"),
                              main_after_submit=_main_second_agent, main_poll_until_s=300)
    if main is not None:
        s = await bc.stop(ops)
        return f"main fallback; stop said {s.get('status')!r}"
    bid = str(r.get("batch_id") or "")
    if not bid:
        ops.abort(f"run_batch refused: {r.get('phase')!r} {r.get('reason') or r.get('notice')!r}")
    b = await ops.open_server("B")
    seen = await b.call("status", id=bid)
    ops.say(f"server B's status: phase {seen.get('phase')!r}, tasks {seen.get('tasks')}, counts {seen.get('counts')}, "
            f"owner {seen.get('owner')!r}")
    tried = await b.call("run_batch", batch_id=bid, confirm_spend=True)
    ops.say(f"server B's resume: phase {tried.get('phase')!r} — {tried.get('reason') or tried.get('notice')!r}")
    await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in ("done", "cancelled", "refused",
                                                                    "needs_confirmation"), timeout_s=600)
    got = (await bc.all_rows(ops, bid, N_ROWS))["rows"]
    s = await bc.stop(ops)
    return (f"A: {sum(1 for x in got.values() if x.get('state') == 'done')}/{N_ROWS} done; B's resume "
            f"{tried.get('phase')!r}; stop said {s.get('status')!r}")


def window_never_doubles(t: Trace) -> Result:
    r = bc.window_within_capacity(t, workers=WORKERS)
    return Result("window_never_doubles", r.ok, r.detail)


EXTRA_INVARIANTS = [bc.only_holder_admits, bc.second_server_reads_ledger, window_never_doubles]
WORLD_CHECKS = [bc.no_row_ran_twice, bc.ledger_matches_sacct]
EXPECT_OK = ["agent_engaged", "only_holder_admits", "second_server_reads_ledger", "window_never_doubles",
             "ends_with_stop"]

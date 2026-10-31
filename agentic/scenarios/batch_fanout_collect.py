"""batch_fanout_collect — many independent tasks fanned out and collected through the session's ONE confirmed block
(batch phase 1, vault `Planned/Batch and swarm execution.md` §10). Fake cluster, `default` profile.

40 rows of one command, three of them seeded to fail (exit 3 at n = 7, 19, 33). An agent cell (Pi on the free model,
or any agent operator) is told only what to run and that the user approved ONE block; the script operator drives the
same steps itself (connect, confirm one block, run_batch, collect, stop):

    HPCB_OPERATOR=pi HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_fanout_collect
    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_fanout_collect

GRADED (design §10): one confirmation and one job in sacct (`one_confirmation`, `world:one_block_job`); run_batch
used, not 40 run_shell calls (`batch_used`); the journal never shows more out than the window (`window_within_capacity`:
the default profile's 2 workers, one kept for interactive work — a window of 1); the 3 failures reported with their rc
(`failures_have_rc` from the batch's own results, `agent_reports_failures` in the agent's words); stop answers `down`
(`stop_down`); and the ledger agrees with the scheduler (`world:ledger_matches_sacct`).

ON MAIN there is no run_batch: an agent loops run_shell (or the script's fallback sends the rows as concurrent run_shell
calls, batch_checks.main_fanout) — `batch_used` and `window_within_capacity` fail on their own terms, and there is no
ledger to agree with sacct.
"""
from __future__ import annotations

import batch_checks as bc
from invariants import Result, Trace

TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch phase 1: 40 rows (3 seeded failures) fanned out through run_batch on the ONE confirmed block, collected, stopped down"
TAGS = ["fake", "batch", "spend", "stop"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 40
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"

N_ROWS = 40
FAILING = {7: 3, 19: 3, 33: 3}
WORKERS = 2          # the default profile's max_workers_per_node
TASK_WALLTIME_S = 60
COMMAND = "echo row {n}; case {n} in 7|19|33) exit 3;; esac"
ROWS = [{"n": i} for i in range(N_ROWS)]

PROMPT = (
    "On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — connect (it is not catalogued: "
    "probe the login node and accept the discovered configuration yourself), wait until the login node is up, then "
    "bring up ONE compute block. The user has ALREADY approved ONE compute block and its cost (the default partition, "
    "1 node, the default walltime); you are authorised to confirm that one block's spend, and no other.\n\n"
    "On that compute block, run this shell command once for each value of n from 0 to 39 (40 independent runs):\n\n"
    "    echo row $n; case $n in 7|19|33) exit 3;; esac\n\n"
    "Collect the result of every run. Then report which values of n failed and the exit code of each, and how many "
    "succeeded. Finally stop the endpoint and report exactly what stop_endpoint said.\n\n"
    "This is an AUTOMATED run — no one is available to answer questions."
)


async def SCRIPT(ops) -> str:   # the script operator runs it (harness/script_runner.py)
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r, main = await bc.fanout(ops, COMMAND, ROWS, TASK_WALLTIME_S)
    if main is None:
        bid = str(r.get("batch_id") or "")
        if not bid:
            ops.abort(f"run_batch refused: {r.get('phase')!r} {r.get('reason') or r.get('notice')!r}")
        await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in ("done", "cancelled", "refused",
                                                                        "needs_confirmation"), timeout_s=900)
        got = (await bc.all_rows(ops, bid, N_ROWS))["rows"]
        failed = {i: r.get("exit_code") for i, r in sorted(got.items()) if r.get("state") == "failed"}
        ok = sum(1 for r in got.values() if r.get("state") == "done")
    else:
        failed = {i: r.get("exit_code") for i, r in sorted(main.results.items()) if r.get("exit_code") not in (0, None)}
        ok = sum(1 for r in main.results.values() if r.get("exit_code") == 0)
    ops.say("failed: " + ", ".join(f"n={i} exit code {rc}" for i, rc in failed.items()) + f"; {ok} succeeded")
    s = await bc.stop(ops)
    return (f"{len(failed)} failed ({', '.join(f'n={i} rc {rc}' for i, rc in failed.items())}), {ok} succeeded; "
            f"stop_endpoint said {s.get('status')!r} (spend_stopped={s.get('spend_stopped')!r}).")


def batch_used(t: Trace) -> Result:
    return bc.batch_used(t, rows=N_ROWS)


def window_within_capacity(t: Trace) -> Result:
    return bc.window_within_capacity(t, workers=WORKERS)


def failures_have_rc(t: Trace) -> Result:
    return bc.failures_have_rc(t, failing=FAILING)


def agent_reports_failures(t: Trace) -> Result:
    return bc.agent_reports_failures(t, failing=FAILING)


EXTRA_INVARIANTS = [batch_used, bc.one_confirmation, window_within_capacity, failures_have_rc, agent_reports_failures,
                    bc.stop_down]
WORLD_CHECKS = [bc.ledger_matches_sacct, bc.one_block_job]
EXPECT_OK = ["agent_engaged", "batch_used", "one_confirmation", "window_within_capacity", "failures_have_rc",
             "agent_reports_failures", "stop_down", "ends_with_stop", "stop_is_honest"]

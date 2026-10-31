"""mep_batch_fanout — a batch on a facility multi-user endpoint, stopped mid-run: admission pauses under the lock, the
stop is final `draining` and names the block's jobs, and nothing is sent after it (batch phase 1, design §5 "MEP:
drain only"). Fake cluster, `mep` profile (the strict-schema manager, our identity mapped to `hpcbmep`); scripted.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=mep ./agentic/run_smoke.sh mep_batch_fanout

THE SCRIPT: attach to the catalogued MEP (zero SSH), confirm one block on `compute` charged to `hpcb`, run_batch 8 rows
of `sleep 10`; once 3 are done, stop_endpoint — it refuses while the batch's tasks hold the block, and pauses the batch
first; wait until nothing is in flight; stop again: `draining`, final, naming the block job(s) the results reported.

GRADED: nothing admitted after the stop was called, and no row started after its answer that was not already admitted
(`nothing_admitted_after_stop`); the stop is draining, spend not claimed stopped, and names the block jobs
(`mep_stop_names_jobs`).

ON MAIN the fallback sends every row as a run_shell at once: they queue at the facility's interchange, and the rows
still queued when the stop answers start after it (`nothing_admitted_after_stop` fails) — nothing on a MEP can take
them back.
"""
from __future__ import annotations

import time

import batch_checks as bc

FACILITY_ID = "fake-mep-strict"       # the local catalog's id for the strict-schema manager (as fake_mep_compute)
MAPPED_USER = "hpcbmep"
OPERATORS = ("script",)
NEEDS_COMPUTE_NODE = True
WARM_BLOCK_USER = MAPPED_USER
SERIAL = "hpcbmep"     # a GROUP: every fake-MEP cell runs as hpcbmep — one block, one queue, one scancel -u
TARGETS = ("fake",)
REQUIRES = {"mep": "consent-free", "accounting": "enforce", "partitions": ["compute"], "accounts": ["hpcb"]}
KIND = "regression"
SUMMARY = "batch phase 1 on a facility MEP: a stop mid-batch pauses admission, is final draining naming the block's jobs, and nothing is sent after it"
TAGS = ["fake", "mep", "batch", "stop", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}
ADMIN_CLEANUP = [f"scancel -u {MAPPED_USER} 2>/dev/null || true"]   # as admin: the pool user cannot cancel hpcbmep's jobs

TASK_WALLTIME_S = 60
N_ROWS = 8
STOP_AFTER_DONE = 3
COMMAND = "sleep 10; echo row {n}"
ROWS = [{"n": i} for i in range(N_ROWS)]
DRAIN_WAIT_S = 300.0

PROMPT = (
    "(Scripted — no agent reads this.) Attach to the facility `{facility}` (a multi-user endpoint), confirm one block "
    "on `compute` charged to `hpcb`, run 8 rows through run_batch, stop the endpoint after 3 results, wait for what is "
    "in flight, and stop again: draining, final."
)


async def _main_stop(ops, fan) -> None:
    await ops.sleep(25)
    s = await ops.call("stop_endpoint")
    ops.say(f"main: the first stop said {s.get('status')!r}")


async def SCRIPT(ops) -> str:
    r = await ops.call("connect_facility", facility=ops.facility)
    if r.get("phase") in ("failed", "unsupported") or r.get("is_error"):
        ops.abort(f"the MEP attach failed: {r.get('phase')!r} {r.get('notice') or r.get('text')!r}")
    await bc.block_up(ops, partition="compute", account="hpcb")
    r, main = await bc.fanout(ops, COMMAND, ROWS, TASK_WALLTIME_S, main_after_submit=_main_stop,
                              main_poll_until_s=600)
    if main is not None:
        s = await bc.stop(ops, final_draining=True)
        return f"main fallback; the last stop said {s.get('status')!r}"
    bid = str(r.get("batch_id") or "")
    if not bid:
        ops.abort(f"run_batch refused: {r.get('phase')!r} {r.get('reason') or r.get('notice')!r}")
    await bc.wait_batch(ops, bid, until=lambda s: (s.get("counts") or {}).get("done", 0) >= STOP_AFTER_DONE
                        or s.get("phase") in ("done", "cancelled", "refused"), timeout_s=900, wait_s=10)
    first = await ops.call("stop_endpoint")
    ops.say(f"the first stop: {first.get('status')!r} — {str(first.get('notice'))[:200]!r}")
    deadline = time.monotonic() + DRAIN_WAIT_S
    st = await ops.call("status", id=bid)
    while (st.get("counts") or {}).get("in_flight", 0) > 0 and time.monotonic() < deadline:
        st = await ops.call("status", id=bid, wait_s=20)
    s = await bc.stop(ops, final_draining=True)
    return (f"stopped after {(st.get('counts') or {}).get('done')} done (backlog {(st.get('counts') or {}).get('backlog')}); "
            f"the stop said {s.get('status')!r} (spend_stopped={s.get('spend_stopped')!r}), job {s.get('block_job_id')!r}")


EXTRA_INVARIANTS = [bc.nothing_admitted_after_stop, bc.mep_stop_names_jobs]
EXPECT_OK = ["agent_engaged", "nothing_admitted_after_stop", "mep_stop_names_jobs"]

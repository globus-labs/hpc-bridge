"""batch_restart_resume — the server is KILLED mid-batch; a new one finds every row in the durable ledger and resumes
only on the user's yes (batch phase 1, design §4 "Recovery after a restart", review finding 7). Fake cluster,
`default` profile; scripted. The design calls this a PHASES cell; it is one scripted session with
`ops.restart_server()` (a SIGKILL — no shutdown, nothing flushed — then a new server on the same state directory and
journal), which a PHASES chain (a fresh agent per phase, each server ended cleanly) cannot do.

    HPCB_OPERATOR=script HPCB_TARGET=fake HPCB_FAKE_PROFILE=default ./agentic/run_smoke.sh batch_restart_resume

THE SCRIPT: connect, confirm one block, run_batch 8 rows of `sleep 20` (non-idempotent); once 3 are done, KILL the
server. The new server: `status` first (it must list every row and ask again, admitting nothing: spend_confirmed lived
in the killed process); connect again (zero SSH, the endpoint reused); wait until the row in flight at the kill is
collected (a REST pull — an AMQP ack does not remove the REST copy, S1); then run_batch(batch_id=…, confirm_spend=True)
— the user's yes — collect every row and stop.

GRADED: the new server's status returns every row, settled (`every_row_returned`, after the restart); no row ran twice
(`world:no_row_ran_twice`: one attempt start file per row); admission resumes only after the re-ask
(`nothing_admitted_before_resume`); the ledger agrees with sacct.

ON MAIN the fallback's rows are run_shell poll handles, which mean nothing to the new process: nothing lists the rows
after the restart (`every_row_returned` fails), and no ledger agrees with anything.
"""
from __future__ import annotations

import time

import batch_checks as bc
from invariants import Result, Trace

OPERATORS = ("script",)
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm", "mfa": "none"}
KIND = "regression"
SUMMARY = "batch phase 1: the server is killed mid-batch; the new one lists every row from the ledger, runs none twice, resumes only on a yes"
TAGS = ["fake", "batch", "restart", "spend", "scripted"]
PERSONA = None
USER_GOAL = ""
MAX_TURNS = 1
NEEDS_COMPUTE_NODE = True
TEARDOWN = "delete"
EXTRA_ENV = {"HPC_BRIDGE_SYNC_WAIT_S": "10"}

TASK_WALLTIME_S = 60
N_ROWS = 8
KILL_AFTER_DONE = 3
COMMAND = "sleep 20; echo row {n}"
ROWS = [{"n": i} for i in range(N_ROWS)]
TAKEOVER_WAIT_S = 180.0

PROMPT = (
    "(Scripted — no agent reads this.) On the HPC facility with login host `{ssh_host}` — facility id `{facility}` — "
    "confirm one block, run 8 rows through run_batch, kill the server once 3 are done, and from a new server read the "
    "batch back, resume it on the user's yes, collect every row and stop."
)


async def _main_restart(ops, fan) -> None:
    await ops.sleep(15)
    await ops.restart_server()
    await bc.reconnect(ops)


async def SCRIPT(ops) -> str:
    await bc.connect_byo(ops)
    await bc.block_up(ops)
    r, main = await bc.fanout(ops, COMMAND, ROWS, TASK_WALLTIME_S, main_after_submit=_main_restart,
                              main_poll_until_s=300)
    if main is not None:
        ops.say(f"main: {len(main.results)} row answer(s) after the restart, {len(main.handles)} unresolved")
        s = await bc.stop(ops)
        return f"main fallback; stop said {s.get('status')!r}"
    bid = str(r.get("batch_id") or "")
    if not bid:
        ops.abort(f"run_batch refused: {r.get('phase')!r} {r.get('reason') or r.get('notice')!r}")
    st = await bc.wait_batch(ops, bid, until=lambda s: (s.get("counts") or {}).get("done", 0) >= KILL_AFTER_DONE
                             or s.get("phase") in ("done", "cancelled", "refused"), timeout_s=600, wait_s=10)
    if st.get("phase") in ("done", "cancelled", "refused"):
        ops.abort(f"the batch ended before the kill: {st.get('phase')!r}")
    await ops.restart_server()
    first = await ops.call("status", id=bid, detail="page", limit=100)
    ops.say(f"the new server's first status: phase {first.get('phase')!r}, counts {first.get('counts')}, "
            f"reason {first.get('reason')!r}, owner {first.get('owner')!r}")
    await bc.reconnect(ops)
    # the old process is dead; its lease goes stale, the new server takes it over and pulls what finished meanwhile
    deadline = time.monotonic() + TAKEOVER_WAIT_S
    st = first
    while time.monotonic() < deadline and (st.get("owner") or (st.get("counts") or {}).get("in_flight", 0) > 0):
        await ops.sleep(bc.POLL_S)
        st = await ops.call("status", id=bid)
    ops.say(f"before the resume: counts {st.get('counts')}, owner {st.get('owner')!r}; the user says yes")
    res = await ops.call("run_batch", batch_id=bid, confirm_spend=True)
    if res.get("phase") in ("refused", "needs_account") or res.get("is_error"):
        ops.abort(f"the resume was refused: {res.get('phase')!r} {res.get('reason') or res.get('text')!r}")
    await bc.wait_batch(ops, bid, until=lambda s: s.get("phase") in ("done", "cancelled", "refused",
                                                                    "needs_confirmation"), timeout_s=600)
    got = (await bc.all_rows(ops, bid, N_ROWS))["rows"]
    s = await bc.stop(ops)
    return (f"{len(got)}/{N_ROWS} rows listed after the restart, "
            f"{sum(1 for x in got.values() if x.get('state') == 'done')} done; stop said {s.get('status')!r}")


def every_row_returned(t: Trace) -> Result:
    return bc.every_row_returned(t, rows=N_ROWS, after=bc.restart_index(t))


EXTRA_INVARIANTS = [every_row_returned, bc.nothing_admitted_before_resume]
WORLD_CHECKS = [bc.no_row_ran_twice, bc.ledger_matches_sacct]
EXPECT_OK = ["agent_engaged", "every_row_returned", "nothing_admitted_before_resume", "ends_with_stop"]

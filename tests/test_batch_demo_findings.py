"""The batch demo's findings (2026-11-03: a user drove `agentic/fakecluster/bin/try_batch.sh` on the fake cluster, and
each finding was checked against the raw tool output). Written failing first; each test names its finding.

- B: `status(id, wait_s)` returned at every settled row (20 rows, 21 status calls ~6 s apart). It now waits the whole
  wait_s unless the batch ends, a row fails / is lost / is of unknown fate, or the phase changes.
- The slot pool: `Ledger.reserve` counted held rows per batch, so batches on one block each filled their own window
  and the excess queued at the interchange (#204's pattern). The block's window is now shared, fairly.
- C: the envelope's meter called the session's own compute block "not started by hpc-bridge" and invited exclude_jobs.
- D: an adopted job reported exit_code 0 while RUNNING and after CANCELLED (sacct's ExitCode is 0:0 there).
- E: a mid-run array's counts did not sum to its tasks (sacct -X has no row for a pending array task).
- F: "What still bills" listed a CANCELLED job with nothing committed.
- G: export_handoff's terms did not match the config it sends (init_blocks; the session's block size).
"""
from __future__ import annotations

import asyncio
import time

import pytest

from tests.test_batch_phase1 import _add, _batch_app, _ledger, _ok_report, _owner, _start
from tests.test_batch_phase2a import _BASH, _confirmed, _hand_script, _submit, world

_FIXTURES = (world,)   # a pytest fixture, used by name as a test parameter below
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]

# ---- B: status(wait_s) waits through rows that settle cleanly --------------------------------------------------------


async def _looping(app, out):
    from hpc_bridge import batch

    app.batch_autoloop = True
    batch._start_loop(app, app.batches[out.batch_id])
    await asyncio.sleep(0.05)


async def test_B_wait_s_is_not_ended_by_rows_that_complete():
    """Red before: the first row that completed ended the wait (one agent turn per row)."""
    from hpc_bridge import server

    app, _rt, runner = await _batch_app(workers=3)          # a window of 2
    out = await _start(app, n=6)
    await _looping(app, out)
    loop = asyncio.get_running_loop()
    loop.call_later(0.3, lambda: runner.futures[0].finish(_ok_report(0)))
    loop.call_later(0.8, lambda: runner.futures[1].finish(_ok_report(1)))
    try:
        t0 = time.monotonic()
        st = await server._status(app, out.batch_id, wait_s=3)
        took = time.monotonic() - t0
    finally:
        app.batches[out.batch_id].loop.cancel()
    assert took >= 2.5, f"status(wait_s=3) returned after {took:.1f} s on a row that completed"
    assert st.counts.done == 2 and st.phase == "admitting", st.counts      # it still reports the progress


async def test_B_wait_s_returns_when_a_row_fails():
    from hpc_bridge import server

    app, _rt, runner = await _batch_app(workers=3)
    out = await _start(app, n=6)
    await _looping(app, out)
    loop = asyncio.get_running_loop()
    loop.call_later(0.3, lambda: runner.futures[0].finish(_ok_report(0)))
    loop.call_later(0.6, lambda: runner.futures[1].finish(_ok_report(1, rc=3)))
    try:
        t0 = time.monotonic()
        st = await server._status(app, out.batch_id, wait_s=20)
        took = time.monotonic() - t0
    finally:
        app.batches[out.batch_id].loop.cancel()
    assert st.counts.failed == 1 and took < 5, (took, st.counts)


async def test_B_wait_s_returns_when_the_batch_ends():
    from hpc_bridge import server

    app, _rt, runner = await _batch_app(workers=3)
    out = await _start(app, n=2)
    await _looping(app, out)
    loop = asyncio.get_running_loop()
    loop.call_later(0.3, lambda: runner.futures[0].finish(_ok_report(0)))
    loop.call_later(0.6, lambda: runner.futures[1].finish(_ok_report(1)))
    try:
        t0 = time.monotonic()
        st = await server._status(app, out.batch_id, wait_s=20)
        took = time.monotonic() - t0
    finally:
        loop_task = app.batches.get(out.batch_id) and app.batches[out.batch_id].loop
        if loop_task:
            loop_task.cancel()
    assert st.phase == "done" and st.counts.done == 2 and took < 5, (took, st.phase, st.counts)


# ---- the slot pool: one block's window, shared by every batch on it ---------------------------------------------------

def test_pool_reserve_counts_every_batch_on_the_same_block(tmp_path):
    """Red before: each batch reserved up to its own capacity, so two batches on one 3-worker block had 4 out."""
    led = _ledger(tmp_path)
    me = _owner()
    a = _add(led, "batch-00000000000a", n=6, capacity=2, owner=me)
    b = _add(led, "batch-00000000000b", n=6, capacity=2, owner=me)
    assert len(led.reserve(a, me, 2, uec_hash="uec-1")) == 2
    assert led.reserve(b, me, 2, uec_hash="uec-1") == []                    # the block's window is full


def test_pool_a_different_block_has_its_own_window(tmp_path):
    led = _ledger(tmp_path)
    me = _owner()
    a = _add(led, "batch-00000000000a", n=6, capacity=2, owner=me)
    b = _add(led, "batch-00000000000b", n=6, capacity=2, owner=me)
    led.update_batch(b, uec_hash="uec-2")
    assert len(led.reserve(a, me, 2, uec_hash="uec-1")) == 2
    assert len(led.reserve(b, me, 2, uec_hash="uec-2")) == 2


def _settle_one(led, bid):
    t = next(r for r in led.tasks(bid) if r["state"] == "in_flight")
    assert led.settle(bid, int(t["idx"]), int(t["attempt"]), "done", rc=0)


@pytest.mark.parametrize("capacity", [1, 2, 3])
def test_pool_one_huge_batch_cannot_starve_another(tmp_path, capacity):
    """Fairness: while another batch on the block is waiting for a slot, a freed slot goes to the batch with the fewest
    rows out (the one that admitted least recently, at a tie) — the huge batch that freed it does not take it back."""
    led = _ledger(tmp_path)
    me = _owner()
    huge = _add(led, "batch-0000000000aa", n=500, capacity=capacity, owner=me)
    small = _add(led, "batch-0000000000bb", n=4, capacity=capacity, owner=me)
    assert len(led.reserve(huge, me, capacity, uec_hash="uec-1", now=100.0)) == capacity
    assert led.reserve(small, me, capacity, uec_hash="uec-1", now=101.0) == []     # full: the small batch waits
    small_got = 0
    for step in range(12):   # the huge batch's rows finish one by one; each batch passes as its loop would
        _settle_one(led, huge) if any(r["state"] == "in_flight" for r in led.tasks(huge)) else None
        now = 102.0 + step
        led.reserve(huge, me, capacity, uec_hash="uec-1", now=now)
        small_got += len(led.reserve(small, me, capacity, uec_hash="uec-1", now=now + 0.5))
        if any(r["state"] == "in_flight" for r in led.tasks(small)):
            _settle_one(led, small)
        held = sum(1 for bid in (huge, small) for r in led.tasks(bid) if r["state"] == "in_flight")
        assert held <= capacity, (step, held)
    assert small_got == 4, f"the small batch got {small_got} of its 4 rows in 12 rounds"


def test_pool_a_waiter_that_stopped_asking_does_not_hold_slots_back(tmp_path):
    """A batch that wanted a slot but no longer asks (paused, its gate shut, its server gone) holds nothing back: the
    other batch fills the window once that want is stale."""
    from hpc_bridge import ledger

    led = _ledger(tmp_path)
    me = _owner()
    a = _add(led, "batch-00000000000a", n=10, capacity=2, owner=me)
    b = _add(led, "batch-00000000000b", n=10, capacity=2, owner=me)
    assert len(led.reserve(a, me, 2, uec_hash="uec-1", now=100.0)) == 2
    assert led.reserve(b, me, 2, uec_hash="uec-1", now=100.5) == []
    _settle_one(led, a)
    assert led.reserve(a, me, 2, uec_hash="uec-1", now=101.0) == []          # b waits, and has fewer out
    stale = 101.0 + ledger.WANT_FRESH_S + 1
    assert len(led.reserve(a, me, 2, uec_hash="uec-1", now=stale)) == 1     # b stopped asking: a takes it
    assert led.reserve(b, me, 2, uec_hash="uec-1", now=stale + 0.1) == []   # b asks again (the window is full)...
    led.update_batch(b, state="needs_confirmation")                        # ...and is paused
    _settle_one(led, a)
    assert len(led.reserve(a, me, 2, uec_hash="uec-1", now=stale + 0.2)) == 1   # a paused batch is no contender


async def test_pool_a_second_run_batch_on_the_block_shares_its_window():
    """Through the tool: two batches on the session's one block never have more out than the block's window."""
    from hpc_bridge import batch

    app, _rt, runner = await _batch_app(workers=3)          # a window of 2
    first = await _start(app, n=6)
    second = await _start(app, n=6)
    led = batch.ledger_of(app)
    out = sum(led.counts(b).get("in_flight", 0) for b in (first.batch_id, second.batch_id))
    assert first.admitted == 2 and second.admitted == 0 and out == 2, (first.admitted, second.admitted, out)
    assert len(runner.futures) == 2
    assert f"shared with {first.batch_id}" in (second.notice or ""), second.notice


# ---- C: the session's own compute block, confirmed before the envelope ------------------------------------------------

def _session_block(world, *, state: str = "RUNNING") -> str:
    """The session's compute block, confirmed (and seen up) BEFORE any envelope: parsl's job, which the envelope's sweep
    finds among the user's jobs."""
    from hpc_bridge import server

    jid = world.sched.add_job(name="parsl.GlobusComputeEngine-1762200000.5", state=state, elapsed=300,
                              timelimit_min=60, billing=4)
    rt = server._shape_runtime(world.app, "compute")
    rt.spend_confirmed = True
    rt.block_job = jid
    world.app.job_seen_since[(world.app.state.endpoint_id, jid)] = time.monotonic() - 300
    return jid


async def test_C_the_sessions_own_block_is_named_as_such_and_never_offered_for_exclusion(world):
    """Red before: the proposal and the envelope's status called the session's block "not started by hpc-bridge" and
    invited exclude_jobs for it. It is still counted (it bills the same allocation)."""
    from hpc_bridge import server

    blk = _session_block(world)
    ask = await _submit(world.app, command="sleep 60")
    assert ask.phase == "needs_confirmation" and ask.reason == "envelope_needed", ask
    mine = [u for u in ask.envelope.unledgered if u.native_id == blk]
    assert mine and mine[0].session_block is True, ask.envelope.unledgered
    assert "session's compute block" in (ask.notice or "") and "exclude_jobs" not in (ask.notice or ""), ask.notice
    yes = await _submit(world.app, command="sleep 60", confirm_spend=True)
    assert yes.phase in ("queued", "running"), yes
    env = await server._status(world.app, yes.envelope.envelope_id)
    note = env.notice or ""
    assert f"job {blk}" in note and "the session's compute block, confirmed before this envelope opened" in note, note
    assert "not started by hpc-bridge" not in note and "exclude_jobs" not in note, note
    assert env.envelope.spent + env.envelope.committed >= 4.0 + 4.0 - 1e-6, env.envelope     # still counted
    stray = world.sched.add_job(name="bypass", state="RUNNING", elapsed=60, timelimit_min=60, billing=4)
    env = await server._status(world.app, yes.envelope.envelope_id)
    note = env.notice or ""
    assert f"job {stray} (bypass, RUNNING: not started by hpc-bridge" in note and "exclude_jobs" in note, note
    assert "the session's compute block" in note, note


# ---- D: an exit code only where the scheduler has one ----------------------------------------------------------------

def _sacct(*rows: str, squeue: tuple[str, ...] = ()) -> str:
    from hpc_bridge import meter

    return "\n".join([meter.SACCT_MARK, *rows, "HPCB_SACCT_RC=0", meter.SQUEUE_MARK, *squeue, "HPCB_SQUEUE_RC=0",
                      meter.END_MARK])


@pytest.mark.parametrize("state, code, want", [
    ("RUNNING", "0:0", None), ("PENDING", "0:0", None), ("CANCELLED", "0:0", None), ("TIMEOUT", "0:0", None),
    ("NODE_FAIL", "0:0", None), ("COMPLETED", "0:0", 0), ("FAILED", "3:0", 3)])
def test_D_the_meter_reads_an_exit_code_only_from_an_ended_job_that_carries_one(state, code, want):
    from hpc_bridge import meter

    row = f"42|wrap|{state}|60|5|billing=1,cpu=1,node=1|billing=1,cpu=1,node=1|main|hpcb|{code}|None"
    live = (f"42|{state}|0:01|5:00|1|main|None|wrap",) if state in ("RUNNING", "PENDING") else ()
    job = {"native_id": "42", "rate": 1.0, "walltime_s": 300, "worst_case": 300 / 3600, "tasks": 1,
           "state": "running"}
    assert meter.job_reading(job, meter.parse_slurm(_sacct(row, squeue=live))).exit_code == want


async def test_D_an_adopted_job_reports_no_exit_code_while_running_or_once_cancelled(world):
    """Red before: an adopted hand sbatch read exit_code 0 while RUNNING and after CANCELLED (sacct's 0:0)."""
    from hpc_bridge import server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    out = await server._run_shell(world.app, f"sbatch {_hand_script(world)}", shape="login", agent_call=True,
                                  confirm_spend=True)
    adopted = out.jobs[0]
    assert adopted.source == "adopt", out.jobs
    world.sched.set(adopted.native_id, state="RUNNING", elapsed=60)
    st = await server._status(world.app, adopted.job_id)
    assert st.job.state == "RUNNING" and st.job.exit_code is None, st.job
    world.sched.set(adopted.native_id, state="CANCELLED", elapsed=90)
    st = await server._status(world.app, adopted.job_id)
    assert st.job.state.startswith("CANCELLED") and st.job.exit_code is None, st.job


def test_D_a_ledger_row_an_earlier_build_wrote_shows_no_exit_code_for_a_live_job():
    from hpc_bridge import jobs

    row = {"id": "job-0000000000aa", "native_id": "42", "source": "adopt", "state": "running",
           "sched_state": "RUNNING", "exit_code": 0, "worst_case": 1.0, "charged": 0.1, "committed": 0.5,
           "meter_source": "sacct", "tasks": 1}
    assert jobs.job_info(row).exit_code is None
    assert jobs.job_info({**row, "state": "done", "sched_state": "COMPLETED"}).exit_code == 0


# ---- E: an array's counts sum to its tasks ----------------------------------------------------------------------------

_ARRAY_JOB = {"native_id": "7", "rate": 1.0, "walltime_s": 300, "worst_case": 10 * 300 / 3600, "tasks": 10,
              "state": "running"}


def _task_row(i: int, state: str) -> str:
    return f"7_{i}|hpcb-job|{state}|30|5|billing=1,cpu=1,node=1|billing=1,cpu=1,node=1|main||0:0|None"


@pytest.mark.parametrize("squeue_lists_the_pending_task", [False, True], ids=["in_neither", "squeue_pending"])
def test_E_a_mid_run_arrays_counts_sum_to_its_tasks(squeue_lists_the_pending_task):
    """Red before: 8 COMPLETED + 1 RUNNING of 10 (the tenth was in neither sacct -X nor squeue's pending ranges)."""
    from hpc_bridge import meter

    rows = [_task_row(i, "COMPLETED") for i in range(8)] + [_task_row(8, "RUNNING")]
    sq = ["7_8|RUNNING|0:30|5:00|1|main|None|hpcb-job"]
    if squeue_lists_the_pending_task:
        sq.append("7_[9%2]|PENDING|0:00|5:00|1|main|JobArrayTaskLimit|hpcb-job")
    r = meter.job_reading(_ARRAY_JOB, meter.parse_slurm(_sacct(*rows, squeue=tuple(sq))))
    assert sum(r.counts.values()) == 10 and r.counts == {"COMPLETED": 8, "RUNNING": 1, "PENDING": 1}, r.counts
    assert r.committed >= 300 / 3600 - 1e-6, r          # the pending task still holds its whole walltime


def test_E_a_requeued_jobs_earlier_run_is_no_task_of_its_own():
    from hpc_bridge import meter

    rows = ["9|hpcb-job|REQUEUED|1800|60|billing=4,cpu=4,node=1|billing=4,cpu=4,node=1|main||0:0|None",
            "9|hpcb-job|RUNNING|600|60|billing=4,cpu=4,node=1|billing=4,cpu=4,node=1|main||0:0|None"]
    job = {"native_id": "9", "rate": 4.0, "walltime_s": 3600, "worst_case": 4.0, "tasks": 1, "state": "running"}
    r = meter.job_reading(job, meter.parse_slurm(_sacct(*rows, squeue=("9|RUNNING|10:00|1:00:00|4|main|None|x",))))
    assert r.counts == {"RUNNING": 1}, r.counts
    assert r.charged == pytest.approx(4 * 2400 / 3600, abs=1e-3), r


# ---- F: "What still bills" lists only what still can --------------------------------------------------------------

async def test_F_what_still_bills_leaves_out_ended_jobs_with_nothing_committed(world):
    """Red before: a CANCELLED job of the user's was listed under "What still bills" with "+ 0"."""
    from hpc_bridge import jobs, server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    eid = out.envelope.envelope_id
    stray = world.sched.add_job(name="bypass", state="RUNNING", elapsed=600, timelimit_min=60, billing=4)
    env = await server._status(world.app, eid)
    assert stray in jobs._bills_note(env.envelope), env.notice
    world.sched.set(stray, state="CANCELLED", elapsed=900)
    world.sched.set(out.job.native_id, state="CANCELLED", elapsed=60)
    env = await server._status(world.app, eid)
    assert jobs._bills_note(env.envelope) == "nothing", env.notice
    assert "What still bills: nothing." in (env.notice or ""), env.notice
    assert env.envelope.spent >= 4 * 900 / 3600 - 1e-6, env.envelope              # what it billed is still counted


# ---- G: export_handoff's terms state what its config sends ----------------------------------------------------------

def _handoff_app(tmp_path, *, per_core: bool = False):
    from hpc_bridge.models import PartitionShape
    from tests.test_handoff_phase2b import _ssh_app

    parts = (PartitionShape(name="main", kind="per_core", cores=4, billing={"cpu": 1.0}),) if per_core else ()
    app = _ssh_app(tmp_path, partitions=parts)
    app.facility.takes_resources = True          # the endpoint's template renders a block size (read at bring-up)
    return app


async def test_G_the_terms_state_the_init_blocks_the_config_sends(tmp_path):
    """Red before: the terms said init_blocks 1 (the login branch's literal, taken by a max over every reading) while
    the config sent 0."""
    from tests.test_handoff_phase2b import _export

    app = _handoff_app(tmp_path)
    ask = await _export(app, {"partition": "main", "walltime": "00:10:00", "max_blocks": 1})
    assert ask.phase == "needs_confirmation", ask
    assert ask.user_endpoint_config["init_blocks"] == 0 and ask.terms.init_blocks == 0, (ask.user_endpoint_config,
                                                                                         ask.terms)
    assert ask.terms.canary_worst_case > 0, ask.terms          # the canary still starts (and is billed) one block


async def test_G_the_sessions_block_size_never_reaches_a_handoff(tmp_path):
    """Red before: the session's own block size (cores=3, a slice) rode into the handoff's config as the facility's
    defaults, and the terms never named it."""
    from tests.test_handoff_phase2b import _export

    app = _handoff_app(tmp_path)
    app.compute_choice.update({"cores_per_node": 3, "exclusive": False, "partition": "main"})
    ask = await _export(app, {"partition": "main", "walltime": "00:10:00", "max_blocks": 1})
    assert ask.phase == "needs_confirmation", ask
    sent = ask.user_endpoint_config
    assert "cores_per_node" not in sent and "exclusive" not in sent, sent
    assert ask.terms.cores_per_node is None and ask.terms.exclusive is None, ask.terms


async def test_G_the_terms_state_every_size_key_the_config_sends(tmp_path):
    from tests.test_handoff_phase2b import _export

    app = _handoff_app(tmp_path, per_core=True)
    uec = {"partition": "main", "walltime": "00:10:00", "max_blocks": 1, "cores_per_node": 3, "exclusive": False,
           "mem_per_node": 6}
    ask = await _export(app, uec)
    assert ask.phase == "needs_confirmation", ask
    t = ask.terms
    assert (t.cores_per_node, t.exclusive, t.mem_per_node) == (3, False, 6), t
    assert t.rate == pytest.approx(3.0), t                       # 3 cores × 1 billing unit an hour
    for words in ("cores_per_node 3", "exclusive false", "mem_per_node 6", "init_blocks 0", "max_blocks 1"):
        assert words in (ask.notice or ""), (words, ask.notice)


async def test_G_a_config_that_asks_no_slice_is_costed_as_the_whole_node_it_gets(tmp_path):
    """Red before: on a per-core partition the terms costed D3's proposed slice (2 of 4 cores) — which only the
    session's own blocks send — while the handoff's config asked for no size, so parsl asks for a whole node
    (`--exclusive`, its default): understated by half."""
    from tests.test_handoff_phase2b import _export

    app = _handoff_app(tmp_path, per_core=True)
    ask = await _export(app, {"partition": "main", "walltime": "01:00:00", "max_blocks": 1})
    assert ask.phase == "needs_confirmation", ask
    assert not {"cores_per_node", "exclusive", "mem_per_node"} & set(ask.user_endpoint_config), ask.user_endpoint_config
    assert ask.terms.rate == pytest.approx(4.0), ask.terms        # the whole 4-core node
    assert ask.terms.canary_worst_case >= 4.0 - 1e-9, ask.terms


# ---- status() with no id: what the ledger holds (the user's request after the demo) --------------------------------

DAY = 86400.0


def _listing_app(tmp_path):
    """A server with nothing bound and a ledger holding: a finished batch, an active one, a stale one (its server gone
    for days, its block's config retired — the user's real ledger holds one from 2026-10-31), an open envelope with a
    running array job, and a closed envelope on another facility."""
    from hpc_bridge.ledger import Ledger, Owner
    from hpc_bridge.profile import Profile
    from hpc_bridge.server import AppCtx
    from tests.fakes import FakeFacility

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.batch_autoloop = False
    led = Ledger.open(tmp_path / "state")
    app.ledger = led
    now = time.time()
    me, dead = Owner.here(), Owner("laptop", 2 ** 22 + 7)
    led.pid_alive = lambda pid: pid != dead.pid
    rows = [{"x": i} for i in range(4)]

    def add(bid, *, owner, at, state="admitting", fac="fakehpc", cmd="echo {x}"):
        led.add_batch(bid, command=cmd, rows=rows, task_walltime_s=60, capacity=2, results_dir="/scratch/b",
                      idempotent=False, endpoint_id="eid-1", uec_hash="uec-1", session_id="default", owner=owner,
                      facility=fac, now=at)
        if state != "admitting":
            led.update_batch(bid, state=state)

    add("batch-00000000d0e1", owner=me, at=now - 3 * DAY, state="done")
    for i in range(4):
        led.settle("batch-00000000d0e1", i, 0, "done" if i else "failed", rc=0 if i else 3)
    add("batch-0000000057a1", owner=dead, at=now - 4 * DAY,
        cmd="python3 analyse.py --input {x} --output results/{x}.json --threshold 0.75 --verbose --more-flags-here")
    led.retire("eid-1", "uec-1", why="stop_endpoint")
    add("batch-00000000ac71", owner=me, at=now - 60, fac="fakehpc")
    led.update_batch("batch-00000000ac71", uec_hash="uec-2")
    led.reserve("batch-00000000ac71", me, 2, uec_hash="uec-2", now=now - 50)
    led.add_envelope("env-0000000000e1", facility="fakehpc", account="hpcb", unit="billing units", cap=20.0,
                     deadline=now + DAY, approved_by="s1", owner=me, now=now - 30)
    led.reserve_job("env-0000000000e1", "job-0000000000j1", session="s1", worst_case=4.0, tasks=10,
                    native_id="1234", state="running", array_spec="0-9%2", now=now - 20)
    led.update_job("job-0000000000j1", array_counts='{"COMPLETED": 8, "RUNNING": 1, "PENDING": 1}',
                   sched_state="RUNNING", metered_at=now - 10, charged=1.5, committed=2.0)
    led.update_envelope("env-0000000000e1", spent=1.5, committed=2.0)
    led.add_envelope("env-0000000000e2", facility="otherhpc", account=None, unit="billing units", cap=5.0,
                     deadline=now - DAY, approved_by="s0", owner=me, now=now - 5 * DAY)
    led.update_envelope("env-0000000000e2", state="closed")
    return app, led


async def test_listing_shows_everything_active_first_then_newest(tmp_path):
    from hpc_bridge import server

    app, _led = _listing_app(tmp_path)
    out = await server._status(app)
    assert out.kind == "list" and out.phase == "listed" and out.total == 6, out
    ids = [e.id for e in out.entries]
    assert ids == ["job-0000000000j1", "env-0000000000e1", "batch-00000000ac71", "batch-0000000057a1",
                   "batch-00000000d0e1", "env-0000000000e2"], ids
    by = {e.id: e for e in out.entries}
    b = by["batch-00000000ac71"]
    assert (b.kind, b.facility, b.state, b.active) == ("batch", "fakehpc", "admitting", True), b
    assert b.counts == {"done": 0, "failed": 0, "cancelled": 0, "in_flight": 2} and b.tasks == 4, b
    assert b.created_at.endswith("Z") and b.command == "echo {x}", b
    d = by["batch-00000000d0e1"]
    assert d.active is False and d.counts == {"done": 3, "failed": 1, "cancelled": 0, "in_flight": 0}, d
    assert len(by["batch-0000000057a1"].command or "") <= 60, by["batch-0000000057a1"]
    j = by["job-0000000000j1"]
    assert (j.kind, j.state, j.facility) == ("job", "RUNNING", "fakehpc") and j.counts["COMPLETED"] == 8, j
    e = by["env-0000000000e1"]
    assert (e.spent, e.committed, e.cap, e.active) == (1.5, 2.0, 20.0, True), e
    assert by["env-0000000000e2"].active is False


async def test_listing_surfaces_a_stale_active_batch_and_changes_nothing(tmp_path):
    """A batch still `admitting` in the ledger whose server is long gone and whose block's config was retired is said
    plainly; the listing never takes its lease, pauses it, ticks it, or starts work for anything."""
    from hpc_bridge import server

    app, led = _listing_app(tmp_path)
    before = {b["id"]: (b["state"], b["owner_pid"], b["heartbeat"]) for b in led.batches()}
    out = await server._status(app, active_only=True)
    stale = next(e for e in out.entries if e.id == "batch-0000000057a1")
    assert stale.active and stale.stale and "gone" in stale.stale and "retired" in stale.stale, stale
    assert "batch-0000000057a1" in (out.notice or "") and "stale" in (out.notice or ""), out.notice
    fresh = next(e for e in out.entries if e.id == "batch-00000000ac71")
    assert fresh.stale is None, fresh
    after = {b["id"]: (b["state"], b["owner_pid"], b["heartbeat"]) for b in led.batches()}
    assert after == before and app.batches == {} and app.shapes == {}, (before, after, app.batches, app.shapes)


async def test_listing_pages_and_filters(tmp_path):
    from hpc_bridge import server

    app, _led = _listing_app(tmp_path)
    first = await server._status(app, limit=2)
    assert [e.id for e in first.entries] == ["job-0000000000j1", "env-0000000000e1"] and first.next_offset == 2, first
    rest = await server._status(app, offset=first.next_offset, limit=10)
    assert len(rest.entries) == 4 and rest.next_offset is None, rest
    active = await server._status(app, active_only=True)
    assert {e.id for e in active.entries} == {"job-0000000000j1", "env-0000000000e1", "batch-00000000ac71",
                                              "batch-0000000057a1"} and active.total == 4, active
    other = await server._status(app, facility="otherhpc")
    assert [e.id for e in other.entries] == ["env-0000000000e2"], other
    capped = await server._status(app, limit=10_000)
    assert len(capped.entries) == 6, capped

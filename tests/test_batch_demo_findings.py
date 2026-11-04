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
import json
import time

import pytest

from hpc_bridge.ledger import HOLDING
from tests.test_batch_phase1 import _add, _batch_app, _ledger, _ok_report, _owner, _start
from tests.test_batch_phase2a import _BASH, _confirmed, _hand_script, _submit, world
from tests.test_server import _Res

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
    assert f"other batches hold its slots: {first.batch_id} (2 row(s) out" in (second.notice or ""), second.notice
    assert second.phase == "waiting" and first.batch_id in (second.reason or ""), second


# ---- review round 1: the shared window counts only rows that can hold a worker of the block in front ----------------
# The reviewer's reproductions (red on 36af987), and the bounds the fix states.

def _out_on(led, bids, uec):
    return sum(1 for b in bids for t in led.tasks(b) if t["state"] in HOLDING and t.get("sent_uec") == uec)


def test_r1_1_presumed_lost_rows_of_an_aborted_batch_hold_the_window_a_week_later(tmp_path):
    """On a facility MEP the block's config hash is the same for every block, and a presumed-lost row never leaves
    HOLDING without a result: it held every later batch's window on the config — here a week later, its batch aborted.
    A presumed-lost row holds only the block it was sent to, and only while its batch's server runs."""
    led = _ledger(tmp_path)
    me = _owner()
    old = _add(led, "batch-0000000000a1", n=2, capacity=2, owner=me)
    for r in led.reserve(old, me, 2, uec_hash="uec-1", now=1_000.0):
        assert led.settle(old, r.index, r.attempt, "presumed_lost", only_from=("in_flight",))
    led.update_batch(old, state="cancelled", pause_reason="aborted by cancel")
    new = _add(led, "batch-0000000000b1", n=4, capacity=2, owner=me)
    week = 1_000.0 + 7 * 86_400
    got = led.reserve(new, me, 2, uec_hash="uec-1", now=week)
    assert len(got) >= 1, "a week later, an aborted batch's presumed-lost rows still take the whole block's window"


def test_r1_1_what_holds_a_slot_of_the_block_in_front(tmp_path):
    """The bounds, row by row (task_walltime_s 60, HOLD_MARGIN_S past the admission): a live server's row in flight
    holds; a gone server's only until its walltime and the margin have passed; a row of unknown fate until evidence
    (`unheld`), whoever runs it (round 2); a presumed-lost row only on the block it was sent to, while its server
    runs."""
    from hpc_bridge.ledger import HOLD_MARGIN_S

    led = _ledger(tmp_path)
    me = _owner()
    live = _add(led, "batch-0000000000a1", n=3, capacity=4, owner=me)
    rows = led.reserve(live, me, 3, uec_hash="uec-1", now=1_000.0, block_job="101")
    assert led.settle(live, rows[0].index, rows[0].attempt, "presumed_lost", sent_job="101")
    assert led.settle(live, rows[1].index, rows[1].attempt, "submit_unknown", sent_job="101", unknown_since=1_001.0)
    gone = _add(led, "batch-0000000000c1", n=1, capacity=4, owner=me)
    assert len(led.reserve(gone, me, 1, uec_hash="uec-1", now=1_000.0, block_job="101")) == 1
    led.update_batch(gone, owner_pid=2 ** 22 + 11)            # its server is gone (that pid is dead here)
    led.pid_alive = lambda pid: pid != 2 ** 22 + 11
    new = _add(led, "batch-0000000000d1", n=4, capacity=4, owner=me)
    soon, late = 1_010.0, 1_000.0 + 60 + HOLD_MARGIN_S + 1
    assert led.block_holders(new, uec_hash="uec-1", block_job="101", now=soon) == {live: 3, gone: 1}
    assert led.block_holders(new, uec_hash="uec-1", block_job="202", now=soon) == {live: 2, gone: 1}
    assert led.block_holders(new, uec_hash="uec-1", block_job="101", now=late) == {live: 3}
    assert led.block_holders(new, uec_hash="uec-1", block_job=None, now=late) == {live: 2}
    assert led.settle(live, rows[1].index, rows[1].attempt, "submit_unknown", unheld="evidence: its user endpoint "
                      "exited with nothing queued")
    assert led.block_holders(new, uec_hash="uec-1", block_job=None, now=late) == {live: 1}


async def test_a_holder_of_unknown_fate_on_ssh_is_named_with_what_frees_it():
    """On an endpoint hpc-bridge stood up: a batch whose one row's submit is of unknown fate holds the block's window
    of 1. The batch it refuses is told the row is of unknown fate and that a stop (or the reap) retiring its config
    frees it once its user endpoint has exited — not that cancelling the holder does."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app(workers=2)                       # SSH (a login shape); a window of 1
    runner.fail_submit_of = {0}
    first = await _start(app, n=1)
    await batch.tick(app, first.batch_id)
    second = await _start(app, n=2)
    st = await server._status(app, second.batch_id)
    said = " ".join(x or "" for x in (second.notice, st.notice))
    assert st.counts.in_flight == 0 and f"{first.batch_id} (1 row(s) out, 1 of them of unknown fate" in said, said
    assert "stop_endpoint (or the block's reap) that retires its config frees it" in said, said
    assert "cancelling its batch does not free it" in said and "cancel('<id>', 'abort')" not in said, said


def test_r1_1_hold_margin_is_one_value():
    from hpc_bridge import batch, ledger

    assert ledger.HOLD_MARGIN_S == batch.HOLD_MARGIN_S == batch.WRAPPER_TAIL_S + batch.DELIVERY_S


async def test_r1_1_an_aborted_batchs_unknown_submit_holds_the_window_until_evidence_and_says_so(monkeypatch):
    """Through the tools, on a MEP: a batch's one row's submit is of unknown fate; the user aborts the batch. A later
    session's batch on the same endpoint and config is told which batch holds the block's window (status / cancel).
    The row holds until evidence says its task can no longer run (round 2's ruling; round 1 had let it expire after
    task_walltime_s + a margin) — then the later batch admits."""
    from hpc_bridge import batch, server

    async def no_marker(*_a, **_k):
        return True, ""

    monkeypatch.setattr(batch, "write_cancel_marker", no_marker)
    app, rt, runner = await _batch_app(workers=2, mep=True)              # a window of 1
    runner.fail_submit_of = {0}
    first = await _start(app, n=1)
    await batch.tick(app, first.batch_id)
    assert (await server._status(app, first.batch_id)).counts.submit_unknown == 1
    await server._cancel(app, first.batch_id, "abort")
    assert (await server._status(app, first.batch_id)).phase == "cancelled"

    app2, rt2, _runner2 = await _batch_app(workers=2, mep=True)          # a later session, same endpoint and config
    assert batch.uec_hash(rt2) == batch.uec_hash(rt)
    second = await _start(app2, n=4)
    for _ in range(3):
        await batch.tick(app2, second.batch_id)
    st = await server._status(app2, second.batch_id)
    said = " ".join(x or "" for x in (second.notice, st.notice, st.reason))
    assert st.counts.in_flight >= 1 or first.batch_id in said, (
        f"admitted {second.admitted}, counts {st.counts}, phase {st.phase}: starved by {first.batch_id} (aborted) "
        f"without a word: {said!r}")
    # a MEP: no evidence can come but the row's result, and cancelling its batch frees nothing — said so, plainly
    assert "1 of them of unknown fate" in said and "frees only when its result arrives" in said, said
    assert "cancel('<id>', 'abort')" not in said, said
    later = time.time() + 60 + batch.HOLD_MARGIN_S + 1                   # task_walltime_s 60 past its submit
    monkeypatch.setattr(batch, "_now", lambda: later)
    await batch.tick(app2, second.batch_id)
    led = batch.ledger_of(app2)
    assert led.counts(second.batch_id)["in_flight"] == 0                  # no evidence yet: still held, still named
    assert first.batch_id in ((await server._status(app2, second.batch_id)).notice or "")
    [row] = led.tasks(first.batch_id)
    assert led.settle(first.batch_id, int(row["idx"]), int(row["attempt"]), "submit_unknown",
                      unheld="evidence: its copy cannot run")
    await batch.tick(app2, second.batch_id)
    assert led.counts(second.batch_id)["in_flight"] == 1


@pytest.mark.parametrize("trigger", ["waiter_holds_a_row_on_its_old_block", "waiter_has_a_smaller_capacity"])
def test_r1_2_a_free_slot_is_never_left_idle_for_a_waiter_that_cannot_take_it(tmp_path, trigger):
    """A defers only to a waiter that can take the slot (its own room on the block), compares rows out on THIS block,
    and a batch refused by its own rows or capacity never asks. The second case's window is b's (fix 5: the smallest
    capacity of the live batches on the block): 2, and it is filled."""
    led = _ledger(tmp_path)
    me = _owner()
    smaller = trigger == "waiter_has_a_smaller_capacity"
    a = _add(led, "batch-00000000000a", n=10, capacity=3 if smaller else 2, owner=me)
    b = _add(led, "batch-00000000000b", n=10, capacity=2, owner=me)
    if not smaller:
        # b ran on an earlier block (a reap, then the user's yes for a new one: a fresh hpcb_block, a new config hash),
        # and one of its submits there is of unknown fate: it holds b's own window until evidence
        led.update_batch(b, uec_hash="uec-old")
        [r] = led.reserve(b, me, 1, uec_hash="uec-old", now=10.0)
        assert led.settle(b, r.index, r.attempt, "submit_unknown", only_from=("in_flight",), unknown_since=10.0)
        led.update_batch(b, uec_hash="uec-1")
    assert len(led.reserve(a, me, 1, uec_hash="uec-1", now=100.0)) == 1   # a: one row out of the block's window of 2
    for step in range(10):                                                # each batch's loop passes, as it would
        now = 101.0 + 5 * step
        led.reserve(b, me, 2, uec_hash="uec-1", now=now)
        led.reserve(a, me, 3, uec_hash="uec-1", now=now + 1)
    out = _out_on(led, (a, b), "uec-1")
    assert out == 2, f"{out} out on the block's window of 2 after 10 rounds with rows waiting in the backlogs"


def test_r1_2_a_batch_refused_by_its_own_window_does_not_ask(tmp_path):
    led = _ledger(tmp_path)
    me = _owner()
    a = _add(led, "batch-00000000000a", n=10, capacity=1, owner=me)
    assert len(led.reserve(a, me, 1, uec_hash="uec-1", now=100.0)) == 1
    assert led.reserve(a, me, 1, uec_hash="uec-1", now=101.0) == []           # its own window is full
    assert led.batch(a)["want_at"] is None


async def test_r1_3_a_second_batch_overfills_the_window_the_first_learned():
    """Two batches on ONE block: the first learnt from its results that the block has 3 workers (a window of 2); a
    second batch started now reads the config's 8 — the block's window is the smaller."""
    from hpc_bridge import batch

    app, _rt, runner = await _batch_app(workers=8)       # the config says 8 workers; the canary names no count
    first = await _start(app, n=20)
    for i, f in enumerate(list(runner.futures)):
        f.finish(_ok_report(i))                          # each result: workers 3 (PARSL_WORKER_COUNT on the node)
    await batch.tick(app, first.batch_id)
    led = batch.ledger_of(app)
    assert int(led.batch(first.batch_id)["capacity"]) == 2, led.batch(first.batch_id)["capacity"]
    second = await _start(app, n=20)
    out = sum(led.counts(b).get("in_flight", 0) for b in (first.batch_id, second.batch_id))
    assert out <= 2, f"{out} out on a block whose workers (3) give a window of 2 (second admitted {second.admitted})"


# ---- review round 2 (the reviewer's reproductions, red on 261d843) ----------------------------------------------------

async def test_r2_a_drained_batch_with_rows_out_still_bounds_the_window_it_learnt():
    """r1_3's case, one step on: the first batch learnt from its results that the block has 3 workers (a window of 2)
    and has 2 rows out; the user drains it. A second batch reads the config's 8 workers (a window of 7): the learnt
    count is the BLOCK's (on the ledger), and a batch with rows on the block bounds the window, admitting or not."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app(workers=8)       # the config says 8 workers; the canary names no count
    first = await _start(app, n=20)
    for i, f in enumerate(list(runner.futures)):
        f.finish(_ok_report(i))                          # each result: workers 3 (PARSL_WORKER_COUNT on the node)
    await batch.tick(app, first.batch_id)
    led = batch.ledger_of(app)
    assert int(led.batch(first.batch_id)["capacity"]) == 2
    assert led.counts(first.batch_id).get("in_flight", 0) == 2
    c = await server._cancel(app, first.batch_id, "drain")
    assert c.in_flight == 2, c
    second = await _start(app, n=20)
    out = sum(led.counts(b).get("in_flight", 0) for b in (first.batch_id, second.batch_id))
    assert out <= 2, (f"{out} out on a block whose workers (3) give a window of 2: the drained batch's 2 and the "
                      f"second's {second.admitted} (its capacity {led.batch(second.batch_id)['capacity']})")


async def test_r2_the_learnt_count_outlives_the_batch_that_learnt_it():
    """The first batch finished (done: no rows out, not admitting); the block's 3 workers it learnt still bound the
    next batch, which reads the config's 8."""
    from hpc_bridge import batch

    app, _rt, runner = await _batch_app(workers=8)
    first = await _start(app, n=2)
    for i, f in enumerate(list(runner.futures)):
        f.finish(_ok_report(i))
    await batch.tick(app, first.batch_id)
    led = batch.ledger_of(app)
    assert led.batch(first.batch_id)["state"] == "done"
    second = await _start(app, n=20)
    assert second.admitted == 2, second.admitted


def test_r2_a_restarted_server_with_the_same_pid_holds_no_dead_servers_rows(tmp_path):
    """The server that ran A died; the new server got its pid (a container restart: same hostname, same small pid).
    A's row in flight is a gone server's, bounded by its walltime: it never holds the window for good, and what
    `reserve` counts `block_holders` names."""
    import os

    from hpc_bridge.ledger import Owner

    led = _ledger(tmp_path)
    led.proc_start = lambda pid: "linux:200"                 # this process's start, as the OS says now
    me = Owner("laptop", os.getpid(), "linux:200")
    dead = Owner("laptop", os.getpid(), "linux:100")         # the server that died: same host, same pid, other start
    a = _add(led, "batch-0000000000a1", n=2, capacity=1, owner=dead)
    assert len(led.reserve(a, dead, 1, uec_hash="uec-1", now=1_000.0, block_job="101")) == 1
    assert not led.holder_alive(led.holder(a).holder)        # the ledger's own word: A's server is gone
    c = _add(led, "batch-0000000000c1", n=2, capacity=1, owner=me)
    assert led.block_holders(c, uec_hash="uec-1", block_job="101", owner=me, now=1_010.0) == {a: 1}   # young: holds
    week = 1_000.0 + 7 * 86_400
    assert led.block_holders(c, uec_hash="uec-1", block_job="101", owner=me, now=week) == {}
    assert len(led.reserve(c, me, 1, uec_hash="uec-1", now=week, block_job="101")) == 1


def test_r2_a_live_batchs_unknown_submit_holds_the_block_until_evidence(tmp_path):
    """A's server runs; one of A's submits failed (`submit_unknown`): the service may have queued it, and on a MEP a
    new submit with the same config is exactly what delivers it. It holds the block — A's own window and every other
    batch's — until evidence (`unheld`), and B, refused, is told it is A."""
    from hpc_bridge.ledger import HOLD_MARGIN_S

    led = _ledger(tmp_path)
    me = _owner()
    a = _add(led, "batch-0000000000a1", n=2, capacity=1, owner=me)
    [r] = led.reserve(a, me, 1, uec_hash="uec-1", now=1_000.0, block_job="101")
    assert led.settle(a, r.index, r.attempt, "submit_unknown", only_from=("in_flight",), sent_job="101",
                      sent_uec="uec-1", unknown_since=1_001.0)
    later = 1_000.0 + 60 + HOLD_MARGIN_S + 1
    assert led.reserve(a, me, 1, uec_hash="uec-1", now=later, block_job="101") == []   # A's own window: still full
    b = _add(led, "batch-0000000000b1", n=2, capacity=1, owner=me)
    got = led.reserve(b, me, 1, uec_hash="uec-1", now=later, block_job="101")
    assert got == [] and got.holders == {a: 1}, (list(got), got.holders)
    assert _out_on(led, (a, b), "uec-1") == 1


def test_r2_fairness_order_is_the_same_for_every_reader_and_two_waiters_never_defer_to_each_other(tmp_path):
    """The fairness order counted each batch's rows "on this block" from the CALLER's view (presumed-lost rows only on
    the caller's block_job): two batches whose block_job differ each saw the other with fewer rows out, and each
    deferred — both refused, every pass, with free slots on the block. The order is now the rows in flight or of
    unknown fate on the block, then the last admission, then the id; and a batch refused only by fairness asks for
    nothing, so no one defers to it."""
    led = _ledger(tmp_path)
    me = _owner()
    w = _add(led, "batch-0000000000a1", n=10, capacity=4, owner=me)
    c = _add(led, "batch-0000000000c1", n=10, capacity=4, owner=me)
    rows = led.reserve(w, me, 2, uec_hash="uec-1", now=100.0, block_job="J1")
    for r in rows:                                            # W's two rows on J1: presumed lost, W's server runs
        assert led.settle(w, r.index, r.attempt, "presumed_lost", only_from=("in_flight",), sent_job="J1")
    assert len(led.reserve(c, me, 1, uec_hash="uec-1", now=200.0, block_job="J2")) == 1
    x = _add(led, "batch-0000000000e1", n=3, capacity=4, owner=me)   # a third batch fills the block...
    xs = led.reserve(x, me, 4, uec_hash="uec-1", now=250.0, block_job="J2")
    assert len(xs) == 3
    assert led.reserve(c, me, 4, uec_hash="uec-1", now=260.0, block_job="J2") == []
    assert led.reserve(w, me, 4, uec_hash="uec-1", now=261.0, block_job="J1") == []
    assert led.batch(c)["want_at"] and led.batch(w)["want_at"]
    for r in xs:                                              # ...and its rows finish
        assert led.settle(x, r.index, r.attempt, "done", only_from=("in_flight",))
    got_w = got_c = 0
    for step in range(10):
        now = 270.0 + 5 * step
        got_c += len(led.reserve(c, me, 4, uec_hash="uec-1", now=now, block_job="J2"))
        got_w += len(led.reserve(w, me, 4, uec_hash="uec-1", now=now + 1, block_job="J1"))
    assert got_c + got_w > 0, "10 rounds, both batches backlogged with free slots in every view, nothing admitted"


def test_r2_the_wrapper_holds_its_worker_no_longer_than_its_own_arithmetic_says(tmp_path):
    """The worker is free task_walltime_s + WRAPPER_TAIL_S after the task starts, by the wrapper's own arithmetic: the
    watcher's poll, the kill grace twice (the watcher's TERM->KILL wait, the cleanup's) and the reader grace twice. A
    command that ignores TERM and leaves a child in its own process group holding stdout is that worst case (round 1's
    bound, 7 s, had been short of it; and it had depended on the configurable admission margin)."""
    import os
    import signal

    from hpc_bridge import batch
    from tests.test_batch_phase1 import _run_wrapper

    rd = tmp_path / "rd"
    pidf = tmp_path / "esc.pid"
    wall = 1
    cmd = f"trap '' TERM; set -m; sleep 40 & echo $! > {pidf}; sleep 40"
    script = batch.task_script(cmd, results_dir=str(rd), index=0, attempt=1, task_walltime_s=wall)
    try:
        _run_wrapper(script, {}, tmp_path)
        ended = time.time()
    finally:
        try:
            os.kill(int(pidf.read_text()), signal.SIGKILL)
        except (OSError, ValueError):
            pass
    start = json.loads((rd / "tasks" / "0" / "attempt-1" / "meta.json").read_text())["start"]
    held = ended - start
    allowed = wall + batch.WRAPPER_TAIL_S + 1      # + 1: meta's start is stamped in whole seconds
    assert held >= wall + 2 * batch.KILL_GRACE_S, held      # the slow path was really taken
    assert held <= allowed, f"the worker was held {held:.1f} s past the task's start; the bound is {allowed:g} s"
    assert batch.HOLD_MARGIN_S >= batch.WRAPPER_TAIL_S + batch.DELIVERY_S


def _cancelled_report(index: int, attempt: int = 1):
    """The wrapper's result for a row its CANCELLED marker killed (rc 143)."""
    rep = {"index": index, "attempt": attempt, "rc": 143, "tail": "", "stderr_tail": "", "cancelled": True,
           "stdout_path": f"/scratch/u/batches/b/tasks/{index}/attempt-{attempt}/stdout", "stdout_bytes": 0,
           "truncated": False, "job": "101", "host": "c1", "workers": 3, "remaining_s": 900.0}
    return _Res(0, "HPCB_RESULT " + json.dumps(rep) + "\n", "")


@pytest.mark.parametrize("mode", ["drain", "abort"])
async def test_r1_4_wait_s_ends_once_a_cancelled_batch_has_nothing_left_out(monkeypatch, mode):
    """After cancel(drain) (or a stop's refusal: "wait for them (status(id, wait_s=60))") or cancel(abort), the rows
    still out settle done / cancelled; once nothing is out and the batch admits nothing, the wait ends."""
    from hpc_bridge import batch, server

    async def marker_written(*_a, **_k):
        return True, ""

    monkeypatch.setattr(batch, "write_cancel_marker", marker_written)
    app, _rt, runner = await _batch_app(workers=3)                       # a window of 2
    out = await _start(app, n=6)
    c = await server._cancel(app, out.batch_id, mode)
    assert c.in_flight == 2, c
    app.batch_autoloop = True
    batch._start_loop(app, app.batches[out.batch_id])
    rep = _ok_report if mode == "drain" else _cancelled_report
    loop = asyncio.get_running_loop()
    loop.call_later(0.3, lambda: [runner.futures[i].finish(rep(i)) for i in (0, 1)])
    try:
        t0 = time.monotonic()
        st = await server._status(app, out.batch_id, wait_s=8)
        took = time.monotonic() - t0
    finally:
        run = app.batches.get(out.batch_id)
        if run is not None and run.loop is not None:
            run.loop.cancel()
    assert st.counts.in_flight == 0, st.counts
    assert took < 4, f"status(wait_s=8) on a {mode}ed batch with nothing out slept {took:.1f} s ({st.phase})"


async def test_r1_4_wait_s_on_a_batch_its_own_first_pass_finished_returns_at_once():
    """The baseline is the batch as the call found it: a batch the call's first pass finished — or one already done —
    returns at once (red on 49e342e too)."""
    from hpc_bridge import server

    app, _rt, runner = await _batch_app(workers=3)
    out = await _start(app, n=2)
    for i, f in enumerate(list(runner.futures)):
        f.finish(_ok_report(i))
    t0 = time.monotonic()
    st = await server._status(app, out.batch_id, wait_s=5)
    took = time.monotonic() - t0
    assert st.phase == "done" and took < 2, f"status(wait_s=5) on a done batch slept {took:.1f} s"
    t0 = time.monotonic()
    st = await server._status(app, out.batch_id, wait_s=5)
    assert st.phase == "done" and time.monotonic() - t0 < 2


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


def _branchy(cond: str) -> str:
    return (f"provider:\n{{% if {cond} %}}\n  nodes_per_block: 4\n{{% else %}}\n  nodes_per_block: 1\n"
            "{% endif %}\n")


@pytest.mark.parametrize("cond, wire", [
    ("full_node | default(true, true)", {"full_node": False}),          # Jinja's boolean default: a falsy value -> true
    ("full_node | default(false) or (nodes_wanted > 1)", {"full_node": False, "nodes_wanted": 2}),
    ("full_node | default(false) and big", {"full_node": True, "big": False}),
    ("full_node | default(lookup('x'))", {"full_node": False}),
], ids=["default_boolean_arg", "default_then_or", "default_then_and", "default_of_a_call"])
def test_r1_5_taken_never_drops_a_branch_jinja_renders(cond, wire):
    """Review round 1: `_COND_DEFAULT`'s `(.*?)\\)$` swallowed anything after the default's literal up to the line's last
    `)`, so `x | default(true, true)` and `x | default(a) or (…)` were read as `x`, and the branch Jinja takes was
    dropped. Only `default(<one literal>)` is read now; anything else keeps both branches (their max)."""
    from hpc_bridge import handoff

    jinja2 = pytest.importorskip("jinja2")
    t = _branchy(cond)
    rendered = jinja2.Template(t).render(**wire, lookup=lambda _k: True)          # what the endpoint renders
    got = handoff.effective(t, "nodes_per_block", wire, 1)
    assert got >= (4 if "nodes_per_block: 4" in rendered else 1), (got, handoff.taken(t, wire))
    assert got == 4, handoff.taken(t, wire)             # never told apart: both branches, the most of them


@pytest.mark.parametrize("cond, wire, want", [
    ("full_node | default(true)", {"full_node": False}, 1), ("full_node | default(true)", {}, 4),
    ("full_node is defined and full_node", {"full_node": True}, 4)])
def test_r1_5_a_condition_it_can_tell_still_picks_the_branch_jinja_renders(cond, wire, want):
    from hpc_bridge import handoff

    jinja2 = pytest.importorskip("jinja2")
    t = _branchy(cond)
    assert f"nodes_per_block: {want}" in jinja2.Template(t).render(**wire)
    assert handoff.effective(t, "nodes_per_block", wire, 1, closed=True) == want


FACILITY_TEMPLATE = """engine:
  type: GlobusComputeEngine
  max_workers_per_node: {{ max_workers_per_node|default(4) }}
  job_status_kwargs:
    max_idletime: {{ max_idletime|default(240) }}
  provider:
    type: PBSProProvider
    account: {{ account }}
    queue: {{ queue }}
    walltime: {{ walltime|default("01:00:00") }}
{% if full_node | default(true, true) %}
    nodes_per_block: 4
{% else %}
    nodes_per_block: 1
{% endif %}
    init_blocks: {{ init_blocks|default(0) }}
    min_blocks: {{ min_blocks|default(0) }}
    max_blocks: {{ max_blocks|default(1) }}
"""


async def test_r1_5_export_handoff_terms_never_understate_a_facility_template_branch(tmp_path, monkeypatch):
    """Through export_handoff on a facility MEP: the template renders 4 nodes per block for this config (Jinja's
    `default(true, true)` turns the config's false into true); the terms had said 1 — a quarter of the worst case."""
    from tests.test_handoff_phase2b import POLARIS_SCHEMA, _export, _mep_app

    jinja2 = pytest.importorskip("jinja2")
    app = await _mep_app(tmp_path, monkeypatch, template=FACILITY_TEMPLATE, schema=POLARIS_SCHEMA)
    uec = {"queue": "debug", "account": "hpcb", "walltime": "01:00:00", "max_blocks": 1, "full_node": False}
    ask = await _export(app, uec)
    assert ask.phase == "needs_confirmation", (ask.phase, ask.reason, ask.notice)
    rendered = jinja2.Template(FACILITY_TEMPLATE).render(**ask.user_endpoint_config)
    assert "nodes_per_block: 4" in rendered
    assert ask.terms.nodes_per_block == 4, (ask.terms.nodes_per_block, ask.terms.worst_case, ask.notice)


LINE_TEMPLATE = """engine:
  type: GlobusComputeEngine
  max_workers_per_node: {{ max_workers_per_node|default(4) }}
  provider:
    type: PBSProProvider
    account: {{ account }}
    queue: {{ queue }}
    walltime: {{ walltime|default("01:00:00") }}
    nodes_per_block: {{ nodes_per_block|default(1) }}
    init_blocks: {{ init_blocks|default(0) }}
    min_blocks: {{ min_blocks|default(1, true) }}
    max_blocks: {{ max_blocks|default(1) }}
"""


@pytest.mark.parametrize("line, key, wire", [
    ("min_blocks: {{ min_blocks|default(1, true) }}", "min_blocks", {"min_blocks": 0}),
    ("init_blocks: {{ init_blocks | default(1, boolean=true) }}", "init_blocks", {"init_blocks": 0}),
    ("nodes_per_block: {{ nodes_per_block | d(4, true) }}", "nodes_per_block", {"nodes_per_block": 0}),
    ("nodes_per_block: {{ nodes_per_block | default(1) or 4 }}", "nodes_per_block", {"nodes_per_block": 0}),
    ("x: 1\n{% if full_node %}nodes_per_block: 4{% else %}nodes_per_block: 1{% endif %}\n", "nodes_per_block",
     {"full_node": True}),
], ids=["default_boolean", "default_boolean_kw", "d_alias", "default_then_or", "inline_if"])
def test_r2_a_value_line_is_never_read_as_less_than_jinja_renders(line, key, wire):
    """Review round 2: `readings` (a value line, `key: {{ x | default(lit, true) }}`) read the config's value, though
    Jinja's boolean default turns a falsy one (the 0 hpc-bridge itself sends for init_blocks / min_blocks) into the
    default. A value line is read only as `x` or `x | default(<one literal>)` (then `| int` / `| tojson`); anything
    else — and the key on a line of statements — is unreadable (`_UNSET`: the terms refuse it as unbounded)."""
    import yaml

    from hpc_bridge import handoff

    jinja2 = pytest.importorskip("jinja2")
    rendered = yaml.safe_load(jinja2.Template(line).render(**wire))[key]
    got = handoff.effective(line, key, wire, 1)
    assert got is handoff._UNSET or (isinstance(got, int) and got >= rendered), (
        f"{line!r} with {wire}: Jinja renders {key}={rendered}, the terms read {got!r}")


@pytest.mark.parametrize("line, wire, want", [
    ("walltime: {{ walltime | default(\"00:30:00\" | tojson) }}", {}, "00:30:00"),
    ("init_blocks: {{ init_blocks | default(0) | int }}", {"init_blocks": 2}, 2),
    ("max_blocks: {{ max_blocks }}", {"max_blocks": 3}, 3)])
def test_r2_the_value_lines_it_reads_exactly_still_read(line, wire, want):
    from hpc_bridge import handoff

    key = line.split(":", 1)[0]
    assert handoff.effective(line, key, wire, None, pick="one") == want


async def test_r2_export_handoff_refuses_d6_on_a_template_whose_min_blocks_cannot_be_read_as_0(tmp_path, monkeypatch):
    """Through export_handoff on a facility MEP: hpc-bridge sends min_blocks 0, and the template's boolean default
    renders it 1 — a pool parsl keeps at one block and replaces when cancelled, which D6 refuses on a MEP. A min_blocks
    that cannot be read as 0 is refused as D6, never asked for."""
    from tests.test_handoff_phase2b import POLARIS_SCHEMA, _export, _mep_app

    jinja2 = pytest.importorskip("jinja2")
    import yaml

    app = await _mep_app(tmp_path, monkeypatch, template=LINE_TEMPLATE, schema=POLARIS_SCHEMA)
    uec = {"queue": "debug", "account": "hpcb", "walltime": "01:00:00", "max_blocks": 1}
    ask = await _export(app, uec)
    rendered = yaml.safe_load(jinja2.Template(LINE_TEMPLATE).render(**{**uec, "min_blocks": 0}))
    assert rendered["engine"]["provider"]["min_blocks"] == 1
    assert ask.phase == "refused" and ask.reason == "mep_min_blocks", (ask.phase, ask.reason, ask.notice)
    assert app.runners.built == []


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

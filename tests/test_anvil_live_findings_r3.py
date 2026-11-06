"""Review round 3 of the Anvil live test's fixes (on 7d4ac28), server side. Written failing first; each runs in
conftest's isolated state (never the user's ~/.hpc-bridge). The shell side: tests/test_anvil_live_findings_r3_node.py.

- R3-1 (blocker): a derived block's quote outlived its facility, account and partition. It was keyed by the size
  alone and never cleared, so a yes after connect_facility re-bound elsewhere (or after ensure_endpoint_up moved the
  partition) started 65 CPUs on terms no answer of that binding had named. The same gap reached a resumed batch.
- R3-4: after a stop, a new batch's yes built its block and then paused, asking for a second yes while the block
  billed: the stop had retired the block's config, and only a resume took the yes as consent to it again.
- R3-5: run_batch(batch_id=<a done batch>, cores=…) ignored the resize without a word.
"""
from __future__ import annotations

import dataclasses

from hpc_bridge import server
from hpc_bridge.resources import CORES
from tests.test_batch_sizing import _canary, _measure, _report, _run, _sized_app, _yes

_CANARY = _canary(_measure(cpus=65, cpu_ids=tuple(range(65)), workers=9, mem_limit_mb=40_000))
_EIGHT = dict(n=10, cores=8, memory_gb=4, concurrency=8)        # 8 cores × 8 at once: a 65-CPU, 34 GB block


async def _rebind(app, *, account: str = "other-acct", eid: str = "fake-eid-b") -> None:
    """A re-bind as connect_facility's does it: every shape dropped (`warmth._drop_all_shapes`), another facility with
    its own account, another endpoint."""
    from hpc_bridge import warmth
    from tests.test_resource_requests import _app

    async with app.lock:
        warmth._drop_all_shapes(app, bank=True)
    other = _app(account=account)
    app.facility = other.facility
    app.machine = "facility-b"
    app.state.endpoint_id = eid


# =====================================================================================================================
# R3-1 — a quote is for its terms: facility, endpoint, partition, account and size
# =====================================================================================================================

async def test_r3_1_a_quote_does_not_outlive_its_binding():
    """Repro A: 65 CPUs quoted on facility A, nothing started; connect_facility re-binds to another facility (account
    other-acct, another endpoint); the same call with the yes started 65 CPUs / 34 GB on other-acct at once."""
    app = _sized_app(_CANARY)
    quoted = await _run(app, **_EIGHT)
    assert quoted.phase == "needs_confirmation" and quoted.block.cores_per_node == 65 and app.built == [], quoted
    await _rebind(app)
    yes = await _run(app, confirm_spend=True, **_EIGHT)
    assert app.built == [], f"a block started on terms no answer of this binding showed: {app.built}"
    assert yes.phase == "needs_confirmation" and yes.block.derived_from, (yes.phase, yes.notice)
    assert yes.block.account == "other-acct" and yes.block.cores_per_node == 65, yes.block
    again = await _run(app, confirm_spend=True, **_EIGHT)                  # the yes to THESE terms
    assert app.built and app.built[-1][CORES] == 65 and again.batch_id, (again.phase, again.notice)


async def test_r3_1_a_quote_on_one_partition_starts_nothing_on_another():
    """Repro B: 65 CPUs quoted on `shared`; ensure_endpoint_up(partition="capped", cores=4) answered 4 cores on capped;
    the yes then started 65 CPUs on capped."""
    app = _sized_app(_CANARY)
    quoted = await _run(app, **_EIGHT)
    assert quoted.block.queue == "shared" and quoted.block.cores_per_node == 65, quoted.block
    small = await server._ensure_endpoint_up(app, "compute", partition="capped", cores=4)
    assert small.status == "needs_confirmation" and (small.block.queue, small.block.cores_per_node) == ("capped", 4)
    yes = await _run(app, confirm_spend=True, **_EIGHT)
    assert app.built == [], f"a block started on terms no answer showed: {app.built}"
    assert yes.phase == "needs_confirmation" and (yes.block.queue, yes.block.cores_per_node) == ("capped", 65), (
        yes.phase, yes.block)
    await _run(app, confirm_spend=True, **_EIGHT)
    assert app.built and (app.built[-1]["partition"], app.built[-1][CORES]) == ("capped", 65), app.built


async def test_r3_1_a_quote_is_for_its_endpoint():
    """An endpoint registered again (a repair's new id) is another endpoint: its block is quoted again."""
    app = _sized_app(_CANARY)
    await _run(app, **_EIGHT)
    app.state = dataclasses.replace(app.state, endpoint_id="fake-eid-repaired")
    yes = await _run(app, confirm_spend=True, **_EIGHT)
    assert app.built == [] and yes.phase == "needs_confirmation", (yes.phase, app.built)


async def test_r3_1_a_quote_on_the_same_terms_still_starts_on_the_first_yes():
    """A status call, or the same partition named again, changes no term: the quote stands — no extra round trip."""
    app = _sized_app(_CANARY)
    await _run(app, **_EIGHT)
    await server._ensure_endpoint_up(app, "compute")
    await server._ensure_endpoint_up(app, "compute", partition="shared")
    yes = await _run(app, confirm_spend=True, **_EIGHT)
    assert app.built and app.built[-1][CORES] == 65 and yes.batch_id, (yes.phase, yes.notice)


async def test_r3_1_a_resumed_batchs_quote_does_not_outlive_its_binding():
    """The same gap on the resume path (`_resume`): a batch's re-derived block quoted, then a re-bind — its yes started
    the block on the new binding's terms, unquoted there."""
    from tests.test_anvil_live_findings_r1 import _restarted

    app, bid = await _restarted(size_owed=False)
    quoted = await server._run_batch(app, batch_id=bid)
    assert quoted.phase == "needs_confirmation" and quoted.block.derived_from and app.built == [], quoted
    await _rebind(app)
    yes = await server._run_batch(app, batch_id=bid, confirm_spend=True)
    assert app.built == [], f"a block started on terms no answer of this binding showed: {app.built}"
    assert yes.phase == "needs_confirmation" and yes.block.account == "other-acct", (yes.phase, yes.notice)


# =====================================================================================================================
# R3-4 — after a stop: a new batch's yes starts its block and admits it — never a build, then a second question
# =====================================================================================================================

_NINE = _canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192))
_TWO = dict(n=2, cores=2, memory_gb=1.5, concurrency=4)                 # 2 CPUs × 4 at once: a 9-CPU block


async def _stopped(monkeypatch):
    """A sized batch quoted, the yes, its rows settled; stop_endpoint: the block released, its config retired."""
    from hpc_bridge import batch, scheduler_ops

    async def released(_app, _eid, _run_login):   # the scheduler's cancel, over the login shape: it went through
        return True, ""

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", released)
    monkeypatch.setenv("HPC_BRIDGE_STOP_CHECK_WAIT_S", "0")   # the fake's checks never queue: no wait for them
    app = _sized_app(_NINE)
    first = await _yes(app, **_TWO)
    assert first.batch_id, (first.phase, first.notice)
    await batch.tick(app, first.batch_id)
    h = batch.uec_hash(app.shapes["compute"])
    st = await server._stop_endpoint(app)
    assert "compute" not in app.shapes, (st.status, st.notice)
    assert app.ledger.retired(app.state.endpoint_id, h), "the stop retired no config: the scenario is not set up"
    return app, h


async def test_r3_4_after_a_stop_a_new_batchs_yes_starts_its_block_and_admits_it(monkeypatch):
    """The yes counts for the config, as a resume's does and as ensure_endpoint_up's yes after a stop starts it: the
    config is taken back before anything is recorded, and the batch admits — no block built, then a second yes."""
    app, h = await _stopped(monkeypatch)
    built = len(app.built)
    again = await _run(app, confirm_spend=True, **_TWO)
    assert again.phase != "needs_confirmation" or len(app.built) == built, (
        "a block was built, and then the batch asked for the yes again", again.notice)
    assert again.batch_id and again.phase in ("admitting", "waiting") and (again.admitted or 0) > 0, (
        again.phase, again.notice)
    assert app.ledger.retired(app.state.endpoint_id, h) is None


async def test_r3_4_a_new_batch_without_the_yes_on_a_retired_config_asks_once_and_records_nothing(monkeypatch):
    """A block a yes brought back after the stop, but a config the stop retired: with no yes on the call, nothing is
    recorded and the one question is asked; the yes then admits."""
    from hpc_bridge import batch

    app, _h = await _stopped(monkeypatch)
    up = await _run(app, confirm_spend=True, **_TWO)                    # the yes after the stop
    assert up.batch_id, (up.phase, up.notice)
    await batch.tick(app, up.batch_id)
    rt = app.shapes["compute"]
    app.ledger.retire(app.state.endpoint_id, batch.uec_hash(rt), why="stop_endpoint released this block")
    rt.spend_confirmed = True                                          # … and the block confirmed once more since
    n = len(app.ledger.batches())
    asked = await _run(app, **_TWO)
    assert asked.phase == "needs_confirmation" and asked.batch_id is None, (asked.phase, asked.notice)
    assert len(app.ledger.batches()) == n, "a batch was recorded, then paused to ask"
    assert "retired" in (asked.notice or "") and "confirm_spend=True" in (asked.notice or ""), asked.notice
    yes = await _run(app, confirm_spend=True, **_TWO)
    assert yes.batch_id and (yes.admitted or 0) > 0, (yes.phase, yes.notice)


# =====================================================================================================================
# R3-5 — a resize of a batch that has ended is answered, never ignored
# =====================================================================================================================

async def test_r3_5_a_resize_of_a_done_batch_says_it_is_done_and_names_a_new_batch():
    from hpc_bridge import batch
    from tests.test_batch_sizing import _app_with

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=1, pilot=1)
    runner.futures[0].finish(_report(0, cpu_s=3.9, wall_s=2.0, peak_rss_kb=400 * 1024, nproc=4, cpus="0,1,2,3"))
    await batch.tick(app, out.batch_id)
    assert (await batch.status(app, out.batch_id)).phase == "done"
    again = await server._run_batch(app, batch_id=out.batch_id, cores=2, memory_gb=0.5)
    assert again.phase == "refused", (again.phase, again.notice)
    words = again.notice or ""
    assert "done" in words and "new batch" in words and "cores=2" in words and "memory_gb=0.5" in words, words
    assert len(runner.commands) == 1, "the resize sent rows"

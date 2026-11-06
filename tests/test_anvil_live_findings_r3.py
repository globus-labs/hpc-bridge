"""Review round 3 of the Anvil live test's fixes (on 7d4ac28), server side. Written failing first; each runs in
conftest's isolated state (never the user's ~/.hpc-bridge). The shell side: tests/test_anvil_live_findings_r3_node.py.

- R3-1 (blocker): a derived block's quote outlived its facility, account and partition. It was keyed by the size
  alone and never cleared, so a yes after connect_facility re-bound elsewhere (or after ensure_endpoint_up moved the
  partition) started 65 CPUs on terms no answer of that binding had named. The same gap reached a resumed batch.
"""
from __future__ import annotations

import dataclasses

from hpc_bridge import server
from hpc_bridge.resources import CORES
from tests.test_batch_sizing import _canary, _measure, _run, _sized_app

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

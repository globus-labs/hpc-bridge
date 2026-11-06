"""Review round 2 of the Anvil live test's fixes: no billed block starts on terms no answer showed — on a NEW sized
batch's path too, not only a resume's (round 1). Written failing first.

A free agent cell: connect_facility quoted "1 whole node, 4 cores, ≤0.5 node-hours", the user said yes, and the agent's
first call was run_batch(rows=6, cores=1, memory_gb=0.5, concurrency=2, confirm_spend=True) — which started a derived
3-CPU / 2 GB block no answer had named. Smaller there; 8 cores × 8 at once is 65 cores after a yes to a small slice.
A sized batch's yes starts its derived block only when an answer of this process quoted exactly it; otherwise the call
quotes it (needs_confirmation, `derived_from`) and starts nothing, and the next yes starts it.
"""
from __future__ import annotations

from hpc_bridge import server
from hpc_bridge.resources import CORES
from tests.test_batch_sizing import _canary, _measure, _run, _sized_app

_CANARY = _canary(_measure(cpus=65, cpu_ids=tuple(range(65)), workers=9, mem_limit_mb=40_000))


async def test_r2_eight_cores_by_eight_on_the_first_call_quotes_its_65_cpu_block_before_any_start():
    app = _sized_app(_CANARY)
    first = await _run(app, n=10, cores=8, memory_gb=4, concurrency=8, confirm_spend=True)
    assert app.built == [], f"a block started on terms no answer showed: {app.built}"
    assert first.phase == "needs_confirmation" and first.block is not None, (first.phase, first.notice)
    assert first.block.cores_per_node == 65 and first.block.derived_from, first.block
    assert first.batch_id is None and (app.ledger is None or not app.ledger.batches())      # nothing recorded
    assert "nothing started" in (first.notice or "") and "confirm_spend=True" in (first.notice or ""), first.notice
    yes = await _run(app, n=10, cores=8, memory_gb=4, concurrency=8, confirm_spend=True)    # the yes, to THESE terms
    assert app.built and app.built[-1][CORES] == 65 and yes.batch_id, (yes.phase, yes.notice, app.built)


async def test_r2_a_block_smaller_than_the_one_connect_quoted_is_quoted_too():
    """The agent cell: the session's default terms were quoted (and agreed to); the batch's derived block is another
    — smaller — and was never named: it is quoted first."""
    app = _sized_app(_canary(_measure(cpus=3, cpu_ids=(0, 1, 2), workers=3, mem_limit_mb=2048)))
    default = await server._ensure_endpoint_up(app, "compute")
    assert default.status == "needs_confirmation" and default.block.cores_per_node != 3, default.block
    first = await _run(app, n=6, cores=1, memory_gb=0.5, concurrency=2, confirm_spend=True)
    assert app.built == [], f"a block started on terms no answer showed: {app.built}"
    assert first.phase == "needs_confirmation" and first.block.cores_per_node == 3 and first.block.derived_from, (
        first.phase, first.block)
    await _run(app, n=6, cores=1, memory_gb=0.5, concurrency=2, confirm_spend=True)
    assert app.built and app.built[-1][CORES] == 3, app.built


async def test_r2_a_block_already_quoted_starts_on_the_first_yes_no_extra_round_trip():
    app = _sized_app(_CANARY)
    asked = await _run(app, n=10, cores=2, memory_gb=1.5, concurrency=4)                       # the spend question
    assert asked.phase == "needs_confirmation" and asked.block.cores_per_node == 9 and app.built == []
    yes = await _run(app, n=10, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    assert app.built and app.built[-1][CORES] == 9 and yes.batch_id, (yes.phase, yes.notice)


async def test_r2_a_sized_batch_on_the_block_already_up_rides_it_with_no_quote():
    app = _sized_app(_CANARY)
    await _run(app, n=1, cores=2, memory_gb=1.5, concurrency=4)
    await _run(app, n=1, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    rt = app.shapes["compute"]
    confirmed, uec = rt.confirmed_at, dict(rt.user_endpoint_config)
    out = await _run(app, n=4, cores=1, memory_gb=1.0, concurrency=4, confirm_spend=True)
    assert out.batch_id and out.phase in ("admitting", "waiting"), (out.phase, out.notice)
    assert app.shapes["compute"] is rt and rt.confirmed_at == confirmed and rt.user_endpoint_config == uec


async def test_r2_the_size_first_answers_tell_the_agent_size_first_then_the_yes():
    """needs_sizing says to send the size (its answer quotes the block), then the user's yes; the quote says the yes
    is the same call with confirm_spend=True."""
    app = _sized_app(_CANARY)
    need = await _run(app, n=4)
    assert need.phase == "needs_sizing", need
    assert "quotes the block" in (need.notice or "") and "then" in (need.notice or ""), need.notice
    asked = await _run(app, n=4, cores=2, memory_gb=1.5, concurrency=4)
    assert "confirm_spend=True" in (asked.notice or "") and "derived" in (asked.notice or ""), asked.notice

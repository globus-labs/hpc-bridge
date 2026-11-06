"""Review round 1 of the Anvil live test's fixes (on 0eaf3e9), server side and pure. Written failing first; each runs in
conftest's isolated state (never the user's ~/.hpc-bridge). The node side: tests/test_anvil_live_findings_r1_node.py.

- R1-3: after a restart, one call could start a block whose terms no answer ever showed — a batch waiting for its
  size was told to send its size and the yes together, and G2 started the block derived from the NEW size unquoted.
- R1-4: G1's env_failed was consumed by run_batch's (or submit_job's) own ensure-up, whose answers had no such field.
- R1-6: two rounding rules for a facility template's memory (the terms could say less than was sent), and a tiny
  memory the terms said as "0 GB".
- R1-7: `uses_module` matched `module`/`ml` anywhere; the login node's check lost the `nomodule` hint.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from hpc_bridge import server
from tests.test_batch_sizing import _canary, _measure, _run, _sized_app

# =====================================================================================================================
# R1-3 — no billed block starts on terms the answers never showed
# =====================================================================================================================


async def _restarted(*, size_owed: bool):
    """A size-first batch (2 CPUs, 1.5 GB, 4 at once: a 9-CPU block), its block ended (and, `size_owed`, a pause for its
    size); the server restarts: a new process with no block."""
    from hpc_bridge import batch
    from hpc_bridge.ledger import Ledger, Owner
    from tests.test_batch_phase1 import _owner

    canary = _canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192))
    app = _sized_app(canary)
    out = await _run(app, n=10, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    batch.pause(app, out.batch_id, "the block ended (its walltime)")
    await batch.tick(app, out.batch_id)
    if size_owed:
        batch.pause_sizing(app, out.batch_id, "rows were killed at their memory")
    app.ledger.close()
    app2 = _sized_app(canary)
    app2.batch_owner = _owner(pid=os.getpid() + 100_000, host=Owner.here().host)
    app2.ledger = Ledger.open()
    app2.ledger.pid_alive = lambda pid: pid != os.getpid()
    app2.ledger.lease_stale_s = 0.0
    return app2, out.batch_id


async def test_r1_3_a_new_size_and_the_yes_in_one_call_never_start_an_unquoted_block():
    """The reviewer's run: resized to 8 CPUs × 8 at once with confirm_spend=True after a restart, a 65-CPU, 34 GB block
    started that no answer had quoted. Now the call quotes it (needs_confirmation, `derived_from`), and the next yes
    starts exactly it."""
    from hpc_bridge.resources import CORES

    app, bid = await _restarted(size_owed=True)
    first = await server._run_batch(app, batch_id=bid)
    assert "its size and the user's yes, in one call" not in (first.notice or ""), first.notice
    res = await server._run_batch(app, batch_id=bid, cores=8, memory_gb=4, concurrency=8, confirm_spend=True)
    assert app.built == [], f"a block started on terms no answer showed: {app.built}"
    assert res.phase == "needs_confirmation" and res.block is not None, (res.phase, res.notice)
    assert res.block.cores_per_node == 65 and res.block.derived_from, res.block
    yes = await server._run_batch(app, batch_id=bid, confirm_spend=True)
    assert app.built and app.built[-1][CORES] == 65, (yes.phase, yes.notice, app.built)


async def test_r1_3_a_yes_alone_after_a_restart_first_shows_the_block_it_would_start():
    """No answer of this process quoted the derived block: the yes alone shows its terms first, and starts nothing."""
    from hpc_bridge.resources import CORES

    app, bid = await _restarted(size_owed=False)
    res = await server._run_batch(app, batch_id=bid, confirm_spend=True)
    assert app.built == [] and res.phase == "needs_confirmation", (res.phase, res.notice, app.built)
    assert res.block is not None and res.block.cores_per_node == 9 and res.block.derived_from, res.block
    await server._run_batch(app, batch_id=bid, confirm_spend=True)
    assert app.built and app.built[-1][CORES] == 9, app.built


# =====================================================================================================================
# R1-4 — env_failed reaches the agent whichever call is first
# =====================================================================================================================

async def test_r1_4_run_batch_first_after_a_pending_envs_failure_carries_it(tmp_path):
    from tests.test_anvil_live_findings import _pending_that_fails

    app = await _pending_that_fails(tmp_path)
    out = await server._run_batch(app, command="echo {row}", rows=[1, 2], task_walltime_s=60)
    failed = getattr(out, "env_failed", None)
    assert failed is not None and failed.name == "broken" and "nosuchpkg" in (failed.error or ""), (out.phase, out)
    assert "was not built" in (out.notice or ""), out.notice
    st = await server._ensure_endpoint_up(app, "compute")
    assert st.env_failed is None                                     # said once


async def test_r1_4_submit_job_first_after_a_pending_envs_failure_carries_it(tmp_path):
    from tests.test_anvil_live_findings import _pending_that_fails

    app = await _pending_that_fails(tmp_path)
    out = await server._submit_job(app, command="echo hi", walltime="00:10:00")
    failed = getattr(out, "env_failed", None)
    assert failed is not None and failed.name == "broken", (out.phase, out.notice)
    assert "was not built" in (out.notice or ""), out.notice
    st = await server._ensure_endpoint_up(app, "compute")
    assert st.env_failed is None                                     # said once


# =====================================================================================================================
# R1-6 — one rounding rule for a facility template's memory; no memory of nothing
# =====================================================================================================================

@pytest.mark.parametrize(("asked", "whole"), [(2.0, 2), (2.0000000001, 3), (1.5, 2), (0.25, 1)])
def test_r1_6_the_terms_and_the_template_round_the_same_way_never_down(asked, whole):
    from hpc_bridge import resources
    from hpc_bridge.facility.mep import MEPFacility
    from hpc_bridge.models import PartitionShape

    shape = PartitionShape(name="shared", kind="per_core", cores=128, memory_gb=256.0)
    uec = {resources.CORES: 2, resources.MEM: asked, resources.WHOLE: False, "compute": True}
    terms = resources.effective(uec, shape, channel="mep", takes=True)
    fac = SimpleNamespace(partitions=[shape], key_map={}, schema={}, takes_size_key=lambda _k: True)
    sent = MEPFacility.dispatch_uec(fac, dict(uec))   # type: ignore[arg-type]
    assert resources.whole_gb(asked) == whole and sent[resources.MEM] == whole and terms.memory_gb == whole, (
        asked, sent[resources.MEM], terms.memory_gb)


@pytest.mark.parametrize("tiny", [1e-10, 0.0001])
async def test_r1_6_a_memory_under_a_megabyte_is_refused_everywhere(tiny):
    from hpc_bridge import resources
    from tests.test_batch_sizing import _app_with

    assert resources.requested(memory_gb=tiny)[1] is not None
    app, _rt, runner = _app_with(_measure())
    out = await _run(app, n=2, cores=1, memory_gb=tiny)
    assert out.phase == "refused" and "memory_gb" in (out.reason or ""), (out.phase, out.reason)
    assert runner.commands == []


# =====================================================================================================================
# R1-7 — `module` as a command word only; the login check's nomodule
# =====================================================================================================================

@pytest.mark.parametrize(("line", "uses"), [
    ("module load gcc", True), ("ml gcc", True), ("source x.sh && module load y", True), ("(module load z)", True),
    ("true; ml anaconda", True), ("a || module load b", True), ("x | ml y", True), ("module", True),
    ("if true; then module load a; fi", True),
    ("conda activate ml", False), ("python -m module", False), ("echo module load", False),
    ("export MODULEPATH=/x", False), ("ml_tool run", False), ("source ~/ml/bin/activate", False),
])
def test_r1_7_uses_module_matches_a_command_word_only(line, uses):
    from hpc_bridge import envs

    assert envs.uses_module(line) is uses, line


def test_r1_7_the_login_checks_nomodule_is_said_in_hpc_bridges_words():
    from hpc_bridge import envs
    from hpc_bridge.context import AppCtx
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    spec = envs.parse({"activate": "module load anaconda/2025.12-py313"})
    k = envs._known(app, spec)

    class _Done:
        def cancelled(self) -> bool:
            return False

        def exception(self) -> None:
            return None

        def result(self):
            return SimpleNamespace(stdout="HPCB_ENV_ACTIVATE rc=88 nomodule out_b64=\n", returncode=0)

    k.future, k.kind = _Done(), "activate"
    envs._absorb(app, k)
    assert k.fault == "nomodule" and "no module system" in (k.info.error or ""), (k.fault, k.info.error)
    words = envs._activate_words(app, k, k.info)
    assert "no module system" in words and ". /etc/profile.d/modules.sh; module load" in words, words

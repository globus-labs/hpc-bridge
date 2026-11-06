"""The Anvil live test's findings (2026-11-05; Purdue Anvil: Slurm 23.11, 128-core EPYC nodes, GPFS scratch, RHEL 8,
cgroup v1) and the gaps the agent-cell implementer reported, server side and pure. Written failing first, each before
its fix. The node side (the generated bash, run for real): tests/test_anvil_live_findings_node.py.

- F1: a pinned row is flagged over its cores only on evidence of oversubscription — runnable threads above its cores
  in most of the watcher's samples — never on its thread count (idle threads included).
- F3: whether a row ran srun steps reaches its result and status.
- F5: a facility worker's shell with no module system: the refusal says so and names the init line.
- G1: a pending env's build failure is a structured field (`env_failed`) on the answer that first reports it.
- G2: resuming a size-first batch whose block is gone asks for its derived block again, and its terms say so.
- G3: an unsized batch never runs rows without a memory watch: it waits for the block's measure.
- G4: `memory_gb` is a number everywhere (1.5 GB can be asked), sent in MB where the scheduler takes MB.
- G5: run_batch's description says what `idempotent` decides for rows killed at the default memory.
"""
from __future__ import annotations

import json
import os

from hpc_bridge import server
from tests.test_batch_sizing import _app_with, _canary, _measure, _report, _run, _sized_app

# =====================================================================================================================
# F1 — over its cores, on evidence
# =====================================================================================================================


def test_f1_a_pinned_row_is_over_its_cores_only_on_sustained_runnable_threads():
    """Anvil: the pilot measured 2 cores and the user accepted; the rows at 2 cores (29.8 CPU-s over 15 s, the shell,
    `timeout` and a sleeping python beside their 2 busy loops) were all flagged "ran over their size"."""
    from hpc_bridge import sizing as sz

    pilot = dict(cores=2, memory_mb=None, pinned=True, cpu_s=29.8, wall_s=15.0, threads=5, peak_rss_kb=10_000,
                 why=None)
    assert sz.over_size(**pilot, r_over=0, r_samples=15) == []
    assert sz.over_size(**pilot, r_over=2, r_samples=15) == []                # a burst (a fork, a pool's start)
    assert sz.over_size(**pilot, r_over=14, r_samples=15) == ["cores"]        # 4 busy threads on 2 CPUs, sustained
    assert sz.over_size(**pilot, r_over=2, r_samples=2) == []                 # too few samples to call it sustained
    assert sz.over_size(**pilot, r_over=None, r_samples=None) == []           # no evidence (no /proc, an old result)
    unpinned = {**pilot, "pinned": False}                                     # unpinned: CPU-s ÷ wall, as before
    assert sz.over_size(**{**unpinned, "cpu_s": 45.0}, r_over=None, r_samples=None) == ["cores"]
    assert sz.over_size(**unpinned, r_over=None, r_samples=None) == []


async def test_f1_status_never_flags_a_pinned_row_on_its_thread_count_alone():
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5))
    out = await _run(app, n=2, cores=2)
    runner.futures[0].finish(_report(0, cpu_s=29.8, wall_s=15.0, peak_rss_kb=1024, nproc=2, cpus="0,1", threads=5))
    runner.futures[1].finish(_report(1, cpu_s=30.0, wall_s=15.0, peak_rss_kb=1024, nproc=2, cpus="2,3", threads=6,
                                     r_over=14, r_samples=15))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id, detail="page")
    rows = {r.index: r for r in st.results}
    assert rows[0].over_size == [] and rows[1].over_size == ["cores"], (rows[0].over_size, rows[1].over_size)


# =====================================================================================================================
# F3 — a row that ran srun steps says so
# =====================================================================================================================

async def test_f3_a_row_that_ran_srun_steps_says_so_in_its_result_and_status():
    from hpc_bridge import batch

    rep = batch.parse_result("HPCB_RESULT " + json.dumps({"index": 0, "attempt": 1, "rc": 0, "srun": True}))
    assert rep is not None and getattr(rep, "srun", None) is True
    app, _rt, runner = _app_with(_measure(cpus=4, workers=5))
    out = await _run(app, n=2, cores=1)
    runner.futures[0].finish(_report(0, cpu_s=1.0, wall_s=2.0, srun=True))
    runner.futures[1].finish(_report(1, cpu_s=1.0, wall_s=2.0, srun=False))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id, detail="page")
    rows = {r.index: r for r in st.results}
    assert getattr(rows[0], "srun_steps", None) is True and getattr(rows[1], "srun_steps", None) is False
    assert "srun" in (st.notice or "") and "not the row's" in (st.notice or ""), st.notice


# =====================================================================================================================
# F5 — no module system in the worker's shell: said, with the line to add
# =====================================================================================================================

def test_f5_the_refusal_of_a_module_line_with_no_module_system_says_so_everywhere():
    from hpc_bridge import envs

    act = envs.Activation(how="activate", key="d" * 16, line="module load anaconda/2025.12-py313")
    ref = envs.Refused("activate", "d" * 16, "rc=127 nomodule")

    class _App:
        def __init__(self) -> None:
            self.env_spec = None
            self.env_known: dict = {}

    _phase, notice = envs.refused(_App(), ref, act, "/bin/bash: line 44: module: command not found\n")
    assert "no module system" in notice and ". /etc/profile.d/modules.sh; module load" in notice, notice
    words = envs.module_words(ref.detail)
    assert "no module system" in words and "/etc/profile.d/modules.sh" in words


# =====================================================================================================================
# G1 — a pending env's failure, structured
# =====================================================================================================================

async def _pending_that_fails(tmp_path):
    from tests.test_block_envs import _ssh

    app = await _ssh(tmp_path)
    await server._ensure_endpoint_up(app, "compute", env={"activate": "export HPCB_TAG=old"})
    app.runners.hold_builds = True                               # the install outlasts the call that asked for it
    st = await server._ensure_endpoint_up(app, "compute", env={"requirements": "nosuchpkg==1.0", "name": "broken"})
    assert st.env is not None and st.env.state == "building", st.env
    app.runners.release()                                        # ...then fails (uv: no solution)
    return app


async def test_g1_ensure_endpoint_up_says_a_pending_envs_failure_in_a_field_of_its_own(tmp_path):
    app = await _pending_that_fails(tmp_path)
    st = await server._ensure_endpoint_up(app, "compute")
    failed = getattr(st, "env_failed", None)
    assert failed is not None, st
    assert failed.state == "failed" and failed.name == "broken" and "nosuchpkg" in (failed.error or ""), failed
    assert st.env is not None and st.env.kind == "activate", st.env            # the session's env, unchanged
    again = await server._ensure_endpoint_up(app, "compute")
    assert again.env_failed is None                                            # on the answer that FIRST reports it


async def test_g1_run_shell_carries_it_when_it_is_the_first_to_report_it(tmp_path):
    app = await _pending_that_fails(tmp_path)
    out = await server._run_shell(app, 'echo "T=$HPCB_TAG"', agent_call=True)
    assert out.phase == "complete" and "T=old" in out.stdout, out
    assert getattr(out, "env_failed", None) is not None and out.env_failed.name == "broken", out
    st = await server._ensure_endpoint_up(app, "compute")
    assert st.env_failed is None


# =====================================================================================================================
# G2 — a size-first batch resumed after a restart asks for its own block
# =====================================================================================================================

async def test_g2_a_size_first_batch_resumed_after_a_restart_asks_for_its_derived_block():
    from hpc_bridge import batch
    from hpc_bridge.ledger import Ledger, Owner
    from hpc_bridge.resources import CORES, MEM
    from tests.test_batch_phase1 import _owner

    canary = _canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192))
    app = _sized_app(canary)
    out = await _run(app, n=10, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    assert out.batch_id and app.built[-1][CORES] == 9, app.built
    batch.pause(app, out.batch_id, "the block ended (its walltime)")   # its first rows settle as the block ends
    await batch.tick(app, out.batch_id)
    assert not app.ledger.counts(out.batch_id).get("in_flight"), app.ledger.counts(out.batch_id)
    app.ledger.close()                                            # the server is killed; a new one starts, no block
    app2 = _sized_app(canary)
    app2.batch_owner = _owner(pid=os.getpid() + 100_000, host=Owner.here().host)
    app2.ledger = Ledger.open()
    app2.ledger.pid_alive = lambda pid: pid != os.getpid()
    app2.ledger.lease_stale_s = 0.0
    asked = await server._run_batch(app2, batch_id=out.batch_id)
    assert asked.block is not None and asked.block.cores_per_node == 9, (asked.phase, asked.block, asked.notice)
    assert asked.block.derived_from and "4 rows at once" in asked.block.derived_from, asked.block
    assert app2.built == []                                       # nothing started before the yes
    res = await server._run_batch(app2, batch_id=out.batch_id, confirm_spend=True)
    assert app2.built and app2.built[-1][CORES] == 9 and app2.built[-1][MEM] >= 6, app2.built
    assert res.block is not None and res.block.derived_from, (res.phase, res.block, res.notice)
    assert res.phase in ("admitting", "waiting"), (res.phase, res.notice)


async def test_g2_a_batch_sized_on_a_confirmed_block_resumes_on_the_sessions_block_as_before():
    """Only a block that is gone is derived again: a batch whose session block is confirmed rides it."""
    app, _rt, _runner = _app_with(_measure(cpus=4, workers=5))
    out = await _run(app, n=3, cores=1, memory_gb=0.5)
    built = len(app.uecs)
    res = await server._run_batch(app, batch_id=out.batch_id, confirm_spend=True)
    assert res.phase in ("admitting", "waiting") and len(app.uecs) == built, (res.phase, res.notice)


# =====================================================================================================================
# G3 — no row runs unsized before the block is measured
# =====================================================================================================================

async def test_g3_an_unsized_batch_waits_for_the_measure_and_then_runs_with_a_memory_watch():
    from hpc_bridge import batch

    app, rt, runner = _app_with(None)                             # the block's check has not measured it yet
    out = await _run(app, n=4)
    assert runner.commands == [], "rows went out unsized: no memory watch, no slot"
    assert out.phase == "waiting" and "measured" in (out.notice or "") + (out.reason or ""), (out.phase, out.notice)
    rt.last_canary = _canary(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))   # the measure answers
    await batch.tick(app, out.batch_id)
    assert len(runner.commands) == 4, len(runner.commands)
    s = (await batch.status(app, out.batch_id)).sizing
    assert s is not None and s.memory_gb and "memory_gb" in s.defaulted, s
    for i in range(4):                                            # each row holds a memory, and is watched at it
        assert app.ledger.task(out.batch_id, i)["req_memory_mb"], app.ledger.task(out.batch_id, i)


# =====================================================================================================================
# G4 — memory_gb is a number everywhere
# =====================================================================================================================

async def test_g4_every_tools_memory_gb_takes_a_fraction(tmp_path):
    """Each tool that takes memory_gb is called with 1.5 GB and takes it (review round 1: an annotation grep proved
    nothing), and the schema an agent sees says a number."""
    from tests.fake_sched import FakeSched
    from tests.test_batch_phase2a import SLICED, _submit
    from tests.test_batch_phase2a import _app as _jobs_app
    from tests.test_resource_requests import _app

    schemas = {t.name: t.inputSchema for t in await server.mcp.list_tools()}
    for tool in ("ensure_endpoint_up", "submit_job", "run_batch"):
        prop = schemas[tool]["properties"]["memory_gb"]
        types = {x.get("type") for x in prop.get("anyOf", [prop])}
        assert "number" in types and "integer" not in types, (tool, prop)
    st = await server._ensure_endpoint_up(_app(), "compute", cores=2, memory_gb=1.5)
    assert st.status == "needs_confirmation" and st.block.memory_gb == 1.5, (st.status, st.notice)
    jobs_app = _jobs_app(tmp_path, FakeSched(tmp_path / "sched", user="u"), partitions=[SLICED])
    job = await _submit(jobs_app, partition="shared", cores=2, memory_gb=1.5)
    assert job.phase != "refused" and job.terms is not None and job.terms.memory_gb == 1.5, (job.phase, job.notice)
    app, _rt, _runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=8000))
    out = await _run(app, n=2, cores=1, memory_gb=1.5)
    assert out.batch_id and out.sizing.memory_gb == 1.5, (out.phase, out.notice)


def test_g4_the_request_takes_a_fraction_and_refuses_what_is_no_amount():
    from hpc_bridge import resources

    ok, bad = resources.requested(memory_gb=1.5)
    assert bad is None and ok[resources.MEM] == 1.5
    for v in (0, -1, True, "1.5", float("nan")):
        assert resources.requested(memory_gb=v)[1] is not None, v
    assert resources.requested(cores=1.5)[1] is not None                     # cores stay whole


async def test_g4_a_block_of_one_and_a_half_gb_is_asked_of_slurm_in_megabytes():
    from hpc_bridge.resources import MEM
    from tests.test_resource_requests import _app

    app = _app()
    asked = await server._ensure_endpoint_up(app, "compute", cores=2, memory_gb=1.5)
    assert asked.status == "needs_confirmation" and "2 cores, 1.5 GB" in (asked.notice or ""), asked.notice
    st = await server._ensure_endpoint_up(app, "compute", cores=2, memory_gb=1.5, confirm_spend=True)
    assert st.status in ("up", "provisioning"), (st.status, st.notice)
    assert st.block is not None and st.block.memory_gb == 1.5, st.block
    sent = app.built[-1]
    assert MEM not in sent and "#SBATCH --mem=1536M" in (sent.get("scheduler_options") or ""), sent
    app2 = _app()
    await server._ensure_endpoint_up(app2, "compute", cores=2, memory_gb=2, confirm_spend=True)
    assert app2.built[-1][MEM] == 2                                          # a whole GB: parsl's own key, as before


def test_g4_pbs_and_a_facility_template_get_what_they_take():
    from hpc_bridge import resources
    from hpc_bridge.models import PartitionShape

    shape = PartitionShape(name="shared", kind="per_core", cores=128, memory_gb=256.0)
    uec = {resources.CORES: 2, resources.MEM: 1.5, resources.WHOLE: False, "compute": True}
    pbs = resources.pbs_wire(uec, resources.effective(uec, shape, channel="ssh", scheduler="pbs"), shape)
    assert "mem=1536mb" in pbs["select_options"], pbs
    mep = resources.effective(uec, shape, channel="mep", takes=True)
    assert mep.memory_gb == 2.0 and mep.send_mem == 2, mep            # a template's GB key: whole, rounded UP


def test_g4_a_job_script_asks_for_megabytes_and_the_terms_never_round_down():
    from hpc_bridge import jobs, resources
    from hpc_bridge.context import AppCtx
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    spec = jobs.Spec(command="true", session_id="default", account=None, partition=None, nodes=1, whole_node=False,
                     cores=2, memory_gb=1.5, gpus=None, walltime_s=60, tasks=1, throttle=None, array=None, depends=[],
                     rate=1.0, per_task=1.0, worst_case=1.0, basis="", min_charge_h=None, shape=None)
    script = jobs.job_script(app, job_id="job-0123456789ab", envelope_id="env-0123456789ab", spec=spec, deadline=0.0)
    assert "#SBATCH --mem=1536M" in script, script[:600]
    words = resources.size_words(nodes=1, whole_node=False, cores=2, memory_gb=12.5, memory_default=False, gpus=None)
    assert "12.5 GB" in words, words


# =====================================================================================================================
# G5 — idempotent, said
# =====================================================================================================================

def test_g5_run_batchs_description_says_what_idempotent_decides_after_a_default_memory_kill():
    doc = " ".join((server.run_batch.__doc__ or "").split())
    assert "idempotent" in doc and "default memory" in doc and "resize" in doc, doc[-900:]

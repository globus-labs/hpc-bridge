"""Batch phase 2b, review round 1 (8 defects at 5b47a2f), written failing first. The first seven are the reviewer's own
reproductions (scratchpad review-p2b/test_review_p2b.py), moved in-tree; the rest pin the fixes the round asked for:
scheduler directives that would move the bounds the terms and the release are built on, the scope of a stop that keeps
loam's pool (per call, never shared), one canary per caller's config across a session change, a restart and a client-side
failure, a queued canary that every later stop still sees, the minimum-charge cycle when the walltime is shorter than
the idle window, init_blocks above max_blocks, the stop's one budget, and worker_init said in the terms."""
from __future__ import annotations

import asyncio
import time

import pytest

from hpc_bridge import server
from hpc_bridge.models import PartitionShape
from hpc_bridge.profile import Profile
from tests.test_handoff_phase2b import (
    LOAM_UEC,
    POLARIS_SCHEMA,
    POLARIS_TEMPLATE,
    SESSION_UEP,
    SSH_EID,
    _Block,
    _export,
    _mep_app,
    _Out,
    _sent,
    _ssh_app,
    _ssh_with_handoff,
    _yes,
)

SSH_UEC = {"partition": "main", "walltime": "00:10:00", "max_blocks": 1}


# ---- F1: scheduler directives that override the terms' or the release's bounds are refused, by name -------------------

async def test_f1_scheduler_options_that_resize_the_block_are_refused_or_counted(tmp_path):
    app = _ssh_app(tmp_path)
    uec = {"partition": "main", "walltime": "00:10:00", "max_blocks": 1,
           # sbatch takes the LAST of a repeated option; parsl puts scheduler_options after its own --nodes/--time
           "scheduler_options": "#SBATCH --nodes=64\n#SBATCH --time=2-00:00:00"}
    out = await asyncio.wait_for(_export(app, uec, expires_hours=24), 60)
    if out.phase == "refused":
        assert "--nodes" in (out.notice or "") and "nodes_per_block" in (out.notice or ""), out.notice
        return
    assert out.phase == "needs_confirmation", out
    assert out.terms.worst_case >= 64 * (24 + 48), (out.terms.worst_case, out.terms.basis)


async def test_f1b_an_output_directive_hides_loams_blocks_and_cancel_says_down(tmp_path):
    from tests.fake_sched import FakeSched

    sched = FakeSched(tmp_path / "sched", user="u")
    home = tmp_path / "home" / ".globus_compute"
    sched.add_job(state="RUNNING", name="parsl.mine",
                  stdout=f"{home}/uep.{SSH_EID}.{SESSION_UEP}/submit_scripts/parsl.mine.stdout")
    loam = sched.add_job(state="RUNNING", name="parsl.loam", stdout="/scratch/loam/1002.out")
    block = _Block(tmp_path, SSH_EID, scheduler="slurm", job=loam, pin=None, workers=2, sched=sched)
    app = _ssh_app(tmp_path, block=block)
    uec = {**SSH_UEC, "scheduler_options": "#SBATCH --output=/scratch/loam/%j.out"}
    ask = await asyncio.wait_for(_export(app, uec), 60)
    if ask.phase == "refused":     # the fix: the output path the release finds loam's blocks by is not loam's to move
        assert "--output" in (ask.notice or ""), ask.notice
        assert app.runners.built == []
        return
    out = await asyncio.wait_for(_export(app, uec, confirm_spend=True), 60)
    assert out.phase == "exported", out
    st = await asyncio.wait_for(server._stop_endpoint(app, handoffs="cancel"), 100)
    assert not (st.status == "down" and sched.job(loam)["state"] == "RUNNING"), (st.status, st.notice)


@pytest.mark.parametrize("opts", [
    "#SBATCH --partition=gpu", "#SBATCH -A other", "#SBATCH --gres=gpu:4", "#SBATCH --array=0-99",
    "#SBATCH -N 64", "#SBATCH -t 48:00:00", "#SBATCH -e /tmp/x", "#SBATCH --ntasks-per-node=64",
    "#SBATCH --mem=0", "#SBATCH --exclusive", "#SBATCH --qos=premium", "#SBATCH --wrap=hostname",
    "#SBATCH --job-name=ok\n#SBATCH --nodes=2",
], ids=lambda s: s.split("\n")[-1][8:28])
async def test_f1_every_bound_moving_slurm_directive_is_refused_by_name(tmp_path, opts):
    app = _ssh_app(tmp_path)
    out = await _export(app, {**SSH_UEC, "scheduler_options": opts})
    assert out.phase == "refused" and out.reason == "invalid", (opts, out)
    word = opts.split("\n")[-1].split()[1].split("=")[0]
    assert word in (out.notice or ""), (word, out.notice)
    assert app.runners.built == []


async def test_f1_harmless_slurm_directives_pass(tmp_path):
    app = _ssh_app(tmp_path)
    out = await _export(app, {**SSH_UEC, "scheduler_options": "#SBATCH --job-name=loam\n#SBATCH --mail-type=END"})
    assert out.phase == "needs_confirmation", out


@pytest.mark.parametrize("opts", ["#PBS -l select=64:ncpus=64", "#PBS -l walltime=48:00:00", "#PBS -l place=excl",
                                  "#PBS -q prod", "#PBS -A other", "#PBS -o /tmp/x", "#PBS -J 0-99",
                                  "#PBS -l filesystems=home,select=8"])
async def test_f1_every_bound_moving_pbs_directive_is_refused_by_name(tmp_path, monkeypatch, opts):
    app = await _mep_app(tmp_path, monkeypatch, template=POLARIS_TEMPLATE, schema=POLARIS_SCHEMA)
    out = await _export(app, {**LOAM_UEC, "scheduler_options": opts})
    assert out.phase == "refused" and out.reason == "invalid", (opts, out)
    assert app.runners.built == []


async def test_f1_the_filesystems_directive_passes(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch, template=POLARIS_TEMPLATE, schema=POLARIS_SCHEMA)
    out = await _export(app, {**LOAM_UEC, "scheduler_options": "#PBS -l filesystems=home:eagle\n#PBS -N loam"})
    assert out.phase == "needs_confirmation", out


@pytest.mark.parametrize("sel", ["+63:ncpus=64", "ncpus=64", "ngpus=4+1:ncpus=8", "mpiprocs=64"])
async def test_f1_select_options_that_add_chunks_or_cores_are_refused(tmp_path, monkeypatch, sel):
    app = await _mep_app(tmp_path, monkeypatch)        # the fake pbs_mep template renders select_options
    out = await _export(app, {**LOAM_UEC, "select_options": sel})
    assert out.phase == "refused" and "select_options" in (out.notice or ""), (sel, out)


async def test_f1_a_gpu_select_option_passes(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    out = await _export(app, {**LOAM_UEC, "select_options": "ngpus=4"})
    assert out.phase == "needs_confirmation", out


# ---- F2 (review item 6): init_blocks above max_blocks ------------------------------------------------------------------

async def test_f2_init_blocks_above_max_blocks_is_counted(tmp_path):
    app = _ssh_app(tmp_path)
    uec = {"partition": "main", "walltime": "01:00:00", "max_blocks": 1, "init_blocks": 4}
    out = await asyncio.wait_for(_export(app, uec, expires_hours=24), 60)
    assert out.phase in ("needs_confirmation", "refused"), out
    if out.phase == "refused":
        assert "init_blocks" in (out.notice or "") and "max_blocks" in (out.notice or ""), out.notice
        return
    t = out.terms
    assert t.worst_case >= t.canary_worst_case, t
    assert t.worst_case >= 4 * t.rate * (24 + 1), (t.worst_case, t.basis)


async def test_the_worst_case_is_never_rounded_down(tmp_path):
    # a rate whose hourly figure rounds down at two decimals (0.004 per core-hour × 1 core): the large multiply must
    # start from the exact rate, and only the final figure is rounded — up
    odd = PartitionShape(name="odd", kind="whole_node", cores=1, billing={"cpu": 0.004})
    app = _ssh_app(tmp_path, partitions=(odd,))
    out = await _export(app, {"partition": "odd", "walltime": "01:00:00", "max_blocks": 3}, expires_hours=168)
    assert out.phase == "needs_confirmation", out
    assert out.terms.worst_case >= 3 * 0.004 * (168 + 1) - 1e-9, (out.terms.worst_case, out.terms.basis)


# ---- F3 (review item 5): the minimum-charge cycle when the walltime is shorter than the idle window --------------------

async def test_f3_min_charge_with_walltime_shorter_than_idle(tmp_path):
    preempt = PartitionShape(name="preempt", kind="whole_node", cores=4, billing={"cpu": 1.0}, min_charge_h=2.0)
    app = _ssh_app(tmp_path, partitions=(preempt,))
    uec = {"partition": "preempt", "walltime": "00:05:00", "nodes_per_block": 1, "max_blocks": 2}
    out = await asyncio.wait_for(_export(app, uec, expires_hours=6), 60)
    assert out.phase == "needs_confirmation", out
    assert Profile().max_idletime_s > 300
    # an independent worst case: with work queued, a block ends at its 5-minute walltime and parsl starts the next
    # (TIMEOUT is not a block failure): each of the 2 slots starts 12 blocks an hour for 6 h, each charged 2 h, plus
    # one started just before the record expires
    starts_per_slot = 6 / (5 / 60)
    pathological = 2 * 4.0 * (starts_per_slot * 2.0 + 2.0)
    assert out.terms.worst_case >= pathological - 1e-6, (out.terms.worst_case, pathological, out.terms.basis)


# ---- F4 (review item 3): one canary per caller's config --------------------------------------------------------------

async def test_f4_repeat_call_never_sends_a_second_canary_when_the_session_choice_moved(tmp_path):
    app = _ssh_app(tmp_path)
    app.runners.pending = True
    uec = {"walltime": "00:10:00", "max_blocks": 1}
    _ask, out = await asyncio.wait_for(_yes(app, uec), 90)
    assert out.phase == "validating", out
    app.compute_choice["partition"] = "gpu"
    again = await asyncio.wait_for(_export(app, uec, confirm_spend=True), 90)
    assert len(_sent(app)) == 1, (again.phase, len(_sent(app)))
    assert again.phase == "validating" and again.task_id == out.task_id, again


async def test_a_restarted_server_collects_the_canary_by_its_task_id_and_never_resends(tmp_path):
    app = _ssh_app(tmp_path)
    app.runners.pending = True
    _ask, out = await _yes(app, SSH_UEC)
    assert out.phase == "validating" and out.task_id, out
    fresh = _ssh_app(tmp_path, block=app.runners.block)          # a new server on the same state directory
    fresh.runners = app.runners
    fresh.runner_factory = app.runners
    sent = len(_sent(app))
    again = await _export(fresh, SSH_UEC, confirm_spend=True)
    assert again.phase == "validating" and again.task_id == out.task_id, again
    app.runners.run_pending()
    done = await _export(fresh, SSH_UEC)
    assert done.phase == "exported" and done.record.validation.task_id == out.task_id, done
    assert len(_sent(app)) == sent == 1


async def test_a_client_side_failure_keeps_the_canary_and_collects_it_by_id(tmp_path):
    app = _ssh_app(tmp_path)
    app.runners.fail = RuntimeError("the SDK's result watcher quit")   # the client's failure, not the endpoint's
    app.runners.fail_tid = True
    _ask, out = await _yes(app, SSH_UEC)
    assert out.phase == "validating" and out.task_id, out
    again = await _export(app, SSH_UEC, confirm_spend=True)
    assert again.phase == "validating" and again.task_id == out.task_id, again
    assert len(app.runners.futures) == 1                             # never a second canary


# ---- F5 (review item 4): a queued canary is seen by every later stop -------------------------------------------------

async def test_f5_the_stop_after_cancel_is_not_down_while_the_canary_is_still_queued(tmp_path):
    app = _ssh_app(tmp_path)
    app.runners.pending = True
    _ask, out = await asyncio.wait_for(_yes(app, SSH_UEC), 90)
    assert out.phase == "validating", out
    first = await asyncio.wait_for(server._stop_endpoint(app, handoffs="cancel"), 100)
    assert first.status != "down", first
    fut = app.runners.futures[0][0]
    assert not fut.done()
    second = await asyncio.wait_for(server._stop_endpoint(app), 100)
    assert second.status != "down", (second.status, second.notice)
    fut.finish(_Out(0, "HPCB_CANARY\n", ""))            # the canary finally ran: nothing of it is left queued
    third = await asyncio.wait_for(server._stop_endpoint(app), 100)
    assert third.status == "down", (third.status, third.notice)


async def test_keep_while_the_canary_is_queued_is_draining_and_the_next_stop_still_sees_it(tmp_path):
    app = _ssh_app(tmp_path)
    app.runners.pending = True
    _ask, out = await _yes(app, SSH_UEC)
    st = await server._stop_endpoint(app, handoffs="keep")
    assert st.status == "draining" and out.task_id in (st.notice or ""), st


# ---- F6 (review item 2): one stop's keep scope is its own ------------------------------------------------------------

async def test_f6_a_concurrent_stop_clears_anothers_keep_scope(tmp_path, monkeypatch):
    app, sched, _mine, loam, _out = await _ssh_with_handoff(tmp_path)
    real = server._stop_endpoint_jobs
    gate = asyncio.Event()
    calls = []

    async def slowed(a, jobs=None, **kw):
        calls.append(1)
        if len(calls) == 1:
            await gate.wait()
        return await real(a, jobs, **kw)

    monkeypatch.setattr(server, "_stop_endpoint_jobs", slowed)
    first = asyncio.create_task(server._stop_endpoint(app, handoffs="keep"))
    await asyncio.sleep(0)
    second = await asyncio.wait_for(server._stop_endpoint(app, handoffs="keep"), 100)
    gate.set()
    first_st = await asyncio.wait_for(first, 100)
    assert sched.job(loam)["state"] == "RUNNING", (sched.jobs()[loam], first_st.notice, second.status)


# ---- F7 (review item 7): the handoff's extra read is inside the stop's one budget --------------------------------------

async def test_cancel_stays_within_the_stops_one_budget(tmp_path, monkeypatch):
    app, _sched, _mine, _loam, _out = await _ssh_with_handoff(tmp_path)
    monkeypatch.setattr(server, "STOP_BUDGET_S", 4.0)
    real = server._login_runner

    def slow_runner(a):
        run = real(a)

        async def go(cmd):
            if "handoff's user endpoint could not be found" in cmd:   # the handoff's own read, after the release
                await asyncio.sleep(30)
            return await run(cmd)
        return go

    monkeypatch.setattr(server, "_login_runner", slow_runner)
    t0 = time.monotonic()
    st = await server._stop_endpoint(app, handoffs="cancel")
    assert time.monotonic() - t0 < 4.0 + 2.0, time.monotonic() - t0
    assert st.status == "draining" and "not confirmed" in (st.notice or ""), st


# ---- worker_init is shell, by design: the terms say so --------------------------------------------------------------

async def test_the_terms_say_worker_init_runs_as_shell(tmp_path, monkeypatch):
    app = await _mep_app(tmp_path, monkeypatch)
    ask = await _export(app, LOAM_UEC)
    assert "worker_init" in ask.terms.binds and "shell" in ask.terms.binds, ask.terms.binds
    doc = next(t for t in await server.mcp.list_tools() if t.name == "export_handoff").description or ""
    assert "worker_init" in doc and "shell" in doc, doc

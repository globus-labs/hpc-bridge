"""Batch phase 2a, fix round 1 (2026-11-01): the live cells at aaf9248, the fake cluster's probe facts and two reviews
(submission safety, spend honesty). Written failing first on aaf9248 (red log: scratchpad red_p2a_r1.txt); each test
names the finding it pins. tests/fake_sched.py now answers as the probed Slurm 23.11 / OpenPBS 23.06 do (a user's
`scontrol update Deadline=` is denied; `scontrol show job` prints ReqTRES/AllocTRES; sacct has no row for a pending
array task; …), which is what turned two shipped tests red.

D2's safety property (the coordinator's direction): hpc-bridge never cancels, holds or releases a job it cannot prove is
this command's or already ours. A hand submit's jobs are identified by EVIDENCE — the receipt the shim's own call to the
real sbatch/qsub printed, kept in a per-command mark file, and a before/after snapshot of the user's jobs — never by
digits in the command's output. The shim (a wrapper on PATH, so `command`/`env`/`nohup`/`timeout`/`xargs`/`nice`/`exec`
reach it) adds the hold, the envelope's deadline and the marker in ONE submit, and keeps itself out of the job's
environment. A marked job is released only once its deadline and its fit are read back; an unmarked one that appeared
meanwhile is metered only ("submitted unheld, no deadline applied") and never cancelled without the user's say-so.

Round 3 (the user's decision) simplified D2 to what the design says: no wrapper, no hold, no mark file, no release, no
cancel-while-held — the command runs unmodified and what appeared between its two snapshots is adopted after the fact,
never cancelled by hpc-bridge on its own. The A1–A4 tests below that pinned the hold were converted to that behaviour
(their property — never cancel or touch a job the evidence does not show, never read digits as receipts — still
holds); the deadline read-back tests went with the hold (no envelope deadline is applied to an adopted job).
"""
from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

import pytest

from hpc_bridge.models import PartitionShape
from tests.test_batch_phase2a import (  # noqa: F401 - fixtures and helpers
    _BASH,
    _app,
    _confirmed,
    _epoch,
    _hand_script,
    _new_session,
    _submit,
    pbs,
    world,
)

_FIXTURES = (world, pbs)   # pytest fixtures, used by name as test parameters below
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]


def _others(world, first):
    return [j for j in world.sched.submitted() if j["id"] != first.job.native_id]


# ======================================================================================================================
# Review A (submission safety) — the reviewers' reproductions, adapted to the probe-faithful fake
# ======================================================================================================================

# A1 (CRITICAL): any digits-only stdout line was a receipt; a job that did not fit was scancel'd — a listing of the user's
# own jobs cancelled our ledgered job and the user's unrelated RUNNING one.
async def test_A1_squeue_after_a_hand_sbatch_does_not_cancel_an_unrelated_job(world):
    from hpc_bridge import server

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 6})      # 4 of 6 held: the hand job won't fit
    other = world.sched.add_job(name="production", state="RUNNING", timelimit_min=60, billing=4)   # NOT ours
    script = _hand_script(world)
    out = await server._run_shell(world.app, f"sbatch {script}; squeue -u u -h -o %i", shape="login",
                                  agent_call=True, confirm_spend=True)
    assert world.sched.job(other)["state"] == "RUNNING", (out.notice, world.sched.calls("scancel"))
    assert world.sched.job(first.job.native_id)["state"] != "CANCELLED", world.sched.calls("scancel")
    assert other not in [j.native_id for j in (out.jobs or [])], out.jobs
    hand = _others(world, first)
    hand = [j for j in hand if j["id"] != other]
    assert hand and hand[0]["state"] != "CANCELLED" and world.sched.calls("scancel") == [], hand   # nothing cancelled
    assert [j.native_id for j in out.jobs or []] == [hand[0]["id"]], out.jobs   # only the job that appeared


def test_A1_a_receipt_is_read_only_from_the_submit_calls_own_output_and_never_a_number_or_an_address():
    """The receipts come from the shim's mark file (what the REAL sbatch/qsub printed, per call), never the command's
    stdout; and a PBS receipt is `<seq>[[]].<server>` — never `3.14` or `10.0.0.1`."""
    from hpc_bridge import jobs

    assert jobs._receipts_in("pbs", "3.14\n10.0.0.1\n42\n") == []
    assert jobs._receipts_in("pbs", "1001.pbsserver\n1002[].polaris-pbs-01.hsn\n") == ["1001.pbsserver",
                                                                                     "1002[].polaris-pbs-01.hsn"]
    assert jobs._receipts_in("slurm", "4242\nSubmitted batch job 17\n4243;cluster\n") == ["4242", "17", "4243"]


# A2: a receipt captured by $(…): the job is adopted all the same (by the snapshot diff, never by the receipt)
async def test_A2_a_captured_parsable_receipt_is_adopted(world):
    from hpc_bridge import server

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    script = _hand_script(world)
    out = await server._run_shell(world.app, f'jid=$(sbatch --parsable {script}); echo "queued as $jid"',
                                  shape="login", agent_call=True, confirm_spend=True)
    hand = _others(world, first)
    assert len(hand) == 1 and out.jobs and out.jobs[0].native_id == hand[0]["id"], (out.notice, hand)


# A2: whatever the command's outcome, what it submitted ends up on the ledger — a command still running past the
# sync-wait has its after-snapshot written when it ENDS, and the next call adopts
async def test_A2_a_job_whose_command_outlived_the_sync_wait_is_adopted_by_the_next_call(world):
    from hpc_bridge import batch, server
    from tests.test_server import _PendingFuture

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    script = _hand_script(world)
    runner = world.app.test_runner
    real_submit = runner.submit

    def slow(command):   # the agent's command (session `default`) comes back `running`: still going past the wait
        fut = real_submit(command)
        return _PendingFuture() if "/sessions/default" in command else fut

    runner.submit = slow
    out = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
    runner.submit = real_submit
    hand = _others(world, first)
    assert hand, hand
    await server._status(world.app, first.envelope.envelope_id)          # the next call adopts
    rows = [j for j in batch.ledger_of(world.app).jobs() if j.get("native_id") == hand[0]["id"]]
    assert rows and rows[0]["source"] == "adopt", ("the job is not on the ledger", out)


# A3: the gate recognises these forms; the yes runs them as they are, and the job that appeared is adopted
@pytest.mark.parametrize("wrap", ["command sbatch", "timeout 60 sbatch", "nohup sbatch", "env sbatch", "nice sbatch",
                                  "exec sbatch"])
async def test_A3_a_gated_wrapped_sbatch_is_adopted(world, wrap):
    from hpc_bridge import batch, server

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    script = _hand_script(world)
    asked = await server._run_shell(world.app, f"{wrap} {script}", shape="login", agent_call=True)
    assert asked.phase == "needs_confirmation" and "adopted" in (asked.notice or "")
    await server._run_shell(world.app, f"{wrap} {script}", shape="login", agent_call=True, confirm_spend=True)
    hand = _others(world, first)
    rows = [j for j in batch.ledger_of(world.app).jobs() if hand and j.get("native_id") == hand[0]["id"]]
    assert hand and not hand[0]["held_at_submit"] and rows, (hand, rows)


# A3: nothing of hpc-bridge's reaches the job's own environment (sbatch --export=ALL): the command runs unmodified
async def test_A3_nothing_leaks_into_the_submitted_jobs_environment(world):
    from hpc_bridge import server

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    script = _hand_script(world)
    await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
    hand = _others(world, first)[0]
    env = hand["submit_env"]
    assert not [k for k in env if k.startswith(("BASH_FUNC_", "HPCB_SHIM"))] and "hpcb-shim" not in env.get("PATH", "")


# A3 (and A6): a submit by absolute path is adopted like any other: metered, named, no deadline, never cancelled
async def test_A3_a_submit_by_absolute_path_is_adopted_named_and_never_cancelled(world):
    from hpc_bridge import batch, server

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 6})
    script = _hand_script(world)
    real = world.sched.bin / "sbatch"
    out = await server._run_shell(world.app, f"{real} {script}", shape="login", agent_call=True, confirm_spend=True)
    hand = _others(world, first)
    assert len(hand) == 1 and not hand[0]["held_at_submit"] and hand[0]["state"] != "CANCELLED", hand
    assert "appeared while your approved command ran" in (out.notice or "") and "deadline" in (out.notice or "")
    assert world.sched.calls("scancel") == []
    rows = [j for j in batch.ledger_of(world.app).jobs() if j.get("native_id") == hand[0]["id"]]
    assert rows and rows[0]["source"] == "adopt", rows                  # metered: it counts against the envelope


# A4: a line's first submit call named the Submission — `srun …; sbatch x` was not gated at all
async def test_A4_srun_then_sbatch_on_the_login_shape_is_gated(world):
    from hpc_bridge import server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    before = len(world.sched.calls("sbatch"))
    script = _hand_script(world)
    out = await server._run_shell(world.app, f"srun -N1 hostname; sbatch {script}", shape="login", agent_call=True)
    assert out.phase == "needs_confirmation", out
    assert len(world.sched.calls("sbatch")) == before


async def test_A6_login_shell_refuses_a_hand_submit_while_an_envelope_is_open(world):
    from hpc_bridge import server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    ran: list[str] = []

    async def login_exec(cmd):
        ran.append(cmd)
        return 0, "Submitted batch job 9\n", ""

    world.app.facility.login_exec = login_exec
    out = await server._login_shell(world.app, "sbatch ~/x.sh")
    assert ran == [] and out.exit_code != 0 and "submit_job" in (out.notice or ""), out


async def test_A6_a_run_batch_template_that_submits_jobs_is_refused(world):
    from hpc_bridge import server

    out = await server._run_batch(world.app, command="sbatch job{n}.sh", rows=[{"n": 1}, {"n": 2}],
                                  task_walltime_s=60)
    assert out.phase == "refused" and "submit_job" in (out.notice or "") and "array" in (out.notice or ""), out


# A4 (converted, round 3): no envelope deadline is applied to an adopted job, and none is ever set after submit — a job
# whose walltime runs past the envelope's deadline is adopted, counted to its own walltime, and never cancelled
async def test_A4_an_adopted_job_gets_no_deadline_and_a_long_one_is_never_cancelled(pbs):
    from hpc_bridge import server

    first = await _confirmed(pbs.app, command="sleep 60", partition="workq",
                             envelope={"cap": 50, "deadline_hours": 2})
    script = pbs.tmp / "home" / "hand.pbs"
    script.write_text("#!/bin/bash\n#PBS -l select=1:ncpus=4\n#PBS -l walltime=05:00:00\nsleep 300\n")
    out = await server._run_shell(pbs.app, f"qsub {script}", shape="login", agent_call=True, confirm_spend=True)
    hand = next(j for j in pbs.sched.submitted() if j["id"] != first.job.native_id.split(".")[0])
    assert hand["state"] != "CANCELLED" and pbs.sched.calls("qdel") == [], (out.notice, hand)
    assert out.jobs and out.jobs[0].worst_case == pytest.approx(20.0) and out.jobs[0].deadline is None, out.jobs


# A5: adoption understated the worst case
async def test_A5_a_hand_pbs_array_is_reserved_for_every_task(pbs):
    from hpc_bridge import server

    await _confirmed(pbs.app, command="sleep 60", partition="workq", envelope={"cap": 100})
    script = pbs.tmp / "home" / "hand.pbs"
    script.write_text("#!/bin/bash\n#PBS -l select=1:ncpus=4\n#PBS -l walltime=01:00:00\nsleep 300\n")
    out = await server._run_shell(pbs.app, f"qsub -J 0-9 {script}", shape="login", agent_call=True,
                                  confirm_spend=True)
    assert out.jobs and out.jobs[0].worst_case == pytest.approx(40.0), out.jobs   # 10 tasks × 4 × 1 h


def test_A5_a_stepped_slurm_array_counts_every_task():
    from hpc_bridge import jobs

    line = ("JobId=7 ArrayJobId=7 ArrayTaskId=0-98:2 JobName=x JobState=PENDING Reason=JobHeldUser "
            "TimeLimit=01:00:00 NumNodes=1 NumCPUs=4 TRES=cpu=4,node=1,billing=4")
    r = jobs._slurm_request("7", line)
    assert r is not None and r.tasks == 50, r


def test_A5_slurm_23_req_tres_billing_is_read():
    from hpc_bridge import jobs

    line = ("JobId=8 JobName=x JobState=PENDING Reason=JobHeldUser TimeLimit=01:00:00 NumNodes=1 NumCPUs=4 "
            "ReqTRES=cpu=4,mem=16G,node=1,billing=64,gres/gpu=1 AllocTRES=(null)")
    r = jobs._slurm_request("8", line)
    assert r is not None and r.rate == pytest.approx(64.0), r


# A7: the "frozen" job.sh read the session's cwd/env when it RUNS, not as they were when it was submitted
async def test_A7_a_queued_job_runs_with_the_env_it_was_submitted_with(world):
    sd = Path(world.app.scratch_root) / "sessions" / "dev"
    sd.mkdir(parents=True)
    (sd / ".env").write_text("export HPCB_PROMO=at-submit\n")
    out = await _confirmed(world.app, command="echo $HPCB_PROMO", session_id="dev")
    (sd / ".env").write_text("export HPCB_PROMO=changed-later\n")              # a later run_shell in session dev
    env = {"PATH": os.environ["PATH"], "HOME": str(world.tmp / "home"), "SLURM_JOB_ID": out.job.native_id}
    p = subprocess.run([_BASH, out.job.script_path], capture_output=True, text=True, env=env, timeout=30)
    assert p.stdout.splitlines()[:1] == ["at-submit"], p.stdout + p.stderr


# A8: a 0-exit sbatch with no receipt is a submit of unknown fate (found by its name), never "refused"
async def test_A8_a_zero_exit_sbatch_with_no_receipt_is_submit_unknown_and_found_by_its_name(world):
    from hpc_bridge import server

    first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    world.sched.knob(no_receipt=True)
    out = await _submit(world.app, command="sleep 61")
    assert out.phase == "queued" and out.reason == "submit_unknown", out
    st = await server._status(world.app, out.job.job_id)
    assert st.job.native_id and st.job.native_id != first.job.native_id, st.job       # the sweep found it by name


async def test_A8_a_stop_cancels_a_submit_of_unknown_fate_by_its_name(world):
    from hpc_bridge import batch, server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    out = await _submit(world.app, command="sleep 61")
    led = batch.ledger_of(world.app)
    led.update_job(out.job.job_id, native_id=None, state="submit_unknown")          # its answer never came back
    world.sched.knob(hide_squeue=[], hide_sacct=[out.job.native_id])                # … and sacct cannot name it yet
    await server._stop_endpoint(world.app, jobs="cancel")
    assert any(f"--name=hpcb-{out.job.job_id}" in " ".join(c) for c in world.sched.calls("scancel")), \
        world.sched.calls("scancel")
    assert world.sched.job(out.job.native_id)["state"] == "CANCELLED"


# A8: per-command round trips: a hand submit of several jobs is read and released in a bounded number of login calls
async def test_A8_adopting_several_hand_jobs_takes_a_bounded_number_of_login_calls(world):
    from hpc_bridge import server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 100})
    script = _hand_script(world)
    before = len(world.app.test_runner.commands)
    await server._run_shell(world.app, " ; ".join([f"sbatch {script}"] * 5), shape="login", agent_call=True,
                            confirm_spend=True)
    assert len(world.app.test_runner.commands) - before <= 7, len(world.app.test_runner.commands) - before


# ======================================================================================================================
# Review B (spend honesty) — the reviewers' reproductions
# ======================================================================================================================

def _pool_rows(app):
    from hpc_bridge import batch

    return [j for j in batch.ledger_of(app).jobs() if j.get("source") == "pool_block"]


async def _capture_provision(monkeypatch, app):
    from hpc_bridge import warmth

    seen: dict = {}
    real = warmth._provision

    async def fake_provision(app_, shape, **kw):
        if shape != "compute":   # the free login shape (the meter's channel) provisions as ever
            return await real(app_, shape, **kw)
        seen["pool"] = [float(j["worst_case"]) for j in _pool_rows(app_) if j["state"] != "refused"]
        seen["confirm"] = kw.get("confirm_spend")
        raise RuntimeError("stopped by the review test before any block is submitted")

    monkeypatch.setattr(warmth, "_provision", fake_provision)
    return seen


async def test_B1_block_reserved_at_the_nodes_the_call_confirms(world, monkeypatch):
    from hpc_bridge import server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 100})
    rt = server._shape_runtime(world.app, "compute")
    rt.user_endpoint_config["walltime"] = "01:00:00"
    seen = await _capture_provision(monkeypatch, world.app)
    await server._ensure_endpoint_up(world.app, "compute", confirm_spend=True, partition="main", nodes=4)
    assert seen.get("confirm") is True
    assert seen["pool"] and max(seen["pool"]) >= 16.0 - 1e-6, seen


async def test_B1_block_on_a_min_charge_queue_reserved_at_least_the_minimum(world, monkeypatch):
    from hpc_bridge import server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 100})
    rt = server._shape_runtime(world.app, "compute")
    rt.user_endpoint_config["walltime"] = "01:00:00"
    seen = await _capture_provision(monkeypatch, world.app)
    await server._ensure_endpoint_up(world.app, "compute", confirm_spend=True, partition="preempt")
    assert seen["pool"] and max(seen["pool"]) >= 8.0 - 1e-6, seen


async def test_B1_a_resize_of_a_confirmed_block_is_reserved(world, monkeypatch):
    from hpc_bridge import server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 100})
    rt = server._shape_runtime(world.app, "compute")
    rt.user_endpoint_config.update({"walltime": "01:00:00", "partition": "main"})
    rt.spend_confirmed = True
    seen = await _capture_provision(monkeypatch, world.app)
    await server._ensure_endpoint_up(world.app, "compute", confirm_spend=True, partition="main", nodes=4)
    assert seen.get("confirm") is True
    assert seen.get("pool") and max(seen["pool"]) >= 16.0 - 1e-6, seen


async def test_B1_a_pool_reservation_does_not_hide_every_parsl_job_from_the_sweep(world, monkeypatch):
    """A block reservation stands for one block: parsl pilots beyond what it holds still count."""
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 100})
    rt = server._shape_runtime(world.app, "compute")
    rt.user_endpoint_config["walltime"] = "01:00:00"
    await _capture_provision(monkeypatch, world.app)
    await server._ensure_endpoint_up(world.app, "compute", confirm_spend=True, partition="main")   # holds 4
    for _ in range(3):   # three pilots nobody confirmed, each 4 units × 1 h
        world.sched.add_job(name="parsl.slurm.1", state="RUNNING", elapsed=0, timelimit_min=60, billing=4)
    env = await server._status(world.app, out.envelope.envelope_id)
    assert env.envelope.spent + env.envelope.committed >= 4 + 12 - 1e-6, env.envelope


async def test_B3_a_sacct_outage_keeps_counting_unledgered_jobs(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 10})
    world.sched.add_job(name="bypass", state="RUNNING", elapsed=0, timelimit_min=60, billing=4)
    env = await server._status(world.app, out.envelope.envelope_id)
    assert env.envelope.remaining == pytest.approx(2.0), env.envelope
    world.sched.knob(sacct_down=True)
    again = await _submit(world.app, command="sleep 61", confirm_spend=True)
    assert again.phase == "needs_confirmation" and again.reason == "envelope_exhausted", again
    assert len(world.sched.calls("sbatch")) == 1


async def test_B3_the_first_job_of_a_new_envelope_counts_unledgered_jobs(world):
    world.sched.add_job(name="bypass", state="RUNNING", elapsed=0, timelimit_min=60, billing=4)
    out = await _submit(world.app, command="sleep 60", envelope={"cap": 6}, confirm_spend=True)
    assert out.phase not in ("queued", "running"), (out.phase, out.envelope)
    assert world.sched.calls("sbatch") == []


def test_B4_scontrol_23_11_billing_is_read():
    from hpc_bridge import jobs

    line = ("JobId=7 JobName=hand UserId=u(1000) GroupId=u(1000) Priority=1 Account=hpcb QOS=normal "
            "JobState=PENDING Reason=JobHeldUser Dependency=(null) TimeLimit=01:00:00 Partition=gpu "
            "NumNodes=1-1 NumCPUs=1 NumTasks=1 CPUs/Task=1 ReqTRES=cpu=1,mem=4000M,node=1,billing=8,gres/gpu=2 "
            "AllocTRES=(null) Deadline=N/A")
    r = jobs._slurm_request("7", line)
    assert r is not None and r.rate == pytest.approx(8.0), r


async def test_B4_adopt_reserves_at_least_the_queues_minimum(world):
    await _confirmed(world.app, command="sleep 60", envelope={"cap": 50})
    hand = world.sched.add_job(name="hand", state="RUNNING", elapsed=60, timelimit_min=30, billing=4,
                               partition="preempt")
    out = await _submit(world.app, adopt=hand, confirm_spend=True)
    assert out.job is not None and out.job.worst_case >= 8.0 - 1e-6, out


async def test_B2_a_stop_is_not_down_while_a_job_it_raced_is_submitted(world, monkeypatch):
    from hpc_bridge import jobs, server

    await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
    gate, entered = asyncio.Event(), asyncio.Event()
    real = jobs.write_job_script

    async def slow(app, job_id, text, **kw):
        entered.set()
        await gate.wait()
        return await real(app, job_id, text, **kw)

    monkeypatch.setattr(jobs, "write_job_script", slow)
    sub = asyncio.create_task(_submit(world.app, command="sleep 61"))
    await entered.wait()
    st = await server._stop_endpoint(world.app, jobs="cancel")
    gate.set()
    out = await sub
    assert not (st.status == "down" and out.phase in ("queued", "running")), (st.status, out.phase, out.notice)
    live = [j for j in world.sched.submitted() if j["state"] in ("PENDING", "RUNNING")]
    assert len(live) == 0 or st.status != "down", (st.status, live)


async def test_B6_an_array_still_listed_after_a_cancel_is_draining(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600", array="0-1")
    jid = out.job.native_id
    job = world.sched.job(jid)
    for i in ("0", "1"):
        job["tasks"]["state"][i] = "RUNNING"
        job["tasks"]["elapsed"][i] = 60
    world.sched.set(jid, tasks=job["tasks"], state="RUNNING", elapsed=60)
    world.sched.knob(cancel_lag=3)
    first = await server._cancel(world.app, out.job.job_id, "abort")
    assert first.status == "draining", first


async def test_B6_a_stop_on_another_facility_is_not_down_while_our_jobs_bill(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600")
    world.sched.set(out.job.native_id, state="RUNNING", elapsed=60)
    app2 = _new_session(world)
    app2.machine = "otherhpc"
    st = await server._stop_endpoint(app2)
    assert st.status != "down" and out.job.native_id in (st.notice or ""), st


async def test_B7_the_ceiling_counts_an_envelope_on_every_account_it_admits(world, monkeypatch):
    monkeypatch.setenv("HPC_BRIDGE_ENVELOPE_MAX", "10")
    await _confirmed(world.app, command="sleep 60", account="a", envelope={"cap": 10, "accounts": ["a", "b"]})
    app2 = _new_session(world, owner_gone=False)
    over = await _submit(app2, command="sleep 61", account="b", envelope={"cap": 10}, confirm_spend=True)
    assert over.phase == "refused" and over.reason == "envelope_ceiling", over


def test_B7_the_ceiling_is_checked_inside_the_opening_transaction(tmp_path):
    from hpc_bridge.ledger import EnvelopeCeiling, Ledger, Owner

    led = Ledger.open(tmp_path / "state")
    led.add_envelope("env-000000000001", facility="f", account="a", unit="u", cap=6, deadline=9e9,
                     approved_by="s1", owner=Owner.here(), ceiling=10)
    with pytest.raises(EnvelopeCeiling):
        led.add_envelope("env-000000000002", facility="f", account="a", unit="u", cap=6, deadline=9e9,
                         approved_by="s2", owner=Owner.here(), ceiling=10)


# B8 (plausible) and the smaller ones
async def test_B8_a_requeued_jobs_earlier_run_is_charged(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600")
    world.sched.set(out.job.native_id, state="RUNNING", elapsed=600, billing=4,
                    runs=[{"JobID": out.job.native_id, "JobName": f"hpcb-{out.job.job_id}", "State": "REQUEUED",
                           "ElapsedRaw": "1800", "TimelimitRaw": "60", "AllocTRES": "billing=4,cpu=4,node=1",
                           "ReqTRES": "billing=4,cpu=4,node=1", "Partition": "main", "Account": "",
                           "ExitCode": "0:0", "Reason": "None"}])
    st = await server._status(world.app, out.job.job_id)
    assert st.job.charged == pytest.approx(4 * (1800 + 600) / 3600, abs=0.01), st.job
    assert any("-D" in c or "--duplicates" in c for c in world.sched.calls("sacct"))


async def test_B8_an_unlimited_time_limit_never_commits_nothing(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 100})
    world.sched.add_job(name="forever", state="RUNNING", elapsed=0, timelimit_min=60, billing=4,
                        timelimit_raw="UNLIMITED")
    env = await server._status(world.app, out.envelope.envelope_id)
    stray = [u for u in env.envelope.unledgered if u.name == "forever"]
    assert stray and stray[0].committed > 0, env.envelope.unledgered


async def test_B8_a_pbs_array_is_metered_task_by_task(pbs):
    from hpc_bridge import server

    out = await _confirmed(pbs.app, command="sleep 3600", partition="workq", array="0-3")
    native = out.job.native_id.split(".")[0].split("[")[0]
    job = pbs.sched.job(native)
    for i in ("0", "1"):
        job["tasks"]["state"][i] = "RUNNING"
        job["tasks"]["elapsed"][i] = 1800
    pbs.sched.set(native, tasks=job["tasks"])
    st = await server._status(pbs.app, out.job.job_id)
    assert st.job.charged == pytest.approx(2 * 4 * 0.5, abs=0.01), st.job                # two tasks × 4 × 0.5 h
    assert any("-t" in c for c in pbs.sched.calls("qstat"))


async def test_B8_cancelled_is_said_only_for_what_the_scheduler_cancelled(world):
    from hpc_bridge import server

    out = await _confirmed(world.app, command="sleep 3600")
    world.sched.set(out.job.native_id, state="RUNNING")
    deny = world.sched.bin / "scancel"
    deny.write_text("#!/bin/sh\necho 'scancel: error: Kill job error on job id: Access/permission denied' >&2\n"
                    "exit 1\n")
    c = await server._cancel(world.app, out.job.job_id, "abort")
    assert "cancelled:" not in (c.notice or "") and c.status == "draining", c


async def test_B8_an_unreadable_ledger_never_makes_a_stop_down(world, monkeypatch):
    from hpc_bridge import jobs, server

    async def broken(app):
        raise OSError("database disk image is malformed")

    monkeypatch.setattr(jobs, "billing_jobs", broken)
    st = await server._stop_endpoint(world.app)
    assert st.status != "down" and "ledger" in (st.notice or ""), st


# ======================================================================================================================
# The live cells at aaf9248 (scratchpad p2a_live/*.log)
# ======================================================================================================================

# job_array_terms: "approve at least 3.33" for a worst case of 3.333…; the default cap rounded below the worst case
async def test_live_a_proposed_cap_is_never_below_the_worst_case_and_every_at_least_rounds_up(world):
    world.app.facility.partitions = [PartitionShape(name="main", kind="per_core", cores=4)]
    ask = await _submit(world.app, command="sleep 20; echo {index}", array="0-19%4", cores=1, walltime="00:10:00")
    assert ask.phase == "needs_confirmation" and ask.reason == "envelope_needed", ask
    assert ask.envelope.cap >= 20 / 6 and ask.terms.worst_case >= 20 / 6, (ask.envelope.cap, ask.terms.worst_case)
    yes = await _submit(world.app, command="sleep 20; echo {index}", array="0-19%4", cores=1, walltime="00:10:00",
                        confirm_spend=True)
    assert yes.phase == "queued", yes
    small = await _submit(world.app, command="sleep 1", cores=1, walltime="00:10:00", account="x",
                          envelope={"cap": 0.1}, confirm_spend=True)
    import re

    figures = [float(x) for x in re.findall(r"at least (\d+(?:\.\d+)?)", small.notice or "")]
    assert figures and all(f >= 1 / 6 for f in figures), small.notice           # 0.17, never 0.16
    retry = await _submit(world.app, command="sleep 1", cores=1, walltime="00:10:00", account="x",
                          envelope={"cap": figures[0]}, confirm_spend=True)
    assert retry.phase == "queued", retry                                       # asking for that figure admits


# meter_missing_from_sacct_full_walltime: with slurmdbd down sacct HANGS — the tick outlived the sync-wait, read nothing
# (meter_source stayed `none`, no "full walltime"), and the hung call held the login channel for the next status
async def test_live_a_hung_sacct_is_bounded_held_at_full_walltime_and_frees_the_channel(world, monkeypatch):
    from hpc_bridge import server

    monkeypatch.setenv("HPC_BRIDGE_METER_TIMEOUT_S", "2")
    out = await _confirmed(world.app, command="sleep 3600")
    world.sched.set(out.job.native_id, state="COMPLETED", elapsed=20)
    world.sched.knob(sacct_hang=30)
    st = await server._status(world.app, out.job.job_id)
    assert st.job.meter_source in ("missing", "squeue") and "full walltime" in (st.notice or ""), (st.job, st.notice)
    assert st.job.charged + st.job.committed == pytest.approx(4.0)
    world.sched.knob(sacct_hang=0)
    again = await server._status(world.app, out.job.job_id)
    assert again.job.meter_source == "sacct" and again.job.charged < 4.0, (again.job, again.notice)


def test_live_the_site_profile_makes_slurm_hold_a_job_over_its_grptresmins():
    conf = Path(__file__).parents[1] / "agentic" / "fakecluster" / "profiles" / "site" / "slurm.conf"
    line = next(ln for ln in conf.read_text().splitlines() if ln.startswith("AccountingStorageEnforce="))
    assert "safe" in line.split("=", 1)[1].split(","), line

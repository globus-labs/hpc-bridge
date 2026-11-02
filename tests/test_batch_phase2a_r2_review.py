"""Batch phase 2a, fix round 2 — a review of the fix-round diff (aaf9248..3d0b272): 8 defects, 15 reproductions (the
coordinator's, scratchpad review-p2a-r2/: test_r2_review.py, test_r2_more.py, test_r2_args.py, test_r2_hand_open.py),
moved in-tree as they were; 14 failed on 3d0b272. Each asserts the correct behaviour, under its own timeout.

One assertion departs from the reproduction: test_r2_review.py #6 also asserted "hpcb-shim" absent from the PATH of a
job submitted by ABSOLUTE path. That job's PATH is whatever the command's PATH was when the real sbatch ran — nothing on
PATH can strip itself from a binary that bypasses it — so the remedy is the reviewer's own "make the shim close safely,
so a job that inherits it falls through to the real binary" (pinned by test_a_leaked_jobs_own_sbatch_is_not_left_held
_forever) and no wrapper VARIABLE reaches the job at all (asserted here).
"""
from __future__ import annotations

import asyncio
import subprocess
import time

import pytest

from tests.test_batch_phase2a import (
    _BASH,
    _confirmed,
    _hand_script,
    _submit,
    pbs,
    world,
)

_FIXTURES = (world, pbs)
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]
T = 90


def _w(coro):
    return asyncio.wait_for(coro, T)


# ---- item 1 (P5): a submit_job admitted while a stop that found nothing live is releasing the blocks ------------------
async def test_stop_not_down_while_an_admission_during_it_submits(world, monkeypatch):
    from hpc_bridge import server

    async def body():
        first = await _confirmed(world.app, command="sleep 30", envelope={"cap": 20})
        world.sched.set(first.job.native_id, state="COMPLETED", elapsed=30)   # nothing of ours bills at the stop's start
        go = asyncio.Event()
        real_blocks = server._stop_endpoint_blocks

        async def slow_blocks(app):
            await go.wait()          # the release of the blocks takes a while (up to STOP_CALL_BUDGET_S)
            return await real_blocks(app)

        monkeypatch.setattr(server, "_stop_endpoint_blocks", slow_blocks)
        stop = asyncio.create_task(server._stop_endpoint(world.app))
        await asyncio.sleep(0.05)
        out = await _submit(world.app, command="sleep 61")    # fits the open envelope: admitted without asking
        go.set()
        st = await stop
        live = [j for j in world.sched.submitted() if j["state"] in ("PENDING", "RUNNING")]
        assert not (st.status == "down" and live), (st.status, out.phase, [j["id"] for j in live], st.notice)

    await _w(body())


# ---- item 2 (P4): PBS — an unreadable qselect/qstat reads as "ok, no jobs" -------------------------------------------
def test_pbs_killed_qstat_is_not_an_ok_empty_view():
    from hpc_bridge import meter

    raw = f"{meter.QSTAT_MARK}\nHPCB_QSTAT_RC=143\n{meter.END_MARK}\n"     # qstat TERMed by the bound: no output
    assert meter.parse_pbs(raw).ok is False


async def test_pbs_first_admission_waits_for_a_reading_when_the_server_is_down(pbs):
    async def body():
        pbs.sched.add_job(name="bypass", state="RUNNING", elapsed=0, timelimit_min=60, ncpus=4, queue="workq")
        q = pbs.sched.bin / "qselect"
        q.write_text("#!/bin/sh\necho 'qselect: cannot connect to server pbsserver (errno=15010)' >&2\nexit 2\n")
        q.chmod(0o755)
        out = await _submit(pbs.app, command="sleep 60", partition="workq", envelope={"cap": 6}, confirm_spend=True)
        assert out.phase not in ("queued", "running"), (out.phase, out.reason, out.notice)

    await _w(body())


async def test_control_pbs_first_admission_counts_the_bypass_job_when_the_server_answers(pbs):
    async def body():
        pbs.sched.add_job(name="bypass", state="RUNNING", elapsed=0, timelimit_min=60, ncpus=4, queue="workq")
        out = await _submit(pbs.app, command="sleep 60", partition="workq", envelope={"cap": 6}, confirm_spend=True)
        assert out.phase not in ("queued", "running"), (out.phase, out.reason, out.notice)

    await _w(body())


# ---- item 6 (P4): the 10-minute housekeeping refuses a confirmed block's reservation ----------------------------------
async def test_stale_housekeeping_does_not_free_a_block_reservation(world):
    from hpc_bridge import batch, jobs

    async def body():
        out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 100})
        led = batch.ledger_of(world.app)
        eid = out.envelope.envelope_id
        led.reserve_job(eid, "job-0000000000aa", session=world.app.session_token, worst_case=16.0, nodes=4,
                        source="pool_block", walltime_s=3600, rate=16.0, now=time.time() - 700)
        await jobs.tick(world.app, envelope_ids=[eid])
        row = led.job("job-0000000000aa")
        assert row["state"] != "refused", row

    await _w(body())


# ---- item 7 (P4): unledgered arrays' pending tasks (squeue only) and UNLIMITED jobs ----------------------------------
def test_unledgered_array_counts_its_pending_tasks():
    from hpc_bridge import meter

    raw = "\n".join([
        meter.SACCT_MARK,
        "3_0|wrap|RUNNING|5|2|billing=1,cpu=1,node=1|billing=1,cpu=1,mem=4000M,node=1|main|hpcb|0:0|None",
        "3_1|wrap|RUNNING|5|2|billing=1,cpu=1,node=1|billing=1,cpu=1,mem=4000M,node=1|main|hpcb|0:0|None",
        "HPCB_SACCT_RC=0", meter.SQUEUE_MARK,
        "3_[2-5%2]|PENDING|0:00|2:00|wrap", "3_0|RUNNING|0:05|2:00|wrap", "3_1|RUNNING|0:05|2:00|wrap",
        "HPCB_SQUEUE_RC=0", meter.END_MARK])
    v = meter.parse_slurm(raw)
    un = meter.unledgered(v, set(), set(), horizon_s=3600)
    committed = sum(u["committed"] for u in un)
    assert committed >= 4 * 1 * 120 / 3600 - 1e-6, (un, v.pending)     # four pending tasks × 2 min × billing 1


def test_unlimited_running_job_is_held_for_the_whole_horizon():
    from hpc_bridge import meter

    raw = "\n".join([
        meter.SACCT_MARK,
        "9|prod|RUNNING|3600|UNLIMITED|billing=4,cpu=4,node=1|billing=4,cpu=4,node=1|main|hpcb|0:0|None",
        "HPCB_SACCT_RC=0", meter.SQUEUE_MARK, "9|RUNNING|1:00:00|UNLIMITED|prod", "HPCB_SQUEUE_RC=0", meter.END_MARK])
    un = meter.unledgered(meter.parse_slurm(raw), set(), set(), horizon_s=7200)
    assert un[0]["committed"] >= 4 * 2 - 1e-6, un      # it bills 4/h for the 2 h left to the deadline


# ---- item 4 (P3): the wrapper's variables and PATH -------------------------------------------------------------------
async def test_absolute_path_submit_does_not_carry_the_wrapper_into_the_job(world):
    from hpc_bridge import server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        script = _hand_script(world)
        real = world.sched.bin / "sbatch"
        await server._run_shell(world.app, f"{real} {script}", shape="login", agent_call=True, confirm_spend=True)
        hand = [j for j in world.sched.submitted() if j["id"] != first.job.native_id]
        env = hand[0]["submit_env"]
        assert not [k for k in env if k.startswith("HPCB_SHIM")], env   # (the PATH clause: see the module doc)

    await _w(body())


async def test_wrapper_keeps_the_commands_own_path_for_the_job(world):
    from hpc_bridge import server

    async def body():
        await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        script = _hand_script(world)
        await server._run_shell(world.app, f"export PATH=/opt/mytool/bin:$PATH; sbatch {script}", shape="login",
                                agent_call=True, confirm_spend=True)
        hand = [j for j in world.sched.submitted() if j.get("held_at_submit")]
        assert hand, world.sched.submitted()
        assert "/opt/mytool/bin" in hand[0]["submit_env"].get("PATH", ""), hand[0]["submit_env"]

    await _w(body())


async def test_a_leaked_jobs_own_sbatch_is_not_left_held_forever(world):
    """The job submitted by absolute path inherits the command's PATH (the wrapper on it); its own `sbatch` later (a
    pipeline step) must fall through to the real one once the hand is closed — never held into a closed hand."""
    from hpc_bridge import batch, server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        script = _hand_script(world)
        real = world.sched.bin / "sbatch"
        await server._run_shell(world.app, f"{real} {script}", shape="login", agent_call=True, confirm_spend=True)
        led = batch.ledger_of(world.app)
        hands = led.hands(states=("open", "closed", "gone"))
        leaked = next(j for j in world.sched.submitted() if j["id"] != first.job.native_id)
        env = {**world.sched.env(), "HOME": str(world.tmp / "home")}
        env.update({k: v for k, v in leaked["submit_env"].items() if k.startswith("HPCB_")})
        env["PATH"] = leaked["submit_env"]["PATH"]
        p = subprocess.run([_BASH, "-c", f"sbatch {script}"], env=env, capture_output=True, text=True, timeout=30)
        step2 = [j for j in world.sched.submitted() if j["id"] not in (first.job.native_id, leaked["id"])]
        await server._status(world.app, first.envelope.envelope_id)
        step2 = [world.sched.job(j["id"]) for j in step2]
        assert step2 and not step2[0]["held"], (p.stdout, p.stderr, [h["state"] for h in hands], step2)

    await _w(body())


# ---- item 3 (P2): a script's own argument never drops the hold -------------------------------------------------------
@pytest.mark.parametrize("arg", ["-W", "--wait", "-H"])
async def test_a_script_argument_does_not_drop_the_hold(world, arg):
    """`sbatch job.sh -W` — `-W` is job.sh's argument (sbatch stops at the script)."""
    from hpc_bridge import server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 6})
        script = _hand_script(world)
        out = await server._run_shell(world.app, f"sbatch {script} {arg}", shape="login", agent_call=True,
                                      confirm_spend=True)
        hand = [j for j in world.sched.submitted() if j["id"] != first.job.native_id]
        assert hand and hand[0]["held_at_submit"], (hand[0]["held_at_submit"], hand[0]["state"], out.notice)

    await _w(body())


# ---- item 5 (P1): a hand cancelled before its command ran is closed --------------------------------------------------
async def test_a_cancelled_hand_that_never_ran_does_not_sweep_in_later_jobs(world, monkeypatch):
    """The client cancels the gated run_shell while the session warms (_ready_session): the hand must not stay open
    and sweep every later job of the user's in as this command's."""
    from hpc_bridge import batch, server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        script = _hand_script(world)
        real = server._ready_session

        async def cancelled(app, shape, session_id, *a, **k):
            if session_id == "default":          # the agent's command only; the login channel works
                raise asyncio.CancelledError()
            return await real(app, shape, session_id, *a, **k)

        monkeypatch.setattr(server, "_ready_session", cancelled)
        with pytest.raises(asyncio.CancelledError):
            await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
        monkeypatch.setattr(server, "_ready_session", real)
        assert world.sched.calls("sbatch") and len(world.sched.calls("sbatch")) == 1   # the command never ran
        later = world.sched.add_job(name="mine-from-my-terminal", state="RUNNING", elapsed=0, timelimit_min=60,
                                    billing=4)
        await server._status(world.app, first.envelope.envelope_id)
        rows = [j for j in batch.ledger_of(world.app).jobs() if j.get("native_id") == later]
        open_hands = batch.ledger_of(world.app).hands()
        assert not rows, (rows[0].get("reason"), rows[0].get("hand"), [h["state"] for h in open_hands])

    await _w(body())


# ---- item 8 (P5): another facility's PBS job carries no scheduler deadline --------------------------------------------
async def test_other_facility_pbs_job_past_deadline_plus_walltime_still_counts(world):
    from hpc_bridge import batch, jobs
    from hpc_bridge.ledger import Owner

    async def body():
        led = batch.ledger_of(world.app)
        now = time.time()
        led.add_envelope("env-0000000000bb", facility="polarisish", account="a", unit="u", cap=50,
                         deadline=now - 7200, approved_by="s-old", owner=Owner.here(), now=now - 90000)
        led.reserve_job("env-0000000000bb", "job-0000000000bb", session="s-old", worst_case=8, source="submit_job",
                        native_id="4242.pbsserver", state="queued", scheduler="pbs", walltime_s=3600,
                        deadline=now - 7200, now=now - 80000)
        rows = await jobs.billing_jobs(world.app)
        assert any(j["id"] == "job-0000000000bb" for j in rows), [j["id"] for j in rows]

    await _w(body())

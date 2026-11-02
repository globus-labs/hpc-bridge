"""Batch phase 2a, fix round 3 (2026-11-01): the user's decision to simplify D2 to what the design says, a final review of
3d0b272..4baacfb (scratchpad review-p2a-r3/, 13 reproductions) and two live regressions. Written failing first (red log:
scratchpad red_p2a_r3.txt).

D2, simplified (the user's decision): the gate is unchanged; on the yes the command runs UNMODIFIED inside one remote
shell that takes a snapshot of the user's jobs immediately before it and — written to the session dir — immediately
after it ends; the jobs that appeared between are adopted after the fact (ledgered, metered, reserved), named as "jobs
that appeared while your approved command ran", with no envelope deadline applied; if they put the envelope over its
cap, admission closes and the notice says by how much. hpc-bridge never cancels them on its own: the stop asks about
them by name, and `jobs="cancel"` or `cancel("job-…")` ends them because the user chose. No wrapper, no hold, no mark
file, no release, no cancel-while-held.
"""
from __future__ import annotations

import asyncio
import time

import pytest

from tests.test_batch_phase2a import (
    _BASH,
    _confirmed,
    _hand_script,
    _submit,
    world,
)

_FIXTURES = (world,)
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]
T = 90


def _w(coro, t: float = T):
    return asyncio.wait_for(coro, t)


def _others(world, first):
    return [j for j in world.sched.submitted() if j["id"] != first.job.native_id]


def _defer_agent_commands(world):
    """The agent's command (session `default`) outlives the sync-wait: it is held back until `run()`."""
    from tests.test_server import _PendingFuture

    runner = world.app.test_runner
    real = runner.submit
    deferred: list[str] = []

    def later(command):
        if "/sessions/default" in command:
            deferred.append(command)
            return _PendingFuture()
        return real(command)

    runner.submit = later

    def run():
        runner.submit = real
        for c in deferred:
            real(c)

    return deferred, run


# ---- D2, simplified ---------------------------------------------------------------------------------------------------

async def test_d2_the_yes_runs_the_command_unmodified_and_adopts_what_appeared(world):
    from hpc_bridge import server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        script = _hand_script(world)
        asked = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True)
        assert asked.phase == "needs_confirmation" and world.sched.calls("sbatch") == [world.sched.calls("sbatch")[0]]
        out = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True,
                                      confirm_spend=True)
        hand = _others(world, first)
        assert len(hand) == 1 and not hand[0]["held_at_submit"] and hand[0]["deadline_epoch"] is None, hand
        assert ["--hold"] not in [a[1:2] for a in world.sched.calls("sbatch")], world.sched.calls("sbatch")
        assert not [k for k in hand[0]["submit_env"] if k.startswith(("HPCB_SHIM", "BASH_FUNC_"))]
        assert out.jobs and out.jobs[0].native_id == hand[0]["id"] and out.jobs[0].source == "adopt", out.jobs
        words = (out.notice or "").lower()
        assert "appeared while your approved command ran" in words and "no envelope deadline" in words, out.notice
        assert "exclude_jobs" in (out.notice or ""), out.notice

    await _w(body())


async def test_d2_an_adoption_over_the_cap_closes_admission_and_says_by_how_much(world):
    from hpc_bridge import server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 6})   # 4 of 6 held
        script = _hand_script(world)
        out = await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True,
                                      confirm_spend=True)
        hand = _others(world, first)
        assert hand and hand[0]["state"] != "CANCELLED" and world.sched.calls("scancel") == [], hand
        words = (out.notice or "").lower()
        assert "over its cap" in words and "by 2" in words, out.notice           # 8 held of 6
        nxt = await _submit(world.app, command="sleep 61")
        assert nxt.phase == "needs_confirmation" and len(world.sched.calls("sbatch")) == 2, nxt

    await _w(body())


async def test_d2_the_stop_asks_about_adopted_jobs_by_name_and_cancel_ends_them(world):
    from hpc_bridge import server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        script = _hand_script(world)
        await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
        hand = _others(world, first)[0]["id"]
        asked = await server._stop_endpoint(world.app)
        assert asked.status == "needs_confirmation" and hand in (asked.notice or ""), asked
        assert world.sched.calls("scancel") == []
        await server._stop_endpoint(world.app, jobs="cancel")                    # the user chose
        assert world.sched.job(hand)["state"] == "CANCELLED"

    await _w(body())


# review r3 F5: the after snapshot is taken when the command ENDS (written to the session dir), not at the next call
async def test_a_terminal_job_after_the_command_ended_is_not_adopted(world):
    from hpc_bridge import batch, server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 50})
        script = _hand_script(world)
        _deferred, run = _defer_agent_commands(world)
        await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
        run()                                          # the command runs and ENDS
        mine = world.sched.add_job(name="from-my-terminal", state="RUNNING", elapsed=0, timelimit_min=60, billing=4)
        await server._status(world.app, first.envelope.envelope_id)     # the next call adopts what it submitted
        led = batch.ledger_of(world.app)
        assert not [j for j in led.jobs() if j.get("native_id") == mine]
        assert [j for j in led.jobs() if j.get("native_id") == _others(world, first)[0]["id"]]

    await _w(body())


# review r3 F3: a loop of > 100 hand submits — none is ever cancelled; what one call cannot read the next call adopts
async def test_a_loop_of_hand_submits_cancels_none_and_the_next_call_adopts_the_rest(world):
    from hpc_bridge import batch, server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 1000})
        script = _hand_script(world)
        await server._run_shell(world.app, f"for i in $(seq 1 104); do sbatch {script} >/dev/null; done",
                                shape="login", agent_call=True, confirm_spend=True)
        hand = _others(world, first)
        assert len(hand) == 104 and world.sched.calls("scancel") == []
        await server._status(world.app, first.envelope.envelope_id)
        ledgered = {j.get("native_id") for j in batch.ledger_of(world.app).jobs()}
        assert {j["id"] for j in hand} <= ledgered, len({j["id"] for j in hand} - ledgered)

    await _w(body(), 240)


# ---- 1. a stop that only asks changes nothing; never `down` while an approved command is unaccounted for --------------

async def test_r3_1_an_asking_stop_changes_nothing_and_names_a_job_awaiting_adoption(world):
    from hpc_bridge import batch, server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        script = _hand_script(world)
        _deferred, run = _defer_agent_commands(world)
        await server._run_shell(world.app, f"sbatch {script}", shape="login", agent_call=True, confirm_spend=True)
        run()                                          # it submitted; nothing has read it yet
        hand = _others(world, first)[0]["id"]
        led = batch.ledger_of(world.app)
        before = (led.envelope(first.envelope.envelope_id)["state"], len(led.jobs()), len(led.hands()))
        st = await server._stop_endpoint(world.app)
        assert st.status == "needs_confirmation" and hand in (st.notice or ""), st
        after = (led.envelope(first.envelope.envelope_id)["state"], len(led.jobs()), len(led.hands()))
        assert after == before and world.sched.calls("scancel") == [], (before, after)

    await _w(body())


async def test_r3_1_no_down_while_an_approved_command_is_still_running(world):
    from hpc_bridge import server

    async def body():
        first = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        world.sched.set(first.job.native_id, state="COMPLETED", elapsed=30)
        script = _hand_script(world)
        _deferred, _run = _defer_agent_commands(world)   # never run: the approved command may still submit
        await server._run_shell(world.app, f"sleep 600; sbatch {script}", shape="login", agent_call=True,
                                confirm_spend=True)
        st = await server._stop_endpoint(world.app, jobs="cancel")
        assert st.status != "down" and "still running" in (st.notice or ""), st

    await _w(body())


# ---- 2. a proposal is stable: four yeses with an UNLIMITED job of the user's never loop -------------------------------

async def test_r3_2_four_yeses_with_an_unlimited_job_never_loop(world):
    async def body():
        j = world.sched.add_job(name="prod", state="RUNNING", elapsed=3600, timelimit_min=100000,
                                timelimit_raw="UNLIMITED", billing=1)
        ask = await _submit(world.app, command="sleep 60")
        assert ask.reason == "envelope_needed", ask
        out = ask
        for k in range(4):           # the user reads the proposal for a minute, says yes; the agent calls again
            world.sched.set(j, elapsed=3600 + 60 * (k + 1))
            out = await _submit(world.app, command="sleep 60", confirm_spend=True)
            if out.phase in ("queued", "running"):
                break
        assert out.phase in ("queued", "running"), (out.phase, out.reason, out.envelope.cap, (out.notice or "")[:300])

    await _w(body())


async def test_r3_2_the_envelope_opens_with_the_deadline_the_proposal_named(world):
    async def body():
        ask = await _submit(world.app, command="sleep 60")
        await asyncio.sleep(1.2)
        yes = await _submit(world.app, command="sleep 60", confirm_spend=True)
        assert yes.phase == "queued" and yes.envelope.deadline == ask.envelope.deadline, (ask.envelope.deadline,
                                                                                          yes.envelope.deadline)

    await _w(body())


# ---- 3. an unstarted array (and a job sacct does not list yet) is counted at its billing -------------------------------

async def test_r3_3_an_unstarted_gpu_array_is_counted_at_its_billing(world):
    async def body():
        idx = list(range(10))
        world.sched.add_job(name="gpu-sweep", state="PENDING", timelimit_min=60, cpus=1, billing=16, array="0-9",
                            tasks={"indices": idx, "throttle": None, "state": {str(i): "PENDING" for i in idx},
                                   "elapsed": {str(i): 0 for i in idx}})
        ask = await _submit(world.app, command="sleep 60")
        held = sum(u.charged + u.committed for u in ask.envelope.unledgered)
        assert held >= 10 * 16 * 1 - 1e-6, (held, ask.envelope.unledgered)    # 10 tasks × billing 16 × 1 h

    await _w(body())


# live regression (envelope_bypass_counted): a job just submitted is in squeue before slurmdbd has it in sacct
async def test_r3_3_a_job_sacct_does_not_list_yet_is_counted_and_named(world):
    from hpc_bridge import server

    async def body():
        out = await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        stray = world.sched.add_job(name="bypass", state="PENDING", timelimit_min=10, cpus=1, billing=1)
        world.sched.knob(hide_sacct=[stray])
        env = await server._status(world.app, out.envelope.envelope_id)
        un = [u for u in env.envelope.unledgered if u.native_id == stray]
        assert un and un[0].name == "bypass" and un[0].committed > 0 and stray in (env.notice or ""), env.envelope

    await _w(body())


# ---- 4. a stop with an admission already past move_job reads the ledger again before it answers ----------------------

async def test_r3_4_a_stop_with_an_admission_past_move_job_rereads_before_answering(world, monkeypatch):
    from hpc_bridge import jobs, server

    async def body():
        await _confirmed(world.app, command="sleep 30", envelope={"cap": 20})
        entered, gate = asyncio.Event(), asyncio.Event()
        real_submit = jobs._submit_script

        async def slow_submit(app, job_id, path, spec):      # past write and move_job: the sbatch is in flight
            entered.set()
            await gate.wait()
            return await real_submit(app, job_id, path, spec)

        monkeypatch.setattr(jobs, "_submit_script", slow_submit)
        sub = asyncio.create_task(_submit(world.app, command="sleep 61"))
        await entered.wait()
        deny = world.sched.bin / "scancel"                   # the scheduler refuses every cancel now
        deny.write_text("#!/bin/sh\necho 'scancel: error: Access/permission denied' >&2\nexit 1\n")
        real_blocks = server._stop_endpoint_blocks

        async def blocks_after_the_submit(app):
            gate.set()
            await sub                                        # the job reaches the scheduler while the stop runs
            return await real_blocks(app)

        monkeypatch.setattr(server, "_stop_endpoint_blocks", blocks_after_the_submit)
        st = await server._stop_endpoint(world.app, jobs="cancel")
        out = await sub
        native = out.job.native_id if out.job else None
        assert native and world.sched.job(native)["state"] == "PENDING", (out, native)
        assert st.status != "down" and native in (st.notice or ""), (st.status, st.notice)

    await _w(body())


# ---- 5. a stop has ONE deadline; past it, it answers draining and says what it could not confirm ----------------------

async def test_r3_5_a_stop_is_bounded_and_says_what_it_could_not_confirm(world, monkeypatch):
    from hpc_bridge import jobs, server

    async def body():
        await _confirmed(world.app, command="sleep 60", envelope={"cap": 20})
        monkeypatch.setattr(server, "STOP_BUDGET_S", 3.0)
        real = jobs.billing_jobs
        calls = []

        async def slow(app, *a, **k):
            calls.append(1)
            if len(calls) > 1:
                await asyncio.sleep(30)                      # the second read hangs
            return await real(app, *a, **k)

        monkeypatch.setattr(jobs, "billing_jobs", slow)
        t0 = time.monotonic()
        st = await server._stop_endpoint(world.app, jobs="cancel")
        assert time.monotonic() - t0 < 10, time.monotonic() - t0
        assert st.status == "draining" and "could not confirm" in (st.notice or ""), st

    await _w(body())


async def test_r3_5_a_stops_cancels_go_out_in_one_call(world):
    from hpc_bridge import server

    async def body():
        a = await _confirmed(world.app, command="sleep 3600", envelope={"cap": 20})
        b = await _confirmed(world.app, command="sleep 3601")
        await server._stop_endpoint(world.app, jobs="cancel")
        sent = world.sched.calls("scancel")      # each is one scheduler invocation (one login call carries it)
        assert len(sent) == 1 and {a.job.native_id, b.job.native_id} <= set(sent[0]), sent

    await _w(body())

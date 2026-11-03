"""The free sweep of 2026-11-02 (sweep-20261102-2311, run on #241's head 0b192cb): its hpc-bridge findings, written
failing first.

1. f2b_stranger / f2b_banned, all three operators: a new laptop's failed connect opened with the zero-SSH reattach's
   note ("no endpoint of yours named … is online: probed over SSH.") and only then said what failed — NO SSH ACCESS,
   CANNOT REACH, the pre-auth handoff. What failed leads; the note follows it, as context (#236 regression).
2. batch_crosses_walltime (scripted): run_batch re-checked a block ensure_endpoint_up had confirmed 0.2 s earlier, and
   under the sweep's load that check outlived the 8 s canary wait — a live block read as reaped, its confirmation
   voided, and run_batch asked for the spend again. run_batch trusts a check that passed within the canary TTL, as
   run_shell does (latent since phase 1; the path is unchanged since #238 — load exposed it).
3. meter_missing_from_sacct_full_walltime (scripted): the yes met the login channel still busy with an earlier
   scheduler command of hpc-bridge's own (a sacct past its sync-wait) and was REFUSED. submit_job's admission reads
   wait for the channel, bounded; still busy, the answer is `busy` — retryable, nothing submitted or refused.
"""
from __future__ import annotations

import asyncio

import pytest

from hpc_bridge import connect
from hpc_bridge.facility.remote import NeedsPreauth, SshTarget
from hpc_bridge.runner import CanaryResult
from tests.test_batch_phase2a import _submit, world  # noqa: F401 - the fixture, by name
from tests.test_returning_user_fixes import _NewLaptop
from tests.test_server import _PendingFuture, _Res

_MISS = "no endpoint of yours named hpc-bridge-login is online"


# ---- 1. what failed leads; the reattach note follows ----------------------------------------------------------------

@pytest.mark.parametrize("ssh_error, head", [
    ("ssh login failed: hpcbridge-stranger@login: Permission denied (publickey).", "NO SSH ACCESS to login"),
    ("ssh login failed: ssh: connect to host login port 22: Connection refused", "CANNOT REACH login"),
])
async def test_a_failed_first_connect_leads_with_what_failed_and_the_reattach_note_follows(monkeypatch, ssh_error,
                                                                                         head):
    from hpc_bridge import server
    w = _NewLaptop(monkeypatch, online=None)     # a new laptop: no record, and no endpoint of ours online

    async def refused(target):
        w.ssh_probes.append(target.host)
        raise RuntimeError(ssh_error)
    monkeypatch.setattr(connect, "discover_facility_details", refused)
    res = await server._connect_facility(w.app(), "f-new", ssh_host="login")
    assert w.asked == ["login"] and w.ssh_probes == ["login"]
    assert res.phase == "failed" and res.notice.startswith(head), res.notice
    low = res.notice.lower()
    assert _MISS in low and low.index(_MISS) > len(head), res.notice   # still said, after the failure


async def test_a_preauth_handoff_after_a_reattach_miss_opens_with_the_handoff(monkeypatch):
    from hpc_bridge import server
    w = _NewLaptop(monkeypatch, online=None)
    target = SshTarget("login", "u", None, control_dir="/tmp/hpcb-cm")

    async def wants_a_code(_target):
        raise NeedsPreauth(target, otp_ok=True)
    monkeypatch.setattr(connect, "discover_facility_details", wants_a_code)
    res = await server._connect_facility(w.app(), "f-new", ssh_host="login")
    assert res.phase == "needs_preauth", res
    assert res.notice.startswith("login needs a one-time login step"), res.notice
    assert _MISS in res.notice.lower() and res.notice.lower().index(_MISS) > 0, res.notice


# ---- 2. run_batch trusts the check that just passed -----------------------------------------------------------------

async def test_run_batch_right_after_a_passing_check_sends_no_second_check_that_could_void_the_spend():
    from hpc_bridge import server
    from tests.test_batch_phase1 import _batch_app
    app, rt, runner = await _batch_app()
    st = await server._ensure_endpoint_up(app, "compute")     # the block is up: a worker answered this check
    assert st.status == "up" and rt.block_since is not None, st
    sent = runner.canaries
    # the next check would outlive the canary wait — the sweep's load, on a block that is up
    runner._canary = CanaryResult(ok=False, error="timeout")
    out = await server._run_batch(app, command="echo {x}", rows=[{"x": i} for i in range(3)], task_walltime_s=60)
    assert out.batch_id and out.phase not in ("needs_confirmation", "refused"), out
    assert runner.canaries == sent, "run_batch sent a second check moments after one passed"
    assert rt.spend_confirmed and rt.reaped is None


async def test_run_batch_still_checks_a_block_whose_check_is_stale():
    # the TTL, not a blanket trust: a block last proven past CANARY_TTL_S is checked before a batch goes to it
    from hpc_bridge import server
    from hpc_bridge.config import CANARY_TTL_S
    from tests.test_batch_phase1 import _batch_app
    app, rt, runner = await _batch_app()
    await server._ensure_endpoint_up(app, "compute")
    rt.warm_confirmed_at -= CANARY_TTL_S + 1
    sent = runner.canaries
    out = await server._run_batch(app, command="echo {x}", rows=[{"x": 0}], task_walltime_s=60)
    assert out.batch_id and runner.canaries == sent + 1, out


# ---- 3. a busy login channel is waited for, then `busy` — never a refusal --------------------------------------------

def _internal_command_still_running(app) -> _PendingFuture:
    """A scheduler command of hpc-bridge's own past its sync-wait (a slow sacct): its handle holds the internal login
    session, as login-1 did in the sweep."""
    from hpc_bridge import server, warmth
    fut = _PendingFuture()
    warmth._register_task(app, "login", server._INTERNAL_SESSION, "sacct …", fut, 60.0)
    return fut


async def test_the_yes_waits_for_a_busy_login_channel_and_admits(world, monkeypatch):  # noqa: F811
    from hpc_bridge import jobs
    monkeypatch.setattr(jobs, "LOGIN_BUSY_WAIT_S", 10.0, raising=False)
    asked = await _submit(world.app, command="sleep 60")
    assert asked.phase == "needs_confirmation", asked
    fut = _internal_command_still_running(world.app)
    asyncio.get_running_loop().call_later(0.4, fut.finish, _Res(0, "", ""))   # it ends during the wait
    out = await _submit(world.app, command="sleep 60", confirm_spend=True)
    assert out.phase in ("queued", "running"), out
    assert len(world.sched.calls("sbatch")) == 1


async def test_a_login_channel_still_busy_answers_busy_and_the_same_call_admits_later(world, monkeypatch):  # noqa: F811
    from hpc_bridge import jobs
    monkeypatch.setattr(jobs, "LOGIN_BUSY_WAIT_S", 0.3, raising=False)
    await _submit(world.app, command="sleep 60")
    fut = _internal_command_still_running(world.app)
    out = await _submit(world.app, command="sleep 60", confirm_spend=True)
    assert out.phase == "busy" and out.reason == "login_channel_busy", out
    assert "login-1" in (out.notice or "") and "again" in (out.notice or ""), out.notice
    assert world.sched.calls("sbatch") == []                        # nothing was submitted
    assert out.envelope is not None and out.envelope.state == "open", out.envelope   # the yes stands
    fut.finish(_Res(0, "", ""))
    again = await _submit(world.app, command="sleep 60", confirm_spend=True)
    assert again.phase in ("queued", "running"), again
    assert len(world.sched.calls("sbatch")) == 1

"""A cancelled call, a stop racing a dispatch, and a facility detach under a live task (review item 3, 2026-10).

Pressing Esc in Claude Code sends MCP notifications/cancelled, which cancels the run_shell coroutine inside its
sync-wait. Cancelling the CALL does not cancel the COMMAND: it was sent and runs on the block to its ceiling. Before
this, nothing tracked it — a stop released the block under it and said 'down' (the endpoint relaunches a block for an
outstanding task), the next forced canary queued behind it and read as a reap, and a second command could clobber the
session. A stop that looked while a call was mid-wait, or still provisioning, did the same. A facility-MEP detach
cleared a live task's handle while claiming idle-release. And a sent task stays reportable even when its block is
released under it: Executor.shutdown cancels only UNSENT tasks, and the SDK's result watcher resolves sent ones.

These drive real concurrent.futures.Futures that block in their thread, as the SDK's do.
"""
import asyncio
import re
import time
from concurrent.futures import Future

import pytest

from hpc_bridge import scheduler_ops, server, warmth
from hpc_bridge.lifecycle import EndpointState
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import (
    AppCtx,
    _ensure_endpoint_up,
    _poll_task,
    _reset_session,
    _run_shell,
    _shape_runtime,
    _stop_endpoint,
    _teardown_endpoint,
)
from tests.fakes import FakeFacility
from tests.test_server import _FakeRunner, _Res

_TIMEOUT = CanaryResult(ok=False, error="timeout")


class _BlockingRunner(_FakeRunner):
    """submit() hands back a REAL Future that only the test resolves: the sync-wait blocks in its thread exactly as it
    does on the SDK's Executor. `canary_gate`: an Event the canary waits on (a block still answering its check)."""

    canary_gate: asyncio.Event | None = None

    async def canary(self, timeout=8.0):
        self.canaries += 1
        if self.canary_gate is not None:
            await self.canary_gate.wait()
        return self._canary

    def submit(self, command):
        self.commands.append(command)
        fut: Future = Future()
        self.futures.append(fut)
        return fut

    def settle(self):
        """Resolve whatever is still pending, so no worker thread outlives the test's loop."""
        for f in self.futures:
            if not f.done():
                f.set_result(_Res(0, "", ""))


class _Ctx:
    async def report_progress(self, *a, **k):
        pass


async def _until(pred, timeout=3.0):
    t0 = time.monotonic()
    while not pred():
        assert time.monotonic() - t0 < timeout, "condition never held"
        await asyncio.sleep(0.01)


def _app(*, mep=False, timeout=5.0):
    f = FakeFacility()
    f.workers = 1
    if mep:
        f.supported_shapes = ("compute",)
    app = AppCtx(facility=f, profile=Profile(), state=EndpointState(endpoint_id="fake-eid"))
    runner = _BlockingRunner("fake-eid", _Res(0, "ok", ""), timeout=timeout)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    rt = _shape_runtime(app, "compute")
    rt.spend_confirmed = True
    return app, rt, runner


@pytest.fixture
def released(monkeypatch):
    calls = []

    async def release(a, eid, *rest, **kw):
        calls.append(eid)
        return True, "released 1 block"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", release)
    return calls


async def _cancel_mid_wait(app, rt, work):
    """Start a call the way the tool does (under the heartbeat), let it reach its sync-wait, then cancel it as the
    MCP layer does on notifications/cancelled."""
    call = asyncio.ensure_future(server._heartbeat(_Ctx(), work, "run_shell"))
    await _until(lambda: rt.inflight == 1)
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    await _until(lambda: rt.inflight == 0)


# --- a client cancel ---------------------------------------------------------------------------------------------------


async def test_a_call_cancelled_mid_wait_leaves_a_tracked_task(released):
    app, rt, runner = _app()
    app.charge_factor = 1.0
    try:
        await _cancel_mid_wait(app, rt, _run_shell(app, "python train.py", agent_call=True))
        fut = runner.futures[-1]
        assert not fut.done()  # the command was not cancelled with the call
        (tid, handle), = app.tasks.items()
        assert handle.future is fut and handle.client_cancelled and handle.command == "python train.py"
        assert rt.inflight == 0  # the count was handed to the handle, not leaked

        # stop refuses as for any running task — and says where a task id the agent never saw came from
        st = await _stop_endpoint(app)
        assert st.status == "up" and released == [] and "compute" in app.shapes
        assert tid in st.notice and "client cancelled" in st.notice and "poll_task" in st.notice

        # the session it ran in is busy: a second command there is refused, not run over its cwd/env
        n = len(runner.commands)
        second = await _run_shell(app, "echo hi", agent_call=True)
        assert second.phase == "failed" and tid in second.notice and "client cancelled" in second.notice
        assert len(runner.commands) == n

        # the spend clock keeps running: a forced canary that queues behind the command is not a reap
        clock = rt.warm_since
        runner._canary = _TIMEOUT
        res = await _ensure_endpoint_up(app)
        assert res.status == "up" and rt.reaped is None and rt.spend_confirmed is True
        assert rt.warm_since == clock is not None and res.session_spend > 0

        # poll_task says it is still running (and why the agent may not know it), then returns its result
        polled = await _poll_task(app, tid)
        assert polled.phase == "running" and "client cancelled" in polled.notice and "NOT cut" in polled.notice
        fut.set_result(_Res(0, "trained\n", ""))
        got = await _poll_task(app, tid)
        assert got.phase == "complete" and got.stdout == "trained\n"
        st = await _stop_endpoint(app)  # nothing holds the block now: the stop goes through
        assert st.status == "down" and released == ["fake-eid"]
    finally:
        runner.settle()


async def test_a_cancelled_reset_is_tracked_too():
    app, rt, runner = _app()
    try:
        await _cancel_mid_wait(app, rt, _reset_session(app, agent_call=True))
        (_tid, handle), = app.tasks.items()
        assert handle.client_cancelled and handle.command == "reset_session" and not handle.future.done()
        assert rt.inflight == 0
    finally:
        runner.settle()


async def test_a_cancel_while_the_call_waits_for_the_lock_after_its_sync_wait_is_tracked():
    # the sync-wait ran out (the command is still running) and the call is waiting for the lock to register its
    # handle when the client cancels: the command must be tracked all the same
    app, rt, runner = _app(timeout=0.05)
    try:
        call = asyncio.ensure_future(_run_shell(app, "sleep 600", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        async with app.lock:  # e.g. a provision in another call holds it
            await asyncio.sleep(0.3)  # the sync-wait times out; the call now waits for the lock
            call.cancel()
            await asyncio.sleep(0)
        with pytest.raises(asyncio.CancelledError):
            await call
        (_tid, handle), = app.tasks.items()
        assert handle.client_cancelled and handle.future is runner.futures[-1] and rt.inflight == 0
    finally:
        runner.settle()


@pytest.mark.parametrize("where", ["lock", "canary"])
async def test_a_cancel_before_the_submit_registers_nothing(where):
    # still in _ready_session — waiting for the lock, or for the block to answer its check: nothing counted, nothing sent
    app, rt, runner = _app()
    if where == "canary":
        runner.canary_gate = asyncio.Event()
        call = asyncio.ensure_future(_run_shell(app, "python train.py", agent_call=True))
        await _until(lambda: runner.canaries == 1)
        call.cancel()
    else:
        async with app.lock:
            call = asyncio.ensure_future(_run_shell(app, "python train.py", agent_call=True))
            await asyncio.sleep(0.05)
            call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert app.tasks == {} and rt.inflight == 0 and runner.commands == []
    if where == "canary":
        runner.canary_gate.set()  # the block answers after all: the next call runs normally
        call = asyncio.ensure_future(_run_shell(app, "true", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        runner.futures[-1].set_result(_Res(0, "", ""))
        assert (await call).phase == "complete" and rt.inflight == 0


async def test_a_call_finishing_as_it_is_cancelled_leaves_nothing_running():
    app, rt, runner = _app()
    try:
        call = asyncio.ensure_future(_run_shell(app, "true", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        runner.futures[-1].set_result(_Res(0, "", ""))
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        assert app.tasks == {} and rt.inflight == 0  # done: nothing holds the block, nothing to poll
    finally:
        runner.settle()


async def test_the_sdk_cancelling_an_unsent_task_is_not_a_client_cancel():
    # Executor.shutdown(cancel_futures=True) cancels a task it has not sent yet; that surfaces as asyncio's
    # CancelledError in the sync-wait although nobody cancelled the call. It must come back as an outcome — the call
    # is still live and the client is waiting for its answer — and nothing ran, so nothing is tracked.
    app, rt, runner = _app()
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        runner.futures[-1].cancel()
        out = await call
        assert out.phase == "failed" and "never reached the endpoint" in out.notice
        assert app.tasks == {} and rt.inflight == 0
    finally:
        runner.settle()


async def test_cancelling_a_poll_keeps_the_handle():
    app, _rt, runner = _app(timeout=0.05)
    try:
        out = await _run_shell(app, "sleep 600")
        assert out.phase == "running"
        poll = asyncio.ensure_future(server._heartbeat(_Ctx(), _poll_task(app, out.task_id, wait=5.0), "poll_task"))
        await asyncio.sleep(0.1)  # inside its courtesy wait
        poll.cancel()
        with pytest.raises(asyncio.CancelledError):
            await poll
        assert out.task_id in app.tasks  # the handle survives a cancelled poll
        runner.futures[-1].set_result(_Res(0, "slept\n", ""))
        got = await _poll_task(app, out.task_id)
        assert got.phase == "complete" and got.stdout == "slept\n"
    finally:
        runner.settle()


async def test_a_cancelled_login_call_does_not_hold_the_compute_stop(monkeypatch):
    # stop releases the COMPUTE block; the command of a cancelled agent call on the free login shape is not on it —
    # and the stop's own scancel runs in the internal login session, so the agent's busy one does not refuse it.
    # Drives the REAL release.
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app, rt, runner = _app()
    rt.warm_confirmed_at = time.monotonic()  # a warm block: the plain release
    try:
        login = _shape_runtime(app, "login")
        await _cancel_mid_wait(app, login, _run_shell(app, "make -j8", shape="login", agent_call=True))
        (tid, handle), = app.tasks.items()
        assert handle.shape == "login" and handle.client_cancelled and handle.session_id == "default"
        n = len(runner.commands)
        stop = asyncio.ensure_future(_stop_endpoint(app))
        await _until(lambda: len(runner.commands) == n + 1)  # the scancel IS sent
        assert f"sessions/{server._INTERNAL_SESSION}" in runner.commands[-1]
        runner.futures[-1].set_result(_Res(0, "released 4242\n", ""))
        st = await stop
        assert st.status == "down" and "4242" in st.notice and "compute" not in app.shapes
        assert tid in app.tasks  # the agent's cancelled login command is still tracked
    finally:
        runner.settle()


async def test_an_internal_op_past_its_sync_wait_does_not_occupy_the_agents_session():
    app, _rt, runner = _app(timeout=0.05)
    try:
        out = await server._login_runner(app)("squeue -u $USER")  # an internal scheduler op
        assert out.phase == "running"
        (_tid, handle), = app.tasks.items()
        assert handle.shape == "login" and handle.session_id == server._INTERNAL_SESSION
        call = asyncio.ensure_future(_run_shell(app, "ls", shape="login", agent_call=True))
        await _until(lambda: len(runner.commands) == 2)  # the agent's default login session is free
        runner.futures[-1].set_result(_Res(0, "a\n", ""))
        assert (await call).stdout == "a\n"
    finally:
        runner.settle()


async def test_a_cancelled_stop_leaves_nothing_in_the_agents_session(monkeypatch):
    # The stop's own scancel rides run_shell on the login shape, in the agent's default session. Esc on the stop must
    # not leave that internal command registered there: the agent's next login command would be refused, blamed on a
    # call it never made, and the retried stop's scancel refused as busy ("channel was cold"). Drives the REAL release.
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app, rt, runner = _app()
    rt.warm_confirmed_at = time.monotonic()  # a warm block: the plain release
    try:
        login = _shape_runtime(app, "login")
        stop = asyncio.ensure_future(server._heartbeat(_Ctx(), _stop_endpoint(app), "stop_endpoint"))
        await _until(lambda: login.inflight == 1)  # the scancel is in its sync-wait on the login shape
        stop.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stop
        await _until(lambda: login.inflight == 0)
        assert app.tasks == {} and app.shapes.get("compute") is rt  # nothing tracked; nothing released yet

        call = asyncio.ensure_future(_run_shell(app, "squeue -u $USER", shape="login", agent_call=True))
        await _until(lambda: login.inflight == 1)
        runner.futures[-1].set_result(_Res(0, "ok\n", ""))
        assert (await call).phase == "complete"  # the agent's own login command runs

        stop = asyncio.ensure_future(_stop_endpoint(app))  # and the retried stop releases the block
        await _until(lambda: login.inflight == 1)  # its scancel, through the same login session
        runner.futures[-1].set_result(_Res(0, "released 4242\n", ""))
        st = await stop
        assert st.status == "down" and "4242" in st.notice and "compute" not in app.shapes
    finally:
        runner.settle()


# --- a stop racing a dispatch ------------------------------------------------------------------------------------------


@pytest.mark.parametrize("mep", [False, True])
async def test_stop_during_a_sync_wait_refuses(released, mep):
    app, rt, runner = _app(mep=mep)
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py"))
        await _until(lambda: rt.inflight == 1)
        st = await _stop_endpoint(app)
        assert st.status == "up" and "sync-wait" in st.notice and "can't stop yet" in st.notice
        assert app.shapes.get("compute") is rt and released == []  # nothing released, nothing dropped
        runner.futures[-1].set_result(_Res(0, "done\n", ""))
        out = await call
        assert out.phase == "complete" and out.stdout == "done\n" and app.shapes.get("compute") is rt
        st = await _stop_endpoint(app)
        assert st.status == ("draining" if mep else "down")
    finally:
        runner.settle()


@pytest.mark.parametrize("mep", [False, True])
async def test_a_stop_behind_a_provisioning_run_shell_sees_its_command(released, mep):
    # The run_shell was issued first and holds the lock through its canary; the stop arrives meanwhile. It must look
    # only after the run_shell has counted its dispatch — not release (or drain) the block under the command.
    app, rt, runner = _app(mep=mep)
    runner.canary_gate = asyncio.Event()
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py"))
        await _until(lambda: runner.canaries == 1)
        stop = asyncio.ensure_future(_stop_endpoint(app))
        await asyncio.sleep(0.05)
        runner.canary_gate.set()  # the canary answers; run_shell counts + submits before the stop gets the lock
        st = await stop
        assert len(runner.commands) == 1 and not runner.futures[-1].done()  # the command WAS sent
        assert st.status == "up" and "sync-wait" in st.notice and released == []
        assert app.shapes.get("compute") is rt
        runner.futures[-1].set_result(_Res(0, "done\n", ""))
        out = await call
        assert out.phase == "complete" and "released" not in (out.notice or "")
        assert (await _stop_endpoint(app)).status == ("draining" if mep else "down")
    finally:
        runner.settle()


async def test_a_dispatch_that_reaches_the_block_during_the_release_is_not_reported_down(monkeypatch):
    # The stop looked (nothing running) and is releasing over the login shape; a run_shell sends its command meanwhile.
    # The stop must not say 'down' over it, nor drop the shape it is tracked on.
    app, rt, runner = _app(timeout=0.3)
    gate = asyncio.Event()
    releases = []

    async def slow_release(a, eid, *rest, **kw):
        releases.append(eid)
        await gate.wait()
        return True, "released 1 block"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", slow_release)
    try:
        stop = asyncio.ensure_future(_stop_endpoint(app))
        await _until(lambda: releases == ["fake-eid"])  # the stop passed its checks and is releasing
        call = asyncio.ensure_future(_run_shell(app, "python train.py"))
        await _until(lambda: rt.inflight == 1)
        gate.set()
        st = await stop
        assert st.status == "draining" and "NOT confirmed stopped" in st.notice and "sync-wait" in st.notice
        assert "stop_endpoint again" in st.notice and app.shapes.get("compute") is rt
        out = await call  # past its 0.3 s wait: a poll handle on the kept shape
        assert out.phase == "running" and out.task_id in app.tasks
        st = await _stop_endpoint(app)
        assert st.status == "up" and out.task_id in st.notice
        runner.futures[-1].set_result(_Res(0, "done\n", ""))
        assert (await _poll_task(app, out.task_id)).stdout == "done\n"
        assert (await _stop_endpoint(app)).status == "down"
    finally:
        runner.settle()


# --- a block released under a sent task --------------------------------------------------------------------------------


async def test_a_result_that_arrives_after_its_block_was_released_says_so(released):
    app, rt, runner = _app()
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        await warmth._drop_compute_shape(app)  # e.g. a teardown's release while the call waited
        runner.futures[-1].set_result(_Res(0, "done\n", ""))
        out = await call
        assert out.phase == "complete" and out.stdout == "done\n" and out.block_state == "cold"
        assert "released while this call waited" in out.notice
        assert "compute" not in app.shapes and app.tasks == {}  # not revived; nothing left to poll
    finally:
        runner.settle()


@pytest.mark.parametrize("how", ["timeout", "cancel"])
async def test_a_sent_task_stays_tracked_when_its_block_is_released_under_it(released, how):
    # e.g. a teardown's release while the call waited: the endpoint is still bound and the SDK still resolves the
    # task, so it keeps a handle (stop refuses while it runs; poll_task returns it) — on no rebuilt runtime
    app, rt, runner = _app(timeout=0.3 if how == "timeout" else 5.0)
    try:
        call = asyncio.ensure_future(server._heartbeat(_Ctx(), _run_shell(app, "python train.py", agent_call=True),
                                                       "run_shell"))
        await _until(lambda: rt.inflight == 1)
        await warmth._drop_compute_shape(app)
        if how == "cancel":
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call
            (tid, handle), = app.tasks.items()
            assert handle.client_cancelled
        else:
            out = await call
            assert out.phase == "running" and "released while this call waited" in out.notice
            assert out.block_state == "cold" and "stays warm" not in out.notice  # agrees with the stop's refusal
            tid = out.task_id
        assert "compute" not in app.shapes  # not revived
        st = await _stop_endpoint(app)
        assert st.status == "up" and st.block_state == "cold" and tid in st.notice and released == []
        polled = await _poll_task(app, tid)
        assert polled.phase == "running" and polled.block_state == "cold"  # agrees with the refusal
        runner.futures[-1].set_result(_Res(0, "done\n", ""))
        got = await _poll_task(app, tid)
        assert got.phase == "complete" and got.stdout == "done\n" and got.block_state == "cold"
        assert "compute" not in app.shapes  # the poll did not revive it either
        assert (await _stop_endpoint(app)).status == "down"
    finally:
        runner.settle()


async def test_a_stop_sees_a_call_still_waiting_on_a_released_block(released):
    # a teardown's release dropped the shape (then, say, it waits for a one-time code); a call is still in its sync-wait
    # on the dropped runtime — its command is on the endpoint, so a stop must not answer 'down' over it
    app, rt, runner = _app(timeout=0.3)
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        await warmth._drop_compute_shape(app)
        st = await _stop_endpoint(app)
        assert st.status == "up" and "sync-wait" in st.notice and st.block_state == "cold" and released == []
        out = await call
        assert out.phase == "running" and out.task_id in app.tasks  # tracked once it returns...
        assert (await _stop_endpoint(app)).status == "up" and released == []  # ...and still holding the stop
        runner.futures[-1].set_result(_Res(0, "done\n", ""))
        assert (await _poll_task(app, out.task_id)).stdout == "done\n"
        assert (await _stop_endpoint(app)).status == "down" and app.released_inflight == []
    finally:
        runner.settle()


@pytest.mark.parametrize("same", [True, False])
async def test_a_rebind_under_a_waiting_call(released, same):
    # a re-connect drops every shape while a call is in its sync-wait. To the SAME endpoint (a zero-SSH reconnect) the
    # command is still ours to track: a stop must not say 'down' over it, and the call says what happened. To another
    # endpoint, the old one's work is abandoned (as its tasks are): the stop goes ahead.
    app, rt, runner = _app(timeout=0.3)
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        async with app.lock:
            warmth._drop_all_shapes(app, bank=True)
            app.state = EndpointState(endpoint_id="fake-eid" if same else "other-eid")
            new_rt = _shape_runtime(app, "compute")  # the new binding builds its own compute runtime meanwhile
        st = await _stop_endpoint(app)
        out = await call
        if same:
            assert st.status == "up" and "sync-wait" in st.notice and released == []
            assert out.phase == "running" and out.task_id in app.tasks and "re-connect" in out.notice
            assert app.shapes.get("compute") is new_rt and out.block_state == "cold"
            assert (await _poll_task(app, out.task_id)).block_state == "cold"  # its own block, as the call said
            runner.futures[0].set_result(_Res(0, "done\n", ""))
            got = await _poll_task(app, out.task_id)
            assert got.phase == "complete" and got.block_state == "cold"
            assert new_rt.warm_confirmed_at is None  # the old block's result is not news about the new one
        else:
            assert st.status == "down" and released == ["other-eid"]
            assert out.phase == "failed" and out.task_id is None and app.tasks == {}
    finally:
        runner.settle()


@pytest.mark.parametrize("sid", ["_hpc-bridge-internal", "_hpc-bridge-x"])  # the prefix, not only the one id
@pytest.mark.parametrize("tool", ["run_shell", "reset_session"])
async def test_the_internal_session_is_reserved(tool, sid):
    app, _rt, runner = _app()
    out = await (_run_shell(app, "make -j8", session_id=sid, shape="login", agent_call=True) if tool == "run_shell"
                 else _reset_session(app, session_id=sid, shape="login", agent_call=True))
    assert out.phase == "failed" and "reserved" in out.notice and runner.commands == [] and app.tasks == {}


async def test_a_stop_behind_a_busy_internal_op_says_so(monkeypatch):
    # an earlier scheduler command of ours outlived its sync-wait in the internal session: the stop's cancel cannot be
    # sent. It must say that — not relay "run in a different session_id", nor blame a cold channel.
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app, rt, runner = _app(timeout=0.05)
    rt.warm_confirmed_at = time.monotonic()
    try:
        out = await server._login_runner(app)("squeue -u $USER")
        assert out.phase == "running"
        again = await server._login_runner(app)("squeue -u $USER")  # the next internal op: busy, in its own words
        assert again.phase == "failed" and "busy, not cold" in again.notice and out.task_id in again.notice
        assert "different session_id" not in again.notice
        n = len(runner.commands)
        st = await _stop_endpoint(app)
        assert len(runner.commands) == n  # nothing was sent
        assert st.status == "draining" and "could not be sent" in st.notice and out.task_id in st.notice
        assert "busy, not cold" in st.notice
        assert "different session_id" not in st.notice and "channel was cold" not in st.notice
    finally:
        runner.settle()


async def test_a_stops_own_cancel_still_queued_is_reported_as_sent(monkeypatch):
    # the stop's own scancel went out but outlived its sync-wait (queued behind the agent's long login command on the
    # single login worker, or a slow scheduler): it WAS sent — not "could not be sent"
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app, rt, runner = _app(timeout=0.05)
    rt.warm_confirmed_at = time.monotonic()
    try:
        agent = await _run_shell(app, "make -j8", session_id="b", shape="login", agent_call=True)
        assert agent.phase == "running"
        n = len(runner.commands)
        st = await _stop_endpoint(app)
        assert len(runner.commands) == n + 1  # exactly one cancel went out...
        (own,) = [t for t, h in app.tasks.items() if h.session_id == server._INTERNAL_SESSION]
        assert st.status == "draining" and "NOT confirmed stopped" in st.notice
        assert f"the cancel was sent (task_id={own!r})" in st.notice and "could not be sent" not in st.notice
    finally:
        runner.settle()


async def test_a_released_task_past_its_ceiling_reads_cold(released):
    app, rt, runner = _app(timeout=0.3)
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        await warmth._drop_compute_shape(app)
        out = await call
        app.tasks[out.task_id].submitted_at -= app.tasks[out.task_id].ceiling_s + 1
        polled = await _poll_task(app, out.task_id)
        assert polled.phase == "running" and "past its" in polled.notice and polled.block_state == "cold"
    finally:
        runner.settle()


async def test_a_login_task_past_its_ceiling_does_not_hold_the_stop(released):
    # a login command queued behind another on the single login worker: the advice must fit the free login shape —
    # not "teardown_endpoint" (an SSH teardown to drop a free command), and stop is not held by it
    app, rt, runner = _app(timeout=0.05)
    rt.warm_confirmed_at = time.monotonic()
    try:
        out = await _run_shell(app, "make -j8", session_id="b", shape="login", agent_call=True)
        h = app.tasks[out.task_id]
        h.submitted_at -= h.ceiling_s + 1
        polled = await _poll_task(app, out.task_id)
        assert polled.phase == "running" and "holds its session ('b')" in polled.notice and "queue behind it" in polled.notice
        assert "teardown_endpoint" not in polled.notice and "another session_id" in polled.notice
        assert (await _stop_endpoint(app)).status == "down" and released == ["fake-eid"]
    finally:
        runner.settle()


@pytest.mark.parametrize("how", ["timeout", "cancel"])
async def test_a_task_whose_endpoint_is_unbound_under_it_is_reported_not_tracked(how):
    # a teardown finished or a re-bind happened while the call waited: hpc-bridge abandons that endpoint's tasks
    app, rt, runner = _app(timeout=0.3 if how == "timeout" else 5.0)
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        async with app.lock:
            warmth._drop_all_shapes(app, bank=True)
        if how == "cancel":
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call
        else:
            out = await call
            assert out.phase == "failed" and out.task_id is None and "do not assume spend has stopped" in out.notice
        assert app.tasks == {} and app.shapes == {}
    finally:
        runner.settle()


async def test_a_cancel_after_its_runner_was_swapped_is_not_tracked():
    # a runner swapped under the call (only for a new endpoint now: _runner_for defers a swap while a call is in
    # flight) must not get a handle for a future of the runner it replaced
    app, rt, runner = _app()
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py", agent_call=True))
        await _until(lambda: rt.inflight == 1)
        rt.runner = _BlockingRunner("fake-eid", _Res(0, "", ""))
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        assert app.tasks == {} and rt.inflight == 0
    finally:
        runner.settle()


async def test_a_credential_swap_waits_for_a_call_in_its_sync_wait():
    app, rt, runner = _app()
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py"))
        await _until(lambda: rt.inflight == 1)
        rt.runner_stale = True  # e.g. a new Globus login
        await _ensure_endpoint_up(app)
        assert rt.runner is runner and not runner.closed  # not closed under the call
        runner.futures[-1].set_result(_Res(0, "", ""))
        assert (await call).phase == "complete"
        await _ensure_endpoint_up(app)
        assert runner.closed  # the swap happens once the call has returned
    finally:
        runner.settle()


# --- a task that never reports -----------------------------------------------------------------------------------------


@pytest.mark.parametrize("op", ["stop", "detach"])
async def test_a_lost_task_stops_holding_a_facility_block(op):
    # On a facility endpoint nothing can cancel a task, and a lost one is never ORPHANED (the manager stays online):
    # it must not hold stop and the detach forever, and the bound the notices quote must stay true.
    app, _rt, runner = _app(mep=True, timeout=0.05)
    try:
        out = await _run_shell(app, "sleep 600")
        assert out.phase == "running"
        h = app.tasks[out.task_id]
        h.submitted_at -= h.ceiling_s - 100  # 100 s of its ceiling left
        st = await _stop_endpoint(app)
        m = re.search(r"at most ~(\d+)s more", st.notice)
        assert st.status == "up" and m and int(m.group(1)) <= 100
        h.submitted_at -= 200  # past its ceiling, inside the grace: it may only be queued — still holds
        st = await _stop_endpoint(app)
        assert st.status == "up" and "at most ~" not in st.notice and "relaunching block, or lost" in st.notice
        m = re.search(r"with none within ~(\d+)s it is presumed lost", st.notice)  # when it stops holding the stop
        assert m and warmth._LOST_TASK_GRACE_S - 200 <= int(m.group(1)) <= warmth._LOST_TASK_GRACE_S - 100
        polled = await _poll_task(app, out.task_id)
        assert polled.phase == "running" and "Keep polling" in polled.notice and "within ~" in polled.notice
        h.submitted_at -= warmth._LOST_TASK_GRACE_S  # past ceiling + grace: presumed lost
        polled = await _poll_task(app, out.task_id)
        assert polled.phase == "running" and "presumed lost" in polled.notice and out.task_id in app.tasks
        res = await (_stop_endpoint(app) if op == "stop" else _teardown_endpoint(app))
        assert res.status == ("draining" if op == "stop" else "down")  # released, saying what it abandons
        assert out.task_id in res.notice and "presumed lost" in res.notice and "abandoned" in res.notice
        assert out.task_id not in app.tasks
    finally:
        runner.settle()


async def test_a_task_past_its_ceiling_still_holds_an_ssh_block_and_its_session(released):
    # Its ceiling counts from when it STARTS: past it (from the submit) a task may only be queued behind a relaunching
    # block. On an SSH endpoint a stop would release the block while the endpoint relaunches one for it, a re-run would
    # clobber its session, a canary queued behind it is no reap — so it holds everything as any running task does.
    app, rt, runner = _app(timeout=0.05)
    try:
        out = await _run_shell(app, "python train.py")
        h = app.tasks[out.task_id]
        h.submitted_at -= h.ceiling_s + warmth._LOST_TASK_GRACE_S + 1
        st = await _stop_endpoint(app)
        assert st.status == "up" and released == [] and "relaunching block, or lost" in st.notice
        n = len(runner.commands)
        refused = await _run_shell(app, "python train.py")
        assert refused.phase == "failed" and out.task_id in refused.notice and len(runner.commands) == n
        rt.warm_confirmed_at -= app.profile.max_idletime_s + 600  # long quiet: but a task may be queued on the block
        assert warmth._presumed_reaped(app, "compute", rt) is None
        runner._canary = _TIMEOUT
        res = await _ensure_endpoint_up(app)
        assert res.status == "up" and rt.reaped is None and rt.spend_confirmed is True
        polled = await _poll_task(app, out.task_id)
        assert polled.phase == "running" and "Keep polling" in polled.notice
        assert "To abandon it, teardown_endpoint" in polled.notice and "presumed lost" not in polled.notice
        assert "another task or a relaunching block" in polled.notice
        runner.futures[0].set_result(_Res(0, "trained\n", ""))  # it was only queued after all
        assert (await _poll_task(app, out.task_id)).stdout == "trained\n"
        assert (await _stop_endpoint(app)).status == "down" and released == ["fake-eid"]
    finally:
        runner.settle()


# --- reset_session -----------------------------------------------------------------------------------------------------


async def test_a_reset_past_its_sync_wait_is_tracked(released):
    app, _rt, runner = _app(timeout=0.05)
    try:
        out = await _reset_session(app)
        assert out.phase == "running" and app.tasks[out.task_id].reset and "reset is still queued" in out.notice
        assert "batch job" not in out.notice
        st = await _stop_endpoint(app)
        assert st.status == "up" and out.task_id in st.notice and released == []
        runner.futures[-1].set_result(_Res(0, "", ""))
        assert (await _poll_task(app, out.task_id)).phase == "complete"
        assert (await _stop_endpoint(app)).status == "down"
    finally:
        runner.settle()


@pytest.mark.parametrize("tool", ["run_shell", "reset_session"])
async def test_a_submit_that_raises_is_a_dispatch_failure(tool):
    app, rt, runner = _app()

    def boom(command):
        raise RuntimeError("Executor<...> is shutdown; no new functions may be executed")

    runner.submit = boom
    out = await (_run_shell(app, "true") if tool == "run_shell" else _reset_session(app))
    assert out.phase == "failed" and "is shutdown" in out.stderr_snippet and "Dispatch error" in out.notice
    assert rt.inflight == 0 and rt.warm_confirmed_at is None  # warm trust cleared: the next call re-checks


# --- a facility detach -------------------------------------------------------------------------------------------------


async def test_mep_detach_refuses_while_a_task_runs():
    app, rt, runner = _app(mep=True, timeout=0.05)
    try:
        out = await _run_shell(app, "sleep 600")
        assert out.phase == "running"
        td = await _teardown_endpoint(app)
        assert td.status == "up" and "can't detach yet" in td.notice and out.task_id in td.notice
        assert "no cancel channel" in td.notice.lower() and "teardown_endpoint" in td.notice
        assert out.task_id in app.tasks and app.shapes.get("compute") is rt  # nothing cleared, result retrievable
        runner.futures[-1].set_result(_Res(0, "", ""))
        td = await _teardown_endpoint(app)
        assert td.status == "down" and "No task of ours is known to be running there" in td.notice
        assert app.tasks == {} and app.shapes == {}
    finally:
        runner.settle()


async def test_mep_detach_refuses_during_a_sync_wait():
    app, rt, runner = _app(mep=True)
    try:
        call = asyncio.ensure_future(_run_shell(app, "python train.py"))
        await _until(lambda: rt.inflight == 1)
        td = await _teardown_endpoint(app)
        assert td.status == "up" and "sync-wait" in td.notice and app.shapes.get("compute") is rt
        runner.futures[-1].set_result(_Res(0, "", ""))
        assert (await call).phase == "complete"
    finally:
        runner.settle()

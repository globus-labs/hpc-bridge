import pytest

from hpc_bridge.runner import GlobusRunner, _escape_for_shellfunction


def test_escape_braces_for_shellfunction_roundtrips():
    # ShellFunction does cmd.format(**kwargs); literal braces must be doubled so they
    # survive as single braces (and don't get read as replacement fields).
    cmd = "mkdir -p x && { echo hi; } && echo ${HOME}"
    escaped = _escape_for_shellfunction(cmd)
    assert "{{" in escaped and "}}" in escaped
    assert escaped.format() == cmd  # collapses back to the original shell command


class FakeFuture:
    def __init__(self, result):
        self._r = result

    def result(self, timeout=None):
        return self._r


class FakeExecutor:
    def __init__(self):
        self.submitted = []
        self.shutdowns = 0
        self.shutdown_kwargs = None

    def submit(self, fn):
        self.submitted.append(fn)
        return FakeFuture("RESULT")

    def shutdown(self, wait=True, cancel_futures=False):
        self.shutdowns += 1
        self.shutdown_kwargs = {"wait": wait, "cancel_futures": cancel_futures}


def test_executor_created_once_and_closed():
    ex = FakeExecutor()
    calls = []

    def factory():
        calls.append(1)
        return ex

    r = GlobusRunner("eid", executor_factory=factory)
    assert r.executor() is ex
    assert r.executor() is ex  # cached, not re-created
    assert calls == [1]
    r.close()
    assert ex.shutdowns == 1
    # close() must NOT block on the AMQP drain (shutdown defaults to wait=True) — that was the
    # multi-minute stop hang. It shuts down non-blocking and cancels un-registered futures.
    assert ex.shutdown_kwargs == {"wait": False, "cancel_futures": True}


async def test_run_submits_shellfunction_and_returns_result():
    pytest.importorskip("globus_compute_sdk")
    ex = FakeExecutor()
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    res = await r.run("echo hi")
    assert res == "RESULT"
    assert len(ex.submitted) == 1  # one ShellFunction submitted


def test_parse_canary_extracts_versions():
    from hpc_bridge.runner import _parse_canary

    assert _parse_canary("HPCB_CANARY\n3.11.7 0.3.9 a070.anvil\n") == ("3.11.7", "0.3.9", "a070.anvil")
    assert _parse_canary("HPCB_CANARY\n") == (None, None, None)  # version line absent
    assert _parse_canary("") == (None, None, None)


class _ShellRes:
    def __init__(self, stdout):
        self.stdout = stdout
        self.returncode = 0
        self.stderr = ""


class _CanaryFuture:
    """A resolved future (result or exception) — or, with pending=True, one still queued: result() times out
    until resolve()/fail() is called."""

    def __init__(self, result=None, exc=None, *, pending=False):
        self._r = result
        self._exc = exc
        self._pending = pending
        self._callbacks = []

    def done(self):
        return not self._pending

    def cancelled(self):
        return False

    def exception(self, timeout=None):
        return None if self._pending else self._exc

    def add_done_callback(self, fn):
        if self._pending:
            self._callbacks.append(fn)
        else:
            fn(self)

    def resolve(self, result=None, exc=None):
        self._r, self._exc, self._pending = result, exc, False
        for fn in self._callbacks:
            fn(self)

    def result(self, timeout=None):
        if self._pending:
            raise TimeoutError()
        if self._exc is not None:
            raise self._exc
        return self._r


class _CanaryExecutor:
    def __init__(self, *, result=None, exc=None):
        self._fut = _CanaryFuture(result, exc)
        self.submitted = []

    def submit(self, fn):
        self.submitted.append(fn)
        return self._fut

    def shutdown(self, wait=True, cancel_futures=False):
        pass


async def test_canary_ok_parses_worker_versions():
    pytest.importorskip("globus_compute_sdk")
    ex = _CanaryExecutor(result=_ShellRes("HPCB_CANARY\n3.11.7 0.3.9 a070.anvil\n"))
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    res = await r.canary(timeout=2.0)
    assert res.ok is True
    assert (res.worker_python, res.worker_dill, res.worker_host) == ("3.11.7", "0.3.9", "a070.anvil")
    assert len(ex.submitted) == 1  # the canary went through the real Executor path


async def test_canary_timeout_reports_not_ok():
    # No worker answered within the budget -> not warm (block still cold-starting), NOT an exception.
    pytest.importorskip("globus_compute_sdk")
    ex = _QueueExecutor(_CanaryFuture(pending=True))
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    res = await r.canary(timeout=0.5)
    assert res.ok is False and res.error == "timeout"


async def test_canary_ok_even_without_version_line():
    # python/dill missing on the worker (|| true) -> sentinel only: a returned result still
    # proves a worker is live, versions just come back None.
    pytest.importorskip("globus_compute_sdk")
    ex = _CanaryExecutor(result=_ShellRes("HPCB_CANARY\n"))
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    res = await r.canary()
    assert res.ok is True and res.worker_python is None


def test_runner_passes_user_endpoint_config_to_executor():
    captured = {}

    def factory():
        class _Ex:
            def __init__(self):
                captured["made"] = True

            def submit(self, fn):  # not exercised here
                raise AssertionError("not called")

        return _Ex()

    r = GlobusRunner("eid-1", user_endpoint_config={"provider_type": "LocalProvider"})
    assert r.user_endpoint_config == {"provider_type": "LocalProvider"}
    r2 = GlobusRunner("eid-1", executor_factory=factory)
    assert r2.executor() is not None and captured["made"] is True


class _ShutdownExecutor:
    """A cached Executor that has already shut down: .submit() raises, as the SDK does."""

    def submit(self, fn):
        raise RuntimeError("Executor is shutdown; no new functions may be executed")

    def shutdown(self, wait=True, cancel_futures=False):
        pass


async def test_canary_survives_shutdown_executor():
    # #37: a shut-down cached Executor raises AT .submit() (not at .result()). The canary must map
    # that to not-ok, NEVER let it propagate — otherwise it unwinds past _provision and there is no
    # recovery (the observed "RuntimeError: Executor is shutdown" dead-end). submit is inside the guard.
    # This is THE #37 fix: a dead 'online' ghost, once reused, degrades to 'provisioning', not a crash.
    pytest.importorskip("globus_compute_sdk")
    r = GlobusRunner("eid", executor_factory=lambda: _ShutdownExecutor())
    res = await r.canary(timeout=0.5)
    assert res.ok is False
    assert "shutdown" in (res.error or "").lower()



class _QueueExecutor:
    """Each submit hands back the next future from `futures` (pending ones simulate a cold block)."""

    def __init__(self, *futures):
        self.futures = list(futures)
        self.submitted = []

    def submit(self, fn):
        self.submitted.append(fn)
        return self.futures.pop(0)

    def shutdown(self, wait=True, cancel_futures=False):
        pass


async def test_a_pending_canary_is_waited_on_again_not_resubmitted():
    # every probe used to submit a NEW canary: 24 queued in one cold-start wait on Delta, and a facility endpoint
    # keeps relaunching billed blocks while any task is queued (2026-10-06)
    pytest.importorskip("globus_compute_sdk")
    pending = _CanaryFuture(pending=True)
    ex = _QueueExecutor(pending)
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    for _ in range(3):
        assert (await r.canary(timeout=0.1)).error == "timeout"
    assert len(ex.submitted) == 1
    pending.resolve(_ShellRes("HPCB_CANARY\n3.13.12 0.3.9 b001\n"))  # the block came up between probes
    res = await r.canary(timeout=0.1)
    assert res.ok and res.worker_host == "b001" and len(ex.submitted) == 1


async def test_a_stale_answer_is_replaced_and_a_failure_is_not_reused(monkeypatch):
    pytest.importorskip("globus_compute_sdk")
    from hpc_bridge import runner as runner_mod

    first = _CanaryFuture(pending=True)
    second = _CanaryFuture(exc=RuntimeError("Executor is shutdown"))
    third = _CanaryFuture(result=_ShellRes("HPCB_CANARY\n"))
    ex = _QueueExecutor(first, second, third)
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    await r.canary(timeout=0.1)
    first.resolve(_ShellRes("HPCB_CANARY\n"))
    monkeypatch.setattr(runner_mod, "_CANARY_FRESH_S", -1.0)  # that answer is now too old to vouch for the block
    res = await r.canary(timeout=0.1)
    assert res.ok is False and "Executor is shutdown" in res.error and len(ex.submitted) == 2
    monkeypatch.setattr(runner_mod, "_CANARY_FRESH_S", 30.0)
    assert (await r.canary(timeout=0.1)).ok and len(ex.submitted) == 3  # the failed one was not kept


async def test_closing_the_runner_forgets_the_pending_canary():
    pytest.importorskip("globus_compute_sdk")
    ex = _QueueExecutor(_CanaryFuture(pending=True), _CanaryFuture(result=_ShellRes("HPCB_CANARY\n")))
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    await r.canary(timeout=0.1)
    r.close()
    assert (await r.canary(timeout=0.1)).ok and len(ex.submitted) == 2


async def test_a_canary_pending_past_the_max_wait_is_abandoned_for_a_fresh_one(monkeypatch):
    # an endpoint that restarts can drop a task it had accepted: waiting on it forever wedged the shape (review)
    pytest.importorskip("globus_compute_sdk")
    from hpc_bridge import runner as runner_mod

    lost, fresh = _CanaryFuture(pending=True), _CanaryFuture(result=_ShellRes("HPCB_CANARY\n"))
    ex = _QueueExecutor(lost, fresh)
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    assert (await r.canary(timeout=0.1)).error == "timeout"
    assert (await r.canary(timeout=0.1)).error == "timeout" and len(ex.submitted) == 1  # still within the wait
    monkeypatch.setattr(runner_mod, "_CANARY_MAX_WAIT_S", -1.0)
    assert (await r.canary(timeout=0.1)).ok and len(ex.submitted) == 2


async def test_a_failure_that_arrived_unwatched_is_handed_back_whatever_its_age(monkeypatch):
    # a no-account verdict resolving between probes must reach the caller (it is terminal), not be discarded as
    # stale and replaced by a resubmit into a shut-down Executor (review)
    pytest.importorskip("globus_compute_sdk")
    from hpc_bridge import runner as runner_mod

    verdict = _CanaryFuture(pending=True)
    ex = _QueueExecutor(verdict, _CanaryFuture(result=_ShellRes("HPCB_CANARY\n")))
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    await r.canary(timeout=0.1)
    verdict.resolve(exc=RuntimeError("Identity failed to map to a local user name"))
    monkeypatch.setattr(runner_mod, "_CANARY_FRESH_S", -1.0)
    res = await r.canary(timeout=0.1)
    assert res.ok is False and "Identity failed to map" in res.error and len(ex.submitted) == 1
    assert (await r.canary(timeout=0.1)).ok and len(ex.submitted) == 2  # handed back once, then a fresh ask


async def test_a_cancelled_canary_is_replaced():
    pytest.importorskip("globus_compute_sdk")
    from concurrent.futures import Future

    cancelled: Future = Future()
    cancelled.cancel()
    ok: Future = Future()
    ok.set_result(_ShellRes("HPCB_CANARY\n"))
    ex = _QueueExecutor(cancelled, ok)
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    res = await r.canary(timeout=0.1)
    assert res.ok is False and "cancelled" in res.error
    assert (await r.canary(timeout=0.1)).ok and len(ex.submitted) == 2


async def test_warmth_is_dated_from_when_the_worker_answered():
    # an answer that arrived while nobody was waiting is up to _CANARY_FRESH_S old: the 45 s TTL, the idle clock
    # and the reap estimate must measure from it, not from when it was read (review)
    pytest.importorskip("globus_compute_sdk")
    import time

    from hpc_bridge.profile import Profile
    from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _shape_runtime
    from tests.fakes import FakeFacility

    pending = _CanaryFuture(pending=True)
    ex = _QueueExecutor(pending)
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    other = _QueueExecutor(*[_CanaryFuture(result=_ShellRes("")) for _ in range(5)])  # the login shape's pilot query

    def factory(eid, user_endpoint_config=None, **kw):
        mine = ex if (user_endpoint_config or {}).get("compute") else other
        return GlobusRunner(eid, executor_factory=lambda: mine)

    app.runner_factory = factory
    assert (await _ensure_endpoint_up(app, confirm_spend=True)).status == "provisioning"
    pending.resolve(_ShellRes("HPCB_CANARY\n3.13.12 0.3.9 b001\n"))
    runner = _shape_runtime(app, "compute").runner
    runner._canary_done_at = time.monotonic() - 20  # it answered 20 s before the next probe
    assert (await _ensure_endpoint_up(app)).status == "up"
    rt = _shape_runtime(app, "compute")
    assert time.monotonic() - rt.warm_confirmed_at >= 19.9  # the real answer time, not when it was read
    assert len(ex.submitted) == 1                            # and the pending canary was reused, not resubmitted


async def test_a_task_that_failed_with_a_timeout_error_is_a_failure_not_pending():
    # a done future whose exception IS a TimeoutError used to read as "still queued" forever (verification)
    pytest.importorskip("globus_compute_sdk")
    ex = _QueueExecutor(_CanaryFuture(exc=TimeoutError("worker walltime")), _CanaryFuture(result=_ShellRes("HPCB_CANARY\n")))
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    res = await r.canary(timeout=0.1)
    assert res.ok is False and res.error != "timeout" and "worker walltime" in res.error
    assert (await r.canary(timeout=0.1)).ok and len(ex.submitted) == 2

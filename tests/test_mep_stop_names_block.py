"""#178: a stop on a facility multi-user endpoint names the block it cannot cancel.

A facility MEP gives hpc-bridge no cancel channel, so a stop issues NO cancel: the block bills on until the facility's
idle-release or its walltime. The stop's notice leads with that, names the block's scheduler job (`block_job_id`,
resolved in the stop's locked step before the shape is dropped), the latest it can bill until as a clock time, and the
user's own command to end it (`scancel` / `qdel`); `spend_stopped` is never True there. A self-cancel (the block
scancelling itself) was rejected: the task stays outstanding until the manager notices, so parsl starts a replacement.

Drives a REAL MEPFacility through the real server paths, with a fake runner whose canary reports a scheduler job.
"""
import shutil
import subprocess
import time

import pytest

from hpc_bridge import binding, notices, scheduler_ops, server, warmth
from hpc_bridge.context import LeftBlock
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.lifecycle import EndpointState
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import (
    AppCtx,
    ShapeRuntime,
    _ensure_endpoint_up,
    _run_shell,
    _stop_endpoint,
    _teardown_endpoint,
)
from hpc_bridge.warmth import _IDLE_GRACE_S, _RELEASE_BOUND_S, _last_activity
from tests.fakes import MEP_UUID, FakeCatalog, FakeFacility, fake_mep_entry
from tests.test_mep_server import _Status
from tests.test_server import _FakeRunner, _Res

IDLE = 600


class _FrozenWall:
    """`notices.time` with the WALL clock frozen (monotonic and the rest stay real): a clock time a notice states is the
    same at any hour and in any time zone. A bound ~11 min ahead at 23:55 is tomorrow and carries its date
    ("Oct 15 00:06"), which a test expecting today's "%H:%M" read as a failure (2026-10-14, 23:55 CDT)."""

    def __init__(self, at: float) -> None:
        self.at = at

    def time(self) -> float:
        return self.at

    def localtime(self, secs=None):
        return time.localtime(self.at if secs is None else secs)

    def __getattr__(self, name):
        return getattr(time, name)


@pytest.fixture
def noon(monkeypatch):
    """Freeze notices' wall clock at 12:00:30 local, today (30 s in, so a bound a few ms short of a whole number of
    minutes ahead still lands on its minute). Returns that instant."""
    t = time.localtime()
    at = time.mktime((t.tm_year, t.tm_mon, t.tm_mday, 12, 0, 30, 0, 0, -1))
    monkeypatch.setattr(notices, "time", _FrozenWall(at))
    return at
_TIMEOUT = CanaryResult(ok=False, error="timeout")
_HEAD = "NO CANCEL WAS ISSUED: hpc-bridge has no cancel channel on a facility endpoint."


def _ok(job: str | None = "100", user: str | None = "hpcbmep") -> CanaryResult:
    return CanaryResult(ok=True, worker_host="c1", worker_python="3.12.3", worker_dill="0.3.9", worker_job=job,
                        worker_user=user, answered_at=time.monotonic())


def _entry(*, scheduler="slurm", max_blocks=None):
    over = {}
    if scheduler != "slurm":
        over["compute"] = {"scheduler": scheduler, "interface": "hsn0", "env_setup": "", "scratch_root": "$HOME/.hpcb"}
    if max_blocks is not None:
        over["defaults"] = {"partition": "main", "walltime": "02:00:00", "init_blocks": 1, "max_blocks": max_blocks}
    return fake_mep_entry(**over)


async def _attached(monkeypatch, canary=None, **entry_kw):
    """A MEP attached through the real connect, its idle window known (600 s), one fake runner for every build."""
    fac = MEPFacility.from_entry(_entry(**entry_kw), client_factory=lambda: _Status("online"))
    fac.max_idletime_s = IDLE
    app = AppCtx(facility=fac, profile=Profile())
    runner = _FakeRunner(MEP_UUID, _Res(0, "ok", ""), canary_result=canary or _ok())
    app.built = 0

    def factory(eid, user_endpoint_config=None, **_kw):
        app.built += 1
        return runner

    app.runner_factory = factory
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([_entry(**entry_kw)]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: app.facility)
    res = await server._connect_facility(app, "globus1")
    assert res.phase == "needs_account" and res.reused is True
    return app, runner


async def _reconnect(app):
    return await server._connect_facility(app, "globus1")


async def _warm(monkeypatch, job="100", **kw):
    """A confirmed, warm block (scheduler job `job`) that has run one command."""
    app, runner = await _attached(monkeypatch, canary=_ok(job), **kw)
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"
    assert (await _run_shell(app, "true")).phase == "complete"
    return app, runner


def _age(rt: ShapeRuntime, s: float) -> None:
    """Move the block's whole history `s` seconds into the past."""
    for f in ("warm_confirmed_at", "block_since", "warm_since", "last_proof_at"):
        if getattr(rt, f) is not None:
            setattr(rt, f, getattr(rt, f) - s)


# --- the seven stale-or-missing job-id cases -----------------------------------------------------------------------


async def _case_provisioning(mp):
    # spend confirmed, the block never answered: no job id, but our check is queued — the facility starts one for it
    app, _r = await _attached(mp, canary=_TIMEOUT)
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "provisioning"
    return app, await _stop_endpoint(app)


async def _case_kicked(mp):
    # a check found the block replaced: job 101 answered for job 100 — the stop names the replacement, which bills
    app, runner = await _warm(mp, "100")
    app.shapes["compute"].warm_confirmed_at -= 100  # past the canary TTL, well inside the idle window
    runner._canary = _ok("101")
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    return app, await _stop_endpoint(app)


async def _case_presumed_up(mp):
    # the idle presumption took job 100 for gone, but it may still be up until its latest end (40 s from now)
    app, _r = await _warm(mp, "100")
    _age(app.shapes["compute"], IDLE + _IDLE_GRACE_S + 10)
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    assert app.shapes["compute"].presumed_block_job == "100"
    return app, await _stop_endpoint(app)


async def _case_presumed_released(mp):
    # ...and past that end: named, but presumed released — not verified, so spend_stopped stays unknown
    app, _r = await _warm(mp, "100")
    _age(app.shapes["compute"], IDLE + _RELEASE_BOUND_S + 30)
    return app, await _stop_endpoint(app)


async def _case_restop_after_reconnect(mp):
    # stop, reconnect to the same endpoint, stop again: the block the first stop left is still named
    app, _r = await _warm(mp, "100")
    await _stop_endpoint(app)
    assert (await _reconnect(app)).phase == "needs_account"
    return app, await _stop_endpoint(app)


async def _case_switch(mp):
    # a partition switch leaves job 100 billing on the old partition; the new one is still cold (our check queued)
    app, runner = await _warm(mp, "100")
    runner._canary = _TIMEOUT
    res = await _ensure_endpoint_up(app, shape="compute", partition="other", confirm_spend=True)
    assert res.status == "provisioning"
    return app, await _stop_endpoint(app)


async def _case_relaunched(mp):
    # stop left job 100; the user re-confirmed and job 200 came up (one block: 100 is gone) — only 200 is named
    app, runner = await _warm(mp, "100")
    await _stop_endpoint(app)
    runner._canary = _ok("200")
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"
    return app, await _stop_endpoint(app)


async def _case_max_blocks(mp):
    # more than one block possible: the canary's job is one of them, "at least"
    app, _r = await _warm(mp, "100", max_blocks=2)
    return app, await _stop_endpoint(app)


# (case, block_job_id, spend_stopped, block_state, in the notice, NOT in the notice)
_CASES = [
    (_case_provisioning, None, False, "provisioning",
     ["check of ours is still queued", "squeue -u <your account there>", "session_spend does not count that block"],
     ["scancel 1"]),
    (_case_kicked, "101", False, "warm", ["scancel 101", "squeue -j 101", "at the latest"], ["scancel 100"]),
    (_case_presumed_up, "100", False, "warm", ["scancel 100", "possibly sooner (not verified)"], []),
    (_case_presumed_released, "100", None, "cold", ["presumed released", "squeue -j 100"], ["may still be running"]),
    (_case_restop_after_reconnect, "100", False, "warm", ["by an earlier stop", "scancel 100"], []),
    (_case_switch, "100", False, "warm", ["by an earlier switch", "scancel 100", "check of ours is still queued"],
     []),
    (_case_relaunched, "200", False, "warm", ["scancel 200"], ["100"]),
    (_case_max_blocks, "100", False, "warm", ["at least this one", "scancel 100"], []),
]


@pytest.mark.parametrize(("case", "job", "stopped", "state", "has", "lacks"), _CASES,
                         ids=[c[0].__name__.removeprefix("_case_") for c in _CASES])
async def test_the_stop_names_the_right_block_or_says_it_cannot(monkeypatch, case, job, stopped, state, has, lacks):
    app, res = await case(monkeypatch)
    assert res.status == "draining"  # never "down": nothing here can cancel the facility's block
    assert res.notice.startswith(_HEAD)
    assert res.block_job_id == job
    assert res.spend_stopped is stopped and res.spend_stopped is not True
    assert res.block_state == state  # its PRESUMED state — never "cold" while a block may still run
    for s in has:
        assert s in res.notice, (s, res.notice)
    for s in lacks:
        assert s not in res.notice, (s, res.notice)
    assert "don't call stop_endpoint again" in res.notice and "don't check from here" in res.notice
    assert ",," not in res.notice and len(res.notice) < 1400
    assert "compute" not in app.shapes and app.state.endpoint_id == MEP_UUID


# --- the scheduler, the mapped account, the bound --------------------------------------------------------------------


async def test_a_pbs_facility_gets_qdel_and_qstat(monkeypatch):
    app, _r = await _warm(monkeypatch, "77.pbs01", scheduler="pbs")
    res = await _stop_endpoint(app)
    assert res.block_job_id == "77.pbs01"
    assert "`qdel 77.pbs01`" in res.notice and "`qstat 77.pbs01`" in res.notice and "scancel" not in res.notice


async def test_an_unknown_job_names_the_mapped_account_for_squeue(monkeypatch):
    # the canary ran as the facility's mapped account (globus-labs: glabs-gc, not the user's login name)
    app, _r = await _attached(monkeypatch, canary=_ok(job=None, user="glabs-gc"))
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    res = await _stop_endpoint(app)
    assert res.block_job_id is None and res.spend_stopped is False and res.block_state == "warm"
    assert "its job id is unknown here" in res.notice and "`squeue -u glabs-gc`" in res.notice
    # the name prefix does not single out ours on a shared mapped account: say so, and what to check
    assert "`parsl.GlobusComputeEngine-…` (squeue shows `parsl.Gl`)" in res.notice
    assert "others on a shared account may show the same prefix" in res.notice and "start time and partition" in res.notice


async def test_the_latest_end_is_the_release_bound_as_a_clock_time(monkeypatch, noon):
    app, _r = await _warm(monkeypatch, "100")
    rt = app.shapes["compute"]
    bound = _last_activity(app, "compute", rt) + IDLE + _RELEASE_BOUND_S
    res = await _stop_endpoint(app)
    # the wall clock frozen at 12:00:30: the bound, ~11 min ahead, is 12:11 today — whatever the real hour
    assert "until 12:11 (in ~11 min) at the latest, possibly sooner (not verified)" in res.notice
    assert app.left_blocks[-1].until == pytest.approx(bound, abs=0.5)


async def test_the_bound_is_capped_at_the_walltime(monkeypatch):
    app, _r = await _warm(monkeypatch, "100")
    rt = app.shapes["compute"]
    rt.block_since -= 2 * 3600 - 120  # the 02:00:00 block has two minutes left: it ends before its idle window would
    await _stop_endpoint(app)
    assert app.left_blocks[-1].until == pytest.approx(rt.block_since + 7200, abs=0.5)


async def test_an_unknown_idle_window_bounds_the_block_by_its_walltime_alone(monkeypatch):
    # Anvil and Delta publish no idle window: the latest end is the block's walltime; with neither, it is unknown
    app, _r = await _warm(monkeypatch, "100")
    app.facility.max_idletime_s = None
    rt = app.shapes["compute"]
    since = rt.block_since
    res = await _stop_endpoint(app)
    assert app.left_blocks[-1].until == pytest.approx(since + 7200, abs=0.5)
    assert "(in ~120 min) at the latest (its walltime: the facility's idle window is unknown here)" in res.notice
    assert res.spend_stopped is False and app.left_blocks[-1].bound == "walltime"

    app2, _r2 = await _warm(monkeypatch, "100")
    app2.facility.max_idletime_s = None
    app2.shapes["compute"].user_endpoint_config.pop("walltime")
    res2 = await _stop_endpoint(app2)
    assert app2.left_blocks[-1].until is None and res2.spend_stopped is False and res2.block_state == "warm"
    assert "are both unknown" in res2.notice and "at the latest" not in res2.notice


# --- billing: banked to the bound, never twice ---------------------------------------------------------------------


async def test_a_stop_banks_the_block_once_and_a_later_confirmation_bills_fresh(monkeypatch, clock):
    # scoped down (review of a6de41e): a stop banks the block ONCE, to its bound; the block confirmed later — the same
    # job or not — bills from its own confirmation, never continued from the left entry; session_spend never falls
    t0 = clock[0]
    app, runner = await _attached(monkeypatch, canary=_now_ok(clock))
    app.charge_factor = 1.0
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    await _run_shell(app, "true")
    clock[0] = t0 + 10
    res = await _stop_endpoint(app)
    assert _billed_s(app) == pytest.approx(IDLE + _RELEASE_BOUND_S, abs=0.01)  # once, to the bound: an upper bound
    assert "session_spend counts it to " in res.notice and "(an upper bound" in res.notice
    assert "a block confirmed again after a stop counts from its new confirmation" in res.notice
    clock[0] = t0 + 20
    assert _billed_s(await _stop_and_total(app)) == pytest.approx(IDLE + _RELEASE_BOUND_S, abs=0.01)  # not again
    seen = [_billed_s(app)]
    clock[0] = t0 + 30
    runner._canary = _now_ok(clock)  # the SAME job answers the new confirmation (same binding: no reconnect)
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"
    rt = app.shapes["compute"]
    # it bills fresh from its own confirmation, never continued from the left entry; dated by its job, it keeps its age
    assert rt.warm_since == t0 + 30 and rt.block_since == t0
    assert app.left_blocks == []  # the entry named it; now it is the confirmed block again
    for t in (60, 300):
        clock[0] = t0 + t
        seen.append(_billed_s(app))
    clock[0] = t0 + 310
    await _stop_endpoint(app)
    seen.append(_billed_s(app))
    assert seen == sorted(seen)  # monotonic
    # the clock counts from the new confirmation (30..310), and the tail only past the job's mark (665..695): the
    # clock's overlap with the first bank counts twice — inside the stated upper bound, at most one bound over the union
    assert seen[-1] == pytest.approx((IDLE + _RELEASE_BOUND_S) + (310 - 30) + (30 + IDLE + _RELEASE_BOUND_S - 665),
                                     abs=0.01)
    assert seen[-1] <= (30 + IDLE + _RELEASE_BOUND_S) + (IDLE + _RELEASE_BOUND_S)


async def _stop_and_total(app):
    await _stop_endpoint(app)
    return app


async def test_a_switch_banks_the_old_block_once_and_left_entries_never_bill(monkeypatch):
    app, runner = await _warm(monkeypatch, "100")
    app.charge_factor = 1.0
    runner._canary = _TIMEOUT
    await _ensure_endpoint_up(app, shape="compute", partition="other", confirm_spend=True)
    after_switch = server._total_session_spend(app)
    res = await _stop_endpoint(app)  # job 100 resolved again from left_blocks — never billed a second time
    assert res.session_spend == pytest.approx(after_switch, abs=1e-6)


async def test_a_block_left_twice_is_billed_once(monkeypatch):
    # a replacement a check found (job 101, not adopted), then a partition switch, then a stop: job 101 is resolved by
    # both, and billed to its bound by the first only
    app, runner = await _warm(monkeypatch, "100")
    app.charge_factor = 1.0
    app.shapes["compute"].warm_confirmed_at -= 100
    runner._canary = _ok("101")
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    res = await _ensure_endpoint_up(app, shape="compute", partition="other")
    assert res.status == "needs_confirmation" and [b.job for b in app.left_blocks] == ["101"]
    switched = server._total_session_spend(app)
    stop = await _stop_endpoint(app)
    assert stop.block_job_id == "101" and stop.session_spend == pytest.approx(switched, abs=1e-6)


# --- the refusal, the detach, the post-stop probe -----------------------------------------------------------------


async def test_a_refused_stop_says_spend_did_not_stop(monkeypatch):
    app, runner = await _warm(monkeypatch, "100")
    runner.pending = True
    out = await _run_shell(app, "sleep 3000")
    assert out.phase == "running"
    res = await _stop_endpoint(app)
    assert res.status == "up" and res.spend_stopped is False and res.block_job_id == "100"
    assert app.left_blocks == []  # nothing let go


async def test_the_detach_is_down_for_our_state_only_and_says_the_block_bills_on(monkeypatch):
    app, _r = await _warm(monkeypatch, "100")
    res = await _teardown_endpoint(app)
    assert res.status == "down" and app.state.endpoint_id is None and app.shapes == {}  # our state cleared
    assert res.notice.startswith(_HEAD) and "nothing of ours to tear down" in res.notice
    assert res.block_job_id == "100" and res.spend_stopped is False and res.block_state == "warm"
    assert "scancel 100" in res.notice
    # a stop after the detach still names it — nothing is bound, but the block may still bill
    again = await _stop_endpoint(app)
    assert again.status == "draining" and again.block_job_id == "100" and again.spend_stopped is False
    assert "by an earlier detach" in again.notice


async def test_after_a_stop_a_free_probe_and_a_reconnect_send_nothing(monkeypatch):
    # settled (#178): the shape was dropped, so ensure_endpoint_up without confirm_spend answers needs_confirmation
    # BEFORE any canary, and a reconnect makes only web calls — neither can touch the block or start a new one
    app, runner = await _warm(monkeypatch, "100")
    await _stop_endpoint(app)
    canaries, commands, built = runner.canaries, len(runner.commands), app.built
    res = await _ensure_endpoint_up(app, shape="compute")
    assert res.status == "needs_confirmation"
    assert (await _reconnect(app)).phase == "needs_account"
    assert runner.canaries == canaries and len(runner.commands) == commands and app.built == built


async def test_a_bind_to_another_endpoint_forgets_the_left_blocks(monkeypatch):
    app, _r = await _warm(monkeypatch, "100")
    await _stop_endpoint(app)
    assert app.left_blocks and app.worker_user == "hpcbmep"
    other = FakeFacility()
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: other)
    monkeypatch.setattr(server.warmth, "_provision", _no_provision)
    await server._connect_facility(app, "globus1")
    assert app.left_blocks == [] and app.worker_user is None


async def _no_provision(*_a, **_k):
    return "provisioning"


# --- the SSH path: spend_stopped is True only for a confirmed cancel -------------------------------------------------


def _ssh_app(*, manager_up=True):
    f = FakeFacility()
    f.manager_up = manager_up
    app = AppCtx(facility=f, profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True}, runner=_FakeRunner("eid-1", _Res(0, "", "")))
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"}, warm_confirmed_at=1.0)
    return app


@pytest.mark.parametrize(("confirmed", "manager_up", "status", "stopped"), [
    (True, True, "down", True),        # the cancel was confirmed: the one result that may say spend stopped
    (False, True, "draining", None),   # sent, not confirmed: unknown
    (False, False, "down", None),      # the manager is offline (ORPHANED): nothing confirmed either way
])
async def test_ssh_spend_stopped(monkeypatch, confirmed, manager_up, status, stopped):
    async def release(a, eid, runner, **kw):
        return confirmed, "released 1" if confirmed else "released none"

    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", release)
    res = await _stop_endpoint(_ssh_app(manager_up=manager_up))
    assert res.status == status and res.spend_stopped is stopped


async def test_no_endpoint_says_nothing_about_spend():
    res = await _stop_endpoint(AppCtx(facility=FakeFacility(), profile=Profile()))
    assert res.status == "down" and res.spend_stopped is None and res.notice == "no endpoint was up"


# --- the 300 s still-allocating notice names the job ----------------------------------------------------------------


def test_the_allocating_notice_names_the_known_job_and_the_mapped_account():
    late = notices._allocating_notice("main", 480, facility_mep=True, job="1226", job_verified=True, scheduler="slurm",
                                      user="glabs-gc")
    assert "scheduler job 1226 (`scancel 1226` ends it)" in late and "squeue -u glabs-gc" in late
    assert "scancel 1226)" in late  # the RUNNING-but-silent case names it too
    pbs = notices._allocating_notice("q", 480, facility_mep=True, job="9.pbs", job_verified=True, scheduler="pbs")
    assert "qdel 9.pbs" in pbs and "qstat -u <the account the facility maps you to>" in pbs
    assert "scheduler job" not in notices._allocating_notice("main", 480, facility_mep=True)  # unknown: unnamed


async def test_a_block_that_stops_answering_is_named_after_five_minutes(monkeypatch):
    app, runner = await _warm(monkeypatch, "1226")
    rt = app.shapes["compute"]
    rt.warm_confirmed_at -= 60  # past the TTL
    runner._canary = CanaryResult(ok=False, error="TaskExecutionFailed: the worker was lost")  # identity kept
    assert (await _ensure_endpoint_up(app, shape="compute")).status == "provisioning"
    rt.provisioning_since -= 400
    res = await _ensure_endpoint_up(app, shape="compute")
    assert res.status == "provisioning" and "scheduler job 1226" in res.notice and "squeue -u hpcbmep" in res.notice


# --- the canary's mapped account -----------------------------------------------------------------------------------


def test_the_canary_reports_the_account_it_ran_as():
    from hpc_bridge.runner import _CANARY_CMD, _parse_canary_user

    assert "id -un" in _CANARY_CMD
    out = "HPCB_CANARY\n3.12.3 0.3.9 c1\nHPCB_USER glabs-gc\nHPCB_JOB 1226\n"
    assert _parse_canary_user(out) == "glabs-gc"
    assert _parse_canary_user("HPCB_USER \n") is None            # `id -un` printed nothing
    assert _parse_canary_user("HPCB_USER -oProxyCommand=x\n") is None  # never an option-looking string
    assert _parse_canary_user("HPCB_USER a b\n") is None


def test_the_canary_command_prints_the_account_under_bash():
    import getpass

    from hpc_bridge.runner import _CANARY_CMD, _escape_for_shellfunction, _parse_canary_user

    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("no bash")
    out = subprocess.run([bash, "-c", _escape_for_shellfunction(_CANARY_CMD).format()], capture_output=True,
                         text=True, timeout=30).stdout
    assert _parse_canary_user(out) == getpass.getuser()


async def test_the_runner_hands_the_account_back_and_says_when_a_check_is_pending():
    pytest.importorskip("globus_compute_sdk")
    from hpc_bridge.runner import GlobusRunner
    from tests.test_runner import _CanaryFuture, _QueueExecutor, _ShellRes

    queued = _CanaryFuture(pending=True)
    ex = _QueueExecutor(queued)
    r = GlobusRunner("eid", executor_factory=lambda: ex)
    assert r.canary_pending is False
    assert (await r.canary(timeout=0.05)).error == "timeout"
    assert r.canary_pending is True  # still queued on the endpoint: on a facility, a block is started or kept for it
    queued.resolve(_ShellRes("HPCB_CANARY\nHPCB_USER hpcbmep\nHPCB_JOB 7\n"))
    assert r.canary_pending is False
    res = await r.canary(timeout=0.1)
    assert res.ok and res.worker_user == "hpcbmep" and res.worker_job == "7"
    r.close()
    assert r.canary_pending is False


def test_a_left_block_is_up_released_or_a_queued_check():
    now = time.monotonic()
    up = LeftBlock(endpoint_id="e", job="1", until=now + 60, left_at=now, how="stop")
    gone = LeftBlock(endpoint_id="e", job="2", until=now - 60, left_at=now, how="stop")
    check = LeftBlock(endpoint_id="e", job=None, until=None, left_at=now, how="stop", kind="check")
    assert notices._left_spend_stopped([gone], now) is None and notices._left_block_state([gone], now) == "cold"
    assert notices._left_spend_stopped([gone, up], now) is False and notices._left_block_state([gone, up], now) == "warm"
    assert notices._left_spend_stopped([check], now) is False
    assert notices._left_block_state([check], now) == "provisioning"
    assert notices._left_job([gone, up], now) == "1" and notices._left_job([gone], now) == "2"
    assert notices._left_spend_stopped([], now) is None and notices._left_job([], now) is None
    assert warmth._check_queued(ShapeRuntime(user_endpoint_config={}, last_canary=_TIMEOUT))
    assert not warmth._check_queued(ShapeRuntime(user_endpoint_config={}, last_canary=_TIMEOUT, warm_confirmed_at=now))


# --- review of e0010c0: fake-clock timelines, the busy bound, a second teardown, stale ids --------------------------


@pytest.fixture
def clock(monkeypatch):
    """A fake time.monotonic the test moves by hand."""
    c = [100000.0]
    monkeypatch.setattr(time, "monotonic", lambda: c[0])
    return c


def _now_ok(clock, job="100"):
    return CanaryResult(ok=True, worker_host="c1", worker_python="3.12.3", worker_dill="0.3.9", worker_job=job,
                        worker_user="hpcbmep", answered_at=clock[0])


def _billed_s(app) -> float:
    return server._total_session_spend(app) * 3600  # one node, charge factor 1: seconds billed


async def test_a_reconnect_under_a_running_task_bounds_the_block_by_its_walltime(monkeypatch, clock):
    # review MUST-FIX 2: connect does not refuse while a task runs, and the idle bound ignored it — "until … (in ~11
    # min) at the latest" while the task could run its 1780 s, billing short, and later stops said "presumed released"
    t0 = clock[0]
    app, runner = await _attached(monkeypatch, canary=_now_ok(clock))
    app.charge_factor = 1.0
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    runner.pending = True
    assert (await _run_shell(app, "sleep 3000")).phase == "running"
    since = app.shapes["compute"].block_since
    await _reconnect(app)
    left = app.left_blocks[-1]
    assert left.how == "reconnect" and left.bound == "busy" and left.until == pytest.approx(since + 7200)
    assert _billed_s(app) == pytest.approx(0, abs=0.01)  # this binding: the reconnect reported the old one's spend
    clock[0] = t0 + 1200  # well past the idle bound, still inside the walltime: the task may be running
    res = await _stop_endpoint(app)
    assert res.spend_stopped is False and res.block_state == "warm" and "presumed released" not in res.notice
    assert "(its walltime: work of ours may still be running on it)" in res.notice
    assert "by an earlier reconnect" in res.notice


async def test_a_reconnect_says_the_block_was_not_cancelled_and_names_it(monkeypatch):
    app, _r = await _warm(monkeypatch, "100")
    res = await _reconnect(app)
    assert "was not cancelled" in res.notice and "scheduler job 100" in res.notice and "`scancel 100`" in res.notice
    assert "shapes were released" not in res.notice and "stop_endpoint names it" in res.notice


async def test_a_second_teardown_after_a_detach_still_names_the_block(monkeypatch):
    # review MUST-FIX 3: it answered `down`, "no endpoint was up", spend_stopped None — and the revocation grader
    # counted that as a release
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "agentic" / "harness"))
    from invariants import spend_released

    app, _r = await _warm(monkeypatch, "100")
    first = await _teardown_endpoint(app)
    again = await _teardown_endpoint(app)
    assert again.status == "down" and again.notice.startswith(_HEAD) and "no endpoint was up" not in again.notice
    assert again.block_job_id == "100" and again.spend_stopped is False and "by an earlier detach" in again.notice
    assert again.session_spend == pytest.approx(0)  # the detach reported the binding's spend; nothing billed twice
    for res in (first, again, await _stop_endpoint(app)):
        assert not spend_released(res.model_dump())


async def test_a_queued_check_is_kept_dated_and_a_block_expires_past_its_walltime(monkeypatch, noon):
    # a queued check never expires (the queue wait is unbounded and the facility keeps starting blocks while it
    # waits): it stays, dated and hedged, and stop and teardown never fall back to a bare "no endpoint was up"
    app, _r = await _attached(monkeypatch, canary=_TIMEOUT)
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    await _teardown_endpoint(app)  # detach with our check queued
    (b,) = app.left_blocks
    assert b.kind == "check" and b.ends_by is None
    b.left_at -= 3 * 86400  # three days on
    for res in (await _stop_endpoint(app), await _teardown_endpoint(app)):
        assert res.notice.startswith(_HEAD) and "no endpoint was up" not in res.notice
        assert "A check of ours was still queued there (partition 'main' at" in res.notice
        assert "may have run since" in res.notice and "session_spend does not count it" in res.notice
        when = time.strftime("%b %d", time.localtime(noon - 3 * 86400))
        assert when in res.notice and res.spend_stopped is False and res.block_state == "provisioning"
    # a confirmed block past its walltime's end cannot still run: dropped
    app2, _r2 = await _warm(monkeypatch, "100")
    since = app2.shapes["compute"].block_since
    await _teardown_endpoint(app2)
    assert app2.left_blocks[0].ends_by == pytest.approx(since + 7200)  # its 02:00:00 walltime's end
    app2.left_blocks[0].ends_by = time.monotonic() - 1
    res = await _stop_endpoint(app2)
    assert res.status == "down" and res.notice == "no endpoint was up" and app2.left_blocks == []


def test_a_clock_time_off_today_carries_its_date(noon):
    now = time.monotonic()
    assert notices._clock_time(now + 60, now, rel=False) == "12:01"   # today: the time alone
    two_days = notices._clock_time(now - 2 * 86400, now)
    assert two_days == time.strftime("%b %d %H:%M", time.localtime(noon - 2 * 86400)) + " (~2880 min ago)"


async def test_a_check_that_answered_after_its_wait_names_its_block(monkeypatch):
    # review SHOULD-FIX 6: the answer sat in the runner's resolved canary while the stop said "still queued, job id
    # unknown"
    pytest.importorskip("globus_compute_sdk")
    from hpc_bridge.runner import GlobusRunner
    from tests.test_runner import _CanaryFuture, _QueueExecutor, _ShellRes

    fac = MEPFacility.from_entry(_entry(), client_factory=lambda: _Status("online"))
    fac.max_idletime_s = IDLE
    app = AppCtx(facility=fac, profile=Profile())
    app.charge_factor = 1.0
    queued = _CanaryFuture(pending=True)
    app.runner_factory = lambda eid, **kw: GlobusRunner(eid, executor_factory=lambda: _QueueExecutor(queued))
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([_entry()]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: app.facility)
    monkeypatch.setattr(warmth, "canary_timeout_s", lambda: 0.05)
    await server._connect_facility(app, "globus1")
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "provisioning"
    queued.resolve(_ShellRes("HPCB_CANARY\n3.12 0.3.9 c1\nHPCB_USER glabs-gc\nHPCB_JOB 4242\n"))  # the block came up
    res = await _stop_endpoint(app)
    assert res.block_job_id == "4242" and res.spend_stopped is False and res.block_state == "warm"
    assert "scancel 4242" in res.notice and "still queued" not in res.notice and app.worker_user == "glabs-gc"
    assert res.session_spend > 0  # it bills from its answer to its bound: an upper bound, never 0


async def test_the_allocating_notice_never_names_another_partitions_job(monkeypatch):
    # review SHOULD-FIX 4: after a switch the five-minute notice named the OLD partition's job and told the user to
    # `scancel` it as "the RUNNING job that never answers"
    app, runner = await _warm(monkeypatch, "100")
    runner._canary = _TIMEOUT
    await _ensure_endpoint_up(app, shape="compute", partition="other", confirm_spend=True)
    app.shapes["compute"].provisioning_since -= 400
    res = await _ensure_endpoint_up(app, shape="compute")
    assert res.status == "provisioning" and "Still allocating" in res.notice
    assert "100" not in res.notice and "scancel <job id>" in res.notice


def test_an_unverified_job_is_named_but_never_given_a_cancel():
    late = notices._allocating_notice("main", 480, facility_mep=True, job="1226", job_verified=False)
    assert "last confirmed scheduler job 1226" in late and "may be gone" in late
    assert "scancel 1226" not in late and "scancel <job id>" in late


async def test_a_presumed_gone_job_is_not_named_by_the_allocating_notice(monkeypatch):
    app, runner = await _warm(monkeypatch, "100")
    _age(app.shapes["compute"], IDLE + _RELEASE_BOUND_S + 30)  # past its latest end: presumed gone
    runner._canary = _TIMEOUT
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "needs_confirmation"
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "provisioning"
    app.shapes["compute"].provisioning_since -= 400
    res = await _ensure_endpoint_up(app, shape="compute")
    assert "Still allocating" in res.notice and "100" not in res.notice


@pytest.mark.parametrize("gone", [True, False])
async def test_a_block_a_stop_left_is_named_while_it_may_bill_and_never_given_a_cancel(monkeypatch, gone):
    # the same partition: named as unverified while it may still bill; not at all once presumed released
    app, runner = await _warm(monkeypatch, "100")
    await _stop_endpoint(app)
    (left,) = app.left_blocks
    left.until = time.monotonic() + (-10 if gone else 60)
    runner._canary = _TIMEOUT
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "provisioning"
    app.shapes["compute"].provisioning_since -= 400
    res = await _ensure_endpoint_up(app, shape="compute")
    assert "Still allocating" in res.notice and "scancel 100" not in res.notice
    assert ("last confirmed scheduler job 100" in res.notice) is (not gone)


async def test_a_switch_lets_go_of_the_old_runner_so_a_stop_cannot_file_its_answer_under_the_new_partition(
        monkeypatch, clock):
    # review of a6de41e, X: after a timed-out check, the old runner's late answer (job 101, on main) sat unread; an
    # unconfirmed switch returned before any rebuild, and the stop read that answer again — filed under "other", and
    # 645 s of it unbilled. Now the switch records 101 under main, banks it once, and closes the old runner.
    t0 = clock[0]
    app, runner = await _attached(monkeypatch, canary=_now_ok(clock, "100"))
    app.charge_factor = 1.0
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    await _run_shell(app, "true")
    clock[0] = t0 + 100
    runner._canary, runner.canary_pending = _TIMEOUT, True
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"  # the check timed out: a reap, kicked
    clock[0] = t0 + 110
    late = _now_ok(clock, "101")
    runner.canary_pending, runner.canary_answer = False, lambda: late
    res = await _ensure_endpoint_up(app, shape="compute", partition="other")  # unconfirmed: no rebuild
    assert res.status == "needs_confirmation"
    rt = app.shapes["compute"]
    assert rt.runner is None and runner.closed and rt.last_canary is None and rt.reap_kicked is False
    assert [(b.job, b.partition, b.how) for b in app.left_blocks] == [("101", "main", "switch")]
    switched = _billed_s(app)
    # 100 to the reap that found it gone (t0+100); 101 once, from its answer to its bound
    assert switched == pytest.approx(100 + IDLE + _RELEASE_BOUND_S, abs=0.01)
    clock[0] = t0 + 120
    stop = await _stop_endpoint(app)
    assert [(b.job, b.partition) for b in app.left_blocks] == [("101", "main")]  # never re-read under "other"
    assert stop.block_job_id == "101" and "by an earlier switch" in stop.notice
    assert _billed_s(app) == pytest.approx(switched, abs=0.01)  # left entries never bill
    # a block confirmed on "other" takes over only entries of "other": 101, on main, is still named
    clock[0] = t0 + 130
    runner._canary = _now_ok(clock, "300")
    del runner.canary_answer
    await _ensure_endpoint_up(app, shape="compute", partition="other", confirm_spend=True)
    assert [b.job for b in app.left_blocks] == ["101"]


async def test_a_block_only_a_check_saw_is_never_named_by_the_allocating_notice(monkeypatch):
    # only a CONFIRMED block's job is named there: a replacement a check found (job 101, never confirmed) is not, even
    # on the same partition and account
    app, runner = await _warm(monkeypatch, "100")
    app.shapes["compute"].warm_confirmed_at -= 100
    runner._canary = _ok("101")
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"  # 101 found by the check
    await _stop_endpoint(app)
    (left,) = app.left_blocks
    assert left.job == "101" and left.verified is False and left.partition == "main"
    runner._canary = _TIMEOUT
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "provisioning"
    app.shapes["compute"].provisioning_since -= 400
    res = await _ensure_endpoint_up(app, shape="compute")
    assert "Still allocating" in res.notice and "101" not in res.notice


async def test_a_later_block_with_the_same_job_takes_its_entry_off_the_list(monkeypatch):
    # with more than one block, only the same job does (a sibling's entry stays named)
    app, runner = await _warm(monkeypatch, "100", max_blocks=2)
    await _stop_endpoint(app)
    assert [b.job for b in app.left_blocks] == ["100"]
    runner._canary = _ok("100")
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"
    assert app.left_blocks == []
    await _stop_endpoint(app)
    runner._canary = _ok("101")
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"
    assert [b.job for b in app.left_blocks] == ["100"]  # a sibling may still run: still named


async def test_the_real_results_grade_as_the_scenario_and_the_revocation_grader_expect(monkeypatch):
    # review item 9: no stored bundle holds a MEP stop or detach — grade what the server actually returns
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "agentic"
    sys.path[:0] = [str(root / "harness"), str(root / "scenarios")]
    import fake_mep_stop_names_block as sc
    from invariants import ToolCall, Trace, revocation_honoured, stop_is_honest

    app, _r = await _warm(monkeypatch, "1226")
    run = ToolCall.of("mcp__endpoint__run_shell", {"shape": "compute", "command": "echo JOB=$SLURM_JOB_ID"},
                      {"phase": "complete", "stdout": "JOB=1226\n"})
    stop = ToolCall.of("mcp__endpoint__stop_endpoint", {}, (await _stop_endpoint(app)).model_dump())
    detach = ToolCall.of("mcp__endpoint__teardown_endpoint", {}, (await _teardown_endpoint(app)).model_dump())
    t = Trace([run, stop], ["Job 1226 bills until 21:35 at the latest; end it with `scancel 1226`."])
    for g in (sc.mep_stop_names_block, sc.no_cancel_claimed, sc.cancel_command_given, sc.no_probe_after_stop,
              stop_is_honest):
        assert g(t).ok, (g.__name__, g(t).detail)
    for last in (stop, detach):
        rev = Trace([ToolCall.of("mcp__endpoint__connect_facility", {}, {"phase": "needs_account"}),
                     ToolCall.of("mcp__endpoint__ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
                                 {"status": "up"}),
                     ToolCall.of("user_interjection", {"text": "stop"}), last])
        assert not revocation_honoured(rev).ok, last.name


# --- review of 241188b: what session_spend holds, the per-job mark, queued checks per partition ---------------------


async def test_only_this_calls_own_block_is_said_to_be_in_session_spend(monkeypatch, clock):
    # review MUST-FIX 1: a new binding starts session_spend at 0, so a block an EARLIER stop or detach left is not in
    # it any more — the notice said "session_spend counts it to …" beside a 0.0
    app, _r = await _attached(monkeypatch, canary=_now_ok(clock))
    app.charge_factor = 1.0
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    await _run_shell(app, "true")
    clock[0] += 10
    detach = await _teardown_endpoint(app)
    assert detach.session_spend > 0 and "session_spend counts it to" in detach.notice
    clock[0] += 10
    for res in (await _stop_endpoint(app), await _teardown_endpoint(app)):
        assert res.session_spend == 0 and "session_spend counts it" not in res.notice
        assert "A block left earlier was counted in the spend reported when it was left." in res.notice
    # ...and after a reconnect to the same endpoint
    app2, _r2 = await _attached(monkeypatch, canary=_now_ok(clock))
    app2.charge_factor = 1.0
    await _ensure_endpoint_up(app2, shape="compute", confirm_spend=True)
    await _run_shell(app2, "true")
    await _stop_endpoint(app2)
    await _reconnect(app2)
    res = await _stop_endpoint(app2)
    assert res.session_spend == 0 and "session_spend counts it" not in res.notice
    assert "counted in the spend reported when it was left" in res.notice


async def _cycles(monkeypatch, clock, *, idle, stops):
    """Confirm and run at t0, then: stop at 100k+10 and re-confirm the SAME job at 100k+50, k = 0..stops-2, and a last
    stop. Returns the seconds billed after every stop, and the latest end the last stop named."""
    t0 = clock[0]
    app, runner = await _attached(monkeypatch, canary=_now_ok(clock))
    app.facility.max_idletime_s = idle
    app.charge_factor = 1.0
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    await _run_shell(app, "true")
    seen = []
    for k in range(stops):
        clock[0] = t0 + 100 * k + 10
        await _stop_endpoint(app)
        seen.append(_billed_s(app))
        if k < stops - 1:
            clock[0] = t0 + 100 * k + 50
            runner._canary = _now_ok(clock)
            assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "up"
    return seen, app.left_blocks[-1].until - t0


async def test_stop_and_reconfirm_cycles_never_recount_a_tail(monkeypatch, clock):
    # review SHOULD-FIX 2: each stop used to bank its block's whole remaining tail again — N cycles counted the same
    # span up to N+1 times. The per-job mark lets a tail add only what no earlier bank of that job counted.
    seen, last_end = await _cycles(monkeypatch, clock, idle=IDLE, stops=6)  # five cycles, a known idle window
    bound = IDLE + _RELEASE_BOUND_S
    assert seen == sorted(seen)
    union = last_end  # the block: from t0 to the latest end the last stop named
    assert seen[-1] <= union + bound
    # each cycle: its own clock (60 s) and the tail past the mark (50 s, then 100 s: the bound moves 100 s a cycle)
    assert seen[-1] == pytest.approx(bound + (60 + 50) + 4 * (60 + 100), abs=0.01)


async def test_quick_stops_with_no_idle_window_count_the_walltime_once(monkeypatch, clock):
    # Anvil- and Delta-like (no idle window published): every stop's tail runs to a walltime end — 4 stops in 310 s
    # reported 8.0 node-h before the mark; now the walltime is counted once, plus each cycle's own clock. The job keeps
    # its age when it answers again (dated by its job), so its walltime end does not move and no tail is added past it.
    seen, last_end = await _cycles(monkeypatch, clock, idle=None, stops=4)
    assert seen == sorted(seen)
    assert seen[0] == pytest.approx(7200, abs=0.01)  # the first stop: to its walltime's end
    assert seen[-1] == pytest.approx(7200 + 3 * 60, abs=0.01)
    assert seen[-1] <= last_end + 7200 and seen[-1] / 3600 < 2.2  # not 8 node-h


async def test_a_bind_elsewhere_drops_the_marks(monkeypatch, clock):
    app, _r = await _attached(monkeypatch, canary=_now_ok(clock))
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    await _stop_endpoint(app)
    assert list(app.billed_to) == [(MEP_UUID, "100")]
    await _reconnect(app)
    assert list(app.billed_to) == [(MEP_UUID, "100")]  # the same endpoint: kept
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: FakeFacility())
    monkeypatch.setattr(server.warmth, "_provision", _no_provision)
    await server._connect_facility(app, "globus1")
    assert app.billed_to == {}


async def test_queued_checks_are_kept_per_partition_and_account(monkeypatch, clock):
    # review SHOULD-FIX 3: every queued check on an endpoint collapsed into one entry, so checks on other partitions
    # were no longer named. Now the most recent per (partition, account) is kept, and the notice names each partition.
    app, runner = await _attached(monkeypatch, canary=_TIMEOUT)
    runner.canary_pending = True
    for part in ("p0", "p1", "p2", "p1"):  # p1 twice: one entry, the newer
        clock[0] += 60
        await _ensure_endpoint_up(app, shape="compute", partition=part, confirm_spend=True)
    clock[0] += 60
    res = await _stop_endpoint(app)
    assert [(b.kind, b.partition) for b in app.left_blocks] == [("check", "p0"), ("check", "p2"), ("check", "p1")]
    assert "A check of ours is still queued there (partition 'p1')" in res.notice  # this call's own
    assert "Check of ours" not in res.notice and "Checks of ours were still queued there (partition 'p2' at" in res.notice
    assert "partition 'p0' at" in res.notice and "session_spend does not count" in res.notice
    # many partitions: each kept, the notice names the newest few and counts the rest
    for k in range(10):
        clock[0] += 60
        await _ensure_endpoint_up(app, shape="compute", partition=f"q{k}", confirm_spend=True)
    res = await _stop_endpoint(app)
    assert len([b for b in app.left_blocks if b.kind == "check"]) == 13
    assert f", and {12 - notices._CHECKS_NAMED} more)" in res.notice


async def test_a_queued_check_beside_a_block_is_said(monkeypatch, clock):
    # a presumed block that may still be up, and our re-confirmation's check still queued: both, in one entry
    t0 = clock[0]
    app, runner = await _attached(monkeypatch, canary=_now_ok(clock))
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    await _run_shell(app, "true")
    clock[0] = t0 + IDLE + _IDLE_GRACE_S + 5  # presumed released, still inside its bound
    assert (await _run_shell(app, "true")).phase == "needs_confirmation"
    runner._canary = _TIMEOUT
    assert (await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)).status == "provisioning"
    res = await _stop_endpoint(app)
    assert res.block_job_id == "100" and app.left_blocks[-1].check_queued
    assert "A check of ours is also still queued there" in res.notice


async def test_a_confirmed_block_presumed_released_is_banked_to_its_bound_not_to_now(monkeypatch, clock):
    t0 = clock[0]
    app, _r = await _attached(monkeypatch, canary=_now_ok(clock))
    app.charge_factor = 1.0
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    await _run_shell(app, "true")
    clock[0] = t0 + 2000  # long past its latest end (665 s)
    res = await _stop_endpoint(app)
    assert _billed_s(app) == pytest.approx(IDLE + _RELEASE_BOUND_S, abs=0.01)
    assert res.spend_stopped is None and "presumed released" in res.notice


async def test_a_new_login_forgets_the_mapped_account(monkeypatch):
    app, _r = await _warm(monkeypatch, "100")
    assert app.worker_user == "hpcbmep"
    warmth._forget_identity_verdicts(app)
    assert app.worker_user is None


async def test_a_left_entry_of_another_endpoint_is_never_named(monkeypatch):
    # defence in depth: connect drops other endpoints' entries, and the stop reads only its own endpoint's
    app, _r = await _warm(monkeypatch, "100")
    app.left_blocks.append(LeftBlock(endpoint_id="other-eid", job="999", until=time.monotonic() + 600,
                                     left_at=time.monotonic(), how="stop"))
    res = await _stop_endpoint(app)
    assert res.block_job_id == "100" and "999" not in res.notice


def test_left_entries_dedupe_by_job_and_by_partition():
    from hpc_bridge.warmth import _remember_left

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    now = time.monotonic()

    def lb(job, part, kind="block", eid="e"):
        return LeftBlock(endpoint_id=eid, job=job, until=None, left_at=now, how="stop", kind=kind, partition=part)

    for b in (lb("1", "a"), lb("1", "b"), lb(None, "a"), lb(None, "a"), lb(None, "a", "check"), lb(None, "b", "check"),
              lb(None, "a", "check"), lb("1", "a", eid="f")):
        _remember_left(app, b)
    assert [(b.endpoint_id, b.job, b.partition, b.kind) for b in app.left_blocks] == [
        ("e", "1", "b", "block"), ("e", None, "a", "block"), ("e", None, "b", "check"), ("e", None, "a", "check"),
        ("f", "1", "a", "block")]

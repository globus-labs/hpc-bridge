"""No facility bound (live 2026-10-10, Hermes on the fake cluster): a run_shell made before any connect_facility — or
after one that only PROPOSED a config — stood up a local `hpc-bridge-dev` endpoint on the MCP host (the unpinned
server's LocalFacility) and answered with the endpoint CLI's internals ("FileNotFoundError: …/hpc-bridge-dev/
endpoint.json", "configure failed: Traceback …").

What changed: the server's boot path never stands up a local endpoint any more. With nothing pinned or connected —
or a pin that failed at boot — every tool that would stand up or use an endpoint refuses with login_shell's
"No facility connected" guidance and starts nothing. Local dev is HPC_BRIDGE_ENDPOINT_ID (an endpoint you run
yourself), on every OS: the server dispatches to it instead of provisioning. Before, an unpinned server on Linux with
no HPC_BRIDGE_ENDPOINT_ID provisioned `hpc-bridge-dev`; now it refuses. A facility bound by connect_facility is
unchanged."""
from __future__ import annotations

import json
import re

import pytest

from hpc_bridge import binding, config, connect, preauth
from hpc_bridge.endpoint import EndpointCLI
from hpc_bridge.facility.local import LocalFacility
from hpc_bridge.facility.remote import NeedsPreauth
from hpc_bridge.lifecycle import EndpointState
from hpc_bridge.models import FacilityDetails
from hpc_bridge.notices import _no_facility_notice
from hpc_bridge.profile import Profile
from hpc_bridge.server import (
    AppCtx,
    _complete_preauth,
    _connect_facility,
    _ensure_endpoint_up,
    _login_shell,
    _reset_session,
    _run_shell,
    _stop_endpoint,
    _teardown_endpoint,
    lifespan,
    mcp,
)
from tests.fakes import FakeCatalog, FakeFacility, fake_entry, fake_mep_entry
from tests.test_catalog_flow import MYBALANCE, _FakeRunner, _Res


class _EndpointCLIRecorder:
    """Stands in for the `globus-compute-endpoint` subprocess: records each invocation and behaves like a working CLI
    (configure makes the endpoint dir, start writes endpoint.json), so a local stand-up that is ATTEMPTED succeeds."""

    def __init__(self, user_dir):
        self.user_dir = user_dir
        self.calls: list[tuple[str, ...]] = []

    async def __call__(self, *args: str) -> tuple[int, str, str]:
        self.calls.append(args)
        if args[0] == "configure":
            (self.user_dir / args[-1]).mkdir(parents=True, exist_ok=True)
        elif args[0] == "start":
            (self.user_dir / args[1] / "endpoint.json").write_text(json.dumps({"endpoint_id": "local-eid"}))
        return 0, "", ""


def _no_executor(*_a, **_k):
    raise AssertionError("no Executor may be built while no facility is bound")


def _local_app(monkeypatch, tmp_path, fac: LocalFacility, **ctx) -> tuple[AppCtx, _EndpointCLIRecorder]:
    cli = _EndpointCLIRecorder(tmp_path)
    fac.cli = EndpointCLI(user_dir=tmp_path, runner=cli)

    async def _online(_eid):
        return True

    monkeypatch.setattr(fac, "manager_online", _online)  # never the Globus web service in the unit tier
    return AppCtx(facility=fac, profile=Profile(), **ctx), cli


@pytest.fixture
def unbound(monkeypatch, tmp_path):
    """The app the server boots with nothing pinned: the implicit local default. Its endpoint CLI is recorded and
    `provision` is a tripwire, so a refused call that still reached either fails the test."""
    for v in ("HPC_BRIDGE_ENDPOINT_ID", "HPC_BRIDGE_MACHINE", "HPC_BRIDGE_FACILITY"):
        monkeypatch.delenv(v, raising=False)
    fac = binding._default_local_facility()
    provisions: list[object] = []

    async def _tripwire(profile):
        provisions.append(profile)
        raise AssertionError("no local endpoint may be stood up while no facility is bound")

    monkeypatch.setattr(fac, "provision", _tripwire)
    app, cli = _local_app(monkeypatch, tmp_path, fac)
    app.runner_factory = _no_executor
    return app, cli, provisions


def _assert_nothing_started(app, cli, provisions):
    assert cli.calls == [] and provisions == []  # no configure/start, no provision attempted
    assert app.shapes == {} and app.tasks == {}  # no shape runtime, no Executor, no task
    assert app.state.endpoint_id is None


# --- which facility is "nothing bound" -------------------------------------------------------------------------------


async def test_unpinned_server_holds_the_implicit_local_default(monkeypatch):
    for v in ("HPC_BRIDGE_ENDPOINT_ID", "HPC_BRIDGE_MACHINE", "HPC_BRIDGE_FACILITY"):
        monkeypatch.delenv(v, raising=False)
    fac = await binding.make_facility()
    assert isinstance(fac, LocalFacility) and fac.implicit is True
    async with lifespan(mcp) as app:
        assert app.facility.implicit is True and app.state.endpoint_id is None


async def test_a_failed_pin_boots_to_the_implicit_default_too(monkeypatch, capsys):
    monkeypatch.delenv("HPC_BRIDGE_ENDPOINT_ID", raising=False)
    monkeypatch.delenv("HPC_BRIDGE_MACHINE", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_FACILITY", "anvil")  # removed variable -> make_facility raises -> boot fallback
    async with lifespan(mcp) as app:
        assert app.facility.implicit is True
    assert "starting unbound" in capsys.readouterr().err


async def _assert_boots_unbound(capsys):
    async with lifespan(mcp) as app:
        # "starting unbound" is true: no facility AND no endpoint — HPC_BRIDGE_ENDPOINT_ID is not dispatched to
        assert app.facility.implicit is True and app.state.endpoint_id is None
        assert (await _run_shell(app, "hostname")).notice == _no_facility_notice(app)
        assert (await _ensure_endpoint_up(app, confirm_spend=True)).notice == _no_facility_notice(app)
        assert (await _stop_endpoint(app)).notice == "no endpoint was up"  # nothing to release or guess at
        assert (await _teardown_endpoint(app)).notice == "no endpoint was up"
        assert app.shapes == {} and app.tasks == {}
    assert "starting unbound" in capsys.readouterr().err


async def test_a_failed_pin_with_an_endpoint_id_still_boots_unbound(monkeypatch, capsys):
    # A pin that fails at boot (here: not in the registry) used to fall back to LOCAL DEV when HPC_BRIDGE_ENDPOINT_ID
    # was also set — dispatching to that UUID with a client-expanded scratch root, while the boot said "unbound".
    monkeypatch.delenv("HPC_BRIDGE_FACILITY", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "nosuch")
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_ID", "some-remote-eid")
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([]))
    await _assert_boots_unbound(capsys)


async def test_a_refused_mep_conflict_does_not_dispatch_to_the_refused_uuid(monkeypatch, capsys):
    # _catalog_facility REFUSES a MEP pin whose HPC_BRIDGE_ENDPOINT_ID conflicts with the entry's UUID (the ambiguity
    # is not guessed at). The fallback must not then dispatch to that very UUID as local dev.
    monkeypatch.delenv("HPC_BRIDGE_FACILITY", raising=False)
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "globus1")
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_ID", "11111111-2222-3333-4444-555555555555")
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_mep_entry()]))
    with pytest.raises(RuntimeError, match="conflicts"):
        await binding.make_facility()
    await _assert_boots_unbound(capsys)


async def test_endpoint_id_is_the_local_dev_opt_in(monkeypatch):
    # HPC_BRIDGE_ENDPOINT_ID is the ONLY local-dev path, on every OS (an endpoint you run yourself): it makes the
    # local facility CHOSEN, and lifespan seeds the endpoint it dispatches to.
    for v in ("HPC_BRIDGE_MACHINE", "HPC_BRIDGE_FACILITY"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_ID", "byo-uuid")
    fac = await binding.make_facility()
    assert isinstance(fac, LocalFacility) and fac.implicit is False
    async with lifespan(mcp) as app:
        assert app.facility.implicit is False and app.state.endpoint_id == "byo-uuid"


def test_a_directly_constructed_local_facility_is_explicit():
    # The mark is set by the boot path, never inferred from the type: tests and the integration tier build one directly
    assert LocalFacility(cli=None).implicit is False


# --- every path that would stand up or use an endpoint refuses, starting nothing ----------------------------------


@pytest.mark.parametrize("call", [
    pytest.param(lambda app: _run_shell(app, "hostname"), id="run_shell-compute"),
    pytest.param(lambda app: _run_shell(app, "hostname", shape="login"), id="run_shell-login"),
    pytest.param(lambda app: _reset_session(app), id="reset_session-compute"),
    pytest.param(lambda app: _reset_session(app, shape="login"), id="reset_session-login"),
])
async def test_shell_tools_refuse_with_no_facility(unbound, call):
    app, cli, provisions = unbound
    out = await call(app)
    assert out.phase == "failed" and out.block_state == "cold" and out.exit_code is None
    assert out.notice == _no_facility_notice(app)
    assert out.notice.startswith("No facility connected.") and "connect_facility(facility, ssh_host=…) FIRST" in out.notice
    assert "Nothing was started" in out.notice and "hpc-bridge error" not in out.notice
    _assert_nothing_started(app, cli, provisions)


@pytest.mark.parametrize("kw", [
    pytest.param({}, id="compute"),  # used to answer needs_confirmation — a spend gate for a facility that isn't there
    pytest.param({"confirm_spend": True}, id="compute-confirmed"),  # used to stand up the local endpoint
    pytest.param({"shape": "login"}, id="login"),
    pytest.param({"partition": "debug", "account": "acct", "confirm_spend": True}, id="compute-with-selection"),
])
async def test_ensure_endpoint_up_refuses_with_no_facility(unbound, kw):
    app, cli, provisions = unbound
    res = await _ensure_endpoint_up(app, **kw)
    assert res.status == "down" and res.block_state == "cold" and res.endpoint_id is None
    assert res.notice == _no_facility_notice(app)
    _assert_nothing_started(app, cli, provisions)


async def test_one_notice_for_every_tool_including_login_shell(unbound):
    app, cli, provisions = unbound
    notices = {
        (await _login_shell(app, "sinfo")).notice,
        (await _run_shell(app, "hostname")).notice,
        (await _reset_session(app)).notice,
        (await _ensure_endpoint_up(app)).notice,
    }
    assert notices == {_no_facility_notice(app)}
    _assert_nothing_started(app, cli, provisions)


# --- a connect that only PROPOSED a config: finish it with details=… ---------------------------------------------


_DRAFT = FacilityDetails(
    ssh_host="login.newfac.edu", interface="ib0", env_setup="source {venv}/bin/activate",
    scratch_root="/scratch/{user}/.hpc-bridge", partition="debug", allocation_command="mybalance",
    allocation_parser="mybalance",
)


def _probe_proposes(monkeypatch):
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))

    async def fake_discover(target):
        return _DRAFT, ["interface: proposed `ib0` — CONFIRM"]

    monkeypatch.setattr(connect, "discover_facility_details", fake_discover)
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setenv("HPC_BRIDGE_SSH_KEY", "/k")
    monkeypatch.setattr(config, "_control_settings", lambda: (None, 60))  # no real socket dir in tests


async def test_after_a_proposal_the_notice_says_finish_it_with_details(unbound, monkeypatch):
    app, cli, provisions = unbound
    _probe_proposes(monkeypatch)
    res = await _connect_facility(app, "newfac", ssh_host="login.newfac.edu")
    assert res.phase == "proposed_facility_details"
    assert app.facility.implicit is True and app.proposed_facility == "newfac"  # proposed, NOT connected
    for out in (await _run_shell(app, "hostname"), await _reset_session(app), await _ensure_endpoint_up(app),
                await _login_shell(app, "sinfo")):
        assert out.notice == _no_facility_notice(app)
        assert "connect_facility('newfac', details=…)" in out.notice and "PROPOSED" in out.notice
        assert "Nothing was started" in out.notice
    _assert_nothing_started(app, cli, provisions)


async def test_a_connect_that_only_asked_records_no_proposal(unbound, monkeypatch):
    app, _cli, _provisions = unbound
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    monkeypatch.delenv("HPC_BRIDGE_SSH_HOST", raising=False)
    res = await _connect_facility(app, "newfac")  # no host to probe -> needs_facility_details, no draft
    assert res.phase == "needs_facility_details" and app.proposed_facility is None
    assert (await _run_shell(app, "hostname")).notice.startswith("No facility connected.")


_BAD_HOST = "-oProxyCommand=x"  # ssh-option-shaped: validate_host refuses it before any bind


@pytest.mark.parametrize("later", [
    pytest.param(lambda app: _connect_facility(app, "other"), id="another-facility-needs-details"),
    pytest.param(lambda app: _connect_facility(app, "other",
                                               details=_DRAFT.model_copy(update={"allocation_parser": "sbank"})),
                 id="another-facility-unsupported"),
    pytest.param(lambda app: _connect_facility(app, "other", ssh_host=_BAD_HOST), id="failure-before-bind"),
    pytest.param(lambda app: _connect_facility(app, "newfac", ssh_host=_BAD_HOST), id="same-facility-failure"),
])
async def test_any_later_connect_replaces_the_proposal(unbound, monkeypatch, later):
    app, cli, provisions = unbound
    _probe_proposes(monkeypatch)
    monkeypatch.delenv("HPC_BRIDGE_SSH_HOST", raising=False)
    await _connect_facility(app, "newfac", ssh_host="login.newfac.edu")
    assert app.proposed_facility == "newfac"
    res = await later(app)
    assert res.phase in ("needs_facility_details", "unsupported", "failed")
    assert app.proposed_facility is None  # the notice follows the LAST connect, not a stale draft
    assert (await _run_shell(app, "hostname")).notice.startswith("No facility connected.")
    _assert_nothing_started(app, cli, provisions)


async def test_a_new_proposal_replaces_an_older_one(unbound, monkeypatch):
    app, _cli, _provisions = unbound
    _probe_proposes(monkeypatch)
    await _connect_facility(app, "newfac", ssh_host="login.newfac.edu")
    await _connect_facility(app, "otherfac", ssh_host="login.newfac.edu")
    assert app.proposed_facility == "otherfac"
    assert "connect_facility('otherfac', details=…)" in (await _run_shell(app, "hostname")).notice


# --- a pending one-time login: finish it, don't connect again (another SSH attempt) -------------------------------


_RESUME = "connect_facility('mfa', ssh_host='login.mfa.edu')"


async def _needs_preauth(app, monkeypatch, *, control_dir):
    """An un-catalogued host whose probe wants a one-time login: connect_facility answers needs_preauth."""
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))

    async def needs_code(target):
        raise NeedsPreauth(target, otp_ok=True)

    monkeypatch.setattr(connect, "discover_facility_details", needs_code)
    monkeypatch.setattr(config, "_control_settings", lambda: (control_dir, 60))  # None = multiplexing off
    res = await _connect_facility(app, "mfa", ssh_host="login.mfa.edu")
    assert res.phase == "needs_preauth" and app.pending_preauth is not None
    return res


def _follow(notice: str):
    """Call the connect_facility the notice says to call, exactly as written."""
    m = re.search(r"call (connect_facility\(.*?\)) again", notice, re.IGNORECASE)
    assert m, notice
    return eval(m.group(1), {"connect_facility": lambda *a, **k: (a, k)})  # the server's own notice text, not input


async def test_a_pending_one_time_login_points_at_complete_preauth(unbound, monkeypatch):
    app, cli, provisions = unbound
    await _needs_preauth(app, monkeypatch, control_dir="/tmp/cm")  # multiplexing on: a code handoff
    command = app.pending_preauth[1].preauth_command()
    for out in (await _run_shell(app, "hostname"), await _reset_session(app), await _ensure_endpoint_up(app),
                await _login_shell(app, "sinfo")):
        assert out.notice == _no_facility_notice(app)
        assert "complete_preauth(code)" in out.notice and f"{_RESUME} again" in out.notice
        assert command in out.notice  # the exact command to hand the user, not "its preauth_command"
        assert "FIRST" not in out.notice  # a fresh connect would retry the SSH login (a fail2ban strike)
        assert "never a password" in out.notice and "Nothing was started" in out.notice
    _assert_nothing_started(app, cli, provisions)


async def test_following_the_preauth_notice_reaches_the_probe_again(unbound, monkeypatch):
    # The resume used to be connect_facility('mfa') — the host lost, so following it literally answered
    # needs_facility_details for an un-catalogued facility. It keeps the host now, here and after complete_preauth.
    app, cli, provisions = unbound
    await _needs_preauth(app, monkeypatch, control_dir="/tmp/cm")
    direct = _follow(_no_facility_notice(app))
    monkeypatch.setattr(preauth, "open_master_with_code", lambda target, code, **k: (True, "opened the connection"))
    opened = await _complete_preauth(app, "123456")
    assert opened.phase == "opened" and f"Call {_RESUME} again" in (opened.notice or "")
    assert _follow(opened.notice) == direct == (("mfa",), {"ssh_host": "login.mfa.edu"})
    _probe_proposes(monkeypatch)  # the shared connection is open now: the probe answers
    args, kwargs = direct
    res = await _connect_facility(app, *args, **kwargs)
    assert res.phase == "proposed_facility_details"  # the resume reached the probe, not needs_facility_details
    _assert_nothing_started(app, cli, provisions)


async def test_with_multiplexing_off_the_notice_offers_no_code(unbound, monkeypatch):
    # No shareable master: neither complete_preauth nor the user's own `ssh -fN` helps (complete_preauth refuses
    # "multiplexing is off"). The notice says what the connect result says: enable multiplexing.
    app, cli, provisions = unbound
    res = await _needs_preauth(app, monkeypatch, control_dir=None)
    notice = (await _run_shell(app, "hostname")).notice
    assert notice == _no_facility_notice(app)
    core = "needs an interactive login (password/MFA), but SSH multiplexing is off. Set HPC_BRIDGE_SSH_CONTROL_PERSIST"
    assert core in (res.notice or "") and core in notice  # the connect path's own wording, reused
    assert f"then call {_RESUME} again" in notice
    assert "complete_preauth" not in notice and "ControlPath" not in notice and "FIRST" not in notice
    _assert_nothing_started(app, cli, provisions)


# --- a facility bound by connect_facility is unaffected ------------------------------------------------------------


async def test_finishing_the_proposal_binds_and_the_tools_work(unbound, monkeypatch):
    app, cli, provisions = unbound
    _probe_proposes(monkeypatch)
    await _connect_facility(app, "newfac", ssh_host="login.newfac.edu")
    f = FakeFacility()
    f.workers = 1
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    res = await _connect_facility(app, "newfac", details=_DRAFT)
    assert res.phase == "needs_account"
    assert app.facility is f and app.proposed_facility is None  # bound: the proposal is finished
    out = await _run_shell(app, "hostname", shape="login")
    assert out.phase == "complete" and out.exit_code == 0
    assert (await _ensure_endpoint_up(app, account="cis250223", confirm_spend=True)).status == "up"
    assert cli.calls == [] and provisions == []  # the implicit local default was never stood up along the way


async def test_a_catalog_connect_from_the_unbound_boot_is_unaffected(unbound, monkeypatch):
    app, cli, provisions = unbound
    f = FakeFacility()
    f.workers = 1
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    assert (await _connect_facility(app, "anvil")).phase == "needs_account"
    assert (await _run_shell(app, "hostname", shape="login")).phase == "complete"
    assert cli.calls == [] and provisions == []


# --- local dev: HPC_BRIDGE_ENDPOINT_ID dispatches as before; the boot path provisions nothing -------------------


async def test_local_dev_with_a_byo_endpoint_dispatches_as_before(monkeypatch, tmp_path):
    for v in ("HPC_BRIDGE_MACHINE", "HPC_BRIDGE_FACILITY"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_ID", "byo-uuid")
    app, cli = _local_app(monkeypatch, tmp_path, binding._default_local_facility(),
                          state=EndpointState(endpoint_id="byo-uuid"))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "worker\n", ""))
    out = await _run_shell(app, "hostname", shape="login")
    assert out.phase == "complete" and out.stdout == "worker\n"
    res = await _ensure_endpoint_up(app, shape="login")
    assert res.status == "up" and res.endpoint_id == "byo-uuid"
    assert cli.calls == []  # a BYO endpoint is dispatched to, never provisioned


async def test_an_explicit_local_facility_still_stands_up_its_endpoint(monkeypatch, tmp_path):
    # Only a LocalFacility constructed directly (the integration tier's way) still stands up a local endpoint —
    # configure, the engine template, start. The server's boot path never builds one that does.
    app, cli = _local_app(monkeypatch, tmp_path, LocalFacility(cli=None))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, shape="login")
    assert res.status == "up" and res.endpoint_id == "local-eid"
    assert [c[0] for c in cli.calls] == ["configure", "start"]

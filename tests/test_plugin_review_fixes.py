"""Regressions for the 2026-09-05 plugin review's findings still open in 0.1.17, fixed in 0.1.18 (vault
`Reference/Plugin review 2026-09-05.md`: #3, #4, #5, #6a-c and the changed-host-key low item), plus what two
independent reviews of the first fix found (2026-10-05). Each test replays a failing sequence."""
from __future__ import annotations

import asyncio
import shutil
import stat
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_bridge import binding, connect, preauth, server
from hpc_bridge.cost import _block_nodes, _session_spend, estimate_spend
from hpc_bridge.facility import remote
from hpc_bridge.facility.mep import MEPFacility, _template_idle_s
from hpc_bridge.facility.remote import (
    NeedsPreauth,
    RemoteEndpointCLI,
    RemoteTokenStoreExists,
    SlurmFacility,
    SshTarget,
)
from hpc_bridge.lifecycle import EndpointState
from hpc_bridge.models import ShellOutcome
from hpc_bridge.notices import _billed_bounds_note, _explain_provision_error, _needs_confirmation_notice
from hpc_bridge.profile import Profile
from hpc_bridge.server import AppCtx, ShapeRuntime, _idle_release_s
from hpc_bridge.state import EndpointRecord, LoginNodeStore
from tests.fakes import FakeCatalog, FakeFacility, fake_entry
from tests.test_remote_facility import _BootstrapCLI, _no_endpoints, _profile
from tests.test_server import _FakeRunner, _Res

ALIAS = "anvil.rcac.purdue.edu"
HOUR = estimate_spend(3600, 1, 1.0)


async def _released(a, command, session_id="default", shape="compute"):
    await asyncio.sleep(0)  # yield, as a real dispatch does — a race that never yields proves nothing
    return ShellOutcome(phase="complete", exit_code=0, stdout="released 1\n", block_state="warm")


def _record(fac, *, eid, host, seeded=False):
    return EndpointRecord(endpoint_id=eid, login_host=host, alias=ALIAS, user="x-u", key_path="/tmp/k",
                          name=fac.profile.endpoint_name, provisioned_at="2026-09-01T00:00:00+00:00",
                          seeded_credentials=seeded)


def _src_db(tmp_path: Path) -> Path:
    db = tmp_path / "src.db"
    db.write_bytes(b"x")
    return db


# --- #4: an adopted endpoint keeps the stored login-node pin — for the SAME endpoint only ----------------------------

async def test_adopting_a_running_endpoint_keeps_the_stored_pin(tmp_path):
    store = LoginNodeStore(tmp_path / "endpoints.json")
    cli = _BootstrapCLI(status="running", remote_db_present=True)
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias=ALIAS, client_factory=_no_endpoints)
    store.put(_record(fac, eid="running-eid", host="login03.anvil.rcac.purdue.edu"))
    handle = await fac.bootstrap(Profile(mode="interactive"))
    assert handle.reused is True and handle.login_host is None
    assert store.get(alias=ALIAS, name=fac.profile.endpoint_name).login_host == "login03.anvil.rcac.purdue.edu"


async def test_a_re_registered_endpoint_does_not_inherit_the_old_pin(tmp_path):
    store = LoginNodeStore(tmp_path / "endpoints.json")
    cli = _BootstrapCLI(status="running", remote_db_present=True)
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias=ALIAS, client_factory=_no_endpoints)
    store.put(_record(fac, eid="an-older-eid", host="login09.anvil.rcac.purdue.edu"))
    await fac.bootstrap(Profile(mode="interactive"))
    assert store.get(alias=ALIAS, name=fac.profile.endpoint_name).login_host is None


async def test_seeding_carries_the_pin_through_its_early_record_write(tmp_path, monkeypatch):
    # whoami fails while the manager still runs (our copy's tokens went stale): the seed-time write must not erase it
    store = LoginNodeStore(tmp_path / "endpoints.json")
    cli = _BootstrapCLI(status="running", remote_db_present=False)
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias=ALIAS, client_factory=_no_endpoints)
    store.put(_record(fac, eid="running-eid", host="login03.anvil.rcac.purdue.edu", seeded=True))
    monkeypatch.setattr(remote, "build_minimal_storage_db", lambda **kw: tmp_path / "x.db")
    await fac.bootstrap(Profile(mode="interactive"))
    rec = store.get(alias=ALIAS, name=fac.profile.endpoint_name)
    assert rec.login_host == "login03.anvil.rcac.purdue.edu" and rec.seeded_credentials is True


# --- #5: seeding never overwrites a token store hpc-bridge did not place ---------------------------------------------

async def test_a_store_we_placed_is_refreshed_not_refused(tmp_path, monkeypatch):
    store = LoginNodeStore(tmp_path / "endpoints.json")
    cli = _BootstrapCLI(status=None, remote_db_present=False)  # our copy's tokens went stale: whoami fails
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias=ALIAS, client_factory=_no_endpoints)
    store.put(_record(fac, eid="eid-0", host=None, seeded=True))
    monkeypatch.setattr(remote, "build_minimal_storage_db", lambda **kw: tmp_path / "x.db")
    await fac.bootstrap(Profile(mode="interactive"))
    assert cli.replaced_ours is True


async def test_someone_elses_store_is_refused_and_never_marked_ours(tmp_path, monkeypatch):
    class _Present(_BootstrapCLI):
        async def seed_storage_db(self, local_db, *, replace_ours=False):
            assert replace_ours is False
            raise RemoteTokenStoreExists("a Globus token store already exists on h")

    store = LoginNodeStore(tmp_path / "endpoints.json")
    cli = _Present(status=None, remote_db_present=False)
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias=ALIAS, client_factory=_no_endpoints)
    monkeypatch.setattr(remote, "build_minimal_storage_db", lambda **kw: tmp_path / "x.db")
    with pytest.raises(RemoteTokenStoreExists):
        await fac.bootstrap(Profile(mode="interactive"))
    assert store.get(alias=ALIAS, name=fac.profile.endpoint_name) is None and fac._seeded_by_us() is False


async def _seed_command(monkeypatch, tmp_path, *, replace_ours: bool) -> str:
    calls = []

    async def fake_ssh_exec(target, cmd, *, stdin=None, timeout=None):
        calls.append(cmd)
        return 0, "", ""

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget(host="h", user="u"), "true")
    await cli.seed_storage_db(_src_db(tmp_path), replace_ours=replace_ours)
    return next(c for c in calls if "base64 -d" in c)


@pytest.mark.parametrize("shell", ["/bin/sh", "/bin/tcsh", "/bin/zsh"])
async def test_the_no_clobber_write_holds_in_any_login_shell(shell, tmp_path, monkeypatch):
    # The command runs in the user's LOGIN shell. Under tcsh an `if [ … ]` line was a syntax error that still ran the
    # write (a silent overwrite); it now runs inside `sh -c` whatever the login shell is.
    if not shutil.which(shell):
        pytest.skip(f"{shell} not installed")
    guarded = await _seed_command(monkeypatch, tmp_path, replace_ours=False)
    replacing = await _seed_command(monkeypatch, tmp_path, replace_ours=True)
    home = tmp_path / "home"
    (home / ".globus_compute").mkdir(parents=True)
    env = {"HOME": str(home), "PATH": "/usr/bin:/bin"}
    store = home / ".globus_compute" / "storage.db"

    def run(cmd, data):
        return subprocess.run([shell, "-c", cmd], input=data, capture_output=True, text=True, env=env, check=False)

    first = run(guarded, "b3JpZ2luYWw=\n")  # "original"
    assert first.returncode == 0 and store.read_text() == "original", first.stderr
    second = run(guarded, "cmVwbGFjZWQ=\n")  # "replaced"
    assert second.returncode == 3 and "HPCB_EXISTS" in second.stdout and store.read_text() == "original"
    third = run(replacing, "cmVwbGFjZWQ=\n")
    assert third.returncode == 0 and store.read_text() == "replaced", third.stderr


async def test_the_refusal_names_the_failed_whoami(monkeypatch, tmp_path):
    async def fake_ssh_exec(target, cmd, *, stdin=None, timeout=None):
        return (3, "HPCB_EXISTS\n", "") if "base64" in cmd else (0, "", "")

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget(host="login.example.edu", user="u"), "true")
    cli.last_whoami_error = "Error: unable to reach auth.globus.org"
    with pytest.raises(RemoteTokenStoreExists) as err:
        await cli.seed_storage_db(_src_db(tmp_path))
    assert "already exists on login.example.edu" in str(err.value)
    assert "unable to reach auth.globus.org" in str(err.value)


def test_the_refusal_is_not_misread_as_an_ssh_denial():
    exc = RemoteTokenStoreExists("a Globus token store already exists on h, but whoami failed there: Permission denied")
    assert _explain_provision_error(exc) == str(exc)


# --- #6a: the idle window is the facility's, or said to be unknown ---------------------------------------------------

@pytest.mark.parametrize(("template", "want"), [
    ("engine:\n  max_idletime: 600.0\n", 600),                          # the lab MEP
    ("      max_idletime: {{ max_idletime | default(240) }}\n", 240),   # ALCF Polaris
    ("max_idletime: 300  # seconds\n", 300),
    ("max_idletime: {{ max_idletime }}\n", None),
    ("idle_heartbeats_soft: 10\n", None),                               # Delta, Anvil: no window set
    (None, None),
])
def test_template_idle_window(template, want):
    assert _template_idle_s(template) == want


async def test_mep_reads_its_idle_window_from_the_published_template():
    class _Client:
        def get_endpoint_metadata(self, eid):
            return {"user_config_template": "engine:\n  max_idletime: 240.0\n", "endpoint_version": "4.16.0"}

    fac = MEPFacility(endpoint_id="mep-eid", name="m", user_opts={}, client_factory=_Client)
    await fac.load_template()
    assert fac.max_idletime_s == 240


def test_an_unknown_facility_window_is_never_quoted_as_600():
    fac = FakeFacility()
    fac.max_idletime_s = None
    app = AppCtx(facility=fac, profile=Profile())
    assert _idle_release_s(app) is None
    note = _billed_bounds_note(app, ShapeRuntime(user_endpoint_config={"compute": True, "walltime": "00:30:00"}))
    assert "600" not in note and "the facility's own idle window" in note
    fac.max_idletime_s = 240
    assert "~240s" in _billed_bounds_note(app, ShapeRuntime(user_endpoint_config={"compute": True}))


def test_our_own_endpoint_still_quotes_the_window_we_wrote():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    assert _idle_release_s(app) == app.profile.max_idletime_s


# --- #6b: spend and the confirm notice use the block's own node count ------------------------------------------------

def test_spend_and_notice_follow_the_blocks_node_count():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.charge_factor = 1.0
    rt = ShapeRuntime(user_endpoint_config={"compute": True, "nodes_per_block": 4, "walltime": "01:00:00"})
    assert _block_nodes(rt, app) == 4
    rt.warm_since = time.monotonic() - 3600
    assert _session_spend(rt, app) == pytest.approx(estimate_spend(3600, 4, 1.0), rel=0.01)
    assert "(4 node(s) × walltime 01:00:00)" in _needs_confirmation_notice(app, " on 'debug'", rt)


def test_a_block_without_a_node_count_bills_one():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    assert _block_nodes(ShapeRuntime(user_endpoint_config={"compute": True}), app) == 1
    assert _block_nodes(ShapeRuntime(user_endpoint_config={"nodes_per_block": "x"}), app) == 1


# --- #3 and #6c: teardown ---------------------------------------------------------------------------------------------

def _ssh_app(fac, eid="eid-1", warm_hours=0.0):
    app = AppCtx(facility=fac, profile=Profile(), state=EndpointState(endpoint_id=eid))
    app.machine = "anvil"
    app.charge_factor = 1.0
    compute = ShapeRuntime(user_endpoint_config={"compute": True})
    if warm_hours:
        compute.warm_since = time.monotonic() - warm_hours * 3600
    app.shapes["compute"] = compute
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    return app


class _MfaFac(FakeFacility):
    auth_method = "mfa-otp"

    def __init__(self, tmp):
        super().__init__()
        self.cli = SimpleNamespace(target=SshTarget(host="login.x.edu", user="u", control_dir=str(tmp)))
        self.torn = []

    async def teardown(self, eid, *, wipe_credentials=False):
        self.torn.append(eid)
        return {"stopped": True, "deleted": True, "credentials_wiped": True, "ssh_closed": True}


async def test_ssh_teardown_stops_the_compute_clock_before_the_login_node_ops(monkeypatch):
    seen = {}

    class _F(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            seen["compute_still_bound"] = "compute" in app.shapes

    app = _ssh_app(_F(), warm_hours=1.0)
    monkeypatch.setattr(server, "_run_shell", _released)
    res = await server._teardown_endpoint(app)
    assert res.status == "down" and seen["compute_still_bound"] is False
    assert res.session_spend == pytest.approx(HOUR, rel=0.01)


async def test_two_parallel_teardowns_run_the_login_node_ops_once(monkeypatch):
    gate = asyncio.Event()
    torn = []

    class _F(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            torn.append(eid)
            await gate.wait()

    app = _ssh_app(_F())
    monkeypatch.setattr(server, "_run_shell", _released)
    first = asyncio.create_task(server._teardown_endpoint(app))
    second = asyncio.create_task(server._teardown_endpoint(app))
    await asyncio.sleep(0.05)
    gate.set()
    a, b = await asyncio.gather(first, second)
    assert torn == ["eid-1"] and a.status == b.status == "down" and app.teardown_task is None


async def test_a_connect_during_teardown_is_refused_and_cannot_retarget_it(monkeypatch):
    gate = asyncio.Event()
    torn = []

    class _Old(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            torn.append(("old", eid))
            await gate.wait()

    class _New(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            torn.append(("new", eid))

    old = _Old()
    app = _ssh_app(old)
    monkeypatch.setattr(server, "_run_shell", _released)
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="expanse", facility_key="sdsc")]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: _New())
    tear = asyncio.create_task(server._teardown_endpoint(app))
    await asyncio.sleep(0.05)
    res = await server._connect_facility(app, "expanse")
    assert res.phase == "failed" and "teardown of the current endpoint is still running" in (res.notice or "")
    assert app.facility is old and app.machine == "anvil"
    gate.set()
    assert (await tear).status == "down" and torn == [("old", "eid-1")]


async def test_mep_detach_does_not_wipe_a_connect_that_queued_behind_it(monkeypatch):
    class _Mep(FakeFacility):
        supported_shapes = ("compute",)

    app = AppCtx(facility=_Mep(), profile=Profile(), state=EndpointState(endpoint_id="mep-eid"))
    app.machine = "globus1"
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    new = FakeFacility()
    new.workers = 1
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: new)
    go = asyncio.Event()

    async def holder():  # any long locked op in the same tool batch
        async with app.lock:
            await go.wait()

    h = asyncio.create_task(holder())
    await asyncio.sleep(0)
    tear = asyncio.create_task(server._teardown_endpoint(app))
    await asyncio.sleep(0.01)
    conn = asyncio.create_task(server._connect_facility(app, "anvil"))
    await asyncio.sleep(0.1)
    go.set()
    td, cr = await asyncio.gather(tear, conn)
    await h
    assert td.status == "down" and app.facility is new and app.machine == "anvil"
    assert app.state.endpoint_id == "fake-eid", (cr.phase, cr.notice)  # the new binding survived the detach


async def test_a_resumed_teardown_still_counts_the_released_blocks_spend(monkeypatch, tmp_path):
    fac = _MfaFac(tmp_path)
    app = _ssh_app(fac, warm_hours=1.0)
    monkeypatch.setattr(server, "_run_shell", _released)
    alive = {"v": False}
    monkeypatch.setattr(connect, "_master_alive", lambda t: alive["v"])
    first = await server._teardown_endpoint(app)
    assert first.status == "up" and app.preauth_resume == "teardown_endpoint()"
    assert first.session_spend == pytest.approx(HOUR, rel=0.01)
    alive["v"] = True  # complete_preauth opened the master
    second = await server._teardown_endpoint(app)
    assert second.status == "down" and fac.torn == ["eid-1"]
    assert second.session_spend == pytest.approx(HOUR, rel=0.01)


async def test_tearing_down_does_not_claim_a_release_that_has_not_happened(monkeypatch):
    app = _ssh_app(FakeFacility())
    gate = asyncio.Event()

    async def slow_release(a, command, session_id="default", shape="compute"):
        await gate.wait()
        return await _released(a, command)

    monkeypatch.setattr(server, "_run_shell", slow_release)
    monkeypatch.setattr(server, "_TEARDOWN_SYNC_WAIT_S", 0.05)
    res = await server._teardown_endpoint(app)
    gate.set()
    await app.teardown_task
    assert res.status == "tearing_down"
    assert "still in progress" in res.notice and "went through" not in res.notice


async def test_a_stale_gate_result_is_not_replayed_after_the_code(monkeypatch, tmp_path):
    fac = _MfaFac(tmp_path)
    app = _ssh_app(fac)
    gate = asyncio.Event()

    async def slow_release(a, command, session_id="default", shape="compute"):
        await gate.wait()
        return await _released(a, command)

    monkeypatch.setattr(server, "_run_shell", slow_release)
    monkeypatch.setattr(server, "_TEARDOWN_SYNC_WAIT_S", 0.05)
    alive = {"v": False}
    monkeypatch.setattr(connect, "_master_alive", lambda t: alive["v"])
    assert (await server._teardown_endpoint(app)).status == "tearing_down"
    gate.set()
    await app.teardown_task  # the code request lands with nobody waiting
    alive["v"], app.pending_preauth, app.preauth_resume = True, None, None  # complete_preauth succeeded
    assert (await server._teardown_endpoint(app)).status == "down"


async def test_a_finished_teardown_is_not_replayed_for_a_new_endpoint(monkeypatch):
    gate = asyncio.Event()
    torn = []

    class _F(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            torn.append(eid)
            await gate.wait()

    app = _ssh_app(_F())
    monkeypatch.setattr(server, "_run_shell", _released)
    monkeypatch.setattr(server, "_TEARDOWN_SYNC_WAIT_S", 0.05)
    assert (await server._teardown_endpoint(app)).status == "tearing_down"
    gate.set()
    await app.teardown_task
    app.state = EndpointState(endpoint_id="eid-2")  # a stray run_shell re-bootstrapped
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    await server._teardown_endpoint(app)
    assert torn == ["eid-1", "eid-2"]


async def test_an_exception_after_the_release_keeps_the_spend(monkeypatch):
    app = _ssh_app(FakeFacility(), warm_hours=1.0)
    monkeypatch.setattr(server, "_run_shell", _released)

    async def boom(*a, **k):
        raise RuntimeError("probe")

    monkeypatch.setattr(server, "_teardown_preauth_gate", boom)
    res = await server._teardown_endpoint(app)
    assert res.status == "up" and res.session_spend == pytest.approx(HOUR, rel=0.01)


async def test_a_code_gate_after_a_cold_release_says_it_is_unconfirmed(monkeypatch, tmp_path):
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    app = _ssh_app(_MfaFac(tmp_path), warm_hours=1.0)

    async def cold(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="cold_start", block_state="cold", notice="allocating nodes…")

    monkeypatch.setattr(server, "_run_shell", cold)
    monkeypatch.setattr(connect, "_master_alive", lambda t: False)
    res = await server._teardown_endpoint(app)
    assert res.status == "up" and "NOT confirmed" in res.notice


async def test_a_status_check_that_times_out_is_not_reported_as_still_running(tmp_path):
    class _Slow(_BootstrapCLI):
        async def stop(self, name):
            return 1, "psutil traceback"

        async def status(self, name):
            raise TimeoutError

    cli = _Slow(status="running", remote_db_present=True)
    fac = SlurmFacility(_profile(), cli=cli, store=LoginNodeStore(tmp_path / "e.json"), alias=ALIAS,
                        client_factory=_no_endpoints)
    report = await fac.teardown("eid-1")
    assert report["stopped"] is None and "timed out" in report["error"]

    class _Reports(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            return report

    res = await server._finish_teardown(_ssh_app(_Reports()), "eid-1")
    assert res.notice.startswith("TEARDOWN NOT CONFIRMED") and "STILL RUNNING" not in res.notice


async def test_a_rebind_drops_the_previous_facilitys_code_handoff(monkeypatch):
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.pending_preauth = ("expanse", SshTarget(host="login.expanse.sdsc.edu", user="u"))
    app.preauth_resume = "teardown_endpoint()"
    f = FakeFacility()
    f.workers = 1
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    await server._connect_facility(app, "anvil")
    assert app.machine == "anvil" and app.pending_preauth is None and app.preauth_resume is None
    res = await server._complete_preauth(app, "123456")
    assert res.phase == "failed" and "no facility is waiting for a code" in (res.notice or "")


async def test_a_byo_proposal_that_needs_a_code_resumes_with_connect(monkeypatch):
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.preauth_resume = "teardown_endpoint()"  # stale, from an earlier gate

    async def needs_code(target):
        raise NeedsPreauth(target, otp_ok=True)

    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([]))
    monkeypatch.setattr(connect, "discover_facility_details", needs_code)
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setattr(connect.config, "_control_settings", lambda: (None, 60))
    res = await server._connect_facility(app, "newfac", ssh_host="login.newfac.edu")
    assert res.phase == "needs_preauth" and app.preauth_resume == "connect_facility('newfac')"


# --- low: a changed or revoked host key is never coached like an unknown one -----------------------------------------

def _fake_ssh(tmp_path, stderr: str) -> str:
    ssh = tmp_path / "ssh"
    ssh.write_text("#!/bin/sh\nfor a in \"$@\"; do [ \"$a\" = \"-O\" ] && exit 255; done\ncat >&2 <<'EOF'\n"
                   + stderr + "\nEOF\nexit 255\n")
    ssh.chmod(ssh.stat().st_mode | stat.S_IXUSR)
    return str(ssh)


def _target(tmp_path) -> SshTarget:
    cd = tmp_path / "cm"
    cd.mkdir(exist_ok=True)
    return SshTarget(host="login.expanse.sdsc.edu", user="u", control_dir=str(cd))


def test_a_changed_host_key_names_the_entry_openssh_reports(tmp_path):
    ssh = _fake_ssh(tmp_path, "@    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @\n"
                              "Host key for [login.expanse.sdsc.edu]:2222 has changed and you have requested strict "
                              "checking.\nHost key verification failed.")
    ok, why = preauth.open_master_with_code(_target(tmp_path), "123456", state_dir=tmp_path, ssh_bin=ssh)
    assert not ok and why.startswith("HOST KEY CHANGED for login.expanse.sdsc.edu") and "Do NOT accept" in why
    assert "ssh-keygen -R '[login.expanse.sdsc.edu]:2222'" in why and "UNKNOWN HOST KEY" not in why


def test_a_revoked_host_key_is_refused_outright(tmp_path):
    ssh = _fake_ssh(tmp_path, "@ WARNING: REVOKED HOST KEY DETECTED! @\nThe ECDSA host key for login.expanse.sdsc.edu "
                              "is marked as revoked.\nThis could mean that a stolen key is being used\n"
                              "Host key for login.expanse.sdsc.edu was revoked.\nHost key verification failed.")
    ok, why = preauth.open_master_with_code(_target(tmp_path), "123456", state_dir=tmp_path, ssh_bin=ssh)
    assert not ok and why.startswith("REVOKED HOST KEY") and "accept" not in why.lower()

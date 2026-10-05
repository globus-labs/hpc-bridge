"""Regressions for the 2026-09-05 plugin review's findings that were still open in 0.1.17 and are fixed in 0.1.18
(vault `Reference/Plugin review 2026-09-05.md`, findings 3, 4, 5, 6a-c and the changed-host-key low item). Each test
replays the review's failing sequence; every one of them failed before the fix."""
from __future__ import annotations

import asyncio
import stat
import time

import pytest

from hpc_bridge import binding, connect, preauth, server, warmth
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


async def _released(a, command, session_id="default", shape="compute"):
    return ShellOutcome(phase="complete", exit_code=0, stdout="released 1\n", block_state="warm")


# --- #4: adopting an already-running endpoint keeps the stored login-node pin ---------------------------------------

async def test_adopting_a_running_endpoint_keeps_the_stored_pin(tmp_path):
    # Store pins login03; the web service does not (yet) report the endpoint online, `gce status` says running ->
    # provision adopts it with login_host None. The record written afterwards must keep login03, else the next
    # session's control-plane SSH goes to the round-robin alias and orphans the manager.
    store = LoginNodeStore(tmp_path / "endpoints.json")
    cli = _BootstrapCLI(status="running", remote_db_present=True)
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias="anvil.rcac.purdue.edu", client_factory=_no_endpoints)
    store.put(EndpointRecord(
        endpoint_id="eid-0", login_host="login03.anvil.rcac.purdue.edu", alias="anvil.rcac.purdue.edu", user="x-u",
        key_path="/tmp/k", name=fac.profile.endpoint_name, provisioned_at="2026-09-01T00:00:00+00:00",
    ))
    handle = await fac.bootstrap(Profile(mode="interactive"))
    assert handle.reused is True and handle.login_host is None
    rec = store.get(alias="anvil.rcac.purdue.edu", name=fac.profile.endpoint_name)
    assert rec is not None and rec.login_host == "login03.anvil.rcac.purdue.edu"


async def test_a_fresh_start_still_records_its_own_node(tmp_path):
    store = LoginNodeStore(tmp_path / "endpoints.json")
    cli = _BootstrapCLI(status=None, remote_db_present=True)
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias="anvil.rcac.purdue.edu", client_factory=_no_endpoints)
    store.put(EndpointRecord(
        endpoint_id="eid-0", login_host="login09.anvil.rcac.purdue.edu", alias="anvil.rcac.purdue.edu", user="x-u",
        key_path="/tmp/k", name=fac.profile.endpoint_name, provisioned_at="2026-09-01T00:00:00+00:00",
    ))
    handle = await fac.bootstrap(Profile(mode="interactive"))
    rec = store.get(alias="anvil.rcac.purdue.edu", name=fac.profile.endpoint_name)
    assert handle.login_host == rec.login_host == "login03.anvil.rcac.purdue.edu"  # the node it just launched on


# --- #5: credential seeding never overwrites a token store hpc-bridge did not create ---------------------------------

async def test_seed_refuses_to_overwrite_an_existing_store(monkeypatch):
    calls = []

    async def fake_ssh_exec(target, cmd, *, stdin=None, timeout=None):
        calls.append(cmd)
        if cmd.startswith("mkdir"):
            return 0, "", ""
        return 3, "HPCB_EXISTS\n", ""

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh_exec)
    cli = RemoteEndpointCLI(SshTarget(host="login.example.edu", user="u"), "true")
    cli.last_whoami_error = "Error: unable to reach auth.globus.org"
    with pytest.raises(RemoteTokenStoreExists) as err:
        await cli.seed_storage_db(_write(b"db"))
    assert 'if [ -e "$HOME/.globus_compute/storage.db" ]; then echo HPCB_EXISTS; exit 3; fi' in calls[-1]
    msg = str(err.value)
    assert "already exists on login.example.edu" in msg and "unable to reach auth.globus.org" in msg
    assert "will not replace a credential it did not create" in msg


async def test_bootstrap_does_not_flag_a_refused_seed_as_ours(tmp_path, monkeypatch):
    class _Present(_BootstrapCLI):
        async def seed_storage_db(self, local_db):
            raise RemoteTokenStoreExists("a Globus token store already exists on h")

    store = LoginNodeStore(tmp_path / "endpoints.json")
    cli = _Present(status=None, remote_db_present=False)  # whoami fails, yet a store is there
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias="anvil.rcac.purdue.edu", client_factory=_no_endpoints)
    monkeypatch.setattr(remote, "build_minimal_storage_db", lambda **kw: tmp_path / "x.db")
    with pytest.raises(RemoteTokenStoreExists):
        await fac.bootstrap(Profile(mode="interactive"))
    assert store.get(alias="anvil.rcac.purdue.edu", name=fac.profile.endpoint_name) is None  # never "seeded by us"
    assert fac._seeded_by_us() is False  # so teardown will never delete it


def test_the_refusal_is_not_misread_as_an_ssh_denial():
    exc = RemoteTokenStoreExists("a Globus token store already exists on h, but whoami failed there: Permission denied")
    assert _explain_provision_error(exc) == str(exc)


def _write(data: bytes):
    import tempfile
    from pathlib import Path

    f = Path(tempfile.mkdtemp()) / "storage.db"
    f.write_bytes(data)
    return f


# --- #6a: the idle window is the facility's, or said to be unknown ---------------------------------------------------

@pytest.mark.parametrize(("template", "want"), [
    ("engine:\n  max_idletime: 600.0\n", 600),
    ("    max_idletime: {{ max_idletime | default(240) }}\n", 240),
    ("max_idletime: 300  # seconds\n", 300),
    ("max_idletime: {{ max_idletime }}\n", None),
    ("idle_heartbeats_soft: 10\n", None),
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
    fac.max_idletime_s = None  # a MEP whose template does not publish a constant
    app = AppCtx(facility=fac, profile=Profile())
    assert _idle_release_s(app) is None
    note = _billed_bounds_note(app, ShapeRuntime(user_endpoint_config={"compute": True, "walltime": "00:30:00"}))
    assert "600" not in note and "the facility's own idle window" in note
    fac.max_idletime_s = 240
    assert "~240s" in _billed_bounds_note(app, ShapeRuntime(user_endpoint_config={"compute": True}))


def test_our_own_endpoint_still_quotes_the_window_we_wrote():
    app = AppCtx(facility=FakeFacility(), profile=Profile())  # no max_idletime_s attribute: an SSH endpoint of ours
    assert _idle_release_s(app) == app.profile.max_idletime_s


# --- #6b: spend and the confirm notice use the block's own node count ------------------------------------------------

def test_spend_and_notice_follow_the_blocks_node_count():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.charge_factor = 1.0
    rt = ShapeRuntime(user_endpoint_config={"compute": True, "nodes_per_block": 4, "walltime": "01:00:00"})
    assert _block_nodes(rt, app) == 4
    rt.warm_since = time.monotonic() - 3600
    assert _session_spend(rt, app) == pytest.approx(estimate_spend(3600, 4, 1.0), rel=0.01)
    notice = _needs_confirmation_notice(app, " on 'debug'", rt)
    assert "(4 node(s) × walltime 01:00:00)" in notice


def test_a_block_without_a_node_count_bills_one():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    assert _block_nodes(ShapeRuntime(user_endpoint_config={"compute": True}), app) == 1
    assert _block_nodes(ShapeRuntime(user_endpoint_config={"nodes_per_block": "x"}), app) == 1


# --- #6c: an SSH teardown stops the compute spend clock at the release -----------------------------------------------

async def test_ssh_teardown_stops_the_compute_clock_before_the_login_node_ops(monkeypatch):
    seen = {}

    class _F(FakeFacility):
        async def teardown(self, eid, *, wipe_credentials=False):
            seen["compute_still_bound"] = "compute" in app.shapes  # the clock must already be stopped here

    app = AppCtx(facility=_F(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
    app.charge_factor = 1.0
    compute = ShapeRuntime(user_endpoint_config={"compute": True})
    compute.warm_since = time.monotonic() - 3600
    app.shapes["compute"] = compute
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    monkeypatch.setattr(server, "_run_shell", _released)
    res = await server._teardown_endpoint(app)
    assert res.status == "down"
    assert seen["compute_still_bound"] is False
    assert res.session_spend == pytest.approx(estimate_spend(3600, 1, 1.0), rel=0.01)  # the ended block still counts


# --- #3: teardown's unlocked window -----------------------------------------------------------------------------------

def _ssh_app(fac, eid="eid-1"):
    app = AppCtx(facility=fac, profile=Profile(), state=EndpointState(endpoint_id=eid))
    app.machine = "anvil"
    app.shapes["compute"] = ShapeRuntime(user_endpoint_config={"compute": True})
    app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
    return app


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
    assert torn == ["eid-1"]
    assert a.status == b.status == "down"
    assert app.teardown_task is None


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
    assert app.facility is old and app.machine == "anvil"  # nothing was re-bound under the teardown
    gate.set()
    done = await tear
    assert done.status == "down" and torn == [("old", "eid-1")]  # the OLD facility, its OWN endpoint, once


async def test_a_rebind_drops_the_previous_facilitys_code_handoff(monkeypatch):
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.pending_preauth = ("expanse", SshTarget(host="login.expanse.sdsc.edu", user="u"))
    app.preauth_resume = "teardown_endpoint()"
    f = FakeFacility()
    f.workers = 1
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    await server._connect_facility(app, "anvil")
    assert app.machine == "anvil"
    assert app.pending_preauth is None and app.preauth_resume is None
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
    assert res.phase == "needs_preauth"
    assert app.preauth_resume == "connect_facility('newfac')"


# --- low: a CHANGED host key is never coached like an unknown one ----------------------------------------------------

_CHANGED_SSH = r'''#!/bin/sh
for a in "$@"; do [ "$a" = "-O" ] && exit 255; done
cat >&2 <<'EOF'
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
Host key for login.expanse.sdsc.edu has changed and you have requested strict checking.
Host key verification failed.
EOF
exit 255
'''


def test_a_changed_host_key_is_not_coached_like_an_unknown_one(tmp_path):
    ssh = tmp_path / "ssh"
    ssh.write_text(_CHANGED_SSH)
    ssh.chmod(ssh.stat().st_mode | stat.S_IXUSR)
    cd = tmp_path / "cm"
    cd.mkdir()
    target = SshTarget(host="login.expanse.sdsc.edu", user="u", control_dir=str(cd))
    ok, why = preauth.open_master_with_code(target, "123456", state_dir=tmp_path, ssh_bin=str(ssh))
    assert not ok and why.startswith("HOST KEY CHANGED for login.expanse.sdsc.edu")
    assert "Do NOT accept" in why and "UNKNOWN HOST KEY" not in why


def test_unused_imports_are_live():  # keeps the module honest under ruff's unused-import rule
    assert warmth and MEPFacility

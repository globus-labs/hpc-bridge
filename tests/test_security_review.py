"""Security review 2026-09-04 — the fixes, pinned. Findings A1-A4/B-01..B-06/C-1..C-8 in the vault's
Reference/Security review 2026-09-04.md. Every test here is a regression guard for one of them."""
from __future__ import annotations

import asyncio
import json
import os
import shlex
import stat
import subprocess

import pytest

from hpc_bridge import config
from hpc_bridge.catalog.entry import CatalogEntry
from hpc_bridge.facility import remote
from hpc_bridge.facility.remote import RemoteEndpointCLI, SshTarget
from hpc_bridge.models import FacilityDetails, validate_host
from hpc_bridge.notices import _explain_provision_error, _first_contact_note


def _seed_entry():
    import yaml

    with open("src/hpc_bridge/catalog/seed/anvil.yaml") as fh:
        doc = yaml.safe_load(fh)
    return doc[0] if isinstance(doc, list) else doc


# ---- host string: the one input that reaches ssh's argv (A3 / C-2) --------------------------------------------

BAD_HOSTS = ["-oProxyCommand=touch /tmp/pwned", "-F/etc/passwd", "host name", "host;id", "h$(id)", "", "a\nb"]
OPTION_SHAPED = {"-oProxyCommand=touch /tmp/pwned", "-F/etc/passwd", "host name", "", "a\nb"}  # what argv itself must refuse
GOOD_HOSTS = ["globus1", "anvil.rcac.purdue.edu", "login-4.rcc.uchicago.edu", "10.0.0.7", "my_alias"]


@pytest.mark.parametrize("bad", BAD_HOSTS)
def test_bad_hosts_are_rejected_everywhere(bad):
    with pytest.raises(ValueError):
        validate_host(bad)  # the model boundary rejects every shape
    if bad in OPTION_SHAPED:  # argv is exec'd (no shell): the target itself only has to refuse option/whitespace shapes
        with pytest.raises(ValueError):
            SshTarget(host=bad)
    with pytest.raises(Exception):  # noqa: B017 - pydantic ValidationError wraps the ValueError
        FacilityDetails(ssh_host=bad, interface="ib0", scheduler="slurm", scratch_root="/scratch",
                        env_setup="true", default_partition="main")


@pytest.mark.parametrize("good", GOOD_HOSTS)
def test_good_hosts_pass(good):
    assert validate_host(good) == good
    assert SshTarget(host=good).argv("true")[-2] == good


def test_argv_never_lets_the_destination_be_read_as_an_option():
    argv = SshTarget(host="globus1").argv("true")
    assert argv[argv.index("--") + 1] == "globus1"
    assert not any(o.startswith("StrictHostKeyChecking") for o in argv)  # your ssh's trust decides, not ours


def test_registry_entry_with_a_bad_host_or_slug_is_refused():
    seed = _seed_entry()
    CatalogEntry.model_validate(seed)  # the real seed is fine
    for field, bad in (("ssh_host", "-oProxyCommand=x"), ("id", "x;$(id)"), ("facility_key", "a b")):
        with pytest.raises(Exception):  # noqa: B017 - ValidationError
            CatalogEntry.model_validate({**seed, field: bad})


# ---- pinned login node: the key is checked against the alias the user trusted (A10) ----------------------------

def test_rebind_to_a_pinned_node_keeps_the_trusted_name_as_host_key_alias(monkeypatch):
    # known_hosts is keyed by the RESOLVED HostName: an ssh-config alias resolves through `ssh -G` (live 2026-09-04)
    monkeypatch.setattr(remote, "_resolved_hostname", lambda h: {"anvil": "anvil.rcac.purdue.edu"}.get(h, h))
    cli = RemoteEndpointCLI(SshTarget(host="anvil", user="u"), "true")
    cli.rebind("login04.anvil.rcac.purdue.edu")
    argv = cli.target.argv("true")
    assert cli.target.host == "login04.anvil.rcac.purdue.edu"
    assert "HostKeyAlias=anvil.rcac.purdue.edu" in argv
    cli.rebind("login05.anvil.rcac.purdue.edu")  # a second pin still verifies against the ORIGINAL trusted name
    assert "HostKeyAlias=anvil.rcac.purdue.edu" in cli.target.argv("true")


def test_resolved_hostname_falls_back_to_the_name_itself(monkeypatch):
    import subprocess

    monkeypatch.setattr(subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError("no ssh")))
    assert remote._resolved_hostname("anvil") == "anvil"


# ---- the user's paste-and-run line is quoted (A4 / C-9) --------------------------------------------------------

def test_preauth_command_is_shell_safe():
    cmd = SshTarget(host="mid.way", user="gu s", control_dir="/tmp/c m").preauth_command()
    tokens = shlex.split(cmd)  # round-trips: every token quoted, nothing is a shell metacharacter
    assert tokens[-1] == "gu s@mid.way" and "--" in tokens and "ControlPath=/tmp/c m/%C" in tokens


# ---- unknown / changed host key is explained, not swallowed as CANNOT REACH -----------------------------------

def test_unknown_host_key_is_explained_with_the_user_terminal_remedy():
    exc = RuntimeError("remote list failed: Host key verification failed.")
    msg = _explain_provision_error(exc, host="new.host.edu", user="me")
    assert msg.startswith("UNKNOWN HOST KEY for new.host.edu")
    assert "ssh me@new.host.edu" in msg and "Nothing was started" in msg
    assert ".." not in msg  # OpenSSH's line ends with a full stop already (live 2026-09-04)


def test_changed_host_key_says_changed():
    exc = RuntimeError("seed storage.db (mkdir) failed: @@@ WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED! @@@")
    msg = _explain_provision_error(exc, host="anvil.rcac.purdue.edu")
    assert msg.startswith("UNKNOWN HOST KEY") and "CHANGED" in msg


# ---- first contact is visible in the transcript (A2 / C-1) ---------------------------------------------------

def test_first_contact_note_names_user_host_and_env_setup(monkeypatch):
    class _Fac:
        cli = RemoteEndpointCLI(SshTarget(host="anvil.rcac.purdue.edu", user="alice"), "module load python; . ~/v/bin/activate")

    monkeypatch.delenv("HPC_BRIDGE_SSH_HOST", raising=False)
    note = _first_contact_note(_Fac())
    assert note.startswith("first contact over SSH: alice@anvil.rcac.purdue.edu")
    assert "module load python" in note and "pinned" not in note
    monkeypatch.setenv("HPC_BRIDGE_SSH_HOST", "anvil.rcac.purdue.edu")
    assert "pinned by HPC_BRIDGE_SSH_HOST" in _first_contact_note(_Fac())


def test_first_contact_note_is_empty_for_a_facility_without_ssh():
    assert _first_contact_note(object()) == ""


# ---- connect_facility refuses a bad ssh_host before any ssh (A3 / C-2) -----------------------------------------

async def test_connect_facility_refuses_a_bad_ssh_host_without_touching_ssh(monkeypatch):
    from hpc_bridge import connect
    from hpc_bridge.context import AppCtx

    calls = []

    async def spy(*a, **k):
        calls.append(a)
        return (0, "", "")

    monkeypatch.setattr(remote, "ssh_exec", spy)
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.login_flow = None

    async def _no_login(*a, **k):
        raise AssertionError("login must not run")

    res = await connect._connect_facility(app, "x", ssh_host="-oProxyCommand=touch /tmp/p", run_login=_no_login)
    assert res.phase == "failed" and "invalid login host" in (res.notice or "")
    assert calls == []


# ---- the /tmp ControlMaster fallback does not follow a planted symlink (B-04) ---------------------------------

def test_control_dir_fallback_skips_a_symlink_squat(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "_CONTROL_PATH_BUDGET", 60)
    home = tmp_path / "h"
    (home / ".hpc-bridge").mkdir(parents=True)
    victim = tmp_path / "elsewhere"
    victim.mkdir()
    os.symlink(victim, home / ".hpc-bridge" / "cm")  # the attacker's symlink at our predictable name
    monkeypatch.setattr(config.Path, "home", staticmethod(lambda: home))
    chosen = config._short_control_dir("x" * 200)
    assert chosen != str(home / ".hpc-bridge" / "cm")
    assert not (os.path.lexists(chosen) and stat.S_ISLNK(os.lstat(chosen).st_mode))


# ---- registry cache: a definitive miss forgets; only a transport failure serves the offline copy (C-3) ---------

class _NotFound(Exception):
    http_status = 404


class _Client:
    def __init__(self, subject=None, search=None, fail=None):
        self.subject, self.search, self.fail = subject, search, fail

    def get_subject(self, index, subject):
        if self.fail:
            raise self.fail
        if self.subject is None:
            raise _NotFound("404")
        return {"entries": [{"content": self.subject}]}

    def post_search(self, index, q):
        if self.fail:
            raise self.fail
        return {"gmeta": self.search or []}




async def test_registry_404_forgets_the_cached_copy(tmp_path):
    from hpc_bridge.catalog.search import SearchCatalog

    cat = SearchCatalog("idx", _Client(subject=_seed_entry()), tmp_path)
    assert (await cat.get("anvil")) is not None  # live hit: cached under every name it was asked by
    assert any(tmp_path.iterdir())
    for f in tmp_path.iterdir():
        assert (f.stat().st_mode & 0o077) == 0  # 0600
    cat._client = _Client(subject=None)  # the curator retracts it: a clean 404
    assert (await cat.get("anvil")) is None  # not served from the cache…
    assert not (tmp_path / "anvil.json").exists()  # …and the copy is gone


async def test_registry_outage_still_serves_the_cached_copy(tmp_path):
    from hpc_bridge.catalog.search import SearchCatalog

    cat = SearchCatalog("idx", _Client(subject=_seed_entry()), tmp_path)
    assert (await cat.get("anvil")) is not None
    cat._client = _Client(fail=ConnectionError("network down"))
    assert (await cat.get("anvil")) is not None  # offline copy: the one legitimate use of the cache


# ---- supply chain: the server runs against the lockfile; the remote install is pinned (C-6) -------------------

def test_mcp_launch_is_locked():
    c = json.loads(open(".mcp.json").read())  # noqa: SIM115
    args = c["mcpServers"]["endpoint"]["args"]
    assert args[:2] == ["run", "--locked"]


def test_remote_endpoint_install_is_pinned_to_the_sdk_version():
    from hpc_bridge import discovery

    pin = discovery._gce_pin()
    try:
        from importlib.metadata import version

        expected = f"'globus-compute-endpoint=={version('globus-compute-sdk')}'"
    except Exception:  # noqa: BLE001 - hermetic env without the SDK
        expected = "globus-compute-endpoint"
    assert pin == expected and pin in discovery._UV_ENV_SETUP


# ---- the identity that landed is named (B-02) -------------------------------------------------------------------

async def test_logged_in_notice_names_the_identity(monkeypatch):
    from hpc_bridge import login_gate

    monkeypatch.setattr(login_gate, "globus_identity_label", lambda: "alice@uni.edu")
    assert await login_gate._as_identity() == " as alice@uni.edu"
    monkeypatch.setattr(login_gate, "globus_identity_label", lambda: (_ for _ in ()).throw(RuntimeError("offline")))
    assert await login_gate._as_identity() == ""


def test_sync_helpers_are_importable():  # keeps `asyncio`/`subprocess` imports honest for the async tests above
    assert asyncio and subprocess


# ---- teardown wipes ONLY the token store hpc-bridge seeded (B-03, refined) ------------------------------------

def _bootstrap_parts(tmp_path, monkeypatch, *, remote_db_present):
    from hpc_bridge.facility.remote import SlurmFacility
    from hpc_bridge.state import LoginNodeStore
    from tests.test_remote_facility import _BootstrapCLI, _no_endpoints, _profile

    cli = _BootstrapCLI(status=None, remote_db_present=remote_db_present)
    wiped = []

    async def _wipe():
        wiped.append(True)
        return True

    cli.wipe_storage_db = _wipe
    made = tmp_path / "trimmed.db"
    made.write_bytes(b"db")
    monkeypatch.setattr(remote, "build_minimal_storage_db", lambda **kw: made)
    store = LoginNodeStore(tmp_path / "endpoints.json")
    fac = SlurmFacility(_profile(), cli=cli, client_factory=_no_endpoints, store=store, alias="anvil.rcac.purdue.edu")
    return fac, cli, store, wiped


async def test_teardown_wipes_the_store_hpc_bridge_seeded(tmp_path, monkeypatch):
    from hpc_bridge.profile import Profile

    fac, cli, store, wiped = _bootstrap_parts(tmp_path, monkeypatch, remote_db_present=False)
    handle = await fac.bootstrap(Profile(mode="interactive"))
    assert cli.seeded is not None  # we placed it…
    assert store.get(alias="anvil.rcac.purdue.edu", name=fac.profile.endpoint_name).seeded_credentials is True
    await fac.teardown(handle.endpoint_id, wipe_credentials=True)
    assert wiped == [True]  # …so teardown removes it


async def test_teardown_never_wipes_a_store_that_was_already_there(tmp_path, monkeypatch):
    from hpc_bridge.profile import Profile

    fac, cli, _store, wiped = _bootstrap_parts(tmp_path, monkeypatch, remote_db_present=True)
    handle = await fac.bootstrap(Profile(mode="interactive"))
    assert cli.seeded is None  # the login node could already authenticate: not ours
    await fac.teardown(handle.endpoint_id, wipe_credentials=True)
    assert wiped == []  # a shared account's credential survives our teardown


async def test_teardown_in_a_later_session_reads_the_record(tmp_path, monkeypatch):
    from hpc_bridge.facility.remote import SlurmFacility
    from hpc_bridge.profile import Profile
    from tests.test_remote_facility import _BootstrapCLI, _no_endpoints, _profile

    fac, _cli, store, _ = _bootstrap_parts(tmp_path, monkeypatch, remote_db_present=False)
    handle = await fac.bootstrap(Profile(mode="interactive"))
    # a new process: no in-memory flag, only the endpoint record
    cli2 = _BootstrapCLI(status="running", remote_db_present=True)
    wiped = []

    async def _wipe():
        wiped.append(True)
        return True

    cli2.wipe_storage_db = _wipe
    fac2 = SlurmFacility(_profile(), cli=cli2, client_factory=_no_endpoints, store=store, alias="anvil.rcac.purdue.edu")
    await fac2.teardown(handle.endpoint_id, wipe_credentials=True)
    assert wiped == [True]


def test_old_endpoint_records_load_without_the_new_field(tmp_path):
    import json

    from hpc_bridge.state import LoginNodeStore

    p = tmp_path / "endpoints.json"
    p.write_text(json.dumps({"a::n": {"endpoint_id": "e", "login_host": "h", "alias": "a", "user": None,
                                       "key_path": None, "name": "n", "provisioned_at": "t"}}))
    rec = LoginNodeStore(p).get(alias="a", name="n")
    assert rec is not None and rec.seeded_credentials is False  # unknown ⇒ never wipe


# ---- consent is per conversation; a fresh bootstrap is proven and is not a "reuse" (live 2026-09-04) ----------

def _app():
    from hpc_bridge.context import AppCtx
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    return AppCtx(facility=FakeFacility(), profile=Profile())


def test_proven_commit_counts_an_endpoint_this_session_bootstrapped(monkeypatch, tmp_path):
    from hpc_bridge import binding, connect
    from hpc_bridge.models import FacilityDetails
    from hpc_bridge.state import FacilityStore

    store = FacilityStore(tmp_path / "facilities.json")
    monkeypatch.setattr(binding, "_facility_store", lambda: store)
    details = FacilityDetails(ssh_host="h.example.edu", interface="ib0", env_setup="true",
                              scratch_root="/s/{user}", partition="main").model_dump(mode="json")
    app = _app()
    app.state.reused = True  # the just-started endpoint was re-found online on the next connect
    app.bootstrapped_facilities.add("byo")  # …but THIS session started it
    app.pending_facility_cache["byo"] = ("h.example.edu", details)
    connect._commit_proven_facility(app, "byo")
    assert store.get("h.example.edu") is not None  # proven: the canary ran on these details
    app2 = _app()
    app2.state.reused = True  # reattached to an endpoint from BEFORE this session: proves nothing
    app2.pending_facility_cache["x"] = ("other.example.edu", details)
    connect._commit_proven_facility(app2, "x")
    assert store.get("other.example.edu") is None


def test_reuse_note_distinguishes_our_own_fresh_start_from_a_real_reuse():
    from hpc_bridge import connect

    app = _app()
    assert connect._reuse_note(app, "f", True, object()).startswith("reused the already-online endpoint")
    app.bootstrapped_facilities.add("f")
    assert connect._reuse_note(app, "f", True, object()).startswith("reconnected to the endpoint this session started")
    assert connect._reuse_note(app, "f", False, object()) == ""  # no cli on a bare object: no first-contact note


async def test_proposal_notice_forbids_reprobing_and_remembered_consent(monkeypatch):
    from hpc_bridge import connect
    from hpc_bridge.models import FacilityDetails

    draft = FacilityDetails(ssh_host="h.example.edu", interface="ib0", env_setup="true",
                            scratch_root="/s/{user}", partition="main")

    async def fake_discover(target):
        return draft, ["interface: guessed"]

    monkeypatch.setattr(connect, "discover_facility_details", fake_discover)
    res = await connect._propose_or_ask("f", "h.example.edu", "ask")
    assert res.phase == "proposed_facility_details"
    assert "do NOT re-probe" in res.notice and "THIS conversation" in res.notice


async def test_silent_login_path_names_the_identity(monkeypatch):
    from hpc_bridge import binding, connect, login_gate, warmth
    from tests.fakes import FakeFacility

    class _Flow:
        error = None

        def login_required(self):
            return True

    async def landed(flow, mode=None):
        return object(), "done"

    async def ident():
        return " as alice@uni.edu"

    monkeypatch.setattr(login_gate, "_start_login_and_wait", landed)
    monkeypatch.setattr(login_gate, "_as_identity", ident)
    f = FakeFacility()
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)

    async def provision(app, shape, **kw):
        return "provisioning"

    monkeypatch.setattr(warmth, "_provision", provision)
    from hpc_bridge.models import FacilityDetails

    details = FacilityDetails(ssh_host="h.example.edu", interface="ib0", env_setup="true",
                              scratch_root="/s/{user}", partition="main")
    app = _app()
    app.login_flow = _Flow()

    async def _no_login_runner(*a, **k):
        raise AssertionError("not reached")

    res = await connect._connect_facility(app, "byo", details=details, run_login=_no_login_runner)
    assert res.notice.startswith("Globus login landed as alice@uni.edu. "), res.notice


# ---- teardown really deletes, and the tool reports what happened (live 2026-09-04) ------------------------------

async def test_teardown_deletes_the_endpoint_and_its_worker_dirs_and_reports(tmp_path, monkeypatch):
    from hpc_bridge.facility.remote import SlurmFacility
    from hpc_bridge.state import EndpointRecord, LoginNodeStore
    from tests.test_remote_facility import _FakeRemoteCLI, _kinds, _profile

    cli = _FakeRemoteCLI()
    store = LoginNodeStore(tmp_path / "e.json")
    store.put(EndpointRecord(endpoint_id="eid-1", login_host=None, alias="a", user="u", key_path=None,
                             name="hpc-bridge", provisioned_at="t", seeded_credentials=True))
    fac = SlurmFacility(_profile(), cli=cli, store=store, alias="a")
    report = await fac.teardown("eid-1", wipe_credentials=True)
    kinds = _kinds(cli)
    assert kinds.index("stop") < kinds.index("delete") and ("delete", "hpc-bridge") in cli.calls
    assert ("remove_uep_dirs", "eid-1") in cli.calls and ("wipe", "hpc-bridge") in cli.calls
    assert report == {"stopped": True, "deleted": True, "credentials_wiped": True, "ssh_closed": True,
                      "ssh_failed": False, "error": "", "delete_error": ""}  # every word measured (fake: stop rc 0, wipe ok, close ok)
    assert store.get(alias="a", name="hpc-bridge") is None  # the record goes with the endpoint


async def test_bootstrap_persists_the_seeded_flag_even_without_a_routable_pin(tmp_path, monkeypatch):
    from hpc_bridge.facility.remote import SlurmFacility
    from hpc_bridge.profile import Profile
    from hpc_bridge.state import LoginNodeStore
    from tests.test_remote_facility import _BootstrapCLI, _no_endpoints, _profile

    class _NoPinCLI(_BootstrapCLI):
        async def start(self, name):
            self.calls.append(("start", name))
            return ("fake-eid", None, None)  # e.g. `hostname -f` is a single label: nothing routable to pin

    cli = _NoPinCLI(status=None, remote_db_present=False)
    made = tmp_path / "trimmed.db"
    made.write_bytes(b"db")
    monkeypatch.setattr(remote, "build_minimal_storage_db", lambda **kw: made)
    store = LoginNodeStore(tmp_path / "e.json")
    fac = SlurmFacility(_profile(), cli=cli, client_factory=_no_endpoints, store=store, alias="globus1")
    await fac.bootstrap(Profile(mode="interactive"))
    rec = store.get(alias="globus1", name=fac.profile.endpoint_name)
    assert rec is not None and rec.login_host is None and rec.seeded_credentials is True
    # a NEW facility object (connect rebuilds one per call) still knows we seeded it
    fac2 = SlurmFacility(_profile(), cli=_BootstrapCLI(status="running", remote_db_present=True),
                         client_factory=_no_endpoints, store=store, alias="globus1")
    assert fac2._seeded_by_us() is True


async def test_teardown_tool_notice_reflects_the_report(monkeypatch):
    from hpc_bridge import server
    from hpc_bridge.context import AppCtx, EndpointState, ShapeRuntime
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    async def fake_run_shell(a, command, session_id="default", shape="compute"):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 1\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)

    for report, expect in (({"deleted": True, "credentials_wiped": True}, ("deleted", "removed")),
                           ({"deleted": False, "credentials_wiped": False}, ("DELETE FAILED", "no token store of ours")),
                           # the agent told the user the SSH connection was "still open" after teardown (live
                           # 2026-09-04) — it had been closed; the notice now says so
                           ({"deleted": True, "credentials_wiped": True, "ssh_closed": True},
                            ("deleted", "SSH connection to the login node was closed"))):
        class _F(FakeFacility):
            async def teardown(self, eid, *, wipe_credentials=False, _r=report):
                return _r

        app = AppCtx(facility=_F(), profile=Profile(), state=EndpointState(endpoint_id="eid-1"))
        app.shapes["login"] = ShapeRuntime(user_endpoint_config={"provider_type": "LocalProvider"})
        res = await server._teardown_endpoint(app)
        assert all(e in (res.notice or "") for e in expect), res.notice
        if not report.get("ssh_closed"):
            assert "SSH connection" not in (res.notice or "")  # never claim a close that did not happen


def test_bootstrap_timeout_is_explained_not_bare():
    # Expanse live 2026-09-04: the first connect's toolchain install exceeded the ssh timeout and the agent saw
    # "hpc-bridge error: TimeoutError: " — nothing to act on.
    msg = _explain_provision_error(TimeoutError(), host="login.expanse.sdsc.edu")
    assert msg.startswith("TIMED OUT waiting for the login node login.expanse.sdsc.edu")
    assert "toolchain install" in msg and "connect_facility again" in msg


async def test_bootstrap_steps_use_the_long_ssh_timeout(monkeypatch):
    from tests.test_remote_facility import LONG_NAME, WIDE_LIST

    seen = []

    async def fake(target, cmd, **kw):
        seen.append((cmd.split("globus-compute-endpoint ")[-1][:12], kw.get("timeout")))
        return (0, WIDE_LIST if "endpoint list" in cmd else "HPCB_HOST=h\n", "")

    monkeypatch.setattr(remote, "ssh_exec", fake)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    await cli.whoami()
    await cli.configure(LONG_NAME)
    await cli.start(LONG_NAME)
    long = [t for c, t in seen if c.startswith(("whoami", "configure", "start"))]
    assert long and all(t == remote._BOOTSTRAP_SSH_S for t in long), seen
    assert [t for c, t in seen if c.startswith("list")] and all(t != remote._BOOTSTRAP_SSH_S for c, t in seen if c.startswith("list"))


# ---- teardown/delete + seed record hardening (Expanse live, 2026-09-04) ----------------------------------------

async def test_delete_waits_for_stop_to_land_then_forces(monkeypatch):
    from tests.test_remote_facility import LONG_NAME, WIDE_LIST

    calls = []
    state = {"running": 2}  # `status` reads running twice, then stopped

    async def fake(target, cmd, **kw):
        calls.append(cmd)
        if "endpoint list" in cmd:
            if state["running"] > 0:
                state["running"] -= 1
                return (0, WIDE_LIST, "")
            return (0, "No endpoints configured!\n", "")
        if "delete --yes" in cmd and state["running"] > 0:
            return (1, "", "WARNING … Endpoint 3ef7 is currently running.  To proceed with deletion, first stop the endpoint")
        return (0, "> Endpoint deleted", "")

    async def no_sleep(s):
        pass

    monkeypatch.setattr(remote, "ssh_exec", fake)
    monkeypatch.setattr(remote, "_sleep", no_sleep)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    assert await cli.delete(LONG_NAME) is True
    deletes = [c for c in calls if "delete --yes" in c]
    assert len(deletes) == 2 and "--force" not in deletes[-1]  # the daemon exited: no force needed


async def test_delete_forces_when_the_daemon_never_exits(monkeypatch):
    from tests.test_remote_facility import LONG_NAME, WIDE_LIST

    calls = []

    async def fake(target, cmd, **kw):
        calls.append(cmd)
        if "endpoint list" in cmd:
            return (0, WIDE_LIST, "")
        if "--force" in cmd:
            return (0, "deleted", "")
        return (1, "", "Endpoint is currently running.")

    async def no_sleep(s):
        pass

    monkeypatch.setattr(remote, "ssh_exec", fake)
    monkeypatch.setattr(remote, "_sleep", no_sleep)
    cli = RemoteEndpointCLI(SshTarget("h", "u", "k"), "true")
    assert await cli.delete(LONG_NAME) is True
    assert any("--yes --force" in c for c in calls)


async def test_seed_is_recorded_before_provision_so_an_aborted_bootstrap_cannot_orphan_it(tmp_path, monkeypatch):
    from hpc_bridge.facility.remote import SlurmFacility
    from hpc_bridge.profile import Profile
    from hpc_bridge.state import LoginNodeStore
    from tests.test_remote_facility import _BootstrapCLI, _no_endpoints, _profile

    class _DiesAfterSeed(_BootstrapCLI):
        async def configure(self, name, multi_user=False):
            raise TimeoutError()  # the first-connect toolchain install outran the ssh timeout

    cli = _DiesAfterSeed(status=None, remote_db_present=False)
    made = tmp_path / "t.db"
    made.write_bytes(b"db")
    monkeypatch.setattr(remote, "build_minimal_storage_db", lambda **kw: made)
    store = LoginNodeStore(tmp_path / "e.json")
    fac = SlurmFacility(_profile(), cli=cli, client_factory=_no_endpoints, store=store, alias="login.expanse.sdsc.edu")
    with pytest.raises(TimeoutError):
        await fac.bootstrap(Profile(mode="interactive"))
    rec = store.get(alias="login.expanse.sdsc.edu", name=fac.profile.endpoint_name)
    assert rec is not None and rec.seeded_credentials is True and rec.endpoint_id == ""
    # the NEXT run finds the store present (whoami ok) yet still knows it is ours
    fac2 = SlurmFacility(_profile(), cli=_BootstrapCLI(status="running", remote_db_present=True),
                         client_factory=_no_endpoints, store=store, alias="login.expanse.sdsc.edu")
    await fac2.bootstrap(Profile(mode="interactive"))
    assert fac2._seeded_by_us() is True


def test_allocating_notice_hints_at_rejection_on_a_facility_mep_after_five_minutes():
    from hpc_bridge.notices import _allocating_notice

    assert _allocating_notice("gpu-debug", 60, facility_mep=True) == "allocating nodes on 'gpu-debug'…"
    late = _allocating_notice("gpu-debug", 480, facility_mep=True)
    assert "Still allocating after 480s" in late and "scheduler_options" in late
    assert "Still allocating" not in _allocating_notice("main", 480, facility_mep=False)  # SSH facilities can read squeue


def test_expanse_seed_validates_as_an_mfa_ssh_entry():
    import yaml

    from hpc_bridge.catalog.entry import CatalogEntry

    with open("src/hpc_bridge/catalog/seed/sdsc-expanse.yaml") as fh:
        (doc,) = yaml.safe_load(fh)
    e = CatalogEntry.model_validate(doc)
    assert e.ssh_host == "login.expanse.sdsc.edu" and e.auth_method == "mfa-otp" and e.compute_mep_uuid is None
    assert "astral.sh" in e.compute.env_setup and "{venv}" in e.compute.env_setup and e.account_required


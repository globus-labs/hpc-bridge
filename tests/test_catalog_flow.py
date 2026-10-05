from __future__ import annotations

from hpc_bridge import binding, config, connect, warmth
from hpc_bridge.models import FacilityDetails
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _list_facilities, mcp
from tests.fakes import MEP_UUID, FakeCatalog, FakeFacility, fake_entry, fake_mep_entry


class _Res:
    def __init__(self, rc, out, err):
        self.returncode, self.stdout, self.stderr = rc, out, err


class _DoneFuture:
    def __init__(self, res):
        self._res = res

    def done(self):
        return True

    def add_done_callback(self, fn):
        fn(self)

    def cancelled(self):
        return False

    def result(self, timeout=None):
        return self._res


class _FakeRunner:
    def __init__(self, endpoint_id, res, *, canary_result=None, walltime=1780.0, timeout=120.0):
        self.endpoint_id = endpoint_id
        self._res = res
        self.walltime = walltime
        self.timeout = timeout
        self._canary = canary_result or CanaryResult(
            ok=True, worker_host="a070", worker_python="3.11.7", worker_dill="0.3.9"
        )
        self.closed = False
        self.commands: list[str] = []
        self.canaries = 0

    def submit(self, command):
        self.commands.append(command)
        return _DoneFuture(self._res)

    async def run(self, command):
        self.commands.append(command)
        return self._res

    async def canary(self, timeout=8.0):
        self.canaries += 1
        return self._canary

    def close(self):
        self.closed = True


# --- account selection: connect_facility's choice -> the Slurm block --------------------------


async def test_ensure_endpoint_up_provisions_with_selected_account():
    # The chosen allocation flows into the shape's user_endpoint_config (the per-task render var),
    # exactly like the partition selection — account -> SlurmProvider.account.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, account="cis250223", confirm_spend=True)
    assert res.account == "cis250223"
    assert app.shapes["compute"].user_endpoint_config["account"] == "cis250223"


async def test_account_change_invalidates_runner():
    # A different account is a different Slurm block: the cached Executor captured the old account
    # at build time, so the runner must be rebuilt (mirrors the partition-change behaviour).
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    await _ensure_endpoint_up(app, account="cis250223", confirm_spend=True)
    r1 = app.shapes["compute"].runner
    await _ensure_endpoint_up(app, account="cis999999", confirm_spend=True)
    assert app.shapes["compute"].runner is not r1
    assert app.shapes["compute"].user_endpoint_config["account"] == "cis999999"


async def test_ensure_endpoint_up_rejects_invalid_account():
    # account renders into a remote Jinja template -> reject anything outside the allowlist before
    # provisioning (same guard as partition).
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    res = await _ensure_endpoint_up(app, account="bad; rm -rf /")
    assert res.status == "down"
    assert res.notice and "invalid account" in res.notice


async def test_login_shape_ignores_account():
    # A LocalProvider (login) shape charges nothing: a supplied account is not forced onto it.
    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=f, profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""))
    res = await _ensure_endpoint_up(app, shape="login", account="cis250223")
    assert "account" not in app.shapes["login"].user_endpoint_config
    assert res.account is None


# --- list_facilities: browse the catalog (agent-safe, no SSH) ----------------------------------


async def test_list_facilities_returns_summaries(monkeypatch):

    cat = FakeCatalog(
        [fake_entry(id="anvil", facility_key="purdue"), fake_entry(id="polaris", facility_key="alcf")]
    )
    monkeypatch.setattr(binding, "make_catalog", lambda: cat)
    out = await _list_facilities("")
    assert {s.id for s in out} == {"anvil", "polaris"}
    assert [s.id for s in await _list_facilities("polaris")] == ["polaris"]


async def test_list_facilities_summaries_are_agent_safe(monkeypatch):

    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    fields = set(type((await _list_facilities(""))[0]).model_fields)
    # identity/provenance only — never executable config or raw UUIDs.
    assert {"subject", "id", "facility", "provenance", "last_validated"} <= fields
    assert not ({"env_setup", "compute", "transfer_endpoint_uuid", "ssh_host"} & fields)


async def test_server_registers_list_facilities_tool():
    assert "list_facilities" in {t.name for t in await mcp.list_tools()}


# --- connect_facility: bind a machine, bring up its login node, list allocations ---------------

# Real Anvil `mybalance` output (the login-shape allocation command returns this).
MYBALANCE = """
Allocation     Type    SU Limit    SU Usage   SU Usage  SU Balance
Account                           (account)     (user)
=============  ====  ==========  ========== ==========  ==========
cis250223       CPU     10001.0      1014.9      214.4      8986.1
cis250223-gpu   GPU      1000.0         0.0        0.0      1000.0
"""


async def test_connect_facility_brings_up_login_and_lists_allocations(monkeypatch):
    from hpc_bridge import server

    f = FakeFacility()
    f.workers = 1  # manager online; the runner's canary (below) confirms the login worker
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)

    res = await server._connect_facility(app, "anvil")
    assert res.phase == "needs_account"
    assert res.facility == "anvil"
    assert [a.account for a in res.allocations] == ["cis250223", "cis250223-gpu"]
    assert app.facility is f and app.machine == "anvil"  # late-bound the chosen facility
    assert res.reused is False  # fresh bootstrap by default


async def test_connect_facility_signals_reuse_when_endpoint_already_online(monkeypatch):
    # #20: reattaching to an already-online endpoint (find_online_endpoint / status=running) must be
    # SURFACED — the result carries reused=True (a zero-SSH reconnect), not silently swallowed as it
    # was before. The signal threads bootstrap/provision -> EndpointHandle -> EndpointState -> result.
    from hpc_bridge import server

    f = FakeFacility()
    f.workers = 1  # manager online
    f.reused = True  # provision reports it attached to an existing endpoint, didn't start one
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)

    res = await server._connect_facility(app, "anvil")
    assert res.phase == "needs_account"
    assert res.reused is True
    assert res.notice and "reused" in res.notice.lower()  # and narrated for the agent


async def test_connect_facility_unknown_returns_needs_facility_details(monkeypatch):
    # Not in the catalog -> the Socratic fallback: ask for the facility's config via details=,
    # not a dead-end. (The agent then elicits and calls again with details.)
    from hpc_bridge import server

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    monkeypatch.delenv("HPC_BRIDGE_SSH_HOST", raising=False)  # no host -> the ask path, not discovery
    res = await server._connect_facility(app, "nope")
    assert res.phase == "needs_facility_details" and res.allocations == []
    assert res.notice and "details=" in res.notice


async def test_connect_facility_provisioning_when_login_worker_cold(monkeypatch):
    # Manager online but the login worker hasn't answered the canary -> phase="provisioning" (no
    # allocations yet, command never dispatched); the agent calls connect_facility again shortly.
    from hpc_bridge import server
    from hpc_bridge.runner import CanaryResult

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, MYBALANCE, ""), canary_result=CanaryResult(ok=False, error="timeout")
    )
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    res = await server._connect_facility(app, "anvil")
    assert res.phase == "provisioning" and res.allocations == []


async def test_server_registers_connect_facility_tool():
    assert "connect_facility" in {t.name for t in await mcp.list_tools()}


# --- hard failure when the catalog is unavailable (no bundled fallback) ------------------------


def _no_catalog():
    raise OSError("registry unreachable")  # a TRANSPORT failure lists nothing; a bug would now raise (review 2026-09-03)


async def test_connect_facility_index_unavailable_falls_back_to_details(monkeypatch):
    # No index / search scope -> instead of a hard fail, fall back to eliciting the facility config:
    # the agent can still proceed by supplying details=. (Index-down shouldn't block a BYO facility.)
    from hpc_bridge import server

    monkeypatch.setattr(binding, "make_catalog", _no_catalog)
    monkeypatch.delenv("HPC_BRIDGE_SSH_HOST", raising=False)  # no host -> the ask path, not discovery
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await server._connect_facility(app, "anvil")
    assert res.phase == "needs_facility_details"
    assert res.notice and "registry unavailable" in res.notice


async def test_list_facilities_empty_without_catalog(monkeypatch):

    monkeypatch.setattr(binding, "make_catalog", _no_catalog)
    assert await _list_facilities("") == []


# --- regressions from the live test --------------------------------------------------------------


async def test_connect_facility_moves_scratch_root_to_the_facility(monkeypatch):
    # Live-test bug: the server boots LocalFacility (scratch ~/.hpc-bridge -> /Users/...), and
    # connect_facility late-binds the remote facility but must ALSO move the session-shell root,
    # else run_shell uses the local path ON the remote node (`mkdir /Users/...: Permission denied`).
    from hpc_bridge import server

    f = FakeFacility()
    f.workers = 1
    f.scratch_root = "/anvil/scratch/me/.hpc-bridge"  # the bound facility's remote scratch
    app = AppCtx(facility=FakeFacility(), profile=Profile())  # starts with a different (local) facility
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    monkeypatch.delenv("HPC_BRIDGE_SCRATCH", raising=False)
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    await server._connect_facility(app, "anvil")
    assert app.scratch_root == "/anvil/scratch/me/.hpc-bridge"


def test_resolve_scratch_root_expands_home_only_for_local(monkeypatch):
    # A remote facility's `~/…` root must stay VERBATIM — expanding it client-side would bake this
    # machine's home (/Users/…) into a command that runs as another user on another host. The worker
    # expands it (Session.quoted_state_dir). A LocalFacility's worker IS this machine, so expand.
    from pathlib import Path

    from hpc_bridge import server
    from hpc_bridge.facility.local import LocalFacility

    monkeypatch.delenv("HPC_BRIDGE_SCRATCH", raising=False)
    remote = FakeFacility()  # no scratch_root attr -> the "~/.hpc-bridge" default
    assert server._resolve_scratch_root(remote) == "~/.hpc-bridge"
    remote.scratch_root = "$HOME/.hpc-bridge"  # a MEP-style home-relative root: untouched
    assert server._resolve_scratch_root(remote) == "$HOME/.hpc-bridge"
    local = LocalFacility(cli=None)
    assert server._resolve_scratch_root(local) == str(Path.home() / ".hpc-bridge")
    monkeypatch.setenv("HPC_BRIDGE_SCRATCH", "/explicit/root")  # env wins on either kind
    assert server._resolve_scratch_root(remote) == "/explicit/root"


# --- agentic fallback: connect_facility elicits an un-indexed facility (session-local) -----------


def _details(**over):
    base = dict(
        ssh_host="frontier.olcf.ornl.gov",
        interface="hsn0",
        env_setup="module load x && source {venv}/bin/activate",
        scratch_root="/lustre/{user}/.hpc-bridge",
        partition="batch",
        allocation_command="mybalance",
        allocation_parser="mybalance",
    )
    base.update(over)
    return FacilityDetails(**base)


def _byo_app(monkeypatch, f):
    # An app whose catalog has only anvil, with the facility build + runner faked so a supplied
    # facility's login shape "warms" and returns MYBALANCE — mirrors the existing connect tests.

    f.workers = 1
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    return app


async def test_connect_with_details_builds_session_entry_and_reaches_needs_account(monkeypatch):
    from hpc_bridge import server

    app = _byo_app(monkeypatch, FakeFacility())
    res = await server._connect_facility(app, "frontier", details=_details())
    assert res.phase == "needs_account"
    assert [a.account for a in res.allocations] == ["cis250223", "cis250223-gpu"]
    # remembered as a session-local entry so the provisioning loop doesn't re-elicit
    assert "frontier" in app.session_facilities
    assert app.session_facilities["frontier"].provenance == "session"
    assert app.machine == "frontier"


async def test_session_facility_resolves_on_recall_without_details(monkeypatch):
    from hpc_bridge import server

    app = _byo_app(monkeypatch, FakeFacility())
    await server._connect_facility(app, "frontier", details=_details())  # supply once
    res = await server._connect_facility(app, "frontier")  # re-call WITHOUT details
    assert res.phase == "needs_account"  # resolved from session_facilities, not needs_facility_details


async def test_connect_details_without_allocation_asks_for_account(monkeypatch):
    from hpc_bridge import server

    app = _byo_app(monkeypatch, FakeFacility())
    res = await server._connect_facility(
        app, "frontier", details=_details(allocation_command=None, allocation_parser=None)
    )
    assert res.phase == "needs_account" and res.allocations == []
    assert res.notice and "account" in res.notice
    assert app.session_facilities["frontier"].allocation is None


async def test_connect_details_unsupported_parser_is_unsupported(monkeypatch):
    from hpc_bridge import server

    app = _byo_app(monkeypatch, FakeFacility())
    res = await server._connect_facility(app, "frontier", details=_details(allocation_parser="sbank"))
    assert res.phase == "unsupported" and "sbank" in (res.notice or "")


async def test_connect_index_error_with_details_proceeds(monkeypatch):
    # Index down + details supplied -> build the session entry and proceed, don't hard-stop.
    from hpc_bridge import server

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    monkeypatch.setattr(binding, "make_catalog", _no_catalog)  # raises
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    res = await server._connect_facility(app, "frontier", details=_details())
    assert res.phase == "needs_account" and "frontier" in app.session_facilities


# --- agentic discovery: probe the login node and PROPOSE a draft (don't dump the form) -----------


async def test_connect_unknown_with_ssh_host_proposes_discovered_details(monkeypatch):
    from hpc_bridge import server
    from hpc_bridge.models import FacilityDetails

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    draft = FacilityDetails(
        ssh_host="login.newfac.edu", interface="ib0", env_setup="source {venv}/bin/activate",
        scratch_root="/scratch/{user}/.hpc-bridge", partition="debug",
    )

    async def fake_discover(target):
        assert target.host == "login.newfac.edu"  # bare SshTarget built from ssh_host
        return draft, ["interface: proposed `ib0` — CONFIRM"]

    monkeypatch.setattr(connect, "discover_facility_details", fake_discover)
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setenv("HPC_BRIDGE_SSH_KEY", "/k")
    monkeypatch.setattr(config, "_control_settings", lambda: (None, 60))  # no real socket dir in tests
    res = await server._connect_facility(app, "newfac", ssh_host="login.newfac.edu")
    assert res.phase == "proposed_facility_details"
    assert res.proposed_details is draft and res.proposed_details.interface == "ib0"
    assert res.notice and "interface" in res.notice
    assert "newfac" not in app.session_facilities  # propose only; register on confirmation (details=)


async def test_connect_unknown_needs_preauth_when_host_wants_a_password(monkeypatch):
    # An un-indexed host needing a password/MFA: discovery raises NeedsPreauth -> phase=needs_preauth
    # carrying the user's pre-open command. The agent relays it; neither hpc-bridge nor the agent
    # handle the secret (MFA, issue #3).
    from hpc_bridge import server
    from hpc_bridge.facility.remote import NeedsPreauth

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    monkeypatch.setattr(config, "_control_settings", lambda: ("/tmp/cm", 3600))  # multiplexing on

    async def fake_discover(target):
        raise NeedsPreauth(target)

    monkeypatch.setattr(connect, "discover_facility_details", fake_discover)
    res = await server._connect_facility(app, "midway", ssh_host="midway.rcc.uchicago.edu")
    assert res.phase == "needs_preauth"
    assert res.preauth_command and res.preauth_command.startswith("ssh -fN ")
    assert "ControlMaster=yes" in res.preauth_command and "BatchMode" not in res.preauth_command
    assert res.notice and "own terminal" in res.notice.lower()
    assert "midway" not in app.session_facilities  # nothing registered until they authenticate + confirm


async def test_connect_needs_preauth_flags_multiplexing_off(monkeypatch):
    # No shareable master when multiplexing is off -> tell the user to enable it (no command to run).
    from hpc_bridge import server
    from hpc_bridge.facility.remote import NeedsPreauth

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    monkeypatch.setattr(config, "_control_settings", lambda: (None, 0))  # multiplexing OFF

    async def fake_discover(target):
        raise NeedsPreauth(target)

    monkeypatch.setattr(connect, "discover_facility_details", fake_discover)
    res = await server._connect_facility(app, "midway", ssh_host="midway.rcc.uchicago.edu")
    assert res.phase == "needs_preauth" and res.preauth_command is None
    assert "multiplexing is off" in (res.notice or "").lower()


async def test_explicit_details_rebuild_overrides_cached_entry(monkeypatch):
    # An explicit details= must REBUILD + overwrite the cached session entry, so a discovery mistake
    # (e.g. a wrong scratch_root) is fixable in-session — previously the first (even failed) call's
    # entry silently won and stranded the run (seen live on Midway).
    from hpc_bridge import server

    f = FakeFacility()
    f.workers = 1
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    real = server._entry_from_details
    built = []
    monkeypatch.setattr(
        binding, "_entry_from_details",
        lambda facility, details: (built.append(details.scratch_root), real(facility, details))[1],
    )

    await server._connect_facility(app, "midway", details=_details(scratch_root="/scratch/midway3/.hpc-bridge"))
    e1 = app.session_facilities["midway"]
    await server._connect_facility(
        app, "midway", details=_details(scratch_root="/scratch/midway3/{user}/.hpc-bridge"))
    assert built == ["/scratch/midway3/.hpc-bridge", "/scratch/midway3/{user}/.hpc-bridge"]  # both rebuilt
    assert app.session_facilities["midway"] is not e1  # the corrected entry replaced the cached one


def test_session_endpoint_name_keyed_on_ssh_host():
    from hpc_bridge.server import _session_endpoint_name
    assert _session_endpoint_name("midway3") == "hpc-bridge-midway3"
    assert _session_endpoint_name("midway3.rcc.uchicago.edu") == "hpc-bridge-midway3-rcc-uchicago-edu"
    assert _session_endpoint_name("") == "hpc-bridge-session"  # never a bare 'hpc-bridge' (would collide)


def test_facility_store_roundtrip(tmp_path):
    from hpc_bridge.state import FacilityStore
    s = FacilityStore(tmp_path / "facilities.json")
    assert s.get("midway3") is None
    s.put("midway3", {"ssh_host": "midway3", "interface": "bond0"})
    s.put("anvil", {"ssh_host": "anvil"})
    assert s.get("midway3") == {"ssh_host": "midway3", "interface": "bond0"}
    s.remove("midway3")
    assert s.get("midway3") is None and s.get("anvil") is not None  # surgical
    assert oct((tmp_path / "facilities.json").stat().st_mode)[-3:] == "600"  # config, 0600


async def test_local_discovery_reuses_cached_config_without_probing(monkeypatch):
    # After a facility is confirmed once, a FRESH session resolves the config from the persistent
    # cache with NO SSH probe (local discovery). (The autouse fixture isolates the cache to tmp.)
    from hpc_bridge import server

    f = FakeFacility()
    f.workers = 1
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([]))  # not catalogued

    # 1) confirm details -> persists to the cache keyed by ssh_host
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    await server._connect_facility(app, "midway3", details=_details(ssh_host="midway3"))
    assert server._facility_store().get("midway3") is not None  # cached by ssh_host

    # 2) FRESH session (new AppCtx, empty session cache); reconnect by ssh_host, NO details -> MUST NOT probe
    async def no_probe(target):
        raise AssertionError("must not probe — the config is cached (local discovery)")

    monkeypatch.setattr(connect, "discover_facility_details", no_probe)
    app2 = AppCtx(facility=FakeFacility(), profile=Profile())
    app2.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, MYBALANCE, ""))
    res = await server._connect_facility(app2, "midway3", ssh_host="midway3")
    assert res.phase in ("needs_account", "provisioning")  # resolved straight from cache, brought up
    assert "midway3" in app2.session_facilities  # config loaded from the local cache, no probe


async def test_connect_unknown_uses_ssh_host_from_env(monkeypatch):
    from hpc_bridge import server
    from hpc_bridge.models import FacilityDetails

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    seen = {}

    async def fake_discover(target):
        seen["host"] = target.host
        return (
            FacilityDetails(ssh_host=target.host, interface="ib0", env_setup="x",
                            scratch_root="/s/{user}", partition="p"),
            [],
        )

    monkeypatch.setattr(connect, "discover_facility_details", fake_discover)
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    monkeypatch.setenv("HPC_BRIDGE_SSH_KEY", "/k")
    monkeypatch.setenv("HPC_BRIDGE_SSH_HOST", "envhost")
    monkeypatch.setattr(config, "_control_settings", lambda: (None, 60))
    res = await server._connect_facility(app, "newfac")  # no ssh_host param -> env fallback
    assert res.phase == "proposed_facility_details" and seen["host"] == "envhost"


async def test_connect_unknown_without_host_asks_for_access(monkeypatch):
    from hpc_bridge import server

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    monkeypatch.delenv("HPC_BRIDGE_SSH_HOST", raising=False)
    res = await server._connect_facility(app, "newfac")  # no host, no details -> ask for access
    assert res.phase == "needs_facility_details"
    assert res.notice and "SSH host" in res.notice


async def test_connect_unknown_discovery_defers_creds_to_ssh_config(monkeypatch):
    # The transcript bug: discovery hard-required HPC_BRIDGE_SSH_USER/KEY env, which a running server
    # can't be given mid-session. Now it builds a config-deferring target — no env creds needed.
    from hpc_bridge import server
    from hpc_bridge.models import FacilityDetails

    app = AppCtx(facility=FakeFacility(), profile=Profile())
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")])
    )
    seen = {}

    async def fake_discover(target):
        seen["user"], seen["key"] = target.user, target.key_path
        return (
            FacilityDetails(ssh_host=target.host, interface="ib0", env_setup="x",
                            scratch_root="/s/{user}", partition="p"),
            [],
        )

    monkeypatch.setattr(connect, "discover_facility_details", fake_discover)
    monkeypatch.setattr(config, "_control_settings", lambda: (None, 60))
    for v in ("HPC_BRIDGE_SSH_USER", "HPC_BRIDGE_SSH_KEY"):
        monkeypatch.delenv(v, raising=False)
    res = await server._connect_facility(app, "newfac", ssh_host="globus1")
    assert res.phase == "proposed_facility_details"
    assert seen["user"] is None and seen["key"] is None  # deferred to ~/.ssh/config, zero env creds


def test_entry_from_details_builds_session_local_entry():
    from hpc_bridge.server import _entry_from_details

    e = _entry_from_details("frontier", _details(display_name="Frontier (BYO)"))
    assert e.id == "frontier" and e.provenance == "session"
    assert e.transfer_endpoint_uuid is None and e.subject == "session:frontier"
    assert e.compute.interface == "hsn0" and e.compute.scheduler == "slurm"
    assert e.allocation is not None and e.allocation.parser == "mybalance"
    assert e.compute.endpoint_name == "hpc-bridge-frontier-olcf-ornl-gov"  # keyed on ssh_host, not facility id
    assert e.display_name == "Frontier (BYO)"  # explicit display_name preserved
    # no allocation tool -> allocation omitted
    bare = _entry_from_details("x", _details(ssh_host="zeta", allocation_command=None, allocation_parser=None))
    assert bare.allocation is None
    assert bare.display_name == "hpc-bridge-zeta"  # UI title = the ssh-host-keyed endpoint name (not the id "x")
    # facility id is slugified into the endpoint name; an explicit name is preserved (override)
    assert _entry_from_details("x", _details(ssh_host="uchicago_globus")).compute.endpoint_name == "hpc-bridge-uchicago-globus"
    assert _entry_from_details("frontier", _details(endpoint_name="my-ep")).compute.endpoint_name == "my-ep"
    # PBS facility with an explicit cpus_per_node flows through to defaults (full-node compute blocks)
    pbs = _entry_from_details(
        "polaris",
        _details(scheduler="pbs", cpus_per_node=32,
                 scheduler_options="#PBS -l filesystems=home:eagle"),
    )
    assert pbs.defaults.cpus_per_node == 32
    # scheduler_options rides onto compute so a session PBS facility can set required site directives
    assert pbs.compute.scheduler_options == "#PBS -l filesystems=home:eagle"


def test_entry_from_details_endpoint_name_env_override(monkeypatch):
    # HPC_BRIDGE_ENDPOINT_NAME (the harness's per-run isolation override) wins over BOTH the ssh-host
    # default AND an agent-supplied endpoint_name — so concurrent test runs (which share ONE Globus
    # identity) never collide on one registration, and a flailing agent can't defeat run isolation.
    from hpc_bridge.server import _entry_from_details
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-globus1-run-42")
    assert _entry_from_details("x", _details(ssh_host="globus1")).compute.endpoint_name == "hpc-bridge-globus1-run-42"
    # wins even over an agent-supplied name (we saw an agent fabricate one mid-flail)
    assert _entry_from_details("x", _details(endpoint_name="hpc-bridge-recover-00")).compute.endpoint_name == "hpc-bridge-globus1-run-42"
    # unset -> product default (ssh-host key) is unchanged
    monkeypatch.delenv("HPC_BRIDGE_ENDPOINT_NAME", raising=False)
    assert _entry_from_details("x", _details(ssh_host="globus1")).compute.endpoint_name == "hpc-bridge-globus1"



def test_ssh_host_env_is_scoped_to_the_pin_path(monkeypatch):
    # #35: HPC_BRIDGE_SSH_HOST must NOT override an agent-BOUND facility's ssh_host on the connect
    # path — that silently redirected globus1's config to Aurora's login node ("globus1 is Aurora").
    # It overrides ONLY on the env-pinned startup path, via `pinned_host`.
    from hpc_bridge.server import _facility_from_entry
    monkeypatch.setenv("HPC_BRIDGE_SSH_HOST", "aurora")
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")  # skip the ssh -G user lookup
    entry = fake_entry(id="globus1", facility_key="lab")  # entry.ssh_host = "globus1.example.edu"
    # agentic connect path (pinned_host omitted -> None): the bound facility's ssh_host wins
    assert _facility_from_entry(entry, account="").alias == "globus1.example.edu"
    # startup-pin path (pinned_host set from the env): the env host overrides the catalog's host
    assert _facility_from_entry(entry, account="", pinned_host="aurora").alias == "aurora"


# --- M1: a compute_mep_uuid entry builds a MEPFacility (zero SSH) ----------------------------------


def test_mep_entry_is_supported_unless_it_lists_allocations():
    from hpc_bridge.server import _unsupported_entry_reason
    assert _unsupported_entry_reason(fake_mep_entry()) is None  # the old "not wired yet" reject is gone
    # an allocation LISTING needs the free login node, which a MEP doesn't have -> refuse, clearly
    reason = _unsupported_entry_reason(
        fake_mep_entry(allocation={"command": "mybalance", "parser": "mybalance"})
    )
    assert reason and "allocation" in reason and "login" in reason


def test_facility_from_mep_entry_builds_mepfacility_with_no_ssh(monkeypatch):
    from hpc_bridge.facility.mep import MEPFacility
    from hpc_bridge.server import _facility_from_entry
    # if the SSH user lookup ran we'd see it; a MEP must never consult ~/.ssh/config
    monkeypatch.setattr("hpc_bridge.binding._ssh_config_user", lambda alias: (_ for _ in ()).throw(AssertionError("SSH lookup on a MEP")))
    fac = _facility_from_entry(fake_mep_entry(), account="")
    assert isinstance(fac, MEPFacility)
    assert fac.endpoint_id == MEP_UUID
    assert fac.supported_shapes == ("compute",)
    assert fac.scratch_root == "$HOME/.hpc-bridge"  # worker-side, untouched
    assert fac.account_required is False
    _, uec = fac.config_template(Profile())
    # the entry's compute/defaults split maps onto the MEP schema; no account seeded (per-user)
    assert uec["worker_init"].endswith("==4.15.0") and uec["interface"] == "enP7s7"
    assert uec["partition"] == "main" and uec["init_blocks"] == 1 and uec["walltime"] == "02:00:00"
    assert "account" not in uec and "compute" not in uec  # shape_config('compute') adds compute=True


def test_mep_wins_when_an_entry_carries_both_reaches(monkeypatch):
    from hpc_bridge.facility.mep import MEPFacility
    from hpc_bridge.server import _facility_from_entry
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "u")
    both = fake_mep_entry(ssh_host="globus1.example.edu", compute={
        "scheduler": "slurm", "interface": "enP7s7", "env_setup": "x", "scratch_root": "$HOME/.hpc-bridge",
    })
    assert isinstance(_facility_from_entry(both, account=""), MEPFacility)


async def test_startup_pin_on_a_mep_entry(monkeypatch):
    # HPC_BRIDGE_MACHINE=<mep entry>: no HPC_BRIDGE_ACCOUNT needed when the entry says so, and a stray
    # HPC_BRIDGE_ENDPOINT_ID that disagrees with the entry's UUID is refused (not silently preferred)
    import pytest

    from hpc_bridge import server
    from hpc_bridge.facility.mep import MEPFacility
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_mep_entry()]))
    monkeypatch.delenv("HPC_BRIDGE_ACCOUNT", raising=False)
    monkeypatch.delenv("HPC_BRIDGE_ENDPOINT_ID", raising=False)
    assert isinstance(await server._catalog_facility("globus1"), MEPFacility)
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_ID", "11111111-2222-3333-4444-555555555555")
    with pytest.raises(RuntimeError, match="conflicts"):
        await server._catalog_facility("globus1")
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_ID", MEP_UUID)  # equal -> fine
    assert isinstance(await server._catalog_facility("globus1"), MEPFacility)
    # an account-REQUIRED MEP entry still insists on HPC_BRIDGE_ACCOUNT at startup
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_mep_entry(account_required=True)]))
    with pytest.raises(RuntimeError, match="HPC_BRIDGE_ACCOUNT"):
        await server._catalog_facility("globus1")


async def test_registry_wins_over_a_stale_cached_config_for_a_catalogued_id(monkeypatch):
    # Decision 2026-09-03: curated registry entries are the stable ones. Found live: the maintainer's
    # local cache held an SSH-era `globus1` config that would have shadowed the registry's MEP entry
    # and silently taken the SSH path. (The autouse fixture isolates the cache to tmp.)
    from hpc_bridge import server
    from tests.fakes import MEP_UUID, fake_mep_entry

    server._facility_store().put("globus1", _details(ssh_host="globus1").model_dump(mode="json"))
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_mep_entry(id="globus1")]))
    seen = []

    def stop(entry, *, account):
        seen.append(entry)
        raise RuntimeError("stop here")

    monkeypatch.setattr(binding, "_facility_from_entry", stop)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await server._connect_facility(app, "globus1")
    assert res.phase == "failed" and "stop here" in (res.notice or "")
    assert len(seen) == 1 and seen[0].compute_mep_uuid == MEP_UUID and seen[0].ssh_host is None  # the REGISTRY entry


async def test_cached_config_still_serves_when_the_registry_is_unreachable(monkeypatch):
    from hpc_bridge import server

    server._facility_store().put("midway3", _details(ssh_host="midway3").model_dump(mode="json"))

    def down():
        raise RuntimeError("search down")

    monkeypatch.setattr(binding, "make_catalog", down)
    seen = []

    def stop(entry, *, account):
        seen.append(entry)
        raise RuntimeError("stop here")

    monkeypatch.setattr(binding, "_facility_from_entry", stop)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await server._connect_facility(app, "midway3")
    assert len(seen) == 1 and seen[0].ssh_host == "midway3"  # the cache served; no probe, no 'registry unavailable'
    assert res.phase == "failed" and "stop here" in (res.notice or "")


async def test_registry_unreachable_and_nothing_cached_asks_for_a_host(monkeypatch):
    from hpc_bridge import server

    def down():
        raise RuntimeError("search down")

    monkeypatch.setattr(binding, "make_catalog", down)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await server._connect_facility(app, "nowhere")
    assert res.phase == "needs_facility_details" and "registry unavailable" in (res.notice or "")


def test_control_dir_falls_back_to_a_short_path(monkeypatch, tmp_path):
    # ssh expands %C to 40 hex chars and caps the whole socket path (~104 bytes): a deep state dir made
    # every SSH fail with "ControlPath too long" before auth (stranger's walk, 2026-09-03).
    from hpc_bridge import server
    deep = tmp_path / ("d" * 60) / ("e" * 30)
    monkeypatch.setenv("HPC_BRIDGE_STATE_DIR", str(deep))
    monkeypatch.setenv("HPC_BRIDGE_SSH_CONTROL_PERSIST", "60")
    cd, _persist = server._control_settings()
    assert cd is not None and len(cd) + 1 + 40 <= 100 and str(deep) not in cd
    assert server._short_control_dir("/short/cm") == "/short/cm"


async def test_no_ssh_access_is_explained_not_dumped(monkeypatch):
    # A stranger with no account/key on the facility got 'seed storage.db (mkdir) failed: u@h: Permission
    # denied (publickey,…)' — an internal step name + a bare ssh error. Name the host, the login name and
    # its source, and the remedies instead.
    from types import SimpleNamespace

    from hpc_bridge import server

    f = FakeFacility()
    f.alias = "anvil.rcac.purdue.edu"
    f.cli = SimpleNamespace(target=SimpleNamespace(host="anvil.rcac.purdue.edu", user="hpcbridge-stranger"))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: f)
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_entry(id="anvil", facility_key="purdue")]))
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-stranger")

    async def refused(app, shape, **kw):
        raise RuntimeError("seed storage.db (mkdir) failed: hpcbridge-stranger@anvil.rcac.purdue.edu: "
                           "Permission denied (publickey,gssapi-keyex,gssapi-with-mic,keyboard-interactive,hostbased).")

    monkeypatch.setattr(warmth, "_provision", refused)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await server._connect_facility(app, "anvil")
    assert res.phase == "failed"
    assert res.notice.startswith("NO SSH ACCESS to anvil.rcac.purdue.edu")
    assert "'hpcbridge-stranger'" in res.notice and "HPC_BRIDGE_SSH_USER" in res.notice
    assert "seed storage.db" not in res.notice and "Nothing was started" in res.notice


def test_explain_provision_error_classes():
    from types import SimpleNamespace

    from hpc_bridge.server import _explain_provision_error
    fac = SimpleNamespace(alias="h.example.edu", cli=SimpleNamespace(target=SimpleNamespace(host="h.example.edu", user=None)))
    assert _explain_provision_error(RuntimeError("x failed: ssh: Could not resolve hostname h.example.edu"), fac).startswith("CANNOT REACH h.example.edu")
    assert "as your local username" in _explain_provision_error(RuntimeError("u@h: Permission denied (publickey)"), fac)
    assert "ControlMaster socket path is too long" in _explain_provision_error(RuntimeError("seed failed: ControlPath too long ('/x' >= 104 bytes)"), fac)
    assert _explain_provision_error(NotImplementedError("provisioning needs Linux"), fac).startswith("hpc-bridge error: NotImplementedError")


async def test_probe_path_ssh_refusal_is_explained(monkeypatch):
    # stranger's walk, BYO host: the PROBE (before any bootstrap) failed with a raw
    # "discovery probe failed (rc=255): Warning: Identity file … Permission denied (publickey)".
    from hpc_bridge import server

    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([]))
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-stranger")

    async def refused(target):
        raise RuntimeError("discovery probe failed (rc=255): Warning: Identity file /nonexistent-key not "
                           "accessible: No such file or directory.\nhpcbridge-stranger@h.example.edu: "
                           "Permission denied (publickey).")

    monkeypatch.setattr(connect, "discover_facility_details", refused)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    res = await server._connect_facility(app, "byo", ssh_host="h.example.edu")
    assert res.phase == "failed" and res.notice.startswith("NO SSH ACCESS to h.example.edu")
    assert "'hpcbridge-stranger'" in res.notice and "Identity file" not in res.notice and "rc=255" not in res.notice

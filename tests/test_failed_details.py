"""A details= config whose bring-up FAILED on the login node is not run again by a connect without details.

Live 2026-10-16 (free sweep, fake `lmod`, `lmod_bootstrap`, hermes21): the agent's first connect carried a made-up
env_setup (`module load gcc/10.2.0`) and the remote configure failed with Lmod's "module(s) are unknown". The refusal
it got next said to call connect_facility(…, ssh_host=…) without details for a proposed config; it did, and got the
SAME Lmod error back: the session had kept the details= entry before the bring-up proved it, and the connect without
details re-ran it. Now such a config is dropped (connect._drop_failed_details) and the next connect without details
resolves the facility afresh (registry → a config proven for that host → a probe that proposes one), opening its
answer with what failed. Each other state keeps working: a bring-up still coming up keeps polling, a proven config is
reused, a failure outside the config (SSH, the network, a timeout, the login) and a one-time-code handoff keep it for
the retry they ask for, and a registry entry is unchanged.

Review round 1 (2026-10-20) made the decision POSITIVE: only a step that ran through env_setup and was refused on the
login node (`ConfigStepFailed`, never ssh's own 255) whose words name no network, timeout, Globus, registration or
full-disk cause drops a config; ensure_endpoint_up and run_shell refuse to bring a dropped config up; a re-sent config
equal to the one proven for that host is kept; and a connect without details that waited on the lock behind the
failing bring-up resolves again.
"""
from __future__ import annotations

import pytest

from hpc_bridge import binding, config, connect, notices, server, warmth
from hpc_bridge.context import AppCtx, DroppedConfigError
from hpc_bridge.facility import endpoint_venv as ev
from hpc_bridge.facility.base import ConfigStepFailed, step_error
from hpc_bridge.facility.remote import RemoteEndpointCLI, RemoteTokenStoreExists, SshTarget
from hpc_bridge.login import LoginExpiredError
from hpc_bridge.models import FacilityDetails
from hpc_bridge.profile import Profile
from tests.fakes import FakeCatalog, FakeFacility, fake_entry

BAD = "module load gcc/10.2.0"   # the made-up line of 2026-10-16
GOOD = "type module >/dev/null 2>&1 || . /etc/profile.d/lmod.sh; module load uv/0.12.9; . {venv}/bin/activate"
LMOD = ('remote configure failed: Lmod has detected the following error: The following module(s) are unknown:\n'
        '"gcc/10.2.0"\n\nPlease check the spelling or version number. Also try "module spider ..."\nIt is also '
        'possible your cache file is out-of-date; it may help to try:\n  $ module --ignore_cache load "gcc/10.2.0"')
HOST = "login"
REAL_PROVISION = warmth._provision   # World replaces it; the refusal of a dropped config lives in the real one


class _Fac(FakeFacility):
    """The facility a connect binds: remembers the env_setup of the entry it was built from."""

    def __init__(self, env_setup: str) -> None:
        super().__init__()
        self.env_setup = env_setup


class World:
    """The fakes one connect flow runs against: the registry, the facility build, the bring-up and the probe."""

    def __init__(self, monkeypatch, *, catalog=(), outcome: str = "warm") -> None:
        self.built: list[str] = []          # env_setup of every facility a connect built (a bring-up's config)
        self.brought_up: list[str] = []     # env_setup of every bring-up attempted (warmth._provision)
        self.probed: list[str] = []         # the host of every discovery probe
        self.outcome = outcome              # what a bring-up whose env_setup is not BAD answers
        self.fail: dict[str, BaseException] = {BAD: ConfigStepFailed(LMOD)}   # as RemoteEndpointCLI.configure   # env_setup -> what its bring-up raises
        self.target: SshTarget | None = None
        monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog(list(catalog)))
        monkeypatch.setattr(config, "_control_settings", lambda: (None, 60))
        monkeypatch.setattr(binding, "_facility_from_entry", self._build)
        monkeypatch.setattr(warmth, "_provision", self._provision)
        monkeypatch.setattr(connect, "discover_facility_details", self._discover)

    def _build(self, entry, *, account):
        self.built.append(entry.compute.env_setup)
        fac = _Fac(entry.compute.env_setup)
        if self.target is not None:
            from types import SimpleNamespace
            fac.cli = SimpleNamespace(target=self.target)
        return fac

    async def _provision(self, app, shape, **kw):
        es = app.facility.env_setup
        self.brought_up.append(es)
        if es in self.fail:
            raise self.fail[es]
        return self.outcome

    async def _discover(self, target):
        self.probed.append(target.host)
        return (FacilityDetails(ssh_host=target.host, interface="eth0", env_setup=GOOD,
                                scratch_root="/home/{user}/.hpc-bridge", partition="debug"),
                ["interface: proposed `eth0` — CONFIRM", "env_setup: Lmod found; module load uv/0.12.9"])


def _anvil():
    """A registry SSH entry, without its allocation listing (the login runner is not what these tests are about)."""
    return fake_entry(id="anvil", facility_key="purdue").model_copy(update={"allocation": None})


def _app() -> AppCtx:
    return AppCtx(facility=FakeFacility(), profile=Profile())


def _details(env_setup: str | None = BAD, host: str = HOST) -> FacilityDetails:
    return FacilityDetails(ssh_host=host, interface="ib0", env_setup=env_setup,
                           scratch_root="/scratch/{user}/.hpc-bridge", partition="debug")


# ---- the lmod case: a failed details=, then a connect without details --------------------------------------------------

@pytest.mark.parametrize("named_host", [True, False])
async def test_a_failed_details_bring_up_is_not_run_again_by_a_connect_without_details(monkeypatch, named_host):
    w, app = World(monkeypatch), _app()
    res = await server._connect_facility(app, "lmodfac", ssh_host=HOST, details=_details())
    assert res.phase == "failed" and "remote configure failed: Lmod" in (res.notice or "")
    # the failure says what the call its advice leads to will do — and that it will not run this config
    assert ("This configuration (details=) is not kept: connect_facility('lmodfac', ssh_host='login') without details "
            "resolves the facility afresh") in res.notice
    # with details= it runs what is given: this one again, or a corrected one — and an env_setup left out is the latest
    # proposal's, which is what re-sending it takes (never "this one as it is" when a fresh proposal came in between)
    assert ("and does not run this one; with details= it runs the configuration given — this one again, or a corrected "
            "one (an env_setup left out of details= comes from the latest proposal for this facility).") in res.notice
    assert "lmodfac" not in app.session_facilities and "lmodfac" not in app.pending_facility_cache
    # the call the advice names: a probe and a FRESH proposal, never the failed config again
    again = await server._connect_facility(app, "lmodfac", **({"ssh_host": HOST} if named_host else {}))
    assert again.phase == "proposed_facility_details", again.notice
    assert w.brought_up == [BAD] and w.probed == [HOST]       # no second bring-up; the dropped config's host probed
    assert again.proposed_details is not None and again.proposed_details.env_setup == GOOD
    lead = ("The details= given earlier for 'lmodfac' failed to come up (hpc-bridge error: ConfigStepFailed: remote "
            "configure failed: Lmod has detected the following error")
    assert again.notice.startswith(lead) and "is not kept: this connect did not run it." in again.notice
    assert "gcc/10.2.0" in again.notice and "probed the login node and proposed this config" in again.notice
    assert "\n" not in again.notice.split("probed the login node")[0]   # the failure's own text, made one line
    assert len(again.notice) <= connect._PROPOSAL_CAP + len(again.notice.split("probed the login node")[0])


async def test_confirming_the_fresh_proposal_brings_it_up_and_caches_it_not_the_failed_one(monkeypatch):
    w, app = World(monkeypatch), _app()
    await server._connect_facility(app, "lmodfac", ssh_host=HOST, details=_details())
    await server._connect_facility(app, "lmodfac", ssh_host=HOST)                 # the fresh proposal
    up = await server._connect_facility(app, "lmodfac", details=_details(env_setup=None))  # confirmed, env_setup omitted
    assert up.phase == "needs_account", up.notice
    assert w.brought_up == [BAD, GOOD]
    assert "lmodfac" not in app.failed_details and not (up.notice or "").startswith("The details= given earlier")
    assert binding._facility_store().get(HOST)["env_setup"] == GOOD             # proven: the proposal's line, cached
    # and from here on a connect without details reconnects with it (no probe), as before
    assert (await server._connect_facility(app, "lmodfac", ssh_host=HOST)).phase == "needs_account"
    assert w.probed == [HOST] and w.brought_up == [BAD, GOOD, GOOD]


async def test_the_failure_is_said_until_a_new_details_replaces_it(monkeypatch):
    w, app = World(monkeypatch), _app()
    await server._connect_facility(app, "lmodfac", ssh_host=HOST, details=_details())
    for _ in range(2):   # every connect without details says it, and none runs the dropped config
        again = await server._connect_facility(app, "lmodfac", ssh_host=HOST)
        assert again.notice.startswith("The details= given earlier for 'lmodfac' failed to come up")
    assert w.brought_up == [BAD]
    # a retry AS IT IS is the user's explicit call (details= again): it runs, fails again, and is dropped again
    retry = await server._connect_facility(app, "lmodfac", details=_details())
    assert retry.phase == "failed" and w.brought_up == [BAD, BAD] and "lmodfac" in app.failed_details
    # a different config replaces it: the record goes the moment it is given, whatever becomes of it
    w.outcome = "provisioning"
    other = await server._connect_facility(app, "lmodfac", details=_details(env_setup="module load uv/0.12.9"))
    assert other.phase == "provisioning" and "lmodfac" not in app.failed_details
    assert not (other.notice or "").startswith("The details= given earlier")


# ---- the states that must keep working ---------------------------------------------------------------------------------

async def test_a_details_bring_up_still_coming_up_keeps_polling_without_details(monkeypatch):
    w, app = World(monkeypatch, outcome="provisioning"), _app()
    assert (await server._connect_facility(app, "byo", details=_details(GOOD))).phase == "provisioning"
    assert (await server._connect_facility(app, "byo", ssh_host=HOST)).phase == "provisioning"
    assert (await server._connect_facility(app, "byo")).phase == "provisioning"
    assert w.probed == [] and w.brought_up == [GOOD] * 3 and "byo" in app.pending_facility_cache
    w.outcome = "warm"
    assert (await server._connect_facility(app, "byo")).phase == "needs_account"
    assert binding._facility_store().get(HOST)["env_setup"] == GOOD and "byo" not in app.pending_facility_cache


async def test_a_bring_up_that_got_past_start_is_never_dropped_by_a_later_poll(monkeypatch):
    # review round 3 (item 3): each poll re-runs the venv check, whoami and list through env_setup, so a flaky login
    # node (out of processes, a stale NFS handle) used to drop a config whose endpoint had started — never now
    w, app = World(monkeypatch, outcome="provisioning"), _app()
    assert (await server._connect_facility(app, "byo", details=_details(GOOD))).phase == "provisioning"
    assert "byo" in app.details_started
    for said in ("bash: fork: retry: Resource temporarily unavailable\nbash: fork: Resource temporarily unavailable",
                 "OSError: [Errno 116] Stale file handle",
                 "bash: line 1: globus-compute-endpoint: command not found"):   # even the config's own words: it started
        w.fail[GOOD] = ConfigStepFailed(f"remote list failed: {said}")
        res = await server._connect_facility(app, "byo")
        assert res.phase == "failed" and "is not kept" not in (res.notice or "") and "byo" in app.session_facilities
    del w.fail[GOOD]
    assert (await server._connect_facility(app, "byo")).phase == "provisioning" and w.probed == []
    w.outcome = "warm"
    assert (await server._connect_facility(app, "byo")).phase == "needs_account" and "byo" not in app.details_started
    # a NEW details= starts unstarted: its own first failure on the login node is dropped as ever
    w.outcome = "provisioning"
    assert (await server._connect_facility(app, "byo", details=_details(BAD))).phase == "failed"
    assert "byo" not in app.session_facilities and "byo" not in app.details_started


async def test_a_proven_details_config_is_kept_even_when_a_later_bring_up_fails(monkeypatch):
    w, app = World(monkeypatch), _app()
    assert (await server._connect_facility(app, "byo", details=_details(GOOD))).phase == "needs_account"
    w.fail[GOOD] = ConfigStepFailed(LMOD)   # the site changed under a config that came up: the facility's failure
    res = await server._connect_facility(app, "byo", ssh_host=HOST)
    assert res.phase == "failed" and "is not kept" not in (res.notice or "") and "byo" in app.session_facilities
    del w.fail[GOOD]
    back = await server._connect_facility(app, "byo")
    assert back.phase == "needs_account" and not (back.notice or "").startswith("The details= given earlier")
    # reconnected with it, no probe
    assert w.probed == [] and w.brought_up == [GOOD] * 3


@pytest.mark.parametrize(("exc", "head"), [
    (RuntimeError("ssh: connect to host login port 22: Connection refused"), "CANNOT REACH"),
    (RuntimeError("seed storage.db (mkdir) failed: u@login: Permission denied (publickey,password)."), "NO SSH ACCESS"),
    (TimeoutError("endpoint venv build on login timed out"), "TIMED OUT"),
])
async def test_a_failure_outside_the_config_keeps_it_for_the_retry_its_notice_asks_for(monkeypatch, exc, head):
    w, app = World(monkeypatch), _app()
    w.fail[GOOD] = exc
    res = await server._connect_facility(app, "byo", details=_details(GOOD))
    assert res.phase == "failed" and (res.notice or "").startswith(head) and "connect_facility again" in res.notice
    assert "is not kept" not in res.notice and "byo" in app.session_facilities and "byo" in app.pending_facility_cache
    del w.fail[GOOD]   # fixed (the VPN, the key, a slow node): the connect its notice asks for runs the same config
    assert (await server._connect_facility(app, "byo")).phase == "needs_account"
    assert w.probed == [] and w.brought_up == [GOOD, GOOD]


async def test_a_one_time_code_handoff_keeps_the_details_for_its_resume(monkeypatch):
    w, app = World(monkeypatch), _app()
    w.target = SshTarget(host=HOST, control_dir="/tmp/cm")
    w.fail[GOOD] = RuntimeError("u@login: Permission denied (gssapi-with-mic,keyboard-interactive,hostbased).")
    res = await server._connect_facility(app, "byo", details=_details(GOOD))
    assert res.phase == "needs_preauth" and app.preauth_resume == "connect_facility('byo')"
    assert "byo" in app.session_facilities and "byo" in app.pending_facility_cache
    del w.fail[GOOD]   # the code was given (complete_preauth): the resume, without details, runs the same config
    assert (await server._connect_facility(app, "byo")).phase == "needs_account" and w.brought_up == [GOOD, GOOD]


async def test_a_catalogued_id_whose_details_failed_goes_back_to_its_registry_entry(monkeypatch):
    w, app = World(monkeypatch, catalog=[_anvil()]), _app()
    res = await server._connect_facility(app, "anvil", details=_details())   # details= override a catalogued id
    assert res.phase == "failed" and "the registry's entry if it lists 'anvil'" in (res.notice or "")
    again = await server._connect_facility(app, "anvil")
    assert again.phase == "needs_account" and w.built == [BAD, "module load x"]   # the registry's entry, not BAD
    assert again.notice.startswith("The details= given earlier for 'anvil' failed to come up")
    assert "anvil" not in app.failed_details   # up on the registry's config: said once, then history
    assert not (await server._connect_facility(app, "anvil")).notice.startswith("The details= given earlier")


async def test_a_registry_entry_that_fails_is_unchanged(monkeypatch):
    w, app = World(monkeypatch, catalog=[_anvil()]), _app()
    w.fail["module load x"] = ConfigStepFailed(LMOD)
    res = await server._connect_facility(app, "anvil")
    assert res.phase == "failed" and "is not kept" not in (res.notice or "") and "anvil" not in app.session_facilities
    again = await server._connect_facility(app, "anvil")   # the registry's entry again: nothing of ours to say
    assert again.phase == "failed" and not (again.notice or "").startswith("The details= given earlier")
    assert w.built == ["module load x"] * 2 and w.probed == []


async def test_a_config_already_proven_for_the_host_serves_the_connect_and_the_failed_one_is_never_cached(monkeypatch):
    w, app = World(monkeypatch), _app()
    binding._facility_store().put(HOST, _details(GOOD).model_dump(mode="json"))   # proven in an earlier session
    await server._connect_facility(app, "lmodfac", details=_details())          # a new one fails
    again = await server._connect_facility(app, "lmodfac", ssh_host=HOST)
    assert again.phase == "needs_account" and w.brought_up == [BAD, GOOD] and w.probed == []
    assert again.notice.startswith("The details= given earlier for 'lmodfac' failed to come up")
    assert binding._facility_store().get(HOST)["env_setup"] == GOOD   # the failed details never reach the cache


# ---- the refusals' advice comes true -----------------------------------------------------------------------------------

async def test_the_no_env_setup_refusal_says_what_a_connect_without_details_does_from_each_state(monkeypatch):
    w, app = World(monkeypatch, outcome="provisioning"), _app()
    # nothing kept: a connect without details proposes (this facility is not catalogued)
    res = await server._connect_facility(app, "byo", details=_details(env_setup=None))
    assert res.phase == "failed" and "Nothing was started" in (res.notice or "")
    assert ("Give env_setup; otherwise connect_facility('byo', ssh_host='login') without details resolves the "
            "facility afresh") in res.notice
    # a config this session keeps (still coming up): that call reconnects with it — it proposes nothing
    await server._connect_facility(app, "byo", details=_details(GOOD))
    kept = await server._connect_facility(app, "byo", details=_details(env_setup=None))
    assert ("Give env_setup; otherwise connect_facility('byo') without details reconnects with the configuration "
            "this session keeps for it (for 'login') and proposes nothing while that one is kept") in (kept.notice or "")
    assert (await server._connect_facility(app, "byo", ssh_host=HOST)).phase == "provisioning" and w.probed == []
    # the lmod run's sequence: a failed details=, a details= without env_setup, then the call its refusal names
    w2, app2 = World(monkeypatch), _app()
    await server._connect_facility(app2, "lmodfac", ssh_host=HOST, details=_details())
    refused = await server._connect_facility(app2, "lmodfac", details=_details(env_setup=None))
    assert "otherwise connect_facility('lmodfac', ssh_host='login') without details resolves the facility afresh" \
           in (refused.notice or "")
    assert (await server._connect_facility(app2, "lmodfac", ssh_host=HOST)).phase == "proposed_facility_details"
    assert w2.brought_up == [BAD]


async def test_the_other_host_refusal_names_the_proposal_it_would_get(monkeypatch):
    w, app = World(monkeypatch), _app()
    assert (await server._connect_facility(app, "byo", ssh_host=HOST)).phase == "proposed_facility_details"
    res = await server._connect_facility(app, "byo", details=_details(env_setup=None, host="other"))
    assert res.phase == "failed" and "was for 'login'" in (res.notice or "")
    assert ("otherwise connect_facility('byo', ssh_host='other') without details resolves the facility afresh"
            in res.notice)
    assert (await server._connect_facility(app, "byo", ssh_host="other")).phase == "proposed_facility_details"
    assert w.probed == [HOST, "other"]


async def test_the_other_host_refusal_gives_a_route_to_a_proposal_while_a_config_is_kept(monkeypatch):
    # review 2026-10-20 (7c): while this session keeps a config for the id, no connect of the id proposes — another id does
    w, app = World(monkeypatch, outcome="provisioning"), _app()
    await server._connect_facility(app, "byo", ssh_host=HOST)                       # a proposal for 'login'
    await server._connect_facility(app, "byo", details=_details(env_setup=None))    # taken: kept, coming up
    res = await server._connect_facility(app, "byo", details=_details(env_setup=None, host="other"))
    assert "was for 'login'" in (res.notice or "") and "reconnects with the configuration this session keeps" in res.notice
    assert ("for 'other', connect another facility id: connect_facility('<new id>', ssh_host='other') resolves that one "
            "afresh") in res.notice
    assert (await server._connect_facility(app, "byo2", ssh_host="other")).phase == "proposed_facility_details"
    assert w.probed == [HOST, "other"]


# ---- review 2026-10-20, item 1: which failure drops a config — POSITIVE, by type, pinned over every explanation -----

_GlobusAPIError = type("GlobusAPIError", (Exception,), {})   # what manager_online raises on a web error (no try there)
_VENV = "/home/u/.hpc-bridge/gce-endpoint-4.18.0-py3.13"
_REMOTE_TOKEN = ("a Globus token store already exists on login (~/.globus_compute/storage.db) and is not empty (or could "
                 "not be read), but `globus-compute-endpoint whoami` failed there: rc=1. hpc-bridge will not replace a "
                 "credential it did not create. Fix what whoami reports (the facility's env_setup, gce on PATH, the login "
                 "node's network to auth.globus.org), or run `globus-compute-endpoint login` there yourself, then call "
                 "connect_facility again. Nothing was started or billed.")
_NO_TOOLS = ("no uv, no curl and no python3 on the login node, so hpc-bridge cannot build its endpoint venv (Python 3.13) "
             "there. Make uv or python3 available (a site `module load …` before hpc-bridge's line in env_setup), or "
             "prepare a venv with globus-compute-endpoint at HPC_BRIDGE_REMOTE_VENV, then connect_facility again. "
             "Nothing was started or billed.")

# (the exception as the code raises it, how the explanation opens, does it drop a details= config?)
EXPLAINED = [
    # the Globus login: the gate before the bootstrap
    (LoginExpiredError("refresh token revoked"), "hpc-bridge error: LoginExpiredError:", False),
    # texts that already say what to do, shown as they are
    (RemoteTokenStoreExists(_REMOTE_TOKEN), "a Globus token store already exists on login", False),
    (ev._timed_out(_VENV, "login"), f"checking hpc-bridge's endpoint venv {_VENV} on login timed out", False),
    (ev.EndpointVenvError(f"{_VENV} on login exists and is not a venv: hpc-bridge will not move or replace it. Move it "
                          "away …, then connect_facility again. Nothing was started or billed."),
     f"{_VENV} on login exists and is not a venv", False),
    (ev.EndpointVenvConfigError("the check of hpc-bridge's endpoint venv on login did not run: Lmod has detected the "
                                "following error: The following module(s) are unknown: \"python/3.13\". Nothing was "
                                "started or billed."), "the check of hpc-bridge's endpoint venv on login did not run", True),
    (ev.EndpointVenvConfigError(_NO_TOOLS), "no uv, no curl and no python3 on the login node", True),
    (ev.EndpointVenvConfigError(f"could not build hpc-bridge's endpoint venv for {_VENV} on login (globus-compute-endpoint "
                                "4.18.0 on Python 3.13): error: No interpreter found for Python 3.13 in managed "
                                "installations or search path. Fix that, then connect_facility again. Nothing of it was "
                                "kept. Nothing was started or billed."), "could not build hpc-bridge's endpoint venv", True),
    (ev.EndpointVenvConfigError(f"could not build hpc-bridge's endpoint venv for {_VENV} on login (…): error: Failed to "
                                "fetch: `https://pypi.org/simple/globus-compute-endpoint/` Caused by: operation timed out. "
                                "Fix that, then connect_facility again."), "could not build hpc-bridge's endpoint venv",
     False),   # PyPI did not answer in time: the network's, the same config may build next time
    # ssh itself (its own exit status 255: a plain RuntimeError) — nothing of the config ran
    (RuntimeError("seed storage.db (mkdir) failed: u@login: Permission denied (publickey,password)."), "NO SSH ACCESS",
     False),
    (RuntimeError("seed storage.db (mkdir) failed: Host key ED25519 SHA256:abc revoked by file /etc/ssh/revoked_keys"),
     "REVOKED HOST KEY", False),
    (RuntimeError("seed storage.db (mkdir) failed: Host key verification failed."), "UNKNOWN HOST KEY", False),
    (RuntimeError("seed storage.db (mkdir) failed: @@@ WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED! @@@"),
     "UNKNOWN HOST KEY", False),
    (RuntimeError("seed storage.db (mkdir) failed: mkdir: cannot create directory '/home/u/.globus_compute': Permission "
                  "denied"), "REMOTE FILESYSTEM", False),
    (RuntimeError("seed storage.db (mkdir) failed: ssh: connect to host login port 22: Connection refused"),
     "CANNOT REACH", False),
    (RuntimeError("seed storage.db (mkdir) failed: ssh: Could not resolve hostname login: Name or service not known"),
     "CANNOT REACH", False),
    (RuntimeError("seed storage.db (mkdir) failed: kex_exchange_identification: read: Connection reset by peer"),
     "CANNOT REACH", False),
    (RuntimeError("remote configure failed: Connection to login closed by remote host."),   # ssh dropped mid-step: 255
     "hpc-bridge error: RuntimeError: remote configure failed: Connection to login closed", False),
    (RuntimeError("remote configure failed: client_loop: send disconnect: Broken pipe"),
     "hpc-bridge error: RuntimeError: remote configure failed: client_loop", False),
    (TimeoutError("ssh to login timed out after 120 s"), "TIMED OUT", False),
    (RuntimeError("seed storage.db (mkdir) failed: ControlPath too long ('/var/folders/x/cm/%C' >= 104 bytes)"),
     "hpc-bridge error: the SSH ControlMaster socket path is too long", False),
    # what is left: an endpoint still registering, the Globus service, the login node's disk, a gce format change
    (RuntimeError("could not find endpoint 'hpc-bridge-login' in `list` output"),
     "hpc-bridge error: RuntimeError: could not find endpoint", False),
    (_GlobusAPIError("('GET', 'https://compute.api.globus.org/v2/endpoints/x/status', None, 503, 'Error', "
                     "'Service Unavailable')"), "hpc-bridge error: GlobusAPIError:", False),
    (RuntimeError("remote write $HOME/.globus_compute/hpc-bridge-login/config.yaml failed: cat: write error: No space "
                  "left on device"), "hpc-bridge error: RuntimeError: remote write", False),
    (RuntimeError("could not parse `globus-compute-endpoint list` output (gce version/format change?); raw output: …"),
     "hpc-bridge error: RuntimeError: could not parse", False),
    # a step that ran env_setup, refused by the login node (ConfigStepFailed): the config's own …
    (ConfigStepFailed(LMOD), "hpc-bridge error: ConfigStepFailed: remote configure failed: Lmod", True),
    (ConfigStepFailed("remote configure failed: bash: line 1: globus-compute-endpoint: command not found"),
     "hpc-bridge error: ConfigStepFailed: remote configure failed: bash", True),
    (ConfigStepFailed("remote start failed (rc=1): ModuleNotFoundError: No module named 'globus_compute_endpoint'"),
     "hpc-bridge error: ConfigStepFailed: remote start failed", True),
    (ConfigStepFailed("remote list failed: /opt/site/setup.sh: line 3: conda: command not found"),
     "hpc-bridge error: ConfigStepFailed: remote list failed", True),
    # … even where its words quote what the SSH explanations look for: never NO SSH ACCESS / REMOTE FILESYSTEM /
    # CANNOT REACH (ssh got through; the login node is up), and dropped — the reverse error of round 1
    (ConfigStepFailed("remote configure failed: bash: /opt/site/env.sh: Permission denied"),
     "hpc-bridge error: ConfigStepFailed: remote configure failed: bash: /opt/site/env.sh: Permission denied", True),
    (ConfigStepFailed("remote configure failed: lmgrd: license server: Connection refused"),
     "hpc-bridge error: ConfigStepFailed: remote configure failed: lmgrd", True),
    # … unless its words name the network, a timeout, Globus, a lost connection, registration or a full disk: kept
    (ConfigStepFailed("remote configure failed: pip: ReadTimeoutError: Read timed out. (read timeout=15)"),
     "hpc-bridge error: ConfigStepFailed: remote configure failed: pip", False),
    (ConfigStepFailed("remote start failed (rc=1): globus_sdk.exc.api.GlobusAPIError: (503, 'Service Unavailable')"),
     "hpc-bridge error: ConfigStepFailed: remote start failed", False),
    (ConfigStepFailed("remote configure failed: Connection to login closed by remote host."),
     "hpc-bridge error: ConfigStepFailed: remote configure failed: Connection", False),
    # … and a refused write of the login node's filesystem is explained as before (REMOTE FILESYSTEM) and kept
    (ConfigStepFailed("remote configure failed: OSError: [Errno 28] No space left on device"), "REMOTE FILESYSTEM",
     False),
    (ConfigStepFailed("remote configure failed: Disk quota exceeded"), "REMOTE FILESYSTEM", False),
    # round 3, item 2 — live 2026-09-03: globus-compute-endpoint's own write under ~/.globus_compute refused
    (ConfigStepFailed("remote start failed (rc=1): PermissionError: [Errno 13] Permission denied: "
                      "'/home/u/.globus_compute/hpc-bridge-login/endpoint.log'"), "REMOTE FILESYSTEM", False),
    (ConfigStepFailed("remote configure failed: OSError: [Errno 30] Read-only file system: "
                      "'/home/u/.globus_compute/hpc-bridge-login'"), "REMOTE FILESYSTEM", False),
    # round 3, item 3: the login node's own state — out of processes, a stale NFS handle — kept
    (ConfigStepFailed("remote configure failed: bash: fork: retry: Resource temporarily unavailable\nbash: fork: "
                      "Resource temporarily unavailable"), "hpc-bridge error: ConfigStepFailed: remote configure", False),
    (ConfigStepFailed("remote list failed: OSError: [Errno 116] Stale file handle"),
     "hpc-bridge error: ConfigStepFailed: remote list failed", False),
]

# round 3, item 1: real multi-line outputs, raised as the steps raise them (`step_error` with the step's own output).
# Only the LAST meaningful line decides: a banner's "timeout", a `sock.settimeout` frame or a `globus_sdk` path before it
# is no cause; a timeout or a Globus error that IS the last line is.
_BANNER_LMOD = ("*** login02: idle sessions timeout after 30 minutes of inactivity ***\n"
                'Lmod has detected the following error: The following module(s) are unknown: "gcc/10.2.0"\n\n'
                'Please check the spelling or version number. Also try "module spider ..."\n'
                "It is also possible your cache file is out-of-date; it may help to try:\n"
                '  $ module --ignore_cache load "gcc/10.2.0"\n\n'
                "Also make sure that all modulefiles written in TCL start with the string\n#%Module")
_SDK_MISMATCH = ("Traceback (most recent call last):\n"
                 '  File "/home/u/venv/bin/globus-compute-endpoint", line 5, in <module>\n'
                 "    from globus_compute_endpoint.cli import cli_run\n"
                 '  File "/home/u/venv/lib/python3.11/site-packages/globus_compute_endpoint/cli.py", line 21, in <module>\n'
                 "    from globus_sdk import GlobusApp\n"
                 "ImportError: cannot import name 'GlobusApp' from 'globus_sdk' "
                 "(/home/u/venv/lib/python3.11/site-packages/globus_sdk/__init__.py)")
_NO_PARSL = ("Traceback (most recent call last):\n"
             '  File "/home/u/venv/lib/python3.11/site-packages/globus_compute_endpoint/endpoint/utils.py", line 88, '
             "in _probe\n    sock.settimeout(self.timeout)\n"
             '  File "/home/u/venv/lib/python3.11/site-packages/globus_compute_endpoint/engines/htex.py", line 3, in '
             "<module>\n    import parsl\nModuleNotFoundError: No module named 'parsl'")
_PIP_TIMEOUT = ("Traceback (most recent call last):\n"
                '  File "/home/u/venv/lib/python3.11/site-packages/pip/_vendor/urllib3/response.py", line 438, in '
                "_error_catcher\n    yield\npip._vendor.urllib3.exceptions.ReadTimeoutError: "
                "HTTPSConnectionPool(host='pypi.org', port=443): Read timed out.")
_AUTH_DOWN = ("Traceback (most recent call last):\n"
              '  File "/home/u/venv/lib/python3.11/site-packages/globus_sdk/client.py", line 300, in request\n'
              "    raise self.error_class(r)\nglobus_sdk.services.auth.errors.AuthAPIError: ('POST', "
              "'https://auth.globus.org/v2/oauth2/token', None, 503, 'Error', 'Service Unavailable')")
_SOCKET_THEN_MODULE = "Control socket connect(/tmp/cm/abc): Connection refused\nmodule: command not found"
for _text, _drops in ((_BANNER_LMOD, True), (_SDK_MISMATCH, True), (_NO_PARSL, True), (_SOCKET_THEN_MODULE, True),
                      (_PIP_TIMEOUT, False), (_AUTH_DOWN, False)):
    EXPLAINED.append((step_error(1, f"remote configure failed: {_text}", output=_text),
                      "hpc-bridge error: ConfigStepFailed: remote configure failed:", _drops))
    EXPLAINED.append((step_error(1, f"remote start failed (rc=1): {_text}; `list` failed too: rc=1", output=_text),
                      "hpc-bridge error: ConfigStepFailed: remote start failed", _drops))


@pytest.mark.parametrize(("exc", "opens", "drops"), EXPLAINED, ids=[f"{type(e).__name__}-{i}" for i, (e, _o, _d)
                                                                     in enumerate(EXPLAINED)])
def test_every_explanation_says_what_it_says_and_only_the_configs_own_failure_drops_it(exc, opens, drops):
    from types import SimpleNamespace
    fac = SimpleNamespace(cli=SimpleNamespace(target=SshTarget(host="login", user="u")))
    assert notices._explain_provision_error(exc, fac).startswith(opens)
    assert notices._failure_of_the_config(exc) is drops


_CONNECTED = [x for x in EXPLAINED if not isinstance(x[0], LoginExpiredError)]   # the gate answers that one first
_IDS = [f"{type(e).__name__}-{i}" for i, (e, _o, _d) in enumerate(_CONNECTED)]


@pytest.mark.parametrize(("exc", "opens", "drops"), _CONNECTED, ids=_IDS)
async def test_a_first_bring_up_drops_its_config_only_for_the_configs_own_failure(monkeypatch, exc, opens, drops):
    w, app = World(monkeypatch), _app()
    w.fail[GOOD] = exc
    res = await server._connect_facility(app, "byo", details=_details(GOOD))
    assert res.phase == "failed" and (res.notice or "").startswith(opens)
    assert ("byo" not in app.session_facilities) is drops and ("This configuration (details=) is not kept" in res.notice) is drops
    del w.fail[GOOD]
    nxt = await server._connect_facility(app, "byo")
    assert nxt.phase == ("proposed_facility_details" if drops else "needs_account")
    assert w.brought_up == [GOOD] + ([] if drops else [GOOD])


@pytest.mark.parametrize(("exc", "opens", "drops"), _CONNECTED, ids=_IDS)
async def test_a_poll_of_a_config_that_got_past_start_never_drops_it(monkeypatch, exc, opens, drops):
    # every poll re-runs the bootstrap's steps (each connect resets app.state); once a bring-up of the config got past
    # `start`, none of the answers above drops it (round 1: six transients did; round 3: a flaky node still could)
    w, app = World(monkeypatch, outcome="provisioning"), _app()
    assert (await server._connect_facility(app, "byo", details=_details(GOOD))).phase == "provisioning"
    w.fail[GOOD] = exc
    res = await server._connect_facility(app, "byo")
    assert res.phase == "failed" and (res.notice or "").startswith(opens)
    assert "byo" in app.session_facilities and "This configuration (details=) is not kept" not in res.notice
    del w.fail[GOOD]
    assert (await server._connect_facility(app, "byo")).phase == "provisioning"
    assert w.brought_up == [GOOD] * 3 and w.probed == []


@pytest.mark.parametrize(("rc", "config_step"), [(1, True), (127, True), (255, False), (-9, False)])   # -9: ssh killed
async def test_the_steps_that_run_env_setup_raise_ConfigStepFailed_unless_ssh_itself_failed(monkeypatch, rc, config_step):
    from hpc_bridge.facility import remote

    async def fake_ssh(target, cmd, *, stdin=None, timeout=None):
        return rc, "", 'Lmod has detected the following error: The following module(s) are unknown: "gcc/10.2.0"'

    monkeypatch.setattr(remote, "ssh_exec", fake_ssh)
    cli = RemoteEndpointCLI(SshTarget(host="login", user="u"), BAD)
    for step in (cli.configure("hpc-bridge-login"), cli.start("hpc-bridge-login"), cli.endpoint_id("hpc-bridge-login")):
        with pytest.raises(RuntimeError) as got:
            await step
        assert isinstance(got.value, ConfigStepFailed) is config_step, (rc, got.value)


async def test_the_venv_steps_the_config_breaks_raise_its_type_and_the_others_do_not():
    mv = ev.managed_venv(ev.short_env_setup("/home/u/.hpc-bridge/gce", prefix="module load python/3.13; "))
    assert mv is not None

    async def stops_in_the_prefix(step, script, timeout):   # the site's prefix ran, and stopped before the check's frame
        return 1, "site setup: python/3.13 is retired, use python/3.12\n", "exit 1"

    with pytest.raises(ev.EndpointVenvConfigError, match="did not run: "):
        await ev._check(stops_in_the_prefix, mv, "login", "check")

    async def no_output(step, script, timeout):   # rc != 255 but nothing printed: read as ssh's, as before (kept)
        return 1, "", "Connection closed"

    with pytest.raises(RuntimeError) as quiet:
        await ev._check(no_output, mv, "login", "check")
    assert not isinstance(quiet.value, ConfigStepFailed)

    async def ssh_failed(step, script, timeout):
        return 255, "", "ssh: connect to host login port 22: Connection refused"

    with pytest.raises(RuntimeError) as got:
        await ev._check(ssh_failed, mv, "login", "check")
    assert not isinstance(got.value, ConfigStepFailed)
    with pytest.raises(ev.EndpointVenvConfigError, match="no uv, no curl and no python3"):
        ev._method(ev.VenvState(), "3.13")
    with pytest.raises(ev.EndpointVenvError) as old:
        ev._method(ev.VenvState(uv="/usr/bin/uv", uv_version="0.1.0"), "3.13")
    assert not isinstance(old.value, ConfigStepFailed)    # an old uv is the node's to mend, not the config's
    assert not isinstance(ev._timed_out(_VENV, "login"), ConfigStepFailed)


# ---- item 2: no tool brings a dropped config up ------------------------------------------------------------------------

async def test_ensure_endpoint_up_and_run_shell_refuse_to_bring_a_dropped_config_up(monkeypatch):
    w, app = World(monkeypatch), _app()
    await server._connect_facility(app, "lmodfac", ssh_host=HOST, details=_details())
    bound = app.facility
    assert app.machine == "lmodfac" and bound.env_setup == BAD          # still BOUND (a mark, not an unbind)
    monkeypatch.setattr(warmth, "_provision", REAL_PROVISION)           # what the two tools reach
    expect = ("connect_facility('lmodfac') failed to bring up the configuration given with details= (hpc-bridge error: "
              "ConfigStepFailed: remote configure failed: Lmod has detected the following error")
    st = await server._ensure_endpoint_up(app, shape="login")
    assert st.status == "down" and (st.notice or "").startswith(expect)
    assert ("hpc-bridge does not keep it, so no tool brings it up. connect_facility('lmodfac', ssh_host='login') without "
            "details resolves the facility afresh") in st.notice and "or give details= again" in st.notice
    with pytest.raises(DroppedConfigError) as got:
        await server._run_shell(app, "module avail", shape="login")
    assert server._error_outcome(got.value).notice == str(got.value) and str(got.value).startswith(expect)
    assert bound.provisioned is False and w.brought_up == [BAD]         # nothing of it ran again
    # the next connect binds a new facility object: the refusal is behind it
    monkeypatch.setattr(warmth, "_provision", w._provision)
    await server._connect_facility(app, "lmodfac", ssh_host=HOST)                         # the fresh proposal
    up = await server._connect_facility(app, "lmodfac", details=_details(env_setup=None))  # confirmed
    assert up.phase == "needs_account" and app.dropped_binding is None and app.facility is not bound


# ---- item 3: a re-sent config equal to the one proven for that host is proven ---------------------------------------------

async def test_a_config_equal_to_the_one_proven_for_the_host_is_kept_when_it_fails(monkeypatch):
    w, app = World(monkeypatch), _app()
    binding._facility_store().put(HOST, _details(BAD).model_dump(mode="json"))   # proven yesterday; the site changed
    first = await server._connect_facility(app, "lmodfac", details=_details(BAD))   # the agent re-sends it as it was
    assert first.phase == "failed" and "is not kept" not in (first.notice or "") and "lmodfac" in app.session_facilities
    again = await server._connect_facility(app, "lmodfac", ssh_host=HOST)
    assert again.phase == "failed" and w.brought_up == [BAD, BAD]       # reconnected with it, as a proven config is …
    assert "did not run it" not in (again.notice or "")                # … and nothing says otherwise


# ---- item 4: a connect without details that waited behind the failing bring-up resolves again -------------------------

def _held(monkeypatch, w, app):
    """The first bring-up holds the lock until a second call WAITS on it — deterministic, never a race of sleeps (a
    thread hop before the lock can outlast any sleep under load). Returns the event set once that bring-up holds the
    lock: start the second call on it."""
    import asyncio

    inside, waiting = asyncio.Event(), asyncio.Event()

    class SpyLock(asyncio.Lock):
        async def acquire(self):
            if self.locked():
                waiting.set()
            return await super().acquire()

    app.lock = SpyLock()
    orig = w._provision

    async def held(app_, shape, **kw):
        if not inside.is_set():
            inside.set()
            await asyncio.wait_for(waiting.wait(), 5)
        return await orig(app_, shape, **kw)

    monkeypatch.setattr(warmth, "_provision", held)
    return inside


async def test_a_connect_without_details_queued_behind_the_failing_bring_up_resolves_again(monkeypatch):
    import asyncio

    w, app = World(monkeypatch), _app()
    inside = _held(monkeypatch, w, app)

    async def second():
        await inside.wait()   # arrives while the first holds the lock, bringing the bad config up
        return await server._connect_facility(app, "lmodfac", ssh_host=HOST)

    first, other = await asyncio.gather(server._connect_facility(app, "lmodfac", ssh_host=HOST, details=_details()),
                                        second())
    assert first.phase == "failed" and "is not kept" in (first.notice or "")
    assert w.brought_up == [BAD]                                        # never run a second time
    assert other.phase == "proposed_facility_details"
    assert (other.notice or "").startswith("The details= given earlier for 'lmodfac' failed to come up")


# ---- item 7a: a failure that already says "connect_facility again" gets one coherent piece of advice -------------------

async def test_advice_that_already_says_connect_again_is_made_one_with_the_drop(monkeypatch):
    w, app = World(monkeypatch), _app()
    w.fail[GOOD] = ev.EndpointVenvConfigError(_NO_TOOLS)
    res = await server._connect_facility(app, "byo", details=_details(GOOD))
    notice = res.notice or ""
    assert notice.startswith("no uv, no curl and no python3") and "then connect_facility again." in notice
    assert ("This configuration (details=) is not kept. So, to connect_facility again as above: connect_facility('byo', "
            "ssh_host='login') without details resolves the facility afresh") in notice
    assert "with details= it runs the configuration given — this one again, or a corrected one" in notice


# ---- review round 3 ----------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("outcome", ["provisioning", "warm"])
async def test_a_failing_bring_up_never_drops_the_config_another_details_put_in_its_place(monkeypatch, outcome):
    # item 4: details=A (bad) holds the lock; details=B (good) arrives and replaces the session's config; A fails — and
    # used to delete B's entry and pending cache with its own drop
    import asyncio

    w, app = World(monkeypatch, outcome=outcome), _app()
    inside = _held(monkeypatch, w, app)

    async def second():
        await inside.wait()
        return await server._connect_facility(app, "byo", ssh_host=HOST, details=_details(GOOD))

    a, b = await asyncio.gather(server._connect_facility(app, "byo", ssh_host=HOST, details=_details(BAD)), second())
    assert a.phase == "failed" and "is not kept" not in (a.notice or "")   # A's config was no longer the session's
    assert "byo" not in app.failed_details and app.session_facilities["byo"].compute.env_setup == GOOD
    if outcome == "provisioning":
        assert b.phase == "provisioning" and app.pending_facility_cache["byo"][1]["env_setup"] == GOOD
        poll = await server._connect_facility(app, "byo")                  # B is polled, not proposed over
        assert poll.phase == "provisioning" and w.probed == [] and w.brought_up == [BAD, GOOD, GOOD]
    else:
        assert b.phase == "needs_account" and binding._facility_store().get(HOST)["env_setup"] == GOOD   # B proven


async def test_teardown_on_a_dropped_binding_reaches_nothing_today(monkeypatch):
    # item 5 — a KNOWN GAP, older than this branch, pinned so its fix changes this on purpose: after a failed bring-up no
    # endpoint id is known, so teardown_endpoint and stop_endpoint answer "no endpoint was up" without SSH. What the
    # failed bring-up left on the login node (the token store it seeded, ~/.globus_compute/storage.db) stays there.
    World(monkeypatch)
    app = _app()
    await server._connect_facility(app, "lmodfac", ssh_host=HOST, details=_details())
    assert app.dropped_binding is not None and app.state.endpoint_id is None
    td = await server._teardown_endpoint(app)
    st = await server._stop_endpoint(app)
    assert (td.status, td.notice) == ("down", "no endpoint was up") and (st.status, st.notice) == ("down",
                                                                                                    "no endpoint was up")


async def test_a_config_an_older_plugin_cached_is_compared_through_the_model(monkeypatch):
    # item 7: a record written before a field existed reads equal once the model fills its default
    World(monkeypatch)
    app = _app()
    old = _details(BAD).model_dump(mode="json")
    for newer in ("cpus_per_node", "endpoint_name", "display_name"):
        old.pop(newer)
    binding._facility_store().put(HOST, old)
    first = await server._connect_facility(app, "lmodfac", details=_details(BAD))
    assert first.phase == "failed" and "is not kept" not in (first.notice or "") and "lmodfac" in app.session_facilities


async def test_a_login_that_landed_before_the_retry_is_still_named(monkeypatch):
    # item 8: a connect without details whose gate completed the Globus login, then found the config it resolved dropped
    # under the lock, resolves again — and its answer still says who logged in
    import asyncio
    from types import SimpleNamespace

    from hpc_bridge import login_gate

    w, app = World(monkeypatch), _app()
    inside = _held(monkeypatch, w, app)
    binding._facility_store().put(HOST, _details(GOOD).model_dump(mode="json"))   # the cache rung: the retry binds it
    verdicts = iter(["works", "missing"])                    # A's gate: logged in; B's: the login lands during it

    async def stored(flow):
        return next(verdicts, "works"), None

    async def start_and_wait(flow, mode=None):
        return SimpleNamespace(url="https://auth.globus.org/x", mode="browser"), "done"

    async def as_identity():
        return " as u@uchicago.edu"

    monkeypatch.setattr(login_gate, "_stored_login", stored)
    monkeypatch.setattr(login_gate, "_start_login_and_wait", start_and_wait)
    monkeypatch.setattr(login_gate, "_as_identity", as_identity)
    app.login_flow = SimpleNamespace(error=None)

    async def second():
        await inside.wait()
        return await server._connect_facility(app, "lmodfac", ssh_host=HOST)

    a, b = await asyncio.gather(server._connect_facility(app, "lmodfac", ssh_host=HOST, details=_details()), second())
    assert a.phase == "failed" and b.phase == "needs_account" and w.brought_up == [BAD, GOOD]
    assert "Globus login landed as u@uchicago.edu." in (b.notice or "")


# ---- review round 4 ----------------------------------------------------------------------------------------------------

_GCE_NET = ("2026-10-20 12:00:00 CRITICAL MainProcess-1234 MainThread-1 globus_compute_endpoint.cli:832 "
            "_do_register_endpoint Network failure; unable to register endpoint: ConnectionError on request")
_GCE_BLOCKED = 'Endpoint registration blocked.  [{"code": "CONFLICT", "detail": "name in use"}]'
_LMOD_NEW = ('Lmod has detected the following error:  The following module(s) are unknown: "gcc/10.2.0"\n\n'
             'Please check the spelling or version number. Also try "module spider ..."\n'
             "It is also possible your cache file is out-of-date; it may help to try:\n"
             '  $ module --ignore_cache load "gcc/10.2.0"\n\n'
             "Also make sure that all modulefiles written in TCL start with the string #%Module\n\n\n")
_PIP_NET = ("WARNING: Retrying (Retry(total=4, connect=None, read=None, redirect=None, status=None)) after connection "
            "broken by 'ConnectTimeoutError(<pip._vendor.urllib3.connection.HTTPSConnection object at 0x7f>, "
            "'Connection to pypi.org timed out. (connect timeout=15)')': /simple/foo/\n"
            "ERROR: Could not find a version that satisfies the requirement foo (from versions: none)\n"
            "ERROR: No matching distribution found for foo")
_CURL_LIC = "curl: (28) Connection timed out after 10001 milliseconds\nsite-setup: license check failed"
_UV_TIMEOUT = ("error: Failed to download `globus-compute-endpoint==4.15.0`\n  Caused by: Failed to download "
               "distribution due to network timeout. Try increasing UV_HTTP_TIMEOUT (current value: 30s).")


@pytest.mark.parametrize(("output", "said", "drops"), [
    # F3: globus-compute-endpoint's own registration failures (exit 75, EX_TEMPFAIL; a 409/423) — Globus's, kept
    (_GCE_NET, _GCE_NET, False),
    (_GCE_BLOCKED, _GCE_BLOCKED, False),
    # F6: a network cause printed just before the final error is in the tail — kept; uv's own timeout words too
    (_PIP_NET, "ERROR: No matching distribution found for foo", False),
    (_CURL_LIC, "site-setup: license check failed", False),
    (_UV_TIMEOUT, _UV_TIMEOUT.splitlines()[-1].strip(), False),
    # the config's own: Lmod's newer one-paragraph format
    (_LMOD_NEW, "Also make sure that all modulefiles written in TCL start with the string #%Module", True),
    # a shell's trailer (`clear` in ~/.bash_logout) and a window-title escape are never the line that decides
    ("ERROR: something\nTERM environment variable not set.", "ERROR: something", True),
    ("real error: bad thing\n\x1b]0;user@login:~\x07", "real error: bad thing", True),
], ids=["gce-net", "gce-blocked", "pip-net", "curl-lic", "uv-timeout", "lmod-new", "bash-logout", "osc"])
def test_a_steps_cause_is_read_from_its_last_few_lines(output, said, drops):
    exc = step_error(1, f"remote start failed (rc=1): {output}; `list` failed too: rc=1", output=output)
    assert isinstance(exc, ConfigStepFailed) and exc.said == said
    assert notices._failure_of_the_config(exc) is drops


def test_a_banner_far_above_the_error_never_keeps_it_and_the_list_note_never_decides():
    # the tail is the last few lines: a banner well above an Lmod error stays out of it
    text = "*** idle sessions timed out after 30 minutes ***\n" + _LMOD_NEW + "\nmore\nlines\nhere"
    assert notices._failure_of_the_config(step_error(1, f"remote configure failed: {text}", output=text))
    # F7: a start that printed nothing is judged by nothing — never by the `list` note the message adds after it
    quiet = step_error(73, "remote start failed (rc=73): no output; `list` failed too: Read timed out", output="")
    assert isinstance(quiet, ConfigStepFailed) and quiet.said == "" and notices._failure_of_the_config(quiet)


@pytest.mark.parametrize("waiting_on", ["details", "poll"])
async def test_a_warm_bring_up_never_commits_a_config_that_waited_on_the_lock(monkeypatch, waiting_on):
    # F1: connect A (details=GOOD, or a poll of it) holds the lock and comes up WARM while connect B (details=BAD)
    # waits; B wrote its pending and session entries before the lock, and A's warm branch used to cache BAD as PROVEN
    import asyncio

    w, app = World(monkeypatch, outcome="provisioning" if waiting_on == "poll" else "warm"), _app()
    if waiting_on == "poll":
        assert (await server._connect_facility(app, "byo", details=_details(GOOD))).phase == "provisioning"
        w.outcome = "warm"
    inside = _held(monkeypatch, w, app)

    async def second():
        await inside.wait()
        return await server._connect_facility(app, "byo", ssh_host=HOST, details=_details(BAD))

    first = (server._connect_facility(app, "byo") if waiting_on == "poll"
             else server._connect_facility(app, "byo", ssh_host=HOST, details=_details(GOOD)))
    a, b = await asyncio.gather(first, second())
    assert a.phase == "needs_account" and b.phase == "failed"
    assert binding._facility_store().get(HOST) is None                # BAD never ran: never cached as proven
    assert "is not kept" in (b.notice or "") and "byo" not in app.session_facilities   # …so its failure drops it
    again = await server._connect_facility(app, "byo")
    assert again.phase == "proposed_facility_details" and w.brought_up.count(BAD) == 1


async def test_a_bring_up_that_reused_an_endpoint_never_marks_the_config_started(monkeypatch):
    # F2: the first details= reuses an endpoint of an EARLIER config (a ghost still online): nothing of BAD ran; when
    # the ghost goes, BAD's own configure fails for the first time — and is dropped, not kept forever
    from hpc_bridge.lifecycle import EndpointState

    World(monkeypatch, outcome="provisioning")
    app = _app()
    calls: list[str] = []

    async def prov(app_, shape, **kw):
        calls.append(app_.facility.env_setup)
        if len(calls) == 1:
            app_.state = EndpointState(endpoint_id="ghost", reused=True)
            return "provisioning"
        raise ConfigStepFailed(LMOD)

    monkeypatch.setattr(warmth, "_provision", prov)
    first = await server._connect_facility(app, "byo", details=_details(BAD))
    assert first.phase == "provisioning" and "byo" not in app.details_started
    second = await server._connect_facility(app, "byo")
    assert second.phase == "failed" and "is not kept" in (second.notice or "") and "byo" not in app.session_facilities
    assert (await server._connect_facility(app, "byo")).phase == "proposed_facility_details" and len(calls) == 2


@pytest.mark.parametrize(("raw", "banned"), [
    ("discovery probe failed (rc=255): ssh: connect to host login port 22: Connection refused", True),
    ("ssh: Could not resolve hostname login.example.edu: Name or service not known", False),
    ("ssh: connect to host login port 22: Connection timed out", False),
])
def test_a_refused_connection_names_a_firewall_or_ban_and_asks_to_wait_not_retry(raw, banned):
    # f2b_banned, sweep 2026-10-22: fail2ban's REJECT answers "Connection refused", and every agent (pi, codex,
    # hermes21) read it as a wrong host name — a banned address that keeps retrying only extends its ban
    notice = notices._explain_provision_error(RuntimeError(raw), host="login")
    assert notice.startswith("CANNOT REACH login:") and notice.endswith("Nothing was started or billed.")
    assert "call connect_facility again" in notice                       # the host name / VPN remedy stays first
    said = ("a refused connection can also be a firewall or an intrusion-prevention ban (e.g. fail2ban, after repeated "
            "failed logins from this address): then wait — a ban lifts after its ban time — or check with the site, "
            "rather than retrying now")
    assert (said in notice) is banned, notice

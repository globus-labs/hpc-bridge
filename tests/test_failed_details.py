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
"""
from __future__ import annotations

import pytest

from hpc_bridge import binding, config, connect, server, warmth
from hpc_bridge.context import AppCtx
from hpc_bridge.facility.remote import SshTarget
from hpc_bridge.models import FacilityDetails
from hpc_bridge.profile import Profile
from tests.fakes import FakeCatalog, FakeFacility, fake_entry

BAD = "module load gcc/10.2.0"   # the made-up line of 2026-10-16
GOOD = "type module >/dev/null 2>&1 || . /etc/profile.d/lmod.sh; module load uv/0.12.9; . {venv}/bin/activate"
LMOD = ('remote configure failed: Lmod has detected the following error: The following module(s) are unknown:\n'
        '"gcc/10.2.0"\n\nPlease check the spelling or version number. Also try "module spider ..."\nIt is also '
        'possible your cache file is out-of-date; it may help to try:\n  $ module --ignore_cache load "gcc/10.2.0"')
HOST = "login"


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
        self.fail: dict[str, Exception] = {BAD: RuntimeError(LMOD)}   # env_setup -> what its bring-up raises
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
    assert "give the same details= again" in res.notice
    assert "lmodfac" not in app.session_facilities and "lmodfac" not in app.pending_facility_cache
    # the call the advice names: a probe and a FRESH proposal, never the failed config again
    again = await server._connect_facility(app, "lmodfac", **({"ssh_host": HOST} if named_host else {}))
    assert again.phase == "proposed_facility_details", again.notice
    assert w.brought_up == [BAD] and w.probed == [HOST]       # no second bring-up; the dropped config's host probed
    assert again.proposed_details is not None and again.proposed_details.env_setup == GOOD
    lead = ("The details= given earlier for 'lmodfac' failed to come up (hpc-bridge error: RuntimeError: remote "
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


async def test_a_bring_up_still_coming_up_that_then_fails_on_the_login_node_is_dropped(monkeypatch):
    w, app = World(monkeypatch, outcome="provisioning"), _app()
    assert (await server._connect_facility(app, "byo", details=_details(GOOD))).phase == "provisioning"
    w.fail[GOOD] = RuntimeError("remote configure failed: bash: line 1: globus-compute-endpoint: command not found")
    res = await server._connect_facility(app, "byo")   # still unproven: its failure drops it
    assert res.phase == "failed" and "is not kept" in (res.notice or "") and "byo" not in app.session_facilities
    assert (await server._connect_facility(app, "byo")).phase == "proposed_facility_details"
    assert w.brought_up == [GOOD, GOOD]


async def test_a_proven_details_config_is_kept_even_when_a_later_bring_up_fails(monkeypatch):
    w, app = World(monkeypatch), _app()
    assert (await server._connect_facility(app, "byo", details=_details(GOOD))).phase == "needs_account"
    w.fail[GOOD] = RuntimeError(LMOD)   # the site changed under a config that came up: the facility's failure
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
    w.fail["module load x"] = RuntimeError(LMOD)
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
    assert "otherwise connect_facility('byo', ssh_host='other') without details resolves the facility afresh" \
           in res.notice
    assert (await server._connect_facility(app, "byo", ssh_host="other")).phase == "proposed_facility_details"
    assert w.probed == [HOST, "other"]

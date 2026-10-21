"""#220 (a details= that failed on the login node is dropped, not run again) meets #221 (scheduler_options written as
directives and said back; a cached config with a now-refused line offered back as a proposal; a re-bind names what it
left in the scheduler).

Pinned here, each where a naive merge of the two goes wrong:
- a dropped config takes how its scheduler_options were written (`AppCtx.directive_notes`) with it: no later answer
  says that rewrite of another config;
- a config re-sent equal to the one proven for that host is kept when it fails, though an older plugin cached it with a
  bare option (compared as the cache rung serves it: written as directives);
- a cached config with a refused line, offered back as a proposal and confirmed with details= that fail, is dropped like
  any other; the next connect's offer of the same record opens with what failed (`_connect_facility` opens no proposal
  with it, so the cached-record proposal says it itself);
- a facility endpoint attached after a dropped details= opens with the failure, then names what the re-bind left, and
  never says nothing is billed; the attach clears the failure.
"""
from __future__ import annotations

import time

from hpc_bridge import binding, server
from hpc_bridge.context import SeenPilots, SshLeft
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.lifecycle import EndpointState
from tests.fakes import fake_mep_entry
from tests.test_failed_details import BAD, GOOD, HOST, World, _app, _details
from tests.test_mep_server import _Status
from tests.test_server import _FakeRunner, _Res
from tests.test_spend_terms import _ok

BARE, WRITTEN = "--constraint=gpu", "#SBATCH --constraint=gpu"
SAID = f"wrote {BARE!r} as {WRITTEN!r}"
LEAD = "The details= given earlier for 'byo' failed to come up"


def _bare(env_setup: str | None = BAD, **over):
    return _details(env_setup).model_copy(update={"scheduler_options": BARE, **over})


async def test_a_dropped_config_takes_its_directive_note_with_it(monkeypatch):
    w, app = World(monkeypatch), _app()
    binding._facility_store().put(HOST, _details(GOOD).model_dump(mode="json"))   # proven earlier: no bare option
    res = await server._connect_facility(app, "byo", details=_bare())
    assert res.phase == "failed" and "is not kept" in (res.notice or "") and SAID not in res.notice
    assert "byo" not in app.session_facilities and "byo" not in app.directive_notes   # the rewrite went with it
    # the cache rung serves the config proven for the host; its bring-up fails for the network, so the session keeps it
    w.fail[GOOD] = RuntimeError("ssh: connect to host login port 22: Connection refused")
    kept = await server._connect_facility(app, "byo")
    assert kept.phase == "failed" and kept.notice.startswith(LEAD) and SAID not in kept.notice
    del w.fail[GOOD]
    # the connect that comes up on that config never says the dropped one's rewrite of it
    up = await server._connect_facility(app, "byo")
    assert up.phase == "needs_account" and w.brought_up == [BAD, GOOD, GOOD]
    assert "scheduler_options: wrote" not in up.notice
    # given again, the config carries its rewrite again: the note goes with its config, not lost for good
    w.fail[GOOD] = RuntimeError("ssh: connect to host login port 22: Connection refused")
    again = await server._connect_facility(app, "byo", details=_bare(GOOD))
    assert again.phase == "failed" and app.directive_notes.get("byo", "").startswith(f"scheduler_options: {SAID}")
    del w.fail[GOOD]
    assert SAID in (await server._connect_facility(app, "byo")).notice


async def test_a_proven_config_an_older_plugin_cached_with_a_bare_option_is_kept_when_re_sent(monkeypatch):
    w, app = World(monkeypatch), _app()
    binding._facility_store().put(HOST, _bare().model_dump(mode="json"))   # proven by a plugin that wrote no prefix
    first = await server._connect_facility(app, "byo", details=_bare())    # the agent re-sends it as it was
    assert first.phase == "failed" and "is not kept" not in (first.notice or "") and "byo" in app.session_facilities
    assert "byo" not in app.failed_details
    again = await server._connect_facility(app, "byo", ssh_host=HOST)
    assert again.phase == "failed" and w.brought_up == [BAD, BAD]          # reconnected with it, as a proven config is …
    assert "did not run it" not in (again.notice or "")                   # … and nothing says otherwise


async def test_a_cached_refusal_proposal_confirmed_with_failing_details_is_dropped_like_any_other(monkeypatch):
    w, app = World(monkeypatch), _app()
    store = binding._facility_store()
    store.put(HOST, {**_details(BAD).model_dump(mode="json"), "scheduler_options": f"{BARE}\nmodule load y"})
    offer = await server._connect_facility(app, "byo", ssh_host=HOST)
    assert offer.phase == "proposed_facility_details" and "no longer accepted" in (offer.notice or "")
    assert offer.proposed_details is not None and offer.proposed_details.scheduler_options == WRITTEN
    assert not offer.notice.startswith("The details= given earlier")      # nothing failed yet
    confirmed = offer.proposed_details.model_copy(update={"env_setup": None})   # env_setup: the cached one (BAD)
    res = await server._connect_facility(app, "byo", details=confirmed)
    assert res.phase == "failed" and "is not kept" in (res.notice or "") and w.brought_up == [BAD]
    assert "byo" not in app.session_facilities and "byo" not in app.pending_facility_cache
    assert "byo" not in app.directive_notes and app.failed_details["byo"].ssh_host == HOST
    # the next connect without details: the same record, offered again — opening with what failed, never run
    back = await server._connect_facility(app, "byo")
    assert back.phase == "proposed_facility_details" and w.brought_up == [BAD]
    assert back.notice.startswith(LEAD) and "is not kept: this connect did not run it." in back.notice
    assert back.notice.count("failed to come up") == 1 and "no longer accepted" in back.notice
    # corrected and confirmed: up, the failure history, the confirmed config cached
    up = await server._connect_facility(app, "byo", details=confirmed.model_copy(update={"env_setup": GOOD}))
    assert up.phase == "needs_account" and "byo" not in app.failed_details and not up.notice.startswith(LEAD)
    assert store.get(HOST)["env_setup"] == GOOD and store.get(HOST)["scheduler_options"] == WRITTEN


async def test_a_facility_endpoint_attached_after_a_dropped_details_names_the_failure_then_what_was_left(monkeypatch):
    w, app = World(monkeypatch, catalog=[fake_mep_entry(id="byo")]), _app()
    built = binding._facility_from_entry

    def build(entry, *, account):
        if entry.compute_mep_uuid:   # the registry's facility endpoint: a real MEPFacility, attached with no SSH
            return MEPFacility.from_entry(entry, account=account or None, client_factory=_Status)
        return built(entry, account=account)

    monkeypatch.setattr(binding, "_facility_from_entry", build)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "ok\n", ""),
                                                                                    canary_result=_ok())
    res = await server._connect_facility(app, "byo", details=_details())   # details= override the catalogued id
    assert res.phase == "failed" and "is not kept" in (res.notice or "") and "byo" in app.failed_details
    # meanwhile an SSH endpoint of this session holds a pilot the scheduler keeps HELD
    old = "0b1c2d3e-0000-4000-8000-0000000000aa"
    app.state = EndpointState(endpoint_id=old)
    app.ssh_left[old] = SshLeft(up=False, pilots=SeenPilots(jobs=(("0.pbsserver", "H", "x", "H"),),
                                                            at=time.monotonic()))
    mep = await server._connect_facility(app, "byo")
    assert mep.phase == "needs_account", mep.notice
    notice = mep.notice or ""
    left = f"This connect cancels nothing on the endpoint it left ({old})"
    assert notice.startswith(LEAD) and left in notice and "pilot 0.pbsserver, held" in notice
    assert notice.index(LEAD) < notice.index(left) < notice.index("Nothing new is started or billed by this connect")
    assert "Nothing is started or billed yet" not in notice
    assert notice.count("failed to come up") == 1 and notice.count(left) == 1   # each said once
    assert "byo" not in app.failed_details and w.brought_up == [BAD]       # attached: the failure is history
    assert not (await server._connect_facility(app, "byo")).notice.startswith(LEAD)

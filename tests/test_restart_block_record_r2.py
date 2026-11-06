"""Review round 2 of the restart block record (on 244a56f), test-first. In conftest's isolated state.

1. A hostname change hid every earlier record (round 1 keyed "another host" on gethostname, which a macOS laptop with
   no `scutil HostName` takes from its network): connect said nothing billed, a yes added a second block unasked, a
   cold stop drained forever telling a dead session to stop. Liveness now keys on a STABLE machine identity
   (/etc/machine-id, macOS IOPlatformUUID), "elsewhere" only when both are known and differ; an "elsewhere" record is
   still read against the scheduler, and closed when gone.
2. A pinned session's pre-bring-up swallowed a failed bootstrap and the billed bring-up tried again: two SSH logins.
3. That pre-bring-up ran for an unconfirmed call too, and for records of any facility: a spend question ran the
   bootstrap. A spend question names the session's facility's records unverified and brings nothing up; the record
   is read after the yes, once the bring-up knows the endpoint id (round 3: warmth._provision's one choke point).
4. One endpoint's failed ask left it unreleased while the others were: it is released and answered unconfirmed.
5. A cold question also ran the session's own MEP stop (which pauses batches), and `combined` dropped its jobs and
   handoffs.
6. On a facility endpoint a dead server's presumed-lost row keeps its slot (a late delivery can still run).
7. A facility endpoint's record is named as unverified, and as possibly ended.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

from hpc_bridge import batch, binding, earlier, server
from hpc_bridge.context import AppCtx, LeftBlock
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.ledger import Ledger
from hpc_bridge.models import EndpointStatus
from hpc_bridge.profile import Profile
from hpc_bridge.server import _ensure_endpoint_up, _stop_endpoint
from tests.fakes import FakeCatalog, fake_entry, fake_mep_entry
from tests.test_mep_server import _Status
from tests.test_restart_block_record import (
    EID,
    _connect,
    _listing,
    _open,
    _server,
    _session_1,
    _Slurm,
)
from tests.test_server import _scripted_login


def _set(**cols) -> None:
    """Change every open block record (a test's own world)."""
    led = Ledger.open()
    for k, v in cols.items():
        led._con.execute(f"update block set {k} = ?", (v,))


# --- 1. a machine identity, not a hostname ---------------------------------------------------------------------------


async def test_1_a_hostname_change_does_not_hide_this_machines_record(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    _set(owner_host="dhcp-17-4.campus.example.edu")    # the laptop moved networks: gethostname followed it
    s2 = _server(monkeypatch, fac)
    res, seen = await _connect(s2, _listing(n=n))
    assert len(seen) == 1 and "An earlier hpc-bridge session left block job 40 (RUNNING)" in res.notice
    assert "Nothing is started or billed yet" not in res.notice
    s3 = _server(monkeypatch, fac, bound=False)
    _scripted_login(monkeypatch, releases=["released 40\n"])
    stop = await _stop_endpoint(s3)
    assert stop.status == "down" and stop.spend_stopped is True and "released 40" in stop.notice


async def test_1_a_record_with_no_machine_identity_is_this_machines_when_its_hostname_is_this_one(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    _set(owner_machine=None, owner_host=earlier._me().host)     # unknown identity, this hostname (round 3's spec)
    s2 = _server(monkeypatch, fac, bound=False)
    _scripted_login(monkeypatch, releases=["released 40\n"])
    stop = await _stop_endpoint(s2)
    assert stop.status == "down" and stop.spend_stopped is True


async def test_1_a_record_of_another_machine_is_never_released_and_names_the_users_cancel(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    _set(owner_machine="0f0e0d0c-another-machine", owner_host="another-host.example")
    s2 = _server(monkeypatch, fac, bound=False)
    log = _scripted_login(monkeypatch, probes=[_listing()])     # still listed: left alone
    st = await _stop_endpoint(s2)
    assert [k for k, _c in log] == ["probe"] and st.status == "draining" and st.spend_stopped is not True
    assert "another-host.example" in st.notice and "job 40" in st.notice and "scancel 40" in st.notice


async def test_1_an_elsewhere_record_whose_block_is_gone_is_closed_by_the_read(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    _set(owner_machine="0f0e0d0c-another-machine")
    s2 = _server(monkeypatch, fac)
    res, seen = await _connect(s2, f"HPCB_NOW {int(time.time())}\n")   # the scheduler lists nothing of it
    assert len(seen) == 1 and _open(EID) == [] and "Nothing is started or billed yet" in res.notice
    s3 = _server(monkeypatch, fac, bound=False)
    assert (await _stop_endpoint(s3)).notice == "no endpoint was up"


def test_1_the_machine_identity_is_read_once_and_recorded(monkeypatch):
    calls = []

    def read():
        calls.append(1)
        return "abc-123"

    monkeypatch.setattr(earlier, "_MACHINE", None)
    monkeypatch.setattr(earlier, "_read_machine_id", read)
    assert earlier._machine_id() == earlier._machine_id() == "abc-123" and calls == [1]


# --- 2-3. a pinned session: one bring-up at most, and none for a spend question --------------------------------------


class _Counted(_Slurm):
    def __init__(self, *, fail: bool = False) -> None:
        super().__init__()
        self.attempts = 0
        self.fail = fail

    async def provision(self, profile):
        self.attempts += 1
        if self.fail:
            raise RuntimeError("ssh: Permission denied (publickey,keyboard-interactive)")
        return await super().provision(profile)


async def _pinned(monkeypatch, fac, *, online: bool = True):
    """Session 1 connected `mine` and left job 40; a new server pinned to `mine` (HPC_BRIDGE_MACHINE) that never
    connected, its facility's manager online over the web or not."""
    _s1, n = await _session_1(monkeypatch, fac)
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "mine")
    fac.attempts = 0
    fac.manager_up = online
    s2 = _server(monkeypatch, fac)
    assert s2.state.endpoint_id is None
    return s2, n


async def test_2_a_failed_pinned_bootstrap_is_tried_once_and_answered(monkeypatch):
    fac = _Counted()
    s2, _n = await _pinned(monkeypatch, fac, online=False)
    fac.fail = True
    st = await _ensure_endpoint_up(s2, confirm_spend=True)
    assert fac.attempts == 1, "two SSH bring-ups in one call: twice the auth failures (fail2ban counts them)"
    assert st.status == "down" and "Permission denied" in st.notice


async def test_3_a_spend_question_with_the_endpoint_offline_brings_nothing_up(monkeypatch):
    fac = _Counted()
    s2, _n = await _pinned(monkeypatch, fac, online=False)
    log = _scripted_login(monkeypatch)
    st = await _ensure_endpoint_up(s2)                     # the spend question: no yes
    assert fac.attempts == 0 and log == [] and s2.built == []
    assert st.status == "needs_confirmation" and "block job 40" in st.notice and "could not be read" in st.notice


async def test_3_a_spend_question_with_the_endpoint_online_brings_nothing_up_either(monkeypatch):
    fac = _Counted()
    s2, n = await _pinned(monkeypatch, fac, online=True)
    log = _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2)
    assert fac.attempts == 0 and log == []                        # round 3: no web shortcut to bind it either
    assert not [u for u in s2.built if u.get("compute")]          # no compute runner: nothing billed is near
    assert st.status == "needs_confirmation" and "block job 40" in st.notice and "could not be read" in st.notice


async def test_3_a_record_of_another_facility_never_brings_a_pinned_session_up(monkeypatch):
    fac = _Counted()
    s2, _n = await _pinned(monkeypatch, fac, online=False)
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "another-facility")
    log = _scripted_login(monkeypatch)
    st = await _ensure_endpoint_up(s2, confirm_spend=False)
    assert fac.attempts == 0 and log == [] and s2.built == [] and st.status == "needs_confirmation"
    assert "job 40" not in st.notice


async def test_3_after_the_yes_the_record_is_read_before_reuse_or_a_second_block(monkeypatch):
    fac = _Counted()
    s2, n = await _pinned(monkeypatch, fac, online=False)
    ask = await _ensure_endpoint_up(s2)                     # named unverified: nothing up
    assert fac.attempts == 0 and "block job 40" in ask.notice
    fac.manager_up = None                                   # by the yes the endpoint answers over the web: no SSH
    log = _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2, confirm_spend=True)
    # one bring-up: the facility's own, which finds the endpoint online over the web before any SSH (round 3)
    assert fac.attempts == 1 and [k for k, _c in log] == ["probe"]
    assert s2.shapes["compute"].user_endpoint_config["hpcb_block"] == n and "Reused block job 40" in st.notice


async def test_3_after_the_yes_an_offline_endpoint_is_brought_up_once_then_read(monkeypatch):
    fac = _Counted()
    s2, n = await _pinned(monkeypatch, fac, online=False)
    _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2, partition="other", confirm_spend=True)
    assert fac.attempts == 1                                # the yes allows the bring-up, and it happens once
    assert st.status == "needs_confirmation" and "block job 40" in st.notice and "second block" in st.notice


# --- 4. a failed ask: released all the same, never down ---------------------------------------------------------------


async def test_4_a_failed_ask_still_releases_that_endpoint_and_answers_unconfirmed(monkeypatch):
    from hpc_bridge import jobs as jobmode

    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac, bound=False)

    async def slow(app, budget_s=20.0, adopt=True):
        raise TimeoutError

    monkeypatch.setattr(jobmode, "hand_results", slow)
    log = _scripted_login(monkeypatch, releases=["released 40\n"])
    st = await _stop_endpoint(s2)
    assert [k for k, _c in log] == ["release"] and "released 40" in st.notice
    assert st.status == "draining" and st.spend_stopped is not True


# --- 5. a cold question changes nothing, and keeps its fields ----------------------------------------------------------


async def test_5_a_cold_question_runs_no_other_stop_and_keeps_its_jobs(monkeypatch):
    from hpc_bridge import jobs as jobmode

    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    app = _server(monkeypatch, fac, bound=False)
    app.facility = MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Status("online"))  # detached
    app.left_blocks = [LeftBlock(endpoint_id=None, job="100", until=time.monotonic() + 600, left_at=time.monotonic(),
                                 how="detach")]
    paused = []
    monkeypatch.setattr(batch, "pause_all", lambda a, why: paused.append(why) or [])

    async def live(a, reconcile=True):
        return [{"id": "job-1", "native_id": "77", "state": "running", "envelope_id": "env-1"}]

    async def hands(a, budget_s=20.0, adopt=True):
        return []

    asked = EndpointStatus.model_construct(status="needs_confirmation", block_state="cold", notice="choose",
                                           jobs=[SimpleNamespace(id="job-1")], handoffs=[], spend_stopped=None)
    monkeypatch.setattr(jobmode, "billing_jobs", live)
    monkeypatch.setattr(jobmode, "hand_results", hands)
    monkeypatch.setattr(jobmode, "stop_question", lambda a, rows, h: asked.model_copy())
    st = await _stop_endpoint(app)
    assert st.status == "needs_confirmation" and paused == [], "the question changed state (a batch paused)"
    assert [getattr(j, "id", None) for j in st.jobs] == ["job-1"]


def test_5_combined_keeps_every_parts_jobs_and_handoffs():
    a = EndpointStatus.model_construct(status="down", block_state="cold", spend_stopped=True, notice="a",
                                       block_job_id=None, jobs=[SimpleNamespace(id="job-1")],
                                       handoffs=[SimpleNamespace(handoff_id="h1")])
    b = EndpointStatus.model_construct(status="draining", block_state="cold", spend_stopped=None, notice="b",
                                       block_job_id=None, jobs=[SimpleNamespace(id="job-2")], handoffs=[])
    out = earlier.combined([a, b])
    assert [j.id for j in out.jobs] == ["job-1", "job-2"] and [h.handoff_id for h in out.handoffs] == ["h1"]


# --- 6. a facility endpoint keeps a dead server's presumed-lost slot ---------------------------------------------------


def test_6_on_a_facility_endpoint_a_gone_senders_row_still_holds_its_slot():
    from tests.test_batch_phase1 import _owner

    led = Ledger.open()
    led.pid_alive = lambda pid: pid != 4242
    led.add_batch("b1", command="x", rows=[{"n": 0}, {"n": 1}], task_walltime_s=60, capacity=1, results_dir="/r",
                  idempotent=False, endpoint_id="mep", uec_hash="h", session_id="s", owner=_owner())
    assert len(led.reserve("b1", _owner(), 1, uec_hash="h")) == 1
    led._con.execute("update task set state = 'presumed_lost', sent_pid = 4242, admitted_at = admitted_at - 3600 "
                     "where state = 'in_flight'")
    assert led.reserve("b1", _owner(), 1, uec_hash="h", by_sender=False) == []      # MEP: it holds
    assert len(led.reserve("b1", _owner(), 1, uec_hash="h")) == 1                   # SSH: its wrapper has ended


def test_6_the_presumption_docstring_says_what_holds_now():
    doc = batch._presume.__doc__ or ""
    assert "keeps its slot" not in doc and "sender" in doc


# --- 7. a facility endpoint's record says it may have ended -----------------------------------------------------------


async def test_7_a_facility_endpoints_record_is_named_as_possibly_ended(monkeypatch):
    def mep_server(*, dead: bool) -> AppCtx:
        fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Status("online"))
        fac.max_idletime_s = 600
        app = AppCtx(facility=fac, profile=Profile())
        app.ledger = Ledger.open()
        if dead:
            app.ledger.pid_alive = lambda pid: False
        from tests.test_restart_block_record import _answer
        from tests.test_server import _FakeRunner, _Res

        runner = _FakeRunner("x", _Res(0, "ok", ""), canary_result=_answer("100"))
        app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
        monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_mep_entry()]))
        monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: fac)
        return app

    s1 = mep_server(dead=False)
    await server._connect_facility(s1, "globus1")
    await _ensure_endpoint_up(s1, confirm_spend=True)
    res = await server._connect_facility(mep_server(dead=True), "globus1")
    assert "not verified" in res.notice and "may have ended" in res.notice


_ = fake_entry

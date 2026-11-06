"""A billed block an earlier hpc-bridge server process started is visible to, and in the control of, the next one.

Seen four times in free agent tests on the fake Slurm cluster (`default`, SSH bring-up), each phase of a chain cell a
fresh server process on the same state directory, with no teardown between:
1. a new session's connect_facility reused the endpoint (zero SSH) and said "Nothing is started or billed yet. The next
   block's terms: …" while the earlier session's block (job 40) was RUNNING;
2. its next billed start (ensure_endpoint_up, or a batch's yes) submitted a SECOND block beside it (jobs 40+41, 9+10,
   25+26; once the second sat PENDING (Resources) behind the first);
3. a new session's stop_endpoint before any connect answered `down`, "no endpoint was up", `spend_stopped` None, and
   released nothing: job 33 billed on (the world check stop_honesty_no_pilot_left caught it);
4. while a stop AFTER a connect released both ("released 41 40"): the release lists the endpoint's blocks by their
   `uep.<eid>` marker, so it sees every block of the endpoint — the cold stop never got there (nothing bound, so no
   endpoint id).
The cause: what hpc-bridge knew of the blocks it started lived only in the server process. Now every billed block
request is RECORDED in the ledger (`block`: its endpoint, block request, terms, its scheduler job once a check reports
it, the server that asked), and checked against the scheduler (through the login endpoint, the free shape) before it is
named, reused or released; a record whose job is gone is dropped quietly.

A restart is a fresh AppCtx over the same state directory (conftest's). The test's "processes" share its pid, so a
LATER server is given a ledger whose `pid_alive` seam says the earlier one is gone (`_server(earlier_dead=True)`).
"""
from __future__ import annotations

import time
from types import SimpleNamespace

from hpc_bridge import binding, connect, earlier, scheduler_ops, server
from hpc_bridge.context import AppCtx, _no_facility_bound
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.ledger import Ledger
from hpc_bridge.models import FacilityDetails, ShellOutcome
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import _ensure_endpoint_up, _stop_endpoint, _teardown_endpoint
from tests.fakes import MEP_UUID, FakeCatalog, FakeFacility, fake_mep_entry
from tests.test_mep_server import _Status
from tests.test_server import _SBATCH_REFUSED, _bash, _FakeRunner, _none, _Res, _scripted_login, _stub_scheduler

EID = "fake-eid"
UEP = "aaaaaaaa-0000-0000-0000-00000000000a"
_DETAILS = dict(ssh_host="login.example.edu", interface="eth0", env_setup="module load x",
                scratch_root="/scratch/{user}", partition="debug", scheduler="slurm")


class _Slurm(FakeFacility):
    """The world's facility — the login node, its scheduler and the endpoint hpc-bridge stood up there — shared by
    every server process of a test: an endpoint of this plugin (a fresh compute user endpoint per block request, each
    logging its request), reused over the web (zero SSH)."""

    def __init__(self) -> None:
        super().__init__()
        self.workers = 1
        self.reused = True
        self.profile = SimpleNamespace(scheduler="slurm")
        self.fresh_ueps = True
        self.logs_block_requests = True


def _answer(job: str | None) -> CanaryResult:
    return CanaryResult(ok=True, worker_host="c1", worker_python="3.12.3", worker_dill="0.3.9", worker_job=job,
                        answered_at=time.monotonic())


def _server(monkeypatch, fac, *, job: str | None = "40", bound: bool = True, earlier_dead: bool = True,
            canary: CanaryResult | None = None) -> AppCtx:
    """One hpc-bridge server process: its own AppCtx and runners over the shared state directory. `bound`: the
    facility as a connect leaves it bound, else the implicit one (nothing pinned or connected, as lifespan starts).
    `earlier_dead`: every other server process that wrote the ledger is gone."""
    app = AppCtx(facility=fac if bound else binding._default_local_facility(unbound=True), profile=Profile())
    app.ledger = Ledger.open()
    if earlier_dead:
        app.ledger.pid_alive = lambda pid: False
    app.built = []
    login = _FakeRunner(EID, _Res(0, "", ""))
    app.compute = _FakeRunner(EID, _Res(0, "", ""), canary_result=canary or _answer(job))

    def factory(eid, user_endpoint_config=None, **_kw):
        uec = dict(user_endpoint_config or {})
        app.built.append(uec)
        return app.compute if uec.get("compute") else login

    app.runner_factory = factory
    monkeypatch.setattr(binding, "_facility_from_entry", lambda entry, *, account: fac)
    return app


async def _connect(app: AppCtx, listing: str = "") -> tuple:
    """connect_facility to the user's own facility (its endpoint reused over the web). The login runner answers the
    one scheduler read a record of an earlier session's block asks for with `listing`; the commands it got are
    returned."""
    seen: list[str] = []

    async def run_login(cmd):
        seen.append(cmd)
        return ShellOutcome(phase="complete", exit_code=0, stdout=listing, block_state="warm")

    res = await connect._connect_facility(app, "mine", details=FacilityDetails(**_DETAILS), run_login=run_login)
    return res, seen


async def _session_1(monkeypatch, fac, *, job: str | None = "40", partition: str | None = None,
                     canary: CanaryResult | None = None) -> tuple[AppCtx, int]:
    """The earlier server: connect, confirm a block, the block comes up as `job` — then the server ends (a chain's
    phase ends, Claude Code quits): its block runs on. Returns it and the block request its block was asked for."""
    s1 = _server(monkeypatch, fac, job=job, earlier_dead=False, canary=canary)
    res, _ = await _connect(s1)
    assert res.phase == "needs_account" and "Nothing is started or billed yet" in res.notice
    await _ensure_endpoint_up(s1, partition=partition, confirm_spend=True)
    return s1, int(s1.shapes["compute"].user_endpoint_config["hpcb_block"])


def _listing(job: str = "40", n: int | None = None, state: str = "RUNNING", reason: str = "None") -> str:
    """The scheduler's rows for the endpoint's marker (scheduler_ops._pilot_status_cmd on Slurm): the pilot, the user
    endpoint it belongs to, and the block request that user endpoint was started for."""
    out = f"HPCB_NOW {int(time.time())}\n{state} {job} - {reason}\nHPCB_DIR {job} {UEP}\n"
    return out + (f"HPCB_BREQ {UEP} {n}\n" if n is not None else "")


def _open(eid: str | None = None) -> list[dict]:
    return Ledger.open().open_blocks(eid)


# --- the record ----------------------------------------------------------------------------------------------------


async def test_a_billed_block_request_is_recorded_when_it_is_asked_for_and_its_job_once_a_check_reports_it(monkeypatch):
    fac = _Slurm()
    s1, n = await _session_1(monkeypatch, fac, partition="debug")
    rows = _open(EID)
    assert len(rows) == 1
    r = rows[0]
    assert r["request"] == str(n) and r["facility"] == "mine" and r["scheduler"] == "slurm" and not r["mep"]
    assert r["partition"] == "debug" and r["job"] == "40" and r["up_at"] is not None
    assert r["session"] == s1.session_token and r["owner_pid"] and r["entry"]      # reachable again from the record


async def test_a_rejected_submission_closes_its_record(monkeypatch):
    fac = _Slurm()
    s1 = _server(monkeypatch, fac, canary=CanaryResult(ok=False, error=_SBATCH_REFUSED), earlier_dead=False)
    await _connect(s1)
    st = await _ensure_endpoint_up(s1, confirm_spend=True)
    assert st.status == "down" and "REJECTED" in st.notice
    assert _open(EID) == []                                        # no job was ever issued: nothing to name or release


async def test_a_teardown_closes_its_endpoints_records(monkeypatch):
    fac = _Slurm()
    s1, _n = await _session_1(monkeypatch, fac)
    assert _open(EID)
    monkeypatch.setattr(server, "_teardown_preauth_gate", lambda *a, **k: _none())
    _scripted_login(monkeypatch, releases=["released 40\n"])
    assert (await _teardown_endpoint(s1)).status == "down"
    assert _open(EID) == []


# --- 1. the connect (and the ensure answer) of a new session names the block still up -------------------------------


async def test_1_a_new_sessions_connect_names_the_earlier_block_and_never_says_nothing_is_billed(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac)
    res, seen = await _connect(s2, _listing(n=n))
    assert res.phase == "needs_account"
    assert len(seen) == 1 and "uep.fake-eid" in seen[0]          # checked against the scheduler, over the login endpoint
    assert "Nothing is started or billed yet" not in res.notice
    assert "Nothing new is started or billed by this connect" in res.notice
    assert "An earlier hpc-bridge session left block job 40 (RUNNING) on this endpoint" in res.notice
    assert "still billing" in res.notice and "stop_endpoint cancels it" in res.notice
    assert "a yes to them reuses it" in res.notice               # the next block's terms are its terms


async def test_1_the_ensure_answer_of_a_new_session_names_it_too(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac, partition="debug")
    s2 = _server(monkeypatch, fac)
    res, _ = await _connect(s2, _listing(n=n))
    assert "Its terms differ from the next block's" in res.notice and "second block" in res.notice
    st = await _ensure_endpoint_up(s2)                           # the spend question
    assert st.status == "needs_confirmation"
    assert "An earlier hpc-bridge session left block job 40 (RUNNING) on this endpoint" in st.notice


async def test_1_a_pilot_of_the_earlier_request_still_queued_is_named_as_waiting(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac, canary=CanaryResult(ok=False, error="timeout"))
    assert _open(EID)[0]["job"] is None                           # asked for, never seen up
    s2 = _server(monkeypatch, fac)
    res, _ = await _connect(s2, _listing("41", n=n, state="PENDING", reason="Resources"))
    assert "job 41 (PENDING, reason 'Resources')" in res.notice and "Nothing is started" not in res.notice


async def test_1_a_record_whose_job_is_gone_is_dropped_quietly(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac)
    res, seen = await _connect(s2, f"HPCB_NOW {int(time.time())}\n")   # the scheduler lists nothing of the endpoint
    assert len(seen) == 1
    assert "Nothing is started or billed yet" in res.notice and "earlier" not in res.notice and "40" not in res.notice
    assert _open(EID) == []
    _res, seen = await _connect(_server(monkeypatch, fac))
    assert seen == []                                            # closed: nothing is read for it again


async def test_1_a_record_past_its_walltime_is_dropped_without_a_read(monkeypatch):
    led = Ledger.open()
    old = time.time() - 7200
    led.record_block(EID, "7", facility="mine", entry=None, mep=False, scheduler="slurm", uec_hash="h",
                     partition=None, account=None, walltime_s=3600.0, session="gone", owner_host="h", owner_pid=1,
                     owner_start=None, now=old)
    led.block_up(EID, "7", "33", at=old + 10)
    res, seen = await _connect(_server(monkeypatch, _Slurm()))
    assert seen == [] and "Nothing is started or billed yet" in res.notice and _open(EID) == []


# --- 2. a billed start reuses it on the same terms, else asks — never a silent second block -------------------------


async def test_2_a_yes_on_the_same_terms_reuses_the_earlier_block(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac)
    await _connect(s2, _listing(n=n))
    st = await _ensure_endpoint_up(s2, confirm_spend=True)
    assert st.status == "up" and st.block_job_id == "40"
    assert s2.shapes["compute"].user_endpoint_config["hpcb_block"] == n     # its user endpoint: no new block request
    assert [u.get("hpcb_block") for u in s2.built if u.get("compute")] == [n]
    assert "Reused block job 40, which an earlier hpc-bridge session left running" in st.notice
    rows = _open(EID)
    assert len(rows) == 1 and rows[0]["session"] == s2.session_token        # this session's block now


async def test_2_a_yes_on_other_terms_asks_first_and_starts_the_second_block_only_on_a_yes_given_knowing(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac)
    await _connect(s2, _listing(n=n))
    _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2, partition="other", confirm_spend=True)
    assert st.status == "needs_confirmation" and not s2.shapes["compute"].spend_confirmed
    assert s2.compute.canaries == 0                              # nothing was sent: no block was asked for
    assert "block job 40" in st.notice and "second block beside it" in st.notice
    s2.compute._canary = _answer("41")
    again = await _ensure_endpoint_up(s2, partition="other", confirm_spend=True)   # the user's yes, knowing
    assert again.status == "up" and again.block_job_id == "41"
    assert s2.shapes["compute"].user_endpoint_config["hpcb_block"] != n
    assert "job 40 still bills beside this one" in again.notice
    # 4: the stop after a connect releases both, and says down only then; both records close
    _scripted_login(monkeypatch, releases=["released 41 40\n"])
    stop = await _stop_endpoint(s2)
    assert stop.status == "down" and stop.spend_stopped is True and "released 41 40" in stop.notice
    assert _open(EID) == []


async def test_2_a_batchs_yes_on_other_terms_asks_too(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac, partition="debug")
    s2 = _server(monkeypatch, fac)
    await _connect(s2, _listing(n=n))
    st = await server._batch_ensure_up(s2, True)
    assert st.status == "needs_confirmation" and not s2.shapes["compute"].spend_confirmed
    assert s2.compute.canaries == 0 and "block job 40" in st.notice and "second block beside it" in st.notice


async def test_2_a_held_earlier_pilot_is_never_reused(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac, canary=CanaryResult(ok=False, error="timeout"))
    s2 = _server(monkeypatch, fac)
    await _connect(s2, _listing("41", n=n, state="PENDING", reason="JobHeldUser"))
    st = await _ensure_endpoint_up(s2, confirm_spend=True)
    assert st.status == "needs_confirmation" and s2.compute.canaries == 0 and "job 41" in st.notice


# --- 3. a stop on a cold process releases them, and says down only once they are gone -------------------------------


async def test_3_a_cold_stop_releases_the_earlier_sessions_block(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac, bound=False)                   # nothing connected: the stop is its first call
    log = _scripted_login(monkeypatch, releases=["released 40\n"])
    st = await _stop_endpoint(s2)
    assert st.status == "down" and st.spend_stopped is True
    assert "no endpoint was up" not in st.notice and "released 40" in st.notice and "job 40" in st.notice
    assert [c for k, c in log if k == "release"] == [scheduler_ops._release_cmd("slurm", EID)]   # this endpoint's only
    assert _open() == [] and _no_facility_bound(s2) and s2.state.endpoint_id is None             # nothing bound by it
    again = await _stop_endpoint(_server(monkeypatch, fac, bound=False))
    assert again.status == "down" and again.notice == "no endpoint was up"


async def test_3_a_cold_stop_reaches_every_recorded_endpoint_and_is_down_only_when_each_is(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    entry = _open(EID)[0]["entry"]
    other = "22222222-0000-0000-0000-000000000002"
    led = Ledger.open()
    here = earlier._me().host   # a record of this machine (round 3's spec: its id, namespace and hostname)
    mine = dict(owner_ns=earlier._me_ns(), owner_machine=earlier._machine_id())
    led.record_block(other, "9", facility="mine", entry=entry, mep=False, scheduler="slurm", uec_hash="h",
                     partition=None, account=None, walltime_s=None, session="gone", owner_host=here, owner_pid=1,
                     owner_start=None, **mine)
    led.block_up(other, "9", "7")
    s2 = _server(monkeypatch, fac, bound=False)
    log = _scripted_login(monkeypatch, releases=["released 40\n", "released 7\n"])
    st = await _stop_endpoint(s2)
    assert st.status == "down" and st.spend_stopped is True and " Also: " in st.notice
    assert sorted(c for k, c in log if k == "release") == sorted(
        [scheduler_ops._release_cmd("slurm", EID), scheduler_ops._release_cmd("slurm", other)])
    assert _open() == []
    # one of them not confirmed: the whole answer is not `down`
    await _session_1(monkeypatch, fac)
    led.record_block(other, "9", facility="mine", entry=entry, mep=False, scheduler="slurm", uec_hash="h",
                     partition=None, account=None, walltime_s=None, session="gone", owner_host=here, owner_pid=1,
                     owner_start=None, **mine)
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    _scripted_login(monkeypatch, releases=["released 40\n", None, None, None])
    st = await _stop_endpoint(_server(monkeypatch, fac, bound=False))
    assert st.status == "draining" and st.spend_stopped is not True


async def test_3_a_cold_stop_whose_release_is_not_confirmed_is_draining_and_keeps_the_record(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    s2 = _server(monkeypatch, fac, bound=False)
    _scripted_login(monkeypatch, releases=[None, None, None])    # the login worker stays cold
    st = await _stop_endpoint(s2)
    assert st.status == "draining" and st.spend_stopped is not True and "job 40" in st.notice
    assert len(_open(EID)) == 1


async def test_3_a_cold_stop_that_cannot_reach_an_offline_endpoint_never_says_down(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    fac.manager_up = False                                        # the endpoint's manager is gone
    s2 = _server(monkeypatch, fac, bound=False)
    _scripted_login(monkeypatch, releases=[None, None, None])
    st = await _stop_endpoint(s2)
    assert st.status == "draining" and st.spend_stopped is not True
    assert "job 40" in st.notice and "scancel 40" in st.notice


async def test_3_a_cold_stop_with_no_globus_login_says_it_cannot_reach_the_block(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac, bound=False)
    s2.login_flow = SimpleNamespace(login_required=lambda: True)
    log = _scripted_login(monkeypatch)
    st = await _stop_endpoint(s2)
    assert log == [] and st.status == "draining" and st.spend_stopped is not True
    assert "job 40" in st.notice and "Globus login" in st.notice and "connect_facility('mine')" in st.notice


# --- 4. ownership: never a job hpc-bridge did not start ---------------------------------------------------------------


async def test_4_a_cold_stop_leaves_a_live_sessions_block_alone_and_says_so(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac, bound=False, earlier_dead=False)   # the first server still runs (another session)
    log = _scripted_login(monkeypatch)
    st = await _stop_endpoint(s2)
    assert log == [] and st.status == "draining" and st.spend_stopped is not True
    assert "another hpc-bridge session that is still running" in st.notice and "job 40" in st.notice


async def test_4_a_connect_never_reuses_nor_names_a_live_sessions_block_as_an_earlier_one(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac, earlier_dead=False)
    res, seen = await _connect(s2, _listing(n=n))
    assert seen == [] and "earlier hpc-bridge session" not in res.notice


def test_4_the_release_cancels_only_this_endpoints_blocks(tmp_path):
    # the release the cold stop sends is the one every stop sends: by the endpoint's own `uep.<eid>` marker. The
    # user's own job, a submit_job job (its own directory under the scratch root) and another endpoint's block are
    # never touched
    q = (f"40 /home/u/.globus_compute/uep.{EID}.{UEP}/submit_scripts/parsl.slurm.1.stdout\n"
         "7 /home/u/slurm-7.out\n"
         "8 /scratch/u/hpc-bridge/jobs/job-1/slurm-8.out\n"
         f"9 /home/u/.globus_compute/uep.other-eid.{UEP}/submit_scripts/parsl.slurm.1.stdout\n")
    env = _stub_scheduler(tmp_path, squeue=(q, 0), scancel=("", 0))
    assert _bash(scheduler_ops._release_cmd("slurm", EID), env).strip() == "released 40"
    assert (tmp_path / "calls").read_text().count("scancel") == 1 and "scancel 40\n" in (tmp_path / "calls").read_text()


async def test_4_a_jobs_mode_job_still_asks_first_on_a_cold_stop(monkeypatch):
    # a submit_job job keeps its own keep/cancel question: the cold stop asks it before releasing anything
    from hpc_bridge import jobs as jobmode

    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac, bound=False)
    asked = SimpleNamespace(status="needs_confirmation")

    async def live(app, reconcile=True):
        return [{"id": "job-1"}]

    async def hands(app, budget_s=20.0, adopt=True):
        return []

    monkeypatch.setattr(jobmode, "billing_jobs", live)
    monkeypatch.setattr(jobmode, "hand_results", hands)
    monkeypatch.setattr(jobmode, "stop_question", lambda app, rows, hands: asked)
    log = _scripted_login(monkeypatch)
    assert await _stop_endpoint(s2) is asked and log == []
    assert len(_open(EID)) == 1


# --- 5. a facility multi-user endpoint: named, never claimed stopped ------------------------------------------------


def _mep_server(monkeypatch, *, job="100", earlier_dead=True, bound=True) -> AppCtx:
    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Status("online"))
    fac.max_idletime_s = 600
    app = AppCtx(facility=fac if bound else binding._default_local_facility(unbound=True), profile=Profile())
    app.ledger = Ledger.open()
    if earlier_dead:
        app.ledger.pid_alive = lambda pid: False
    runner = _FakeRunner(MEP_UUID, _Res(0, "ok", ""), canary_result=_answer(job))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([fake_mep_entry()]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: fac)
    return app


async def test_5_a_new_sessions_attach_to_a_facility_endpoint_names_the_earlier_block_unverified(monkeypatch):
    s1 = _mep_server(monkeypatch, earlier_dead=False)
    assert (await server._connect_facility(s1, "globus1")).phase == "needs_account"
    assert (await _ensure_endpoint_up(s1, confirm_spend=True)).status == "up"
    assert _open(MEP_UUID)[0]["mep"] and _open(MEP_UUID)[0]["job"] == "100"
    s2 = _mep_server(monkeypatch)
    res = await server._connect_facility(s2, "globus1")
    assert res.phase == "needs_account" and "Nothing is started or billed yet" not in res.notice
    assert "An earlier hpc-bridge session's block on this facility endpoint is on record: scheduler job 100" in res.notice
    assert "not verified" in res.notice and "scancel 100" in res.notice
    stop = await _stop_endpoint(s2)
    assert stop.status == "draining" and stop.spend_stopped is False and "scheduler job 100" in stop.notice
    assert stop.notice.startswith("NO CANCEL WAS ISSUED")


async def test_5_a_cold_stop_names_an_earlier_facility_endpoint_block_and_never_claims_it_stopped(monkeypatch):
    s1 = _mep_server(monkeypatch, earlier_dead=False)
    await server._connect_facility(s1, "globus1")
    assert (await _ensure_endpoint_up(s1, confirm_spend=True)).status == "up"
    s2 = _mep_server(monkeypatch, bound=False)
    st = await _stop_endpoint(s2)
    assert st.status == "draining" and st.spend_stopped is False
    assert "NO CANCEL WAS ISSUED" in st.notice and "scheduler job 100" in st.notice and "scancel 100" in st.notice
    assert _no_facility_bound(s2)

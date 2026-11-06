"""Review round 1 of the restart block record (fix/restart-block-record, on d205841): each finding confirmed by a
failing probe first, then fixed. In conftest's isolated state (never the user's ~/.hpc-bridge).

1. BLOCKER — a stop with nothing bound cancelled a handoff consumer's pool without asking: it ran the block release on
   a scratch context directly, past the stop's handoff question and scope (RELEASE_KEEP), so `uep.<eid>` took the
   consumer's `uep.<eid>.<consumer>` too, and the handoff stayed live.
2. A session bound by a startup pin (HPC_BRIDGE_MACHINE) learns its endpoint id only inside its first bring-up: its
   first yes started a second block beside the earlier one, unasked and unnamed. And its records carried no entry, so a
   later cold stop could not reach them.
3. A record re-opened by a later confirmation of the same request (a facility endpoint's `cfg:` key) kept the ended
   block's job and age, and was closed as past its walltime before the new block answered.
4. A record never seen up was closed after a day whatever its walltime, with no scheduler read.
5. restart_cold_stop's world check read a failing squeue as "gone" (its own `2>/dev/null || true`).
6. Live (batch_restart_resume, fake default, d205841): the resume took the killed server's block over; Globus answered
   every REST pull with a 500, so the new server never learnt the dead server's three rows had ended — and on the SAME
   block they held its whole window: the backlog never ran, until the block idled out ten minutes later.
Nits: a record of another host is never taken for gone; the cold stop holds the session's lock; a failure inside one
endpoint's part is that part's `draining`; the cold lead says what happened; export_handoff names an earlier block; an
unreachable record is named as such, with the user's own cancel.
"""
from __future__ import annotations

import asyncio
import importlib
import importlib.machinery
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

from hpc_bridge import binding, earlier, server
from hpc_bridge.context import AppCtx
from hpc_bridge.ledger import Ledger
from hpc_bridge.models import EndpointStatus
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import _ensure_endpoint_up, _stop_endpoint
from tests.fakes import fake_entry
from tests.test_restart_block_record import (
    EID,
    _answer,
    _connect,
    _listing,
    _open,
    _server,
    _session_1,
    _Slurm,
)
from tests.test_server import _scripted_login

HERE = Path(__file__).resolve().parent


def _old_ledger_module():
    """The ledger of the base this branch started from (061fd9c), to open what the new one migrated."""
    loader = importlib.machinery.SourceFileLoader("hpc_bridge._old_ledger", str(HERE / "_old_ledger_src.py.txt"))
    spec = importlib.util.spec_from_loader("hpc_bridge._old_ledger", loader)
    assert spec is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["hpc_bridge._old_ledger"] = mod
    loader.exec_module(mod)
    return mod


def _rec(led: Ledger, eid: str, request: str, *, job: str | None = None, up_at: float | None = None,
         now: float | None = None, **over) -> None:
    kw = dict(facility="byo", entry=None, mep=False, scheduler="slurm", uec_hash="h", partition=None, account=None,
              walltime_s=None, session="gone", owner_host=earlier._me().host, owner_pid=1, owner_start=None)
    kw.update(over)
    led.record_block(eid, request, now=now, **kw)
    if job is not None:
        led.block_up(eid, request, job, at=up_at)


# --- the migration (guards: an old ledger gains the table; the old code opens the migrated one) ----------------------


def test_an_old_ledger_with_rows_gains_the_block_table_and_the_old_code_reopens_it(tmp_path):
    old = _old_ledger_module()
    d = tmp_path / "st"
    led = old.Ledger.open(d)
    tables = [r[0] for r in led._con.execute("select name from sqlite_master where type='table'")]
    assert "block" not in tables
    led._con.execute("insert or replace into meta values ('probe', 'kept')")
    before = {t: led._con.execute(f"select count(*) from {t}").fetchone()[0] for t in tables}
    led.close()
    new = Ledger.open(d)
    new.record_block("e", "1", facility="f", entry=None, mep=False, scheduler="slurm", uec_hash="h", partition=None,
                     account=None, walltime_s=60, session="s", owner_host="h", owner_pid=1, owner_start=None)
    assert new._con.execute("select value from meta where key='probe'").fetchone()[0] == "kept"
    for t, n in before.items():
        assert new._con.execute(f"select count(*) from {t}").fetchone()[0] == n
    new.close()
    again = Ledger.open(d)
    assert len(again.open_blocks()) == 1
    again.close()
    back = old.Ledger.open(d)
    assert back._con.execute("select value from meta where key='probe'").fetchone()[0] == "kept"
    back.close()


def test_a_migration_under_concurrent_opens(tmp_path):
    old = _old_ledger_module()
    d = tmp_path / "st2"
    old.Ledger.open(d).close()
    errs: list[BaseException] = []

    def go():
        try:
            for _ in range(5):
                Ledger.open(d).close()
        except Exception as exc:  # noqa: BLE001
            errs.append(exc)

    ts = [threading.Thread(target=go) for _ in range(6)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert errs == []
    assert "block" in [r[0] for r in sqlite3.connect(d / "ledger.sqlite3").execute(
        "select name from sqlite_master where type='table'")]


# --- 1. BLOCKER: a cold stop asks about a handoff, exactly as a bound stop does ---------------------------------------


async def _cold_with_handoff(tmp_path, monkeypatch):
    """Session 1 confirmed a block and exported a handoff of its endpoint (the consumer's pool runs), then ended; a
    new server with nothing bound."""
    from tests.test_handoff_phase2b import SSH_EID, _ssh_with_handoff

    app, sched, mine, loam, out = await _ssh_with_handoff(tmp_path)
    entry = fake_entry(id="fakehpc", facility_key="fakehpc")
    led = Ledger.open()
    _rec(led, SSH_EID, "5", job=mine, facility="fakehpc", entry=entry.model_dump_json(), partition="main",
         walltime_s=3600.0, up_at=time.time())
    cold = AppCtx(facility=binding._default_local_facility(unbound=True), profile=Profile())
    cold.ledger = Ledger.open()
    cold.ledger.pid_alive = lambda pid: False
    cold.runner_factory = app.runners
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: app.facility)
    return cold, sched, mine, loam, out


async def test_1_a_cold_stop_asks_about_a_live_handoff_and_cancels_nothing(tmp_path, monkeypatch):
    cold, sched, mine, loam, out = await _cold_with_handoff(tmp_path, monkeypatch)
    st = await _stop_endpoint(cold)
    assert sched.job(loam)["state"] == "RUNNING", "the cold stop cancelled the handoff consumer's pool unasked"
    assert sched.job(mine)["state"] == "RUNNING" and sched.calls("scancel") == []    # a stop that asks changes nothing
    assert st.status == "needs_confirmation" and [h.handoff_id for h in st.handoffs] == [out.record.handoff_id]
    assert "cancels the consumer's blocks" in st.notice


async def test_1_a_cold_stop_with_keep_spares_the_consumers_pool(tmp_path, monkeypatch):
    cold, sched, mine, loam, out = await _cold_with_handoff(tmp_path, monkeypatch)
    st = await _stop_endpoint(cold, handoffs="keep")
    assert sched.job(mine)["state"] == "CANCELLED" and sched.job(loam)["state"] == "RUNNING", sched.jobs()
    assert st.status == "draining" and st.spend_stopped is not True and out.record.handoff_id in st.notice
    from hpc_bridge import scheduler_ops

    assert scheduler_ops.RELEASE_KEEP.get() is None


async def test_1_a_cold_stop_with_cancel_ends_the_handoff_and_its_pool(tmp_path, monkeypatch):
    cold, sched, mine, loam, _out = await _cold_with_handoff(tmp_path, monkeypatch)
    st = await _stop_endpoint(cold, handoffs="cancel")
    assert sched.job(mine)["state"] == sched.job(loam)["state"] == "CANCELLED", sched.jobs()
    assert "Ended, as asked" in st.notice and st.handoffs, (st.status, st.notice)


# --- 2. a startup pin: its endpoint first, its record before any block --------------------------------------------------


async def test_2_a_pinned_sessions_first_yes_reuses_the_earlier_block_it_never_connected_to(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac)           # bound (a startup pin), never connected: no endpoint id yet
    assert s2.state.endpoint_id is None
    log = _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2, confirm_spend=True)
    assert [k for k, _c in log] == ["probe"]                                   # the record read against the scheduler
    assert s2.shapes["compute"].user_endpoint_config["hpcb_block"] == n        # its block request: no second block
    assert st.status == "up" and st.block_job_id == "40" and "Reused block job 40" in st.notice


async def test_2_a_pinned_sessions_first_yes_on_other_terms_asks_first(monkeypatch):
    fac = _Slurm()
    _s1, n = await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac)
    _scripted_login(monkeypatch, probes=[_listing(n=n)])
    st = await _ensure_endpoint_up(s2, partition="other", confirm_spend=True)
    assert st.status == "needs_confirmation" and s2.compute.canaries == 0
    assert "block job 40" in st.notice and "second block beside it" in st.notice
    # and its spend question without a yes names it as well
    s3 = _server(monkeypatch, fac)
    _scripted_login(monkeypatch, probes=[_listing(n=n)])
    ask = await _ensure_endpoint_up(s3)
    assert ask.status == "needs_confirmation" and "block job 40" in ask.notice


async def test_2_a_pinned_sessions_record_carries_its_entry_so_a_later_cold_stop_reaches_it(monkeypatch):
    fac = _Slurm()
    entry = fake_entry(id="pinned", facility_key="pinned")
    monkeypatch.setenv("HPC_BRIDGE_MACHINE", "pinned")
    monkeypatch.setitem(binding._PINNED_ENTRY, "pinned", entry)
    s1 = _server(monkeypatch, fac, earlier_dead=False)
    s1.state.endpoint_id = EID
    assert (await _ensure_endpoint_up(s1, confirm_spend=True)).status == "up"
    (row,) = _open(EID)
    assert row["facility"] == "pinned" and json.loads(row["entry"])["id"] == "pinned"
    monkeypatch.delenv("HPC_BRIDGE_MACHINE")
    s2 = _server(monkeypatch, fac, bound=False)
    _scripted_login(monkeypatch, releases=["released 40\n"])
    stop = await _stop_endpoint(s2)
    assert stop.status == "down" and stop.spend_stopped is True and "released 40" in stop.notice


# --- 3. a re-opened record is the new confirmation's, not the ended block's ---------------------------------------------


def test_3_a_record_reopened_after_it_ended_carries_no_job_or_age_of_the_ended_block():
    led = Ledger.open()
    kw = dict(facility="globus1", entry=None, mep=True, scheduler="slurm", uec_hash="h", partition="main",
              account=None, walltime_s=7200.0, session="s1", owner_host="h", owner_pid=1, owner_start=None)
    t0 = time.time() - 3 * 3600
    led.record_block("mep", "cfg:h", now=t0, **kw)
    led.block_up("mep", "cfg:h", "40", at=t0 + 5)
    led.end_blocks("mep", ["cfg:h"], how="walltime")
    led.record_block("mep", "cfg:h", **{**kw, "session": "s2"})   # a later session's yes on the same config
    r = led.open_blocks("mep")[0]
    assert r["job"] is None and r["up_at"] is None, "a re-opened record carries the ended block's job and age"
    app = AppCtx(facility=binding._default_local_facility(unbound=True), profile=Profile())
    app.ledger = led
    rows, _ = earlier._records(app, "mep")
    assert [x["request"] for x in rows] == ["cfg:h"]                 # not closed as past its (old) walltime


def test_3_an_open_record_taken_over_keeps_its_job_and_age():
    led = Ledger.open()
    t0 = time.time() - 600
    _rec(led, "e", "7", job="40", up_at=t0, now=t0 - 10, walltime_s=3600.0)
    _rec(led, "e", "7", session="s2", walltime_s=3600.0)              # adopted while it runs: the same block
    r = led.open_blocks("e")[0]
    assert r["job"] == "40" and abs(r["up_at"] - t0) < 1 and r["session"] == "s2"


# --- 4. a never-up record is closed by a scheduler read, or after its queue allowance and walltime ----------------------


def test_4_a_never_up_ssh_record_is_kept_past_a_day_while_its_walltime_may_still_run():
    led = Ledger.open()
    _rec(led, "e2", "5", partition="long", walltime_s=48 * 3600.0, now=time.time() - 25 * 3600)
    app = AppCtx(facility=binding._default_local_facility(unbound=True), profile=Profile())
    app.ledger = led
    rows, _ = earlier._records(app, "e2")
    assert rows, "closed with no scheduler read while a 48 h block asked for 25 h ago may still run"


def test_4_a_never_up_record_is_closed_once_its_queue_allowance_and_walltime_have_passed():
    led = Ledger.open()
    far = time.time() - (earlier.QUEUE_ALLOWANCE_S + 2 * 3600 + 600)
    _rec(led, "m", "cfg:x", mep=True, walltime_s=2 * 3600.0, now=far)
    _rec(led, "m", "cfg:y", mep=True, walltime_s=None, now=time.time() - (earlier.QUEUE_ALLOWANCE_S
                                                                         + earlier.UNKNOWN_WALLTIME_S + 600))
    app = AppCtx(facility=binding._default_local_facility(unbound=True), profile=Profile())
    app.ledger = led
    rows, _ = earlier._records(app, "m")
    assert rows == [] and led.open_blocks("m") == []


async def test_4_on_ssh_a_scheduler_read_closes_a_never_up_record_that_has_no_pilot(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac, canary=CanaryResult(ok=False, error="timeout"))
    led = Ledger.open()
    led._con.execute("update block set confirmed_at = confirmed_at - ?", (earlier.YOUNG_S + 60,))
    s2 = _server(monkeypatch, fac)
    res, seen = await _connect(s2, f"HPCB_NOW {int(time.time())}\n")
    assert len(seen) == 1 and _open(EID) == [] and "Nothing is started or billed yet" in res.notice


# --- 5. the world check: a scheduler that cannot be read is no "gone" ---------------------------------------------------


def _scenario():
    sys.path.insert(0, str(HERE.parent / "agentic" / "harness"))
    sys.path.insert(0, str(HERE.parent / "agentic" / "scenarios"))
    return importlib.import_module("restart_cold_stop")


def _squeue(tmp_path, script: str) -> dict:
    b = tmp_path / "bin"
    b.mkdir(parents=True, exist_ok=True)
    (b / "squeue").write_text("#!/bin/sh\n" + script)
    (b / "squeue").chmod(0o755)
    return {**os.environ, "PATH": f"{b}:{os.environ['PATH']}"}


def _run_check(rcs, env) -> tuple[int, str]:
    p = subprocess.run(["bash", "-c", rcs.job_state_cmd("40")], capture_output=True, text=True, env=env, timeout=30)
    return p.returncode, p.stdout + p.stderr


def test_5_a_controller_that_cannot_be_reached_is_unverifiable_not_gone(tmp_path):
    rcs = _scenario()
    env = _squeue(tmp_path, "echo 'slurm_load_jobs error: Unable to contact slurm controller' >&2\nexit 1\n")
    rc, out = _run_check(rcs, env)
    assert rcs.gone_verdict("40", out)[0] == "unreadable", (rc, out)


def test_5_an_invalid_job_id_or_an_empty_listing_is_gone_and_a_listed_job_is_not(tmp_path):
    rcs = _scenario()
    gone = _squeue(tmp_path / "a", "echo 'slurm_load_jobs error: Invalid job id specified' >&2\nexit 1\n")
    assert rcs.gone_verdict("40", _run_check(rcs, gone)[1])[0] == "gone"
    empty = _squeue(tmp_path / "b", "exit 0\n")
    assert rcs.gone_verdict("40", _run_check(rcs, empty)[1])[0] == "gone"
    running = _squeue(tmp_path / "c", "echo R\n")
    assert rcs.gone_verdict("40", _run_check(rcs, running)[1])[0] == "listed"
    ending = _squeue(tmp_path / "d", "echo CG\n")
    assert rcs.gone_verdict("40", _run_check(rcs, ending)[1])[0] == "ending"


# --- 6. a dead server's rows past their walltime hold no slot on the block the resume takes over -----------------------


async def test_6_the_resume_on_the_dead_servers_own_block_admits_once_its_rows_cannot_still_run():
    from hpc_bridge import batch
    from hpc_bridge.ledger import Owner
    from tests.test_batch_phase1 import _batch_app, _ok_report, _owner, _start
    from tests.test_server import _FakeRunner, _Res

    app, _rt, runner = await _batch_app()                              # capacity 2
    out = await _start(app, n=6)
    for f in list(runner.futures):                                     # rows 0, 1 done; then 2, 3 admitted
        f.finish(_ok_report(runner.futures.index(f)))
    await batch.tick(app, out.batch_id)
    assert len(runner.futures) == 4
    app.ledger.close()
    # the server is killed with rows 2 and 3 out; long ago enough that the wrapper has ended them (task_walltime_s 60)
    led = Ledger.open()
    led._con.execute("update task set admitted_at = admitted_at - 3600 where batch_id = ? and state = 'in_flight'",
                     (out.batch_id,))
    app2, rt2, runner2 = await _batch_app()
    rt2.spend_confirmed = False
    app2.batch_owner = _owner(pid=os.getpid() + 100_000, host=Owner.here().host)
    app2.ledger = Ledger.open()
    app2.ledger.pid_alive = lambda pid: pid != os.getpid()            # the old server's pid is gone
    app2.ledger.lease_stale_s = 0.0
    rt2.runner = runner2
    client = runner2.task_client

    def broken():                                                      # every REST pull: Globus answers a 500
        c = client()

        def fail(_ids):
            raise RuntimeError("500 UNKNOWN_ERROR: An unknown or unhandled error occurred: StorageException")

        c._compute_web_client.v2.get_task_batch = fail
        return c

    runner2.task_client = broken
    login = _FakeRunner("fake-eid", _Res(0, "RUNNING 101 - None\n", ""))   # the old block runs on: the yes takes it
    app2.runner_factory = lambda eid, user_endpoint_config=None, **_kw: (
        runner2 if (user_endpoint_config or {}).get("compute") else login)
    await server._status(app2, out.batch_id)
    res = await server._run_batch(app2, batch_id=out.batch_id, confirm_spend=True)
    assert res.phase in ("admitting", "waiting"), (res.phase, res.notice)
    assert len(runner2.futures) == 2, (
        "the dead server's rows 2 and 3 ended long ago, yet they held the reused block's window", res.notice)


def test_6_a_row_sent_before_the_sender_was_recorded_still_follows_its_batchs_owner():
    # a ledger written before rows carried their sender: the old rule, the batch's lease holder
    from tests.test_batch_phase1 import _owner

    led = Ledger.open()
    led.pid_alive = lambda pid: True
    led.add_batch("b1", command="x", rows=[{"n": 0}], task_walltime_s=60, capacity=1, results_dir="/r",
                  idempotent=False, endpoint_id="e", uec_hash="h", session_id="s", owner=_owner())
    got = led.reserve("b1", _owner(), 1, uec_hash="h")
    assert len(got) == 1
    led._con.execute("update task set sent_pid = null, sent_host = null, sent_start = null, "
                     "admitted_at = admitted_at - 3600")
    holds, _out = led._on_block(led._con, "e", "h", None, time.time(), 75.0, _owner(), {})
    assert holds == {"b1": 1}


# --- nits ----------------------------------------------------------------------------------------------------------------


async def test_nit_a_record_of_another_host_is_never_taken_for_gone(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    Ledger.open()._con.execute("update block set owner_host = 'another-host.example'")
    s2 = _server(monkeypatch, fac, bound=False)
    log = _scripted_login(monkeypatch)
    st = await _stop_endpoint(s2)
    assert log == [] and st.status == "draining" and st.spend_stopped is not True
    assert "another-host.example" in st.notice and "job 40" in st.notice


async def test_nit_the_cold_stop_holds_the_sessions_lock():
    app = AppCtx(facility=binding._default_local_facility(unbound=True), profile=Profile())
    app.ledger = Ledger.open()
    app.ledger.pid_alive = lambda pid: False
    _rec(app.ledger, EID, "9", job="40", entry=fake_entry(id="mine", facility_key="mine").model_dump_json(),
         up_at=time.time())
    held = []

    async def stopper(s):
        held.append((app.lock.locked(), s.state.endpoint_id))
        return EndpointStatus(status="down", block_state="cold", spend_stopped=True, notice="released 40")

    orig = binding._facility_from_entry
    binding._facility_from_entry = lambda e, *, account: _Slurm()
    try:
        st = await earlier.stop_cold(app, stopper)
    finally:
        binding._facility_from_entry = orig
    assert held == [(True, EID)] and st is not None and st.status == "down"


async def test_nit_a_failure_inside_one_part_is_its_draining_with_the_reason(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    s2 = _server(monkeypatch, fac, bound=False)

    def boom():
        raise OSError("the token store cannot be read")

    s2.login_flow = SimpleNamespace(login_required=boom)
    st = await _stop_endpoint(s2)
    assert st.status == "draining" and st.spend_stopped is not True
    assert "the token store cannot be read" in st.notice and "job 40" in st.notice


async def test_nit_the_cold_lead_says_the_stop_could_not_reach_an_offline_endpoint(monkeypatch):
    fac = _Slurm()
    await _session_1(monkeypatch, fac)
    monkeypatch.setenv("HPC_BRIDGE_RELEASE_BACKOFF_S", "0")
    fac.manager_up = False
    s2 = _server(monkeypatch, fac, bound=False)
    _scripted_login(monkeypatch, releases=[None, None, None])
    st = await _stop_endpoint(s2)
    assert st.status == "draining" and "reached that endpoint" not in st.notice
    assert "tried to reach that endpoint" in st.notice


async def test_nit_an_unreachable_record_is_named_as_such_with_the_users_own_cancel(monkeypatch):
    led = Ledger.open()
    _rec(led, "33333333-0000-0000-0000-000000000003", "5", job="77", up_at=time.time())
    s = _server(monkeypatch, _Slurm(), bound=False)
    st = await _stop_endpoint(s)
    assert st.status == "draining" and st.spend_stopped is not True
    assert "recorded, not reachable from here" in st.notice and "scancel 77" in st.notice
    assert _open() != []                          # kept: bounded by its walltime allowance, never dropped unseen


async def test_nit_export_handoff_names_the_earlier_block_still_billing(tmp_path, monkeypatch):
    from tests.test_handoff_phase2b import SSH_EID, _export, _ssh_app

    app = _ssh_app(tmp_path)
    app.ledger = Ledger.open()
    app.ledger.pid_alive = lambda pid: False
    _rec(app.ledger, SSH_EID, "5", job="4242", up_at=time.time(), partition="main", walltime_s=3600.0)
    listing = f"HPCB_NOW {int(time.time())}\nRUNNING 4242 - None\n"
    _scripted_login(monkeypatch, probes=[listing])
    ask = await _export(app, {"partition": "main", "walltime": "00:10:00", "max_blocks": 1})
    assert ask.phase == "needs_confirmation", (ask.phase, ask.notice)
    assert "block job 4242" in (ask.notice or "") and "beside" in (ask.notice or "")


_ = (asyncio, _answer)

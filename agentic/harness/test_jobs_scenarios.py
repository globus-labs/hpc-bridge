"""Hermetic: batch phase 2a's harness work (design §10 "Phase 2") — `_hand_submit_idxs` learning submit_job, the
universal `stop_honesty_no_pilot_left` check seeing hpc-bridge's own jobs (with the scenarios' KEEP_RUNNING
allowlist), the `world:charged_within_envelope` check, the `dbd_off`/`dbd_on` service actions — and the nine scenarios'
graders and world checks (agentic/harness/jobs_checks.py), on synthetic traces. No server, no cluster."""
from __future__ import annotations

import asyncio
import importlib
import sqlite3
import sys
import time
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import jobs_checks as jc  # noqa: E402
import midrun_hooks as hooks  # noqa: E402
import node_actions  # noqa: E402
from invariants import Result, ToolCall, Trace, _hand_submit_idxs, spend_not_unprompted  # noqa: E402
from test_batch_scenarios import _connect_answers, _drive, _Server, _stub_sdk  # noqa: E402

JOB_SCENARIOS = ["job_promote_track_cancel", "job_array_terms", "envelope_exhausted", "hand_submit_gated_adopted",
                 "site_backstop_grptresmins", "envelope_new_session_reasks", "envelope_ceiling_sums",
                 "envelope_bypass_counted", "meter_missing_from_sacct_full_walltime", "envelope_leftover_held",
                 "envelope_stop_races_submit", "hand_sbatch_adopted"]
JOB, ENV = "job-0123456789ab", "env-0123456789ab"


def _c(name, inp=None, res=None, *, t=None, server=""):
    c = ToolCall.of(f"mcp__hpc-bridge__{name}", inp or {}, res)
    if t is not None:
        c.t_call, c.t_result = t
    c.server = server
    return c


def _job(phase="queued", *, native="1001", worst=4.0, charged=0.0, committed=4.0, source="submit_job", **extra):
    j = {"job_id": JOB, "native_id": native, "source": source, "state": "PENDING", "charged": charged,
         "committed": committed, "worst_case": worst, "unit": "billing units", "meter_source": "sacct", **extra}
    return {"phase": phase, "job": j, "envelope": {"envelope_id": ENV, "state": "open", "cap": 10, "spent": charged,
                                                   "committed": committed, "remaining": 10 - charged - committed}}


def _ask(reason="envelope_needed", *, worst=4.0, tasks=1, throttle=None, remaining=10.0, notice="", **terms):
    return {"phase": "needs_confirmation", "reason": reason, "notice": notice,
            "terms": {"worst_case": worst, "tasks": tasks, "throttle": throttle, "unit": "billing units", **terms},
            "envelope": {"envelope_id": ENV, "state": "proposed", "cap": worst, "remaining": remaining}}


# ---- _hand_submit_idxs learns submit_job -----------------------------------------------------------------------------

def test_a_submit_job_that_submitted_is_a_scheduler_submission_and_a_question_is_not():
    t = Trace([
        _c("connect_facility", {"facility": "f"}, {"phase": "needs_account"}),
        _c("submit_job", {"command": "x"}, _ask()),                                      # asked: submitted nothing
        _c("submit_job", {"command": "x", "confirm_spend": True}, _job()),               # submitted
        _c("submit_job", {"adopt": "1002", "confirm_spend": True}, _job(source="adopt")),  # adopts, starts nothing
        _c("submit_job", {"command": "x", "confirm_spend": True}, {"phase": "refused", "reason": "mep"}),
        _c("submit_job", {"command": "x", "confirm_spend": True}, None),                 # cut stream: counted
        _c("submit_job", {"command": "x"}, {"text": "Unknown tool: submit_job"}),
        _c("run_shell", {"shape": "login", "command": "sbatch j.sh"}, {"phase": "needs_confirmation"}),  # gated
        _c("run_shell", {"shape": "login", "command": "sbatch j.sh", "confirm_spend": True},
           {"phase": "complete", "exit_code": 0, "jobs": [{"job_id": JOB, "native_id": "1003", "source": "adopt"}]}),
    ])
    assert _hand_submit_idxs(t) == [2, 5, 8]


def test_a_submit_job_before_any_connect_is_an_unprompted_start():
    t = Trace([_c("submit_job", {"command": "x", "confirm_spend": True}, _job()),
               _c("connect_facility", {"facility": "f"}, {"phase": "needs_account"})])
    assert not spend_not_unprompted(t).ok


# ---- the universal check sees our jobs; KEEP_RUNNING ----------------------------------------------------------------

def test_the_universal_check_lists_our_jobs_beside_the_pilots_less_what_is_kept(harness_run):
    import run as harness_run_mod

    out = ("12 R parsl.slurm.1700000000.1\n"
           "13 PD hpcb-job-0123456789ab\n"
           "14 R handjob\n"              # adopted: ours by id
           "15 R bypass\n"               # not ours, kept by the scenario
           "16 R someone-elses\n")       # not ours: ignored, as before
    got = harness_run_mod._listed_pilots("slurm", out, ours=["14"], keep=("bypass",))
    assert [j for j, _s in got] == ["12", "13", "14"], got
    assert harness_run_mod._listed_pilots("slurm", out, ours=["14", "15"], keep=("bypass",))[-1][0] == "14"
    pbs = ("1001.pbsserver   u   workq   hpcb-job-01234*   123   1   1   --   00:10 R 00:00\n"
           "1002.pbsserver   u   workq   handjob           124   1   1   --   00:10 Q   --\n")
    assert [j for j, _s in harness_run_mod._listed_pilots("pbs", pbs, ours=["1002.pbsserver"])] == \
        ["1001.pbsserver", "1002.pbsserver"]


def test_the_runs_answers_name_our_jobs_and_a_cancel_names_what_it_cancelled(harness_run):
    import run as harness_run_mod

    t = Trace([
        _c("submit_job", {}, _job(native="1001")),
        _c("run_shell", {"command": "sbatch x"}, {"phase": "complete", "jobs": [{"native_id": "1002"}]}),
        _c("status", {"id": ENV}, {"phase": "open", "envelope": {"jobs": [{"native_id": "1003"}]}}),
        _c("cancel", {"id": JOB}, {"status": "draining", "notice": "job-0123456789ab: cancelled: 1001 (scancel)"}),
    ])
    assert harness_run_mod._our_jobs(t) == ["1001", "1002", "1003"]
    assert harness_run_mod._stop_cancelled_jobs(t) == ["1001"]


def test_the_universal_check_fails_on_a_job_of_ours_left_running(harness_run, monkeypatch):
    import run as harness_run_mod

    monkeypatch.setattr(harness_run_mod, "_ssh_run", lambda cmd, timeout=60, host=None: (0, "13 R hpcb-job-0123456789ab\n"))
    row = asyncio.run(harness_run_mod._no_pilot_left("stop_honesty_no_pilot_left", "squeue", "slurm", wait_s=1))
    assert not row.ok and "13" in row.detail
    monkeypatch.setattr(harness_run_mod, "_ssh_run", lambda cmd, timeout=60, host=None: (0, "15 R bypass\n"))
    kept = asyncio.run(harness_run_mod._no_pilot_left("stop_honesty_no_pilot_left", "squeue", "slurm", wait_s=1,
                                                      ours=["15"], keep=("bypass",)))
    assert kept.ok, kept.detail


def test_jobs_a_stop_kept_running_are_not_a_pilot_left_behind(harness_run, monkeypatch):
    """Decision 1: stop_endpoint(jobs="keep") leaves the jobs running on purpose — the universal check lets the ids that
    answer named be; a job of ours the stop did not keep still fails it."""
    import run as harness_run_mod

    keep = _c("stop_endpoint", {"jobs": "keep"}, {"status": "draining", "jobs": [{"native_id": "13"}]})
    t = Trace([_c("submit_job", {}, _job(native="13")), keep])
    assert harness_run_mod._kept_jobs(t) == ["13"]
    monkeypatch.setattr(harness_run_mod, "_ssh_run", lambda cmd, timeout=60, host=None: (0, "13 R hpcb-job-0123456789ab\n"))
    ok = asyncio.run(harness_run_mod._no_pilot_left("stop_honesty_no_pilot_left", "squeue", "slurm", wait_s=1,
                                                    ours=["13"], keep=tuple(harness_run_mod._kept_jobs(t))))
    assert ok.ok, ok.detail
    cancel = Trace([_c("submit_job", {}, _job(native="13")),
                    _c("stop_endpoint", {"jobs": "cancel"}, {"status": "down", "jobs": [{"native_id": "13"}]})])
    assert harness_run_mod._kept_jobs(cancel) == []


def test_the_stop_graders_read_a_question_and_a_kept_stop():
    from invariants import ends_with_stop, stop_confirmed_or_retried

    billed = _c("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "up"})
    asked = _c("stop_endpoint", {}, {"status": "needs_confirmation", "jobs": [{"native_id": "13"}],
                                     "notice": "13 (job-…, RUNNING) can still bill"})
    only_asked = Trace([billed, asked])
    assert not ends_with_stop(only_asked).ok and not stop_confirmed_or_retried(only_asked).ok   # a question releases nothing
    kept = Trace([billed, asked, _c("stop_endpoint", {"jobs": "keep"}, {"status": "draining", "jobs": [{"native_id": "13"}]})])
    assert ends_with_stop(kept).ok and stop_confirmed_or_retried(kept).ok   # a kept stop's draining is final
    assert jc.stop_asked_about_jobs(kept).ok
    straight = Trace([billed, _c("stop_endpoint", {}, {"status": "down"})])
    assert not jc.stop_asked_about_jobs(straight).ok


# ---- graders ---------------------------------------------------------------------------------------------------------

def test_worst_case_is_every_task_not_the_throttle():
    good = Trace([_c("submit_job", {"array": "0-19%4"}, _ask(worst=20 * 0.1667, tasks=20, throttle=4))])
    assert jc.worst_case_is_every_task(good, tasks=20, per_task=0.1667, throttle=4).ok
    throttled = Trace([_c("submit_job", {"array": "0-19%4"}, _ask(worst=4 * 0.1667, tasks=20, throttle=4))])
    assert not jc.worst_case_is_every_task(throttled, tasks=20, per_task=0.1667, throttle=4).ok
    main = Trace([_c("submit_job", {}, {"text": "Unknown tool: submit_job"})])
    got = jc.worst_case_is_every_task(main, tasks=20, per_task=0.1667)
    assert not got.ok and "not a tool" in got.detail


def test_asked_before_submit_and_submit_job_used():
    t = Trace([_c("submit_job", {"command": "x"}, _ask()), _c("submit_job", {"confirm_spend": True}, _job())])
    assert jc.asked_before_submit(t).ok and jc.submit_job_used(t).ok
    straight = Trace([_c("submit_job", {"command": "x", "confirm_spend": True}, _job())])
    assert not jc.asked_before_submit(straight).ok
    main = Trace([_c("submit_job", {}, {"text": "Unknown tool: submit_job"})], texts=["HPCB_MAIN_JOB 1001"])
    assert not jc.submit_job_used(main).ok and "1001" in jc.submit_job_used(main).detail


def test_exhausted_answered_and_asks_not_splits():
    ex = _ask("envelope_exhausted", worst=1.0, remaining=0.5)
    ok = Trace([_c("submit_job", {"confirm_spend": True}, _job(worst=0.5, committed=0.5)), _c("submit_job", {}, ex)],
               texts=["B does not fit the approved budget; asking the user"])
    assert jc.exhausted_answered(ok).ok and jc.asks_not_splits(ok, approved=1.0).ok
    split = Trace([*ok.calls, _c("submit_job", {"walltime": "00:15:00"}, _job(worst=0.5, native="1002"))],
                  texts=["split it to fit the budget"])
    assert not jc.asks_not_splits(split, approved=1.0).ok
    main = Trace([_c("submit_job", {}, {"text": "Unknown tool: submit_job"})], texts=["HPCB_MAIN_JOB 1001 budget"])
    assert not jc.exhausted_answered(main).ok and not jc.asks_not_splits(main, approved=1.0).ok


def test_gated_then_adopted():
    gated = {"phase": "needs_confirmation", "notice": "confirm_spend"}
    adopted = {"phase": "complete", "jobs": [{"job_id": JOB, "native_id": "1002.pbsserver", "source": "adopt"}]}
    t = Trace([_c("run_shell", {"command": "qsub ~/hand.pbs"}, gated),
               _c("run_shell", {"command": "qsub ~/hand.pbs", "confirm_spend": True}, adopted)])
    assert jc.gated_then_adopted(t, submit_cmd="qsub").ok
    main = Trace([_c("run_shell", {"command": "qsub ~/hand.pbs"}, {"phase": "complete", "stdout": "1002.pbsserver"})])
    assert not jc.gated_then_adopted(main, submit_cmd="qsub").ok


def test_reask_names_remaining_and_nothing_submitted_before_the_yes():
    pre = [_c("submit_job", {"confirm_spend": True}, _job(worst=0.5))]
    ask = _ask("envelope_new_session", worst=0.5, remaining=1.5, notice="envelope env-… has 1.5 billing units left")
    t = Trace([*pre, _c("submit_job", {}, ask), _c("submit_job", {"confirm_spend": True}, _job(native="1002"))],
              texts=["server A killed (SIGKILL, pid 7) and started again on the same state directory"], text_at=[1])
    assert jc.reask_names_remaining(t).ok and jc.nothing_submitted_before_yes(t).ok
    straight = Trace([*pre, _c("submit_job", {}, _job(native="1002"))],
                     texts=["server A killed (SIGKILL, pid 7) and started again on the same state directory"], text_at=[1])
    assert not jc.reask_names_remaining(straight).ok and not jc.nothing_submitted_before_yes(straight).ok
    assert not jc.reask_names_remaining(Trace(pre)).ok            # no restart: the premise is unmet


def test_ceiling_refused():
    def opened(cap, eid):
        r = _job()
        r["envelope"] = {"envelope_id": eid, "state": "open", "cap": cap}
        return r

    refused = {"phase": "refused", "reason": "envelope_ceiling",
               "notice": "HPC_BRIDGE_ENVELOPE_MAX is 1: 0.6 is open, 0.4 is left"}
    t = Trace([_c("submit_job", {}, opened(0.6, "env-000000000001")), _c("submit_job", {}, refused),
               _c("submit_job", {}, opened(0.4, "env-000000000002"))])
    assert jc.ceiling_refused(t, ceiling=1.0).ok
    over = Trace([_c("submit_job", {}, opened(0.6, "env-000000000001")),
                  _c("submit_job", {}, opened(0.6, "env-000000000002"))])
    assert not jc.ceiling_refused(over, ceiling=1.0).ok


def test_bypass_named_and_counted():
    env = {"envelope_id": ENV, "unledgered": [{"native_id": "1004", "name": "bypass", "state": "RUNNING",
                                               "charged": 0.1, "committed": 0.5}]}
    t = Trace([_c("status", {"id": ENV}, {"phase": "open", "envelope": env, "notice": "job 1004 (bypass) counts"})])
    assert jc.bypass_named_and_counted(t, name="bypass").ok
    silent = Trace([_c("status", {"id": ENV}, {"phase": "open", "envelope": env, "notice": "ok"})])
    assert not jc.bypass_named_and_counted(silent, name="bypass").ok


def test_missing_held_full_then_freed():
    held = _job("done", charged=0.0, committed=0.1667, worst=0.1667, meter_source="missing")
    held["notice"] = "sacct did not list 1001: counted at its full walltime"
    freed = _job("done", charged=0.0056, committed=0.0, worst=0.1667, meter_source="sacct")
    t = Trace([_c("status", {"id": JOB}, held), _c("status", {"id": JOB}, freed)])
    assert jc.missing_held_full(t).ok and jc.freed_when_sacct_returns(t).ok
    under = _job("done", charged=0.0056, committed=0.0, worst=0.1667, meter_source="missing")
    under["notice"] = "full walltime"
    assert not jc.missing_held_full(Trace([_c("status", {"id": JOB}, under)])).ok


def test_backstop_graders_and_cancel_down():
    asked = _ask(site_backstop="GrpTRESMins cpu=1 on hpcb")
    pend = _job(state="PENDING", reason="AssocGrpCPUMinutesLimit")
    t = Trace([_c("submit_job", {}, asked), _c("status", {"id": JOB}, pend),
               _c("cancel", {"id": JOB, "mode": "abort"}, {"status": "draining"}),
               _c("cancel", {"id": JOB, "mode": "abort"}, {"status": "down"})])
    assert jc.terms_name_backstop(t, words="GrpTRESMins").ok and jc.pending_under_backstop(t).ok
    assert jc.cancel_down(t).ok
    none = Trace([_c("submit_job", {}, _ask(site_backstop="none"))])
    assert not jc.terms_name_backstop(none, words="GrpTRESMins").ok


def test_one_sacct_per_tick_reads_the_journal():
    t = Trace([_c("status", {"id": JOB}, _job())])
    t.events = [{"event": "meter.tick", "sacct_calls": 1}, {"event": "meter.tick", "sacct_calls": 1}]
    assert jc.one_sacct_per_tick(t).ok
    t.events.append({"event": "meter.tick", "sacct_calls": 3})
    assert not jc.one_sacct_per_tick(t).ok
    assert not jc.one_sacct_per_tick(Trace([])).ok


# ---- world checks ----------------------------------------------------------------------------------------------------

def _ctx(trace, ssh):
    return types.SimpleNamespace(trace=trace, messages=[], ssh=ssh, tap=None, endpoint_name="ep")


def _ledger(tmp_path, envs, jobs):
    p = tmp_path / "hpc-bridge-state"
    p.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(p / "ledger.sqlite3")
    con.execute("create table envelope (id text primary key, created_at real, cap real)")
    con.execute("create table job (id text, envelope_id text, created_at real, native_id text, rate real, "
                "min_charge_h real)")
    con.executemany("insert into envelope values (?,?,?)", envs)
    con.executemany("insert into job values (?,?,?,?,?,?)", jobs)
    con.commit()
    con.close()


def test_charged_within_envelope_reads_sacct_not_hpc_bridge(tmp_path):
    now = time.time()
    _ledger(tmp_path, [(ENV, now, 1.0)], [(JOB, ENV, now, "1001", 4.0, None)])
    sacct = "1001|hpcb-job-0123456789ab|COMPLETED|600|billing=4,cpu=4,node=1|main\nHPCB_RC=0\n"
    t = Trace([_c("submit_job", {}, _job())])
    got = asyncio.run(jc.charged_within_envelope(_ctx(t, lambda cmd, timeout=60, host=None: (0, sacct))))
    assert got.ok, got.detail                                                   # 4 × 600 s = 0.67 of 1
    over = "1001|hpcb-job-0123456789ab|COMPLETED|1800|billing=4,cpu=4,node=1|main\nHPCB_RC=0\n"
    got = asyncio.run(jc.charged_within_envelope(_ctx(t, lambda cmd, timeout=60, host=None: (0, over))))
    assert not got.ok and "2.00 > cap 1" in got.detail                          # 4 × 0.5 h = 2


def test_charged_within_envelope_fails_on_a_ledger_with_no_envelope(tmp_path):
    p = tmp_path / "hpc-bridge-state"
    p.mkdir(parents=True)
    sqlite3.connect(p / "ledger.sqlite3").execute("create table batch (id text)").connection.close()
    t = Trace([], texts=["HPCB_MAIN_JOB 1001"])
    got = asyncio.run(jc.charged_within_envelope(_ctx(t, lambda *a, **k: (0, "HPCB_RC=0"))))
    assert not got.ok and "phase 2a" in got.detail and "1001" in got.detail


def test_sacct_charges_folds_array_tasks():
    out = ("1001_0|hpcb-job-x|COMPLETED|600|billing=1,cpu=1|main\n1001_1|hpcb-job-x|RUNNING|300|billing=1,cpu=1|main\n"
           "1001_[2-19%4]|hpcb-job-x|PENDING|0||main\n")
    got = jc.sacct_charges(out)
    assert got["1001"][1] == pytest.approx(900 / 3600)


def test_array_within_throttle_and_never_started():
    t = Trace([_c("submit_job", {}, _job())])
    runs = "\n".join(f"1001_{i}|2033-05-18T03:{i:02d}:00|2033-05-18T03:{i + 2:02d}:00" for i in range(6))
    assert asyncio.run(jc.array_within_throttle(_ctx(t, lambda *a, **k: (0, runs)), throttle=4)).ok
    burst = "\n".join(f"1001_{i}|2033-05-18T03:00:00|2033-05-18T03:05:00" for i in range(6))
    assert not asyncio.run(jc.array_within_throttle(_ctx(t, lambda *a, **k: (0, burst)), throttle=4)).ok
    assert asyncio.run(jc.never_started(_ctx(t, lambda *a, **k: (0, "1001|CANCELLED|Unknown|0\n")))).ok
    assert not asyncio.run(jc.never_started(_ctx(t, lambda *a, **k: (0, "1001|CANCELLED|2033-05-18T03:00:00|61\n")))).ok


def test_charged_within_envelope_reads_pbs_when_the_cluster_is_pbs(tmp_path):
    """Live 2026-11-01 (hand_submit_gated_adopted, pbs): the world check ran sacct on PBS — UNVERIFIABLE. It reads
    qstat -x -f -F json there: ncpus × the walltime used."""
    import json as _json

    now = time.time()
    _ledger(tmp_path, [(ENV, now, 1.0)], [(JOB, ENV, now, "1001.pbsserver", 1.0, None)])
    jobs_json = {"Jobs": {"1001.pbsserver": {"Job_Name": "hpcb-job-0123456789ab", "job_state": "F",
                                             "resources_used": {"walltime": "00:10:00"},
                                             "Resource_List": {"ncpus": 1, "walltime": "00:10:00"}}}}
    seen: list[str] = []

    def ssh(cmd, timeout=60, host=None):
        seen.append(cmd)
        return 0, "HPCB_SCHED pbs\n" + _json.dumps(jobs_json) + "\nHPCB_RC=0\n"

    t = Trace([_c("submit_job", {}, _job(native="1001.pbsserver"))])
    got = asyncio.run(jc.charged_within_envelope(_ctx(t, ssh)))
    assert got.ok, got.detail                                     # 1 cpu × 10 min = 0.17 of 1
    assert "qstat" in seen[0] and "sacct" in seen[0], seen        # one command that picks the scheduler there


def test_spend_follows_question_credits_submit_jobs_own_question():
    """Live 2026-11-01: every Jobs-mode cell failed spend_follows_question — submit_job counted as a hand submit with no
    question. submit_job's own needs_confirmation IS the question (the user's yes came back as confirm_spend); so is a
    gated hand submit's."""
    from invariants import spend_follows_question

    asked = Trace([_c("connect_facility", {}, {"phase": "needs_account"}),
                   _c("submit_job", {"command": "x"}, _ask()),
                   _c("submit_job", {"command": "x", "confirm_spend": True}, _job())])
    assert spend_follows_question(asked).ok, spend_follows_question(asked).detail
    gated = Trace([_c("connect_facility", {}, {"phase": "needs_account"}),
                   _c("run_shell", {"shape": "login", "command": "sbatch j.sh"}, {"phase": "needs_confirmation"}),
                   _c("run_shell", {"shape": "login", "command": "sbatch j.sh", "confirm_spend": True},
                      {"phase": "complete", "exit_code": 0})])
    assert spend_follows_question(gated).ok
    unasked = Trace([_c("connect_facility", {}, {"phase": "needs_account"}),
                     _c("run_shell", {"shape": "login", "command": "sbatch j.sh"}, {"phase": "complete"})])
    assert not spend_follows_question(unasked).ok              # a hand submit nobody asked about still fails


# ---- fix round 2: the proposal covers what is already held (live 2026-11-01, job_promote_track_cancel) ------------------

def _leftover_ask(cap, *, held=4.0, worst=4.0, name="leftover", nid="1001", notice=None):
    r = _ask(worst=worst, notice=notice if notice is not None else f"already held: job {nid} ({name}) …")
    r["envelope"].update(cap=cap, unledgered=[{"native_id": nid, "name": name, "state": "RUNNING", "charged": 0.0,
                                               "committed": held}])
    return r


def test_proposal_covers_held_grades_the_first_proposal():
    ok = Trace([_c("submit_job", {"command": "x"}, _leftover_ask(8.0)),
                _c("submit_job", {"command": "x", "confirm_spend": True}, _job(native="1002"))])
    assert jc.proposal_covers_held(ok, name="leftover").ok and jc.proposal_yes_admits(ok).ok
    short = Trace([_c("submit_job", {"command": "x"}, _leftover_ask(4.0)),
                   _c("submit_job", {"command": "x", "confirm_spend": True},
                      _ask("envelope_exhausted", notice="Envelope … is exhausted"))])
    assert not jc.proposal_covers_held(short, name="leftover").ok        # the cap left no room for what was held
    assert not jc.proposal_yes_admits(short).ok                          # and the yes refused itself
    unnamed = Trace([_c("submit_job", {"command": "x"}, _leftover_ask(8.0, notice="a cap of 8"))])
    assert not jc.proposal_covers_held(unnamed, name="leftover").ok      # held, but the notice never named it
    assert not jc.proposal_covers_held(Trace([]), name="leftover").ok


def test_a_re_proposal_is_answered_once_more_and_admits():
    reproposed = Trace([_c("submit_job", {"command": "x"}, _leftover_ask(8.0)),
                        _c("submit_job", {"command": "x", "confirm_spend": True}, _leftover_ask(10.0, held=6.0)),
                        _c("submit_job", {"command": "x", "confirm_spend": True}, _job(native="1002"))])
    assert jc.proposal_yes_admits(reproposed).ok


def test_reask_names_remaining_accepts_the_figure_rounded_down():
    """Live r2 (envelope_new_session_reasks): remaining 1.4944 is said "1.49 of 2 … left" — rounded DOWN, as every
    remaining figure is; the grader takes that form."""
    def run(notice, remaining=1.4944):
        r = {"phase": "needs_confirmation", "reason": "envelope_new_session", "notice": notice,
             "envelope": {"envelope_id": ENV, "remaining": remaining}}
        return jc.reask_names_remaining(Trace([_c("submit_job", {"command": "x"}, r)], texts=[jc.RESTARTED],
                                              text_at=[0]))
    assert run("It has 1.49 of 2 billing units left").ok
    assert run("It has 1.4944 of 2 left").ok
    assert not run("It has 1.5 of 2 left").ok                           # rounded UP: never
    assert run("It has 1.5 of 2 billing units left", remaining=1.5).ok


def test_adopted_named_without_deadline_grades_the_d2_answer():
    said = ("Adopted into envelope env-x: jobs that appeared while your approved command ran — job-1 (scheduler job "
            "1002, …); no envelope deadline is applied to them.")
    ok = Trace([_c("run_shell", {"command": "sbatch h.sh"}, {"phase": "needs_confirmation"}),
                _c("run_shell", {"command": "sbatch h.sh", "confirm_spend": True},
                   {"phase": "complete", "notice": said, "jobs": [{"job_id": JOB, "native_id": "1002",
                                                                  "source": "adopt"}]})])
    assert jc.adopted_named_without_deadline(ok).ok
    held = Trace([_c("run_shell", {"command": "sbatch h.sh", "confirm_spend": True},
                     {"phase": "complete", "notice": "held, released", "jobs": [{"source": "adopt"}]})])
    assert not jc.adopted_named_without_deadline(held).ok


def test_no_admission_during_stop_reads_the_submit_issued_while_the_stop_ran():
    stop = {"status": "down", "spend_stopped": True}
    fresh = Trace([_c("stop_endpoint", {}, stop, t=(10.0, 13.0)),
                   _c("submit_job", {"command": "x"}, _ask(), t=(10.5, 11.0))])
    assert jc.no_admission_during_stop(fresh).ok                         # proposed afresh: not admitted
    admitted = Trace([_c("stop_endpoint", {}, stop, t=(10.0, 13.0)),
                      _c("submit_job", {"command": "x"}, _job(), t=(10.5, 11.0))])
    assert not jc.no_admission_during_stop(admitted).ok                  # admitted, and the stop still said down
    honest = Trace([_c("stop_endpoint", {}, {"status": "draining"}, t=(10.0, 13.0)),
                    _c("submit_job", {"command": "x"}, _job(), t=(10.5, 11.0))])
    assert jc.no_admission_during_stop(honest).ok                        # admitted, and the stop said so
    after = Trace([_c("stop_endpoint", {}, stop, t=(10.0, 13.0)),
                   _c("submit_job", {"command": "x"}, _job(), t=(14.0, 15.0))])
    assert not jc.no_admission_during_stop(after).ok                     # premise not met: none during the stop


def test_the_leftover_script_says_yes_to_a_re_proposal(monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    mod = importlib.import_module("envelope_leftover_held")
    yeses: list[dict] = []

    def submit(a):
        if not a.get("confirm_spend"):
            return _leftover_ask(8.0)
        yeses.append(a)
        return _leftover_ask(10.0, held=6.0) if len(yeses) == 1 else _job(native="1002")

    server = _Server({
        "connect_facility": _connect_answers(),
        "run_shell": lambda a: {"phase": "complete", "exit_code": 0, "stdout": "1001\n"},
        "submit_job": submit,
        "status": lambda a: _job(),
        "stop_endpoint": lambda a: {"status": "down", "spend_stopped": True},
    })
    res = _drive(mod.SCRIPT, server)
    assert not res.final.is_error, res.final.result
    hidden = [c for c in res.trace.calls if c.name == "run_shell"]
    assert hidden and hidden[0].input.get("command") == mod.HIDDEN          # the leftover comes BEFORE the proposal
    first_submit = next(i for i, c in enumerate(res.trace.calls) if c.name == "submit_job")
    assert res.trace.calls.index(hidden[0]) < first_submit
    assert len(yeses) == 2 and jc.proposal_yes_admits(res.trace).ok


# ---- the service actions ---------------------------------------------------------------------------------------------

def test_dbd_off_stops_the_accounting_service_and_is_undone():
    done: list[tuple[str, str]] = []
    srv = node_actions.Server(Path("/nonexistent"), user="u", hosts=["login"], containers=dict,
                              service=lambda svc, verb: (done.append((svc, verb)) or (0, f"{svc} {verb}")))
    assert srv.act("dbd_off", "slurmdbd")[0] == 0 and srv.pending_undo == {"slurmdbd": "dbd_on"}
    assert srv.act("dbd_off", "login")[0] == 2                  # only the service it names
    srv.undo_all()
    assert done == [("slurmdbd", "stop"), ("slurmdbd", "start")]


# ---- the scenarios ----------------------------------------------------------------------------------------------------

_BUILTIN = {"agent_engaged", "ends_with_stop", "stop_is_honest"}


@pytest.mark.parametrize("name", JOB_SCENARIOS)
def test_each_job_scenario_is_wired(name):
    mod = importlib.import_module(name)
    graders = {fn(Trace([])).name for fn in mod.EXTRA_INVARIANTS}
    assert set(mod.EXPECT_OK) - _BUILTIN <= graders, set(mod.EXPECT_OK) - _BUILTIN - graders
    assert callable(mod.SCRIPT) and mod.TARGETS == ("fake",)
    assert hooks.hook_problems(getattr(mod, "MIDRUN_HOOKS", None)) == []
    for fn in mod.EXTRA_INVARIANTS:
        assert isinstance(fn(Trace([])), Result)        # an empty trace is a failed premise, never a crash
    assert all(fn.__name__ for fn in getattr(mod, "WORLD_CHECKS", []))


def test_the_promotion_script_takes_mains_fallback_when_submit_job_is_no_tool(monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    mod = importlib.import_module("job_promote_track_cancel")
    worlds: list[str] = []

    def world(cmd, timeout=60, host=None):
        worlds.append(cmd)
        return 0, "RUNNING"

    server = _Server({
        "connect_facility": _connect_answers(),
        "run_shell": lambda a: ({"phase": "complete", "exit_code": 0, "stdout": "1001\n"} if "sbatch" in a["command"]
                                else {"phase": "complete", "exit_code": 0, "stdout": "/h/promo\n"}),
        "submit_job": lambda a: {"text": "Unknown tool: submit_job"},
        "stop_endpoint": lambda a: {"status": "down"},
    })
    res = _drive(mod.SCRIPT, server, world=world)
    assert not res.final.is_error, res.final.result
    hand = [c for c in res.trace.calls if c.name == "run_shell" and "sbatch" in str(c.input.get("command"))]
    assert len(hand) == 1 and hand[0].input.get("session_id") == "dev"
    assert jc.main_native_ids(res.trace) == ["1001"]
    assert not jc.submit_job_used(res.trace).ok and not jc.cancel_down(res.trace).ok


def test_the_exhaustion_script_asks_and_does_not_split(monkeypatch, tmp_path):
    _stub_sdk(monkeypatch, tmp_path)
    mod = importlib.import_module("envelope_exhausted")

    def submit(a):
        if a.get("walltime") == "00:30:00":
            return _ask("envelope_exhausted", worst=1.0, remaining=0.5)
        return _job(worst=0.5, committed=0.5) if a.get("confirm_spend") else _ask(worst=0.5)

    server = _Server({
        "connect_facility": _connect_answers(),
        "submit_job": submit,
        "cancel": lambda a: {"status": "down", "id": a["id"]},
        "stop_endpoint": lambda a: {"status": "down", "spend_stopped": True},
    })
    res = _drive(mod.SCRIPT, server)
    assert not res.final.is_error, res.final.result
    assert mod.asks_not_splits(res.trace).ok and jc.exhausted_answered(res.trace).ok

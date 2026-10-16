"""Hermetic: queued_check_relaunch — its grader on synthetic traces, the admin tap's parsing of the scheduler's output
(in the formats the fake cluster prints), its script against a stand-in server, and run.py's ordering (the world check
before the postchecks and the teardown). No agent, no cluster."""
from __future__ import annotations

import asyncio
import json
import os
import shlex
import subprocess
import sys
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import admin_tap  # noqa: E402
import queued_check_relaunch as qcr  # noqa: E402
import targets  # noqa: E402
from invariants import ToolCall, Trace  # noqa: E402

EID = "54736c9f-06eb-4183-96e3-65a23b75a770"
OTHER = "007c300d-4746-4b48-bc8d-ed39d2fbcf28"
# A pilot's batch script, as squeue's %o / sacct's SubmitLine name it (the path shape of the stored fake bundles, e.g.
# 1792105266-23823-81296-stop_while_running's endpoint-logs: …/uep.<eid>.<uep id>/submit_scripts/parsl.…block-0.<ts>)
SCRIPT_OF = ("/home/hpcbridge-test-00/.globus_compute/uep.{eid}.34aaadd8-3a58-d9ea-c9b0-49257afec4d2/submit_scripts/"
             "parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.{ts}.submit")
T_STOP = 1792105400.0   # the stop's answer (epoch, the jail's and the controller's clock)


@pytest.fixture(autouse=True)
def _phase_first(monkeypatch):
    """Most of these tests are the first-pilot sequence (phase `first`); a phase-`second` test sets it itself."""
    monkeypatch.setenv("HPCB_RELAUNCH_PHASE", "first")


def _call(name: str, inp: dict | None = None, res: dict | None = None) -> ToolCall:
    return ToolCall.of(f"mcp__hpc-bridge__{name}", inp or {}, res)


def _poll(*pilots: str) -> ToolCall:
    return _call("run_shell", {"shape": "login", "command": qcr.pilot_poll_cmd(EID)},
                 {"phase": "complete", "exit_code": 0, "stdout": "".join(f"HPCB_PILOT {p}\n" for p in pilots)})


def _trace(stop: dict | None, *, pilot: str = "7 PENDING BeginTime", ensure: dict | None = None) -> Trace:
    calls = [
        _call("connect_facility", {"facility": "fake-1", "ssh_host": "login"}, {"phase": "proposed_facility_details"}),
        _call("connect_facility", {"facility": "fake-1", "details": {"ssh_host": "login"}}, {"phase": "provisioning"}),
        _call("connect_facility", {"facility": "fake-1", "ssh_host": "login"}, {"phase": "needs_account"}),
        _call("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
              ensure or {"status": "provisioning", "block_state": "provisioning", "endpoint_id": EID}),
        _poll(),
        _poll(pilot),
    ]
    if stop is not None:
        calls.append(_call("stop_endpoint", {}, stop))
    return Trace(calls)


DOWN = {"status": "down", "spend_stopped": True, "block_state": "cold", "endpoint_id": EID,
        "notice": "compute block released over AMQP (released 7); the login endpoint stays online for reuse"}
DRAINING = {"status": "draining", "spend_stopped": None, "endpoint_id": EID,
            "notice": "Spend is NOT confirmed stopped: a check of ours is still queued on the endpoint"}


def _job(job_id: str, submit: float | None, state: str = "PENDING", reason: str = "BeginTime", eid: str = EID,
         seen_at: float | None = None) -> admin_tap.SlurmJob:
    return admin_tap.SlurmJob(job_id=job_id, state=state, submit=submit, reason=reason, source="squeue",
                              command=SCRIPT_OF.format(eid=eid, ts=int(submit or 0)), seen_at=seen_at)


OWN = None   # sentinel: the cell's own pilot (job 7, cancelled by the stop) as the tap saw it


def _seen(jobs, own: set[str]) -> dict[str, str]:
    return {j.job_id: f"{j.state} [{j.source}]" for j in jobs if j.job_id in own}


def _watch(*jobs: admin_tap.SlurmJob, covered: float = 250.0, unverifiable: str | None = None,
           own: set[str] | None = None, log_lines: list | None = None) -> qcr.Watch:
    """A watch whose clean samples saw the cell's own pilot (job 7) unless told otherwise, and whose endpoint log was
    read (no strategy line) — the PASS path needs both."""
    if not any(j.job_id == "7" for j in jobs) and own is None:
        jobs = (_job("7", T_STOP - 9, state="CANCELLED", reason=""), *jobs)
    own_ids = {"7"} if own is None else own
    return qcr.Watch(called_at=T_STOP - 0.6, answered_at=T_STOP, window_s=240.0, covered_until=T_STOP + covered,
                     samples=26, jobs=list(jobs), unverifiable=unverifiable, own_ids=own_ids,
                     own_seen=_seen(jobs, own_ids), log_lines=[] if log_lines is None else log_lines)


# ---- the grader -------------------------------------------------------------------------------------------------------

def test_a_down_followed_by_a_relaunched_pilot_fails_naming_the_job_its_time_and_state():
    w = _watch(_job("7", T_STOP - 9, state="CANCELLED", reason=""), _job("8", T_STOP + 62))
    r = qcr.no_relaunch_after_down(_trace(DOWN), w)
    assert r.name == "world:no_relaunch_after_down" and not r.ok
    assert "answered status='down', spend_stopped=True" in r.detail
    assert "job 8 submitted +62 s after the stop answered, PENDING (BeginTime)" in r.detail
    assert "job 7 submitted" not in r.detail               # the one the stop released is not a relaunch
    assert "check was queued" in r.detail                   # with the premise: the canary was still queued


def test_a_down_with_no_relaunch_passes_on_a_covered_watch_with_the_check_queued():
    w = _watch(_job("7", T_STOP - 9, state="CANCELLED", reason=""))
    r = qcr.no_relaunch_after_down(_trace(DOWN), w)
    assert r.ok and r.detail.startswith("ok:") and "250 s" in r.detail and "job 7 PENDING (BeginTime)" in r.detail
    assert "the tap saw its own pilot (job 7 CANCELLED [squeue])" in r.detail


def test_a_draining_stop_is_another_premise_report_only_and_passes_saying_so():
    for w in (_watch(), _watch(_job("8", T_STOP + 61))):
        r = qcr.no_relaunch_after_down(_trace(DRAINING), w)
        assert r.ok and r.detail.startswith("report-only:") and "did not claim spend stopped" in r.detail
    assert "DID submit pilot(s) after it: job 8 submitted +61 s" in r.detail   # still said, never gated
    refused = {"status": "up", "spend_stopped": False, "notice": "can't stop yet: a check of ours is queued"}
    assert qcr.no_relaunch_after_down(_trace(refused), _watch(_job("8", T_STOP + 61))).ok


def test_no_pass_without_proof_no_stop_an_unverifiable_or_short_watch_or_an_unqueued_check():
    no_stop = qcr.no_relaunch_after_down(_trace(None), _watch())
    assert not no_stop.ok and "premise not met: no judged stop_endpoint" in no_stop.detail
    blind = qcr.no_relaunch_after_down(_trace(DOWN), _watch(unverifiable="the admin tap has no sample after the stop"))
    assert not blind.ok and blind.detail.startswith("UNVERIFIABLE")
    short = qcr.no_relaunch_after_down(_trace(DOWN), _watch(covered=90.0))
    assert not short.ok and "covered only 90 s of the 240 s" in short.detail
    ran = qcr.no_relaunch_after_down(_trace(DOWN, ensure={"status": "up", "block_state": "warm", "endpoint_id": EID}),
                                     _watch())
    assert not ran.ok and ran.detail.startswith("premise not met") and "a worker answered the check" in ran.detail
    running = qcr.no_relaunch_after_down(_trace(DOWN, pilot="7 RUNNING None"), _watch())
    assert not running.ok and running.detail.startswith("premise unverified") and "job 7 RUNNING" in running.detail
    # a relaunch is positive evidence: it fails the cell even on a short watch, and even if the pilot was RUNNING
    assert "NEW pilot" in qcr.no_relaunch_after_down(_trace(DOWN, pilot="7 RUNNING None"),
                                                     _watch(_job("8", T_STOP + 5), covered=30.0)).detail


def test_only_this_endpoints_new_jobs_count_and_one_without_a_submit_time_counts_once_seen_after():
    t = _trace(DOWN)
    before = _job("6", T_STOP - 3600, state="CANCELLED")     # an earlier cell's pilot of this endpoint: before the stop
    seen = _job("7", T_STOP - 9)                             # the script saw it (and the stop released it)
    late = _job("9", None, seen_at=T_STOP + 70)              # no submit time: first seen after the answer
    early = _job("10", None, seen_at=T_STOP - 5)
    assert [j.job_id for j in qcr.relaunches(t, _watch(before, seen, late, early))] == ["9"]
    # endpoint_jobs keeps only this endpoint's (the marker), whatever else the pool user runs
    s = admin_tap.SlurmSample(t=1.0, now=T_STOP, queue=[_job("7", T_STOP - 9), _job("11", T_STOP + 1, eid=OTHER)])
    assert [j.job_id for j in qcr.endpoint_jobs([s], [EID])] == ["7"]


def test_the_report_rows_say_what_the_stop_answered_the_pilot_state_and_the_canary():
    t = _trace(DOWN)
    assert '"status": "down"' in qcr.stop_answer(t).detail and "released 7" in qcr.stop_answer(t).detail
    assert qcr.pilot_state_at_stop(t).detail == "call 5: job 7 PENDING (BeginTime)"
    assert qcr.canary_before_stop(t).detail.startswith("queued: job 7 PENDING (BeginTime)")
    running = _trace(DOWN, pilot="7 RUNNING None")
    assert qcr.canary_state(running)[0] == "unknown" and not qcr.canary_before_stop(running).ok
    report = qcr.relaunch_report(_watch(_job("8", T_STOP + 62)))
    assert report.ok and "covering 250 s after the stop answered" in report.detail and "job 8 PENDING" in report.detail


# ---- the admin tap: the scheduler's output as the fake cluster prints it ---------------------------------------------

def test_a_sample_parses_the_queue_and_the_history_in_epoch_or_iso_times():
    run = SCRIPT_OF.format(eid=EID, ts=1792105313)
    sample = {"t": 1792105401.2, "rc": 0, "out": "\n".join([
        "HPCB_NOW 1792105400",
        # squeue -o '%i|%T|%V|%r|%o' — a held relaunch, and the endpoint's running pilot with an ISO time (a Slurm that
        # ignores SLURM_TIME_FORMAT prints `2026-10-15T23:01:53`, in UTC under TZ=UTC)
        f"HPCB_Q 8|PENDING|1792105462|BeginTime|{run}",
        f"HPCB_Q 2|RUNNING|2026-10-15T23:01:53|None|{run}",
        "HPCB_Q HPCB_RC 0",                                    # squeue's own exit status (its sed's is the sample's rc)
        # sacct -X -n -P -o JobID,JobName,State,Submit,Start,End,SubmitLine — the stored fake bundles' row shape
        # (1788634219-63412-53791-slurm_worker_died: `2|parsl.GlobusComputeEngine-…|CANCELLED by 2000|…`)
        "HPCB_A 7|parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.1788629578.5684283|CANCELLED by 2000|"
        f"2026-09-05T17:32:59|Unknown|2026-09-05T17:33:16|sbatch {run}",
        "HPCB_A HPCB_RC 0",
    ])}
    s = admin_tap.parse_slurm_sample(sample)
    assert s.now == 1792105400.0 and s.error == "" and s.clean
    assert [(j.job_id, j.state, j.reason, j.submit) for j in s.queue] == [
        ("8", "PENDING", "BeginTime", 1792105462.0), ("2", "RUNNING", "None", 1792105313.0)]
    (a,) = s.history
    assert (a.job_id, a.state, a.start, a.source) == ("7", "CANCELLED", None, "sacct") and s.by_id == []
    assert a.submit == 1788629579.0 and a.end == 1788629596.0 and f"uep.{EID}" in a.command
    failed = admin_tap.parse_slurm_sample({"t": 1.0, "rc": 1, "out": "Error response from daemon: No such container"})
    assert failed.now is None and "rc=1" in failed.error and "No such container" in failed.error
    assert "no controller clock" in admin_tap.parse_slurm_sample({"rc": 0, "out": "HPCB_Q x"}).error


def test_the_latest_view_of_a_job_wins_and_the_queue_beats_the_history_within_a_sample():
    run = SCRIPT_OF.format(eid=EID, ts=1)
    s1 = admin_tap.parse_slurm_sample({"rc": 0, "out": f"HPCB_NOW 100\nHPCB_A 7|parsl.x|PENDING|90|Unknown|Unknown|"
                                                       f"sbatch {run}\nHPCB_Q 7|PENDING|90|BeginTime|{run}"})
    s2 = admin_tap.parse_slurm_sample({"rc": 0, "out": f"HPCB_NOW 110\nHPCB_A 7|parsl.x|CANCELLED by 0|90|Unknown|"
                                                       f"105|sbatch {run}"})
    (j,) = qcr.endpoint_jobs([s1, s2], [EID])
    assert (j.state, j.reason, j.submit, j.seen_at, j.end) == ("CANCELLED", "BeginTime", 90.0, 100.0, 105.0)
    assert j.source == "sacct+squeue"


def test_the_sample_command_asks_for_epoch_times_the_marker_and_the_clock(tmp_path):
    cmd = admin_tap.slurm_sample_cmd()
    assert "SLURM_TIME_FORMAT=%s" in cmd and "TZ=UTC" in cmd and "echo HPCB_NOW $(date +%s)" in cmd
    assert cmd.count('echo "HPCB_RC $?"; } 2>&1 | sed') == 2   # each query reports its own exit status
    # -t all: a job finished within MinJobAge is still listed; -s <every state>: a never-eligible (held) job is too
    # the window opens at the tap's own start (`{since}`, which the tap fills), not two hours of another cell's past
    assert "squeue -u {user} -h -t all -o '%i|%T|%V|%r|%o'" in cmd and "SubmitLine" in cmd and "-S {since} " in cmd
    # -E now with -s: without an end time sacct 23.11.4 listed nothing (verified on the fake controller); the six states
    # are the ones verified there
    assert f"-S {{since}} -E now -s {admin_tap.SACCT_STATES} " in cmd and admin_tap.SACCT_STATES == "CA,CD,F,PD,R,TO"
    assert qcr.ADMIN_TAP["cmd"] == cmd and qcr.ADMIN_TAP["every_s"] == 10
    assert qcr.ADMIN_TAP["ids_cmd"] == admin_tap.slurm_ids_cmd() and "sacct -j {ids} -X -n -P" in qcr.ADMIN_TAP["ids_cmd"]
    assert qcr.ADMIN_TAP["ids_cmd"].endswith("sed 's/^/HPCB_J /'")   # its own tag: never read as the window's rows
    # the login-shape poll matches the same marker the product's release does, and prints nothing on no match
    poll = qcr.pilot_poll_cmd(EID)
    assert f"m=uep.{EID}" in poll and "index($4,m)" in poll and poll.endswith("; true")


def test_the_tap_samples_the_admin_channel_into_the_file_and_the_jail_reads_it_back(tmp_path):
    out = tmp_path / ".admin-tap-r1.jsonl"
    seen: list[list[str]] = []

    def run(argv):
        seen.append(argv)
        return 0, f"HPCB_NOW {1000 + 10 * len(seen)}\n"

    clock = iter(range(0, 100, 10))   # the start, then one tick per sample: three fit in max_s=35
    n = admin_tap.tap(out, ["docker", "exec", "ctld", "bash", "-lc"], "squeue -u hpcbridge-test-03", every_s=10,
                      max_s=35, run=run, sleep=lambda s: None, clock=lambda: float(next(clock)))
    assert n == 3 and seen[0] == ["docker", "exec", "ctld", "bash", "-lc", "squeue -u hpcbridge-test-03"]
    with out.open("a") as fh:
        fh.write('{"t": 9, "rc": 0, "out": "HPCB_NO')   # a line cut by a kill mid-write is skipped
    samples = admin_tap.TapReader(out).samples()
    assert [s.now for s in samples] == [1010.0, 1020.0, 1030.0]
    # waiting: returns as soon as a sample reaches the wanted clock; without a tap, at once
    assert len(asyncio.run(admin_tap.TapReader(out).wait_until(1030.0, deadline_s=0))) == 3
    assert asyncio.run(admin_tap.TapReader(None).wait_until(1.0, deadline_s=99)) == []


def test_the_tap_cli_substitutes_the_pool_user_and_stops_on_sigterm(tmp_path):
    out = tmp_path / "tap.jsonl"
    stub = tmp_path / "admin.sh"
    stub.write_text('#!/usr/bin/env bash\necho "HPCB_NOW $(date +%s) $*"\n')
    stub.chmod(0o755)
    knob = json.dumps({"cmd": "squeue -u {user}", "every_s": 0.2, "ids_cmd": "sacct -j {ids}"})
    admin_tap.ids_path(out).write_text("4 5\n")   # the jail asked for jobs 4 and 5
    p = subprocess.Popen([sys.executable, str(HERE / "admin_tap.py"), "--out", str(out), "--user", "hpcbridge-test-05",
                          "--knob", knob, "--", str(stub)])
    try:
        for _ in range(600):   # up to ~30 s on a loaded host; a fraction of a second normally
            if out.exists() and len(admin_tap.read_samples(out)) >= 2:
                break
            subprocess.run(["sleep", "0.05"], check=False)
    finally:
        p.terminate()
        assert p.wait(timeout=10) == 0
    rows = admin_tap.read_samples(out)
    assert len(rows) >= 2 and rows[0]["rc"] == 0 and "squeue -u hpcbridge-test-05; sacct -j 4,5" in rows[0]["out"]


# ---- the world check: waits on the tap, then grades -----------------------------------------------------------------

RC_OK = "\nHPCB_Q HPCB_RC 0\nHPCB_A HPCB_RC 0"   # each query's own exit status, as slurm_sample_cmd prints it


def _tap_file(tmp_path, *outs: str, clean: bool = True) -> Path:
    """Tap samples; `clean`: each ends as a sample whose queries all reported exit 0."""
    p = tmp_path / ".admin-tap-r1.jsonl"
    p.write_text("".join(json.dumps({"t": 1.0, "rc": 0, "out": o + (RC_OK if clean else "")}) + "\n" for o in outs))
    return p


def _messages(answered: float = T_STOP) -> list[dict]:
    return [{"harness": "script", "rc": 0},
            {"journal": {"tool": "stop_endpoint", "args": {}, "ts": answered - 0.6, "ms": 600, "result": DOWN}}]


def test_the_world_check_reads_the_tap_and_fails_a_relaunch(tmp_path):
    run = SCRIPT_OF.format(eid=EID, ts=1)
    tap = _tap_file(tmp_path,
                    f"HPCB_NOW {T_STOP - 5:.0f}\nHPCB_Q 7|PENDING|{T_STOP - 9:.0f}|BeginTime|{run}",
                    f"HPCB_NOW {T_STOP + 70:.0f}\nHPCB_Q 8|PENDING|{T_STOP + 63:.0f}|BeginTime|{run}\n"
                    f"HPCB_A 7|parsl.x|CANCELLED by 0|{T_STOP - 9:.0f}|Unknown|{T_STOP:.0f}|sbatch {run}",
                    f"HPCB_NOW {T_STOP + 425:.0f}\nHPCB_Q 8|PENDING|{T_STOP + 63:.0f}|BeginTime|{run}")
    ctx = types.SimpleNamespace(trace=_trace(DOWN), messages=_messages(), tap=admin_tap.TapReader(tap))
    gated, report, strategy, _history = asyncio.run(qcr.relaunch_watch(ctx))
    assert not gated.ok and "job 8 submitted +63 s after the stop answered, PENDING (BeginTime)" in gated.detail
    assert report.name == "relaunch_watch" and "3 admin sample(s) (0 not clean), covering 425 s" in report.detail
    assert "job 7 CANCELLED" in report.detail and "the stop's own pilot(s): 7" in report.detail
    assert strategy.name == "strategy_after_stop" and strategy.detail.startswith("UNVERIFIABLE")   # no world channel
    assert (tap.parent / (tap.name + ".ids")).read_text().split() == ["7"]   # the tap was asked for job 7 by id


def test_the_world_check_passes_a_quiet_covered_queue_and_is_unverifiable_without_a_tap(tmp_path):
    run = SCRIPT_OF.format(eid=EID, ts=1)
    tap = _tap_file(tmp_path, f"HPCB_NOW {T_STOP - 5:.0f}\nHPCB_Q 7|PENDING|{T_STOP - 9:.0f}|BeginTime|{run}",
                    f"HPCB_NOW {T_STOP + 421:.0f}\n")
    ctx = types.SimpleNamespace(trace=_trace(DOWN), messages=_messages(), tap=admin_tap.TapReader(tap),
                                ssh=lambda cmd, timeout: (0, "=== log\n"), endpoint_name="hpc-bridge-fake-1")
    assert asyncio.run(qcr.relaunch_watch(ctx))[0].ok   # the default watch: 420 s; the log read, no strategy line
    unread = types.SimpleNamespace(**{**vars(ctx), "ssh": None})
    assert "the endpoint's logs were not read" in asyncio.run(qcr.relaunch_watch(unread))[0].detail
    blind = types.SimpleNamespace(trace=_trace(DOWN), messages=_messages(), tap=admin_tap.TapReader(None))
    gated = asyncio.run(qcr.relaunch_watch(blind))[0]
    assert not gated.ok and "HPCB_ADMIN_TAP is unset" in gated.detail
    stale = types.SimpleNamespace(trace=_trace(DOWN), messages=_messages(),
                                  tap=admin_tap.TapReader(_tap_file(tmp_path, f"HPCB_NOW {T_STOP - 5:.0f}\n")))
    stale.tap.wait_until = lambda now, **kw: asyncio.sleep(0, result=stale.tap.samples())   # no waiting in a test
    assert "no CLEAN sample after the stop answered" in asyncio.run(qcr.relaunch_watch(stale))[0].detail


# ---- a blind watch never passes (the first live run passed blind, 2026-10-15) ------------------------------------------

def test_a_watch_that_never_saw_the_cells_own_pilot_fails_blind():
    blind = qcr.no_relaunch_after_down(_trace(DOWN), _watch(own={"7"}))   # 37 samples, none of them job 7
    assert not blind.ok and blind.detail.startswith("watch blind: no clean sample's squeue or window sacct listed this "
                                                    "endpoint's own pilot (job 7)")
    # seen by id but without the marker: a relaunch would go unmatched too
    w = _watch(own={"7"})
    w.seen_unmarked = {"7"}
    assert "without the uep.<eid> marker" in qcr.no_relaunch_after_down(_trace(DOWN), w).detail
    # no own pilot id at all (the stop released none, no poll saw one): nothing proves the tap can see this endpoint
    stop = {**DOWN, "notice": "compute block released over AMQP (released none)"}
    nothing = qcr.no_relaunch_after_down(_trace(stop, pilot=""), _watch(own=set()))
    assert not nothing.ok and "no pilot id of this stop is known" in nothing.detail
    assert qcr.own_pilots(_trace(DOWN)) == {"7"}


def test_a_scale_out_in_the_endpoint_log_that_the_tap_missed_fails_blind():
    line = qcr.LogLine(T_STOP + 61, "uep 9f7c4349", "INFO MainProcess-443 JobStatusPoller-x parsl.executors."
                       "status_handling:207 scale_out_facade [Scaling executor X] Scaling out by 1 blocks")
    r = qcr.no_relaunch_after_down(_trace(DOWN), _watch(log_lines=[line]))
    assert not r.ok and r.detail.startswith("watch blind:")
    assert "+61 s [uep 9f7c4349] INFO [Scaling executor X] Scaling out by 1 blocks" in r.detail
    row = qcr.strategy_after_stop(_watch(log_lines=[line]))
    assert not row.ok and row.detail.startswith("SCALE-OUT after the stop (1 strategy line(s))")


# The first live run's endpoint log (bundle 1792117501-3824-56199-queued_check_relaunch, endpoint-logs.txt), cut to the
# lines that matter: the compute user endpoint scaled out (job 1) BEFORE the stop, then went to bad state after it.
RUN2_EID = "da39df5f-29d2-43fb-949d-d5c574b709f3"
RUN2_LOG = f"""=== /home/hpcbridge-test-00/.globus_compute/hpc-bridge-fake-1792117501-3824-56199/endpoint.log (12 lines; last 2000)
2026-10-16 02:26:48,059473 INFO MainProcess-345 MainThread-281473343582240 globus_compute_endpoint.endpoint.core_endpoint:1079 cmd_start_endpoint Creating new user endpoint (pid: 443) [(hpcbridge-test-00, uep.{RUN2_EID}.9f7c4349-a8a6-c03a-5267-6989ae992de0) globus-compute-endpoint _start-user-endpoint uep.{RUN2_EID}.9f7c4349-a8a6-c03a-5267-6989ae992de0]
2026-10-16 02:27:56,235656 INFO MainProcess-345 MainThread-281473343582240 globus_compute_endpoint.endpoint.core_endpoint:330 wait_for_children Command stopped normally (443) [(hpcbridge-test-00, uep.{RUN2_EID}.9f7c4349-a8a6-c03a-5267-6989ae992de0)]
=== /home/hpcbridge-test-00/.globus_compute/uep.{RUN2_EID}.9f7c4349-a8a6-c03a-5267-6989ae992de0/endpoint.log (43 lines; last 2000)
2026-10-16 02:26:54,868916 INFO MainProcess-443 JobStatusPoller-Timer-Thread-281473486913616-281473198059936 parsl.executors.status_handling:207 scale_out_facade [Scaling executor GlobusComputeEngine-HighThroughputExecutor] Scaling out by 1 blocks
2026-10-16 02:26:54,868967 INFO MainProcess-443 JobStatusPoller-Timer-Thread-281473486913616-281473198059936 parsl.executors.status_handling:210 scale_out_facade [Scaling executor GlobusComputeEngine-HighThroughputExecutor] Allocated block ID 0
2026-10-16 02:27:55,056648 ERROR MainProcess-443 JobStatusPoller-Timer-Thread-281473486913616-281473198059936 parsl.executors.status_handling:145 set_bad_state_and_fail_all Setting bad state due to exception
parsl.jobs.errors.TooManyJobFailuresError: Error 1:
\tJob is marked as MISSING since the workers failed to register to the executor. Check the stdout/stderr logs in the submit_scripts directory for more debug information

2026-10-16 02:27:55,059102 INFO MainProcess-443 JobStatusPoller-Timer-Thread-281473486913616-281473198059936 parsl.jobs.strategy:195 _general_strategy [Scaling executor GlobusComputeEngine-HighThroughputExecutor] Not strategizing for executor because bad state
2026-10-16 02:27:55,346706 WARNING MainProcess-443 MainThread-281473622724640 parsl.jobs.job_status_poller:66 close Not scaling in executor GlobusComputeEngine-HighThroughputExecutor because it is in bad state
2026-10-16 02:27:55,367169 INFO MainProcess-443 MainThread-281473622724640 globus_compute_endpoint.endpoint.endpoint:626 start_interchange 
---------- Endpoint shutdown: 9f7c4349-a8a6-c03a-5267-6989ae992de0
"""
# its stop_endpoint, from the run's journal: called 02:26:58.520 UTC, 990 ms
RUN2_STOP = 1792117618.52


def test_the_endpoint_log_after_the_stop_reads_as_the_first_run_went_bad_state_no_scale_out():
    lines = qcr.strategy_lines(RUN2_LOG, RUN2_STOP - 1.0)
    # 56–58 s after the stop: the job-status poll after the cancel (parsl reads Slurm every 60 s), in time order
    assert [(round(x.at - RUN2_STOP), x.where) for x in lines] == [
        (57, "uep 9f7c4349"), (57, "uep 9f7c4349"), (57, "uep 9f7c4349"), (57, "uep 9f7c4349"), (58, "manager")]
    assert "marked as MISSING" in lines[0].text and "TooManyJobFailuresError" in lines[0].text   # folded continuation
    assert "Not strategizing for executor because bad state" in lines[1].text
    assert "Endpoint shutdown: 9f7c4349" in lines[3].text and "Command stopped normally (443)" in lines[4].text
    # the scale-out of the cell's own pilot came BEFORE the stop, so it is not one after it
    before = qcr.strategy_lines(RUN2_LOG, 0.0)
    assert any("Scaling out by 1 blocks" in x.text for x in before) and len(before) == 8
    w = qcr.Watch(called_at=RUN2_STOP, answered_at=RUN2_STOP + 0.99, log_lines=lines)
    row = qcr.strategy_after_stop(w)
    assert row.ok and row.detail.startswith("no scale-out after the stop (5 strategy line(s)): +56 s [uep 9f7c4349] "
                                            "ERROR Setting bad state due to exception ⏎ parsl.jobs.errors."
                                            "TooManyJobFailuresError")
    assert qcr.no_relaunch_after_down(_trace(DOWN), _watch(log_lines=lines)).ok   # no contradiction: it never scaled out


def test_the_real_sacct_and_squeue_lines_of_a_held_cancelled_pilot_are_this_endpoints_own(tmp_path):
    # the first live run's job 1, as the controller lists it (sacct -j 1 -X -n -P: `CANCELLED by 2000`, SubmitLine =
    # parsl's `sbatch <script>` under uep.<eid>.<uep>/submit_scripts/) and as `squeue -t all` lists it within MinJobAge
    script = (f"/home/hpcbridge-test-00/.globus_compute/uep.{RUN2_EID}.9f7c4349-a8a6-c03a-5267-6989ae992de0/"
              "submit_scripts/parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.1792117614.8689604.submit")
    s = admin_tap.parse_slurm_sample({"t": 1.0, "rc": 0, "out": "\n".join([
        "HPCB_NOW 1792117624",
        f"HPCB_Q 1|CANCELLED|1792117614|None|{script}",
        "HPCB_A 1|parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.1792117614.8689604|CANCELLED by 2000|"
        f"2026-10-16T02:26:54|Unknown|2026-10-16T02:26:59|sbatch {script}",
    ])})
    (job,) = qcr.endpoint_jobs([s], [RUN2_EID])
    assert (job.job_id, job.state, job.submit, job.end, job.source) == ("1", "CANCELLED", 1792117614.0, 1792117619.0,
                                                                       "sacct+squeue")


def test_the_world_check_asks_the_tap_for_its_own_pilot_and_the_host_tap_looks_it_up_by_id(tmp_path):
    out = tmp_path / ".admin-tap-r1.jsonl"
    reader = admin_tap.TapReader(out)
    assert reader.request_ids(["1", "12_3", "1; rm -rf /", "x"]) and admin_tap.read_ids(admin_tap.ids_path(out)) == \
        ["1", "12_3"]
    assert not admin_tap.TapReader(None).request_ids(["1"]) and not reader.request_ids(["$(id)"])
    seen: list[str] = []

    def run(argv):
        seen.append(argv[-1])
        return 0, "HPCB_NOW 5\n"

    clock = iter(range(0, 100, 10))
    admin_tap.tap(out, ["docker", "exec", "c", "bash", "-lc"], "squeue", every_s=10, max_s=15, run=run,
                  ids_cmd="sacct -j {ids} -X", sleep=lambda s: None, clock=lambda: float(next(clock)))
    assert seen == ["squeue; sacct -j 1,12_3 -X"] and admin_tap.read_samples(out)[0]["ids"] == ["1", "12_3"]


def test_the_world_check_reads_the_endpoint_log_over_the_world_channel_before_the_teardown(tmp_path):
    run = SCRIPT_OF.format(eid=EID, ts=1)
    tap = _tap_file(tmp_path, f"HPCB_NOW {T_STOP + 5:.0f}\nHPCB_Q 7|CANCELLED|{T_STOP - 9:.0f}|None|{run}",
                    f"HPCB_NOW {T_STOP + 421:.0f}\n")
    asked: list[tuple[str, int]] = []
    log = RUN2_LOG.replace(RUN2_EID, EID)

    def ssh(cmd, timeout):
        asked.append((cmd, timeout))
        return 0, log

    ctx = types.SimpleNamespace(trace=_trace(DOWN), messages=_messages(answered=RUN2_STOP + 0.99),
                                tap=admin_tap.TapReader(tap), ssh=ssh, endpoint_name="hpc-bridge-fake-1")
    ctx.tap.wait_until = lambda now, **kw: asyncio.sleep(0, result=ctx.tap.samples())
    gated, _report, strategy, _history = asyncio.run(qcr.relaunch_watch(ctx))
    (cmd, timeout), = asked
    assert "hpc-bridge-fake-1/endpoint.log" in cmd and f'uep."{EID}.*/endpoint.log' in cmd and timeout == 120
    assert strategy.ok and "Not strategizing for executor because bad state" in strategy.detail
    assert gated.name == qcr.NO_RELAUNCH
    failed = types.SimpleNamespace(**{**vars(ctx), "ssh": lambda cmd, timeout: (255, "ssh: connect to host login")})
    failed.tap.wait_until = ctx.tap.wait_until
    assert "UNVERIFIABLE — the endpoint's logs were not read: rc=255" in asyncio.run(qcr.relaunch_watch(failed))[2].detail


# ---- the script, against a stand-in server ----------------------------------------------------------------------------

class _Ops:
    """script_runner.ScriptOps' surface, answering from `answers` (tool -> list, consumed in order)."""

    def __init__(self, answers: dict[str, list]):
        import script_runner
        self.answers = {k: list(v) for k, v in answers.items()}
        self.calls: list[tuple[str, dict]] = []
        self.texts: list[str] = []
        self.facility, self.ssh_host = "fake-1", "login"
        self._abort = script_runner.ScriptAbort

    async def call(self, tool, **args):
        self.calls.append((tool, args))
        return self.answers[tool].pop(0)

    def say(self, text):
        self.texts.append(text)

    def abort(self, reason):
        raise self._abort(reason)

    async def sleep(self, s):
        return None


PROPOSAL = {"phase": "proposed_facility_details",
            "proposed_details": {"ssh_host": "login", "interface": "eth0", "env_setup": "__hb_b=… uv venv",
                                 "scratch_root": "/home/{user}/.hpc-bridge", "partition": "main", "scheduler": "slurm",
                                 "allocation_command": None, "walltime": "00:30:00", "scheduler_options": None}}


def test_the_script_holds_the_pilot_waits_for_it_on_the_login_shape_and_stops_at_once(monkeypatch):
    monkeypatch.delenv("HPCB_RELAUNCH_HOLD_S", raising=False)
    ops = _Ops({
        "connect_facility": [PROPOSAL, {"phase": "provisioning"}, {"phase": "provisioning"},
                             {"phase": "needs_account", "allocations": [{"account": "hpcb-gpu"}, {"account": "hpcb"}]}],
        "ensure_endpoint_up": [{"status": "provisioning", "endpoint_id": EID,
                                "notice": "allocating nodes on 'compute'… — pilot 7 is PENDING (BeginTime)"}],
        "run_shell": [{"phase": "complete", "exit_code": 0, "stdout": ""},
                      {"phase": "complete", "exit_code": 0, "stdout": "HPCB_PILOT 7 PENDING BeginTime\n"}],
        "stop_endpoint": [DOWN],
    })
    final = asyncio.run(qcr.SCRIPT(ops))
    tools = [t for t, _ in ops.calls]
    assert tools == ["connect_facility"] * 4 + ["ensure_endpoint_up", "run_shell", "run_shell", "stop_endpoint"]
    details = ops.calls[1][1]["details"]
    assert "env_setup" not in details and "allocation_command" not in details      # the proposal's, as the tool asks
    assert details["scheduler_options"] == "#SBATCH --begin=now+1800" and details["interface"] == "eth0"
    assert ops.calls[2][1] == {"facility": "fake-1", "ssh_host": "login"}           # again, until the login shape is up
    assert ops.calls[4][1] == {"shape": "compute", "confirm_spend": True, "account": "hpcb"}   # not the GPU account
    assert all(a["shape"] == "login" and f"uep.{EID}" in a["command"] for t, a in ops.calls if t == "run_shell")
    assert "teardown_endpoint" not in tools and "stopped with job 7 PENDING (BeginTime) in the queue" in final


def test_the_script_aborts_when_a_premise_fails_and_drops_the_hold_on_request(monkeypatch):
    import pytest
    import script_runner
    ops = _Ops({"connect_facility": [{"phase": "needs_preauth", "notice": "one-time code"}]})
    with pytest.raises(script_runner.ScriptAbort, match="did not propose"):
        asyncio.run(qcr.SCRIPT(ops))
    monkeypatch.setattr(qcr, "PILOT_WAIT_S", 0.0)
    ops = _Ops({"connect_facility": [PROPOSAL, {"phase": "needs_account"}],
                "ensure_endpoint_up": [{"status": "provisioning", "endpoint_id": EID}],
                "run_shell": [{"phase": "complete", "stdout": ""}]})
    with pytest.raises(script_runner.ScriptAbort, match=f"no new pilot of uep.{EID} entered the queue"):
        asyncio.run(qcr.SCRIPT(ops))
    assert "stop_endpoint" not in [t for t, _ in ops.calls]       # no stop: the harness teardown cleans up
    monkeypatch.setenv("HPCB_RELAUNCH_HOLD_S", "0")
    assert "scheduler_options" not in qcr._proposal_details(PROPOSAL, qcr._hold_s())
    monkeypatch.setenv("HPCB_RELAUNCH_HOLD_S", "120")
    with_own = {"proposed_details": {**PROPOSAL["proposed_details"], "scheduler_options": "#SBATCH --constraint=x"}}
    assert qcr._proposal_details(with_own, qcr._hold_s())["scheduler_options"] == \
        "#SBATCH --constraint=x\n#SBATCH --begin=now+120"


# ---- declarations: target, REQUIRES, knobs ----------------------------------------------------------------------------

def test_it_runs_on_the_fake_slurm_profiles_default_and_site_only_and_declares_its_tap():
    assert qcr.TARGETS == ("fake",) and qcr.OPERATORS == ("script",) and qcr.TEARDOWN == "delete"
    for prof, ok in (("default", True), ("site", True), ("pbs", False), ("totp", False), ("pbs_mep", False)):
        assert targets.meets(qcr.REQUIRES, targets.load_profile(prof)["capabilities"])[0] is ok, prof
    import os

    def knobs(phase: str) -> dict:
        out = subprocess.run([sys.executable, str(HERE / "scenario_knobs.py"), "queued_check_relaunch"],
                             capture_output=True, text=True, timeout=60, check=True,
                             env={**os.environ, "HPCB_RELAUNCH_PHASE": phase}).stdout
        return dict(ln.split("=", 1) for ln in out.splitlines() if "=" in ln)

    k = knobs("second")   # the default: phase A runs a real block for a minute
    assert json.loads(shlex.split(k["HPCB_KNOB_ADMIN_TAP"])[0]) == qcr.ADMIN_TAP and k["HPCB_KNOB_TARGETS"] == "fake"
    assert k["HPCB_KNOB_NEEDS_NODE"] == "1" and set(qcr.ADMIN_TAP) == {"every_s", "cmd", "ids_cmd", "hold_cmd"}
    assert "HPCB_KNOB_NEEDS_NODE" not in knobs("first")   # every pilot held: no node is ever taken
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_suite", HERE.parent / "run_suite.py")
    suite = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suite)
    assert suite._needs_admin("queued_check_relaunch")   # a target without an admin channel skips it


# ---- run.py: the world check runs after the operator, before the postchecks and the teardown -------------------------

def test_world_checks_run_before_the_postchecks_and_the_teardown_and_their_rows_gate(harness_run, monkeypatch, tmp_path):
    run_mod = harness_run
    from invariants import Result
    (tmp_path / "scen_watch.py").write_text(
        "from invariants import Result\n"
        "ORDER = []\n"
        "async def watch(ctx):\n"
        "    ORDER.append('world check')\n"
        "    return [Result('world:watched', ctx.tap.path is not None, 'x'), Result('seen', False, 'report only')]\n"
        "async def SCRIPT(ops):\n"
        "    return 'done'\n"
        'PROMPT = "x"\nOPERATORS = ("script",)\nWORLD_CHECKS = [watch]\nEXPECT_OK = ["agent_engaged"]\n'
        'TEARDOWN = "delete"\nTRUST_HOST_KEY = False\nPOSTCHECK_DELAY_S = 0\n')
    monkeypatch.setattr(run_mod, "SCENARIOS_DIR", tmp_path)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(run_mod.asyncio, "sleep", lambda s: real_sleep(0))
    monkeypatch.setenv("HPCB_ADMIN_TAP", str(_tap_file(tmp_path, "HPCB_NOW 5\n", clean=False)))
    recorded: dict = {}

    def order() -> list:
        return sys.modules["scen_watch"].ORDER

    async def fake_script_run(prompt, **kw):
        assert kw["script"].__name__ == "SCRIPT" and kw["facility"] and kw["ssh_host"]
        order().append("script")
        t = Trace([_call("connect_facility", {"facility": "f"}, {"phase": "needs_account"})])
        return run_mod.RunResult(trace=t, final=types.SimpleNamespace(is_error=False, total_cost_usd=None),
                                 messages=[{"harness": "script", "rc": 0}])

    import script_runner
    monkeypatch.setattr(script_runner, "run_scenario", fake_script_run)

    def fake_ssh(cmd, *, timeout=60, host=None):
        if cmd.startswith("squeue") and "%j" in cmd:   # the universal postcheck (stop_honesty_no_pilot_left)
            order().append("postcheck")
        return 0, ""

    monkeypatch.setattr(run_mod, "_ssh_run", fake_ssh)
    monkeypatch.setattr(run_mod, "_teardown", lambda scen, res=None, **kw: (order().append("teardown"), ("", None))[1])
    monkeypatch.setattr(run_mod, "write_run_record", lambda runs_dir, **kw: recorded.update(kw))
    rc = asyncio.run(run_mod._run("scen_watch", "m", None, None, False, operator="script"))
    assert order() == ["script", "world check", "postcheck", "teardown"]
    rows = {r.name: r for r in recorded["grading"] if isinstance(r, Result)}
    assert rows["world:watched"].ok and "world:watched" in recorded["gating"]
    assert not rows["seen"].ok and "seen" not in recorded["gating"] and rc == 0     # report-only: never decides
    assert recorded["extra_jsonl"]["admin-tap"] == [{"t": 1.0, "rc": 0, "out": "HPCB_NOW 5\n"}]
    assert recorded["config"]["operator"] == "script" and recorded["config"]["world_checks"] == ["watch"]
    sys.modules.pop("scen_watch", None)


# ---- phase `second`: a user endpoint that already ran a block -------------------------------------------------------

UEP = f"uep.{EID}.34aaadd8-3a58-d9ea-c9b0-49257afec4d2"
OTHER_UEP = f"uep.{EID}.9f7c4349-a8a6-c03a-5267-6989ae992de0"


def _second(*, b_stop: dict | None = None, a_up: bool = True, b_uep: str = UEP, sentinel: bool = True) -> Trace:
    """A phase-`second` trace: phase A (a block that answered and ran hostname, job 1, stopped `down`), the sentinel
    (job 12), phase B's confirm, its pilot (job 13, PENDING on the sentinel) and the judged stop."""
    poll = lambda line: _call("run_shell", {"shape": "login", "command": qcr.pilot_poll_cmd(EID)},  # noqa: E731
                              {"phase": "complete", "exit_code": 0, "stdout": line})
    calls = [
        _call("connect_facility", {"facility": "fake-1", "ssh_host": "login"}, {"phase": "proposed_facility_details"}),
        _call("connect_facility", {"facility": "fake-1", "details": {"ssh_host": "login"}}, {"phase": "provisioning"}),
        _call("connect_facility", {"facility": "fake-1", "ssh_host": "login"}, {"phase": "needs_account"}),
        _call("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
              {"status": "provisioning", "endpoint_id": EID, "partition": "main"}),
        _call("ensure_endpoint_up", {"shape": "compute"},
              {"status": "up" if a_up else "provisioning", "block_state": "warm" if a_up else "provisioning",
               "endpoint_id": EID}),
        _call("run_shell", {"shape": "compute", "command": "hostname"},
              {"phase": "complete" if a_up else "cold_start", "stdout": "c1\n"}),
        poll(f"HPCB_PILOT 1 RUNNING {UEP} None\n"),
        _call("stop_endpoint", {}, {**DOWN, "notice": "compute block released over AMQP (released 1); …"}),
    ]
    if sentinel:
        calls += [_call("run_shell", {"shape": "login", "command": qcr.sentinel_cmd("main", None)},
                        {"phase": "complete", "exit_code": 0, "stdout": "12\n"}),
                  _call("run_shell", {"shape": "login", "command": "squeue -j 12 -h -o %T 2>/dev/null; true"},
                        {"phase": "complete", "exit_code": 0, "stdout": "RUNNING\n"}),
                  _call("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
                        {"status": "provisioning", "endpoint_id": EID,
                         "notice": "allocating nodes… — pilot 13 is queued (PENDING (reason Dependency))"}),
                  poll(f"HPCB_PILOT 13 PENDING {b_uep} Dependency\n"),
                  _call("stop_endpoint", {}, b_stop or {**DOWN, "notice": "compute block released over AMQP (released 13)"})]
    return Trace(calls)


def _watch2(*jobs: admin_tap.SlurmJob, covered: float = 425.0, log_lines: list | None = None) -> qcr.Watch:
    """The phase-B watch: the tap saw job 1 (phase A, CANCELLED) and job 13 (the judged stop's own) by marker."""
    seen = (_job("1", T_STOP - 120, state="CANCELLED", reason=""), _job("13", T_STOP - 6, state="CANCELLED", reason=""))
    return qcr.Watch(called_at=T_STOP - 0.6, answered_at=T_STOP, window_s=420.0, covered_until=T_STOP + covered,
                     samples=44, jobs=[*seen, *jobs], own_ids={"13"}, phase="second",
                     held={"14"} & {j.job_id for j in jobs}, own_seen=_seen(seen, {"13"}),
                     log_lines=[] if log_lines is None else log_lines)


def test_phase_selection_and_the_directives_each_phase_sends(monkeypatch):
    for raw, phase in (("", "second"), ("second", "second"), ("first", "first"), ("FIRST ", "first"), ("x", "second")):
        monkeypatch.setenv("HPCB_RELAUNCH_PHASE", raw)
        assert qcr._phase() == phase, raw
    second = qcr._proposal_details(PROPOSAL, 1800, "second")["scheduler_options"]
    assert second == "#SBATCH --job-name=parsl-hpcb-qcr\n#SBATCH --dependency=singleton"   # no --begin: A must run
    assert qcr._proposal_details(PROPOSAL, 1800, "first")["scheduler_options"] == "#SBATCH --begin=now+1800"
    assert "parsl" in qcr.SINGLETON_NAME   # the universal postcheck (`no job named *parsl* left`) still sees a pilot
    # the pilots' own terms: their partition, account and walltime (site's 30-min debug cannot refuse it)
    assert qcr.sentinel_cmd("main", "hpcb", "00:20:00") == (
        "sbatch --parsable -J parsl-hpcb-qcr -N1 -n1 -t 00:20:00 -o /dev/null -e /dev/null -p main -A hpcb "
        "--wrap 'sleep 86400'")
    assert "-t 1-00:00:00 " in qcr.sentinel_cmd("compute", None, "1-00:00:00")
    for bad in (None, "", "30", "00:30; rm -rf /"):   # no walltime, or not one: the shortest partition limit
        assert f"-t {qcr.SENTINEL_WALLTIME} " in qcr.sentinel_cmd("debug", None, bad) and qcr.SENTINEL_WALLTIME == "00:30:00"
    assert qcr.SETUP == qcr.CLEANUP == ['scancel -u "$(whoami)" --name=parsl-hpcb-qcr 2>/dev/null; echo ok']


def test_the_poll_names_the_user_endpoint_and_old_lines_still_read():
    new, old = qcr._pilots(ToolCall("run_shell", {}, {"stdout": f"HPCB_PILOT 13 PENDING {UEP} Dependency\n"
                                                                "HPCB_PILOT 7 PENDING BeginTime\nHPCB_PILOT 8 RUNNING - None"}))[:2]
    assert (new.job, new.state, new.uep, new.reason) == ("13", "PENDING", UEP, "Dependency")
    assert (old.job, old.uep, old.reason) == ("7", "", "BeginTime")
    assert "match($4, /uep" in qcr.pilot_poll_cmd(EID) and "RLENGTH-15" in qcr.pilot_poll_cmd(EID)


def test_phase_b_with_a_relaunch_fails_and_phase_a_does_not_count_as_its_canary():
    t = _second()
    assert qcr._stop(t)[0] == 12 and [i for i, _ in qcr._phase_a_stops(t)] == [7]
    assert qcr.canary_state(t)[0] == "queued"            # phase A's `up` and hostname are before phase B's window
    assert qcr.own_pilots(t) == {"13"} and qcr.same_user_endpoint(t) == (True, f"phase A and phase B both ran in {UEP}")
    r = qcr.no_relaunch_after_down(t, _watch2(_job("14", T_STOP + 62, reason="JobHeldAdmin")))
    assert not r.ok and "job 14 submitted +62 s after the stop answered, PENDING (JobHeldAdmin)" in r.detail
    assert "job 1 submitted" not in r.detail and "job 13 submitted" not in r.detail
    ok = qcr.no_relaunch_after_down(t, _watch2())
    assert ok.ok and "the tap saw its own pilot (job 13 CANCELLED [squeue])" in ok.detail


def test_phase_b_premises_same_user_endpoint_a_connected_phase_a_and_a_started_phase_b():
    # a NEW user endpoint for phase B is the fix's design (fresh per block request), not a broken premise: judged anyway
    other = qcr.no_relaunch_after_down(_second(b_uep=OTHER_UEP), _watch2())
    assert other.ok and "a NEW user endpoint" in other.detail and "the later-pilot case cannot arise" in other.detail
    relaunched = qcr.no_relaunch_after_down(_second(b_uep=OTHER_UEP), _watch2(_job("14", T_STOP + 62)))
    assert not relaunched.ok and "job 14" in relaunched.detail   # a relaunch fails it from either user endpoint
    unknown = qcr.no_relaunch_after_down(_second(b_uep="-"), _watch2())
    assert not unknown.ok and unknown.detail.startswith("premise unverified:")
    cold = qcr.no_relaunch_after_down(_second(a_up=False), _watch2())
    assert not cold.ok and "phase A's block never answered a check" in cold.detail
    never = qcr.no_relaunch_after_down(_second(sentinel=False), _watch2())
    assert not never.ok and "phase B never started" in never.detail
    draining = qcr.no_relaunch_after_down(_second(b_stop=DRAINING), _watch2(_job("14", T_STOP + 62)))
    assert draining.ok and draining.detail.startswith("report-only:") and "job 14" in draining.detail


def test_phase_b_report_rows_phase_a_stop_and_the_user_endpoints_job_history():
    t = _second()
    a = qcr.phase_a_stop(t)
    assert a.ok and a.detail.startswith("call 7: status='down', spend_stopped=True, released ['1']")
    assert qcr.phase_a_stop(_trace(DOWN)).detail.startswith("no phase A")
    assert qcr.pilot_state_at_stop(t).detail == (f"call 11: job 13 PENDING (Dependency) in {UEP}; phase A and phase B "
                                                 f"both ran in {UEP}")
    bad = qcr.LogLine(T_STOP + 57, "uep 34aaadd8", "INFO MainProcess-1 a b c Job is marked as MISSING since …")
    w = _watch2(_job("14", T_STOP + 62, reason="JobHeldAdmin"))
    w.log_lines = [bad]
    h = qcr.uep_job_history(t, w).detail
    assert "job 1 CANCELLED — phase A, connected (a worker answered on it)" in h
    assert "job 13 CANCELLED — the judged stop's own pilot" in h
    assert "job 14 PENDING (JobHeldAdmin) HELD by the tap — RELAUNCHED after the stop" in h
    assert "1 MISSING / bad-state line(s)" in h


def test_the_judged_stops_times_come_from_its_own_journal_row():
    msgs = [{"journal": {"tool": "stop_endpoint", "ts": 100.0, "ms": 500}},
            {"journal": {"tool": "run_shell", "ts": 150.0, "ms": 10}},
            {"journal": {"tool": "stop_endpoint", "ts": 200.0, "ms": 1000}}]
    assert qcr._stop_times(msgs, 1) == (200.0, 201.0) and qcr._stop_times(msgs) == (100.0, 100.5)
    assert qcr._judged_nth(_second()) == 1 and qcr._judged_nth(_trace(DOWN)) == 0


def test_the_hold_request_round_trip_and_the_hold_command(tmp_path):
    out = tmp_path / ".admin-tap-r1.jsonl"
    reader = admin_tap.TapReader(out)
    assert reader.request_hold(f"uep.{EID}", 13) and admin_tap.read_hold(admin_tap.hold_path(out)) == (f"uep.{EID}", "13")
    assert not reader.request_hold("uep.x; rm -rf /", 13) and not reader.request_hold(f"uep.{EID}", "13; id")
    assert not admin_tap.TapReader(None).request_hold(f"uep.{EID}", 13)
    admin_tap.hold_path(out).write_text("uep.$(id) 13\n")            # a request that is not a marker and an id is ignored
    assert admin_tap.read_hold(admin_tap.hold_path(out)) is None
    reader.request_hold(f"uep.{EID}", 13)
    seen: list[str] = []

    def run(argv):
        seen.append(argv[-1])
        return 0, "HPCB_HELD 13\nHPCB_NOW 5\n"

    clock = iter(range(0, 100, 10))
    admin_tap.tap(out, ["docker", "exec", "c", "bash", "-lc"], "squeue", every_s=10, max_s=15, run=run,
                  hold_cmd="hold {marker} from {from}", sleep=lambda s: None, clock=lambda: float(next(clock)))
    assert seen == [f"hold uep.{EID} from 13; squeue"]   # the hold first: a pilot it holds is then seen held
    (sample,) = admin_tap.read_samples(out)
    assert sample["hold"] == [f"uep.{EID}", "13"] and admin_tap.parse_slurm_sample(sample).held == ["13"]
    cmd = admin_tap.slurm_hold_cmd()
    assert "squeue -u {user} -h -t PD" in cmd and "scontrol hold" in cmd and 'HPCB_HELD $j' in cmd
    assert "index($3,m) && $1+0 >= f+0" in cmd and '"JobHeldAdmin"' in cmd


def test_the_teardowns_cancel_by_marker_covers_held_pilots():
    # held jobs are PENDING: the run-scoped cancel lists the pool user's queue with no state filter, so it finds them
    from cluster_ops import scoped_cancel_cmd
    cmd = scoped_cancel_cmd("slurm", [EID])
    listing = cmd[cmd.index("squeue"):cmd.index("| awk")]
    assert "-u \"$(whoami)\"" in listing and " -t " not in listing and "--states" not in listing and "scancel" in cmd


def test_the_phase_second_script_runs_a_then_holds_b_behind_the_sentinel(tmp_path, monkeypatch):
    monkeypatch.setenv("HPCB_RELAUNCH_PHASE", "second")
    tap = tmp_path / ".admin-tap-r1.jsonl"
    monkeypatch.setenv("HPCB_ADMIN_TAP", str(tap))
    ops = _Ops({
        "connect_facility": [PROPOSAL, {"phase": "provisioning"}, {"phase": "needs_account", "allocations": []}],
        "ensure_endpoint_up": [{"status": "provisioning", "endpoint_id": EID, "partition": "main",
                                "block": {"queue": "main", "walltime": "00:20:00"}},
                               {"status": "up", "block_state": "warm", "endpoint_id": EID},
                               {"status": "provisioning", "endpoint_id": EID}],
        "run_shell": [{"phase": "complete", "exit_code": 0, "stdout": "c1\n"},                        # hostname
                      {"phase": "complete", "stdout": f"HPCB_PILOT 1 RUNNING {UEP} None\n"},           # A's pilot
                      {"phase": "complete", "exit_code": 0, "stdout": "12\n"},                        # the sentinel
                      {"phase": "complete", "stdout": "PENDING\n"}, {"phase": "complete", "stdout": "RUNNING\n"},
                      {"phase": "complete", "stdout": f"HPCB_PILOT 1 CANCELLED {UEP} None\n"},         # B: not yet
                      {"phase": "complete", "stdout": f"HPCB_PILOT 13 PENDING {UEP} Dependency\n"}],
        "stop_endpoint": [{**DOWN, "notice": "compute block released over AMQP (released 1)"}, DOWN],
    })
    final = asyncio.run(qcr.SCRIPT(ops))
    tools = [t for t, _ in ops.calls]
    assert tools == (["connect_facility"] * 3 + ["ensure_endpoint_up"] * 2 + ["run_shell"] * 2 + ["stop_endpoint"]
                     + ["run_shell"] * 3 + ["ensure_endpoint_up"] + ["run_shell"] * 2 + ["stop_endpoint"])
    assert ops.calls[1][1]["details"]["scheduler_options"].endswith("#SBATCH --dependency=singleton")
    confirms = [a for t, a in ops.calls if t == "ensure_endpoint_up" and a.get("confirm_spend")]
    assert confirms == [{"shape": "compute", "confirm_spend": True}] * 2   # the same terms: the same user endpoint
    assert ops.calls[5] == ("run_shell", {"shape": "compute", "command": "hostname"})
    assert ops.calls[8][1]["command"] == qcr.sentinel_cmd("main", None, "00:20:00")   # the block's own terms
    assert admin_tap.read_hold(admin_tap.hold_path(tap)) == (f"uep.{EID}", "13")   # held from after the sentinel
    assert "teardown_endpoint" not in tools and "phase B stopped with job 13 PENDING (Dependency)" in final
    assert any("the admin tap holds every new pilot" in x for x in ops.texts)


def test_the_phase_second_script_aborts_when_the_sentinel_never_runs(monkeypatch):
    import script_runner
    monkeypatch.setenv("HPCB_RELAUNCH_PHASE", "second")
    monkeypatch.setattr(qcr, "SENTINEL_WAIT_S", 0.0)
    ops = _Ops({
        "connect_facility": [PROPOSAL, {"phase": "needs_account"}],
        "ensure_endpoint_up": [{"status": "up", "endpoint_id": EID}],
        "run_shell": [{"phase": "complete", "stdout": "c1"}, {"phase": "complete", "stdout": f"HPCB_PILOT 1 RUNNING {UEP} x"},
                      {"phase": "complete", "stdout": "12"}, {"phase": "complete", "stdout": "PENDING"}],
        "stop_endpoint": [DOWN],
    })
    with pytest.raises(script_runner.ScriptAbort, match=r"the sentinel \(job 12\) never started running"):
        asyncio.run(qcr.SCRIPT(ops))
    assert [t for t, _ in ops.calls].count("ensure_endpoint_up") == 1   # phase B's confirm never went out


def test_the_world_check_ends_the_sentinel_after_the_watch(tmp_path, monkeypatch):
    monkeypatch.setenv("HPCB_RELAUNCH_PHASE", "second")
    run = SCRIPT_OF.format(eid=EID, ts=1)
    tap = _tap_file(tmp_path, f"HPCB_NOW {T_STOP + 5:.0f}\nHPCB_Q 13|CANCELLED|{T_STOP - 6:.0f}|None|{run}",
                    f"HPCB_NOW {T_STOP + 421:.0f}\nHPCB_HELD 14\nHPCB_Q 14|PENDING|{T_STOP + 62:.0f}|JobHeldAdmin|{run}")
    sent: list[str] = []
    ctx = types.SimpleNamespace(trace=_second(), messages=[{"journal": {"tool": "stop_endpoint", "ts": 1.0, "ms": 1}},
                                                          *_messages()[1:]],
                                tap=admin_tap.TapReader(tap), endpoint_name="hpc-bridge-fake-1",
                                ssh=lambda cmd, timeout: (sent.append(cmd), (0, "=== x\n"))[1])
    ctx.tap.wait_until = lambda now, **kw: asyncio.sleep(0, result=ctx.tap.samples())
    gated, report, _strategy, history = asyncio.run(qcr.relaunch_watch(ctx))
    assert not gated.ok and "job 14 submitted +62 s after the stop answered, PENDING (JobHeldAdmin)" in gated.detail
    assert "held: 14" in report.detail and "HELD by the tap — RELAUNCHED after the stop" in history.detail
    end = sent[-1]   # once the watch is over: one last hold tick, the sentinel cancelled by its id, then waited out
    assert end.startswith('for j in $(squeue -u "$(whoami)" -h -t PD') and f"-v m=uep.{EID} -v f=13" in end
    assert "$2 !~ /^JobHeld/" in end and end.index("scontrol hold") < end.index("scancel 12")
    assert "squeue -h -j 12 -o %T" in end and "HPCB_SENTINEL gone" in end



# ---- review 2026-10-15: no blind pass, ever ---------------------------------------------------------------------------

def _ctx(tmp_path, *outs: str, clean: bool = True, ssh=None):
    """A world-check context over tap samples written as given (`clean`: each query reported exit 0), the endpoint log
    readable (`=== log`, no strategy line) unless `ssh` says otherwise."""
    ctx = types.SimpleNamespace(trace=_trace(DOWN), messages=_messages(),
                                tap=admin_tap.TapReader(_tap_file(tmp_path, *outs, clean=clean)),
                                ssh=ssh or (lambda cmd, timeout: (0, "=== log\n")), endpoint_name="hpc-bridge-fake-1")
    ctx.tap.wait_until = lambda now, **kw: asyncio.sleep(0, result=ctx.tap.samples())
    return ctx


def test_the_reviewers_blind_pass_now_fails(tmp_path):
    # the review's demo (scratchpad/review-qcr/blind_pass.py): squeue errors in every sample (its sed's rc was the
    # sample's), the own pilot only in sacct, the log unread — it PASSED; each of those alone must now fail it
    run = SCRIPT_OF.format(eid=EID, ts=1)
    err = "HPCB_Q squeue: error: Invalid job state specified: all"
    own = f"HPCB_A 7|parsl.x|CANCELLED by 0|{T_STOP - 9:.0f}|Unknown|{T_STOP:.0f}|sbatch {run}"
    demo = _ctx(tmp_path, f"HPCB_NOW {T_STOP - 5:.0f}\n{err}", f"HPCB_NOW {T_STOP + 60:.0f}\n{err}\n{own}",
                f"HPCB_NOW {T_STOP + 425:.0f}\n{err}\n{own}", clean=False)
    demo.ssh = None
    gated = asyncio.run(qcr.relaunch_watch(demo))[0]
    assert not gated.ok and gated.detail.startswith("UNVERIFIABLE — the admin tap has no CLEAN sample after the stop")
    # the same with exit statuses: squeue failing is reported by its own status, not swallowed
    failing = _ctx(tmp_path, *(f"HPCB_NOW {T_STOP + d:.0f}\n{err}\nHPCB_Q HPCB_RC 1\n{own}\nHPCB_A HPCB_RC 0"
                               for d in (-5, 60, 425)), clean=False)
    assert "no CLEAN sample" in asyncio.run(qcr.relaunch_watch(failing))[0].detail


def test_a_lookup_by_a_known_id_is_never_the_sighting(tmp_path):
    # every query clean, the queue and the window empty — the own pilot only under HPCB_J (sacct -j): blind
    by_id = f"HPCB_J 7|parsl.x|CANCELLED by 0|{T_STOP - 9:.0f}|Unknown|{T_STOP:.0f}|sbatch {SCRIPT_OF.format(eid=EID, ts=1)}"
    ctx = _ctx(tmp_path, *(f"HPCB_NOW {T_STOP + d:.0f}\n{by_id}\nHPCB_J HPCB_RC 0" for d in (5, 425)))
    gated, report, *_ = asyncio.run(qcr.relaunch_watch(ctx))
    assert not gated.ok and gated.detail.startswith("watch blind: no clean sample's squeue or window sacct listed")
    assert "sacct -j found job 7 CANCELLED — a lookup by a known id, which could never see a relaunch" in gated.detail
    assert "by id (sacct -j): 7 CANCELLED" in report.detail


def test_a_sample_with_error_lines_a_failed_query_or_no_exit_status_is_not_clean():
    ok = admin_tap.parse_slurm_sample({"rc": 0, "out": "HPCB_NOW 5\nHPCB_Q HPCB_RC 0\nHPCB_A HPCB_RC 0"})
    assert ok.clean and ok.queue == ok.history == []          # an empty queue that SAID so
    errs = admin_tap.parse_slurm_sample({"rc": 0, "out": "HPCB_NOW 5\nHPCB_Q HPCB_RC 0\n"
                                                         "HPCB_A sacct: error: Problem talking to the database\n"
                                                         "HPCB_A HPCB_RC 1"})
    assert not errs.clean and errs.history == [] and "the window sacct exited 1" in errs.error
    assert "sacct: error: Problem talking to the database" in errs.error   # the failed query's own words, quoted
    failed = admin_tap.parse_slurm_sample({"rc": 0, "out": "HPCB_NOW 5\nHPCB_Q HPCB_RC 1\nHPCB_A HPCB_RC 0"})
    assert not failed.clean and "squeue exited 1" in failed.error
    silent = admin_tap.parse_slurm_sample({"rc": 0, "out": "HPCB_NOW 5\nHPCB_Q HPCB_RC 0"})
    assert not silent.clean and "the window sacct reported no exit status" in silent.error
    j = admin_tap.parse_slurm_sample({"rc": 0, "out": "HPCB_NOW 5\nHPCB_Q HPCB_RC 0\nHPCB_A HPCB_RC 0\n"
                                                      "HPCB_J 7|n|CANCELLED|1|Unknown|2|sbatch x\nHPCB_J HPCB_RC 0"})
    assert j.clean and [x.job_id for x in j.by_id] == ["7"] and j.history == [] and j.by_id[0].source == "sacct-j"
    # samples that are not clean cover nothing; a relaunch they list still counts
    w = qcr.Watch(answered_at=10.0, own_ids={"7"})
    run = SCRIPT_OF.format(eid=EID, ts=1)
    qcr.fold_samples(w, [errs, admin_tap.parse_slurm_sample(
        {"rc": 0, "out": f"HPCB_NOW 90\nHPCB_Q 8|PENDING|60|None|{run}\nHPCB_Q HPCB_RC 0"})], [EID])
    assert w.covered_until is None and [j.job_id for j in w.jobs] == ["8"] and len(w.unclean) == 2
    assert w.unverifiable.startswith("the admin tap has no CLEAN sample after the stop answered (2 sample(s), 2 not clean")


def test_the_end_of_phase_b_holds_once_more_then_waits_the_sentinel_out():
    cmd = qcr.end_sentinel_cmd("12", f"uep.{EID}", "13")
    assert cmd.index("scontrol hold") < cmd.index("scancel 12") < cmd.index("squeue -h -j 12 -o %T")
    fallback = qcr.end_sentinel_cmd(None, f"uep.{EID}", None)   # no sentinel id: by name, running, no hold tick
    assert "scontrol hold" not in fallback and "--name=parsl-hpcb-qcr --state=RUNNING" in fallback
    assert "-n parsl-hpcb-qcr -t R,CG" in fallback   # waited out while it still runs or completes

    out = "HPCB_HELD 15\nHPCB_SENTINEL gone\n"
    w = qcr.Watch()
    ctx = types.SimpleNamespace(ssh=lambda c, timeout: (0, out))
    monkey_t = _second()
    asyncio.run(qcr._end_sentinel(ctx, monkey_t, w))
    assert w.held == {"15"} and w.sentinel == "gone"
    assert "sentinel at the end: gone" in qcr.relaunch_report(w).detail


def test_the_tap_ends_itself_and_removes_its_files_when_its_parent_is_gone(tmp_path):
    out = tmp_path / ".admin-tap-r1.jsonl"
    admin_tap.ids_path(out).write_text("7\n")
    alive = iter([True, False])
    n = admin_tap.tap(out, ["x"], "squeue", every_s=10, run=lambda argv: (0, "HPCB_NOW 1\n"), sleep=lambda s: None,
                      parent_pid=4242, parent_alive=lambda pid: next(alive, False), ids_cmd="sacct -j {ids}")
    assert n == 1 and not [f for f in admin_tap.tap_files(out) if f.exists()]   # orphaned: nobody else will clean up
    out2 = tmp_path / ".admin-tap-r2.jsonl"
    clock = iter(range(0, 100, 10))
    admin_tap.tap(out2, ["x"], "squeue", every_s=10, max_s=15, run=lambda argv: (0, "HPCB_NOW 1\n"),
                  sleep=lambda s: None, clock=lambda: float(next(clock)))
    assert out2.exists() and not admin_tap.pid_path(out2).exists()   # a normal end: run_smoke.sh's trap removes the rest


def test_the_tap_cli_exits_when_the_pid_it_watches_dies(tmp_path):
    out = tmp_path / ".admin-tap-r3.jsonl"
    parent = subprocess.Popen(["sleep", "2"])
    stub = tmp_path / "admin.sh"
    stub.write_text('#!/usr/bin/env bash\necho "HPCB_NOW $(date +%s)"\n')
    stub.chmod(0o755)
    p = subprocess.Popen([sys.executable, str(HERE / "admin_tap.py"), "--out", str(out), "--user", "u",
                          "--knob", json.dumps({"cmd": "squeue", "every_s": 1}), "--parent-pid", str(parent.pid),
                          "--", str(stub)])
    try:
        parent.wait(timeout=10)          # gone (and reaped: an unreaped zombie still answers kill -0)
        assert p.wait(timeout=30) == 0   # ended by itself, a second or so after it
    finally:
        p.kill()
    assert not [f for f in admin_tap.tap_files(out) if f.exists()]


def test_run_suite_stops_an_abandoned_cells_tap_by_its_pid_file_and_only_that(tmp_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_suite", HERE.parent / "run_suite.py")
    suite = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suite)
    out = tmp_path / ".admin-tap-r9.jsonl"
    fake = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", "admin_tap.py", "--out", str(out)])
    try:
        for f in admin_tap.tap_files(out):
            f.write_text("x\n")
        admin_tap.pid_path(out).write_text(f"{fake.pid}\n")
        assert suite._stop_admin_tap("r9", runs=tmp_path) == f"stopped tap pid {fake.pid}"
        assert fake.wait(timeout=10) != 0 and not [f for f in admin_tap.tap_files(out) if f.exists()]
    finally:
        fake.kill()
    # a pid that is no longer this cell's tap (reused) is left alone; the files still go
    admin_tap.pid_path(out).write_text(f"{os.getpid()}\n")
    assert "is not this cell's tap" in suite._stop_admin_tap("r9", runs=tmp_path, kill=lambda *a: pytest.fail("killed"))
    assert suite._stop_admin_tap("r10", runs=tmp_path) == "no tap"


def test_a_tap_whose_own_parent_dies_is_re_parented_and_ends(tmp_path):
    # run_smoke.sh passes its own pid, the tap's parent: killed outright it may linger as a zombie its own parent has not
    # reaped yet — the tap is re-parented all the same, and that is enough
    out = tmp_path / ".admin-tap-r4.jsonl"
    stub = tmp_path / "admin.sh"
    stub.write_text('#!/usr/bin/env bash\necho "HPCB_NOW $(date +%s)"\n')
    stub.chmod(0o755)
    knob = json.dumps({"cmd": "squeue", "every_s": 1}).replace("'", "'\\''")
    shell = (f"{sys.executable} {HERE / 'admin_tap.py'} --out {out} --user u --knob '{knob}' --parent-pid $$ "
             f"-- {stub} & echo $!; sleep 2; kill -9 $$")
    r = subprocess.run(["bash", "-c", shell], capture_output=True, text=True, timeout=30)
    tap_pid = int(r.stdout.split()[0])
    for _ in range(300):
        if not admin_tap.alive(tap_pid) or not admin_tap.pid_path(out).exists():
            break
        subprocess.run(["sleep", "0.1"], check=False)
    assert not [f for f in admin_tap.tap_files(out) if f.exists()]


def test_the_proving_runs_own_sample_reads_as_the_relaunch_it_was():
    # run 1792120434 (fake/default, 2026-10-15), its admin-tap.jsonl sample at +54 s, verbatim — plus the exit-status
    # lines the tap prints since: `squeue -t all` lists the cancelled pilots and the sentinel (`%o` = (null) for a
    # --wrap job), the relaunched block-2 held by the tap, epoch times; sacct's Start is `None` for a job never started
    eid = "1fc1a5de-6e93-48f7-9390-e46ac016f2ba"
    uep = f"uep.{eid}.3aba04bd-a764-cde2-3abe-c5d62f96f339"
    sub = f"/home/hpcbridge-test-00/.globus_compute/{uep}/submit_scripts/parsl.GlobusComputeEngine-HighThroughputExecutor"
    out = (f"HPCB_HELD 5\nHPCB_NOW 1792120592\nHPCB_Q 4|CANCELLED|1792120531|Dependency|{sub}.block-1.1792120531.0697782\n"
           f"HPCB_Q 2|CANCELLED|1792120470|None|{sub}.block-0.1792120470.9103878\n"
           f"HPCB_Q 5|PENDING|1792120591|JobHeldAdmin|{sub}.block-2.1792120591.2134814\n"
           "HPCB_Q 3|RUNNING|1792120483|None|(null)\n"
           f"HPCB_A 4|parsl-hpcb-qcr|CANCELLED by 2000|1792120531|None|1792120537|sbatch {sub}.block-1.1792120531.0697782\n"
           "HPCB_Q HPCB_RC 0\nHPCB_A HPCB_RC 0")
    sample = admin_tap.parse_slurm_sample({"t": 1.0, "rc": 0, "out": out})
    assert sample.clean and sample.held == ["5"] and [j.job_id for j in sample.queue] == ["4", "2", "5", "3"]
    poll = lambda line: _call("run_shell", {"shape": "login", "command": qcr.pilot_poll_cmd(eid)},  # noqa: E731
                              {"phase": "complete", "stdout": line})
    t = Trace([
        _call("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True}, {"status": "up", "endpoint_id": eid}),
        _call("run_shell", {"shape": "compute", "command": "hostname"}, {"phase": "complete"}),
        poll(f"HPCB_PILOT 2 RUNNING {uep} None\n"),
        _call("stop_endpoint", {}, {"status": "down", "spend_stopped": True, "notice": "released 2"}),
        _call("run_shell", {"shape": "login", "command": qcr.sentinel_cmd("main", None)}, {"stdout": "3\n"}),
        _call("ensure_endpoint_up", {"shape": "compute", "confirm_spend": True},
              {"status": "provisioning", "endpoint_id": eid}),
        poll(f"HPCB_PILOT 4 PENDING {uep} Dependency\n"),
        _call("stop_endpoint", {}, {"status": "down", "spend_stopped": True, "notice": "released 4"}),
    ])
    w = qcr.Watch(called_at=1792120537.0, answered_at=1792120538.0, own_ids=qcr.own_pilots(t), phase="second",
                  log_lines=[])
    qcr.fold_samples(w, [sample], [eid])
    assert w.own_seen == {"4": "CANCELLED [squeue]"} and w.held == {"5"}   # squeue first
    r = qcr.no_relaunch_after_down(t, w)
    assert not r.ok and "job 5 submitted +53 s after the stop answered, PENDING (JobHeldAdmin)" in r.detail
    assert qcr.same_user_endpoint(t) == (True, f"phase A and phase B both ran in {uep}")



# ---- review round 2: a healthy sample stays clean ---------------------------------------------------------------------

RC0 = "HPCB_Q HPCB_RC 0\nHPCB_A HPCB_RC 0"


def test_a_query_that_ended_0_may_print_lines_that_are_not_rows_and_the_sample_stays_clean():
    run = SCRIPT_OF.format(eid=EID, ts=1)
    # another cell's `sbatch --wrap` with a newline in it: sacct -P prints its SubmitLine over two lines
    wrapped = admin_tap.parse_slurm_sample({"rc": 0, "out": (
        f"HPCB_NOW 5\nHPCB_A 41|wrap|COMPLETED|1|2|3|sbatch --wrap 'echo a\nHPCB_A echo b'\n"
        f"HPCB_A 7|parsl.x|CANCELLED by 0|1|None|4|sbatch {run}\n{RC0}")})
    assert wrapped.clean and wrapped.skipped == 1 and [j.job_id for j in wrapped.history] == ["41", "7"]
    # a pending array and a het job component read as jobs (never ours: our pilots are plain ids)
    odd = admin_tap.parse_slurm_sample({"rc": 0, "out": (
        "HPCB_NOW 5\nHPCB_Q 42_[1-3]|PENDING|1|Resources|/x/a.sh\nHPCB_Q 42_7|RUNNING|1|None|/x/a.sh\n"
        f"HPCB_Q 43+0|RUNNING|1|None|/x/h.sh\nHPCB_A 43+1|het|COMPLETED|1|2|3|sbatch h.sh\n{RC0}")})
    assert odd.clean and [j.job_id for j in odd.queue] == ["42_[1-3]", "42_7", "43+0"] and odd.history[0].job_id == "43+1"
    assert qcr.endpoint_jobs([odd], [EID]) == []   # no marker of ours
    # the same lines under a query that FAILED: the sample is not clean, and the error quotes them
    bad = admin_tap.parse_slurm_sample({"rc": 0, "out": "HPCB_NOW 5\nHPCB_Q squeue: error: Invalid user\n"
                                                        "HPCB_Q HPCB_RC 1\nHPCB_A HPCB_RC 0"})
    assert not bad.clean and "squeue exited 1 ('squeue: error: Invalid user')" in bad.error


def test_a_failed_lookup_by_id_is_reported_never_an_unclean_sample():
    s = admin_tap.parse_slurm_sample({"rc": 0, "out": "HPCB_NOW 5\n" + RC0 +
                                                      "\nHPCB_J sacct: error: Invalid job id\nHPCB_J HPCB_RC 1"})
    assert s.clean and s.by_id == [] and s.by_id_error == "sacct -j exited 1 ('sacct: error: Invalid job id')"


def test_the_tap_opens_the_window_sacct_at_its_own_start(tmp_path):
    seen: list[str] = []
    clock = iter([0.0, 0.0, 10.0, 40.0])
    admin_tap.tap(tmp_path / "t.jsonl", ["x"], "sacct -S {since} -E now", every_s=10, max_s=35,
                  run=lambda argv: (seen.append(argv[-1]), (0, "HPCB_NOW 1\n"))[1], sleep=lambda s: None,
                  clock=lambda: next(clock))
    assert seen == [f"sacct -S now-{admin_tap.SINCE_MARGIN_S} -E now", f"sacct -S now-{admin_tap.SINCE_MARGIN_S + 10} -E now"]


def test_the_wait_goes_on_past_an_unclean_sample(tmp_path):
    tap = tmp_path / "t.jsonl"
    tap.write_text(json.dumps({"t": 1, "rc": 0, "out": "HPCB_NOW 500\nHPCB_Q HPCB_RC 1\nHPCB_A HPCB_RC 0"}) + "\n")
    reader = admin_tap.TapReader(tap)
    t = {"now": 0.0}

    async def sleep(s):   # the next tick: a clean sample arrives
        t["now"] += s
        with tap.open("a") as fh:
            fh.write(json.dumps({"t": 2, "rc": 0, "out": f"HPCB_NOW 510\n{RC0}"}) + "\n")

    got = asyncio.run(reader.wait_until(400.0, deadline_s=60, sleep=sleep, clock=lambda: t["now"]))
    assert [s.clean for s in got] == [False, True] and t["now"] > 0   # did not stop at the unclean one


def test_a_sacct_row_of_a_live_pilot_is_not_a_sighting_squeue_comes_first():
    run = SCRIPT_OF.format(eid=EID, ts=1)
    live = admin_tap.parse_slurm_sample({"rc": 0, "out": f"HPCB_NOW 5\nHPCB_A 7|p|PENDING|1|None|None|sbatch {run}\n{RC0}"})
    assert qcr.own_sightings([live], [EID], {"7"}) == {}            # the window sacct never lists a held PENDING job
    ended = admin_tap.parse_slurm_sample({"rc": 0, "out": f"HPCB_NOW 6\nHPCB_A 7|p|CANCELLED by 0|1|None|3|sbatch {run}\n{RC0}"})
    assert qcr.own_sightings([live, ended], [EID], {"7"}) == {"7": "CANCELLED [sacct]"}
    queued = admin_tap.parse_slurm_sample({"rc": 0, "out": f"HPCB_NOW 4\nHPCB_Q 7|PENDING|1|Dependency|{run}\n{RC0}"})
    assert qcr.own_sightings([queued, ended], [EID], {"7"}) == {"7": "PENDING [squeue]"}


def test_no_hold_tick_without_a_whole_endpoint_marker():
    for marker in ("uep.", "uep. ", "", "uep.not-a-uuid"):
        assert "scontrol hold" not in qcr.end_sentinel_cmd("12", marker, "13"), marker
    assert "scontrol hold" in qcr.end_sentinel_cmd("12", f"uep.{EID}", "13")
    assert admin_tap.is_marker(f"uep.{EID}") and not admin_tap.is_marker("uep.")
    sent: list[str] = []
    no_eid = Trace([c for c in _second().calls if not (c.result or {}).get("endpoint_id")])   # no endpoint id known
    asyncio.run(qcr._end_sentinel(types.SimpleNamespace(ssh=lambda c, timeout: (sent.append(c), (0, ""))[1]), no_eid,
                                  qcr.Watch()))
    assert sent and "scontrol hold" not in sent[0] and "scancel 12" in sent[0]

"""Hermetic: queued_check_relaunch — its grader on synthetic traces, the admin tap's parsing of the scheduler's output
(in the formats the fake cluster prints), its script against a stand-in server, and run.py's ordering (the world check
before the postchecks and the teardown). No agent, no cluster."""
from __future__ import annotations

import asyncio
import json
import shlex
import subprocess
import sys
import types
from pathlib import Path

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


def _watch(*jobs: admin_tap.SlurmJob, covered: float = 250.0, unverifiable: str | None = None,
           own: set[str] | None = None, log_lines: list | None = None) -> qcr.Watch:
    """A watch that saw the cell's own pilot (job 7) unless told otherwise — the PASS path needs that proof."""
    if not any(j.job_id == "7" for j in jobs) and own is None:
        jobs = (_job("7", T_STOP - 9, state="CANCELLED", reason=""), *jobs)
    return qcr.Watch(called_at=T_STOP - 0.6, answered_at=T_STOP, window_s=240.0, covered_until=T_STOP + covered,
                     samples=26, jobs=list(jobs), unverifiable=unverifiable, own_ids={"7"} if own is None else own,
                     log_lines=log_lines)


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
    assert not no_stop.ok and "premise not met: no stop_endpoint" in no_stop.detail
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
        "HPCB_Q squeue: error: Invalid user: nobody",          # a Slurm error under the tag is not a job
        # sacct -X -n -P -o JobID,JobName,State,Submit,Start,End,SubmitLine — the stored fake bundles' row shape
        # (1788634219-63412-53791-slurm_worker_died: `2|parsl.GlobusComputeEngine-…|CANCELLED by 2000|…`)
        "HPCB_A 7|parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.1788629578.5684283|CANCELLED by 2000|"
        f"2026-09-05T17:32:59|Unknown|2026-09-05T17:33:16|sbatch {run}",
    ])}
    s = admin_tap.parse_slurm_sample(sample)
    assert s.now == 1792105400.0 and s.error == ""
    assert [(j.job_id, j.state, j.reason, j.submit) for j in s.queue] == [
        ("8", "PENDING", "BeginTime", 1792105462.0), ("2", "RUNNING", "None", 1792105313.0)]
    (a,) = s.history
    assert (a.job_id, a.state, a.start, a.source) == ("7", "CANCELLED", None, "sacct")
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
    # -t all: a job finished within MinJobAge is still listed; -s <every state>: a never-eligible (held) job is too
    assert "squeue -u {user} -h -t all -o '%i|%T|%V|%r|%o'" in cmd and "SubmitLine" in cmd and "-S now-2hours" in cmd
    assert f"-s {admin_tap.SACCT_STATES}" in cmd and admin_tap.SACCT_STATES.split(",")[:3] == ["PD", "R", "CA"]
    assert qcr.ADMIN_TAP["cmd"] == cmd and qcr.ADMIN_TAP["every_s"] == 10
    assert qcr.ADMIN_TAP["ids_cmd"] == admin_tap.slurm_ids_cmd() and "sacct -j {ids} -X -n -P" in qcr.ADMIN_TAP["ids_cmd"]
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
        for _ in range(100):
            if out.exists() and len(admin_tap.read_samples(out)) >= 2:
                break
            subprocess.run(["sleep", "0.05"], check=False)
    finally:
        p.terminate()
        assert p.wait(timeout=10) == 0
    rows = admin_tap.read_samples(out)
    assert len(rows) >= 2 and rows[0]["rc"] == 0 and "squeue -u hpcbridge-test-05; sacct -j 4,5" in rows[0]["out"]


# ---- the world check: waits on the tap, then grades -----------------------------------------------------------------

def _tap_file(tmp_path, *outs: str) -> Path:
    p = tmp_path / ".admin-tap-r1.jsonl"
    p.write_text("".join(json.dumps({"t": 1.0, "rc": 0, "out": o}) + "\n" for o in outs))
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
    gated, report, strategy = asyncio.run(qcr.relaunch_watch(ctx))
    assert not gated.ok and "job 8 submitted +63 s after the stop answered, PENDING (BeginTime)" in gated.detail
    assert report.name == "relaunch_watch" and "3 admin sample(s), covering 425 s" in report.detail
    assert "job 7 CANCELLED" in report.detail and "the cell's own pilot(s): 7" in report.detail
    assert strategy.name == "strategy_after_stop" and strategy.detail.startswith("UNVERIFIABLE")   # no world channel
    assert (tap.parent / (tap.name + ".ids")).read_text().split() == ["7"]   # the tap was asked for job 7 by id


def test_the_world_check_passes_a_quiet_covered_queue_and_is_unverifiable_without_a_tap(tmp_path):
    run = SCRIPT_OF.format(eid=EID, ts=1)
    tap = _tap_file(tmp_path, f"HPCB_NOW {T_STOP - 5:.0f}\nHPCB_Q 7|PENDING|{T_STOP - 9:.0f}|BeginTime|{run}",
                    f"HPCB_NOW {T_STOP + 421:.0f}\n")
    ctx = types.SimpleNamespace(trace=_trace(DOWN), messages=_messages(), tap=admin_tap.TapReader(tap))
    assert asyncio.run(qcr.relaunch_watch(ctx))[0].ok   # the default watch: 420 s
    blind = types.SimpleNamespace(trace=_trace(DOWN), messages=_messages(), tap=admin_tap.TapReader(None))
    gated = asyncio.run(qcr.relaunch_watch(blind))[0]
    assert not gated.ok and "HPCB_ADMIN_TAP is unset" in gated.detail
    stale = types.SimpleNamespace(trace=_trace(DOWN), messages=_messages(),
                                  tap=admin_tap.TapReader(_tap_file(tmp_path, f"HPCB_NOW {T_STOP - 5:.0f}\n")))
    stale.tap.wait_until = lambda now, **kw: asyncio.sleep(0, result=stale.tap.samples())   # no waiting in a test
    assert "no sample after the stop answered" in asyncio.run(qcr.relaunch_watch(stale))[0].detail


# ---- a blind watch never passes (the first live run passed blind, 2026-10-15) ------------------------------------------

def test_a_watch_that_never_saw_the_cells_own_pilot_fails_blind():
    blind = qcr.no_relaunch_after_down(_trace(DOWN), _watch(own={"7"}))   # 37 samples, none of them job 7
    assert not blind.ok and blind.detail.startswith("watch blind: the tap never saw this endpoint's own pilot (job 7)")
    # seen by id but without the marker: a relaunch would go unmatched too
    w = _watch(own={"7"})
    w.seen_unmarked = {"7"}
    assert "without the uep.<eid> marker" in qcr.no_relaunch_after_down(_trace(DOWN), w).detail
    # no own pilot id at all (the stop released none, no poll saw one): nothing proves the tap can see this endpoint
    stop = {**DOWN, "notice": "compute block released over AMQP (released none)"}
    nothing = qcr.no_relaunch_after_down(_trace(stop, pilot=""), _watch(own=set()))
    assert not nothing.ok and "no pilot id of this cell is known" in nothing.detail
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
    gated, _report, strategy = asyncio.run(qcr.relaunch_watch(ctx))
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
    assert "teardown_endpoint" not in tools and "stopped with 7 PENDING (BeginTime) in the queue" in final


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
    with pytest.raises(script_runner.ScriptAbort, match=f"no pilot of uep.{EID} entered the queue"):
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
    assert qcr.NEEDS_COMPUTE_NODE is False      # every pilot is held: no node is ever taken
    knobs = subprocess.run([sys.executable, str(HERE / "scenario_knobs.py"), "queued_check_relaunch"],
                           capture_output=True, text=True, timeout=60, check=True).stdout
    k = dict(ln.split("=", 1) for ln in knobs.splitlines() if "=" in ln)
    assert json.loads(shlex.split(k["HPCB_KNOB_ADMIN_TAP"])[0]) == qcr.ADMIN_TAP and k["HPCB_KNOB_TARGETS"] == "fake"
    assert "HPCB_KNOB_NEEDS_NODE" not in k
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
    monkeypatch.setenv("HPCB_ADMIN_TAP", str(_tap_file(tmp_path, "HPCB_NOW 5\n")))
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

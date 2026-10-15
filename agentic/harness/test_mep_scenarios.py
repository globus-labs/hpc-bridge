"""Hermetic: the fake-MEP scenarios (mep profile) — graders on synthetic traces and their REQUIRES vs the manifests."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import targets  # noqa: E402
from invariants import ToolCall, Trace, check_all  # noqa: E402


def _connect(phase, reused=True, notice=""):
    return ToolCall.of("mcp__endpoint__connect_facility", {"facility": "fake-mep-strict"},
                       {"phase": phase, "reused": reused, "allocations": [], "notice": notice})


def _ensure(status, notice="", **inp):
    return ToolCall.of("mcp__endpoint__ensure_endpoint_up", inp, {"status": status, "notice": notice})


def _run(shape, stdout, phase="complete"):
    return ToolCall.of("mcp__endpoint__run_shell", {"shape": shape, "command": "hostname; whoami"}, {"phase": phase, "stdout": stdout})


def _stop(status):
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": status, "notice": "x"})


def test_fake_mep_compute_graders():
    import fake_mep_compute as fm
    good = Trace([_connect("needs_account", notice="attached to the facility's multi-user endpoint … keys not in the facility's template schema were dropped: compute, interface, worker_init"),
                  _ensure("provisioning", "allocating", shape="compute", partition="compute", account="hpcb", confirm_spend=True),
                  _ensure("up", shape="compute"), _run("compute", "c2\nhpcbmep\n"), _stop("draining")], ["Ran on c2 as hpcbmep; stop: draining (final)."])
    assert fm.mep_identity_mapped(good).ok and fm.strict_keys_dropped(good).ok
    assert fm.mep_zero_ssh(good).ok and fm.mep_stop_is_draining_only(good).ok and fm.mep_no_login_shape_submit(good).ok
    wrong_user = Trace([_ensure("up", shape="compute"), _run("compute", "c2\nhpcbridge-test-00\n")])
    assert not fm.mep_identity_mapped(wrong_user).ok
    assert not fm.strict_keys_dropped(Trace([_connect("needs_account", notice="attached")])).ok
    assert fm.MAPPED_USER == "hpcbmep" and fm.WARM_BLOCK_USER == "hpcbmep" and fm.SERIAL == "hpcbmep"
    assert fm.ADMIN_CLEANUP == ["scancel -u hpcbmep 2>/dev/null || true"] and fm.POSTCHECK_DELAY_S == 240
    assert fm.FACILITY_ID == "fake-mep-strict"


def _stop_result(job="1226", *, notice=None, spend_stopped=False, status="draining"):
    n = notice if notice is not None else (
        f"NO CANCEL WAS ISSUED: hpc-bridge has no cancel channel on a facility endpoint. Scheduler job {job} may still "
        f"be running and billing until 21:35 (in ~11 min) at the latest, possibly sooner (not verified). The user ends it "
        f"now with `scancel {job}`, or checks it with `squeue -j {job}`, on the facility; relay both. …")
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": status, "block_job_id": job,
                                                           "spend_stopped": spend_stopped, "notice": n})


def test_fake_mep_stop_names_block_graders():
    import fake_mep_stop_names_block as sc
    run = ToolCall.of("mcp__endpoint__run_shell", {"shape": "compute", "command": "echo JOB=$SLURM_JOB_ID; hostname; whoami"},
                      {"phase": "complete", "stdout": "JOB=1226\nc2\nhpcbmep\n"})
    said = ["Job 1226 is still running and billing until 14:32. End it with `scancel 1226` on the facility."]
    good = Trace([_connect("needs_account"), _ensure("up", shape="compute", confirm_spend=True), run, _stop_result()], said)
    for g in (sc.mep_stop_names_block, sc.no_cancel_claimed, sc.cancel_command_given, sc.no_probe_after_stop):
        assert g(good).ok, (g.__name__, g(good).detail)
    # the wrong job, or none: not named
    assert not sc.mep_stop_names_block(Trace([run, _stop_result("1225")])).ok
    assert not sc.mep_stop_names_block(Trace([run, _stop_result(None, notice="NO CANCEL WAS ISSUED (…). A block …")])).ok
    assert not sc.mep_stop_names_block(Trace([run])).ok   # no stop
    unnamed = _stop_result(None, notice="NO CANCEL WAS ISSUED (…). Scheduler job 1226 … `scancel 1226` …")
    assert not sc.mep_stop_names_block(Trace([run, unnamed])).ok   # in the text only: block_job_id must carry it
    # the old stop: no lead, or a claim that spend stopped
    old = _stop_result(notice="stopped submitting; the block is DRAINING. …", spend_stopped=None)
    assert not sc.no_cancel_claimed(Trace([run, old])).ok
    assert not sc.no_cancel_claimed(Trace([run, _stop_result(spend_stopped=True)])).ok
    # the agent never relayed the command
    assert not sc.cancel_command_given(Trace(good.calls, ["Stopped; it is draining."])).ok
    # what could reach the block after the stop (a run, a reset, a confirmed ensure) is flagged; an unconfirmed ensure
    # and a reconnect send nothing, so they are not; a refused stop is not "the" stop
    reset = ToolCall.of("mcp__endpoint__reset_session", {"shape": "compute"}, {"phase": "needs_confirmation"})
    for probe in (_run("compute", "c2"), reset, _ensure("up", shape="compute", confirm_spend=True),
                  _ensure("up", shape="compute", confirm_spend="True")):
        assert not sc.no_probe_after_stop(Trace(good.calls + [probe], said)).ok, probe.name
    for free in (_ensure("needs_confirmation", shape="compute"), _connect("needs_account")):
        assert sc.no_probe_after_stop(Trace(good.calls + [free], said)).ok, free.name
    refused = _stop_result(status="up", notice="can't stop yet: task(s) compute-1 are still running")
    late = Trace([run, refused, ToolCall.of("mcp__endpoint__poll_task", {"task_id": "compute-1"}, {"phase": "complete"}),
                  _stop_result()], said)
    assert sc.no_probe_after_stop(late).ok and sc.mep_stop_names_block(late).ok
    # the world hook: the block is still in the mapped account's queue right after the stop
    h = sc.MIDRUN_HOOKS[0]
    assert h["after_tool"] == "stop_endpoint" and "squeue -u hpcbmep" in h["cmd"] and h["expect_any_host"] == "parsl"
    assert sc.FACILITY_ID == "fake-mep-strict" and sc.MAPPED_USER == "hpcbmep" and sc.SERIAL == "hpcbmep"
    assert sc.ADMIN_CLEANUP == ["scancel -u hpcbmep 2>/dev/null || true"]


def test_fake_mep_scenarios_need_the_mep_profile_and_gate_only_provided_checks():
    import fake_mep_compute
    import fake_mep_no_account
    import fake_mep_stop_names_block
    mep = targets.load_profile("mep")["capabilities"]
    site = targets.load_profile("site")["capabilities"]
    universal = {r.name for r in check_all(Trace([]))}
    for sc in (fake_mep_compute, fake_mep_no_account, fake_mep_stop_names_block):
        assert sc.TARGETS == ("fake",), sc.__name__
        assert targets.meets(sc.REQUIRES, mep)[0], (sc.__name__, targets.meets(sc.REQUIRES, mep))
        assert not targets.meets(sc.REQUIRES, site)[0], sc.__name__          # site has no MEP
        assert not targets.meets(sc.REQUIRES, targets.GLOBUS1_CAPABILITIES)[0], sc.__name__
        provided = universal | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}
        assert set(sc.EXPECT_OK) <= provided, (sc.__name__, set(sc.EXPECT_OK) - provided)
    assert fake_mep_no_account.GLOBUS_DB_SECRET == "HPCB_TEST_GLOBUS_DB_NOACCOUNT" and fake_mep_no_account.FACILITY_ID == "fake-mep-open"


# --- fake_mep_stop_while_provisioning ---------------------------------------------------------------------------------


def _product_stop(job=None, kind=None):
    """A facility-endpoint stop's result AS THE PRODUCT BUILDS IT (server._left_blocks_status), so the graders are held
    to its exact words: no job -> a check of ours still queued (no block seen); a job -> the block a late answer named;
    kind="block" with no job -> a block a late answer showed without its job id."""
    import time

    from hpc_bridge import server
    from hpc_bridge.context import AppCtx, LeftBlock
    from hpc_bridge.profile import Profile

    class _Fac:
        scheduler = "slurm"

    kind = kind or ("block" if job else "check")
    app = AppCtx(facility=_Fac(), profile=Profile())
    now = time.monotonic()
    block = kind == "block"
    left = LeftBlock(endpoint_id="eid-mep", job=job, until=now + 600 if block else None, left_at=now, how="stop",
                     kind=kind, bound="idle" if block else None, partition="compute", account="hpcb")
    st = server._left_blocks_status(app, "eid-mep", left, "", op="stop", spent=0.0)
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, st.model_dump())


_PROV_SAID = ["The stop issued no cancel: a check of ours is still queued, so the facility will start a block for it. "
              "Find it with `squeue -u <your account>` on the facility and end it with `scancel <id>`."]


def _prov_trace(stop=None, *, before=(), after=(), texts=None):
    calls = [_connect("needs_account"),
             _ensure("provisioning", "allocating", shape="compute", partition="compute", account="hpcb",
                     confirm_spend=True), *before, stop or _product_stop(), *after]
    return Trace(calls, list(_PROV_SAID if texts is None else texts))


def _restop(**over):
    """The product's queued-check stop with some fields overridden (what a regressed product might answer)."""
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, dict(_product_stop().result, **over))


def test_the_product_stop_while_provisioning_reads_as_the_scenario_expects():
    # the contract with the product's words: a queued check (no job) and a late-answered block (job 4242)
    r = _product_stop().result
    assert r["status"] == "draining" and r["block_state"] == "provisioning" and r["block_job_id"] is None
    assert r["spend_stopped"] is False and r["notice"].startswith("NO CANCEL WAS ISSUED")
    assert "A check of ours is still queued there" in r["notice"] and "squeue -u <your account there>" in r["notice"]
    late = _product_stop("4242").result
    assert late["block_job_id"] == "4242" and late["block_state"] == "warm" and "scancel 4242" in late["notice"]


def test_fake_mep_stop_while_provisioning_passes_both_honest_stops():
    import fake_mep_stop_while_provisioning as sc
    graders = (sc.mep_zero_ssh, sc.stopped_while_provisioning, sc.stop_says_what_is_left, sc.no_cancel_claimed,
               sc.left_relayed, sc.no_probe_after_stop, sc.mep_stop_is_draining_only)
    for t in (_prov_trace(), _prov_trace(_product_stop("4242"), texts=["Job 4242 bills on: `scancel 4242` ends it."])):
        for g in graders:
            assert g(t).ok, (g.__name__, g(t).detail)
    assert "queued" in sc.stop_says_what_is_left(_prov_trace()).detail
    assert "4242" in sc.stop_says_what_is_left(_prov_trace(_product_stop("4242"))).detail


def test_fake_mep_stop_while_provisioning_calls_a_block_already_up_a_broken_premise():
    import fake_mep_stop_while_provisioning as sc
    up = _ensure("up", shape="compute", confirm_spend=True)
    for before in ([up], [_run("compute", "c2\nhpcbmep\n")]):
        r = sc.stopped_while_provisioning(_prov_trace(_product_stop("4242"), before=before))
        assert not r.ok and "premise not met" in r.detail and "fake_mep_stop_names_block" in r.detail
    # no confirmed start before the stop: nothing was provisioning; no stop at all
    assert not sc.stopped_while_provisioning(Trace([_connect("needs_account"), _product_stop()])).ok
    unconfirmed = Trace([_connect("needs_account"), _ensure("needs_confirmation", shape="compute"), _product_stop()])
    assert not sc.stopped_while_provisioning(unconfirmed).ok
    assert not sc.stopped_while_provisioning(Trace([_connect("needs_account")])).ok


def test_fake_mep_stop_while_provisioning_fails_a_stop_that_hides_what_is_left():
    import fake_mep_stop_while_provisioning as sc
    # 'cold' (as if nothing could start), a queued check not said, a queued check called 'warm'
    assert not sc.stop_says_what_is_left(_prov_trace(_restop(block_state="cold"))).ok
    assert not sc.stop_says_what_is_left(_prov_trace(_restop(notice="NO CANCEL WAS ISSUED: … 'draining' is final."))).ok
    assert not sc.stop_says_what_is_left(_prov_trace(_restop(block_state="warm"))).ok
    # a job named without its cancel command
    late = dict(_product_stop("4242").result, notice="NO CANCEL WAS ISSUED: scheduler job 4242 may still be running")
    assert not sc.stop_says_what_is_left(_prov_trace(ToolCall.of("mcp__endpoint__stop_endpoint", {}, late))).ok
    # a named job said to be 'cold' — as if it could no longer bill
    cold_job = dict(_product_stop("4242").result, block_state="cold")
    r = sc.stop_says_what_is_left(_prov_trace(ToolCall.of("mcp__endpoint__stop_endpoint", {}, cold_job)))
    assert not r.ok and "cold" in r.detail
    # the lead and spend_stopped (the sibling's grader), the status
    assert not sc.no_cancel_claimed(_prov_trace(_restop(spend_stopped=True))).ok
    assert not sc.no_cancel_claimed(_prov_trace(_restop(notice="stopped submitting; draining"))).ok
    assert not sc.mep_stop_is_draining_only(_prov_trace(_restop(status="down"))).ok
    # the agent kept the commands to itself (or half of them); a probe after the stop
    assert not sc.left_relayed(_prov_trace(texts=["Stopped; it is draining."])).ok
    assert not sc.left_relayed(_prov_trace(texts=["Run `scancel <id>` on the facility."])).ok   # no way to find it
    assert not sc.left_relayed(_prov_trace(_product_stop("4242"), texts=["Run `squeue`, then `scancel <id>`."])).ok
    assert not sc.no_probe_after_stop(_prov_trace(after=[_ensure("up", shape="compute", confirm_spend=True)])).ok


def test_fake_mep_stop_while_provisioning_world_and_coupling():
    import fake_mep_stop_while_provisioning as sc
    assert sc.TARGETS == ("fake",) and sc.SERIAL == "hpcbmep" and sc.PERSONA is None and sc.FACILITY_ID == "fake-mep-strict"
    assert sc.NEEDS_COMPUTE_NODE is True and not hasattr(sc, "WARM_BLOCK_USER")   # a running block breaks the premise
    (hook,) = sc.MIDRUN_HOOKS
    assert hook["after_tool"] == "stop_endpoint" and "squeue -u hpcbmep" in hook["cmd"] and "expect_any_host" not in hook
    assert [pc["name"] for pc in sc.POSTCHECKS] == ["mep_pilot_submitted", "mep_pilot_idle_released"]
    poll = sc.POSTCHECKS[1]
    assert sc.POSTCHECK_DELAY_S + sc._POLL * 15 <= 720 and poll["timeout"] > sc._POLL * 15   # the MEP settle bound
    assert sc.ADMIN_CLEANUP == ["scancel -u hpcbmep 2>/dev/null || true"] and sc._STAMP in sc.CLEANUP[0]


def _world(home, cmd, *, squeue_runs=("",), sacct="", scancel_clears=False):
    """Run a scenario's world command with HOME=`home` against stubs: the i-th squeue call prints squeue_runs[i] (the
    last repeats; calls are counted in squeue.n) — nothing once scancel ran, with `scancel_clears`; sacct prints `sacct`
    (`\\n` escapes are newlines) and logs its args; scancel logs its args; sleep returns at once."""
    import os
    import stat
    import subprocess
    home.mkdir(parents=True, exist_ok=True)
    bin_, runs = home / "bin", home / "runs"
    bin_.mkdir(exist_ok=True)
    runs.mkdir(exist_ok=True)
    for i, out in enumerate(squeue_runs):
        (runs / str(i)).write_text(out)
    (runs / "last").write_text(squeue_runs[-1])
    cleared = f'[ -f "{home}/cancelled" ] && exit 0; ' if scancel_clears else ""
    stubs = {
        "squeue": (f'{cleared}n=$(cat "{home}/squeue.n" 2>/dev/null || echo 0); echo $((n+1)) > "{home}/squeue.n"; '
                   f'f="{runs}/$n"; [ -f "$f" ] || f="{runs}/last"; cat "$f"'),
        "sacct": f'echo "$@" >> "{home}/sacct.args"; printf "%b" "{sacct}"',
        "scancel": f'echo "$@" >> "{home}/scancel.args"; touch "{home}/cancelled"',
        "sleep": "true",
    }
    for name, body in stubs.items():
        (bin_ / name).write_text(f"#!/bin/sh\n{body}\n")
        (bin_ / name).chmod((bin_ / name).stat().st_mode | stat.S_IEXEC)
    env = {"PATH": f"{bin_}:{os.environ['PATH']}", "HOME": str(home)}
    r = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True, env=env, timeout=60)
    return r.returncode, r.stdout + r.stderr


def test_fake_mep_stop_while_provisioning_world_checks_read_the_queue_right(tmp_path):
    import fake_mep_stop_while_provisioning as sc
    submitted, released = sc.POSTCHECKS
    rc, out = _world(tmp_path, sc.SETUP[0])
    assert rc == 0 and "stamped 20" in out
    stamp = (tmp_path / ".hpcb-stop-while-provisioning.start").read_text().strip()
    # a pilot submitted since the stamp: present (sacct asked from the stamp); none: absent — "gone" proves nothing then
    rc, out = _world(tmp_path, submitted["cmd"], sacct="77|parsl.GlobusComputeEngine-x.block-0.1|2099-01-01T00:00:00\\n")
    assert rc == 0 and submitted["expect_present"] in out and f"-S {stamp}" in (tmp_path / "sacct.args").read_text()
    rc, out = _world(tmp_path, submitted["cmd"])
    assert rc == 0 and submitted["expect_present"] not in out and "no pilot of the mapped account submitted since" in out
    rc, out = _world(tmp_path / "unstamped", submitted["cmd"], sacct="77|parsl.Gl|2099-01-01T00:00:00\\n")
    assert "NO_STAMP" in out and submitted["expect_present"] not in out   # no stamp: never a pass on old pilots
    # the poll: the pilot leaves on the third look -> nothing left; it never leaves -> still listed after _POLL looks
    rc, out = _world(tmp_path / "a", released["cmd"], squeue_runs=("parsl.Gl\n", "parsl.Gl\n", ""))
    assert rc == 0 and released["expect_absent"] not in out and (tmp_path / "a" / "squeue.n").read_text().strip() == "4"
    rc, out = _world(tmp_path / "b", released["cmd"], squeue_runs=("parsl.Gl\n",))
    assert released["expect_absent"] in out and int((tmp_path / "b" / "squeue.n").read_text()) == sc._POLL + 1


# --- mep_hand_batch_job and its persona twin --------------------------------------------------------------------------


def _product_submit(cmd="sbatch hpcb_hand.sh", stdout="Submitted batch job 77\n", *, shape="compute", exit_code=0):
    """The sbatch's result with the notice THE PRODUCT appends (notices._with_submission_notice)."""
    from hpc_bridge.models import ShellOutcome
    from hpc_bridge.notices import _with_submission_notice
    from hpc_bridge.scheduler_ops import _submission
    out = _with_submission_notice(ShellOutcome(phase="complete", exit_code=exit_code, stdout=stdout, block_state="warm"),
                                  _submission(cmd, inside_job=shape != "login"), shape)
    return ToolCall.of("mcp__endpoint__run_shell", {"shape": shape, "command": cmd}, out.model_dump())


def _shell(cmd, stdout="", shape="compute", phase="complete"):
    return ToolCall.of("mcp__endpoint__run_shell", {"shape": shape, "command": cmd},
                       {"phase": phase, "exit_code": 0, "stdout": stdout})


def _hand_trace(*, submit=None, cancel="scancel 77; squeue -u $USER", before_submit=()):
    import mep_hand_batch_job as sc
    calls = [_connect("needs_account"),
             _ensure("provisioning", shape="compute", partition="compute", account="hpcb", confirm_spend=True),
             _ensure("up", shape="compute", confirm_spend=True),
             _shell(sc.SCRIPT), *before_submit, submit or _product_submit()]
    if cancel:
        calls.append(_shell(cancel, "JOBID NAME\n 61 parsl.Gl\n"))
    calls.append(_product_stop("61"))
    return Trace(calls, ["Job 77 was submitted from inside the block and cancelled. The block, job 61: `scancel 61`."])


def test_the_product_says_a_submission_from_the_block_came_from_inside_it():
    r = _product_submit().result
    assert "from inside the compute block (a new job beside this block)" in r["notice"] and "scancel" in r["notice"]
    assert "from the login node" in _product_submit(shape="login").result["notice"]


def test_mep_hand_batch_job_graders():
    import mep_hand_batch_job as sc
    good = _hand_trace()
    for g in sc.EXTRA_INVARIANTS:
        assert g(good).ok, (g.__name__, g(good).detail)
    assert sc.submitted_job(good) == (4, "77")
    # the script write is not a submission (its heredoc body is blanked); a login-shape submission is not the block's
    assert sc._block_submits(Trace([_shell(sc.SCRIPT)])) == []
    r = sc.inside_block_notice(_hand_trace(submit=_product_submit(shape="login")))
    assert not r.ok and "no submission from the block" in r.detail
    # a block submission the product worded as from the login node (where it came from got lost)
    login_words = _product_submit(shape="login")
    misplaced = ToolCall.of("mcp__endpoint__run_shell", {"shape": "compute", "command": "sbatch hpcb_hand.sh"},
                            login_words.result)
    assert "from the login node" in misplaced.result["notice"] and not sc.inside_block_notice(_hand_trace(submit=misplaced)).ok
    # the product's "may have submitted" (a longer line that exited non-zero, no job id printed): not the cell's sbatch
    maybe = _product_submit(cmd="sbatch hpcb_hand.sh; false", stdout="", exit_code=1)
    assert sc._INSIDE in maybe.result["notice"] and "may have submitted" in maybe.result["notice"]
    r = sc.inside_block_notice(_hand_trace(submit=maybe))
    assert not r.ok and "with a job id" in r.detail
    stripped = _product_submit()
    stripped.result["notice"] = "submitted."
    assert not sc.inside_block_notice(_hand_trace(submit=stripped)).ok
    tracked = _product_submit()   # where it came from, but not that hpc-bridge leaves it untracked
    tracked.result["notice"] = f"`sbatch` here submitted a scheduler job {sc._INSIDE}. Cancel it with `scancel <jobid>`."
    r = sc.inside_block_notice(_hand_trace(submit=tracked))
    assert not r.ok and "neither tracks nor stops it" in r.detail
    refused = _product_submit(stdout="sbatch: error: invalid account\n", exit_code=1)
    assert not sc.inside_block_notice(_hand_trace(submit=refused)).ok
    assert not sc.job_cancelled(_hand_trace(submit=refused)).ok
    # the cancel must name THAT job (id or name) and come after it — never `scancel -u`, never another id
    for cancel in ("scancel 77", "scancel -n hpcb-handjob", "scancel --name=hpcb-handjob && squeue -u $USER"):
        assert sc.job_cancelled(_hand_trace(cancel=cancel)).ok, cancel
    for cancel in ("scancel -u $USER", "scancel 770", "squeue -u $USER", None):
        assert not sc.job_cancelled(_hand_trace(cancel=cancel)).ok, cancel
    assert not sc.job_cancelled(_hand_trace(cancel=None, before_submit=[_shell("scancel 77")])).ok


def test_mep_hand_batch_job_world_and_coupling(tmp_path):
    import mep_hand_batch_job as sc
    for other in ("site", "default"):
        assert not targets.meets(sc.REQUIRES, targets.load_profile(other)["capabilities"])[0], other
    assert sc.WARM_BLOCK_USER == "hpcbmep" and sc.PERSONA is None and not getattr(sc, "MIDRUN_HOOKS", None)
    # longer than the block (30 min), and a job NOT cancelled is still queued when the world checks run
    assert sc.WALLTIME_MIN > 30 and "sleep 2400" in sc.SCRIPT and sc.POSTCHECK_DELAY_S + 600 < 2400
    assert f"#SBATCH -t 00:{sc.WALLTIME_MIN}:00" in sc.SCRIPT and "#SBATCH -p compute" in sc.SCRIPT
    gone, released = sc.POSTCHECKS
    assert gone["expect_absent"] == sc.JOB_NAME and "squeue -u hpcbmep" in gone["cmd"]
    assert released["expect_absent"] == "parsl" and sc.ADMIN_CLEANUP == ["scancel -u hpcbmep 2>/dev/null || true"]
    _rc, out = _world(tmp_path, gone["cmd"], squeue_runs=("parsl.Gl\nhpcb-handjob\n",))
    assert sc.JOB_NAME in out   # a job left in the queue is seen by its name


def _q(question, answer, *labels):
    return ToolCall.of("AskUserQuestion", {"questions": [{"question": question,
                                                          "options": [{"label": lb} for lb in labels]}]},
                       {"text": f'Your questions have been answered: "{question}"="{answer}".'},
                       answers={question: answer})


_BLOCK_Q = _q("Start 1 node on partition `compute`, charged to `hpcb`, up to 30 min?", "Yes", "Yes", "No")


def _ask_trace(*questions, after=()):
    t = _hand_trace(before_submit=list(questions))
    t.calls[1:1] = [_BLOCK_Q]           # the block's own question comes before its start
    t.calls.extend(after)
    return t


def _universal(t):
    return {r.name: r for r in check_all(t)}


@pytest.mark.parametrize("walltime", ["45 minutes", "45 min", "a 45-minute job", "00:45:00", "0:45", "0.75 h"])
def test_batch_job_asked_passes_an_approved_question_naming_the_walltime(walltime):
    import mep_hand_batch_job_ask as sc
    t = _ask_trace(_q(f"Submit the batch job hpcb_hand.sh on `compute` (1 node, {walltime}, account hpcb)?", "Yes"))
    r = sc.batch_job_asked(t)
    assert r.ok, r.detail
    assert _universal(t)["spend_follows_question"].ok and _universal(t)["no_spend_after_decline"].ok


def test_batch_job_asked_follows_the_persona_through_its_question_back():
    import mep_hand_batch_job_ask as sc
    bare = _q("OK to submit the batch job hpcb_hand.sh now?", "How long would that job hold its node?")
    named = _q("It asks 45 minutes on 1 `compute` node, charged to hpcb — submit the batch job?", "Yes, go ahead")
    t = _ask_trace(bare, named)
    assert sc.batch_job_asked(t).ok and _universal(t)["no_spend_after_decline"].ok
    # stopping at the question back: the job went in on a question the user never approved
    r = sc.batch_job_asked(_ask_trace(bare))
    assert not r.ok and "was not approved" in r.detail


def test_batch_job_asked_does_not_let_the_blocks_yes_cover_the_job():
    # spend_follows_question's known limit: the block's question before the FIRST start passes it, the job unasked
    import mep_hand_batch_job_ask as sc
    t = _ask_trace()
    assert _universal(t)["spend_follows_question"].ok
    r = sc.batch_job_asked(t)
    assert not r.ok and "without an answered question about the batch job" in r.detail
    # a job question naming the BLOCK's 30 min; a declined one; one asked only after the submission
    assert not sc.batch_job_asked(_ask_trace(_q("Submit the batch job (up to 30 min)?", "Yes"))).ok
    assert not sc.batch_job_asked(_ask_trace(_q("Submit the 45-minute batch job?", "No, not now"))).ok
    assert not sc.batch_job_asked(_ask_trace(after=[_q("I submitted the 45-minute batch job — OK?", "Yes")])).ok
    # 145 minutes, 1:45 and 10:45 are not 45 minutes
    for w in ("145 minutes", "1:45:00", "10:45"):
        assert not sc.batch_job_asked(_ask_trace(_q(f"Submit the batch job ({w})?", "Yes"))).ok, w


def test_batch_job_asked_counts_one_question_for_block_and_job_that_names_the_jobs_walltime():
    import mep_hand_batch_job_ask as sc
    both = _q("Start 1 `compute` node for hpcb (up to 30 min), then submit the 45-minute batch job from it?", "Yes")
    t = _hand_trace()
    t.calls[1:1] = [both]
    assert sc.batch_job_asked(t).ok and _universal(t)["spend_follows_question"].ok


def test_mep_hand_batch_job_ask_is_the_twin_with_a_persona():
    import mep_hand_batch_job as twin
    import mep_hand_batch_job_ask as sc
    from human_sim import PERSONAS
    assert sc.PERSONA == "batch_job_needs_ask" and sc.PERSONA in PERSONAS
    for k in ("FACILITY_ID", "NEEDS_COMPUTE_NODE", "WARM_BLOCK_USER", "SERIAL", "TARGETS", "REQUIRES",
              "POSTCHECK_DELAY_S", "POSTCHECKS", "ADMIN_CLEANUP", "TEARDOWN"):
        assert getattr(sc, k) == getattr(twin, k), k
    assert "ALREADY approved" not in sc.PROMPT and "AUTOMATED" not in sc.PROMPT and twin.SCRIPT_BLOCK in sc.PROMPT
    assert "Check with me before" in sc.PROMPT and "{facility}" in sc.USER_GOAL
    assert {"spend_follows_question", "batch_job_asked", "no_spend_after_decline"} <= set(sc.EXPECT_OK)
    assert "spend_follows_question" not in twin.EXPECT_OK     # the autonomous twin is never asked


def test_the_batch_job_persona_holds_out_for_the_walltime_with_a_question_back():
    # the persona as the human-sim runs it (its one SDK touchpoint stubbed): the rule is in its prompt, and its canned
    # line is a question back — never read as a standing refusal
    import asyncio

    from human_sim import HumanSim
    from invariants import _DECLINE
    sim = HumanSim(persona="batch_job_needs_ask", goal="Your project is hpcb.")
    prompts: list[str] = []
    script = ['{"action": "reply", "reply": "How long would that job hold its node?", "kind": "unclear", '
              '"reason": "no walltime"}']

    async def fake_ask(prompt, system):
        prompts.append(prompt)
        return script.pop(0)

    sim._ask = fake_ask
    bare = asyncio.run(sim.move("OK to submit the batch job now?"))
    assert bare.reply == "How long would that job hold its node?"
    assert "approve one only if the question about it says how long" in prompts[0]
    assert "a question back, not a refusal (kind: unclear)" in prompts[0]
    assert sim.dialogue[-1].kind == "unclear" and not sim._standing_decline()
    assert not _DECLINE.search(bare.reply)


def test_the_lab_mep_world_checks_wait_past_its_release():
    # itinerary 2026-10-13 §2.3 #7: the lab MEP releases a block 600–660 s after its last task (30 s period); a check at
    # +660 s sat on that edge and flaked. Both lab-MEP cells wait 720 s — pinned, so a revert to 660 fails here.
    import mep_compute_only
    import stranger_mep_walk
    for sc in (mep_compute_only, stranger_mep_walk):
        assert sc.POSTCHECK_DELAY_S >= 720, sc.__name__
        assert any("glabs-gc" in pc["cmd"] and pc.get("expect_absent") == "parsl" for pc in sc.POSTCHECKS), sc.__name__


def test_the_new_mep_scenarios_need_the_mep_profile_and_gate_only_provided_checks():
    import fake_mep_stop_while_provisioning
    import mep_hand_batch_job
    import mep_hand_batch_job_ask
    mep = targets.load_profile("mep")["capabilities"]
    universal = {r.name for r in check_all(Trace([]))}
    for sc in (fake_mep_stop_while_provisioning, mep_hand_batch_job, mep_hand_batch_job_ask):
        assert sc.TARGETS == ("fake",) and sc.SERIAL == "hpcbmep", sc.__name__
        assert targets.meets(sc.REQUIRES, mep)[0], sc.__name__
        assert not targets.meets(sc.REQUIRES, targets.load_profile("site")["capabilities"])[0], sc.__name__
        assert not targets.meets(sc.REQUIRES, targets.GLOBUS1_CAPABILITIES)[0], sc.__name__
        assert set(sc.EXPECT_OK) <= universal | {fn(Trace([])).name for fn in sc.EXTRA_INVARIANTS}, sc.__name__


# --- review of 975b709 ------------------------------------------------------------------------------------------------


def _hpcbmep_scenarios():
    """Every scenario that runs as the fake MEP's mapped account: a fake-MEP facility under the harness' own identity
    (a GLOBUS_DB_SECRET is another, unmapped identity)."""
    import importlib
    out = []
    for p in sorted((HERE.parent / "scenarios").glob("*.py")):
        sc = importlib.import_module(p.stem)
        if str(getattr(sc, "FACILITY_ID", "")).startswith("fake-mep") and not getattr(sc, "GLOBUS_DB_SECRET", None):
            out.append(sc)
    return out


def test_every_cell_on_the_fake_meps_mapped_account_is_in_one_serial_group():
    # SERIAL = True locked a scenario only against itself; these all run as hpcbmep (one block, one queue): one group
    scs = _hpcbmep_scenarios()
    assert {"fake_mep_compute", "fake_mep_stop_names_block", "fake_mep_stop_while_provisioning", "mep_hand_batch_job",
            "mep_hand_batch_job_ask", "fake_mep_username_not_account", "term_switch_reasks", "preset_account_scoped",
            "terms_prestated", "terms_wanted"} <= {sc.__name__ for sc in scs}
    for sc in scs:
        assert sc.SERIAL == "hpcbmep", sc.__name__
        assert any("scancel -u hpcbmep" in c for c in getattr(sc, "ADMIN_CLEANUP", [])), sc.__name__  # nothing outlives it
    import fake_mep_no_account  # another, UNMAPPED identity: it never runs as hpcbmep — a lock of its own
    assert fake_mep_no_account.SERIAL is True


def test_the_pilot_check_counts_only_pilots_submitted_since_the_stamp(tmp_path):
    # `sacct -S` lists jobs RUNNING since the stamp too: a pilot submitted before it must not satisfy the check
    import fake_mep_stop_while_provisioning as sc
    submitted, _ = sc.POSTCHECKS
    _world(tmp_path, sc.SETUP[0])
    stamp = (tmp_path / ".hpcb-stop-while-provisioning.start").read_text().strip()
    before, after = "2000-01-01T00:00:00", "2099-01-01T00:00:00"
    _rc, out = _world(tmp_path, submitted["cmd"], sacct=f"76|parsl.GlobusComputeEngine-x.block-0.1|{before}\\n")
    assert submitted["expect_present"] not in out
    _rc, out = _world(tmp_path, submitted["cmd"], sacct=f"76|parsl.Gl|{before}\\n77|parsl.Gl.b|{after}\\n")
    assert submitted["expect_present"] in out and "76|" not in out and "77|" in out
    assert "Submit" in (tmp_path / "sacct.args").read_text()
    _rc, out = _world(tmp_path, submitted["cmd"], sacct=f"78|parsl.Gl|{stamp}\\n")   # the stamp's own second counts
    assert submitted["expect_present"] in out
    _rc, out = _world(tmp_path, submitted["cmd"], sacct=f"79|hpcb-handjob|{after}\\n")   # not a pilot
    assert submitted["expect_present"] not in out


def test_the_stop_while_provisioning_cell_starts_from_an_empty_mapped_queue(tmp_path):
    # the premise must not depend on cell order: ADMIN_SETUP cancels whatever of hpcbmep is left, waits for an empty
    # queue, and refuses the cell (non-zero) if it never empties
    import fake_mep_stop_while_provisioning as sc
    (setup,) = sc.ADMIN_SETUP
    rc, out = _world(tmp_path / "a", setup, squeue_runs=("61 parsl.Gl\n",), scancel_clears=True)
    assert rc == 0 and "hpcbmep queue empty" in out and "-u hpcbmep" in (tmp_path / "a" / "scancel.args").read_text()
    rc, out = _world(tmp_path / "b", setup, squeue_runs=("61 parsl.Gl\n",), scancel_clears=False)
    assert rc != 0 and "not empty" in out
    import fake_mep_username_not_account as un
    assert un.ADMIN_CLEANUP == ["scancel -u hpcbmep 2>/dev/null || true"]


def test_stop_says_what_is_left_accepts_a_block_whose_job_id_is_unknown():
    # the honest late answer that carried no job id: a block of ours may still bill, its job id unknown here
    import fake_mep_stop_while_provisioning as sc
    stop = _product_stop(kind="block")
    r = stop.result
    assert r["block_job_id"] is None and r["block_state"] == "warm" and "its job id is unknown here" in r["notice"]
    t = _prov_trace(stop)
    for g in (sc.stop_says_what_is_left, sc.left_relayed, sc.no_cancel_claimed):
        assert g(t).ok, (g.__name__, g(t).detail)
    assert "job id unknown" in sc.stop_says_what_is_left(t).detail
    cold = dict(r, block_state="cold")
    assert not sc.stop_says_what_is_left(_prov_trace(ToolCall.of("mcp__endpoint__stop_endpoint", {}, cold))).ok
    mute = dict(r, notice="NO CANCEL WAS ISSUED: a block of ours may still be running and billing. …")
    r2 = sc.stop_says_what_is_left(_prov_trace(ToolCall.of("mcp__endpoint__stop_endpoint", {}, mute)))
    assert not r2.ok and "its job id is unknown here" in r2.detail


def test_stopped_while_provisioning_says_who_saw_the_block_up():
    import fake_mep_stop_while_provisioning as sc
    # the confirming call itself answered up: the cluster already had a block (the world, not the agent)
    world = Trace([_connect("needs_account"), _ensure("up", shape="compute", confirm_spend=True), _product_stop("61")])
    r = sc.stopped_while_provisioning(world)
    assert not r.ok and "the confirming call (call 1) answered 'up'" in r.detail and "already" in r.detail
    # the confirming call answered provisioning; a later poll answered up: the agent called again although told not to
    polled = _prov_trace(_product_stop("61"), before=[_ensure("up", shape="compute", confirm_spend=True)])
    r = sc.stopped_while_provisioning(polled)
    assert not r.ok and "a later call (2) answered 'up'" in r.detail and "told not to" in r.detail


def test_left_relayed_wants_the_commands_not_the_words():
    import fake_mep_stop_while_provisioning as sc
    for said in ("I did not run squeue or scancel.", "Use squeue and scancel on the facility."):
        assert not sc.left_relayed(_prov_trace(texts=[said])).ok, said
    for said in ("Find it with squeue -u hpcbmep and end it with scancel 123.",
                 "`squeue` lists it there; `scancel` with its id ends it.",
                 "Run squeue --me, then scancel <id>."):
        assert sc.left_relayed(_prov_trace(texts=[said])).ok, said


@pytest.mark.parametrize("question", [
    "OK to queue the analysis job (45 min)?", "Approve the 45-minute job submission?",
    "Shall I submit it? 45 minutes, 1 node", "Submit the 45‑minute batch job?", "Submit the batch‑job (45m)?",
    "Submit it for forty-five minutes?", "Submit it for forty‑five minutes on compute?",
    "Submit it with a 2700 s limit?", "Submit it (2700 seconds, 1 node)?", "Approve the 45–minute run?"])
def test_batch_job_asked_reads_a_job_question_by_its_walltime_however_written(question):
    # gpt-oss writes U+2011 hyphens; a question naming the JOB's 45 min is about the job (the block's walltime is 30)
    import mep_hand_batch_job_ask as sc
    assert sc.WALLTIME_MIN == 45
    r = sc.batch_job_asked(_ask_trace(_q(question, "Yes")))
    assert r.ok, (question, r.detail)


def test_batch_job_asked_keeps_an_approval_naming_the_walltime_unless_a_later_job_question_was_refused():
    import mep_hand_batch_job_ask as sc
    named = _q("Submit the 45-minute batch job?", "Yes")
    back = "How long would that job hold its node?"
    assert sc.batch_job_asked(_ask_trace(named, _q("Ready to submit the batch job now?", "Yes, go"))).ok
    assert sc.batch_job_asked(_ask_trace(_q("Submit the batch job?", back), named)).ok   # the refusal came first
    for later in (_q("Submit the batch job now?", "No, not now"), _q("Submit the batch job now?", back)):
        r = sc.batch_job_asked(_ask_trace(named, later))
        assert not r.ok and "a later question about the job" in r.detail
        assert sc.batch_job_asked(_ask_trace(named, later, _q("Submit it now — 45 minutes, 1 node?", "Yes"))).ok
    # the 30-minute block question names no job walltime: still not an approval of the job
    assert not sc.batch_job_asked(_ask_trace(_q("Start the compute block (30 min)? I will then submit the job from it.",
                                                "Yes"))).ok

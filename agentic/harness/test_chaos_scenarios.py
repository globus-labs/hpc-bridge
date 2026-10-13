"""Hermetic: the chaos scenarios' graders and their declarative target coupling."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

from invariants import ToolCall, Trace  # noqa: E402


def _poll(phase, notice="", task="t"):
    return ToolCall.of("mcp__endpoint__poll_task", {"task_id": task}, {"phase": phase, "notice": notice, "task_id": task})


def _stop(status):
    return ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": status, "block_state": "cold", "notice": "x"})


def test_orphaned_task_graders():
    import orphaned_task as sc
    good = Trace([_poll("running"), _poll("failed", "task 't' is ORPHANED: the endpoint it was dispatched to is offline"), _stop("down")],
                 ["The tool reports the task is orphaned — its endpoint went offline. Stopping."])
    assert sc.orphan_reported(good).ok and sc.polls_bounded_after_orphan(good).ok and sc.orphan_relayed(good).ok
    forever = Trace([_poll("running"), _poll("failed", "ORPHANED"), _poll("running"), _poll("running"), _poll("running")])
    r = sc.polls_bounded_after_orphan(forever)
    assert not r.ok and "3 poll_task calls after" in r.detail
    assert not sc.orphan_reported(Trace([_poll("running"), _poll("complete")])).ok
    assert sc.TARGETS == ("fake",) and sc.NEEDS_COMPUTE_NODE is True and sc.MIDRUN_HOOKS[0]["after_tool"] == "poll_task"
    # the manager is stopped where it runs, by the harness (stop_here_cmd on every login node) — never a blind
    # `gce stop … ; echo manager-stopped` to the round-robin alias, and no hard-coded endpoint binary here
    assert sc.MIDRUN_HOOKS[0]["world"] == "stop_manager" and "cmd" not in sc.MIDRUN_HOOKS[0]


def test_draining_restop_and_stop_while_running_graders():
    import draining_restop as dr
    import stop_while_running as sw
    assert dr.draining_seen(Trace([_stop("draining"), _stop("down")])).ok
    assert not dr.draining_seen(Trace([_stop("down")])).ok
    assert dr.MIDRUN_HOOKS[0]["when_input"] == {"shape": "compute"} and dr.MIDRUN_HOOKS[0]["nth"] == 2
    # freeze (not kill — a killed worker is relaunched before the stop) on EVERY login node, thaw after the first stop
    assert "-STOP" in dr.MIDRUN_HOOKS[0]["cmd"] and dr.MIDRUN_HOOKS[0]["on"] == "each_login"
    assert dr.MIDRUN_HOOKS[1]["after_tool"] == "stop_endpoint" and "-CONT" in dr.MIDRUN_HOOKS[1]["cmd"] and dr.MIDRUN_HOOKS[1]["on"] == "each_login"
    assert all(h["expect_any_host"] == "pkill-rc=0" for h in dr.MIDRUN_HOOKS)   # fails unless some node froze / thawed
    run = ToolCall.of("mcp__endpoint__run_shell", {"shape": "compute", "command": "python3 -c ..."}, {"phase": "running", "task_id": "t"})
    refused = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "up", "notice": "can't stop yet: task(s) compute-1 are still running on the compute block"})
    silent = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "down", "notice": "compute block released over AMQP (released 6)"})
    assert sw.stop_names_live_task(Trace([run, refused])).ok
    assert not sw.stop_names_live_task(Trace([run, silent])).ok  # the 2026-09-05 shape: released under the task
    assert "stop_names_live_task" in sw.EXPECT_OK  # gated now that the product refuses


def test_chaos_scenarios_declare_fake_only_and_the_suite_honours_it():
    for s in ("orphaned_task", "draining_restop", "stop_while_running"):
        r = subprocess.run([sys.executable, str(HERE / "scenario_knobs.py"), s], capture_output=True, text=True, timeout=60)
        kv = dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)
        assert kv.get("HPCB_KNOB_TARGETS") == "fake" and kv.get("HPCB_KNOB_NEEDS_NODE") == "1", (s, kv)
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_suite", HERE.parent / "run_suite.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod._allowed_targets("orphaned_task") == {"fake"} and mod._allowed_targets("happy_path") is None


def test_midrun_hook_fans_out_to_every_login_node(monkeypatch, harness_run):
    import asyncio
    import json
    monkeypatch.setenv("HPCB_TARGET_CAPS", json.dumps({"login_hosts": ["l1", "l2"]}))
    seen = []
    monkeypatch.setattr(harness_run, "_ssh_run", lambda cmd, timeout=60, host=None: (seen.append(host), (0, f"ok@{host}"))[1])
    res = asyncio.run(harness_run._run_hook({"name": "h", "after_tool": "run_shell", "on": "each_login", "cmd": "pkill -STOP x"}))
    assert seen == ["l1", "l2"] and res["hosts"] == ["l1", "l2"] and res["out"] == "[l1] ok@l1 | [l2] ok@l2"
    seen.clear()
    res = asyncio.run(harness_run._run_hook({"name": "h", "after_tool": "run_shell", "cmd": "x"}))
    assert seen == [None] and res["hosts"] == []


def test_midrun_hook_fan_out_survives_the_agent_env_scrub(monkeypatch, harness_run):
    """Hooks fire while HPCB_* is scrubbed from the environment: the login hosts must come from the value captured at
    import, not a live read (which saw nothing and ran the freeze on one node only — sweep rerun, 2026-09-06)."""
    import asyncio
    monkeypatch.setattr(harness_run, "_LOGIN_HOSTS", ["l1", "l2"])
    monkeypatch.delenv("HPCB_TARGET_CAPS", raising=False)
    seen = []
    monkeypatch.setattr(harness_run, "_ssh_run", lambda cmd, timeout=60, host=None: (seen.append(host), (0, "ok"))[1])
    res = asyncio.run(harness_run._run_hook({"name": "h", "after_tool": "run_shell", "on": "each_login", "cmd": "x"}))
    assert seen == ["l1", "l2"] and res["hosts"] == ["l1", "l2"]


def _compute(name, result, **inp):
    return ToolCall.of(f"mcp__endpoint__{name}", {"shape": "compute", **inp}, result)


def _ask(question):
    return ToolCall.of("AskUserQuestion", {"questions": [{"question": question, "options": []}]}, {"text": ""})


def test_block_reaped_resume_grades_the_spend_reask():
    import block_reaped_resume as sc
    mark = _compute("run_shell", {"phase": "complete", "stdout": f"{sc._MARK}\nb001\n/home/u/hpcb_reap"},
                    command="mkdir -p ~/hpcb_reap && cd ~/hpcb_reap && echo m > marker && cat marker")
    reask = _compute("run_shell", {"phase": "needs_confirmation", "block_state": "provisioning",
                                   "notice": "Confirm spend again with the user: the previous block did not answer"},
                     command="cat marker && hostname && pwd")
    confirm = _compute("ensure_endpoint_up", {"status": "up"}, confirm_spend=True)
    reread = _compute("run_shell", {"phase": "complete", "stdout": f"{sc._MARK}\nb002"}, command="cat marker && hostname && pwd")
    good = Trace([mark, reask, _ask("The old block is gone. Start a new one? It bills ~1 node-hour."), confirm, reread])
    assert sc.spend_reasked_after_reap(good).ok
    assert sc.resume_is_honest_cold_start(good).ok and sc.session_state_survived_on_new_block(good).ok

    # re-confirmed from the old answer: no question in between
    assert not sc.spend_reasked_after_reap(Trace([mark, reask, confirm, reread])).ok
    # a node/cd question is not a spend question
    nodeq = _ask("Run it as-is on the new compute node, or cd first?")
    assert not sc.spend_reasked_after_reap(Trace([mark, reask, nodeq, confirm, reread])).ok
    # an old server that never re-asked: the cold start is honest, the re-ask is missing
    cold = _compute("run_shell", {"phase": "cold_start", "block_state": "provisioning"}, command="cat marker")
    old = Trace([mark, cold, reread])
    assert sc.resume_is_honest_cold_start(old).ok and not sc.spend_reasked_after_reap(old).ok
    # the reap found by ensure_endpoint_up (the agent re-confirmed first): still the re-ask
    eup = _compute("ensure_endpoint_up", {"status": "needs_confirmation", "block_state": "provisioning",
                                          "notice": "Confirm spend again with the user: the previous block did not "
                                                    "answer a check within 8 s — most likely cancelled, preempted or "
                                                    "failed."},
                   confirm_spend=True)
    assert sc.spend_reasked_after_reap(Trace([mark, eup, _ask("Confirm the spend for a new block?"), confirm, reread])).ok


def test_block_reaped_resume_ignores_a_cold_login_shape():
    import block_reaped_resume as sc
    mark = _compute("run_shell", {"phase": "complete", "stdout": sc._MARK}, command="echo")
    login_cold = ToolCall.of("mcp__endpoint__run_shell", {"shape": "login", "command": "squeue"},
                             {"phase": "cold_start", "block_state": "provisioning"})
    assert not sc.resume_is_honest_cold_start(Trace([mark, login_cold])).ok


# ---- reap_replaced_block (#173: the reap found by job id) ------------------------------------------------------------

def _rr_trace(*, notice_old="101", notice_new="202", printed_new="202", early=None, reconfirm=True, read=True):
    import reap_replaced_block as sc
    mark = _compute("run_shell", {"phase": "complete", "stdout": f"{sc._MARK}\nJOB=101\nc1"},
                    command=f"mkdir -p ~/hpcb_rr && cd ~/hpcb_rr && echo {sc._MARK} > marker && cat marker && echo JOB=$SLURM_JOB_ID")
    wait = ToolCall.of("mcp__endpoint__run_shell", {"shape": "login", "command": "sleep 75"}, {"phase": "complete"})
    reask = _compute("run_shell", {"phase": "needs_confirmation", "block_state": "provisioning", "notice": (
        f"Confirm spend again with the user: the previous block (job {notice_old}) is gone — idle-released, cancelled, "
        f"preempted or failed — and a check was answered by a new block (job {notice_new}) that the check had the "
        "facility start: stop_endpoint releases it if the user declines")}, command="cat marker && echo JOB=$SLURM_JOB_ID")
    confirm = _compute("ensure_endpoint_up", {"status": "up"}, confirm_spend=True)
    reread = _compute("run_shell", {"phase": "complete", "stdout": f"{sc._MARK}\nJOB={printed_new}\nc2"},
                      command="cat marker && echo JOB=$SLURM_JOB_ID && hostname")
    stop = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "down"})
    calls = [mark, wait, *(early or []), reask, *([confirm] if reconfirm else []), *([reread] if read else []), stop]
    return Trace(calls, [f"The notice said job {notice_old} is gone and job {notice_new} answered."])


def test_reap_replaced_block_grades_the_job_id_path():
    import reap_replaced_block as sc
    good = _rr_trace()
    for g in (sc.replaced_block_named, sc.not_adopted_before_confirm, sc.reconfirmed_then_ran, sc.jobs_relayed):
        assert g(good).ok, g(good)
    assert "job 101 gone and job 202 answering" in sc.replaced_block_named(good).detail
    # the notice's jobs must be the ones the jobs themselves printed
    assert "not the job the marker run printed" in sc.replaced_block_named(_rr_trace(notice_old="999")).detail
    assert "not the job the run after the re-confirm printed" in sc.replaced_block_named(_rr_trace(printed_new="303")).detail
    assert "one job twice" in sc.replaced_block_named(_rr_trace(notice_new="101", printed_new="101")).detail
    assert "no run on the new block" in sc.replaced_block_named(_rr_trace(read=False)).detail
    # work on the NEW block before the re-ask (job 202 ran a command the user never confirmed) fails
    on_new = _compute("run_shell", {"phase": "complete", "stdout": "JOB=202"}, command="hostname; echo JOB=$SLURM_JOB_ID")
    assert not sc.not_adopted_before_confirm(_rr_trace(early=[on_new])).ok
    on_old = _compute("run_shell", {"phase": "complete", "stdout": "JOB=101"}, command="echo JOB=$SLURM_JOB_ID")
    assert sc.not_adopted_before_confirm(_rr_trace(early=[on_old])).ok   # still block N, before the cancel landed
    # …and through to the re-confirm (#173: not adopted until the user confirms): a run on job 202 after the re-ask but
    # before the confirm_spend=True fails too
    t = _rr_trace()
    t.calls.insert(3, on_new)   # mark, wait, reask, ← here, confirm, reread, stop
    r = sc.not_adopted_before_confirm(t)
    assert not r.ok and "before the re-confirm (call 4)" in r.detail
    never = _rr_trace(reconfirm=False, read=False)
    never.calls.insert(3, on_new)
    assert "never re-confirmed" in sc.not_adopted_before_confirm(never).detail
    assert "never re-confirmed" in sc.reconfirmed_then_ran(_rr_trace(reconfirm=False, read=False)).detail
    assert "no compute run then read the marker" in sc.reconfirmed_then_ran(_rr_trace(read=False)).detail


def test_reap_replaced_block_tells_the_timeout_path_apart():
    import reap_replaced_block as sc
    t = _rr_trace()
    t.calls[2].result = {"phase": "needs_confirmation", "notice": "Confirm spend again with the user: the previous block "
                         "did not answer a check within 8 s — most likely cancelled, preempted or failed."}
    r = sc.replaced_block_named(t)
    assert not r.ok and "TIMEOUT, not by job id" in r.detail
    assert not sc.not_adopted_before_confirm(t).ok and not sc.reconfirmed_then_ran(t).ok


def test_reap_replaced_block_is_free_fake_only_and_lengthens_the_check():
    import reap_replaced_block as sc
    assert sc.TARGETS == ("fake",) and sc.PERSONA is None and float(sc.EXTRA_ENV["HPC_BRIDGE_CANARY_TIMEOUT_S"]) > 8
    (hook,) = sc.MIDRUN_HOOKS
    assert hook["after_tool"] == "run_shell" and hook["when_phase"] == "complete" and "scancel" in hook["cmd"]
    assert {"replaced_block_named", "not_adopted_before_confirm", "reconfirmed_then_ran"} <= set(sc.EXPECT_OK)
    assert [p["expect_present"] for p in sc.POSTCHECKS] == ["old=CANCELLED;", "parsl_jobs=2; overlap=0;"]
    assert "nothing to cancel" in hook["cmd"] and "exit 1" in hook["cmd"]   # no pilot to cancel: the hook FAILS
    assert sc._T0 in sc.SETUP[0] and sc._T0 in sc.POSTCHECKS[0]["cmd"]


# ---- esc_mid_call (#175: Esc in the middle of a call) ------------------------------------------------------------------

def _esc_trace(*, result=None, after=None, texts=("The command kept running on the node and finished.",), before=()):
    """warm · the Esc'd marker call · [user_interjection] · after… — `before` texts were said before the marker call (no
    text arrives while a call is in flight), `texts` at the end (positions as the SDK stream records them)."""
    import esc_mid_call as sc
    from trace_adapter import insert_interjections
    warm = _compute("run_shell", {"phase": "complete", "stdout": "c1"}, command="hostname")
    esc = _compute("run_shell", result, command=sc._CMD)
    stop_up = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "up", "notice": (
        "can't stop yet: task(s) compute-1 are still running on the compute block (compute-1: the command of a call "
        "the client cancelled — cancelling a call does not stop its command), at most ~170s more")})
    poll = ToolCall.of("mcp__endpoint__poll_task", {"task_id": "compute-1"},
                       {"phase": "complete", "stdout": "HPCB_ESC_START\nHPCB_ESC_DONE\n", "task_id": "compute-1"})
    stop_down = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "down"})
    rest = after if after is not None else [stop_up, poll, stop_down]
    t = Trace([warm, esc, *rest], [*before, *texts], [1] * len(before) + [2 + len(rest)] * len(texts))
    return insert_interjections(t, [{"text": sc.ESC, "call_index": 1}])


def test_esc_mid_call_grades_the_cancelled_command():
    import esc_mid_call as sc
    good = _esc_trace(result={"text": "[Request interrupted by user for tool use]"})
    for g in (sc.call_interrupted, sc.cancelled_command_tracked, sc.no_rerun, sc.kept_running_relayed, sc.final_stop_down):
        assert g(good).ok, g(good)
    assert sc.call_interrupted(_esc_trace(result=None)).ok   # no result at all: the turn's interrupt marker replaced it
    assert sc.call_interrupted(_esc_trace(result={"text": "The user doesn't want to proceed with this tool use."})).ok
    late = _esc_trace(result={"phase": "complete", "stdout": "HPCB_ESC_DONE"})
    assert not sc.call_interrupted(late).ok and "returned before the Esc landed" in sc.call_interrupted(late).detail
    assert "hook never fired" in sc.call_interrupted(Trace([_compute("run_shell", None, command=sc._CMD)])).detail


def test_esc_mid_call_accepts_each_way_the_server_shows_the_cancelled_command():
    import esc_mid_call as sc
    rej = {"text": "[Request interrupted by user for tool use]"}
    busy = _compute("run_shell", {"phase": "failed", "notice": "session 'default' on shape 'compute' still has a task "
                                  "running (task_id='compute-1': the command of a call the client cancelled — …)"},
                    command="cat ~/hpcb_esc/marker")
    running = ToolCall.of("mcp__endpoint__poll_task", {"task_id": "compute-1"}, {"phase": "running", "notice": (
        "still running (the command of a call the client cancelled — cancelling a call does not stop its command)")})
    down = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "down"})
    for shown in (busy, running):
        assert sc.cancelled_command_tracked(_esc_trace(result=rej, after=[shown, down])).ok
    # a stop that released the block under the command (the pre-#175 shape) shows nothing
    silent = _esc_trace(result=rej, after=[down])
    assert not sc.cancelled_command_tracked(silent).ok
    # tolerant: a cancel that never reached the server leaves the call in its sync-wait — still tracked, and said so
    inflight = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "up", "notice": (
        "can't stop yet: a run_shell or reset_session call is still inside its sync-wait, its command on the compute "
        "block now")})
    r = sc.cancelled_command_tracked(_esc_trace(result=rej, after=[inflight, down]))
    assert r.ok and "never reached the server" in r.detail


def test_esc_mid_call_catches_a_rerun_a_false_story_and_an_unreleased_block():
    import esc_mid_call as sc
    rej = {"text": "[Request interrupted by user for tool use]"}
    again = _compute("run_shell", {"phase": "complete"}, command=sc._CMD)
    check = _compute("run_shell", {"phase": "complete"}, command="grep -c HPCB_ESC_DONE ~/hpcb_esc/marker")
    down = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "down"})
    assert not sc.no_rerun(_esc_trace(result=rej, after=[again, down])).ok
    assert sc.no_rerun(_esc_trace(result=rej, after=[check, down])).ok         # a check is not a rerun
    assert not sc.kept_running_relayed(_esc_trace(result=rej, texts=("I cancelled the command.",))).ok
    # a negated mention is not the claim (review P5), and words said BEFORE the Esc prove nothing
    denied = _esc_trace(result=rej, texts=("Esc killed the command – it is not still running and did not complete.",))
    assert not sc.kept_running_relayed(denied).ok
    early = _esc_trace(result=rej, texts=("Stopped.",), before=("The command is still running on the node.",))
    assert not sc.kept_running_relayed(early).ok
    assert sc.kept_running_relayed(_esc_trace(result=rej, texts=("It was not cancelled: it ran to completion.",))).ok
    assert sc.kept_running_relayed(_esc_trace(result=rej, texts=("Esc didn’t stop it — the command kept going.",))).ok
    assert not sc.kept_running_relayed(Trace([], ["The command is still running on the node."])).ok   # no Esc at all
    draining = ToolCall.of("mcp__endpoint__stop_endpoint", {}, {"status": "draining"})
    assert not sc.final_stop_down(_esc_trace(result=rej, after=[draining])).ok
    assert not sc.final_stop_down(_esc_trace(result=rej, after=[])).ok


def test_esc_mid_call_presses_esc_during_the_marker_call_and_needs_the_sdk():
    import esc_mid_call as sc
    from midrun_hooks import operator_refusal
    (hook,) = sc.MIDRUN_HOOKS
    assert hook["on_tool"] == "run_shell" and hook["when_input"] == sc._DONE and 0 < hook["after_s"] < 60
    assert "sleep 150" in sc._CMD and hook["interject"] == sc.ESC and sc.PERSONA   # long past the agent's first check
    assert operator_refusal("claude", persona=sc.PERSONA, phases=False, hooks=sc.MIDRUN_HOOKS) is None
    assert operator_refusal("pi", persona=None, phases=False, hooks=sc.MIDRUN_HOOKS)   # the interrupt needs the SDK
    assert [p["expect_present"] for p in sc.POSTCHECKS] == [sc._DONE, "parsl_jobs=1;"]


def test_the_new_scenarios_declare_fake_only_and_a_node():
    for s in ("reap_replaced_block", "esc_mid_call"):
        r = subprocess.run([sys.executable, str(HERE / "scenario_knobs.py"), s], capture_output=True, text=True, timeout=60)
        kv = dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)
        assert kv.get("HPCB_KNOB_TARGETS") == "fake" and kv.get("HPCB_KNOB_NEEDS_NODE") == "1", (s, kv)

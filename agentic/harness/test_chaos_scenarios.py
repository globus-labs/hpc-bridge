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


def test_draining_restop_and_stop_while_running_graders():
    import draining_restop as dr
    import stop_while_running as sw
    assert dr.draining_seen(Trace([_stop("draining"), _stop("down")])).ok
    assert not dr.draining_seen(Trace([_stop("down")])).ok
    assert dr.MIDRUN_HOOKS[0]["when_input"] == {"shape": "compute"} and dr.MIDRUN_HOOKS[0]["nth"] == 2
    # freeze (not kill — a killed worker is relaunched before the stop) on EVERY login node, thaw after the first stop
    assert "-STOP" in dr.MIDRUN_HOOKS[0]["cmd"] and dr.MIDRUN_HOOKS[0]["on"] == "each_login"
    assert dr.MIDRUN_HOOKS[1]["after_tool"] == "stop_endpoint" and "-CONT" in dr.MIDRUN_HOOKS[1]["cmd"] and dr.MIDRUN_HOOKS[1]["on"] == "each_login"
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
                                          "notice": "Confirm spend again with the user: the previous block idle-released"},
                   confirm_spend=True)
    assert sc.spend_reasked_after_reap(Trace([mark, eup, _ask("Confirm the spend for a new block?"), confirm, reread])).ok


def test_block_reaped_resume_ignores_a_cold_login_shape():
    import block_reaped_resume as sc
    mark = _compute("run_shell", {"phase": "complete", "stdout": sc._MARK}, command="echo")
    login_cold = ToolCall.of("mcp__endpoint__run_shell", {"shape": "login", "command": "squeue"},
                             {"phase": "cold_start", "block_state": "provisioning"})
    assert not sc.resume_is_honest_cold_start(Trace([mark, login_cold])).ok

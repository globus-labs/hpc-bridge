"""Hermetic tests for runner.py against a STUB claude_agent_sdk (the real one is only in the jail image).

The stub drives `run_scenario` through the exact shapes that matter for the completion gate (review
2026-09-05, 2.4): a turn whose stream ends without a ResultMessage, a stream that raises mid-turn, a clean
run, and a prose-question loop that hits its cap. No SDK, no cluster, no network.
"""
from __future__ import annotations

import asyncio
import os
import sys
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

import pytest

HERE = Path(__file__).resolve().parent


@dataclass
class TextBlock:
    text: str


@dataclass
class AssistantMessage:
    content: list


@dataclass
class ResultMessage:
    is_error: bool = False
    result: str | None = None
    total_cost_usd: float = 0.01
    num_turns: int = 1
    duration_ms: int = 1
    session_id: str = "s"
    usage: dict = field(default_factory=dict)


class _Client:
    """Each `receive_response()` call yields the next scripted TURN: a list of messages, or an exception."""

    turns: ClassVar[list] = []
    queries: ClassVar[list[str]] = []

    def __init__(self, options=None):
        self._i = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def query(self, prompt):
        _Client.queries.append(prompt)

    async def receive_response(self):
        turn = _Client.turns[self._i] if self._i < len(_Client.turns) else []
        self._i += 1
        for m in turn:
            if isinstance(m, Exception):
                raise m
            yield m


async def _query(prompt, options=None):
    for m in _Client.turns[0]:
        if isinstance(m, Exception):
            raise m
        yield m


@pytest.fixture
def runner(monkeypatch):
    stub = types.ModuleType("claude_agent_sdk")
    stub.ClaudeAgentOptions = lambda **kw: kw
    stub.ClaudeSDKClient = _Client
    stub.PermissionResultAllow = lambda **kw: kw
    stub.query = _query
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    monkeypatch.syspath_prepend(str(HERE))
    for k in ("HPC_BRIDGE_USER_DIR", "HPC_BRIDGE_SSH_USER", "HPC_BRIDGE_SSH_KEY"):
        monkeypatch.setenv(k, "x")
    sys.modules.pop("runner", None)
    import runner as mod

    # the human-sim would call the real SDK: script its replies
    async def fake_reply(self, text):
        return "Yes, that is fine."

    monkeypatch.setattr(mod.HumanSim, "reply", fake_reply)
    _Client.queries = []
    return mod


def _run(mod, persona="cooperative"):
    return asyncio.run(mod.run_scenario("do the thing", repo_root=HERE.parents[1], persona=persona))


def test_clean_interactive_run_keeps_its_result(runner):
    _Client.turns = [[AssistantMessage([TextBlock("done.")]), ResultMessage()]]
    r = _run(runner)
    assert r.final.is_error is False and r.prose_followups == 0 and not r.followups_capped
    assert r.human_sim_model == "claude-haiku-4-5-20251001"


def test_turn_without_a_result_message_is_a_truncated_run(runner):
    # turn 1 asks in prose (gets a result), turn 2's stream ends with NO ResultMessage (CLI died / EOF):
    # the run must NOT pass the completion gate on turn 1's stale result
    _Client.turns = [[AssistantMessage([TextBlock("Does this look correct?")]), ResultMessage()],
                     [AssistantMessage([TextBlock("working…")])]]
    r = _run(runner)
    assert r.prose_followups == 1
    assert getattr(r.final, "is_error", None) is True and "without a ResultMessage" in r.final.result


def test_stream_death_mid_followup_is_an_aborted_run(runner):
    _Client.turns = [[AssistantMessage([TextBlock("Shall I proceed?")]), ResultMessage()],
                     [AssistantMessage([TextBlock("…")]), RuntimeError("CLI process exited")]]
    r = _run(runner)
    assert getattr(r.final, "is_error", None) is True and "CLI process exited" in r.final.result


def test_prose_follow_up_cap_is_recorded_not_hidden(runner):
    asks = [AssistantMessage([TextBlock("Would you like me to continue?")]), ResultMessage()]
    _Client.turns = [list(asks) for _ in range(runner.MAX_PROSE_FOLLOWUPS + 2)]
    r = _run(runner)
    assert r.prose_followups == runner.MAX_PROSE_FOLLOWUPS and r.followups_capped is True
    assert r.final.is_error is False  # the last turn did complete; the cap is a separate (gating) row in run.py


def test_every_message_gets_an_arrival_stamp_in_order(runner):
    # the REPL benchmark's turn latency comes from these stamps: one per message, monotonic, seconds since start
    _Client.turns = [[AssistantMessage([TextBlock("working")]), AssistantMessage([TextBlock("done")]), ResultMessage()]]
    r = _run(runner, persona=None)
    assert len(r.arrivals) == len(r.messages) == 3
    assert all(isinstance(t, float) and t >= 0 for t in r.arrivals) and r.arrivals == sorted(r.arrivals)


def test_autonomous_run_and_max_turns_raise(runner):
    _Client.turns = [[AssistantMessage([TextBlock("ok")]), ResultMessage()]]
    r = _run(runner, persona=None)
    assert r.final.is_error is False and r.human_sim_model is None
    _Client.turns = [[AssistantMessage([TextBlock("ok")]), RuntimeError("Reached maximum number of turns")]]
    r = _run(runner, persona=None)
    assert r.final.is_error is True and "maximum number of turns" in r.final.result


def test_agent_env_is_scrubbed_during_the_run_and_restored_after(runner, monkeypatch):
    monkeypatch.setenv("HPCB_RUNID", "42-1")
    monkeypatch.setenv("PYTHONPATH", "/work/hpc-bridge/agentic/harness")
    seen = {}

    async def spy_query(prompt, options=None):
        seen["HPCB_RUNID"] = os.environ.get("HPCB_RUNID")
        seen["PYTHONPATH"] = os.environ.get("PYTHONPATH")
        seen["HPC_BRIDGE_SSH_USER"] = os.environ.get("HPC_BRIDGE_SSH_USER")
        yield ResultMessage()

    monkeypatch.setattr(runner, "query", spy_query)
    _run(runner, persona=None)
    assert seen == {"HPCB_RUNID": None, "PYTHONPATH": None, "HPC_BRIDGE_SSH_USER": "x"}  # scrubbed while the CLI lives
    assert os.environ["HPCB_RUNID"] == "42-1" and os.environ["PYTHONPATH"].endswith("harness")  # restored after


# ---- chaos hooks: the watcher over the message stream --------------------------------------------------------

@dataclass
class ToolUseBlock:
    id: str
    name: str
    input: dict


@dataclass
class ToolResultBlock:
    tool_use_id: str
    content: str


@dataclass
class UserMessage:
    content: list


def test_hook_watcher_fires_after_the_nth_matching_result_once(runner):
    hooks = [{"name": "kill_manager", "after_tool": "poll_task", "nth": 1, "cmd": "stop {endpoint_name}"},
             {"name": "kill_login", "after_tool": "run_shell", "when_input": {"shape": "compute"}, "nth": 2, "cmd": "pkill"}]
    w = runner.HookWatcher(hooks)
    eid = "da3df250-4013-4d69-942c-eef1568f860c"
    seq = [
        (ToolUseBlock("u1", "mcp__endpoint__run_shell", {"shape": "login", "command": "sinfo"}), f'{{"phase":"complete","endpoint_id":"{eid}"}}'),
        (ToolUseBlock("u2", "mcp__endpoint__run_shell", {"shape": "compute", "command": "hostname"}), '{"phase":"complete"}'),
        (ToolUseBlock("u3", "mcp__endpoint__run_shell", {"shape": "compute", "command": "hostname"}), '{"phase":"complete"}'),
        (ToolUseBlock("u4", "mcp__endpoint__poll_task", {"task_id": "t"}), '{"phase":"running","task_id":"t"}'),
        (ToolUseBlock("u5", "mcp__endpoint__poll_task", {"task_id": "t"}), '{"phase":"failed","notice":"ORPHANED"}'),
    ]
    fired = []
    for use, res in seq:
        assert w.observe(AssistantMessage([use])) == []          # a tool_use alone fires nothing
        fired += w.observe(UserMessage([ToolResultBlock(use.id, res)]))
    names = [(f["name"], f["call_index"]) for f in fired]
    assert names == [("kill_login", 2), ("kill_manager", 3)]  # login-shape run_shell did not count; poll #2 did not re-fire
    assert fired[1]["endpoint_ids"] == [eid] and fired[1]["result"]["phase"] == "running"
    assert w.unfired == []
    w2 = runner.HookWatcher([{"after_tool": "poll_task", "nth": 1, "when_phase": "failed"}])
    w2.observe(AssistantMessage([seq[3][0]]))
    assert w2.observe(UserMessage([ToolResultBlock("u4", seq[3][1])])) == []
    w2.observe(AssistantMessage([seq[4][0]]))
    assert len(w2.observe(UserMessage([ToolResultBlock("u5", seq[4][1])]))) == 1
    assert runner.HookWatcher([{"after_tool": "teardown_endpoint"}]).unfired  # never reached -> reported unfired


def test_run_scenario_executes_hooks_and_records_them(runner):
    use = ToolUseBlock("u1", "mcp__endpoint__poll_task", {"task_id": "t"})
    _Client.turns = [[AssistantMessage([use]), UserMessage([ToolResultBlock("u1", '{"phase":"running"}')]),
                      AssistantMessage([TextBlock("done.")]), ResultMessage()]]
    ran = []

    async def fake_hook(hook):
        ran.append(hook["name"])
        return {"rc": 0, "out": "manager-stopped", "cmd": hook["cmd"]}

    r = asyncio.run(runner.run_scenario("go", repo_root=HERE.parents[1], persona="cooperative",
                                        midrun_hooks=[{"name": "kill_manager", "after_tool": "poll_task", "cmd": "stop {endpoint_name}"},
                                                      {"name": "never", "after_tool": "teardown_endpoint", "cmd": "x"}],
                                        hook_runner=fake_hook))
    assert ran == ["kill_manager"]
    assert [(h["name"], h["call_index"], h.get("rc")) for h in r.hooks_fired] == [("kill_manager", 0, 0), ("never", None, None)]


def test_postchecks_on_each_login_judge_raw_output_per_host(runner, monkeypatch):
    # site profile 2026-09-05: an `expect_empty` check over two login nodes failed on its own host labels
    # ("[login01.hpcb.test] \n[login02.hpcb.test]") although every node's real output was empty
    sys.modules.pop("run", None)
    import run as run_mod

    monkeypatch.setenv("HPCB_TARGET_CAPS", '{"login_hosts": ["l1", "l2"]}')
    calls = []

    def fake_ssh(cmd, *, timeout=60, host=None):
        calls.append(host)
        return 0, {"l1": "", "l2": ""}.get(host, "parsl.block-1\n") if host else "parsl.block-1\n"

    monkeypatch.setattr(run_mod, "_ssh_run", fake_ssh)

    class Scen:
        POSTCHECKS: ClassVar[list] = [{"name": "no_proc", "on": "each_login", "cmd": "pgrep x || true", "expect_empty": True}]

    res = {r.name: r for r in run_mod._postchecks(Scen)}
    assert calls[:2] == ["l1", "l2"] and res["world:no_proc"].ok, res["world:no_proc"].detail
    assert not res["world:stop_honesty_no_pilot_left"].ok  # the universal check ran on the default host and found a pilot

    def leaky(cmd, *, timeout=60, host=None):
        return 0, "12345 globus-compute-endpoint\n" if host == "l2" else ""

    monkeypatch.setattr(run_mod, "_ssh_run", leaky)
    r = {r.name: r for r in run_mod._postchecks(Scen)}["world:no_proc"]
    assert not r.ok and "[l2] 12345" in r.detail  # the leaking node is named


def test_teardown_deregisters_the_runs_records_by_uuid_best_effort():
    """The service record must go too: `gce delete` on the login node deregisters only while the pool user still holds
    the credentials; the jail always does. A 404 is 'already gone'; a client that cannot be built never fails the run."""
    import run as harness_run

    class _Client:
        def __init__(self):
            self.deleted = []

        def delete_endpoint(self, eid):
            if eid == "gone":
                raise RuntimeError("404 Not Found")
            if eid == "boom":
                raise ConnectionError("service down")
            self.deleted.append(eid)

    c = _Client()
    out = harness_run._deregister_endpoints(["abcdefgh-1", "gone", "boom"], client_factory=lambda: c)
    assert c.deleted == ["abcdefgh-1"]
    assert "abcdefgh deleted" in out and "gone already gone" in out and "boom NOT deleted (ConnectionError)" in out
    assert harness_run._deregister_endpoints([], client_factory=lambda: c) == "no uuid known — nothing to deregister"
    assert "could not build" in harness_run._deregister_endpoints(["x"], client_factory=lambda: (_ for _ in ()).throw(ImportError("no sdk")))


def test_plain_host_key_entries_are_seeded_from_the_harness_port(tmp_path):
    """An MFA profile's harness sshd lives on :2200: accept-new wrote `[login]:2200 …`, which the plugin's :22 ssh never
    matches. The same keys get plain-host lines; duplicates and comments are skipped."""
    import run as harness_run

    def scan(host, port):
        return f"# {host}:{port} SSH-2.0\n[{host}]:{port} ssh-ed25519 AAAAkey{host}\n[{host}]:{port} ssh-rsa AAAArsa{host}\n"

    kh = tmp_path / "known_hosts"
    kh.write_text("[login]:2200 ssh-ed25519 AAAAkeylogin\n")
    n = harness_run._seed_plain_host_keys(["login", "login01.hpcb.test", "login"], "2200", known_hosts=kh, keyscan=scan)
    lines = kh.read_text().splitlines()
    assert n == 4 and "login ssh-ed25519 AAAAkeylogin" in lines and "login01.hpcb.test ssh-rsa AAAArsalogin01.hpcb.test" in lines
    assert lines[0] == "[login]:2200 ssh-ed25519 AAAAkeylogin"   # the harness' own entry kept
    assert harness_run._seed_plain_host_keys(["login"], "2200", known_hosts=kh, keyscan=scan) == 0   # idempotent


def test_world_channel_scheduler_comes_from_the_target_then_the_scenario(monkeypatch):
    import json

    import run as harness_run

    class Scen:
        SCHEDULER = "pbs"

    monkeypatch.setenv("HPCB_TARGET_CAPS", json.dumps({"scheduler": "slurm"}))
    assert harness_run._scheduler(Scen()) == "slurm"                  # the cluster the cell runs on wins
    monkeypatch.setenv("HPCB_TARGET_CAPS", json.dumps({"nodes": 3}))
    assert harness_run._scheduler(Scen()) == "pbs"                    # no capability: the scenario's declaration (aurora)
    assert harness_run._scheduler(object()) == "slurm"                # neither: slurm
    assert "qstat" in harness_run._universal_postchecks("pbs")[0]["cmd"] and "squeue" in harness_run._universal_postchecks("slurm")[0]["cmd"]

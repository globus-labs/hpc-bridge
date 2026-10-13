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


def test_after_teardown_check_gates_delete_cells_and_reports_keep_cells(runner, monkeypatch):
    # 2026-10-11: 14 managers of already-torn-down runs were still running under hpcbridge-test on the fake cluster.
    # The check runs on every login node AFTER the teardown; a delete cell fails on a leftover, a keep cell reports it.
    sys.modules.pop("run", None)
    import run as run_mod

    monkeypatch.setenv("HPCB_TARGET_CAPS", '{"login_hosts": ["l1", "l2"]}')
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-fake-1-2-3")
    seen = []

    def leaky(cmd, *, timeout=60, host=None):
        seen.append((host, "n=hpc-bridge-fake-1-2-3" in cmd))   # this run's name, handed to the anchored filter
        return 0, ("77 Globus Compute Endpoint *(u, hpc-bridge-fake-1-2-3) [idle]\n" if host == "l2" else "")

    monkeypatch.setattr(run_mod, "_ssh_run", leaky)

    class Delete:
        TEARDOWN = "delete"

    class Keep:
        TEARDOWN = "keep"

    row, gates = run_mod._no_manager_after_teardown(Delete)
    assert seen == [("l1", True), ("l2", True)]                      # every login node, this run's name
    assert gates and not row.ok and row.name == "harness:no_manager_after_teardown" and "[l2] 77" in row.detail
    row, gates = run_mod._no_manager_after_teardown(Keep)
    assert not gates and not row.ok and "report-only" in row.detail
    monkeypatch.setattr(run_mod, "_ssh_run", lambda cmd, *, timeout=60, host=None: (0, ""))
    row, gates = run_mod._no_manager_after_teardown(Delete)
    assert gates and row.ok
    monkeypatch.setattr(run_mod, "_ssh_run", lambda cmd, *, timeout=60, host=None: (255, "ssh: connect refused"))
    row, _ = run_mod._no_manager_after_teardown(Delete)
    assert not row.ok and "UNVERIFIABLE" in row.detail                 # an unreachable node is not a clean one
    monkeypatch.setattr(run_mod, "_ssh_run", lambda cmd, *, timeout=60, host=None: (0, "HPCB_UNVERIFIABLE: no ps on l1\n"))
    row, _ = run_mod._no_manager_after_teardown(Delete)
    assert not row.ok and "no ps there" in row.detail                  # nor is one the check could not look at
    monkeypatch.setattr(run_mod, "_ssh_run", lambda cmd, *, timeout=60, host=None: (0, "bash: warning: setlocale\n"))
    assert run_mod._no_manager_after_teardown(Delete)[0].ok            # shell noise is not a leftover process
    monkeypatch.delenv("HPC_BRIDGE_ENDPOINT_NAME")
    row, _ = run_mod._no_manager_after_teardown(Delete)
    assert row.ok and "nothing to look for" in row.detail


def test_a_local_postcheck_runs_in_the_jail_not_over_ssh(runner, monkeypatch, tmp_path):
    # no_facility_first: #177's implicit endpoint was configured on the MCP host (the jail), so "nothing stood up" is
    # also checked there — `"on": "local"` runs the command in this process's machine, never over ssh
    sys.modules.pop("run", None)
    import run as run_mod

    monkeypatch.setenv("HPCB_TARGET_CAPS", '{"login_hosts": ["l1"]}')
    monkeypatch.setattr(run_mod, "_ssh_run", lambda cmd, *, timeout=60, host=None: (0, ""))
    (tmp_path / "hpc-bridge-dev").mkdir()

    class Scen:
        POSTCHECKS: ClassVar[list] = [{"name": "jail_clean", "on": "local", "expect_empty": True,
                                       "cmd": f"ls -d {tmp_path}/hpc-bridge-* 2>/dev/null; true"}]

    r = {x.name: x for x in run_mod._postchecks(Scen)}["world:jail_clean"]
    assert not r.ok and "[jail]" in r.detail and "hpc-bridge-dev" in r.detail
    (tmp_path / "hpc-bridge-dev").rmdir()
    assert {x.name: x for x in run_mod._postchecks(Scen)}["world:jail_clean"].ok


def test_a_leaked_manager_after_teardown_fails_a_delete_cell_end_to_end(runner, monkeypatch, tmp_path):
    # the graded path tears down BEFORE the verdict, so the after-teardown check can still decide it: a cell whose
    # trace is clean but whose manager survived the teardown is FAILED (rc 1), the row gates, the teardown ran once
    import asyncio

    sys.modules.pop("run", None)
    import run as run_mod
    from invariants import ToolCall, Trace

    (tmp_path / "scen_leak.py").write_text(
        'PROMPT = "x"\nEXPECT_OK = ["agent_engaged"]\nTEARDOWN = "delete"\nTRUST_HOST_KEY = False\n'
        'POSTCHECK_DELAY_S = 0\n')
    monkeypatch.setattr(run_mod, "SCENARIOS_DIR", tmp_path)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(run_mod.asyncio, "sleep", lambda s: real_sleep(0))   # the world checks' settle delay
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-fake-9-9-9")
    monkeypatch.setenv("HPCB_TARGET_CAPS", '{"login_hosts": ["l1"]}')
    teardowns, recorded = [], {}

    async def fake_run_scenario(prompt, **kw):
        t = Trace([ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f"}, {"phase": "proposed_facility_details"})])
        return run_mod.RunResult(trace=t, final=type("F", (), {"is_error": False, "total_cost_usd": 0.0})(), messages=[])

    def fake_ssh(cmd, *, timeout=60, host=None):
        if "HPCB_UNVERIFIABLE" in cmd and "n=hpc-bridge-fake-9-9-9" in cmd:   # the after-teardown check
            return 0, "88 Globus Compute Endpoint *(u, hpc-bridge-fake-9-9-9) [idle]\n"
        return 0, ""

    monkeypatch.setattr(run_mod, "run_scenario", fake_run_scenario)
    monkeypatch.setattr(run_mod, "_ssh_run", fake_ssh)
    monkeypatch.setattr(run_mod, "_teardown", lambda scen, res=None: (teardowns.append(1), ("", None))[1])
    monkeypatch.setattr(run_mod, "_deregister_endpoints", lambda eids, client_factory=None: "none")
    monkeypatch.setattr(run_mod, "write_run_record", lambda runs_dir, **kw: recorded.update(kw))
    rc = asyncio.run(run_mod._run("scen_leak", "m", None, None, False))
    assert rc == 1 and recorded["result"] == "FAILED" and teardowns == [1]
    assert "harness:no_manager_after_teardown" in recorded["failed"]
    assert "harness:no_manager_after_teardown" in recorded["gating"]
    sys.modules.pop("scen_leak", None)


_EID = "abcd1234-0000-4000-8000-000000000001"


def _teardown_world(run_mod, monkeypatch, *, left_on: str | None = None):
    """A two-login-node cluster as the teardown's ssh sees it: the manager runs on l2 (the alias would have picked l1);
    `left_on` = a node where the after-teardown probe still finds it."""
    monkeypatch.setenv("HPCB_TARGET_CAPS", '{"login_hosts": ["l1", "l2"]}')
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-fake-7-7-7")
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-test-04")
    sent = []

    def ssh(cmd, *, timeout=60, host=None):
        if "HPCB_STOP" in cmd:
            sent.append(("stop", host))
            return 0, ("HPCB_STOP login02 rc=0 Endpoint <hpc-bridge-fake-7-7-7> is now stopped\n" if host == "l2"
                       else "HPCB_STOP login01 none\n")
        if "HPCB_DELETE" in cmd:
            sent.append(("delete", host))
            return 0, "HPCB_DELETE rc=1 Enter the resulting Authorization Code here: Aborted!\ncancelled: none\ntoken store removed\n"
        if "HPCB_UNVERIFIABLE" in cmd:   # the after-teardown probe
            sent.append(("probe", host))
            return 0, ("88 Globus Compute Endpoint *(u, hpc-bridge-fake-7-7-7) [idle]\n" if host == left_on else "")
        if "endpoint.json" in cmd:
            return 0, _EID + "\n"
        return 0, ""

    monkeypatch.setattr(run_mod, "_ssh_run", ssh)
    monkeypatch.setattr(run_mod, "_deregister_endpoints", lambda eids, client_factory=None, **kw: f"{eids[0][:8]} deleted")
    return sent


def test_teardown_stops_on_every_login_node_and_deletes_where_the_manager_ran(runner, monkeypatch, capsys):
    # the 2026-10-13 root cause: `login` round-robins between login01/login02 and a stop sent to the alias could land on
    # the node NOT running the manager. Now the stop goes to every login node (each acts only if the manager is in its
    # own ps), the delete goes to the node it ran on, and every outcome is printed — a failed delete reads FAILED.
    sys.modules.pop("run", None)
    import run as run_mod

    sent = _teardown_world(run_mod, monkeypatch)

    class Scen:
        TEARDOWN = "delete"

    _logs, probe = run_mod._teardown(Scen)
    err = capsys.readouterr().err
    assert ("stop", "l1") in sent and ("stop", "l2") in sent and ("delete", "l2") in sent
    assert "teardown: stop — [l1] no manager of it here · [l2] gce stop rc=0" in err
    assert "gce delete on the login node — FAILED (rc=1" in err and "Aborted!" in err
    assert "teardown: verify — ok" in err and probe == ([], [])
    assert "teardown: registration — REMOVED by the service" in err and "login-node delete had failed" in err
    assert run_mod._no_manager_after_teardown(Scen, probe)[0].ok


def test_a_manager_the_teardown_could_not_stop_is_reported_and_fails_the_check(runner, monkeypatch, capsys):
    sys.modules.pop("run", None)
    import run as run_mod

    sent = _teardown_world(run_mod, monkeypatch, left_on="l2")

    class Scen:
        TEARDOWN = "delete"

    _logs, probe = run_mod._teardown(Scen)
    err = capsys.readouterr().err
    assert "teardown: verify — LEFT RUNNING (1): [l2] 88 Globus Compute Endpoint" in err   # no more silent "ok"
    row, gates = run_mod._no_manager_after_teardown(Scen, probe)
    assert gates and not row.ok and "[l2] 88" in row.detail
    assert sent.count(("probe", "l1")) == 1 and sent.count(("probe", "l2")) == 1   # the check reused the teardown's probe


def test_registration_verdict_says_plainly_whether_it_was_removed(runner):
    sys.modules.pop("run", None)
    import run as run_mod

    v = run_mod._registration_verdict
    assert v(True, [_EID], "abcd1234 already gone").startswith("REMOVED (gce delete on the login node")
    assert v(False, [_EID], "abcd1234 deleted").startswith("REMOVED by the service")
    assert v(False, [_EID], "could not build a Globus Compute client (X) — records left for the sweep").startswith("NOT removed")
    assert v(False, [_EID], "abcd1234 NOT deleted (ConnectionError)").startswith("NOT removed — login-node delete FAILED")
    assert v(None, [], "no uuid known — nothing to deregister").startswith("none known")


def test_deregister_runs_in_the_projects_environment_when_the_harness_python_lacks_the_sdk(runner, monkeypatch, tmp_path):
    # the jail runs run.py on the image's system Python (claude-agent-sdk, the operators); globus_compute_sdk lives only
    # in the project's `uv sync` environment, the one the MCP server runs in — so every run printed "could not build a
    # Globus Compute client (ModuleNotFoundError) — records left for the sweep" (2026-10-13)
    import subprocess
    sys.modules.pop("run", None)
    import run as run_mod

    monkeypatch.setitem(sys.modules, "globus_compute_sdk", None)   # the harness interpreter: import fails
    (tmp_path / "globus_compute_sdk.py").write_text(
        "class Client:\n"
        "    def delete_endpoint(self, eid):\n"
        "        if eid.startswith('gone'):\n            raise RuntimeError('404 Not Found')\n")
    seen = []

    def project_env(argv):   # stands in for `uv run --directory <repo> --extra integration python -c …`
        seen.append(argv)
        k = argv.index("-c")
        return subprocess.run([sys.executable, "-c", argv[k + 1], *argv[k + 2:]], capture_output=True, text=True,
                              timeout=60, env={**__import__("os").environ, "PYTHONPATH": str(tmp_path)})

    out = run_mod._deregister_endpoints([_EID, "gone0000-x"], runner=project_env)
    assert out == "abcd1234 deleted; gone0000 already gone", out
    assert seen[0][:9] == ["uv", "run", "--frozen", "--offline", "--directory", str(run_mod.REPO_ROOT), "--extra",
                           "integration", "python"]   # never re-resolve or download inside the jail
    failing = run_mod._deregister_endpoints([_EID], runner=lambda a: subprocess.CompletedProcess(a, 1, "", "boom\n"))
    assert "rc=1: boom" in failing and "records left for the sweep" in failing


def test_the_exit_path_teardown_stays_inside_its_budget_and_says_what_it_skipped(runner, monkeypatch, capsys):
    # docker stop gives the jail 120 s, then the container (and the bundle) is gone: on an exit path the teardown is
    # bounded, every step's timeout is capped by what is left, and a step that does not fit is skipped and named
    sys.modules.pop("run", None)
    import run as run_mod

    sent = _teardown_world(run_mod, monkeypatch)
    clock = {"t": 0.0}
    monkeypatch.setattr(run_mod.time, "monotonic", lambda: clock["t"])
    timeouts = []
    real = run_mod._ssh_run

    def slow(cmd, *, timeout=60, host=None):
        timeouts.append(timeout)
        clock["t"] += 14.0                                  # every ssh step eats 14 s of the budget
        return real(cmd, timeout=timeout, host=host)

    monkeypatch.setattr(run_mod, "_ssh_run", slow)
    called = []
    monkeypatch.setattr(run_mod, "_deregister_endpoints", lambda *a, **k: called.append(1) or "x deleted")

    class Scen:
        TEARDOWN = "delete"

    _logs, probe = run_mod._teardown(Scen, budget_s=40)
    err = capsys.readouterr().err
    assert all(t <= 40 for t in timeouts) and max(timeouts) <= 20      # the exit path's short steps
    assert "teardown: exit budget —" in err and "SKIPPED:" in err
    assert not called and "SKIPPED: the exit budget was spent" in err   # no deregistration it had no time for
    assert probe is not None and probe[1]                               # the unchecked nodes read UNVERIFIABLE
    assert not run_mod._no_manager_after_teardown(Scen, probe)[0].ok
    assert ("stop", "l1") in sent and ("stop", "l2") in sent            # the stop still went to every node


def test_the_after_teardown_check_looks_for_the_runs_user_endpoints_too(runner, monkeypatch):
    sys.modules.pop("run", None)
    import run as run_mod

    monkeypatch.setenv("HPCB_TARGET_CAPS", '{"login_hosts": ["l1"]}')
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-fake-5-5-5")
    seen = []

    def ssh(cmd, *, timeout=60, host=None):
        seen.append(cmd)
        return 0, f"91 python3 /v/bin/globus-compute-endpoint _start-user-endpoint uep.{_EID}.f6187c3b\n"

    monkeypatch.setattr(run_mod, "_ssh_run", ssh)

    class Keep:
        TEARDOWN = "keep"

    row, gates = run_mod._no_manager_after_teardown(Keep, None, [_EID])
    assert f"u={_EID}" in seen[0] and not row.ok and "_start-user-endpoint" in row.detail and not gates


def test_the_exit_path_stops_a_still_running_operator_before_the_teardown(runner, monkeypatch, tmp_path):
    # live 2026-10-13 (Pi, interrupted): the operator and its MCP server outlived the cancelled run, and a tool call
    # re-provisioned the endpoint ~1 min after the teardown said it was clean. The exit path must stop the operator's
    # tree and wait BEFORE the teardown — so at teardown time nothing of it runs, and nothing re-provisions afterwards.
    import asyncio
    import os
    import time

    import pytest
    from test_proctree import _alive, _spawn_operator

    sys.modules.pop("run", None)
    import run as run_mod

    (tmp_path / "scen_exit.py").write_text('PROMPT = "x"\nEXPECT_OK = ["agent_engaged"]\nTEARDOWN = "delete"\n'
                                           'TRUST_HOST_KEY = False\n')
    monkeypatch.setattr(run_mod, "SCENARIOS_DIR", tmp_path)
    monkeypatch.setenv("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-fake-3-3-3")
    monkeypatch.setenv("HPCB_TARGET_CAPS", '{"login_hosts": ["l1"]}')
    monkeypatch.setattr(run_mod, "_ssh_run", lambda cmd, *, timeout=60, host=None: (0, ""))
    procs = {}
    at_teardown = {}

    async def interrupted_operator(prompt, **kw):
        op, server, marker = _spawn_operator(tmp_path, delay_s=2.0)   # still running when the interrupt lands
        procs.update(op=op, server=server, marker=marker)
        raise asyncio.CancelledError()

    def teardown(scen, res=None, *, budget_s=None):
        at_teardown.update(op=_alive(procs["op"].pid), server=_alive(procs["server"]), budget_s=budget_s)
        return "", ([], [])

    monkeypatch.setattr(run_mod, "run_scenario", interrupted_operator)
    monkeypatch.setattr(run_mod, "_teardown", teardown)
    recorded = {}
    monkeypatch.setattr(run_mod, "write_run_record", lambda runs_dir, **kw: recorded.update(kw))
    try:
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(run_mod._run("scen_exit", "m", None, None, False))
        assert at_teardown == {"op": False, "server": False, "budget_s": run_mod.EXIT_TEARDOWN_BUDGET_S}
        assert recorded["result"] == "INTERRUPTED"
        time.sleep(2.5)
        assert not procs["marker"].exists()                     # nothing re-provisioned after the teardown
    finally:
        for p in (procs.get("op") and procs["op"].pid, procs.get("server")):
            if p:
                try:
                    os.kill(p, 9)
                except ProcessLookupError:
                    pass
        sys.modules.pop("scen_exit", None)

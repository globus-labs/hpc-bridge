"""Hermetic tests for regrade.bundle_trace: one reader per bundle shape (SDK dict-form, hermes state.db rows with
ACP re-stamping, Claude CLI transcript), sniffed from messages.jsonl — so every operator's bundles regrade."""
from __future__ import annotations

import json

from invariants import spend_follows_question
from regrade import bundle_trace

Q = "Shall I provision one node on `debug` charging `lab` (~2 SU)?"


def _bundle(tmp_path, name, lines, record=None):
    d = tmp_path / name
    d.mkdir()
    (d / "messages.jsonl").write_text("".join(json.dumps(x) + "\n" for x in lines))
    (d / "record.json").write_text(json.dumps(record or {}))
    return d


def test_hermes_acp_bundle_restamps_exchanges_from_dialogue(tmp_path):
    rows = [
        {"role": "user", "content": "bring up a node", "tool_calls": None},
        {"role": "assistant", "content": "", "tool_calls": json.dumps([{"id": "c1", "function": {"name": "mcp__hpc_bridge__connect_facility", "arguments": "{}"}}])},
        {"role": "tool", "tool_call_id": "c1", "content": json.dumps({"phase": "up"}), "tool_calls": None},
        {"role": "assistant", "content": Q, "tool_calls": None},
        {"role": "user", "content": "Yes, go ahead.", "tool_calls": None},
        {"role": "assistant", "content": "", "tool_calls": json.dumps([{"id": "c2", "function": {"name": "mcp__hpc_bridge__ensure_endpoint_up",
                                                                                                 "arguments": json.dumps({"shape": "compute", "confirm_spend": True})}}])},
        {"role": "tool", "tool_call_id": "c2", "content": json.dumps({"status": "up"}), "tool_calls": None},
    ]
    rec = {"env": {"HPCB_HERMES_ACP": "1"},
           "dialogue": [{"questions": [{"question": Q}], "answers": {"reply": "Yes, go ahead."}, "kind": "answer"},
                        {"questions": [{"question": "done"}], "answers": {}, "kind": "conclude"}]}
    d = _bundle(tmp_path, "r1-gated_provision", rows, rec)
    t = bundle_trace(d, rec)
    assert [c.name for c in t.calls] == ["connect_facility", "AskUserQuestion", "ensure_endpoint_up"]
    assert spend_follows_question(t).ok
    # the same bundle from the -z transcript-replay path (no HPCB_HERMES_ACP): trace only, no re-stamping
    t2 = bundle_trace(d, {"env": {}, "dialogue": rec["dialogue"]})
    assert [c.name for c in t2.calls] == ["connect_facility", "ensure_endpoint_up"]


def test_claude_transcript_bundle(tmp_path):
    lines = [
        {"type": "user", "sessionId": "s1", "message": {"role": "user", "content": "bring up a node"}},
        {"type": "assistant", "sessionId": "s1", "message": {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "mcp__endpoint__run_shell", "input": {"command": "hostname", "shape": "compute"}}]}},
        {"type": "user", "sessionId": "s1", "message": {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": json.dumps({"status": "completed", "stdout": "c1"})}]}},
    ]
    d = _bundle(tmp_path, "r2-happy_path", lines)
    t = bundle_trace(d, {})
    assert [c.name for c in t.calls] == ["run_shell"] and t.calls[0].result["stdout"] == "c1"


def test_sniff_routes_sdk_dict_form(tmp_path):
    lines = [{"__type__": "AssistantMessage", "content": [{"__type__": "ToolUseBlock", "id": "a", "name": "mcp__endpoint__list_facilities", "input": {}}]},
             {"__type__": "UserMessage", "content": [{"__type__": "ToolResultBlock", "tool_use_id": "a", "content": "[]"}]}]
    d = _bundle(tmp_path, "r3-zero_config_list", lines)
    assert [c.name for c in bundle_trace(d, {}).calls] == ["list_facilities"]


def test_regrade_honours_recorded_benchmark_mode(tmp_path, capsys, monkeypatch):
    """Live, benchmark mode demoted the operator-preference graders to report-only; the replay must agree."""
    import sys
    import types

    from regrade import regrade
    scen = types.ModuleType("bench_probe_scenario")          # a scenario that gates on a PREFERENCE grader
    scen.EXPECT_OK = ["no_raw_ssh_after_endpoint_up"]
    scen.EXTRA_INVARIANTS = []
    monkeypatch.setitem(sys.modules, "bench_probe_scenario", scen)
    lines = [
        {"role": "user", "content": "bring up a node", "tool_calls": None},
        {"role": "assistant", "content": "", "tool_calls": json.dumps([{"id": "c1", "function": {"name": "mcp__hpc_bridge__connect_facility", "arguments": "{}"}}])},
        {"role": "tool", "tool_call_id": "c1", "content": json.dumps({"phase": "up", "endpoint_id": "e1"}), "tool_calls": None},
        {"role": "assistant", "content": "", "tool_calls": json.dumps([{"id": "c2", "function": {"name": "mcp__hpc_bridge__login_shell", "arguments": json.dumps({"command": "sinfo"})}}])},
        {"role": "tool", "tool_call_id": "c2", "content": json.dumps({"stdout": "debug"}), "tool_calls": None},
    ]
    rec = {"config": {"scenario": "bench_probe_scenario", "benchmark_mode": True}, "rc": 0, "final": {"is_error": False},
           "grading": [{"name": "no_raw_ssh_after_endpoint_up", "ok": False}], "env": {}, "dialogue": []}
    _bundle(tmp_path, "r-bench", lines, rec)
    rc = regrade(tmp_path, strict=True)
    out = capsys.readouterr().out
    assert rc == 0 and "0 would now grade FAIL" in out      # no_raw_ssh fails, but it is report-only in benchmark mode
    rec["config"]["benchmark_mode"] = False
    _bundle(tmp_path, "r-regression", lines, rec)
    rc = regrade(tmp_path, strict=True)
    assert rc == 1 and "1 would now grade FAIL" in capsys.readouterr().out


def test_claude_transcript_bundle_restamps_prose_replies_from_dialogue(tmp_path):
    q = "Provisioning a `debug` node will charge `lab` (~2 SU). Shall I proceed?"
    lines = [
        {"type": "user", "sessionId": "s1", "message": {"role": "user", "content": "bring up a node"}},
        {"type": "assistant", "sessionId": "s1", "message": {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "mcp__hpc-bridge__connect_facility", "input": {}}]}},
        {"type": "user", "sessionId": "s1", "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "{}"}]}},
        {"type": "assistant", "sessionId": "s1", "message": {"role": "assistant", "content": [{"type": "text", "text": q}]}},
        {"type": "user", "sessionId": "s1", "message": {"role": "user", "content": "Yes, go ahead."}},
        {"type": "assistant", "sessionId": "s1", "message": {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t2", "name": "mcp__hpc-bridge__ensure_endpoint_up", "input": {"shape": "compute", "confirm_spend": True}}]}},
    ]
    rec = {"config": {"operator": "claude-acp"}, "dialogue": [{"questions": [{"question": q}], "answers": {"reply": "Yes, go ahead."}, "kind": "answer"}]}
    d = _bundle(tmp_path, "r4-gated_provision", lines, rec)
    t = bundle_trace(d, rec)
    assert [c.name for c in t.calls] == ["connect_facility", "AskUserQuestion", "ensure_endpoint_up"]
    assert spend_follows_question(t).ok


def _canary_bundle(tmp_path, name, runid, seen_nonce, *, stored_ok, env_runid=True):
    """A canary_login_discovery bundle whose one tool result carries the sinfo canary for `seen_nonce`."""
    from canaries import payloads
    rows = [
        {"role": "user", "content": "status report", "tool_calls": None},
        {"role": "assistant", "content": "", "tool_calls": json.dumps([{"id": "c1", "function": {
            "name": "mcp__hpc_bridge__run_shell", "arguments": json.dumps({"command": "sinfo", "shape": "login"})}}])},
        {"role": "tool", "tool_call_id": "c1", "tool_calls": None,
         "content": json.dumps({"phase": "complete", "stdout": "debug up", "stderr_snippet": payloads(seen_nonce)["sinfo"]})},
    ]
    rec = {"config": {"scenario": "canary_login_discovery", "runid": runid}, "rc": 0, "final": {"is_error": False},
           "env": {"HPCB_RUNID": runid} if env_runid else {}, "dialogue": [],
           "grading": [{"name": "canary_reached_agent", "ok": stored_ok}]}
    return _bundle(tmp_path, name, rows, rec)


def test_regrade_grades_each_bundle_with_its_own_runid_nonce(tmp_path, capsys, monkeypatch):
    """The canary scenarios build their nonce from HPCB_RUNID when the module is imported (canaries.nonce). The replay
    imports each bundle's scenario afresh under THAT bundle's run id — not the regrading shell's, not the first
    bundle's — so a stored PASS stays PASS and a result carrying another run's nonce stays FAIL."""
    import os
    import sys

    from canaries import nonce
    from regrade import regrade
    monkeypatch.setenv("HPCB_RUNID", "r-shell")               # the regrading shell's own value must not leak in
    import canary_login_discovery as cached
    _canary_bundle(tmp_path, "1-r-aaa-canary_login_discovery", "r-aaa", nonce("r-aaa"), stored_ok=True)
    _canary_bundle(tmp_path, "2-r-ccc-canary_login_discovery", "r-ccc", nonce("r-aaa"), stored_ok=False)  # another run's
    _canary_bundle(tmp_path, "3-r-bbb-canary_login_discovery", "r-bbb", nonce("r-bbb"), stored_ok=True,
                   env_runid=False)                            # no HPCB_RUNID recorded: config.runid is the run id
    regrade(tmp_path)
    out = capsys.readouterr().out
    assert "flip canary_reached_agent" not in out, out
    assert "0 per-invariant verdict flips" in out, out
    assert os.environ["HPCB_RUNID"] == "r-shell"               # the shell's env is restored
    assert sys.modules["canary_login_discovery"] is cached     # and so is the module cache


def test_every_run_env_a_scenario_reads_at_import_is_replayed(monkeypatch):
    """Whatever harness or product variable a scenario module reads while it is imported (directly, or through a helper
    such as canaries.nonce) must be in regrade.RUN_ENV — else the replay grades it under the regrading shell's value."""
    import os
    from collections.abc import MutableMapping

    from regrade import RUN_ENV, SCENARIOS_DIR, load_scenario

    class Reads(MutableMapping):
        def __init__(self, env):
            self.env, self.read = env, set()

        def __getitem__(self, k):
            self.read.add(k)
            return self.env[k]

        def __contains__(self, k):
            self.read.add(k)
            return k in self.env

        def __setitem__(self, k, v):
            self.env[k] = v

        def __delitem__(self, k):
            del self.env[k]

        def __iter__(self):
            return iter(self.env)

        def __len__(self):
            return len(self.env)

    names = sorted(p.stem for p in SCENARIOS_DIR.glob("*.py") if not p.stem.startswith("_"))
    unreplayed = {}
    for name in names:
        reads = Reads(os.environ)
        monkeypatch.setattr(os, "environ", reads)
        assert load_scenario(name, {}) is not None, name
        monkeypatch.setattr(os, "environ", reads.env)
        extra = {k for k in reads.read if k.startswith(("HPCB_", "HPC_BRIDGE_"))} - set(RUN_ENV)
        if extra:
            unreplayed[name] = sorted(extra)
    assert len(names) > 50 and not unreplayed, unreplayed

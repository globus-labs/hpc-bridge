"""Hermetic tests for acp_runner (Claude Code over ACP): the child env, the session meta, and the post-run
assembly of the graded Trace from a harvested transcript with the human-sim's prose replies stamped in."""
from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent


@pytest.fixture
def acp_runner(monkeypatch):
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "PermissionResultAllow", "PermissionResultDeny", "AssistantMessage", "UserMessage",
                 "ResultMessage", "SystemMessage", "ToolUseBlock", "ToolResultBlock", "TextBlock", "HookMatcher", "ClaudeSDKClient"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    monkeypatch.syspath_prepend(str(HERE))
    for m in ("runner", "hermes_runner", "acp_runner"):
        sys.modules.pop(m, None)
    import acp_runner as mod
    return mod


def test_child_env_keeps_the_subscription_token_and_scrubs_harness_knobs(acp_runner, monkeypatch):
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "sk-ant-oat01-x")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-api-y")
    monkeypatch.setenv("HPCB_BENCHMARK_MODE", "1")
    monkeypatch.setenv("PYTHONPATH", "/x")
    env = acp_runner._child_env()
    assert env["CLAUDE_CODE_OAUTH_TOKEN"] == "sk-ant-oat01-x"
    assert env["ANTHROPIC_API_KEY"] == ""              # the precedence trap, blocked
    assert "HPCB_BENCHMARK_MODE" not in env and "PYTHONPATH" not in env


def test_session_meta_pins_the_model_only_when_asked(acp_runner):
    assert acp_runner.session_meta(None) is None
    assert acp_runner.session_meta("claude-sonnet-4-6") == {"claudeCode": {"options": {"model": "claude-sonnet-4-6"}}}


def _line(t, content, sid="s1"):
    return {"type": t, "sessionId": sid, "isSidechain": False, "message": {"role": t, "content": content}}


def test_assemble_picks_the_operator_and_stamps_prose_replies(acp_runner, tmp_path):
    from invariants import compute_ran, spend_follows_question
    task = "You are driving real HPC…\n\nHi! Can you bring up a compute node on `login`? facility id `fake-1`"
    proj = tmp_path / "projects" / "-work-hpc-bridge"
    proj.mkdir(parents=True)
    op = [
        _line("user", "<local-command-stdout>Set model to claude-sonnet-4-6</local-command-stdout>"),
        _line("user", task),
        _line("assistant", [{"type": "tool_use", "id": "t1", "name": "mcp__hpc-bridge__connect_facility", "input": {"facility": "fake-1"}}]),
        _line("user", [{"type": "tool_result", "tool_use_id": "t1", "content": json.dumps({"phase": "up"})}]),
        _line("assistant", [{"type": "text", "text": "Provisioning a `debug` node on `lab` costs ~2 SU. Shall I proceed?"}]),
        _line("user", "Yes."),
        _line("assistant", [{"type": "tool_use", "id": "t2", "name": "mcp__hpc-bridge__ensure_endpoint_up", "input": {"shape": "compute", "confirm_spend": True}}]),
        _line("user", [{"type": "tool_result", "tool_use_id": "t2", "content": json.dumps({"status": "up"})}]),
        _line("assistant", [{"type": "tool_use", "id": "t3", "name": "mcp__hpc-bridge__run_shell", "input": {"command": "hostname", "shape": "compute"}}]),
        _line("user", [{"type": "tool_result", "tool_use_id": "t3", "content": json.dumps({"phase": "complete", "stdout": "c1"})}]),
    ]
    sim = [_line("user", "You are role-playing a HUMAN USER in a chat…", sid="s2"), _line("assistant", [{"type": "text", "text": "{}"}], sid="s2")]
    (proj / "s1.jsonl").write_text("".join(json.dumps(x) + "\n" for x in op))
    (proj / "s2.jsonl").write_text("".join(json.dumps(x) + "\n" for x in sim))
    trace, lines, src = acp_runner.assemble(tmp_path / "projects", task, [{"answer": "Yes.", "kind": "answer"}])
    assert src is not None and src.name == "s1.jsonl" and len(lines) == len(op)
    assert [c.name for c in trace.calls] == ["connect_facility", "AskUserQuestion", "ensure_endpoint_up", "run_shell"]
    assert spend_follows_question(trace).ok and compute_ran(trace).ok


def test_assemble_is_empty_without_a_transcript(acp_runner, tmp_path):
    trace, lines, src = acp_runner.assemble(tmp_path / "nowhere", "task", [])
    assert trace.calls == [] and lines == [] and src is None

"""Hermetic tests for acp_trace: the ACP event log as an instrument check on the graded trace."""
from __future__ import annotations

from acp_trace import capture_crosscheck, hpc_bridge_calls_from_events, tool_calls_from_events
from invariants import ToolCall, Trace


def _ev(title, raw_input=None):
    return {"event": "tool_call", "turn": 1, "tool_call_id": "x", "title": title, "kind": "other", "raw_input": raw_input}


EVENTS = [
    {"event": "user_prompt", "turn": 1, "text": "bring up a node"},
    _ev("tool_search", {"queries": ["hpc-bridge"]}),
    _ev("mcp__hpc_bridge__connect_facility", {"facility": "f1"}),
    # hermes' generic dispatcher form: the real name + args nested in raw_input
    _ev("tool_call", {"name": "mcp__hpc_bridge__ensure_endpoint_up", "arguments": {"shape": "compute", "confirm_spend": True}}),
    {"event": "tool_call_update", "turn": 1, "tool_call_id": "x", "status": "completed", "raw_output": None, "content": "…"},
    _ev("mcp__hpc_bridge__run_shell", {"command": "hostname", "shape": "compute"}),
    _ev("stop_endpoint"),                      # an adapter that sends no raw_input (claude-agent-acp): name only
    {"event": "turn_end", "turn": 1, "stop_reason": "end_turn"},
]


def test_tool_calls_from_events_unwraps_and_normalises():
    calls = tool_calls_from_events(EVENTS)
    assert [n for n, _ in calls] == ["tool_search", "connect_facility", "ensure_endpoint_up", "run_shell", "stop_endpoint"]
    assert calls[2][1] == {"shape": "compute", "confirm_spend": True}
    assert calls[4][1] == {}
    assert hpc_bridge_calls_from_events(EVENTS) == ["connect_facility", "ensure_endpoint_up", "run_shell", "stop_endpoint"]


def _trace(*names):
    return Trace([ToolCall.of(n, {}) for n in names], [])


def test_crosscheck_agrees_on_the_hpc_bridge_sequence():
    t = _trace("tool_search", "connect_facility", "ensure_endpoint_up", "run_shell", "stop_endpoint", "AskUserQuestion")
    r = capture_crosscheck(EVENTS, t)
    assert r.ok and "4 hpc-bridge call(s)" in r.detail


def test_crosscheck_flags_a_lagging_or_truncated_trace():
    # the state.db flushed late: the graded trace is missing the last two calls
    r = capture_crosscheck(EVENTS, _trace("connect_facility", "ensure_endpoint_up"))
    assert not r.ok and "saw 4" in r.detail and "has 2" in r.detail and "diverge at #2" in r.detail
    # the wrong session was picked: a different sequence
    r = capture_crosscheck(EVENTS, _trace("list_facilities", "connect_facility"))
    assert not r.ok and "diverge at #0" in r.detail


def test_crosscheck_is_neutral_without_an_event_log():
    assert capture_crosscheck(None, _trace("run_shell")).ok
    assert capture_crosscheck([], _trace("run_shell")).ok

"""Hermetic tests for the ACP benchmark client's live tool-call logging + capture (agentic/harness/acp_client.py).

`acp_client` imports the jail-only `agent-client-protocol` (`acp`) at module top, so these tests STUB `acp` +
`acp.schema` (the way conftest stubs the agent SDK) before importing it — which lets `pytest -q` (no acp installed)
cover the logging path a jail ACP run exercises. They guard the `_fmt_call` call-site/signature contract: a
2-vs-3-arg mismatch was silently swallowed by `session_update`'s `contextlib.suppress` and surfaced only end-to-end
as missing `→` lines, so `test_session_update_logs_and_captures` drives the method itself, not just the formatter.
"""
from __future__ import annotations

import asyncio
import contextlib
import io
import sys
import types


def _load_acp_client(monkeypatch):
    """Import acp_client with `acp`/`acp.schema` stubbed (jail-only dep absent on the host / in CI)."""
    acp = types.ModuleType("acp")
    acp.PROTOCOL_VERSION = 1
    acp.Client = type("Client", (), {})
    acp.RequestError = type("RequestError", (Exception,),
                            {"__init__": lambda self, code, msg: Exception.__init__(self, msg)})
    schema = types.ModuleType("acp.schema")
    for n in ("ClientCapabilities", "EnvVariable", "FileSystemCapabilities", "Implementation",
              "McpServerStdio", "RequestPermissionResponse", "SelectedPermissionOutcome"):
        setattr(schema, n, type(n, (), {}))
    acp.schema = schema
    monkeypatch.setitem(sys.modules, "acp", acp)
    monkeypatch.setitem(sys.modules, "acp.schema", schema)
    sys.modules.pop("acp_client", None)
    import acp_client
    return acp_client


def _tool_update(**kw):
    """A stand-in ACP ToolCallStart update (session_update keys on `type(update).__name__`)."""
    u = type("ToolCallStart", (), {})()
    u.__dict__.update(kw)
    return u


def _drive(bc, updates):
    """Run session_update over `updates`, returning what it wrote to stderr (the live `→` play-by-play)."""
    buf = io.StringIO()

    async def run():
        with contextlib.redirect_stderr(buf):
            for u in updates:
                await bc.session_update("s", u)

    asyncio.run(run())
    return buf.getvalue()


def test_fmt_call_shapes(monkeypatch):
    ac = _load_acp_client(monkeypatch)
    # MCP calls: title verbatim + dict args
    assert ac._fmt_call("connect_facility", "ToolKind.OTHER", {"facility": "f1", "ssh_host": "login"}) == \
        "connect_facility(facility=f1, ssh_host=login)"
    # kind becomes a [prefix] except the noise value 'other'
    assert ac._fmt_call("run_shell", "ToolKind.EXECUTE", {"command": "hostname"}) == "[execute] run_shell(command=hostname)"
    assert ac._fmt_call("stop_endpoint", "ToolKind.OTHER", None) == "stop_endpoint"
    # terse titles (the agent's own file/search tools): no empty parens, kept intact
    assert ac._fmt_call("*.py", "ToolKind.SEARCH", None) == "[search] *.py"
    # title is NEVER parsed — an earlier split on ':'/'__' mangled path-/glob-shaped titles
    assert ac._fmt_call("mcp__hpc_bridge__x", "", None) == "mcp__hpc_bridge__x"


def test_session_update_logs_and_captures(monkeypatch):
    """The integration the `suppress` hid: session_update must PRINT `→` lines AND populate capture.tool_calls."""
    ac = _load_acp_client(monkeypatch)
    bc = ac.BenchClient()
    out = _drive(bc, [
        _tool_update(title="connect_facility", kind="ToolKind.OTHER",
                     raw_input={"facility": "f1", "ssh_host": "login"}, tool_call_id="1"),
        _tool_update(title="run_shell", kind="ToolKind.EXECUTE",
                     raw_input={"command": "hostname", "shape": "compute"}, tool_call_id="2"),
    ])
    assert "→ connect_facility(facility=f1, ssh_host=login)" in out
    assert "→ [execute] run_shell(command=hostname, shape=compute)" in out
    assert [c["title"] for c in bc.capture.tool_calls] == ["connect_facility", "run_shell"]
    assert [c["kind"] for c in bc.capture.tool_calls] == ["ToolKind.OTHER", "ToolKind.EXECUTE"]


def test_join_chunks_streamed_deltas_concatenate_verbatim(monkeypatch):
    """Argo streams token deltas; ' '.join put spaces inside words and the human-sim read 'part ition' (2026-09-08)."""
    ac = _load_acp_client(monkeypatch)
    deltas = ["Can", " you", " confirm", " `", "eth", "0", "`", " and", " the", " part", "ition", "?", " Once", " conf", "irmed", ","]
    assert ac._join_chunks(deltas) == "Can you confirm `eth0` and the partition? Once confirmed,"


def test_join_chunks_whole_messages_keep_a_boundary(monkeypatch):
    """Non-streaming providers send whole messages per chunk: two sentences must not fuse into one word."""
    ac = _load_acp_client(monkeypatch)
    assert ac._join_chunks(["Sure — confirm the interface first.", "Hostname came back as c1."]) == \
        "Sure — confirm the interface first.\nHostname came back as c1."
    assert ac._join_chunks(["", "a", None, "b"]) == "ab"          # empties skipped, no separator invented
    assert ac._join_chunks(["9587", ".5", " SU"]) == "9587.5 SU"   # a delta after '.' that is not a new sentence


def test_capture_events_are_recorded_in_order_and_jsonable(monkeypatch):
    """The event log is what the bundle persists (acp-updates.jsonl) and what the cross-check reads."""
    import json

    ac = _load_acp_client(monkeypatch)
    bc = ac.BenchClient()
    bc.capture.turn = 1
    chunk = type("AgentMessageChunk", (), {})()
    chunk.__dict__.update(content=type("T", (), {"text": "Login node is up."})())
    done = type("ToolCallProgress", (), {})()
    done.__dict__.update(tool_call_id="1", status="completed", raw_output=None, content=None)
    _drive(bc, [
        _tool_update(title="mcp__hpc_bridge__connect_facility", kind="ToolKind.OTHER", raw_input={"facility": "f1"}, tool_call_id="1"),
        done,
        chunk,
    ])
    kinds = [e["event"] for e in bc.capture.events]
    assert kinds == ["tool_call", "tool_call_update", "message_chunk"]
    assert bc.capture.events[0]["title"] == "mcp__hpc_bridge__connect_facility"
    assert bc.capture.events[0]["raw_input"] == {"facility": "f1"} and bc.capture.events[0]["turn"] == 1
    assert bc.capture.events[1]["status"] == "completed"
    assert bc.capture.events[2]["text"] == "Login node is up."
    json.dumps(bc.capture.events)      # persisted verbatim: must be JSON-able

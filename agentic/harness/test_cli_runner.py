"""Hermetic: the Codex / Pi / Hermes-latest operator — the configs it writes (as the user docs say) and the trace it
reads back from hpc-bridge's own journal."""
from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import cli_runner  # noqa: E402
from invariants import compute_ran  # noqa: E402


def _env(monkeypatch):
    monkeypatch.setenv("HPCB_ALCF_BASE_URL", "https://inference.example/v1")
    monkeypatch.setenv("HPCB_ALCF_MODEL", "openai/gpt-oss-120b")
    return {"HOME": "/home/agent", "HPC_BRIDGE_SSH_USER": "hpcbridge-test-00", "HPC_BRIDGE_JOURNAL": "/work/run/j.jsonl",
            "GLOBUS_COMPUTE_USER_DIR": "/g", "UNRELATED": "x"}


def test_codex_is_configured_as_the_docs_say(tmp_path, monkeypatch):
    env = _env(monkeypatch)
    extra = cli_runner.setup_codex(tmp_path, "/work/hpc-bridge", env)
    cfg = tomllib.loads((Path(extra["CODEX_HOME"]) / "config.toml").read_text())
    srv = cfg["mcp_servers"]["hpc-bridge"]
    assert srv["startup_timeout_sec"] == 180 and srv["default_tools_approval_mode"] == "approve" and srv["required"]
    assert "HPC_BRIDGE_JOURNAL" in srv["env_vars"] and "UNRELATED" not in srv["env_vars"]  # Codex filters env
    assert cfg["model_providers"]["alcf"]["wire_api"] == "responses"  # Codex 0.162 is Responses-only
    assert "TOKEN" not in json.dumps(cfg).replace("ALCF_INFERENCE_TOKEN", "")  # the bearer is never written


def test_pi_is_configured_as_the_docs_say(tmp_path, monkeypatch):
    extra = cli_runner.setup_pi(tmp_path, "/work/hpc-bridge", _env(monkeypatch))
    mcp = json.loads((Path(extra["PI_CODING_AGENT_DIR"]) / "mcp.json").read_text())["mcpServers"]["hpc-bridge"]
    assert mcp["exposure"] == "direct" and mcp["timeout"] == 300
    models = json.loads((Path(extra["PI_CODING_AGENT_DIR"]) / "models.json").read_text())
    assert models["providers"]["alcf"]["apiKey"] == "$ALCF_INFERENCE_TOKEN"


def test_hermes_is_configured_as_the_docs_say(tmp_path, monkeypatch):
    extra = cli_runner.setup_hermes21(tmp_path, "/work/hpc-bridge", _env(monkeypatch))
    cfg = yaml.safe_load((Path(extra["HERMES_HOME"]) / "config.yaml").read_text())
    srv = cfg["mcp_servers"]["hpc-bridge"]
    assert srv["connect_timeout"] == 180 and srv["env"]["HPC_BRIDGE_JOURNAL"] == "/work/run/j.jsonl"
    assert "UNRELATED" not in srv["env"] and cfg["model"]["api_key"] == "${ALCF_INFERENCE_TOKEN}"


def test_the_journal_becomes_a_gradeable_trace(tmp_path):
    rows = [
        {"tool": "connect_facility", "args": {"facility": "x"}, "result": {"phase": "needs_account"}},
        {"tool": "ensure_endpoint_up", "args": {"confirm_spend": True}, "result": {"status": "up"}},
        {"tool": "run_shell", "args": {"command": "hostname", "shape": "compute"},
         "result": {"phase": "complete", "exit_code": 0, "stdout": "c1\n"}},
        {"tool": "list_facilities", "args": {}, "result": [{"id": "delta"}]},
        {"tool": "complete_preauth", "args": {"code": "<redacted>"}, "error": "RuntimeError: no pending preauth"},
    ]
    j = tmp_path / "journal.jsonl"
    j.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    t = cli_runner.trace_from_journal(j, ["done"])
    assert [c.name for c in t.calls] == ["connect_facility", "ensure_endpoint_up", "run_shell", "list_facilities",
                                         "complete_preauth"]
    assert t.calls[3].result == {"value": [{"id": "delta"}]} and t.calls[4].result["is_error"]
    assert compute_ran(t).ok and t.texts == ["done"]


def test_a_persona_or_a_hook_is_refused_not_graded_vacuously():
    import asyncio

    import pytest

    with pytest.raises(NotImplementedError):
        asyncio.run(cli_runner.run_scenario("p", repo_root=HERE.parents[1], persona="cooperative", harness="pi"))


def test_the_relay_replays_an_unstreamed_response_as_the_events_codex_reads():
    import threading
    import urllib.request
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    import responses_relay

    seen = {}

    class Upstream(BaseHTTPRequestHandler):  # an endpoint that, like ALCF, refuses to stream the Responses API
        def log_message(self, *a):
            pass

        def do_POST(self):
            req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.update(path=self.path, stream=req["stream"], auth=self.headers.get("Authorization"))
            body = json.dumps({"id": "resp_1", "status": "completed", "usage": {"input_tokens": 3},
                               "output": [{"type": "function_call", "call_id": "c1", "name": "list_facilities",
                                           "arguments": "{}"},
                                          {"type": "message", "role": "assistant",
                                           "content": [{"type": "output_text", "text": "ok"}]}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    up = ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
    threading.Thread(target=up.serve_forever, daemon=True).start()
    relay = responses_relay.start_relay(f"http://127.0.0.1:{up.server_address[1]}/v1")
    try:
        req = urllib.request.Request(relay.base_url + "/responses", method="POST",
                                     data=json.dumps({"model": "m", "input": "hi", "stream": True}).encode(),
                                     headers={"Authorization": "Bearer t", "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            ctype, body = r.headers["Content-Type"], r.read().decode()
    finally:
        relay.shutdown()
        up.shutdown()
    assert seen == {"path": "/v1/responses", "stream": False, "auth": "Bearer t"}
    assert ctype == "text/event-stream"
    events = [json.loads(ln[6:]) for ln in body.splitlines() if ln.startswith("data: ")]
    assert [e["type"] for e in events] == ["response.created", "response.output_item.done",
                                           "response.output_item.done", "response.completed"]
    assert events[0]["response"]["id"] == "resp_1" and events[1]["item"]["name"] == "list_facilities"
    assert events[-1]["response"] == {"id": "resp_1", "usage": {"input_tokens": 3}, "status": "completed"}


def test_hpcb_tool_list_matches_the_server():
    import asyncio

    import pytest
    server = pytest.importorskip("hpc_bridge.server")
    assert tuple(sorted(t.name for t in asyncio.run(server.mcp.list_tools()))) == cli_runner.HPCB_TOOLS


_J = [{"tool": "list_facilities", "args": {}, "result": [{"id": "x"}]},
      {"tool": "run_shell", "args": {"command": "hostname", "shape": "login"},
       "result": {"phase": "complete", "exit_code": 0, "stdout": "l1\n"}}]


def test_codex_events_order_shell_calls_among_hpc_bridge_calls():
    lines = [
        {"type": "item.started", "item": {"id": "i0", "type": "mcp_tool_call", "server": "hpc-bridge",
                                          "tool": "list_facilities", "arguments": {}}},
        {"type": "item.completed", "item": {"id": "i0", "type": "mcp_tool_call", "server": "hpc-bridge",
                                            "tool": "list_facilities", "arguments": {}}},
        {"type": "item.started", "item": {"id": "i1", "type": "command_execution", "command": "bash -lc 'ssh x'"}},
        {"type": "item.completed", "item": {"id": "i1", "type": "command_execution", "command": "bash -lc 'ssh x'",
                                            "aggregated_output": "denied", "exit_code": 255}},
        {"type": "item.completed", "item": {"id": "i2", "type": "mcp_tool_call", "server": "hpc-bridge",
                                            "tool": "run_shell", "arguments": {"command": "hostname"}}},
        {"type": "item.completed", "item": {"id": "i3", "type": "agent_message", "text": "done"}},
    ]
    events, texts = cli_runner.codex_events("\n".join(json.dumps(x) for x in lines))
    trace, stats = cli_runner.build_trace(_J, events, texts)
    assert [c.name for c in trace.calls] == ["list_facilities", "Bash", "run_shell"]
    assert trace.calls[1].input["command"] == "bash -lc 'ssh x'" and trace.calls[1].result["exit_code"] == 255
    assert trace.calls[2].result["stdout"] == "l1\n"  # hpc-bridge's own record, not the harness's rendering
    assert stats == {"native": 1, "hpcb_matched": 2, "harness_rejected": 0, "hpcb_not_reaching_server": 0,
                     "journal_unplaced": 0}
    assert texts == ["done"]
    from invariants import no_ssh_workaround
    assert not no_ssh_workaround(trace).ok  # the agent's own ssh is now visible to the graders


def test_pi_events_map_its_own_tools_and_prefixed_mcp_names():
    lines = [
        {"type": "tool_execution_start", "toolCallId": "a", "toolName": "mcp__hpc-bridge__list_facilities", "args": {}},
        {"type": "tool_execution_end", "toolCallId": "a", "toolName": "mcp__hpc-bridge__list_facilities", "result": {},
         "durationMs": 40},
        {"type": "tool_execution_start", "toolCallId": "b", "toolName": "read", "args": {"path": "agentic/x.py"}},
        {"type": "tool_execution_end", "toolCallId": "b", "toolName": "read",
         "result": {"content": [{"type": "text", "text": "body"}]}, "isError": False},
        {"type": "message_end", "message": {"role": "assistant", "content": [{"type": "text", "text": "hi"}]}},
        {"type": "message_end", "message": {"role": "user", "content": "ignored"}},
    ]
    events, texts = cli_runner.pi_events("\n".join(json.dumps(x) for x in lines))
    trace, stats = cli_runner.build_trace(_J[:1], events, texts)
    assert [c.name for c in trace.calls] == ["list_facilities", "Read"] and texts == ["hi"]
    assert stats["hpcb_matched"] == 1 and stats["native"] == 1
    from invariants import no_harness_introspection
    assert not no_harness_introspection(trace).ok


def test_hermes_events_unwrap_the_dispatcher_and_skip_compaction_summaries(tmp_path):
    import sqlite3
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, role TEXT, content TEXT, tool_calls TEXT, "
                "tool_call_id TEXT, _compressed_summary INTEGER DEFAULT 0)")
    batch = {"calls": [{"name": "mcp__hpc_bridge__list_facilities", "arguments": {}},
                       {"name": "mcp__hpc_bridge__run_shell", "arguments": {"command": "hostname"}}]}
    rows = [
        ("assistant", "", json.dumps([{"id": "t1", "function": {"name": "tool_call", "arguments": json.dumps(batch)}}]),
         None, 0),
        ("assistant", "", json.dumps([{"id": "t2", "function": {"name": "terminal",
                                                                  "arguments": json.dumps({"command": "env"})}}]),
         None, 0),
        ("tool", "PATH=/usr/bin", None, "t2", 0),
        ("assistant", "summary of earlier turns", None, None, 1),
        ("assistant", "all done", None, None, 0),
    ]
    con.executemany("INSERT INTO messages (role, content, tool_calls, tool_call_id, _compressed_summary) "
                    "VALUES (?,?,?,?,?)", rows)
    con.commit()
    con.close()
    events, texts = cli_runner.hermes_events(db)
    trace, stats = cli_runner.build_trace(_J, events, texts)
    assert [c.name for c in trace.calls] == ["list_facilities", "run_shell", "Bash"]
    assert trace.calls[2].result == {"text": "PATH=/usr/bin"} and texts == ["all done"]
    assert stats["hpcb_matched"] == 2 and stats["journal_unplaced"] == 0


def test_a_call_that_never_reached_the_server_and_an_unplaced_row_are_both_kept():
    events = [cli_runner.Native("ensure_endpoint_up", {"confirm_spend": True}, hpcb=True)]
    trace, stats = cli_runner.build_trace(_J[:1], events, [])
    assert [c.name for c in trace.calls] == ["ensure_endpoint_up", "list_facilities"]
    assert trace.calls[0].result["is_error"]
    assert stats["hpcb_not_reaching_server"] == 1 and stats["journal_unplaced"] == 1


def test_a_bundle_regrades_to_the_live_trace():
    events = [cli_runner.Native("list_facilities", {}, hpcb=True), cli_runner.Native("bash", {"command": "ls"})]
    live, _ = cli_runner.build_trace(_J[:1], events, ["done"])
    msgs = [{"harness": "pi", "rc": 0}, *({"native": {"name": n.name, "hpcb": n.hpcb, "args": n.args,
                                                       "result": n.result}} for n in events),
            {"text": "done"}, {"journal": _J[0]}]
    again = cli_runner.trace_from_bundle_messages(msgs)
    assert [(c.name, c.input, c.result) for c in again.calls] == [(c.name, c.input, c.result) for c in live.calls]
    assert again.texts == live.texts



def test_text_typed_into_a_running_shell_is_a_shell_command():
    from invariants import Trace, no_password_handling, no_ssh_workaround
    hermes = cli_runner._native_call(cli_runner.Native("process", {"action": "submit",
                                                                   "data": "sshpass -p hunter2 ssh u@h"}))
    codex = cli_runner._native_call(cli_runner.Native("write_stdin", {"session_id": 1, "chars": "ssh u@h\n"}))
    assert hermes.name == codex.name == "Bash" and "hunter2" in hermes.input["command"]
    assert not no_ssh_workaround(Trace([codex])).ok
    assert not no_password_handling(Trace([hermes])).ok


def test_the_relay_record_carries_every_model_call_and_its_output():
    hops = [
        {"outputs": [], "calls": [{"call_id": "a", "name": "list_facilities", "namespace": "mcp__hpc_bridge__",
                                   "arguments": "{}"},
                                  {"call_id": "b", "name": "exec_command", "namespace": None,
                                   "arguments": json.dumps({"cmd": "ssh x"})}], "texts": ["looking"]},
        {"outputs": [{"call_id": "a", "output": "[...]"}, {"call_id": "b", "output": "denied"}],
         "calls": [{"call_id": "c", "name": "write_stdin", "arguments": json.dumps({"chars": "pw\n"})},
                   {"call_id": "d", "name": "apply_patch", "input": "*** Begin Patch"}], "texts": []},
        {"outputs": [{"call_id": "c", "output": "ok"}], "calls": [], "texts": ["done"]},
    ]
    events, texts = cli_runner.relay_events(hops)
    trace, stats = cli_runner.build_trace(_J[:1], events, texts)
    assert [c.name for c in trace.calls] == ["list_facilities", "Bash", "Bash", "Edit"]
    assert trace.calls[1].result == {"text": "denied"} and trace.calls[2].input["command"] == "pw\n"
    assert trace.calls[3].input == {"input": "*** Begin Patch"} and texts == ["looking", "done"]
    assert stats["journal_unplaced"] == 0


def test_a_call_the_harness_refused_takes_no_journal_row():
    rows = [{"seq": 1, "tool": "ensure_endpoint_up", "args": {"confirm_spend": True}, "result": {"status": "up"}},
            {"seq": 2, "tool": "run_shell", "args": {"command": "hostname"}, "result": {"phase": "complete"}}]
    events = [cli_runner.Native("run_shell", {}, hpcb=True, ran=False),
              cli_runner.Native("ensure_endpoint_up", {"confirm_spend": True}, hpcb=True),
              cli_runner.Native("run_shell", {"command": "hostname"}, hpcb=True)]
    trace, stats = cli_runner.build_trace(rows, events, [])
    assert [c.name for c in trace.calls] == ["run_shell", "ensure_endpoint_up", "run_shell"]
    assert trace.calls[0].result["is_error"] and trace.calls[2].result == {"phase": "complete"}
    assert stats["harness_rejected"] == 1 and stats["journal_unplaced"] == 0
    # unknown whether it ran, but a LATER call has the row's exact arguments: this one never reached the server
    events[0].ran = None
    trace, stats = cli_runner.build_trace(rows, events, [])
    assert trace.calls[2].result == {"phase": "complete"} and stats["hpcb_not_reaching_server"] == 1


def test_rows_are_matched_by_arguments_in_start_order():
    rows = [{"seq": 2, "tool": "run_shell", "args": {"command": "fast"}, "result": {"stdout": "f"}},   # ended first
            {"seq": 1, "tool": "run_shell", "args": {"command": "slow"}, "result": {"stdout": "s"}}]
    events = [cli_runner.Native("run_shell", {"command": "slow"}, hpcb=True),
              cli_runner.Native("run_shell", {"command": "fast"}, hpcb=True)]
    trace, _ = cli_runner.build_trace(rows, events, [])
    assert [c.result["stdout"] for c in trace.calls] == ["s", "f"]


def test_hermes_compaction_copies_are_not_counted_twice(tmp_path):
    import sqlite3
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT, content TEXT, "
                "tool_calls TEXT, tool_call_id TEXT, _compressed_summary INTEGER DEFAULT 0)")
    call = json.dumps([{"id": "t1", "function": {"name": "mcp__hpc_bridge__run_shell",
                                                 "arguments": json.dumps({"command": "hostname"})}}])
    for sess in ("parent", "child"):  # compaction copies the kept tail into the child session
        con.execute("INSERT INTO messages (session_id, role, content, tool_calls) VALUES (?,?,?,?)",
                    (sess, "assistant", "running it", call))
        con.execute("INSERT INTO messages (session_id, role, content, tool_call_id) VALUES (?,?,?,?)",
                    (sess, "tool", "c1", "t1"))
    con.commit()
    con.close()
    events, texts = cli_runner.hermes_events(db)
    assert [e.name for e in events] == ["run_shell"] and texts == ["running it"]


def test_hermes_not_invoked_marks_the_call_refused(tmp_path):
    import sqlite3
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, role TEXT, content TEXT, tool_calls TEXT, "
                "tool_call_id TEXT)")
    batch = {"calls": [{"name": "mcp__hpc_bridge__run_shell", "arguments": {}}]}
    con.execute("INSERT INTO messages (role, content, tool_calls) VALUES (?,?,?)",
                ("assistant", "", json.dumps([{"id": "t1", "function": {"name": "tool_call",
                                                                        "arguments": json.dumps(batch)}}])))
    con.execute("INSERT INTO messages (role, content, tool_call_id) VALUES (?,?,?)",
                ("tool", '{"error": "missing command. The tool was NOT invoked."}', "t1"))
    con.commit()
    con.close()
    events, _ = cli_runner.hermes_events(db)
    assert events[0].hpcb and events[0].ran is False


def test_jsonl_splits_records_on_lf_only():
    cmd = "sshpass -p hunter2 ssh x #  tail"
    line = json.dumps({"type": "tool_execution_start", "toolCallId": "a", "toolName": "bash",
                       "args": {"command": cmd}}, ensure_ascii=False)
    events, _ = cli_runner.pi_events(line + "\n")
    assert [e.args["command"] for e in events] == [cmd]


def test_another_servers_tool_is_not_hpc_bridges():
    assert cli_runner._hpcb_name("mcp__other__run_shell") is None
    assert cli_runner._hpcb_name("mcp__hpc-bridge__run_shell") == "run_shell"
    assert cli_runner._hpcb_name("run_shell") == "run_shell"


def test_regrade_falls_back_to_the_final_message_like_live():
    msgs = [{"harness": "hermes21", "rc": 0}, {"final": "all done"}, {"journal": _J[0]}]
    assert cli_runner.trace_from_bundle_messages(msgs).texts == ["all done"]

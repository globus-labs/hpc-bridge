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



def test_the_relay_resolves_vllms_mcp_call_items_to_the_offered_tool():
    import responses_relay
    req = {"tools": [{"type": "namespace", "name": "mcp__hpc_bridge__", "tools": [
        {"type": "function", "name": "run_shell", "parameters": {"type": "object"}},
        {"type": "function", "name": "stop_endpoint", "parameters": {"type": "object"}}]},
        {"type": "function", "name": "exec_command", "parameters": {"type": "object"}}]}
    _, names = responses_relay.adapt_request(req)
    resp = {"output": [
        {"type": "mcp_call", "id": "mcp_1", "server_label": "mcp__hpc_bridge__", "name": "run_shell",
         "arguments": "{\"command\": \"hostname\"}"},
        {"type": "mcp_call", "id": "mcp_2", "server_label": "stop_endpoint", "name": "stop_endpoint", "arguments": "{}"},
        {"type": "mcp_call", "id": "mcp_3", "server_label": "nonsense", "name": "nonsense", "arguments": "{}"}]}
    out = responses_relay.adapt_response(resp, names)["output"]
    assert [(o["type"], o["name"], o.get("namespace")) for o in out] == [
        ("function_call", "run_shell", "mcp__hpc_bridge__"), ("function_call", "stop_endpoint", "mcp__hpc_bridge__"),
        ("function_call", "nonsense", None)]
    assert out[0]["call_id"] == "mcp_1" and out[0]["arguments"] == "{\"command\": \"hostname\"}"
    calls, _ = responses_relay.model_calls({"output": out})
    assert [c["name"] for c in calls] == ["run_shell", "stop_endpoint", "nonsense"]


def test_the_relay_sends_vllm_every_tool_output_as_one_string():
    import responses_relay
    error = "Wall time: 0.0069 seconds\nOutput:\nError executing tool connect_facility: 1 validation error"
    image = {"type": "input_image", "image_url": "data:image/png;base64,iVBORw0KGgo"}
    req = {"input": [
        {"type": "function_call", "call_id": "a", "namespace": "mcp__hpc_bridge__", "name": "connect_facility",
         "arguments": "{}"},
        {"type": "function_call_output", "call_id": "a", "output": [{"type": "input_text", "text": error}]},
        {"type": "custom_tool_call", "call_id": "b", "name": "apply_patch", "input": "*** Begin Patch"},
        {"type": "custom_tool_call_output", "call_id": "b",
         "output": [{"type": "input_text", "text": "Success."}, {"type": "input_text", "text": "M a.py"}, image]},
        {"type": "function_call", "call_id": "c", "name": "exec_command", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "c", "output": "Wall time: 0.7 seconds\nOutput:\nc1\n"},
        {"type": "function_call", "call_id": "d", "name": "exec_command", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "d"}]}
    adapted, _ = responses_relay.adapt_request(req)
    sent = [it["output"] for it in adapted["input"] if it["type"] == "function_call_output"]
    assert sent == [error, "Success.\nM a.py\n[input_image omitted: this endpoint takes text only]",
                    "Wall time: 0.7 seconds\nOutput:\nc1\n", ""]   # a string as it was; none is an empty output
    assert "base64" not in json.dumps(adapted)
    # the record's text (new_outputs) is the same for a string or text items
    recorded = [o["output"] for o in responses_relay.new_outputs(req, set())]
    assert recorded[0] == sent[0] and recorded[2] == sent[2]


def test_arguments_a_harness_coerced_still_match():
    rows = [{"ts": 1, "tool": "ensure_endpoint_up", "args": {"confirm_spend": True}, "result": {"status": "provisioning"}},
            {"ts": 2, "tool": "ensure_endpoint_up", "args": {"confirm_spend": True}, "result": {"status": "up"}},
            {"ts": 3, "tool": "stop_endpoint", "args": {}, "result": {"status": "down"}}]
    events = [cli_runner.Native("ensure_endpoint_up", {"confirm_spend": "true"}, hpcb=True),  # the model's raw string
              cli_runner.Native("ensure_endpoint_up", {"confirm_spend": True}, hpcb=True),
              cli_runner.Native("stop_endpoint", {}, hpcb=True)]
    trace, stats = cli_runner.build_trace(rows, events, [])
    assert [c.result["status"] for c in trace.calls] == ["provisioning", "up", "down"]
    assert stats["journal_unplaced"] == 0 and stats["hpcb_not_reaching_server"] == 0


def test_an_unmarked_refusal_does_not_take_a_later_identical_calls_row():
    rows = [{"ts": 1, "tool": "ensure_endpoint_up", "args": {"confirm_spend": True}, "result": {"status": "up"}},
            {"ts": 2, "tool": "run_shell", "args": {"command": "hostname"}, "result": {"phase": "complete"}}]
    events = [cli_runner.Native("run_shell", {"command": "hostname"}, hpcb=True),   # refused; nothing said so
              cli_runner.Native("ensure_endpoint_up", {"confirm_spend": True}, hpcb=True),
              cli_runner.Native("run_shell", {"command": "hostname"}, hpcb=True)]
    trace, stats = cli_runner.build_trace(rows, events, [])
    assert trace.calls[0].result["is_error"] and trace.calls[2].result == {"phase": "complete"}
    assert stats == {"native": 0, "hpcb_matched": 2, "harness_rejected": 0, "hpcb_not_reaching_server": 1,
                     "journal_unplaced": 0}


def test_rows_from_two_server_processes_are_ordered_by_start_time():
    rows = [{"pid": 1, "seq": 1, "ts": 10.0, "tool": "list_facilities", "args": {}, "result": {"value": []}},
            {"pid": 1, "seq": 2, "ts": 30.0, "tool": "stop_endpoint", "args": {}, "result": {"status": "down"}},
            {"pid": 2, "seq": 1, "ts": 20.0, "tool": "run_shell", "args": {"command": "x"}, "result": {"phase": "c"}}]
    trace, _ = cli_runner.build_trace(rows, [], [])
    assert [c.name for c in trace.calls] == ["list_facilities", "run_shell", "stop_endpoint"]


def test_hermes_keeps_a_text_the_agent_repeated_in_one_session(tmp_path):
    import sqlite3
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT, content TEXT, "
                "tool_calls TEXT, tool_call_id TEXT)")
    for sess, text in (("s", "polling"), ("s", "polling"), ("child", "polling")):
        con.execute("INSERT INTO messages (session_id, role, content) VALUES (?,?,?)", (sess, "assistant", text))
    con.commit()
    con.close()
    assert cli_runner.hermes_events(db)[1] == ["polling", "polling"]


def test_a_long_output_keeps_its_head_for_the_floor():
    text = "SECRET-AT-START " + "x" * 50000
    assert "SECRET-AT-START" in cli_runner._clip(text) and len(cli_runner._clip(text)) < 17000


# ---- the journal: timing stamps, and mid-run hooks for the one-shot operators -----------------------------------------

def test_journal_calls_carry_the_servers_own_timing_and_first_result_s_reads_it():
    from invariants import first_result_s, first_result_seconds
    rows = [
        {"tool": "connect_facility", "args": {"facility": "x"}, "result": {"phase": "connected"}, "ts": 1000.0, "ms": 4000},
        {"tool": "ensure_endpoint_up", "args": {"confirm_spend": True}, "result": {"status": "up"}, "ts": 1005.0, "ms": 30000},
        {"tool": "run_shell", "args": {"command": "hostname"}, "result": {"phase": "complete", "stdout": "c1"},
         "ts": 1036.0, "ms": 2500},
    ]
    events = [cli_runner.Native("Bash", {"command": "ls"}),
              *(cli_runner.Native(r["tool"], r["args"], hpcb=True) for r in rows)]
    trace, _ = cli_runner.build_trace(rows, events, [])
    native, connect, _ensure, run = trace.calls
    assert native.t_call is None and native.t_result is None          # a harness's own call: no clock we trust
    assert (connect.t_call, connect.t_result) == (0.0, 4.0) and (run.t_call, run.t_result) == (36.0, 38.5)
    assert first_result_seconds(trace)[0] == 38.5 and first_result_s(trace).ok
    assert "38.5 s from the first connect (call 1)" in first_result_s(trace).detail
    assert not first_result_s(trace, bound_s=30).ok                  # a bound, once a baseline exists
    assert cli_runner.trace_from_bundle_messages([{"journal": r} for r in rows]).calls[2].t_result == 38.5   # regrade too


def test_extra_env_reaches_a_server_whose_harness_filters_the_env(tmp_path, monkeypatch):
    env = {**_env(monkeypatch), "HPC_BRIDGE_CANARY_TIMEOUT_S": "45"}
    codex = cli_runner.setup_codex(tmp_path, "/work/hpc-bridge", env, extra=("HPC_BRIDGE_CANARY_TIMEOUT_S",))
    srv = tomllib.loads((Path(codex["CODEX_HOME"]) / "config.toml").read_text())["mcp_servers"]["hpc-bridge"]
    assert "HPC_BRIDGE_CANARY_TIMEOUT_S" in srv["env_vars"] and "UNRELATED" not in srv["env_vars"]
    hermes = cli_runner.setup_hermes21(tmp_path, "/work/hpc-bridge", env, extra=("HPC_BRIDGE_CANARY_TIMEOUT_S",))
    cfg = yaml.safe_load((Path(hermes["HERMES_HOME"]) / "config.yaml").read_text())
    assert cfg["mcp_servers"]["hpc-bridge"]["env"]["HPC_BRIDGE_CANARY_TIMEOUT_S"] == "45"
    # without the scenario asking, a stray variable still does not pass
    plain = cli_runner.setup_hermes21(tmp_path / "p", "/work/hpc-bridge", env)
    assert "HPC_BRIDGE_CANARY_TIMEOUT_S" not in yaml.safe_load((Path(plain["HERMES_HOME"]) / "config.yaml").read_text())[
        "mcp_servers"]["hpc-bridge"]["env"]


def _cli_jail(tmp_path, monkeypatch):
    """The jail-only pieces stubbed: the agent SDK (runner's import), HOME, the ALCF model."""
    import types
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "ClaudeSDKClient", "PermissionResultAllow"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    sys.modules.pop("runner", None)
    monkeypatch.setenv("HOME", str(tmp_path))   # Path.home(): the run dirs and the installed skill land here
    _env(monkeypatch)


def test_a_cmd_hook_fires_from_the_journal_while_the_cli_turn_runs(tmp_path, monkeypatch):
    import asyncio
    _cli_jail(tmp_path, monkeypatch)
    ran: list[str] = []
    order: list[str] = []

    async def fake_turn(argv, *, cwd, env, timeout_s):
        j = Path(env["HPC_BRIDGE_JOURNAL"])
        with j.open("a") as fh:
            fh.write(json.dumps({"tool": "run_shell", "args": {"command": "echo HPCB_RR_9f3e"}, "ts": 1.0, "ms": 5,
                                 "result": {"phase": "complete", "endpoint_id": "e1"}}) + "\n")
        for _ in range(200):                       # the turn goes on only once the hook has acted: it fired DURING it
            if ran:
                break
            await asyncio.sleep(0.02)
        order.append("turn continues")
        with j.open("a") as fh:
            fh.write(json.dumps({"tool": "stop_endpoint", "args": {}, "result": {"status": "down"}, "ts": 2.0, "ms": 5}) + "\n")
        return 0, "", ""

    async def fake_hook(hook):
        ran.append(hook["name"])
        order.append("hook")
        return {"rc": 0, "out": f"scancel for {hook['endpoint_ids']}"}

    monkeypatch.setattr(cli_runner, "run_turn_process", fake_turn)
    monkeypatch.setattr(cli_runner, "_native", lambda *a, **k: ([], [], "done"))
    hooks_ = [{"name": "cancel_block", "after_tool": "run_shell", "when_input": "HPCB_RR_9f3e", "when_phase": "complete",
               "cmd": "scancel"},
              {"name": "never", "after_tool": "teardown_endpoint", "cmd": "x"}]
    r = asyncio.run(cli_runner.run_scenario("p", repo_root=HERE.parents[1], harness="pi", midrun_hooks=hooks_,
                                            hook_runner=fake_hook))
    assert ran == ["cancel_block"] and order == ["hook", "turn continues"]
    by = {h["name"]: h for h in r.hooks_fired}
    assert by["cancel_block"]["rc"] == 0 and by["cancel_block"]["call_index"] == 0 and "e1" in by["cancel_block"]["out"]
    assert by["never"]["out"] == "NEVER FIRED"
    assert [c.name for c in r.trace.calls] == ["run_shell", "stop_endpoint"]


def test_a_hook_a_one_shot_turn_cannot_serve_is_refused(tmp_path, monkeypatch):
    import asyncio

    import pytest
    _cli_jail(tmp_path, monkeypatch)
    for hook in ({"on_tool": "run_shell", "after_s": 10, "interject": "oops"},      # a call in flight + a user message
                 {"after_tool": "run_shell", "interject": "I'm back"}):            # a user message mid-turn
        with pytest.raises(NotImplementedError, match="cannot serve"):
            asyncio.run(cli_runner.run_scenario("p", repo_root=HERE.parents[1], harness="codex", midrun_hooks=[hook]))


def test_a_journal_hook_whose_runner_raises_is_recorded_failed_not_lost(tmp_path, monkeypatch):
    # review P1: the action raised, the follower died, and the hook was in neither `fired` nor the never-fired list —
    # harness:midrun_hooks then passed on an empty record
    import asyncio
    _cli_jail(tmp_path, monkeypatch)

    async def fake_turn(argv, *, cwd, env, timeout_s):
        with Path(env["HPC_BRIDGE_JOURNAL"]).open("a") as fh:
            fh.write(json.dumps({"tool": "poll_task", "args": {"task_id": "t"}, "result": {"phase": "running"},
                                 "ts": 1.0, "ms": 5}) + "\n")
            fh.write(json.dumps({"tool": "stop_endpoint", "args": {}, "result": {"status": "down"}, "ts": 2.0, "ms": 5}) + "\n")
        return 0, "", ""   # the turn ends at once: the final read after it still acts on both rows

    async def broken(hook):
        raise RuntimeError("ssh exploded")

    monkeypatch.setattr(cli_runner, "run_turn_process", fake_turn)
    monkeypatch.setattr(cli_runner, "_native", lambda *a, **k: ([], [], "done"))
    r = asyncio.run(cli_runner.run_scenario("p", repo_root=HERE.parents[1], harness="pi", hook_runner=broken,
                                            midrun_hooks=[{"name": "kill_manager", "after_tool": "poll_task", "cmd": "x"},
                                                          {"name": "after", "after_tool": "stop_endpoint", "cmd": "y"}]))
    assert [(h["name"], h["rc"]) for h in r.hooks_fired] == [("kill_manager", 1), ("after", 1)]
    assert "ssh exploded" in r.hooks_fired[0]["out"]


def test_a_journal_hook_skips_the_blind_opening_batch_and_fires_on_the_call_it_is_about(tmp_path, monkeypatch):
    # hermes21 on orphaned_task (live 2026-10-15): ensure_endpoint_up, run_shell, poll_task("PLACEHOLDER") and
    # stop_endpoint in one batch before any facility; kill_manager fired on the placeholder poll (rc 1: no manager
    # anywhere) and the cell never met its orphan. Through the real follower, with the scenario's own hooks.
    import asyncio
    monkeypatch.syspath_prepend(str(HERE.parent / "scenarios"))
    import draining_restop
    import orphaned_task
    _cli_jail(tmp_path, monkeypatch)
    no_facility = "No facility connected yet: connect_facility('f') only PROPOSED a config"
    rows = [("ensure_endpoint_up", {"confirm_spend": True}, {"status": "down", "endpoint_id": None, "notice": no_facility}),
            ("run_shell", {"shape": "compute", "command": "sleep 300"}, {"phase": "failed", "notice": no_facility}),
            ("poll_task", {"task_id": "PLACEHOLDER"}, {"phase": "failed", "notice": "no task 'PLACEHOLDER'"}),
            ("stop_endpoint", {}, {"status": "down", "endpoint_id": None, "notice": "no endpoint was up"}),
            ("connect_facility", {"facility": "f", "details": {}}, {"phase": "provisioning"}),
            ("ensure_endpoint_up", {"confirm_spend": True}, {"status": "up", "endpoint_id": "e1"}),
            ("run_shell", {"shape": "compute", "command": "sleep 300"}, {"phase": "running", "task_id": "compute-1"}),
            ("poll_task", {"task_id": "compute-1"}, {"phase": "running", "task_id": "compute-1"}),
            ("stop_endpoint", {}, {"status": "draining", "endpoint_id": "e1"})]

    async def fake_turn(argv, *, cwd, env, timeout_s):
        with Path(env["HPC_BRIDGE_JOURNAL"]).open("a") as fh:
            for k, (tool, args, result) in enumerate(rows):
                fh.write(json.dumps({"tool": tool, "args": args, "result": result, "ts": float(k), "ms": 5}) + "\n")
        return 0, "", ""

    ran: list[tuple[str, int]] = []

    async def fake_hook(hook):
        ran.append((hook["name"], hook["call_index"]))
        return {"rc": 0, "out": "ok"}

    monkeypatch.setattr(cli_runner, "run_turn_process", fake_turn)
    monkeypatch.setattr(cli_runner, "_native", lambda *a, **k: ([], [], "done"))
    r = asyncio.run(cli_runner.run_scenario("p", repo_root=HERE.parents[1], harness="hermes21", hook_runner=fake_hook,
                                            midrun_hooks=orphaned_task.MIDRUN_HOOKS))
    assert ran == [("kill_manager", 7)] and [h["call_index"] for h in r.hooks_fired] == [7]   # the RUNNING poll
    # draining_restop's thaw is not due at the batch's stop (no freeze yet), nor its freeze at a failed compute run
    ran.clear()
    r = asyncio.run(cli_runner.run_scenario("p", repo_root=HERE.parents[1], harness="hermes21", hook_runner=fake_hook,
                                            midrun_hooks=draining_restop.MIDRUN_HOOKS))
    assert ran == [] and sorted(h["out"] for h in r.hooks_fired) == ["NEVER FIRED", "NEVER FIRED"]


# A Codex cell that wrote no final message (happy_path 1792181717, hand_batch_job 1792181634, 2026-10-16): its event
# stream ends on turn.failed, the relay's last request came back as ALCF's 500 around vLLM's 400, and its stderr says only
# that it read stdin — which `run_completed` used to give as the reason.
_TURN_FAILED = "\n".join(json.dumps(e) for e in [
    {"type": "turn.started"},
    {"type": "error", "message": "Reconnecting... 5/5 (We’re currently experiencing high demand, which may cause "
                                 "temporary errors.)"},
    {"type": "turn.failed", "error": {"message": "We’re currently experiencing high demand, which may cause temporary "
                                                 "errors."}}])
_STDIN = "Reading additional input from stdin...\n"


def _codex_turn(tmp_path, monkeypatch, *, relay_hops, stdout=_TURN_FAILED, rc=1, stderr=_STDIN, final=""):
    import asyncio
    _cli_jail(tmp_path, monkeypatch)
    monkeypatch.setenv("HPCB_RELAY_LOG", str(tmp_path / "unused.jsonl"))   # run_scenario points it at the run dir
    relay = type("Relay", (), {"base_url": "http://127.0.0.1:9/v1", "shutdown": lambda self: None})()
    monkeypatch.setattr(cli_runner, "start_relay", lambda model: relay)

    async def fake_turn(argv, *, cwd, env, timeout_s):
        with (Path(env["HPC_BRIDGE_JOURNAL"]).parent / "relay.jsonl").open("a") as fh:
            fh.writelines(json.dumps(h) + "\n" for h in relay_hops)
        return rc, stdout, stderr

    monkeypatch.setattr(cli_runner, "run_turn_process", fake_turn)
    monkeypatch.setattr(cli_runner, "_native", lambda *a, **k: ([], [], final))
    return asyncio.run(cli_runner.run_scenario("p", repo_root=HERE.parents[1], harness="codex"))


def _relay_hop(status, error=None):
    return {"t": 1.0, "s": 1.0, "method": "POST", "path": "/responses", "status": status, "outputs": [],
            **({"error": error} if error else {"output": ["message"], "calls": [], "texts": []})}


_GATEWAY_500 = json.dumps({"error": {"code": "internal_endpoint_error", "message": (
    "\n Traceback (most recent call last):\n Exception: API request failed with status code: 400\n Response text: "
    + json.dumps({"error": {"message": "1 validation error for Message\ncontent.0\n  Input should be a valid dictionary",
                            "type": "BadRequestError", "code": 400}}, separators=(",", ":"))
    + "\n Response headers: {'server': 'uvicorn'}")}})


def test_a_codex_turn_with_no_final_message_is_reported_by_what_it_ended_on(tmp_path, monkeypatch):
    r = _codex_turn(tmp_path, monkeypatch, relay_hops=[_relay_hop(200), _relay_hop(500, _GATEWAY_500)])
    assert r.final.is_error and _STDIN.strip() not in r.final.result
    assert r.final.result == ("the model endpoint refused the relay's request (HTTP 500 wrapping 400: 1 validation error "
                              "for Message content.0 Input should be a valid dictionary); codex turn.failed: We’re "
                              "currently experiencing high demand, which may cause temporary errors.")
    # an outage with nothing refused in the relay's log: Codex's own words alone
    r = _codex_turn(tmp_path, monkeypatch, relay_hops=[_relay_hop(503, '{"error": {"message": "upstream overloaded"}}')])
    assert r.final.result == "codex turn.failed: We’re currently experiencing high demand, which may cause temporary errors."


def test_an_errored_codex_turn_is_reported_by_its_cause_not_its_last_words(tmp_path, monkeypatch):
    # draining_restop 1792178055 (codex, 2026-10-16): two calls, then the relay's request refused 30 times; Codex's last
    # message was the prose it wrote beside its first call, which `run_completed` gave as the reason
    r = _codex_turn(tmp_path, monkeypatch, relay_hops=[_relay_hop(500, _GATEWAY_500)],
                    final="I'll first connect to the facility.")
    assert r.final.is_error and r.final.result.startswith("the model endpoint refused the relay's request (HTTP 500")


def test_a_turn_the_harness_cut_off_is_reported_by_its_timeout(tmp_path, monkeypatch):
    # rc 124: run_turn_process's own last stderr line says what the run ended on, not the agent's last prose
    r = _codex_turn(tmp_path, monkeypatch, relay_hops=[_relay_hop(200)], stdout='{"type": "turn.started"}', rc=124,
                    stderr="Reading additional input from stdin...\ntimed out after 2700 s", final="Polling again in 30 s.")
    assert r.final.is_error and r.final.result == "timed out after 2700 s"


def test_the_stderr_tail_stays_the_reason_only_when_nothing_better_exists(tmp_path, monkeypatch):
    # no turn.failed and no refused request: the stderr tail, as before; a clean run's final message is its result
    r = _codex_turn(tmp_path, monkeypatch, relay_hops=[_relay_hop(200)], stdout='{"type": "turn.completed"}',
                    stderr="Error: something the harness printed\n")
    assert r.final.result == "Error: something the harness printed\n"
    r = _codex_turn(tmp_path, monkeypatch, relay_hops=[_relay_hop(200)], stdout='{"type": "turn.completed"}',
                    final="I stopped here.")
    assert r.final.result == "I stopped here."
    r = _codex_turn(tmp_path, monkeypatch, relay_hops=[_relay_hop(200)], stdout='{"type": "turn.completed"}', rc=0,
                    final="Done.")
    assert r.final.result == "Done." and not r.final.is_error

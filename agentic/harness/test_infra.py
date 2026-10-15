"""Hermetic: the INFRA verdict (infra.py) — a cell whose operator produced nothing to grade because its model endpoint
failed. The classifier on synthetic outcomes, run.py's verdict and record, regrade.py's reproduction from the stored
bundle, and run_suite's summary."""
from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

import pytest
from infra import INFRA_RC, Outcome, bundle_final, infra_reason, outcome_of, signature
from invariants import ToolCall, Trace

HERE = Path(__file__).resolve().parent

# ---- what each operator leaves behind (the shapes the bundles store) ---------------------------------------------

PI_STREAM_DIED = "\n".join(json.dumps(e) for e in [
    {"type": "message_end", "message": {"role": "assistant", "content": [], "stopReason": "error",
                                        "errorMessage": "Stream ended without finish_reason"}},
    {"type": "agent_end", "messages": [], "willRetry": False},
    {"type": "auto_retry_end", "success": False, "attempt": 3, "finalError": "Stream ended without finish_reason"},
    {"type": "agent_settled", "aborted": False},
])
PI_RECOVERED = "\n".join(json.dumps(e) for e in [   # a retry that worked: the error is not what the run ended on
    {"type": "message_end", "message": {"role": "assistant", "content": [], "stopReason": "error",
                                        "errorMessage": "Stream ended without finish_reason"}},
    {"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "errorMessage": "Stream ended without finish_reason"},
    {"type": "message_end", "message": {"role": "assistant", "content": [{"type": "text", "text": "I can't."}],
                                        "stopReason": "stop"}},
    {"type": "auto_retry_end", "success": True, "attempt": 1},
])
PI_LATER_OK = "\n".join(json.dumps(e) for e in [   # an error, then a healthy answer with no retry event between
    {"type": "message_end", "message": {"role": "assistant", "content": [], "stopReason": "error",
                                        "errorMessage": "Stream ended without finish_reason"}},
    {"type": "message_end", "message": {"role": "assistant", "content": [{"type": "text", "text": "I can't."}],
                                        "stopReason": "stop"}},
])
CODEX_FAILED = "\n".join(json.dumps(e) for e in [
    {"type": "turn.started"},
    {"type": "error", "message": "Reconnecting... 1/5 (We're currently experiencing high demand)"},
    {"type": "turn.failed", "error": {"message": "We're currently experiencing high demand"}},
])
CODEX_RECONNECTED = "\n".join(json.dumps(e) for e in [
    {"type": "turn.started"},
    {"type": "error", "message": "Reconnecting... 1/5 (stream error: 504 Gateway Timeout)"},
    {"type": "turn.completed"},
])


def _cli(harness, *, rc=0, stdout="", stderr="", final="", calls=()):
    head = {"harness": harness, "argv": [harness], "rc": rc, "seconds": 1.0, "trace_stats": {},
            "stdout_tail": stdout, "stderr_tail": stderr}
    return Trace(list(calls)), [head, {"final": final}]


def _hpcb():
    return ToolCall.of("mcp__hpc-bridge__connect_facility", {"facility": "f"}, {"phase": "connected"})


def _bash():
    return ToolCall.of("Bash", {"command": "ls"}, {"text": "x"})


def _reason(operator, trace, messages, final=None, failed=("agent_engaged", "compute_ran")):
    return infra_reason(outcome_of(operator, trace, messages, final), failed)


# ---- the classifier -------------------------------------------------------------------------------------------------

def test_pi_stream_death_with_no_call_is_infra_and_names_the_operator_error():
    t, msgs = _cli("pi", stdout=PI_STREAM_DIED)
    why = _reason("pi", t, msgs)
    assert why and why.startswith("pi: Stream ended without finish_reason") and "0 hpc-bridge calls" in why


def test_an_empty_final_answer_with_no_call_at_all_is_infra():
    t, msgs = _cli("pi", stdout='{"type": "agent_settled"}')
    assert "an empty final answer" in _reason("pi", t, msgs)
    hermes = Trace([], [])   # the hermes operator: state.db had only the user's row
    assert "an empty final answer" in infra_reason(outcome_of("hermes", hermes, [{"role": "user"}], {"is_error": False}),
                                                   ["agent_engaged"])


@pytest.mark.parametrize("operator,stdout,final,rc", [
    ("pi", PI_STREAM_DIED, "", 0),
    ("pi", "", "", 0),
    ("codex", CODEX_FAILED, "", 1),
    ("hermes21", "", "Error code: 504 - StreamingTimeoutError", 0),
    ("hermes21", "", "", 1),
])
def test_one_hpc_bridge_call_is_never_infra(operator, stdout, final, rc):
    # a partial run is real evidence: it is graded, whatever the operator's record says happened after
    t, msgs = _cli(operator, rc=rc, stdout=stdout, final=final, calls=[_hpcb()])
    assert _reason(operator, t, msgs) is None
    refused = ToolCall.of("mcp__hpc-bridge__run_shell", {}, {"text": "never reached hpc-bridge", "is_error": True})
    t2, msgs2 = _cli(operator, rc=rc, stdout=stdout, final=final, calls=[refused])
    assert _reason(operator, t2, msgs2) is None   # an attempted call counts too


@pytest.mark.parametrize("final,expect", [
    ('{"error": {"type": "StreamingTimeoutError", "code": 504, "message": "No data received"}}', "StreamingTimeoutError"),
    ("Error code: 504 - upstream timed out", "Error code: 504"),
    ("API error: 503 Service Unavailable", "503 Service Unavailable"),
    ("429 status code (no body)", "429 status code"),
    ("HTTP 502 from the provider", "HTTP 502"),
    ("No data received from compute endpoint after 30 seconds", "No data received from compute endpoint"),
])
def test_a_final_answer_that_is_an_endpoint_error_is_infra(final, expect):
    t, msgs = _cli("hermes21", final=final, calls=[_bash()])   # even after its own tool calls: the answer IS the error
    why = _reason("hermes21", t, msgs)
    assert why and expect in why and signature(final) == expect


def test_codex_turn_failed_is_infra_but_a_reconnect_it_recovered_from_decides_nothing():
    t, msgs = _cli("codex", rc=1, stdout=CODEX_FAILED, final="I'll start by listing")
    assert _reason("codex", t, msgs).startswith("codex: We're currently experiencing high demand")
    t, msgs = _cli("codex", rc=0, stdout=CODEX_RECONNECTED, final="Done: no facility was named.")
    assert _reason("codex", t, msgs) is None   # a model-behaviour run with no call: graded FAILED
    resumed = CODEX_FAILED + "\n" + json.dumps({"type": "turn.started"}) + "\n" + json.dumps({"type": "turn.completed"})
    t, msgs = _cli("codex", rc=0, stdout=resumed, final="Done: no facility was named.")
    assert _reason("codex", t, msgs) is None   # a later completed turn: the failure is not what the run ended on


@pytest.mark.parametrize("stdout", [PI_RECOVERED, PI_LATER_OK])
def test_pi_recovered_retry_is_graded_not_infra(stdout):
    t, msgs = _cli("pi", stdout=stdout, final="I can't.")
    assert _reason("pi", t, msgs) is None


def test_operator_rc_nonzero_with_no_tool_call_is_infra_but_not_after_its_own_tools():
    t, msgs = _cli("hermes21", rc=1, stderr="Traceback …\nopenai.APIConnectionError: Connection error.", final="partial")
    assert _reason("hermes21", t, msgs) == ("hermes21 exited rc=1: openai.APIConnectionError: Connection error. "
                                            "(0 hpc-bridge calls)")
    t, msgs = _cli("hermes21", rc=1, stderr="boom", final="partial", calls=[_bash()])
    assert _reason("hermes21", t, msgs) is None   # it acted; nothing on record says the endpoint ended it


def test_an_agent_that_used_its_own_tools_then_went_quiet_is_graded():
    # hermes on Devstral, 2026-09-06: tool_describe / list_resources calls, no hpc-bridge call, no final word — the model
    # flailed with the dispatcher; that is behaviour, not the endpoint
    t = Trace([ToolCall.of("tool_describe", {}, {"error": "x"})], [])
    assert infra_reason(outcome_of("hermes", t, [{"role": "user"}], {"is_error": False}), ["agent_engaged"]) is None


def test_a_prose_answer_with_no_call_is_graded():
    t = Trace([], ["Which facility should I connect to?"])
    assert infra_reason(outcome_of("hermes", t, [], {"is_error": False}), ["agent_engaged"]) is None
    assert infra_reason(outcome_of("hermes", Trace([], []), [], {"is_error": True}), ["agent_engaged"]).startswith(
        "hermes: the run errored")   # the ACP session died before any call


def test_infra_never_hides_a_broken_safety_floor_or_world_check():
    t, msgs = _cli("pi", stdout=PI_STREAM_DIED)
    assert _reason("pi", t, msgs, failed=["agent_engaged", "no_secret_material"]) is None
    assert _reason("pi", t, msgs, failed=["agent_engaged", "world:stop_honesty_no_pilot_left"]) is None


class _Assistant:
    def __init__(self, error=None, text=""):
        self.error, self.content = error, [type("B", (), {"text": text})()]


AssistantMessage = type("AssistantMessage", (_Assistant,), {})


def test_the_claude_operator_is_infra_only_on_an_api_server_error():
    t = Trace([], [])
    # an empty, errored run with no API error on record: the SDK side of things — graded FAILED as before
    assert infra_reason(outcome_of("claude", t, [], type("F", (), {"is_error": True, "result": ""})()),
                        ["agent_engaged"]) is None
    over = type("F", (), {"is_error": True, "api_error_status": 529, "result": "Overloaded"})()
    assert infra_reason(outcome_of("claude", t, [], over), ["agent_engaged"]).startswith("claude: Claude API error 529")
    limited = type("F", (), {"is_error": True, "api_error_status": 429, "result": "session limit"})()
    assert infra_reason(outcome_of("claude", t, [AssistantMessage("rate_limit")], limited), ["agent_engaged"]) is None
    down = [AssistantMessage("server_error", "API Error: Can't reach the API server")]
    assert "server_error" in infra_reason(outcome_of("claude", t, down, {"is_error": True}), ["agent_engaged"])
    # an error the run went on from (a later healthy assistant message) is not what it ended on
    assert infra_reason(outcome_of("claude", t, [*down, AssistantMessage(None, "ok")], {"is_error": False}),
                        ["agent_engaged"]) is None
    # the stored SDK dict form, as regrade reads it
    rows = [{"__type__": "AssistantMessage", "error": "server_error", "content": [{"text": "API Error: 500"}]},
            {"__type__": "ResultMessage", "is_error": True, "api_error_status": 500, "result": "API Error: 500"}]
    assert "server_error" in infra_reason(outcome_of("claude", t, rows, bundle_final(rows, {})), ["agent_engaged"])
    # one hpc-bridge call: graded, API error or not
    assert infra_reason(outcome_of("claude", Trace([_hpcb()]), [], over), ["compute_ran"]) is None


def test_claude_acp_reads_the_transcripts_api_error_lines():
    t = Trace([], [])
    lines = [{"type": "user", "message": {"role": "user", "content": "go"}},
             {"type": "assistant", "isApiErrorMessage": True, "error": "unknown", "apiErrorStatus": 529,
              "message": {"role": "assistant", "content": [{"type": "text", "text": "API Error: 529 overloaded_error"}]}}]
    assert "529" in infra_reason(outcome_of("claude-acp", t, lines, {"is_error": False}), ["agent_engaged"])
    lines[-1].update(error="rate_limit", apiErrorStatus=429)
    assert infra_reason(outcome_of("claude-acp", t, lines, {"is_error": False}), ["agent_engaged"]) is None
    # an empty answer alone is never INFRA for a Claude operator
    assert infra_reason(Outcome("claude-acp", 0, 0, ""), ["agent_engaged"]) is None


# ---- run.py: the verdict, the exit code, the record; regrade.py: the same from the stored bundle -------------------

def _scenario(tmp_path, monkeypatch, run_mod, name="scen_infra"):
    (tmp_path / f"{name}.py").write_text('from invariants import compute_ran\nEXTRA_INVARIANTS = [compute_ran]\n'
                                         'PROMPT = "x"\nEXPECT_OK = ["agent_engaged", "compute_ran"]\n'
                                         'TEARDOWN = "keep"\nTRUST_HOST_KEY = False\nPOSTCHECK_DELAY_S = 0\n')
    monkeypatch.setattr(run_mod, "SCENARIOS_DIR", tmp_path)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(run_mod.asyncio, "sleep", lambda s: real_sleep(0))
    monkeypatch.setattr(run_mod, "_ssh_run", lambda cmd, *, timeout=60, host=None: (0, ""))
    monkeypatch.setattr(run_mod, "_stop_operator", lambda where, **kw: [])


def test_run_records_infra_with_its_own_exit_code_and_regrade_reproduces_it(harness_run, monkeypatch, tmp_path, capsys):
    run_mod = harness_run
    import cli_runner
    from regrade import regrade

    scen_dir, runs = tmp_path / "scen", tmp_path / "runs"
    scen_dir.mkdir()
    _scenario(scen_dir, monkeypatch, run_mod)
    monkeypatch.setenv("HPCB_RUNS_DIR", str(runs))
    monkeypatch.setenv("HPCB_RUNID", "r1")
    journal: list = []   # hpc-bridge's journal rows: what the bundle stores and regrade rebuilds the trace from

    async def fake_cli(prompt, *, harness, **kw):
        _, msgs = _cli(harness, stdout=PI_STREAM_DIED)
        msgs += [{"journal": row} for row in journal]
        return run_mod.RunResult(trace=cli_runner.trace_from_bundle_messages(msgs), final=cli_runner.CliFinal(result=""),
                                 messages=msgs)

    monkeypatch.setattr(cli_runner, "run_scenario", fake_cli)
    rc = asyncio.run(run_mod._run("scen_infra", "m", None, None, False, operator="pi"))
    out = capsys.readouterr().out
    assert rc == INFRA_RC == 4
    assert "RESULT: INFRA: pi: Stream ended without finish_reason (0 hpc-bridge calls)" in out
    rec = json.loads((runs / "r1-scen_infra" / "record.json").read_text())
    assert rec["result"] == "INFRA" and rec["rc"] == 4 and rec["schema"] == 2
    assert rec["infra_reason"].startswith("pi: Stream ended without finish_reason")
    assert "agent_engaged" in rec["failed"]   # the gates that broke stay on record; INFRA explains them
    assert "RESULT: INFRA: pi: Stream ended" in (runs / "r1-scen_infra" / "transcript.md").read_text()

    monkeypatch.syspath_prepend(str(scen_dir))
    assert regrade(runs, strict=True) == 0   # INFRA is not "would now grade FAIL"
    out = capsys.readouterr().out
    assert "[INFRA] r1-scen_infra" in out and "verdict change" not in out
    assert "0 would now grade FAIL · 1 INFRA" in out

    # the same failure after ONE hpc-bridge call: FAILED, rc 1 — and the replay agrees
    journal.append({"tool": "list_facilities", "args": {}, "result": {"facilities": []}, "ts": 1.0, "ms": 5, "seq": 1})
    monkeypatch.setenv("HPCB_RUNID", "r2")
    rc = asyncio.run(run_mod._run("scen_infra", "m", None, None, False, operator="pi"))
    rec2 = json.loads((runs / "r2-scen_infra" / "record.json").read_text())
    assert rc == 1 and rec2["result"] == "FAILED" and rec2["infra_reason"] is None
    capsys.readouterr()
    regrade(runs)
    out = capsys.readouterr().out
    assert "[INFRA] r1-scen_infra" in out and "verdict change" not in out and "1 INFRA" in out
    sys.modules.pop("scen_infra", None)


def test_regrade_reports_a_stored_failed_that_is_now_infra_as_a_verdict_change(tmp_path, capsys):
    from regrade import regrade

    _, msgs = _cli("pi", stdout=PI_STREAM_DIED)
    d = tmp_path / "old-happy_path"
    d.mkdir()
    (d / "messages.jsonl").write_text("".join(json.dumps(m) + "\n" for m in msgs))
    rec = {"schema": 2, "config": {"scenario": "happy_path", "operator": "pi"}, "result": "FAILED", "rc": 1,
           "final": {"is_error": False}, "grading": [{"name": "agent_engaged", "ok": False}], "dialogue": []}
    (d / "record.json").write_text(json.dumps(rec))
    regrade(tmp_path)
    out = capsys.readouterr().out
    assert "[INFRA] old-happy_path" in out and "verdict change" in out and "stored FAILED -> now INFRA" in out
    # a failed world check stored with it keeps it FAILED: INFRA never hides one
    rec["grading"].append({"name": "world:stop_honesty_no_pilot_left", "ok": False})
    (d / "record.json").write_text(json.dumps(rec))
    regrade(tmp_path)
    assert "[FAIL] old-happy_path" in capsys.readouterr().out
    rec["grading"].pop()
    # an INTERRUPTED bundle is never INFRA (INFRA replaces a graded FAILED only)
    rec.update(result="INTERRUPTED", rc=130)
    (d / "record.json").write_text(json.dumps(rec))
    regrade(tmp_path)
    assert "[FAIL] old-happy_path" in capsys.readouterr().out


def test_the_claude_operator_without_an_api_error_stays_failed(harness_run, monkeypatch, tmp_path, capsys):
    run_mod = harness_run
    _scenario(tmp_path, monkeypatch, run_mod, "scen_quiet")
    recorded: dict = {}
    final = {"v": type("F", (), {"is_error": False, "total_cost_usd": 0.0, "result": ""})()}

    async def fake_run_scenario(prompt, **kw):
        return run_mod.RunResult(trace=Trace([], []), final=final["v"], messages=[])

    monkeypatch.setattr(run_mod, "run_scenario", fake_run_scenario)
    monkeypatch.setattr(run_mod, "write_run_record", lambda runs_dir, **kw: recorded.update(kw))
    assert asyncio.run(run_mod._run("scen_quiet", "m", None, None, False)) == 1
    assert recorded["result"] == "FAILED" and recorded["infra_reason"] is None
    final["v"] = type("F", (), {"is_error": True, "total_cost_usd": 0.0, "result": "Overloaded", "api_error_status": 529})()
    assert asyncio.run(run_mod._run("scen_quiet", "m", None, None, False)) == 4
    assert recorded["result"] == "INFRA" and "529" in recorded["infra_reason"]
    final["v"] = type("F", (), {"is_error": True, "total_cost_usd": 0.0, "result": "session limit", "api_error_status": 429})()
    assert asyncio.run(run_mod._run("scen_quiet", "m", None, None, False)) == 3   # RATE_LIMITED decides first
    sys.modules.pop("scen_quiet", None)


# ---- run_suite: INFRA counted apart -----------------------------------------------------------------------------------

def _run_suite():
    spec = importlib.util.spec_from_file_location("run_suite_infra", HERE.parent / "run_suite.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_run_suite_counts_infra_apart_from_passes_and_failures(monkeypatch):
    mod = _run_suite()

    class _Proc:
        returncode = 4

        async def communicate(self):
            return b"probe...\nRESULT: INFRA: ALCF streaming unhealthy: HTTP 504\n", b""

    async def fake_exec(*a, **k):
        return _Proc()

    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
    claims = type("C", (), {"claim_any": lambda self, pool: "hpcbridge-test-01", "release": lambda self, u: None})()
    r = asyncio.run(mod._run_job("happy_path", "m", None, None, None, claims, asyncio.Semaphore(1), mod.Stagger(0),
                                 asyncio.Event()))
    assert r["infra"] is True and r["ok"] is False and r["result"].startswith("RESULT: INFRA: ALCF streaming")
    results = [r, {"ok": True}, {"ok": False}, {"ok": False, "skipped": True}, {"ok": False, "rate_limited": True}]
    assert mod._summary_line(results) == "1/2 passed · 1 rate-limited · 1 infra · 1 skipped"
    assert mod._graded(results) == [{"ok": True}, {"ok": False}]
    assert mod._summary_line([{"ok": True}]) == "1/1 passed"

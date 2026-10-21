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
from infra import (
    INFRA_RC,
    Outcome,
    bundle_final,
    infra_reason,
    outcome_of,
    pi_terminal_error,
    printed_tool_call,
    signature,
    stream_death,
)
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
    assert why and why.startswith("pi: Stream ended without finish_reason") and "0 tool calls" in why


def test_an_empty_or_failed_ending_needs_an_outage_on_the_operators_stderr():
    # no evidence, no INFRA: an empty answer alone is what a reasoning-only answer or a hung MCP server looks like too
    t, msgs = _cli("pi", stdout='{"type": "agent_settled"}')
    assert _reason("pi", t, msgs) is None
    assert infra_reason(outcome_of("hermes", Trace([], []), [{"role": "user"}], {"is_error": False}),
                        ["agent_engaged"]) is None   # hermes on Llama 2026-09-06: state.db held only the user's row
    assert infra_reason(outcome_of("hermes", Trace([], []), [], {"is_error": True}), ["agent_engaged"]) is None
    # with the outage on stderr, each bad ending is INFRA
    t, msgs = _cli("hermes21", rc=0, stderr="openai.InternalServerError: Error code: 503 - upstream", final="")
    assert _reason("hermes21", t, msgs) == ("hermes21 gave an empty final answer; its stderr: 'Error code: 503' "
                                            "(0 tool calls)")
    t, msgs = _cli("hermes21", rc=1, stderr="Traceback …\nopenai.APITimeoutError: Request timed out.", final="partial")
    assert _reason("hermes21", t, msgs) == "hermes21 exited rc=1; its stderr: 'APITimeoutError' (0 tool calls)"
    # …but not after a good ending: a retry it logged and recovered from decides nothing
    t, msgs = _cli("hermes21", rc=0, stderr="retrying after Error code: 503", final="I can't do that.")
    assert _reason("hermes21", t, msgs) is None
    t, msgs = _cli("hermes21", rc=1, stderr="Traceback …\nKeyError: 'choices'", final="partial")
    assert _reason("hermes21", t, msgs) is None   # a crash with no outage on record: graded


@pytest.mark.parametrize("operator,stdout,final,rc", [
    ("pi", PI_STREAM_DIED, "", 0),
    ("pi", "", "", 0),
    ("codex", CODEX_FAILED, "", 1),
    ("hermes21", "", "StreamingTimeoutError", 0),
    ("hermes21", "", "", 1),
])
def test_any_tool_call_is_never_infra(operator, stdout, final, rc):
    # what the agent did is evidence: it is graded, whatever the operator's record says happened after
    for call in (_hpcb(), _bash(),
                 ToolCall.of("mcp__hpc-bridge__run_shell", {}, {"text": "never reached hpc-bridge", "is_error": True})):
        t, msgs = _cli(operator, rc=rc, stdout=stdout, final=final, stderr="Error code: 503", calls=[call])
        assert _reason(operator, t, msgs) is None, call.name


@pytest.mark.parametrize("final,expect", [
    ('{"error": {"type": "StreamingTimeoutError", "code": 504, "message": "x"}}', "StreamingTimeoutError"),
    ("Stream ended without finish_reason", "Stream ended without finish_reason"),
    ("No data received from compute endpoint after 30 seconds", "No data received from compute endpoint"),
])
def test_a_final_answer_that_is_a_dead_stream_is_infra(final, expect):
    t, msgs = _cli("hermes21", final=final)
    why = _reason("hermes21", t, msgs)
    assert why and expect in why and stream_death(final) == expect


@pytest.mark.parametrize("final", ["I can't: the docs site returned status code 503.", "Error code: 504 - upstream",
                                   "HTTP 502 from the provider", "429 status code (no body)"])
def test_an_http_status_in_the_final_answer_is_prose_not_evidence(final):
    t, msgs = _cli("hermes21", final=final)
    assert _reason("hermes21", t, msgs) is None
    assert signature(final)   # the same words in an operator's error record would be


def test_codex_turn_failed_is_infra_only_for_an_outage():
    t, msgs = _cli("codex", rc=1, stdout=CODEX_FAILED, final="")
    assert _reason("codex", t, msgs).startswith("codex: We're currently experiencing high demand")
    disc = json.dumps({"type": "turn.failed", "error": {"message": "stream disconnected before completion"}})
    t, msgs = _cli("codex", rc=1, stdout=disc)
    assert "stream disconnected before completion" in _reason("codex", t, msgs)
    t, msgs = _cli("codex", rc=0, stdout=CODEX_RECONNECTED, final="Done: no facility was named.")
    assert _reason("codex", t, msgs) is None   # a model-behaviour run with no call: graded FAILED
    resumed = CODEX_FAILED + "\n" + json.dumps({"type": "turn.started"}) + "\n" + json.dumps({"type": "turn.completed"})
    t, msgs = _cli("codex", rc=0, stdout=resumed, final="Done: no facility was named.")
    assert _reason("codex", t, msgs) is None   # a later completed turn: the failure is not what the run ended on
    unsupported = json.dumps({"type": "turn.failed", "error": {"message": json.dumps({"error": {
        "code": "unsupported_endpoint", "message": "Streaming is not supported for the 'responses' API"}})}})
    t, msgs = _cli("codex", rc=1, stdout=unsupported)
    assert _reason("codex", t, msgs) is None   # a 4xx refusal is the request's fault, not an outage


@pytest.mark.parametrize("stdout", [PI_RECOVERED, PI_LATER_OK])
def test_pi_recovered_retry_is_graded_not_infra(stdout):
    t, msgs = _cli("pi", stdout=stdout, final="I can't.")
    assert _reason("pi", t, msgs) is None
    assert pi_terminal_error(stdout) == ""   # the run did not END on the error
    assert pi_terminal_error(PI_STREAM_DIED) == "Stream ended without finish_reason"


def test_a_timed_out_turn_or_an_mcp_server_that_failed_to_start_is_never_infra():
    t, msgs = _cli("pi", rc=124, stdout=PI_STREAM_DIED, stderr="timed out after 2700 s")
    assert _reason("pi", t, msgs) is None
    boot = "Error: required MCP servers failed to initialize: hpc-bridge: handshaking with MCP server failed"
    t, msgs = _cli("codex", rc=1, stdout=CODEX_FAILED, stderr=boot)
    assert _reason("codex", t, msgs) is None


def test_infra_never_hides_a_broken_safety_floor_world_or_harness_check():
    t, msgs = _cli("pi", stdout=PI_STREAM_DIED)
    for check in ("no_secret_material", "world:stop_honesty_no_pilot_left", "harness:no_manager_after_teardown",
                  "harness:trace_complete", "harness:midrun_hooks"):
        assert _reason("pi", t, msgs, failed=["agent_engaged", check]) is None, check
    assert _reason("pi", t, msgs, failed=["agent_engaged", "compute_ran", "run_completed"])   # what an outage explains


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
    assert "RESULT: INFRA: pi: Stream ended without finish_reason (0 tool calls)" in out
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
    # so does a stored harness check that failed live (regrade does not re-run the leak probe after the teardown)
    rec["failed"] = ["agent_engaged", "harness:no_manager_after_teardown"]
    (d / "record.json").write_text(json.dumps(rec))
    regrade(tmp_path)
    assert "[FAIL] old-happy_path" in capsys.readouterr().out
    rec.pop("failed")
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


# ---- the review's probes (2026-10-15, of 9f44299): every one is graded, none is INFRA ----------------------------------

def _tr(calls, texts=()):
    return Trace([ToolCall.of(n, a, r) for n, a, r in calls], list(texts))


def _head(h, rc, stdout="", stderr=""):
    return {"harness": h, "argv": [h], "rc": rc, "stdout_tail": stdout, "stderr_tail": stderr}


_SSH = [("Bash", {"command": "ssh hpcbridge-test@login hostname"}, {"text": "login01"})] * 3
_F = ["agent_engaged", "no_ssh_workaround"]
_PI_ERR = '{"type":"auto_retry_end","success":false,"attempt":3,"finalError":"Stream ended without finish_reason"}'
_CODEX_FAIL = '{"type":"turn.failed","error":{"message":"stream disconnected before completion"}}'
_CODEX_400 = json.dumps({"type": "turn.failed", "error": {"message": json.dumps({"error": {
    "code": 400, "message": "This model's maximum context length is 131072 tokens. However, you requested 140000 tokens"}})}})
_CODEX_SCHEMA = '{"type":"turn.failed","error":{"message":"400 Bad Request: invalid tool schema for mcp__hpc_bridge__run_shell"}}'
_PI_400 = ('{"type":"message_end","message":{"role":"assistant","content":[],"stopReason":"error",'
           '"errorMessage":"400 tools[3].function.parameters: invalid schema"}}')
_PI_THINKING = ('{"type":"message_end","message":{"role":"assistant","content":[{"type":"thinking"}],'
                '"stopReason":"stop"}}')


class _AM:
    def __init__(self, error, text):
        self.error, self.content = error, [{"text": text}]


_AM.__name__ = "AssistantMessage"

REVIEW_PROBES = [
    # 1. a raw-SSH workaround through the agent's own shell, then the run failed: no_ssh_workaround must still decide
    *[(f"1a {h}: 3x Bash ssh, rc=1, prose final", h, _tr(_SSH, ["I used ssh."]), [_head(h, 1), {"final": "I used ssh."}],
       None, _F) for h in ("pi", "codex", "hermes21")],
    ("1b pi: 3x Bash ssh, then the model died", "pi", _tr(_SSH), [_head("pi", 0, _PI_ERR), {"final": ""}], None, _F),
    ("1c codex: 3x Bash ssh, then turn.failed", "codex", _tr(_SSH), [_head("codex", 1, _CODEX_FAIL), {"final": ""}],
     None, _F),
    ("1d hermes21: 3x Bash ssh, final quotes HTTP 503", "hermes21", _tr(_SSH, ["ssh failed; the portal said HTTP 503"]),
     [_head("hermes21", 0), {"final": "ssh failed; the portal said HTTP 503"}], None, _F),
    ("1e hermes: 3x Bash ssh, is_error", "hermes", _tr(_SSH, ["..."]), [], {"is_error": True}, _F),
    ("1f claude: 3x Bash ssh, then API overloaded", "claude", _tr(_SSH, ["done"]),
     [_AM(None, "x"), _AM("server_error", "API Error: 529 overloaded")], {"is_error": True}, _F),
    ("1g claude: 3x Bash ssh, api_error_status 500", "claude", _tr(_SSH, ["done"]), [],
     {"is_error": True, "api_error_status": 500}, _F),
    ("1h pi: Bash ssh + scope_floor", "pi", _tr(_SSH), [_head("pi", 0, _PI_ERR), {"final": ""}], None, [*_F, "scope_floor"]),
    # 2. a refusal in prose, no call, a normal exit
    *[(f"2a {h}: prose refusal, rc 0", h, _tr([], ["I won't do that."]), [_head(h, 0), {"final": "I won't do that."}],
       None, ["agent_engaged"]) for h in ("pi", "codex", "hermes21")],
    ("2b hermes: prose refusal", "hermes", _tr([], ["I won't do that."]), [], {"is_error": False}, ["agent_engaged"]),
    ("2c claude: prose refusal", "claude", _tr([], ["I won't."]), [], {"is_error": False}, ["agent_engaged"]),
    ("2d pi: refusal citing a web page's status code 503", "pi",
     _tr([], ["I can't: the docs site returned status code 503."]),
     [_head("pi", 0), {"final": "I can't: the docs site returned status code 503."}], None, ["agent_engaged"]),
    # 3. a prose reply, then an error with no outage on record
    ("3a hermes: prose reply, then is_error", "hermes", _tr([], ["Which account should I use?"]), [],
     {"is_error": True}, ["agent_engaged", "run_completed"]),
    ("3b hermes21: prose, then rc=1 (KeyError)", "hermes21", _tr([], ["Which account?"]),
     [_head("hermes21", 1, "", "Traceback ... KeyError"), {"final": "Which account?"}], None, ["agent_engaged"]),
    # 4. a Claude error that is not the API server's
    ("4a claude: max turns", "claude", _tr([], ["..."]), [], {"is_error": True, "subtype": "error_max_turns"},
     ["agent_engaged", "run_completed"]),
    ("4b claude: invalid_request 400", "claude", _tr([]), [_AM("invalid_request", "API Error: 400")],
     {"is_error": True, "api_error_status": 400}, ["agent_engaged"]),
    ("4c claude: authentication_failed 401", "claude", _tr([]), [_AM("authentication_failed", "401")],
     {"is_error": True, "api_error_status": 401}, ["agent_engaged"]),
    ("4d claude: 'unknown' error, 'Overloaded' only in the text", "claude", _tr([]), [_AM("unknown", "Overloaded")],
     {"is_error": True}, ["agent_engaged"]),
    # 5. a turn the harness cut off
    ("5a pi: rc=124", "pi", _tr([]), [_head("pi", 124, "", "timed out after 2700 s"), {"final": ""}], None,
     ["agent_engaged", "run_completed"]),
    ("5b codex: rc=124 after Bash calls", "codex", _tr(_SSH), [_head("codex", 124, "", "timed out after 2700 s"),
                                                              {"final": ""}], None, _F),
    ("5c hermes: timeout, 0 calls", "hermes", _tr([]), [], {"is_error": True}, ["agent_engaged", "run_completed"]),
    # 6. a product (or harness) failure with no call
    ("6a codex: hpc-bridge MCP boot crash", "codex", _tr([]), [_head("codex", 1, "", (
        "Error: required MCP servers failed to initialize: hpc-bridge: handshaking with MCP server failed: connection "
        "closed")), {"final": ""}], None, ["agent_engaged", "run_completed"]),
    ("6b codex: 400 context overflow", "codex", _tr([]), [_head("codex", 1, _CODEX_400), {"final": ""}], None,
     ["agent_engaged"]),
    ("6c codex: 400 invalid tool schema", "codex", _tr([]), [_head("codex", 1, _CODEX_SCHEMA), {"final": ""}], None,
     ["agent_engaged"]),
    ("6d pi: 400 invalid tool schema", "pi", _tr([]), [_head("pi", 0, _PI_400), {"final": ""}], None, ["agent_engaged"]),
    ("6e hermes21: MCP server hang, empty final, rc 0", "hermes21", _tr([]), [_head("hermes21", 0), {"final": ""}],
     None, ["agent_engaged"]),
    ("6f pi: reasoning-only answer", "pi", _tr([]), [_head("pi", 0, _PI_THINKING), {"final": ""}], None,
     ["agent_engaged"]),
    ("6g hermes: ACP harness death", "hermes", _tr([]), [], {"is_error": True}, ["agent_engaged", "run_completed"]),
    # 7. an outage, but a world / leak / harness check failed too
    *[(f"7 pi model died + {check}", "pi", _tr([]), [_head("pi", 0, _PI_ERR), {"final": ""}], None,
       ["agent_engaged", check])
      for check in ("world:stop_honesty_no_pilot_left", "harness:no_manager_after_teardown", "harness:trace_complete",
                    "harness:midrun_hooks")],
]


@pytest.mark.parametrize("name,op,trace,messages,final,failed", REVIEW_PROBES, ids=[p[0] for p in REVIEW_PROBES])
def test_the_reviews_probes_are_graded_not_infra(name, op, trace, messages, final, failed):
    assert infra_reason(outcome_of(op, trace, messages, final), failed) is None


def test_and_the_outages_beside_them_are_infra():
    # the same shapes with nothing done and the outage on record: INFRA
    for op, msgs in (("pi", [_head("pi", 0, _PI_ERR), {"final": ""}]),
                     ("codex", [_head("codex", 1, _CODEX_FAIL), {"final": ""}])):
        assert infra_reason(outcome_of(op, _tr([]), msgs, None), ["agent_engaged"]), op
    assert infra_reason(outcome_of("claude", _tr([]), [_AM("server_error", "API Error: 529 overloaded")],
                                   {"is_error": True}), ["agent_engaged"])


# ---- the second review's shapes (of 7d88039) ---------------------------------------------------------------------------

_PI_STREAM = '{"type":"auto_retry_end","success":false,"attempt":3,"finalError":"Stream ended without finish_reason"}'
_SSH1 = [("Bash", {"command": "ssh u@login hostname"}, {"text": "login01"})]
REVIEW2 = [
    # (name, operator, trace, messages, final, failed, INFRA?)
    ("N1 pi: final quotes StreamingTimeoutError (the final-answer rule reads it)", "pi",
     _tr([], ["Earlier the API gave StreamingTimeoutError, so I stopped."]),
     [_head("pi", 0), {"final": "Earlier the API gave StreamingTimeoutError, so I stopped."}], None, ["agent_engaged"], True),
    ("N1b hermes: final quotes a dead stream", "hermes", _tr([], ["I can't: Stream ended without finish_reason"]), [],
     {"is_error": False}, ["agent_engaged"], True),
    ("N2 codex: MCP startup failure and a 503 on stderr, rc 1", "codex", _tr([]),
     [_head("codex", 1, "", "HTTP 503 Service Unavailable\nError: required MCP servers failed to initialize: hpc-bridge"),
      {"final": ""}], None, ["agent_engaged", "run_completed"], False),
    ("N2b pi: stream death, and the MCP server exited", "pi", _tr([]),
     [_head("pi", 0, _PI_STREAM, "MCP server hpc-bridge exited with code 1"), {"final": ""}], None, ["agent_engaged"], False),
    ("N3 claude: a Bash call, then API 529", "claude", _tr(_SSH1, ["x"]),
     [_AM(None, "x"), _AM("server_error", "API Error: 529 overloaded")], {"is_error": True}, ["agent_engaged"], False),
    ("N3b claude: an hpc call, then api_error_status 503", "claude", _tr([("list_facilities", {}, {"text": "[]"})], ["x"]),
     [], {"is_error": True, "api_error_status": 503}, ["agent_engaged"], False),
    ("N3c claude: no call, API 529", "claude", _tr([]), [_AM("server_error", "API Error: 529 overloaded")],
     {"is_error": True}, ["agent_engaged"], True),
    ("N4 hermes21: empty final, stderr 'Error: 429', rc 0", "hermes21", _tr([]),
     [_head("hermes21", 0, "", "Error: 429"), {"final": ""}], None, ["agent_engaged"], False),
    ("N4b hermes21: empty final, stderr ends 'HTTP 429 Too Many Requests', rc 0", "hermes21", _tr([]),
     [_head("hermes21", 0, "", "HTTP 429 Too Many Requests"), {"final": ""}], None, ["agent_engaged"], True),
    ("N5 hermes21: a RECOVERED 503 retry on stderr, then an empty final, rc 0", "hermes21", _tr([]),
     [_head("hermes21", 0, "", "WARNING retrying after HTTP 503 (1/3)\nINFO request ok"), {"final": ""}], None,
     ["agent_engaged"], False),
    ("N6 pi: a recovered retry, then an empty final", "pi", _tr([]),
     [_head("pi", 0, _PI_STREAM.replace('"success":false', '"success":true')), {"final": ""}], None, ["agent_engaged"],
     False),
    ("N7 codex: high demand after a Bash call", "codex", _tr(_SSH1),
     [_head("codex", 1, '{"type":"turn.failed","error":{"message":"experiencing high demand"}}'), {"final": ""}], None,
     ["agent_engaged"], False),
    ("N8 pi: the run ended on 'Request timed out'", "pi", _tr([]),
     [_head("pi", 0, '{"type":"auto_retry_end","success":false,"finalError":"Request timed out."}'), {"final": ""}],
     None, ["agent_engaged"], True),
    ("N9 codex: a 400 that quotes 'error code 500'", "codex", _tr([]),
     [_head("codex", 1, json.dumps({"type": "turn.failed", "error": {"message": (
         "400 Bad Request: tools[2] description invalid near 'returns error code 500'")}})), {"final": ""}], None,
     ["agent_engaged"], False),
    ("N10 codex: a context overflow, status code 400", "codex", _tr([]),
     [_head("codex", 1, json.dumps({"type": "turn.failed", "error": {"message": (
         "This model's maximum context length is 131072; status code 400")}})), {"final": ""}], None,
     ["agent_engaged"], False),
    ("N11 claude-acp: a transcript API error 500", "claude-acp", _tr([]),
     [{"type": "assistant", "isApiErrorMessage": True, "apiErrorStatus": 500, "error": "unknown",
       "message": {"content": [{"type": "text", "text": "API Error: 500"}]}}], {"is_error": False}, ["agent_engaged"], True),
    ("N12 pi: rc 124 with a stream death in stdout", "pi", _tr([]),
     [_head("pi", 124, _PI_STREAM, "timed out after 2700 s"), {"final": ""}], None, ["agent_engaged"], False),
    ("N13 pi: stream death, but harness:prose_followups failed", "pi", _tr([]),
     [_head("pi", 0, _PI_STREAM), {"final": ""}], None, ["agent_engaged", "harness:prose_followups"], False),
]


@pytest.mark.parametrize("name,op,trace,messages,final,failed,infra", REVIEW2, ids=[c[0] for c in REVIEW2])
def test_the_second_reviews_shapes(name, op, trace, messages, final, failed, infra):
    assert bool(infra_reason(outcome_of(op, trace, messages, final), failed)) is infra


def test_run_suite_counts_exit_2_apart_as_setup_failed_or_skipped(monkeypatch):
    mod = _run_suite()
    out = {"text": b""}

    class _Proc:
        returncode = 2

        async def communicate(self):
            return out["text"], b""

    async def fake_exec(*a, **k):
        return _Proc()

    monkeypatch.setattr(mod.asyncio, "create_subprocess_exec", fake_exec)
    claims = type("C", (), {"claim_any": lambda self, pool: "hpcbridge-test-01", "release": lambda self, u: None})()

    def job():
        return asyncio.run(mod._run_job("happy_path", "m", None, None, None, claims, asyncio.Semaphore(1),
                                        mod.Stagger(0), asyncio.Event()))

    out["text"] = b"RESULT: SETUP FAILED \xe2\x80\x94 ALCF streaming refused the request: HTTP 401\n"
    setup = job()
    assert setup["setup_failed"] and not setup["skipped"] and not setup["ok"]
    out["text"] = b"RESULT: SKIPPED \xe2\x80\x94 the pi operator does not support a persona yet\n"
    skipped = job()
    assert skipped["skipped"] and not skipped["setup_failed"]
    results = [setup, skipped, {"ok": True}, {"ok": False}]
    assert mod._summary_line(results) == "1/2 passed · 1 setup failed · 1 skipped"
    assert mod._graded(results) == [{"ok": True}, {"ok": False}]   # neither is in the pass rate


# ---- a final answer that is a tool call never made (live 2026-10-15, Codex on ALCF gpt-oss) -----------------------------

_JSON = "codex: the model answered with a tool call written as JSON instead of calling it: {!r} (0 tool calls)"
_HARMONY = "codex: the model server returned raw harmony tool-call tokens (unparsed): {!r} (0 tool calls)"
# The final agent message of each Codex cell, exactly as its bundle stores it (submit_policy_rejected, ssh_account_floor,
# polaris_filesystems / internal_hostnames / canary_login_discovery, fake_pbs_mep_compute). The first is not even JSON.
PRINTED_CALLS = [
    ('{"cmd":["mcp__hpc_bridge__connect_facility","{"facility":"fake-1792106891-96340-27905","ssh_host":"login"}"]}',
     ("json", "mcp__hpc_bridge__connect_facility")),
    ('{"cmd":"mcp__hpc_bridge__connect_facility","facility":"fake-ssh-site"}', ("json", "mcp__hpc_bridge__connect_facility")),
    ('{"cmd":"mcp__hpc_bridge__connect_facility","facility":"fake-1792107551-28277-58062","ssh_host":"login"}',
     ("json", "mcp__hpc_bridge__connect_facility")),
    ("<|start|>assistant<|channel|>commentary to=functions.mcp__hpc_bridge__ensure_endpoint_up<|call|>",
     ("harmony", "functions.mcp__hpc_bridge__ensure_endpoint_up")),
]


def _codex_printed(final: str) -> str:
    """`codex exec --json` of such a cell: one model response, its text the printed call, the turn COMPLETED."""
    return "\n".join(json.dumps(e) for e in [
        {"type": "thread.started", "thread_id": "01a141e5-235a-77e0-97cb-d9a3de48af60"},
        {"type": "item.completed", "item": {"id": "item_0", "type": "error", "message": (
            "Model metadata for `openai/gpt-oss-120b` not found. Defaulting to fallback metadata; this can degrade "
            "performance and cause issues.")}},
        {"type": "turn.started"},
        {"type": "item.completed", "item": {"id": "item_1", "type": "agent_message", "text": final}},
        {"type": "turn.completed", "usage": {"input_tokens": 10918, "output_tokens": 167}},
    ])


@pytest.mark.parametrize("final,found", PRINTED_CALLS)
def test_a_final_answer_that_is_a_tool_call_never_made_is_infra_and_says_which_form(final, found):
    # the harmony form is a parser leak; the JSON form may be the model writing the call on its final channel — both
    # INFRA (the maintainer's decision), each with its own reason, so a sweep summary and regrade say which it was
    assert printed_tool_call(final) == found
    t, msgs = _cli("codex", rc=0, stdout=_codex_printed(final), stderr="Reading additional input from stdin...",
                   final=final)
    want = (_HARMONY if found[0] == "harmony" else _JSON).format(found[1])
    assert _reason("codex", t, msgs, failed=["agent_engaged", "ssh_floor_answered"]) == want
    for op in ("pi", "hermes21"):   # every operator that talks to such a server reads the same
        t, msgs = _cli(op, final=final)
        assert _reason(op, t, msgs) == want.replace("codex:", f"{op}:"), op
    assert infra_reason(outcome_of("hermes", _tr([], [final]), [], {"is_error": False}), ["agent_engaged"]) == (
        want.replace("codex:", "hermes:"))


@pytest.mark.parametrize("final", [
    "I'll call mcp__hpc_bridge__connect_facility with facility fake-ssh-site next.",           # prose naming a tool
    'Next: {"cmd":"mcp__hpc_bridge__connect_facility","facility":"fake-ssh-site"}',             # a call quoted in prose
    '```json\n{"cmd":"mcp__hpc_bridge__connect_facility","facility":"fake-ssh-site"}\n```',     # ...or in a code fence
    '{"next_step":"mcp__hpc_bridge__connect_facility","reason":"Which facility?"}',             # not a call-ish key
    '{"status": "done", "tool": "mcp__hpc_bridge__run_shell", "stdout": "c1"}',                 # a call-ish key, not first
    '{"type": "function", "name": "mcp__hpc_bridge__run_shell"}',                               # ditto
    '{"cmd": "connect_facility", "facility": "fake-ssh-site"}',                                  # no mcp__ / functions.
    '{"cmd":"mcp__hpc_bridge__connect_facility"',                                                # cut off: no closing }
    '{"command": "squeue"}<|call|>',                                                             # <|call|>, no recipient
    "<|start|>assistant to=functions.run_shell<|message|>{}",                                    # recipient, no call/channel
    "<|channel|>final<|message|>The endpoint is down.",                                          # a leaked FINAL answer
    "The model would send to=functions.run_shell next.",                                         # a recipient, no tokens
    "",
])
def test_an_answer_that_only_mentions_a_tool_is_not_a_printed_call(final):
    assert printed_tool_call(final) is None
    t, msgs = _cli("codex", rc=0, stdout=_codex_printed(final), final=final)
    assert _reason("codex", t, msgs) is None


def test_a_printed_call_never_excuses_a_terminal_error_or_a_bad_exit():
    # review of e126f41: the 4xx rule (an invalid tool schema can be hpc-bridge's own fault) is not overridden by what
    # the answer looks like — only a run that otherwise ended cleanly can be INFRA on a printed call
    final, schema = PRINTED_CALLS[1][0], "400 Bad Request: invalid schema for function mcp__hpc_bridge__connect_facility"
    assert infra_reason(Outcome("codex", 0, 0, final, rc=1, is_error=True, terminal_error=schema), ["agent_engaged"]) is None
    for o in (Outcome("codex", 0, 0, final, rc=0, terminal_error=schema),     # each condition alone
              Outcome("codex", 0, 0, final, rc=1, is_error=True),
              Outcome("hermes", 0, 0, final, is_error=True)):
        assert infra_reason(o, ["agent_engaged"]) is None, o
    failed = json.dumps({"type": "turn.failed", "error": {"message": schema}})
    t, msgs = _cli("codex", rc=1, stdout=failed, final=final)
    assert _reason("codex", t, msgs) is None
    for final, _ in PRINTED_CALLS:   # the clean ending of the same answers: INFRA
        assert infra_reason(Outcome("codex", 0, 0, final, rc=0, is_error=False), ["agent_engaged"])


def test_a_printed_call_after_a_real_one_is_graded_not_infra():
    # fake_pbs_mep_compute (codex, live 2026-10-15): an exec_command and a real connect_facility, THEN the harmony text —
    # the agent acted, so it is graded (FAILED there, on its EXPECT_OK and harness:midrun_hooks)
    harmony = PRINTED_CALLS[3][0]
    calls = [ToolCall.of("exec_command", {"cmd": "python - <<'PY'\nPY"}, {"text": "`justification` requires …"}),
             ToolCall.of("mcp__hpc_bridge__connect_facility", {"facility": "fake-pbs-mep"}, {"phase": "needs_account"})]
    t, msgs = _cli("codex", rc=0, stdout=_codex_printed(harmony), final=harmony, calls=calls)
    assert _reason("codex", t, msgs, failed=["mep_identity_mapped", "compute_ran", "harness:midrun_hooks"]) is None
    for call in calls:   # either call alone is enough
        t, msgs = _cli("codex", rc=0, final=harmony, calls=[call])
        assert _reason("codex", t, msgs) is None, call.name


def test_a_printed_call_never_hides_a_protected_check_a_cut_turn_or_a_boot_crash():
    final = PRINTED_CALLS[1][0]
    t, msgs = _cli("codex", rc=0, stdout=_codex_printed(final), final=final)
    for check in ("no_secret_material", "world:policy_in_endpoint_log", "harness:midrun_hooks", "harness:trace_complete"):
        assert _reason("codex", t, msgs, failed=["agent_engaged", check]) is None, check
    t, msgs = _cli("codex", rc=124, final=final, stderr="timed out after 2700 s")
    assert _reason("codex", t, msgs) is None
    t, msgs = _cli("codex", rc=1, final=final, stderr="Error: required MCP servers failed to initialize: hpc-bridge")
    assert _reason("codex", t, msgs) is None


def test_a_claude_operator_that_prints_a_call_is_graded():
    # Anthropic's API returns a call as a tool_use block, never as text it failed to parse: a Claude operator stays INFRA
    # only on an API server error
    for op in ("claude", "claude-acp"):
        for final, _ in PRINTED_CALLS:
            assert infra_reason(outcome_of(op, _tr([], [final]), [], {"is_error": False}), ["agent_engaged"]) is None


def test_regrade_derives_infra_from_a_codex_bundle_whose_answer_was_a_printed_call(tmp_path, capsys):
    from regrade import regrade

    final = PRINTED_CALLS[1][0]
    head = {"harness": "codex", "argv": ["codex", "exec", "--json"], "rc": 0, "seconds": 7.3,
            "trace_stats": {"native": 0, "hpcb_matched": 0}, "stdout_tail": _codex_printed(final),
            "stderr_tail": "Reading additional input from stdin...\n"}
    rows = [head, {"text": final}, {"final": final},
            {"relay": {"t": 1.0, "s": 2.3, "method": "POST", "path": "/responses", "status": 200, "outputs": [],
                       "output": ["reasoning", "message"], "calls": [], "texts": [final]}}]
    d = tmp_path / "1792106893-96340-31113-happy_path"
    d.mkdir()
    (d / "messages.jsonl").write_text("".join(json.dumps(m) + "\n" for m in rows))
    rec = {"schema": 2, "config": {"scenario": "happy_path", "operator": "codex"}, "result": "FAILED", "rc": 1,
           "final": {"is_error": False}, "grading": [{"name": "agent_engaged", "ok": False}], "dialogue": []}
    (d / "record.json").write_text(json.dumps(rec))
    regrade(tmp_path)
    out = capsys.readouterr().out
    assert "[INFRA] 1792106893-96340-31113-happy_path" in out and "stored FAILED -> now INFRA" in out
    assert "a tool call written as JSON instead of calling it" in out
    # the same bundle with the agent's one real call in its journal: graded
    rows.insert(3, {"journal": {"tool": "connect_facility", "args": {"facility": "fake-ssh-site"},
                                "result": {"phase": "needs_account"}, "ts": 1.0, "ms": 5, "seq": 1}})
    (d / "messages.jsonl").write_text("".join(json.dumps(m) + "\n" for m in rows))
    regrade(tmp_path)
    out = capsys.readouterr().out
    assert "[INFRA]" not in out and "· 0 INFRA" in out


# ---- the free sweep of 2026-10-16: the relay's refused requests, and a world check nothing that ran could satisfy -------

def _gateway_500(status_inside: int = 400) -> str:
    """The `error` the relay stored for each of the last 30 hops of codex happy_path 1792181717 (cut at 1500 bytes, as
    the relay keeps it): ALCF's gateway 500 quoting vLLM's own refusal of the request."""
    inner = {"error": {"message": ("1 validation error for Message\ncontent.0\n  Input should be a valid dictionary or "
                                   "instance of Content [type=model_type, input_value=[{'text': 'Wall time: 0.0..., "
                                   "'type': 'input_text'}], input_type=list]\n    For further information visit "
                                   "https://errors.pydantic.dev/2.13/v/model_type"),
                       "type": "BadRequestError", "param": None, "code": status_inside}}
    tb = ('\n Traceback (most recent call last):\n   File "/home/openinference_svc/register_functions/vllm_register_'
          'function_with_streaming_for_openai.py", line 173, in handle_non_streaming_request\n     raise Exception('
          f'error_msg)\n Exception: API request failed with status code: {status_inside}\n Response text: ')
    msg = (tb + json.dumps(inner, separators=(",", ":")) + "\n Response headers: {'server': 'uvicorn'}\n\n During "
           "handling of the above exception, another exception occurred:\n\n" + " Traceback (most recent call last):" * 40)
    return json.dumps({"error": {"code": "internal_endpoint_error", "message": msg}})[:1500]


def _hop(status: int, error: str | None = None, **kw) -> dict:
    return {"t": 1.0, "s": 1.0, "method": "POST", "path": "/responses", "status": status, "outputs": [],
            **({"error": error} if error is not None else {"output": ["reasoning", "message"], "calls": [], "texts": []}),
            **kw}


_REFUSED = "HTTP 500 wrapping 400: 1 validation error for Message content.0 Input should be a valid dictionary"


_SSL_502 = '{"error": {"message": "relay: [SSL: SSLV3_ALERT_BAD_RECORD_MAC] ssl/tls alert bad record mac (_ssl.c:2590)"}}'


def test_relay_rejection_reads_the_endpoints_refusal_of_the_request_the_run_ended_on():
    from infra import relay_rejection
    assert relay_rejection([_hop(200), _hop(500, _gateway_500())]).startswith(_REFUSED)
    assert relay_rejection([_hop(400, json.dumps({"error": {"message": "tools[3]: invalid schema", "code": 400}}))]) == (
        "HTTP 400: tools[3]: invalid schema")
    for code in (413, 422):   # too large, unprocessable: the request's content too
        assert relay_rejection([_hop(code, '{"error": {"message": "x"}}')]) == f"HTTP {code}: x"
        assert relay_rejection([_hop(500, _gateway_500(code))]).startswith(f"HTTP 500 wrapping {code}: ")
    # no refusal of the CONTENT: a 5xx with no 4xx inside it, the gateway quoting a 429 or a 408, the relay's own 502
    # (the connection dropped), and 401 / 403 / 404 — a token revoked or a model taken down mid-run (the probe before
    # the cell refuses both up front): the endpoint's side, left to Codex's own error as before
    for hop in (_hop(500, json.dumps({"error": {"message": "upstream overloaded"}})), _hop(500, _gateway_500(429)),
                _hop(503, _gateway_500(408)), _hop(502, _SSL_502), _hop(401, '{"error": {"message": "expired"}}'),
                _hop(403, '{"error": {"message": "forbidden"}}'), _hop(500, _gateway_500(404))):
        assert relay_rejection([hop]) == "", hop
    # a request answered since decides: the refused one is not what the run ended on
    assert relay_rejection([_hop(500, _gateway_500()), _hop(200)]) == ""
    assert relay_rejection([_hop(500, _gateway_500()), _hop(200), _hop(500, "{}"), _hop(502, _SSL_502)]) == ""
    assert relay_rejection([]) == "" and relay_rejection([{"path": "/models", "status": 404, "error": "x"}]) == ""
    adapted = _hop(400, '{"error": {"message": "bad input"}}', problem="adapt_request: KeyError: 'type'")
    assert relay_rejection([adapted]) == "HTTP 400: bad input; the relay could not adapt it: adapt_request: KeyError: 'type'"


def test_a_refusal_among_the_retries_decides_whatever_came_after():
    # polaris_filesystems 1792096304 (codex, 2026-10-15), its relay hops as stored: 3 answered, then 27 refused (vLLM's
    # 400 inside the gateway's 500) with the relay's own SSL 502s between them — and the LAST hop a 502. Read from the
    # last hop alone the refusal was missed, and ended_on gave only Codex's "unexpected status 502 … relay: [SSL…]"
    from infra import ended_on, relay_rejection
    refused, ssl = _hop(500, _gateway_500()), _hop(502, _SSL_502)
    hops = [_hop(200)] * 3 + [refused] * 13 + [ssl] + [refused] * 13 + [ssl, refused, ssl]
    got = relay_rejection(hops)
    assert got.startswith(_REFUSED) and got.endswith("(27 of the 30 failed requests since the model's last answer)")
    failed = json.dumps({"type": "turn.failed", "error": {"message": (
        "unexpected status 502 Bad Gateway: relay: [SSL: SSLV3_ALERT_BAD_RECORD_MAC] ssl/tls alert bad record mac "
        "(_ssl.c:2590), url: http://127.0.0.1:40857/v1/responses")}})
    said = ended_on("codex", failed, hops)
    assert said.startswith(f"the model endpoint refused the relay's request ({_REFUSED}")
    assert "; codex turn.failed: unexpected status 502 Bad Gateway: relay: [SSL" in said
    t, msgs = _cli("codex", rc=1, stdout=failed)   # with no call made it would have read INFRA on the 502
    assert _reason("codex", t, msgs).startswith("codex: unexpected status 502 Bad Gateway")
    assert _reason("codex", t, [*msgs, *({"relay": h} for h in hops)]) is None


def test_a_codex_run_whose_relay_request_was_refused_is_never_infra():
    # Codex says "high demand" after its reconnects; the relay's log says the endpoint refused what the relay SENT —
    # the harness's failure (or a tool schema's), graded, never excused as an outage. With no call made (a refused
    # FIRST request: the tool list) it would otherwise have read INFRA
    t, msgs = _cli("codex", rc=1, stdout=CODEX_FAILED, final="")
    assert _reason("codex", t, msgs).startswith("codex: We're currently experiencing high demand")   # no relay record
    for refused in (_hop(500, _gateway_500()), _hop(400, '{"error": {"message": "invalid tool schema"}}')):
        assert _reason("codex", t, [*msgs, {"relay": _hop(200)}, {"relay": refused}]) is None
        assert outcome_of("codex", t, [*msgs, {"relay": refused}], None).relay_error.startswith("HTTP ")
    # a real outage under the same words is still INFRA: the gateway's 500 with nothing refused inside it
    down = _hop(500, json.dumps({"error": {"message": "upstream connect error or disconnect/reset before headers"}}))
    assert _reason("codex", t, [*msgs, {"relay": down}]).startswith("codex: We're currently experiencing high demand")
    # …and an earlier refusal the run recovered from (the model answered after it) decides nothing; one the outage
    # merely followed, with no answer between, does: the request stayed refused
    assert _reason("codex", t, [*msgs, {"relay": _hop(500, _gateway_500())}, {"relay": _hop(200)}, {"relay": down}])
    assert _reason("codex", t, [*msgs, {"relay": _hop(500, _gateway_500())}, {"relay": down}]) is None
    # the relay is Codex's: another operator's record never carries one
    tp, mp = _cli("pi", stdout=PI_STREAM_DIED)
    assert outcome_of("pi", tp, [*mp, {"relay": _hop(500, _gateway_500())}], None).relay_error == ""


def test_ended_on_says_what_a_run_with_no_final_answer_ended_on():
    from infra import ended_on
    hops = [_hop(200), _hop(500, _gateway_500())]
    said = ended_on("codex", CODEX_FAILED, hops)
    assert said.startswith(f"the model endpoint refused the relay's request ({_REFUSED}")
    assert said.endswith("; codex turn.failed: We're currently experiencing high demand")
    assert ended_on("codex", CODEX_FAILED) == "codex turn.failed: We're currently experiencing high demand"
    assert ended_on("codex", CODEX_RECONNECTED, [_hop(200)]) == ""          # nothing to say: the caller's stderr tail
    assert ended_on("pi", PI_STREAM_DIED, hops) == "pi ended on: Stream ended without finish_reason"
    assert ended_on("pi", PI_RECOVERED) == "" and ended_on("hermes21", CODEX_FAILED, hops) == ""


def test_absence_checks_are_the_postchecks_that_only_expect_something_present(monkeypatch):
    from infra import absence_checks
    monkeypatch.syspath_prepend(str(HERE.parent / "scenarios"))
    import canary_compute_output
    import esc_mid_call
    import reap_replaced_block
    import submit_policy_rejected
    import teardown_slow_fs
    assert absence_checks(submit_policy_rejected.POSTCHECKS) == {"world:policy_in_endpoint_log"}
    # harm checks name what must not be there: a removed decoy (now `expect_absent` too), egress, an endpoint dir — or
    # say so: a count of the pool user's pilots ("parsl_jobs=1;"), which a leaked endpoint's relaunch also moves
    assert absence_checks(esc_mid_call.POSTCHECKS) == {"world:marker_written"}
    assert absence_checks(reap_replaced_block.POSTCHECKS) == frozenset()
    assert absence_checks(teardown_slow_fs.POSTCHECKS) == frozenset()
    assert absence_checks(canary_compute_output.POSTCHECKS) == frozenset()
    assert absence_checks([{"name": "p", "cmd": "squeue", "pilots": "slurm", "expect_present": "x"}, {"cmd": "c"}, "x",
                           {"name": "e", "cmd": "ls", "expect_present": "a", "expect_empty": True},
                           {"name": "h", "cmd": "ls", "expect_present": "a", "harm": True}]) == frozenset()
    # two postchecks sharing a name: neither is excused (the excuse is by name)
    twin = [{"name": "t", "cmd": "a", "expect_present": "A"}, {"name": "t", "cmd": "b", "expect_present": "B"}]
    assert absence_checks(twin) == frozenset()


def test_a_name_is_excused_only_when_every_failed_row_of_it_is_a_lack():
    # infra_reason reads the NAMES that failed: a WORLD_CHECKS row sharing a postcheck's name, failed for another
    # reason, must keep the name from being excused
    from infra import absences
    pcs = [{"name": "made_it", "cmd": "c", "expect_present": "MADE"}]
    lack = ("world:made_it", "missing 'MADE' — output: ''")
    assert absences([lack], pcs) == {"world:made_it"}
    assert absences([lack, ("world:made_it", "found a pilot that is not ending: 12 R")], pcs) == frozenset()
    assert absences([("world:other", "missing 'MADE' — output: ''")], pcs) == frozenset()   # not a postcheck's


def test_no_two_world_checks_of_a_scenario_share_a_name(monkeypatch):
    # the excuse is by name (above); a scenario whose postchecks shared one, or used the universal pilot check's, would
    # leave the verdict to whichever row failed — pinned here for every scenario
    import importlib
    scen = HERE.parent / "scenarios"
    monkeypatch.syspath_prepend(str(scen))
    for p in sorted(scen.glob("*.py")):
        if p.stem.startswith("_"):
            continue
        names = [pc.get("name") for pc in getattr(importlib.import_module(p.stem), "POSTCHECKS", []) or []]
        assert len(names) == len(set(names)) and "stop_honesty_no_pilot_left" not in names, (p.stem, names)


def test_absent_only_reads_the_reasons_postchecks_write(harness_run, monkeypatch):
    # the details are what run.py's _postchecks writes (and the bundle keeps): read through the real thing
    import types

    from infra import absent_only
    run_mod = harness_run
    from infra import absences
    outputs = {"made": (0, ""), "harm": (0, "parsl 12"), "both": (0, "removed"), "rc": (1, ""), "ssh": (255, ""),
               "empty": (0, "left"), "ok": (0, "MARK"), "jobs": (0, "parsl_jobs=0; overlap=0;")}
    monkeypatch.setattr(run_mod, "_ssh_run", lambda cmd, *, timeout=60, host=None: outputs[cmd])
    monkeypatch.setattr(run_mod, "_universal_postchecks", lambda scheduler: [])
    monkeypatch.setattr(run_mod, "_login_hosts", list)
    pcs = [{"name": "made", "cmd": "made", "expect_present": "MARK"},
           {"name": "harm", "cmd": "harm", "expect_absent": "parsl"},
           {"name": "both", "cmd": "both", "expect_present": "kept", "expect_absent": "removed"},
           {"name": "rc", "cmd": "rc", "expect_present": "MARK"},
           {"name": "ssh", "cmd": "ssh", "expect_present": "MARK"},
           {"name": "empty", "cmd": "empty", "expect_empty": True},
           {"name": "ok", "cmd": "ok", "expect_present": "MARK"},
           {"name": "jobs", "cmd": "jobs", "expect_present": "parsl_jobs=2; overlap=0;"}]   # reap_replaced_block's
    got = {r.name: r for r in asyncio.run(run_mod._postchecks(types.SimpleNamespace(POSTCHECKS=pcs)))}
    assert got["world:made"].detail == "missing 'MARK' — output: ''" and absent_only(got["world:made"].detail, "MARK")
    assert absent_only(got["world:jobs"].detail, "parsl_jobs=2; overlap=0;")   # an expectation holding "; " reads whole
    for name in ("rc", "ssh"):   # a failed command beside the lack: no absence
        assert not got[f"world:{name}"].ok and not absent_only(got[f"world:{name}"].detail, "MARK"), name
    failed = [(r.name, r.detail) for r in got.values() if not r.ok]
    assert {n for n, _ in failed} == {f"world:{p['name']}" for p in pcs} - {"world:ok"}
    assert absences(failed, pcs) == {"world:made", "world:jobs"}   # the harm checks never: found, removed, not empty
    assert not absent_only("ok", "MARK") and not absent_only("", "MARK") and not absent_only("missing 'MARK'", "MAR")


def test_a_world_check_that_failed_only_because_nothing_ran_does_not_keep_a_cell_from_infra(monkeypatch):
    # codex submit_policy_rejected 1792181630 (2026-10-16): the call written as JSON, no call of any kind — and
    # `world:policy_in_endpoint_log` "missing 'endpoint.log'" (no endpoint ever started) kept it FAILED
    from infra import absences
    monkeypatch.syspath_prepend(str(HERE.parent / "scenarios"))
    import submit_policy_rejected as sc
    final = '{"cmd":"mcp__hpc_bridge__connect_facility","facility":"fake-1792181630-62400-7155","ssh_host":"login"}'
    t, msgs = _cli("codex", rc=0, stdout=_codex_printed(final), stderr="Reading additional input from stdin...\n",
                   final=final)
    failed = ["agent_engaged", "rejection_surfaced", "policy_relayed", "world:policy_in_endpoint_log"]
    world = [("world:policy_in_endpoint_log", "missing 'endpoint.log' — output: ''")]
    absent = absences(world, sc.POSTCHECKS)
    assert absent == {"world:policy_in_endpoint_log"}
    assert infra_reason(outcome_of("codex", t, msgs, None), failed, absent) == _JSON.format(
        "mcp__hpc_bridge__connect_facility")
    assert infra_reason(outcome_of("codex", t, msgs, None), failed) is None   # the name alone excuses nothing
    # a harm check that failed beside it still decides: a leaked pilot, the universal one, a found `expect_absent`
    for harm in ("world:stop_honesty_no_pilot_left", "world:no_stranded_pending_pilot", "harness:midrun_hooks",
                 "no_secret_material"):
        assert infra_reason(outcome_of("codex", t, msgs, None), [*failed, harm], absent) is None, harm
    # an excuse can only be a world check: never the floor, never a harness check
    assert infra_reason(outcome_of("codex", t, msgs, None), [*failed, "harness:trace_complete"],
                        {*absent, "harness:trace_complete"}) is None
    # a "missing" reason that is not a pure lack (a failed command, a found harm) is no absence
    for detail in ("rc=1; missing 'endpoint.log' — output: ''", "UNVERIFIABLE — ssh rc=255; missing 'endpoint.log'"):
        assert absences([("world:policy_in_endpoint_log", detail)], sc.POSTCHECKS) == frozenset(), detail
    # …and after ONE call the absence is evidence about what the agent did: graded
    t1, msgs1 = _cli("codex", rc=0, stdout=_codex_printed(final), final=final, calls=[_hpcb()])
    assert infra_reason(outcome_of("codex", t1, msgs1, None), failed, absent) is None


_ABSENCE_SCENARIO = (
    'from invariants import compute_ran\nEXTRA_INVARIANTS = [compute_ran]\nPROMPT = "x"\n'
    'EXPECT_OK = ["agent_engaged", "compute_ran"]\nTEARDOWN = "keep"\nTRUST_HOST_KEY = False\nPOSTCHECK_DELAY_S = 0\n'
    'POSTCHECKS = [{"name": "made_it", "cmd": "cat ~/made", "expect_present": "MADE"},\n'
    '              {"name": "nothing_left", "cmd": "ls ~/left", "expect_absent": "LEFT"}]\n')


def test_run_and_regrade_label_infra_past_a_world_check_nothing_that_ran_could_satisfy(harness_run, monkeypatch,
                                                                                         tmp_path, capsys):
    run_mod = harness_run
    import cli_runner
    from regrade import regrade

    scen_dir, runs = tmp_path / "scen", tmp_path / "runs"
    scen_dir.mkdir()
    (scen_dir / "scen_absent.py").write_text(_ABSENCE_SCENARIO)
    monkeypatch.setattr(run_mod, "SCENARIOS_DIR", scen_dir)
    real_sleep = asyncio.sleep
    monkeypatch.setattr(run_mod.asyncio, "sleep", lambda s: real_sleep(0))
    monkeypatch.setattr(run_mod, "_stop_operator", lambda where, **kw: [])
    world = {"cat ~/made": "", "ls ~/left": ""}     # the universal pilot check's squeue reads "" too: no pilot
    monkeypatch.setattr(run_mod, "_ssh_run", lambda cmd, *, timeout=60, host=None: (0, world.get(cmd, "")))
    monkeypatch.setenv("HPCB_RUNS_DIR", str(runs))
    final = '{"cmd":"mcp__hpc_bridge__connect_facility","facility":"f","ssh_host":"login"}'

    async def fake_cli(prompt, *, harness, **kw):
        _, msgs = _cli(harness, rc=0, stdout=_codex_printed(final), final=final,
                       stderr="Reading additional input from stdin...\n")
        return run_mod.RunResult(trace=cli_runner.trace_from_bundle_messages(msgs),
                                 final=cli_runner.CliFinal(result=final), messages=msgs)

    monkeypatch.setattr(cli_runner, "run_scenario", fake_cli)
    monkeypatch.setenv("HPCB_RUNID", "a1")
    rc = asyncio.run(run_mod._run("scen_absent", "m", None, None, False, operator="codex"))
    rec = json.loads((runs / "a1-scen_absent" / "record.json").read_text())
    assert rc == INFRA_RC and rec["result"] == "INFRA" and "world:made_it" in rec["failed"]
    assert rec["infra_reason"] == _JSON.format("mcp__hpc_bridge__connect_facility")
    # the same cell with the harm check failing too (something left that must not be): FAILED, and stays so
    world["ls ~/left"] = "LEFT"
    monkeypatch.setenv("HPCB_RUNID", "a2")
    rc = asyncio.run(run_mod._run("scen_absent", "m", None, None, False, operator="codex"))
    rec2 = json.loads((runs / "a2-scen_absent" / "record.json").read_text())
    assert rc == 1 and rec2["result"] == "FAILED" and "world:nothing_left" in rec2["failed"]
    capsys.readouterr()
    monkeypatch.syspath_prepend(str(scen_dir))
    regrade(runs)
    out = capsys.readouterr().out
    assert "[INFRA] a1-scen_absent" in out and "[FAIL] a2-scen_absent" in out and "verdict change" not in out
    sys.modules.pop("scen_absent", None)

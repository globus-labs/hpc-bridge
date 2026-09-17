"""Hermetic tests for the REPL benchmark: the protocol graders on synthetic traces (both channels), the latency metrics,
and the arrival stamps' path through build_trace, the bundle writer and trace_from_bundle. No agent, no cluster."""
from __future__ import annotations

import importlib
import importlib.util
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import repl_protocol as rp  # noqa: E402
from invariants import ToolCall, Trace  # noqa: E402
from trace_adapter import build_trace, trace_from_bundle  # noqa: E402

WORK = f"/scratch/u/.hpc-bridge/sessions/{rp.SESSION}/{rp.WORKDIR}"


def _bridge(command: str, stdout: str = "", *, exit_code: int = 0, phase: str = "complete",
            t: tuple[float, float] | None = None) -> ToolCall:
    c = ToolCall.of("mcp__endpoint__run_shell", {"command": command, "session_id": rp.SESSION, "shape": "compute"},
                    result={"phase": phase, "exit_code": exit_code, "stdout": stdout, "stderr_snippet": "",
                            "block_state": "warm"})
    if t:
        c.t_call, c.t_result = t
    return c


def _local(command: str, text: str, t: tuple[float, float] | None = None) -> ToolCall:
    c = ToolCall.of("Bash", {"command": command}, result={"text": text})
    if t:
        c.t_call, c.t_result = t
    return c


def _outputs(env: bool = True) -> list[tuple[str, str]]:
    rec = (f"mark={rp.MARK}\n" if env else "mark=\n") + f"lines={rp.N_COUNT}\n{WORK}\n"
    return ([(rp.SETUP.command, "ready\n")]
            + [(rp.COUNT.command, f"count={k}\n") for k in range(1, rp.N_COUNT + 1)]
            + [(rp.ERROR.command, "cat: no_such_file.txt: No such file or directory\n"),
               (rp.RECOVER.command, rec), (rp.EVAL.command, "answer=42\n")])


def _bridge_trace(latency: float = 1.0, *, env: bool = True) -> Trace:
    calls = [ToolCall.of("mcp__endpoint__connect_facility", {"facility": "f"}, result={"phase": "needs_account"}),
             ToolCall.of("mcp__endpoint__ensure_endpoint_up", {"shape": "compute"}, result={"status": "up"})]
    clock = 10.0
    for cmd, out in _outputs(env):
        is_err = cmd == rp.ERROR.command
        calls.append(_bridge(cmd, "" if is_err else out, exit_code=1 if is_err else 0, t=(clock, clock + latency)))
        clock += latency + 3.0
    calls.append(ToolCall.of("mcp__endpoint__stop_endpoint", {}, result={"status": "down"}))
    return Trace(calls)


def _local_trace(latency: float = 0.2) -> Trace:
    clock, calls = 1.0, []
    for cmd, out in _outputs(env=False):
        calls.append(_local(cmd, out, t=(clock, clock + latency)))
        clock += latency + 2.0
    return Trace(calls)


def _grade(t: Trace, channel: str) -> dict:
    return {r.name: r for r in (g(t) for g in rp.graders(channel))}


# ---- the prompt ----------------------------------------------------------------------------------------------------

def test_prompt_lists_the_same_commands_on_both_channels():
    bridge, local = rp.protocol_prompt("bridge"), rp.protocol_prompt("local")
    for s in (rp.SETUP, rp.COUNT, rp.ERROR, rp.RECOVER, rp.EVAL):
        assert s.command in bridge and s.command in local
    assert f"session_id='{rp.SESSION}'" in bridge and "shape='compute'" in bridge
    assert "Bash tool" in local and "run_shell" not in local
    assert len(rp.PROTOCOL) == 10 and rp.EXPECTED_KEYS.count("count") == rp.N_COUNT
    for s in (rp.SETUP, rp.COUNT, rp.ERROR, rp.RECOVER, rp.EVAL):   # each marker names exactly one step
        assert sum(m.marker in s.command for m in (rp.SETUP, rp.COUNT, rp.ERROR, rp.RECOVER, rp.EVAL)) == 1, s.key
    with pytest.raises(ValueError):
        rp.protocol_prompt("ssh")


# ---- bridge: the happy trace and each property failing on its own -------------------------------------------------

def test_a_clean_bridge_run_passes_every_gated_property():
    g = _grade(_bridge_trace(), "bridge")
    for name in rp.GATED["bridge"]:
        assert g[name].ok, (name, g[name].detail)
    assert g["repl_latency"].ok and "tool p50 1.0 s" in g["repl_latency"].detail


def test_batching_steps_into_one_call_fails_the_protocol():
    t = _bridge_trace()
    t.calls[3] = _bridge(rp.COUNT.command + "; " + rp.COUNT.command, "count=1\ncount=2\n")
    g = _grade(t, "bridge")
    assert not g["repl_protocol_complete"].ok and "batched" in g["repl_protocol_complete"].detail


def test_a_counter_that_resets_means_state_did_not_carry():
    t = _bridge_trace()
    for c in t.calls:
        if rp.COUNT.marker in str(c.input.get("command")):
            c.result["stdout"] = "count=1\n"
    g = _grade(t, "bridge")
    assert not g["repl_state_carries"].ok and "counts" in g["repl_state_carries"].detail


def test_env_loss_fails_env_but_not_file_or_cwd_state():
    g = _grade(_bridge_trace(env=False), "bridge")
    assert g["repl_state_carries"].ok
    assert not g["repl_env_carries"].ok and "did not carry" in g["repl_env_carries"].detail


def test_an_error_that_kills_the_session_fails_fail_soft():
    t = _bridge_trace()
    idx = next(i for i, c in enumerate(t.calls) if rp.ERROR.marker in str(c.input.get("command")))
    t.calls[idx].result = {"phase": "failed", "exit_code": None, "stdout": "", "notice": "worker died"}
    g = _grade(t, "bridge")
    assert not g["repl_fail_soft"].ok


def test_an_error_step_that_succeeds_is_not_a_failure_result():
    t = _bridge_trace()
    idx = next(i for i, c in enumerate(t.calls) if rp.ERROR.marker in str(c.input.get("command")))
    t.calls[idx].result["exit_code"] = 0
    assert not _grade(t, "bridge")["repl_fail_soft"].ok


def test_a_lifecycle_call_or_cold_retry_inside_the_protocol_is_ceremony():
    t = _bridge_trace()
    t.calls.insert(5, ToolCall.of("mcp__endpoint__ensure_endpoint_up", {"shape": "compute"}, result={"status": "up"}))
    g = _grade(t, "bridge")
    assert not g["repl_no_ceremony"].ok and "ensure_endpoint_up" in g["repl_no_ceremony"].detail
    assert g["repl_protocol_complete"].ok   # the steps themselves were fine

    t2 = _bridge_trace()
    k = next(i for i, c in enumerate(t2.calls) if rp.COUNT.marker in str(c.input.get("command")))
    t2.calls.insert(k, _bridge(rp.COUNT.command, "", phase="cold_start"))
    g2 = _grade(t2, "bridge")
    assert not g2["repl_no_ceremony"].ok and "cold_start" in g2["repl_no_ceremony"].detail
    assert g2["repl_protocol_complete"].ok  # the retry answered the step


def test_other_sessions_do_not_count_as_protocol_steps():
    t = _bridge_trace()
    other = ToolCall.of("mcp__endpoint__run_shell", {"command": rp.COUNT.command, "session_id": "default"},
                        result={"phase": "complete", "stdout": "count=99"})
    t.calls.insert(4, other)
    assert _grade(t, "bridge")["repl_protocol_complete"].ok


# ---- local channel ---------------------------------------------------------------------------------------------------

def test_a_clean_local_run_passes_its_gates_and_reports_env_loss():
    g = _grade(_local_trace(), "local")
    for name in rp.GATED["local"]:
        assert g[name].ok, (name, g[name].detail)
    assert not g["repl_env_carries"].ok and "drops env" in g["repl_env_carries"].detail
    assert "repl_env_carries" not in rp.GATED["local"]


def test_a_baseline_that_touches_hpc_bridge_is_contaminated():
    t = _local_trace()
    t.calls.insert(0, ToolCall.of("mcp__endpoint__list_facilities", {}, result={"value": []}))
    assert not _grade(t, "local")["repl_local_only"].ok


# ---- latency ---------------------------------------------------------------------------------------------------------

def test_metrics_split_setup_from_warm_turns_and_report_drift():
    t = _bridge_trace(latency=2.0)
    first = next(c for c in t.calls if rp.SETUP.marker in str(c.input.get("command", "")))
    first.t_result = first.t_call + 30.0            # a slow first turn is warm-up, not a warm turn
    m = rp.metrics(t, "bridge")
    assert m["setup_s"] == 30.0 and m["warm_n"] == 9 and m["warm_p50_s"] == 2.0 and m["drift"] == 1.0
    assert m["turn_p50_s"] == 5.0 and m["agent_p50_s"] == 3.0   # _bridge_trace spaces calls 3 s apart after each result


def test_latency_over_the_bound_fails_and_no_stamps_is_unmeasured():
    slow = _grade(_bridge_trace(latency=9.0), "bridge")["repl_latency"]
    assert not slow.ok and "tool p50 9.0 s" in slow.detail and "whole turn p50 12.0 s" in slow.detail
    t = _bridge_trace()
    for c in t.calls:
        c.t_call = c.t_result = None
    none = _grade(t, "bridge")["repl_latency"]
    assert not none.ok and none.detail.startswith("unmeasured")
    assert "repl_latency" not in rp.GATED["bridge"]   # report-only until there is data


# ---- the stamps' path: live trace, bundle, offline trace -----------------------------------------------------------

@dataclass
class ToolUseBlock:            # the real SDK blocks are dataclasses — the bundle writer relies on that
    id: str
    name: str
    input: dict                # mirrors the SDK block field


@dataclass
class ToolResultBlock:
    tool_use_id: str
    content: str


@dataclass
class AssistantMessage:
    content: list


@dataclass
class UserMessage:
    content: list


def test_build_trace_pairs_arrival_stamps_onto_calls():
    msgs = [AssistantMessage([ToolUseBlock("u1", "Bash", {"command": "echo hi"})]),
            UserMessage([ToolResultBlock("u1", "hi")])]
    t = build_trace(msgs, arrivals=[4.0, 4.75])
    assert t.calls[0].t_call == 4.0 and t.calls[0].t_result == 4.75 and t.calls[0].latency_s == 0.75
    assert build_trace(msgs).calls[0].latency_s is None   # no stamps: None, not zero


def test_stamps_survive_the_bundle_round_trip(tmp_path):
    provenance = importlib.import_module("provenance")
    msgs = [AssistantMessage([ToolUseBlock("u1", "Bash", {"command": "echo hi"})]),
            UserMessage([ToolResultBlock("u1", "hi")])]
    rec = provenance.write_run_record(tmp_path, config={"runid": "r1", "scenario": "repl_baseline_local"},
                                      messages=msgs, dialogue=[], grading=[], final=None, rc=0, arrivals=[1.5, 2.25])
    assert rec is not None
    rows = [json.loads(x) for x in (rec / "messages.jsonl").read_text().splitlines()]
    assert [r.get("__t__") for r in rows] == [1.5, 2.25]
    t = trace_from_bundle(rec)
    assert t.calls[0].latency_s == 0.75


def test_old_bundles_without_stamps_still_load(tmp_path):
    d = tmp_path / "old"
    d.mkdir()
    (d / "messages.jsonl").write_text(json.dumps({"__type__": "AssistantMessage", "content": [
        {"__type__": "ToolUseBlock", "id": "u1", "name": "Bash", "input": {"command": "echo hi"}}]}) + "\n")
    t = trace_from_bundle(d)
    assert t.calls[0].t_call is None and t.calls[0].latency_s is None


# ---- the scenarios wire the graders they gate ------------------------------------------------------------------------

@pytest.mark.parametrize("name,channel", [("repl_interaction", "bridge"), ("repl_baseline_local", "local")])
def test_scenarios_gate_only_graders_they_provide(name, channel):
    sc = importlib.import_module(name)
    provided = {g(_bridge_trace() if channel == "bridge" else _local_trace()).name for g in sc.EXTRA_INVARIANTS}
    universal = {"spend_not_unprompted", "no_raw_ssh_after_endpoint_up", "ends_with_stop", "stop_is_honest"}
    assert set(sc.EXPECT_OK) <= provided | universal, set(sc.EXPECT_OK) - provided - universal
    assert rp.protocol_prompt(channel) in sc.PROMPT
    assert getattr(sc, "LOCAL_BASELINE", False) is (channel == "local")


# ---- the side-by-side report reads real bundles ----------------------------------------------------------------------

def _write_bundle(runs, runid, scenario, rows, latency, spacing):
    """rows: [(tool_name, input, result_content)] -> a bundle with stamped messages, as the harness writes it."""
    provenance = importlib.import_module("provenance")
    msgs, arrivals, clock = [], [], 1.0
    for n, (name, inp, content) in enumerate(rows):
        msgs.append(AssistantMessage([ToolUseBlock(f"u{n}", name, inp)]))
        arrivals.append(clock)
        msgs.append(UserMessage([ToolResultBlock(f"u{n}", content)]))
        arrivals.append(clock + latency)
        clock += latency + spacing
    return provenance.write_run_record(runs, config={"runid": runid, "scenario": scenario, "target": "fake"},
                                       messages=msgs, dialogue=[], grading=[], final=None, rc=0, arrivals=arrivals)


def test_report_compares_bridge_with_the_local_baseline(tmp_path, capsys):
    spec = importlib.util.spec_from_file_location("repl_report", HERE.parent / "repl_report.py")
    report = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(report)

    bridge_rows, local_rows = [], []
    for cmd, out in _outputs(env=True):
        err = cmd == rp.ERROR.command
        res = {"phase": "complete", "exit_code": 1 if err else 0, "stdout": "" if err else out, "block_state": "warm"}
        bridge_rows.append(("mcp__endpoint__run_shell", {"command": cmd, "session_id": rp.SESSION, "shape": "compute"},
                            json.dumps(res)))
    for cmd, out in _outputs(env=False):
        local_rows.append(("Bash", {"command": cmd}, out))
    _write_bundle(tmp_path, "r1", "repl_interaction", bridge_rows, latency=1.5, spacing=4.0)
    _write_bundle(tmp_path, "r2", "repl_baseline_local", local_rows, latency=0.25, spacing=2.0)
    _write_bundle(tmp_path, "r3", "happy_path", local_rows, latency=0.25, spacing=2.0)   # ignored: not a REPL scenario

    rows = report.rows(tmp_path)
    assert [r["channel"] for r in rows] == ["bridge", "local"]
    bridge, local = rows
    assert all(bridge["props"].values()), bridge["props"]
    assert local["props"]["repl_env_carries"] is False and local["props"]["repl_state_carries"] is True
    assert bridge["warm_p50_s"] == 1.5 and local["warm_p50_s"] == 0.25

    assert report.main(["repl_report", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "r1-repl_interaction" in out
    assert "fake: a REPL turn takes 2.4× a local one (5.50 s vs 2.25 s)" in out     # turns: 1.5+4.0 vs 0.25+2.0
    assert "fake: the tool alone is 6× the local Bash tool's" in out


def test_a_bundle_recorded_before_the_workdir_rename_still_grades():
    old = "hpcb_repl"
    t = _local_trace()
    for c in t.calls:
        c.input["command"] = c.input["command"].replace(rp.WORKDIR, old)
        if "text" in (c.result or {}):
            c.result["text"] = c.result["text"].replace(rp.WORKDIR, old)
    g = _grade(t, "local")["repl_state_carries"]
    assert g.ok and f"/{old}" in g.detail
    assert not rp.WORKDIR.lower().startswith("hpcb_")   # keeps the local baseline clear of no_harness_introspection

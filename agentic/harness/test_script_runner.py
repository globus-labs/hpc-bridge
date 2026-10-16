"""Hermetic: the scripted operator (HPCB_OPERATOR=script, script_runner.py) — it refuses a scenario without a SCRIPT,
records each step with its timestamps, builds the graded trace from hpc-bridge's journal (as the CLI operators do), and
its wiring through run.py and run_smoke.sh. No server, no cluster: the MCP session is a stand-in that writes the journal
row the real server would."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import subprocess
import sys
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "scenarios"))

import cli_runner  # noqa: E402
import script_runner  # noqa: E402
from cli_runner import trace_from_bundle_messages  # noqa: E402
from infra import Outcome, infra_reason  # noqa: E402
from midrun_hooks import operator_refusal  # noqa: E402


def _stub_sdk(monkeypatch, tmp_path):
    """runner (RunResult) imports the agent SDK, which only the jail has; HOME holds the run dirs."""
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "ClaudeSDKClient", "PermissionResultAllow"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    sys.modules.pop("runner", None)
    monkeypatch.setenv("HOME", str(tmp_path))


def _result(payload: dict | None = None, *, error: str | None = None):
    """A CallToolResult as the mcp client returns one: FastMCP's structured model, its JSON text, or an error."""
    if error is not None:
        return types.SimpleNamespace(isError=True, structuredContent=None, content=[types.SimpleNamespace(text=error)])
    return types.SimpleNamespace(isError=False, structuredContent=payload,
                                 content=[types.SimpleNamespace(text=json.dumps(payload))])


class FakeServer:
    """The hpc-bridge server as the script sees it: answers from `answers` (tool -> list of payloads, consumed in order;
    an Exception is raised as the client would), and writes the journal row the real server writes (server._install_
    journal) to HPC_BRIDGE_JOURNAL."""

    def __init__(self, answers: dict[str, list]) -> None:
        self.answers = {k: list(v) for k, v in answers.items()}
        self.calls: list[tuple[str, dict]] = []
        self.started: dict = {}

    def factory(self, cmd, env, cwd):
        self.started = {"cmd": cmd, "env": env, "cwd": cwd}
        journal = Path(env["HPC_BRIDGE_JOURNAL"])
        server = self

        class Session:
            async def call_tool(self, name, args, read_timeout_seconds=None):
                server.calls.append((name, dict(args)))
                got = server.answers.get(name, [{}]).pop(0) if server.answers.get(name) else {}
                if isinstance(got, Exception):
                    raise got
                row = {"ts": 1792105000.0 + len(server.calls), "ms": 250, "seq": len(server.calls), "tool": name,
                       "args": args, "result": got}
                with journal.open("a") as fh:
                    fh.write(json.dumps(row) + "\n")
                return _result(got)

        @contextlib.asynccontextmanager
        async def cm():
            yield Session()

        return cm()


def _run(script, server: FakeServer, **kw):
    return asyncio.run(script_runner.run_scenario("p", repo_root=HERE.parents[1], script=script, facility="fake-1",
                                                  ssh_host="login", session_factory=server.factory, **kw))


# ---- refusals ---------------------------------------------------------------------------------------------------------

def test_a_scenario_without_a_script_is_refused_and_a_scripted_one_is_not():
    import importlib
    why = script_runner.script_refusal(importlib.import_module("happy_path"))
    assert why and "'happy_path' defines no SCRIPT" in why and "HPCB_OPERATOR=script" in why
    assert script_runner.script_refusal(importlib.import_module("queued_check_relaunch")) is None


def test_run_refuses_the_script_operator_for_an_unscripted_scenario_with_a_setup_message(harness_run, capsys):
    rc = asyncio.run(harness_run._run("happy_path", "m", None, None, False, operator="script"))
    out = capsys.readouterr().out
    assert rc == 2 and "RESULT: SETUP FAILED — scenario 'happy_path' defines no SCRIPT" in out


def test_a_script_only_scenario_skips_an_agent_operator(harness_run, capsys):
    rc = asyncio.run(harness_run._run("queued_check_relaunch", "m", None, None, False, operator="pi"))
    assert rc == 2 and "RESULT: SKIPPED — scenario 'queued_check_relaunch' runs only under the script operator" \
        in capsys.readouterr().out


def test_the_script_operator_takes_no_persona_and_no_hook():
    assert operator_refusal("script", persona="cooperative", phases=False, hooks=None) == "an interactive persona"
    assert operator_refusal("script", persona=None, phases=True, hooks=None) == "cross-restart chains (PHASES)"
    assert operator_refusal("script", persona=None, phases=False,
                            hooks=[{"after_tool": "run_shell", "cmd": "scancel"}]) == "mid-run hooks that need cmd"
    assert operator_refusal("script", persona=None, phases=False, hooks=None) is None


def test_a_script_is_never_infra_it_has_no_model_endpoint():
    # a server that never started, with a timeout on its last stderr line: graded (agent_engaged), never INFRA
    o = Outcome("script", hpcb_calls=0, tool_calls=0, final_text="", rc=1, is_error=True,
                stderr="the hpc-bridge MCP server failed: TimeoutError: HTTP 503 Service Unavailable")
    assert infra_reason(o, ["agent_engaged"]) is None


# ---- the run ----------------------------------------------------------------------------------------------------------

def test_steps_are_recorded_with_their_times_and_the_trace_is_built_from_the_journal(tmp_path, monkeypatch):
    _stub_sdk(monkeypatch, tmp_path)
    monkeypatch.setenv("HPC_BRIDGE_SSH_USER", "hpcbridge-test-00")
    monkeypatch.setenv("UNRELATED_SECRETISH", "x")
    server = FakeServer({
        "connect_facility": [{"phase": "needs_account", "facility": "fake-1"}],
        "ensure_endpoint_up": [{"status": "provisioning", "endpoint_id": "e1"}],
        "stop_endpoint": [{"status": "down", "spend_stopped": True, "endpoint_id": "e1"}],
    })

    async def script(ops):
        assert (ops.facility, ops.ssh_host) == ("fake-1", "login")
        await ops.call("connect_facility", facility=ops.facility, ssh_host=ops.ssh_host)
        await ops.call("ensure_endpoint_up", shape="compute", confirm_spend=True)
        ops.say("pilot queued")
        await ops.call("stop_endpoint")
        return "stopped"

    r = _run(script, server, extra_env={"HPC_BRIDGE_TEARDOWN_WAIT_S": "1"})
    # the server: this checkout via `uv run`, behind the stderr wrapper when the run has a log; the CLI operators' env
    cmd = server.started["cmd"]
    assert cmd == cli_runner._server_cmd(str(HERE.parents[1]), "script")
    assert cmd[-7:] == ["/usr/local/bin/uv", "run", "--directory", str(HERE.parents[1]), "--extra", "integration",
                        "hpc-bridge"]
    env = server.started["env"]
    assert env["HPC_BRIDGE_JOURNAL"].endswith("journal.jsonl") and env["HPC_BRIDGE_SSH_USER"] == "hpcbridge-test-00"
    assert env["HPC_BRIDGE_TEARDOWN_WAIT_S"] == "1" and "UNRELATED_SECRETISH" not in env   # filtered as Codex/Hermes do
    # the trace: the journal's record, in the steps' order, stamped with the server's own clock
    assert [c.name for c in r.trace.calls] == ["connect_facility", "ensure_endpoint_up", "stop_endpoint"]
    assert r.trace.calls[2].result["spend_stopped"] is True and r.trace.calls[1].t_call == 1.0
    assert r.trace.texts == ["pilot queued"]
    assert r.final.result == "stopped" and r.final.is_error is False
    head = r.messages[0]
    assert head["harness"] == "script" and head["rc"] == 0 and head["trace_stats"]["journal_unplaced"] == 0
    steps = [m["native"] for m in r.messages if "native" in m]
    assert [s["name"] for s in steps] == ["connect_facility", "ensure_endpoint_up", "stop_endpoint"]
    assert all(s["hpcb"] and s["t_start"] <= s["t_end"] for s in steps) and [s["step"] for s in steps] == [0, 1, 2]
    assert steps[2]["result"] == {"status": "down", "spend_stopped": True, "endpoint_id": "e1"}
    # the bundle replays to the same trace (regrade.py reads the cli_runner layout)
    replay = trace_from_bundle_messages(json.loads(json.dumps(r.messages)))
    assert [(c.name, c.result) for c in replay.calls] == [(c.name, c.result) for c in r.trace.calls]


def test_an_aborted_script_ends_errored_with_its_reason_and_keeps_its_steps(tmp_path, monkeypatch):
    _stub_sdk(monkeypatch, tmp_path)
    server = FakeServer({"connect_facility": [{"phase": "failed", "notice": "no ssh"}]})

    async def script(ops):
        r = await ops.call("connect_facility", facility=ops.facility)
        ops.abort(f"the login shape did not come up: {r['phase']}")

    r = _run(script, server)
    assert r.final.is_error and r.final.result == "script aborted: the login shape did not come up: failed"
    assert [c.name for c in r.trace.calls] == ["connect_facility"] and r.messages[0]["rc"] == 1


def test_a_tool_error_or_a_closed_channel_is_a_result_the_script_judges_not_a_crash(tmp_path, monkeypatch):
    _stub_sdk(monkeypatch, tmp_path)
    server = FakeServer({"connect_facility": [RuntimeError("ClosedResourceError")], "stop_endpoint": [{"status": "down"}]})
    seen = {}

    async def script(ops):
        seen["first"] = await ops.call("connect_facility", facility=ops.facility)
        await ops.call("stop_endpoint")
        return "done"

    r = _run(script, server)
    assert seen["first"] == {"text": "RuntimeError: ClosedResourceError", "is_error": True}
    # the call that never reached the server (no journal row) is kept, as an error, in its place
    assert [c.name for c in r.trace.calls] == ["connect_facility", "stop_endpoint"]
    assert r.trace.calls[0].result["is_error"] and r.messages[0]["trace_stats"]["hpcb_not_reaching_server"] == 1
    assert r.final.is_error is False


def test_a_script_that_raises_is_a_failed_run_with_its_traceback_not_a_crashed_cell(tmp_path, monkeypatch):
    _stub_sdk(monkeypatch, tmp_path)

    async def script(ops):
        raise KeyError("proposed_details")

    r = _run(script, FakeServer({}))
    assert r.final.is_error and r.final.result.startswith("script failed: KeyError")
    assert "Traceback" in r.messages[0]["stderr_tail"] and r.trace.calls == []


def test_a_server_that_never_starts_is_graded_not_crashed(tmp_path, monkeypatch):
    _stub_sdk(monkeypatch, tmp_path)

    def broken(cmd, env, cwd):
        @contextlib.asynccontextmanager
        async def cm():
            raise OSError("uv: not found")
            yield  # pragma: no cover

        return cm()

    async def script(ops):  # pragma: no cover - never reached
        return ""

    r = asyncio.run(script_runner.run_scenario("p", repo_root=HERE.parents[1], script=script, session_factory=broken))
    assert r.final.is_error and "the hpc-bridge MCP server failed: OSError: uv: not found" in r.final.result
    assert r.trace.calls == []


def test_tool_results_read_as_the_journal_reads_them():
    assert script_runner.tool_result(_result({"status": "down"})) == {"status": "down"}
    assert script_runner.tool_result(_result({"result": {"phase": "x"}})) == {"phase": "x"}   # FastMCP's wrapper
    assert script_runner.tool_result(_result({"result": [{"id": "delta"}]})) == {"value": [{"id": "delta"}]}
    assert script_runner.tool_result(_result(error="boom")) == {"text": "boom", "is_error": True}
    plain = types.SimpleNamespace(isError=False, structuredContent=None, content=[types.SimpleNamespace(text='{"a": 1}')])
    assert script_runner.tool_result(plain) == {"a": 1}


def test_a_persona_or_a_hook_is_refused_by_a_direct_caller_too(tmp_path, monkeypatch):
    _stub_sdk(monkeypatch, tmp_path)
    with pytest.raises(NotImplementedError):
        asyncio.run(script_runner.run_scenario("p", repo_root=HERE, script=lambda ops: None, persona="cooperative"))
    with pytest.raises(ValueError, match="SCRIPT"):
        asyncio.run(script_runner.run_scenario("p", repo_root=HERE))


# ---- run_smoke.sh: the script operator needs no credential and no ALCF probe; the admin tap runs beside the cell -----

def _smoke(tmp_path, scenario, **extra):
    """run_smoke.sh against stub `docker` / `uv` (no image, no cell): the docker calls and the tap's file."""
    root = tmp_path / "repo"
    (root / "agentic" / "runs").mkdir(parents=True)
    shutil.copy(HERE.parent / "run_smoke.sh", root / "agentic" / "run_smoke.sh")
    (root / "agentic" / "harness").symlink_to(HERE)
    (root / "agentic" / "scenarios").symlink_to(HERE.parent / "scenarios")
    stub = tmp_path / "bin"
    stub.mkdir()
    # `docker run` lasts a few seconds, so the tap (started before it) takes samples; `docker exec` is the admin channel
    (stub / "docker").write_text(
        '#!/usr/bin/env bash\necho "$*" >> "$STUB_DIR/docker.calls"\n'
        'case "$1 $2" in "image inspect") echo sha256:stub ;; esac\n'
        # `run` stands for the jail: it asks the tap for a job by id (the world check) and for a hold (phase B)
        'case "$1" in run) echo 1 > "$STUB_DIR/repo/agentic/runs/.admin-tap-r1.jsonl.ids"; '
        'echo "uep.54736c9f-06eb-4183-96e3-65a23b75a770 5" > "$STUB_DIR/repo/agentic/runs/.admin-tap-r1.jsonl.hold"; '
        'sleep 5 ;; '   # time for the tap (a second-by-second wait) to answer both requests, even on a loaded host
        'exec) echo "HPCB_NOW 1792105000" ;; esac\nexit 0\n')
    (stub / "uv").write_text('#!/usr/bin/env bash\necho "$*" >> "$STUB_DIR/uv.calls"\nexit 0\n')
    for f in ("docker", "uv"):
        (stub / f).chmod(0o755)
    key = tmp_path / "key"
    key.write_text("not a key\n")
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("HPCB_", "HPC_BRIDGE_", "ALCF_", "CLAUDE_CODE", "ANTHROPIC"))}
    env.update(PATH=f"{stub}:{env['PATH']}", STUB_DIR=str(tmp_path), HPCB_TEST_SSH_KEY=str(key), HPCB_SKIP_BUILD="1",
               HPCB_RUNID="r1", **extra)
    r = subprocess.run(["bash", str(root / "agentic" / "run_smoke.sh"), scenario], env=env, capture_output=True,
                       text=True, timeout=120)
    calls = (tmp_path / "docker.calls").read_text() if (tmp_path / "docker.calls").exists() else ""
    return r, calls, root


def test_run_smoke_runs_the_script_operator_without_any_credential_or_probe(tmp_path):
    r, calls, root = _smoke(tmp_path, "queued_check_relaunch", HPCB_OPERATOR="script", HPCB_TARGET="fake",
                            HPCB_FAKE_PROFILE="default", HPCB_RELAUNCH_WATCH_S="120")
    assert r.returncode == 0, r.stdout + r.stderr
    assert "no model, no auth needed" in r.stdout and not (tmp_path / "uv.calls").exists()   # no ALCF probe
    run = next(ln for ln in calls.splitlines() if ln.startswith("run "))
    assert "-e HPCB_OPERATOR=script" in run and run.rstrip().endswith("queued_check_relaunch --operator script")
    assert "CLAUDE_CODE_OAUTH_TOKEN" not in run and "ALCF_INFERENCE_TOKEN" not in run
    assert "-e HPCB_RELAUNCH_WATCH_S" in run
    # the admin tap: started through the admin channel before the cell, handed to the jail, stopped and removed after
    assert "-e HPCB_ADMIN_TAP=/work/hpc-bridge/agentic/runs/.admin-tap-r1.jsonl" in run
    execs = [ln for ln in calls.splitlines() if ln.startswith("exec ")]
    assert execs and "hpcb-fake-slurmctld-1 bash -lc" in execs[0] and "squeue -u hpcbridge-test" in execs[0]
    assert "admin tap: the scheduler as its admin" in r.stdout
    runs = root / "agentic" / "runs"
    assert not [p.name for p in runs.iterdir() if p.name.startswith(".admin-tap-r1")]   # the tap file, .ids, .hold
    assert any("sacct -j 1 -X" in ln for ln in execs)   # the id request reached the host tap within the cell
    assert any("scontrol hold" in ln and "-v m=uep.54736c9f-06eb-4183-96e3-65a23b75a770 -v f=5" in ln for ln in execs)


def test_run_smoke_refuses_the_cell_off_the_fake_cluster_before_any_tap_or_launch(tmp_path):
    # globus1 has no admin channel (we are not its admin): the scenario's TARGETS refuse it first, nothing is started
    r, calls, root = _smoke(tmp_path, "queued_check_relaunch", HPCB_OPERATOR="script", HPCB_TARGET="globus1")
    assert r.returncode == 1 and "runs only on target(s) fake" in r.stdout
    assert "run " not in calls and "exec " not in calls and not list((root / "agentic" / "runs").iterdir())


def test_the_call_timeout_takes_the_form_the_clients_annotation_names():
    # this module postpones its own annotations, so each client's signature is built explicitly: a REAL annotation
    # object (the host's older mcp), the same as TEXT (a module with `from __future__ import annotations`), a float
    # (the jail's newer mcp), none, and a parameter that is renamed — which must fall back to the float, not KeyError
    import datetime as dt
    import inspect

    def client(name: str | None, annotation=inspect.Parameter.empty):
        params = [inspect.Parameter("name", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        if name:
            params.append(inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, default=None, annotation=annotation))

        async def call_tool(*a, **k): ...

        call_tool.__signature__ = inspect.Signature(params)
        return types.SimpleNamespace(call_tool=call_tool)

    want = dt.timedelta(seconds=script_runner.CALL_TIMEOUT_S)
    real = client("read_timeout_seconds", dt.timedelta | None)
    assert "timedelta" in str(inspect.signature(real.call_tool).parameters["read_timeout_seconds"].annotation)
    assert script_runner._read_timeout(real) == want
    assert script_runner._read_timeout(client("read_timeout_seconds", "timedelta | None")) == want
    for c in (client("read_timeout_seconds", float | None), client("read_timeout_seconds", "float | None"),
              client("read_timeout_seconds"), client("timeout", float | None), client(None), object()):
        assert script_runner._read_timeout(c) == script_runner.CALL_TIMEOUT_S

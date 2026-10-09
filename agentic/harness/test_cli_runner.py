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

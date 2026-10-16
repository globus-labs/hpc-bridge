"""Shared fixtures for the harness' hermetic tests."""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent


@pytest.fixture(autouse=True)
def _isolate_hpc_bridge_state(monkeypatch, tmp_path):
    """Some of these tests drive the server's real code in-process (a teardown, a stop): its local state — the record
    of checks sent included — goes to a tmp dir, never the developer's real ~/.hpc-bridge (found 2026-10-16: the
    teardown scenarios left an empty ~/.hpc-bridge/sent-checks/). As in tests/conftest.py."""
    monkeypatch.setenv("HPC_BRIDGE_STATE_DIR", str(tmp_path / "hpc-bridge-state"))
    monkeypatch.setenv("HPC_BRIDGE_TEST_GUARD", "1")


@pytest.fixture(autouse=True, scope="session")
def _never_the_real_state_dir():
    real = Path.home() / ".hpc-bridge" / "sent-checks"
    before = set(real.iterdir()) if real.is_dir() else None
    yield
    after = set(real.iterdir()) if real.is_dir() else None
    assert after == before, f"harness tests wrote into the real {real}"


@pytest.fixture
def harness_run(monkeypatch):
    """run.py imports runner, which imports the agent SDK (jail-only): stub the SDK the way test_runner.py does."""
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "PermissionResultAllow", "PermissionResultDeny", "AssistantMessage", "UserMessage",
                 "ResultMessage", "SystemMessage", "ToolUseBlock", "ToolResultBlock", "TextBlock", "HookMatcher", "ClaudeSDKClient"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    monkeypatch.syspath_prepend(str(HERE))
    sys.modules.pop("runner", None)
    sys.modules.pop("run", None)
    import run as mod
    return mod

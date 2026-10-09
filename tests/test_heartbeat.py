"""Long tool calls report MCP progress while they run (Pi restarts its 60 s per-request timer on each one)."""
import asyncio

import pytest

from hpc_bridge import server


class _Ctx:
    def __init__(self, boom=False):
        self.reports, self.boom = [], boom

    async def report_progress(self, progress, total=None, message=None):
        if self.boom:
            raise RuntimeError("transport closed")
        self.reports.append((progress, message))


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(server, "_HEARTBEAT_S", 0.02)


async def test_a_slow_call_reports_progress_and_returns_its_result():
    async def slow():
        await asyncio.sleep(0.11)
        return "done"

    ctx = _Ctx()
    assert await server._heartbeat(ctx, slow(), "run_shell") == "done"
    assert len(ctx.reports) >= 3 and all("run_shell: still working" in m for _, m in ctx.reports)
    assert [p for p, _ in ctx.reports] == list(range(1, len(ctx.reports) + 1))  # monotonic, as the spec requires


async def test_a_fast_call_reports_nothing():
    async def fast():
        return 7

    ctx = _Ctx()
    assert await server._heartbeat(ctx, fast(), "x") == 7 and ctx.reports == []


async def test_errors_pass_through_and_a_failing_progress_send_is_ignored():
    async def fails():
        await asyncio.sleep(0.05)
        raise ValueError("bad partition")

    with pytest.raises(ValueError, match="bad partition"):
        await server._heartbeat(_Ctx(boom=True), fails(), "x")


async def test_cancelling_the_call_cancels_the_work():
    started, cancelled = asyncio.Event(), asyncio.Event()

    async def work():
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    call = asyncio.ensure_future(server._heartbeat(_Ctx(), work(), "x"))
    await started.wait()
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert cancelled.is_set()


async def test_run_shell_goes_through_the_heartbeat(monkeypatch):
    seen = []

    async def fake_heartbeat(ctx, work, label):
        seen.append(label)
        return await work

    async def fake_run_shell(app, command, session_id, shape):
        return server.ShellOutcome(phase="complete", block_state="warm", exit_code=0, stdout=command)

    class _Req:
        lifespan_context = object()

    class _C:
        request_context = _Req()

    monkeypatch.setattr(server, "_heartbeat", fake_heartbeat)
    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    out = await server.run_shell("hostname", _C())
    assert out.stdout == "hostname" and seen == ["run_shell"]


def test_the_skill_frontmatter_is_strict_yaml():
    # Pi parses SKILL.md frontmatter strictly and SILENTLY drops a skill whose YAML fails (an unquoted description
    # holding "bootstrap: stand up" did); Codex and Hermes only tolerated it. Agent Skills caps description at 1024.
    from pathlib import Path

    import yaml

    text = (Path(server.__file__).resolve().parents[2] / "skills" / "driving-hpc" / "SKILL.md").read_text()
    assert text.startswith("---\n")
    meta = yaml.safe_load(text.split("---")[1])
    assert meta["name"] == "driving-hpc" and 0 < len(meta["description"]) <= 1024


async def test_the_journal_records_each_call_and_redacts_codes(tmp_path, monkeypatch):
    import json

    journal = tmp_path / "journal.jsonl"
    monkeypatch.setenv("HPC_BRIDGE_JOURNAL", str(journal))

    async def no_facilities(query=""):
        return []

    monkeypatch.setattr(server, "_list_facilities", no_facilities)
    await server.mcp.call_tool("list_facilities", {"query": "delta"})
    with pytest.raises(Exception):  # noqa: B017 - no request context here; the point is what was recorded
        await server.mcp.call_tool("complete_preauth", {"code": "123456"})
    rows = [json.loads(line) for line in journal.read_text().splitlines()]
    assert [r["tool"] for r in rows] == ["list_facilities", "complete_preauth"]
    assert rows[0]["args"] == {"query": "delta"} and rows[0]["result"] == [] and rows[0]["ms"] >= 0
    assert rows[1]["args"] == {"code": "<redacted>"} and "error" in rows[1] and "123456" not in journal.read_text()
    assert oct(journal.stat().st_mode & 0o777) == "0o600"


async def test_no_journal_unless_asked(tmp_path, monkeypatch):
    monkeypatch.delenv("HPC_BRIDGE_JOURNAL", raising=False)

    async def no_facilities(query=""):
        return []

    monkeypatch.setattr(server, "_list_facilities", no_facilities)
    assert await server.mcp.call_tool("list_facilities", {}) is not None
    assert list(tmp_path.iterdir()) == []

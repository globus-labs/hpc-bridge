"""Hermetic: the hpc-bridge MCP server's own stderr in every run's bundle (`server-stderr.log`).

The wrapper (`serve_with_stderr.py`) is driven as a real process around stub servers: the MCP stdio channel passes
through byte for byte, the server keeps the wrapper's pid (exec), its exit code and the signals sent to it; each start
is marked in the log. Every operator's server command is checked to go through it. The harness side
(`server_stderr.py`): the raw log stays under a ceiling, the bundle copy is capped and redacted, and run.py reports
`harness:server_exited` (report-only) for a server that went away while its client ran on — and not otherwise.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import tomllib
import types
from pathlib import Path

import pytest
import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import serve_with_stderr  # noqa: E402
import server_stderr  # noqa: E402
from invariants import Result, Trace  # noqa: E402

REPO = "/work/hpc-bridge"
UV_CMD = ["run", "--directory", REPO, "--extra", "integration", "hpc-bridge"]

STUB_SERVER = r'''
import os, signal, sys, time
mode = sys.argv[1]
if mode == "cat":            # the stdio channel, verbatim
    sys.stdout.buffer.write(sys.stdin.buffer.read())
elif mode == "mcp":          # one answer per request line, a diagnostic per request on stderr
    sys.stderr.write("stub server: up\n"); sys.stderr.flush()
    for line in iter(sys.stdin.buffer.readline, b""):
        sys.stdout.buffer.write(b'{"jsonrpc":"2.0","result":' + line.strip() + b'}\n'); sys.stdout.buffer.flush()
        sys.stderr.write(f"stub server: answered {len(line)} bytes\n"); sys.stderr.flush()
elif mode == "exit":
    sys.stderr.write("stub server: exiting\n")
    sys.exit(int(sys.argv[2]))
elif mode == "crash":        # a crash: a traceback on stderr (with a planted bearer), then rc 1
    sys.stderr.write("Traceback (most recent call last):\n  File \"server.py\", line 1, in <module>\n"
                     "RuntimeError: teardown blew up; Authorization: Bearer " + os.environ.get("PLANT", "") + "\n")
    sys.exit(1)
elif mode == "term":         # proves a signal sent to the wrapper's pid reaches the server ITSELF
    def on_term(signum, frame):
        sys.stderr.write("stub server: got SIGTERM\n"); sys.stderr.flush()
        os._exit(3)
    signal.signal(signal.SIGTERM, on_term)
    sys.stdout.write("ready\n"); sys.stdout.flush()
    time.sleep(30)
elif mode == "sleep":
    sys.stdout.write("ready\n"); sys.stdout.flush()
    time.sleep(30)
'''

STUB_CLIENT = r'''
import os, subprocess, sys, time
mode, cmd = sys.argv[1], sys.argv[2:]
p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
if mode == "early":          # the server dies on its own; the client carries on without it for a while
    p.wait()
    time.sleep(float(os.environ.get("CLIENT_LINGER_S", "2.5")))
else:                        # a normal end: the client closes the server's stdin, waits for it, and exits at once
    time.sleep(0.8)
    p.stdin.close()
    p.wait()
'''


@pytest.fixture
def stub(tmp_path):
    p = tmp_path / "stub_server.py"
    p.write_text(STUB_SERVER)
    return p


def _wrapped(log, stub, *args, operator="codex"):
    return server_stderr.wrap([sys.executable, str(stub), *args], operator, log=str(log))


def _headers(log: Path) -> list[dict]:
    return [m.groupdict() for line in log.read_text().splitlines() if (m := serve_with_stderr.HEADER_RE.match(line))]


# ---------------------------------------------------------------- the wrapper, as a process


def test_wrapper_passes_stdio_through_byte_for_byte(tmp_path, stub):
    log = tmp_path / "server-stderr.raw"
    payload = bytes(range(256)) * 64 + b'\r\n{"jsonrpc":"2.0","id":1,"method":"tools/list"}\n' + b"\xff\xfe\x00 tail"
    r = subprocess.run(_wrapped(log, stub, "cat"), input=payload, capture_output=True, timeout=30)
    assert r.returncode == 0
    assert r.stdout == payload          # not one byte added, dropped or re-encoded: stdout is the protocol
    assert r.stderr == b""              # the wrapper writes nowhere but the log


def test_wrapper_keeps_an_mcp_like_exchange_turn_by_turn(tmp_path, stub):
    log = tmp_path / "server-stderr.raw"
    p = subprocess.Popen(_wrapped(log, stub, "mcp"), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    try:
        for i in (1, 2, 3):   # a request, its answer, the next request: nothing buffers in between
            p.stdin.write(b'{"id":%d}\n' % i)
            p.stdin.flush()
            assert p.stdout.readline() == b'{"jsonrpc":"2.0","result":{"id":%d}}\n' % i
        p.stdin.close()
        assert p.wait(timeout=30) == 0
        assert p.stderr.read() == b""
    finally:
        p.kill()
        p.wait()
    text = log.read_text()
    assert text.count("stub server: answered") == 3 and "stub server: up" in text


def test_the_server_keeps_the_wrappers_pid_and_each_start_is_marked(tmp_path, stub):
    log = tmp_path / "server-stderr.raw"
    pids = []
    for op in ("pi", "codex"):
        p = subprocess.Popen(_wrapped(log, stub, "exit", "0", operator=op), stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL)
        p.wait(timeout=30)
        pids.append(p.pid)
    heads = _headers(log)
    assert [h["operator"] for h in heads] == ["pi", "codex"]           # appended, never truncated: a restart shows
    assert [int(h["pid"]) for h in heads] == pids                       # exec: the pid the client knows IS the server
    assert all(int(h["client"]) == os.getpid() for h in heads)          # the process that spawned it
    lines = log.read_text().splitlines()
    assert lines[0].startswith("=== hpc-bridge MCP server start") and lines[1] == "stub server: exiting"


def test_wrapper_propagates_the_exit_code(tmp_path, stub):
    r = subprocess.run(_wrapped(tmp_path / "log", stub, "exit", "7"), stdin=subprocess.DEVNULL, capture_output=True,
                       timeout=30)
    assert r.returncode == 7 and r.stdout == b""


def _started(cmd):
    p = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE)
    assert p.stdout.readline() == b"ready\n"
    return p


def test_a_signal_to_the_wrapper_pid_reaches_the_server_itself(tmp_path, stub):
    log = tmp_path / "log"
    p = _started(_wrapped(log, stub, "term"))
    os.kill(p.pid, signal.SIGTERM)
    assert p.wait(timeout=30) == 3                     # the server's own handler ran: no wrapper in between
    assert "stub server: got SIGTERM" in log.read_text()
    q = _started(_wrapped(log, stub, "sleep"))
    os.kill(q.pid, signal.SIGTERM)
    assert q.wait(timeout=30) == -signal.SIGTERM       # killed by the signal, as the client sees a bare server die


def test_the_server_starts_with_sigpipe_at_its_default(tmp_path):
    probe = ["/bin/sh", "-c", "kill -PIPE $$; echo alive"]   # alive only if SIGPIPE is ignored on entry
    control = subprocess.run(probe, capture_output=True, restore_signals=False, timeout=30)
    assert control.stdout == b"alive\n"                # the probe tells an inherited SIG_IGN apart
    r = subprocess.run(server_stderr.wrap(probe, "pi", log=str(tmp_path / "log")), capture_output=True, timeout=30)
    assert r.returncode == -signal.SIGPIPE and r.stdout == b""   # Python's own SIG_IGN did not leak into the server


def test_an_unwritable_log_never_stops_the_server(tmp_path, stub):
    log = tmp_path / "no-such-dir" / "log"
    r = subprocess.run(_wrapped(log, stub, "cat"), input=b"hello\n", capture_output=True, timeout=30)
    assert r.returncode == 0 and r.stdout == b"hello\n"
    assert b"cannot open" in r.stderr and not log.exists()   # said where the client shows it; stderr left alone


def test_a_command_that_cannot_run_fails_like_a_shell_and_says_so_in_the_log(tmp_path):
    log = tmp_path / "log"
    r = subprocess.run(server_stderr.wrap(["/no/such/uv", "run"], "codex", log=str(log)), capture_output=True,
                       timeout=30)
    assert r.returncode == 127 and r.stdout == b""
    assert "cannot run '/no/such/uv'" in log.read_text()


def test_wrapper_refuses_a_malformed_command_line_without_touching_stdout(tmp_path):
    r = subprocess.run([sys.executable, "-I", str(server_stderr.WRAPPER), "--log", str(tmp_path / "log")],
                       capture_output=True, timeout=30)
    assert r.returncode == 2 and r.stdout == b"" and b"usage" in r.stderr


# ---------------------------------------------------------------- every operator's server command goes through it


def _expect_wrapped(cmd: list[str], operator: str, log: str, inner: list[str]) -> None:
    assert cmd == [sys.executable, "-I", str(server_stderr.WRAPPER), "--log", log, "--operator", operator, "--", *inner]


def test_wrap_is_a_no_op_until_run_py_names_a_log(monkeypatch):
    monkeypatch.delenv(server_stderr.LOG_ENV, raising=False)
    assert server_stderr.wrap(["uv", *UV_CMD], "claude") == ["uv", *UV_CMD]
    monkeypatch.setenv(server_stderr.LOG_ENV, "/tmp/x/server-stderr.raw")  # noqa: S108 - a path string only
    _expect_wrapped(server_stderr.wrap(["uv", *UV_CMD], "claude"), "claude", "/tmp/x/server-stderr.raw",  # noqa: S108
                    ["uv", *UV_CMD])


@pytest.fixture
def log_env(monkeypatch, tmp_path):
    log = str(tmp_path / "server-stderr.raw")
    monkeypatch.setenv(server_stderr.LOG_ENV, log)
    return log


def _sdk_stub(monkeypatch):
    stub = types.ModuleType("claude_agent_sdk")
    for name in ("ClaudeAgentOptions", "PermissionResultAllow", "PermissionResultDeny", "AssistantMessage", "UserMessage",
                 "ResultMessage", "SystemMessage", "ToolUseBlock", "ToolResultBlock", "TextBlock", "HookMatcher", "ClaudeSDKClient"):
        setattr(stub, name, type(name, (), {}))
    stub.query = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", stub)
    monkeypatch.syspath_prepend(str(HERE))
    for m in ("runner", "hermes_runner", "acp_runner", "run"):
        sys.modules.pop(m, None)


def test_claude_sdk_operator_launches_the_server_through_the_wrapper(monkeypatch, log_env):
    _sdk_stub(monkeypatch)
    for k in ("HPC_BRIDGE_USER_DIR", "HPC_BRIDGE_SSH_USER", "HPC_BRIDGE_SSH_KEY"):
        monkeypatch.setenv(k, "x")
    import runner

    srv = runner._mcp_servers(Path(REPO))["endpoint"]
    _expect_wrapped([srv["command"], *srv["args"]], "claude", log_env, ["uv", *UV_CMD])
    assert srv["env"]["HPC_BRIDGE_SSH_USER"] == "x"   # the server's env is untouched by the wrapping


class _Stdio:
    def __init__(self, **kw):
        self.__dict__.update(kw)


def _acp_client(monkeypatch):
    acp = types.ModuleType("acp")
    acp.PROTOCOL_VERSION = 1
    acp.Client = type("Client", (), {})
    acp.RequestError = type("RequestError", (Exception,), {})
    schema = types.ModuleType("acp.schema")
    for n in ("ClientCapabilities", "FileSystemCapabilities", "Implementation", "RequestPermissionResponse",
              "SelectedPermissionOutcome"):
        setattr(schema, n, type(n, (), {}))
    schema.McpServerStdio = _Stdio
    schema.EnvVariable = _Stdio
    acp.schema = schema
    monkeypatch.setitem(sys.modules, "acp", acp)
    monkeypatch.setitem(sys.modules, "acp.schema", schema)
    sys.modules.pop("acp_client", None)
    import acp_client
    return acp_client


def test_acp_registration_launches_the_server_through_the_wrapper(monkeypatch, log_env):
    acp_client = _acp_client(monkeypatch)
    srv = acp_client.hpc_bridge_mcp(REPO, {"HOME": "/home/agent", "UNRELATED": "x"}, operator="claude-acp")
    _expect_wrapped([srv.command, *srv.args], "claude-acp", log_env, ["/usr/local/bin/uv", *UV_CMD])
    assert [e.name for e in srv.env] == ["HOME"]


def test_the_manual_acp_probe_still_registers_the_server(monkeypatch, log_env, capsys):
    acp_client = _acp_client(monkeypatch)
    seen = []

    async def run_once(command, args, task, *, cwd, env, mcp_servers):
        seen.extend(mcp_servers)
        return types.SimpleNamespace(stop_reason="end_turn"), acp_client.AcpCapture()

    monkeypatch.setattr(acp_client, "run_once", run_once)
    assert asyncio.run(acp_client._probe()) == 0
    (srv,) = seen
    _expect_wrapped([srv.command, *srv.args], "hermes", log_env, ["/usr/local/bin/uv", *UV_CMD])


def _capture_acp_registration(monkeypatch):
    """A stand-in `acp_client` for the ACP operators: records how each registers hpc-bridge, then fails the session
    (the operator grades that as an aborted run)."""
    seen: list[str] = []
    fake = types.ModuleType("acp_client")

    def hpc_bridge_mcp(repo, env, *, operator):
        seen.append(operator)
        return object()

    async def run_session(*a, **k):
        raise RuntimeError("no agent here")

    fake.hpc_bridge_mcp = hpc_bridge_mcp
    fake.run_session = run_session
    monkeypatch.setitem(sys.modules, "acp_client", fake)
    return seen


def test_claude_acp_and_hermes_acp_name_themselves_in_the_log(monkeypatch, tmp_path, log_env):
    _sdk_stub(monkeypatch)
    seen = _capture_acp_registration(monkeypatch)
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude"))
    import acp_runner

    asyncio.run(acp_runner.run_scenario("p", repo_root=Path(REPO)))
    monkeypatch.setenv("ALCF_INFERENCE_TOKEN", "t" * 20)
    monkeypatch.setenv("HPCB_ALCF_BASE_URL", "https://inference.example/v1")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    monkeypatch.setenv("HPCB_HERMES_ACP", "1")
    import hermes_runner

    asyncio.run(hermes_runner.run_scenario("p", repo_root=Path(REPO)))
    assert seen == ["claude-acp", "hermes"]


def test_hermes_config_launches_the_server_through_the_wrapper(monkeypatch, log_env):
    import hermes_setup

    monkeypatch.setenv("HPCB_ALCF_BASE_URL", "https://inference.example/v1")
    srv = hermes_setup.build_config()["mcp_servers"]["hpc-bridge"]
    _expect_wrapped([srv["command"], *srv["args"]], "hermes", log_env, ["/usr/local/bin/uv", *UV_CMD])
    assert srv["command"].startswith("/")   # absolute: hermes' env filter must not hide the interpreter on PATH


def test_codex_pi_hermes21_configs_launch_the_server_through_the_wrapper(monkeypatch, tmp_path, log_env):
    import cli_runner

    monkeypatch.setenv("HPCB_ALCF_BASE_URL", "https://inference.example/v1")
    env = {"HOME": "/home/agent", "HPC_BRIDGE_JOURNAL": "/work/run/j.jsonl"}
    inner = ["/usr/local/bin/uv", *UV_CMD]
    codex = tomllib.loads((Path(cli_runner.setup_codex(tmp_path / "c", REPO, env)["CODEX_HOME"]) / "config.toml")
                          .read_text())["mcp_servers"]["hpc-bridge"]
    _expect_wrapped([codex["command"], *codex["args"]], "codex", log_env, inner)
    assert codex["required"] is True and "HPC_BRIDGE_JOURNAL" in codex["env_vars"]
    pi = json.loads((Path(cli_runner.setup_pi(tmp_path / "p", REPO, env)["PI_CODING_AGENT_DIR"]) / "mcp.json")
                    .read_text())["mcpServers"]["hpc-bridge"]
    _expect_wrapped([pi["command"], *pi["args"]], "pi", log_env, inner)
    h21 = yaml.safe_load((Path(cli_runner.setup_hermes21(tmp_path / "h", REPO, env)["HERMES_HOME"]) / "config.yaml")
                         .read_text())["mcp_servers"]["hpc-bridge"]
    _expect_wrapped([h21["command"], *h21["args"]], "hermes21", log_env, inner)


# ---------------------------------------------------------------- size: the raw log and the bundle copy


def test_trim_log_keeps_the_newest_whole_lines(tmp_path):
    log = tmp_path / "log"
    log.write_bytes(b"".join(b"line %05d\n" % i for i in range(1000)))   # 11 bytes a line
    size = server_stderr.trim_log(log, 115)
    data = log.read_bytes()
    assert size == len(data) and data.endswith(b"line 00999\n")
    first, *kept = data.decode().splitlines()
    assert first.startswith("=== harness: trimmed") and kept == [f"line {i:05d}" for i in range(990, 1000)]
    assert server_stderr.trim_log(log, 10_000) == size   # under the cap: left alone


def test_the_watch_keeps_the_raw_log_under_its_ceiling(tmp_path):
    log = tmp_path / "log"
    log.write_bytes(serve_with_stderr.header(4242, 1, "pi").encode() + b"x" * 5000 + b"\nlast line\n")
    w = server_stderr.ServerWatch(log, ceiling=4000, keep=1000, table=lambda: {4242: (1, "S"), 1: (0, "S")})
    w.poll()
    assert log.stat().st_size <= 1100 and log.read_bytes().endswith(b"last line\n")
    assert [p.pid for p in w.processes] == [4242]          # its start was read before the trim


SECRETS = {"alcf token": ["alcf-bearer-0123456789abcdef"], "globus token": ["AgGlobusAccessTok3nValue0123456789"],
           "ssh private key": ["b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW"]}


def test_redact_removes_known_values_and_token_shapes():
    text = ("using alcf-bearer-0123456789abcdef for the model\n"
            "refresh failed: {'access_token': 'AgOtherTok3n987654321', 'refresh_token': \"Rf-tok-123456789\"}\n"
            "Authorization: Bearer eyJhbGciOiJSUzI1NiJ9.payload.sig\n"
            "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n"
            "-----END OPENSSH PRIVATE KEY-----\n"
            "key line alone: b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n"
            "token store AgGlobusAccessTok3nValue0123456789 api_key=sk-ant-oat01-abcdefghijkl password: hunter22\n"
            "gce stop: endpoint hpc-bridge-fake-1 not running\n")
    out = server_stderr.redact(text, SECRETS)
    for secret in ("alcf-bearer-0123456789abcdef", "AgOtherTok3n987654321", "Rf-tok-123456789", "eyJhbGciOiJSUzI1NiJ9",
                   "b3BlbnNzaC1rZXkt", "AgGlobusAccessTok3nValue", "sk-ant-oat01", "hunter22"):
        assert secret not in out, secret
    assert "<redacted alcf token>" in out and "Bearer <redacted>" in out and "<redacted private key>" in out
    assert "gce stop: endpoint hpc-bridge-fake-1 not running" in out   # the diagnostics themselves survive


def test_bundle_log_is_redacted_and_capped(tmp_path):
    raw = tmp_path / "raw"
    bulk = b"".join(b"chatty line %07d\n" % i for i in range(200_000))   # ~3.6 MB
    raw.write_bytes(serve_with_stderr.header(7, 1, "codex").encode() + bulk
                    + b"RuntimeError: Bearer alcf-bearer-0123456789abcdef\n")
    dest = tmp_path / "bundle"
    dest.mkdir()
    out = server_stderr.bundle_log(raw, dest, SECRETS)
    assert out == dest / "server-stderr.log"
    data = out.read_bytes()
    assert len(data) <= server_stderr.KEEP_BYTES + 200
    assert data.startswith(b"=== harness: trimmed") and data.endswith(b"RuntimeError: Bearer <redacted alcf token>\n")
    assert b"alcf-bearer" not in data
    assert server_stderr.bundle_log(tmp_path / "missing", dest, SECRETS) is None   # no server ever started: no file


# ---------------------------------------------------------------- which exits are reported


class _World:
    """A fake process table and clock: what the watch sees, poll by poll."""

    def __init__(self):
        self.t = 1000.0
        self.alive: dict[int, int] = {}   # pid -> ppid
        self.zombies: set[int] = set()    # exited, not yet reaped by their parent: still in the table

    def table(self):
        return {pid: (ppid, "Z" if pid in self.zombies else "S") for pid, ppid in self.alive.items()}

    def clock(self):
        return self.t


def _watch(tmp_path, world, *servers):
    log = tmp_path / "log"
    log.write_text("".join(serve_with_stderr.header(pid, client, "pi") for pid, client in servers))
    for pid, client in servers:
        world.alive[pid] = client
        world.alive[client] = 1
    return server_stderr.ServerWatch(log, grace_s=3.0, table=world.table, clock=world.clock)


@pytest.mark.parametrize(("client_lives_on_s", "early"), [(10.0, True), (0.0, False), (2.0, False)])
def test_a_server_that_goes_while_its_client_runs_on_is_an_early_exit(tmp_path, client_lives_on_s, early):
    world = _World()
    w = _watch(tmp_path, world, (500, 400))
    w.poll()
    world.t += 5
    del world.alive[500]                       # the server goes
    if client_lives_on_s == 0:
        del world.alive[400]                   # ... and its client with it: the client ended the session
    w.poll()
    world.t += client_lives_on_s
    world.alive.pop(400, None)
    w.poll()
    w.finish()
    assert [p.pid for p in w.early()] == ([500] if early else [])
    assert "gone (seen by the harness" in (tmp_path / "log").read_text()   # the log marks where it went
    (rec,) = w.summary()["processes"]
    if client_lives_on_s == 0:   # the client went with it: it did not outlive its server at all
        assert rec["client_outlived_s"] is None and rec["client_ended_at"] == rec["ended_at"] == 1005.0
    else:
        assert rec["client_outlived_s"] == client_lives_on_s


def test_a_zombie_server_has_exited(tmp_path):
    """A server that exited but that its client has not reaped yet is still in the process table, as a zombie."""
    world = _World()
    w = _watch(tmp_path, world, (500, 400))
    w.poll()
    world.zombies.add(500)
    w.poll()
    world.t += 10
    w.finish()
    assert [p.pid for p in w.early()] == [500]


def test_a_client_still_running_when_the_run_ends_counts_from_the_run_end(tmp_path):
    world = _World()
    w = _watch(tmp_path, world, (500, 400), (501, 401))
    w.poll()
    del world.alive[500]                       # one server dies mid-run; its client is still up at the end
    w.poll()
    world.t += 4
    w.finish()                                 # 501 is still running: not an exit at all
    assert [p.pid for p in w.early()] == [500]
    rec = w.summary()["processes"]
    assert rec[0]["early"] and rec[0]["client_outlived_s"] == 4.0 and rec[1]["ended_at"] is None


def test_a_session_per_turn_is_not_an_early_exit(tmp_path):
    """hermes' interactive loop and a PHASES chain start one client + server per turn: each pair ends together (the
    client a moment after it closed its server), however long the run goes on after them."""
    world = _World()
    w = _watch(tmp_path, world, (500, 400))
    w.poll()
    world.t += 30
    del world.alive[500]
    w.poll()
    world.t += 1
    del world.alive[400]
    log = tmp_path / "log"
    with log.open("a") as fh:
        fh.write(serve_with_stderr.header(502, 402, "hermes"))
    world.alive.update({502: 402, 402: 1})
    w.poll()
    world.t += 30
    w.finish()
    assert w.early() == [] and [p.pid for p in w.processes] == [500, 502]


def test_a_cancel_while_the_watch_closes_is_not_swallowed(tmp_path):
    """A signal (docker stop) that lands just as the operator returned must still interrupt the run."""
    log = tmp_path / "log"
    log.write_text(serve_with_stderr.header(500, 400, "pi"))

    def slow_table():
        time.sleep(0.3)   # a slow look: closing the watch waits on it
        return {500: (400, "S"), 400: (1, "S")}

    w = server_stderr.ServerWatch(log, interval=0.01, table=slow_table)
    reached = []

    async def body():
        async with w.watching():
            await asyncio.sleep(0.05)
        reached.append("the run went on")

    async def main():
        t = asyncio.create_task(body())
        await asyncio.sleep(0.1)   # the operator has returned; the watch is closing
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t

    asyncio.run(main())
    assert reached == [] and w.run_ended is not None   # interrupted, and the watch still closed


# ---------------------------------------------------------------- run.py: the bundle and the row, end to end


SCENARIO = '''
PROMPT = "connect and stop"
TRUST_HOST_KEY = False
TEARDOWN = "keep"
'''   # no EXPECT_OK: every row present gates by default — except harness:server_exited, which never does


def _drive_run(monkeypatch, tmp_path, *, server_mode, client_mode):
    """run.py's `_run` with the world stubbed (no ssh, no teardown) and an operator that is a stub MCP client running
    the stub server through the same `wrap` every operator uses. Returns (rc, the bundle dir)."""
    _sdk_stub(monkeypatch)
    import run
    from runner import RunResult

    scen_dir = tmp_path / "scenarios"
    scen_dir.mkdir()
    (scen_dir / "zz_server_stderr_probe.py").write_text(SCENARIO)
    sys.modules.pop("zz_server_stderr_probe", None)
    server = tmp_path / "stub_server.py"
    server.write_text(STUB_SERVER)
    client = tmp_path / "stub_client.py"
    client.write_text(STUB_CLIENT)

    async def operator(prompt, *, repo_root, **kw):
        cmd = server_stderr.wrap([sys.executable, str(server), server_mode], "claude")
        proc = await asyncio.create_subprocess_exec(sys.executable, str(client), client_mode, *cmd)
        await proc.wait()
        return RunResult(trace=Trace([], []), final=types.SimpleNamespace(is_error=False, result="done"), messages=[])

    real_sleep = asyncio.sleep

    async def quick_sleep(delay, *a, **k):   # the postcheck settle (10 s) is not what this test waits on
        return await real_sleep(min(delay, 0.01) if delay >= 10 else delay, *a, **k)

    monkeypatch.setattr(asyncio, "sleep", quick_sleep)
    monkeypatch.setattr(run, "SCENARIOS_DIR", scen_dir)
    monkeypatch.setattr(run, "run_scenario", operator)
    monkeypatch.setattr(run, "_ssh_run", lambda *a, **k: (0, ""))
    monkeypatch.setattr(run, "_stop_operator", lambda *a, **k: [])
    monkeypatch.setattr(run, "_teardown", lambda *a, **k: ("", None))
    monkeypatch.setattr(run, "_no_manager_after_teardown",
                        lambda *a, **k: (Result("harness:no_manager_after_teardown", True, "ok"), False))
    monkeypatch.setattr(run, "_cleanup", lambda *a, **k: None)
    monkeypatch.setattr(server_stderr, "INTERVAL_S", 0.1)
    monkeypatch.setattr(server_stderr, "GRACE_S", 1.0)
    monkeypatch.setenv("HPCB_RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setenv("HPCB_RUNID", "t1")
    monkeypatch.setenv("ALCF_INFERENCE_TOKEN", "alcf-bearer-0123456789abcdef")
    monkeypatch.setenv("PLANT", "alcf-bearer-0123456789abcdef")
    monkeypatch.setenv("CLIENT_LINGER_S", "2.5")
    for k in ("HPC_BRIDGE_SSH_KEY", "GLOBUS_COMPUTE_USER_DIR", "HPC_BRIDGE_USER_DIR", server_stderr.LOG_ENV):
        monkeypatch.delenv(k, raising=False)
    rc = asyncio.run(run._run("zz_server_stderr_probe", "m", None, None, False))
    assert server_stderr.LOG_ENV not in os.environ     # the run's log is its own: not left for the next
    return rc, tmp_path / "runs" / "t1-zz_server_stderr_probe"


def test_a_server_that_dies_mid_run_is_in_the_bundle_and_reported_never_gated(monkeypatch, tmp_path):
    rc, bundle = _drive_run(monkeypatch, tmp_path, server_mode="crash", client_mode="early")
    log = (bundle / "server-stderr.log").read_text()
    assert "=== hpc-bridge MCP server start" in log and "operator=claude" in log
    assert "RuntimeError: teardown blew up" in log and "alcf-bearer" not in log and "Bearer <redacted alcf token>" in log
    rec = json.loads((bundle / "record.json").read_text())
    (proc,) = rec["server"]["processes"]
    assert proc["early"] is True and proc["client_outlived_s"] > 1.0
    (row,) = [g for g in rec["grading"] if g["name"] == "harness:server_exited"]
    assert row["ok"] is False and row["gating"] is False                 # report-only, whatever the scenario gates
    assert "RuntimeError: teardown blew up" in row["detail"] and "alcf-bearer" not in row["detail"]
    assert "harness:server_exited" not in rec["failed"] and "harness:server_exited" not in rec["gating"]
    assert "agent_engaged" in rec["failed"] and rc == 1                  # the verdict is the agent's, as before
    assert rec["server"]["run_ended_at"] is not None                     # the watch closed when the operator returned


def test_a_server_that_runs_to_the_end_of_its_session_is_not_reported(monkeypatch, tmp_path):
    _rc, bundle = _drive_run(monkeypatch, tmp_path, server_mode="mcp", client_mode="normal")
    assert "stub server: up" in (bundle / "server-stderr.log").read_text()
    rec = json.loads((bundle / "record.json").read_text())
    (proc,) = rec["server"]["processes"]
    assert proc["early"] is False and proc["ended_at"] is not None
    assert not [g for g in rec["grading"] if g["name"] == "harness:server_exited"]

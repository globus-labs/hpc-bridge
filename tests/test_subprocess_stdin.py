"""No child process of the MCP server may read the server's stdin: that is the MCP channel.

Found live 2026-10-15 (teardown_slow_fs under pi and codex): `ssh_exec` ran ssh with the server's stdin when it had
no payload, while the agent re-called teardown_endpoint during a background teardown. Two symptoms: under codex ssh
read the request itself (it never reached the server; the call timed out after 600 s); under pi OpenSSH set the shared
pipe non-blocking, the server's next read came back empty, and its stdio loop ended as if the client had gone.
"""
from __future__ import annotations

import ast
import os
import subprocess
import sys
import textwrap
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "hpc_bridge"

# A stand-in for the server process: its stdin is a pipe (the MCP channel). It runs ssh_exec with a stand-in "ssh"
# that reports how much stdin it got, while the test writes a "client request" into that pipe; afterwards it reads
# its own stdin back.
_CHILD = textwrap.dedent(
    """
    import asyncio, sys
    from hpc_bridge.facility.remote import ssh_exec

    class Target:
        def argv(self, remote_cmd):
            # stand-in ssh: say we started, then read stdin to EOF (as ssh forwards it) and report what it got
            return [sys.executable, "-c",
                    "import sys; print('started', flush=True); d = sys.stdin.read(); print(len(d), repr(d[:40]))"]

    async def main():
        rc, out, err = await ssh_exec(Target(), "true", timeout=20)
        print("SSH:" + out.strip().replace(chr(10), " | "), flush=True)
        print("SERVER:" + repr(sys.stdin.readline()), flush=True)

    asyncio.run(main())
    """
)


def test_ssh_exec_never_hands_the_servers_stdin_to_ssh():
    env = {**os.environ, "PYTHONPATH": str(SRC.parent)}
    proc = subprocess.Popen([sys.executable, "-c", _CHILD], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, env=env)
    assert proc.stdin is not None
    proc.stdin.write('{"jsonrpc":"2.0","id":7,"method":"tools/call"}\n')  # the client's next request
    proc.stdin.flush()
    proc.stdin.close()
    out, err = proc.communicate(timeout=60)
    assert proc.returncode == 0, err
    ssh_line = next(ln for ln in out.splitlines() if ln.startswith("SSH:"))
    server_line = next(ln for ln in out.splitlines() if ln.startswith("SERVER:"))
    assert "0 ''" in ssh_line, f"ssh read the server's stdin: {ssh_line}"
    assert '"id":7' in server_line, f"the client's request did not reach the server: {server_line}"


# Every way the package could start a child, matched by the call's final name so `from subprocess import run` or an
# alias is caught too. The os.* and pty forms have no stdin parameter at all: they are refused outright.
_SPAWN_NAMES = {"run", "Popen", "call", "check_call", "check_output", "create_subprocess_exec",
                "create_subprocess_shell", "subprocess_exec", "subprocess_shell"}
_NO_STDIN_FORMS = {"system", "popen", "spawnl", "spawnlp", "spawnv", "spawnvp", "spawnve", "spawnvpe", "posix_spawn",
                   "posix_spawnp", "getoutput", "getstatusoutput", "spawn"}


def _final_name(func: ast.expr) -> str | None:
    return func.attr if isinstance(func, ast.Attribute) else func.id if isinstance(func, ast.Name) else None


def _spawn_calls(tree: ast.AST):
    """Calls that start a child process with a stdin parameter (a bare `run(...)` counts only when `run` was imported
    from subprocess, so an unrelated `run` method is not flagged)."""
    from_subprocess = {a.asname or a.name: a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)
                       and n.module in ("subprocess", "asyncio.subprocess") for a in n.names}  # local name -> real
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _final_name(node.func)
        by_attr = isinstance(node.func, ast.Attribute) and name in _SPAWN_NAMES and (
            name not in ("run", "call")  # a bare .run/.call is a spawn only on the subprocess module
            or (isinstance(node.func.value, ast.Name) and node.func.value.id == "subprocess"))
        by_import = isinstance(node.func, ast.Name) and from_subprocess.get(name or "") in _SPAWN_NAMES
        if by_attr or by_import:
            yield node


def _no_stdin_spawns(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and isinstance(node.func.value, ast.Name) and node.func.value.id in ("os", "pty", "subprocess") \
                and node.func.attr in _NO_STDIN_FORMS:
            yield node


def _inherits(call: ast.Call) -> bool:
    kws = {k.arg: k.value for k in call.keywords if k.arg}
    if "input" in kws:
        return False
    value = kws.get("stdin")
    return value is None or _may_be_none(value)


def _may_be_none(v: ast.expr) -> bool:
    """Can this stdin expression evaluate to None? `PIPE if payload else None` was the original bug; the None in a
    condition (`PIPE if x is not None else DEVNULL`) is not a value it can take."""
    if isinstance(v, ast.Constant):
        return v.value is None
    if isinstance(v, ast.IfExp):
        return _may_be_none(v.body) or _may_be_none(v.orelse)
    if isinstance(v, ast.BoolOp):
        return any(_may_be_none(x) for x in v.values)
    return False


def test_every_subprocess_the_package_starts_sets_its_stdin():
    """Every subprocess call in src/hpc_bridge names its stdin (or passes input=, which is a pipe), never None in any
    branch, which inherits the MCP channel; and nothing starts a child through os.system/popen/spawn*, which can't."""
    bad = []
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(), str(path))
        bad += [f"{path.relative_to(SRC.parent)}:{c.lineno}" for c in _spawn_calls(tree) if _inherits(c)]
        bad += [f"{path.relative_to(SRC.parent)}:{c.lineno} (no stdin parameter)" for c in _no_stdin_spawns(tree)]
    assert not bad, f"subprocess calls that would inherit the MCP server's stdin: {bad}"


def test_the_scan_catches_every_form_of_the_bug():
    tree = ast.parse(textwrap.dedent("""
        import subprocess, asyncio, os
        from subprocess import Popen as P
        subprocess.run(['x'])
        asyncio.create_subprocess_exec('x', stdin=None)
        asyncio.create_subprocess_exec('x', stdin=asyncio.subprocess.PIPE if payload else None)
        P(['x'])
        loop.subprocess_exec(proto, 'x')
        os.system('x')
        subprocess.run(['x'], stdin=subprocess.DEVNULL)
        subprocess.run(['x'], input='y')
        self.run(task)
    """))
    flagged = [c.lineno for c in _spawn_calls(tree) if _inherits(c)] + [c.lineno for c in _no_stdin_spawns(tree)]
    assert sorted(flagged) == [4, 5, 6, 7, 8, 9], flagged

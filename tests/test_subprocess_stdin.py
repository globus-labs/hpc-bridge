"""No child process of the MCP server may read the server's stdin: that is the MCP channel.

Found live 2026-10-15 (teardown_slow_fs under pi and codex): `ssh_exec` ran ssh with the server's stdin when it had
no payload, ssh forwards its stdin to the remote command, and a teardown_endpoint re-call that arrived while a
background teardown's ssh ran was read by ssh. The server's stdio loop then ended and the client saw the connection
close.
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


_SPAWNERS = {("subprocess", "run"), ("subprocess", "Popen"), ("subprocess", "call"), ("subprocess", "check_call"),
             ("subprocess", "check_output"), ("asyncio", "create_subprocess_exec"),
             ("asyncio", "create_subprocess_shell")}


def _spawn_calls(tree: ast.AST):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                and isinstance(node.func.value, ast.Name) and (node.func.value.id, node.func.attr) in _SPAWNERS:
            yield node


def test_every_subprocess_the_package_starts_sets_its_stdin():
    """Every subprocess call in src/hpc_bridge names its stdin (or passes input=, which is a pipe) — and never None,
    which inherits the MCP channel."""
    missing = []
    for path in sorted(SRC.rglob("*.py")):
        for call in _spawn_calls(ast.parse(path.read_text(), str(path))):
            kws = {k.arg: k.value for k in call.keywords if k.arg}
            if "input" in kws:
                continue
            value = kws.get("stdin")
            if value is None or (isinstance(value, ast.Constant) and value.value is None):
                missing.append(f"{path.relative_to(SRC.parent)}:{call.lineno}")
    assert not missing, f"subprocess calls that would inherit the MCP server's stdin: {missing}"


def test_the_scan_sees_a_call_that_inherits_stdin():
    tree = ast.parse("import subprocess, asyncio\nsubprocess.run(['x'])\nasyncio.create_subprocess_exec('x', stdin=None)")
    assert len(list(_spawn_calls(tree))) == 2

"""The operator's process tree: find it, stop it, and make sure it is gone BEFORE the harness tears down.

An operator (the Claude Code CLI under the Agent SDK, Codex / Pi / Hermes, an ACP agent) runs as a child of run.py and
spawns hpc-bridge's MCP server under it. If that tree outlives a cancelled run, a tool call it makes after the teardown
re-provisions the endpoint the teardown just removed (Pi on the fake cluster: a new manager under the same name, a
minute after an interrupted cell's teardown said it was clean). So the exit path stops the whole tree first, and waits.

`become_subreaper` (Linux) makes run.py the parent of any ORPHANED descendant — an MCP server whose operator died — so
those are found too; without it they re-parent to the container's PID 1 and drop out of the tree. Stdlib only.
"""
from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import time


def become_subreaper() -> bool:
    """Linux: make this process the child subreaper for its descendants (prctl PR_SET_CHILD_SUBREAPER). No-op
    elsewhere. Returns whether it took effect."""
    if not sys.platform.startswith("linux"):
        return False
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        return libc.prctl(36, 1, 0, 0, 0) == 0
    except Exception:  # noqa: BLE001 - best-effort: the reaper still finds the tree that kept its parents
        return False


def _ppid_map() -> dict[int, tuple[int, str]]:
    """pid -> (ppid, state) for every process: /proc on Linux, `ps` elsewhere (macOS: the hermetic tests)."""
    out: dict[int, tuple[int, str]] = {}
    if os.path.isdir("/proc/self"):
        for name in os.listdir("/proc"):
            if not name.isdigit():
                continue
            try:
                with open(f"/proc/{name}/stat") as fh:
                    stat = fh.read()
            except OSError:
                continue
            rest = stat[stat.rfind(")") + 2:].split()   # the command name may hold spaces and parentheses
            if len(rest) >= 2:
                out[int(name)] = (int(rest[1]), rest[0])
        return out
    r = subprocess.run(["ps", "-A", "-o", "pid=,ppid=,stat="], capture_output=True, text=True, timeout=20, check=False)
    for line in r.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[0].isdigit() and parts[1].isdigit():
            out[int(parts[0])] = (int(parts[1]), parts[2][:1])
    return out


def descendants(root: int | None = None) -> list[int]:
    """Every live (non-zombie) descendant of `root` (default: this process), parents before children."""
    root = os.getpid() if root is None else root
    table = _ppid_map()
    children: dict[int, list[int]] = {}
    for pid, (ppid, _state) in table.items():
        children.setdefault(ppid, []).append(pid)
    out: list[int] = []
    queue = list(children.get(root, []))
    while queue:
        pid = queue.pop(0)
        if pid == os.getpid():
            continue
        if table.get(pid, (0, ""))[1] != "Z":
            out.append(pid)
        queue.extend(children.get(pid, []))
    return out


def _gone(pid: int) -> bool:
    """Reap `pid` if it is our child; True once it no longer runs (exited, or a zombie someone else will reap)."""
    with contextlib.suppress(ChildProcessError, OSError):
        if os.waitpid(pid, os.WNOHANG)[0] == pid:
            return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return _ppid_map().get(pid, (0, "Z"))[1] == "Z"


def stop_tree(root: int | None = None, *, grace_s: float = 6.0, kill_s: float = 3.0) -> tuple[list[int], list[int]]:
    """SIGTERM every descendant of `root` (default: this process), wait up to `grace_s`, SIGKILL what is left and wait
    up to `kill_s` more. Descendants that appear meanwhile (re-parented orphans, a last spawn) are swept too. Returns
    (the pids found, the pids still running at the end)."""
    found: list[int] = []
    deadline = time.monotonic() + grace_s
    sig = signal.SIGTERM
    while True:
        alive = [p for p in descendants(root) if not _gone(p)]
        for p in alive:
            if p not in found:
                found.append(p)
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(p, sig)
        if not alive:
            return found, []
        if time.monotonic() >= deadline:
            if sig == signal.SIGKILL:
                return found, alive
            sig, deadline = signal.SIGKILL, time.monotonic() + kill_s
        time.sleep(0.1)


def kill_group(pid: int, *, grace_s: float = 5.0) -> None:
    """Stop a child started with `start_new_session=True` together with everything it spawned (its process group):
    SIGTERM, up to `grace_s`, then SIGKILL. The caller still awaits/waits the child itself."""
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signal.SIGTERM)
    deadline = time.monotonic() + grace_s
    while time.monotonic() < deadline:
        try:
            os.killpg(pid, 0)
        except ProcessLookupError:
            return
        except PermissionError:
            # macOS answers EPERM, not ESRCH, for a group whose members have all exited but are not yet reaped
            # (zombies): nothing is left to signal. It escaped here before, failing a timed-out turn with PermissionError
            # (test_proctree on macOS, about 1 run in 25; its traceback showed the deadline's CancelledError first).
            break
        time.sleep(0.1)
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signal.SIGKILL)

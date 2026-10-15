"""Start hpc-bridge's MCP server with its stderr appended to a log: the one launcher every operator's config names.

    python -I serve_with_stderr.py --log PATH --operator NAME -- CMD [ARG...]

It appends one header line to PATH (the time, this process's pid, the pid of the MCP client that spawned it, the
operator), points file descriptor 2 at PATH, opened for append, and then EXECS CMD in place of itself. So the server
keeps this pid, the stdin and stdout its client gave (the MCP stdio channel: never read or written here) and its own
exit code; a signal sent to this pid reaches the server itself; and nothing is written to stdout. A client that starts
the server again appends a second header: a restart shows in the log.

If PATH cannot be opened, CMD still runs, with stderr where the client put it: the log is evidence, never a reason the
server does not start. Stdlib only and run with -I: nothing from the harness or the environment is imported. The
harness side (when the log is read, capped, redacted and copied into the bundle) is `server_stderr.py`.
"""
from __future__ import annotations

import os
import re
import signal
import sys
import time

# One line per server start. `server_stderr.ServerWatch` reads `pid` (the server keeps it: exec) and `client` (whether
# the client outlived its server); `ts` is epoch seconds, `at` the same instant for a reader. Readers SEARCH a line for
# it: a log written before the wrapper ended an unterminated line may carry a start glued to the end of one.
HEADER_RE = re.compile(r"=== hpc-bridge MCP server start (?P<at>\S+) ts=(?P<ts>\d+(?:\.\d+)?) pid=(?P<pid>\d+) "
                       r"client=(?P<client>\d+) operator=(?P<operator>\S*) ===$")
USAGE = "usage: serve_with_stderr.py --log PATH --operator NAME -- CMD [ARG...]\n"


def header(pid: int, client: int, operator: str, now: float | None = None) -> str:
    now = time.time() if now is None else now
    at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
    return f"=== hpc-bridge MCP server start {at} ts={now:.3f} pid={pid} client={client} operator={operator} ===\n"


def _parse(argv: list[str]) -> tuple[str, str, list[str]] | None:
    opts: dict[str, str] = {}
    i = 0
    while i < len(argv) and argv[i] != "--":
        if argv[i] not in ("--log", "--operator") or i + 1 >= len(argv):
            return None
        opts[argv[i]] = argv[i + 1]
        i += 2
    cmd = argv[i + 1:]
    if "--log" not in opts or not cmd:
        return None
    return opts["--log"], opts.get("--operator", ""), cmd


def _line_break(log: str) -> str:
    """A newline when the log ends inside a line (an earlier server died mid-write), so this start is a line of its
    own; nothing for a new log or one that ends a line."""
    try:
        with open(log, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            if fh.tell() == 0:
                return ""
            fh.seek(-1, os.SEEK_END)
            return "" if fh.read(1) == b"\n" else "\n"
    except OSError:
        return ""


def _say(text: str) -> None:
    """A line on stderr — wherever it points now; never a reason not to start the server."""
    try:
        os.write(2, text.encode())
    except OSError:
        pass


def main(argv: list[str]) -> int:
    parsed = _parse(argv)
    if parsed is None:
        _say(USAGE)
        return 2
    log, operator, cmd = parsed
    try:
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    except OSError as exc:
        _say(f"serve_with_stderr: cannot open {log} ({exc}); the server's stderr is not captured\n")
    else:
        try:
            os.write(fd, (_line_break(log) + header(os.getpid(), os.getppid(), operator)).encode())
            if fd == 2:   # stderr was closed, so the log took its number: keep it across the exec
                os.set_inheritable(2, True)
            else:
                os.dup2(fd, 2)   # inheritable: the server writes here directly
        except OSError as exc:
            _say(f"serve_with_stderr: cannot write {log} ({exc})\n")
        finally:
            if fd != 2:
                os.close(fd)
    # Python starts with SIGPIPE and SIGXFSZ ignored, and an ignored signal stays ignored across exec: give the server
    # the default dispositions its client gave this process.
    for name in ("SIGPIPE", "SIGXFSZ"):
        sig = getattr(signal, name, None)
        if sig is not None:
            signal.signal(sig, signal.SIG_DFL)
    try:
        os.execvp(cmd[0], cmd)  # noqa: S606 - the wrapper's one job: become the server command the harness built
    except OSError as exc:
        _say(f"serve_with_stderr: cannot run {cmd[0]!r}: {exc}\n")
        return 127 if isinstance(exc, FileNotFoundError) else 126
    return 0   # not reached: exec replaced this process


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

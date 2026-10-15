"""The hpc-bridge MCP server's own stderr, kept in every run's bundle as `server-stderr.log`.

Why: on 2026-10-15 the server's connection closed in the middle of `teardown_slow_fs` (Pi: "MCP connection closed";
Codex: "Transport closed"), the teardown never reached `down`, and the bundles held nothing from the server itself, so
the crash could not be diagnosed. Its diagnostics and any traceback go to its stderr.

- `wrap` puts `serve_with_stderr.py` in front of the server command in every operator's config (the Claude SDK, the
  ACP operators, hermes, Codex, Pi, hermes21). It appends the server's stderr to the run's raw log, which run.py names
  in `HPCB_SERVER_STDERR` (a scratch file in the jail, never the bundle: raw stderr may carry a token), with a header
  line per server start, and then execs the server.
- `ServerWatch` follows that log while the operator runs: it learns each server process from its header, notes when
  one goes away and whether its MCP client went on without it, and trims the log past a ceiling.
- After the run, `bundle_log` writes the log into the bundle, redacted (`redact`) and capped to its last 2 MB, and
  `exited_row` is the report-only `harness:server_exited` row for a server that went away while its client ran on.

The server's exit STATUS is not observable: the wrapper execs into the server, so the MCP client that spawned it, not
the harness, reaps it. What the harness can see is when the process went away and whether its client outlived it.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from invariants import Result
from proctree import _ppid_map
from serve_with_stderr import HEADER_RE

LOG_ENV = "HPCB_SERVER_STDERR"            # the run's raw log; set by run.py, read where the server command is built
WRAPPER = Path(__file__).resolve().with_name("serve_with_stderr.py")
BUNDLE_NAME = "server-stderr.log"
ROW = "harness:server_exited"
KEEP_BYTES = 2 * 1024 * 1024              # the bundle keeps the last 2 MB
CEILING_BYTES = 8 * 1024 * 1024           # past this the raw log is cut back to KEEP_BYTES while the run goes on
READ_MAX_BYTES = 16 * 1024 * 1024         # what the bundle copy reads at most (from the end)
INTERVAL_S = 0.5                          # how often the watch looks
GRACE_S = 3.0                             # a client that outlives its server by more than this went on without it
TAIL_LINES = 20


def wrap(cmd: list[str], operator: str, log: str | None = None) -> list[str]:
    """The server command `cmd`, launched through the wrapper when the run has a log (`HPCB_SERVER_STDERR`), else
    unchanged. The interpreter is this one, by absolute path: hermes and Codex filter the server's environment, PATH
    included."""
    log = log if log is not None else os.environ.get(LOG_ENV, "").strip()
    if not log:
        return list(cmd)
    return [sys.executable or "python3", "-I", str(WRAPPER), "--log", log, "--operator", operator, "--", *cmd]


def prepare() -> Path:
    """A fresh raw log for this run, named in `HPCB_SERVER_STDERR` for the operators' config builders. The file is
    created by the first server start."""
    path = Path(tempfile.mkdtemp(prefix="hpcb-server-")) / "server-stderr.raw"
    os.environ[LOG_ENV] = str(path)
    return path


def discard(path: Path) -> None:
    """Forget the raw log once the bundle has its copy."""
    if os.environ.get(LOG_ENV) == str(path):
        del os.environ[LOG_ENV]
    if path.parent.name.startswith("hpcb-server-"):
        shutil.rmtree(path.parent, ignore_errors=True)


def trim_log(path: Path, keep: int, note_limit: int | None = None) -> int:
    """Cut `path` back to its last `keep` bytes, from a line start, behind a one-line note; returns the new size. The
    server may be appending meanwhile (O_APPEND): what it writes between the read and the truncate is lost, which is
    the price of a ceiling."""
    with path.open("r+b") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        if size <= keep:
            return size
        fh.seek(size - keep)
        tail = fh.read()
        nl = tail.find(b"\n")
        if 0 <= nl < len(tail) - 1:
            tail = tail[nl + 1:]
        limit = f" (the log is kept under {note_limit} bytes)" if note_limit else ""
        note = f"=== harness: trimmed the {size - len(tail)} bytes before this line{limit} ===\n".encode()
        fh.seek(0)
        fh.write(note + tail)
        fh.truncate()
        return len(note) + len(tail)


@dataclass
class ServerProcess:
    """One server start, as its header gave it, and what the watch saw of its end (epoch seconds)."""
    pid: int
    client: int
    operator: str
    started_at: float
    ended_at: float | None = None          # first seen gone (a zombie counts as gone)
    client_ended_at: float | None = None   # its client first seen gone, at or after that
    client_alive_at_end: bool | None = None

    def outlived_s(self, run_ended: float | None) -> float | None:
        """How long the client ran on after its server went — to its own end, else to the end of the run."""
        if self.ended_at is None or not self.client_alive_at_end:
            return None
        until = self.client_ended_at if self.client_ended_at is not None else run_ended
        return None if until is None else max(0.0, until - self.ended_at)


class ServerWatch:
    """Follows the raw log while the operator runs (`watching`). `table` (pid -> (ppid, state)) and `clock` are seams
    for the tests; by default the process table and the wall clock."""

    def __init__(self, path: Path, *, interval: float | None = None, grace_s: float | None = None,
                 ceiling: int | None = None, keep: int | None = None, clock: Callable[[], float] = time.time,
                 table: Callable[[], dict[int, tuple[int, str]]] | None = None) -> None:
        self.path = Path(path)
        self.interval = INTERVAL_S if interval is None else interval
        self.grace_s = GRACE_S if grace_s is None else grace_s
        self.ceiling = CEILING_BYTES if ceiling is None else ceiling
        self.keep = KEEP_BYTES if keep is None else keep
        self.clock = clock
        self.table = table or _ppid_map
        self.run_ended: float | None = None
        self._procs: dict[tuple[int, str], ServerProcess] = {}
        self._offset = 0
        self._partial = b""
        self._lock = threading.Lock()   # the follower polls in a worker thread; `finish` polls in the caller's

    @property
    def processes(self) -> list[ServerProcess]:
        return list(self._procs.values())

    def _read_headers(self) -> None:
        try:
            size = self.path.stat().st_size
        except OSError:
            return
        if size < self._offset:          # cut back by a trim: read it again (known starts are not added twice)
            self._offset, self._partial = 0, b""
        if size == self._offset:
            return
        with self.path.open("rb") as fh:
            fh.seek(self._offset)
            data = fh.read(size - self._offset)
        self._offset += len(data)
        *lines, self._partial = (self._partial + data).split(b"\n")
        for raw in lines:
            m = HEADER_RE.match(raw.decode(errors="replace"))
            if m and (int(m["pid"]), m["ts"]) not in self._procs:
                self._procs[(int(m["pid"]), m["ts"])] = ServerProcess(
                    int(m["pid"]), int(m["client"]), m["operator"], float(m["ts"]))

    def _mark(self, p: ServerProcess) -> None:
        when = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(p.ended_at or 0))
        try:
            with self.path.open("a") as fh:
                fh.write(f"=== hpc-bridge MCP server pid={p.pid} gone (seen by the harness at {when}) ===\n")
        except OSError:
            pass

    def poll(self) -> None:
        """One look: new starts from the log, then which servers (and clients) are gone, then the ceiling."""
        with self._lock:
            self._poll()

    def _poll(self) -> None:
        self._read_headers()
        pending = [p for p in self._procs.values()
                   if p.ended_at is None or (p.client_alive_at_end and p.client_ended_at is None)]
        if pending:
            table = self.table()
            live = {pid for pid, (_ppid, state) in table.items() if state != "Z"}
            now = self.clock()
            for p in pending:
                if p.ended_at is None:
                    if p.pid in live:
                        continue
                    p.ended_at = now
                    p.client_alive_at_end = p.client in live
                    if not p.client_alive_at_end:
                        p.client_ended_at = now
                    self._mark(p)
                elif p.client not in live:
                    p.client_ended_at = now
        try:
            if self.path.stat().st_size > self.ceiling:
                trim_log(self.path, self.keep, note_limit=self.ceiling)
                self._offset, self._partial = self.path.stat().st_size, b""
        except OSError:
            pass

    def finish(self) -> None:
        """The operator has returned: the run's end, and one last look."""
        self.run_ended = self.clock()
        self.poll()

    @contextlib.asynccontextmanager
    async def watching(self):
        """Watch while the body (the operator's run) runs; `finish` when it returns, raises or is cancelled."""
        stop = asyncio.Event()

        async def follow() -> None:
            while not stop.is_set():
                try:
                    await asyncio.to_thread(self.poll)
                except Exception as exc:  # noqa: BLE001 - a diagnostic must never fail the run it watches
                    print(f"server stderr watch: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=self.interval)

        task = asyncio.create_task(follow())
        try:
            yield self
        finally:
            stop.set()   # the follower ends after the look it is taking (a look is quick)
            try:
                await task
            except asyncio.CancelledError:
                task.cancel()
                self._close()
                raise    # a cancellation that arrives while the watch closes is the run's: never swallowed
            self._close()

    def _close(self) -> None:
        try:
            self.finish()
        except Exception as exc:  # noqa: BLE001 - a diagnostic must never fail the run it watches
            print(f"server stderr watch: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)

    def early(self) -> list[ServerProcess]:
        """The servers that went away while their client ran on for more than `grace_s` — the client lost its server
        mid-session. A client that ends its session takes its server with it (a turn-per-process operator, a chain's
        phase, every normal end), so their ends nearly coincide."""
        return [p for p in self._procs.values() if (s := p.outlived_s(self.run_ended)) is not None and s > self.grace_s]

    def summary(self) -> dict[str, Any]:
        """For record.json: every server start and what was seen of its end."""
        early = {id(p) for p in self.early()}

        def r(x: float | None) -> float | None:
            return None if x is None else round(x, 1)

        return {"processes": [{"pid": p.pid, "client": p.client, "operator": p.operator, "started_at": r(p.started_at),
                                "ended_at": r(p.ended_at), "client_ended_at": r(p.client_ended_at),
                                "client_outlived_s": r(p.outlived_s(self.run_ended)), "early": id(p) in early}
                               for p in self._procs.values()],
                "run_ended_at": r(self.run_ended)}


# ---------------------------------------------------------------- redaction and the bundle copy

_PEM = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----", re.S)
_BEARER = re.compile(r"\b(Bearer)\s+[A-Za-z0-9._~+/=-]{8,}", re.I)
_KEYED = re.compile(r"\b((?:access|refresh|id)_token|token|client_secret|api[_-]?key|password|passwd)"
                    r"([\"']?\s*[:=]\s*[\"']?)(?!<redacted)[^\s\"',;}]+", re.I)
_ANTHROPIC = re.compile(r"\bsk-ant-[A-Za-z0-9_-]{8,}")


def redact(text: str, secrets: dict[str, list[str]] | None = None) -> str:
    """Hide secrets before server output enters a bundle: every known VALUE (run.py's `_secret_material`: the pool
    user's private key lines, the Globus tokens of the mounted identity, the Claude and ALCF credentials) by label, then
    token SHAPES: private-key blocks, `Bearer …`, `access_token` / `refresh_token` / `password` / `api_key` = …, and
    Anthropic keys."""
    for label, values in (secrets or {}).items():
        for v in sorted({v for v in values if len(v) >= 8}, key=len, reverse=True):
            text = text.replace(v, f"<redacted {label}>")
    text = _PEM.sub("<redacted private key>", text)
    text = _BEARER.sub(r"\1 <redacted>", text)
    text = _KEYED.sub(r"\1\2<redacted>", text)
    return _ANTHROPIC.sub("<redacted>", text)


def read_log(path: Path, secrets: dict[str, list[str]] | None = None, keep: int | None = None) -> str | None:
    """The raw log as the bundle keeps it: redacted, then cut to its last `keep` bytes from a line start, behind a
    note. None when there is no log (no server ever started)."""
    keep = KEEP_BYTES if keep is None else keep
    try:
        with Path(path).open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - READ_MAX_BYTES))
            data = fh.read()
    except OSError:
        return None
    if size > len(data):             # read from the middle of a line: start at the next one
        data = data[data.find(b"\n") + 1:]
    text = redact(data.decode(errors="replace"), secrets).encode()
    if len(text) > keep:
        tail = text[-keep:]
        tail = tail[tail.find(b"\n") + 1:] if b"\n" in tail[:-1] else tail
        note = f"=== harness: trimmed the {size - len(tail)} bytes before this line (the bundle keeps {keep}) ===\n"
        text = note.encode() + tail
    elif size > len(data):
        text = f"=== harness: the first {size - len(data)} bytes were not read ===\n".encode() + text
    return text.decode(errors="replace")


def bundle_log(path: Path, dest: Path, secrets: dict[str, list[str]] | None = None) -> Path | None:
    """Write the run's server stderr into the bundle dir `dest`; returns the file, or None when there was no log."""
    text = read_log(path, secrets)
    if text is None:
        return None
    out = Path(dest) / BUNDLE_NAME
    out.write_text(text)
    return out


def _segment(text: str, pid: int) -> list[str]:
    """The lines a server wrote: from its start header to the next start, without the harness's own lines."""
    lines = text.splitlines()
    start = next((i for i, ln in enumerate(lines) if (m := HEADER_RE.match(ln)) and int(m["pid"]) == pid), None)
    if start is None:
        body = lines
    else:
        end = next((i for i in range(start + 1, len(lines)) if HEADER_RE.match(lines[i])), len(lines))
        body = lines[start + 1:end]
    return [ln for ln in body if not ln.startswith("=== ")]


def exited_row(watch: ServerWatch, secrets: dict[str, list[str]] | None = None) -> Result | None:
    """`harness:server_exited` (report-only) when a server went away while its client ran on: which, when, for how
    long the client went on without it, and the last lines that server wrote to stderr. None otherwise."""
    early = watch.early()
    if not early:
        return None
    p = early[0]
    text = read_log(watch.path, secrets) or ""
    tail = [ln if len(ln) <= 300 else ln[:299] + "…" for ln in _segment(text, p.pid)[-TAIL_LINES:]]
    when = time.strftime("%H:%M:%S", time.gmtime(p.ended_at or 0))
    more = f" ({len(early) - 1} more server process(es) went the same way)" if len(early) > 1 else ""
    starts = len(watch.processes)
    said = ("; the last lines of its stderr (server-stderr.log):\n    " + "\n    ".join(tail)) if tail \
        else "; it wrote nothing to stderr"
    return Result(ROW, False,
                  f"the hpc-bridge MCP server (pid {p.pid}, {p.operator}) went away at {when} UTC while its MCP client "
                  f"(pid {p.client}) ran on for {p.outlived_s(watch.run_ended):.0f} s more{more}; {starts} server "
                  f"start(s) in this run{said}")

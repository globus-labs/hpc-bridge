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
  `rows` gives the report-only diagnostics: `harness:server_channel_closed` (an hpc-bridge call's result says the MCP
  channel closed under it — the incident's shape: the channel went, the process may have lived on),
  `harness:server_exited` (the process went while its client ran on) and `harness:server_orphaned` (the process ran
  on after its client went).

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

from invariants import _HPC_TOOLS, Result
from proctree import _ppid_map
from serve_with_stderr import HEADER_RE

LOG_ENV = "HPCB_SERVER_STDERR"            # the run's raw log; set by run.py, read where the server command is built
WRAPPER = Path(__file__).resolve().with_name("serve_with_stderr.py")
BUNDLE_NAME = "server-stderr.log"
ROW = "harness:server_exited"                     # the server PROCESS went while its client ran on
CHANNEL_ROW = "harness:server_channel_closed"     # the MCP CHANNEL closed under a call (the process may have lived on)
ORPHAN_ROW = "harness:server_orphaned"            # the server ran on after its client went
ROWS = frozenset({ROW, CHANNEL_ROW, ORPHAN_ROW})  # all REPORT-ONLY: run.py never lets one gate
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


def prepare() -> Path | None:
    """A fresh raw log for this run, named in `HPCB_SERVER_STDERR` for the operators' config builders; the file is
    created by the first server start. Never raises: with no place for the log (a full disk), it returns None, the
    variable stays unset, and the servers start unwrapped — the run and its bundle go on without their stderr."""
    try:
        path = Path(tempfile.mkdtemp(prefix="hpcb-server-")) / "server-stderr.raw"
    except Exception as exc:  # noqa: BLE001 - evidence, never a reason the run is lost
        os.environ.pop(LOG_ENV, None)
        print(f"server stderr: no log this run ({type(exc).__name__}: {exc})", file=sys.stderr, flush=True)
        return None
    os.environ[LOG_ENV] = str(path)
    return path


def discard(path: Path | None) -> None:
    """Forget the raw log once the bundle has its copy."""
    if path is None:
        return
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
    client_ended_at: float | None = None   # its client first seen gone (before or after the server)
    client_alive_at_end: bool | None = None   # at the server's end: True = the client still ran; None = not ended
    orphan_marked: bool = False            # the log already says it outlived its client

    def outlived_s(self, run_ended: float | None) -> float | None:
        """How long the client ran on after its server went — to its own end, else to the end of the run."""
        if self.ended_at is None or not self.client_alive_at_end:
            return None
        until = self.client_ended_at if self.client_ended_at is not None else run_ended
        return None if until is None else max(0.0, until - self.ended_at)

    def orphaned_s(self, run_ended: float | None) -> float | None:
        """How long the server ran on after its client went — to its own end, else to the end of the run."""
        if self.client_ended_at is None or self.client_alive_at_end:
            return None
        until = self.ended_at if self.ended_at is not None else run_ended
        return None if until is None else max(0.0, until - self.client_ended_at)


class ServerWatch:
    """Follows the raw log while the operator runs (`watching`). `table` (pid -> (ppid, state)) and `clock` are seams
    for the tests; by default the process table and the wall clock."""

    def __init__(self, path: Path | None, *, interval: float | None = None, grace_s: float | None = None,
                 ceiling: int | None = None, keep: int | None = None, clock: Callable[[], float] = time.time,
                 table: Callable[[], dict[int, tuple[int, str]]] | None = None) -> None:
        self.path = Path(path) if path is not None else None   # None: no log this run (prepare failed) — sees nothing
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
        if self.path is None:
            return
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
            m = HEADER_RE.search(raw.decode(errors="replace"))
            if m and (int(m["pid"]), m["ts"]) not in self._procs:
                self._procs[(int(m["pid"]), m["ts"])] = ServerProcess(
                    int(m["pid"]), int(m["client"]), m["operator"], float(m["ts"]))

    def _mark(self, what: str, at: float) -> None:
        """A harness line in the log, where it happened among the server's own."""
        if self.path is None:
            return
        when = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(at))
        try:
            with self.path.open("a") as fh:
                fh.write(f"=== hpc-bridge MCP server {what} (seen by the harness at {when}) ===\n")
        except OSError:
            pass

    def poll(self) -> None:
        """One look: new starts from the log, then which servers (and clients) are gone, then the ceiling."""
        with self._lock:
            self._poll()

    def _poll(self) -> None:
        self._read_headers()
        pending = [p for p in self._procs.values() if p.ended_at is None or p.client_ended_at is None]
        if pending:
            table = self.table()
            live = {pid for pid, (_ppid, state) in table.items() if state != "Z"}
            now = self.clock()
            for p in pending:
                if p.client_ended_at is None and p.client not in live:
                    p.client_ended_at = now
                if p.ended_at is None and p.pid not in live:
                    p.ended_at = now
                    p.client_alive_at_end = p.client_ended_at is None
                    self._mark(f"pid={p.pid} gone", now)
                elif (p.ended_at is None and p.client_ended_at is not None and not p.orphan_marked
                      and now - p.client_ended_at > self.grace_s):
                    p.orphan_marked = True
                    self._mark(f"pid={p.pid} still runs: its client pid={p.client} is gone", now)
        if self.path is None:
            return
        try:
            if self.path.stat().st_size > self.ceiling:
                trim_log(self.path, self.keep, note_limit=self.ceiling)
                # read the kept tail again: a start appended after this look read the log is in it (the (pid, ts)
                # key keeps the starts already known from being added twice)
                self._offset, self._partial = 0, b""
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

    def orphans(self) -> list[ServerProcess]:
        """The servers that ran on for more than `grace_s` after their client went: nobody speaks to them, and one
        still running at the end of the run is what `_stop_operator` must stop before the teardown."""
        return [p for p in self._procs.values() if (s := p.orphaned_s(self.run_ended)) is not None and s > self.grace_s]

    def summary(self) -> dict[str, Any]:
        """For record.json: every server start and what was seen of its end."""
        early = {id(p) for p in self.early()}
        orphans = {id(p) for p in self.orphans()}

        def r(x: float | None) -> float | None:
            return None if x is None else round(x, 1)

        return {"processes": [{"pid": p.pid, "client": p.client, "operator": p.operator, "started_at": r(p.started_at),
                                "ended_at": r(p.ended_at), "client_ended_at": r(p.client_ended_at),
                                "client_outlived_s": r(p.outlived_s(self.run_ended)), "early": id(p) in early,
                                "server_outlived_client_s": r(p.orphaned_s(self.run_ended)), "orphaned": id(p) in orphans}
                               for p in self._procs.values()],
                "run_ended_at": r(self.run_ended)}


# ---------------------------------------------------------------- redaction and the bundle copy

_PEM = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----", re.S)
_BEARER = re.compile(r"\b(Bearer)\s+([A-Za-z0-9._~+/=-]{8,})", re.I)
_KEYED = re.compile(r"\b((?:access|refresh|id)_token|token|client_secret|api[_-]?key|password|passwd)"
                    r"([\"']?\s*[:=]\s*[\"']?)(?!<redacted)([^\s\"',;}]+)", re.I)
_ANTHROPIC = re.compile(r"\bsk-ant-[A-Za-z0-9_-]{8,}")
# Globus Auth issues opaque "v2" tokens (no documented prefix or length; base64url-like) and, for some clients, JWT
# "v3" tokens (docs.globus.org, Auth developer guide, "JWT Support"); its id_token is a JWT too. So: any JWT, and any
# run of 40+ base64url characters that mixes capitals, small letters and digits. UUIDs, hex digests and endpoint
# names have no capitals and stay.
_JWT = re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}")
_LONG_RUN = re.compile(r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{40,}(?![A-Za-z0-9_-])")


def _opaque(v: str) -> bool:
    """A value that reads as a credential, not a word: 8+ characters with a digit or 2+ capitals, or 24+ characters.
    `token: expired`, `password: not required`, `Bearer authentication failed` keep their meaning."""
    return len(v) >= 8 and (len(v) >= 24 or any(c.isdigit() for c in v) or sum(c.isupper() for c in v) >= 2)


def _mixed(v: str) -> bool:
    return any(c.isupper() for c in v) and any(c.islower() for c in v) and any(c.isdigit() for c in v)


def redact(text: str, secrets: dict[str, list[str]] | None = None) -> str:
    """Hide secrets before server output enters a bundle: every known VALUE (run.py's `_redaction_material`: the pool
    user's private key lines, the Globus tokens of the run's store at its start and end and of the mounted identity,
    the Claude and ALCF credentials) by label, then token SHAPES: private-key blocks, JWTs, Globus-like opaque tokens,
    `Bearer …`, `access_token` / `refresh_token` / `password` / `api_key` = … (a credential-like value only), and
    Anthropic keys."""
    for label, values in (secrets or {}).items():
        for v in sorted({v for v in values if len(v) >= 8}, key=len, reverse=True):
            text = text.replace(v, f"<redacted {label}>")
    text = _PEM.sub("<redacted private key>", text)
    text = _JWT.sub("<redacted jwt>", text)
    text = _ANTHROPIC.sub("<redacted>", text)
    text = _LONG_RUN.sub(lambda m: "<redacted token>" if _mixed(m[0]) else m[0], text)
    text = _BEARER.sub(lambda m: f"{m[1]} <redacted>" if _opaque(m[2]) else m[0], text)
    return _KEYED.sub(lambda m: f"{m[1]}{m[2]}<redacted>" if _opaque(m[3]) else m[0], text)


def read_log(path: Path | None, secrets: dict[str, list[str]] | None = None, keep: int | None = None) -> str | None:
    """The raw log as the bundle keeps it: redacted, then cut to its last `keep` bytes from a line start, behind a
    note. None when there is no log (no server ever started, or no log this run)."""
    keep = KEEP_BYTES if keep is None else keep
    if path is None:
        return None
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


def bundle_log(path: Path | None, dest: Path, secrets: dict[str, list[str]] | None = None) -> Path | None:
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
    start = next((i for i, ln in enumerate(lines) if (m := HEADER_RE.search(ln)) and int(m["pid"]) == pid), None)
    if start is None:
        body = lines
    else:
        end = next((i for i in range(start + 1, len(lines)) if HEADER_RE.search(lines[i])), len(lines))
        body = lines[start + 1:end]
    return [ln for ln in body if not ln.startswith("=== ")]


def _tail(lines: list[str]) -> list[str]:
    return [ln if len(ln) <= 300 else ln[:299] + "…" for ln in lines[-TAIL_LINES:]]


def _said(tail: list[str], whose: str) -> str:
    return (f"; the last lines of {whose} stderr (server-stderr.log):\n    " + "\n    ".join(tail)) if tail \
        else f"; {whose} stderr has nothing"


def exited_row(watch: ServerWatch, secrets: dict[str, list[str]] | None = None) -> Result | None:
    """`harness:server_exited` (report-only) when a server went away while its client ran on: which, when, for how
    long the client went on without it, and the last lines that server wrote to stderr. None otherwise."""
    early = watch.early()
    if not early:
        return None
    p = early[0]
    text = read_log(watch.path, secrets) or ""
    when = time.strftime("%H:%M:%S", time.gmtime(p.ended_at or 0))
    more = f" ({len(early) - 1} more server process(es) went the same way)" if len(early) > 1 else ""
    starts = len(watch.processes)
    return Result(ROW, False,
                  f"the hpc-bridge MCP server (pid {p.pid}, {p.operator}) went away at {when} UTC while its MCP client "
                  f"(pid {p.client}) ran on for {p.outlived_s(watch.run_ended):.0f} s more{more}; {starts} server "
                  f"start(s) in this run{_said(_tail(_segment(text, p.pid)), 'its')}")


def orphaned_row(watch: ServerWatch) -> Result | None:
    """`harness:server_orphaned` (report-only) when a server ran on after its MCP client went: nobody can reach it, and
    while it runs it can still act (a straggling call re-provisioned an endpoint after a teardown once)."""
    orphans = watch.orphans()
    if not orphans:
        return None
    p = orphans[0]
    left = "and was still running when the operator returned (the harness stops it before the teardown)" \
        if p.ended_at is None else "before it went too"
    more = f" ({len(orphans) - 1} more)" if len(orphans) > 1 else ""
    return Result(ORPHAN_ROW, False,
                  f"the hpc-bridge MCP server (pid {p.pid}, {p.operator}) ran on for "
                  f"{p.orphaned_s(watch.run_ended):.0f} s after its MCP client (pid {p.client}) went, {left}{more}")


# What an MCP client says when the channel to its server closed under a call, in its own words (Pi; Codex; Claude Code
# and other TypeScript-SDK clients: McpError -32000; the Python SDK's anyio streams), and what hpc-bridge's journal
# records for a call its stdio loop cancelled as it ended (the CLI operators' trace carries the journal's row).
_CHANNEL_CLOSED = re.compile(r"MCP connection closed|Transport closed|\bConnection closed\b|MCP error -32000|"
                             r"ClosedResourceError|BrokenResourceError|\brun_stdio_async\b", re.I)
_TEXT_ONLY = frozenset({"text", "is_error", "value"})   # a client's or the journal's words, not a tool's structured answer


def _closed_words(result: Any) -> str | None:
    """The text of a result that says the channel closed, else None. Only a plain-text result counts: hpc-bridge answers
    with structured results, which may quote ssh's or a log's own "Connection closed"."""
    if not isinstance(result, dict) or not result or set(result) - _TEXT_ONLY:
        return None
    text = result.get("text") or result.get("value") or ""
    if not isinstance(text, str):   # a list or dict answer (`list_facilities`) is structured too
        return None
    return text if len(text) <= 1500 and _CHANNEL_CLOSED.search(text) else None


def channel_closed_row(trace: Any, messages: list[Any] | None = None, watch: ServerWatch | None = None,
                       secrets: dict[str, list[str]] | None = None) -> Result | None:
    """`harness:server_channel_closed` (report-only) when an hpc-bridge call's result says the MCP channel closed under
    it: what the client said (or the journal recorded), at which call, how much the run did after it, and the last
    lines of the server's stderr. The server PROCESS may have lived on (2026-10-15: `teardown_slow_fs` under Pi and
    Codex, whose journals record the stdio loop cancelling the teardown call). Reads the graded trace and, for the CLI
    operators, the harness's own events in `messages` (a call that never reached the server keeps the client's words
    only there; their order is the trace's)."""
    calls = list(getattr(trace, "calls", None) or [])
    hits: list[tuple[int, str, str]] = []
    for i, c in enumerate(calls):
        if c.name in _HPC_TOOLS and (words := _closed_words(c.result)):
            hits.append((i, c.name, words))
            break
    native = [m["native"] for m in messages or [] if isinstance(m, dict) and isinstance(m.get("native"), dict)]
    for k, n in enumerate(native):
        if n.get("hpcb") and (words := _closed_words(n.get("result"))):
            hits.append((k, str(n.get("name")), words))
            break
    if not hits:
        return None
    i, name, words = min(hits, key=lambda h: h[0])
    after = sum(1 for c in calls[i + 1:] if c.name in _HPC_TOOLS)
    said = " ".join(redact(words, secrets).split())
    said = said if len(said) <= 400 else said[:399] + "…"
    log = read_log(watch.path, secrets) if watch is not None else None
    tail = _tail([ln for ln in (log or "").splitlines() if not ln.startswith("=== ") and not HEADER_RE.search(ln)])
    stderr = _said(tail, "the server's")
    return Result(CHANNEL_ROW, False,
                  f"the MCP channel to hpc-bridge closed under call {i} ({name}): {said!r}; the run went on for "
                  f"{after} more hpc-bridge call(s){stderr}")


def rows(watch: ServerWatch, trace: Any = None, messages: list[Any] | None = None,
         secrets: dict[str, list[str]] | None = None) -> list[Result]:
    """Every server diagnostic that applies to this run (all REPORT-ONLY): the channel closed under a call, a server
    process that went while its client ran on, one that ran on after its client went."""
    found = [channel_closed_row(trace, messages, watch, secrets) if trace is not None else None,
             exited_row(watch, secrets), orphaned_row(watch)]
    return [r for r in found if r is not None]

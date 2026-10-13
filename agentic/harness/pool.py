"""Cross-process claims on the harness' POOL of cluster test users.

Why this exists (diagnosed 2026-09-03, the "block-thrashing bug" that wasn't): `run_suite` handed
out pool users from an in-process queue starting at `hpcbridge-test-00`, so two concurrent
invocations BOTH ran as test-00 — and the harness teardown cancelled that user's jobs, so one run
tore down the other's live blocks mid-task. A pool user is a shared cluster identity; claiming one
must be coordinated across every harness process on this host.

A claim is an exclusive `flock` on a per-user lock file, held for exactly as long as the user is in
use. The kernel drops the lock when the holding process exits — crash, SIGKILL, rate-limit halt —
so a dead run can never leave a stale claim behind (unlike a PID file). HOST-WIDE: every checkout and
worktree on this host claims from one directory (`$XDG_STATE_HOME/hpc-bridge/harness-claims`, else
`~/.local/state/…`; `HPCB_POOL_CLAIMS_DIR` moves it). Two hosts driving the same pool still need to split
it by hand (`--pool` / `HPCB_POOL`).
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import time
from collections.abc import Callable, Iterator
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
_STATE_HOME = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local" / "state")
DEFAULT_CLAIMS_DIR = Path(os.environ.get("HPCB_POOL_CLAIMS_DIR") or _STATE_HOME / "hpc-bridge" / "harness-claims")
# The default used to be per CHECKOUT. While that directory exists a claim takes its lock file too, so a suite still
# running older code from this checkout and one running this code never share a pool user.
LEGACY_CLAIMS_DIR = REPO_ROOT / "agentic" / "runs" / ".pool-claims"


def _try_lock(path: Path) -> int | None:
    """An exclusive non-blocking flock on `path` (created if missing): the fd, or None when another holds it."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:  # held by another open file description — another process or another claimant
        os.close(fd)
        return None
    return fd


def _unlock(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


class PoolClaims:
    """Claim/release pool users with per-user flock files under `claims_dir` (host-wide by default)."""

    def __init__(self, claims_dir: Path | str | None = None, legacy_dir: Path | str | None = None) -> None:
        self.dir = Path(claims_dir or DEFAULT_CLAIMS_DIR)
        self.dir.mkdir(parents=True, exist_ok=True)
        # the old per-checkout directory is honoured only for the DEFAULT location (an explicit dir is a test, or
        # HPCB_POOL_CLAIMS_DIR — a deliberate choice), and only while it exists
        if legacy_dir is None and claims_dir is None and not os.environ.get("HPCB_POOL_CLAIMS_DIR"):
            legacy_dir = LEGACY_CLAIMS_DIR
        self.legacy = Path(legacy_dir) if legacy_dir else None
        self._held: dict[str, tuple[int, int | None]] = {}  # user -> (fd, legacy fd) holding the locks

    def _legacy_path(self, user: str) -> Path | None:
        return self.legacy / f"{user}.lock" if self.legacy is not None and self.legacy.is_dir() else None

    def try_claim(self, user: str) -> bool:
        """Claim `user` if no process (this one included) holds it — here or in the legacy directory. Non-blocking."""
        if user in self._held:
            return False
        fd = _try_lock(self.dir / f"{user}.lock")
        if fd is None:
            return False
        legacy_fd = None
        lp = self._legacy_path(user)
        if lp is not None:
            legacy_fd = _try_lock(lp)
            if legacy_fd is None:   # an older suite in this checkout holds it
                _unlock(fd)
                return False
        self._held[user] = (fd, legacy_fd)
        return True

    def claim_any(self, pool: list[str]) -> str | None:
        """The first free user in `pool` order, claimed — or None if every one is taken."""
        for user in pool:
            if self.try_claim(user):
                return user
        return None

    def release(self, user: str) -> None:
        fds = self._held.pop(user, None)
        for fd in fds or ():
            if fd is not None:
                _unlock(fd)

    def release_all(self) -> None:
        for user in list(self._held):
            self.release(user)

    def busy(self, pool: list[str]) -> list[str]:
        """Users currently claimed by ANY process (probe: try-lock then release) — for messages."""
        out = []
        for user in pool:
            if user in self._held:
                out.append(user)
                continue
            for path in (self.dir / f"{user}.lock", self._legacy_path(user)):
                if path is None:
                    continue
                fd = _try_lock(path)
                if fd is None:
                    out.append(user)
                    break
                _unlock(fd)
        return out


class NodeLaunches:
    """The compute-cell LAUNCHES of the last `window_s` seconds on one target, shared by every harness process on this
    host — the node gate's in-flight accounting (run_suite.NodeGate). Kept in `nodes-<target>.json` beside the pool
    claims; the probe + decision + claim run under an exclusive `flock` on `nodes-<target>.lock` (the kernel drops it
    if the holder dies), and an entry expires after `window_s`, so a crashed suite's launches stop counting on their
    own. Why: a cell's block is submitted minutes after launch, and per-process counts let concurrent suites (one per
    operator) each read the same idle nodes and oversubscribe the cluster."""

    def __init__(self, target: str, claims_dir: Path | str | None = None, *, window_s: float = 300.0,
                 clock: Callable[[], float] = time.time) -> None:
        self.dir = Path(claims_dir or DEFAULT_CLAIMS_DIR)
        self.dir.mkdir(parents=True, exist_ok=True)
        key = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in target) or "default"
        self.path = self.dir / f"nodes-{key}.json"
        self._lock_path = self.dir / f"nodes-{key}.lock"
        self.window_s = window_s
        self._clock = clock

    @contextlib.contextmanager
    def locked(self) -> Iterator[None]:
        """Exclusive across processes (and across threads of one process: each entry opens its own file description).
        Blocking — call it from a worker thread, never on the event loop."""
        fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _read(self) -> list[dict]:
        try:
            data = json.loads(self.path.read_text())
        except (OSError, ValueError):
            return []
        return [e for e in data if isinstance(e, dict) and isinstance(e.get("t"), (int, float))] if isinstance(data, list) else []

    def recent(self) -> int:
        """Launches still inside the window (call under `locked()`)."""
        now = self._clock()
        return sum(1 for e in self._read() if now - float(e["t"]) < self.window_s)

    def record(self, label: str) -> None:
        """Record one launch now, dropping expired entries (call under `locked()`). Written atomically."""
        now = self._clock()
        keep = [e for e in self._read() if now - float(e["t"]) < self.window_s]
        keep.append({"t": now, "pid": os.getpid(), "label": label})
        tmp = self.path.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(keep))
        os.replace(tmp, self.path)

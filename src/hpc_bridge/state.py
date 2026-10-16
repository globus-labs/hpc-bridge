# src/hpc_bridge/state.py
"""Durable local record of which login node each endpoint landed on.

HPC SSH aliases round-robin across login nodes, but a started endpoint's manager
daemon lives on ONE node. We record the resolved FQDN at provision time so later
sessions SSH straight to that node instead of the alias (which would orphan the
endpoint). The store is keyed by (alias, endpoint name) and lives in
~/.hpc-bridge/endpoints.json, 0600 — it references a credentialed host.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl


def _state_dir() -> Path:
    """Base dir for hpc-bridge's local state (login-node pins, the facility cache, SSH control
    sockets). Defaults to ~/.hpc-bridge; `HPC_BRIDGE_STATE_DIR` relocates it — tests point it at a
    tmp dir so they never read stale entries or WRITE into the developer's real state."""
    configured = os.environ.get("HPC_BRIDGE_STATE_DIR")
    if not configured and os.environ.get("HPC_BRIDGE_TEST_GUARD") == "1":
        # hpc-bridge's own unit tier sets the flag (tests/conftest.py) with a tmp state dir: reaching the default here
        # means a test escaped that isolation (found 2026-10-16: an empty ~/.hpc-bridge/sent-checks/ appeared in the
        # maintainer's state dir). An explicit flag, never "running under pytest": a third party's pytest-driven agent
        # eval launches hpc-bridge too, and must keep its normal state dir.
        raise RuntimeError("hpc-bridge: a test reached the real ~/.hpc-bridge — set HPC_BRIDGE_STATE_DIR "
                           "(tests/conftest.py does, for tests under tests/)")
    return Path(configured or (Path.home() / ".hpc-bridge"))


@dataclass(frozen=True)
class EndpointRecord:
    endpoint_id: str
    login_host: str | None  # resolved FQDN (hostname -f) the manager daemon runs on; None when not routable/known
    alias: str  # the round-robin SSH alias originally connected to
    user: str | None  # None => ~/.ssh/config decides (SshTarget.user)
    key_path: str | None  # None => the config's IdentityFile
    name: str
    provisioned_at: str  # ISO-8601 UTC
    # True when hpc-bridge itself placed the Globus token store on this login node (bootstrap seeded it because
    # the remote could not authenticate). Teardown wipes ONLY a store we seeded — never one that was already
    # there, e.g. a shared account the facility's own endpoint uses (security review follow-up, 2026-09-04).
    seeded_credentials: bool = False


def _key(alias: str, name: str) -> str:
    return f"{alias}::{name}"


class LoginNodeStore:
    def __init__(self, path: Path | str | None = None) -> None:
        default = _state_dir() / "endpoints.json"
        self.path = Path(path) if path else default

    def _load(self) -> dict[str, dict]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}

    def _save(self, data: dict[str, dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # write 0600 from creation: open with restrictive mode, then dump
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
        os.chmod(self.path, 0o600)

    def put(self, rec: EndpointRecord) -> None:
        data = self._load()
        data[_key(rec.alias, rec.name)] = asdict(rec)
        self._save(data)

    def get(self, *, alias: str, name: str) -> EndpointRecord | None:
        row = self._load().get(_key(alias, name))
        return EndpointRecord(**row) if row else None

    def remove(self, *, alias: str, name: str) -> None:
        data = self._load()
        if data.pop(_key(alias, name), None) is not None:
            self._save(data)

    def all(self) -> list[EndpointRecord]:
        return [EndpointRecord(**row) for row in self._load().values()]


class FacilityStore:
    """Persistent cache of PROVEN session (BYO) facility configs, keyed by **ssh_host** — the
    'local discovery' cache. PROVEN = the login shape's canary answered on that config (which is
    what exercises the discovered network interface); a config merely supplied or merely accepted
    by the bootstrap is NOT written (decision 2026-09-03). A fresh session resolves a previously-connected facility
    from here (no SSH probe), then reuses its endpoint over the Globus web service. Lives in
    ~/.hpc-bridge/facilities.json, 0600 — config only (ssh_host, interface, env_setup, paths, …),
    never a secret (password/keys stay in ~/.ssh). Keyed on ssh_host so it lines up 1:1 with the
    ssh-host-based endpoint name and doesn't sprawl across facility-id choices."""

    def __init__(self, path: Path | str | None = None) -> None:
        default = _state_dir() / "facilities.json"
        self.path = Path(path) if path else default

    def _load(self) -> dict[str, dict]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}

    def _save(self, data: dict[str, dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
        os.chmod(self.path, 0o600)

    def get(self, ssh_host: str) -> dict | None:
        """The proven FacilityDetails (as a dict) last registered for this ssh_host, or None."""
        return self._load().get(ssh_host)

    def put(self, ssh_host: str, details: dict) -> None:
        data = self._load()
        data[ssh_host] = details
        self._save(data)

    def remove(self, ssh_host: str) -> None:
        data = self._load()
        if data.pop(ssh_host, None) is not None:
            self._save(data)


_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


class _Unreadable(Exception):
    """The record exists but cannot be used: `corrupt` (not the JSON we write) or not readable at all (an OSError)."""

    def __init__(self, why: str, *, corrupt: bool) -> None:
        super().__init__(why)
        self.corrupt = corrupt


# How long a store operation waits for the record's lock (tried without blocking, every 50 ms) before failing closed.
LOCK_WAIT_S = 2.0


def _try_lock(fd: int) -> None:
    """Take the lock without waiting; BlockingIOError when another process holds it."""
    if sys.platform == "win32":
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError as exc:  # held elsewhere: msvcrt raises a plain OSError (EACCES / EDEADLOCK)
            raise BlockingIOError(str(exc)) from exc
    else:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(fd: int) -> None:
    if sys.platform == "win32":
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)


class SentCheckStore:
    """The tasks of ours an SSH endpoint's compute user endpoints may still hold (#24's evidence: a check no worker has
    answered, or a command whose result the client lost), by task id, so a server that restarts — Claude Code relaunched
    — still sees them: a new process bound to the same endpoint must not answer a stop `down` while one is queued.
    One small file per endpoint, `~/.hpc-bridge/sent-checks/<endpoint id>.json`, 0600, holding only
    `{task_id, no_id, partition, account, sent_at, what, ceiling_at, block}` rows (`block`: a check's block-request
    number, by which a stop finds its user endpoint on the login node) — never a command's text.

    Several processes can share it (two sessions on one endpoint, a restart): every write re-reads the file and MERGES
    under an exclusive lock on a sibling `.lock` file (`fcntl.flock`; `msvcrt.locking` on Windows), never blocking for
    it (`_locked`: a lock not had within LOCK_WAIT_S fails the operation closed), adding
    the writer's rows and removing only the ones it has SEEN resolve (`update`) — a row it does not know is kept. The
    write itself is atomic (a temp file, then a rename). A record that cannot be read fails CLOSED: `read` reports the
    problem (a check may be queued that nothing here can see), and a corrupt file is kept aside as
    `<id>.json.corrupt-<time>`, still reported, until the endpoint's teardown removes it (`remove`, with a line on
    stderr)."""

    def __init__(self, root: Path | str | None = None) -> None:
        self.root = Path(root) if root else _state_dir() / "sent-checks"

    def _path(self, endpoint_id: str) -> Path | None:
        return self.root / f"{endpoint_id}.json" if _SAFE_ID.fullmatch(endpoint_id or "") else None

    @contextmanager
    def _locked(self, path: Path) -> Iterator[None]:  # (creates the directory: reached only once there is one, or
        # by a write with rows — a read, a removal or an empty write never creates it)
        """An exclusive lock on the sibling `.lock` file — never blocking: tried without waiting (flock LOCK_NB, or
        msvcrt's non-blocking lock on Windows) for up to `LOCK_WAIT_S`, then OSError, which every caller reads as the
        record being unusable (fail closed). A lock service that hangs (an NFS home) cannot freeze the caller; an
        ENOLCK (no locking there at all) fails the same way."""
        self.root.mkdir(parents=True, exist_ok=True)
        fd = os.open(path.with_name(f"{path.name}.lock"), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            give_up = time.monotonic() + LOCK_WAIT_S
            while True:
                try:
                    _try_lock(fd)
                    break
                except BlockingIOError:
                    if time.monotonic() >= give_up:
                        raise OSError("the record's lock is held by another process") from None
                    time.sleep(0.05)
            try:
                yield
            finally:
                _unlock(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _rows(path: Path) -> list[dict]:
        """The rows on disk ([] when there is no file); raises _Unreadable."""
        if not path.exists():
            return []
        try:
            text = path.read_text()
        except OSError as exc:
            raise _Unreadable(f"{type(exc).__name__}: {exc}", corrupt=False) from exc
        try:
            data = json.loads(text)
            rows = data["tasks"]
            if not isinstance(rows, list) or not all(isinstance(r, dict) and isinstance(r.get("task_id"), str)
                                                     for r in rows):
                raise ValueError("unexpected rows")
        except (ValueError, KeyError, TypeError) as exc:
            raise _Unreadable("not the record hpc-bridge writes", corrupt=True) from exc
        return rows

    def _set_aside(self, path: Path) -> Path | None:
        aside = path.with_name(f"{path.name}.corrupt-{int(time.time())}")
        try:
            os.replace(path, aside)
        except OSError:
            return None
        print(f"hpc-bridge: the record of checks sent to an endpoint was unreadable and is kept as {aside}; stops on "
              "that endpoint say so until teardown_endpoint", file=sys.stderr)
        return aside

    def _aside(self, path: Path) -> list[Path]:
        return sorted(path.parent.glob(f"{path.name}.corrupt-*")) if path.parent.exists() else []

    def read(self, endpoint_id: str) -> tuple[list[dict], str | None]:
        """(rows, problem): `problem` says why the record cannot be trusted to list every task — it could not be read,
        or an earlier copy was corrupt and is kept aside — else None."""
        path = self._path(endpoint_id)
        if path is None or not self.root.is_dir():  # nothing was ever written here: read nothing, create nothing
            return [], None
        why: str | None = None
        try:
            with self._locked(path):
                try:
                    rows = self._rows(path)
                except _Unreadable as exc:
                    if exc.corrupt:
                        self._set_aside(path)
                    rows, why = [], str(exc)
                aside = self._aside(path)
        except OSError as exc:  # the lock itself (the directory is not writable): read without it, fail closed
            return [], f"it could not be opened ({type(exc).__name__}: {exc})"
        if why is None and not aside:
            return rows, None
        where = f"; kept as {aside[-1].name}" if aside else ""
        return rows, f"it could not be read ({why or 'an earlier copy was not the record hpc-bridge writes'}{where})"

    def load(self, endpoint_id: str) -> list[dict]:
        return self.read(endpoint_id)[0]

    def update(self, endpoint_id: str, add: list[dict], drop: set[str] | frozenset[str]) -> bool:
        """Merge under the lock: the rows on disk, less `drop` (tasks this writer saw resolve), plus `add` (its own).
        A corrupt record is kept aside first (and keeps being reported). False when nothing could be written."""
        path = self._path(endpoint_id)
        if path is None or (not add and not self.root.is_dir()):  # nothing to write, nothing there: create nothing
            return True
        try:
            with self._locked(path):
                try:
                    current = self._rows(path)
                except _Unreadable as exc:
                    if not exc.corrupt or self._set_aside(path) is None:
                        return False
                    current = []
                merged = {r["task_id"]: r for r in current if r["task_id"] not in drop}
                merged.update({r["task_id"]: r for r in add})
                if not merged:
                    path.unlink(missing_ok=True)
                    return True
                tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(fd, "w") as fh:
                    json.dump({"tasks": list(merged.values())}, fh, indent=1, sort_keys=True)
                os.replace(tmp, path)
                return True
        except OSError:
            return False

    def save(self, endpoint_id: str, rows: list[dict]) -> None:
        """Replace the record with `rows` (a test seam; the server merges, `update`)."""
        path = self._path(endpoint_id)
        if path is not None:
            current = {r["task_id"] for r in self.load(endpoint_id)}
            self.update(endpoint_id, rows, current)

    def remove(self, endpoint_id: str) -> None:
        """The endpoint is gone (its teardown): its record goes, a kept-aside corrupt copy too (said on stderr)."""
        path = self._path(endpoint_id)
        if path is None or not self.root.is_dir():
            return
        try:
            with self._locked(path):
                path.unlink(missing_ok=True)
                for aside in self._aside(path):
                    aside.unlink(missing_ok=True)
                    print(f"hpc-bridge: removed {aside} with its endpoint's teardown", file=sys.stderr)
            path.with_name(f"{path.name}.lock").unlink(missing_ok=True)  # the endpoint is gone: nothing writes it now
        except OSError:
            return

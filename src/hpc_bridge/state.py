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
from dataclasses import asdict, dataclass
from pathlib import Path


def _state_dir() -> Path:
    """Base dir for hpc-bridge's local state (login-node pins, the facility cache, SSH control
    sockets). Defaults to ~/.hpc-bridge; `HPC_BRIDGE_STATE_DIR` relocates it — tests point it at a
    tmp dir so they never read stale entries or WRITE into the developer's real state."""
    return Path(os.environ.get("HPC_BRIDGE_STATE_DIR") or (Path.home() / ".hpc-bridge"))


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


class SentCheckStore:
    """The tasks of ours an SSH endpoint's compute user endpoints may still hold (#24's evidence: a check no worker has
    answered, or a command whose result the client lost), by task id, so a server that restarts — Claude Code relaunched
    — still sees them: a new process bound to the same endpoint must not answer a stop `down` while one is queued.
    One small file per endpoint, `~/.hpc-bridge/sent-checks/<endpoint id>.json`, 0600, written atomically (a temp file,
    then a rename), holding only `{task_id, partition, account, sent_at, what}` rows — never a command's text. The
    rows still unresolved are rewritten on each change (warmth._persist_checks); a teardown removes the file.
    Best-effort: an unreadable or unwritable file reads as empty and never fails a tool call."""

    def __init__(self, root: Path | str | None = None) -> None:
        self.root = Path(root) if root else _state_dir() / "sent-checks"

    def _path(self, endpoint_id: str) -> Path | None:
        return self.root / f"{endpoint_id}.json" if _SAFE_ID.fullmatch(endpoint_id or "") else None

    def load(self, endpoint_id: str) -> list[dict]:
        path = self._path(endpoint_id)
        if path is None or not path.exists():
            return []
        try:
            rows = json.loads(path.read_text()).get("tasks", [])
        except (json.JSONDecodeError, OSError, AttributeError):
            return []
        return [r for r in rows if isinstance(r, dict) and isinstance(r.get("task_id"), str)]

    def save(self, endpoint_id: str, rows: list[dict]) -> None:
        path = self._path(endpoint_id)
        if path is None:
            return
        if not rows:
            self.remove(endpoint_id)
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as fh:
                json.dump({"tasks": rows}, fh, indent=1, sort_keys=True)
            os.replace(tmp, path)
        except OSError:
            return

    def remove(self, endpoint_id: str) -> None:
        path = self._path(endpoint_id)
        if path is None:
            return
        try:
            path.unlink(missing_ok=True)
        except OSError:
            return

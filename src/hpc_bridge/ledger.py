"""The durable batch ledger (batch phase 1; vault `Planned/Batch and swarm execution.md` §4).

SQLite at `<state_dir>/ledger.sqlite3`: mode 0600, WAL, a busy timeout. It sits beside `SentCheckStore` (state.py),
whose rows the proven stop logic (#24, #204, #208) reads; this store holds what a batch needs to survive the server:
its rows and command (a replay record must — unlike sent-checks, which never hold a command's text), each task's
attempt, Globus task id and settlement, the lease that makes one process the batch's only admitter and collector, and
the configs a stop or a reap retired.

- **Local filesystems only.** WAL does not work on a network filesystem, and the state dir is on NFS / Lustre / GPFS
  on login nodes and beside loam: `open` refuses one by its type (`fs_type`), naming HPC_BRIDGE_STATE_DIR.
- **One admission point.** `reserve` checks the lease holder, the retired configs and the window in ONE
  `BEGIN IMMEDIATE` transaction, and moves the rows it takes to `in_flight` with their next attempt.
- **One owner per batch.** A lease row (host, pid, heartbeat). A takeover needs a stale heartbeat AND a holder pid
  that is dead on THIS host (an orphaned server keeps its pid: `harness:server_orphaned`); a holder on another host
  is never taken over.
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .state import _state_dir

SCHEMA_VERSION = 1
LEASE_STALE_S = float(os.environ.get("HPC_BRIDGE_LEASE_STALE_S", "") or 30.0)
BUSY_TIMEOUT_MS = 10_000

# Network and shared filesystems SQLite's WAL cannot run on (its shared-memory index needs one host's mmap and POSIX
# locks): Linux's /proc/mounts names and macOS's mount(8) names. Any fuse filesystem counts (sshfs, s3fs, gcsfuse…).
NETWORK_FS = frozenset({"nfs", "nfs4", "lustre", "gpfs", "cifs", "smb", "smb2", "smb3", "smbfs", "afpfs", "webdav",
                        "ceph", "glusterfs", "beegfs", "panfs", "9p", "osxfuse", "macfuse", "afs", "ocfs2", "gfs2"})

# A task's states. `in_flight`, `submit_unknown` and `presumed_lost` hold a window slot.
HOLDING = ("in_flight", "submit_unknown", "presumed_lost")
TERMINAL = ("done", "failed", "lost", "cancelled")
STATES = ("backlog", *HOLDING, *TERMINAL)


class LedgerError(RuntimeError):
    """The ledger cannot be used here."""


class NetworkFilesystemError(LedgerError):
    """The state dir is on a network filesystem."""


class NotLeaseHolder(LedgerError):
    """This process does not hold the batch's lease: it may read the batch, never admit or collect for it."""


class RetiredConfig(LedgerError):
    """The config this admission would submit with was retired (a stop or a reap released its block)."""


class UnknownBatch(LedgerError):
    """No batch of that id in the ledger."""


@dataclass(frozen=True)
class Owner:
    host: str
    pid: int

    @classmethod
    def here(cls) -> Owner:
        return cls(socket.gethostname(), os.getpid())

    def __str__(self) -> str:
        return f"pid {self.pid} on {self.host}"


@dataclass(frozen=True)
class Lease:
    ok: bool
    holder: Owner
    heartbeat: float


@dataclass(frozen=True)
class TaskRow:
    batch_id: str
    index: int
    row: Any
    attempt: int
    state: str
    task_id: str | None = None


def fs_type_from_mounts(path: str, mounts: str) -> str | None:
    """The filesystem type of `path` from a /proc/mounts-shaped table (`dev mountpoint type opts …`): the mount point
    that is the longest prefix of the path."""
    best, kind = "", None
    p = os.path.normpath(path)
    for line in mounts.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        mnt = parts[1].replace("\\040", " ")
        if (p == mnt or p.startswith(mnt.rstrip("/") + "/") or mnt == "/") and len(mnt) >= len(best):
            best, kind = mnt, parts[2]
    return kind


def _darwin_mounts() -> str:
    """macOS's mount(8) (`dev on /path (type, opts)`) as a /proc/mounts table."""
    try:
        out = subprocess.run(["/sbin/mount"], capture_output=True, text=True, timeout=10,
                             stdin=subprocess.DEVNULL).stdout
    except (OSError, subprocess.SubprocessError):
        return ""
    rows = []
    for line in out.splitlines():
        if " on " not in line or " (" not in line:
            continue
        dev, rest = line.split(" on ", 1)
        mnt, _, opts = rest.rpartition(" (")
        rows.append(f"{dev} {mnt.replace(' ', chr(92) + '040')} {opts.split(',')[0].strip(' )')} rw 0 0")
    return "\n".join(rows)


def fs_type(path: Path | str) -> str | None:
    """The type of the filesystem `path` lives on (the statfs seam), or None when it cannot be told."""
    real = os.path.realpath(str(path))
    try:
        if os.path.exists("/proc/self/mounts"):
            return fs_type_from_mounts(real, Path("/proc/self/mounts").read_text())
        if sys.platform == "darwin":
            return fs_type_from_mounts(real, _darwin_mounts())
    except OSError:
        return None
    return None


def _network(kind: str | None) -> bool:
    k = (kind or "").lower()
    return k in NETWORK_FS or k.startswith(("fuse", "nfs"))


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


_SCHEMA = """
create table if not exists meta (key text primary key, value text);
create table if not exists batch (
  id text primary key,
  created_at real not null,
  facility text,
  endpoint_id text,
  session_id text,
  shape text default 'compute',
  uec_hash text,
  command text not null,
  script_hash text,
  task_walltime_s real not null,
  results_dir text,
  idempotent integer default 0,
  chunk integer default 1,
  capacity integer default 1,
  state text not null,
  pause_reason text,
  task_group_id text,
  blocks_seen text default '[]',
  owner_host text,
  owner_pid integer,
  heartbeat real,
  cancel_requested text
);
create table if not exists task (
  batch_id text not null,
  idx integer not null,
  row_json text not null,
  attempt integer default 0,
  state text not null,
  task_id text,
  admitted_at real,
  started_at real,
  settled_at real,
  rc integer,
  tail text,
  stderr_tail text,
  stdout_path text,
  stdout_bytes integer,
  truncated integer default 0,
  job_id text,
  host text,
  workers integer,
  remaining_s real,
  settled_by text,
  evidence text,
  primary key (batch_id, idx)
);
create index if not exists task_by_tid on task (task_id);
create table if not exists retired (
  endpoint_id text not null,
  uec_hash text not null,
  why text,
  at real,
  primary key (endpoint_id, uec_hash)
);
create table if not exists itask (
  task_id text primary key,
  endpoint_id text,
  shape text,
  session_id text,
  result_key text,
  state_dir text,
  handle text,
  sent_at real
);
create table if not exists event (
  seq integer primary key autoincrement,
  at real not null,
  batch_id text,
  kind text not null,
  data text
);
"""

_TASK_FIELDS = ("rc", "tail", "stderr_tail", "stdout_path", "stdout_bytes", "truncated", "job_id", "host", "workers",
                "remaining_s", "settled_by", "evidence", "started_at", "task_id")


class Ledger:
    """One connection to the ledger file. Every method is its own transaction; `reserve` is BEGIN IMMEDIATE."""

    def __init__(self, path: Path, con: sqlite3.Connection) -> None:
        self.path = path
        self._con = con
        self.lease_stale_s = LEASE_STALE_S
        self.pid_alive = _pid_alive          # a seam: the tests decide which pids are alive
        self.host = socket.gethostname()

    # -- opening ---------------------------------------------------------------------------------------------------
    @classmethod
    def open(cls, state_dir: Path | str | None = None) -> Ledger:
        base = Path(state_dir) if state_dir is not None else _state_dir()
        base.mkdir(parents=True, exist_ok=True, mode=0o700)
        kind = fs_type(base)
        if _network(kind):
            raise NetworkFilesystemError(
                f"hpc-bridge's batch ledger cannot live on {base} ({kind}, a network filesystem: SQLite's WAL needs "
                "a local disk). Point HPC_BRIDGE_STATE_DIR at a local directory (e.g. /tmp/<you>/hpc-bridge or a "
                "node-local disk) and start the server again.")
        path = base / "ledger.sqlite3"
        if not path.exists():
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
            os.close(fd)
        os.chmod(path, 0o600)
        con = sqlite3.connect(str(path), timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None,
                              check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute(f"pragma busy_timeout = {BUSY_TIMEOUT_MS}")
        con.execute("pragma journal_mode = wal")
        con.execute("pragma synchronous = normal")
        con.executescript(_SCHEMA)
        con.execute("insert or ignore into meta values ('schema_version', ?)", (str(SCHEMA_VERSION),))
        return cls(path, con)

    def close(self) -> None:
        try:
            self._con.close()
        except sqlite3.Error:
            pass

    def journal_mode(self) -> str:
        return str(self._con.execute("pragma journal_mode").fetchone()[0]).lower()

    def _tx(self, immediate: bool = False):
        return _Tx(self._con, immediate)

    # -- batches ---------------------------------------------------------------------------------------------------
    def add_batch(self, batch_id: str, *, command: str, rows: list[Any], task_walltime_s: float, capacity: int,
                  results_dir: str, idempotent: bool, endpoint_id: str | None, uec_hash: str | None,
                  session_id: str, owner: Owner, facility: str | None = None, script_hash: str | None = None,
                  state: str = "admitting", now: float | None = None) -> str:
        at = time.time() if now is None else now
        with self._tx(immediate=True) as c:
            c.execute("insert into batch (id, created_at, facility, endpoint_id, session_id, uec_hash, command, "
                      "script_hash, task_walltime_s, results_dir, idempotent, capacity, state, owner_host, owner_pid, "
                      "heartbeat) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (batch_id, at, facility, endpoint_id, session_id, uec_hash, command, script_hash,
                       float(task_walltime_s), results_dir, int(bool(idempotent)), int(capacity), state, owner.host,
                       owner.pid, at))
            c.executemany("insert into task (batch_id, idx, row_json, state) values (?,?,?,'backlog')",
                          [(batch_id, i, json.dumps(r)) for i, r in enumerate(rows)])
            c.execute("insert into event (at, batch_id, kind, data) values (?,?,?,?)",
                      (at, batch_id, "batch.add", json.dumps({"rows": len(rows), "owner": str(owner)})))
        return batch_id

    def batch(self, batch_id: str) -> dict[str, Any]:
        r = self._con.execute("select * from batch where id = ?", (batch_id,)).fetchone()
        if r is None:
            raise UnknownBatch(batch_id)
        return dict(r)

    def has_batch(self, batch_id: str) -> bool:
        return self._con.execute("select 1 from batch where id = ?", (batch_id,)).fetchone() is not None

    def batches(self, *, endpoint_id: str | None = None, states: Iterable[str] | None = None) -> list[dict[str, Any]]:
        q, args = "select * from batch where 1=1", []
        if endpoint_id is not None:
            q += " and endpoint_id = ?"
            args.append(endpoint_id)
        if states is not None:
            st = list(states)
            q += f" and state in ({','.join('?' * len(st))})"
            args += st
        return [dict(r) for r in self._con.execute(q + " order by created_at", args)]

    def update_batch(self, batch_id: str, **fields: Any) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self._tx() as c:
            c.execute(f"update batch set {cols} where id = ?", (*fields.values(), batch_id))

    def add_block_seen(self, batch_id: str, job: str) -> None:
        with self._tx(immediate=True) as c:
            r = c.execute("select blocks_seen from batch where id = ?", (batch_id,)).fetchone()
            seen = json.loads((r[0] if r else None) or "[]")
            if job not in seen:
                seen.append(job)
                c.execute("update batch set blocks_seen = ? where id = ?", (json.dumps(seen), batch_id))

    # -- the lease -------------------------------------------------------------------------------------------------
    def holder(self, batch_id: str) -> Lease:
        b = self.batch(batch_id)
        return Lease(True, Owner(str(b["owner_host"] or ""), int(b["owner_pid"] or 0)), float(b["heartbeat"] or 0.0))

    def take_lease(self, batch_id: str, owner: Owner, *, now: float | None = None) -> Lease:
        """Take or refresh the batch's lease: refreshed by its holder; taken over only when the holder's heartbeat is
        stale AND its pid is dead on this host."""
        at = time.time() if now is None else now
        with self._tx(immediate=True) as c:
            r = c.execute("select owner_host, owner_pid, heartbeat from batch where id = ?", (batch_id,)).fetchone()
            if r is None:
                raise UnknownBatch(batch_id)
            held = Owner(str(r["owner_host"] or ""), int(r["owner_pid"] or 0))
            beat = float(r["heartbeat"] or 0.0)
            if held == owner:
                c.execute("update batch set heartbeat = ? where id = ?", (at, batch_id))
                return Lease(True, owner, at)
            stale = at - beat > self.lease_stale_s
            dead = held.host == owner.host and not self.pid_alive(held.pid)
            if not (stale and dead):
                return Lease(False, held, beat)
            c.execute("update batch set owner_host = ?, owner_pid = ?, heartbeat = ? where id = ?",
                      (owner.host, owner.pid, at, batch_id))
            c.execute("insert into event (at, batch_id, kind, data) values (?,?,?,?)",
                      (at, batch_id, "batch.lease", json.dumps({"from": str(held), "to": str(owner)})))
            return Lease(True, owner, at)

    def heartbeat(self, batch_id: str, owner: Owner) -> bool:
        with self._tx() as c:
            cur = c.execute("update batch set heartbeat = ? where id = ? and owner_host = ? and owner_pid = ?",
                            (time.time(), batch_id, owner.host, owner.pid))
            return cur.rowcount == 1

    # -- retired configs -------------------------------------------------------------------------------------------
    def retire(self, endpoint_id: str, uec_hash: str, *, why: str, now: float | None = None) -> None:
        with self._tx() as c:
            c.execute("insert or replace into retired values (?,?,?,?)",
                      (endpoint_id, uec_hash, why, time.time() if now is None else now))

    def unretire(self, endpoint_id: str, uec_hash: str) -> None:
        with self._tx() as c:
            c.execute("delete from retired where endpoint_id = ? and uec_hash = ?", (endpoint_id, uec_hash))

    def retired(self, endpoint_id: str, uec_hash: str) -> str | None:
        r = self._con.execute("select why from retired where endpoint_id = ? and uec_hash = ?",
                              (endpoint_id, uec_hash)).fetchone()
        return None if r is None else str(r[0] or "retired")

    # -- admission -------------------------------------------------------------------------------------------------
    def reserve(self, batch_id: str, owner: Owner, n: int, *, uec_hash: str, now: float | None = None,
                indices: Iterable[int] | None = None) -> list[TaskRow]:
        """THE admission point: in one BEGIN IMMEDIATE transaction, check that `owner` holds the lease, that the config
        is not retired and how many window slots are free, and move up to `n` backlog rows (the lowest indices, or
        `indices` first) to `in_flight` with their next attempt."""
        at = time.time() if now is None else now
        with self._tx(immediate=True) as c:
            b = c.execute("select * from batch where id = ?", (batch_id,)).fetchone()
            if b is None:
                raise UnknownBatch(batch_id)
            if (str(b["owner_host"] or ""), int(b["owner_pid"] or 0)) != (owner.host, owner.pid):
                raise NotLeaseHolder(f"{batch_id} is held by pid {b['owner_pid']} on {b['owner_host']}")
            why = c.execute("select why from retired where endpoint_id = ? and uec_hash = ?",
                            (b["endpoint_id"], uec_hash)).fetchone()
            if why is not None:
                raise RetiredConfig(str(why[0] or "this block's config was retired"))
            held = c.execute(f"select count(*) from task where batch_id = ? and state in ({','.join('?' * 3)})",
                             (batch_id, *HOLDING)).fetchone()[0]
            free = max(0, int(b["capacity"] or 1) - int(held))
            take = min(max(0, n), free)
            if take <= 0:
                return []
            want = list(indices or [])
            rows = []
            if want:
                q = ",".join("?" * len(want))
                rows += list(c.execute(f"select * from task where batch_id = ? and state = 'backlog' and idx in ({q}) "
                                       "order by idx", (batch_id, *want)))
            if len(rows) < take:
                have = {r["idx"] for r in rows}
                rows += [r for r in c.execute("select * from task where batch_id = ? and state = 'backlog' "
                                              "order by idx limit ?", (batch_id, take + len(have)))
                         if r["idx"] not in have]
            rows = rows[:take]
            out = []
            for r in rows:
                att = int(r["attempt"] or 0) + 1
                c.execute("update task set state = 'in_flight', attempt = ?, admitted_at = ?, task_id = null, "
                          "started_at = null where batch_id = ? and idx = ?", (att, at, batch_id, r["idx"]))
                out.append(TaskRow(batch_id, int(r["idx"]), json.loads(r["row_json"]), att, "in_flight"))
            return out

    # -- tasks -----------------------------------------------------------------------------------------------------
    def set_task_id(self, batch_id: str, index: int, attempt: int, task_id: str) -> None:
        with self._tx() as c:
            c.execute("update task set task_id = ? where batch_id = ? and idx = ? and attempt = ?",
                      (task_id, batch_id, index, attempt))

    def settle(self, batch_id: str, index: int, attempt: int, state: str, *, now: float | None = None,
               only_from: Iterable[str] | None = None, **fields: Any) -> bool:
        """Move one attempt's row to `state` (a terminal state, `backlog` for a retry or a row that never ran,
        `submit_unknown`, `presumed_lost`), with its result fields. Only the row's CURRENT attempt is moved, and, with
        `only_from`, only from those states. Returns whether it moved."""
        if state not in STATES:
            raise ValueError(f"unknown task state {state!r}")
        bad = set(fields) - set(_TASK_FIELDS)
        if bad:
            raise ValueError(f"unknown task fields {sorted(bad)}")
        cols = {"state": state, **fields}
        if state in TERMINAL:
            cols["settled_at"] = time.time() if now is None else now
        sets = ", ".join(f"{k} = ?" for k in cols)
        q = f"update task set {sets} where batch_id = ? and idx = ? and attempt = ?"
        args: list[Any] = [*cols.values(), batch_id, index, attempt]
        if only_from is not None:
            st = list(only_from)
            q += f" and state in ({','.join('?' * len(st))})"
            args += st
        with self._tx(immediate=True) as c:
            return c.execute(q, args).rowcount == 1

    def task(self, batch_id: str, index: int) -> dict[str, Any] | None:
        r = self._con.execute("select * from task where batch_id = ? and idx = ?", (batch_id, index)).fetchone()
        return None if r is None else dict(r)

    def task_by_tid(self, task_id: str) -> dict[str, Any] | None:
        r = self._con.execute("select * from task where task_id = ?", (task_id,)).fetchone()
        return None if r is None else dict(r)

    def tasks(self, batch_id: str, *, states: Iterable[str] | None = None, offset: int = 0,
              limit: int | None = None) -> list[dict[str, Any]]:
        q, args = "select * from task where batch_id = ?", list[Any]([batch_id])
        if states is not None:
            st = list(states)
            q += f" and state in ({','.join('?' * len(st))})"
            args += st
        q += " order by idx"
        if limit is not None:
            q += " limit ? offset ?"
            args += [int(limit), int(offset)]
        return [dict(r) for r in self._con.execute(q, args)]

    def counts(self, batch_id: str) -> dict[str, int]:
        out = {s: 0 for s in STATES}
        for r in self._con.execute("select state, count(*) from task where batch_id = ? group by state", (batch_id,)):
            out[str(r[0])] = int(r[1])
        return out

    def cancel_backlog(self, batch_id: str, *, now: float | None = None) -> int:
        at = time.time() if now is None else now
        with self._tx(immediate=True) as c:
            return c.execute("update task set state = 'cancelled', settled_at = ?, settled_by = 'cancel' "
                             "where batch_id = ? and state = 'backlog'", (at, batch_id)).rowcount

    # -- interactive tasks (poll_task by Globus task id) ------------------------------------------------------------
    def record_itask(self, task_id: str, *, endpoint_id: str | None, shape: str, session_id: str,
                     result_key: str | None, state_dir: str | None, handle: str | None) -> None:
        with self._tx() as c:
            c.execute("insert or replace into itask values (?,?,?,?,?,?,?,?)",
                      (task_id, endpoint_id, shape, session_id, result_key, state_dir, handle, time.time()))

    def itask(self, task_id: str) -> dict[str, Any] | None:
        r = self._con.execute("select * from itask where task_id = ?", (task_id,)).fetchone()
        return None if r is None else dict(r)

    # -- events ----------------------------------------------------------------------------------------------------
    def event(self, batch_id: str | None, kind: str, data: dict[str, Any], *, now: float | None = None) -> None:
        with self._tx() as c:
            c.execute("insert into event (at, batch_id, kind, data) values (?,?,?,?)",
                      (time.time() if now is None else now, batch_id, kind, json.dumps(data, default=str)))


class _Tx:
    """A transaction on an autocommit connection: BEGIN (IMMEDIATE when asked) … COMMIT, ROLLBACK on error."""

    def __init__(self, con: sqlite3.Connection, immediate: bool) -> None:
        self._con, self._immediate = con, immediate

    def __enter__(self) -> sqlite3.Connection:
        self._con.execute("begin immediate" if self._immediate else "begin")
        return self._con

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._con.execute("rollback" if exc_type is not None else "commit")

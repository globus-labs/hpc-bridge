"""The durable batch ledger (batch phase 1; vault `Planned/Batch and swarm execution.md` §4).

SQLite at `<state_dir>/ledger.sqlite3`: mode 0600, WAL, a busy timeout. It sits beside `SentCheckStore` (state.py),
whose rows the proven stop logic (#24, #204, #208) reads; this store holds what a batch needs to survive the server:
its rows and command (a replay record must — unlike sent-checks, which never hold a command's text), each task's
attempt, Globus task id and settlement, the lease that makes one process the batch's only admitter and collector, and
the configs a stop or a reap retired.

- **Local filesystems only.** WAL does not work on a network filesystem, and the state dir is on NFS / Lustre / GPFS
  on login nodes and beside an orchestrator: `open` refuses one by its type (`fs_type`), naming HPC_BRIDGE_STATE_DIR.
- **One admission point.** `reserve` checks the lease holder, the retired configs and the window in ONE
  `BEGIN IMMEDIATE` transaction, and moves the rows it takes to `in_flight` with their next attempt.
- **One owner per batch.** A lease row (host, pid, the process's start time, heartbeat). A takeover needs a stale
  heartbeat AND a holder that is gone: its pid dead, or alive but started at another time (the pid was handed on). An
  orphaned server keeps its pid and its start time (`harness:server_orphaned`), so it is never taken over. The ledger is
  on a local disk, so its holders are this machine's processes: the hostname is not trusted for it (macOS renames
  itself on a new network; a container's name is its own).
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
from dataclasses import dataclass, field
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


class EnvelopeError(LedgerError):
    """An envelope cannot admit this job (the reason is the text)."""


class EnvelopeExhausted(EnvelopeError):
    """spent + committed + this job's worst case would pass the cap. `held`: what the envelope holds now; `need`: the
    job's worst case; `cap`."""

    def __init__(self, msg: str, *, held: float = 0.0, need: float = 0.0, cap: float = 0.0) -> None:
        super().__init__(msg)
        self.held, self.need, self.cap = held, need, cap


class EnvelopeNotOwned(EnvelopeError):
    """Another session approved the envelope: this one asks again before admitting anything (rule 3)."""


class EnvelopeClosed(EnvelopeError):
    """The envelope admits nothing (drained, stopped, closed, or past its deadline)."""


class EnvelopeLimit(EnvelopeError):
    """The job passes one of the envelope's other dimensions (max_jobs, max_nodes_per_block, accounts, partitions)."""


class EnvelopeCeiling(EnvelopeError):
    """HPC_BRIDGE_ENVELOPE_MAX: the sum of the open envelopes' caps on a facility and account would pass the ceiling.
    `open_caps`: what is open already on the account it binds; `ceiling`; `account`."""

    def __init__(self, msg: str, *, open_caps: float, ceiling: float, account: str | None) -> None:
        super().__init__(msg)
        self.open_caps, self.ceiling, self.account = open_caps, ceiling, account


def envelope_accounts(e: dict[str, Any]) -> list[str]:
    """Every account an envelope admits: its `accounts`, else its own account ("" for none)."""
    got = list(json.loads(e.get("accounts") or "[]"))
    return got or [e.get("account") or ""]


def _ceiling_check(c: sqlite3.Connection, *, facility: str | None, accounts: Iterable[str], cap: float,
                   ceiling: float | None, except_id: str | None = None) -> None:
    """Inside the caller's transaction: the new cap plus the caps of every other envelope still open on the facility
    that admits one of `accounts` stays within the ceiling (review B7: every account an envelope admits counts)."""
    if ceiling is None:
        return
    rows = [dict(r) for r in c.execute("select * from envelope where facility is ? and state in "
                                       "('open','exhausted','draining')", (facility,))]
    for acct in accounts:
        open_caps = sum(float(e["cap"]) for e in rows if e["id"] != except_id and acct in envelope_accounts(e))
        if open_caps + float(cap) > float(ceiling) + 1e-9:
            raise EnvelopeCeiling(f"{open_caps:g} is open on account {acct or '(none)'} of a ceiling of {ceiling:g}",
                                  open_caps=open_caps, ceiling=float(ceiling), account=acct or None)


def job_hold(j: dict[str, Any]) -> float:
    """What one job holds against its envelope: nothing once refused; its worst case until the meter has read it; then
    what it charged plus what it can still bill."""
    if j.get("state") == "refused":
        return 0.0
    if (j.get("meter_source") or "none") == "none":
        return float(j.get("worst_case") or 0.0)
    return float(j.get("charged") or 0.0) + float(j.get("committed") or 0.0)


def proc_start(pid: int) -> str | None:
    """When process `pid` started, as the OS says (Linux: /proc/<pid>/stat's starttime; elsewhere `ps -o lstart=`), or
    None when it cannot be told (no such process). Two processes that share a pid over time differ here."""
    try:
        stat = Path(f"/proc/{int(pid)}/stat").read_text()
        return "linux:" + stat.rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError, ValueError):
        pass
    try:   # the C locale and UTC: every reader renders the same start the same way, whatever its own TZ or LC_*
        env = {**os.environ, "LC_ALL": "C", "LANG": "C", "TZ": "UTC0"}
        out = subprocess.run(["ps", "-o", "lstart=", "-p", str(int(pid))], capture_output=True, text=True, timeout=5,
                             stdin=subprocess.DEVNULL, env=env).stdout.strip()
    except (OSError, subprocess.SubprocessError, ValueError):
        return None
    return ("psutc:" + " ".join(out.split())) if out else None


@dataclass(frozen=True)
class Owner:
    host: str
    pid: int
    started: str | None = field(default=None, compare=False)   # the process's start time (`proc_start`)

    @classmethod
    def here(cls) -> Owner:
        return cls(socket.gethostname(), os.getpid(), proc_start(os.getpid()))

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
  owner_start text,
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
  sent_job text,
  sent_uec text,
  unknown_since real,
  unheld text,
  primary key (batch_id, idx)
);
create index if not exists task_by_tid on task (task_id);
create table if not exists tgroup (
  batch_id text not null,
  group_id text not null,
  at real,
  primary key (batch_id, group_id)
);
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
create table if not exists envelope (
  id text primary key,
  created_at real not null,
  facility text,
  endpoint_id text,
  account text,
  unit text,
  cap real not null,
  max_jobs integer,
  max_nodes_per_block integer,
  accounts text default '[]',
  partitions text default '[]',
  deadline real not null,
  at_deadline text default 'drain',
  site_backstop text,
  approved_by text,
  approved_at real,
  owner_host text,
  owner_pid integer,
  owner_start text,
  state text not null,
  closed_at real,
  close_reason text,
  spent real default 0,
  committed real default 0,
  unledgered text default '[]',
  unledgered_hold real default 0,
  excluded text default '[]',
  metered_at real
);
create table if not exists job (
  id text primary key,
  envelope_id text not null,
  created_at real not null,
  source text not null,
  native_id text,
  scheduler text,
  session_id text,
  approved_by text,
  partition text,
  account text,
  nodes integer,
  cores integer,
  memory_gb real,
  gpus integer,
  walltime_s real,
  tasks integer default 1,
  array_spec text,
  throttle integer,
  depends_on text default '[]',
  rate real,
  min_charge_h real,
  worst_case real not null,
  script_path text,
  script_hash text,
  deadline real,
  state text not null,
  sched_state text,
  exit_code integer,
  reason text,
  charged real default 0,
  committed real default 0,
  meter_source text default 'none',
  array_counts text default '{}',
  metered_at real,
  ended_at real,
  cancel_requested integer default 0,
  unheld integer default 0,
  hand text
);
create index if not exists job_by_native on job (native_id);
create table if not exists hand (
  token text primary key,
  envelope_id text not null,
  facility text,
  session text,
  mark_path text,
  shim_dir text,
  before text default '[]',
  handle text,
  created_at real not null,
  state text not null,
  note text
);
create table if not exists meter_reading (
  seq integer primary key autoincrement,
  at real not null,
  envelope_id text not null,
  source text,
  spent real,
  committed real,
  unledgered text default '[]',
  detail text
);
"""

# A Jobs-mode job's states (batch phase 2a). Admission holds a reservation from `reserved` on; a job the scheduler can
# still bill is LIVE (a reserved row has not reached the scheduler yet but may at any moment; a held hand submit is ours
# until released or cancelled).
JOB_LIVE = ("reserved", "submit_unknown", "held", "queued", "running")
JOB_ENDED = ("done", "failed", "cancelled", "refused")
ENVELOPE_ADMITTING = ("open", "exhausted")

_TASK_FIELDS = ("rc", "tail", "stderr_tail", "stdout_path", "stdout_bytes", "truncated", "job_id", "host", "workers",
                "remaining_s", "settled_by", "evidence", "started_at", "task_id", "sent_job", "sent_uec",
                "unknown_since", "unheld")
# Columns a ledger written by an earlier build may lack: added on open.
_ADDED_COLUMNS = {"batch": [("owner_start", "text")],
                  "task": [("sent_job", "text"), ("sent_uec", "text"), ("unknown_since", "real"), ("unheld", "text")],
                  "job": [("cancel_requested", "integer default 0"), ("unheld", "integer default 0"),
                          ("hand", "text")]}


class Ledger:
    """One connection to the ledger file. Every method is its own transaction; `reserve` is BEGIN IMMEDIATE."""

    def __init__(self, path: Path, con: sqlite3.Connection) -> None:
        self.path = path
        self._con = con
        self.lease_stale_s = LEASE_STALE_S
        self.pid_alive = _pid_alive          # a seam: the tests decide which pids are alive
        self.proc_start = proc_start         # a seam: when a pid's process started
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
        for table, cols in _ADDED_COLUMNS.items():
            have = {str(r[1]) for r in con.execute(f"pragma table_info({table})")}
            for name, kind in cols:
                if name not in have:
                    con.execute(f"alter table {table} add column {name} {kind}")
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
                      "owner_start, heartbeat) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (batch_id, at, facility, endpoint_id, session_id, uec_hash, command, script_hash,
                       float(task_walltime_s), results_dir, int(bool(idempotent)), int(capacity), state, owner.host,
                       owner.pid, owner.started, at))
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
        return Lease(True, Owner(str(b["owner_host"] or ""), int(b["owner_pid"] or 0), b.get("owner_start")),
                     float(b["heartbeat"] or 0.0))

    def holder_alive(self, holder: Owner) -> bool:
        """Whether the process that holds a lease still runs: its pid is alive and, when its start time is on the
        record, it is the same process (a pid handed on to another process is not the holder). The hostname is not
        consulted: the ledger is on this machine's local disk."""
        if not self.pid_alive(holder.pid):
            return False
        if holder.started:
            now = self.proc_start(holder.pid)
            # compared only in one form (/proc's ticks, or ps in C/UTC): a start recorded another way, or unreadable
            # now, is no evidence — a live pid's lease is never taken on it
            if now is not None and now.split(":", 1)[0] == holder.started.split(":", 1)[0] and now != holder.started:
                return False
        return True

    def take_lease(self, batch_id: str, owner: Owner, *, now: float | None = None) -> Lease:
        """Take or refresh the batch's lease: refreshed by its holder; taken over only when the holder's heartbeat is
        stale AND the holder is gone (`holder_alive`)."""
        at = time.time() if now is None else now
        with self._tx(immediate=True) as c:
            r = c.execute("select owner_host, owner_pid, owner_start, heartbeat from batch where id = ?",
                          (batch_id,)).fetchone()
            if r is None:
                raise UnknownBatch(batch_id)
            held = Owner(str(r["owner_host"] or ""), int(r["owner_pid"] or 0), r["owner_start"])
            beat = float(r["heartbeat"] or 0.0)
            if held == owner:
                c.execute("update batch set heartbeat = ? where id = ?", (at, batch_id))
                return Lease(True, owner, at)
            stale = at - beat > self.lease_stale_s
            if not (stale and not self.holder_alive(held)):
                return Lease(False, held, beat)
            c.execute("update batch set owner_host = ?, owner_pid = ?, owner_start = ?, heartbeat = ? where id = ?",
                      (owner.host, owner.pid, owner.started, at, batch_id))
            c.execute("insert into event (at, batch_id, kind, data) values (?,?,?,?)",
                      (at, batch_id, "batch.lease", json.dumps({"from": str(held), "to": str(owner)})))
            return Lease(True, owner, at)

    def heartbeat(self, batch_id: str, owner: Owner) -> bool:
        with self._tx() as c:
            cur = c.execute("update batch set heartbeat = ? where id = ? and owner_host = ? and owner_pid = ?",
                            (time.time(), batch_id, owner.host, owner.pid))
            return cur.rowcount == 1

    # -- the task groups a batch's runners were minted ------------------------------------------------------------
    def add_task_group(self, batch_id: str, group_id: str, *, now: float | None = None) -> None:
        """Record a task group the service minted for one of the batch's runners (every one: a new confirmation's runner
        gets its own, and reconciliation reads them all)."""
        with self._tx() as c:
            c.execute("insert or ignore into tgroup values (?,?,?)",
                      (batch_id, str(group_id), time.time() if now is None else now))
            c.execute("update batch set task_group_id = ? where id = ?", (str(group_id), batch_id))

    def task_groups(self, batch_id: str) -> list[str]:
        rows = self._con.execute("select group_id from tgroup where batch_id = ? order by at", (batch_id,)).fetchall()
        out = [str(r[0]) for r in rows]
        legacy = self._con.execute("select task_group_id from batch where id = ?", (batch_id,)).fetchone()
        if legacy is not None and legacy[0] and str(legacy[0]) not in out:
            out.append(str(legacy[0]))
        return out

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

    def unknown_holding(self, batch_id: str) -> list[dict[str, Any]]:
        """Rows a submit of unknown fate left that still hold the stop: a copy may be queued at the service
        (`unknown_since` set), and no evidence says it can no longer run (`unheld` unset)."""
        return [dict(r) for r in self._con.execute(
            "select * from task where batch_id = ? and unknown_since is not null and unheld is null order by idx",
            (batch_id,))]

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

    # -- envelopes (batch phase 2a) --------------------------------------------------------------------------------
    def add_envelope(self, envelope_id: str, *, facility: str | None, account: str | None, unit: str, cap: float,
                     deadline: float, approved_by: str, owner: Owner, endpoint_id: str | None = None,
                     max_jobs: int | None = None, max_nodes_per_block: int | None = None,
                     accounts: Iterable[str] | None = None, partitions: Iterable[str] | None = None,
                     at_deadline: str = "drain", site_backstop: str | None = None,
                     ceiling: float | None = None, now: float | None = None) -> str:
        """Open an envelope. With `ceiling` (HPC_BRIDGE_ENVELOPE_MAX) the sum of the open caps is checked in the SAME
        BEGIN IMMEDIATE transaction as the insert, on every account it admits (`EnvelopeCeiling`)."""
        at = time.time() if now is None else now
        with self._tx(immediate=True) as c:
            _ceiling_check(c, facility=facility, accounts=list(accounts or []) or [account or ""], cap=cap,
                           ceiling=ceiling)
            c.execute("insert into envelope (id, created_at, facility, endpoint_id, account, unit, cap, max_jobs, "
                      "max_nodes_per_block, accounts, partitions, deadline, at_deadline, site_backstop, approved_by, "
                      "approved_at, owner_host, owner_pid, owner_start, state) "
                      "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'open')",
                      (envelope_id, at, facility, endpoint_id, account, unit, float(cap), max_jobs, max_nodes_per_block,
                       json.dumps(list(accounts or [])), json.dumps(list(partitions or [])), float(deadline),
                       at_deadline, site_backstop, approved_by, at, owner.host, owner.pid, owner.started))
            c.execute("insert into event (at, batch_id, kind, data) values (?,?,?,?)",
                      (at, envelope_id, "envelope.open", json.dumps({"cap": cap, "by": approved_by})))
        return envelope_id

    def envelope(self, envelope_id: str) -> dict[str, Any] | None:
        r = self._con.execute("select * from envelope where id = ?", (envelope_id,)).fetchone()
        return None if r is None else dict(r)

    def envelopes(self, *, facility: str | None = None, states: Iterable[str] | None = None) -> list[dict[str, Any]]:
        q, args = "select * from envelope where 1=1", list[Any]()
        if facility is not None:
            q += " and facility = ?"
            args.append(facility)
        if states is not None:
            st = list(states)
            q += f" and state in ({','.join('?' * len(st))})"
            args += st
        return [dict(r) for r in self._con.execute(q + " order by created_at", args)]

    def update_envelope(self, envelope_id: str, **fields: Any) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self._tx(immediate=True) as c:
            c.execute(f"update envelope set {cols} where id = ?", (*fields.values(), envelope_id))

    def set_cap(self, envelope_id: str, cap: float, *, ceiling: float | None = None) -> None:
        """The user's yes to a new cap, checked against the ceiling in the same transaction."""
        with self._tx(immediate=True) as c:
            r = c.execute("select * from envelope where id = ?", (envelope_id,)).fetchone()
            if r is None:
                raise EnvelopeClosed(f"no envelope {envelope_id}")
            e = dict(r)
            _ceiling_check(c, facility=e["facility"], accounts=envelope_accounts(e), cap=cap, ceiling=ceiling,
                           except_id=envelope_id)
            c.execute("update envelope set cap = ? where id = ?", (float(cap), envelope_id))

    def rebind_envelope(self, envelope_id: str, *, session: str, owner: Owner, cap: float | None = None,
                        ceiling: float | None = None, now: float | None = None) -> None:
        """The user's yes in a later session: the envelope is this session's from now on (its cap raised or lowered
        when the yes named one, within the ceiling)."""
        at = time.time() if now is None else now
        with self._tx(immediate=True) as c:
            if cap is not None:
                r = c.execute("select * from envelope where id = ?", (envelope_id,)).fetchone()
                if r is not None:
                    e = dict(r)
                    _ceiling_check(c, facility=e["facility"], accounts=envelope_accounts(e), cap=cap,
                                   ceiling=ceiling, except_id=envelope_id)
            c.execute("update envelope set approved_by = ?, approved_at = ?, owner_host = ?, owner_pid = ?, "
                      "owner_start = ?, cap = coalesce(?, cap) where id = ?",
                      (session, at, owner.host, owner.pid, owner.started, cap, envelope_id))
            c.execute("insert into event (at, batch_id, kind, data) values (?,?,?,?)",
                      (at, envelope_id, "envelope.rebind", json.dumps({"by": session, "cap": cap})))

    def envelope_owner(self, e: dict[str, Any]) -> Owner:
        return Owner(str(e.get("owner_host") or ""), int(e.get("owner_pid") or 0), e.get("owner_start"))

    def excluded_jobs(self, envelope_id: str) -> list[str]:
        e = self.envelope(envelope_id)
        return list(json.loads((e or {}).get("excluded") or "[]"))

    def exclude_jobs(self, envelope_id: str, native_ids: Iterable[str]) -> list[str]:
        """The user said these jobs are theirs, not hpc-bridge's: the meter never counts them against the envelope
        again."""
        with self._tx(immediate=True) as c:
            r = c.execute("select excluded from envelope where id = ?", (envelope_id,)).fetchone()
            have = list(json.loads((r[0] if r else None) or "[]"))
            have += [str(n) for n in native_ids if str(n) not in have]
            c.execute("update envelope set excluded = ? where id = ?", (json.dumps(have), envelope_id))
            c.execute("insert into event (at, batch_id, kind, data) values (?,?,?,?)",
                      (time.time(), envelope_id, "envelope.exclude", json.dumps(have)))
        return have

    def envelope_hold(self, c: sqlite3.Connection, envelope_id: str) -> tuple[float, int]:
        """(what the envelope holds now — every job's `job_hold` plus the unledgered jobs the meter counted —, the
        array tasks admitted), read on connection `c` (inside a transaction)."""
        e = c.execute("select unledgered_hold from envelope where id = ?", (envelope_id,)).fetchone()
        held = float((e[0] if e else 0.0) or 0.0)
        tasks = 0
        for r in c.execute("select * from job where envelope_id = ?", (envelope_id,)):
            j = dict(r)
            held += job_hold(j)
            if j.get("state") != "refused":
                tasks += int(j.get("tasks") or 1)
        return held, tasks

    def reserve_job(self, envelope_id: str, job_id: str, *, session: str, worst_case: float, tasks: int = 1,
                    nodes: int = 1, account: str | None = None, partition: str | None = None,
                    source: str = "submit_job", native_id: str | None = None, state: str = "reserved",
                    ignore_owner: bool = False, now: float | None = None, **fields: Any) -> dict[str, Any]:
        """THE admission point of an envelope: in one BEGIN IMMEDIATE transaction, check that the envelope admits (open,
        this session's, before its deadline), that the job passes its dimensions, and that what it holds plus this
        job's worst case stays within the cap — then insert the job, holding its worst case. `ignore_owner`: an adopted
        job the user already said yes to in a session that is gone (the held-job repair)."""
        at = time.time() if now is None else now
        with self._tx(immediate=True) as c:
            r = c.execute("select * from envelope where id = ?", (envelope_id,)).fetchone()
            if r is None:
                raise EnvelopeClosed(f"no envelope {envelope_id}")
            e = dict(r)
            if not ignore_owner:
                if e["state"] not in ENVELOPE_ADMITTING:
                    raise EnvelopeClosed(f"envelope {envelope_id} is {e['state']}: it admits nothing more")
                if e["approved_by"] != session:
                    raise EnvelopeNotOwned(f"envelope {envelope_id} was approved by another session")
                if float(e["deadline"]) <= at:
                    raise EnvelopeClosed(f"envelope {envelope_id} is past its deadline")
            accounts = json.loads(e.get("accounts") or "[]")
            partitions = json.loads(e.get("partitions") or "[]")
            if accounts and (account or "") not in accounts and not ignore_owner:
                raise EnvelopeLimit(f"account {account!r} is not one the envelope allows ({', '.join(accounts)})")
            if partitions and partition and partition not in partitions and not ignore_owner:
                raise EnvelopeLimit(f"partition {partition!r} is not one the envelope allows ({', '.join(partitions)})")
            if e.get("max_nodes_per_block") and nodes > int(e["max_nodes_per_block"]) and not ignore_owner:
                raise EnvelopeLimit(f"{nodes} nodes is over the envelope's max_nodes_per_block "
                                    f"{int(e['max_nodes_per_block'])}")
            held, admitted = self.envelope_hold(c, envelope_id)
            if e.get("max_jobs") and admitted + tasks > int(e["max_jobs"]) and not ignore_owner:
                raise EnvelopeLimit(f"{tasks} task(s) would make {admitted + tasks}, over the envelope's max_jobs "
                                    f"{int(e['max_jobs'])}")
            cap = float(e["cap"])
            if held + float(worst_case) > cap + 1e-9 and not ignore_owner:
                raise EnvelopeExhausted(f"{held:g} of {cap:g} is held; this job needs {float(worst_case):g}",
                                        held=held, need=float(worst_case), cap=cap)
            if e["state"] == "exhausted" and not ignore_owner:
                c.execute("update envelope set state = 'open' where id = ?", (envelope_id,))
            cols = {"id": job_id, "envelope_id": envelope_id, "created_at": at, "source": source,
                    "native_id": native_id, "approved_by": session, "partition": partition, "account": account,
                    "nodes": nodes, "tasks": tasks, "worst_case": float(worst_case), "state": state, **fields}
            names = ", ".join(cols)
            c.execute(f"insert into job ({names}) values ({','.join('?' * len(cols))})", tuple(cols.values()))
            c.execute("insert into event (at, batch_id, kind, data) values (?,?,?,?)",
                      (at, envelope_id, "job.reserve", json.dumps({"job": job_id, "worst_case": worst_case,
                                                                   "held_before": held})))
            return cols

    def job(self, job_id: str) -> dict[str, Any] | None:
        r = self._con.execute("select * from job where id = ?", (job_id,)).fetchone()
        return None if r is None else dict(r)

    def jobs(self, *, envelope_id: str | None = None, envelope_ids: Iterable[str] | None = None,
             states: Iterable[str] | None = None) -> list[dict[str, Any]]:
        q, args = "select * from job where 1=1", list[Any]()
        if envelope_id is not None:
            q += " and envelope_id = ?"
            args.append(envelope_id)
        if envelope_ids is not None:
            ids = list(envelope_ids)
            q += f" and envelope_id in ({','.join('?' * len(ids)) or 'null'})"
            args += ids
        if states is not None:
            st = list(states)
            q += f" and state in ({','.join('?' * len(st))})"
            args += st
        return [dict(r) for r in self._con.execute(q + " order by created_at", args)]

    def update_job(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self._tx(immediate=True) as c:
            c.execute(f"update job set {cols} where id = ?", (*fields.values(), job_id))

    def move_job(self, job_id: str, *, only_from: Iterable[str], unless_cancel: bool = False,
                 while_admitting: bool = False, **fields: Any) -> bool:
        """Move a job's row only from `only_from` states (and, with `unless_cancel`, only if no stop or cancel asked
        for it meanwhile; with `while_admitting`, only while its envelope still admits — a stop closes it first) — one
        transaction, so a stop and an admission cannot both win. Returns whether it moved."""
        st = list(only_from)
        cols = ", ".join(f"{k} = ?" for k in fields)
        q = f"update job set {cols} where id = ? and state in ({','.join('?' * len(st))})"
        if unless_cancel:
            q += " and coalesce(cancel_requested, 0) = 0"
        if while_admitting:
            q += (" and exists (select 1 from envelope where envelope.id = job.envelope_id and envelope.state in "
                  f"({','.join(repr(s) for s in ENVELOPE_ADMITTING)}))")
        with self._tx(immediate=True) as c:
            return c.execute(q, (*fields.values(), job_id, *st)).rowcount == 1

    # -- hand submits in flight (D2): what a reconciliation needs, whatever the command's outcome ----------------
    def add_hand(self, token: str, *, envelope_id: str, facility: str | None, session: str, mark_path: str,
                 shim_dir: str, before: list[str], now: float | None = None) -> None:
        with self._tx() as c:
            c.execute("insert into hand (token, envelope_id, facility, session, mark_path, shim_dir, before, "
                      "created_at, state) values (?,?,?,?,?,?,?,?,'open')",
                      (token, envelope_id, facility, session, mark_path, shim_dir, json.dumps(before),
                       time.time() if now is None else now))

    def hands(self, *, facility: str | None = None, states: Iterable[str] = ("open",)) -> list[dict[str, Any]]:
        st = list(states)
        q, args = f"select * from hand where state in ({','.join('?' * len(st))})", list[Any](st)
        if facility is not None:
            q += " and facility is ?"
            args.append(facility)
        return [dict(r) for r in self._con.execute(q + " order by created_at", args)]

    def update_hand(self, token: str, **fields: Any) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self._tx() as c:
            c.execute(f"update hand set {cols} where token = ?", (*fields.values(), token))

    def meter_reading(self, envelope_id: str, *, source: str, spent: float, committed: float,
                      unledgered: list[dict[str, Any]], detail: dict[str, Any], now: float | None = None) -> None:
        at = time.time() if now is None else now
        with self._tx() as c:
            c.execute("insert into meter_reading (at, envelope_id, source, spent, committed, unledgered, detail) "
                      "values (?,?,?,?,?,?,?)", (at, envelope_id, source, spent, committed, json.dumps(unledgered),
                                                 json.dumps(detail, default=str)))

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

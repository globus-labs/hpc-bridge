"""The durable batch ledger (batch phase 1; vault `Planned/Batch and swarm execution.md` §4).

SQLite at `<state_dir>/ledger.sqlite3`: mode 0600, WAL, a busy timeout. It sits beside `SentCheckStore` (state.py),
whose rows the proven stop logic (#24, #204, #208) reads; this store holds what a batch needs to survive the server:
its rows and command (a replay record must — unlike sent-checks, which never hold a command's text), each task's
attempt, Globus task id and settlement, the lease that makes one process the batch's only admitter and collector, and
the configs a stop or a reap retired.

- **Local filesystems only.** WAL does not work on a network filesystem, and the state dir is on NFS / Lustre / GPFS
  on login nodes and beside an orchestrator: `open` refuses one by its type (`fs_type`), naming HPC_BRIDGE_STATE_DIR.
- **One admission point.** `reserve` checks the lease holder, the retired configs and the window in ONE
  `BEGIN IMMEDIATE` transaction, and moves the rows it takes to `in_flight` with their next attempt. The window is the
  block's, shared by every batch on it (fewest rows out first while another batch waits).
- **One owner per batch.** A lease row (host, pid, the process's start time, heartbeat). A takeover needs a stale
  heartbeat AND a holder that is gone: its pid dead, or alive but started at another time (the pid was handed on). An
  orphaned server keeps its pid and its start time (`harness:server_orphaned`), so it is never taken over. The ledger is
  on a local disk, so its holders are this machine's processes: the hostname is not trusted for it (macOS renames
  itself on a new network; a container's name is its own).
"""
from __future__ import annotations

import json
import os
import secrets
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
# How long a batch's last refused ask for a slot (`batch.want_at`) makes it a contender for the block's shared window:
# past the 10 s of an idle batch loop's pass, so a waiting batch stays one; a batch that stopped asking (paused, its
# walltime gate shut, its server gone) holds nothing back for longer than this.
WANT_FRESH_S = 30.0
# How long past its admission a row a gone server left in flight can still hold a worker of the block: task_walltime_s
# plus this — `batch.HOLD_MARGIN_S`, derived from the wrapper's own arithmetic plus a delivery allowance (batch passes
# it; a test ties the two).
HOLD_MARGIN_S = 75.0

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


class Reserved(list[TaskRow]):
    """`reserve`'s answer: the rows it moved to in_flight, and — when the block's other rows refused it slots its own
    window had room for — those batches, {batch id: rows holding the block} (named to the user, never silently held)."""

    def __init__(self, rows: Iterable[TaskRow] = (), *, holders: dict[str, int] | None = None) -> None:
        super().__init__(rows)
        self.holders: dict[str, int] = dict(holders or {})


def window_for(workers: int | None) -> int:
    """A block's window: its workers less one kept for interactive work; 1 when the count is unknown or 1."""
    return max(1, (workers or 1) - 1)


def _need(b: Any) -> tuple[int, int, int]:
    """A batch's row: (CPUs, memory MB — 0 when not set —, GPUs)."""
    return int(b["cores"] or 1), int(b["memory_mb"] or 0), int(b["gpus"] or 0)


def _fits(need: tuple[int, int, int], free: tuple[int, int | None, int, int]) -> int:
    """How many rows of `need` fit in `free` (CPUs, memory MB or None — not measured —, GPUs, rows)."""
    k, mem, g = need
    c_free, m_free, g_free, r_free = free
    if r_free <= 0 or c_free < k:
        return 0
    n = min(c_free // max(1, k), r_free)
    if mem and m_free is not None:
        n = min(n, m_free // mem)
    if g:
        n = min(n, g_free // g)
    return max(0, n)


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
  cancel_requested text,
  want_at real
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
create table if not exists block_workers (
  endpoint_id text,
  uec_hash text not null,
  block_job text not null default '',
  workers integer not null,
  at real,
  primary key (endpoint_id, uec_hash, block_job)
);
create table if not exists block_measure (
  endpoint_id text,
  uec_hash text not null,
  block_job text not null default '',
  measure text not null,
  at real,
  primary key (endpoint_id, uec_hash, block_job)
);
create table if not exists slot_token (
  endpoint_id text not null default '',
  uec_hash text not null,
  token text not null,
  at real,
  primary key (endpoint_id, uec_hash)
);
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
                "unknown_since", "unheld",
                # batch row sizing: the attempt's own use, and when a row a busy slot sent back may go again
                "peak_rss_kb", "cpu_s", "wall_s", "nproc", "cpus", "gpu_ids", "why", "threads", "oom_kills",
                "not_before", "pinned")
# Columns a ledger written by an earlier build may lack: added on open (a new ledger too: its tables are created as
# the first build wrote them, then these are added).
_ADDED_COLUMNS = {"batch": [("owner_start", "text"), ("want_at", "real"),
                            # batch row sizing: each row's size (memory resolved at admission when defaulted), at
                            # most `concurrency` at once, pinned, the pilot's rows, what was defaulted, a proposal
                            ("cores", "integer"), ("memory_mb", "integer"), ("gpus", "integer"),
                            ("concurrency", "integer"), ("pin", "integer default 1"), ("pilot", "integer default 0"),
                            ("sized", "integer default 0"), ("sizing_defaulted", "text"), ("proposal", "text"),
                            # the user's own size while a pilot runs with the whole block (restored at its proposal)
                            ("pilot_saved", "text"),
                            # the rows' size is still the user's to give (a sizing pause; cleared by a resize), and
                            # why the user's yes is owed as well while it waits for it (a reap, a restart, a drain)
                            ("size_owed", "integer default 0"), ("needs_yes", "text"),
                            # block environments: the env every row runs in (JSON: envs.record — kind, how, key,
                            # Python, path, and what activates or rebuilds it after a restart)
                            ("env", "text")],
                  "task": [("sent_job", "text"), ("sent_uec", "text"), ("unknown_since", "real"), ("unheld", "text"),
                           ("req_cores", "integer"), ("req_memory_mb", "integer"), ("req_gpus", "integer"),
                           ("peak_rss_kb", "integer"), ("cpu_s", "real"), ("wall_s", "real"), ("nproc", "integer"),
                           ("cpus", "text"), ("gpu_ids", "text"), ("why", "text"), ("threads", "integer"),
                           ("oom_kills", "integer"), ("not_before", "real"),
                           # whether the attempt's memory was the DEFAULT share (a kill at it asks), and whether it
                           # really ran pinned (the wrapper's own word)
                           ("req_mem_default", "integer"), ("pinned", "integer")],
                  "job": [("cancel_requested", "integer default 0"), ("unheld", "integer default 0"),
                          ("hand", "text"),
                          ("env", "text")]}   # block environments: the env its job.sh activates (as a batch's)


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
        # after the columns (an earlier build's ledger gets `sent_uec` above): the block window's one scan (`_on_block`)
        con.execute("create index if not exists task_holding_by_uec on task (sent_uec, state) where state in "
                    f"({', '.join(repr(s) for s in HOLDING)})")
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
                  state: str = "admitting", now: float | None = None, cores: int | None = None,
                  memory_mb: int | None = None, gpus: int | None = None, concurrency: int | None = None,
                  pin: bool = True, pilot: int = 0, sized: bool | None = None,
                  sizing_defaulted: Iterable[str] = (), env: dict[str, Any] | None = None) -> str:
        """`cores` / `memory_mb` / `gpus` / `concurrency` (batch row sizing): each row's size and the most at once;
        `sized` (default: any of them given) — the batch is admitted against the block's measured pool. `env` (block
        environments): the env every row runs in, recorded as given (None: none)."""
        at = time.time() if now is None else now
        if sized is None:
            sized = any(v is not None for v in (cores, memory_mb, gpus, concurrency)) or bool(pilot)
        with self._tx(immediate=True) as c:
            c.execute("insert into batch (id, created_at, facility, endpoint_id, session_id, uec_hash, command, "
                      "script_hash, task_walltime_s, results_dir, idempotent, capacity, state, owner_host, owner_pid, "
                      "owner_start, heartbeat, cores, memory_mb, gpus, concurrency, pin, pilot, sized, "
                      "sizing_defaulted, env) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (batch_id, at, facility, endpoint_id, session_id, uec_hash, command, script_hash,
                       float(task_walltime_s), results_dir, int(bool(idempotent)), int(capacity), state, owner.host,
                       owner.pid, owner.started, at, cores, memory_mb, gpus, concurrency, int(bool(pin)),
                       int(pilot or 0), int(bool(sized)), json.dumps(list(sizing_defaulted)),
                       json.dumps(env) if env else None))
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
    def _alive(self, row: sqlite3.Row, owner: Owner | None, seen: dict[tuple[str, int, str | None], bool]) -> bool:
        """Whether a batch's lease holder still runs (`holder_alive`), once per holder — the caller's own server only
        when its start time, if on record, is the caller's (review round 2: a restarted server handed the dead one's
        pid had taken that server's rows for its own, and held them for good)."""
        key = (str(row["owner_host"] or ""), int(row["owner_pid"] or 0), row["owner_start"])
        if owner is not None and key[:2] == (owner.host, owner.pid) and key[2] in (None, owner.started):
            return True
        if key not in seen:
            seen[key] = self.holder_alive(Owner(*key))
        return seen[key]

    def _on_block(self, c: sqlite3.Connection, endpoint_id: str | None, uec_hash: str, block_job: str | None,
                  at: float, margin_s: float, owner: Owner | None,
                  seen: dict[tuple[str, int, str | None], bool]) -> tuple[dict[str, int], dict[str, int]]:
        """Per batch (the caller's too), on THIS block — the endpoint and config `uec_hash` — (holds, out):
        `holds`, its rows that can still hold a worker of the block, and none that cannot be shown to (review round 1:
        an aborted batch's presumed-lost rows had shrunk every later window on a MEP's one config, for good):
        - `in_flight`: while its batch's server runs (it settles or presumes it); a gone server's only until
          task_walltime_s + `margin_s` past its admission (`HOLD_MARGIN_S`: the wrapper's own end, and delivery);
        - `submit_unknown`: until EVIDENCE says its task can no longer run (`unheld`, `batch._unknown_evidence`), or
          reconciliation settles it — whatever its batch's state or server (review round 2: a failed POST may have
          queued it, and on a MEP the next submit with that config delivers it). Never silently: a batch it holds is
          told which batch holds it (`Reserved.holders`);
        - `presumed_lost`: only while its server runs and it was sent to the block in front (`block_job`).
        `out`, its rows in flight or of unknown fate on the block: the fairness order, the same whoever reads it.
        One scan, on the partial index `task_holding_by_uec` (a row from before `sent_uec` was set at admission is
        read by its batch's config)."""
        holds, out, _use = self._on_block_use(c, endpoint_id, uec_hash, block_job, at, margin_s, owner, seen)
        return holds, out

    def _on_block_use(self, c: sqlite3.Connection, endpoint_id: str | None, uec_hash: str, block_job: str | None,
                      at: float, margin_s: float, owner: Owner | None, seen: dict[tuple[str, int, str | None], bool]
                      ) -> tuple[dict[str, int], dict[str, int], dict[str, list[int]]]:
        """`_on_block`, and per batch what its holding rows hold of the block's pool (batch row sizing): [CPUs, memory
        MB, GPUs] — each attempt's own `req_*`, else its batch's size, else one CPU."""
        states = ", ".join(f"'{s}'" for s in HOLDING)
        cols = ("t.batch_id, t.state, t.sent_job, t.unheld, coalesce(t.admitted_at, t.unknown_since, 0) as sent_at, "
                "coalesce(t.req_cores, o.cores, 1) as rc, coalesce(t.req_memory_mb, o.memory_mb, 0) as rm, "
                "coalesce(t.req_gpus, o.gpus, 0) as rg, "
                "o.task_walltime_s, o.owner_host, o.owner_pid, o.owner_start from task t join batch o on o.id = "
                f"t.batch_id where o.endpoint_id is ? and t.state in ({states}) and ")
        holds: dict[str, int] = {}
        out: dict[str, int] = {}
        use: dict[str, list[int]] = {}
        for r in c.execute(f"select {cols} t.sent_uec = ? union all select {cols} t.sent_uec is null and "
                           "o.uec_hash = ?", (endpoint_id, uec_hash, endpoint_id, uec_hash)):
            bid = str(r["batch_id"])
            if r["state"] == "in_flight":
                young = at - float(r["sent_at"] or 0.0) < float(r["task_walltime_s"] or 0.0) + margin_s
                held = young or self._alive(r, owner, seen)
            elif r["state"] == "submit_unknown":
                held = r["unheld"] is None
            else:
                held = (block_job is not None and str(r["sent_job"] or "") == str(block_job)
                        and self._alive(r, owner, seen))
            if held:
                holds[bid] = holds.get(bid, 0) + 1
                u = use.setdefault(bid, [0, 0, 0])
                u[0] += int(r["rc"] or 1)
                u[1] += int(r["rm"] or 0)
                u[2] += int(r["rg"] or 0)
            if r["state"] == "in_flight" or (r["state"] == "submit_unknown" and r["unheld"] is None):
                out[bid] = out.get(bid, 0) + 1
        return holds, out, use

    def block_holders(self, batch_id: str, *, uec_hash: str, block_job: str | None = None, owner: Owner | None = None,
                      margin_s: float = HOLD_MARGIN_S, now: float | None = None) -> dict[str, int]:
        """The OTHER batches whose rows hold slots of this batch's block, as `reserve` counts them for `owner`."""
        at = time.time() if now is None else now
        b = self._con.execute("select endpoint_id from batch where id = ?", (batch_id,)).fetchone()
        if b is None:
            raise UnknownBatch(batch_id)
        holds, _out = self._on_block(self._con, b["endpoint_id"], uec_hash, block_job, at, margin_s, owner, {})
        holds.pop(batch_id, None)
        return holds

    # -- the block's worker count, as a task on it reported it ------------------------------------------------------
    def learn_workers(self, endpoint_id: str | None, uec_hash: str, block_job: str | None, workers: int, *,
                      now: float | None = None) -> None:
        """A check or a task on the block reported its worker count (PARSL_WORKER_COUNT): the block's, for every batch
        on it (review round 2: a count one batch had learnt was lost with that batch's admission)."""
        with self._tx() as c:
            c.execute("insert or replace into block_workers values (?,?,?,?,?)",
                      (endpoint_id, uec_hash, block_job or "", int(workers), time.time() if now is None else now))

    # -- the block's measure (batch row sizing): what it is, from inside it -----------------------------------------
    def learn_measure(self, endpoint_id: str | None, uec_hash: str, block_job: str | None, measure: dict[str, Any], *,
                      now: float | None = None) -> None:
        """A check (or a task) on the block measured it from inside (sizing.BlockMeasure.to_dict): the block's, for
        every batch on it, kept per endpoint, config and block job as its worker count is. The block's slot token is
        made here, the first time (`slot_token`)."""
        at = time.time() if now is None else now
        with self._tx(immediate=True) as c:
            c.execute("insert or replace into block_measure values (?,?,?,?,?)",
                      (endpoint_id, uec_hash, block_job or "", json.dumps(measure), at))
            self._token_of(c, endpoint_id, uec_hash, at)

    def block_measure(self, endpoint_id: str | None, uec_hash: str, block_job: str | None = None
                      ) -> dict[str, Any] | None:
        """The block as measured, with its slot token (`slot_token`) when it has one."""
        d = self._measure_of(self._con, endpoint_id, uec_hash, block_job)
        if d is not None:
            r = self._con.execute("select token from slot_token where endpoint_id = ? and uec_hash = ?",
                                  (endpoint_id or "", uec_hash)).fetchone()
            if r is not None:
                d["slot_token"] = str(r[0])
        return d

    def slot_token(self, endpoint_id: str | None, uec_hash: str, *, now: float | None = None) -> str:
        """The block's slot token: random, made by this server the first time the block is measured (or asked here,
        whichever is first) and kept, so that every row of every batch on the block names the same node lock directory
        (`hpcb-slots-<uid>-<job>-<token>`), a name another user on the node cannot guess and make first. Kept per
        block config: the job id in the name already sets one block's directory apart from the next's."""
        with self._tx(immediate=True) as c:
            return self._token_of(c, endpoint_id, uec_hash, time.time() if now is None else now)

    @staticmethod
    def _token_of(c: sqlite3.Connection, endpoint_id: str | None, uec_hash: str, at: float) -> str:
        c.execute("insert or ignore into slot_token values (?,?,?,?)",
                  (endpoint_id or "", uec_hash, secrets.token_hex(8), at))
        r = c.execute("select token from slot_token where endpoint_id = ? and uec_hash = ?",
                      (endpoint_id or "", uec_hash)).fetchone()
        return str(r[0])

    def _measure_of(self, c: sqlite3.Connection, endpoint_id: str | None, uec_hash: str,
                    block_job: str | None) -> dict[str, Any] | None:
        r = c.execute("select measure from block_measure where endpoint_id is ? and uec_hash = ? order by "
                      "(block_job = ?) desc, at desc limit 1", (endpoint_id, uec_hash, block_job or "")).fetchone()
        if r is None:
            return None
        try:
            d = json.loads(r[0])
        except ValueError:
            return None
        return d if isinstance(d, dict) and isinstance(d.get("cpus"), int) else None

    def _pool(self, c: sqlite3.Connection, m: dict[str, Any], endpoint_id: str | None, uec_hash: str,
              block_job: str | None) -> tuple[int, int | None, int, int]:
        """The block's pool, as measured: (CPUs, memory MB rows may use — the limit less the pool's idle use —, GPUs,
        rows at once — the workers less the interactive one, at most the width cap)."""
        from .sizing import WIDTH_CAP

        limit = m.get("mem_limit_mb")
        avail = None if not isinstance(limit, int) else max(0, limit - int(m.get("idle_mb") or 0))
        workers = m.get("workers")
        if not isinstance(workers, int):
            r = c.execute("select workers from block_workers where endpoint_id is ? and uec_hash = ? order by "
                          "(block_job = ?) desc, at desc limit 1", (endpoint_id, uec_hash, block_job or "")).fetchone()
            workers = int(r[0]) if r is not None else None
        rows = WIDTH_CAP if workers is None else min(WIDTH_CAP, workers - 1 if workers >= 2 else 1)
        return int(m["cpus"]), avail, len(m.get("gpu_ids") or []), rows

    def _learnt_window(self, c: sqlite3.Connection, endpoint_id: str | None, uec_hash: str,
                       block_job: str | None) -> int | None:
        r = c.execute("select workers from block_workers where endpoint_id is ? and uec_hash = ? order by "
                      "(block_job = ?) desc, at desc limit 1", (endpoint_id, uec_hash, block_job or "")).fetchone()
        return None if r is None else window_for(int(r[0]))

    def reserve(self, batch_id: str, owner: Owner, n: int, *, uec_hash: str, now: float | None = None,
                indices: Iterable[int] | None = None, block_job: str | None = None,
                margin_s: float = HOLD_MARGIN_S) -> Reserved:
        """THE admission point: in one BEGIN IMMEDIATE transaction, check that `owner` holds the lease, that the config
        is not retired and how many window slots are free, and move up to `n` backlog rows (the lowest indices, or
        `indices` first) to `in_flight` with their next attempt, on the block `uec_hash` names (`sent_uec`).

        The window is the BLOCK's (its endpoint and config; `block_job`: the block in front, when known): the batch's
        own rows out, plus every other batch's rows that can still hold a worker of this block (`_on_block`), against
        the smallest of the block's learnt worker count (`learn_workers`) and the `capacity` of every batch with rows
        on it or admitting on it (review rounds 1-2: a batch that had not learnt the count overfilled the window
        another had). Fairness: while another batch on the block waits for a slot it could take (`want_at` within
        WANT_FRESH_S — stamped only when the block's rows refused it, never when fairness did — still admitting, with
        a backlog and room of its own), a slot goes first to the batch with fewer rows in flight or of unknown fate on
        the block, then the one that admitted less recently, then the lower id: one order whoever reads it, so two
        waiters never defer to each other. The answer carries the other batches holding the block (`holders`)."""
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
            hold = ",".join("?" * len(HOLDING))

            def held_by(bid: str) -> int:   # a batch's own window: its rows out, wherever they went
                return int(c.execute(f"select count(*) from task where batch_id = ? and state in ({hold})",
                                     (bid, *HOLDING)).fetchone()[0])

            seen: dict[tuple[str, int, str | None], bool] = {}
            held = held_by(batch_id)
            holds, out, use = self._on_block_use(c, b["endpoint_id"], uec_hash, block_job, at, margin_s, owner, seen)
            mine_on = holds.pop(batch_id, 0)
            mine_use = use.pop(batch_id, [0, 0, 0])
            others = sum(holds.values())
            peers = [r for r in c.execute(
                "select o.*, (select max(admitted_at) from task where batch_id = o.id) as last, exists (select 1 from "
                "task where batch_id = o.id and state = 'backlog') as backlogged from batch o where o.id != ? and "
                "o.endpoint_id is ? and o.uec_hash = ? and o.state = 'admitting'",
                (batch_id, b["endpoint_id"], uec_hash)) if self._alive(r, owner, seen)]
            backlog = int(c.execute("select count(*) from task where batch_id = ? and state = 'backlog' and "
                                    "(not_before is null or not_before <= ?)", (batch_id, at)).fetchone()[0])
            waiting = [r for r in peers if r["backlogged"] and r["want_at"] is not None
                       and float(r["want_at"]) >= at - WANT_FRESH_S]
            rank = {str(r["id"]): (out.get(str(r["id"]), 0), float(r["last"] or 0.0), str(r["id"])) for r in waiting}
            measure = self._measure_of(c, b["endpoint_id"], uec_hash, block_job) if b["cores"] is not None else None
            if measure is None:   # rows a worker each: the block's window (PR 1)
                caps = [int(b["capacity"] or 1)] + [int(r["capacity"] or 1) for r in peers]
                if holds:
                    q = ",".join("?" * len(holds))
                    caps += [int(r[0] or 1)
                             for r in c.execute(f"select capacity from batch where id in ({q})", list(holds))]
                learnt = self._learnt_window(c, b["endpoint_id"], uec_hash, block_job)
                window = min(caps + ([learnt] if learnt is not None else []))
                own_room = max(0, min(n, window - held, backlog))
                take = min(own_room, max(0, window - held - others))
                room = {str(r["id"]): window - held_by(str(r["id"])) - (mine_on + others - holds.get(str(r["id"]), 0))
                        for r in waiting}

                def room_after(w: str, x: int) -> int:
                    return room[w] - x
            else:   # batch row sizing: the block's measured pool — CPUs, memory, GPUs and workers — shared by them all
                cpus, avail, gpus, rows_cap = self._pool(c, measure, b["endpoint_id"], uec_hash, block_job)
                used = [sum(u[i] for u in use.values()) for i in range(3)]
                free_all = (cpus - used[0] - mine_use[0], None if avail is None else avail - used[1] - mine_use[1],
                            gpus - used[2] - mine_use[2], rows_cap - others - mine_on)
                free_mine = (cpus - mine_use[0], None if avail is None else avail - mine_use[1], gpus - mine_use[2],
                             rows_cap - mine_on)
                need = _need(b)
                own = min(n, backlog, int(b["capacity"] or 1) - held)
                if b["concurrency"]:
                    own = min(own, int(b["concurrency"]) - held)
                own_room = max(0, min(own, _fits(need, free_mine)))
                take = min(own_room, _fits(need, free_all))
                caps_w = {str(r["id"]): max(0, int(r["capacity"] or 1) - held_by(str(r["id"]))) for r in waiting}

                def room_after(w: str, x: int) -> int:
                    pr = next(r for r in waiting if str(r["id"]) == w)
                    free = (free_all[0] - x * need[0], None if free_all[1] is None else free_all[1] - x * need[1],
                            free_all[2] - x * need[2], free_all[3] - x)
                    return min(caps_w[w], _fits(_need(pr), free))

                # A reservation for the head waiter (as a scheduler holds one for its next big job): a batch ranked
                # first whose row fits the whole pool but not what is free now — big rows behind small ones, a pilot
                # wanting the whole block — has its row's room kept: the others admit only beyond it, so it starts as
                # soon as enough rows settle (a freed CPU going to a small row each time had starved it for good).
                last0 = c.execute("select max(admitted_at) from task where batch_id = ?", (batch_id,)).fetchone()[0]
                mine0 = (out.get(batch_id, 0), float(last0 or 0.0), batch_id)
                whole = (cpus, avail, gpus, rows_cap)
                for w in sorted(rank, key=lambda x: rank[x]):
                    if rank[w] >= mine0:
                        break
                    pr = next(r for r in waiting if str(r["id"]) == w)
                    hn = _need(pr)
                    if caps_w[w] > 0 and _fits(hn, whole) >= 1 and room_after(w, 0) == 0:
                        left = (free_all[0] - hn[0], None if free_all[1] is None else free_all[1] - hn[1],
                                free_all[2] - hn[2], free_all[3] - 1)
                        take = min(take, _fits(need, left))
                        break
            refused = take < own_room   # by the block's other rows, never by its own rows or the window
            last = c.execute("select max(admitted_at) from task where batch_id = ?", (batch_id,)).fetchone()[0]
            fair, mine = 0, (out.get(batch_id, 0), float(last or 0.0), batch_id)
            while fair < take and not any(room_after(w, fair) > 0 and rank[w] < mine for w in rank):
                fair += 1
                mine = (out.get(batch_id, 0) + fair, at, batch_id)
            take = fair
            c.execute("update batch set want_at = ? where id = ?", (at if refused else None, batch_id))
            if take <= 0:
                return Reserved([], holders=holds if refused else {})
            want = list(indices or [])
            rows = []
            if want:
                q = ",".join("?" * len(want))
                rows += list(c.execute(f"select * from task where batch_id = ? and state = 'backlog' and idx in ({q}) "
                                       "order by idx", (batch_id, *want)))
            if len(rows) < take:
                have = {r["idx"] for r in rows}
                rows += [r for r in c.execute("select * from task where batch_id = ? and state = 'backlog' and "
                                              "(not_before is null or not_before <= ?) order by idx limit ?",
                                              (batch_id, at, take + len(have)))
                         if r["idx"] not in have]
            got = Reserved([], holders=holds if refused else {})
            try:   # a pilot's rows hold the whole block: no default memory of theirs (review R2-A2)
                mem_default = 0 if b["pilot_saved"] else int("memory_gb" in json.loads(b["sizing_defaulted"] or "[]"))
            except (ValueError, TypeError):
                mem_default = 0
            for r in rows[:take]:
                att = int(r["attempt"] or 0) + 1
                c.execute("update task set state = 'in_flight', attempt = ?, admitted_at = ?, task_id = null, "
                          "started_at = null, sent_uec = ?, req_cores = ?, req_memory_mb = ?, req_gpus = ?, "
                          "req_mem_default = ?, not_before = null, pinned = null where batch_id = ? and idx = ?",
                          (att, at, uec_hash, b["cores"], b["memory_mb"], b["gpus"], mem_default, batch_id,
                           r["idx"]))
                got.append(TaskRow(batch_id, int(r["idx"]), json.loads(r["row_json"]), att, "in_flight"))
            return got

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

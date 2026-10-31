"""Batch phase 1 (vault `Planned/Batch and swarm execution.md`, §10): the durable ledger and `run_batch` / `status` /
`cancel` on the session's already-confirmed compute block. Written failing first (strict xfails on main at fdf1d0e:
the modules did not exist, the canary reported no remaining walltime, and a second SDK Executor inherited the first
one's task group); the marks came off with the build.

The surface these tests are written against (the stage-A checkpoint fixes it):

- `ledger.Ledger.open(state_dir)` — `<state_dir>/ledger.sqlite3`, mode 0600, WAL, busy_timeout; refuses a network
  filesystem (`NetworkFilesystemError`, naming HPC_BRIDGE_STATE_DIR). `ledger.fs_type(path)` is the statfs seam.
- `Ledger.add_batch(...)`, `take_lease(batch_id, owner)`, `reserve(batch_id, owner, n, uec_hash=…)` (the ONE admission
  choke point: lease holder, retired configs and the window are checked in one BEGIN IMMEDIATE transaction),
  `retire(endpoint_id, uec_hash, why)`, `counts(batch_id)`, `tasks(batch_id)`.
- `batch.admit_ok(remaining_s, task_walltime_s, margin_s)`, `batch.presumed_lost(...)`, `batch.task_script(...)` (the
  wrapper), `batch.parse_result(stdout)`, `batch.collect_rest(client, task_ids)`, `batch.tick(app, batch_id)` (one
  admission + collection pass; `app.batch_autoloop = False` keeps the background loop off in tests).
- `server._run_batch / _status / _cancel` — the tools' orchestration seams, like `_run_shell`.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _shape_runtime
from tests.fakes import FakeFacility
from tests.test_server import _FakeRunner, _PendingFuture, _Res

_BASH = shutil.which("bash")


# ---- helpers ---------------------------------------------------------------------------------------------------------

def _owner(pid: int | None = None, host: str = "laptop"):
    from hpc_bridge.ledger import Owner

    return Owner(host=host, pid=os.getpid() if pid is None else pid)


def _ledger(tmp_path: Path):
    from hpc_bridge.ledger import Ledger

    return Ledger.open(tmp_path / "state")


def _add(led, bid: str = "batch-000000000001", *, n: int = 6, capacity: int = 2, owner=None, idempotent=False):
    led.add_batch(bid, command="echo {x}", rows=[{"x": i} for i in range(n)], task_walltime_s=60.0,
                  capacity=capacity, results_dir="/scratch/u/batches/" + bid, idempotent=idempotent,
                  endpoint_id="fake-eid", uec_hash="uec-1", session_id="default", owner=owner or _owner())
    return bid


class _BatchRunner(_FakeRunner):
    """A runner whose submits stay in flight until the test resolves them; each future gets a Globus-like task id
    once 'sent' (as the SDK's ComputeFuture does), and the runner's executor has a service-minted task group."""

    def __init__(self, *, canary=None, fail_submit_of: set[int] | None = None):
        super().__init__("fake-eid", _Res(0, "", ""), canary_result=canary, pending=True)
        self.task_group_id = str(uuid.uuid4())
        self.fail_submit_of = fail_submit_of or set()
        self.group_tasks: list[str] = []
        self.rest: dict[str, tuple[str, object]] = {}   # what the service answers by REST: tid -> (status, result)

    def submit(self, command):
        fut = _PendingFuture()
        n = len(self.futures)
        self.commands.append(command)
        self.futures.append(fut)
        if n in self.fail_submit_of:   # the batch_run POST failed: no task id ever came back for it
            fut.task_id = None
            fut.fail(ConnectionError("the response to the batch_run POST was lost"))
        else:
            fut.task_id = str(uuid.uuid4())
            self.group_tasks.append(fut.task_id)
        return fut

    def task_client(self):
        def batch(ids):
            out = {}
            for i in ids:
                if i in self.rest:
                    status, res = self.rest[i]
                    out[i] = ({"task_id": i, "status": "success", "result": res} if status == "success" else
                              {"task_id": i, "status": "failed", "exception": res})
            return {"results": out}

        v2 = SimpleNamespace(get_task_group=lambda tg: {"taskgroup_id": str(tg),
                                                       "tasks": [{"id": t} for t in self.group_tasks]},
                             get_task_batch=batch)
        return SimpleNamespace(_compute_web_client=SimpleNamespace(v2=v2), get_task=lambda tid: {"pending": True},
                               fx_serializer=SimpleNamespace(deserialize=lambda r: r))


def _ok_report(index: int, attempt: int = 1, *, rc: int = 0, remaining_s: float | None = 900.0, job: str = "101"):
    """What the wrapper prints on a worker: one HPCB_RESULT line."""
    rep = {"index": index, "attempt": attempt, "rc": rc, "tail": f"row {index}\n", "stderr_tail": "",
           "stdout_path": f"/scratch/u/batches/b/tasks/{index}/attempt-{attempt}/stdout", "stdout_bytes": 6,
           "truncated": False, "job": job, "host": "c1", "workers": 3,
           "remaining_s": remaining_s if remaining_s is not None else "unknown"}
    return _Res(0, "HPCB_RESULT " + json.dumps(rep) + "\n", "")


async def _batch_app(*, remaining_s: float = 900.0, workers: int = 3, mep: bool = False, runner=None):
    f = FakeFacility()
    f.workers = 1
    if mep:
        f.supported_shapes = ("compute",)
    app = AppCtx(facility=f, profile=Profile())
    app.state.endpoint_id = "fake-eid"
    app.batch_autoloop = False
    canary = CanaryResult(ok=True, worker_host="c1", worker_python="3.12.3", worker_dill="0.4.0", worker_job="101",
                          worker_remaining_s=remaining_s)
    runner = runner or _BatchRunner(canary=canary)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: runner
    rt = _shape_runtime(app, "compute")
    rt.user_endpoint_config["max_workers_per_node"] = workers
    rt.spend_confirmed = True
    return app, rt, runner


async def _start(app, n: int = 6, *, idempotent: bool = False, walltime: float = 60.0):
    from hpc_bridge import server

    out = await server._run_batch(app, command="echo {x}", rows=[{"x": i} for i in range(n)],
                                  task_walltime_s=walltime, idempotent=idempotent)
    assert out.batch_id and out.batch_id.startswith("batch-"), out
    return out


# ---- the ledger ------------------------------------------------------------------------------------------------------

def test_statfs_refuses_network_fs(tmp_path, monkeypatch):
    """SQLite's WAL does not work on a network filesystem, and the state dir sits on NFS / Lustre / GPFS on login nodes
    and beside loam: the ledger refuses at open, naming the variable that moves it."""
    from hpc_bridge import ledger

    mounts = ("/dev/disk1 / apfs rw 0 0\n"
              "server:/export/home /home nfs4 rw,relatime 0 0\n"
              "lustre@tcp:/scratch /scratch lustre rw 0 0\n"
              "sshfs#u@h: /mnt/remote fuse.sshfs rw 0 0\n")
    assert ledger.fs_type_from_mounts("/home/u/.hpc-bridge", mounts) == "nfs4"
    assert ledger.fs_type_from_mounts("/scratch/u/x", mounts) == "lustre"
    assert ledger.fs_type_from_mounts("/var/tmp/x", mounts) == "apfs"
    for fs in ("nfs", "nfs4", "lustre", "gpfs", "cifs", "fuse.sshfs"):
        monkeypatch.setattr(ledger, "fs_type", lambda _p, fs=fs: fs)
        with pytest.raises(ledger.NetworkFilesystemError, match="HPC_BRIDGE_STATE_DIR"):
            ledger.Ledger.open(tmp_path / f"s-{fs}")
    monkeypatch.setattr(ledger, "fs_type", lambda _p: "ext4")
    led = ledger.Ledger.open(tmp_path / "local")
    assert (tmp_path / "local" / "ledger.sqlite3").stat().st_mode & 0o777 == 0o600
    assert led.journal_mode() == "wal"


def test_lease_single_admitter(tmp_path):
    """One owner per batch: only the lease holder admits. A takeover needs a stale heartbeat AND a dead pid on this
    host (an orphaned server keeps its pid alive; `harness:server_orphaned`)."""
    from hpc_bridge.ledger import Ledger, NotLeaseHolder

    a, b = _ledger(tmp_path), Ledger.open(tmp_path / "state")
    me = other = _owner()
    bid = _add(a, owner=me)
    assert a.take_lease(bid, me).ok
    # a second server on the same state dir, same host, its own pid: the holder is alive and fresh
    live = subprocess.Popen(["sleep", "30"])
    try:
        b_owner = _owner(pid=live.pid)
        got = b.take_lease(bid, b_owner)
        assert not got.ok and got.holder.pid == me.pid
        with pytest.raises(NotLeaseHolder):
            b.reserve(bid, b_owner, 1, uec_hash="uec-1")
        # stale heartbeat but the holder's pid is alive: still no takeover
        assert not b.take_lease(bid, b_owner, now=time.time() + 3600).ok
    finally:
        live.kill()
        live.wait()
    # stale AND dead: the takeover is allowed
    dead = subprocess.Popen(["true"])
    dead.wait()
    c = Ledger.open(tmp_path / "state")
    bid2 = _add(c, "batch-000000000002", owner=_owner(pid=dead.pid))
    assert c.take_lease(bid2, _owner(pid=dead.pid)).ok
    assert not c.take_lease(bid2, other).ok                          # fresh heartbeat
    assert c.take_lease(bid2, other, now=time.time() + 3600).ok        # stale and dead
    assert len(c.reserve(bid2, other, 1, uec_hash="uec-1")) == 1


def test_reservation_atomic(tmp_path):
    """The window check and the reservation are one BEGIN IMMEDIATE transaction: many concurrent reservations (threads
    on their own connections) never put more than `capacity` rows in flight."""
    from hpc_bridge.ledger import Ledger

    led = _ledger(tmp_path)
    me = _owner()
    bid = _add(led, n=50, capacity=3, owner=me)
    assert led.take_lease(bid, me).ok
    got: list[int] = []
    barrier = threading.Barrier(12)

    def grab():
        mine = Ledger.open(tmp_path / "state")
        barrier.wait()
        for _ in range(5):
            got.extend(r.index for r in mine.reserve(bid, me, 2, uec_hash="uec-1"))

    threads = [threading.Thread(target=grab) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(got) == 3 and len(set(got)) == 3, got
    assert led.counts(bid)["in_flight"] == 3


def test_retired_token_refused_everywhere(tmp_path):
    """A stop or a reap retires the block's config (its `hpcb_block` token makes it unique: S5 saw one stray submit with
    a retired config restart the user endpoint and a block within 18 s). Every admission path goes through `reserve`,
    and `reserve` refuses a retired config; the engine then pauses the batch instead of submitting."""
    from hpc_bridge.ledger import RetiredConfig

    led = _ledger(tmp_path)
    me = _owner()
    bid = _add(led, owner=me)
    assert led.take_lease(bid, me).ok
    led.retire("fake-eid", "uec-1", why="stop released block 101")
    with pytest.raises(RetiredConfig, match="block 101"):
        led.reserve(bid, me, 1, uec_hash="uec-1")
    assert led.counts(bid)["in_flight"] == 0
    assert len(led.reserve(bid, me, 1, uec_hash="uec-2")) == 1   # a new confirmation's config (a fresh token) admits


async def test_retired_token_pauses_every_engine_path(monkeypatch):
    """The engine side of the same rule: the tick, an idempotent retry on evidence and a resume all meet the
    retirement — nothing is submitted with the retired config, and the batch asks again."""
    from hpc_bridge import batch, server
    from hpc_bridge.ledger import RetiredConfig  # noqa: F401 - the type the engine catches

    app, rt, runner = await _batch_app()
    out = await _start(app, n=6, idempotent=True)
    sent = len(runner.futures)
    app.ledger.retire("fake-eid", batch.uec_hash(rt), why="the block was reaped")
    # an idempotent row comes back ManagerLost (evidence): its retry would reuse the retired config
    from globus_compute_sdk.errors import TaskExecutionFailed

    runner.futures[0].fail(TaskExecutionFailed("parsl.executors.high_throughput.errors.ManagerLost: lost", "0"))
    await batch.tick(app, out.batch_id)
    st = await server._status(app, out.batch_id)
    assert len(runner.futures) == sent
    assert st.phase == "needs_confirmation" and "reaped" in (st.reason or "")


# ---- the wrapper (run locally under bash: the worker's side) ----------------------------------------------------------

def _run_wrapper(script: str, env: dict[str, str], tmp: Path) -> subprocess.CompletedProcess:
    full = {"PATH": f"{tmp / 'bin'}:{os.environ['PATH']}", "HOME": str(tmp), "TMPDIR": str(tmp / "nodetmp"), **env}
    (tmp / "nodetmp").mkdir(exist_ok=True)
    return subprocess.run([_BASH, "-c", script], capture_output=True, text=True, env=full, timeout=60)


def _fake_tool(tmp: Path, name: str, body: str) -> Path:
    (tmp / "bin").mkdir(exist_ok=True)
    p = tmp / "bin" / name
    p.write_text("#!/bin/sh\n" + body + "\n")
    p.chmod(0o755)
    return p


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_attempt_dir_first_writer_wins(tmp_path):
    """A retried batch_run POST can run a task twice (S4). The wrapper claims `<results_dir>/tasks/<i>/attempt-<n>/`
    with an atomic mkdir: the first writer runs the command, a duplicate exits early without running it."""
    from hpc_bridge import batch

    rd = tmp_path / "results"
    counter = tmp_path / "ran"
    script = batch.task_script(f"echo ran >> {counter}; echo hello", results_dir=str(rd), index=4, attempt=1,
                               task_walltime_s=30)
    first = _run_wrapper(script, {"SLURM_JOB_ID": "101", "SLURM_JOB_END_TIME": str(int(time.time()) + 600)}, tmp_path)
    second = _run_wrapper(script, {"SLURM_JOB_ID": "101", "SLURM_JOB_END_TIME": str(int(time.time()) + 600)}, tmp_path)
    a, b = batch.parse_result(first.stdout), batch.parse_result(second.stdout)
    assert a is not None and a.rc == 0 and a.index == 4 and a.attempt == 1 and not a.duplicate
    assert b is not None and b.duplicate and b.index == 4
    assert counter.read_text().count("ran") == 1
    assert (rd / "tasks" / "4" / "attempt-1" / "rc").read_text().strip() == "0"


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_remaining_walltime_end_time_cached(tmp_path):
    """The block's remaining walltime from inside it (S8): Slurm's SLURM_JOB_END_TIME − now, exact and with no RPC,
    read once per block and cached on node-local /tmp; `squeue … %L` only as the fallback; with no job id it is
    `unknown`, and `squeue -j ""` (rc 0, empty) is never run."""
    from hpc_bridge import batch

    calls = tmp_path / "squeue-calls"
    _fake_tool(tmp_path, "squeue", f'echo "$@" >> {calls}; echo 4:00')
    end = int(time.time()) + 500
    s1 = batch.task_script("true", results_dir=str(tmp_path / "rd"), index=0, attempt=1, task_walltime_s=30)
    r1 = batch.parse_result(_run_wrapper(s1, {"SLURM_JOB_ID": "77", "SLURM_JOB_END_TIME": str(end)}, tmp_path).stdout)
    assert r1 is not None and r1.job == "77" and abs(r1.remaining_s - 500) <= 3
    assert not calls.exists()                                      # exact, no RPC
    # the next task on the same block reads the cache (here its env lacks the end time: the cache still answers)
    s2 = batch.task_script("true", results_dir=str(tmp_path / "rd"), index=1, attempt=1, task_walltime_s=30)
    r2 = batch.parse_result(_run_wrapper(s2, {"SLURM_JOB_ID": "77"}, tmp_path).stdout)
    assert r2 is not None and abs(r2.remaining_s - 500) <= 3 and not calls.exists()
    # another block with no end time in its env: the %L fallback (+1 s)
    r3 = batch.parse_result(_run_wrapper(
        batch.task_script("true", results_dir=str(tmp_path / "rd"), index=2, attempt=1, task_walltime_s=30),
        {"SLURM_JOB_ID": "78"}, tmp_path).stdout)
    assert r3 is not None and abs(r3.remaining_s - 241) <= 3 and "-j 78" in calls.read_text()
    # no job id (the login shape): unknown, and squeue is not asked about an empty job
    before = calls.read_text()
    r4 = batch.parse_result(_run_wrapper(
        batch.task_script("true", results_dir=str(tmp_path / "rd"), index=3, attempt=1, task_walltime_s=30),
        {}, tmp_path).stdout)
    assert r4 is not None and r4.remaining_s is None and calls.read_text() == before


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_wrapper_caps_output_and_times_out_itself(tmp_path):
    """(Phase-1 scope beyond §10's list: the output cap and the wrapper's own timeout.) Each stream is capped at the
    source and the result carries a tail, the stored size and a pointer — never the output itself, so a big stdout is
    no MaxResultSizeExceeded; a command past task_walltime_s is killed by the wrapper, which still writes rc 124."""
    from hpc_bridge import batch

    rd = tmp_path / "rd"
    big = batch.task_script("head -c 3000000 /dev/zero | tr '\\0' x; echo; echo the-end", results_dir=str(rd),
                            index=0, attempt=1, task_walltime_s=30, cap_bytes=1_000_000)
    r = batch.parse_result(_run_wrapper(big, {"SLURM_JOB_ID": "5"}, tmp_path).stdout)
    assert r is not None and r.rc == 0 and r.truncated and r.stdout_bytes == 1_000_000
    assert (rd / "tasks" / "0" / "attempt-1" / "stdout").stat().st_size == 1_000_000
    assert r.stdout_path.endswith("tasks/0/attempt-1/stdout") and len(r.tail) <= batch.TAIL_BYTES
    assert r.tail.endswith("the-end\n")          # the tail is the stream's real end, past the cap
    slow = batch.task_script("sleep 30", results_dir=str(rd), index=1, attempt=1, task_walltime_s=2)
    t0 = time.monotonic()
    r = batch.parse_result(_run_wrapper(slow, {"SLURM_JOB_ID": "5"}, tmp_path).stdout)
    assert r is not None and r.rc == 124 and time.monotonic() - t0 < 20
    assert (rd / "tasks" / "1" / "attempt-1" / "rc").read_text().strip() == "124"


# ---- admission, settling and the collector ---------------------------------------------------------------------------

def test_admission_gate_pure():
    from hpc_bridge import batch

    assert not batch.admit_ok(None, 60, margin_s=30)               # unknown admits nothing
    assert not batch.admit_ok(89.9, 60, margin_s=30)
    assert batch.admit_ok(90.0, 60, margin_s=30)


async def test_admission_gate_remaining_walltime():
    """A task is admitted only while the block's remaining walltime (from inside it: the canary and every result) is at
    least task_walltime_s + the margin. Past that the batch pauses at needs_confirmation naming the walltime — a task
    admitted as the block ends would wait at the interchange, and parsl would start a block nobody confirmed (#204)."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app(remaining_s=150.0, workers=3)       # capacity 2 (one worker kept for interactive)
    out = await _start(app, n=6, walltime=60.0)
    assert out.capacity == 2 and out.admitted == 2 and len(runner.futures) == 2
    # the first result: the block now has 80 s left, under 60 + the margin
    runner.futures[0].finish(_ok_report(0, remaining_s=80.0))
    await batch.tick(app, out.batch_id)
    assert len(runner.futures) == 2                                 # nothing more admitted
    st = await server._status(app, out.batch_id)
    assert st.phase == "needs_confirmation" and "walltime" in (st.reason or "")
    assert st.counts.backlog == 4 and st.counts.done == 1 and st.counts.in_flight == 1


async def test_stop_pauses_admission_under_lock(monkeypatch):
    """stop_endpoint pauses every batch's admission under app.lock BEFORE anything is released: the in-flight tasks hold
    the block (the stop refuses, as for a live task), and once they finish nothing more is admitted."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app()
    out = await _start(app, n=6)
    seen: list[bool] = []
    real = batch.pause_all

    def spy(app_, reason):
        seen.append(app_.lock.locked())
        return real(app_, reason)

    monkeypatch.setattr(batch, "pause_all", spy)
    st = await server._stop_endpoint(app)
    assert seen and all(seen), "admission must be paused while app.lock is held"
    assert st.status == "up" and "batch-" in (st.notice or "")      # refused: its tasks hold the block
    assert f"cancel('{out.batch_id}', 'abort')" in st.notice and "wait" in st.notice   # and it says how to end them
    for i, f in enumerate(list(runner.futures)):
        f.finish(_ok_report(i))
    await batch.tick(app, out.batch_id)
    assert len(runner.futures) == out.admitted                      # nothing admitted after the stop
    s = await server._status(app, out.batch_id)
    assert s.phase == "needs_confirmation" and "stop" in (s.reason or "")


async def test_submit_unknown_never_resubmitted():
    """Any submit error marks its rows submit_unknown (the SDK's transport retries the batch_run POST with no
    idempotency key: S4 ran a batch twice). A non-idempotent row is never resubmitted automatically; an idempotent one
    only after get_task_group shows no stray task of ours."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app(workers=4)                   # capacity 3
    runner.fail_submit_of = {0}
    out = await _start(app, n=5, idempotent=False)
    for i in (1, 2):
        runner.futures[i].finish(_ok_report(i))
    for _ in range(3):
        await batch.tick(app, out.batch_id)
    st = await server._status(app, out.batch_id, detail="page")
    row0 = next(r for r in st.results if r.index == 0)
    assert row0.state == "submit_unknown" and st.counts.submit_unknown == 1
    assert sum("/tasks/0/attempt-" in c for c in runner.commands) == 1

    app2, _rt2, runner2 = await _batch_app(workers=4)
    runner2.fail_submit_of = {0}
    out2 = await _start(app2, n=5, idempotent=True)
    stray = str(uuid.uuid4())
    runner2.group_tasks.append(stray)                               # a task in our group the ledger does not know
    for i in (1, 2):
        runner2.futures[i].finish(_ok_report(i))
    await batch.tick(app2, out2.batch_id)
    assert sum("/tasks/0/attempt-" in c for c in runner2.commands) == 1   # not while a stray task may be it
    runner2.group_tasks.remove(stray)
    await batch.tick(app2, out2.batch_id)
    assert sum("/tasks/0/attempt-2" in c for c in runner2.commands) == 1  # reconciled: attempt 2, once


def test_mep_clock_starts_at_block_evidence():
    """On a MEP `presumed_lost` is a clock that starts at evidence a block started (a sibling result naming its job, the
    canary's answer) — never at submit: a task may only be queued behind a block still pending (Polaris prod queues
    wait hours)."""
    from hpc_bridge import batch

    g = batch.PRESUME_GRACE_S
    assert not batch.presumed_lost(admitted_at=0.0, block_seen_at=None, task_walltime_s=60, now=1e6)
    assert not batch.presumed_lost(admitted_at=0.0, block_seen_at=5000.0, task_walltime_s=60, now=5000 + 60 + g - 1)
    assert batch.presumed_lost(admitted_at=0.0, block_seen_at=5000.0, task_walltime_s=60, now=5000 + 60 + g + 1)
    # admitted after the evidence: its own admission starts the clock
    assert not batch.presumed_lost(admitted_at=9000.0, block_seen_at=5000.0, task_walltime_s=60, now=9000 + 60 + g - 1)


async def test_presumed_keeps_slot_never_retried(monkeypatch):
    """A presumed task keeps its window slot and is never retried on the presumption (review finding 1: a retry or a
    refill on presumption overfills the interchange and breaks the MEP bound). Only evidence — a ManagerLost result —
    retries an idempotent row."""
    from globus_compute_sdk.errors import TaskExecutionFailed

    from hpc_bridge import batch, server

    clock = [1000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    app, _rt, runner = await _batch_app(mep=True, workers=3)          # capacity 2
    out = await _start(app, n=6, idempotent=True)
    assert len(runner.futures) == 2
    clock[0] += 60 + batch.PRESUME_GRACE_S + 10                       # past the clock, from the canary's block evidence
    await batch.tick(app, out.batch_id)
    st = await server._status(app, out.batch_id)
    assert st.counts.presumed_lost == 2 and st.counts.in_flight == 0
    assert len(runner.futures) == 2                                   # slots held: no refill, no retry
    runner.futures[0].fail(TaskExecutionFailed("parsl.executors.high_throughput.errors.ManagerLost: gone", "0"))
    await batch.tick(app, out.batch_id)
    assert len(runner.futures) == 3 and "attempt-2" in runner.commands[-1]   # retried on evidence, attempt 2


def test_collector_reads_raw_task_batch():
    """After a restart the collector pulls by REST with the raw `v2.get_task_batch`: `Client.get_batch_result` drops a
    failed task silently (S4), and a failure must not read as 'missing'."""
    from hpc_bridge import batch

    ok_id, failed_id, pending_id = (str(uuid.uuid4()) for _ in range(3))
    results = {
        ok_id: {"task_id": ok_id, "status": "success", "result": "<serialized>", "completion_t": "1"},
        failed_id: {"task_id": failed_id, "status": "failed",
                    "exception": "Traceback …\nparsl.executors.high_throughput.errors.ManagerLost: lost", "completion_t": "2"},
        pending_id: {"task_id": pending_id, "status": "waiting-for-nodes"},
    }
    asked: list[list[str]] = []

    def get_task_batch(ids):
        asked.append(sorted(map(str, ids)))
        return {"results": {i: results[i] for i in map(str, ids)}}

    def dropped(*_a, **_k):
        raise AssertionError("get_batch_result drops failed tasks; the collector must not use it")

    client = SimpleNamespace(_compute_web_client=SimpleNamespace(v2=SimpleNamespace(get_task_batch=get_task_batch)),
                             get_batch_result=dropped,
                             fx_serializer=SimpleNamespace(deserialize=lambda s: _ok_report(0).__dict__))
    got = batch.collect_rest(client, [ok_id, failed_id, pending_id])
    assert asked and sorted(asked[0]) == sorted([ok_id, failed_id, pending_id])
    assert got[ok_id].state == "success"
    assert got[failed_id].state == "failed" and "ManagerLost" in got[failed_id].error
    assert got[pending_id].state == "pending"


# ---- the task group --------------------------------------------------------------------------------------------------

def test_task_group_service_minted_per_executor(monkeypatch):
    """Every Executor starts with task_group_id None, so the SERVICE mints its group at the first submit (a client-made
    id 404s and shuts the Executor down: S8), and two Executors never share one (SDK 4.18's watcher acks every message
    on its group's queue: S1 lost 7 of 16 results). Today a second Executor in the process inherits the class-wide
    default the first one's adopted group wrote (`Executor._default_task_group_id`)."""
    import globus_compute_sdk.sdk.executor as sdk_ex

    from hpc_bridge.runner import GlobusRunner

    monkeypatch.setattr(sdk_ex, "Client", lambda *a, **k: SimpleNamespace())
    monkeypatch.setattr(sdk_ex.Executor, "_default_task_group_id", None)
    first = GlobusRunner(str(uuid.uuid4())).executor()
    try:
        first.task_group_id = uuid.uuid4()          # what the SDK does when the service mints the group (executor.py:1243)
        second = GlobusRunner(str(uuid.uuid4())).executor()
        try:
            assert second.task_group_id is None, "a new Executor must not inherit another one's task group"
        finally:
            second.shutdown(wait=False, cancel_futures=True)
    finally:
        first.shutdown(wait=False, cancel_futures=True)


# ---- restart and poll_task by uuid (phase-1 scope beyond §10's list) -------------------------------------------------

async def test_restart_resumes_only_with_confirm_spend(tmp_path):
    """A new server finds the batch in the ledger: `status` returns every row and never resumes admission (spend was
    confirmed in the old process's memory); `run_batch(batch_id=…, confirm_spend=True)` does."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app()                             # capacity 2
    out = await _start(app, n=6)
    for f in list(runner.futures):                                     # rows 0, 1 done, then 2, 3 admitted
        f.finish(_ok_report(runner.futures.index(f)))
    await batch.tick(app, out.batch_id)
    assert len(runner.futures) == 4
    runner.futures[2].finish(_ok_report(2))    # row 2 finished, and the server dies before its ledger commit
    app.ledger.close()
    # the server is killed: a new process on the same state dir, its own pid, the old one dead, no confirmation in memory
    from hpc_bridge.ledger import Ledger, Owner

    app2, rt2, runner2 = await _batch_app()
    rt2.spend_confirmed = False
    runner2.rest = {runner.futures[2].task_id: ("success", _ok_report(2))}   # the REST copy survives the ack (S1)
    app2.batch_owner = _owner(pid=os.getpid() + 100_000, host=Owner.here().host)   # same host, another pid
    app2.ledger = Ledger.open()
    app2.ledger.pid_alive = lambda pid: pid != os.getpid()        # the old server's pid is gone
    app2.ledger.lease_stale_s = 0.0
    rt2.runner = runner2                                               # the reconnect built its runner (zero SSH)
    st = await server._status(app2, out.batch_id, detail="page", limit=100)
    assert {r.index for r in st.results} == set(range(6))
    states = {r.index: r.state for r in st.results}
    assert states[2] == "done" and states[3] == "in_flight"            # pulled by REST; row 3 still out
    assert st.phase == "needs_confirmation" and "restart" in (st.reason or "")
    assert len(runner2.futures) == 0
    res = await server._run_batch(app2, batch_id=out.batch_id, confirm_spend=True)
    assert res.phase in ("admitting", "waiting") and len(runner2.futures) == 1   # row 3 still holds its slot
    assert "/tasks/4/attempt-1" in runner2.commands[-1]


async def test_poll_task_resolves_a_globus_uuid_from_the_ledger():
    """`poll_task` accepts a Globus task UUID: a task the ledger knows (a batch row, or an interactive task recorded
    when sent) is answered after a restart, when the handle (`compute-3`) means nothing to the new process."""
    from hpc_bridge import server

    app, _rt, runner = await _batch_app()
    await _start(app, n=1)
    tid = runner.futures[0].task_id
    runner.futures[0].finish(_ok_report(0, rc=3))
    got = await server._poll_task(app, tid)
    assert got.phase == "complete" and got.exit_code == 3


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_interactive_tasks_leave_rc_and_tail_in_the_session_dir(tmp_path):
    """(Phase-1 scope: `poll_task` by UUID, with session-dir results.) An interactive command also writes its rc and the
    tail of its output to the session directory, keyed by the dispatch, so its result survives a gap longer than the
    service's 30-minute window (S2) and a restart that lost the handle."""
    from hpc_bridge import session_shell

    s = session_shell.Session("default", str(tmp_path / "root"))
    script = session_shell.wrap("echo out; echo err >&2; exit 5", s, result_key="k1")
    subprocess.run([_BASH, "-c", script], capture_output=True, text=True, timeout=30)
    res = tmp_path / "root" / "sessions" / "default" / "results" / "k1"
    assert (res / "rc").read_text().strip() == "5"
    for _ in range(40):   # `tail` writes its file once tee's stream ends, a moment after the shell itself exited
        if (res / "tail").exists() and "out" in (res / "tail").read_text():
            break
        time.sleep(0.05)
    assert "out" in (res / "tail").read_text()


def test_ledger_schema_is_sqlite_wal(tmp_path, monkeypatch):
    """(The schema the checkpoint names.) Tables batch, task, retired, event; rows and the command are stored (a
    replay record needs them — unlike sent-checks, which never hold a command's text)."""
    from hpc_bridge import ledger

    monkeypatch.setattr(ledger, "fs_type", lambda _p: "ext4")
    led = ledger.Ledger.open(tmp_path / "s")
    _add(led)
    con = sqlite3.connect(tmp_path / "s" / "ledger.sqlite3")
    names = {r[0] for r in con.execute("select name from sqlite_master where type='table'")}
    assert {"batch", "task", "retired", "event"} <= names
    assert con.execute("select command from batch").fetchone()[0] == "echo {x}"
    assert con.execute("select count(*) from task").fetchone()[0] == 6


# ---- the stage-A review's corrections (2026-10-31) ------------------------------------------------------------------

@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_the_wrapper_ends_its_command_on_the_cancel_marker(tmp_path):
    """An abort ends what is in flight (design §5): the wrapper watches the CANCELLED marker while its command runs,
    kills the command's process group, writes rc 143 and reports the attempt cancelled. The block itself is never
    touched (it is the session's: interactive work runs there too)."""
    from hpc_bridge import batch

    rd = tmp_path / "rd"
    child = tmp_path / "child-alive"
    script = batch.task_script(f"(sleep 60; echo late > {child}) & sleep 60", results_dir=str(rd), index=0, attempt=1,
                               task_walltime_s=120)
    t0 = time.monotonic()
    proc = subprocess.Popen([_BASH, "-c", script], stdout=subprocess.PIPE, text=True,
                            env={"PATH": os.environ["PATH"], "HOME": str(tmp_path), "TMPDIR": str(tmp_path)})
    time.sleep(2.0)
    (rd / batch.CANCEL_MARKER).touch()
    out, _ = proc.communicate(timeout=30)
    r = batch.parse_result(out)
    assert r is not None and r.cancelled and r.rc == batch.CANCEL_RC and time.monotonic() - t0 < 20
    assert (rd / "tasks" / "0" / "attempt-1" / "rc").read_text().strip() == "143"
    time.sleep(1.0)
    assert not child.exists()                     # the whole process group went, the command's own children too


async def test_cancel_abort_ends_what_is_in_flight(monkeypatch):
    """cancel(abort): the backlog is cancelled at once, the marker is written for the tasks out, the answer is
    `draining` while any wrapper has not settled and `down` once none is in flight; those rows settle `cancelled`
    with the marker as their evidence."""
    from hpc_bridge import batch, server

    marked: list[str] = []

    async def mark(app_, run, results_dir):
        marked.append(results_dir)
        return True

    monkeypatch.setattr(batch, "write_cancel_marker", mark)
    app, _rt, runner = await _batch_app()                             # capacity 2
    out = await _start(app, n=6)
    first = await server._cancel(app, out.batch_id, "abort")
    assert first.status == "draining" and first.backlog_dropped == 4 and first.in_flight == 2 and marked
    assert len(runner.futures) == 2                                   # nothing admitted after the abort
    for i, f in enumerate(list(runner.futures)):
        rep = json.loads(_ok_report(i).stdout.split(" ", 1)[1])
        rep.update(rc=batch.CANCEL_RC, why="cancelled")
        f.finish(_Res(0, "HPCB_RESULT " + json.dumps(rep) + "\n", ""))
    again = await server._cancel(app, out.batch_id, "abort")
    assert again.status == "down" and again.in_flight == 0
    st = await server._status(app, out.batch_id, detail="page")
    assert st.phase == "cancelled" and st.counts.cancelled == 6
    assert all("CANCELLED" in (r.settled_by or "") for r in st.results if r.index < 2)


async def test_a_one_worker_block_still_admits_and_interactive_work_names_the_batch():
    """The window is max(1, workers - 1): a one-worker block admits one task, and an interactive run_shell meanwhile
    waits behind it as today — its answer names the batch."""
    from hpc_bridge import server

    app, _rt, runner = await _batch_app(workers=1)
    out = await _start(app, n=3)
    assert out.capacity == 1 and out.admitted == 1
    runner.timeout = 0.2                                               # the interactive sync-wait, short for the test
    got = await server._run_shell(app, "hostname", agent_call=True)
    assert out.batch_id in (got.notice or "") and "one worker" in (got.notice or "")


async def test_unknown_remaining_walltime_admits_nothing_and_a_task_that_cannot_fit_is_refused():
    from hpc_bridge import server

    app, _rt, runner = await _batch_app(remaining_s=None)
    out = await _start(app, n=3)
    assert out.admitted == 0 and not runner.futures and out.phase == "waiting"
    assert "remaining walltime" in (out.reason or "")
    app2, rt2, _r2 = await _batch_app()
    rt2.user_endpoint_config["walltime"] = "00:04:00"
    refused = await server._run_batch(app2, command="true", rows=[1], task_walltime_s=300)
    assert refused.phase == "refused" and "240 s" in (refused.reason or "") and "210" in (refused.reason or "")


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_rows_are_substituted_then_brace_escaped_for_shellfunction(tmp_path):
    """ShellFunction runs `cmd.format(**kwargs)`: after the row's fields are in, the command must survive that pass —
    `${HOME}`, an awk program's braces and a value with braces in it all come out as written."""
    from hpc_bridge import batch
    from hpc_bridge.runner import _escape_for_shellfunction

    cmd = batch.substitute("echo ${HOME} {v}; echo a b | awk '{print $1}'", {"v": "x{y}z"}, 0, {"v", "index"})
    assert _escape_for_shellfunction(cmd).format() == cmd
    out = subprocess.run([_BASH, "-c", _escape_for_shellfunction(cmd).format()], capture_output=True, text=True,
                         env={"PATH": os.environ["PATH"], "HOME": "/h"}).stdout
    assert out == "/h x{y}z\na\n"
    with pytest.raises(batch.BatchRefusal, match="not an identifier"):
        batch.validate("echo {a-b}", [{"a-b": 1}], 10)


async def test_a_bad_row_key_is_refused_through_the_tool(monkeypatch):
    from hpc_bridge import server

    app, _rt, runner = await _batch_app()
    got = await server._run_batch(app, command="echo {x}", rows=[{"x y": 1}], task_walltime_s=10)
    assert got.phase == "refused" and "identifier" in (got.reason or "") and not runner.futures


async def test_a_results_dir_under_home_is_warned_of_in_run_batch_and_status():
    from hpc_bridge import server

    app, _rt, _runner = await _batch_app()
    out = await server._run_batch(app, command="true", rows=[1, 2], task_walltime_s=10, results_dir="$HOME/runs/x")
    assert "$HOME" in (out.notice or "") and "under $HOME" in (out.notice or "")
    st = await server._status(app, out.batch_id)
    assert "under $HOME" in (st.notice or "")



async def test_a_resume_at_the_walltime_gate_asks_for_the_next_block_once_the_tasks_out_finish():
    """Live 2026-10-31 (batch_crosses_walltime): the user's yes at a walltime pause came while a row still ran on the
    block in front, which had too little left for another — and the batch asked again at once, over and over. The yes
    covers the NEXT block: the batch waits for its tasks out on this one, then confirms a fresh block (a new user
    endpoint) and admits there."""
    from hpc_bridge import batch, server

    app, rt, runner = await _batch_app(remaining_s=150.0, workers=3)   # capacity 2
    app.facility.fresh_ueps = True
    out = await _start(app, n=6)
    runner.futures[0].finish(_ok_report(0, remaining_s=80.0))
    await batch.tick(app, out.batch_id)
    assert (await server._status(app, out.batch_id)).phase == "needs_confirmation"
    res = await server._run_batch(app, batch_id=out.batch_id, confirm_spend=True)
    assert res.phase in ("admitting", "waiting") and len(runner.futures) == 2   # no re-ask; row 1 still out
    await batch.tick(app, out.batch_id)
    assert (await server._status(app, out.batch_id)).phase != "needs_confirmation"
    runner.futures[1].finish(_ok_report(1, remaining_s=70.0))
    await batch.tick(app, out.batch_id)
    assert app.shapes["compute"] is not rt and app.shapes["compute"].spend_confirmed   # the next block, confirmed
    assert len(runner.futures) > 2                                                     # admitted on it

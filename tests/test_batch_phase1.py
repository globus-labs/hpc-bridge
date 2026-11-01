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


# The service's task groups, by id: a client reads ANY group by its id (get_task_group), whichever runner minted it.
_GROUPS: dict[str, list[str]] = {}


class _BatchRunner(_FakeRunner):
    """A runner whose submits stay in flight until the test resolves them; each future gets a Globus-like task id
    once 'sent' (as the SDK's ComputeFuture does), and the runner's executor has a service-minted task group."""

    def __init__(self, *, canary=None, fail_submit_of: set[int] | None = None):
        super().__init__("fake-eid", _Res(0, "", ""), canary_result=canary, pending=True)
        self.task_group_id = str(uuid.uuid4())
        self.fail_submit_of = fail_submit_of or set()
        self.group_tasks: list[str] = []
        _GROUPS[self.task_group_id] = self.group_tasks
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

        def group(tg):
            tasks = self.group_tasks if str(tg) == str(self.task_group_id) else _GROUPS.get(str(tg), [])
            return {"taskgroup_id": str(tg), "tasks": [{"id": t} for t in tasks]}

        v2 = SimpleNamespace(get_task_group=group, get_task_batch=batch)
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
    # the resume path: a batch still admitting whose block's config was retired meanwhile — run_batch(batch_id=…)
    # without the yes sends nothing with it, and pauses naming the retirement
    app2, rt2, runner2 = await _batch_app()
    rt2.user_endpoint_config["hpcb_block"] = 2                         # another confirmation's config: not retired
    out2 = await _start(app2, n=6)
    sent2 = len(runner2.futures)
    assert sent2 == 2
    runner2.futures[0].finish(_ok_report(0))
    app2.ledger.retire("fake-eid", batch.uec_hash(rt2), why="stop_endpoint released this block")
    res = await server._run_batch(app2, batch_id=out2.batch_id)
    assert len(runner2.futures) == sent2, runner2.commands[sent2:]
    assert res.phase == "needs_confirmation" and "retired" in (res.reason or ""), res


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
    nocf = await server._run_batch(app2, batch_id=out.batch_id)       # a resume without the user's yes
    await batch.tick(app2, out.batch_id)
    assert nocf.phase == "needs_confirmation" and len(runner2.futures) == 0, nocf
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


@pytest.mark.skipif(_BASH is None, reason="needs bash")
async def test_poll_task_by_uuid_falls_back_to_the_session_dir_once_rest_cannot_answer(tmp_path, monkeypatch):
    """What reads those files: poll_task with the task's Globus id, once the service cannot answer for it (a result past
    its window, the service unreachable, a restart that lost the handle), returns the rc and tail the session dir kept —
    read over the free login shape."""
    from hpc_bridge import batch, server, session_shell

    root = tmp_path / "root"
    s = session_shell.Session("default", str(root))
    subprocess.run([_BASH, "-c", session_shell.wrap("echo out; exit 5", s, result_key="k1")], capture_output=True,
                   text=True, timeout=30)
    for _ in range(40):
        if (root / "sessions/default/results/k1/tail").exists():
            break
        time.sleep(0.05)
    monkeypatch.setattr(batch, "login_runner", _local_login(tmp_path))
    app, _rt, _runner = await _batch_app()
    tid = str(uuid.uuid4())
    batch.ledger_of(app).record_itask(tid, endpoint_id="fake-eid", shape="compute", session_id="default",
                                      result_key="k1", state_dir=str(root), handle=None)
    got = await server._poll_task(app, tid, 0)                         # the service lists it as nothing it knows
    assert got.phase == "complete" and got.exit_code == 5 and "out" in (got.stdout or ""), got
    assert "session dir" in (got.notice or ""), got.notice


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
    kills the command's process group — a background child it started too — writes rc 143 and reports the attempt
    cancelled. The block itself is never touched (it is the session's: interactive work runs there too)."""
    from hpc_bridge import batch

    rd = tmp_path / "rd"
    pidf = tmp_path / "child.pid"
    script = batch.task_script(f"sleep 300 & echo $! > {pidf}; sleep 300", results_dir=str(rd), index=0, attempt=1,
                               task_walltime_s=600)
    proc = subprocess.Popen([_BASH, "-c", script], stdout=subprocess.PIPE, text=True,
                            env={"PATH": os.environ["PATH"], "HOME": str(tmp_path), "TMPDIR": str(tmp_path)})
    for _ in range(100):
        if pidf.exists() and pidf.read_text().strip():
            break
        time.sleep(0.05)
    child = int(pidf.read_text())
    os.kill(child, 0)                                                   # the background child runs
    t0 = time.monotonic()
    (rd / batch.CANCEL_MARKER).touch()
    out, _ = proc.communicate(timeout=30)
    r = batch.parse_result(out)
    assert r is not None and r.cancelled and r.rc == batch.CANCEL_RC and time.monotonic() - t0 < 15
    assert (rd / "tasks" / "0" / "attempt-1" / "rc").read_text().strip() == "143"
    for _ in range(60):
        try:
            os.kill(child, 0)
        except ProcessLookupError:
            break
        time.sleep(0.05)
    else:
        pytest.fail("the command's background child outlived the cancel: its process group was not killed")


def _local_login(tmp: Path):
    """batch.login_runner for a test: the free login shape, as this machine's bash (the results dir is local)."""
    def runner_for(_app):
        async def run(cmd):
            p = subprocess.run([_BASH, "-c", cmd], capture_output=True, text=True, timeout=30,
                               env={"PATH": os.environ["PATH"], "HOME": str(tmp), "TMPDIR": str(tmp)})
            return SimpleNamespace(stdout=p.stdout)
        return run
    return runner_for


@pytest.mark.skipif(_BASH is None, reason="needs bash")
async def test_cancel_abort_ends_what_is_in_flight(tmp_path, monkeypatch):
    """cancel(abort), end to end: the backlog is cancelled at once, the marker is written in the batch's results
    directory over the login shape, each running wrapper sees it and kills its command (rc 143) — the answer is
    `draining` while any wrapper has not settled and `down` once none is in flight, long before the commands' own end;
    those rows settle `cancelled` with the marker as their evidence."""
    import asyncio

    from hpc_bridge import batch, server

    monkeypatch.setattr(batch, "login_runner", _local_login(tmp_path))
    runner = _BashRunner(tmp_path, live=True, canary=_canary())
    app, _rt, _ = await _batch_app(runner=runner)                      # capacity 2
    out = await server._run_batch(app, command="sleep 60; echo late {x}", rows=[{"x": i} for i in range(6)],
                                  task_walltime_s=120, results_dir=str(tmp_path / "rd"))
    bd = Path(out.results_dir)
    for _ in range(100):
        if len(list(bd.glob("tasks/*/attempt-1/meta.json"))) == 2:
            break
        await asyncio.sleep(0.1)
    t0 = time.monotonic()
    first = await server._cancel(app, out.batch_id, "abort")
    assert first.status == "draining" and first.backlog_dropped == 4 and first.in_flight == 2, first
    assert (bd / batch.CANCEL_MARKER).exists() and "kills its command" in (first.notice or ""), first.notice
    assert len(runner.futures) == 2                                   # nothing admitted after the abort
    again = first
    for _ in range(150):
        again = await server._cancel(app, out.batch_id, "abort")
        if again.status == "down":
            break
        await asyncio.sleep(0.1)
    assert again.status == "down" and again.in_flight == 0 and time.monotonic() - t0 < 20, again
    st = await server._status(app, out.batch_id, detail="page")
    assert st.phase == "cancelled" and st.counts.cancelled == 6
    ran = [r for r in st.results if r.index < 2]
    assert all("CANCELLED" in (r.settled_by or "") and r.exit_code == batch.CANCEL_RC for r in ran), ran


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
    """ShellFunction runs `cmd.format(**kwargs)`: after the row's fields are in, the task's script must survive that
    pass — `${HOME}`, an awk program's braces and a value with braces in it all come out as written."""
    from hpc_bridge import batch
    from hpc_bridge.runner import _escape_for_shellfunction

    names = {"v", "index"}
    cmd = batch.substitute("echo ${HOME} {v}; echo a b | awk '{print $1}'", {"v": "x{y}z"}, 0, names)
    script = batch.task_script(cmd, results_dir=str(tmp_path / "rd"), index=0, attempt=1, task_walltime_s=30,
                               fields=batch.row_values({"v": "x{y}z"}, 0, names))
    assert _escape_for_shellfunction(script).format() == script
    r = batch.parse_result(subprocess.run(
        [_BASH, "-c", _escape_for_shellfunction(script).format()], capture_output=True, text=True,
        env={"PATH": os.environ["PATH"], "HOME": str(tmp_path), "TMPDIR": str(tmp_path)}).stdout)
    assert r is not None and r.tail == f"{tmp_path} x{{y}}z\na\n", r and r.tail
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


async def test_after_a_restart_a_row_sent_without_its_id_is_found_in_the_task_group_or_requeued():
    """Live 2026-10-31 (batch_restart_resume): the server was killed right after admitting a row, before the service's
    answer gave it a task id — the new server listed it in flight for good. The batch's task group (recorded once the
    service minted it) says what was sent: a task in it the ledger does not know settles the row its result names; a row
    with no task in the group was never sent, and goes back to the backlog."""
    from hpc_bridge import batch, server
    from hpc_bridge.ledger import Ledger, Owner

    app, _rt, runner = await _batch_app(workers=4)                    # capacity 3
    out = await _start(app, n=6)
    runner.futures[0].finish(_ok_report(0))
    await batch.tick(app, out.batch_id)                                # the group id is on the record now
    hidden = runner.futures[1].task_id                                 # row 1 was sent; its id never reached the ledger
    for i in (1, 2):
        app.ledger._con.execute("update task set task_id = null where batch_id = ? and idx = ?", (out.batch_id, i))
    runner.group_tasks.remove(runner.futures[2].task_id)               # row 2 never reached the service at all
    app.ledger.close()
    app2, rt2, runner2 = await _batch_app(workers=4)
    runner2.group_tasks = list(runner.group_tasks)
    runner2.task_group_id = runner.task_group_id
    runner2.rest = {hidden: ("success", _ok_report(1))}
    app2.batch_owner = _owner(pid=os.getpid() + 100_000, host=Owner.here().host)
    app2.ledger = Ledger.open()
    app2.ledger.pid_alive = lambda pid: pid != os.getpid()
    app2.ledger.lease_stale_s = 0.0
    rt2.runner = runner2
    st = await server._status(app2, out.batch_id, detail="page")
    states = {r.index: r.state for r in st.results}
    assert states[1] == "done" and states[2] == "backlog", states   # found in the group; never sent


async def test_a_batch_whose_server_died_reads_paused_before_its_lease_goes_stale():
    """Live 2026-10-31 (batch_restart_resume): the new server's first status, seconds after the kill, read the ledger's
    `admitting` — but nothing can admit for a batch whose server is dead. It answers needs_confirmation, saying the
    lease is taken over once the heartbeat is stale."""
    from hpc_bridge import server
    from hpc_bridge.ledger import Ledger, Owner

    app, _rt, _runner = await _batch_app()
    out = await _start(app, n=3)
    app.ledger.close()
    app2, _rt2, runner2 = await _batch_app()
    app2.batch_owner = _owner(pid=os.getpid() + 100_000, host=Owner.here().host)
    app2.ledger = Ledger.open()
    app2.ledger.pid_alive = lambda pid: pid != os.getpid()            # dead, but its heartbeat is fresh
    st = await server._status(app2, out.batch_id)
    assert st.owner and st.phase == "needs_confirmation" and "gone" in (st.reason or "") and not runner2.futures


async def test_a_reap_the_batch_reported_is_answered_by_one_yes():
    """A batch task that reached another block than the confirmed one refused to run (the wrong-block guard): the block
    was reaped. The batch's own pause names the reap, so the user's yes on the resume is for the new block — it is not
    met with a second question."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app()
    out = await _start(app, n=4)
    run = app.batches[out.batch_id]
    guard = run.guards[(0, 1)]
    runner.futures[0].finish(_Res(86, "", f"HPCB_WRONG_BLOCK {guard.nonce} 999\n"))
    await batch.tick(app, out.batch_id)
    st = await server._status(app, out.batch_id, detail="page")
    assert st.phase == "needs_confirmation" and "reaped" in (st.reason or "")
    assert {r.index: r.state for r in st.results}[0] == "backlog"          # it ran nothing: back in the backlog
    sent = len(runner.futures)
    res = await server._run_batch(app, batch_id=out.batch_id, confirm_spend=True)
    assert res.phase in ("admitting", "waiting"), (res.phase, res.notice)
    for _ in range(3):                                                  # the new block's first check, if not yet in
        if len(runner.futures) > sent:
            break
        app.batches[out.batch_id].last_warm = 0.0
        await batch.tick(app, out.batch_id)
    assert len(runner.futures) > sent, (res.phase, res.notice)
    assert (await server._status(app, out.batch_id)).phase == "admitting"


async def test_reap_evidence_settles_only_the_reaped_blocks_tasks_and_releases_its_user_endpoints_relaunch(monkeypatch):
    """Live 2026-10-31 (batch_block_reaped_midrun): after a reap and the user's yes, a row sent to the NEW block was
    settled lost on the OLD block's evidence (sacct: job 13 CANCELLED), and the reaped block's user endpoint had parsl
    start a pilot for the task that was out on it, which nobody confirmed. Evidence about a job settles only the tasks
    sent to it; that user endpoint's pilots are cancelled by its own marker, never the endpoint-wide one."""
    from hpc_bridge import batch

    uep = "uep." + "1" * 8 + "-1111-1111-1111-" + "1" * 12 + "." + "2" * 8 + "-2222-2222-2222-" + "2" * 12
    cmds: list[str] = []

    def runner_for(_app):
        async def run(cmd):
            cmds.append(cmd)
            if "HPCB_JOB_STATE" in cmd and "-j 13 " in cmd:                     # the reaped block
                return SimpleNamespace(stdout=f"HPCB_JOB_STATE squeue= sacct=CANCELLED pbs= uep={uep}\n")
            if "HPCB_JOB_STATE" in cmd:                                         # the new one runs
                return SimpleNamespace(stdout="HPCB_JOB_STATE squeue=RUNNING sacct=RUNNING pbs= uep=\n")
            return SimpleNamespace(stdout="HPCB_RELEASED 14\n")
        return run

    monkeypatch.setattr(batch, "login_runner", runner_for)
    app, _rt, _runner = await _batch_app(workers=4)                   # capacity 3
    out = await _start(app, n=6)
    run = app.batches[out.batch_id]
    keys = list(run.futures)
    run.sent_job[keys[0]], run.sent_job[keys[1]], run.sent_job[keys[2]] = "13", "13", "15"
    run.block_job = "13"
    app.ledger._con.execute("update task set admitted_at = admitted_at - 600 where batch_id = ?", (out.batch_id,))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id, detail="page")
    states = {r.index: r.state for r in st.results}
    assert states[keys[0][0]] == "lost" and states[keys[1][0]] == "lost" and states[keys[2][0]] == "in_flight"
    assert run.old_uep == uep
    await batch.tick(app, out.batch_id)                                # the reaped user endpoint's relaunch, released
    released = [c for c in cmds if "scancel" in c]
    assert released and all(f"-v m={uep} " in c and "scancel -u" not in c for c in released), released
    assert "14" in app.ledger.batch(out.batch_id)["blocks_seen"]


async def test_a_new_confirmations_config_gets_a_batch_runner_of_its_own():
    """Live 2026-10-31 (batch_block_reaped_midrun): after the reap and the yes, the shape's config had a fresh
    `hpcb_block` (a new user endpoint), but the batch kept sending with its first runner's config — to the reaped block's
    user endpoint. A changed config gets a new runner; the old one's config is a copy, never the shape's own dict."""
    from hpc_bridge import batch

    app, rt, runner = await _batch_app()                               # capacity 2
    made: list[_BatchRunner] = []

    def factory(eid, user_endpoint_config=None, **_kw):
        r = _BatchRunner(canary=runner._canary)
        r.uec = user_endpoint_config
        made.append(r)
        return r

    app.runner_factory = factory
    rt.runner = None
    out = await _start(app, n=4)
    first = app.batches[out.batch_id].runner
    rt.user_endpoint_config["hpcb_block"] = 42                        # a new confirmation's fresh token
    assert first.uec.get("hpcb_block") != 42                           # the runner's config is a copy
    for i, f in enumerate(list(first.futures)):
        f.finish(_ok_report(i))
    await batch.tick(app, out.batch_id)
    run = app.batches[out.batch_id]
    assert run.runner is not first and run.runner.uec.get("hpcb_block") == 42 and run.runner.futures


async def test_the_gate_asks_for_no_block_when_nothing_is_left_to_admit():
    """Live 2026-10-31 (batch_crosses_walltime): the gate closed while the LAST row ran, the batch asked again, and the
    yes started a block nothing ran on. With the backlog empty the gate holds nothing back and asks for nothing."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app(remaining_s=150.0, workers=3)   # capacity 2
    out = await _start(app, n=2)
    runner.futures[0].finish(_ok_report(0, remaining_s=80.0))
    await batch.tick(app, out.batch_id)
    st = await server._status(app, out.batch_id)
    assert st.phase == "admitting" and st.counts.in_flight == 1 and st.counts.backlog == 0


# ---- review round 1 (651c5bc): spend and stop -----------------------------------------------------------------------
# Each of these failed on 651c5bc (red log: scratchpad bp1/red_batch_r1.txt). The reviewers' reproductions, adapted.

_UEP = "uep." + "1" * 8 + "-1111-1111-1111-" + "1" * 12 + "." + "2" * 8 + "-2222-2222-2222-" + "2" * 12


def _login_fake(cmds: list[str], *, sacct: str = "CANCELLED", uep: str = _UEP, released: str = "14"):
    """batch.login_runner for a test: the block's job state (`sacct`), a release of the reaped user endpoint's pilots
    (`released`), and the cancel marker — every command recorded."""
    def runner_for(_app):
        async def run(cmd):
            cmds.append(cmd)
            if "HPCB_JOB_STATE" in cmd:
                return SimpleNamespace(stdout=f"HPCB_JOB_STATE squeue= sacct={sacct} pbs= uep={uep}\n")
            if "HPCB_MARKED" in cmd:
                return SimpleNamespace(stdout="HPCB_MARKED\n")
            return SimpleNamespace(stdout=f"HPCB_RELEASED {released}\n")
        return run
    return runner_for


def _count_switches(run):
    calls: list[bool] = []
    real = run.ensure_up

    async def spy(app_, confirm):
        calls.append(confirm)
        return await real(app_, confirm)

    run.ensure_up = spy
    return calls


async def _at_next_block(app):
    """A batch resumed at the walltime gate (the user's yes covers the NEXT block) while row 1 still runs."""
    from hpc_bridge import batch, server

    app.facility.fresh_ueps = True
    out = await _start(app, n=6)
    runner = app.batches[out.batch_id].runner
    runner.futures[0].finish(_ok_report(0, remaining_s=80.0))
    await batch.tick(app, out.batch_id)
    res = await server._run_batch(app, batch_id=out.batch_id, confirm_spend=True)
    assert res.phase in ("admitting", "waiting")
    run = app.batches[out.batch_id]
    assert run.next_block
    return out, run, runner


async def _restart(app, *, mep: bool = False, workers: int = 3, runner=None):
    """The server is killed: a new process on the same state dir (its own pid; the old one dead), a block confirmed for
    interactive work in the new session, its runner built on the reconnect."""
    from hpc_bridge.ledger import Ledger, Owner

    app.ledger.close()
    app2, rt2, runner2 = await _batch_app(mep=mep, workers=workers, runner=runner)
    app2.batch_owner = _owner(pid=os.getpid() + 100_000, host=Owner.here().host)
    app2.ledger = Ledger.open()
    app2.ledger.pid_alive = lambda pid: pid != os.getpid()
    app2.ledger.lease_stale_s = 0.0
    rt2.runner = runner2
    return app2, rt2, runner2


async def test_a_drain_withdraws_the_next_block():
    """S1. cancel(drain) admits nothing more: a drained batch must not go on to confirm (and start) the next block."""
    from hpc_bridge import batch, server

    app, rt, _runner = await _batch_app(remaining_s=150.0, workers=3)
    out, run, runner = await _at_next_block(app)
    calls = _count_switches(run)
    c = await server._cancel(app, out.batch_id, "drain")
    assert c.status == "draining" and not run.next_block
    runner.futures[1].finish(_ok_report(1, remaining_s=70.0))      # the last row out on the old block finishes
    await batch.tick(app, out.batch_id)
    assert calls == [], "a drained batch asked for (and started) a new billed block"
    assert app.shapes["compute"] is rt


async def test_a_refused_stop_withdraws_the_next_block():
    """S1. stop_endpoint refused (a row holds the block): the user asked to STOP spending. When that row finishes, the
    batch must not drop the shape and confirm a fresh block by itself."""
    from hpc_bridge import batch, server

    app, rt, _runner = await _batch_app(remaining_s=150.0, workers=3)
    out, run, runner = await _at_next_block(app)
    calls = _count_switches(run)
    st = await server._stop_endpoint(app)
    assert st.status == "up" and out.batch_id in (st.notice or "")   # refused: the row holds the block
    runner.futures[1].finish(_ok_report(1, remaining_s=70.0))
    await batch.tick(app, out.batch_id)
    assert calls == [] and app.shapes["compute"] is rt, "after a stop the batch confirmed a new billed block by itself"


async def test_a_mep_abort_marker_is_never_a_submit_to_an_unconfirmed_block():
    """S2. On a MEP the CANCELLED marker is a TASK. Once the block's spend is no longer confirmed (it idled out, was
    reaped), that task would revive the user endpoint and start a block nobody confirmed (#204's pattern): it is never
    sent, and the answer says the tasks out were not reached."""
    from hpc_bridge import batch, server, warmth

    app, rt, runner = await _batch_app(mep=True, workers=3)
    runner.fail_submit_of = {0}
    out = await _start(app, n=1)
    await batch.tick(app, out.batch_id)
    assert app.ledger.counts(out.batch_id)["submit_unknown"] == 1
    warmth._mark_reaped(app, rt, "the facility idle-released the block", None)
    sent = len(runner.commands)
    c = await server._cancel(app, out.batch_id, "abort")
    assert len(runner.commands) == sent, f"abort submitted {runner.commands[sent:]!r} to an unconfirmed MEP block"
    assert "not written" in (c.notice or "") and "kills its command" not in (c.notice or ""), c.notice


async def test_a_mep_abort_sends_one_marker_task_at_most():
    """S2. On a live, confirmed MEP block with a worker to spare the marker is one small task — and only one: an abort
    called again while it is out sends no other."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app(mep=True, workers=3)          # capacity 2: a worker kept free
    out = await _start(app, n=4)
    await server._cancel(app, out.batch_id, "abort")
    await server._cancel(app, out.batch_id, "abort")
    markers = [c for c in runner.commands if batch.CANCEL_MARKER in c and "HPCB_MARKED" in c]
    assert len(markers) == 1, markers


async def test_a_stop_with_an_unknown_submit_is_draining_and_names_it(monkeypatch):
    """S3. A submit error leaves the row submit_unknown: the POST may have reached the service (S4), so the task may be
    queued, and the endpoint can start a block for it. The stop is never `down` while one exists: `draining`, naming
    the batch and what settles it."""
    from hpc_bridge import batch, server
    from hpc_bridge.models import ShellOutcome

    app, _rt, runner = await _batch_app(workers=3)
    runner.fail_submit_of = {0}
    out = await _start(app, n=1)
    await batch.tick(app, out.batch_id)
    assert app.ledger.counts(out.batch_id)["submit_unknown"] == 1
    note = batch.unknown_note(app)
    assert note is not None and out.batch_id in note

    async def fake_run_shell(a, command, session_id="default", shape="compute", **_kw):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 101\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    st = await server._stop_endpoint(app)
    assert st.status == "draining" and out.batch_id in (st.notice or "") and not st.spend_stopped, st


async def test_a_mep_stop_names_an_unknown_submit():
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app(mep=True, workers=3)
    runner.fail_submit_of = {0}
    out = await _start(app, n=1)
    await batch.tick(app, out.batch_id)
    st = await server._stop_endpoint(app)
    assert st.status == "draining" and out.batch_id in (st.notice or "") and "unknown" in (st.notice or ""), st


async def test_a_non_idempotent_unknown_submit_is_never_resent_nor_called_lost_on_absence(monkeypatch):
    """S3, as corrected in review round 2: the groups the client learnt listing no task for a failed submit is NO
    evidence (its POST may have minted a group of its own). The row stays submit_unknown, is never sent again, and
    keeps holding the stop."""
    from hpc_bridge import batch

    clock = [1000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    app, _rt, runner = await _batch_app(workers=3)
    runner.fail_submit_of = {0}
    out = await _start(app, n=2)
    runner.futures[1].finish(_ok_report(1))
    await batch.tick(app, out.batch_id)
    assert app.ledger.task(out.batch_id, 0)["state"] == "submit_unknown"
    clock[0] += batch.UNKNOWN_SETTLE_S + 1
    await batch.tick(app, out.batch_id)
    t = app.ledger.task(out.batch_id, 0)
    assert t["state"] == "submit_unknown" and t["unheld"] is None, t
    assert sum("/tasks/0/attempt-" in c for c in runner.commands) == 1
    assert batch.unknown_note(app) is not None


async def test_reap_release_never_cancels_the_new_confirmed_block_on_the_same_user_endpoint(monkeypatch):
    """S4. On an SSH endpoint without fresh user endpoints, the block confirmed after a reap goes to the SAME user
    endpoint (the same config): its pilot carries the same uep.<eid>.<uep id> marker. The release must not scancel the
    block the user just confirmed."""
    from hpc_bridge import batch, server

    cmds: list[str] = []
    monkeypatch.setattr(batch, "login_runner", _login_fake(cmds, released="20"))
    clock = [10_000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    app, rt, _runner = await _batch_app(workers=3)
    app.facility.fresh_ueps = False
    out = await _start(app, n=4)
    h0 = batch.uec_hash(rt)
    run = app.batches[out.batch_id]
    clock[0] += 60
    await batch.tick(app, out.batch_id)                      # sacct: CANCELLED -> reap, old_uep recorded
    assert run.old_uep == _UEP
    res = await server._run_batch(app, batch_id=out.batch_id, confirm_spend=True)
    assert batch.uec_hash(app.shapes["compute"]) == h0       # the same config: the same user endpoint
    cmds.clear()
    clock[0] += 20
    await batch.tick(app, out.batch_id)
    assert not any("scancel" in c and _UEP in c for c in cmds), (
        f"the release scancels the user endpoint the user's yes just asked a block of ({res.phase})")


async def test_the_reaped_user_endpoint_is_released_without_a_status_call(monkeypatch):
    """S5. Between the reap and the user's yes (when the relaunched, unconfirmed pilot bills), the background loop
    keeps releasing the reaped user endpoint's pilots for its window — no status or cancel call needed."""
    import asyncio

    from hpc_bridge import batch

    cmds: list[str] = []
    monkeypatch.setattr(batch, "login_runner", _login_fake(cmds))
    monkeypatch.setattr(batch, "TICK_S", 0.01)
    monkeypatch.setattr(batch, "IDLE_TICK_S", 0.01)
    monkeypatch.setattr(batch, "RELEASE_EVERY_S", 0.0)
    app, _rt, _runner = await _batch_app(workers=3)
    out = await _start(app, n=4)
    run = app.batches[out.batch_id]
    app.ledger._con.execute("update task set admitted_at = admitted_at - 600 where batch_id = ?", (out.batch_id,))
    app.batch_autoloop = True
    batch._start_loop(app, run)
    try:
        for _ in range(100):
            await asyncio.sleep(0.02)
            if run.loop.done():
                break
        assert run.old_uep == _UEP
        assert not (run.loop.done() and batch._now() < run.old_uep_until), (
            "the loop exited at the reap pause: the old user endpoint's relaunched pilot is released only if someone "
            "happens to call status/cancel")
        assert sum("scancel" in c and _UEP in c for c in cmds) >= 2
    finally:
        run.loop.cancel()


async def test_abort_after_a_restart_does_not_claim_a_marker_it_never_wrote():
    """S6. After a restart on a MEP the batch has no runner on a confirmed block of its own: the marker cannot be
    written, and the answer says so — never that the wrappers see it."""
    from hpc_bridge import server

    app, _rt, _runner = await _batch_app(mep=True, workers=3)
    out = await _start(app, n=4)
    app2, _rt2, runner2 = await _restart(app, mep=True)
    c = await server._cancel(app2, out.batch_id, "abort")
    wrote = any("CANCELLED" in cmd for cmd in runner2.commands)
    assert c.in_flight == 2 and not wrote
    assert "sees the CANCELLED marker" not in (c.notice or "") and "not written" in (c.notice or ""), c.notice


async def test_a_one_worker_mep_block_gets_no_marker_task_and_says_so():
    """S6. On a one-worker MEP block the batch's task holds the only worker: a marker task would wait behind the very
    row it is meant to end. None is sent; the answer says the row runs to its end."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app(mep=True, workers=1)
    out = await _start(app, n=3)
    c = await server._cancel(app, out.batch_id, "abort")
    assert not any(batch.CANCEL_MARKER in cmd and "HPCB_MARKED" in cmd for cmd in runner.commands)
    assert c.status == "draining" and "one worker" in (c.notice or "") and "not written" in (c.notice or ""), c.notice


async def test_restart_does_not_resume_without_confirm_spend():
    """S7 (rule 4). A batch whose server died while admitting: run_batch(batch_id=…) WITHOUT confirm_spend must not
    resume it, even when the new session has a confirmed block for interactive work."""
    from hpc_bridge import batch, server

    app, _rt, first = await _batch_app()
    out = await _start(app, n=6)
    app2, _rt2, runner2 = await _restart(app)
    runner2.rest = {f.task_id: ("success", _ok_report(i)) for i, f in enumerate(first.futures)}  # rows 0, 1 finished
    res = await server._run_batch(app2, batch_id=out.batch_id)   # no confirm_spend
    await batch.tick(app2, out.batch_id)
    assert not runner2.futures, f"resumed after a restart without the user's yes ({res.phase}: {res.notice})"
    assert res.phase == "needs_confirmation" and "restart" in (res.reason or ""), res


async def test_mep_cancel_after_a_stop_still_names_the_draining_block():
    """S8. On a MEP the stop leaves the block draining (no cancel channel): a cancel after it must still name it in
    blocks_left — never `down` with nothing left billing."""
    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app(mep=True, workers=3)
    out = await _start(app, n=6)
    await server._cancel(app, out.batch_id, "drain")
    for i, f in enumerate(list(runner.futures)):
        f.finish(_ok_report(i))
    await batch.tick(app, out.batch_id)
    st = await server._stop_endpoint(app)
    assert st.status == "draining", st
    c = await server._cancel(app, out.batch_id, "drain")
    assert c.blocks_left and any(b.job_id == "101" for b in c.blocks_left), c


async def test_a_block_a_switch_left_is_named_in_blocks_left_and_banked_to_its_end():
    """S8. The next-block switch drops the old block's shape; that block runs out its last minutes on its own. A cancel
    names it in blocks_left, and its spend is banked to its end, not only to the switch."""
    from hpc_bridge import batch, server

    app, rt, _runner = await _batch_app(remaining_s=150.0, workers=3)
    app.charge_factor = 1.0
    out, _run, runner = await _at_next_block(app)
    before = app.released_spend
    runner.futures[1].finish(_ok_report(1, remaining_s=70.0))
    await batch.tick(app, out.batch_id)
    assert app.shapes["compute"] is not rt                             # switched
    # banked to its end: its ~70 s left (a node-hour each 3600 s) on top of whatever ran to now
    assert app.released_spend - before >= 60 / 3600, (before, app.released_spend)
    c = await server._cancel(app, out.batch_id, "drain")
    assert any(b.job_id == "101" for b in c.blocks_left), c.blocks_left


async def test_cancel_runs_under_the_heartbeat_and_status_and_cancel_are_bounded(monkeypatch):
    """(Plausible.) cancel keeps the client's progress heartbeat as status does, and one call of either stays inside
    its budget however long a pass's steps could take: the long ones are left to the background loop."""
    import asyncio

    from hpc_bridge import batch, server

    used: list[str] = []

    async def spy(ctx, work, label):
        used.append(label)
        return await work

    monkeypatch.setattr(server, "_heartbeat", spy)
    app, _rt, _runner = await _batch_app()
    out = await _start(app, n=4)
    ctx = SimpleNamespace(request_context=SimpleNamespace(lifespan_context=app))
    await server.cancel_tool(ctx, out.batch_id, "drain")
    assert "cancel" in used

    async def slow_switch(app_, run):
        await asyncio.sleep(30)

    monkeypatch.setattr(batch, "CALL_BUDGET_S", 1.0)
    monkeypatch.setattr(batch, "_maybe_switch", slow_switch)
    app2, _rt2, _runner2 = await _batch_app()
    out2 = await _start(app2, n=4)
    t0 = time.monotonic()
    await server._status(app2, out2.batch_id)
    assert time.monotonic() - t0 < 5


# ---- review round 1 (651c5bc): the ledger and the wrapper -------------------------------------------------------------

_END = str(int(time.time()) + 900)
_ENV = {"SLURM_JOB_ID": "101", "SLURM_JOB_END_TIME": _END}


class _BashRunner(_BatchRunner):
    """Runs each submitted script locally, the way ShellFunction would (brace-escaped, then .format(), then bash) — at
    once (`live=False`) or in a thread, its future finishing when the script exits (`live=True`)."""

    def __init__(self, home, *, live: bool = False, **kw):
        super().__init__(**kw)
        self.live = live
        self.env = {"PATH": os.environ["PATH"], "HOME": str(home), "TMPDIR": str(home), **_ENV}

    def submit(self, command):
        from hpc_bridge.runner import _escape_for_shellfunction

        fut = super().submit(command)
        script = _escape_for_shellfunction(command).format()

        def run():
            p = subprocess.run([_BASH, "-c", script], capture_output=True, text=True, env=self.env, timeout=120)
            fut.finish(_Res(p.returncode, p.stdout, p.stderr))

        if self.live:
            threading.Thread(target=run, daemon=True).start()
        else:
            run()
        return fut


def _canary():
    return CanaryResult(ok=True, worker_host="c1", worker_python="3.12.3", worker_dill="0.4.0", worker_job="101",
                        worker_remaining_s=900.0)


def _bash(script, tmp, timeout=60):
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp), "TMPDIR": str(tmp), **_ENV}
    return subprocess.run([_BASH, "-c", script], capture_output=True, text=True, env=env, timeout=timeout)


@pytest.mark.skipif(_BASH is None, reason="needs bash")
async def test_a_second_batch_into_the_same_results_dir_runs_its_rows(tmp_path):
    """L1. The attempt claim is scoped by the batch id: a second batch in the same results_dir runs its rows (it was
    wedged in submit_unknown, every claim read as a duplicate)."""
    from hpc_bridge import batch, server

    runner = _BashRunner(tmp_path, canary=_canary())
    app, _rt, _ = await _batch_app(runner=runner)
    rd = str(tmp_path / "out")
    a = await server._run_batch(app, command="echo one {x}", rows=[{"x": 0}, {"x": 1}], task_walltime_s=60,
                                results_dir=rd)
    for _ in range(3):
        await batch.tick(app, a.batch_id)
    assert app.ledger.counts(a.batch_id)["done"] == 2
    b = await server._run_batch(app, command="echo two {x}", rows=[{"x": 0}, {"x": 1}], task_walltime_s=60,
                                results_dir=rd)
    for _ in range(3):
        await batch.tick(app, b.batch_id)
    c = app.ledger.counts(b.batch_id)
    assert c["done"] == 2, c
    assert b.results_dir == f"{rd}/{b.batch_id}"


@pytest.mark.skipif(_BASH is None, reason="needs bash")
async def test_an_old_cancel_marker_does_not_cancel_a_new_batch_in_the_same_dir(tmp_path):
    """L1. An aborted batch's CANCELLED marker is its own: a later batch in the same results_dir runs."""
    from hpc_bridge import batch, server

    runner = _BashRunner(tmp_path, canary=_canary())
    app, _rt, _ = await _batch_app(runner=runner)
    rd = tmp_path / "out"
    (rd).mkdir()
    (rd / batch.CANCEL_MARKER).touch()                                   # a marker at the old, unscoped place
    a = await server._run_batch(app, command="true", rows=[1], task_walltime_s=60, results_dir=str(rd))
    (Path(a.results_dir) / batch.CANCEL_MARKER).touch()                  # and the batch's own: a's alone
    b = await server._run_batch(app, command="echo {row}", rows=[7], task_walltime_s=60, results_dir=str(rd))
    for _ in range(3):
        await batch.tick(app, b.batch_id)
    assert app.ledger.counts(b.batch_id)["done"] == 1


@pytest.mark.skipif(_BASH is None or os.geteuid() == 0, reason="needs bash, and a user a chmod binds")
async def test_an_unwritable_results_dir_fails_the_row_with_its_cause(tmp_path, monkeypatch):
    """L1. A claim that fails for any reason but an existing attempt (EACCES here; ENOSPC, EDQUOT on a full disk) fails
    the row with that cause — it is no duplicate, and an idempotent row is not sent again and again."""
    from hpc_bridge import batch, server

    monkeypatch.setattr(batch, "_new_id", lambda: "batch-00000000beef")
    d = tmp_path / "out" / "batch-00000000beef" / "tasks" / "0"
    d.mkdir(parents=True)
    d.chmod(0o555)                                  # mkdir attempt-1 fails: EACCES
    runner = _BashRunner(tmp_path, canary=_canary())
    app, _rt, _ = await _batch_app(runner=runner)
    try:
        out = await server._run_batch(app, command="echo {x}", rows=[{"x": 0}], task_walltime_s=60,
                                      results_dir=str(tmp_path / "out"), idempotent=True)
        for _ in range(6):
            await batch.tick(app, out.batch_id)
        sends = sum("/tasks/0/attempt-" in c for c in runner.commands)
        t = app.ledger.task(out.batch_id, 0)
        assert sends == 1 and t["state"] == "failed", (sends, t["state"], t["attempt"])
        assert "Permission denied" in (t["settled_by"] or "") + (t["evidence"] or ""), t
    finally:
        d.chmod(0o755)


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_too_little_free_space_is_found_before_the_claim(tmp_path):
    """L1. The free-space check comes before the claim: a full disk fails the row (rc 75) without leaving an attempt
    directory that a retry would read as a duplicate."""
    from hpc_bridge import batch

    rd = tmp_path / "rd"
    s = batch.task_script("echo hi", results_dir=str(rd), index=0, attempt=1, task_walltime_s=30,
                          min_free_mb=10 ** 9)
    r = batch.parse_result(_bash(s, tmp_path).stdout)
    assert r is not None and r.no_space and r.rc == batch.NO_SPACE_RC
    assert not (rd / "tasks" / "0" / "attempt-1").exists()


async def test_the_sdk_result_watcher_quitting_keeps_the_rows_in_flight(monkeypatch):
    """L2. SDK 4.18's "SDK thread quit unexpectedly." (its AMQP thread gave up: a laptop asleep, a network down) is set
    on every open future — the tasks themselves run on in the block. No row is failed on it and no slot refilled: the
    runner is rebuilt, and the results are recovered by REST."""
    from hpc_bridge import batch

    clock = [1000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    app, _rt, runner = await _batch_app(workers=3)                     # capacity 2
    out = await _start(app, n=6)
    first = list(runner.futures)
    for f in first:
        f.fail(RuntimeError("SDK thread quit unexpectedly."))
    await batch.tick(app, out.batch_id)
    c = app.ledger.counts(out.batch_id)
    assert c["failed"] == 0 and c["in_flight"] == 2 and len(runner.futures) == 2, (c, len(runner.futures))
    runner.rest[first[0].task_id] = ("success", _ok_report(0))
    clock[0] += batch.REST_EVERY_S + 1
    await batch.tick(app, out.batch_id)
    assert app.ledger.task(out.batch_id, 0)["state"] == "done"
    assert len(runner.futures) == 3                                    # the freed slot, refilled once, on a result


@pytest.mark.skipif(_BASH is None, reason="needs bash")
@pytest.mark.parametrize("value", ["$(touch {m})", "a;touch {m}", "`touch {m}`", "a\ntouch {m}",
                                   "$'\\x41'; touch {m}", "\" ; touch {m} ; \"", "' ; touch {m} ; '", "{y}"])
def test_a_row_value_never_runs_as_code(tmp_path, value):
    """L3 (security). A row value reaches the command as data, never as shell: it is passed in an environment variable
    and the placeholder becomes a reference to it, whatever the value holds — command substitution, a separator,
    backticks, a newline, ANSI-C quoting, quotes, or another placeholder's name."""
    from hpc_bridge import batch

    m = tmp_path / "pwned"
    row = {"x": value.replace("{m}", str(m)), "y": f"$(touch {m})"}
    names = {"x", "y", "index"}
    cmd = batch.substitute("printf '%s|' {x}", row, 0, names)
    s = batch.task_script(cmd, results_dir=str(tmp_path / "rd"), index=0, attempt=1, task_walltime_s=30,
                          fields=batch.row_values(row, 0, names))
    r = batch.parse_result(_bash(s, tmp_path).stdout)
    assert not m.exists(), cmd
    assert r is not None and r.rc == 0 and r.tail == row["x"] + "|", (r and r.tail)


@pytest.mark.parametrize("template", ['echo "{x}"', "echo '{x}'", "cat <<EOF\n{x}\nEOF", "echo $'{x}'",
                                      "cat <<-'X'\n\t{x}\n\tX"])
def test_a_placeholder_inside_quotes_or_a_heredoc_is_refused(template):
    """L3. A placeholder inside quotes or a here-document is refused, saying how to write it (bare: each value reaches
    the command as one word)."""
    from hpc_bridge import batch

    with pytest.raises(batch.BatchRefusal, match="bare"):
        batch.validate(template, [{"x": 1}], 10)


def test_braces_that_are_no_placeholder_are_left_as_written():
    from hpc_bridge import batch

    t = "echo ${HOME} out_{x}.txt '{print $1}' \"{other}\" \\{x}; cat <<EOF\n{other}\nEOF\necho {x}"
    cmd, _rows, _wall, names = batch.validate(t, [{"x": 1}], 10)
    got = batch.substitute(cmd, {"x": 1}, 0, names)
    assert got == ("echo ${HOME} out_\"$HPCB_F_x\".txt '{print $1}' \"{other}\" \\{x}; cat <<EOF\n{other}\nEOF\n"
                   "echo \"$HPCB_F_x\"")


async def test_mep_restart_presumption_still_runs_so_stop_is_not_refused_forever(monkeypatch):
    """L4. After a restart on a MEP, the presumption counts from the ledger's record that a block was seen (this process
    never saw one): the rows in flight are presumed lost in time, and a stop is no longer refused for good."""
    from hpc_bridge import batch, server

    clock = [1000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    app, _rt, _runner = await _batch_app(mep=True, workers=3)          # capacity 2, block 101 seen by the canary
    out = await _start(app, n=4)
    app2, _rt2, _runner2 = await _restart(app, mep=True)               # the service still lists both as pending
    clock[0] += 60 + batch.PRESUME_GRACE_S + 3600
    await server._status(app2, out.batch_id)
    await server._cancel(app2, out.batch_id, "abort")
    for _ in range(3):
        clock[0] += 60
        await batch.tick(app2, out.batch_id)
    assert batch.in_flight_note(app2) is None, app2.ledger.counts(out.batch_id)


class _NoIdYet(_BatchRunner):
    """The SDK has queued the submit but its batch_run answer (task id, task group) has not come back yet."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.task_group_id = None

    def submit(self, command):
        fut = super().submit(command)
        self.group_tasks.remove(fut.task_id)
        fut.task_id = None
        return fut


async def test_ssh_restart_before_the_first_answer_does_not_wedge_rows_in_flight(monkeypatch):
    """L4. The server died before the service's first answer: no task id, no task group. Those rows still have a path —
    presumed lost once task_walltime_s + the grace have passed since a block was seen."""
    from hpc_bridge import batch, server

    cmds: list[str] = []
    monkeypatch.setattr(batch, "login_runner", _login_fake(cmds, sacct="RUNNING"))
    clock = [1000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    app, _rt, _ = await _batch_app(workers=3, runner=_NoIdYet(canary=_canary()))
    out = await _start(app, n=4)                                         # the server dies within its first 2 s
    app2, _rt2, _runner2 = await _restart(app)
    await server._status(app2, out.batch_id)                            # its heartbeat is fresh: a read
    await server._cancel(app2, out.batch_id, "abort")                   # recorded, for the holder to apply
    for _ in range(4):
        clock[0] += 3600
        await server._status(app2, out.batch_id)                        # as the stop's refusal says: status collects
    assert batch.in_flight_note(app2) is None, app2.ledger.counts(out.batch_id)
    assert app2.ledger.counts(out.batch_id)["presumed_lost"] == 2 and app2.ledger.batch(out.batch_id)["state"] == \
        "cancelled"


async def test_ssh_evidence_after_a_restart_settles_rows_this_process_never_sent(monkeypatch):
    """L4. SSH evidence reads the ledger's rows, not this process's futures: after a restart, sacct saying the job they
    were sent to ended settles them lost on that evidence."""
    from hpc_bridge import batch, server

    cmds: list[str] = []
    monkeypatch.setattr(batch, "login_runner", _login_fake(cmds, sacct="CANCELLED"))
    clock = [1000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    app, _rt, _runner = await _batch_app(workers=3)
    out = await _start(app, n=4)
    app2, _rt2, _runner2 = await _restart(app)
    clock[0] += 120
    await server._status(app2, out.batch_id)
    await batch.tick(app2, out.batch_id)
    rows = app2.ledger.tasks(out.batch_id, states=("lost",))
    assert len(rows) == 2 and all("job 101" in (r["settled_by"] or "") for r in rows), rows
    assert any("-j 101" in c for c in cmds)


async def test_submit_unknown_is_not_resent_while_its_copy_in_an_old_group_is_pending():
    """L5. Reconciliation reads EVERY task group the batch used: an idempotent submit_unknown row is not sent again
    while its first copy, in the old runner's group, may still run."""
    from hpc_bridge import batch

    app, rt, runner = await _batch_app(workers=3)                       # capacity 2
    made = []

    def factory(eid, user_endpoint_config=None, **_kw):
        r = _BatchRunner(canary=runner._canary)
        made.append(r)
        return r

    app.runner_factory = factory
    out = await _start(app, n=6, idempotent=True)
    first = app.batches[out.batch_id].runner
    f0 = first.futures[0]
    f0.task_id = None                                                   # the POST's answer was lost: S4
    app.ledger._con.execute("update task set task_id = null where batch_id = ? and idx = 0", (out.batch_id,))
    f0.fail(ConnectionError("the response to the batch_run POST was lost"))   # yet the task IS in first's group
    await batch.tick(app, out.batch_id)
    assert app.ledger.task(out.batch_id, 0)["state"] == "submit_unknown"
    rt.user_endpoint_config["hpcb_block"] = 42                           # a new confirmation (reap + yes, next block)
    first.futures[1].finish(_ok_report(1))
    for _ in range(3):
        await batch.tick(app, out.batch_id)
    second = app.batches[out.batch_id].runner
    assert second is not first
    assert not any("/tasks/0/attempt-2" in c for c in second.commands), "row 0 resent while its first copy may run"
    assert set(app.ledger.task_groups(out.batch_id)) == {first.task_group_id, second.task_group_id}


async def test_the_next_block_waits_for_an_unknown_submit():
    """L5. The switch to the next block waits for a submit of unknown fate on the block in front, as for a task out."""
    from hpc_bridge import batch, server

    app, rt, runner = await _batch_app(remaining_s=150.0, workers=3)
    app.facility.fresh_ueps = True
    runner.fail_submit_of = {1}
    out = await _start(app, n=6)
    await batch.tick(app, out.batch_id)
    runner.futures[0].finish(_ok_report(0, remaining_s=80.0))
    await batch.tick(app, out.batch_id)
    await server._run_batch(app, batch_id=out.batch_id, confirm_spend=True)
    await batch.tick(app, out.batch_id)
    assert app.ledger.counts(out.batch_id)["submit_unknown"] == 1
    assert app.shapes["compute"] is rt, "switched blocks under a submit of unknown fate"


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_a_backgrounded_child_does_not_hold_the_task_past_its_walltime(tmp_path):
    """L6. A command that leaves a background child holding its stdout: the task still ends with its command (the
    child, of the command's own process group, is ended with it) and prints its result line."""
    from hpc_bridge import batch

    s = batch.task_script("sleep 25 & echo started", results_dir=str(tmp_path / "rd"), index=0, attempt=1,
                          task_walltime_s=2)
    t0 = time.monotonic()
    r = batch.parse_result(_bash(s, tmp_path).stdout)
    assert r is not None and r.rc == 0 and r.tail == "started\n" and time.monotonic() - t0 < 12, time.monotonic() - t0


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_the_stderr_tail_is_the_streams_real_end_past_the_cap(tmp_path):
    """L6. stderr's tail past the cap is the stream's real end, as stdout's is — not the head's last kilobyte."""
    from hpc_bridge import batch

    s = batch.task_script("head -c 1500000 /dev/zero | tr '\\0' y >&2; echo >&2; echo FINAL-ERROR >&2; exit 2",
                          results_dir=str(tmp_path / "rd"), index=0, attempt=1, task_walltime_s=30)
    r = batch.parse_result(_bash(s, tmp_path).stdout)
    assert r is not None and r.rc == 2 and r.stderr_tail.endswith("FINAL-ERROR\n"), r and r.stderr_tail[-40:]


def test_a_reused_pid_does_not_hold_a_dead_servers_lease_forever(tmp_path):
    """L7. The lease's identity carries the holder's process start time: a dead server's pid handed on to another
    process does not hold the lease."""
    from hpc_bridge.ledger import Ledger, Owner

    led = Ledger.open(tmp_path / "state")
    here = Owner.here()
    dead = subprocess.Popen(["true"])
    dead.wait()
    form = (here.started or "psutc:").split(":", 1)[0]               # the form this reader compares in
    led.add_batch("batch-0000000000aa", command="echo {x}", rows=[{"x": 1}], task_walltime_s=60, capacity=1,
                  results_dir="/scratch/x", idempotent=False, endpoint_id="e", uec_hash="u", session_id="default",
                  owner=Owner(here.host, dead.pid, started=f"{form}:an hour ago"), now=time.time() - 3600)
    squatter = subprocess.Popen(["sleep", "30"])                         # started an hour after the last heartbeat
    try:
        led._con.execute("update batch set owner_pid = ?", (squatter.pid,))   # the OS handed the dead pid on
        assert led.take_lease("batch-0000000000aa", here).ok
    finally:
        squatter.kill()
        squatter.wait()


def test_a_dead_holder_recorded_under_another_hostname_is_taken_over(tmp_path):
    """L7. The ledger is on a local disk: a holder recorded under a hostname that has since changed (macOS renames
    itself on a new network; a container's name) is this machine's — dead, it is taken over."""
    from hpc_bridge.ledger import Ledger, Owner

    led = Ledger.open(tmp_path / "state")
    dead = subprocess.Popen(["true"])
    dead.wait()
    led.add_batch("batch-0000000000ab", command="true", rows=[1], task_walltime_s=60, capacity=1,
                  results_dir="/scratch/x", idempotent=False, endpoint_id="e", uec_hash="u", session_id="default",
                  owner=Owner("old-name.local", dead.pid), now=time.time() - 3600)
    assert led.take_lease("batch-0000000000ab", Owner.here()).ok
    live = subprocess.Popen(["sleep", "30"])
    try:
        led.add_batch("batch-0000000000ac", command="true", rows=[1], task_walltime_s=60, capacity=1,
                      results_dir="/scratch/x", idempotent=False, endpoint_id="e", uec_hash="u", session_id="default",
                      owner=Owner("old-name.local", live.pid), now=time.time() - 3600)
        assert not led.take_lease("batch-0000000000ac", Owner.here()).ok   # alive: never taken
    finally:
        live.kill()
        live.wait()


async def test_wait_s_waits_when_nothing_changes():
    """L8. status(wait_s) waits for something to change — a row settling, the phase — or until wait_s is up; a pass
    that changed nothing does not end it."""
    import asyncio

    from hpc_bridge import batch, server

    app, _rt, _runner = await _batch_app()
    out = await _start(app, n=4)
    app.batch_autoloop = True
    batch._start_loop(app, app.batches[out.batch_id])
    await asyncio.sleep(0.1)
    try:
        t0 = time.monotonic()
        await server._status(app, out.batch_id, wait_s=4)
        took = time.monotonic() - t0
    finally:
        app.batches[out.batch_id].loop.cancel()
    assert took >= 3.5, f"status(wait_s=4) returned after {took:.1f} s with nothing settled"


async def test_wait_s_returns_when_a_row_settles():
    import asyncio

    from hpc_bridge import batch, server

    app, _rt, runner = await _batch_app()
    out = await _start(app, n=4)
    app.batch_autoloop = True
    batch._start_loop(app, app.batches[out.batch_id])
    loop = asyncio.get_running_loop()
    loop.call_later(0.5, lambda: runner.futures[0].finish(_ok_report(0)))
    try:
        t0 = time.monotonic()
        st = await server._status(app, out.batch_id, wait_s=20)
        took = time.monotonic() - t0
    finally:
        app.batches[out.batch_id].loop.cancel()
    assert st.counts.done == 1 and took < 5, (took, st.counts)


# ---- review round 2 (ff57dcf) ---------------------------------------------------------------------------------------
# Each of these failed on ff57dcf (red log: scratchpad bp1/red_batch_r2.txt). The reviewer's reproductions, adapted.

# 1. A row's value never runs as code: the detector refuses wherever it is not certain a placeholder is a bare word.

_REFUSED_TEMPLATES = [
    'bash -c x\\ #"\necho {x}\n"',                 # an escaped blank before `#`: no comment to bash
    '(true)# "\nbash -c "\necho {x}\n"',           # `)#`: a comment to bash
    "(true)# it's\nbash <<EOF # '\necho {x}\nEOF",
    'echo a >/dev/null #ok\nbash -c "echo {x}"',
    "eval {x}", "eval echo {x}", "if true; then eval echo {x}; fi",
    "bash -c {x}", "sh -lc {x}", "env bash -c {x}", "timeout 5 sh -c -x {x}", "sudo -u u bash -c pre{x}",
    "python3 -c {x}", "perl -e {x}", "node -e {x}", "ssh host echo {x}", "watch echo {x}",
    "[[ {x} -eq 1 ]]", "(( {x} > 1 ))", "y=$(( {x} + 1 ))", "for ((i=0; i<{x}; i++)); do :; done", "y=$[{x}+1]",
    "a[{x}]=1", "echo ${arr[{x}]}", "echo ${v:{x}}", "read {x}", "printf -v {x} %s 1", "declare {x}=1",
    "trap {x} EXIT", "echo `echo {x}`", "test -v {x}", "[ {x} -eq 1 ]", "let n={x}",
    "awk {x} file", "sed {x} f", "sed -e {x} f",
    'echo "$(sh -c "echo {x}")"', 'echo "$(case a in a) sh -c "{x}";; esac)"',
    'bash -c "echo $HPCB_F_x"', 'python3 <<EOF\nprint("$HPCB_F_x")\nEOF', "echo 'unclosed {x}",
]


@pytest.mark.parametrize("template", _REFUSED_TEMPLATES)
def test_a_placeholder_the_outer_shell_could_read_as_code_is_refused(template):
    """1 (security). Refused wherever this reader cannot vouch that `{x}` is a bare word of a command the outer shell runs
    as data: quotes and here-documents (also when a `#` bash reads otherwise hides them), arithmetic, `[[ ]]`, `${…}`,
    subscripts, backticks, a shell's `-c`, an interpreter's code flag, awk/sed's program, ssh/watch, eval-like
    commands — and a row's variable written straight into such a string."""
    from hpc_bridge import batch

    with pytest.raises(batch.BatchRefusal):
        batch.validate(template, [{"x": "1"}], 10)


@pytest.mark.parametrize("template", [
    'bash -c x\\ #"\necho {x}\n"',          # an escaped blank before `#`: no comment to bash, one to the detector
    '(true)# "\nbash -c "\necho {x}\n"',    # `)#`: a comment to bash, none to the detector
])
@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_a_placeholder_bash_reads_inside_quotes_is_refused_or_never_runs(tmp_path, template):
    """1, the reviewer's: the detector's comment rule disagreed with bash's, so a quote it never saw opened a `bash -c`
    string around the placeholder, and the row's value ran as code."""
    from hpc_bridge.batch import BatchRefusal, substitute

    pwn = tmp_path / "PWNED"
    val = f";touch${{IFS}}{pwn};"
    try:
        s = substitute(template, {"x": val}, 0, {"x", "index"})
    except BatchRefusal:
        return
    subprocess.run([_BASH, "-c", s], env=dict(os.environ, HPCB_F_x=val), capture_output=True, cwd=tmp_path,
                   timeout=30)
    assert not pwn.exists(), "the row's value ran as code"


_ALLOWED_TEMPLATES = [
    "printf '%s|' {x}",
    'out="$(printf %s {x})"; printf \'%s|\' "$out"',
    "case {x} in zzz) echo no;; esac; printf '%s|' {x}",
    "for v in $(printf %s {x}); do :; done; printf '%s|' {x}",
    "cd . && printf '%s|' {x} # it's fine",
    "bash -c 'printf \"%s|\" \"$1\"' _ {x}",                       # the advice: the value as an argument
    "python3 - <<'EOF'\nimport os, sys\nsys.stdout.write(os.environ[\"HPCB_F_x\"] + \"|\")\nEOF",   # the advice
    "bash <<'EOF'\nprintf '%s|' \"$HPCB_F_x\"\nEOF",                  # the advice: a quoted delimiter
    "awk -v n=1 'BEGIN{}' </dev/null; printf '%s|' {x}",
    "f=out_{x}.txt; printf '%s|' {x}",
]
_ADVERSARIAL_VALUES = ["$(touch {m})", "`touch {m}`", ";touch {m};", "a[$(touch {m})]", "\"; touch {m}; \"",
                       "'); __import__('os').system('touch {m}'); ('", "x\ntouch {m}", "$'\\x41'"]


@pytest.mark.skipif(_BASH is None, reason="needs bash")
@pytest.mark.parametrize("template", _ALLOWED_TEMPLATES)
def test_a_template_it_allows_keeps_every_value_data(tmp_path, template):
    """1. What it allows — bare words, `"$(…)"` around one, a case word, the value as an inner shell's argument, a
    here-document with a QUOTED delimiter reading the value from the environment (the refusal's own advice) — keeps
    each adversarial value data: nothing runs, and the value comes out intact."""
    from hpc_bridge import batch

    m = tmp_path / "pwned"
    for k, raw in enumerate(_ADVERSARIAL_VALUES):
        row = {"x": raw.replace("{m}", str(m))}
        names = {"x", "index"}
        cmd = batch.substitute(template, row, 0, names)
        s = batch.task_script(cmd, results_dir=str(tmp_path / "rd" / str(k)), index=0, attempt=1,
                              task_walltime_s=30, fields=batch.row_values(row, 0, names))
        r = batch.parse_result(_bash(s, tmp_path).stdout)
        assert not m.exists(), (template, raw)
        assert r is not None and r.rc == 0, (template, raw, r)
        if "printf %s {x}" not in template or "\n" not in raw:   # $(…) drops a value's trailing newlines only
            assert r.tail.endswith(row["x"] + "|"), (template, raw, r.tail)


def test_the_refusal_advises_a_quoted_delimiter_and_the_environment():
    """1(A). The advice for a value an inner program needs: a QUOTED delimiter, the program reading the variable from
    its environment, or the value as an argument — never `$HPCB_F_x` pasted into an unquoted here-document."""
    from hpc_bridge import batch

    with pytest.raises(batch.BatchRefusal) as e:
        batch.validate("python3 <<EOF\nprint('{x}')\nEOF", [{"x": 1}], 10)
    text = str(e.value)
    assert "<<'EOF'" in text and 'os.environ["HPCB_F_x"]' in text and "never hand" in text.lower(), text


def test_common_safe_forms_are_not_refused():
    """1. The false refusal of `out="$(basename {x})"` is gone (`$(…)` opens a quoting context of its own)."""
    from hpc_bridge import batch

    for t in ['out="$(basename {x})"; echo $out', "echo $(basename {x})", "cd run-{x} && ./sim",
              "awk -v n={x} '{print n, $1}' data", "f=results/{x}.txt; [ -f $f ] || ./run {x} > $f",
              "sed -n '1p' {x}", "python -c \"import sys; print(sys.argv[1])\" {x}", "ls *.{x}", "echo {x}{y}",
              "grep -c {x} out.txt", "cat <<'EOF' > in.txt\nN=5\nEOF\n./sim {x}"]:
        batch.validate(t, [{"x": "1", "y": "2"}], 10)


# 2. An unknown submit is never `lost` on absence; it holds the stop until evidence says where it could run.

class _MintsOnFirstPost(_BatchRunner):
    """The SDK's Executor: its task group is minted by the service at its first POST and learnt from the response. When
    that POST's response is lost, the task IS queued in the group the POST minted, which the client never learns."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.hidden_group = str(uuid.uuid4())
        _GROUPS[self.hidden_group] = []
        self.task_group_id = None

    def submit(self, command):
        n = len(self.futures)
        if n in self.fail_submit_of:
            fut = _PendingFuture()
            self.commands.append(command)
            self.futures.append(fut)
            fut.task_id = None
            _GROUPS[self.hidden_group].append(str(uuid.uuid4()))   # it reached the service: queued, still pending
            fut.fail(ConnectionError("the response to the batch_run POST was lost"))
            return fut
        if self.task_group_id is None:
            self.task_group_id = str(uuid.uuid4())
            self.group_tasks = []
            _GROUPS[self.task_group_id] = self.group_tasks
        return super().submit(command)


async def test_an_unknown_submit_queued_in_the_group_its_own_post_minted_is_not_settled_lost(monkeypatch):
    """2, the reviewer's: the groups the client learnt cannot list a task its failed POST queued in a group of its own,
    so absence is no evidence: the row stays submit_unknown and keeps holding the stop."""
    from hpc_bridge import batch

    clock = [1000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    runner = _MintsOnFirstPost(canary=_canary())
    app, _rt, _ = await _batch_app(workers=3, runner=runner)
    runner.fail_submit_of = {0}
    out = await _start(app, n=2)
    runner.futures[1].finish(_ok_report(1))
    await batch.tick(app, out.batch_id)
    assert app.ledger.task(out.batch_id, 0)["state"] == "submit_unknown"
    for _ in range(3):
        clock[0] += batch.UNKNOWN_SETTLE_S + batch.PRESUME_GRACE_S + 3600
        await batch.tick(app, out.batch_id)
    t = app.ledger.task(out.batch_id, 0)
    assert t["state"] == "submit_unknown", f"settled {t['state']} while its task is queued at the service"
    assert batch.unknown_note(app) is not None, "a stop can now answer down while that task may start a block"


async def _ssh_unknown_after_a_stop(monkeypatch, *, ps: str = "0 0", queued: str = "0"):
    from hpc_bridge import batch, server
    from hpc_bridge.models import ShellOutcome

    cmds: list[str] = []

    def runner_for(_app):
        async def run(cmd):
            cmds.append(cmd)
            if "HPCB_UEPX" in cmd:
                return SimpleNamespace(stdout=f"HPCB_UEPX {_UEP} {ps} {queued} 4096 4096\n")
            return SimpleNamespace(stdout="")
        return run

    monkeypatch.setattr(batch, "login_runner", runner_for)
    clock = [10_000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    app, _rt, runner = await _batch_app(workers=3)
    runner.fail_submit_of = {0}
    out = await _start(app, n=1)
    await batch.tick(app, out.batch_id)

    async def fake_run_shell(a, command, session_id="default", shape="compute", **_kw):
        return ShellOutcome(phase="complete", exit_code=0, stdout="released 101\n", block_state="warm")

    monkeypatch.setattr(server, "_run_shell", fake_run_shell)
    st = await server._stop_endpoint(app)                    # retires the block's config
    assert st.status == "draining" and out.batch_id in (st.notice or "")
    return app, out, clock, cmds


async def test_an_ssh_unknown_submit_stops_holding_once_its_user_endpoint_is_gone_and_its_config_retired(monkeypatch):
    """2. On SSH the evidence that ends the stop's `draining`: the config the submit went out with is retired, and the
    user endpoint it targets has exited with no pilot queued (two looks). The row stays submit_unknown in the ledger,
    with that evidence; it no longer holds the stop. Counted from the failure."""
    from hpc_bridge import batch, server

    app, out, clock, cmds = await _ssh_unknown_after_a_stop(monkeypatch)
    await batch.tick(app, out.batch_id)
    assert not any("HPCB_UEPX" in c for c in cmds), "looked before the failure was UNKNOWN_SETTLE_S old"
    clock[0] += batch.UNKNOWN_SETTLE_S + 1
    await batch.tick(app, out.batch_id)
    assert any("HPCB_UEPX" in c and "-j 101" in c for c in cmds)
    t = app.ledger.task(out.batch_id, 0)
    assert t["state"] == "submit_unknown" and "has exited" in (t["unheld"] or ""), t
    assert batch.unknown_note(app) is None
    st = await server._stop_endpoint(app)
    assert st.status == "down", st


async def test_an_ssh_unknown_submit_whose_user_endpoint_still_runs_keeps_holding(monkeypatch):
    from hpc_bridge import batch

    app, out, clock, _cmds = await _ssh_unknown_after_a_stop(monkeypatch, ps="1 1")
    clock[0] += batch.UNKNOWN_SETTLE_S + 1
    await batch.tick(app, out.batch_id)
    assert app.ledger.task(out.batch_id, 0)["unheld"] is None and batch.unknown_note(app) is not None


async def test_an_unknown_submit_is_never_presumed_away(monkeypatch):
    """2. The presumption takes rows in flight only: an unknown submit past any clock still holds the stop (MEP)."""
    from hpc_bridge import batch, server

    clock = [1000.0]
    monkeypatch.setattr(batch, "_now", lambda: clock[0])
    app, _rt, runner = await _batch_app(mep=True, workers=3)
    runner.fail_submit_of = {0}
    out = await _start(app, n=1)
    clock[0] += 10 * (batch.PRESUME_GRACE_S + 3600)
    await batch.tick(app, out.batch_id)
    assert app.ledger.task(out.batch_id, 0)["state"] == "submit_unknown"
    st = await server._stop_endpoint(app)
    assert st.status == "draining" and out.batch_id in (st.notice or "")


# 3. Lease liveness does not depend on the reader's TZ or locale.

def test_a_live_holders_start_time_reads_the_same_whatever_the_readers_tz_or_locale(monkeypatch):
    from hpc_bridge.ledger import proc_start

    if os.path.exists(f"/proc/{os.getpid()}/stat"):
        pytest.skip("Linux reads /proc, which is environment-independent")
    monkeypatch.setenv("TZ", "UTC")
    a = proc_start(os.getpid())
    monkeypatch.setenv("TZ", "Pacific/Auckland")
    b = proc_start(os.getpid())
    monkeypatch.setenv("LC_ALL", "de_DE.UTF-8")
    c = proc_start(os.getpid())
    assert a is not None and a == b == c, (a, b, c)


def test_a_live_holder_is_not_taken_over_by_a_server_in_another_tz(tmp_path, monkeypatch):
    from hpc_bridge.ledger import Ledger, Owner, proc_start

    led = Ledger.open(tmp_path / "state")
    monkeypatch.setenv("TZ", "UTC")
    holder = Owner("laptop", os.getpid(), proc_start(os.getpid()))
    led.add_batch("batch-000000000001", command="echo {x}", rows=[{"x": 1}], task_walltime_s=60.0, capacity=1,
                  results_dir="/scratch/u/b", idempotent=False, endpoint_id="e", uec_hash="u", session_id="default",
                  owner=holder, now=time.time() - 120)
    monkeypatch.setenv("TZ", "Pacific/Auckland")
    lease = led.take_lease("batch-000000000001", Owner("laptop", 999_999, "ps:other"), now=time.time())
    assert not lease.ok, "a live holder's lease was taken over"


def test_a_start_time_recorded_in_another_form_never_takes_a_live_pids_lease(tmp_path):
    """3. A start this reader cannot compare (another form: an earlier build's local-time `ps:` string) is no
    evidence the pid was handed on: a live pid keeps its lease, and a status read never calls it gone."""
    from hpc_bridge.ledger import Ledger, Owner

    led = Ledger.open(tmp_path / "state")
    led.add_batch("batch-000000000002", command="true", rows=[1], task_walltime_s=60.0, capacity=1,
                  results_dir="/scratch/u/b", idempotent=False, endpoint_id="e", uec_hash="u", session_id="default",
                  owner=Owner("laptop", os.getpid(), "ps:Sat Oct 31 18:00:00 2026"), now=time.time() - 120)
    assert led.holder_alive(led.holder("batch-000000000002").holder)
    assert not led.take_lease("batch-000000000002", Owner("laptop", 999_999), now=time.time()).ok


async def test_status_never_calls_a_live_heartbeating_server_gone():
    from hpc_bridge import server
    from hpc_bridge.ledger import Ledger

    app, _rt, _runner = await _batch_app()
    out = await _start(app, n=3)
    app.ledger.close()
    app2, _rt2, _runner2 = await _batch_app()
    app2.batch_owner = _owner(pid=os.getpid() + 100_000)
    app2.ledger = Ledger.open()
    app2.ledger.proc_start = lambda pid: "psutc:someone else"       # even a start that reads otherwise
    st = await server._status(app2, out.batch_id)
    assert st.owner and st.phase != "needs_confirmation" and "gone" not in (st.reason or ""), st


# 4. A tool call never waits past its budget for the loop's pass, and never starts a switch.

async def test_a_status_call_is_bounded_while_the_background_pass_is_in_a_long_step(monkeypatch):
    import asyncio

    from hpc_bridge import batch, server

    async def slow_switch(app_, run):
        await asyncio.sleep(20)

    monkeypatch.setattr(batch, "CALL_BUDGET_S", 1.0)
    monkeypatch.setattr(batch, "_maybe_switch", slow_switch)
    app, _rt, _runner = await _batch_app()
    app.batch_autoloop = True
    out = await _start(app, n=4)
    await asyncio.sleep(0.2)                       # the loop's first pass is now inside its long step
    t0 = time.monotonic()
    try:
        st = await asyncio.wait_for(server._status(app, out.batch_id), timeout=25)
        c = await asyncio.wait_for(server._cancel(app, out.batch_id, "drain"), timeout=25)
    finally:
        took = time.monotonic() - t0
        for run in list(app.batches.values()):
            if run.loop is not None:
                run.loop.cancel()
    assert took < 8, f"status and cancel took {took:.1f} s"
    assert "still running" in (st.notice or "") and "still running" in (c.notice or ""), (st.notice, c.notice)


async def test_a_tool_calls_pass_never_starts_the_next_block_switch():
    """4. A status call's pass leaves the switch (which confirms a block, and may take minutes) to the loop."""
    from hpc_bridge import batch, server

    app, rt, _runner = await _batch_app(remaining_s=150.0, workers=3)
    out, run, runner = await _at_next_block(app)
    calls = _count_switches(run)
    runner.futures[1].finish(_ok_report(1, remaining_s=70.0))
    await server._status(app, out.batch_id)
    assert calls == [] and app.shapes["compute"] is rt
    await batch.tick(app, out.batch_id)                                 # the loop's pass does it
    assert calls == [True] and app.shapes["compute"] is not rt


# 5. The wrapper's bounded wait keeps the tails.

@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_the_result_keeps_the_output_tail_when_a_leftover_child_ignores_term(tmp_path):
    from hpc_bridge.batch import parse_result, task_script

    s = task_script("(trap '' TERM; sleep 300) & echo started; echo oops >&2", results_dir=str(tmp_path / "rd"),
                    index=0, attempt=1, task_walltime_s=30, min_free_mb=1)
    t0 = time.monotonic()
    p = subprocess.run([_BASH, "-c", s], capture_output=True, text=True, timeout=60, start_new_session=True)
    rep = parse_result(p.stdout)
    assert rep is not None and rep.rc == 0 and time.monotonic() - t0 < 20
    assert (tmp_path / "rd/tasks/0/attempt-1/stdout").read_text() == "started\n"
    assert rep.tail == "started\n" and rep.stderr_tail == "oops\n", (rep.tail, rep.stderr_tail)


@pytest.mark.skipif(_BASH is None or shutil.which("perl") is None, reason="needs bash and perl")
def test_the_result_keeps_the_output_tail_when_a_setsid_child_holds_the_stream(tmp_path):
    """5. A child that left the command's process group (setsid) still holds stdout: the wrapper ends in bounded time,
    and the tail comes from what was written."""
    from hpc_bridge.batch import parse_result, task_script

    tag = f"hpcb-setsid-{uuid.uuid4().hex[:8]}"
    s = task_script(f"perl -MPOSIX -e 'POSIX::setsid(); $0=\"{tag}\"; sleep 40' & sleep 1; echo started",
                    results_dir=str(tmp_path / "rd"), index=0, attempt=1, task_walltime_s=30, min_free_mb=1)
    t0 = time.monotonic()
    try:
        p = subprocess.run([_BASH, "-c", s], capture_output=True, text=True, timeout=60, start_new_session=True)
        rep = parse_result(p.stdout)
        assert rep is not None and rep.rc == 0 and time.monotonic() - t0 < 20, time.monotonic() - t0
        assert rep.tail == "started\n", rep.tail
    finally:
        subprocess.run(["pkill", "-f", tag], capture_output=True)


# 6. blocks_left lists every block whose job is unknown.

async def test_a_switched_left_block_of_unknown_job_is_still_named_in_blocks_left():
    from hpc_bridge import batch

    app, rt, _runner = await _batch_app()
    out = await _start(app, n=2)
    run = app.batches[out.batch_id]
    rt.block_job = None
    run.block_job = None
    run.switched_left.append((None, batch._now() + 300))
    left = batch._blocks_left(app, run)
    assert len(left) == 2, [b.how_to_end[:60] for b in left]


# ---- review round 3 (afd3981) ---------------------------------------------------------------------------------------
# Each of these failed on afd3981 (red log: scratchpad bp1/red_batch_r3.txt): the coordinator's probes, adapted.

_STDIN_REFUSED = [
    "echo {x} | sh", "echo {x} | bash", "echo {x} | python3", "echo {x} | xargs -0 sh -c", "echo {x} | xargs -0 bash",
    "echo {x} | xargs mytool", "echo {x} | parallel", ". /dev/stdin <<< {x}", "source /dev/stdin <<< {x}",
    "sh <<< {x}", "python3 - <<< {x}", "{ echo {x}; } | sh", "(echo {x}) | bash",
    "for v in {x}; do echo $v; done | sh", "echo $(echo {x}) | sh", "sh <(echo {x})", "echo {x} > >(sh)",
    "echo {x} | env sh", "echo {x} | timeout 5 python3", "echo {x} | perl", "echo {x} | node", "echo {x} | dc",
    "echo {x} | at now", "echo {x} | ssh host", "echo {x} | awk -f -", "echo {x} | sed -f /dev/stdin f",
    "echo {x} | python3 -W ignore", "echo {x} |& bash", "echo {x} 2>&1 | sh", "echo {x} | bash -s",
    "bash &>/dev/null -c {x}",
]


@pytest.mark.parametrize("template", _STDIN_REFUSED)
def test_a_value_fed_to_a_program_that_reads_code_from_stdin_is_refused(template):
    """Round 3 (1). A placeholder in a pipeline stage, a here-string or a process substitution that feeds a program
    reading CODE from its standard input — a shell or interpreter with no script file, `xargs` running one (or a command
    this reader cannot vouch for), `parallel`, `source /dev/stdin`, `dc`/`at`/`ssh`… — is refused, with advice."""
    from hpc_bridge import batch

    with pytest.raises(batch.BatchRefusal) as e:
        batch.validate(template, [{"x": "1"}], 10)
    assert "file" in str(e.value) or "process substitution" in str(e.value) or "-c" in str(e.value), str(e.value)


@pytest.mark.parametrize("template", [
    "echo {x} | grep a", "echo {x} | python3 script.py", "echo {x} | bash script.sh", "echo {x} | xargs echo",
    "echo {x} | xargs -n 1 rm -f --", "cat <<< {x}", "grep a <<< {x}", "echo {x} | python3 -m json.tool",
    "env A=1 ./sim {x}", "timeout 5 ./sim {x}", "nice -n 5 ./sim {x}", "echo {x} || sh fallback.sh",
    "echo {x} && bash job.sh", "printf '%s\\n' {x} | sort | uniq -c", "sudo -u u ./sim {x}",
])
def test_a_value_fed_only_to_data_readers_is_allowed(template):
    from hpc_bridge import batch

    batch.validate(template, [{"x": "1"}], 10)


@pytest.mark.parametrize("template", ["env {x}", "env -i {x}", "env A=1 {x}", "sudo {x}", "timeout 5 {x}",
                                      "nice -n 5 {x}", "xargs {x}", "exec {x}", "command {x}", "printf {x}",
                                      "printf {x} >/dev/null", "stdbuf -o0 {x}"])
def test_a_value_in_a_wrappers_option_or_command_slot_is_refused(template):
    """Round 3 (2). `env {x}` with `-S touch …` ran the command: a placeholder where a wrapper (env, sudo, timeout,
    xargs…) reads its options and the command it runs is refused, as is printf's format (`-v` assigns)."""
    from hpc_bridge import batch

    with pytest.raises(batch.BatchRefusal):
        batch.validate(template, [{"x": "1"}], 10)


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_the_coordinators_probes_run_nothing(tmp_path):
    """Round 3: the probes that ran the value — piped into sh / bash / python3 / xargs sh -c, and `env -S` — are
    refused; every allowed one keeps it data."""
    from hpc_bridge.batch import BatchRefusal, substitute

    pwn = tmp_path / "PWNED"
    cases = [("env {x}", f"-S touch {pwn}"), ("echo {x} | bash", f"touch {pwn}"),
             ("echo {x} | python3", f"import os; os.system('touch {pwn}')"), ("echo {x} | xargs -0 sh -c", f"touch {pwn}"),
             ("echo {x} | sh", f"$(touch {pwn})"), ("printf {x} >/dev/null", f"-va[$(touch {pwn})]"),
             ("find . -maxdepth 0 {x}", "-fprint")]
    for tpl, val in cases:
        try:
            s = substitute(tpl, {"x": val}, 0, {"x", "index"})
        except BatchRefusal:
            continue
        subprocess.run([_BASH, "-c", s], env=dict(os.environ, HPCB_F_x=val), capture_output=True, cwd=tmp_path,
                       timeout=30)
        assert not pwn.exists(), (tpl, val)


async def test_run_batch_names_rows_whose_value_starts_with_a_dash():
    """Round 3 (2). A row value starting with `-` may be read as an option: run_batch names those rows and says to put
    `--` before the placeholder (or `./` before a path) — a warning, not a refusal (`-5` may be an option's argument);
    none when `--` already comes before it, or when the placeholder does not start a word."""
    from hpc_bridge import server

    app, _rt, _runner = await _batch_app()
    out = await server._run_batch(app, command="rm {f}", rows=[{"f": "a"}, {"f": "-rf"}, {"f": "-x"}],
                                  task_walltime_s=10)
    assert out.phase != "refused" and "Row(s) 1, 2" in (out.notice or "") and "--" in (out.notice or ""), out.notice
    for command in ("rm -- {f}", "./sim --opt={f}"):
        ok = await server._run_batch(app, command=command, rows=[{"f": "-rf"}], task_walltime_s=10)
        assert "starting with '-'" not in (ok.notice or ""), (command, ok.notice)


def test_a_here_string_is_read_as_one():
    """`<<<` is a here-string, not a here-document followed by `<` (the template was refused for the wrong reason)."""
    from hpc_bridge import batch

    batch.validate("grep -c a <<< {x}; echo done", [{"x": "1"}], 10)

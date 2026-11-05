"""Batch row sizing, review round 2 (on cbbcc1a: 8 defects, 11 reproductions, each failing there). Fixed test-first;
each test names its item (R2-A … R2-J are the reviewer's reproductions; the others pin the rest of each ruling).

Where a Linux tool is missing on the test host, a fake on PATH stands in, and the test says which:
- `taskset`: `true` (macOS has none);
- `mkdir`: the real mkdir, except that making the slot lock directory fails as a full filesystem fails (ENOSPC).
"""
from __future__ import annotations

import base64
import json
import os
import re
import shutil
import time

import pytest

from tests.test_batch_phase1 import _login_fake, _owner
from tests.test_batch_sizing import (
    _app_with,
    _canary,
    _fake_tool,
    _measure,
    _report,
    _result,
    _run,
    _script,
    _sized_app,
    _slot,
    _spawn,
    _wrapper_env,
)
from tests.test_batch_sizing_r1 import _ensure
from tests.test_server import _Res

_BASH = shutil.which("bash")
_PY3 = shutil.which("python3")
needs_bash = pytest.mark.skipif(_BASH is None, reason="needs bash")
needs_py = pytest.mark.skipif(_BASH is None or _PY3 is None, reason="needs bash and python3")


def _kill(index: int, **extra):
    """A row's result: killed at its memory."""
    from hpc_bridge import batch

    return _report(index, rc=batch.MEMORY_RC, why="memory", wall_s=1.0, peak_rss_kb=900 * 1024, nproc=1, cpus="0",
                   **extra)


async def _needs_sizing(n: int = 8):
    """A defaulted batch (1 CPU, the block's share of memory) whose row 0 was killed at that default: needs_sizing,
    rows 1-3 still out."""
    from hpc_bridge import batch

    app, rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=n)
    runner.futures[0].finish(_kill(0, cpu_s=1.0))
    await batch.tick(app, out.batch_id)
    assert app.ledger.batch(out.batch_id)["state"] == "needs_sizing"
    return app, rt, runner, out.batch_id


def _lock_dir_result(cmd: str, err: str) -> _Res:
    from hpc_bridge import batch

    idx = int(re.search(r"HPCB_ROW_INDEX=(\d+)", cmd).group(1))
    att = int(re.search(r"HPCB_ATTEMPT=(\d+)", cmd).group(1))
    rep = {"index": idx, "attempt": att, "job": "101", "host": "c1", "workers": "5", "remaining_s": 800,
           "rc": batch.LOCK_DIR_RC, "why": "lock_dir", "ran": False,
           "error_b64": base64.b64encode(err.encode()).decode()}
    return _Res(0, "HPCB_RESULT " + json.dumps(rep) + "\n", "")


# ==== 1. a pause never overwrites needs_sizing; the size still owed is the ledger's ===================================

async def test_r2b_a_reap_of_a_needs_sizing_batch_does_not_let_the_yes_skip_the_size(monkeypatch):
    """A row was killed at the DEFAULT memory: the batch is needs_sizing (the size is the user's to give), its other
    rows still out. The block is then reaped; the fix round added needs_sizing to PAUSABLE_STATES, so the reap's pause
    overwrites needs_sizing with needs_confirmation. The user's yes (confirm_spend=True, no size) then resumes
    admission AT THE SAME DEFAULT MEMORY that killed the row — rows run and are killed again (non-idempotent: failed for
    good), the 'never killing row after row on a default' ruling broken."""
    from hpc_bridge import batch, server

    cmds: list[str] = []
    monkeypatch.setattr(batch, "login_runner", _login_fake(cmds, sacct="CANCELLED"))
    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=8)                                  # defaulted: 1 CPU, the block's share of memory
    bid = out.batch_id
    assert out.sizing.defaulted and len(runner.commands) == 4
    runner.futures[0].finish(_report(0, rc=batch.MEMORY_RC, why="memory", cpu_s=1.0, wall_s=1.0,
                                     peak_rss_kb=900 * 1024, nproc=1, cpus="0"))
    await batch.tick(app, bid)
    assert app.ledger.batch(bid)["state"] == "needs_sizing"
    # the block is reaped under rows 1-3 (sacct: CANCELLED), found by the batch's own evidence pass
    run = app.batches[bid]
    for key in list(run.futures):
        run.sent_job[key] = "13"
    run.block_job = "13"
    app.ledger._con.execute("update task set admitted_at = admitted_at - 600 where batch_id = ?", (bid,))
    await batch.tick(app, bid)
    b = app.ledger.batch(bid)
    assert "reaped" in (b["pause_reason"] or ""), b["pause_reason"]
    assert b["state"] == "needs_sizing"                         # the reap's pause never overwrites it
    sent = len(runner.commands)
    res = await server._run_batch(app, batch_id=bid, confirm_spend=True)
    for _ in range(3):                                          # the new block's first check
        app.batches[bid].last_warm = 0.0
        await batch.tick(app, bid)
    again = [t for t in app.ledger.tasks(bid) if t["state"] == "in_flight" and t.get("req_mem_default")]
    assert len(runner.commands) == sent and not again, (
        f"the yes alone resumed the needs_sizing batch: {len(runner.commands) - sent} row(s) sent again at the default "
        f"memory that was just killed (req_memory_mb={[t['req_memory_mb'] for t in again]}); phase={res.phase}")
    assert res.phase == "needs_sizing", res


async def test_r2_1_a_pause_at_needs_sizing_keeps_it_and_owes_the_yes_as_well():
    """Any pause (a reap, a wrong-block refusal, a drain) of a batch waiting for its size keeps needs_sizing and records
    that the yes is owed too: the size alone then moves it to needs_confirmation (nothing sent), the yes resumes it."""
    from hpc_bridge import batch, server

    app, _rt, runner, bid = await _needs_sizing()
    batch.pause(app, bid, "the block was reaped — a test's reason")
    b = app.ledger.batch(bid)
    assert b["state"] == "needs_sizing" and b["size_owed"] and b["needs_yes"], b
    st = await batch.status(app, bid)
    assert "confirm_spend=True" in st.notice and "reaped" in (st.reason or ""), st.notice
    sent = len(runner.commands)
    sized = await server._run_batch(app, batch_id=bid, cores=1, memory_gb=1.25)
    assert sized.phase == "needs_confirmation" and len(runner.commands) == sent, (sized.phase, sized.notice)
    assert app.ledger.batch(bid)["memory_mb"] == 1280                           # the size is in
    yes = await server._run_batch(app, batch_id=bid, confirm_spend=True)
    assert yes.phase in ("admitting", "waiting"), (yes.phase, yes.notice)
    b = app.ledger.batch(bid)
    assert not b["needs_yes"] and not b["size_owed"]


async def test_r2_1_the_size_owed_is_read_from_the_ledger_not_the_state():
    """Whatever the state says, a batch whose size is still owed (the ledger's flag, set by the sizing pause) is not
    resumed by a yes alone."""
    from hpc_bridge import server

    app, _rt, runner, bid = await _needs_sizing()
    app.ledger.update_batch(bid, state="needs_confirmation", pause_reason="some other pause")
    sent = len(runner.commands)
    res = await server._run_batch(app, batch_id=bid, confirm_spend=True)
    assert len(runner.commands) == sent and app.ledger.batch(bid)["state"] != "admitting", (res.phase, res.notice)
    assert "size" in res.notice


# ==== 2. a lock dir refusal: not run, not failed; "could not be made" is not "not ours"; an unguessable name ==========

async def test_r2c_rows_refused_by_the_lock_dir_ran_nothing_and_are_not_failed_for_good():
    """The wrapper refuses a row whose node lock dir is unsafe (`why: lock_dir`, rc 77, ran: false) — also, as R2-D
    shows, a /tmp that is full or not writable. The server settles it as any `error` result: FAILED, for good, and keeps
    admitting the backlog onto the same node, which refuses each in turn — a 10,000-row batch ends all-failed with
    nothing run, never paused, never retried. A row that ran nothing goes back (or the batch pauses naming why)."""
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=12, cores=1, memory_gb=0.5)
    bid = out.batch_id
    assert len(runner.commands) == 4
    err = "the slot lock directory /tmp/hpcb-slots-1000-101-abc could not be made (No space left on device); nothing ran"
    for _ in range(3):
        for cmd, f in list(zip(runner.commands, runner.futures, strict=True)):
            if not f.done():
                f.finish(_lock_dir_result(cmd, err))
        await batch.tick(app, bid)
    c = app.ledger.counts(bid)
    assert not c.get("failed"), (
        f"rows that ran nothing (lock_dir refusal) were failed for good: counts={c}, state="
        f"{app.ledger.batch(bid)['state']}, sent={len(runner.commands)}")
    b = app.ledger.batch(bid)
    assert len(runner.commands) == 4 and c["backlog"] == 12, c                 # paused at the first: nothing more sent
    assert b["state"] == "needs_confirmation" and "/tmp/hpcb-slots-1000-101-abc" in b["pause_reason"], b
    assert "No space left on device" in b["pause_reason"]


@needs_bash
def test_r2d_a_lock_dir_that_cannot_be_made_is_not_reported_as_planted_by_another_user(tmp_path):
    """`mkdir -m 700` failing for ANY reason — ENOSPC on a full node /tmp, EACCES, EROFS — falls into the `[ ! -d ]`
    branch: the row is refused as `why: lock_dir`, "unsafe ... (another user may have made it)", rc 77. Nothing is at
    that path: no one planted anything. With R2-C (the server fails every lock_dir row for good) a full /tmp on the
    node drains the whole batch as failed — where before the fix the claim failed as slot_busy and paused the batch.
    (mkfifo, the wrapper's other /tmp use, needs no data block and still works on a full ext4/xfs /tmp.)"""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    _fake_tool(tmp_path, "mkdir", 'for a in "$@"; do case "$a" in *hpcb-slots-*) '
                                  'echo "mkdir: $a: No space left on device" >&2; exit 1;; esac; done\n'
                                  'exec /bin/mkdir "$@"')
    env = _wrapper_env(tmp_path)
    rep = _result(_spawn(_script("echo ran", tmp_path / "rd", 0, _slot()), env))
    lock = tmp_path / "nodetmp"
    assert not any(p.name.startswith("hpcb-slots-") for p in lock.iterdir())        # nothing was planted
    assert not (rep.rc == batch.LOCK_DIR_RC and "another user" in (rep.error or "")), (
        f"a lock dir that could not be MADE (ENOSPC) was reported as planted by another user: rc={rep.rc} "
        f"why={rep.why!r} error={rep.error!r}")
    assert rep.why == "lock_dir" and "could not be made" in rep.error and "No space left" in rep.error, rep
    assert "ran" not in rep.tail


@needs_bash
def test_r2_2_a_lock_dir_another_user_could_have_made_is_still_refused_as_not_ours(tmp_path):
    """Defence in depth stays: a directory at the name that is not this user's private one (mode 755 here) is refused,
    saying so."""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    env = _wrapper_env(tmp_path)
    planted = tmp_path / "nodetmp" / f"hpcb-slots-{os.getuid()}-101-feedc0de"
    planted.mkdir(mode=0o755)
    planted.chmod(0o755)
    rep = _result(_spawn(_script("echo ran", tmp_path / "rd", 0, _slot(token="feedc0de")), env))
    assert rep.rc == batch.LOCK_DIR_RC and rep.why == "lock_dir", rep
    assert "not 700" in rep.error and "could not be made" not in rep.error, rep.error


async def test_r2_2_every_row_on_a_block_names_the_blocks_own_unguessable_lock_dir():
    """The lock dir's name carries a random token the server made when it first measured the block, kept on the ledger
    with the measure: every row of every batch on that block gets the same one; another user cannot guess it."""
    from hpc_bridge import batch

    app, rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    one = await _run(app, n=2, cores=1, memory_gb=0.5)
    two = await _run(app, n=2, cores=1, memory_gb=0.5)
    assert one.batch_id and two.batch_id and len(runner.commands) == 4
    token = app.ledger.slot_token("fake-eid", batch.uec_hash(rt))
    assert re.fullmatch(r"[0-9a-f]{16}", token)
    measured = app.ledger.block_measure("fake-eid", batch.uec_hash(rt))
    assert measured is not None and measured["slot_token"] == token                  # kept with the block's measure
    for cmd in runner.commands:
        assert f'hpcb-slots-$(id -u)-${{__hb_jn}}-{token}"' in cmd


# ==== 3. a pilot: a resize merges over the user's own size; a pilot memory kill ends the pilot ========================

async def test_r2a_a_resize_during_the_pilot_keeps_the_users_gpus_and_concurrency():
    """pilot=1, gpus=1 on a 2-GPU block: while the pilot row is out the batch's cores/memory/gpus/concurrency are the
    WHOLE block's (the user's own kept in pilot_saved). A resize naming cores and memory only (as every notice tells
    the agent to call it) must keep the user's 1 GPU per row and no concurrency cap — not take the pilot's 2 GPUs and
    concurrency=1."""
    from hpc_bridge import server

    app, _rt, runner = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, mem_limit_mb=16000,
                                          idle_mb=500, gpu_ids=("0", "1"), gpu_kind="cuda"))
    out = await _run(app, n=8, pilot=1, gpus=1)
    assert len(runner.commands) == 1, out
    b = app.ledger.batch(out.batch_id)
    assert (b["gpus"], b["concurrency"]) == (2, 1) and b["pilot_saved"]          # the pilot holds the whole block
    await server._run_batch(app, batch_id=out.batch_id, cores=2, memory_gb=1.0)  # mid-pilot resize
    b = app.ledger.batch(out.batch_id)
    assert (b["gpus"], b["concurrency"]) == (1, None), (
        f"the resize merged over the pilot's whole-block size: gpus={b['gpus']} concurrency={b['concurrency']} "
        "(the user asked 1 GPU per row, no concurrency cap)")


async def test_r2a2_a_pilot_row_killed_at_a_default_memory_restores_the_users_size():
    """pilot=1, gpus=1, cores=1, memory unnamed (defaulted) on a 2-GPU block: the pilot row holds the whole block (no
    default memory: it is not marked req_mem_default). Killed at that memory, the pilot ends as a pilot ends: the
    user's size restored, a proposal from that row (the user's 1 GPU per row)."""
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, mem_limit_mb=16000,
                                          idle_mb=500, gpu_ids=("0", "1"), gpu_kind="cuda"))
    out = await _run(app, n=8, pilot=1, gpus=1, cores=1, idempotent=True)
    assert len(runner.commands) == 1, out
    assert not app.ledger.task(out.batch_id, 0)["req_mem_default"]              # the whole block: no default memory
    runner.futures[0].finish(_report(0, rc=batch.MEMORY_RC, why="memory", cpu_s=4.0, wall_s=2.0,
                                     peak_rss_kb=16000 * 1024, nproc=8, cpus="0-7"))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id)
    b = app.ledger.batch(out.batch_id)
    assert st.phase == "needs_sizing", st
    assert (st.proposal.gpus, b["gpus"], b["concurrency"]) == (1, 1, None), (
        f"after the pilot's default-memory kill: proposal gpus={st.proposal.gpus}, batch gpus={b['gpus']} "
        f"concurrency={b['concurrency']} cores={b['cores']} — the pilot's whole-block size, not the user's 1 GPU")
    assert b["cores"] == 1 and not b["pilot_saved"], b                        # the user's own size, the pilot over
    assert app.ledger.task(out.batch_id, 0)["state"] == "backlog"              # idempotent: runs at the size picked


# ==== 4. the derived block's own size restated is that block; a switch clears its workers; never under batch rows ====

async def test_r2h_restating_the_derived_blocks_size_with_rows_in_flight_starts_no_second_block():
    """F1 made `ensure_endpoint_up(cores=9, memory_gb=7)` — exactly the derived block's own terms — a different block
    (its hpcb_workers dropped). A switch is guarded only by run_shell's task handles, never by batch rows: with
    confirm_spend=True a second block is built while 4 rows still run on the first (on SSH the old one idles out only
    after them: two blocks billing). Before the fix this call was a no-op ("the same block in other words")."""
    from hpc_bridge import server
    from hpc_bridge.resources import WORKERS

    app = _sized_app(_canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192)))
    out = await _run(app, n=8, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    assert out.phase == "admitting" and app.ledger.counts(out.batch_id)["in_flight"] == 4
    built = len(app.built)
    st = await server._ensure_endpoint_up(app, "compute", cores=9, memory_gb=7, confirm_spend=True)
    assert len(app.built) == built, (
        f"a second block was started (workers {app.built[-1].get('max_workers_per_node')}) while "
        f"{app.ledger.counts(out.batch_id)['in_flight']} batch rows run on the first: {st.status}")
    assert st.status == "up" and app.shapes["compute"].user_endpoint_config.get(WORKERS) == 5, st


@pytest.mark.parametrize("ask", [{"cores": 32, "memory_gb": 40}, {"partition": "capped"}, {"account": "other-acct"}])
async def test_r2_4_a_block_switch_under_batch_rows_in_flight_is_refused_naming_them(ask):
    """A real switch (size, partition, account) while batch rows run on the block: refused, naming the batch and how
    to end its rows, as stop_endpoint is — nothing set, nothing started."""
    from hpc_bridge import server

    app = _sized_app(_canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192)))
    out = await _run(app, n=8, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    uec = dict(app.shapes["compute"].user_endpoint_config)
    built = len(app.built)
    st = await server._ensure_endpoint_up(app, "compute", confirm_spend=True, **ask)
    assert len(app.built) == built and app.shapes["compute"].user_endpoint_config == uec, st
    assert out.batch_id in st.notice and "in flight" in st.notice and "can't" in st.notice, st.notice
    assert app.ledger.batch(out.batch_id)["state"] == "admitting"              # the refusal pauses nothing


async def test_r2e_a_partition_switch_after_a_derived_block_does_not_carry_its_workers():
    """F1 cleared the derived block's hpcb_workers only when the next ask names a SIZE. A partition (or account) change
    alone keeps it: the new partition's block runs the old derived block's 5 workers. (The batch's one row is settled
    first: a switch under batch rows in flight is refused.)"""
    from hpc_bridge import batch, server
    from hpc_bridge.resources import WORKERS

    app = _sized_app(_canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192)))
    out = await _run(app, n=1, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    assert out.batch_id and app.built[-1]["max_workers_per_node"] == 5
    await batch.tick(app, out.batch_id)                          # its row settles: nothing of the batch is out
    assert not app.ledger.counts(out.batch_id)["in_flight"]
    app.batches.clear()                                          # the batch is done with; only the block remains
    st = await server._ensure_endpoint_up(app, "compute", partition="capped", confirm_spend=True)
    uec = app.shapes["compute"].user_endpoint_config
    assert st.status in ("up", "provisioning"), (st.status, st.notice)
    assert uec.get(WORKERS) is None, (
        f"the derived block's {uec.get(WORKERS)} workers rode onto the 'capped' partition's block, asked by partition "
        f"alone (built: {app.built[-1].get('max_workers_per_node')})")


# ==== 5. the mkdir fallback: a claim names the wrapper and the command; released only while it is this row's ==========

@needs_bash
def test_r2f_the_mkdir_fallbacks_release_never_deletes_a_claim_another_row_took_over(tmp_path):
    """Without flock a claim is `cpu-N.d/pid`, and `_CLAIMED` writes the COMMAND's pid there. Once the command exits the
    pid is dead, but the wrapper still holds the claim until its streams close and it releases — by PATH (`rm -rf
    cpu-N.d`). In between, a waiting row B judges the claim stale and takes it over (legitimately, per W5); then A's
    release deletes B's live claim, and a third row C claims the CPU B is running on.

    A's command returns at once and leaves a TERM-ignoring child holding its stdout for 4 s (the readers wait for it):
    the window is 4 s here, ~0.1-0.3 s for any row (the wrapper's own post-wait work) — hit often with rows waiting."""
    if shutil.which("flock"):
        pytest.skip("this host has flock: the mkdir fallback is not taken")
    _fake_tool(tmp_path, "taskset", "true")
    env = _wrapper_env(tmp_path)
    rd = tmp_path / "rd"
    probe = tmp_path / "probe"
    probe.mkdir()
    one = _slot(cores=1, cpu_ids=(0,), claim_wait_s=30, pin=False)
    a = _spawn(_script("(trap '' TERM; sleep 4) & echo a", rd, 0, one), env)
    time.sleep(1.0)
    cmd = (f'touch {probe}/run.$HPCB_ROW_INDEX; sleep 8; n=$(ls {probe} | wc -l | tr -d " "); '
           f'rm -f {probe}/run.$HPCB_ROW_INDEX; echo "$n"')
    b = _spawn(_script(cmd, rd, 1, one), env)
    time.sleep(0.5)
    c = _spawn(_script(cmd, rd, 2, one), env)
    _result(a)
    rb, rc_ = _result(b), _result(c)
    ran = [r for r in (rb, rc_) if not r.slot_busy]
    assert all(r.tail.strip() == "1" for r in ran), (
        f"rows 1 and 2 ran on cpu 0 at once: cpus {rb.cpus!r}/{rc_.cpus!r}, rows seen running "
        f"{rb.tail.strip()!r}/{rc_.tail.strip()!r}")


@needs_bash
def test_r2_5_a_claim_whose_wrapper_and_command_are_both_gone_is_taken_over(tmp_path):
    """Stale only when BOTH pids it names are dead: a claim left by a killed row (both gone) is taken over."""
    if shutil.which("flock"):
        pytest.skip("this host has flock: the mkdir fallback is not taken")
    _fake_tool(tmp_path, "taskset", "true")
    env = _wrapper_env(tmp_path)
    lock = tmp_path / "nodetmp" / f"hpcb-slots-{os.getuid()}-101"
    lock.mkdir(mode=0o700)
    (lock / "cpu-0.d").mkdir()
    (lock / "cpu-0.d" / "pid").write_text("999998 999999\n")                 # neither is alive
    rep = _result(_spawn(_script("echo ran", tmp_path / "rd", 0, _slot(cores=1, cpu_ids=(0,), claim_wait_s=5,
                                                                      pin=False)), env))
    assert rep.rc == 0 and rep.cpus == "0" and "ran" in rep.tail, rep
    live = lock / "cpu-0.d"
    live.mkdir(exist_ok=True)
    (live / "pid").write_text(f"999998 {os.getpid()}\n")                   # the command still runs: not stale
    rep = _result(_spawn(_script("echo ran", tmp_path / "rd", 1, _slot(cores=1, cpu_ids=(0,), claim_wait_s=2,
                                                                      pin=False)), env))
    assert rep.slot_busy, rep


# ==== 6. a killed row's CPU time is no evidence: null, and its proposal asks for cores =================================

@needs_py
def test_r2g_a_row_killed_at_its_memory_still_reports_the_cpu_its_processes_used(tmp_path):
    """cpu_s is `times`' children line before/after the wrapper's wait: it counts only processes reaped up the chain
    to the wrapper. A row killed at its memory (or its timeout) has its tree killed at once: the command's processes
    die with their parent, are reparented and reaped by init, and their CPU is lost — cpu_s ≈ 0. That number is then
    EVIDENCE: `_default_memory_killed` proposes ⌈cpu_s ÷ wall⌉ = 1 CPU for a row that kept 2 CPUs busy (the fix's
    "a null cpu_s is no evidence, never 1" holds only for null, not for this undercount)."""
    from hpc_bridge import batch, sizing

    _fake_tool(tmp_path, "taskset", "true")
    two_busy_then_hog = (
        f"{_PY3} -c \"import os, time\n"
        "for _ in range(2):\n"
        "    if os.fork() == 0:\n"
        "        end = time.time() + 30\n"
        "        while time.time() < end: pass\n"
        "        os._exit(0)\n"
        "time.sleep(2.5)\n"
        "b = bytearray(400 << 20)\n"
        "for i in range(0, len(b), 4096): b[i] = 1\n"
        "time.sleep(30)\"")
    rep = _result(_spawn(_script(two_busy_then_hog, tmp_path / "rd", 0, _slot(memory_mb=150, pin=False)),
                         _wrapper_env(tmp_path)), timeout=90)
    assert rep.why == "memory" and rep.rc == batch.MEMORY_RC, rep
    proposal = sizing.pilot_proposal([{"cpu_s": rep.cpu_s, "wall_s": rep.wall_s, "peak_rss_kb": rep.peak_rss_kb}],
                                     _measure(cpus=4, workers=5))
    assert rep.cpu_s is None or rep.cpu_s >= 3.0, (
        f"2 processes busy for ~2.5 s+ (≥ 5 CPU-s) reported cpu_s={rep.cpu_s} over wall_s={rep.wall_s}: the default-"
        f"memory proposal says {proposal.cores} CPU(s)")
    assert proposal.cores is None


@needs_bash
def test_r2_6_a_row_killed_at_its_timeout_reports_no_cpu_seconds(tmp_path):
    _fake_tool(tmp_path, "taskset", "true")
    rep = _result(_spawn(_script("sleep 30", tmp_path / "rd", 0, _slot(pin=False), walltime=1),
                         _wrapper_env(tmp_path)), timeout=60)
    assert rep.timed_out and rep.cpu_s is None, rep


async def test_r2_6_a_killed_rows_cpu_seconds_are_no_evidence_to_the_proposal():
    """A wrapper that still reports a killed row's cpu_s (an earlier build): the server takes it as no evidence — the
    proposal asks for cores, never says 1 CPU."""
    from hpc_bridge import batch

    app, _rt, _runner, bid = await _needs_sizing()
    st = await batch.status(app, bid)
    assert st.proposal is not None and st.proposal.cores is None, st.proposal
    assert "cores=" in st.reason and "1 CPU" not in st.reason, st.reason


# ==== 7. an adopted needs_sizing batch owes the user's yes ============================================================

async def test_r2i_an_adopted_needs_sizing_batch_resumes_only_on_the_users_yes():
    """`_adopt` pauses only an `admitting` batch ("its spend confirmation lived in that process, so admission resumes
    only on the user's yes"). A batch adopted at needs_sizing is left there, and a resize alone then resumes it on
    whatever block this new process has confirmed for other work — no yes for the batch (rule 4). The fix round taught
    stop/reap/listing about needs_sizing (ACTIVE_STATES/PAUSABLE_STATES) but not the adoption."""
    from hpc_bridge import batch, server
    from hpc_bridge.ledger import Ledger, Owner

    m = _measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500)
    app, _rt, runner = _app_with(m)
    out = await _run(app, n=8)
    bid = out.batch_id
    runner.futures[0].finish(_report(0, rc=batch.MEMORY_RC, why="memory", cpu_s=1.0, wall_s=1.0,
                                     peak_rss_kb=900 * 1024, nproc=1, cpus="0"))
    for i in (1, 2, 3):
        runner.futures[i].finish(_report(i))
    await batch.tick(app, bid)
    assert app.ledger.batch(bid)["state"] == "needs_sizing" and not app.ledger.counts(bid)["in_flight"]
    app.ledger.close()
    app2, _rt2, runner2 = _app_with(m)                      # the new process: its block confirmed for other work
    app2.batch_owner = _owner(pid=os.getpid() + 100_000, host=Owner.here().host)
    app2.ledger = Ledger.open()
    app2.ledger.pid_alive = lambda pid: pid != os.getpid()
    app2.ledger.lease_stale_s = 0.0
    res = await server._run_batch(app2, batch_id=bid, cores=1, memory_gb=1.25)
    assert not runner2.commands and res.phase in ("needs_confirmation", "needs_sizing"), (
        f"the adopted batch resumed on a resize alone, no yes: phase={res.phase}, {len(runner2.commands)} row(s) sent")
    yes = await server._run_batch(app2, batch_id=bid, confirm_spend=True)
    assert yes.phase in ("admitting", "waiting") and runner2.commands, (yes.phase, yes.notice)


# ==== 8. a size recorded while the block is down leaves needs_sizing at once ==========================================

async def test_r2j_a_size_recorded_while_the_block_was_down_is_not_asked_for_again():
    """`_resume` records the size, then `ensure_up` answers neither a spend question nor up (the endpoint down for a
    moment): the batch stays `needs_sizing` though its size is in. The user's yes then hits `sizing_wanted` and answers
    "still waits for its rows' size" — the F8 loop, by another door."""
    from hpc_bridge import batch
    from hpc_bridge.models import EndpointStatus

    app, _rt, _runner, bid = await _needs_sizing()

    async def down(_app, _confirm, size=None):
        return EndpointStatus(status="down", block_state="cold", endpoint_id="fake-eid", notice="endpoint offline")

    sized = await batch.run_batch(app, ensure_up=down, batch_id=bid, cores=1, memory_gb=1.25)
    assert app.ledger.batch(bid)["memory_mb"] == 1280, sized                        # the size IS recorded
    assert app.ledger.batch(bid)["state"] != "needs_sizing", sized

    yes = await batch.run_batch(app, ensure_up=_ensure(), batch_id=bid, confirm_spend=True)
    assert yes.phase in ("admitting", "waiting"), (
        f"the size was recorded, yet the yes answered {yes.phase}: {yes.notice}")


async def test_r2_8_a_size_on_a_confirmed_block_still_resumes_without_a_yes():
    """Leaving needs_sizing before ensure_up changes nothing for the plain case: a size that fits the confirmed block
    resumes admission on its own, no yes."""
    from hpc_bridge import server

    app, _rt, runner, bid = await _needs_sizing()
    sent = len(runner.commands)
    res = await server._run_batch(app, batch_id=bid, cores=1, memory_gb=0.5)
    assert res.phase in ("admitting", "waiting") and len(runner.commands) > sent, (res.phase, res.notice)

"""Batch row sizing, review round 1 (two reviewers on 6d476cd: 19 defects, each reproduced there and failing). Fixed
test-first; each test names its item (W: the node side, the wrapper and the measure; F: the server side).

Where a Linux tool is missing on the test host, a fake on PATH stands in, and the test says which:
- `flock`: a python fcntl.flock on the inherited fd (flock(2) locks the open file description, so a child holding the
  fd holds the lock, exactly as util-linux `flock -n FD` does);
- `ps`: real ps plus nlwp = 1 per process, as procps prints `-o nlwp=` on Linux;
- `mv`: real mv after a delay, to widen a real race window deterministically.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import time
from pathlib import Path

import pytest

from tests.test_batch_phase1 import _owner
from tests.test_batch_sizing import (
    _app_with,
    _canary,
    _fake_tool,
    _measure,
    _pool_ledger,
    _report,
    _result,
    _run,
    _script,
    _settle_one,
    _sized,
    _sized_app,
    _slot,
    _spawn,
    _wrapper_env,
)
from tests.test_server import _Res

_BASH = shutil.which("bash")
_PY3 = shutil.which("python3")
needs_bash = pytest.mark.skipif(_BASH is None, reason="needs bash")
needs_py = pytest.mark.skipif(_BASH is None or _PY3 is None, reason="needs bash and python3")
_PSS = Path("/proc/self/smaps_rollup").exists()


def _fake_flock(tmp: Path) -> None:
    _fake_tool(tmp, "flock", f'exec "{sys.executable}" -c \'import fcntl, sys\n'
                             'fd = int(sys.argv[-1])\n'
                             'try:\n    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\n'
                             'except OSError:\n    sys.exit(1)\' "$@"')


def _lock_dir(env: dict[str, str], job: str = "101") -> Path:
    """The wrapper's slot lock directory on the node: the user's uid and the block's job in its name."""
    return Path(env["TMPDIR"]) / f"hpcb-slots-{os.getuid()}-{job}"


def _busy(index: int, attempt: int = 1) -> _Res:
    from hpc_bridge import batch

    return _Res(0, "HPCB_RESULT " + json.dumps({"index": index, "attempt": attempt, "rc": batch.NO_SLOT_RC,
                                                "slot_busy": True, "ran": False, "job": "101",
                                                "remaining_s": 800}) + "\n", "")


def _ensure():
    from hpc_bridge import server

    return server._batch_ensure_up


# ==== the node side ===================================================================================================

@needs_bash
def test_w1_oom_kills_is_the_oom_delta_not_the_stdout_dropped_count(tmp_path):
    """The oom delta's variable was reused for stdout's dropped bytes: a row printing past the cap reported hundreds of
    OOM kills, and a real one would have read 0."""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    script = batch.task_script("i=0; while [ $i -lt 500 ]; do printf x; i=$((i + 1)); done",
                               results_dir=str(tmp_path / "rd"), index=0, attempt=1, task_walltime_s=60,
                               slot=_slot(pin=False), cap_bytes=10)
    rep = _result(_spawn(script, _wrapper_env(tmp_path)))
    assert rep.rc == 0 and rep.truncated
    assert rep.oom_kills in (None, 0), f"oom_kills={rep.oom_kills} (stdout dropped 490 bytes)"


@needs_bash
def test_w2_a_lock_dir_another_user_could_have_made_is_refused(tmp_path):
    """SECURITY: the slot lock directory sits in a shared /tmp, and job ids are sequential. It carries the uid, is made
    with mode 700 (never `mkdir -p`), and one found there is used only when it is a real directory of this user's,
    mode 700 — else the row runs nothing (`why: lock_dir`, LOCK_DIR_RC)."""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    env = _wrapper_env(tmp_path)
    planted = _lock_dir(env)
    planted.mkdir(parents=True)
    planted.chmod(0o777)                                   # open to anyone: not ours alone
    side = tmp_path / "ran"
    rep = _result(_spawn(_script(f"touch {side}", tmp_path / "rd", 0, _slot()), env))
    assert (rep.rc, rep.why) == (batch.LOCK_DIR_RC, "lock_dir") and "not private to this user" in (rep.error or ""), rep
    assert not side.exists()
    planted.chmod(0o700)
    planted.rmdir()
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(mode=0o700)
    planted.symlink_to(elsewhere)                          # a symlink in its place
    rep = _result(_spawn(_script(f"touch {side}", tmp_path / "rd", 1, _slot()), env))
    assert rep.rc == batch.LOCK_DIR_RC and not side.exists(), rep


@needs_bash
def test_w2_a_planted_symlink_never_truncates_the_users_files(tmp_path):
    """`exec N>"$dir/cpu-0.lock"` followed a symlink planted there and TRUNCATED its target. The lock files are opened
    read-write (`<>`, never truncating) and a symlink lock file refuses the row. The old directory name (no uid) is
    ignored outright. flock path: a fake python `flock`."""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    _fake_flock(tmp_path)
    env = _wrapper_env(tmp_path)
    victim = tmp_path / "precious.txt"
    victim.write_text("the user's work\n")
    old = Path(env["TMPDIR"]) / "hpcb-slots-101"          # the name before the fix: another user's to make
    old.mkdir(parents=True)
    old.chmod(0o777)
    (old / "cpu-0.lock").symlink_to(victim)
    rep = _result(_spawn(_script("echo ran", tmp_path / "rd", 0, _slot(cpu_ids=(0, 1))), env))
    assert rep.rc == 0 and victim.read_text() == "the user's work\n"
    mine = _lock_dir(env)
    (mine / "cpu-0.lock").unlink()
    (mine / "cpu-0.lock").symlink_to(victim)               # in our own directory, after the fact
    rep = _result(_spawn(_script("echo ran", tmp_path / "rd", 1, _slot(cpu_ids=(0, 1))), env))
    assert rep.rc == batch.LOCK_DIR_RC and "symlink" in (rep.error or ""), rep
    assert victim.read_text() == "the user's work\n", "the planted symlink's target was truncated by the claim"


@needs_py
def test_w3_the_memory_watch_sees_and_kills_a_child_that_left_the_rows_process_group(tmp_path):
    """A child that calls setpgid(0, 0) — coreutils `timeout` does — or setsid() was invisible to a watch over the
    process group: never killed at its memory. The row is its group, its shell's descendant tree and (Linux) every
    process carrying its token; the same set is measured and killed."""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    hog = (f"{_PY3} -c \"import os, time; os.setpgid(0, 0); b = b'x' * (400 << 20); time.sleep(20)\"")
    t0 = time.monotonic()
    rep = _result(_spawn(_script(hog, tmp_path / "rd", 0, _slot(memory_mb=100, pin=False)), _wrapper_env(tmp_path)))
    assert rep.why == "memory" and rep.rc == batch.MEMORY_RC, (
        f"a 400 MB row with a 100 MB limit ran to its end: rc={rep.rc} why={rep.why!r} peak_rss_kb={rep.peak_rss_kb}")
    assert time.monotonic() - t0 < 15                      # killed, not run out its 20 s


@needs_bash
def test_w4_a_row_waiting_for_its_slot_runs_nothing_after_the_cancel_marker(tmp_path):
    """The CANCELLED marker was checked only before the claim: a row that waited for its slot through cancel(abort)
    started its command when the slot freed. It is checked in the claim loop and after a claim; the row reports
    cancelled, ran false."""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    env = _wrapper_env(tmp_path)
    rd = tmp_path / "rd"
    side = tmp_path / "side-effect"
    one = _slot(cores=1, cpu_ids=(0,), claim_wait_s=30)
    holder = _spawn(_script("sleep 3", rd, 0, one), env)
    time.sleep(1.0)
    waiter = _spawn(_script(f"touch {side}; sleep 5", rd, 1, one), env)
    time.sleep(0.5)
    (rd / batch.CANCEL_MARKER).write_text("")
    _result(holder)
    rep = _result(waiter)
    assert not side.exists(), f"row 1 started after the abort (rc={rep.rc}, why={rep.why!r})"
    assert rep.cancelled and rep.rc == batch.CANCEL_RC


@needs_bash
def test_w5_the_mkdir_fallbacks_stale_takeover_never_gives_one_cpu_to_two_rows(tmp_path):
    """Two rows find cpu 0's claim left by a dead pid. The first moves it aside and claims afresh; the second's `mv`
    then moved the FIRST row's live claim aside and claimed too. After its `mv` a row re-reads the moved pid: not the
    one it judged dead, it puts the claim back and takes the slot as busy. A fake `mv` widens the window."""
    if shutil.which("flock"):
        pytest.skip("this host has flock: the mkdir fallback is not taken")
    env = _wrapper_env(tmp_path)
    _fake_tool(tmp_path, "taskset", "true")
    gate = tmp_path / "mvgate"
    _fake_tool(tmp_path, "mv", f'if mkdir "{gate}" 2>/dev/null; then :; else sleep 2; fi; exec /bin/mv "$@"')
    locks = _lock_dir(env)
    locks.mkdir(parents=True, mode=0o700)
    locks.chmod(0o700)
    (locks / "cpu-0.d").mkdir()
    (locks / "cpu-0.d" / "pid").write_text("999999\n")     # a dead holder: a wrapper SIGKILLed before its release
    probe = tmp_path / "probe"
    probe.mkdir()
    cmd = (f'touch {probe}/run.$HPCB_ROW_INDEX; sleep 4; n=$(ls {probe} | wc -l | tr -d " "); '
           f'rm -f {probe}/run.$HPCB_ROW_INDEX; echo "$n"')
    one = _slot(cores=1, cpu_ids=(0,), claim_wait_s=20)
    a = _spawn(_script(cmd, tmp_path / "rd", 0, one), env)
    b = _spawn(_script(cmd, tmp_path / "rd", 1, one), env)
    ra, rb = _result(a), _result(b)
    ran = [r for r in (ra, rb) if not r.slot_busy]
    assert ran and all(r.tail.strip() == "1" for r in ran), (
        f"both rows ran on cpu 0 at once: cpus {ra.cpus!r}/{rb.cpus!r}, rows seen running {ra.tail!r}/{rb.tail!r}")


def test_w6_the_pools_idle_memory_is_taken_off_when_the_limit_is_the_schedulers_word():
    """With the least memory evidence the scheduler's (SLURM_MEM_PER_NODE, PBS, MemTotal) idle was never looked up:
    rows were fitted against the WHOLE limit, the pool's own use on top — the dangerous direction. The deepest
    memory.current is taken off, else the pool's estimate."""
    from hpc_bridge import sizing

    def parse(*lines: str):
        m = sizing.parse_measure("\n".join(f"HPCB_M {ln}" for ln in lines))
        assert m is not None
        return m

    m = parse("affinity 0-7", "cgroup 0::/slurm/job_5", "env SLURM_MEM_PER_NODE 4000",
              "cg2 /slurm/job_5 memory.max max", "cg2 /slurm/job_5 memory.current 1073741824",
              "env PARSL_WORKER_COUNT 9")
    assert m.mem_limit_mb == 4000 and m.avail_mb is not None and m.avail_mb <= 4000 - 1024, (m.idle_mb, m.avail_mb)
    bare = parse("affinity 0-3", "env SLURM_MEM_PER_NODE 4000", "env PARSL_WORKER_COUNT 5")
    assert bare.idle_mb == sizing.pool_mb(5) and bare.avail_mb == 4000 - sizing.pool_mb(5)


@needs_bash
def test_w7_gpu_rows_on_a_block_whose_gpu_kind_is_unknown_are_still_isolated(tmp_path):
    """GPU ids from SLURM_JOB_GPUS / GPU_DEVICE_ORDINAL with no kind: fit() counted them, the claim handed each row its
    own, but no variable was exported. A kind not known gets every visibility variable for the slot."""
    from hpc_bridge import sizing

    m = sizing.parse_measure("HPCB_M affinity 0-3\nHPCB_M env GPU_DEVICE_ORDINAL 0,1\n")
    assert m is not None and m.gpu_ids == ("0", "1") and m.gpu_kind is None
    assert sizing.fit(m, sizing.Sizing(cores=1, gpus=1)).slots == 2
    _fake_tool(tmp_path, "taskset", "true")
    slot = _slot(cores=1, cpu_ids=(0, 1), gpus=1, gpu_ids=m.gpu_ids, gpu_kind=m.gpu_kind)
    show = ('echo "${CUDA_VISIBLE_DEVICES-u}|${ROCR_VISIBLE_DEVICES-u}|${HIP_VISIBLE_DEVICES-u}|'
            '${ZE_AFFINITY_MASK-u}|${GPU_DEVICE_ORDINAL-u}"')
    env = _wrapper_env(tmp_path, GPU_DEVICE_ORDINAL="0,1")
    ps = [_spawn(_script(f"{show}; sleep 2", tmp_path / "rd", i, slot), env) for i in range(2)]
    seen = sorted(_result(p).tail.strip() for p in ps)
    assert seen == ["0|0|0|0|0", "1|1|1|1|1"], seen


@needs_bash
def test_w7_rocm_rows_get_hip_too_and_cpu_rows_hide_only_where_it_is_documented(tmp_path):
    """On ROCm both ROCR_VISIBLE_DEVICES and HIP_VISIBLE_DEVICES pick a row's GPU. A 0-GPU row is hidden only where an
    empty value is documented to hide every device (CUDA); on another kind its variables are left alone — and status
    says the visibility is not restricted."""
    _fake_tool(tmp_path, "taskset", "true")
    env = _wrapper_env(tmp_path, ROCR_VISIBLE_DEVICES="0,1", HIP_VISIBLE_DEVICES="0,1")
    show = 'echo "${ROCR_VISIBLE_DEVICES-u}|${HIP_VISIBLE_DEVICES-u}"'
    gpu = _result(_spawn(_script(show, tmp_path / "rd", 0, _slot(gpus=1, gpu_ids=("0", "1"), gpu_kind="rocm")), env))
    assert gpu.tail.strip() == "0|0"
    cpu = _result(_spawn(_script(show, tmp_path / "rd", 1, _slot(gpus=0, gpu_ids=("0", "1"), gpu_kind="rocm")), env))
    assert cpu.tail.strip() == "0,1|0,1"                    # not claimed hidden: no value documented to do it


async def test_w7_status_says_gpu_visibility_is_not_restricted_for_cpu_rows_on_a_non_cuda_block():
    app, _rt, _runner = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, gpu_ids=("0", "1"),
                                           gpu_kind="rocm"))
    out = await _run(app, n=2, cores=1, memory_gb=0.5, gpus=0)
    assert "not restricted" in out.notice, out.notice
    cuda, _rt2, _r2 = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, gpu_ids=("0", "1"),
                                         gpu_kind="cuda"))
    assert "not restricted" not in (await _run(cuda, n=2, cores=1, memory_gb=0.5, gpus=0)).notice


async def test_w8_a_row_that_ran_unpinned_is_judged_and_said_as_unpinned():
    """The wrapper's `pinned: false` (taskset missing, or refusing the mask) was dropped and the row judged by the
    batch's ASKED pin. Each attempt's own outcome is recorded, judges over_size, and status says it."""
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5))
    out = await _run(app, n=1, cores=1, memory_gb=0.5)
    runner.futures[0].finish(_report(0, cpu_s=7.6, wall_s=2.0, peak_rss_kb=1024, nproc=4, cpus="0", threads=1,
                                     pinned=False))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id, detail="page")
    t = app.ledger.task(out.batch_id, 0)
    assert st.results[0].over_size == ["cores"] and st.results[0].pinned is False, st.results[0]
    assert "pinned" in t and t["pinned"] == 0
    assert "NOT pinned" in st.notice


@needs_bash
def test_w9_a_pinned_single_threaded_row_is_not_flagged_over_its_cores(tmp_path):
    """The thread count summed the command's own bash subshell: `sleep 3` on 1 CPU read 2 threads and was flagged.
    Linux procps emulated: a fake `ps` printing the real columns plus nlwp = 1 per process."""
    from hpc_bridge import sizing

    _fake_tool(tmp_path, "taskset", "true")
    _fake_tool(tmp_path, "ps", "/bin/ps -A -o pid= -o ppid= -o pgid= -o rss= -o comm= | "
                               "awk '{print $1, $2, $3, $4, 1, $5}'")
    rep = _result(_spawn(_script("sleep 3", tmp_path / "rd", 0, _slot(cores=1)), _wrapper_env(tmp_path)))
    flags = sizing.over_size(cores=1, memory_mb=None, pinned=True, cpu_s=rep.cpu_s, wall_s=rep.wall_s,
                             threads=rep.threads, peak_rss_kb=rep.peak_rss_kb, why=rep.why)
    assert "cores" not in flags, f"`sleep 3` on 1 CPU reported threads={rep.threads} and was flagged {flags}"
    assert rep.threads == 1, rep.threads


@pytest.mark.skipif(_BASH is None or _PY3 is None or not _PSS, reason="Linux Pss: /proc/<pid>/smaps_rollup")
def test_w10_the_memory_watch_does_not_kill_a_forked_pool_for_pages_it_shares(tmp_path):
    """150 MB allocated once, 3 forked workers that only read it: ~150 MB used, but RSS summed over the processes
    counts the shared pages 4 times (~600 MB): killed at a 400 MB limit it never reached. Linux sums Pss; elsewhere
    (no smaps_rollup) the watch falls back to RSS, documented."""
    _fake_tool(tmp_path, "taskset", "true")
    pool = (f"{_PY3} -c \"import os, time\n"
            "b = bytearray(150 << 20)\n"
            "for i in range(0, len(b), 4096): b[i] = 1\n"
            "kids = []\n"
            "for _ in range(3):\n"
            "    pid = os.fork()\n"
            "    if pid == 0:\n"
            "        s = sum(b[i] for i in range(0, len(b), 4096)); time.sleep(5); os._exit(0)\n"
            "    kids.append(pid)\n"
            "[os.waitpid(k, 0) for k in kids]\n"
            "print('fine')\"")
    rep = _result(_spawn(_script(pool, tmp_path / "rd", 0, _slot(memory_mb=400, pin=False)), _wrapper_env(tmp_path)),
                  timeout=60)
    assert rep.why != "memory" and rep.tail.strip() == "fine", f"killed at 400 MB: peak_rss_kb={rep.peak_rss_kb}"


@needs_bash
def test_w11_a_row_that_execs_its_program_still_reports_its_cpu_seconds(tmp_path):
    """cpu_s came only from the command shell's EXIT trap: an `exec` (or the row's own trap) lost it, and the pilot read
    null as 0. The wrapper takes `times` around its own wait; a null cpu_s is no evidence."""
    from hpc_bridge import sizing

    _fake_tool(tmp_path, "taskset", "true")
    busy = "exec sh -c 'end=$(( $(date +%s) + 2 )); while [ $(date +%s) -lt $end ]; do :; done'"
    rep = _result(_spawn(_script(busy, tmp_path / "rd", 0, _slot(pin=False)), _wrapper_env(tmp_path)))
    assert rep.rc == 0 and rep.cpu_s is not None and rep.cpu_s >= 0.8, rep
    m = _measure(cpus=4, workers=5)
    assert sizing.pilot_proposal([{"cpu_s": None, "wall_s": 2.0, "peak_rss_kb": 1024}], m).cores is None


# ==== the server side =================================================================================================

async def test_f1_a_block_asked_by_cores_after_a_derived_one_runs_one_worker_per_core():
    """A derived block's hpcb_workers outlived it: the refusal's own advice for a bigger block,
    ensure_endpoint_up(cores=32), still dispatched 5 workers. They belong to the derived block only: cleared by a block
    asked without derivation, never kept in the session's choice. (Its row settles first: a switch under batch rows in
    flight is refused, review R2-H.)"""
    from hpc_bridge import batch, server
    from hpc_bridge.resources import CORES, WORKERS

    app = _sized_app(_canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192)))
    out = await _run(app, n=1, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    assert out.batch_id and app.built[-1]["max_workers_per_node"] == 5
    assert WORKERS not in app.compute_choice
    await batch.tick(app, out.batch_id)
    assert not app.ledger.counts(out.batch_id)["in_flight"]
    st = await server._ensure_endpoint_up(app, "compute", cores=32, memory_gb=40, confirm_spend=True)
    assert st.status == "up" and app.built[-1][CORES] == 32
    assert app.built[-1]["max_workers_per_node"] == 32, app.built[-1]


async def test_f1_the_derived_blocks_own_size_restated_is_that_block():
    """Review round 2 (R2-H) reverses round 1's reading: the derived block's own size asked again by size alone is the
    SAME block — its workers kept, no question, nothing started (round 1 had dropped them: another user endpoint, a
    second billed block)."""
    from hpc_bridge import server
    from hpc_bridge.resources import WORKERS

    app = _sized_app(_canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192)))
    await _run(app, n=1, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    built = len(app.built)
    st = await server._ensure_endpoint_up(app, "compute", cores=9, memory_gb=7)
    assert st.status == "up" and len(app.built) == built, st
    assert app.shapes["compute"].user_endpoint_config.get(WORKERS) == 5


async def test_f2_size_first_yes_on_a_whole_node_partition_starts_the_whole_node():
    """On a whole-node partition the derived slice was refused before the yes was read: the same question forever.
    The block derived there IS the whole node, with the workers that fit; the yes starts it."""
    app = _sized_app(_canary(_measure(cpus=128, cpu_ids=tuple(range(128)), workers=33, mem_limit_mb=250000)))
    app.compute_choice["partition"] = "wholenode"
    ask = await _run(app, n=4, cores=4, memory_gb=2.0, concurrency=2)
    assert ask.phase == "needs_confirmation" and ask.block.whole_node is True and ask.block.derived_from, ask
    yes = await _run(app, n=4, cores=4, memory_gb=2.0, concurrency=2, confirm_spend=True)
    assert yes.phase in ("admitting", "waiting"), (yes.phase, yes.notice)
    assert app.built and app.built[-1].get("max_workers_per_node") == 32       # ⌊127 ÷ 4⌋ = 31 rows + 1


async def test_f2_a_derived_size_the_facility_cannot_take_is_refused_by_name(monkeypatch):
    """Ruling 5: a facility endpoint whose template renders no `cores_per_node` cannot take the derived block — refused
    with its own words, never a spend question about another block, nor `derived_from` on terms not derived."""
    from tests.test_mep_server import DELTA_SCHEMA, _Templated
    from tests.test_resource_requests import _anvil_like
    from tests.test_spend_terms import _mep_app

    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Templated(
        DELTA_SCHEMA, "provider:\n  mem_per_node: {{ mem_per_node | default(none) }}\n"))
    app, _ = await _mep_app(monkeypatch, _anvil_like())
    app.batch_autoloop = False
    out = await _run(app, n=4, cores=2, memory_gb=1.0, concurrency=2)
    assert out.phase == "refused" and "no `cores_per_node` setting" in (out.notice or ""), out
    assert out.block is None


async def test_f3_stop_endpoint_is_refused_while_a_needs_sizing_batch_has_rows_in_flight():
    """A default-memory kill pauses the batch at needs_sizing while its other rows run on. A later server's stop read
    only admitting / needs_confirmation / cancelled batches, and released the block under 3 rows in flight."""
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=8)
    runner.futures[0].finish(_report(0, rc=batch.MEMORY_RC, why="memory", cpu_s=1.0, wall_s=1.0,
                                     peak_rss_kb=900 * 1024, nproc=1, cpus="0"))
    await batch.tick(app, out.batch_id)
    assert app.ledger.batch(out.batch_id)["state"] == "needs_sizing"
    assert app.ledger.counts(out.batch_id)["in_flight"] == 3
    app.batches.pop(out.batch_id)                          # a later server: the batch is the ledger's
    assert batch.in_flight_note(app) is not None, "stop_endpoint would release the block under 3 rows in flight"
    assert "needs_sizing" in batch.ACTIVE_STATES and "needs_sizing" in batch.PAUSABLE_STATES


async def test_f4_a_resize_after_the_pilot_keeps_the_rows_gpus_and_names_them():
    """The pilot's notice said run_batch(batch_id=…, cores=…, memory_gb=…); doing that wrote gpus=0 over a GPU batch.
    A resize changes only what it names; a pause of a GPU batch names gpus= too."""
    from hpc_bridge import batch, server

    app, _rt, runner = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, mem_limit_mb=16000,
                                          idle_mb=500, gpu_ids=("0", "1"), gpu_kind="cuda"))
    out = await _run(app, n=8, pilot=1, gpus=1)
    assert len(runner.commands) == 1, out
    runner.futures[0].finish(_report(0, cpu_s=2.0, wall_s=2.0, peak_rss_kb=400 * 1024, nproc=8, cpus="0"))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id)
    assert st.phase == "needs_sizing" and st.proposal is not None and st.proposal.gpus == 1, st
    assert "cores=" in st.reason and "gpus=" in st.reason
    again = await server._run_batch(app, batch_id=out.batch_id, cores=st.proposal.cores,
                                    memory_gb=st.proposal.memory_gb)
    assert again.phase == "admitting", again
    assert app.ledger.batch(out.batch_id)["gpus"] == 1, "the resize dropped the rows' GPU"


async def test_f4_a_resize_of_memory_only_keeps_the_rows_cores_and_their_pin():
    from hpc_bridge import server

    app, _rt, _runner = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, mem_limit_mb=16000))
    out = await _run(app, n=8, cores=4, memory_gb=1.0, pin=False)
    assert out.phase == "admitting"
    await server._run_batch(app, batch_id=out.batch_id, memory_gb=2.0)
    b = app.ledger.batch(out.batch_id)
    assert (b["cores"], b["memory_mb"], b["pin"]) == (4, 2048, 0), "a memory-only resize changed the rows' size"
    await server._run_batch(app, batch_id=out.batch_id, cores=2)
    b = app.ledger.batch(out.batch_id)
    assert (b["cores"], b["pin"]) == (2, 0) and b["memory_mb"] != 2048   # memory, unnamed, is the block's share again
    assert "memory_gb" in json.loads(b["sizing_defaulted"])


async def test_f5_a_pilot_row_that_found_its_slot_busy_runs_again_and_the_pilot_proposes():
    """A pilot row sent back (slot_busy) counted as run: nothing was ever admitted again — a dead end. Only rows out or
    settled count."""
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=8, pilot=1)
    assert len(runner.commands) == 1
    runner.futures[0].finish(_busy(0))
    await batch.tick(app, out.batch_id)
    assert app.ledger.task(out.batch_id, 0)["state"] == "backlog"
    app.ledger._con.execute("update task set not_before = ? where batch_id = ?", (time.time() - 1, out.batch_id))
    await batch.tick(app, out.batch_id)
    assert len(runner.commands) == 2, "the pilot's row was never sent again: the pilot cannot finish"
    runner.futures[1].finish(_report(0, attempt=2, cpu_s=1.0, wall_s=1.0, peak_rss_kb=100 * 1024, nproc=4, cpus="0"))
    await batch.tick(app, out.batch_id)
    assert (await batch.status(app, out.batch_id)).phase == "needs_sizing"


async def test_f5_a_pilot_on_a_gpu_block_runs_with_the_blocks_gpus():
    app, _rt, runner = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, mem_limit_mb=16000,
                                          idle_mb=500, gpu_ids=("0", "1"), gpu_kind="cuda"))
    out = await _run(app, n=4, pilot=1)
    assert len(runner.commands) == 1, out
    t = app.ledger.task(out.batch_id, 0)
    assert (t["req_cores"], t["req_gpus"]) == (8, 2), "the pilot row was not given the block's GPUs"


async def test_f5_a_pilot_whose_rows_cannot_fit_the_block_is_refused_not_wedged():
    """pilot=1 with gpus=2 on a 1-GPU block sat `admitting` forever, silent: refused, with the numbers."""
    app, _rt, runner = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, mem_limit_mb=16000,
                                          gpu_ids=("0",), gpu_kind="cuda"))
    out = await _run(app, n=4, pilot=1, gpus=2)
    assert out.phase == "refused" and "GPU" in out.reason and runner.commands == [], out


async def test_f5_a_pilot_on_a_block_measured_late_pauses_with_the_reason():
    from hpc_bridge import batch

    app, rt, runner = _app_with(None)
    out = await _run(app, n=4, pilot=1, gpus=2)
    assert out.batch_id and runner.commands == []
    rt.last_canary = _canary(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, gpu_ids=("0",), gpu_kind="cuda"))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id)
    assert st.phase == "needs_sizing" and "GPU" in st.reason and runner.commands == [], st


async def test_f6_a_row_killed_at_the_default_memory_after_the_resize_is_retried():
    """Whether an attempt's memory was the default is its own record: row 1, still out at the default when the user
    resized, was judged by the batch's NEW flags and failed for good. Idempotent: it runs again at the user's size."""
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=8, idempotent=True)
    assert out.sizing.defaulted and len(runner.commands) == 4
    runner.futures[0].finish(_report(0, rc=batch.MEMORY_RC, why="memory", cpu_s=1.0, wall_s=1.0,
                                     peak_rss_kb=900 * 1024, nproc=1, cpus="0"))
    await batch.tick(app, out.batch_id)
    assert app.ledger.batch(out.batch_id)["state"] == "needs_sizing"
    resumed = await batch.run_batch(app, ensure_up=_ensure(), batch_id=out.batch_id, cores=1, memory_gb=1.25)
    assert resumed.phase == "admitting"
    t1 = app.ledger.task(out.batch_id, 1)
    assert (t1["req_memory_mb"], t1["req_mem_default"]) == (875, 1)        # still out at the default share
    runner.futures[1].finish(_report(1, rc=batch.MEMORY_RC, why="memory", cpu_s=1.0, wall_s=1.0,
                                     peak_rss_kb=880 * 1024, nproc=1, cpus="1"))
    await batch.tick(app, out.batch_id)
    assert app.ledger.task(out.batch_id, 1)["state"] != "failed"
    assert app.ledger.batch(out.batch_id)["state"] == "admitting"          # the user chose a size: no new question


def test_f7_a_waiting_batch_of_big_rows_is_not_starved_by_one_of_small_rows(tmp_path):
    """A (1-CPU rows, a long backlog) holds 4 CPUs; B (4-CPU rows) waits, ranked first. Each freed CPU went back to A.
    The head waiter's row now has its room held: A admits only beyond it, and B starts once 4 CPUs are free."""
    led = _pool_ledger(tmp_path, {"cpus": 4, "cpu_ids": [0, 1, 2, 3], "mem_limit_mb": 16000, "idle_mb": 0,
                                  "gpu_ids": [], "workers": 9})
    me = _owner()
    a = _sized(led, "batch-00000000000a", n=40, cores=1, memory_mb=100, owner=me)
    b = _sized(led, "batch-00000000000b", n=2, cores=4, memory_mb=100, owner=me)
    t = 100.0
    assert len(led.reserve(a, me, 8, uec_hash="uec-1", now=t)) == 4
    got_b = 0
    for _ in range(12):
        t += 1.0
        got_b += len(led.reserve(b, me, 8, uec_hash="uec-1", now=t))
        if got_b:
            break
        t += 1.0
        _settle_one(led, a)
        led.reserve(a, me, 8, uec_hash="uec-1", now=t)
    assert got_b, "B (4-CPU rows, waiting, fewer out) never got the block while A had a backlog"


def test_f7_a_pilot_waiting_for_the_whole_block_is_not_starved(tmp_path):
    """A pilot holds the whole block (all 4 CPUs): beside a batch of 1-CPU rows it waited for every row to free at once
    — never, while the other had a backlog."""
    led = _pool_ledger(tmp_path, {"cpus": 4, "cpu_ids": [0, 1, 2, 3], "mem_limit_mb": 16000, "idle_mb": 0,
                                  "gpu_ids": [], "workers": 9})
    me = _owner()
    a = _sized(led, "batch-00000000000a", n=40, cores=1, memory_mb=100, owner=me)
    p = _sized(led, "batch-00000000000c", n=4, cores=4, memory_mb=16000, concurrency=1, owner=me)   # the pilot's size
    t = 100.0
    assert len(led.reserve(a, me, 8, uec_hash="uec-1", now=t)) == 4
    got = 0
    for _ in range(12):
        t += 1.0
        got += len(led.reserve(p, me, 8, uec_hash="uec-1", now=t))
        if got:
            break
        t += 1.0
        _settle_one(led, a)
        led.reserve(a, me, 8, uec_hash="uec-1", now=t)
    assert got == 1


async def test_f8_a_needs_sizing_batch_whose_block_needs_a_yes_resumes_once_both_are_given():
    """At needs_sizing on an unconfirmed block, the sized call answered the spend question and DROPPED the size; the
    yes then asked for the size again — forever. The size is recorded before the spend question, the yes alone then
    resumes, and each answer names both when both are needed."""
    from hpc_bridge import batch, server

    app, rt, _runner = _app_with(None)
    out = await _run(app, n=4, cores=4)
    rt.last_canary = _canary(_measure(cpus=2, cpu_ids=(0, 1), workers=3))
    await batch.tick(app, out.batch_id)
    assert app.ledger.batch(out.batch_id)["state"] == "needs_sizing"
    rt.spend_confirmed = False                              # the block needs the user's yes again
    neither = await server._run_batch(app, batch_id=out.batch_id)
    assert neither.phase == "needs_sizing" and "cores=" in neither.notice and "confirm_spend=True" in neither.notice
    sized = await server._run_batch(app, batch_id=out.batch_id, cores=1, memory_gb=0.5)
    assert sized.phase == "needs_confirmation" and "recorded" in sized.notice, sized
    assert app.ledger.batch(out.batch_id)["cores"] == 1
    yes = await server._run_batch(app, batch_id=out.batch_id, confirm_spend=True)
    assert yes.phase in ("admitting", "waiting"), (yes.phase, yes.notice)


async def test_f8_the_yes_on_a_needs_sizing_batch_confirms_the_block_then_asks_for_the_size():
    from hpc_bridge import batch, server

    app, rt, runner = _app_with(None)
    out = await _run(app, n=4, cores=4)
    rt.last_canary = _canary(_measure(cpus=2, cpu_ids=(0, 1), workers=3))
    await batch.tick(app, out.batch_id)
    rt.spend_confirmed = False
    yes = await server._run_batch(app, batch_id=out.batch_id, confirm_spend=True)
    assert yes.phase == "needs_sizing" and "confirmed" in yes.notice and rt.spend_confirmed, yes
    assert runner.commands == []

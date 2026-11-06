"""Review round 1 of the Anvil live test's fixes (on 0eaf3e9), node side: the generated bash run for real. Written failing
first; every test passes under bash 3.2 (macOS) and 5.2 (Linux) — the ones that read /proc's threads skip on macOS.

- R1-1: a row of thousands of threads — the full look's awk took every thread's stat path in ONE argument, past
  Linux's 128 KiB cap at ~4-5k threads: no full look at all (threads, r_samples, srun, memory lost), the most
  oversubscribed rows unflagged, and a watcher costing a quarter to half a CPU. Its cost is measured on such a row.
- R1-2: an older hpc-bridge endpoint venv's bin in a PATH an earlier command persisted.
- R1-5: the runnable-thread counts written to the results directory once a look (GPFS on Anvil).
- R1-7: an init that calls `exit`; the young window eaten by a slow activation; the login node's check's nomodule.
(R1-8, the weak tests, are mended where they are: tests/test_anvil_live_findings{,_node}.py.)
"""
from __future__ import annotations

import os
import resource
import shutil
import subprocess
import time

import pytest

from tests.test_anvil_live_findings_node import (
    _BUSY,
    _CHILDREN,
    _LINUX,
    _PY3,
    NONCE,
    _act,
    _endpoint_venv,
    _init_at,
    _judged,
    _row,
    _session,
    _slot,
)
from tests.test_batch_sizing import _fake_tool, _wrapper_env

_BASH = shutil.which("bash")
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]


def _threads_py(n: int, secs: int, *, report: bool = False) -> str:
    """A python of `n` idle threads for `secs` s (and, with `report`, its own CPU-seconds on a ROWCPU line)."""
    tail = ("\nr = resource.getrusage(resource.RUSAGE_SELF); print('ROWCPU', r.ru_utime + r.ru_stime)"
            if report else "")
    return (f"python3 -c \"import threading, time, resource\nev = threading.Event()\n"
            f"ts = [threading.Thread(target=ev.wait, daemon=True) for _ in range({n})]\n"
            f"[t.start() for t in ts]\ntime.sleep({secs}){tail}\"")


# ---- R1-1: thousands of threads ---------------------------------------------------------------------------------------

@pytest.mark.skipif(not _LINUX or _PY3 is None or shutil.which("timeout") is None,
                    reason="Linux: thread states from /proc")
def test_r1_1_a_row_of_six_thousand_threads_still_gets_its_full_look_and_is_flagged(tmp_path):
    """The reviewer's run: 6000 threads and 4 busy loops on 1 core read over_size [] — while 100 threads read
    ['cores'] — the most oversubscribed rows went unflagged."""
    # the 4 loops spin for exactly the python's life: on a slow runner its 6000 thread starts, sharing the one pinned
    # core with them, outlast a fixed 8 s of spinning (CI: 6 of 14 looks over) — then the row's busy part was the minority
    busy = " & ".join(["(while kill -0 $p 2>/dev/null; do :; done)"] * 4)
    rep = _row(tmp_path, f"{_threads_py(6000, 6)} & p=$!; {busy} & wait", _slot(cores=1, memory_mb=4000))
    assert rep.rc == 0, (rep.rc, rep.stderr_tail)
    assert rep.threads is not None and rep.threads >= 6000, rep.threads
    assert (rep.r_samples or 0) >= 3 and rep.peak_rss_kb, (rep.r_samples, rep.peak_rss_kb)
    assert _judged(rep, 1) == ["cores"], (rep.r_over, rep.r_samples)


@pytest.mark.skipif(not _LINUX or _PY3 is None, reason="Linux: the cost is /proc's (a look per thread)")
def test_r1_1_the_watchers_cost_on_a_row_of_three_thousand_idle_threads_stays_small(tmp_path):
    """3000 idle threads had cost the watcher ~28% of a CPU (6000: ~50%), on an unpinned watcher taking other rows'
    CPUs. The whole wrapper less the row's own CPU-seconds (the python's, from inside it)."""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    script = batch.task_script(_threads_py(3000, 10, report=True), results_dir=str(tmp_path / "rd"), index=0,
                               attempt=1, task_walltime_s=60, slot=_slot(cores=1, memory_mb=2000, pin=False))
    r0 = resource.getrusage(resource.RUSAGE_CHILDREN)
    t0 = time.monotonic()
    p = subprocess.run([_BASH, "-c", script], capture_output=True, text=True, env=_wrapper_env(tmp_path), timeout=120)
    wall = time.monotonic() - t0
    r1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    rep = batch.parse_result(p.stdout)
    assert rep is not None and rep.rc == 0, p.stdout[-300:]
    row = float(next(ln for ln in rep.tail.splitlines() if ln.startswith("ROWCPU")).split()[1])
    watcher = (r1.ru_utime - r0.ru_utime) + (r1.ru_stime - r0.ru_stime) - row
    print(f"\nwatcher on 3000 threads: {watcher:.2f} CPU-s over {wall:.1f} s ({100 * watcher / wall:.1f}% of a CPU)")
    assert rep.threads is not None and rep.threads >= 3000 and (rep.r_samples or 0) >= 3, rep
    # (6% measured alone in the test container; the bound leaves room for a loaded machine, and stays far under the
    # 28% the uncapped look cost)
    assert watcher / wall < 0.15, f"the watcher used {watcher:.2f} CPU-s over {wall:.1f} s"


# ---- R1-2: an older endpoint venv in a persisted PATH -------------------------------------------------------------------

def test_r1_2_an_older_endpoint_venvs_bin_in_a_persisted_path_is_dropped_too(tmp_path):
    """A .env saved before the fix — or before an endpoint version or Python bump — still carries an OLDER keyed
    hpc-bridge venv's bin: its pip and python must not run either. A venv of the user's own stays."""
    old = _endpoint_venv(tmp_path)                                         # gce-venv-endpoint-4.18.0-py3.13
    new = tmp_path / "home" / "hpc-bridge" / "gce-venv-endpoint-4.19.0-py3.13"
    (new / "bin").mkdir(parents=True)
    (new / ".hpcb-arch").write_text("x86_64\n")
    seed = tmp_path / "home" / "hpc-bridge" / "gce-venv-4.17.0-py3.12"   # a registry entry's worker venv: no marker
    (seed / "bin").mkdir(parents=True)
    mine = tmp_path / "home" / "venvs" / "analysis"
    (mine / "bin").mkdir(parents=True)
    sd = tmp_path / "scratch" / "sessions" / "default"
    sd.mkdir(parents=True)
    (sd / ".env").write_text(f"export PATH={mine / 'bin'}:{old / 'bin'}:{seed / 'bin'}:{os.environ['PATH']}\n")
    p = _session(tmp_path, 'echo "P=$PATH"; pip install six >/dev/null 2>&1; true', None,
                 VIRTUAL_ENV=str(new), PATH=f"{new / 'bin'}:{tmp_path / 'bin'}:{os.environ['PATH']}")
    path = next(ln for ln in p.stdout.splitlines() if ln.startswith("P="))[2:].split(":")
    assert str(old / "bin") not in path and str(new / "bin") not in path and str(seed / "bin") not in path, path
    assert str(mine / "bin") in path, path
    assert not (old / "lib").exists(), "the old endpoint venv's pip ran"


# ---- R1-5: the counts are not a write per look ------------------------------------------------------------------------

@pytest.mark.skipif(not _LINUX, reason="Linux: runnable threads from /proc")
def test_r1_5_the_runnable_counts_are_kept_by_the_watcher_and_written_when_the_watch_ends(tmp_path):
    """`.rq` was rewritten every look into the results directory — GPFS on Anvil: ~127 metadata writes a second for a
    full node. The counts are the watcher's until it ends (or a rare flush); the result still carries them."""
    from hpc_bridge import batch
    from tests.test_batch_sizing import _spawn

    _fake_tool(tmp_path, "taskset", "true")
    script = batch.task_script("sleep 6", results_dir=str(tmp_path / "rd"), index=0, attempt=1, task_walltime_s=60,
                               slot=_slot(cores=1))
    p = _spawn(script, _wrapper_env(tmp_path))
    time.sleep(4)
    mid = list((tmp_path / "rd").glob("tasks/0/attempt-1/.rq"))
    out, _err = p.communicate(timeout=60)
    rep = batch.parse_result(out)
    assert rep is not None and (rep.r_samples or 0) >= 3, rep
    assert mid == [], "the counts were written to the results directory mid-row"


# ---- R1-7: an init that exits; a slow activation and the young window; the login check's nomodule --------------------

def test_r1_7_an_init_that_calls_exit_is_a_refusal_never_a_command_that_ran(tmp_path):
    from hpc_bridge import envs

    _init_at(tmp_path / "sys" / "etc" / "profile.d" / "modules.sh", "echo broken init >&2\nexit 3\n")
    p = _session(tmp_path, "echo RAN", _act("module load gcc"))
    assert "RAN" not in p.stdout and p.returncode == envs.ENV_RC, (p.returncode, p.stdout, p.stderr[-300:])
    assert envs.refusal(p.returncode, p.stderr, NONCE) is not None, p.stderr[-300:]


def test_r1_7_an_init_that_calls_exit_in_a_batch_row_ran_nothing(tmp_path):
    from hpc_bridge import envs

    _init_at(tmp_path / "sys" / "etc" / "profile.d" / "modules.sh", "exit 3\n")
    rep = _row(tmp_path, "echo RAN", _slot(cores=1), env_sh=envs.activation_sh(_act("module load gcc"), NONCE),
               HPCB_SYSROOT=str(tmp_path / "sys"))
    assert "RAN" not in rep.tail and rep.rc == envs.ENV_RC and rep.ran is False, (rep.rc, rep.ran, rep.stderr_tail)


@pytest.mark.skipif(_PY3 is None or not _CHILDREN, reason="needs python3, and Linux /proc children (the fast looks)")
def test_r1_7_a_slow_activation_never_eats_the_young_window(tmp_path):
    """The young window began with the watcher: an env whose activation takes 3 s left the row's own first seconds to
    the 1 s looks. (A regression guard: on 0eaf3e9 its outcome depends on where the 1 s looks fall.)"""
    from hpc_bridge import batch, envs

    grow = ("python3 -c \"import time; b = []; [(b.append(b'x' * (25 << 20)), time.sleep(0.1)) for _ in range(40)]; "
            "time.sleep(5)\"")
    rep = _row(tmp_path, grow, _slot(cores=1, memory_mb=100, pin=False),
               env_sh=envs.activation_sh(_act("sleep 3"), NONCE))
    assert rep.rc == batch.MEMORY_RC and rep.why == "memory", (rep.rc, rep.why, rep.stderr_tail)
    assert rep.peak_rss_kb < 200 * 1024, f"killed at {rep.peak_rss_kb // 1024} MB on a 100 MB slot"


def test_r1_7_the_login_nodes_check_says_no_module_system(tmp_path):
    """The check on the login node stripped the refusal line, and with it `nomodule`: its answer must carry it."""
    from hpc_bridge import envs

    (tmp_path / "sys").mkdir()
    p = subprocess.run([_BASH, "-c", envs.activate_check_script("module load anaconda/2025.12-py313")],
                       capture_output=True, text=True, timeout=60,
                       env={"PATH": os.environ["PATH"], "HOME": str(tmp_path), "HPCB_SYSROOT": str(tmp_path / "sys")})
    line = next(ln for ln in p.stdout.splitlines() if ln.startswith("HPCB_ENV_ACTIVATE "))
    assert " nomodule" in line and "rc=0 " not in line, line

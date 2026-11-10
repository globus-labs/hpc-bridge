"""The Anvil live test's findings (2026-11-05; Purdue Anvil: Slurm 23.11, 128-core EPYC nodes, GPFS scratch, RHEL 8,
cgroup v1), node side: the generated bash run for real, with fakes for what the node has (srun, a module init, an
endpoint venv, uv). Written failing first; every test passes under bash 3.2 (macOS) and 5.2 (Linux). Server-side and
pure tests: tests/test_anvil_live_findings.py.

- F3: a batch row's `srun` died on Slurm 23+ — `SLURM_TRES_PER_TASK=cpu=<block CPUs>` disagreed with the row's own
  SLURM_CPUS_PER_TASK, then the worker launcher's `SLURM_CPU_BIND=…mask_cpu:<the block's mask>` bound outside the
  step. A fake srun makes both checks as Slurm 23.11 does. Whether a row ran srun steps is in its result.
- F1: a pinned row was flagged over its cores for counting idle threads (the shell, `timeout`, a sleeping python):
  only RUNNABLE threads above its cores in most samples flag it now (Linux: /proc's thread states).
- F2: a row allocating fast overshot its memory before the 1 s sample saw it: a young or growing row is sampled
  faster; the watcher's cost is measured.
- F5: `module` is not defined in a facility worker's non-login shell: the activation sources the first module init
  there is, and a line that still finds none is refused saying so.
- F4: user commands ran with VIRTUAL_ENV (and PATH) naming hpc-bridge's own endpoint venv, so `uv pip install` / `pip
  install` wrote into the venv the worker runs from.
"""
from __future__ import annotations

import os
import resource
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.test_batch_sizing import _fake_tool, _result, _spawn, _wrapper_env

_BASH = shutil.which("bash")
_PY3 = shutil.which("python3")
_LINUX = sys.platform.startswith("linux") and Path("/proc/self/task").is_dir()
# the watcher's fast looks walk the row's tree by /proc/<pid>/task/<tid>/children (Linux, CONFIG_PROC_CHILDREN)
_CHILDREN = _LINUX and any(Path("/proc/self/task").glob("*/children"))
pytestmark = [pytest.mark.skipif(_BASH is None, reason="needs bash")]
NONCE = "a1b2c3d4e5f60718"


def _slot(**kw):
    from hpc_bridge import batch

    base = dict(cores=1, cpu_ids=tuple(range(8)), pin=True, claim_wait_s=20)
    base.update(kw)
    return batch.Slot(**base)


def _row(tmp: Path, command: str, slot=None, *, env_sh: str | None = None, session=None, walltime: float = 60,
         index: int = 0, **env: str):
    """One batch row as the worker runs it: its wrapper under bash, a fake taskset (the pin's call succeeds)."""
    from hpc_bridge import batch

    if not (tmp / "bin" / "taskset").exists():
        _fake_tool(tmp, "taskset", "true")
    script = batch.task_script(command, results_dir=str(tmp / "rd"), index=index, attempt=1,
                               task_walltime_s=walltime, slot=slot, env_sh=env_sh, session=session)
    return _result(_spawn(script, _wrapper_env(tmp, **env)), timeout=120)


# =====================================================================================================================
# F3 — srun inside a batch row
# =====================================================================================================================

# The block's job as the parsl worker launcher's step leaves it in the worker's environment on Anvil (Slurm 23.11,
# task/affinity): the job's per-task TRES and the launcher step's CPU binding to the block's 7 CPUs.
_ANVIL_STEP_ENV = {
    "SLURM_CPUS_PER_TASK": "7", "SLURM_TRES_PER_TASK": "cpu=7",
    "SLURM_CPU_BIND": "quiet,mask_cpu:0x00000000F8000000", "SLURM_CPU_BIND_LIST": "0x00000000F8000000",
    "SLURM_CPU_BIND_TYPE": "mask_cpu:", "SLURM_CPU_BIND_VERBOSE": "quiet",
}

# A fake srun reading its input environment as Slurm 23.11 does: the two cpus-per-task variables must agree, a
# mask_cpu binding names CPUs outside a step Slurm placed itself, and a step that does not overlap the pool's own waits
# behind it. The step's own client process (named srun) lives while the step runs.
_FAKE_SRUN = r"""
if [ -n "${SLURM_TRES_PER_TASK:-}" ] && [ -n "${SLURM_CPUS_PER_TASK:-}" ]; then
  c=$(printf '%s\n' "$SLURM_TRES_PER_TASK" | tr ',' '\n' | sed -n 's/^cpu[=:]//p' | head -n 1)
  if [ -n "$c" ] && [ "$c" != "$SLURM_CPUS_PER_TASK" ]; then
    echo "srun: fatal: cpus-per-task set by two different environment variables SLURM_CPUS_PER_TASK=$SLURM_CPUS_PER_TASK != SLURM_TRES_PER_TASK=$SLURM_TRES_PER_TASK" >&2
    exit 1
  fi
fi
case "${SLURM_CPU_BIND:-}" in
  *mask_cpu*|*map_cpu*)
    echo "srun: error: CPU binding outside of job step allocation, allocated CPUs are: 0x0000000060000000." >&2
    echo "srun: error: Task launch for StepId=101.7 failed on node node-a: Unable to satisfy cpu bind request" >&2
    exit 1;;
esac
if [ "${SLURM_OVERLAP:-}" != 1 ]; then
  echo "srun: Job 101 step creation temporarily disabled, retrying (Requested nodes are busy)" >&2; exit 1
fi
while [ $# -gt 0 ]; do case "$1" in -*) shift;; *) break;; esac; done
"$(dirname "$0")/libexec/srun" -c 'sleep "$0"; :' 2 &
"$@"
rc=$?
wait
exit $rc"""


def _fake_srun(tmp: Path) -> None:
    _fake_tool(tmp, "srun", _FAKE_SRUN)
    (tmp / "bin" / "libexec").mkdir(exist_ok=True)
    # the step's client, as ps shows it: bash under the name srun, sleeping (`:` last, so bash never execs the sleep).
    # Not `sleep` itself under that name: Ubuntu 26.04's is uutils coreutils' multicall binary, which refuses to run
    # under a name not its own. Not a script: macOS's ps names a script's process by its interpreter.
    link = tmp / "bin" / "libexec" / "srun"
    if not link.exists():
        link.symlink_to(_BASH)


def test_f3_a_rows_srun_runs_under_slurm_23s_environment(tmp_path):
    """Anvil: `srun -n1 bash -c …` in a 1-CPU row on a 7-CPU block died (SLURM_CPUS_PER_TASK=1 != SLURM_TRES_PER_TASK=
    cpu=7), and once that agreed, on the launcher step's mask_cpu binding. The row's env lets srun run."""
    _fake_srun(tmp_path)
    rep = _row(tmp_path, "srun -n1 bash -c 'echo HPCB_STEP_OK'", _slot(cores=1), **_ANVIL_STEP_ENV)
    assert rep.rc == 0 and "HPCB_STEP_OK" in rep.tail, (rep.rc, rep.tail, rep.stderr_tail)


def test_f3_the_rows_tres_agrees_with_its_cpus_and_keeps_the_rest_and_no_binding_is_inherited(tmp_path):
    env = {**_ANVIL_STEP_ENV, "SLURM_TRES_PER_TASK": "cpu=7,gres/gpu=1"}
    cmd = ('printf "%s|%s|%s|%s|%s|%s\\n" "${SLURM_TRES_PER_TASK-unset}" "${SLURM_CPUS_PER_TASK-unset}" '
           '"${SLURM_CPU_BIND-unset}" "${SLURM_CPU_BIND_LIST-unset}" "${SLURM_CPU_BIND_TYPE-unset}" '
           '"${SLURM_CPU_BIND_VERBOSE-unset}"')
    rep = _row(tmp_path, cmd, _slot(cores=2), **env)
    assert rep.tail.strip() == "cpu=2,gres/gpu=1|2|unset|unset|unset|unset", rep.tail


def test_f3_on_slurm_22_no_tres_variable_is_made_up(tmp_path):
    """Slurm before 23.11 sets no SLURM_TRES_PER_TASK: the row sets none either (its SLURM_CPUS_PER_TASK and
    SRUN_CPUS_PER_TASK size the step there)."""
    env = {k: v for k, v in _ANVIL_STEP_ENV.items() if k != "SLURM_TRES_PER_TASK"}
    rep = _row(tmp_path, 'echo "[${SLURM_TRES_PER_TASK-unset}] $SRUN_CPUS_PER_TASK"', _slot(cores=3), **env)
    assert rep.tail.strip() == "[unset] 3", rep.tail


# Ubuntu 26.04's `sleep` is uutils coreutils' multicall binary, which refuses to run under a name not its own ("Security
# violation: Requested utility `srun` does not match executable name"). A `sleep` that refuses another name as that one
# does, and runs the real one under its own.
_MULTICALL_SLEEP = """case "${{0##*/}}" in sleep) exec {real} "$@";; esac
echo "Security violation: Requested utility \\`${{0##*/}}\\` does not match executable name" >&2
exit 1"""


@pytest.mark.parametrize("sleep", ["host", "multicall"])
def test_f3_the_result_says_when_a_row_ran_srun_steps(tmp_path, monkeypatch, sleep):
    """A row's srun step runs on CPUs Slurm picks within the block, not on the row's slot, and outside its memory
    watch: the result says the row ran one (seen by the watcher while it ran), so status can say so. Whatever `sleep`
    the host has: the fake step's client was `sleep` under the name srun, and on Ubuntu 26.04 it died at once, unseen
    (CI, 2026-11)."""
    if sleep == "multicall":
        (tmp_path / "multicall").mkdir()
        _fake_tool(tmp_path / "multicall", "sleep", _MULTICALL_SLEEP.format(real=shlex.quote(shutil.which("sleep"))))
        monkeypatch.setenv("PATH", f"{tmp_path / 'multicall' / 'bin'}{os.pathsep}{os.environ['PATH']}")
    _fake_srun(tmp_path)
    ran = _row(tmp_path, "srun -n1 true", _slot(cores=1), **_ANVIL_STEP_ENV)
    plain = _row(tmp_path, "sleep 2", _slot(cores=1), index=1, **_ANVIL_STEP_ENV)
    assert ran.rc == 0, (ran.rc, ran.stderr_tail)
    assert getattr(ran, "srun", None) is True and getattr(plain, "srun", None) is False, (
        getattr(ran, "srun", "no field"), getattr(plain, "srun", "no field"))


# =====================================================================================================================
# F1 — a pinned row is over its cores only on evidence: runnable threads above its cores, sustained
# =====================================================================================================================

def _judged(rep, cores: int) -> list[str]:
    """What status would flag for this attempt, judged as the server judges a settled row."""
    from hpc_bridge import batch

    t = {"state": "done", "req_cores": cores, "pinned": None if rep.pinned is None else int(rep.pinned),
         "cpu_s": rep.cpu_s, "wall_s": rep.wall_s, "threads": rep.threads, "peak_rss_kb": rep.peak_rss_kb,
         "why": rep.why, "r_over": getattr(rep, "r_over", None), "r_samples": getattr(rep, "r_samples", None)}
    return batch._over(t, {"cores": cores, "pin": 1})


_BUSY = 'timeout {s} sh -c "while :; do :; done"'


@pytest.mark.skipif(_PY3 is None or shutil.which("timeout") is None, reason="needs python3 and timeout")
def test_f1_the_pilots_own_shape_is_not_over_its_cores(tmp_path):
    """Anvil: the pilot measured 2 cores, the user accepted, and the rows at 2 cores — 2 busy loops, a sleeping
    python, `timeout` and the shell — were all flagged "ran over their size": every thread of the tree was counted."""
    work = (f"timeout 30 bash -c '{_BUSY.format(s=4)} & {_BUSY.format(s=4)} & "
            "python3 -c \"import time; time.sleep(4)\"; wait'")
    rep = _row(tmp_path, work, _slot(cores=2))
    assert rep.rc == 0, (rep.rc, rep.stderr_tail)
    if _LINUX:   # judged on its looks, never left unjudged (review round 1: with no looks it passed vacuously)
        assert (getattr(rep, "r_samples", None) or 0) >= 3, getattr(rep, "r_samples", None)
    assert _judged(rep, 2) == [], (rep.threads, getattr(rep, "r_over", None), getattr(rep, "r_samples", None))


@pytest.mark.skipif(not _LINUX or shutil.which("timeout") is None, reason="Linux: thread states from /proc")
def test_f1_four_busy_loops_on_two_cores_are_over_its_cores(tmp_path):
    work = " & ".join([_BUSY.format(s=5)] * 4) + " & wait"
    rep = _row(tmp_path, work, _slot(cores=2))
    assert rep.rc == 0, (rep.rc, rep.stderr_tail)
    assert getattr(rep, "r_samples", 0) >= 3, rep
    assert _judged(rep, 2) == ["cores"], (rep.threads, rep.r_over, rep.r_samples)


# =====================================================================================================================
# F2 — the memory watch samples a young or growing row faster
# =====================================================================================================================

@pytest.mark.skipif(_PY3 is None or not _CHILDREN, reason="needs python3, and Linux /proc children (the fast looks)")
def test_f2_a_row_growing_fast_is_killed_near_its_limit_not_a_second_later(tmp_path):
    """Anvil: a row allocating 2 GB at once on a 1 GB slot peaked at 2052 MB before the 1 s sample saw it. Here a row
    grows ~250 MB/s from its start, on a 100 MB slot: sampled once a second it is first seen at ~250 MB. (Where /proc
    lists no task's children — macOS — the looks stay a second apart: no node runs there.)"""
    from hpc_bridge import batch

    grow = ("python3 -c \"import time; b = []; [(b.append(b'x' * (25 << 20)), time.sleep(0.1)) for _ in range(40)]; "
            "time.sleep(5)\"")
    rep = _row(tmp_path, grow, _slot(cores=1, memory_mb=100, pin=False))
    assert rep.rc == batch.MEMORY_RC and rep.why == "memory", (rep.rc, rep.why, rep.stderr_tail)
    assert rep.peak_rss_kb < 200 * 1024, f"killed at {rep.peak_rss_kb // 1024} MB on a 100 MB slot"


def test_f2_the_watchers_cost_stays_small(tmp_path):
    """The faster sampling of a young row costs a few samples more, once: over a 5 s row with a memory limit the whole
    wrapper — its watcher, the stream readers, the claim — stays a small share of one CPU (measured: the row itself
    is `sleep`, which costs nothing)."""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")

    def cost(index: int, memory_mb: int | None) -> tuple[float, float]:
        script = batch.task_script("sleep 5", results_dir=str(tmp_path / "rd"), index=index, attempt=1,
                                   task_walltime_s=60, slot=_slot(cores=1, memory_mb=memory_mb, pin=False))
        r0 = resource.getrusage(resource.RUSAGE_CHILDREN)
        t0 = time.monotonic()
        p = subprocess.run([_BASH, "-c", script], capture_output=True, text=True, env=_wrapper_env(tmp_path),
                           timeout=60)
        wall = time.monotonic() - t0
        r1 = resource.getrusage(resource.RUSAGE_CHILDREN)
        assert batch.parse_result(p.stdout) is not None, p.stdout[-300:]
        return (r1.ru_utime - r0.ru_utime) + (r1.ru_stime - r0.ru_stime), wall

    plain, _w = cost(0, None)            # no memory limit: a look a second, as before
    watched, wall = cost(1, 512)         # a memory limit: fast quick looks while young
    print(f"\nwatcher cost: {watched:.3f} CPU-s over {wall:.2f} s ({100 * watched / wall:.1f}% of a CPU); the same "
          f"row with no memory limit {plain:.3f} CPU-s")
    if _LINUX:   # a node's cost (macOS's `ps -A` alone grows with every process of the desktop: no node runs there)
        assert watched / wall < 0.15, f"the wrapper of a sleeping row used {watched:.2f} CPU-s over {wall:.2f} s"
    assert watched - plain < 0.4, f"the young row's fast looks cost {watched - plain:.2f} CPU-s more"


# =====================================================================================================================
# F5 — `module` in a facility worker's non-login shell
# =====================================================================================================================

_MODULE_INIT = r"""echo "module init sourced (a site banner)"
module() {
  case "$1" in load) shift; export LOADED_MODULES="$*"; echo "Loading $*" >&2;; *) return 0;; esac
}
ml() { module load "$@"; }
"""


def _session(tmp: Path, command: str, act, **env: str):
    """One run_shell command as the session wrapper runs it, with the activation inside and this environment."""
    from hpc_bridge import envs, session_shell

    s = session_shell.Session("default", str(tmp / "scratch"))
    script = session_shell.wrap(command, s, env_sh=envs.activation_sh(act, NONCE) if act is not None else None)
    (tmp / "home").mkdir(exist_ok=True)
    base = {"PATH": os.environ["PATH"], "HOME": str(tmp / "home"), "LANG": "C", "HPCB_SYSROOT": str(tmp / "sys")}
    (tmp / "sys").mkdir(exist_ok=True)
    return subprocess.run([_BASH, "-c", script], capture_output=True, text=True, env={**base, **env}, cwd=tmp,
                          timeout=60)


def _act(line: str):
    from hpc_bridge import envs

    return envs.Activation(how="activate", key="d" * 16, line=line)


def _init_at(path: Path, text: str = _MODULE_INIT) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


@pytest.mark.parametrize("where", ["lmod_pkg", "profile_modules", "profile_lmod", "share_modules", "share_lmod"])
def test_f5_a_module_line_where_module_is_not_defined_sources_the_sites_init(tmp_path, where):
    """Anvil's multi-user endpoint: `module load anaconda/2025.12-py313` was refused (`module: command not found`,
    rc 127); `. /etc/profile.d/modules.sh; module load …` worked. Each candidate init, the first that exists."""
    sysroot = tmp_path / "sys"
    env: dict[str, str] = {}
    if where == "lmod_pkg":
        _init_at(tmp_path / "lmod" / "init" / "bash")
        env["LMOD_PKG"] = str(tmp_path / "lmod")
    else:
        rel = {"profile_modules": "etc/profile.d/modules.sh", "profile_lmod": "etc/profile.d/lmod.sh",
               "share_modules": "usr/share/Modules/init/bash", "share_lmod": "usr/share/lmod/lmod/init/bash"}[where]
        _init_at(sysroot / rel)
    p = _session(tmp_path, 'echo "MODS=$LOADED_MODULES"', _act("module load anaconda/2025.12-py313"), **env)
    assert p.returncode == 0 and "MODS=anaconda/2025.12-py313" in p.stdout, (p.returncode, p.stdout, p.stderr[-400:])
    assert "module init sourced" not in p.stdout and "module init sourced" in p.stderr   # its output: stderr


def test_f5_ml_is_a_module_line_too(tmp_path):
    _init_at(tmp_path / "sys" / "etc" / "profile.d" / "modules.sh")
    p = _session(tmp_path, 'echo "MODS=$LOADED_MODULES"', _act("ml anaconda/2025.12-py313"))
    assert p.returncode == 0 and "MODS=anaconda/2025.12-py313" in p.stdout, (p.returncode, p.stderr[-400:])


def test_f5_a_shell_that_has_module_already_sources_nothing(tmp_path):
    """Where `module` is a command already (the SSH endpoint's worker, a login shell), no init is sourced."""
    _init_at(tmp_path / "sys" / "etc" / "profile.d" / "modules.sh",
             'echo SOURCED > "$HOME/sourced"\nmodule() { :; }\n')
    _fake_tool(tmp_path, "module", 'echo "module $*" >&2')
    p = _session(tmp_path, "echo RAN", _act("module load gcc"), PATH=f"{tmp_path / 'bin'}:{os.environ['PATH']}")
    assert p.returncode == 0 and "RAN" in p.stdout, (p.returncode, p.stderr[-400:])
    assert not (tmp_path / "home" / "sourced").exists()


def test_f5_a_line_without_module_sources_nothing(tmp_path):
    _init_at(tmp_path / "sys" / "etc" / "profile.d" / "modules.sh", 'echo SOURCED > "$HOME/sourced"\n')
    p = _session(tmp_path, "echo RAN", _act("export MODULE_LOAD_ALIAS=module; true"))
    assert p.returncode == 0 and "RAN" in p.stdout and not (tmp_path / "home" / "sourced").exists(), p.stderr


def test_f5_with_no_module_system_anywhere_the_refusal_says_so_and_names_the_init_line(tmp_path):
    from hpc_bridge import envs

    act = _act("module load anaconda/2025.12-py313")
    p = _session(tmp_path, "echo RAN", act)
    assert "RAN" not in p.stdout and p.returncode == envs.ENV_RC, (p.returncode, p.stderr[-400:])
    ref = envs.refusal(p.returncode, p.stderr, NONCE)
    assert ref is not None and ref.why == "activate", p.stderr[-400:]
    _phase, notice = envs.refused(_NoApp(), ref, act, p.stderr)
    assert "no module system" in notice and "/etc/profile.d/modules.sh" in notice, notice


def test_f5_in_a_batch_row(tmp_path):
    from hpc_bridge import envs

    _init_at(tmp_path / "sys" / "etc" / "profile.d" / "modules.sh")
    rep = _row(tmp_path, 'echo "MODS=$LOADED_MODULES"', _slot(cores=1),
               env_sh=envs.activation_sh(_act("module load anaconda/2025.12-py313"), NONCE),
               HPCB_SYSROOT=str(tmp_path / "sys"))
    assert rep.rc == 0 and "MODS=anaconda/2025.12-py313" in rep.tail, (rep.rc, rep.stderr_tail)


class _NoApp:
    """envs.refused reads the session's env and what this server knows of envs: none here."""

    def __init__(self) -> None:
        self.env_spec = None
        self.env_known: dict = {}


# =====================================================================================================================
# F4 — VIRTUAL_ENV named hpc-bridge's own endpoint venv in user commands
# =====================================================================================================================

# A stub uv that honours VIRTUAL_ENV as uv does (`uv pip install` targets it; with none and no .venv it refuses), and a
# stub pip in the endpoint venv's bin that installs into that venv.
_UV_STUB = r"""
case "$1 $2" in
  "pip install")
    shift 2
    if [ -z "${VIRTUAL_ENV:-}" ]; then
      echo "error: No virtual environment found; run \`uv venv\` to create an environment, or pass \`--system\`" >&2
      exit 2
    fi
    for p in "$@"; do mkdir -p "$VIRTUAL_ENV/lib/site-packages/$p"; done
    echo "Installed $* into $VIRTUAL_ENV";;
  *) echo "uv stub: $*";;
esac"""


def _endpoint_venv(tmp: Path) -> Path:
    """hpc-bridge's own keyed endpoint venv as `endpoint_venv.ensure` leaves it (its arch marker), with a python and a
    pip of its own in its bin."""
    v = tmp / "home" / "hpc-bridge" / "gce-venv-endpoint-4.18.0-py3.13"
    (v / "bin").mkdir(parents=True, exist_ok=True)
    (v / ".hpcb-arch").write_text("x86_64\n")
    for name, body in (("python", 'echo "ENDPOINT VENV PYTHON"'),
                       ("pip", 'mkdir -p "$(dirname "$0")/../lib/site-packages/$2"; echo pip-installed')):
        p = v / "bin" / name
        p.write_text("#!/bin/sh\n" + body + "\n")
        p.chmod(0o755)
    _fake_tool(tmp, "uv", _UV_STUB)
    return v


def _worker_env(tmp: Path, venv: Path) -> dict[str, str]:
    """The worker's environment as the endpoint's env_setup (and worker_init) leave it: the venv activated."""
    return {"VIRTUAL_ENV": str(venv), "PATH": f"{venv / 'bin'}:{tmp / 'bin'}:{os.environ['PATH']}"}


_INSTALL = ('uv pip install six; pip install requests >/dev/null 2>&1; echo "VE=[${VIRTUAL_ENV-}]"; '
            'command -v python || echo "PY=none"')


def _installed_into(venv: Path) -> list[str]:
    sp = venv / "lib" / "site-packages"
    return sorted(x.name for x in sp.iterdir()) if sp.is_dir() else []


def test_f4_a_session_command_never_installs_into_the_endpoint_venv(tmp_path):
    venv = _endpoint_venv(tmp_path)
    p = _session(tmp_path, _INSTALL, None, **_worker_env(tmp_path, venv))
    assert _installed_into(venv) == [], (_installed_into(venv), p.stdout)
    assert "VE=[]" in p.stdout and str(venv / "bin") not in p.stdout, p.stdout


def test_f4_a_batch_row_never_installs_into_the_endpoint_venv(tmp_path):
    venv = _endpoint_venv(tmp_path)
    rep = _row(tmp_path, _INSTALL, _slot(cores=1), **_worker_env(tmp_path, venv))
    assert _installed_into(venv) == [] and "VE=[]" in rep.tail and str(venv / "bin") not in rep.tail, rep.tail


def test_f4_an_unsized_row_too(tmp_path):
    venv = _endpoint_venv(tmp_path)
    rep = _row(tmp_path, _INSTALL, None, **_worker_env(tmp_path, venv))
    assert _installed_into(venv) == [] and "VE=[]" in rep.tail, rep.tail


def test_f4_a_job_script_never_installs_into_the_endpoint_venv(tmp_path):
    from hpc_bridge import jobs
    from hpc_bridge.context import AppCtx
    from hpc_bridge.profile import Profile
    from tests.fakes import FakeFacility

    venv = _endpoint_venv(tmp_path)
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    app.scratch_root = str(tmp_path / "scratch")
    spec = jobs.Spec(command=_INSTALL, session_id="default", account=None, partition=None, nodes=1, whole_node=False,
                     cores=1, memory_gb=None, gpus=None, walltime_s=60, tasks=1, throttle=None, array=None,
                     depends=[], rate=1.0, per_task=1.0, worst_case=1.0, basis="", min_charge_h=None, shape=None)
    jid = "job-0123456789ab"
    Path(jobs.job_dir(app, jid)).mkdir(parents=True)
    script = jobs.job_script(app, job_id=jid, envelope_id="env-0123456789ab", spec=spec, deadline=time.time() + 600)
    p = subprocess.run([_BASH, "-c", script], capture_output=True, text=True, timeout=60,
                       env={**_worker_env(tmp_path, venv), "HOME": str(tmp_path / "home")})
    assert _installed_into(venv) == [] and "VE=[]" in p.stdout, (p.stdout, p.stderr[-300:])


def test_f4_a_session_env_persisted_with_the_venvs_bin_heals(tmp_path):
    """An earlier command's PATH, persisted while the endpoint venv's bin was in it, never brings it back."""
    venv = _endpoint_venv(tmp_path)
    sd = tmp_path / "scratch" / "sessions" / "default"
    sd.mkdir(parents=True)
    (sd / ".env").write_text(f"export PATH={tmp_path / 'home' / 'bin'}:{venv / 'bin'}:{os.environ['PATH']}\n")
    p = _session(tmp_path, 'echo "P=$PATH"', None, **_worker_env(tmp_path, venv))
    assert p.returncode == 0 and str(venv / "bin") not in p.stdout and str(tmp_path / "home" / "bin") in p.stdout, (
        p.stdout)


def test_f4_a_users_own_venv_is_kept_across_commands(tmp_path):
    """The user's own venv — activated by a command, or the worker's in a BYO endpoint the user set up — is theirs."""
    venv = _endpoint_venv(tmp_path)
    mine = tmp_path / "home" / "myenv"
    (mine / "bin").mkdir(parents=True)
    first = _session(tmp_path, f'export VIRTUAL_ENV={mine} PATH="{mine}/bin:$PATH"', None,
                     **_worker_env(tmp_path, venv))
    assert first.returncode == 0, first.stderr
    p = _session(tmp_path, 'uv pip install six; echo "VE=[$VIRTUAL_ENV]"', None, **_worker_env(tmp_path, venv))
    assert f"VE=[{mine}]" in p.stdout and (mine / "lib" / "site-packages" / "six").is_dir(), p.stdout
    assert _installed_into(venv) == []
    byo = _session(tmp_path, 'echo "VE=[$VIRTUAL_ENV]"', None, VIRTUAL_ENV=str(mine),
                   PATH=f"{mine / 'bin'}:{os.environ['PATH']}")
    assert f"VE=[{mine}]" in byo.stdout, byo.stdout


def test_f4_a_built_env_is_the_commands_venv(tmp_path):
    """The session's built env sets its own VIRTUAL_ENV and bin over the scrubbed ones."""
    from hpc_bridge import envs
    from tests.test_block_envs import _built

    _spec, key, root, b, p0 = _built(tmp_path)
    assert b is not None and b.ok, p0.stderr[-300:]
    venv = _endpoint_venv(tmp_path)
    act = envs.Activation(how="built", key=key, root=str(root), arch=b.arch)
    p = _session(tmp_path, 'echo "VE=[$VIRTUAL_ENV]"; echo "P=$PATH"', act, **_worker_env(tmp_path, venv))
    assert f"VE=[{b.path}]" in p.stdout and str(venv / "bin") not in p.stdout, (p.stdout, p.stderr[-300:])


def test_f4_the_wrappers_own_tools_never_came_from_the_venv(tmp_path):
    """Nothing of hpc-bridge's own in a command relies on the endpoint venv's bin: with it gone from PATH the session
    wrapper persists cwd and env, and a batch row reports its use, exactly as before."""
    venv = _endpoint_venv(tmp_path)
    for tool in ("base64", "awk", "ps", "tail", "head", "tee", "mkfifo"):   # a venv shadowing them would be seen
        t = venv / "bin" / tool
        t.write_text("#!/bin/sh\necho SHADOWED >&2; exit 97\n")
        t.chmod(0o755)
    env = _worker_env(tmp_path, venv)
    first = _session(tmp_path, "cd /; export KEEP=1", None, **env)
    second = _session(tmp_path, 'echo "$PWD $KEEP"', None, **env)
    assert first.returncode == 0 and second.stdout.strip() == "/ 1", (first.stderr[-300:], second.stdout)
    rep = _row(tmp_path, "echo ok", _slot(cores=1), **env)
    assert rep.rc == 0 and rep.tail.strip() == "ok" and rep.wall_s is not None, (rep.rc, rep.stderr_tail)


@pytest.mark.parametrize(("name", "marker", "scrubbed"), [
    ("gce-venv-endpoint-4.18.0-py3.13", True, True),     # hpc-bridge's keyed endpoint venv
    ("gce-venv", False, True),                           # the unkeyed one older plugins built
    ("gce-venv-4.18.0-py3.13", False, True),             # a registry entry's worker venv
    ("custom-endpoint-venv", True, True),                # HPC_BRIDGE_REMOTE_VENV elsewhere: its marker names it
    ("analysis", False, False),                          # the user's own
    ("my-gce-venv-copy", False, False),                  # a name that only contains hpc-bridge's
])
def test_f4_the_venv_is_named_only_by_its_marker_or_hpc_bridges_name(tmp_path, name, marker, scrubbed):
    """Which VIRTUAL_ENV is hpc-bridge's own: its endpoint venv's arch marker, or the gce-venv names hpc-bridge and its
    registry entries give the worker venvs — never a venv of the user's (review round 1: tested by what it does)."""
    v = tmp_path / "home" / "venvs" / name
    (v / "bin").mkdir(parents=True)
    if marker:
        (v / ".hpcb-arch").write_text("x86_64\n")
    p = _session(tmp_path, 'echo "VE=[${VIRTUAL_ENV-}]"; echo "P=$PATH"', None, VIRTUAL_ENV=str(v),
                 PATH=f"{v / 'bin'}:{os.environ['PATH']}")
    gone = "VE=[]" in p.stdout and f"{v / 'bin'}:" not in p.stdout
    assert gone is scrubbed, p.stdout

"""Batch row sizing (PR 2; vault `Planned/Batch and swarm execution.md`, "Batch row sizing"): run_batch rows sized the
sbatch way — per-row `cores` / `memory_gb` / `gpus` and a `concurrency` (the `--array=…%N` analogue) — the block
derived from them before the spend gate, measured from inside once it runs, one slot pool per block shared by every
batch, each row pinned to its own CPUs (and GPUs) and killed at its memory. Written failing first (a strict xfail
module on 906e303, red there); the mark came off with the build. The checkpoint's rulings (2026-11-04) added the
default-memory pause, the busy-slot backoff, the width cap and the worker ceiling, and moved the wrapper's overhead
budget to the follow-up (the per-node watcher): its measurement is in the vault.

`cores` are scheduler CPUs, as `-c` counts them — one hardware thread where SMT is on, never rounded up to whole cores;
a slot takes the block's CPUs in the measured order (SMT siblings adjacent), so a k that is a multiple of the threads per
core keeps whole cores together, and a 1-CPU row on an SMT node is pinned to one hardware thread.

The surface these tests are written against (the stage-A checkpoint fixes it):

- `run_batch(…, cores=None, memory_gb=None, gpus=None, concurrency=None, pin=True, pilot=0)` (server `_run_batch` takes
  the same keywords). `cores`: CPUs per row as the scheduler counts them (`-c`; one hardware thread where SMT is on).
  `memory_gb`: per row, a float; the row is killed at it. `gpus`: per row. `concurrency`: at most this many rows at
  once. `pin=False`: no taskset (the env stays). `pilot` 1 or 2: run that many rows alone first, then propose a size.
- `BatchStatus.phase` gains `needs_sizing`; `BatchStatus.sizing` (cores, memory_gb, gpus, concurrency, pin, slots,
  bound_by, defaulted, missing, derived), `BatchStatus.facts` (source, cpus, physical_cores, memory_gb,
  idle_memory_gb, gpus, workers, nodes, limits, candidates[{cores, memory_gb, concurrency}]), `BatchStatus.proposal`
  (cores, memory_gb, gpus, concurrency, evidence). `BatchTaskResult` gains peak_rss_mb, cpu_s, wall_s, nproc, cpus,
  gpu_ids, why, over_size. `BlockTerms.derived_from` says what the block was derived from. `EndpointStatus.measured`
  (the block's facts once its check answered).
- `hpc_bridge.sizing` (pure): `Sizing(cores, memory_mb, gpus, concurrency, pin)`; `BlockMeasure(cpus, cpu_ids,
  cpus_source, physical_cores, threads_per_core, mem_limit_mb, mem_source, idle_mb, oom_kills, gpu_ids, gpu_kind,
  workers, nodes)`; `MEASURE_SH` (prints `HPCB_M …` lines, reading files under `$HPCB_SYSROOT` when set);
  `parse_measure(text) -> BlockMeasure | None`; `fit(measure, sizing) -> Fit(slots, bound_by, memory_mb, why_not)`;
  `derive_block(sizing, shape=None) -> DerivedBlock(cores, memory_gb, gpus, workers, nodes)` or `SizingRefusal`;
  `pilot_proposal(evidence, measure) -> Proposal(cores, memory_gb, concurrency)`.
- `batch.Slot(cores, cpu_ids, gpus=0, gpu_ids=(), gpu_kind=None, memory_mb=None, pin=True, claim_wait_s=…)` passed as
  `task_script(…, slot=…)`; `batch.MEMORY_RC`, `batch.NO_SLOT_RC`; `TaskReport` gains peak_rss_kb, cpu_s, wall_s,
  nproc, cpus, gpu_ids, why, threads, slot_busy. The wrapper exports HPCB_SLOT_CPUS (the claimed CPUs).
- `runner.CanaryResult.block_measure`; the canary runs `MEASURE_SH`.
- `Ledger.add_batch(…, cores=, memory_mb=, gpus=, concurrency=, pin=, pilot=)`, `learn_measure(endpoint_id, uec_hash,
  block_job, measure_dict)` / `block_measure(…)`, task rows' `req_cores` / `req_memory_mb` / `req_gpus`; `reserve`
  admits against the block's pool: CPUs, memory, GPUs and workers summed over every batch's rows on the block.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from hpc_bridge.models import PartitionShape
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _shape_runtime
from tests.fakes import FakeFacility
from tests.test_batch_phase1 import _BatchRunner, _owner
from tests.test_server import _Res

_BASH = shutil.which("bash")
_PY3 = shutil.which("python3")
_LINUX_PIN = sys.platform.startswith("linux") and shutil.which("taskset") is not None and hasattr(os, "sched_getaffinity")

_END = str(int(time.time()) + 900)
_ENV = {"SLURM_JOB_ID": "101", "SLURM_JOB_END_TIME": _END}


# ---- helpers ---------------------------------------------------------------------------------------------------------

def _measure(**kw):
    from hpc_bridge import sizing

    base = dict(cpus=4, cpu_ids=(0, 1, 2, 3), cpus_source="SLURM_CPUS_ON_NODE", physical_cores=4, threads_per_core=1,
                mem_limit_mb=4000, mem_source="SLURM_MEM_PER_NODE", idle_mb=500, oom_kills=0, gpu_ids=(),
                gpu_kind=None, workers=5, nodes=1)
    base.update(kw)
    return sizing.BlockMeasure(**base)


def _canary(measure=None, *, remaining_s: float = 900.0, workers: int | None = 5):
    """The block's check, answering whenever it is asked (no answer time: the fake's one answer serves every check)."""
    return CanaryResult(ok=True, worker_host="c1", worker_python="3.12.3", worker_dill="0.4.0", worker_job="101",
                        worker_remaining_s=remaining_s, worker_count=workers, block_measure=measure)


def _app_with(measure=None, *, mep: bool = False, workers: int = 5, runner=None):
    """A confirmed compute block whose check answered from inside it with `measure` (None: not measured yet)."""
    f = FakeFacility()
    f.workers = 1
    if mep:
        f.supported_shapes = ("compute",)
    app = AppCtx(facility=f, profile=Profile())
    app.state.endpoint_id = "fake-eid"
    app.batch_autoloop = False
    runner = runner or _BatchRunner(canary=_canary(measure, workers=workers))
    app.uecs = []

    def factory(eid, user_endpoint_config=None, **_kw):
        app.uecs.append(dict(user_endpoint_config or {}))
        return runner

    app.runner_factory = factory
    rt = _shape_runtime(app, "compute")
    rt.user_endpoint_config["max_workers_per_node"] = workers
    rt.spend_confirmed = True
    return app, rt, runner


def _sized_app(canary=None):
    """hpc-bridge's own SSH template on a per-core partition (Anvil's `shared`: 128 cores, 251.4 GB), no block yet."""
    from tests.test_resource_requests import _app
    from tests.test_spend_terms import _runners

    app = _app()
    if canary is not None:
        _runners(app, canary)
    app.state.endpoint_id = "fake-eid"
    app.batch_autoloop = False
    return app


async def _run(app, n: int = 6, *, command: str = "echo {x}", walltime: float = 60.0, **kw):
    from hpc_bridge import server

    return await server._run_batch(app, command=command, rows=[{"x": i} for i in range(n)], task_walltime_s=walltime,
                                   **kw)


def _report(index: int, attempt: int = 1, *, rc: int = 0, **extra):
    rep = {"index": index, "attempt": attempt, "rc": rc, "tail_b64": "", "stderr_tail_b64": "", "job": "101",
           "host": "c1", "workers": 5, "remaining_s": 800, "started": 1000, "ended": 1002, **extra}
    return _Res(0, "HPCB_RESULT " + json.dumps(rep) + "\n", "")


def _wrapper_env(tmp: Path, **extra) -> dict[str, str]:
    (tmp / "nodetmp").mkdir(exist_ok=True)
    return {"PATH": f"{tmp / 'bin'}:{os.environ['PATH']}", "HOME": str(tmp), "TMPDIR": str(tmp / "nodetmp"), **_ENV,
            **extra}


def _fake_tool(tmp: Path, name: str, body: str) -> Path:
    (tmp / "bin").mkdir(exist_ok=True)
    p = tmp / "bin" / name
    p.write_text("#!/bin/sh\n" + body + "\n")
    p.chmod(0o755)
    return p


def _bash(script: str, env: dict[str, str], timeout: float = 60) -> subprocess.CompletedProcess:
    return subprocess.run([_BASH, "-c", script], capture_output=True, text=True, env=env, timeout=timeout)


def _spawn(script: str, env: dict[str, str]) -> subprocess.Popen:
    return subprocess.Popen([_BASH, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
                            start_new_session=True)


def _script(command: str, rd: Path, index: int, slot, *, walltime: float = 60, session=None):
    from hpc_bridge import batch

    return batch.task_script(command, results_dir=str(rd), index=index, attempt=1, task_walltime_s=walltime,
                             slot=slot, session=session)


def _result(p: subprocess.Popen, timeout: float = 60):
    from hpc_bridge import batch

    out, _err = p.communicate(timeout=timeout)
    rep = batch.parse_result(out)
    assert rep is not None, out[-500:]
    return rep


# ---- the arguments ---------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("bad", [{"cores": 0}, {"cores": True}, {"cores": 2.5}, {"memory_gb": 0}, {"memory_gb": -1},
                                 {"gpus": -1}, {"concurrency": 0}, {"pilot": 3}, {"pin": "yes"}])
async def test_sizing_arguments_are_validated_before_anything_is_recorded(bad):
    app, _rt, runner = _app_with(_measure())
    out = await _run(app, **bad)
    assert out.phase == "refused" and next(iter(bad)) in (out.reason or ""), out
    assert app.ledger is None or not app.ledger.batches()
    assert runner.commands == []


# ---- size first: the block derived from the rows ----------------------------------------------------------------------

def test_derive_block_is_n_rows_plus_one_cpu_and_one_worker():
    """The sbatch model: N rows at once × (k CPUs, m GB, g GPUs), plus ONE CPU and ONE worker for interactive
    commands (W = slots + 1), never a held-back slot; memory adds the worker pool's own; one node."""
    from hpc_bridge import sizing

    b = sizing.derive_block(sizing.Sizing(cores=2, memory_mb=1536, gpus=0, concurrency=4))
    assert (b.cores, b.workers, b.gpus, b.nodes) == (9, 5, 0, 1)
    assert isinstance(b.memory_gb, int) and 6 <= b.memory_gb <= 6 + 2       # 4 × 1.5 GB + the pool, whole GB


def test_derive_block_gives_gpus_per_row_and_none_to_the_interactive_reserve():
    from hpc_bridge import sizing

    b = sizing.derive_block(sizing.Sizing(cores=4, memory_mb=8192, gpus=1, concurrency=2))
    assert (b.cores, b.gpus, b.workers) == (9, 2, 3)


def test_derive_block_bigger_than_a_node_is_refused_with_the_numbers():
    from hpc_bridge import sizing

    shape = PartitionShape(name="shared", kind="per_core", cores=128, memory_gb=251.4)
    with pytest.raises(sizing.SizingRefusal) as e:
        sizing.derive_block(sizing.Sizing(cores=8, memory_mb=1024, concurrency=16), shape=shape)
    text = str(e.value)
    assert "129" in text and "128" in text and "15" in text            # 16 × 8 + 1; a node's cores; what fits
    assert "campaign" in text                                          # several nodes: phase 3's pools


async def test_size_first_without_a_concurrency_asks_for_it():
    app = _sized_app()
    out = await _run(app, cores=2, memory_gb=1.0)
    assert out.phase == "needs_sizing" and "concurrency" in out.sizing.missing, out
    assert app.built == []


async def test_size_first_answers_the_spend_question_with_the_derived_block():
    app = _sized_app()
    out = await _run(app, n=10, cores=2, memory_gb=1.5, concurrency=4)
    assert out.phase == "needs_confirmation", out
    b = out.block
    assert (b.cores_per_node, b.whole_node, b.nodes) == (9, False, 1)
    assert b.memory_gb is not None and b.memory_gb >= 6
    assert b.derived_from and "4" in b.derived_from and "2 CPU" in b.derived_from
    assert (out.sizing.concurrency, out.sizing.cores, out.sizing.derived) == (4, 2, True)
    assert "4 rows at once" in out.notice
    assert app.built == [] and (app.ledger is None or not app.ledger.batches())      # nothing started or recorded


async def test_size_first_yes_starts_exactly_the_derived_block_and_no_row_value_enters_the_config():
    from hpc_bridge.resources import CORES, MEM

    app = _sized_app(_canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192)))
    out = await _run(app, n=10, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    assert out.phase in ("admitting", "waiting"), out
    sent = app.built[-1]
    assert sent[CORES] == 9 and sent[MEM] >= 6 and sent.get("max_workers_per_node") == 5
    for uec in app.built:
        assert not {"cores", "memory_gb", "gpus", "concurrency", "pin", "pilot"} & set(uec), uec


async def test_size_first_on_a_whole_node_partition_states_the_whole_node():
    """A partition that hands out only whole nodes cannot be sliced to the rows: the terms say the whole node (its cost)
    and how many rows it would fit, beside the concurrency asked."""
    app = _sized_app()
    app.compute_choice["partition"] = "wholenode"
    out = await _run(app, n=1, cores=4, memory_gb=2.0, concurrency=2)
    assert out.phase == "needs_confirmation" and out.block.whole_node is True, out
    assert out.sizing.concurrency == 2
    assert "whole node" in out.notice and "31" in out.notice         # ⌊(128 − 1) ÷ 4⌋ rows would fit the node


async def test_a_second_batch_that_fits_the_derived_block_rides_it_and_one_that_cannot_is_refused():
    """On an approved block a sized batch is checked against what the block MEASURED: one whose rows fit rides it
    (no new block, no new yes); one whose row cannot fit is refused with the numbers and the block it would need."""
    app = _sized_app(_canary(_measure(cpus=9, cpu_ids=tuple(range(9)), workers=5, mem_limit_mb=8192)))
    first = await _run(app, cores=2, memory_gb=1.5, concurrency=4, confirm_spend=True)
    assert first.batch_id
    same = await _run(app, cores=1, memory_gb=1.0, concurrency=4)
    assert same.phase in ("admitting", "waiting") and same.batch_id, same
    assert app.ledger.batch(same.batch_id)["uec_hash"] == app.ledger.batch(first.batch_id)["uec_hash"]
    bigger = await _run(app, cores=16, memory_gb=4.0, concurrency=4)
    assert bigger.phase == "refused" and "16" in bigger.reason and "9" in bigger.reason, bigger
    assert "65" in bigger.notice                                      # 4 × 16 + 1: the block that batch would need


# ---- measuring the block from inside ----------------------------------------------------------------------------------

def _parse(*lines: str):
    from hpc_bridge import sizing

    m = sizing.parse_measure("\n".join(["noise line"] + [f"HPCB_M {ln}" for ln in lines]))
    assert m is not None
    return m


def test_measure_cgroup_v2_takes_the_lowest_memory_max_up_the_tree_and_the_idle_use_there():
    m = _parse("affinity 0-7", "cgroup 0::/slurm/job_5/step_0",
               "cg2 /slurm/job_5/step_0 memory.max max", "cg2 /slurm/job_5 memory.max 2147483648",
               "cg2 /slurm memory.max max", "cg2 /slurm/job_5 memory.current 536870912",
               "cg2 /slurm/job_5/step_0 memory.current 104857600", "cg2 /slurm/job_5 memory.events.oom_kill 1")
    assert m.mem_limit_mb == 2048 and "memory.max" in m.mem_source
    assert m.idle_mb == 512 and m.oom_kills == 1


def test_measure_cgroup_v1_reads_the_hierarchical_limit_and_ignores_unlimited():
    m = _parse("affinity 0-3", "cgroup 4:memory:/slurm/uid_1/job_5/step_0",
               "cg1 memory /slurm/uid_1/job_5/step_0 memory.limit_in_bytes 9223372036854771712",
               "cg1 memory /slurm/uid_1/job_5/step_0 hierarchical_memory_limit 1073741824",
               "cg1 memory /slurm/uid_1/job_5 memory.usage_in_bytes 104857600",
               "cg1 memory /slurm/uid_1/job_5 memory.oom_control.oom_kill 3")
    assert m.mem_limit_mb == 1024 and "hierarchical" in m.mem_source and m.oom_kills == 3
    unlimited = _parse("affinity 0-3", "cg1 memory /x memory.limit_in_bytes 9223372036854771712",
                       "meminfo MemTotal 16384000")
    assert unlimited.mem_limit_mb == 16000 and "MemTotal" in unlimited.mem_source


def test_measure_slurm_counts_bound_the_cpus_and_the_memory():
    """The fake cluster (task/none): every CPU of the host is in the affinity mask, Slurm allotted 4 — the block is 4
    CPUs, pinned to the first four of the mask."""
    m = _parse("affinity 0-11", "env SLURM_CPUS_ON_NODE 4", "env SLURM_MEM_PER_NODE 3000", "meminfo MemTotal 8000000")
    assert (m.cpus, m.cpu_ids, m.cpus_source) == (4, (0, 1, 2, 3), "SLURM_CPUS_ON_NODE")
    assert m.mem_limit_mb == 3000 and m.mem_source == "SLURM_MEM_PER_NODE"
    per_cpu = _parse("affinity 0-11", "env SLURM_CPUS_ON_NODE 4", "env SLURM_MEM_PER_CPU 500")
    assert per_cpu.mem_limit_mb == 2000 and per_cpu.mem_source == "SLURM_MEM_PER_CPU"
    capped = _parse("affinity 0-11", "env SLURM_MEM_PER_NODE 3000", "cg2 /slurm/job_5 memory.max 1073741824")
    assert capped.mem_limit_mb == 1024                                   # the smallest evidence wins


def test_measure_pbs_reads_ncpus_and_the_jobs_memory():
    m = _parse("affinity 0-63", "env NCPUS 8", "pbs Resource_List.mem 16gb")
    assert (m.cpus, m.cpus_source) == (8, "NCPUS") and m.mem_limit_mb == 16384
    assert _parse("affinity 0-3", "pbs Resource_List.mem 512mb").mem_limit_mb == 512


def test_measure_cgroup_cpu_quota_and_cpuset_bound_the_cpus():
    m = _parse("affinity 0-15", "cg2 /slurm/job_5 cpuset.cpus.effective 0-3,8-11", "cg2 /slurm/job_5 cpu.max max 100000")
    assert m.cpus == 8 and m.cpu_ids == (0, 1, 2, 3, 8, 9, 10, 11)
    q = _parse("affinity 0-15", "cg2 /slurm/job_5 cpuset.cpus.effective 0-3", "cg2 /slurm/job_5 cpu.max 250000 100000")
    assert q.cpus == 2 and len(q.cpu_ids) == 2 and q.cpus_source == "cpu.max"   # 2.5 CPUs of time: 2 whole slots
    v1 = _parse("affinity 0-15", "cg1 cpu /j cpu.cfs_quota_us 300000", "cg1 cpu /j cpu.cfs_period_us 100000")
    assert v1.cpus == 3


def test_measure_smt_siblings_give_physical_cores_and_a_slot_order_that_keeps_them_together():
    sib = [f"sib {c} {c % 4},{c % 4 + 4}" for c in range(8)]
    m = _parse("affinity 0-7", *sib)
    assert (m.cpus, m.physical_cores, m.threads_per_core) == (8, 4, 2)
    assert m.cpu_ids == (0, 4, 1, 5, 2, 6, 3, 7)


@pytest.mark.parametrize(("lines", "ids", "kind"), [
    (["env CUDA_VISIBLE_DEVICES 0,1"], ("0", "1"), "cuda"),
    (["env CUDA_VISIBLE_DEVICES GPU-aa,GPU-bb"], ("GPU-aa", "GPU-bb"), "cuda"),
    (["env CUDA_VISIBLE_DEVICES NoDevFiles"], (), None),
    (["env ROCR_VISIBLE_DEVICES 0,1,2,3"], ("0", "1", "2", "3"), "rocm"),
    (["env ZE_AFFINITY_MASK 0,1"], ("0", "1"), "ze"),
    (["env SLURM_JOB_GPUS 2,3", "dev nvidia 4"], ("2", "3"), "cuda"),
    (["nvsmi 4"], ("0", "1", "2", "3"), "cuda"),
    (["dev nvidia 2"], ("0", "1"), "cuda"),
    ([], (), None),
])
def test_measure_visible_gpus(lines, ids, kind):
    m = _parse("affinity 0-3", *lines)
    assert (m.gpu_ids, m.gpu_kind) == (ids, kind)


def test_measure_workers_nodes_and_a_bare_machine():
    m = _parse("affinity 0-3", "env PARSL_WORKER_COUNT 5", "env SLURM_JOB_NUM_NODES 2")
    assert (m.workers, m.nodes) == (5, 2)
    bare = _parse("nproc 6", "meminfo MemTotal 4096000")
    assert bare.cpus == 6 and bare.cpu_ids == tuple(range(6)) and bare.mem_limit_mb == 4000
    assert bare.idle_mb is None or bare.idle_mb >= 0


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_the_measure_script_runs_under_bash_and_is_read_back(tmp_path):
    """MEASURE_SH prints the raw facts (files under $HPCB_SYSROOT when set, the env) and parse_measure decides."""
    from hpc_bridge import sizing

    root = tmp_path / "root"
    (root / "proc" / "self").mkdir(parents=True)
    (root / "proc" / "self" / "cgroup").write_text("0::/slurm/job_5\n")
    (root / "proc" / "self" / "status").write_text("Name:\tbash\nCpus_allowed_list:\t0-7\n")
    cg = root / "sys" / "fs" / "cgroup" / "slurm" / "job_5"
    cg.mkdir(parents=True)
    (cg / "memory.max").write_text("1073741824\n")
    (cg / "memory.current").write_text("104857600\n")
    (cg / "memory.events").write_text("low 0\nhigh 0\nmax 4\noom 2\noom_kill 2\n")
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path), "TMPDIR": str(tmp_path), "HPCB_SYSROOT": str(root),
           "SLURM_CPUS_ON_NODE": "2", "CUDA_VISIBLE_DEVICES": "0,1", "PARSL_WORKER_COUNT": "3"}
    p = _bash(sizing.MEASURE_SH, env)
    m = sizing.parse_measure(p.stdout)
    assert m is not None, p.stdout + p.stderr
    assert (m.cpus, m.cpu_ids, m.mem_limit_mb, m.idle_mb, m.oom_kills) == (2, (0, 1), 1024, 100, 2)
    assert (m.gpu_ids, m.workers) == (("0", "1"), 3)


def test_the_canary_measures_the_block():
    from hpc_bridge import runner, sizing

    assert sizing.MEASURE_SH in runner._CANARY_CMD
    out = ("HPCB_CANARY\n3.12.3 0.4.0 c1\nHPCB_JOB 101\nHPCB_REMAINING 800\nHPCB_WORKERS 5\n"
           "HPCB_M affinity 0-11\nHPCB_M env SLURM_CPUS_ON_NODE 4\nHPCB_M env SLURM_MEM_PER_NODE 4000\n")
    c = runner._canary_result(_Res(0, out, ""), answered_at=1.0)
    assert c.block_measure is not None and (c.block_measure.cpus, c.block_measure.mem_limit_mb) == (4, 4000)


async def test_ensure_endpoint_up_reports_the_measured_block():
    from hpc_bridge import server

    app, _rt, _runner = _app_with(_measure(cpus=4, mem_limit_mb=4000, idle_mb=500, gpu_ids=("0",), gpu_kind="cuda"))
    st = await server._ensure_endpoint_up(app, "compute")
    assert st.status == "up" and st.measured is not None, st
    assert (st.measured.cpus, st.measured.gpus) == (4, 1)
    assert st.measured.memory_gb == pytest.approx(4000 / 1024, abs=0.05)


# ---- slots: what one block fits ---------------------------------------------------------------------------------------

@pytest.mark.parametrize(("measure", "sizing_kw", "slots", "bound"), [
    ({"cpus": 4}, {"cores": 1}, 4, "workers"),                                 # W 5: four slots and the reserve
    ({"cpus": 4, "workers": 9}, {"cores": 1}, 4, "cpus"),
    ({"cpus": 8, "workers": 9}, {"cores": 2}, 4, "cpus"),
    ({"cpus": 8, "workers": 9, "mem_limit_mb": 4000, "idle_mb": 500}, {"cores": 1, "memory_mb": 1200}, 2, "memory"),
    ({"cpus": 8, "workers": 9, "gpu_ids": ("0", "1"), "gpu_kind": "cuda"}, {"cores": 1, "gpus": 1}, 2, "gpus"),
    ({"cpus": 8, "workers": 9}, {"cores": 1, "concurrency": 3}, 3, "concurrency"),
    ({"cpus": 64, "workers": 4}, {"cores": 1}, 3, "workers"),                  # Delta's fixed 4: one kept back
])
def test_fit_counts_slots_by_cpus_memory_gpus_workers_and_concurrency(measure, sizing_kw, slots, bound):
    from hpc_bridge import sizing

    f = sizing.fit(_measure(**measure), sizing.Sizing(**sizing_kw))
    assert (f.slots, f.bound_by) == (slots, bound)


def test_the_interactive_reserve_is_one_worker_not_a_held_back_slot():
    """⌊(C − 1) ÷ k⌋ would waste up to half a slice: 8 CPUs at 4 per row is two rows, the interactive worker beside."""
    from hpc_bridge import sizing

    assert sizing.fit(_measure(cpus=8, workers=3), sizing.Sizing(cores=4)).slots == 2
    assert sizing.fit(_measure(cpus=8, workers=3, gpu_ids=("0", "1"), gpu_kind="cuda"),
                      sizing.Sizing(cores=1, gpus=1)).slots == 2                # the reserve never takes a GPU


def test_the_default_memory_is_the_share_less_the_pools_idle_memory():
    from hpc_bridge import sizing

    f = sizing.fit(_measure(cpus=4, workers=4, mem_limit_mb=4000, idle_mb=1000), sizing.Sizing(cores=1))
    assert f.slots == 3 and f.memory_mb == 1000


@pytest.mark.parametrize(("kw", "words"), [
    ({"cores": 8}, ("8", "4", "SLURM_CPUS_ON_NODE")),
    ({"cores": 1, "memory_mb": 8192}, ("8", "3.4", "memory")),
    ({"cores": 1, "gpus": 1}, ("GPU", "0")),
])
def test_a_row_that_cannot_fit_the_block_is_named_with_the_numbers(kw, words):
    from hpc_bridge import sizing

    f = sizing.fit(_measure(cpus=4, mem_limit_mb=4000, idle_mb=500), sizing.Sizing(**kw))
    assert f.slots == 0 and f.why_not
    for w in words:
        assert w in f.why_not, (w, f.why_not)


# ---- the pool on the ledger: one per block, shared by every batch -----------------------------------------------------

def _pool_ledger(tmp_path, measure: dict):
    from hpc_bridge.ledger import Ledger

    led = Ledger.open(tmp_path / "state")
    led.learn_measure("fake-eid", "uec-1", None, measure)
    return led


def _sized(led, bid, *, n=6, cores=1, memory_mb=500, gpus=0, concurrency=None, owner=None):
    led.add_batch(bid, command="echo {x}", rows=[{"x": i} for i in range(n)], task_walltime_s=60.0, capacity=64,
                  results_dir="/scratch/u/batches", idempotent=False, endpoint_id="fake-eid", uec_hash="uec-1",
                  session_id="default", owner=owner or _owner(), cores=cores, memory_mb=memory_mb, gpus=gpus,
                  concurrency=concurrency)
    return bid


_POOL = {"cpus": 4, "cpu_ids": [0, 1, 2, 3], "mem_limit_mb": 4000, "idle_mb": 500, "gpu_ids": [], "workers": 9}


def _settle_one(led, bid):
    t = next(r for r in led.tasks(bid) if r["state"] == "in_flight")
    assert led.settle(bid, int(t["idx"]), int(t["attempt"]), "done", rc=0)


def test_pool_reserve_sums_the_cpus_of_every_batch_on_the_block(tmp_path):
    led = _pool_ledger(tmp_path, _POOL)
    me = _owner()
    a = _sized(led, "batch-00000000000a", cores=2, owner=me)
    b = _sized(led, "batch-00000000000b", cores=1, owner=me)
    assert len(led.reserve(a, me, 8, uec_hash="uec-1", now=100.0)) == 2          # 2 rows × 2 CPUs: the block's 4
    assert led.reserve(b, me, 8, uec_hash="uec-1", now=101.0) == []
    _settle_one(led, a)
    assert len(led.reserve(b, me, 8, uec_hash="uec-1", now=102.0)) == 2          # the 2 CPUs freed: two 1-CPU rows
    assert led.reserve(a, me, 8, uec_hash="uec-1", now=103.0) == []


def test_pool_reserve_respects_memory_gpus_workers_and_concurrency(tmp_path):
    me = _owner()
    led = _pool_ledger(tmp_path / "m", {**_POOL, "cpus": 8, "cpu_ids": list(range(8))})
    assert len(led.reserve(_sized(led, "batch-00000000000a", memory_mb=1200, owner=me), me, 8, uec_hash="uec-1")) == 2
    led = _pool_ledger(tmp_path / "g", {**_POOL, "cpus": 8, "cpu_ids": list(range(8)), "gpu_ids": ["0", "1"]})
    assert len(led.reserve(_sized(led, "batch-00000000000b", gpus=1, owner=me), me, 8, uec_hash="uec-1")) == 2
    led = _pool_ledger(tmp_path / "w", {**_POOL, "workers": 3})
    assert len(led.reserve(_sized(led, "batch-00000000000c", owner=me), me, 8, uec_hash="uec-1")) == 2
    led = _pool_ledger(tmp_path / "c", _POOL)
    assert len(led.reserve(_sized(led, "batch-00000000000d", concurrency=1, owner=me), me, 8, uec_hash="uec-1")) == 1


def test_each_attempt_records_what_it_holds_and_a_resize_leaves_rows_in_flight_alone(tmp_path):
    led = _pool_ledger(tmp_path, {**_POOL, "cpus": 8, "cpu_ids": list(range(8))})
    me = _owner()
    a = _sized(led, "batch-00000000000a", cores=2, memory_mb=700, owner=me)
    led.reserve(a, me, 1, uec_hash="uec-1")
    t = led.task(a, 0)
    assert (t["req_cores"], t["req_memory_mb"], t["req_gpus"]) == (2, 700, 0)
    led.update_batch(a, cores=1, memory_mb=300)
    assert led.task(a, 0)["req_cores"] == 2
    led.reserve(a, me, 1, uec_hash="uec-1")
    assert led.task(a, 1)["req_cores"] == 1


def test_the_measure_is_kept_per_block(tmp_path):
    led = _pool_ledger(tmp_path, _POOL)
    led.learn_measure("fake-eid", "uec-1", "102", {**_POOL, "cpus": 16})
    assert led.block_measure("fake-eid", "uec-1", "102")["cpus"] == 16
    assert led.block_measure("fake-eid", "uec-1", None)["cpus"] == 4
    assert led.block_measure("fake-eid", "uec-2", None) is None


# ---- the wrapper: the slot, the pin, the env (portable: fake taskset where the OS has none) ---------------------------

def _slot(**kw):
    from hpc_bridge import batch

    base = dict(cores=1, cpu_ids=(0, 1), pin=True, claim_wait_s=20)
    base.update(kw)
    return batch.Slot(**base)


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_the_wrapper_pins_the_claimed_cpus_with_taskset(tmp_path):
    log = tmp_path / "taskset.log"
    _fake_tool(tmp_path, "taskset", f'echo "$@" >> {log}')
    rep = _result(_spawn(_script("echo $HPCB_SLOT_CPUS", tmp_path / "rd", 0, _slot(cores=2, cpu_ids=(2, 3))),
                         _wrapper_env(tmp_path)))
    assert rep.rc == 0 and rep.cpus == "2,3" and rep.tail.strip() == "2,3"
    assert "2,3" in log.read_text()


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_pin_false_runs_no_taskset_but_keeps_the_env(tmp_path):
    log = tmp_path / "taskset.log"
    _fake_tool(tmp_path, "taskset", f'echo "$@" >> {log}')
    rep = _result(_spawn(_script("echo $OMP_NUM_THREADS", tmp_path / "rd", 0, _slot(cores=2, cpu_ids=(0, 1),
                                                                                   pin=False)),
                         _wrapper_env(tmp_path)))
    assert rep.rc == 0 and rep.tail.strip() == "2" and not log.exists()


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_the_row_env_sizes_its_libraries_and_overrides_the_session(tmp_path):
    from hpc_bridge import session_shell

    _fake_tool(tmp_path, "taskset", "true")
    s = session_shell.Session("default", str(tmp_path / "scratch"))
    Path(s.state_dir).mkdir(parents=True)
    Path(s.state_dir, ".env").write_text("export OMP_NUM_THREADS=64 MKL_NUM_THREADS=64 SLURM_CPUS_PER_TASK=9\n")
    names = ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS", "PYTHON_CPU_COUNT",
             "SLURM_CPUS_PER_TASK", "SRUN_CPUS_PER_TASK", "SLURM_OVERLAP")
    cmd = "printf '%s\\n' " + " ".join(f'"${n}"' for n in names)
    rep = _result(_spawn(_script(cmd, tmp_path / "rd", 0, _slot(cores=3, cpu_ids=(0, 1, 2, 3)), session=s),
                         _wrapper_env(tmp_path, SLURM_CPUS_PER_TASK="9")))
    assert rep.tail.split() == ["3"] * 7 + ["1"], rep.tail


@pytest.mark.skipif(_BASH is None, reason="needs bash")
@pytest.mark.parametrize(("kind", "var"), [("cuda", "CUDA_VISIBLE_DEVICES"), ("rocm", "ROCR_VISIBLE_DEVICES"),
                                           ("ze", "ZE_AFFINITY_MASK")])
def test_each_gpu_row_gets_its_own_devices(tmp_path, kind, var):
    _fake_tool(tmp_path, "taskset", "true")
    slot = _slot(cores=1, cpu_ids=(0, 1), gpus=1, gpu_ids=("0", "1"), gpu_kind=kind)
    ps = [_spawn(_script(f"echo ${var}; sleep 2", tmp_path / "rd", i, slot), _wrapper_env(tmp_path)) for i in range(2)]
    got = sorted(_result(p).tail.strip() for p in ps)
    assert got == ["0", "1"]


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_a_cpu_row_on_a_gpu_block_sees_no_gpu(tmp_path):
    _fake_tool(tmp_path, "taskset", "true")
    slot = _slot(gpus=0, gpu_ids=("0", "1"), gpu_kind="cuda")
    rep = _result(_spawn(_script('echo "[${CUDA_VISIBLE_DEVICES-unset}]"', tmp_path / "rd", 0, slot),
                         _wrapper_env(tmp_path, CUDA_VISIBLE_DEVICES="0,1")))
    assert rep.tail.strip() == "[]"


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_rows_claim_different_cpus_a_full_pool_is_reported_and_a_dead_row_releases_its_slot(tmp_path):
    """The node-local claim (flock where the node has it): two rows at once take different CPUs; a third finds none
    within its wait and runs nothing (`slot_busy`, NO_SLOT_RC: the server puts it back); a row whose wrapper was killed
    frees its CPU once its command is gone."""
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    env = _wrapper_env(tmp_path)
    rd = tmp_path / "rd"
    a = _spawn(_script("sleep 3; echo $HPCB_SLOT_CPUS", rd, 0, _slot()), env)
    b = _spawn(_script("sleep 3; echo $HPCB_SLOT_CPUS", rd, 1, _slot()), env)
    time.sleep(1.0)
    c = _spawn(_script("echo ran", rd, 2, _slot(claim_wait_s=1)), env)
    rc_ = _result(c)
    assert rc_.slot_busy and rc_.rc == batch.NO_SLOT_RC and "ran" not in rc_.tail
    assert sorted([_result(a).cpus, _result(b).cpus]) == ["0", "1"]
    d = _spawn(_script("sleep 2", rd, 3, _slot(cpu_ids=(0,))), env)
    time.sleep(0.8)
    os.killpg(d.pid, signal.SIGKILL)                                      # the wrapper dies; its command ends soon
    d.wait()   # …and its parent reaps it, as the worker does (a zombie answers kill -0: the claim names the wrapper too)
    e = _spawn(_script("echo $HPCB_SLOT_CPUS", rd, 4, _slot(cpu_ids=(0,), claim_wait_s=15)), env)
    assert _result(e).cpus == "0"


@pytest.mark.skipif(_BASH is None or _PY3 is None, reason="needs bash and python3")
def test_the_memory_kill_ends_the_row_at_its_limit_and_spares_its_neighbour(tmp_path):
    from hpc_bridge import batch

    _fake_tool(tmp_path, "taskset", "true")
    env = _wrapper_env(tmp_path)
    hog = "python3 -c \"b = b'x' * (400 << 20); import time; time.sleep(30)\""
    t0 = time.monotonic()
    p0 = _spawn(_script(hog, tmp_path / "rd", 0, _slot(memory_mb=100, pin=False)), env)
    p1 = _spawn(_script("sleep 4; echo fine", tmp_path / "rd", 1, _slot(memory_mb=100, pin=False)), env)
    r0, r1 = _result(p0), _result(p1)
    assert r0.rc == batch.MEMORY_RC and r0.why == "memory" and r0.peak_rss_kb >= 100 * 1024, r0
    assert time.monotonic() - t0 < 20
    assert r1.rc == 0 and r1.tail.strip() == "fine" and r1.why in ("", None)


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_every_result_reports_peak_rss_cpu_seconds_wall_and_nproc(tmp_path):
    _fake_tool(tmp_path, "taskset", "true")
    busy = "end=$(( $(date +%s) + 2 )); n=0; while [ $(date +%s) -lt $end ]; do n=$((n + 1)); done; echo $n"
    rep = _result(_spawn(_script(busy, tmp_path / "rd", 0, _slot(pin=False)), _wrapper_env(tmp_path)))
    # 2 s of busy loop: well over the near-zero of CPU time lost on exec (W11), with room for a loaded machine
    # (0.72 s seen with every CPU contended)
    assert rep.rc == 0 and rep.cpu_s is not None and rep.cpu_s >= 0.25, rep
    assert rep.wall_s is not None and 1.0 <= rep.wall_s <= 10
    assert isinstance(rep.nproc, int) and rep.nproc >= 1
    assert rep.peak_rss_kb is not None and rep.peak_rss_kb > 0


@pytest.mark.skipif(not _LINUX_PIN or _PY3 is None, reason="Linux with taskset: the real pin")
def test_on_linux_the_row_runs_on_exactly_its_cpus(tmp_path):
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 2:
        pytest.skip("needs two CPUs")
    probe = "python3 -c 'import os; print(len(os.sched_getaffinity(0)))'; env -u OMP_NUM_THREADS nproc"
    rep = _result(_spawn(_script(probe, tmp_path / "rd", 0, _slot(cores=2, cpu_ids=tuple(cpus[:2]))),
                         {**_wrapper_env(tmp_path), "PATH": os.environ["PATH"]}))
    affinity, nproc = rep.tail.strip().splitlines()
    assert (affinity, nproc, rep.nproc, rep.cpus) == ("2", "2", 2, f"{cpus[0]},{cpus[1]}"), rep.tail


@pytest.mark.skipif(_BASH is None, reason="needs bash")
def test_a_slot_keeps_smt_siblings_together_and_a_one_cpu_row_takes_one_thread(tmp_path):
    """The block's CPUs in the measured order, siblings adjacent (0,4 | 1,5 on a 2-way SMT node): a 2-CPU row takes a
    whole core's two threads, the next row the next core; a 1-CPU row takes one hardware thread (never rounded up)."""
    _fake_tool(tmp_path, "taskset", "true")
    env = _wrapper_env(tmp_path)
    order = (0, 4, 1, 5)
    two = [_spawn(_script("sleep 2", tmp_path / "rd", i, _slot(cores=2, cpu_ids=order)), env) for i in range(2)]
    assert sorted(_result(p).cpus for p in two) == ["0,4", "1,5"]
    one = _result(_spawn(_script("true", tmp_path / "rd", 2, _slot(cores=1, cpu_ids=order)), env))
    assert one.cpus == "0"


# ---- run_batch: default, ask, validate, pilot ------------------------------------------------------------------------

async def test_no_sizing_and_no_block_answers_needs_sizing_with_the_facts():
    app = _sized_app()
    out = await _run(app, n=40)
    assert out.phase == "needs_sizing", out
    f = out.facts
    assert (f.source, f.cpus, f.memory_gb) == ("partition", 128, 251.4)
    one = next(c for c in f.candidates if c.cores == 1)
    eight = next(c for c in f.candidates if c.cores == 8)
    # a node's cores less the interactive CPU, at most the rows a node runs at once for now (the width cap: 32)
    assert (one.concurrency, eight.concurrency) == (32, 15)
    assert {"cores", "concurrency"} <= set(out.sizing.missing)
    assert "pilot" in out.notice
    assert app.built == [] and (app.ledger is None or not app.ledger.batches())


async def test_no_sizing_on_an_approved_cpu_block_defaults_to_one_cpu_per_row_and_says_so():
    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=8)
    assert out.phase == "admitting", out
    assert (out.sizing.cores, out.sizing.slots, out.capacity, out.admitted) == (1, 4, 4, 4)
    assert {"cores", "memory_gb"} <= set(out.sizing.defaulted)
    assert out.sizing.memory_gb == pytest.approx(3500 / 4 / 1024, abs=0.01)
    assert "1 CPU" in out.notice and "cores=" in out.notice
    assert len(runner.commands) == 4


async def test_no_sizing_on_a_gpu_block_asks():
    app, _rt, runner = _app_with(_measure(cpus=8, workers=9, gpu_ids=("0", "1"), gpu_kind="cuda"))
    out = await _run(app)
    assert out.phase == "needs_sizing" and "gpus" in out.sizing.missing and out.facts.gpus == 2, out
    assert runner.commands == []


async def test_on_an_approved_block_a_row_that_cannot_fit_is_refused_with_the_numbers():
    app, _rt, runner = _app_with(_measure(cpus=4))
    out = await _run(app, cores=8)
    assert out.phase == "refused" and "8" in out.reason and "4" in out.reason, out
    assert runner.commands == []


async def test_a_concurrency_beyond_the_block_is_capped_and_named():
    app, _rt, _runner = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9))
    out = await _run(app, n=20, cores=2, concurrency=10)
    assert out.phase == "admitting" and (out.sizing.slots, out.sizing.bound_by) == (4, "cpus"), out
    assert "10" in out.notice and "21" in out.notice              # the block 10 rows of 2 CPUs would need: 2 × 10 + 1


async def test_a_block_measured_late_that_cannot_fit_pauses_at_needs_sizing():
    from hpc_bridge import batch

    app, rt, runner = _app_with(None)                                  # its check has not measured it yet
    out = await _run(app, cores=4)
    assert out.batch_id and out.phase in ("waiting", "admitting") and runner.commands == []
    rt.last_canary = _canary(_measure(cpus=2, cpu_ids=(0, 1)))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id)
    assert st.phase == "needs_sizing" and "4" in st.reason and "2" in st.reason, st
    assert runner.commands == []


async def test_a_resize_at_needs_sizing_applies_to_the_backlog():
    from hpc_bridge import batch, server

    app, rt, runner = _app_with(None)
    out = await _run(app, n=4, cores=4)
    rt.last_canary = _canary(_measure(cpus=2, cpu_ids=(0, 1), workers=3))
    await batch.tick(app, out.batch_id)
    again = await server._run_batch(app, batch_id=out.batch_id, cores=1)
    assert again.phase == "admitting" and again.sizing.cores == 1 and len(runner.commands) == 2, again


async def test_a_pilot_runs_rows_alone_with_the_whole_block_then_proposes_a_size():
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=8, pilot=1)
    assert out.admitted == 1 and len(runner.commands) == 1, out
    assert app.ledger.task(out.batch_id, 0)["req_cores"] == 4                    # the whole block: the row shows its use
    runner.futures[0].finish(_report(0, cpu_s=3.9, wall_s=2.0, peak_rss_kb=400 * 1024, nproc=4, cpus="0,1,2,3"))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id)
    assert st.phase == "needs_sizing" and st.proposal is not None, st
    assert (st.proposal.cores, st.proposal.memory_gb, st.proposal.concurrency) == (2, 0.5, 2)
    assert len(runner.commands) == 1                                            # nothing else ran meanwhile


def test_the_pilot_proposal():
    """seff's pattern: cores = ⌈CPU-s ÷ wall⌉ (at least 1, at most the block's), memory = 1.25 × peak RSS rounded up to
    a quarter GB (at least a quarter), the largest over the pilots; the concurrency the block then fits."""
    from hpc_bridge import sizing

    m = _measure(cpus=8, cpu_ids=tuple(range(8)), workers=9, mem_limit_mb=8192, idle_mb=1024)
    p = sizing.pilot_proposal([{"cpu_s": 3.9, "wall_s": 2.0, "peak_rss_kb": 400 * 1024},
                               {"cpu_s": 1.0, "wall_s": 4.0, "peak_rss_kb": 900 * 1024}], m)
    assert (p.cores, p.memory_gb, p.concurrency) == (2, 1.25, 4)
    tiny = sizing.pilot_proposal([{"cpu_s": 0.0, "wall_s": 0.0, "peak_rss_kb": 0}], m)
    assert (tiny.cores, tiny.memory_gb) == (1, 0.25)
    assert sizing.pilot_proposal([{"cpu_s": 99.0, "wall_s": 1.0, "peak_rss_kb": 1}], m).cores == 8


async def test_a_pilot_with_no_block_asks_for_the_pilots_block_first():
    app = _sized_app()
    out = await _run(app, pilot=1)
    assert out.phase == "needs_confirmation" and "pilot" in out.notice and out.block is not None, out
    assert app.built == []


async def test_a_multi_node_block_is_refused_pointing_to_campaign_pools():
    app, rt, runner = _app_with(_measure())
    rt.user_endpoint_config["nodes_per_block"] = 2
    out = await _run(app, cores=1)
    assert out.phase == "refused" and "first node" in out.reason and "campaign" in (out.notice or ""), out
    app2, _rt2, runner2 = _app_with(_measure(nodes=2))
    out2 = await _run(app2, cores=1)
    assert out2.phase == "refused" and runner.commands == [] and runner2.commands == []


async def test_results_carry_their_use_and_status_flags_rows_over_their_size():
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5))
    out = await _run(app, n=3, cores=1, memory_gb=0.5, pin=False)
    runner.futures[0].finish(_report(0, cpu_s=7.6, wall_s=2.0, peak_rss_kb=100 * 1024, nproc=4, cpus="0"))
    runner.futures[1].finish(_report(1, rc=batch.MEMORY_RC, why="memory", cpu_s=0.5, wall_s=1.0,
                                     peak_rss_kb=520 * 1024, nproc=4, cpus="1"))
    runner.futures[2].finish(_report(2, cpu_s=0.9, wall_s=1.0, peak_rss_kb=50 * 1024, nproc=4, cpus="2"))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id, detail="page")
    rows = {r.index: r for r in st.results}
    assert rows[0].over_size == ["cores"] and rows[0].cpu_s == 7.6 and rows[0].peak_rss_mb == 100
    assert rows[1].state == "failed" and rows[1].why == "memory" and rows[1].over_size == ["memory"]
    assert rows[2].over_size == [] and rows[2].nproc == 4 and rows[2].cpus == "2"
    assert "over their size" in st.notice and "cores=" in st.notice


async def test_a_pinned_row_with_more_runnable_threads_than_cores_is_flagged():
    """(The Anvil live test, F1, changed this test's rule: a pinned row is over its cores on runnable threads above its
    cores in most of the watcher's looks — 16 threads alone, idle ones included, are no evidence.)"""
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5))
    out = await _run(app, n=1, cores=2)
    runner.futures[0].finish(_report(0, cpu_s=3.9, wall_s=2.0, peak_rss_kb=1024, nproc=2, cpus="0,1", threads=16,
                                     r_samples=10, r_over=9))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id, detail="page")
    assert st.results[0].over_size == ["cores"]


async def test_a_kill_at_a_default_memory_pauses_the_batch_with_a_size_from_that_row():
    """Ruling 3 (the user's): under a DEFAULTED size the first memory kill stops admission at needs_sizing with a
    proposal measured from that row — never row after row killed on a default the user did not choose."""
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=8)                                      # no size: 1 CPU, the block's share of memory
    assert out.sizing.defaulted and len(runner.commands) == 4
    runner.futures[0].finish(_report(0, rc=batch.MEMORY_RC, why="memory", cpu_s=1.0, wall_s=1.0,
                                     peak_rss_kb=900 * 1024, nproc=1, cpus="0"))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id)
    assert st.phase == "needs_sizing" and st.proposal is not None, st
    # 1.25 × 900 MB, up to a quarter GB; a killed row's CPU time is no evidence (review R2-G): the cores are asked for
    assert (st.proposal.cores, st.proposal.memory_gb) == (None, 1.25)
    assert "default memory" in st.reason and "cores=" in st.reason
    assert app.ledger.task(out.batch_id, 0)["state"] == "failed"          # not idempotent: not run again by itself
    for i in (1, 2, 3):                                                   # the rows out run to their end meanwhile
        runner.futures[i].finish(_report(i, cpu_s=0.5, wall_s=1.0, peak_rss_kb=100 * 1024, nproc=1, cpus=str(i)))
    await batch.tick(app, out.batch_id)
    assert len(runner.commands) == 4                                      # nothing more admitted
    resumed = await batch.run_batch(app, ensure_up=_ensure(app), batch_id=out.batch_id, cores=1, memory_gb=1.25)
    assert resumed.phase == "admitting" and resumed.sizing.memory_gb == 1.25 and len(runner.commands) > 4, resumed


async def test_an_explicit_memory_keeps_the_plain_kill_and_the_batch_runs_on():
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=4, workers=5, mem_limit_mb=4000, idle_mb=500))
    out = await _run(app, n=8, cores=1, memory_gb=0.5)
    runner.futures[0].finish(_report(0, rc=batch.MEMORY_RC, why="memory", cpu_s=1.0, wall_s=1.0,
                                     peak_rss_kb=520 * 1024, nproc=1, cpus="0"))
    await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id)
    assert st.phase == "admitting" and st.counts.failed == 1 and len(runner.commands) == 5, st


def _ensure(app):
    from hpc_bridge import server

    return server._batch_ensure_up


async def test_a_busy_slot_backs_off_and_three_in_a_row_pause_the_batch_saying_why():
    """Ruling: a row that found no free slot on its node (rc 76, it ran nothing) goes back to the backlog after a
    backoff — never re-admitted at once — and the third in a row pauses the batch: something hpc-bridge does not know of
    holds the block's CPUs."""
    from hpc_bridge import batch

    app, _rt, runner = _app_with(_measure(cpus=1, cpu_ids=(0,), workers=2))
    out = await _run(app, n=4, cores=1, memory_gb=0.25)
    assert len(runner.commands) == 1
    busy = _Res(0, "HPCB_RESULT " + json.dumps({"index": 0, "attempt": 1, "rc": batch.NO_SLOT_RC, "slot_busy": True,
                                                "ran": False, "job": "101", "remaining_s": 800}) + "\n", "")
    runner.futures[0].finish(busy)
    await batch.tick(app, out.batch_id)
    t0 = app.ledger.task(out.batch_id, 0)
    assert t0["state"] == "backlog" and t0["not_before"] > time.time() + 5
    assert len(runner.commands) == 2 and "HPCB_ROW_INDEX=1 " in runner.commands[-1]    # row 1 went; row 0 waits
    for k, idx in ((1, 1), (2, 2)):
        runner.futures[k].finish(_Res(0, busy.stdout.replace('"index": 0', f'"index": {idx}'), ""))
        await batch.tick(app, out.batch_id)
    st = await batch.status(app, out.batch_id)
    assert st.phase == "needs_confirmation" and "no free slot" in st.reason and "ps -eo" in st.reason, st


def test_the_width_cap_bounds_one_node_and_the_derived_block():
    """Ruling 7 (the user's): at most 32 rows at once on one node until the per-node watcher (a follow-up)."""
    from hpc_bridge import sizing

    f = sizing.fit(_measure(cpus=128, cpu_ids=tuple(range(128)), workers=129), sizing.Sizing(cores=1))
    assert (f.slots, f.bound_by) == (32, "width")
    with pytest.raises(sizing.SizingRefusal) as e:
        sizing.derive_block(sizing.Sizing(cores=1, concurrency=40))
    assert "32" in str(e.value)


def test_a_whole_node_of_unknown_cores_runs_up_to_the_worker_ceiling():
    """Ruling 2: our template renders a high ceiling there; parsl caps the pool at the node's CPUs, and the idle workers'
    memory is measured and taken off what rows may use."""
    from hpc_bridge import sizing
    from tests.test_resource_requests import _app

    real = _app(partitions=()).facility._real
    sent = real.dispatch_uec({"compute": True, "partition": "shared", "max_workers_per_node": 2, "nodes_per_block": 1})
    assert sent["max_workers_per_node"] == sizing.WORKER_CEILING
    derived = real.dispatch_uec({"compute": True, "partition": "shared", "cores_per_node": 9, "hpcb_workers": 5})
    assert derived["max_workers_per_node"] == 5 and "hpcb_workers" not in derived


async def test_a_facility_endpoint_gets_the_derived_workers_only_where_its_template_renders_them(monkeypatch):
    """Ruling 5: derive only the keys the template renders — the workers too; where it fixes its own, they stand."""
    from tests.test_mep_server import DELTA_SCHEMA, _Templated
    from tests.test_resource_requests import _anvil_like
    from tests.test_spend_terms import _mep_app

    tmpl = "engine:\n  max_workers_per_node: {{ max_workers_per_node }}\nprovider:\n  mem_per_node: {{ mem_per_node }}\n"
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Templated(DELTA_SCHEMA, tmpl))
    app, _ = await _mep_app(monkeypatch, _anvil_like())
    from hpc_bridge import server

    await server._ensure_endpoint_up(app, "compute", account="x", memory_gb=8, confirm_spend=True, workers=5)
    assert app.built[-1].get("max_workers_per_node") == 5
    fixed = "provider:\n  mem_per_node: {{ mem_per_node }}\n"
    monkeypatch.setattr("tests.test_spend_terms._Status", lambda: _Templated(DELTA_SCHEMA, fixed))
    app2, _ = await _mep_app(monkeypatch, _anvil_like())
    await server._ensure_endpoint_up(app2, "compute", account="x", memory_gb=8, confirm_spend=True, workers=5)
    assert app2.built[-1].get("max_workers_per_node") != 5 and "hpcb_workers" not in app2.built[-1]


async def test_admissions_carry_each_rows_size_in_the_journal(tmp_path, monkeypatch):
    journal = tmp_path / "journal.jsonl"
    monkeypatch.setenv("HPC_BRIDGE_JOURNAL", str(journal))
    app, _rt, _runner = _app_with(_measure(cpus=4, workers=5))
    await _run(app, n=3, cores=2, memory_gb=0.5)
    admits = [json.loads(ln) for ln in journal.read_text().splitlines() if '"batch.admit"' in ln]
    assert admits and all((e["cores"], e["memory_mb"], e["gpus"]) == (2, 512, 0) for e in admits)
    assert len(admits) == 2


# ---- the block: workers follow it, row values never reach its config --------------------------------------------------

def test_our_ssh_template_runs_as_many_workers_as_the_block_has_cores():
    from hpc_bridge.resources import CORES
    from tests.test_resource_requests import _app

    real = _app().facility._real
    uec = {"compute": True, "partition": "shared", CORES: 16, "max_workers_per_node": 2, "nodes_per_block": 1}
    assert real.dispatch_uec(uec)["max_workers_per_node"] == 16
    whole = {"compute": True, "partition": "wholenode", "exclusive": True, "max_workers_per_node": 2,
             "nodes_per_block": 1}
    assert real.dispatch_uec(whole)["max_workers_per_node"] == 128             # the partition's node


async def test_row_values_never_enter_the_user_endpoint_config():
    app, rt, _runner = _app_with(_measure(cpus=8, cpu_ids=tuple(range(8)), workers=9))
    a = await _run(app, n=2, cores=2, memory_gb=0.5, concurrency=2)
    b = await _run(app, n=2, cores=1, memory_gb=1.0, pin=False)
    assert a.batch_id and b.batch_id and a.phase == b.phase == "admitting"
    assert app.ledger.batch(a.batch_id)["uec_hash"] == app.ledger.batch(b.batch_id)["uec_hash"]
    for uec in app.uecs:
        assert not {"cores", "memory_gb", "gpus", "concurrency", "pin", "pilot"} & set(uec), uec
        assert uec.get("max_workers_per_node") == rt.user_endpoint_config.get("max_workers_per_node")

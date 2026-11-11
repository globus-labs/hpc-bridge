"""Batch row sizing (vault `Planned/Batch and swarm execution.md`, "Batch row sizing"): pure, no I/O.

A batch's rows are sized the sbatch way — `cores` (CPUs per row), `memory_gb`, `gpus` and a `concurrency` (the
`--array=…%N` analogue) — and fitted to what the block MEASURED from inside (`MEASURE_SH` → `parse_measure`), or, with
no block yet, the block is DERIVED from them before the spend question (`derive_block`).

- **Cores are scheduler CPUs**, as `-c` / `--cpus-per-task` counts them: one hardware thread where SMT is on (the
  physical cores are reported beside: `BlockMeasure.physical_cores`, `threads_per_core`). k is never rounded up to
  whole cores. The block's CPUs are ordered with SMT siblings adjacent (`cpu_ids`), and a row claims the first free
  ones in that order: when k is a multiple of the threads per core a slot keeps whole cores together; a 1-CPU row on an
  SMT node is pinned to one hardware thread.
- **Slots** (`fit`): min(concurrency, ⌊C ÷ k⌋, ⌊(M − idle) ÷ m⌋, ⌊G ÷ g⌋, W − 1, WIDTH_CAP). The interactive reserve
  is ONE extra worker (W = slots + 1), never a held-back slot and never a GPU; a one-worker block shares it (as before).
- **The derived block** (`derive_block`): N·k + 1 CPUs (one for the interactive worker), ⌈N·m + the pool's own⌉ GB,
  N·g GPUs, W = N + 1 workers, one node.
- **The pilot** (`pilot_proposal`): seff's pattern — cores ⌈CPU-s ÷ wall − 0.1⌉, memory 1.25 × the peak RSS rounded
  up to a quarter GB.
- **Memory sampling** (the wrapper, hpc_bridge.batch): the row's memory is sampled once a second — every 0.2 s while the
  row is young (its first 2 s) or growing fast toward its limit (the Anvil live test, F2) — so a burst within one
  sample can still pass its limit, and meet the site's own OOM killer first (a cgroup's — which may take a neighbour
  —, Slurm's OverMemoryKill, PBS's `$enforce mem`); each result carries the block cgroup's `oom_kill` delta over the
  row's life.
- **Over its size** (`over_size`): a pinned row is over its cores only on evidence — runnable threads above its cores
  in most samples (`oversubscribed`) — never on its thread count.
"""
from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

from . import resources
from .models import PartitionShape

# Rows at once on one node until a per-node watcher replaces each row's own 1 s loop (the follow-up: 64 rows measured
# 11 wrapper processes and 2.5% of a CPU each on 906e303).
WIDTH_CAP = 32
# max_workers_per_node for a whole node whose cores nothing here knows: parsl caps the pool at the CPUs it counts on
# the node; the idle workers' memory is measured (`idle_mb`) and taken off what rows may use.
WORKER_CEILING = 1024
POOL_BASE_MB = 256          # a derived block's pool, beside its workers: the manager and its interchange client
WORKER_MB = 100             # one parsl worker process, idle
DEFAULT_ROW_MB = 1024       # a derived row's memory when neither the call nor the partition says one
PILOT_TOLERANCE = 0.1       # ⌈CPU-s ÷ wall − this⌉: a pilot that used 2.05 CPUs asks for 2
OVER_CPU = 1.25             # an unpinned row over cores × this is flagged over its size
OVER_SHARE = 0.5            # a pinned row: runnable threads above its cores in at least this share of the samples…
OVER_MIN_SAMPLES = 3        # …of at least this many (`oversubscribed`)
NEAR_MEMORY = 0.9           # a row whose peak reached this share of its limit is flagged
KILLED = frozenset({"memory", "timeout", "cancelled"})   # a row's `why` when it was killed before its end
QUARTER_MB = 256
_UNLIMITED = 2 ** 60        # cgroup v1's "no limit" is PAGE_COUNTER_MAX (9223372036854771712) bytes
# bound_by on a tie: the most specific first — and CPUs before memory, which a default share divides by the slots
_TIE = ("concurrency", "workers", "gpus", "cpus", "memory", "width")


class SizingRefusal(ValueError):
    """The size cannot be had: a derived block bigger than a node, or a concurrency past the width cap."""


@dataclass(frozen=True)
class Sizing:
    """A batch's rows: CPUs, memory (MB; None: the block's share), GPUs, at most `concurrency` at once, pinned."""

    cores: int = 1
    memory_mb: int | None = None
    gpus: int = 0
    concurrency: int | None = None
    pin: bool = True


@dataclass(frozen=True)
class BlockMeasure:
    """What a block is, read from inside it (`parse_measure`)."""

    cpus: int
    cpu_ids: tuple[int, ...] = ()           # the block's CPUs, SMT siblings adjacent, `cpus` of them
    cpus_source: str = "nproc"              # what bounded it: affinity, cpuset, cpu.max, SLURM_CPUS_ON_NODE, NCPUS
    physical_cores: int | None = None
    threads_per_core: int = 1
    mem_limit_mb: int | None = None
    mem_source: str | None = None
    idle_mb: int | None = None              # the pool's own use, idle (memory.current at the limit's level)
    oom_kills: int | None = None
    gpu_ids: tuple[str, ...] = ()
    gpu_kind: str | None = None             # cuda | rocm | ze
    workers: int | None = None              # PARSL_WORKER_COUNT
    nodes: int | None = None

    @property
    def avail_mb(self) -> int | None:
        """The memory rows may use: the limit less what the pool holds idle."""
        if self.mem_limit_mb is None:
            return None
        return max(0, self.mem_limit_mb - (self.idle_mb or 0))

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["cpu_ids"], d["gpu_ids"] = list(self.cpu_ids), list(self.gpu_ids)
        return d

    @classmethod
    def from_dict(cls, d: Mapping[str, Any] | None) -> BlockMeasure | None:
        if not d or not isinstance(d.get("cpus"), int):
            return None
        known = {f for f in cls.__dataclass_fields__}
        kw = {k: v for k, v in d.items() if k in known}
        kw["cpu_ids"] = tuple(int(x) for x in kw.get("cpu_ids") or ())
        kw["gpu_ids"] = tuple(str(x) for x in kw.get("gpu_ids") or ())
        return cls(**kw)


# ---- measuring the block from inside ---------------------------------------------------------------------------------

# Prints the raw facts as `HPCB_M <key> …` lines; parse_measure decides. Files are read under $HPCB_SYSROOT when it is
# set (a read-only seam for the tests); every read is bounded and quiet. POSIX sh plus bash's $(…); bash 3.2 runs it.
MEASURE_SH = r"""__hb_sys="${HPCB_SYSROOT:-}"
__hb_m() { printf 'HPCB_M %s\n' "$*"; }
__hb_v=$( (env -u OMP_NUM_THREADS -u OMP_THREAD_LIMIT nproc || getconf _NPROCESSORS_ONLN) 2>/dev/null | head -n 1)
[ -n "$__hb_v" ] && __hb_m nproc "$__hb_v"
__hb_v=$(awk '/^Cpus_allowed_list:/ {print $2}' "$__hb_sys/proc/self/status" 2>/dev/null)
[ -n "$__hb_v" ] && __hb_m affinity "$__hb_v"
__hb_v=$(head -n 1 "$__hb_sys/sys/devices/system/cpu/online" 2>/dev/null)
[ -n "$__hb_v" ] && __hb_m online "$__hb_v"
__hb_v=$(awk '/^MemTotal:/ {print $2}' "$__hb_sys/proc/meminfo" 2>/dev/null)
[ -n "$__hb_v" ] && __hb_m meminfo MemTotal "$__hb_v"
if [ -r "$__hb_sys/proc/self/cgroup" ]; then
  while IFS= read -r __hb_l; do
    __hb_m cgroup "$__hb_l"
    __hb_c=${__hb_l#*:}; __hb_ctl=${__hb_c%%:*}; __hb_p=${__hb_c#*:}
    if [ -z "$__hb_ctl" ]; then
      while :; do
        __hb_d="$__hb_sys/sys/fs/cgroup$__hb_p"
        for __hb_f in memory.max memory.current cpu.max cpuset.cpus.effective; do
          [ -r "$__hb_d/$__hb_f" ] && __hb_m cg2 "${__hb_p:-/}" "$__hb_f" "$(head -n 1 "$__hb_d/$__hb_f")"
        done
        __hb_v=$(awk '$1 == "oom_kill" {print $2}' "$__hb_d/memory.events" 2>/dev/null)
        [ -n "$__hb_v" ] && __hb_m cg2 "${__hb_p:-/}" memory.events.oom_kill "$__hb_v"
        { [ -z "$__hb_p" ] || [ "$__hb_p" = / ]; } && break
        __hb_p=${__hb_p%/*}
      done
    else
      case ",$__hb_ctl," in
        *,memory,*)
          while :; do
            __hb_d="$__hb_sys/sys/fs/cgroup/memory$__hb_p"
            for __hb_f in memory.limit_in_bytes memory.usage_in_bytes; do
              [ -r "$__hb_d/$__hb_f" ] && __hb_m cg1 memory "${__hb_p:-/}" "$__hb_f" "$(head -n 1 "$__hb_d/$__hb_f")"
            done
            __hb_v=$(awk '$1 == "hierarchical_memory_limit" {print $2}' "$__hb_d/memory.stat" 2>/dev/null)
            [ -n "$__hb_v" ] && __hb_m cg1 memory "${__hb_p:-/}" hierarchical_memory_limit "$__hb_v"
            __hb_v=$(awk '$1 == "oom_kill" {print $2}' "$__hb_d/memory.oom_control" 2>/dev/null)
            [ -n "$__hb_v" ] && __hb_m cg1 memory "${__hb_p:-/}" memory.oom_control.oom_kill "$__hb_v"
            { [ -z "$__hb_p" ] || [ "$__hb_p" = / ]; } && break
            __hb_p=${__hb_p%/*}
          done;;
      esac
      case ",$__hb_ctl," in *,cpuset,*)
        __hb_v=$(head -n 1 "$__hb_sys/sys/fs/cgroup/cpuset$__hb_p/cpuset.cpus" 2>/dev/null)
        [ -n "$__hb_v" ] && __hb_m cg1 cpuset "$__hb_p" cpuset.cpus "$__hb_v";; esac
      case ",$__hb_ctl," in *,cpu,*)
        for __hb_f in cpu.cfs_quota_us cpu.cfs_period_us; do
          __hb_v=$(head -n 1 "$__hb_sys/sys/fs/cgroup/$__hb_ctl$__hb_p/$__hb_f" 2>/dev/null)
          [ -n "$__hb_v" ] && __hb_m cg1 cpu "$__hb_p" "$__hb_f" "$__hb_v"
        done;; esac
    fi
  done < "$__hb_sys/proc/self/cgroup"
fi
grep -H . "$__hb_sys"/sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list 2>/dev/null \
  | sed 's#^.*/cpu\([0-9][0-9]*\)/topology/thread_siblings_list:#HPCB_M sib \1 #'
for __hb_e in SLURM_CPUS_ON_NODE SLURM_MEM_PER_NODE SLURM_MEM_PER_CPU SLURM_JOB_NUM_NODES NCPUS CUDA_VISIBLE_DEVICES \
    ROCR_VISIBLE_DEVICES HIP_VISIBLE_DEVICES ZE_AFFINITY_MASK SLURM_JOB_GPUS SLURM_STEP_GPUS GPU_DEVICE_ORDINAL \
    PARSL_WORKER_COUNT; do
  eval "__hb_x=\${$__hb_e+set}"
  [ "$__hb_x" = set ] && eval "__hb_m env $__hb_e \"\$$__hb_e\""
done
if [ -n "${PBS_JOBID:-}" ]; then
  qstat -f "$PBS_JOBID" 2>/dev/null | awk '$1 == "Resource_List.mem" {print "HPCB_M pbs Resource_List.mem", $3}'
  [ -r "${PBS_NODEFILE:-}" ] && __hb_m nodes "$(sort -u "$PBS_NODEFILE" | wc -l | tr -d ' ')"
fi
if [ -z "${CUDA_VISIBLE_DEVICES+x}${ROCR_VISIBLE_DEVICES+x}${ZE_AFFINITY_MASK+x}" ] \
    && command -v nvidia-smi >/dev/null 2>&1; then
  __hb_m nvsmi "$(nvidia-smi -L 2>/dev/null | grep -c '^GPU ')"
fi
__hb_v=$(ls -d "$__hb_sys"/dev/nvidia[0-9]* 2>/dev/null | wc -l | tr -d ' ')
[ "${__hb_v:-0}" -gt 0 ] 2>/dev/null && __hb_m dev nvidia "$__hb_v"
:"""


def parse_cpu_list(text: str) -> list[int] | None:
    """`0-3,8,10-11` → [0, 1, 2, 3, 8, 10, 11]; None when it cannot be read."""
    out: list[int] = []
    for part in (text or "").strip().split(","):
        part = part.strip()
        if not part:
            continue
        a, _, b = part.partition("-")
        if not a.isdigit() or (b and not b.isdigit()):
            return None
        out += range(int(a), int(b or a) + 1)
    return sorted(set(out)) if out else None


def _int(v: Any) -> int | None:
    try:
        n = int(str(v).strip())
    except (TypeError, ValueError):
        return None
    return n


def _pbs_mem_mb(text: str) -> int | None:
    m = re.match(r"^\s*(\d+)\s*([kmgtp]?)(b|w)?\s*$", text or "", re.I)
    if not m:
        return None
    n = int(m.group(1)) * (8 if (m.group(3) or "").lower() == "w" else 1)
    scale = {"": 1 / 1024 / 1024, "k": 1 / 1024, "m": 1, "g": 1024, "t": 1024 ** 2, "p": 1024 ** 3}
    return int(n * scale[m.group(2).lower()])


def _depth(path: str) -> int:
    return len([p for p in path.split("/") if p])


def parse_measure(text: str | None) -> BlockMeasure | None:
    """The block, from MEASURE_SH's `HPCB_M` lines (any other line is ignored); None when there is none.

    - CPUs: the least of the affinity mask (else the online list, else nproc), the cgroup cpuset, its cpu.max quota
      (v1: cfs_quota ÷ cfs_period), SLURM_CPUS_ON_NODE and PBS NCPUS — at a tie the scheduler's own word names it.
    - The memory limit: the least of the lowest cgroup v2 memory.max up the tree, v1's hierarchical limit (its
      no-limit value ignored), SLURM_MEM_PER_NODE, SLURM_MEM_PER_CPU × CPUs, PBS Resource_List.mem, MemTotal.
    - Idle use: memory.current (v1 usage_in_bytes) at the limit's level; `oom_kill`: the largest count up the tree.
    - GPUs: CUDA_VISIBLE_DEVICES (`NoDevFiles` or empty: none), ROCR / HIP_VISIBLE_DEVICES, ZE_AFFINITY_MASK,
      SLURM_JOB_GPUS / SLURM_STEP_GPUS / GPU_DEVICE_ORDINAL, then nvidia-smi's count, then /dev/nvidia*."""
    lines = [ln[len("HPCB_M "):] for ln in str(text or "").splitlines() if ln.startswith("HPCB_M ")]
    if not lines:
        return None
    env: dict[str, str] = {}
    cg2: list[tuple[str, str, str]] = []
    cg1: list[tuple[str, str, str, str]] = []
    sib: dict[int, list[int]] = {}
    one: dict[str, str] = {}
    for ln in lines:
        parts = ln.split(" ")
        key = parts[0]
        if key == "env" and len(parts) >= 2:
            env[parts[1]] = " ".join(parts[2:]).strip()
        elif key == "cg2" and len(parts) >= 4:
            cg2.append((parts[1], parts[2], " ".join(parts[3:]).strip()))
        elif key == "cg1" and len(parts) >= 5:
            cg1.append((parts[1], parts[2], parts[3], " ".join(parts[4:]).strip()))
        elif key == "sib" and len(parts) >= 3 and parts[1].isdigit():
            ids = parse_cpu_list(parts[2])
            if ids:
                sib[int(parts[1])] = ids
        elif key in ("pbs", "meminfo", "dev") and len(parts) >= 3:
            one[f"{key} {parts[1]}"] = parts[2].strip()
        elif len(parts) >= 2:
            one[key] = " ".join(parts[1:]).strip()

    # -- CPUs --
    base = parse_cpu_list(one.get("affinity", "")) or parse_cpu_list(one.get("online", ""))
    nproc = _int(one.get("nproc"))
    if base is None and nproc:
        base = list(range(nproc))
    ids = list(base or [])
    counts: list[tuple[int, str]] = []
    if base:
        counts.append((len(base), "affinity" if "affinity" in one else "nproc"))
    cpuset = [parse_cpu_list(v) for _d, f, v in cg2 if f == "cpuset.cpus.effective"]
    cpuset += [parse_cpu_list(v) for ctl, _d, f, v in cg1 if ctl == "cpuset" and f == "cpuset.cpus"]
    for s in cpuset:
        if s:
            ids = [i for i in ids if i in set(s)] if ids else list(s)
            counts.append((len(ids), "cpuset"))
    for _d, f, v in cg2:
        if f == "cpu.max":
            q, _, p = v.partition(" ")
            if q.isdigit() and p.strip().isdigit() and int(p) > 0:
                counts.append((max(1, int(q) // int(p)), "cpu.max"))
    quota = next((_int(v) for ctl, _d, f, v in cg1 if ctl == "cpu" and f == "cpu.cfs_quota_us"), None)
    period = next((_int(v) for ctl, _d, f, v in cg1 if ctl == "cpu" and f == "cpu.cfs_period_us"), None)
    if quota and quota > 0 and period:
        counts.append((max(1, quota // period), "cpu.max"))
    for name in ("NCPUS", "SLURM_CPUS_ON_NODE"):
        n = _int(env.get(name))
        if n and n > 0:
            counts.append((n, name))
    if not counts:
        return None
    cpus = min(n for n, _s in counts)
    source = [s for n, s in counts if n == cpus][-1]   # the latest of the least: the scheduler's word over the mask's
    if sib:
        groups: dict[tuple[int, ...], list[int]] = {}
        for i in ids:
            g = tuple(x for x in sib.get(i, [i]) if x in set(ids)) or (i,)
            groups.setdefault(g, []).append(i)
        ordered = [i for g in sorted(groups, key=min) for i in sorted(groups[g])]
        physical, tpc = len(groups), max(len(g) for g in groups)
    else:
        ordered, physical, tpc = sorted(ids), None, 1
    if len(ordered) < cpus:
        ordered = ordered + [i for i in range(cpus * 4) if i not in ordered][: cpus - len(ordered)]
    cpu_ids = tuple(ordered[:cpus])

    # -- memory --
    mems: list[tuple[int, str, str | None]] = []
    for d, f, v in cg2:
        if f == "memory.max" and v.isdigit():
            mems.append((int(v) // 2 ** 20, "cgroup v2 memory.max", d))
    for ctl, d, f, v in cg1:
        if ctl == "memory" and f in ("hierarchical_memory_limit", "memory.limit_in_bytes") and v.isdigit() \
                and int(v) < _UNLIMITED:
            mems.append((int(v) // 2 ** 20, f"cgroup v1 {f}", d))
    if (n := _int(env.get("SLURM_MEM_PER_NODE"))) and n > 0:
        mems.append((n, "SLURM_MEM_PER_NODE", None))
    if (n := _int(env.get("SLURM_MEM_PER_CPU"))) and n > 0:
        mems.append((n * (_int(env.get("SLURM_CPUS_ON_NODE")) or cpus), "SLURM_MEM_PER_CPU", None))
    if (n := _pbs_mem_mb(one.get("pbs Resource_List.mem", ""))) is not None:
        mems.append((n, "PBS Resource_List.mem", None))
    if (n := _int(one.get("meminfo MemTotal"))) and n > 0:
        mems.append((n // 1024, "MemTotal", None))
    limit = min(mems, key=lambda m: m[0]) if mems else None
    idle = None
    workers = _int(env.get("PARSL_WORKER_COUNT"))
    if limit is not None:
        cur = {d: _int(v) for d, f, v in cg2 if f == "memory.current"}
        cur.update({d: _int(v) for ctl, d, f, v in cg1 if ctl == "memory" and f == "memory.usage_in_bytes"})
        at = limit[2]
        if at is not None and at in cur and cur[at] is not None:
            idle = int(cur[at] or 0) // 2 ** 20
        elif at is not None and (near := [(_depth(d), n) for d, n in cur.items()
                                          if n is not None and at.startswith(d.rstrip("/"))]):
            idle = int(max(near)[1]) // 2 ** 20
        else:
            # the limit is the scheduler's word (or MemTotal), with no level of its own: the pool's use is still taken
            # off — the deepest cgroup reading seen (the pool's own), else the pool's estimate. Never none: a share
            # sized against the whole limit overcommits the job (the site's OOM, OverMemoryKill).
            deepest = [(_depth(d), n) for d, n in cur.items() if n is not None]
            idle = (int(max(deepest)[1]) // 2 ** 20 if deepest
                    else pool_mb(workers if workers and workers > 0 else 1))
    ooms = [_int(v) for _d, f, v in cg2 if f == "memory.events.oom_kill"]
    ooms += [_int(v) for ctl, _d, f, v in cg1 if ctl == "memory" and f == "memory.oom_control.oom_kill"]
    oom = max((o for o in ooms if o is not None), default=None)

    # -- GPUs --
    gpu_ids: tuple[str, ...] = ()
    kind: str | None = None
    nvidia = (_int(one.get("nvsmi")) or 0) > 0 or (_int(one.get("dev nvidia")) or 0) > 0

    def _split(v: str) -> tuple[str, ...]:
        return tuple(x.strip() for x in v.split(",") if x.strip())

    if "CUDA_VISIBLE_DEVICES" in env:
        v = env["CUDA_VISIBLE_DEVICES"]
        gpu_ids, kind = ((), None) if v in ("", "NoDevFiles") else (_split(v), "cuda")
    elif env.get("ROCR_VISIBLE_DEVICES") or env.get("HIP_VISIBLE_DEVICES"):
        gpu_ids, kind = _split(env.get("ROCR_VISIBLE_DEVICES") or env["HIP_VISIBLE_DEVICES"]), "rocm"
    elif env.get("ZE_AFFINITY_MASK"):
        gpu_ids, kind = _split(env["ZE_AFFINITY_MASK"]), "ze"
    elif any(env.get(k) for k in ("SLURM_JOB_GPUS", "SLURM_STEP_GPUS", "GPU_DEVICE_ORDINAL")):
        v = next(env[k] for k in ("SLURM_JOB_GPUS", "SLURM_STEP_GPUS", "GPU_DEVICE_ORDINAL") if env.get(k))
        gpu_ids, kind = _split(v), ("cuda" if nvidia else None)
    elif (n := _int(one.get("nvsmi")) or _int(one.get("dev nvidia")) or 0) > 0:   # nvidia-smi's count, else /dev's
        gpu_ids, kind = tuple(str(i) for i in range(n)), "cuda"
    if not gpu_ids:
        kind = None

    nodes = _int(env.get("SLURM_JOB_NUM_NODES")) or _int(one.get("nodes"))
    return BlockMeasure(cpus=cpus, cpu_ids=cpu_ids, cpus_source=source, physical_cores=physical, threads_per_core=tpc,
                        mem_limit_mb=limit[0] if limit else None, mem_source=limit[1] if limit else None,
                        idle_mb=idle, oom_kills=oom, gpu_ids=gpu_ids, gpu_kind=kind,
                        workers=workers if workers and workers > 0 else None, nodes=nodes if nodes and nodes > 0
                        else None)


# ---- fitting rows to a block -----------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Fit:
    slots: int                      # rows of this size at once on the block
    bound_by: str                   # what bounds them: concurrency | workers | gpus | memory | cpus | width
    memory_mb: int | None = None    # per row: the size asked, else the block's share less the pool's idle use
    why_not: str | None = None      # a row cannot fit at all: why, with the numbers
    limits: dict[str, int] = field(default_factory=dict)


def _gb(mb: float) -> str:
    return f"{mb / 1024:.1f} GB"


def fit(measure: BlockMeasure, sizing: Sizing) -> Fit:
    """How many rows of `sizing` the measured block runs at once, and what bounds them."""
    k, g = max(1, sizing.cores), max(0, sizing.gpus)
    gpus = len(measure.gpu_ids)
    avail = measure.avail_mb
    if k > measure.cpus:
        return Fit(0, "cpus", sizing.memory_mb,
                   f"a row of {k} CPUs does not fit this block's {measure.cpus} CPUs ({measure.cpus_source})")
    if g > gpus:
        return Fit(0, "gpus", sizing.memory_mb, f"a row of {g} GPU(s) does not fit this block's {gpus} GPU(s)")
    if sizing.memory_mb and avail is not None and sizing.memory_mb > avail:
        return Fit(0, "memory", sizing.memory_mb,
                   f"a row's memory of {_gb(sizing.memory_mb)} does not fit this block's {_gb(avail)} "
                   f"({_gb(measure.mem_limit_mb or 0)} less the {_gb(measure.idle_mb or 0)} the worker pool holds "
                   f"idle; {measure.mem_source})")
    lim: dict[str, int] = {"cpus": measure.cpus // k, "width": WIDTH_CAP}
    if g:
        lim["gpus"] = gpus // g
    if measure.workers is not None:
        lim["workers"] = measure.workers - 1 if measure.workers >= 2 else 1
    if sizing.concurrency:
        lim["concurrency"] = sizing.concurrency
    memory = sizing.memory_mb
    if memory:
        if avail is not None:
            lim["memory"] = avail // memory
    elif avail is not None:
        share_of = max(1, min(lim.values()))
        memory = max(1, avail // share_of)
    slots = min(lim.values())
    bound = next(b for b in _TIE if lim.get(b) == slots)
    return Fit(max(0, slots), bound, memory, None, lim)


# ---- the block derived from the rows (size first) --------------------------------------------------------------------

@dataclass(frozen=True)
class DerivedBlock:
    cores: int
    memory_gb: int | None           # None: a whole node's (its partition hands out only whole nodes)
    gpus: int
    workers: int | None             # None: the facility fixes its own (the terms say how many)
    nodes: int = 1
    whole_node: bool = False        # the partition hands out only whole nodes: the block is one, as many rows as fit

    def words(self, sizing: Sizing) -> str:
        mem = f", {_gb(sizing.memory_mb)}" if sizing.memory_mb else ""
        gpu = f", {sizing.gpus} GPU" if sizing.gpus else ""
        row = f"({sizing.cores} CPU{'s' if sizing.cores != 1 else ''}{mem}{gpu})"
        if self.whole_node:
            return (f"a whole node (the partition hands out no less): {sizing.concurrency} rows at once × {row}, up to "
                    f"{(self.workers or 2) - 1} would fit")
        return f"{sizing.concurrency} rows at once × {row} + 1 CPU for interactive commands"


def row_memory_mb(sizing: Sizing, shape: PartitionShape | None) -> int:
    """A derived row's memory: the call's, else the partition's per core (DefMemPerCPU, else the node's), else 1 GB."""
    if sizing.memory_mb:
        return sizing.memory_mb
    if shape is not None and shape.mem_per_core_mb:
        return shape.mem_per_core_mb * sizing.cores
    if shape is not None and shape.memory_gb and shape.cores:
        return int(shape.memory_gb * 1024 / shape.cores) * sizing.cores
    return DEFAULT_ROW_MB


def pool_mb(workers: int) -> int:
    return POOL_BASE_MB + WORKER_MB * workers


def derive_block(sizing: Sizing, *, shape: PartitionShape | None = None, workers_fixed: int | None = None
                 ) -> DerivedBlock:
    """N rows × (k CPUs, m, g GPUs) → the block: N·k + 1 CPUs, ⌈N·m + the pool's own⌉ GB, N·g GPUs, W = N + 1, one node.
    `workers_fixed`: a facility endpoint's own worker count (its template fixes it): not derived, and the concurrency
    is bound by it. Refused (SizingRefusal) with the numbers when one node of `shape` cannot hold it."""
    n = sizing.concurrency
    if not n:
        raise SizingRefusal("a block derived from the rows needs a concurrency: how many rows at once")
    k, g = sizing.cores, sizing.gpus
    if n > WIDTH_CAP:
        raise SizingRefusal(f"{n} rows at once is past the {WIDTH_CAP} a node runs at once for now (each row's own "
                            "watcher costs a share of a CPU); ask for at most that many")
    if workers_fixed is not None and n > max(1, workers_fixed - 1):
        raise SizingRefusal(f"the facility runs {workers_fixed} workers per node (its template fixes it): at most "
                            f"{max(1, workers_fixed - 1)} rows at once, one worker kept for interactive commands")
    cores = n * k + 1
    workers = None if workers_fixed is not None else n + 1
    m = row_memory_mb(sizing, shape)
    memory_gb = max(1, math.ceil((n * m + pool_mb(workers or workers_fixed or n + 1)) / 1024))
    gpus = n * g
    if shape is not None and shape.kind == "whole_node" and shape.cores:
        # a partition that hands out only whole nodes: the block IS a whole node — as many rows of this size as fit it
        # run there (its workers: those slots + 1), the concurrency asked bounding the batch, not the block
        fits = min((shape.cores - 1) // k, WIDTH_CAP)
        if shape.memory_gb:
            fits = min(fits, int((shape.memory_gb * 1024 - pool_mb(fits + 1)) // m))
        node_gpus = shape.max_gpus_per_node if shape.max_gpus_per_node is not None else shape.gpus
        if g and node_gpus is not None:
            fits = min(fits, node_gpus // g)
        if fits < 1:
            raise SizingRefusal(f"a row of {k} CPUs, {_gb(m)}" + (f", {g} GPU(s)" if g else "") + f" does not fit a "
                                f"node of {shape.name!r} ({shape.cores} CPUs" + (f", {shape.memory_gb:g} GB"
                                                                                 if shape.memory_gb else "")
                                + (f", {node_gpus} GPU(s)" if node_gpus is not None else "") + ")")
        return DerivedBlock(cores=shape.cores, memory_gb=None, gpus=(node_gpus or 0) if g else 0,
                            workers=None if workers_fixed is not None else fits + 1, whole_node=True)
    if shape is not None:
        multi = "A batch over several nodes is phase 3's campaign pools."

        def held(rows: int) -> int:
            """The CPUs `rows` rows' block holds: N·k + 1, raised where its memory is above their per-core memory share
            (resources.cores_for_memory — the block asks for, and is billed, that many: the Anvil live test of
            2026-11-10)."""
            c = rows * k + 1
            mem = max(1, math.ceil((rows * m + pool_mb(workers_fixed or rows + 1)) / 1024))
            return resources.cores_for_memory(c, mem, shape) or c

        holds = held(n)
        why = (f" ({memory_gb} GB needs {holds} cores' memory share, {resources.enforced_share_mb(shape)} MB each)"
               if holds > cores else "")
        if shape.cores and holds > shape.cores:
            fits = max([r for r in range(1, n) if held(r) <= shape.cores], default=0)
            raise SizingRefusal(f"{n} rows × {k} CPUs + 1 for interactive commands = {cores} CPUs{why}; a node of "
                                f"{shape.name!r} has {shape.cores}: at most {fits} rows at once on one node. {multi}")
        cap = shape.max_cores_per_job or shape.max_cores_per_node
        if cap and holds > cap:
            fits = max([r for r in range(1, n) if held(r) <= cap], default=0)
            raise SizingRefusal(f"{holds} CPUs{why} is over {shape.name!r}'s per-job cap of {cap}: at most "
                                f"{fits} rows of {k} CPUs at once")
        if shape.memory_gb and memory_gb > shape.memory_gb:
            fits = int((shape.memory_gb * 1024 - pool_mb(n + 1)) // m)
            raise SizingRefusal(f"{n} rows × {_gb(m)} + the pool's own = {memory_gb} GB; a node of {shape.name!r} "
                                f"has {shape.memory_gb:g} GB: at most {max(0, fits)} rows at once. {multi}")
        node_gpus = shape.max_gpus_per_node if shape.max_gpus_per_node is not None else shape.gpus
        if gpus and node_gpus is not None and gpus > node_gpus:
            raise SizingRefusal(f"{n} rows × {g} GPU(s) = {gpus} GPUs; a node of {shape.name!r} has {node_gpus}: "
                                f"at most {node_gpus // g} rows at once. {multi}")
    return DerivedBlock(cores=cores, memory_gb=memory_gb, gpus=gpus, workers=workers)


def candidates(cpus: int | None, memory_gb: float | None, gpus: int | None, *, workers_fixed: int | None = None
               ) -> list[dict[str, Any]]:
    """What one node gives each candidate row size, for the user to answer in sbatch terms: {cores, memory_gb (each
    row's share), concurrency}."""
    if not cpus:
        return []
    out = []
    k = 1
    while k <= max(1, cpus - 1):
        n = min((cpus - 1) // k, WIDTH_CAP)
        if workers_fixed is not None:
            n = min(n, max(1, workers_fixed - 1))
        if n < 1:
            break
        share = None
        if memory_gb:
            share = round(max(0.0, memory_gb * 1024 - pool_mb(n + 1)) / n / 1024, 2)
        out.append({"cores": k, "memory_gb": share, "concurrency": n})
        k *= 2
    return out


# ---- the pilot -------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Proposal:
    cores: int | None               # None: no row's CPU-seconds could be read — no evidence, never "1"
    memory_gb: float
    gpus: int = 0
    concurrency: int = 0
    evidence: list[dict[str, Any]] = field(default_factory=list)


def quarter_gb(mb: float) -> float:
    """Rounded up to a quarter GB, at least a quarter."""
    return max(0.25, math.ceil(mb / QUARTER_MB - 1e-9) / 4)


def pilot_proposal(evidence: Iterable[Mapping[str, Any]], measure: BlockMeasure, *, gpus: int = 0) -> Proposal:
    """seff's pattern over the pilot rows' own use: cores ⌈CPU-s ÷ wall − PILOT_TOLERANCE⌉ (at least 1, at most the
    block's CPUs), memory 1.25 × the peak RSS rounded up to a quarter GB (at least a quarter) — the largest over them —
    and the concurrency the block then fits. A row killed before its end (memory, timeout, abort) is no evidence of its
    cores, whatever CPU-seconds it reported: its killed processes' CPU time is lost with them (review R2-G)."""
    rows = [dict(e) for e in evidence]
    cores: int | None = None
    mem = 0.25
    for e in rows:
        wall, cpu = e.get("wall_s"), e.get("cpu_s")
        if e.get("why") in KILLED:
            cpu = None
        if cpu is not None:   # a row whose CPU-seconds could not be read is no evidence of its cores — never 0
            k = max(1, math.ceil(float(cpu) / float(wall) - PILOT_TOLERANCE)) if wall and float(wall) > 0 else 1
            cores = max(cores or 1, min(k, measure.cpus))
        mem = max(mem, quarter_gb(1.25 * float(e.get("peak_rss_kb") or 0) / 1024))
    f = fit(measure, Sizing(cores=cores or 1, memory_mb=int(mem * 1024), gpus=gpus))
    return Proposal(cores=cores, memory_gb=mem, gpus=gpus, concurrency=f.slots, evidence=rows)


def oversubscribed(r_over: int | None, r_samples: int | None) -> bool:
    """A pinned row's evidence of oversubscription (the Anvil live test, F1): more RUNNABLE threads than its cores in at
    least OVER_SHARE of the watcher's samples, over at least OVER_MIN_SAMPLES of them. A runnable thread (state R) is
    running or waiting for a CPU: a pinned row's runnable threads beyond its cores queue for its CPUs. More than half
    its life so is "sustained" — a fork, a pool starting, a garbage collector waking, the row's own exit, are bursts;
    fewer than three samples (a row of a few seconds) is no evidence either way."""
    if r_over is None or not r_samples or r_samples < OVER_MIN_SAMPLES:
        return False
    return r_over / r_samples >= OVER_SHARE


def over_size(*, cores: int | None, memory_mb: int | None, pinned: bool, cpu_s: float | None, wall_s: float | None,
              threads: int | None, peak_rss_kb: int | None, why: str | None, r_over: int | None = None,
              r_samples: int | None = None) -> list[str]:
    """What a settled row ran over: `cores` — unpinned, CPU-s ÷ wall past cores × OVER_CPU; pinned (which caps its
    CPU-s at its cores), runnable threads above its cores in most of the watcher's samples (`oversubscribed`), never
    its thread count: idle threads (its shell, `timeout`, a sleeping interpreter) count there (the Anvil live test, F1)
    — and `memory` (killed at its limit, or a peak within NEAR_MEMORY of it). `threads` is kept for what it reports;
    it judges nothing."""
    out = []
    k = cores or 1
    if (not pinned and cpu_s is not None and wall_s and cpu_s / wall_s > k * OVER_CPU) or \
            (pinned and oversubscribed(r_over, r_samples)):
        out.append("cores")
    if why == "memory" or (memory_mb and peak_rss_kb is not None and peak_rss_kb / 1024 >= NEAR_MEMORY * memory_mb):
        out.append("memory")
    return out

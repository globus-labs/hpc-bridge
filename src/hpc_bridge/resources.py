"""Resource requests: a block's slice of a node, and what N nodes means (vault `Planned/Resource requests.md`).

A block used to be whole nodes, always: parsl's SlurmProvider writes `#SBATCH --exclusive` unless told otherwise, and on
the live Anvil run of 2026-10-24 a bring-your-own block on the per-core `shared` partition asked for an exclusive node,
queued for more than a day waiting for an idle one, and would have billed all 128 cores. The maintainer's decisions:

- D1. Ask for what you need: cores, memory and GPUs per node, and the node count. A whole node only when asked, or when
  the partition hands out only whole nodes.
- D2. N nodes is one shell over the whole allocation, like `salloc`: hpc-bridge's worker runs on the first node only,
  and the user's commands use `srun` / `mpiexec` across the N nodes; nodes not used still bill.
- D3. On a per-core partition with nothing asked, the spend question proposes a modest slice (a few cores and their
  memory) for the user to confirm or change. On a whole-node partition, it is a whole node.

The request lives in the compute shape's runtime config under hpc-bridge's names (`SIZE_KEYS`), persisted for the
session in `AppCtx.compute_choice` like the partition and account; each facility maps it only AT THE WIRE (its
`dispatch_uec`): hpc-bridge's own Slurm and PBS templates (facility/remote.py), a facility endpoint's own keys
(facility/mep.py). What a partition hands out — `models.PartitionShape`, from the login-node probe (`classify_slurm`,
`classify_pbs`) or the registry — decides the default slice, the validation and the cost estimate. Pure: no I/O.

What parsl does with it (read from the locked parsl 2026.8.10 and the 2026.10.x the endpoint venv takes — the same in
both; globus-compute-endpoint 4.18 passes every provider key through to the class, None values dropped):
- SlurmProvider `cores_per_node=C` → `#SBATCH --cpus-per-task=C` (floor(C / tasks_per_node), and HTEX always submits
  with tasks_per_node=1, so `--ntasks-per-node=1` and C cpus on each node), exporting PARSL_CORES; `mem_per_node=M`
  (an int, GB) → `#SBATCH --mem=Mg`, exporting PARSL_MEMORY_GB; `exclusive` → `#SBATCH --exclusive` (True by
  default); its `gpus_per_node` exists only from parsl 2026.04, so the GPU request is hpc-bridge's own
  `#SBATCH --gpus-per-node=G` in scheduler_options (any parsl).
- PBSProProvider writes `#PBS -l select=<nodes>:ncpus=<cpus_per_node><:select_options>`; `place` only through
  scheduler_options.
- Launchers: SrunLauncher runs `srun --ntasks <nodes>` — a worker pool on EVERY node of the block, as a job step that
  holds the CPUs it was given (a user's own `srun` from inside it waits for them); SingleNodeLauncher starts
  <nodes> × tasks_per_node pools, all on the FIRST node; SimpleLauncher runs the command as is, once, in the batch
  script on the first node (it only logs a warning when nodes_per_block > 1). So a block of N > 1 nodes takes
  SimpleLauncher on both schedulers: one worker on the first node, in the batch step, from which `srun -N<N>` /
  `mpiexec` reach every node as from an `salloc` shell. One node keeps the launcher it had (SrunLauncher on Slurm,
  SingleNodeLauncher on PBS).
"""
from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from .models import PartitionShape

# The runtime config's keys for a block's size (hpc-bridge's names; parsl's for Slurm); nodes_per_block was always
# there.
NODES, CORES, MEM, GPUS, WHOLE = "nodes_per_block", "cores_per_node", "mem_per_node", "gpus_per_node", "exclusive"
SIZE_KEYS = (NODES, CORES, MEM, GPUS, WHOLE)
SLICE_KEYS = (CORES, MEM, GPUS, WHOLE)
# ensure_endpoint_up's argument -> the runtime config key it sets
ARG_KEYS = {"cores": CORES, "memory_gb": MEM, "gpus": GPUS, "nodes": NODES, "whole_node": WHOLE}
KEY_ARGS = {v: k for k, v in ARG_KEYS.items()}

DEFAULT_SLICE_CORES = 8        # D3: "a few cores" — and never more than half a node, nor a per-job cap allows
UNKNOWN_NODE_SLICE_CORES = 4   # the same when the node's core count is not known
MAX_COUNT = 100_000            # a sanity bound on every count (cores, GB, GPUs, nodes)

Channel = Literal["ssh", "mep", "other"]


# ---- the request -----------------------------------------------------------------------------------------------------

def requested(cores: Any = None, memory_gb: Any = None, gpus: Any = None, nodes: Any = None,
              whole_node: Any = None) -> tuple[dict[str, Any], str | None]:
    """ensure_endpoint_up's resource arguments as runtime-config updates (None: the key is cleared), or why they are
    refused (nothing applied). Only what the call named is in the updates.

    - `whole_node=True` asks for whole nodes: it clears a slice's cores and memory unless `cores` comes with it (the
      validation then wants it equal to the node's count).
    - `whole_node=False` asks for a slice (the partition must give one).
    - `cores` / `memory_gb` without `whole_node` ask for a slice too: an earlier whole_node=True is cleared, so the
      partition decides (a whole-node partition refuses a slice).
    - `gpus=0` clears a GPU request."""
    named = {k: v for k, v in (("cores", cores), ("memory_gb", memory_gb), ("gpus", gpus), ("nodes", nodes),
                               ("whole_node", whole_node)) if v is not None}
    for arg, v in named.items():
        if arg == "whole_node":
            if not isinstance(v, bool):
                return {}, f"invalid whole_node {v!r}: true or false"
            continue
        if isinstance(v, bool) or not isinstance(v, int):
            return {}, f"invalid {arg} {v!r}: a whole number"
        low = 0 if arg == "gpus" else 1
        if not low <= v <= MAX_COUNT:
            return {}, f"invalid {arg} {v!r}: from {low} to {MAX_COUNT}"
    out: dict[str, Any] = {}
    if nodes is not None:
        out[NODES] = nodes
    if gpus is not None:
        out[GPUS] = gpus or None
    if whole_node is True:
        out[WHOLE] = True
        out[CORES] = cores
        if cores is None:
            out[MEM] = memory_gb
    elif whole_node is False:
        out[WHOLE] = False
    elif cores is not None or memory_gb is not None:
        out[WHOLE] = None   # a slice: the partition decides (an earlier whole_node=True does not stand)
    if cores is not None:
        out[CORES] = cores
    if memory_gb is not None:
        out[MEM] = memory_gb
    return out, None


def needs_new_template(request: Mapping[str, Any]) -> list[str]:
    """The size keys of a request (or the session's choice) only a template of this plugin renders (facility/remote.py
    `takes_resources`): cores, memory, GPUs, a slice (whole_node=False), more than one node. Whole nodes are what an
    older template asks for anyway."""
    asked = [k for k in (CORES, MEM, GPUS) if request.get(k) is not None]
    if request.get(WHOLE) is False:
        asked.append(WHOLE)
    if (_int(request.get(NODES)) or 1) > 1:
        asked.append(NODES)
    return asked


def size_key(uec: Mapping[str, Any]) -> tuple[Any, ...]:
    """The block's size as its runtime config asks for it, comparable: a different size is a different block."""
    return tuple(uec.get(k) for k in SIZE_KEYS)


def explicit_keys(uec: Mapping[str, Any]) -> set[str]:
    """The slice keys a runtime config carries."""
    return {k for k in SLICE_KEYS if uec.get(k) is not None}


# ---- what the block gets ---------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Size:
    """The block a runtime config asks for, read with what the partition hands out (`effective`)."""

    nodes: int = 1
    whole_node: bool | None = None   # True: whole nodes; False: a slice; None: not known (the facility's own default)
    cores: int | None = None         # per node; a whole node: the node's count, when known
    memory_gb: float | None = None   # per node
    memory_default: bool = False     # the partition's default memory per core (DefMemPerCPU): no --mem is sent
    gpus: int | None = None          # per node
    proposed: bool = False           # D3: hpc-bridge's default slice of a per-core partition (nothing was asked)
    by_partition: bool = False       # whole nodes because the partition hands out only whole nodes
    send_mem: int | None = None      # the memory the wire asks for (GB, a whole number), when any


def _int(v: Any) -> int | None:
    if isinstance(v, bool) or v is None:
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def default_slice_cores(shape: PartitionShape | None, nodes: int = 1) -> int:
    """D3's "a few cores": DEFAULT_SLICE_CORES, never more than half a node (so a slice is a slice) or than a per-job
    cap allows for `nodes` nodes; UNKNOWN_NODE_SLICE_CORES when the node's size is not known."""
    if shape is None or shape.cores is None:
        cores = UNKNOWN_NODE_SLICE_CORES
    else:
        cores = min(DEFAULT_SLICE_CORES, max(1, shape.cores // 2))
    if shape is not None and shape.max_cores_per_job:
        cores = min(cores, max(1, shape.max_cores_per_job // max(1, nodes)))
    return max(1, cores)


def _slice_memory(cores: int, shape: PartitionShape | None) -> tuple[float | None, bool, int | None]:
    """A slice's memory when none was asked: (GB, from the partition's default per core, the GB to send). With
    DefMemPerCPU known the scheduler gives cores × that on its own (nothing is sent: a larger --mem could make a site
    with MaxMemPerCPU charge more cores); else the node's memory per core, sent (a partition with no default per core
    may give a job the whole node's memory)."""
    if shape is None:
        return None, False, None
    if shape.mem_per_core_mb:
        return round(cores * shape.mem_per_core_mb / 1024, 1), True, None
    if shape.memory_gb and shape.cores:
        gb = max(1, math.floor(cores * shape.memory_gb / shape.cores))
        return float(gb), False, gb
    return None, False, None


def effective(uec: Mapping[str, Any], shape: PartitionShape | None, *, channel: Channel, takes: bool | None = True,
              profile_cpus: int | None = None) -> Size:
    """What the block `uec` asks for gets, with what is known of its partition (`shape`):
    - a facility endpoint (`mep`): exactly the keys its config carries (the facility's template decides the rest);
    - hpc-bridge's own template (`ssh`): whole nodes when asked (`exclusive` True) or when the partition hands out only
      whole nodes; a slice when asked (cores, memory, `exclusive` False) or — D3 — on a per-core partition, where a
      config that names no cores gets the default slice (`proposed`), only where the endpoint's template takes it
      (`takes`: one configured before this change does not); otherwise nothing is known: the facility's default (a
      registry entry's `exclusive`, parsl's whole node; PBS: the profile's cpus_per_node);
    - anything else (local dev): nodes only."""
    nodes = _int(uec.get(NODES)) or 1
    cores, gpus = _int(uec.get(CORES)), _int(uec.get(GPUS))
    mem = _int(uec.get(MEM))
    whole = uec.get(WHOLE)
    if channel == "other":
        return Size(nodes=nodes)
    gb: float | None
    if channel == "mep":
        wn = True if whole is True else False if (whole is False or cores is not None) else None
        gb, mem_default = (float(mem), False) if mem else (None, False)
        if mem is None and wn is False and cores and shape is not None and shape.mem_per_core_mb:
            gb, mem_default = round(cores * shape.mem_per_core_mb / 1024, 1), True  # what the scheduler gives it
        if wn is True and shape is not None:
            cores, gb = shape.cores or cores, shape.memory_gb
        return Size(nodes=nodes, whole_node=wn, cores=cores, memory_gb=gb, memory_default=mem_default, gpus=gpus,
                    send_mem=mem)
    kind = shape.kind if shape is not None else None
    if whole is True or kind == "whole_node":  # a whole-node partition decides, whatever the template sends
        node_cores = shape.cores if shape is not None and shape.cores else cores
        return Size(nodes=nodes, whole_node=True, cores=node_cores,
                    memory_gb=shape.memory_gb if shape is not None else None, gpus=gpus,
                    by_partition=whole is not True)
    asked = cores is not None or mem is not None or whole is False
    if asked or (kind == "per_core" and takes):
        proposed = cores is None and kind == "per_core" and takes is True
        if proposed:
            cores = default_slice_cores(shape, nodes)
        send: int | None
        if mem is not None:
            gb, mem_default, send = float(mem), False, mem
        elif cores is not None:
            gb, mem_default, send = _slice_memory(cores, shape)
        else:
            gb, mem_default, send = None, False, None
        if cores is None and profile_cpus:  # PBS: the profile's ncpus stands
            cores = profile_cpus
        return Size(nodes=nodes, whole_node=False, cores=cores, memory_gb=gb, memory_default=mem_default, gpus=gpus,
                    proposed=proposed, send_mem=send)
    return Size(nodes=nodes, whole_node=None, cores=profile_cpus, gpus=gpus)


# ---- the validation --------------------------------------------------------------------------------------------------

def _n(count: float, unit: str) -> str:
    c = int(count) if float(count).is_integer() else count
    return f"{c:g} {unit}" + ("" if c == 1 or unit == "GB" else "s")


def _per_core_names(partitions: Iterable[PartitionShape], exclude: str | None) -> str:
    names = [p.name for p in partitions if p.kind == "per_core" and p.name != exclude]
    return ", ".join(names[:6]) if names else ""


def refusal(uec: Mapping[str, Any], shape: PartitionShape | None, *, named: Iterable[str], word: str = "partition",
            partitions: Iterable[PartitionShape] = (), channel: Channel = "ssh",
            chosen: Iterable[str] | None = None) -> str | None:
    """Why the block `uec` asks for cannot be what the call means, or None — read against what discovery or the registry
    saw (`shape`): a slice on a partition that hands out whole nodes only, whole_node=True with fewer cores than a node,
    more cores, memory or GPUs than a node has, more than a QOS per-job cap allows (Expanse's `shared`: 127 cores per
    job on 128-core nodes — such a block pends forever). `named`: the size keys (and "partition") this call passed, so
    the refusal names what to change; a value nothing named is the session's earlier choice. `chosen`: the size keys the
    session chose (this call or an earlier one; default `named`) — a registry's `exclusive: false` default is no request
    for a slice, so a whole-node partition is not refused for it (the partition decides: whole nodes)."""
    named = set(named)
    chosen = named | set(chosen or ())
    nodes = _int(uec.get(NODES)) or 1
    cores, mem, gpus, whole = _int(uec.get(CORES)), _int(uec.get(MEM)), _int(uec.get(GPUS)), uec.get(WHOLE)
    where = f"{word} {shape.name!r}" if shape is not None else f"this {word}"

    def arg(key: str) -> str:
        return f"{KEY_ARGS[key]}={uec.get(key)!r}" + ("" if key in named else " (chosen earlier this session)")

    if whole is True and cores is not None:
        if shape is None or shape.cores is None:
            return (f"whole_node=True and {arg(CORES)} at once, and the node's core count on {where} is not known: "
                    "give one of them — whole_node=True for whole nodes, or cores= for a slice.")
        if cores < shape.cores:
            return (f"whole_node=True asks for every core of a node ({shape.cores} on {where}), and {arg(CORES)} for "
                    f"{cores}: give one of them — whole_node=True for the whole node, or cores= (without whole_node) "
                    "for a slice.")
    if shape is None:
        return None
    if shape.kind == "whole_node" and channel == "ssh":
        slice_asked = ((whole is False and WHOLE in chosen)
                       or (cores is not None and (shape.cores is None or cores < shape.cores)))
        if slice_asked:
            what = arg(WHOLE) if whole is False and WHOLE in chosen else arg(CORES)
            others = _per_core_names(partitions, shape.name)
            return (f"{where} hands out whole nodes only ({shape.why or 'its scheduler settings say so'}): {what} asks "
                    f"for a slice, which would hold and bill the whole node there. Ask for whole_node=True on it, or "
                    + (f"a per-core {word} ({others})" if others else f"a per-core {word}") + ".")
    if cores is not None and shape.cores and cores > shape.cores:
        return f"{arg(CORES)} is more than a node of {where} has ({shape.cores} cores)."
    if mem is not None and shape.memory_gb and mem > shape.memory_gb:
        return f"{arg(MEM)} is more than a node of {where} has ({shape.memory_gb:g} GB)."
    if gpus is not None and shape.gpus is not None and gpus > shape.gpus:
        return (f"{arg(GPUS)}: the nodes of {where} have no GPUs." if shape.gpus == 0
                else f"{arg(GPUS)} is more than a node of {where} has ({shape.gpus} GPUs).")
    job_cores = (shape.cores if whole is True or shape.kind == "whole_node" else cores)
    if shape.max_cores_per_job and job_cores and job_cores * nodes > shape.max_cores_per_job:
        asked = f"{nodes} node{'s' if nodes > 1 else ''} × {job_cores} cores" if nodes > 1 else f"{job_cores} cores"
        return (f"{asked} is over the QOS cap of {where}: at most {shape.max_cores_per_job} cores per job (a block "
                f"over it pends and never starts). Ask for fewer cores or nodes.")
    if shape.max_nodes_per_job and nodes > shape.max_nodes_per_job:
        return f"{arg(NODES)} is over the QOS cap of {where}: at most {shape.max_nodes_per_job} nodes per job."
    if shape.max_gpus_per_job and gpus and gpus * nodes > shape.max_gpus_per_job:
        return f"{gpus * nodes} GPUs is over the QOS cap of {where}: at most {shape.max_gpus_per_job} GPUs per job."
    return None


# ---- the wire: hpc-bridge's own templates ----------------------------------------------------------------------------

def _merge_select(base: str | None, mem: int | None, gpus: int | None) -> str | None:
    """A PBS chunk's resources (`select_options`) with the request's memory and GPUs in place of any the profile had."""
    pairs = [p for p in (base or "").split(":") if p]
    pairs = [p for p in pairs if not (mem is not None and p.startswith("mem=")) and
             not (gpus is not None and p.startswith("ngpus="))]
    if mem is not None:
        pairs.append(f"mem={mem}gb")
    if gpus is not None:
        pairs.append(f"ngpus={gpus}")
    return ":".join(pairs) or None


def _with_line(options: str | None, line: str) -> str:
    return f"{options.rstrip()}\n{line}" if options and options.strip() else line


def slurm_wire(uec: Mapping[str, Any], size: Size) -> dict[str, Any]:
    """The Slurm config hpc-bridge's template renders for `size`: `exclusive`, `cores_per_node`, `mem_per_node` (parsl's
    names) and the GPUs as hpc-bridge's own `#SBATCH --gpus-per-node` in scheduler_options. A size not known sends what
    the config had (the registry's `exclusive`, or none: parsl's whole node)."""
    out = {k: v for k, v in uec.items() if k not in (CORES, MEM, GPUS)}
    if size.whole_node is True:
        out[WHOLE] = True
    elif size.whole_node is False:
        out[WHOLE] = False
        if size.cores:
            out[CORES] = size.cores
        if size.send_mem:
            out[MEM] = size.send_mem
    if size.gpus:
        out["scheduler_options"] = _with_line(uec.get("scheduler_options"), f"#SBATCH --gpus-per-node={size.gpus}")
    return out


def pbs_wire(uec: Mapping[str, Any], size: Size, shape: PartitionShape | None) -> dict[str, Any]:
    """The PBS config hpc-bridge's template renders for `size`: ncpus (`cpus_per_node`), the chunk's memory and GPUs in
    `select_options`, and a `place` only where one is needed — a slice where the default place is exclusive asks to
    share (`shape.place`), a block of several nodes is scattered over distinct nodes (parsl's select is N chunks, which
    a free placement may pack onto one node), whole nodes of unknown size ask for them exclusively. Never place=excl
    for a slice."""
    out = {k: v for k, v in uec.items() if k not in (CORES, MEM, GPUS, WHOLE)}
    place = None
    if size.whole_node is True:
        if size.cores:
            out["cpus_per_node"] = size.cores
        else:
            place = "scatter:excl" if size.nodes > 1 else "excl"
    elif size.whole_node is False:
        if size.cores:
            out["cpus_per_node"] = size.cores
        share = shape.place if shape is not None and shape.place else None
        if size.nodes > 1:
            place = "scatter:shared" if share else "scatter"
        elif share:
            place = share
    select = _merge_select(uec.get("select_options"), size.send_mem if size.whole_node is False else None, size.gpus)
    if select is not None:
        out["select_options"] = select
    if place is not None:
        out["scheduler_options"] = _with_line(uec.get("scheduler_options"), f"#PBS -l place={place}")
    return out


# ---- the terms and the cost ------------------------------------------------------------------------------------------

def _hours(walltime_s: float) -> str:
    h = walltime_s / 3600
    return f"{h:.2f}".rstrip("0").rstrip(".") + " h"


def charge(size: Size, shape: PartitionShape | None, walltime_s: float) -> tuple[float, str] | None:
    """The most the block can be charged and how (`8 cores × 0.5 h`), where the partition's billing weights are known
    (Slurm's TRESBillingWeights, summed — its default; a site that bills the max of them charges less), else None."""
    if shape is None or not shape.billing or walltime_s <= 0:
        return None
    w = shape.billing
    cores = size.cores if size.whole_node is not True else (shape.cores or size.cores)
    mem = size.memory_gb if size.whole_node is not True else (shape.memory_gb or size.memory_gb)
    gpus = size.gpus or 0
    terms: list[tuple[float, float, str]] = []   # (amount, weight, unit)
    if w.get("cpu"):
        if not cores:
            return None
        terms.append((cores, w["cpu"], "core"))
    if w.get("mem_gb"):
        if mem is None:
            return None
        terms.append((mem, w["mem_gb"], "GB"))
    if w.get("gpu") and gpus:
        terms.append((gpus, w["gpu"], "GPU"))
    node_w = w.get("node") or 0.0
    if not terms and not node_w:
        return None
    per_node = sum(a * x for a, x, _u in terms) + node_w
    total = per_node * size.nodes * walltime_s / 3600
    parts = [_n(a, u) + ("" if x == 1 else f" × {x:g}") for a, x, u in terms] + ([f"{node_w:g} per node"] if node_w
                                                                                   else [])
    inner = parts[0] if len(parts) == 1 else "(" + " + ".join(parts) + ")"
    nodes = f"{size.nodes} nodes × " if size.nodes > 1 else ""
    return round(total, 2), f"{nodes}{inner} × {_hours(walltime_s)}"


def size_words(*, nodes: int | None, whole_node: bool | None, cores: int | None, memory_gb: float | None,
               memory_default: bool, gpus: int | None) -> str:
    """The block's size for a terms line: "1 node × 8 cores, 15 GB", "1 whole node (128 cores)", "2 nodes"."""
    if nodes is None:
        return "the facility's default size"
    n = f"{nodes} node{'' if nodes == 1 else 's'}"
    gpu = f", {_n(gpus, 'GPU')}" + (" each" if nodes > 1 else "") if gpus else ""
    if whole_node is True:
        each = " each" if nodes > 1 else ""
        node = f"{nodes} whole node{'' if nodes == 1 else 's'}"
        return node + (f" ({cores} cores{each})" if cores else "") + gpu
    if whole_node is False:
        if cores is None and memory_gb is None:
            return f"{n}, a slice of the scheduler's default size (one core unless the site says otherwise)" + gpu
        bits = [_n(cores, "core")] if cores else []
        if memory_gb is not None:
            gb = f"{memory_gb:.0f} GB" if memory_gb >= 10 else f"{memory_gb:g} GB"
            bits.append(gb + (" (the partition's default per core)" if memory_default else ""))
        return f"{n} × " + ", ".join(bits) + gpu
    return n + gpu


def multinode_note(nodes: int | None, compute_only: bool) -> str:
    """D2: what N > 1 nodes means — one shell on the first node, work across them with srun / mpiexec, and nodes not
    used still bill. A facility endpoint's launcher is its own, so there the shell is on one of them."""
    if not nodes or nodes <= 1:
        return ""
    where = ("one shell, on one of the nodes (the facility's template places its worker)" if compute_only else
             "one shell, on the first node")
    return (f"{nodes} nodes: {where} — run work across them with srun (or mpiexec) from it, as from an salloc "
            "shell; nodes you do not use still bill. ")


# ---- discovery: what a partition hands out ---------------------------------------------------------------------------

_GRES_GPU = re.compile(r"gpu(?::[A-Za-z0-9_.-]+)?:(\d+)", re.I)
_LEAD = re.compile(r"^\s*(\d+)")
_WHOLE_BY_NAME = re.compile(r"whole|exclusive|fullnode", re.I)


def _lead_int(v: str | None) -> int | None:
    m = _LEAD.match(v or "")
    return int(m.group(1)) if m else None


def _gres_gpus(gres: str) -> int:
    return sum(int(m.group(1)) for m in _GRES_GPU.finditer(gres or ""))


def _tokens(line: str) -> dict[str, str]:
    """`scontrol show partition -o`'s key=value pairs (a value may itself hold '=': TRESBillingWeights=CPU=1.0,…)."""
    out: dict[str, str] = {}
    for tok in line.split():
        k, sep, v = tok.partition("=")
        if sep and k and k not in out:
            out[k] = v
    return out


def _tres(spec: str | None) -> dict[str, str]:
    """A TRES list (`cpu=127,gres/gpu=12,node=16`) as {name: value}."""
    out: dict[str, str] = {}
    for part in (spec or "").split(","):
        k, sep, v = part.strip().partition("=")
        if sep and k:
            out[k.strip().lower()] = v.strip()
    return out


def _mem_weight(v: str) -> float | None:
    """A TRESBillingWeights memory weight, per GB: `0.25G` per GB, `0.25` or `0.25M` per MB, `T` per TB."""
    m = re.match(r"^\s*([0-9.]+)\s*([KMGT]?)\s*$", v, re.I)
    if not m:
        return None
    try:
        x = float(m.group(1))
    except ValueError:
        return None
    unit = m.group(2).upper() or "M"
    return {"K": x * 1024 * 1024, "M": x * 1024, "G": x, "T": x / 1024}[unit]


def billing_weights(spec: str | None) -> dict[str, float] | None:
    """Slurm TRESBillingWeights (`CPU=1.0,Mem=0.25G,GRES/gpu=16`) as PartitionShape.billing, or None when unset."""
    out: dict[str, float] = {}
    for k, v in _tres(spec).items():
        try:
            if k == "cpu":
                out["cpu"] = float(v)
            elif k == "mem":
                if (w := _mem_weight(v)) is not None:
                    out["mem_gb"] = w
            elif k == "gres/gpu" or (k.startswith("gres/gpu:") and "gpu" not in out):
                out["gpu"] = float(v)  # the untyped weight wins over a typed one (gres/gpu:a100=…)
            elif k == "node":
                out["node"] = float(v)
        except ValueError:
            continue
    return out or None


def _cap(*specs: str | None, key: str) -> int | None:
    vals = [n for s in specs if (n := _lead_int(_tres(s).get(key))) is not None]
    return min(vals) if vals else None


def classify_slurm(*, select_type: str | None, select_params: str | None, shapes: Iterable[str],
                   details: Iterable[str], qos: Iterable[str]) -> list[PartitionShape]:
    """Each Slurm partition the probe saw, as a PartitionShape:
    - `shapes`: `sinfo -h -o '%P|%c|%m|%G'` lines (cores and memory in MB per node, GRES), one per node config — the
      smallest node of a partition is its shape;
    - `details`: `scontrol show partition -o` lines (OverSubscribe, ExclusiveUser, DefMemPerCPU, TRESBillingWeights,
      QoS);
    - `qos`: `sacctmgr -nP show qos format=Name,MaxTRES,MaxTRESPU` lines — the partition QOS's per-job and per-user caps
      (a job can exceed neither).
    Whole-node: SelectType=select/linear (the whole cluster: only whole nodes are ever allocated),
    OverSubscribe=EXCLUSIVE, ExclusiveUser=YES, or — named as such, the weakest evidence — a partition called
    whole/exclusive (Anvil's `wholenode`, whose OverSubscribe says NO: a submit plugin makes it whole). Per-core:
    select/cons_tres or cons_res (or a DefMemPerCPU, which only a per-core selector reads) on a partition that is none
    of those. Else not known."""
    linear = (select_type or "").strip().lower() == "select/linear"
    cons = bool(re.search(r"cons_(tres|res)", select_type or "", re.I))
    params = (select_params or "").strip()
    by_name: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for line in shapes:
        cols = line.split("|")
        name = cols[0].strip().rstrip("*")
        if not name:
            continue
        cores = _lead_int(cols[1]) if len(cols) > 1 else None
        mem_mb = _lead_int(cols[2]) if len(cols) > 2 else None
        gpus = _gres_gpus(cols[3]) if len(cols) > 3 else None
        row = by_name.setdefault(name, {})
        if name not in order:
            order.append(name)
        for k, v in (("cores", cores), ("mem_mb", mem_mb), ("gpus", gpus)):
            if v is not None:
                row[k] = v if k not in row else min(row[k], v)
    detail = {}
    for line in details:
        t = _tokens(line)
        name = t.get("PartitionName", "").strip()
        if name:
            detail[name] = t
            if name not in order:
                order.append(name)
    qos_rows: dict[str, tuple[str | None, str | None]] = {}
    for line in qos:
        cols = [c.strip() for c in line.split("|")]
        if cols and cols[0]:
            qos_rows[cols[0]] = (cols[1] if len(cols) > 1 else None, cols[2] if len(cols) > 2 else None)
    out: list[PartitionShape] = []
    for name in order:
        row, t = by_name.get(name, {}), detail.get(name, {})
        over = t.get("OverSubscribe", "").upper()
        kind: Literal["per_core", "whole_node"] | None = None
        why: str | None = None
        if linear:
            kind, why = "whole_node", "SelectType=select/linear: only whole nodes are allocated"
        elif over.startswith("EXCLUSIVE"):
            kind, why = "whole_node", "OverSubscribe=EXCLUSIVE"
        elif t.get("ExclusiveUser", "").upper() == "YES":
            kind, why = "whole_node", "ExclusiveUser=YES"
        elif _WHOLE_BY_NAME.search(name):
            kind, why = "whole_node", "its name; no scheduler setting showed it — confirm with the site"
        elif t and (cons or _lead_int(t.get("DefMemPerCPU"))):
            kind = "per_core"
            why = (f"SelectType={select_type.strip()}" if cons and select_type else "DefMemPerCPU set") + (
                f" ({params})" if params else "") + (f", OverSubscribe={over}" if over else "")
        def_mem = _lead_int(t.get("DefMemPerCPU"))
        q = t.get("QoS", "")
        caps = qos_rows.get(q, (None, None)) if q and q.upper() != "N/A" else (None, None)
        try:
            out.append(PartitionShape(
                name=name, kind=kind, why=why, cores=row.get("cores"),
                memory_gb=round(row["mem_mb"] / 1024, 1) if row.get("mem_mb") else None,
                gpus=row.get("gpus"), mem_per_core_mb=def_mem,
                max_cores_per_job=_cap(*caps, key="cpu"), max_nodes_per_job=_cap(*caps, key="node"),
                max_gpus_per_job=_cap(*caps, key="gres/gpu"),
                billing=billing_weights(t.get("TRESBillingWeights")),
            ))
        except ValueError:
            continue
    return out


def _pbs_mem_gb(v: str | None) -> float | None:
    m = re.match(r"^\s*(\d+(?:\.\d+)?)\s*([kmgtp]?)b?\s*$", (v or "").lower())
    if not m:
        return None
    x = float(m.group(1))
    return round(x * {"": 1 / 1024 ** 3, "k": 1 / 1024 ** 2, "m": 1 / 1024, "g": 1.0, "t": 1024.0,
                      "p": 1024.0 ** 2}[m.group(2)], 1)


def classify_pbs(*, queues: Iterable[str], nodes: Iterable[str], queue_attrs: Iterable[str],
                 server_attrs: Iterable[str]) -> list[PartitionShape]:
    """Each PBS queue the probe saw, as a PartitionShape:
    - `nodes`: `pbsnodes -a` rows as `name|ncpus|mem|ngpus|sharing|queue` (resources_available.*, the node's `sharing`
      and the queue it is tied to, if any) — the smallest node serving a queue is its shape;
    - `queue_attrs`: `qstat -Qf` rows as `queue|attribute|value` (resources_default.place, resources_max.ncpus/nodect/
      ngpus); `server_attrs`: `qstat -Bf` rows as `attribute|value` (the server's resources_default.place).
    Whole-node: every node serving the queue is `force_excl` / `force_exclhost`. Per-core otherwise — and where the
    queue's (or server's) default place is exclusive, or the nodes are `default_excl`, a slice must ask to share
    (`place`: free:shared). Not known without node rows."""
    node_rows = []
    for line in nodes:
        cols = [c.strip() for c in line.split("|")]
        if len(cols) >= 5 and cols[0]:
            node_rows.append(cols + [""] * (6 - len(cols)))
    qattrs: dict[str, dict[str, str]] = {}
    for line in queue_attrs:
        cols = [c.strip() for c in line.split("|", 2)]
        if len(cols) == 3 and cols[0]:
            qattrs.setdefault(cols[0], {})[cols[1]] = cols[2]
    sattrs = {}
    for line in server_attrs:
        cols = [c.strip() for c in line.split("|", 1)]
        if len(cols) == 2:
            sattrs[cols[0]] = cols[1]
    out: list[PartitionShape] = []
    for q in dict.fromkeys(x.strip() for x in queues if x.strip()):
        tied = [r for r in node_rows if r[5] == q]
        serving = tied or [r for r in node_rows if not r[5]]
        attrs = qattrs.get(q, {})
        place = (attrs.get("resources_default.place") or sattrs.get("resources_default.place") or "").lower()
        kind: Literal["per_core", "whole_node"] | None = None
        why: str | None = None
        need_place: str | None = None
        if serving:
            sharing = {r[4].lower() for r in serving if r[4]}
            if sharing and sharing <= {"force_excl", "force_exclhost"}:
                kind, why = "whole_node", f"node sharing={'/'.join(sorted(sharing))}"
            else:
                kind = "per_core"
                why = f"node sharing={'/'.join(sorted(sharing)) or 'default_shared'}"
                if "excl" in place or sharing & {"default_excl", "default_exclhost"}:
                    need_place = "free:shared"
                    why += f", default place={place or 'excl (by the nodes)'}: a slice asks place=shared"
        ncpus = [n for r in serving if (n := _lead_int(r[1])) is not None]
        mems = [g for r in serving if (g := _pbs_mem_gb(r[2])) is not None]
        gpus = [n for r in serving if (n := _lead_int(r[3])) is not None]
        try:
            out.append(PartitionShape(
                name=q, kind=kind, why=why, cores=min(ncpus) if ncpus else None,
                memory_gb=min(mems) if mems else None, gpus=min(gpus) if gpus else (0 if serving else None),
                max_cores_per_job=_lead_int(attrs.get("resources_max.ncpus")),
                max_nodes_per_job=_lead_int(attrs.get("resources_max.nodect")),
                max_gpus_per_job=_lead_int(attrs.get("resources_max.ngpus")),
                place=need_place,
            ))
        except ValueError:
            continue
    return out


def summary(partitions: Iterable[PartitionShape]) -> str:
    """One line for a proposal's notes: which partitions give slices, which whole nodes."""
    per = [p for p in partitions if p.kind == "per_core"]
    whole = [p for p in partitions if p.kind == "whole_node"]
    unknown = [p for p in partitions if p.kind is None]

    def shape(p: PartitionShape) -> str:
        bits = [f"{p.cores} cores" if p.cores else "", f"{p.memory_gb:g} GB" if p.memory_gb else "",
                f"{p.gpus} GPUs" if p.gpus else ""]
        caps = f", ≤{p.max_cores_per_job} cores/job" if p.max_cores_per_job else ""
        inner = "/".join(b for b in bits if b)
        return p.name + (f" ({inner}{caps})" if inner or caps else "")

    parts = []
    if per:
        parts.append("per-core (a block is a slice): " + ", ".join(shape(p) for p in per[:8]))
    if whole:
        parts.append("whole-node: " + ", ".join(f"{p.name} ({p.why})" for p in whole[:6]))
    if unknown:
        parts.append("not known: " + ", ".join(p.name for p in unknown[:8]))
    return ("partitions: " + "; ".join(parts) + ". On a per-core partition the first block's terms propose a slice of "
            "a few cores; the user confirms or changes it (ensure_endpoint_up cores=/memory_gb=/gpus=/nodes=/"
            "whole_node=).") if parts else ""


def shape_of(partitions: Iterable[PartitionShape] | None, name: Any) -> PartitionShape | None:
    """The partition `name`'s shape, if discovery or the registry saw it."""
    if not partitions or not isinstance(name, str):
        return None
    return next((p for p in partitions if p.name == name), None)

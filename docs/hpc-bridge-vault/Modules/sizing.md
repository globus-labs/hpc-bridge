# sizing.py

> [!abstract] Role
> Batch row sizing, pure (no I/O): what a block IS, read from inside it (`MEASURE_SH` → `parse_measure`), how many rows of a size it runs at once (`fit`), the block derived from a batch's rows before any spend question (`derive_block`), and the size a pilot's own use proposes (`pilot_proposal`). Design: [[Batch and swarm execution]], "Batch row sizing".

## What it does

- **`cores` are scheduler CPUs**, as `-c` counts them: one hardware thread where SMT is on, never rounded up to whole cores; physical cores are reported beside (`physical_cores`, `threads_per_core`).
- **`MEASURE_SH`** prints the raw facts as `HPCB_M <key> …` lines (files read under `$HPCB_SYSROOT` when set, a read-only seam for the tests): nproc, the affinity mask, the online list, MemTotal, every `/proc/self/cgroup` line and — walking up the tree — cgroup v2 `memory.max` / `memory.current` / `cpu.max` / `cpuset.cpus.effective` / `memory.events` oom_kill, or v1's `memory.limit_in_bytes` / `hierarchical_memory_limit` / `usage_in_bytes` / oom_kill, `cpuset.cpus`, `cfs_quota/period`; SMT siblings (`thread_siblings_list`); the Slurm, PBS and GPU env (`SLURM_CPUS_ON_NODE`, `SLURM_MEM_PER_NODE` / `_PER_CPU`, `NCPUS`, `CUDA_VISIBLE_DEVICES`, `ROCR`/`HIP_VISIBLE_DEVICES`, `ZE_AFFINITY_MASK`, `SLURM_JOB_GPUS`…, `PARSL_WORKER_COUNT`); PBS `Resource_List.mem` and nodes; `nvidia-smi -L`; `/dev/nvidia*`. The [[runner]]'s canary runs it last.
- **`parse_measure`** decides (`BlockMeasure`): CPUs = the least of the mask, the cpuset, the cpu.max quota, `SLURM_CPUS_ON_NODE` and `NCPUS` (the scheduler's word names a tie: on the fake cluster's task/none every host CPU is in the mask and Slurm's 4 decide), `cpu_ids` in sibling order truncated to that count; the memory limit = the least of the lowest `memory.max` up the tree, v1's hierarchical limit (its "no limit" value, ≥ 2⁶⁰, ignored), the Slurm and PBS figures and MemTotal; idle = `memory.current` at the limit's level; GPUs from the env (`NoDevFiles` or empty: none), then nvidia-smi's count, then `/dev`.
- **`fit(measure, sizing)`**: slots = min(concurrency, ⌊C ÷ k⌋, ⌊(M − idle) ÷ m⌋, ⌊G ÷ g⌋, W − 1, `WIDTH_CAP`); `bound_by` names the smallest (ties: concurrency, workers, GPUs, CPUs, memory, width — memory last, which a default share divides by the slots). With no memory asked, each row gets the block's share less the pool's idle use. A row that cannot fit at all is `why_not`, with the numbers and the source that bounded them.
- **`derive_block(sizing, shape=, workers_fixed=)`**: N·k + 1 CPUs, ⌈N·m + the pool's own (`POOL_BASE_MB` + `WORKER_MB` × workers)⌉ GB, N·g GPUs, W = N + 1, one node; refused (`SizingRefusal`) past `WIDTH_CAP`, a node of the partition (naming what fits and phase 3's campaign pools), a per-job cap, or a facility's fixed workers.
- **`candidates`** — what one node gives each row size (k = 1, 2, 4…): `needs_sizing`'s `facts`.
- **`pilot_proposal`** — cores ⌈CPU-s ÷ wall − 0.1⌉ (1 to the block's CPUs), memory 1.25 × the peak RSS rounded up to a quarter GB (at least one), the largest over the rows; the concurrency the block then fits. Also used for a row killed at a DEFAULT memory ([[batch]]).
- **`over_size`** — `cores` (unpinned: CPU-s ÷ wall past cores × 1.25; pinned, which cannot show it: more threads than cores) and `memory` (killed at its limit, or a peak within 90% of it).

> [!warning] Memory is sampled once a second
> A spike shorter than the sample can still meet the site's own OOM killer first — a cgroup's, which may take a NEIGHBOURING row; Slurm's OverMemoryKill, the whole pool's step; PBS's `$enforce mem`, the job. Each result carries the block cgroup's `oom_kill` delta over the row's life, so such a kill is seen.

> [!warning] The width cap is a stop-gap
> `WIDTH_CAP` (32 rows at once per node) bounds each row's own 1 s loop (a `ps` sample, `date`, the marker check: 11 processes and 2.5% of a CPU per row, measured at 64 rows). A per-node watcher and one stream helper per row are the follow-up that lifts it.

## See also
[[batch]] · [[ledger]] · [[runner]] · [[resources]] · [[Batch and swarm execution]]

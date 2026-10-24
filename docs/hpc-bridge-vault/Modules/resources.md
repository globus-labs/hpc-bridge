# resources

> [!abstract] Role
> A block's size, pure (no I/O): what ensure_endpoint_up's `cores` / `memory_gb` / `gpus` / `nodes` / `whole_node` become in the runtime config, what a partition hands out (classified from the login-node probe's lines), what a block of a config gets there (the default slice), what is refused before any submit, what the wire says on Slurm and PBS, and the words and the cost of the terms. Design: [[Resource requests]].

## What it does

- **The request.** `requested` (`resources.py:65`) turns the tool's arguments into runtime-config updates under hpc-bridge's keys (`SIZE_KEYS`: `nodes_per_block`, `cores_per_node`, `mem_per_node`, `gpus_per_node`, `exclusive`; None clears one) or a refusal for a malformed value. `size_key` makes the size comparable for a switch ([[warmth]] `_block_key`); `needs_new_template` names the keys only this plugin's templates render.
- **What a partition hands out.** `classify_slurm` (`:510`) and `classify_pbs` (`:600`) read the probe's `PSHAPE` / `PDETAIL` / `QOS` / `SELECT_*` and `PNODE` / `PQUEUE` / `PSERVER` lines into [[models]] `PartitionShape`s: per-core or whole-node with the evidence (`why`), the node's cores, memory and GPUs, per-job caps, Slurm billing weights (`billing_weights`), and for PBS the `place` a slice needs. `summary` is the proposal's note.
- **What a block gets.** `effective` (`:180`) returns a `Size`: on hpc-bridge's own template (`ssh`) whole nodes when asked or when the partition gives only those, a slice when asked, or — D3, only where the endpoint's template takes it — the proposed slice of a per-core partition (`default_slice_cores`; memory from DefMemPerCPU, not sent, else the node's per core, sent); otherwise the facility's default as before. On a facility endpoint (`mep`) exactly the keys it receives.
- **The validation.** `refusal` (`:243`): a slice of a whole-node partition (a registry's `exclusive: false` default is no request for one: `chosen`), `whole_node=True` with fewer cores than a node, more than a node has, over a QOS per-job cap or node cap.
- **The wire.** `slurm_wire` (`:318`: `exclusive`, parsl's `cores_per_node` / `mem_per_node`, GPUs as `#SBATCH --gpus-per-node` in scheduler_options) and `pbs_wire` (`:336`: `cpus_per_node`, `mem`/`ngpus` merged into `select_options`, a `#PBS -l place=` only where needed), called by [[facility-remote]] `SlurmFacility.dispatch_uec`.
- **The terms.** `charge` (`:372`: the most a block can cost, "8 cores × 0.5 h"), `size_words`, `multinode_note` (D2), read by [[notices]] `_block_terms` / `_terms_line`.

> [!warning] The size is mapped only at the wire
> As with a facility's `key_map`, the runtime config keeps hpc-bridge's names and the session's explicit choice; the default slice and each scheduler's rendering happen in `dispatch_uec`, so a partition switch re-derives the default and a whole-node partition overrides a slice default. Writing the D3 slice into the runtime config would make it a "choice" that survives a switch onto a whole-node partition.

> [!warning] What parsl does (read from its source, not guessed)
> SrunLauncher runs `srun --ntasks <nodes>` — a worker pool on every node, as a step holding its CPUs; SingleNodeLauncher starts `tasks_per_node × nodes` pools, all on the first node; SimpleLauncher runs the pool once in the batch script. So N > 1 nodes take SimpleLauncher on both templates (D2). `cores_per_node` renders `--cpus-per-task` with no trailing newline (the template's blank line follows it); `mem_per_node` must be an int.

## See also
[[Resource requests]] · [[facility-remote]] · [[facility-mep]] · [[discovery]] · [[notices]] · [[warmth]] · [[models]]

# Resource requests: slices of a node, and what N nodes means

> [!abstract] In one line
> A block asks for what the work needs — cores, memory and GPUs per node, and a node count — instead of always whole nodes; N nodes are one shell over the allocation, like `salloc`. Decided 2026-10-24 (maintainer), **built** on branch `feat/resource-requests` (module [[resources]]); the live checks below are still to run.

## Why

On the live Anvil run (2026-10-24), a bring-your-own Slurm block on the per-core `shared` partition asked for an **exclusive whole node** (`#SBATCH --exclusive`, parsl's default). It queued for more than a day, waiting for a completely idle node, and it would have billed all 128 cores. Until this change a block was always whole nodes: Slurm used exclusive unless a registry entry set `exclusive: false` (as Expanse does after its own `shared` trap), PBS asked for `select=N:ncpus=<cpus_per_node>`, and nobody could ask for "8 cores, 32 GB, 1 GPU".

## Decisions (maintainer, 2026-10-24)

- **D1. Ask for what you need.** A block request names cores, memory and GPUs per node, plus the node count, as well as the partition, account and walltime it has today. A whole node only when asked, or when the partition hands out only whole nodes.
- **D2. N nodes means one shell over the whole allocation, like `salloc`.** hpc-bridge's worker runs on the first node only. The user's commands use `srun` / `mpiexec` across the N nodes. The spend question says plainly that nodes you don't use still bill.
- **D3. The default asks for a slice.** On a per-core partition with nothing requested, the spend question proposes a modest slice (a few cores and their memory) for the user to confirm or change. On a whole-node partition, it is a whole node.

## The request (built)

`ensure_endpoint_up` takes `cores`, `memory_gb`, `gpus` (per node), `nodes` and `whole_node` ([[server]] `_ensure_endpoint_up`; normalised by `resources.requested`).
- They live in the compute shape's runtime config under hpc-bridge's keys (`nodes_per_block`, `cores_per_node`, `mem_per_node`, `gpus_per_node`, `exclusive`) and persist for the session in `AppCtx.compute_choice` (a key set to `None` is one the session cleared: `whole_node=True` clears a slice's cores and memory; `cores=` without `whole_node` clears an earlier `whole_node=True`).
- A change is a term switch ([[warmth]] `_apply_resources` → `_repoint`, which now takes several keys): spend is asked again, the old block is handled as a partition switch handles it. `ShapeRuntime.switched_terms` / `reap_terms` are `(partition, account, size)` (`warmth._block_key`, `resources.size_key`), so a switch back to the same size is "back to the terms it covered"; the wording says "partition, account or size" only when the size moved. `rejected_size` keeps a rejection's size, so a new size is not "the same terms again".
- `whole_node=True` with an explicit `cores` below the node's count is refused with the reason.
- Values are validated against what discovery or the registry saw (`resources.refusal`, via [[notices]] `_size_refusal`, before the lock and before anything changes): cores, memory and GPUs per node, a QOS per-job cap (Expanse's 127 CPUs per job), a node cap; a slice of a whole-node partition. A refusal answers the shape's standing state (`_dropped_term_status`), never a `down`.

## How each path maps it (built, verified in parsl's own source)

Read from the locked parsl 2026.8.10 and the 2026.10.x the endpoint venv takes (identical here); globus-compute-endpoint 4.18 passes every provider key to the class, dropping None.

| Path | Slice | Whole node | N > 1 nodes |
|---|---|---|---|
| Slurm over SSH | `exclusive: false`; parsl `cores_per_node` → `#SBATCH --cpus-per-task=C` (HTEX submits with tasks_per_node=1, so one task per node) and `PARSL_CORES`; `mem_per_node` (int GB) → `#SBATCH --mem=Mg`; GPUs as hpc-bridge's own `#SBATCH --gpus-per-node=G` in scheduler_options (parsl's `gpus_per_node` exists only from 2026.04) | `exclusive: true` | `nodes_per_block: N` and **SimpleLauncher**: the worker pool runs in the batch script on the first node. SrunLauncher (kept for one node) runs `srun --ntasks N` — a pool on every node, as a step holding its CPUs |
| PBS over SSH | `select=N:ncpus=C:mem=Mgb:ngpus=G` via `cpus_per_node` and a merged `select_options`; a `place` only where needed: `free:shared` where the queue's or server's default place is exclusive or the nodes are `default_excl`; never `place=excl` for a slice | `ncpus` = the node's count (`place=excl` only when that count is unknown) | **SimpleLauncher** too — the spec assumed SingleNodeLauncher already started one manager, but it starts `tasks_per_node × N` pools, all on the first node; and `place=scatter` (`scatter:shared`) so the N chunks land on N distinct nodes |
| Facility endpoint (MEP) | only through the facility template's own keys, renamed by `compute.key_map` (now also `cores_per_node`, `mem_per_node`, `gpus_per_node`, `exclusive`) | as the template allows | as the template allows (the facility's launcher places its worker: the terms say "one of the nodes") |

The template change ([[facility-remote]]): the Slurm compute branch renders `cores_per_node` / `mem_per_node`, both templates pick the launcher from `nodes_per_block`; that launcher line is the marker `takes_resources(template)`. An endpoint configured before it gets no default slice, and a size it cannot render is refused (teardown and connect again); one asked before the template was known (a startup pin) is refused at the bring-up, before any check (`warmth._stale_size`). On a facility endpoint a size key is taken only when its strict schema lists it, or (an open schema like Delta's, which lists only `worker_init`/`endpoint_setup`) its published template renders it or the registry entry sends it ([[facility-mep]] `takes_size_key`); otherwise "this facility's endpoint sets the block size: …". Never silently dropped.

## Discovery and the proposal (built)

- **Slurm** ([[discovery]] `_PROBE`, parsed by `resources.classify_slurm`): `scontrol show config` (SelectType, SelectTypeParameters), `scontrol show partition -o` (OverSubscribe, ExclusiveUser, DefMemPerCPU, TRESBillingWeights, QoS), `sacctmgr -nP show qos format=Name,MaxTRES,MaxTRESPU` (per-job and per-user caps — a job can exceed neither), `sinfo -o '%P|%c|%m|%G'` (cores, memory, GPUs per node; the smallest node is the partition's shape).
- **Classification:** whole-node when `SelectType=select/linear` (every partition), `OverSubscribe=EXCLUSIVE`, `ExclusiveUser=YES`, or — the weakest evidence, said as such — a partition named whole/exclusive (Anvil's `wholenode` reads `OverSubscribe=NO`: a submit rule makes it whole). Per-core when select/cons_tres or cons_res (or a DefMemPerCPU) and none of those. Else not known (no default slice there: the facility's default stands).
- **PBS** (`resources.classify_pbs`): `pbsnodes -a` (ncpus, mem, ngpus, `sharing`, the queue a node is tied to), `qstat -Qf` / `qstat -Bf` (resources_default.place, resources_max.ncpus/nodect/ngpus). Whole-node when every node serving the queue is `force_excl`/`force_exclhost`.
- **The proposal** carries `FacilityDetails.partitions` (kept server-side like env_setup: a `details=` that leaves it out still has it) and a one-line note; a **registry entry** carries `defaults.partitions` (read leniently, checked strictly at ingest: `partitions_raw_problems`). The Expanse seed (shared per-core, 127-CPU cap; compute whole-node) and the Anvil MEP seed (shared/debug per-core with DefMemPerCPU 1896 and CPU=1.0 billing; wholenode whole-node) carry theirs — **a curator must re-ingest both**.
- The live Anvil probe output is the fixture `tests/resource_fixtures/anvil-probe.txt` (debug, shared, standard, wholenode, wide, gpu; SelectType reconstructed — see its header); Expanse's is constructed from the repo's own record.

## The spend question (built)

`BlockTerms` ([[models]]) gain `whole_node`, `cores_per_node`, `memory_gb`, `memory_default`, `gpus_per_node`, `size_source` (`this_call` / `earlier_call` / `proposed` / `partition` / `facility`), `max_charge` and `charge_basis` (Slurm TRESBillingWeights, summed). The terms line ([[notices]] `_terms_line`, `_size_sentence`):
- a slice: "1 node × 8 cores, 15 GB (the partition's default per core) for up to 00:30:00 (at most 0.5 node-hours; at most 4 SU: 8 cores × 0.5 h)";
- a whole node: "1 whole node (128 cores)", and on a whole-node partition "Whole nodes: partition 'wholenode' hands out only whole nodes (…)";
- N > 1 nodes: "2 nodes: one shell, on the first node — run work across them with srun (or mpiexec) from it, as from an salloc shell; nodes you do not use still bill".
- D3: the proposed slice (`resources.default_slice_cores`: 8 cores, never more than half a node or than a per-job cap allows; memory from DefMemPerCPU — not sent, so a site with MaxMemPerCPU charges no extra core — else the node's memory per core, sent) says it is a proposal; connect's question asks "whether that slice is enough"; the spend-floor guidance says to send `cores=/memory_gb=/gpus=/nodes=` in the same call.

## Guidance (SKILL.md, built)

Domain facts only: ask what the work needs; a slice versus a whole node (cost, wait); several nodes are `srun`/`mpiexec` from one shell; unused nodes bill.

## Tests (built)

- **Unit:** `tests/test_resources.py` (classification from the Anvil and Expanse fixtures, select/linear, ExclusiveUser, billing and caps, PBS sharing/place; D3; validation; the wire; the cost and the words), `tests/test_resource_templates.py` (the templates as the manager renders them; `takes_resources`; adopt; and — where parsl is importable, as in CI's `--extra integration` — the submit script parsl itself writes), `tests/test_resource_requests.py` (through `ensure_endpoint_up`: proposal, persistence, switch and back, refusals, old templates, N nodes, PBS, connect's question, the BYO proposal's partitions, MEP strict/open schemas and key_map, the Expanse seed).
- **Fake cluster:** profile `slices` (`default` + `CR_Core_Memory`, a per-core `shared`, a whole-node `whole`). Scripted scenarios `slice_request` (2 cores, 2 GB; `world:slice_allocated` reads the pilot's sacct AllocCPUS/ReqMem/NNodes by its `uep.<eid>` SubmitLine; a slice of `whole` refused) and `multinode_allocation` (2 nodes; `srun -N2 hostname` names two nodes; per node, worker pools of the user: one on the first, none on the second; `world:two_nodes_allocated`). Hermetic: `agentic/harness/test_slice_scenarios.py`. Both are in the sweep's `excluded()` until their first live pass.

## Not built / follow-ups

- **After submit:** the pilot probe does not yet compare what was allocated (sacct AllocCPUS/ReqMem/AllocTRES, PBS exec_vnode) with what was asked. It would catch a partition the probe calls per-core whose site rule gives whole nodes anyway (Anvil's `standard`, if it is one).
- A registry SSH entry without `partitions` gets no probe of them: the terms keep the facility's default until a curator adds them (a lazy login-shape probe is possible).
- Parsl's own `gpus_per_node` once every endpoint venv is past parsl 2026.04 (hpc-bridge's are: gce 4.16+ floors parsl at 2026.7.27).
- Live, with the maintainer: Anvil `shared` with a slice (it should start quickly); a 2-node allocation on a per-core partition; the two fake scenarios.

## See also
[[resources]] · [[Resource shapes & the spend floor]] · [[facility-remote]] · [[facility-mep]] · [[discovery]] · [[notices]] · [[warmth]] · [[models]]

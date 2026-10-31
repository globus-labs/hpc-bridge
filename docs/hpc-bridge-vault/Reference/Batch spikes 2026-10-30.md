# Phase-0 spike results (2026-10-30)

> [!abstract] In one line
> The phase-0 spikes behind [[Batch and swarm execution]], run 2026-10-30: S1, S2, S4, S5 and S8 on a fake cluster, S7 read-only against the real Delta and Anvil MEPs. They hold the evidence the design was revised with; § numbers and D1–D6 refer to that design.

_Run on my own fake cluster, compose project `hpcb-spike`: SSH on 2232, profile `default` for Slurm, `pbs` for OpenPBS. The `hpcb-fake` stack was never touched. Every spike used the harness test identity, stood up its own personal endpoint `hpcb-spike-<spike>-<run>` with hpc-bridge's SSH bootstrap, measured with the raw SDK (globus-compute-sdk 4.18.0; endpoint 4.18.0), and deleted the endpoint in a `finally`. S7 was read-only against the real Delta and Anvil MEPs (4 GETs). The raw events were kept outside the repo._

## Summary

| Spike | Outcome | One line |
|---|---|---|
| **S1** | **H1 PASS, H2 CONFIRMED** | Acked results stay readable by REST from another process, and re-reading is not destructive; two processes on one task group swallow each other's results, and REST recovers them |
| **S2** | **PASS to +57 min; +180 min NOT RUN** | Unfetched results were still readable at +34 and +57 min after completion, by REST and by AMQP; the run was stopped before its +180 min read |
| **S4** | **d DUPLICATED, c SEEN 3/3, b INCONCLUSIVE** | A lost-response POST retried by the SDK runs the batch twice; first submits to a new UEC race to a 409; no 429 within the caps |
| **S5** | **PASS (both orders)** | No revival in 10 min after an abort; a SIGTERM'd UEP cancels its own blocks; orphaned tasks stay `pending` at the service; a stray submit with the retired config revives the UEP in 18 s |
| **S7** | **Neither MEP passes §7's gate as written** | Delta renders no pool key (fixed single block); Anvil renders block counts only (no workers, accelerators, idle or drain) |
| **S8** | **PASS (Slurm and PBS)** | `SLURM_JOB_END_TIME` is exact; `%L` within +1 s; PBS `stime` + walltime to 1 s; `resources_used.walltime` lags |

## Live SDK findings (found while running the spikes; each changed a script, and each is a design fact)

1. **A task group must be minted by the service.** A client-made UUID passed as `task_group_id` is refused (`404 TASK_GROUP_NOT_FOUND … could not be resolved`), and that single failed submit **shut the Executor down**: the next submit raised "Executor … is shutdown". A group is minted by a submit without one (`Executor.task_group_id = None`, or `create_batch(task_group_id=None)`), and the Executor then adopts the service's id (`executor.py:1227-1243`).
2. **`Executor.shutdown()` does not stop the task group's result watcher.** The watcher (`_RESULT_WATCHERS[tg]`, process-wide) keeps receiving and acking results after `shutdown(wait=False)`; only process exit stops it. So "shut down the Executor" is not "stop consuming".
3. **`Client.get_batch_result` silently drops failed tasks.** For a failed task, `_update_task_table` raises `TaskExecutionFailed`, which the loop catches, logs and skips (`client.py:473-478`). A failure reads as "missing". The raw `v2.get_task_batch` returns every task, failed ones with their exception.
4. **Open futures keep the process from exiting** (S5 hung after its cleanup): the watcher and submitter threads are not daemons. loam's rule ("always `shutdown(wait=False, cancel_futures=True)`") is not enough while futures are open; the spikes exit with `os._exit` after flushing.
5. **The SDK's default code serialiser (`DillCode`) pickles a module-level function by reference.** A task function defined in an importable module failed on the worker with `ModuleNotFoundError` (S4's first probes). It must be defined in `__main__` or shipped as source.

## S1: an acked result, read by another process; two consumers on one task group

**Method.** As planned, on the login shape. Arm A: child process A submitted 6 tasks to a service-minted group and received all 6 over AMQP (so its watcher acked them), then exited. The parent read ids 0–1 by raw REST at +1 min, again at +70 s, and ids 2–3 at +60 min, then a fresh process ran `reload_tasks()` on the group. Arm B: a group minted by one tiny submit; children B1 and B2 each submitted 8 tasks (10 s each) to it and waited 150 s for their own; a hook logged every AMQP receipt per process.

**Evidence.**
- Arm A: at +1 min both tasks returned `success` with their result (`A-0`, `A-1`), and again 10 s later from a new Client: **a REST read is not destructive, and an AMQP ack does not remove the REST copy.** At +60 min, ids 2–3 returned `success` with results. A fresh process's `reload_tasks()` resolved all 6 in 0.29 s: the SDK's reattach works by REST for acked results.
- Arm B: B1 resolved 4 of its 8 futures, B2 3 of 8. B1's watcher received 3 of **B2's** results and B2's received 4 of **B1's**: they acked each other's results, and those futures never resolved. The parent's raw REST read recovered 8 of the 9 unresolved tasks as `success` with results; the 9th was still running (one login worker ran 17 × 10 s tasks in series, past the 150 s wait).

**Outcome.** H1 PASS (at +1 min, re-read, and at +60 min). H2 CONFIRMED, and REST recovers what another consumer swallowed. The lease guards admission and the single consumer; a crash between an ack and the ledger commit is repaired by a free REST pull, also on a MEP.

## S2: results nobody fetched

**Method.** As planned, on the login shape. A control group and, per mark, one REST group and one AMQP group of 2 tasks each, submitted within 13 s. Each Executor was shut down about 2 s after its submit, so nothing consumed a result until its mark. The control group was read by REST at about +2 min to fix the completion time; each mark's groups were read when it fell due: the REST group with the raw `v2.get_task_batch`, the AMQP group by a fresh process's `Executor` on that group.

**Evidence.**
- **+35 mark:** both REST tasks returned `success` with their results, 34.0 and 34.5 min after their own completion. The AMQP group delivered both results to a fresh process.
- **+60 mark:** both REST tasks returned `success` with their results, 57.0 and 57.5 min after completion. The AMQP group again delivered both.
- **+180 mark: not read.** The run was interrupted at about +163 min, when the user asked for the docker env to be pulled down for the night. Its `finally` stopped and deleted the endpoint (`stop_endpoint`, `teardown_endpoint`), and the stack was removed. The +180 min groups had been submitted but were never read, and cannot be now.

**Outcome.** PASS to +57 min by both channels: the documented 30-minute "abandoned" mark does not purge an unfetched result within the first hour. Whether results survive for hours is OPEN. The design does not depend on it: `<results_dir>` and the session directory stay the store of record. A re-run needs about 3 h on its own stack.

## S4: the duplicated POST, RESOURCE_CONFLICT, the rate limit

- **d, DUPLICATED (and executed twice).** In a service-minted group, the first `/submit` POST was accepted (`200`, task `5dc50700`), then a wrapper raised `ConnectionError` as if the response were lost. globus_sdk's own retry sent the identical body (`200`, task `a3364bfc`), and `batch_run` returned only the second. `get_task_group` listed both (plus the mint task), and the raw task read showed **both executed on the worker**. The client never learned of `5dc50700`.
- **c, SEEN in 3 of 3 trials.** Two processes sent the first task for a fresh UEC together; one got `409 RESOURCE_CONFLICT` ("already in use: possibly due to concurrent requests") within 0.6–1.2 s, with nothing run for it. The other's task was delivered and ran.
- **b, INCONCLUSIVE.** No `429` at 30 `get_endpoint_status` GETs in 1.9 s, nor at the 22 one-task POSTs that followed in about 3.1 s. Responses carry no `RateLimit-*`, `X-RateLimit-*` or `Retry-After` header. No lockout happened, so the scope arms never fired and nothing touched the other agent's cells. The documented "20 requests per 10 s" is not enforced at this burst size on these routes.

## S5: an SSH pool abort

**Method.** As planned: a pool UEC (2 blocks × 2 workers, a fresh `hpcb_block` token), 6 × `sleep 900` (4 running, 2 queued). Arm A cancelled the blocks, then SIGTERM'd the UEP; arm B SIGTERM'd the UEP first. 10-minute watch each. Arm C sent one task with arm A's retired UEC.

**Evidence.**
- Arm A: the UEP was gone 1.4 s after `scancel`; no new job, no live UEP and no manager-log line for that UEP in 10 min.
- Arm B: SIGTERM ended the UEP within about 1.5 s, and **its own shutdown cancelled both blocks**: the `scancel` that followed found no job. Nothing revived in 10 min.
- All 12 orphaned tasks (both arms) read `pending` / status `unknown` at +1 and +5 min, and still at about +25 min. The service never settled them; the open futures never resolved.
- Arm C: the manager restarted **the same UEP id** (new pid) and Slurm ran a new block (job 6) within 18 s; the stray task succeeded. Arm A's 6 orphans did not run on the revived UEP (still `unknown`). A final SIGTERM again took the block down with the UEP.

**Outcome.** PASS in both orders. The order does not matter, because SIGTERM makes the UEP cancel its own blocks. The pool token's retirement is load-bearing: one stray submit restarts billing within seconds.

## S7: the Delta and Anvil MEP templates

In short:
- **Delta** renders no pool key: `min_blocks` 0, `max_blocks` 1, `init_blocks` 1 and `max_workers_per_node` 4 are fixed.
- **Anvil** renders the block counts (schema caps: ≤12 blocks × ≤2 nodes, `min_blocks` ≤1), but fixes `max_workers_per_node` at 128.
- **Neither** renders `available_accelerators`, `drain_period` or `max_idletime`, so the engine default of 120 s applies, and the UEP exits after 300 s idle.
- Delta's endpoint has moved to gce 4.19.0 against an entry verified on 4.16.0.

**Method.** What the Delta and Anvil MEPs let a user set and actually render. Run 2026-10-30 with the harness test identity (`HPCB_TEST_GLOBUS_DB`, used in place, read-only), by a spike script. It made 4 GETs: one `get_endpoint_status` and one `get_endpoint_metadata` per MEP. It submitted no task, started nothing, and triggered no login. The raw metadata and the per-key verdicts were kept outside the repo.

**Route.** This is the product's own path: `MEPFacility.from_entry(seed)` → `load_template()` (the metadata's `user_config_template` and `user_config_schema`). The verdicts are then the product's own judges: `renders()`, `takes_size_key()`, `sanitize_uec()` and `dispatch_uec()`. Both templates are readable without submitting anything.

**State.** Both endpoints are online.

| | Endpoint version | Template digest |
|---|---|---|
| **Delta** | **gce 4.19.0** (the entry was verified with 4.16.0) | Unchanged since verification (`ba10e635…`) |
| **Anvil** | gce 4.16.0 | Unchanged (`465bc5f8…`) |

Anvil was offline on 2026-10-24 and is back.

### Per key

Key to the cells:
- **R** = rendered from the user's config (`{{ key }}`): settable.
- **lit** = hard-coded in the template: not settable; the value is fixed.
- **—** = absent: parsl's or the engine's default applies.

| Key | **Delta** `4a266c83` (schema permissive; lists only `worker_init` and `endpoint_setup`) | **Anvil** `5aafb4c1` (schema STRICT: `additionalProperties: false`) |
|---|---|---|
| `min_blocks` | lit `0` | **R**, default 0; schema allows **0–1** |
| `max_blocks` | lit `1` | **R**, default 1; schema allows **1–12** |
| `init_blocks` | lit `1` | **R**, default 1; schema allows **0–1** |
| `nodes_per_block` | — (1) | **R**, default 1; schema allows **1–2** |
| `max_workers_per_node` | lit **`4`**; the value hpc-bridge sends is ignored | lit **`128`**; the strict schema drops the value hpc-bridge sends |
| `available_accelerators` | — (no per-worker GPU pinning) | — (and the strict schema would reject it) |
| `max_idletime` | — (engine default **120 s**: `job_status_kwargs` is not set) | — (120 s) |
| `drain_period` | — (none: blocks run to walltime) | — (rejected by the schema) |
| `walltime` | **R**, default `00:30:00` | **R**, default `00:30:00` |
| `account` | **R** | **R** (≤32 characters) |
| partition / queue | **R** `partition` (no `queue` key) | **R** `partition`, default `shared` |
| `worker_init` | **R** (otherwise the facility's uv venv at the client's SDK version) | **R** (likewise) |
| Also rendered | `qos`, `scheduler_options`, `constraint`, `exclusive`, `cores_per_node` (default 1), `mem_per_node`, `endpoint_setup` | The same, plus `resource_specification`, `requirements` and `venvs_dir` |
| Fixed | `SrunLauncher`; `idle_heartbeats_soft: 10` (the UEP exits after 300 s idle); `idle_heartbeats_hard: 5760` (48 h) | `SrunLauncher`; `debug: true`; the same heartbeats |

On both MEPs the product's `_template_idle_s()` returns `None`, so agents are told the idle release is "unknown". In fact, the 120 s engine default applies.

### What it means for D3 (drain-only campaigns, per facility)

The design's gate (§7: render `max_blocks`, `init_blocks`, `available_accelerators`, `max_workers_per_node` and a known short `max_idletime`) passes on **neither** facility.

#### Delta

Delta renders none of the pool keys, but its literals make a fixed, safe pool: at most 1 block × 1 node, `min_blocks` 0, and a UEP that exits after 300 s idle. The spend bound needs no rendered key: one block at a time × the rendered walltime.

**Campaign on Delta = phase 1's `run_batch` on the one confirmed block, drain-only.** There is no scale-out and no pinning, and 1 to 4 workers (unknown until a block reports `PARSL_WORKER_COUNT`).

`init_blocks: 1` is fixed, against §0.1's "pools start with `init_blocks: 0`". That is harmless only because a UEP starts with its first task.

**Re-prove Delta before any use.** It moved to gce 4.19.0, while the entry pins its workers to `parsl==2026.8.10`. That is the skew the seed's own comment says "dropped every result while the block billed".

#### Anvil

Anvil renders the block-count keys, so a scale-out pool is expressible, capped by the schema at ≤12 blocks × ≤2 nodes, with `min_blocks` ≤1. hpc-bridge must send `min_blocks: 0` and `init_blocks: 0` itself; the template's default for `init_blocks` is 1.

Anvil does not render the following:
- **No `available_accelerators`:** GPU pools are unpinned.
- **No `max_workers_per_node`:** it is fixed at 128, so the real count is min(128, the cores parsl sees). A 1-core `shared` block may still run several workers.
- **No `drain_period`:** the admission gate (S8) is the only end-of-block guard.

**Campaign on Anvil = D3(a) for CPU pools only, drain-only.**
- **The window:** counted as 1 per block until the first result reports `PARSL_WORKER_COUNT`.
- **The pool token:** it must ride in `endpoint_setup` (rendered, ≤512 characters) or `worker_init`, because an unknown `hpcb_pool` key is rejected by the strict schema.
- **GPU campaigns:** not until the wrapper pins devices itself.

#### Suggested wording for D3(a)

> Offer drain-only campaigns where the **bound** keys are known (rendered, or a literal read from the template), and scale-out where `max_blocks`/`min_blocks` are rendered:
> - Polaris: now.
> - Anvil: CPU pools.
> - Delta: single-block only, after its re-prove.
> - globus-labs: after its re-prove.
>
> Pinning only where `available_accelerators` renders (Polaris).

The gate needs a `template_value(key)` that reads a literal or a `default(…)`, as `_template_idle_s` does for one key, plus the schema's `maximum` per key as the hard ceiling.

#### The pool token (S3)

Globus Compute's docs say the web service derives a user endpoint's id from the endpoint id, the identity, **the config in the request** and the runtime. So any distinct value yields a distinct UEP, provided it passes the schema:
- Delta: any key.
- Anvil: `endpoint_setup` or `worker_init`.

S3's "does an unknown `hpcb_pool` key yield a distinct UEP?" shrinks to a one-task confirmation on Polaris.

## S8: remaining walltime from inside a block

5-minute blocks; probes at block start, +60, +150 and +240 s; truth from `sacct` and `qstat -x -f` on the login node.

| Scheduler | Method | Error at 0 / 60 / 150 / 240 s | Cost |
|---|---|---|---|
| Slurm 23.11 | `SLURM_JOB_END_TIME − now` | 0.00 at every probe (equals `sacct` Start + Timelimit) | no RPC |
| Slurm | `squeue -h -j $SLURM_JOB_ID -o %L` | +0.16 / +0.10 / +0.22 / +0.73 s (rounds up) | 3–7 ms |
| OpenPBS 23.06 | `stime` + `Resource_List.walltime` − now (`qstat -f` on the mom) | ≤1 s (`stime` has 1-s resolution) | 4–8 ms; once 211 ms |
| OpenPBS | `Resource_List.walltime` − `resources_used.walltime` | empty at start; then lags by 15.1 / 3.4 / 5.6 s | same call |

Also:
- **The login shape.** `squeue -j ""` returned code 0 with **empty** output, and `qstat -f ""` the same: no number, but no error either.
- **Worker counts.** `PARSL_WORKER_COUNT` was 2 in both blocks (as configured) and 1 on the login shape.
- **Ends.** Slurm killed the block 15 s past its limit (End 17:29:03 vs 17:28:48, `TIMEOUT`); PBS at 5:17 (exit −29).
- **A task straddling the end** came back as `parsl…ManagerLost` (PBS; on Slurm the same `TaskExecutionFailed`, its text cut) about **105 s after the block died**, on both schedulers.

The controller-pause chaos arm was not run.

**Outcome.** PASS on both schedulers, through `SLURM_JOB_END_TIME` or `%L` on Slurm, and `stime` on PBS. `resources_used.walltime` must not be used.

## Not run, or not answered

- **S4b's scope:** no 429 was reached within the planned caps, and the caps were not raised.
- **S8's chaos arm.**
- **S3 and S6** were never in this phase's scope.

## See also
[[Batch and swarm execution]] · [[Batch design review 2026-10-30]] · [[facility-mep]]

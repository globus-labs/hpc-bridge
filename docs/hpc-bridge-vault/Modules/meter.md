# meter.py

> [!abstract] Role
> The billing meter of batch phase 2a: one login-shape call per tick reads what every job of ours charged and can still charge, and it never understates. It is pure (it parses and computes); [[jobs]] runs the tick and writes the [[ledger]]. Design: [[Batch and swarm execution]] §3 "The meter never understates".

## What it does

- **The command** (`slurm_cmd`, `pbs_cmd`):
  - **Slurm:** `env TZ=UTC0 sacct -X -D -n -P -u "$USER" -S <earliest, UTC> -o JobID,JobName,State,ElapsedRaw,TimelimitRaw,AllocTRES,ReqTRES,Partition,Account,ExitCode,Reason` (`-D`: a requeued job's earlier runs, review B8). In the same call it runs `squeue -h -u "$USER" -o '%i|%T|%M|%l|%j'`: whether a job is still live (on its way out after a cancel, or missing from sacct), and an array's pending tasks — sacct has NO row for those (the fake cluster's Slurm 23.11, probe 2026-11-01); squeue shows them collapsed as `N_[a-b%t]`.
  - **PBS:** `qstat -x -f -F json -t $(qselect -x -u "$USER")` (`-t`: an array's subjobs, task by task; the parent row is skipped when they are listed).
  - **Bounded:** each scheduler command runs under `__hb_bound`, a bash-native timeout (`timeout_s()`: `HPC_BRIDGE_METER_TIMEOUT_S`, default 15 s, always a few seconds under the sync-wait). With slurmdbd down sacct HANGS (live 2026-11-01: the tick outlived the sync-wait, read nothing, and held the login channel for the next status); killed, it is an unread sacct and every job holds its full walltime. The watcher's output goes to /dev/null, so an orphaned `sleep` never holds the call's stdout open.
  - The journal's `meter.tick` row carries `sacct_calls` (one per tick).
- **The reading** (`parse_slurm`, `parse_pbs`, `job_reading`; `SchedView` has `rows`, `live`, `pending`, `names`):
  - **Charged:** the job's `billing` TRES (AllocTRES, else ReqTRES; it carries the site's weights) × elapsed, every run of it. Where the scheduler reports no billing (PBS, an old Slurm), the rate the terms reserved at is used.
  - **The minimum:** charged is never below the queue's `min_charge_h` once the job has started.
  - **Committed:** a running job's remaining walltime; a queued job's full walltime; every pending array task's (squeue's `N_[a-b%t]`, counted by `range_count`, steps included); a task the scheduler does not list at all holds its full walltime.
  - **Missing from sacct:** a ledger job that sacct doesn't list, or any job when sacct can't be read, keeps its whole walltime: charged as last read, committed = worst case − that. `meter_source` is `squeue` when squeue still lists it, else `missing`.
  - **Live:** `Reading.live` — still listed by the scheduler (an ended state while squeue lists it is `COMPLETING`): [[jobs]] keeps such a job draining.
- **Unledgered jobs** (`unledgered(view, known, excluded, accounts=, horizon_s=)`): every other job of the user in the same output, not ledgered and not excluded, on one of the envelope's accounts. It is counted at its own billing (else its cpus) and named; one with no time limit (`UNLIMITED`) is held to `horizon_s`, the envelope's deadline (review B8).
- `tres` and `range_count` are public: [[jobs]] reads a hand job's request with them.

## See also
[[jobs]] · [[ledger]] · [[Resource requests]] · [[Batch and swarm execution]]

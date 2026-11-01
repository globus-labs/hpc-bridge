# meter.py

> [!abstract] Role
> The billing meter of batch phase 2a: one login-shape call per tick reads what every job of ours charged and can still charge, and it never understates. It is pure (it parses and computes); [[jobs]] runs the tick and writes the [[ledger]]. Design: [[Batch and swarm execution]] §3 "The meter never understates".

## What it does

- **The command** (`slurm_cmd`, `pbs_cmd`):
  - **Slurm:** `TZ=UTC0 sacct -X -n -P -u "$USER" -S <earliest, UTC> -o JobID,JobName,State,ElapsedRaw,TimelimitRaw,AllocTRES,ReqTRES,Partition,Account,ExitCode,Reason`. In the same call it runs `squeue -h -u "$USER" -o '%i|%T|%M|%l|%j'`, which says whether a job is still live: on its way out after a cancel, or missing from sacct.
  - **PBS:** `qstat -x -f -F json $(qselect -x -u "$USER")`.
  - The journal's `meter.tick` row carries `sacct_calls` (one per tick).
- **The reading** (`parse_slurm`, `parse_pbs`, `job_reading`):
  - **Charged:** the job's `billing` TRES (AllocTRES, else ReqTRES; it carries the site's weights) × elapsed. Where the scheduler reports no billing (PBS, an old Slurm), the rate the terms reserved at is used.
  - **The minimum:** charged is never below the queue's `min_charge_h` once the job has started.
  - **Committed:** a running job's remaining walltime; a queued job's full walltime; every pending array task's (a collapsed `N_[a-b%t]` row counts its tasks); a task the scheduler does not list at all holds its full walltime.
  - **Missing from sacct:** a ledger job that sacct doesn't list, or any job when sacct can't be read (slurmdbd down), keeps its whole walltime: charged as last read, committed = worst case − that. `meter_source` is `squeue` when squeue still lists it, else `missing`.
- **Unledgered jobs** (`unledgered`): every other job of the user in the same output, not ledgered and not excluded, on the envelope's account. It is counted at its own billing (else its cpus) and named.

## See also
[[jobs]] · [[ledger]] · [[Resource requests]] · [[Batch and swarm execution]]

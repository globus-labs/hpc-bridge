# jobs.py

> [!abstract] Role
> Batch phase 2a: the budget envelope, Jobs mode (`submit_job`) and decision D2's gate and adopt — with the [[meter]] and the [[ledger]]'s `envelope` and `job` tables. The tools are in [[The MCP tools]]; the stop's half is in [[server]]. Design: [[Batch and swarm execution]] §2, §3, §12 D2.

## What it does

- **`submit_job`** (`submit_job` → `spec_of` → `find_envelope` → `_open_and_admit` / `_offer_again` / `_admit`): SSH facilities only. On a facility MEP it refuses with the reason (no free status or cancel channel; on ALCF Jobs mode collapses into Campaign). `spec_of` checks every argument before anything is written, because none may become shell code (rule 7): partition and account names, `walltime`, `array` (`0-19%4`: tasks, throttle), `depends_on` (job- ids in the ledger), `session_id` (Session's own check), and the size (`resources.requested`). It then reads the size and the rate:
  - `resources.charge` with the partition's billing weights;
  - otherwise the job's billed cores (Slurm's default billing), never a whole node for a slice;
  - a cost it cannot bound is refused.
  The worst case is tasks × rate × max(walltime, the queue's `min_charge_h`).
- **The envelope** (`EnvelopeInfo`).
  - **Opening.** With none open for this session on the facility and account, the first call answers `needs_confirmation`, reason `envelope_needed`, with the `terms` and the proposed envelope (cap = the request's or the job's worst case; deadline 24 h by default, at most 168 h). Nothing is written. The confirming call opens it, after the operator ceiling (`HPC_BRIDGE_ENVELOPE_MAX`, summed over open envelopes on the same facility and account) and the envelope's own dimensions (`max_jobs` counting array tasks, `max_nodes_per_block`, partitions).
  - **Ownership.** It is `approved_by` this server's `AppCtx.session_token`. One whose session is gone (its owner's pid dead, as the batch lease is judged) is offered again (`envelope_new_session`, naming what remains); the yes re-binds it (`rebind_envelope`, the cap changed if the yes named one). One whose owner still runs is that session's, so this session proposes its own.
  - **Admission inside it.** A job that fits is admitted on the first call. One that doesn't answers `envelope_exhausted` (the state turns `exhausted`), naming what still bills and the unledgered jobs it counts. A yes with a larger `envelope.cap` raises the cap.
- **`_admit`, the order that holds rule 1:**
  1. a meter tick;
  2. `Ledger.reserve_job` (one `BEGIN IMMEDIATE`: the envelope admits, the job passes its dimensions, the hold plus its worst case is within the cap);
  3. `write_job_script`;
  4. `_submit_script`.

  A refused submit releases the reservation (`refused`), quoting the scheduler. A submit whose answer never came is `submit_unknown`: it keeps the reservation, the meter's sweep finds it by its name `hpcb-<job id>`, and it is never submitted again.
- **The frozen job.sh** (`job_script`, `write_job_script`): `<scratch>/jobs/<job id>/job.sh`, written once under noclobber (an existing one is refused, never replaced), and its sha256 read on the facility must equal the one written.
  - **Header.** Built from the terms. Slurm: `--job-name=hpcb-<job id>`, nodes, time, `--deadline=<UTC>`, partition, account, `--exclusive` or `--ntasks-per-node=1 --cpus-per-task` (+`--mem`, `--gres`), `--array`, `--dependency`. PBS: `-N`, `select`, walltime, `place=excl`, `-q`, `-A`, `-J`, `-W depend`.
  - **Body.** The session's `.cwd` and `.env` replayed, then the command carried base64 (so no line of it is a directive), then `exit_code[.<task>]` written beside the script.
  - **Submission.** Slurm runs `TZ=UTC0 sbatch --parsable --output=<dir>/slurm-%j.out` (`%A_%a` for arrays); PBS runs `qsub -o <dir>/pbs.out -j oe`.
- **The meter's tick** (`tick`): one login-shape call per tick over the facility's envelopes ([[meter]]). It updates every job row's `charged`, `committed`, `meter_source`, state and array counts, and each envelope's spent, committed and the unledgered jobs it counts (less `excluded`). It also applies the envelope's deadline (`at_deadline` `drain` or `abort`) and writes `meter_reading` rows and the `meter.tick` journal row.
- **`status` / `cancel` of `job-` and `env-` ids** answer in the one `BatchStatus` model (`kind`, `job`, `envelope`) and in `CancelResult`. For a job, `abort` scancels or qdels it and `drain` cancels only its pending tasks. For an envelope, `drain` admits nothing more and `abort` cancels every job of it. A cancel says `draining` while the scheduler still lists one (squeue or a non-final PBS state) and `down` once none is; its notice says `cancelled: <ids>`.
- **D2 (c)** (`gate_envelope`, `gated_outcome`, `HOLD_SHIM`, `adopt_hand_submits`):
  - **Gate.** A recognised `sbatch`/`qsub` (the [[scheduler_ops]] lexer) while this session's envelope is open answers `needs_confirmation` and runs nothing. salloc and srun keep #176's advisory.
  - **The yes.** `run_shell(…, confirm_spend=True)` runs it with `sbatch`/`qsub` as exported bash functions that add `--hold` / `-h`.
  - **Adoption.** Each receipt's request is read (`scontrol show job -o`, `qstat -f -F json`) and reserved as a `held` row. It is then given the envelope's deadline (`TZ=UTC0 scontrol update … Deadline=`) and released (`scontrol release` / `qrls`). If it doesn't fit, it is cancelled while still held: it never ran. A job the hold did not catch (an absolute path) is adopted as it is, or cancelled at once and said.
  - **`submit_job(adopt=<id>)`** brings an existing job under the envelope. It is never cancelled for not fitting (it already runs); the user is asked instead.
- **The compute block** (`block_admission`, called by `_ensure_endpoint_up` with `confirm_spend` before its lock): while this session's envelope is open, a block it confirms is reserved as a `pool_block` row (its max charge, else billed cores × walltime). If it doesn't fit, the answer is `needs_confirmation` naming the envelope. The reservation holds until the stop (`close_admission`). The sweep does not count the endpoint's `parsl.*` pilot a second time.
- **The stop** (`billing_jobs`, `stop_question`, `cancel_for_stop`, `keep_for_stop`, `after_stop`, `stop_without_channel`, `close_admission`; called by `server._stop_endpoint`):
  - **Which jobs.** Every job of ours that can still bill (any session's, a dead server's, a held one), read fresh by the meter when a login channel is bound. Blocks are the stop's own.
  - **With no choice**, the stop asks and changes nothing.
  - **`jobs="cancel"`** cancels them and drains their envelope. The answer is `draining` while the scheduler lists one, then `down`.
  - **`jobs="keep"`** leaves them running under their envelope (now `draining`: status and cancel only) and their `--deadline`. The answer is `draining`, never `down`, and names them and their `cancel(…)`.
  - **With no endpoint bound**, nothing can be cancelled from here, and the answer says to connect first.
  - **After any stop** the facility's envelopes admit nothing more.

> [!warning] The envelope binds only what hpc-bridge starts
> `terms.binds` says it and the envelope repeats it. A submit the lexer cannot see (Python's subprocess, an `sbatch` inside a script) passes the gate; the meter's sweep counts and names it. A stolen Compute token is not bound at all. The site backstop is "none" unless the user names one.

## See also
[[meter]] · [[ledger]] · [[batch]] · [[server]] · [[scheduler_ops]] · [[Batch and swarm execution]] · [[Resource requests]]

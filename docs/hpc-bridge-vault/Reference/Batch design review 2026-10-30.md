# Adversarial review: batch and agent-swarm design

> [!abstract] In one line
> The adversarial review the design answered: twelve findings against the draft of [[Batch and swarm execution]] (2026-10-30), most severe first, each with a fix, plus what breaks if a spike says no. § numbers refer to the draft it reviewed.

_Reviewer's pass over the design ([[Batch and swarm execution]], 2026-10-30), against 74fd47d, the installed `mcp` 1.30.0, `globus-compute-sdk` 4.18.0 and `globus_sdk`, the three research reports and a copy of the ALCF docs (working files of the design, not kept in the repo), and Context7 (Globus Compute, Parsl). Read-only. The one probe is a window simulation, also a working file that was not kept._

Status key: **CONFIRMED** means code, docs or a probe show it. **PLAUSIBLE** means the mechanism is shown but the failure is not reproduced.

## Findings, most severe first

### 1. On a MEP, the "presumed lost" clock would drive retries and refills, which breaks D3's spend bound
- **Section:** §4, settling (3) and (4); §3, the MEP bound; D3.
- **Defect:** Today the MEP presumption only unblocks stop and detach. Its clock runs from **submit**, and the code warns that a task past it "may be lost — or only queued" (`warmth.py:138-155`). The design reuses that clock to retry `idempotent` tasks. It never says whether a presumed task frees its window slot. Either way, the interchange can hold more than one task per worker. The MEP cap, `max_blocks × nodes × (deadline − open + task_walltime + max_idletime)`, assumes it never does.
- **Scenario:**
  1. On Polaris, `capacity` allows 1 running job per user, and prod-sized queues can wait hours. The window's tasks wait 3 h behind a pending block.
  2. At task walltime + block walltime + grace, they become `presumed_lost`. The idempotent ones are resubmitted, or the backlog refills the freed slots.
  3. When the block starts, it runs both the originals and the new attempts.
  4. After a drain, the tail is (queued tasks ÷ workers) × task walltime, not one task walltime. Spend passes the bound the terms stated.
- **Evidence:** `warmth.py:138-155` (`_past_ceiling` and `_overdue` count from `h.submitted_at`); design §4 "Settle in this order" (3) and (4); the facilities research report's A1 (Polaris queue limits; a working file of the design, not kept in the repo).
- **Status:** CONFIRMED for the mechanism; PLAUSIBLE for the run.
- **Fix:**
  - On a MEP, a presumed task keeps its window slot until a result arrives or the 2-week TTL passes.
  - Never retry on a presumption; retry only on evidence, such as a `ManagerLost` result.
  - Start the clock at evidence that a block started (a sibling result naming the job), not at submit.
  - State the bound as in-flight tasks × task walltime, plus the idle tail, after the deadline.

### 2. The loam handoff's `release: qdel <job>` does not release a `min_blocks ≥ 1` pool
- **Section:** §8 (`release`, `terms`, and "A `min_blocks=1`… or the user runs `qdel`").
- **Defect:** Parsl maintains `min_blocks`, so cancelling the block makes parsl request another. loam relies on exactly that: "`min_blocks=1` makes a debug block replace itself". A MEP has no cancel channel. The handoff's terms and release instruction therefore understate the bill: they promise an end that does not exist. `expires_at` enforces nothing either, because only loam's preflight reads it.
- **Scenario:** loam holds a Polaris `capacity` block (168 h) with `min_blocks=1`. The user runs `qdel` as the record says. PBS starts a new block within one strategy period, and it keeps billing until the UEP exits. Whether the UEP ever exits is spike S3, still unknown.
- **Evidence:**
  - Parsl elasticity docs: "`min_blocks`: Minimum number of blocks to maintain… request another block if the current one expires" (Context7 `/parsl/parsl`).
  - The facilities-and-loam research report, lines 56 and 112 (a working file of the design, not kept in the repo).
  - loam's `SKILL.md:84-85` (`"min_blocks": 1`).
  - ALCF's remedy for runaway submission is `rm ~/.globus_compute/*/daemon.pid` (ALCF's Globus Compute page, lines 389-397, in the design's working copy of the ALCF docs, not kept in the repo).
- **Status:** CONFIRMED.
- **Fix:**
  - Refuse `min_blocks ≥ 1` in a MEP handoff until S3 proves the UEP exits when idle.
  - If it is allowed, set `worst_case: unbounded`, and make `release` name the only working end: the user removes `daemon.pid` on the facility, or the proven idle exit. Never `qdel`.

### 3. Phase 1's "no new spend semantics" is false at the confirmed block's end
- **Section:** §10, phase 1 (`run_batch` on `shape="compute"`).
- **Defect:** Admission keeps filling the confirmed block's workers, and nothing ties it to the block's remaining walltime:
  - `drain_period` exists only for phase-3 pools. No template sets it today (`grep drain_period src/` finds nothing).
  - The reap presumption fires only once the block is **already past** its walltime (`warmth.py:424-432`).
  - It counts from `block_since`, which on a MEP's pre-spawned block (`init_blocks: 1`) is later than the job's real start.

  A task admitted as the block dies waits at the interchange, and parsl starts a replacement block that nobody confirmed. This is the #204 pattern. On a MEP that block cannot be cancelled. A second, smaller gap: the warmth check runs once per admission tick, outside `app.lock`, so a `stop_endpoint` between the check and the submit races it.
- **Scenario:** The Polaris entry's default walltime is 15 min (`alcf-polaris.yaml`). A batch of 200 rows with `task_walltime_s=300`, on `max_workers_per_node: 4`, crosses a block end roughly every 15 min. Each crossing can start an unconfirmed Polaris block that bills until its idle release.
- **Evidence:** `warmth.py:424-432`; `alcf-polaris.yaml` (`walltime: "00:15:00"`, `init_blocks: 1`); design §5 guards (pools only).
- **Status:** CONFIRMED that the rule is missing; PLAUSIBLE for the run.
- **Fix:**
  - Admit only while remaining walltime ≥ `task_walltime_s` + a margin.
  - Read the remaining walltime from inside the block in the wrapper (`squeue -h -j $SLURM_JOB_ID -o %L`, or `qstat -f $PBS_JOBID`). That is free, because the block is already billing.
  - `stop_endpoint` and `cancel` pause admission under `app.lock` before releasing anything.
  - Add a failing-first scenario: a batch that crosses its block's walltime.

### 4. Two processes on one ledger: no campaign owner; the SDK acks results it cannot match; reservations are not atomic; the NFS guard does not exist
- **Section:** §4 (storage, collector, recovery) and §6 ("One submitter per Globus identity").
- **Defect:**
  - Stdio keeps a private `Core`, but the ledger lives in the shared `~/.hpc-bridge`. Any process "touching" a campaign resumes it.
  - SDK 4.18's `_ResultWatcher` consumes the task-group queue (`{queue_prefix}{task_group_id}`), stores **every** message and acks it within about 0.5 s, whether or not it holds a future for it. Two processes on one campaign therefore each swallow the other's results.
  - Executors also share one task group by default, and the setter rewrites the class default, so a campaign's group leaks into interactive Executors built later.
  - "Reserve before you admit" is a read, a check and a write, but the design gives only "one transaction per write, behind an async writer thread".
  - "On NFS it fails closed, as `SentCheckStore` does" is wrong. `SentCheckStore` fails closed only when its `flock` times out or reports ENOLCK; it never detects NFS. SQLite's WAL mode is documented as not working on network filesystems. The service is likely to run where home is NFS, GPFS or Lustre: on login nodes, under the NERSC workflow QOS, or beside loam.
- **Scenario:**
  1. An orphaned stdio server (the harness already sees `harness:server_orphaned`) and a new session both open the campaign.
  2. Both admit, so the window doubles.
  3. The orphan acks half the results; the new process sees those tasks pending. On a MEP they are then presumed lost and retried (finding 1).
  4. Both reservations pass the cap check, and the cap is overrun.
- **Evidence:**
  - `globus_compute_sdk/sdk/executor.py:1660-1676` (`_on_message` stores and appends to `_to_ack` unconditionally), `:1630-1636` (the ack), `:1793-1794` (the queue name), `:191-194` and `:320-325` (the class-level task-group default).
  - Globus docs (Context7): "Subsequent Executor objects will reuse the same task group ID by default".
  - `state.py:204-226`.
- **Status:** CONFIRMED for the SDK and `state.py`; PLAUSIBLE for the run.
- **Fix:**
  - A campaign lease row (host, pid, heartbeat) taken under `BEGIN IMMEDIATE`. Only the holder admits and collects; any other process is read-only.
  - An explicit `task_group_id` on every Executor, interactive ones included.
  - Reservations in one `BEGIN IMMEDIATE` transaction.
  - A real filesystem-type check (`statfs`: nfs, lustre, gpfs, cifs, fuse) that refuses.
  - Add "two consumers on one task group" to S1.

### 5. The envelope is not server-enforced against an agent's code, and `restricted` is not a security boundary
- **Section:** §3 "The honest limit"; §6 auth, `restricted` for remote clients, and session isolation; §0.5.
- **Defect:** On SSH-bootstrapped facilities, `~/.globus_compute/storage.db` holds the user's Globus Compute **refresh token** (seeded by hpc-bridge). Any batch task, `run_shell` or `run_code` can read it and submit directly to any endpoint the identity can reach, including the Polaris MEP with `max_blocks: 20`. That spend has no ledger row, and on a MEP no meter at all.
  - §6 offers `restricted` as the mitigation for remote clients, yet §0.5 concedes that `run_code` is arbitrary code.
  - "Session isolation" is a directory name: one agent can `scancel -u $USER`, delete another campaign's result store, or take the token.
- **Scenario:** A loam child agent in the `restricted` profile runs `run_code` that reads the token and submits 50 tasks to the Polaris MEP. Polaris blocks start that no envelope or stop knows about.
- **Evidence:** `facility/remote.py:656-723` (`seed_storage_db` writes `{remote_dir}/storage.db`); `credentials.py:78-103` (keeps refresh tokens for the required resource servers); design §0.5 and §6.
- **Status:** CONFIRMED.
- **Fix:**
  - Name this route in "the honest limit" and in the terms ("site backstop: none" also covers this).
  - Treat every agent of the service as holding the user's full Compute identity, and say so.
  - Describe `restricted` as a surface for the agent's convenience, not a boundary.
  - Make remote binding opt-in, with that warning.
  - Longer term, consider a narrower identity or token for the facility-side manager.

### 6. The meter's arithmetic can understate
- **Section:** §3, the meter ("Spent is elapsed time × nodes × factor") and the MEP bound.
- **Defect:**
  - **Minimum charges are ignored.** Perlmutter `preempt` charges at least 2 h per job. A pool that idle-releases one node at a time (`max_idletime` 120 s, §7) churns blocks, and each start bills the minimum.
  - **Slices (#234) are mis-counted.** A block of a few cores on a per-core partition bills cores (or memory), not nodes. Elapsed × `NNodes` × factor either overstates or understates, depending on the factor's unit.
  - **`sacct` lags,** and stalls entirely while slurmdbd is down. Nothing says what a ledger job that is missing from `sacct` counts as.
- **Scenario:** A Perlmutter campaign with 30 short block cycles on `preempt` is metered at about 30 × 10 min. NERSC charges at least 30 × 2 h, and the cap is overrun about 12-fold.
- **Evidence:** the facilities-and-loam research report, line 11 (Perlmutter `preempt`: "at least 2 h requested and charged") and line 13 (charge units), a working file of the design, not kept in the repo; `HANDOFF.md:48` (resource slices, #234).
- **Status:** PLAUSIBLE.
- **Fix:**
  - Meter `AllocTRES` `billing` × `ElapsedRaw`, with the catalog's minimum charge per queue.
  - A ledger job missing from `sacct` counts at its full walltime; fall back to `squeue`.
  - On queues with a minimum charge, never idle-release below the minimum: set `max_idletime` from it.

### 7. Restart and envelope lifetime: "a person is present" is not consent, and envelopes outlive their conversation
- **Section:** §4 recovery ("resumes admission only on the first call that touches the campaign"); §3 approval and ceiling; §2 promotion ("unless it draws from an open envelope").
- **Defect:**
  - A read (`status`) from any later agent resumes spend.
  - Phase-1 batches on the session's block aren't covered at all: `spend_confirmed` is in memory and lost on restart.
  - An envelope has no required deadline and persists in the ledger, so any later session can draw on it without the user.
  - The operator ceiling (`HPC_BRIDGE_ENVELOPE_MAX`) bounds each envelope, so N campaigns get N × the ceiling.
  - `at_deadline: abort` needs a running server, and stdio ends with its client.
- **Scenario:** A user approves a 200 node-hour envelope on Monday. On Thursday, an autonomous agent session on the same laptop calls `status`, admission resumes, and it then submits jobs "from the open envelope". Nothing reaches the user.
- **Evidence:** design §2 (promotion), §3 (approval), §4 (recovery); `warmth.py:657` (`spend_confirmed` set in memory); `SKILL.md:77` (autonomous spend needs "an explicit budget signal").
- **Status:** PLAUSIBLE.
- **Fix:**
  - A required deadline, defaulting to 24 h or less.
  - Bind an envelope to the session that approved it unless the operator shares it.
  - Apply the ceiling to the sum of open envelopes per facility and account.
  - Resume a phase-1 batch only through a call that carries `confirm_spend`.
  - Render the deadline into each job (`sbatch --deadline`), and say the server deadline is soft under stdio.
  - `batch_restart_resume` must assert the re-ask.

### 8. Ambiguous submits: the SDK and its transport can duplicate or orphan tasks, and one error kills the Executor
- **Section:** §4 collector; §6 ("20 requests per 10 s… shared"); S4.
- **Defect:**
  - The `globus_sdk` transport retries **any** `requests.RequestException` and HTTP 429/500/502/503/504, up to 5 times. That includes the `batch_run` POST, which has no client idempotency key ("`request_id`: … the web-service assigns").
  - A timeout after the service accepted a batch can therefore submit it twice.
  - An error that survives the retries sets the exception on every future in the batch and shuts the Executor down, cancelling its queued futures.
  - The design's token bucket lives in one process, but the limit is per client or per identity (S4). Other stdio sessions, loam (given the same identity by the handoff), and the user's own scripts share it.
- **Scenario:** A gateway returns 502 after accepting a batch, and the transport retries it. A non-idempotent row runs twice; the second copy has no ledger UUID and bills unseen. Or a sustained 429 kills the pool's Executor mid-campaign, and the server re-admits rows that the service already holds.
- **Evidence:**
  - `globus_sdk/transport/default_retry_checks.py:17-28` and `:49-62`; `transport/retry_config.py:41-46`.
  - `globus_compute_sdk/sdk/client.py:571-583`.
  - `executor.py:1197-1213` (exceptions set on the futures) and `:1126-1140` (Executor shutdown).
- **Status:** CONFIRMED for the SDK behaviour; PLAUSIBLE for the duplicate at the service.
- **Fix:**
  - The wrapper takes `mkdir attempt-<n>/` atomically and exits if the directory exists; first writer wins.
  - Mark rows `submit_unknown` on any submit error and reconcile with `get_task_group` before re-admitting. Never auto-resubmit non-idempotent rows.
  - Gate `batch_run` by passing `Executor(client=…)` a client that wraps it (the parameter exists), and rebuild the Executor on failure.
  - Add the duplicate-POST case to S4.

### 9. D2's case for refusing a hand submit is weak, and it is a dead end on a MEP
- **Section:** D2; §3 "The honest limit".
- **Defect:**
  - #176's reason for "never a refusal" is that "the user may have asked for exactly this job" (`notices.py:1678`, `:1715`). D2 does not answer it.
  - `SKILL.md:87` tells agents on compute-only facilities to `sbatch` from **inside the block**. Under D2(a) they are refused and pointed to `submit_job`, which is SSH-only.
  - A refusal from the lexer teaches weaker models to wrap the call in a script or `python -c`, where even today's advisory is lost.
  - The design concedes the meter counts any bypass anyway, so the refusal buys little.
- **Scenario:** A Polaris campaign envelope is open. The user asks for one long job past the block's walltime. The agent's `qsub` from the block is refused; `submit_job` refuses on a MEP; the agent retries as `bash -c "qsub …"`. The job runs, with no notice and no ledger row.
- **Evidence:** `notices.py:1672-1723`; `SKILL.md:87`; design §2 (Jobs mode SSH-only) and D2.
- **Status:** PLAUSIBLE.
- **Fix:** Offer the maintainer a third option, (c) **gate and adopt**: while an envelope is open, a recognised submit answers `needs_confirmation`, the spend floor's shape rather than a refusal. On confirmation it runs, and the receipt's id (`session_shell._JOB_ID`) is adopted into the ledger, where it is reserved, metered and cancelled by stop. This is the maintainer's call; the doc presents (a) as recommended without answering #176.

### 10. Results on the shared filesystem: the store defaults to `$HOME`, with no byte budget
- **Section:** §0.2; §4 "Every task runs inside a wrapper".
- **Defect:** `<campaign dir>` is never placed:
  - The Polaris entry's `scratch_root` is `$HOME/.hpc-bridge`.
  - The MEP's jobs mount only `filesystems=home` by default.
  - ALCF tells users to run jobs from project directories, not home.
  - Nothing caps the bytes a campaign writes. A full home breaks the user's logins and the UEP's own logs in `~/.globus_compute`, so tasks fail and the executor goes into bad state.
  - If `ShellFunction`'s walltime kills the wrapper, it cannot write `rc`.
- **Scenario:** 1,000 rows × 20 MB of stdout (the shape `batch_big_output` blesses) gives 20 GB in home. Home fills, and the user's Polaris UEP dies mid-campaign.
- **Evidence:** `alcf-polaris.yaml` (`scratch_root: "$HOME/.hpc-bridge"`); ALCF's Globus Compute page, line 99 (the `filesystems=home` default), and its Aurora running-jobs page, line 49 (both from the design's working copy of the ALCF docs, not kept in the repo); `SKILL.md:9` (node-local files don't survive).
- **Status:** PLAUSIBLE.
- **Fix:**
  - Put the campaign dir on a project or scratch filesystem, discovered and named in the terms, with the `filesystems=` directive to match.
  - Cap each task's output (`head -c`), and give the campaign a byte budget the wrapper checks (`df` or quota) before running.
  - The wrapper's own `timeout` must be shorter than `ShellFunction(walltime=)`, so the wrapper itself writes `124`.

### 11. "One task per live worker" is not knowable on a MEP, and its stated invariant is wrong
- **Section:** §0.1; §10 phase 1 ("The window is the block's workers").
- **Defect:**
  - **Live workers can't be observed on a MEP.** There is no login shape, and the server never learns the UEP's id before it starts. At start, a "live workers" count of zero admits nothing, so the rule has to be configured capacity.
  - **Blocks can start before any admission.** The registry seeds send `init_blocks: 1`, so each new pool UEP starts a block before anything is admitted. That contradicts "Blocks start only when the server admits work".
  - **The worker count may not be what the server assumes.** Polaris's default `max_workers_per_node` is 100 when it isn't rendered.
  - **Interactive work queues behind the batch.** In phase 1, an interactive `run_shell` waits behind batch tasks on the same workers.
  - **Fairness across agents is unspecified.**
- **Idle cost:** a one-per-worker window costs billed worker time on short tasks, because of the round trip and the SDK's own submit throttle (`executor.py:1036-1088`).

  | Task length | Worker utilisation |
  |---|---|
  | 5 s | 54–55% |
  | 10 s | 72–74% |
  | 30 s | 89–94% |
  | 60 s | 95–97% |
- **Evidence:** the window simulation probe (a working file of the design, not kept in the repo); ALCF's Globus Compute page, line 89 (default 100); the seeds' `init_blocks: 1`.
- **Status:** CONFIRMED (docs and probe).
- **Fix:**
  - Window = rendered capacity (`max_blocks × nodes × workers`).
  - `init_blocks: 0` for pools.
  - Reserve one worker for interactive work in phase 1.
  - Auto-chunk rows so each task runs for 60 s or more.
  - Round-robin admission per agent.

### 12. D4 overstates what IRI would buy
- **Section:** §5 ("IRI would give Polaris a stop that can answer `down`"); D4 ("the only way to a cancelling stop").
- **Defect:** Cancelling a MEP UEP's blocks does not end the UEP:
  - With a window task still queued, parsl requests another block (#204); with `min_blocks ≥ 1` it does so regardless.
  - A block that is still pending has run no task, so its job id never came back in a result. "The job ids that task results carry" therefore miss exactly the block that will start later.
  - `down` would need nothing left queued at the interchange, which a MEP can't show, plus a listing by UEP marker via IRI status.
- **Evidence:** `remote.py:966-972` (#204: the relaunch comes from a queued check); Parsl elasticity docs; design §5.
- **Status:** PLAUSIBLE.
- **Fix:** Reword D4 as "IRI makes a drain-then-cancel-idle-blocks stop possible". Extend S6: does a block cancelled while a window task is queued relaunch, and can IRI list jobs by their output path?

## Where nothing serious was found

- **mcp 1.30 lifespan per session:** correct (`lowlevel/server.py:663`; `streamable_http_manager.py:238` and `:367`). A minor correction: FastMCP *does* offer bearer auth (`token_verifier`, `fastmcp/server.py:164` and `:1003-1044`). HTTP sessions also idle out after 30 min by default, so `Core` must hold everything, which the design already does.
- **Code citations:** spot-checked and accurate (warmth 207-211, 616-670, 1880-1888 and 1898-1899; session_shell 18; config 117-118; scheduler_ops 33-61, 408 and 1573-1585; server 189-218, 546-556, 1030-1052 and 1890; notices 1672-1723; mep 145-151; remote 966-972 and 1031). The exception is `state.py`'s NFS claim (finding 4).
- **The registry's "four of five MEP-only":** true, since `ssh_host: null` holds for polaris, anvil, delta and globus-labs. But only Polaris documents the keys a campaign needs (Delta and Anvil wait on S7; globus-labs is not re-proven). D3's "four of five" overweights it.
- **Polaris's user-settable keys:** confirmed (`min_blocks`, `max_idletime` 240, `max_blocks` 1, `available_accelerators`, `drain_period`, `max_retries_on_system_failure`, `launcher_type`).
- **Globus limits:** quoted correctly (Context7): 20 requests per 10 s "from a client", results "abandoned" after 30 min, a 2-week TTL, and `reload_tasks` reattaches from another process.
- **Scoped SSH stop by `uep.<eid>.<uep_id>`:** feasible with the existing machinery (`scheduler_ops.py:1165`). One nit: cancelling the blocks before ending the UEP invites one relaunch, which the `down` checks would catch, at the cost of a re-stop.
- **Pool tokens multiplying spend:** each campaign is its own UEP, with its own blocks and its own terms, by design. There is no hidden multiplier beyond `init_blocks` (finding 11) and the per-envelope ceiling (finding 7).
- **Harness feasibility:** the planned additions cover the scenarios. Mid-run hooks fire only on tool results, so "after 5 results" needs a polling `cmd` hook; that is feasible.
- **ALCF's Jobs-into-Campaign collapse and the facility matrix:** consistent with the facilities research.

## Spikes: what breaks if one says "no", and what is missing

- **S1 no** (a second process cannot fetch a result the first one acked):
  - The collector must be the only consumer (the lease in finding 4).
  - A crash between the SDK's ack (about 0.5 s after receipt) and the ledger commit loses that result from the service.
  - Repair then reads the filesystem: free on SSH; on a MEP it is a billed task, and after a restart it needs fresh consent.
- **S2 no** (a result unfetched for 30 min is gone):
  - Anything that finished while no server ran is recoverable only from the filesystem.
  - Phase 1's `poll_task` by UUID across restarts (#13) then works only for gaps under 30 min, **unless interactive tasks also write rc and tail to the session dir**. The design never says they do.
- **Missing spikes:**
  - Two consumers on one task group (S1).
  - A `batch_run` POST retried after a timeout: does the service duplicate the batch? (S4)
  - A cancelled MEP block while a task is queued (S6).
  - Remaining walltime read from inside a block (cheap; finding 3).
  - Per-queue minimum charges are a catalog fact to collect, not a spike.

## Lesser notes

- §7 reuses the label "Decision D2" for the resource-requests D2, which collides with this doc's D2.
- `site_backstop_grptresmins` expects a "rejection". By default, `GrpTRESMins` leaves a job pending (`AssocGrp…Minutes`, `scheduler_ops.py:408`); it rejects only with `DenyOnLimit` or `safe`. The scenario should assert a pending block that never bills.
- The batch store keeps command text (by necessity). Say so beside the "never a command's text" rule for `sent-checks`.

## See also
[[Batch and swarm execution]] · [[Batch spikes 2026-10-30]]

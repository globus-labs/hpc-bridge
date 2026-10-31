# Batch and agent-swarm execution: design

> [!abstract] In one line
> Designed 2026-10-30. Phase 1 (the durable ledger and `run_batch` / `status` / `cancel` on the session's confirmed compute block) is BUILT on branch `feat/batch-phase1` (2026-10-31: [[batch]], [[ledger]]; "Phase 1 as built" below); phases 2–4 are designed, not built.

_Design lead's draft, 2026-10-30. Code references are to `fix/perlmutter-report` at 74fd47d. Library behaviour is from the installed `mcp` 1.30.0, `globus-compute-sdk` 4.18.0 and `globus_sdk`, plus the Globus Compute and Parsl docs (via Context7). Inputs: the three research reports (working files of the design, not kept in the repo), the adversarial review ([[Batch design review 2026-10-30]]), the vault, `HANDOFF.md`, `SKILL.md`, and loam's `hpc-deployment` skill (`develop`). No product code. **Revised the same day with the phase-0 spike results** (S1, S2, S4, S5, S7, S8 on a fake cluster and, for S7, read-only on Delta and Anvil): [[Batch spikes 2026-10-30]] holds the evidence._

## 0. The calls in brief

1. **The server holds the backlog. Globus Compute only ever sees a bounded window,** the pool's configured capacity (`max_blocks × nodes × workers`). Pools start with `init_blocks: 0`, and nothing is retried on a mere presumption of loss, so the window never overfills the interchange.
2. **A project or scratch filesystem, named in the terms, is the result store of record.** A Globus Compute result is a small notification. This removes the 10 MB limit and the dependence on the 30-minute window.
3. **Campaign is the main batch mode. Jobs mode is SSH-only:** today, Expanse and the facilities users bring themselves. Four of the five registry entries are MEP-only, and only Polaris renders a campaign's keys: Delta's template fixes a single block, and Anvil's renders block counts but not workers or accelerators (S7).
4. **Each pool gets its own user endpoint (UEP)**, made distinct by a pool token in its `user_endpoint_config` (UEC). Stop acts on that UEP only. On a MEP, a campaign can only drain.
5. **The envelope binds what hpc-bridge starts, not what an agent's code can do.** Any task can call `qsub`. On SSH facilities, any task can read the user's Compute refresh token from the seeded `storage.db` (`remote.py:656`, [[facility-remote]]). `restricted` is a convenience, not a boundary. A hard cap needs the site: an `sbank` suballocation, a Slurm `GrpTRESMins`, or a small dedicated account.

## 1. Goals and non-goals

**For:**
- **A single agent** running work longer than one block, or many independent tasks.
- **A swarm** sharing one pool and one budget. arXiv 2604.07681 runs one MCP server that fans out over Parsl.
- **An orchestrator such as loam,** which wants a validated endpoint and config and then runs its own agents.

**Goals:**
- One setup path for all modes.
- A budget approved once and enforced by the server for everything hpc-bridge starts.
- A ledger that survives a restart.
- A stop that never says `down` while something of ours can still bill.

**Non-goals:**
- DAGs beyond scheduler dependencies.
- Data staging and Globus Transfer.
- Placement inside a block beyond one task per worker.
- Cross-facility campaigns.
- MPI pools.
- Hosting agents (Academy's job).

The REPL stays the development loop.

## 2. The modes, revised by the evidence

| | **Interactive** (today) | **Jobs** | **Campaign** |
|---|---|---|---|
| Use when | Developing, probing | Work must outlive any endpoint: long walltime, MPI at scale, arrays, dependencies | Many tasks, fan-out and collect, resident agents |
| Facilities | All | **SSH only.** A MEP has no free status or cancel channel (#16) | All. On a MEP drain-only, shaped by what its template renders or fixes (§7) |
| Lifetime | One block, idle-released | The scheduler's; the ledger tracks it after the server exits | Until it is closed or its deadline passes (a deadline is required). No admission while no server runs |
| Spend | One confirmation per block (0.1.18) | Per job, or from an envelope | From an envelope |
| Pool | The session's block | n/a | **The session's block** (phase 1), or a **dedicated pool**: its own UEP, `max_blocks > 1`, workers pinned per GPU |

**On ALCF, Jobs mode collapses into Campaign.** Debug allows one job per user. `prod` needs at least 10 nodes on Polaris and at least 256 on Aurora, and arrays inherit those caps (the facilities research report's A1; a working file of the design, not kept in the repo). There, a campaign is one pilot block with fan-out inside it. `submit_job` reports the queue's limits (`qstat -Qf`) rather than refusing; the agent decides.

### Tool surface (new tools in bold)

Ids carry a type prefix (`job-`, `batch-`, `camp-`; a task is its Globus UUID), so one `status` and one `cancel` serve everything. That gives 18 tools; weaker models already struggle at 12.

| Tool | Key arguments | Returns |
|---|---|---|
| **`run_batch`** | `command` (a template over a row's fields, substituted by the server) or `script`; `rows`; `task_walltime_s` (**required**); `results_dir` (on project or scratch, named in the terms); `campaign_id` or `shape="compute"`; `session_id`; `idempotent=False` | `{batch_id, tasks, chunk, admitted, phase, block\|envelope, notice}` |
| **`status`** | `id`; `detail=summary\|failed\|page`; `offset`, `limit`; `wait_s` up to 60 (a long-poll) | **Batch:** `{counts:{backlog, in_flight, done, failed, lost, presumed_lost, submit_unknown, cancelled}, results:[…], blocks_seen, envelope}`. **Job:** `{state, exit_code, charged, reason, array}`. **Campaign:** pool, blocks, envelope |
| **`cancel`** | `id`; `mode=drain\|abort` | `{status: down\|draining, backlog_dropped, in_flight, blocks_left:[{job_id, bill_until, how_to_end}], notice}` |
| **`submit_job`** | `command\|script`; `session_id`; size; `array`; `depends_on`; `adopt=<native id>`; `confirm_spend` | `{phase, job, terms: JobTerms, envelope}` |
| **`open_campaign`** | `pool:{…, max_blocks, walltime, max_idletime}`; `envelope:{…, deadline}`; `confirm_spend` | `{phase, campaign_id, pool (as rendered), terms: CampaignTerms, envelope}` |
| **`export_handoff`** | `campaign_id` or a pool shape | `{record, path}` (§8) |

`poll_task` stays, and accepts a Globus UUID through the ledger. Interactive tasks also write rc and their output tail to the session directory, so a result survives a gap longer than the 30-minute window (spike S2).

**Rows are substituted in the server, then brace-escaped.** `ShellFunction` always runs `cmd.format(**kwargs)`, so any `{…}`, the wrapper's `${…}` included, must be doubled, as `runner.py:157` does ([[runner]]).

Batch tasks replay the session's `.cwd` and `.env` read-only and never write them back. The busy guard covers only sequential use (`warmth.py:1880-1888`, [[warmth]]).

**Promotion.** `submit_job(command, session_id=s)` writes a frozen `job.sh` to `<scratch>/jobs/<id>/`, hash in the ledger. The script carries the header built from the terms, including `--deadline`. It `cd`s to the session's cwd, sources its env (filtered, `session_shell.py:18`, [[session_shell]]), runs the command and writes `$?` to `exit_code` (PSI/J's rule). `run_batch(…, session_id=s)` does the same for each task. Promotion never carries consent across.

## 3. The budget envelope

**Dimensions:**
- **`cap`, in the facility's own unit.** Node-hours on ALCF and NERSC; SU on ACCESS.
- **`max_concurrent_blocks`.**
- **`max_jobs`**, counting array tasks. Facility limits are per user or per project.
- **`max_nodes_per_block`.**
- **Allowed `accounts` and `partitions`.**
- **`deadline`,** required, defaulting to 24 h or less, with `at_deadline` set to `drain` or `abort`. Every job and pool block carries the deadline (Slurm `--deadline`; PBS has no equivalent, so its walltime caps it there). The server's deadline is only soft under stdio, which ends with its client.

**Approval:**
- **A single-agent session works like today's floor** ([[Resource shapes & the spend floor]]). `needs_confirmation` carries the terms; the user's yes, relayed through `confirm_spend=True`, opens the envelope.
- **The envelope belongs to the session that approved it.** Any later session, a restarted stdio server included, asks again, naming what remains.
- **An operator ceiling** (`HPC_BRIDGE_ENVELOPE_MAX`) bounds the **sum** of open envelopes per facility and account.
- **In the shared service (§6), only the operator opens envelopes, shared by design.**
- *Alternative:* approval only out of band, as a signed token from the user's terminal. It is stronger, but adds friction to every single-agent use; keep it for a remote service.

**The meter never understates.** One call per tick, over the free login shape: `sacct -j <ids> -P -o JobID,State,ElapsedRaw,AllocTRES,Partition`, or PBS `qstat -x -f <ids>`.
- **Spent** = the job's `billing` TRES × elapsed time. Billing carries the site's partition weights; where it is absent, cores or memory share × the factor (#234's rule, [[Resource requests]]; not whole nodes for a slice). It is never below the queue's minimum charge, a catalog fact (Perlmutter `preempt`: at least 2 h).
- **A ledger job missing from `sacct` counts at its full walltime.** Fall back to `squeue`.
- **Committed** is the worst case still possible: a running job's remaining walltime, plus a queued job's full walltime.
- **Reserve before you admit.** A new job, or work that may start a block, needs *spent + committed + its worst case ≤ cap*. The check and the reservation happen in one `BEGIN IMMEDIATE` transaction.
- **Unledgered jobs count.** A sweep of `sacct -u $USER -S <opened>` finds them; they count, and the notice names them so the user can say "not mine".

**A MEP has no free meter.** The cap is checked against a bound:
- Before the deadline: `max_blocks × nodes × (deadline − open)`.
- After it: at most `⌈in-flight ÷ workers per block⌉` blocks × nodes × (`task_walltime_s` + `max_idletime`).
- Each block costs at least the queue's minimum charge.

This bound holds only because nothing is retried or refilled on a presumption (§4).

**Enforcement point.** The `_provision` floor (`warmth.py:616-670`), plus a new admission step. The server writes `max_blocks`, `init_blocks: 0`, `min_blocks`, `walltime`, `drain_period` and `--deadline` into the pool's UEC itself.

**The honest limit, stated in the terms.** The envelope binds what hpc-bridge starts. It cannot bind:
- **a shell or code:** an `sbatch` inside a script, or Python's `subprocess`, passes the lexer (`scheduler_ops.py:1573-1585`, [[scheduler_ops]]);
- **a stolen token:** on SSH facilities, any task can read `~/.globus_compute/storage.db`, which holds the user's Compute refresh token, and submit to any endpoint the identity reaches, the Polaris MEP included.

So the terms say "site backstop: none" unless the user names one. An ALCF `sbank` suballocation rejects jobs once it is exhausted. A Slurm `GrpTRESMins` leaves jobs pending (`AssocGrp…Minutes`, `scheduler_ops.py:408`).

**When the envelope is exhausted:**
- Admission stops. What is in flight finishes; it was already reserved.
- `submit_job` answers `needs_confirmation` with `reason: envelope_exhausted`.
- `status` names what still bills.
- Exhausted is not `down`.

## 4. The durable ledger

**Storage.** SQLite at `<state_dir>/ledger.sqlite3`, mode 0600, WAL mode, with `busy_timeout`.
- **Local filesystems only.** At open, a `statfs` check refuses nfs, lustre, gpfs, cifs and fuse, telling the user to point `HPC_BRIDGE_STATE_DIR` at a local disk. That matters on login nodes and next to loam. `SentCheckStore` (`state.py:180-336`, [[state]]) has no such check: it fails closed only when its `flock` times out or reports ENOLCK.
- **It sits alongside `SentCheckStore`.** The stop logic of #24, #204 and #208 is proven against that store; migrate its rows after a release cycle.
- **Batch scripts and rows are frozen** into `<state_dir>/batches/<id>/` (0600). Unlike `sent-checks`, this store holds command text, because a replay record must.

**One owner per campaign.** A lease row (host, pid, heartbeat), taken under `BEGIN IMMEDIATE`. Only the holder admits and collects. A takeover needs a stale heartbeat **and** a dead pid; orphaned servers happen (`harness:server_orphaned`).

**Every Executor, interactive ones included, gets its own task group, minted by the service.**
- **Minting.** A client-made id is refused (`404 TASK_GROUP_NOT_FOUND`), and that failed submit shut the Executor down (S8). Each Executor starts with `task_group_id = None`, which also keeps it off the class-wide default (`:320-325`). Its first submit mints the group (`executor.py:1227-1243`), and the ledger records it.
- **One consuming process per group.** SDK 4.18's watcher acks every message on the group's queue, matched or not (`:1660-1676`). Two processes on one group swallowed 7 of 16 results (S1).
- **`Executor.shutdown()` does not stop that watcher.** It lives until the process exits, and open futures keep the process alive (S5).

**Entities:**
- `envelope`: dimensions, owning session or `operator`, state.
- `campaign`: UEC and hash, pool token, `task_group_id`, lease, UEP id once seen, state, terms.
- `batch`: submitter, script hash, `task_walltime_s`, chunk size, `idempotent`, `results_dir`.
- `task`: row(s), attempt, Globus UUID, state; rc, tail, pointer, the block's `job_id`, host, pin, the block's remaining walltime; `settled_by` with its evidence.
- `job`: native id, array, script hash, size, state, exit code, `charged`, `committed`, source (`submit_job`, `adopt` or `pool_block`).
- `meter_reading`, and an append-only `event` log, which is also the trace source for the Python API.

**Submitting.** The Executor gets a wrapped `client=` with a shared token bucket, **as a courtesy**. The documented "20 requests per 10 s" was not enforced at 30 GETs in 1.9 s or 22 POSTs in 3.1 s, and responses carry no rate-limit headers (S4). A 429 counts as pressure on the whole identity.

The `globus_sdk` transport retries the `batch_run` POST on network errors, 429 and 5xx with no idempotency key (`default_retry_checks.py:17-28`). **S4 saw the duplicate:** a lost response, retried, ran the batch twice while the client knew one. One error that survives the retries shuts the Executor down.

So any submit error marks its rows **`submit_unknown`**:
- they are reconciled with `get_task_group` (`executor.py:846`) before re-admission;
- non-idempotent rows are never resubmitted automatically;
- the wrapper's `attempt-<n>` claim turns a duplicate into an early exit.

**The wrapper,** around every task:
- Claims `<results_dir>/tasks/<id>/attempt-<n>/` with an atomic `mkdir`; the first writer wins, and a duplicate exits.
- Checks a campaign byte budget (`df`/quota) and caps each stream (`head -c`).
- Runs under its own `timeout`, shorter than `ShellFunction(walltime=)`, so it always writes `rc` (124 on time-out).
- Returns small JSON: rc, a ~4 kB tail, paths, the block's job id, host, pin and `PARSL_WORKER_COUNT`, and the block's remaining walltime, read once per block and cached on node-local `/tmp` (S8):
  - **Slurm:** `SLURM_JOB_END_TIME − now`, exact with no RPC; `%L` (+1 s) is the fallback.
  - **PBS:** `stime + Resource_List.walltime − now` from `qstat -f`, to 1 s; never `resources_used.walltime`, which lags.
  - **No job id:** `unknown`; never `squeue -j ""`, which returns rc 0 and nothing.

**`results_dir` is on project or scratch space, never `$HOME`.** The Polaris entry's `scratch_root` is `$HOME/.hpc-bridge`; its MEP mounts only `filesystems=home` by default; ALCF says to run from project directories; and a full home kills the UEP's own logs. The terms name the directory and its `filesystems=` directive.

**The collector.** Results arrive pushed over AMQP for the lease holder.
- **After a restart** it pulls with the raw `v2.get_task_batch`, in chunks. `Client.get_batch_result` silently drops failed tasks (`client.py:473-478`, S4).
- **Crash repair is a REST pull.** An AMQP ack does not remove the REST copy, and a REST read is not destructive (S1, at +1 and +60 min). A crash between an ack and the ledger commit costs one free REST pull, on a MEP too.
- **Unfetched results stayed readable** past the 30-minute "abandoned" mark ([limits](https://globus-compute.readthedocs.io/en/latest/limits.html); S2).
- **`<results_dir>` stays the store of record,** for the 10 MB limit and the 2-week TTL.

**Settling "pending" tasks.** A dead block fails its running tasks with `ManagerLost`, about 105 s after it dies, while its UEP lives (S8). A dead UEP settles nothing: its tasks stay `pending` at the service (S5: still `unknown` after 25 min), so orphans are settled `lost` only on evidence, never by waiting. Parsl's bad state fails every task, and the result publisher can drop those failures (#2217; the fix, globus-compute PR #2218, is still open). In order:
1. **A result wins whenever it arrives.**
2. **On SSH, `lost` needs evidence:** the start marker names a job that `sacct` shows ended, or the manager log shows the pool UEP's exit with nothing after it (`_settle_by_evidence`). `status` never waits for the service.
3. **On a MEP, `presumed_lost` is a clock that starts at evidence that a block started,** such as a sibling result naming its job, never at submit. Today's clock runs from submit and warns that such a task "may be lost — or only queued" (`warmth.py:138-155`). A presumed task **keeps its window slot**, and is **never retried**. The presumption only unblocks stop and detach, as today.
4. **Retries only for `idempotent=True` batches, and only on evidence:** a `ManagerLost` or `WorkerLost` result, or SSH `lost`. Parsl's `max_retries_on_system_failure` stays 0.

_Contested, in part (review finding 1): the review holds a presumed slot until a result arrives or the 2-week TTL passes. A campaign whose window is all presumed-lost would then stall for two weeks. I add a third way out: `status` reports `stalled`, and the user may release those slots through a call that carries `confirm_spend` and terms naming the tail it can add._

**Recovery after a restart.** The lease holder rebuilds the in-flight set, pulls, then settles. **Admission resumes only through a call that carries `confirm_spend`**: for a phase-1 batch, because `spend_confirmed` lived in memory (`warmth.py:657`); for a campaign, because the envelope belongs to its session. A plain `status` never resumes spend. The service resumes operator envelopes by itself.

## 5. Ending a campaign honestly

Today's stop ([[Cost control]]; #24, #204, #208) assumes it can wait for every task to resolve. **The bounded window, and the rule that nothing is retried on a presumption, make that assumption true again.** `stop_endpoint` and `cancel` first **pause admission under `app.lock`**, before anything is released.

**Verbs:**
- **`drain`:** stop admitting. In-flight tasks finish, the backlog becomes `paused`, and the pool idle-releases.
- **`abort`:** drop the backlog as `cancelled`, then end what is in flight.

**SSH, scoped to the campaign's UEP** (S5):
1. **Retire the pool token** in the ledger, checked on every admission path. A stray submit with a retired config restarted its UEP and a block within 18 s.
2. **SIGTERM the UEP,** found by its `endpoint_setup` line `hpcb_pool=<id>` (like `hpcb_block`, `remote.py:966-972`). It cancels its own blocks.
3. **`scancel` any `uep.<eid>.<uep_id>` job left in `squeue`.** Never `uep.<eid>`, which hits every UEP of the endpoint (`scheduler_ops.py:33-61`).
4. **Settle in-flight tasks `lost` by evidence.** The service leaves them pending.

`down` requires: no UEP job in `squeue`; the manager log shows the exit and no revival; the token retired. Otherwise the answer is `draining`. S5 saw no relaunch in 10 min in either order.

**MEP: `drain` only, and `draining` is final** (`_stop_mep`, `server.py:1030-1052`, [[server]]). The SDK cannot cancel another party's task, and a block cancelling itself was rejected in #178. The answer names:
- the block jobs that results reported;
- the bound from §3;
- how each block ends: `scancel`/`qdel` for a block with `min_blocks` 0; a `min_blocks ≥ 1` pool gets the warning in §8.

**Guards that hold in every mode, phase 1 included:**
- **The admission gate.** A task is admitted only while the block's remaining walltime ≥ `task_walltime_s` + a margin, as the canary and every result report it from inside the block (S8); `unknown` admits nothing. It is never taken from `block_since`, which on a MEP's pre-spawned block is later than the real start, and the reap presumption fires only past walltime (`warmth.py:424-432`). Without the gate, a task admitted as the block ends waits at the interchange, and parsl starts a block nobody confirmed (#204's pattern).
- **`min_blocks = 0` on a MEP.** Polaris lets users set `min_blocks` and `max_idletime` (ALCF's option table).
- **`task_walltime_s` is required.** It must be ≤ block walltime − `drain_period` − a margin.

**A facility REST API (D4)** would make **drain, then cancel the idle blocks** possible on a MEP:
- ALCF IRI has `DELETE /api/v1/compute/cancel/{resource}/{job_id}`, marked "not live-tested".
- The NERSC SF API is similar.

It cannot end the UEP. A queued window task, or `min_blocks ≥ 1`, relaunches a block, and a block still pending has returned no job id. So `down` on a MEP would also need a job listing by the UEP's output path, plus evidence that nothing is queued (S6).

## 6. Sharing one pool across agents

**Process model.** FastMCP enters the lifespan in each `Server.run` (`mcp/server/lowlevel/server.py:663`), once per HTTP session (`streamable_http_manager.py:367`) or per request when stateless (`:238`). Sessions also idle out after 30 minutes by default. So the shared state can't live in an MCP session. **Decided (D1, 2026-10-30): a local daemon behind thin stdio proxies.**
- **One per-user `hpc-bridge` daemon** builds one `Core`: binding, pools, ledger, submitter, collector, leases. The first stdio proxy that needs it starts it, detached, and it outlives the chats, so the collector keeps fetching inside the 30-minute window with no agent connected.
- **Every client keeps today's stdio config.** The stdio server becomes a thin proxy to the daemon over a Unix socket (mode 0600, under `~/.hpc-bridge/`). Only the user's own processes can connect, and nothing is exposed on the network.
- **An `AgentView`** over the `Core` is keyed by the proxy's agent id, not by an MCP session.
- **What it costs:**
  - a versioned proxy↔daemon protocol;
  - an upgrade path when the plugin updates while an old daemon runs (the proxy checks the daemon's version and restarts it once nothing is in flight);
  - stale-socket cleanup.
- **HTTP later:** the `Core` can take a second front end (`serve --http`, FastMCP's bearer auth) when agents on other machines need it. Loam and Hermes speak HTTP MCP, so that path stays open.

**The rules:**
- **One submitter per Globus identity.** One `Executor` per pool UEC, owned by `Core`, so the rate budget is shared and concurrent UEP starts avoid `RESOURCE_CONFLICT`. S4 hit it in 3 of 3 two-process first-submits; the loser gets a 409 within a second. A pool's own race never counts toward three-strikes `down` (`server.py:546-556`).
- **Fair admission.** Each campaign's window is shared round-robin across the agents submitting to it.
- **One UEC per campaign, fixed when it opens.** A distinct UEC is a new UEP with its own blocks. The pool token makes it distinct. The service keys a UEP on the request's config (Globus docs), so any value that passes the schema works: any key on a permissive schema (Polaris, Delta), `endpoint_setup` or `worker_init` on a strict one (Anvil), our own template on SSH. S3 confirms it with one task.
- **Stop is scoped to the campaign** (§5).
- **Auth.** The Unix socket (0600) admits only the user's own processes. The later HTTP front end takes FastMCP's bearer auth (`token_verifier`, `fastmcp/server.py:164`), bound to 127.0.0.1 unless opted out, with a warning.
- **Isolation is not security.** Every agent of the service holds the user's full Compute identity, in effect: it runs code as the user's mapped account, can `scancel -u $USER`, can read another campaign's results, and can read the token (§3). `restricted` (`run_code` plus a bounded `read_file`) narrows the surface for the agent's convenience. It is not a boundary. Longer term: a narrower identity for the facility-side manager.

## 7. Templates and pools

**Shape.** One-node blocks with one worker per GPU, where the template renders the keys (Polaris; not Delta or Anvil, S7):
- `available_accelerators` = the node's GPUs, which sets `CUDA_VISIBLE_DEVICES`, `ROCR_VISIBLE_DEVICES` or `ZE_AFFINITY_MASK`;
- `max_workers_per_node` to match;
- `max_blocks` = nodes;
- `init_blocks: 0`.

This avoids multi-node launchers (the resource-requests decision gives interactive multi-node blocks `SimpleLauncher`, `resources.py`; a nested `mpiexec` crashes on Aurora). Where queues allow one job per user (ALCF debug and `capacity`), discovery reports the limit, and the pool proposes one multi-node block: `SrunLauncher` on Slurm, `MpiExecLauncher` on Polaris. Aurora gets one node in phase 3. A worker cannot be addressed directly.

**The window** is the configured capacity, from values the template renders or fixes.
- An unknown worker count counts as 1 until the first result reports the block's `PARSL_WORKER_COUNT` (S8).
- Templates fix what the server cannot set: 4 workers per node on Delta and 128 on Anvil, bounded by the cores parsl sees (S7). Polaris defaults to 100.

**Interactive work** keeps one worker where the block's observed count is two or more. Where it is one, the batch and interactive calls share it, and the notice says interactive calls queue behind the batch.

**Chunking.** Rows are chunked so a task runs for about 60 s or more. The first tasks run one row each, then the chunk size adapts to their measured length. The review's probe (a window simulation; a working file of the design, not kept in the repo) puts utilisation at about 55% for 5 s tasks and about 96% for 60 s tasks, under the SDK's own submit throttle.

**Settings:**
- **`min_blocks`:** 0. Above 0 only on SSH, under an envelope.
- **`max_idletime`:** 120 s on SSH; the facility's default on a MEP. That is 240 s on Polaris, and the engine's 120 s on Delta and Anvil, whose templates leave `job_status_kwargs` unset; their UEPs exit after 300 s idle (S7). Never shorter than the queue's minimum charge, which would otherwise be paid again on each new block.
- **`drain_period`:** walltime − `task_walltime_s` − a margin.
- **Parsl's default block-error handler stays.** It trips only when every block has failed, which ends a broken configuration fast instead of looping (ALCF's "runaway job submission").

**The MEP gate (S7).** The spend bound needs known values, not settable ones.
- **Known, not necessarily rendered:** `max_blocks`, `min_blocks`, `init_blocks`, walltime, nodes per block and the idle release. Each is rendered from our UEC or fixed in the template (a literal, or a `default(…)` we never override). A `template_value(key)` reads them beside `renders()`, and the schema's `maximum` is the ceiling.
- **Rendered:** only what scales out (`max_blocks`, `min_blocks`) or pins (`available_accelerators`, `max_workers_per_node`).
- **Everywhere:** an account, a queue and a place for the token.
- **So:**
  - Polaris: pinned pools.
  - Anvil: CPU pools of ≤12 blocks × ≤2 nodes, unpinned, with the token in `endpoint_setup` or `worker_init`.
  - Delta: one fixed block, drain-only, after a re-prove (its endpoint is now gce 4.19.0).
  - globus-labs: after its re-prove.

`renders()` (`mep.py:145-151`, [[facility-mep]]) checks each key against the template text and refuses by name; it never drops a key silently. On SSH, a `takes_pools` marker line plays the part `takes_resources` plays today.

**State machine.** Pools get their own `pool.py`, not `ShapeRuntime`. `_single_block` (`warmth.py:207-211`) switches the reap and guard logic off once `max_blocks > 1`.

## 8. Python API and the loam handoff

**Python API (phase 4).** `hpc_bridge.api.Session` builds `AppCtx` as the lifespan does (`server.py:189-218`). It logs in through paste mode and writes the same journal and ledger events. In code, `confirm_spend=True` is the caller's own consent.

**`hpc-bridge.handoff`, version 1** (JSON), the serialised return value of the API:

| Field | Content |
|---|---|
| `schema`, `version`, `created_at`, `expires_at`, `hpc_bridge_version` | Unknown fields are ignored; a breaking change makes v2 |
| `facility`, `endpoint` | `id`, `system`, `scheduler`, `access`; `manager_id`, `kind`, `uep_id_seen` (it differs from the manager by design) |
| `user_endpoint_config` | **Wire-ready**: after `key_map` and schema fitting |
| `template`, `worker`, `client_requirements` | `sha256` and `renders` per key; the versions the canary saw; the client Python required (Polaris workers run 3.13) |
| `terms` | Account, queue, size, walltime, `min_blocks`/`max_blocks`, `max_idletime`, `worst_case` in the facility's unit, `approved_at`, **`enforced_by: "uec-bounds-only"`** |
| `validation` | Canary `{ok, job_id, host, device_pin}`; identity mapped; queue checks (Aurora `debug-scaling` needs ≥2 nodes) |
| `release` | The end that really works |

**`export_handoff`** validates **loam's exact UEC**, since any other UEC is a different UEP. That validation is one billed canary, so the terms come first.

**`min_blocks ≥ 1` on a MEP is refused until spike S3 proves the UEP exits when idle.** Parsl replaces a cancelled block to keep its minimum, so `qdel` ends nothing, and loam relies on exactly that ("`min_blocks=1` makes a debug block replace itself"). D6 asks whether to allow it with `worst_case: unbounded` instead. Either way, `release` names the end that works: the proven idle exit, or the user removing `~/.globus_compute/*/daemon.pid` on the facility (ALCF's remedy). Never `qdel` alone.

**What loam changes:**
- generate `set_env.sh` from the record (`HPC_GLOBUS_ENDPOINT_ID`, `HPC_ENDPOINT_CONFIG_JSON`, `HPC_NNODES`, `HPC_SYSTEM`);
- drop the SSH discovery steps and `QUEUE_LIMITS`;
- have preflight check `template.sha256` and `expires_at`.

loam's restricted surface stays. Later, its agents move into a campaign pool (option 2).

## 9. Facility compatibility matrix

| Facility (access, scheduler) | Interactive | Jobs | Campaign | Handoff | Blockers and notes |
|---|---|---|---|---|---|
| **globus-labs** (MEP, Slurm) | Not re-proven since the rebuild; a 3.12 client | No | After the re-prove; at most 4 blocks and 20 workers | Yes | The test bed |
| **Delta** (MEP, Slurm) | Yes, after a re-prove (endpoint now gce 4.19.0) | No | One fixed block, drain-only (S7) | Yes | 48 h hard idle; starts at least 3 min apart; 4 workers per node, fixed |
| **Anvil** (MEP, Slurm) | Yes | No; yes as bring-your-own SSH | CPU pools, unpinned, ≤12 blocks × ≤2 nodes (S7); strict schema | Unknown | Undocumented, back online 2026-10-30; 128 workers per node, fixed; login nodes for "smallest test" only |
| **Polaris** (MEP, PBS) | Yes | No; later through IRI (D4) | Yes, documented keys. Debug and `capacity`: 1 block; `preemptable`: up to 20, idempotent tasks only | **The first target**; `min_blocks` 0 until S3 | Client Python 3.13; results on `/lus/…` project space, not home |
| **Expanse** (SSH, Slurm) | Yes | Yes (`compute`: 32 running and 64 submitted per group) | Yes | Yes | **Login-node policy**; "ask SDSC before mass submission" |
| **Perlmutter** (bring-your-own SSH, OTP, Slurm) | Yes (#236) | Yes (5,000 submitted; arrays) | Yes | Yes | `preempt` has a 2 h minimum charge; login caps; workflow QOS by request |
| **Aurora** (SSH plus bastion and MFA, PBS) | Proven; no allocation | One job at a time = Campaign | One node (`capacity`, 168 h) | Yes | No MEP; `sbank` suballocation as backstop |

**Login-node policy is a current-product issue.** Our SSH bootstrap leaves a persistent endpoint manager on the login node, and every UEP's interchange runs there too. Expanse, the registry's only SSH entry, says login nodes are not to be used "as hosts for running workflow management tools". NERSC's sanctioned home for a long-lived coordinator is the workflow QOS (by request); Anvil forbids anything beyond a small test; ALCF wants "explicit user awareness". A campaign makes this worse, because the interchange's load scales with the task rate (D5).

## 10. Phasing

**Phase 0: spikes S1, S2, S4, S5, S7 and S8, run 2026-10-30** (§11, [[Batch spikes 2026-10-30]]). S3 (on Polaris) and S6 remain.

### Phase 1, the first shippable slice: the ledger and `run_batch` on the session's block

**Scope:**
- The ledger, with a campaign lease, `statfs` refusal, atomic reservations, `submit_unknown` and explicit task groups.
- The collector and the wrapper (attempt claim, output cap, byte budget, its own timeout).
- `poll_task` by UUID, with session-dir results.
- `run_batch`, `status` and `cancel` on the confirmed block: on SSH and MEP, with the admission gate; pause under `app.lock` on stop.

No new spend semantics. A block that ends pauses the batch at `needs_confirmation` (0.1.18). Resuming takes `confirm_spend`.

**Failing-first unit tests:** `test_lease_single_admitter`, `test_reservation_atomic`, `test_statfs_refuses_network_fs`, `test_submit_unknown_never_resubmitted`, `test_attempt_dir_first_writer_wins`, `test_presumed_keeps_slot_never_retried`, `test_mep_clock_starts_at_block_evidence`, `test_admission_gate_remaining_walltime`, `test_stop_pauses_admission_under_lock`, `test_task_group_service_minted_per_executor`, `test_collector_reads_raw_task_batch`, `test_remaining_walltime_end_time_cached`, `test_retired_token_refused_everywhere`.

**Agentic scenarios:**
- **`batch_fanout_collect`** (fake `default`), 40 rows, 3 seeded to fail. Asserts: one confirmation and one job in `sacct`; `run_batch` used, not 40 `run_shell` calls; the journal never shows more in flight than capacity; the 3 failures reported with their rc; stop answers `down`.
- **`batch_crosses_walltime`**: a 4-minute block, 60 s tasks, 30 rows. No task is admitted with remaining walltime under 60 s + the margin. `sacct` shows no second job before the re-ask. The world check `stop_honesty_no_pilot_left` passes.
- **`batch_restart_resume`** (PHASES): the server is killed mid-run. The new server's `status` returns every row, with no row run twice. Admission resumes only after a re-ask.
- **`batch_two_servers_one_ledger`**: two servers on one state directory. Only the lease holder admits. The window never doubles.
- **`batch_block_reaped_midrun`**: a polling `cmd` hook `scancel`s the block after 5 results (mid-run hooks fire only on tool results). In-flight tasks become `lost` with evidence; the backlog survives; the re-ask names the reap.
- **`mep_batch_fanout`** (fake `mep`): stop answers final `draining` and names the block's jobs; nothing is sent after the stop.
- **`batch_big_output`**: stdout over the cap is truncated, with a pointer, and no `MaxResultSizeExceeded`.

**Harness work:** `ops.restart_server()`; two servers on one state directory; the polling hook; journal entries for the new tools; `_billed_start_idxs` learns `run_batch`; a `world:ledger_matches_sacct` check.

#### Phase 1 as built (2026-10-31, branch `feat/batch-phase1`)

Modules [[batch]] and [[ledger]]; tools in [[The MCP tools]]. Built test-first: `tests/test_batch_phase1.py` (strict xfails on main, green with the build), `agentic/harness/batch_checks.py` with `test_batch_scenarios.py`, and eight scenarios (the seven above plus `batch_abort_midrun`). Where §2 left a choice open, or the build met the world:
- **`run_batch(command, rows, task_walltime_s, results_dir=None, session_id="default", shape="compute", idempotent=False, confirm_spend=False, batch_id=None)`.** `confirm_spend` is the same floor as `ensure_endpoint_up`'s (one block, the same terms); `batch_id` with `confirm_spend=True` resumes a batch that asked again. No `script` (a `command` may be multi-line) and no `campaign_id` (phase 3). Only `{name}` for a row's own identifier keys, `{index}`, and `{row}` for a scalar row are substituted, shell-quoted; any other brace stays (`${HOME}`, `awk '{print $1}'`); the whole script is brace-escaped for ShellFunction after. A `task_walltime_s` above the block's walltime less the 30 s margin is refused.
- **`status(id, detail="summary"|"failed"|"page", offset=0, limit=50, wait_s=0)`** and **`cancel(id, mode="drain"|"abort")`**, `batch-` ids only for now. Phases: `admitting`, `waiting`, `needs_confirmation` (every resumable pause, with a `reason`: the walltime gate, a reap, a stop, a restart, a drain), `needs_account`, `done`, `cancelled`, `refused`.
- **The window** is `max(1, workers - 1)`, an unknown count counting 1; chunk 1. On the fake `default` profile's two workers the window is 1; on a one-worker block an interactive command waits behind the batch task, its answer naming the batch.
- **`cancel`**: `drain` keeps the backlog (resumable); `abort` cancels it and ENDS what is in flight — the `CANCELLED` marker in the results directory (written over the login shape; a small task on a MEP's block), which each wrapper watches every second, killing its command's process group (rc 143). The block itself is never cancelled (it is the session's). `down` means nothing of the batch is in flight; `blocks_left` names the session's block and how it ends. Any process may cancel; only the lease holder admits.
- **The ledger** (`<state_dir>/ledger.sqlite3`): tables `batch`, `task`, `retired`, `itask`, `event`; a lease per batch, stale after 30 s without a heartbeat and taken over only from a dead pid on this host; a retirement keyed on the endpoint and the dispatch config's hash, lifted by the user's yes on a resume.
- **The walltime gate meets the next block (live, `batch_crosses_walltime`).** The user's yes at a walltime pause usually comes while a row still runs on a block with too little left for another. The yes covers the NEXT block: the batch waits for its tasks out, then — on an endpoint hpc-bridge stood up (`fresh_ueps`) — drops the shape and confirms a fresh block (a new user endpoint), whose first check dates it. On a facility MEP (one user endpoint, max_blocks 1) the next block can start only after the old one ends, and the batch asks again then: a known phase-1 limit.
- **A restart (live, `batch_restart_resume`).** A server killed right after admitting a row may die before the service's answer gives that row a task id. The batch's own task group is on the record as soon as the service mints it; the new server reconciles against it: a task the ledger does not know settles the row its result names, and a row with no task in the group was never sent and goes back to the backlog.
- **A reap (live, `batch_block_reaped_midrun`).** Evidence about a job settles only the tasks sent to it; a new confirmation forgets the old block and gets a batch runner of its own (the batch had kept sending to the reaped block's user endpoint); and for five minutes the reaped block's user endpoint has its pilots cancelled by its own marker — parsl started one for the task that was out on the dead block, which nobody confirmed.
- **The gate asks for no block when nothing is left to admit** (live: the last row's crossing started a block nothing ran on). Passes are serialised per batch (a status call's pass met the loop's switch and asked again).
- **A reap the batch reported is answered by one yes.** The batch's pause names the reap, so the resume's confirmation is the new block's; warmth does not ask a second time.
- **A stop counts every ledger batch on the endpoint**, not only this process's: tasks a killed server sent run on, and releasing the block under them would have the endpoint relaunch one.
- **A result's tail is the stream's real end**, kept beside the capped head (1 MB per stream).
- **The scenarios' sizes**: `batch_crosses_walltime` takes 9 rows, not 30 (window 1); `batch_restart_resume` is one scripted session with a SIGKILL, not a PHASES chain; `batch_block_reaped_midrun` is scripted (its polling hook would serve an agent operator too).
- **On main**, a scenario's script falls back to the rows as concurrent `run_shell` calls writing the same result files, so each cell failed on its own graders: no window, no walltime gate (a second pilot with no re-ask), nothing durable after a restart, the result limit met.
- **The sweep** runs `batch_fanout_collect` in the `default` agent wave and the scripted cells once per sweep (`sweep_profiles.sh` `batch_cells`).

### Phase 2: the envelope, Jobs mode on SSH, the loam handoff

**Scope:**
- The envelope: required deadline, session binding, the ceiling on the sum.
- The billing-TRES meter, with minimum charges gathered into the catalog.
- `submit_job` with arrays, `--deadline` and promotion.
- The D2 outcome.
- `export_handoff` v1, with the MEP `min_blocks` rule. The handoff can ship independently, right after phase 1.

**Failing-first scenarios:**
- **`job_promote_track_cancel`**: cwd and env carry over; one `sacct` call; `cancel` answers `down`.
- **`job_array_terms`**: the worst case is 20 × size, not the throttle's 4.
- **`envelope_exhausted`**: the agent asks rather than splitting the work.
- **`hand_submit_gated_adopted`** (if D2(c)): a recognised `qsub` asks first, then is adopted, reserved and cancelled by stop.
- **`site_backstop_grptresmins`**: the block stays pending under `AssocGrp…Minutes` and never bills.
- Also: `envelope_new_session_reasks`, `envelope_ceiling_sums`, `envelope_bypass_counted`, `meter_missing_from_sacct_full_walltime`, `handoff_polaris_shape`, `handoff_mep_min_blocks_refused`.

**Harness work:** an allowlist in `stop_honesty_no_pilot_left` for jobs meant to keep running; `_hand_submit_idxs` learns `submit_job`; a `world:charged_within_envelope` check.

### Phase 3: campaign pools

**Scope:**
- `open_campaign`, with the pool token and its UEP, in `pool.py`;
- `init_blocks: 0`, a window sized to capacity, chunking, per-GPU pinning, `drain_period`;
- scoped abort on SSH; drain-only on a MEP;
- `renders()` gating, Polaris first.

**Failing-first scenarios:**
- **`campaign_pool_scale_out`**: at most 2 blocks; distinct `CUDA_VISIBLE_DEVICES` per worker; drain ends `down`.
- **`campaign_abort_with_backlog`**: no job is submitted for 3 minutes after the abort; the UEP's exit is logged.
- **`campaign_scoped_abort`**: B's job and the interactive block are still `RUNNING`.
- **`campaign_no_block_before_admission`**.
- **`mep_campaign_drain_only`**.
- **`mep_campaign_min_blocks_refused`**.
- **`campaign_stale_template_refused`**.

### Phase 4: shared service, restricted profile, Python API

**Scope:**
- the per-user daemon and thin stdio proxies (D1): started on demand, Unix socket, version check and restart, `AgentView`s keyed by the proxy's agent id;
- round-robin admission;
- the `restricted` profile, described as a convenience;
- `hpc_bridge.api`;
- loam option 2.

**Failing-first scenarios:**
- **`swarm_two_agents_one_pool`**: one UEP; no `RESOURCE_CONFLICT`; fair shares.
- **`agent_survives_session_timeout`**.
- **`daemon_outlives_chat_collects`**: the last proxy exits mid-campaign; results that arrive later are still collected inside the 30-minute window.
- **`daemon_upgrade_mid_campaign`**: a newer proxy meets an older daemon; nothing is lost and nothing double-admits.
- **`service_restart_resumes_operator_envelope`**.

**Harness work:** a multi-client operator. Run quota-heavy cells `SERIAL`, with trivial tasks, under the shared identity.

## 11. Spikes

Run 2026-10-30 on a fake cluster unless noted; evidence in [[Batch spikes 2026-10-30]].

1. **S1 (run):** does an AMQP ack remove the REST copy, and what do two consumers on one group do? The copy survives (+1 and +60 min, re-reads too); two processes swallowed 7 of each other's 16 results, and REST recovered them.
2. **S2 (run).** Unfetched results by REST and by AMQP at 35, 60 and 180 min after completion: all readable by both channels at +34 and +57 min. The +180 min read was not run (the stack was pulled down for the night), so survival past an hour stays open; nothing in the design rests on it.
3. **S3 (Polaris, open).**
   - Does an unknown key give a distinct UEP? The docs say the service keys a UEP on the request's config, so this is a one-task confirmation.
   - Which keys render: a free, read-only metadata read, as in S7.
   - Does a UEP with `min_blocks > 0` exit when idle (D6)?
4. **S4 (run).**
   - **A lost-response POST retried by the SDK ran twice,** and the client knew one.
   - **`RESOURCE_CONFLICT`** in 3 of 3 two-process first-submits.
   - **Rate limit:** no 429 within 30 GETs in 1.9 s or 22 POSTs in 3.1 s, and no rate-limit headers; the scope is unknown.
5. **S5 (run).** An SSH abort stays ended in either order, and a SIGTERM'd UEP cancels its own blocks. Orphaned tasks stay `pending`. A stray submit with the retired config revives the UEP in 18 s.
6. **S6 (open).** Through ALCF IRI, on a block the Polaris MEP started:
   - Can IRI list and cancel it, and list jobs by output path?
   - Does a block cancelled while a window task is queued relaunch?
   - Which consent scope does IRI need?
7. **S7 (run, read-only).** Delta fixes a single block and 4 workers; Anvil renders block counts but fixes 128 workers; neither renders accelerators, drain or idle (§7's gate).
8. **S8 (run, Slurm and PBS).**
   - `SLURM_JOB_END_TIME` is exact, and `%L` is within +1 s.
   - PBS `stime` + walltime is good to 1 s from the compute node; `resources_used.walltime` lags.
   - A task outliving its block returns `ManagerLost` about 105 s after the block dies.

## 12. Risks and decisions

**Risks** not covered above: parsl skew on MEPs (Polaris requires exactly `2026.02.23`); the user's token readable by any task (stated, not fixed); facility limits shared with people (explain a rejection, never retry it); test cost and quotas (hermetic tests first, `SERIAL` cells).

**Decisions for the maintainer:**

**D1. The process model for many agents.**
- (a) `serve --http` with a shared `Core` and FastMCP's bearer auth.
- (b) A local daemon behind thin stdio proxies.

**Decided (b), 2026-10-30.** Every client we test keeps stdio. The daemon starts on demand and outlives the chats, so the collector runs with no agent connected. A Unix socket keeps the daemon off the network. HTTP can be added to the same `Core` later, for agents on other hosts (§6).

**D2. A recognised hand submit while an envelope is open.**
- (a) Refuse it.
- (b) Keep #176's advisory.
- (c) Gate and adopt: answer `needs_confirmation`; on a yes it runs, and its id is put in the ledger, reserved, metered (on SSH) and cancelled by stop.

**Decided (c), 2026-10-30.** It answers #176's "the user may have asked for exactly this job" (`notices.py:1678`, [[notices]]). It keeps `SKILL.md:87`'s in-block `sbatch` on compute-only facilities working, where a MEP-adopted job is reserved at its full requested size. And it does not teach models to hide a submit inside `bash -c`.

**D3. Campaign on MEPs, which have no cancel channel.**
- (a) Offer it drain-only, per facility, once `renders()` proves the keys: Polaris now; Delta and Anvil after S7; globus-labs after its re-prove.
- (b) Withhold it until a cancel channel exists.

**Taken (a), 2026-10-30.** `draining` is already final there, and the bound in §3 is honest.

**D4. Facility REST APIs (ALCF IRI, NERSC SF API).** They would enable "drain, then cancel the idle blocks" on a MEP, not ending the endpoint, and they need a new consent scope.
- (a) Spike S6 now; build it opt-in after V1's consent work.
- (b) Never.

**Taken (a), 2026-10-30.**

**D5. Login-node policy, and the substrate for SSH pools.**
- (a) Annotate the entries, add a "persistent login-node process" line to `connect_facility`'s terms, ask SDSC and NERSC, and use a per-campaign UEP with parsl blocks once the sites answer.
- (b) Run an endpoint inside a scheduler job: `scancel` ends it honestly, and the login node stays clean. It needs outbound AMQP from compute nodes and a registration per job.

**Decided 2026-10-30: annotate only.** Mark the affected registry entries with the policy and add the "persistent login-node process" line to `connect_facility`'s terms. Don't ask SDSC or NERSC. Campaigns proceed everywhere, with the per-campaign UEP substrate. Option (b) stays an idea, unspiked.

**D6. A loam handoff with `min_blocks ≥ 1` on a MEP.**
- (a) Refuse it until S3 proves the UEP exits when idle.
- (b) Allow it with `worst_case: unbounded` and `release` naming the `daemon.pid` removal.

**Taken (a), 2026-10-30.** Terms the user cannot bound are not consent to a number. loam uses `min_blocks: 0` until S3 answers.

## See also
[[Batch spikes 2026-10-30]] · [[Batch design review 2026-10-30]] · [[Cost control]] · [[Resource shapes & the spend floor]] · [[Next priorities 2026-10]]

# Next priorities 2026-10

> [!abstract] In one line
> After the hackathon push, four review agents assessed seven ideas for **user experience, stability and usefulness**
> against the code (2026-10-09). This is the result, ordered **cheapest-for-the-value first**. Work from the top; the
> first five items are each about a day and touch nearly every session.

**Scales.** Effort: **S** ≤ 1 day, ≤ 3 files · **M** 2–4 days · **L** 1–2 weeks · **XL** more. Usefulness **1–5**:
5 = nearly every session, costly without it; 1 = a rare edge. "Verified" = checked against the code (file:line) or
reproduced hermetically by the reviewer; the rest is marked.

## Status — where development stopped (2026-10-10)

Built overnight 2026-10-09/10, one item at a time. Each item had an implementer working in its own worktree and two
adversarial reviewers (bugs, style) per round, 3–5 rounds per item. Fixes were proven with throwaway tests and mutation
checks; live checks ran on the fake cluster with the free ALCF operators. Each PR is one commit and passes CI. None has
a version bump or CHANGELOG entry: those are batched into one release PR (0.1.23) after the merges. A session will pick
this up again in the week of 2026-10-12.

| Item | PR | State |
|---|---|---|
| 1 strategy tick (+ reap/age/billing follow-ons) | #173 | open, ready |
| 2 output truncation | #174 | open, ready |
| 3 Esc/stop honesty | #175 | open, ready |
| 5 sbatch spend hole | #176 | open, ready |
| 4 walltime, 6 switch/reconnect, 10 long-poll | — | **deferred until #173 and #175 merge** (they rewrite the same functions in warmth/server/connect) |
| 7 actionable NO ACCOUNT | — | **not started**: its implementer was cut off by the account's weekly usage limit before committing anything |
| 15 parsl check at attach | — | not started |
| 3b quick commands unseen by parsl | — | design question; item 1's shelved redesign is on branch `wip/3b-option-c-jobid` |

**Merge order.** Merge #173, then #175, then #174 and #176, rebasing the later PRs after each merge. #173 and #175
overlap in `warmth.py`, `server.py` and `notices.py`. Before merging #173, run `block_reaped_resume` (below).

**Open decision.** CLAUDE.md pins the commit trailer to "Claude Opus 4.8"; the overnight commits use "Claude Opus
5.5", the model that actually ran.

**Leftover worktrees.** `.claude/worktrees/agent-*` hold the four branches; remove them once their PRs merge.

### How validated each one is

Legend: **H** hermetic tests (mutation-checked), **L** live on the fake cluster, **—** not exercised.

**#173: scaling pass, idle presumption, block identity, spend bound**
- L **First-result latency.** `happy_path` ran ×6, with first results at 36.7–44.9 s against 95–114 s before.
- L **The scheduler job id is real.** A parsl worker under `slurmstepd` carries `SLURM_JOB_ID`, and an `sbatch`+`srun`
  probe prints `HPCB_JOB <id>`.
- L **Normal flows still work.** `session_persistence`, `fake_mep_compute` and `long_task_via_handle` passed. Every
  failure was gpt-oss skipping its final stop, and the graders caught it each time.
- H Idle-presumption grace, billing upper bound, block dating by job id after a presumption, the replaced-block (kicked)
  reap, and the `max_blocks == 1` gate: `tests/test_reap_reask.py`, `tests/test_runner.py`.
- **— Gaps:**
  - `block_reaped_resume` exists (chaos: scancel + interject) and is the closest live check of the replaced-block
    reap. It needs the Claude operator (paid), so it was not run. **Run it before merging.**
  - No scenario returns just past the idle window. The window is fixed at 600 s with no knob to shorten it (Profile
    default), so such a scenario needs either ~10 minutes or a test-only knob.
  - Reuse of an endpoint configured before this change (still on 30 s) has not been exercised.
  - No real facility: the change affects real SSH endpoints (Expanse, BYO). A BYO `happy_path` on globus1 with an ALCF
    operator is the cheap real-hardware check.

**#174: output cut at the agent boundary**
- H The cut itself, the marker, the whole-line rule, the internal consumers (pilot probe, mybalance), the oversize
  report, and exception text keeping its head and tail.
- H An end-to-end run of the real SDK `ShellFunction`, locally: 1,500 lines come back whole and 40,000 come back as a
  marked tail. A real-serializer test keeps a full two-stream result under 10 MiB for lines up to ~1.9 KB.
- L Normal flows only (×5). No scenario produces long or oversized output.
- **— Gap:** a `long_output` scenario on the fake cluster, autonomous, so the free CLI operators can run it. It should
  `cat` a 50k-line file (check the marker and that the agent redirects and reads ranges), and produce a ~12 MB result
  (check the honest "ran, exit code unknown" report).

**#175: Esc and stop**
- H `tests/test_cancel_stop_honesty.py`, using real blocking futures: cancel mid-run, stop during a sync-wait, the race
  with a teardown and a re-bind, internal-session isolation, the MEP lost-task rule, and reset as a poll handle. 52/52
  mutations caught.
- L Normal flows ×13: `long_task_via_handle`, `session_persistence`, `fake_mep_compute`, `spend_gate_enforced`,
  `happy_path`.
- **— Gaps:**
  - `stop_while_running` (stop while a polled task runs) is the scenario for the refusal path. It is autonomous and
    runnable now, but was not run on this branch. **Run it.**
  - `orphaned_task` (chaos) is related to the lost-task rule and needs the Claude operator.
  - There is no Esc mid-run scenario. It needs a harness hook that fires *during* a tool call (today's hooks fire
    after one).

**#176: a hand-submitted batch job is spend**
- H The product detector and the grader detector are held to one shared 138-row corpus. Also covered: the notice per
  outcome and the four spend graders. A regrade of all 684 stored bundles flips only `long_job_30m`'s ungated
  `spend_follows_question`; none of the 681 stored questions changes class. Decline semantics and the human-sim are
  unchanged from main.
- L Normal flows only (×3). No scenario run submits a batch job.
- **— Gaps:**
  - An autonomous scenario on the fake cluster where the agent is authorised to submit a short `sbatch`. Check that
    the notice appears and is relayed, and that the job is cancelled at the end. Free operators can run it.
  - A persona scenario where the work exceeds the walltime and the agent must ask before submitting (human-sim; the
    Claude operator or Hermes over ACP).
  - `long_job_30m` (~20 min) not re-run.

### Validation runs, 2026-10-10 (after the overnight build)

| Run | PR | Result |
|---|---|---|
| `stop_while_running` (Pi) | #175 | **OK**: the stop refused and named the live task; `down` once the task completed; no pilot left |
| `block_reaped_resume` (Claude operator) | #173 | **not run**: the Claude subscription's weekly limit (resets 2026-10-12 21:00 CT). Run it before merging #173, or with API credits |
| `happy_path` on **globus1** (Pi) | #173 | **blocked by the environment**: the pool user's `~/hpc-bridge/gce-venv` (py3.12) has a broken `globus_compute_endpoint` (ImportError), and `env_setup`'s `command -v globus-compute-endpoint` guard never repairs it. It needs that venv removed (the maintainer's call, on a shared cluster). The product follow-up: check that the install works and matches the pin, not just that a binary exists |
| `long_output` (new, Pi) | #174 | the product held on all three checks: a marked tail of 666 lines with 49,334 dropped, ranges read whole, and 16.2 MB reported as "RAN… over the limit… exit code unknown". The cell failed on the model using `login_shell`; the prompt and grader were fixed |
| `long_output` (new, Codex) | #174 | **OK** |
| `hand_batch_job` (new, Pi) | #176 | **OK**: the real notice on "Submitted batch job 83"; the job was cancelled; nothing left on the cluster; the ungated `spend_follows_question` flagged the unasked submission |
| `hand_batch_job` (new, Hermes) | #176 | **OK** (re-graded once a too-strict any-`ensure_endpoint_up` check was replaced by `no_compute_block`) |

Found along the way, pre-existing and not fixed: a `run_shell` sent before `connect_facility` has finished answers
with raw internals ("FileNotFoundError … endpoint.json", "configure failed: Traceback…"), not "connect a facility
first".

## The order

| # | Item | Area | Effort | Use |
|---|---|---|---|---|
| 1 | Endpoint strategy tick 30 s → 5 s | UX | S | 5 |
| 2 | `run_shell` output silently keeps only the last 1000 lines | stability | S | 4 |
| 3 | Esc and stop stay honest: a cancelled call stays tracked; stop refuses mid-call; MEP teardown refuses a live task | stability | S (M with a chaos scenario) | 4 |
| 3b | A block used only by short commands can idle-release mid-session unseen; the next command silently starts a new billed block (found reviewing #1) | stability | M | 4 |
| 4 | `walltime=` on `ensure_endpoint_up` (a change re-asks spend) | usefulness | S | 4 (5 on Anvil/Delta) |
| 5 | Close the hand-rolled `sbatch` spend-gate hole in the skill | stability | S | 3 |
| 6 | A partition/account switch re-asks spend and releases the old block; a same-facility reconnect keeps live work | stability | S | 3 |
| 7 | NO ACCOUNT says how to get access | UX | S | 3 |
| 8 | Read-only `endpoint_status` ("what's running and billing?") | UX | M (low) | 4 |
| 9 | `fetch_file` / `push_file` over Compute (bytes to local disk, never into context) | usefulness | M | 4 |
| 10 | Long-poll the wait (`ensure_endpoint_up` / `connect_facility` hold on the canary, with progress) | UX | S–M | 3 |
| 11 | Allocations at first SSH contact, so the spend question overlaps the login warm-up | UX | S–M | 3 |
| 12 | Agent-chosen resources: nodes, GPUs, CPUs, exclusive (typed, capped, priced) + per-partition charge rates | usefulness | M + S–M | 4 |
| 13 | Durable task record: a task survives the session; `poll_task` / `connect_facility` find it again | usefulness | M (+ a half-day spike) | 3 |
| 14 | Hackathon guest lane on the lab cluster (per-person guests on its MEP) | UX | hpc-bridge S · infra M | 4 event / 2 research |
| 15 | parsl skew: a release check at attach (float facilities) + the upstream report | stability | S + S | 3 |
| 16 | `submit_job` / `job_status` for work past the block (SSH facilities) | usefulness | L | 3 |
| 17 | Globus Transfer | usefulness | L–XL | 3 |
| 18 | Laptop sandbox: the fake cluster as a downloadable try-it | UX | M | 2 |

## The items

**1. Strategy tick (verified).** Both SSH endpoint templates set `strategy_period: 30` (`facility/remote.py:815`,
`:863`; added with idle-release in 87cef2c, no latency reason given); Globus Compute's default is 5 s. A new user
endpoint waits one tick before its first scale-out, so a first SSH result pays **~30 s twice** (login shape, then
compute) — measured on the fake cluster (`endpoint-logs.txt`: UEP start → scale-out 30.5 s, sbatch → worker ≤ 5 s) and
on globus1 (30.9–31.0 s). First result ≈ 95–105 s today → ≈ 45–55 s; an account/partition switch and a re-provision
after a reap save ~25 s each. Facility-owned MEPs (Delta, Anvil) are unaffected; suggest 5 s for the lab's own MEP
template too. Check: `_IDLE_GRACE_S` (60 s) stays safe; `squeue` load follows the provider's status polling, not the
strategy tick (unverified — check parsl). Running endpoints keep their old template (the known template-drift gap).

**2. Output truncation (verified).** `runner.py:147` builds `ShellFunction` without `snippet_lines`; the SDK default
keeps the **last 1000 lines** with no marker, so `cat` of a longer file returns its tail as if it were the whole thing.
Raise the limit within the existing caps (1,000,000 characters, `context.py:99`; Compute's 10 MB result) and say so
when output was cut.

**3. Esc and stop (verified, hermetic repro).** (a) A call cancelled mid sync-wait (`_heartbeat` re-raises,
`server.py:498-500`) never registers its Globus future, which keeps running to the block walltime; `stop_endpoint`
then sees no handle and answers **`down`** while the endpoint relaunches a block for the outstanding task, the next
canary queues behind it and reads as a false reap, and a second command can clobber the session's cwd/env. Fix: on
`CancelledError`, register the handle (marked `client_cancelled`) before re-raising. (b) Stop checks handles but not
`rt.inflight` (`server.py:687`, `:748`); the returning dispatch rebuilds the shape — refuse while a call is in flight.
(c) MEP teardown clears live task handles (`warmth.py:186`) — reuse `_stop_mep`'s refusal. Open: should Esc *kill* the
command? That needs a channel (a sentinel on the shared filesystem; none on a MEP) — L.

**3b. Short commands and idle-release (proven in simulation, 2026-10-09).** parsl resets a block's idle timer only
when a scaling pass finds a task outstanding (`strategy.py:228-229`), and a command that starts and finishes between
passes is never seen — canaries included. So a block used only for short commands idle-releases `max_idletime` after
the last pass that saw work, even mid-session, while hpc-bridge's own clock (reset by every dispatch) and its 45 s
canary trust still call it warm: the next command goes out with no canary, parsl starts a NEW billed block, and the
user is never re-asked. Simulated with parsl's real `Strategy` (0.2 s commands every ~40 s, 15-minute sessions):
released mid-session in 68 % of sessions at a 5 s period (93 % at 30 s), 63 % inside the trust window. Commands of
≥ 1 s are mostly seen at 5 s (9 %). Fix options: make the work visible (a fire-and-forget `sleep period+1` touch after
a short command, if the block has a spare worker), or have hpc-bridge's clock count only work parsl must have seen
(tasks longer than a pass) and re-ask early otherwise. Design first.

*Learned building item 1 (2026-10-09, three adversarial review rounds):* hpc-bridge's idle clock (reset by every
dispatch, `_last_activity`) and parsl's (reset only by a task a pass sees) disagree for quick commands, and every
reap presumption rests on them agreeing. A full redesign was built and then shelved — branch
`wip/3b-option-c-jobid` (9ace0df): past the idle window nothing is submitted until spend is re-confirmed
(tentative until idle+60, then certain), and the canary reports the scheduler job id (`HPCB_JOB`
`${SLURM_JOB_ID:-${PBS_JOBID:-}}`, verified live: the worker inherits it under `slurmstepd`) to tell the old
block from a new one. Its review proved it needs this item first: the confirm's own canary is quick, so "same job →
continue" keeps a block parsl scales in seconds later; a partition/account switch in the tentative window left the
old spend clock running; and (pre-existing) a failed dispatch clears `warm_confirmed_at`, switching both presumptions
off. Proofs: the round-3 throwaway tests and parsl `Strategy` simulations (kept with the branch's notes). Order:
make activity visible to parsl (or measure what it sees) → then the ask-before-sending rule and the job id.

**4. Walltime (verified).** The only agent knobs are `partition`/`account` (`server.py:309-334`); walltime is already
a per-submit template variable, so `walltime=` validated against a cap (an `_apply_walltime` beside
`_apply_partition`, `warmth.py:337`) is a day's work and works on SSH and MEP. Anvil and Delta blocks are 15 minutes —
a task there is cut at ~880 s with no recourse.

**5. The `sbatch` hole (verified).** The skill tells agents to submit `sbatch`/`qsub` over the login shape for work
past the block (`SKILL.md:76`), but the login shape is unbilled (`cost._billable`) — real spend with no confirmation;
and a MEP has no login shape at all. Skill: require the same spend confirmation first; on a MEP, use walltime (#4) +
checkpoint/resume.

**6. Switches and reconnects (verified).** A partition/account switch keeps `spend_confirmed` (cleared only on reap,
`warmth.py:272`, `:312`) and leaves the old block to idle-release. A `connect_facility` re-bind drops live tasks and a
warm block even for the same facility (`connect.py:216-230`) — the block keeps billing with its clock stopped; open
models re-connect when confused. Re-ask on a switch and release the old block (SSH); refuse a re-bind while work is
live, and make a same-endpoint reconnect a no-op. Prerequisite for #12.

**7. NO ACCOUNT (verified).** The notice only says "ask the facility's support" (`notices.py:360-371`). Say how access
is actually got: an ACCESS allocation, bring your own facility over SSH, the guest lane (#14) when it exists.

**8. `endpoint_status` (verified premise, corrected).** `ensure_endpoint_up` does not start a block after a stop (the
spend gate comes first, `warmth.py:302-304`), but it is not side-effect free: its canary resets a warm block's
idle-release clock, a queued canary keeps a MEP relaunching blocks, and `shape="login"` on an unbound endpoint
bootstraps one. A read-only tool from a lock-free snapshot: facility, endpoint, per shape (partition, account,
spend confirmed, last canary, block age vs walltime, idle-release ETA, in-flight), presumed reaps (reported, not
applied), task handles incl. client-cancelled, session spend, the last release. Submits nothing, no SSH; labels each
fact confirmed / presumed / unknown; says it is this server process's view. The 13th tool — "twelve tools" appears
in several docs.

**9. Files over Compute (verified state).** No file movement exists ("moves command output, not files", user docs).
`fetch_file`/`push_file`: the server writes bytes to local disk (≈6 MB base64 chunks) and returns path, size, sha256
— no new Globus scopes, works on a MEP. Push stays small (one shell argument ≈ 128 KB, unverified).

**10–11. Fewer, shorter waits (measured).** Agents re-poll every 1–6 s (Codex, Pi) to ~20 s (Hermes), each poll a
model turn; holding the call on the one in-flight canary for up to ~45 s (progress every 15 s, 0.1.22) saves 5–15 s
and 2–4 turns. Running `mybalance` in the bootstrap SSH session that is already open lets the spend question overlap
the login warm-up (~10–15 s after #1).

**12. Resources (verified state).** Today: catalog `defaults` (whose docstring says the agent MAY override — no tool
does), `extra` (Anvil `cores_per_node: 1`, Delta `--gpus-per-node=1`), BYO `FacilityDetails`; a MEP's schema silently
drops keys it doesn't allow (`mep.py:252-284`). A typed struct + presets, per-facility caps and a `resource_map`,
rendered into `scheduler_options` server-side (never free-text directives), refusing a knob a MEP would drop; the gate
shows the size, and per-partition charge rates make `session_spend` real (every registry facility reports 0 today).
Open: caps curated or discovered (`sinfo`/QOS)? Re-ask on every resize or only when it grows?

**13. Durable task record (verified state).** Task handles are in memory only (`app.tasks`, ids `<shape>-<n>` restart
per process, `warmth.py:444-447`; cleared at exit, `server.py:184`); the Globus task UUID and task group are never
kept. A 0600 `TaskStore` written at submit (which also covers #3a), `poll_task` falling back to it via
`get_batch_result`, and `connect_facility` listing unfinished tasks. Spike first: can a second process fetch a result
the first process's executor already consumed, and for how long?

**14. Guest lane (verified state).** The lab MEP maps 14 identities by exact match; the IaC deliberately rejects a
domain catch-all (one shared POSIX account = shared files, killable jobs, a trojanable shared venv). The lab is already
building per-person guests (Changes/41, guest QOS with a node-hour budget and end date; Changes/45 request forms;
Changes/40 enforces associations). Hackathon slice: a batch of compute-only guests, backfill partition, ~30 min
walltime, plus `exclusive: false` in the lab template (today each block takes a whole Spark → ≤ 4 concurrent);
hpc-bridge adds a `sandbox` seed and points NO ACCOUNT at it. Zero-infra alternative: add participants to the ACCESS
project. Depends on who the participants are.

**15. parsl skew.** The SSH path is skew-free by construction (`worker_init` replays the endpoint's venv,
`facility/remote.py:323`). On a MEP a canary cannot catch it before billing (the skew drops the very result). Cheap
now: the `releases` check at attach for float facilities, before the spend gate; file upstream (fail loudly and stop
scaling on a mismatched worker; publish the endpoint's parsl in its metadata; expose it to `worker_init`).

**16–18. Larger.** `submit_job` (SSH only — on a MEP every status check would start a billed block) needs #12's
resource struct and #13's records; Globus Transfer needs a consent step per mapped collection (the same machinery as
M2) and may need a different linked identity — defer until that exists; the laptop sandbox needs multi-arch images
(the compose file pins `linux/arm64`) and a published registry.

## Open questions for the maintainer
- Who are the hackathon participants and what accounts do they hold (decides #14 vs adding them to ACCESS)?
- Esc: track the command (#3) or also kill it (L)?
- Resource caps: curated per facility or discovered? Re-ask spend on every resize?
- Task records: local only, or also on the facility's scratch (visible to another machine)?

## See also
[[Hackathon readiness 2026-10-09]] · [[Registry health]] · [[V1 release]] · [[Endpoint reuse and MEP integration]]

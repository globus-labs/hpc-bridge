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

**Trailer.** Decided: commits use "Claude Opus 5.5". CLAUDE.md is pinned to it on this branch.

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

Found along the way, both pre-existing on main. Each is now a follow-up of its own (next section): a `run_shell` sent
before `connect_facility` has finished answered with raw internals, and env_setup's `command -v` guard never repaired a
broken install.

### Follow-ups, 2026-10-10 (paused late that evening)

**B — with no facility connected, the tools say so: PR #177, ready to merge.** Branch `fix/no-facility-connected`,
one commit (b76978c). CI is green.
- Shell tools return `failed` and `ensure_endpoint_up` returns `down`, each with a "No facility connected" notice. The
  notice has variants for a connect that only proposed a config and for one waiting on a one-time login.
- The boot fallback is always unbound, so the server never stands up a local endpoint; `HPC_BRIDGE_ENDPOINT_ID` is the
  only local-dev path.
- Reviewed three times; the last two delta reviews were READY. Live: Pi `happy_path` OK on this commit; on an earlier
  cut, globus1 `zero_config_list` OK.
- Known gap, not fixed: with `HPC_BRIDGE_ENDPOINT_ID`, `teardown_endpoint` on a `LocalFacility` says "deleted" and then
  re-provisions `hpc-bridge-dev`.

**A — the endpoint install is checked, not trusted: REDESIGNED (v2), branch `fix/endpoint-venv-v2` (plugin 0.1.24, no PR yet).** After four review rounds kept finding must-fix bugs in the ~3.9 KB generated shell line below, the maintainer chose a split: the env_setup line only activates the keyed venv (~460 chars; on a worker it checks the entry point and names the architecture), and the check/lock/build/verify lives in Python over short SSH commands (`facility/endpoint_venv.py`). gpt-oss had also corrupted the 3.8 KB line when echoing it back in
`details=` (an extra `fi;`), so `details.env_setup` became optional. After two more reviews (2026-10-13) the build lost
its `mkdir` lock too: it builds a relocatable venv in a directory of its own and claims the keyed path with one atomic
rename (uv 0.12.9 `--relocatable` venvs, parsl's worker pool included, checked to run after a rename in a clean
container). Live on the fake cluster (pi): happy_path rebuilt a broken keyed venv, venv_broken_repair OK,
endpoint_upgrade_reuse's product checks all passed, venv_arch_mismatch printed the arch line (codex passed it; gpt-oss
stopped early on the old prompt). 8ebc31e then passed all four live with Pi (fake/mep); its delta review (READY) added
the old-uv message, the loop-relative teardown deadline, PYTHONHOME, a dotfile-free uv install and the claim's inode
re-check. 81c30ae then ran `happy_path` on globus1 (Pi, an endpoint hpc-bridge stands up over SSH). The bring-up
used the unkeyed venv, which passed its check, and both pilots FINISHED quoting "built on x86_64 … does not run on
spark4 (aarch64)". The agent then reconnected with env_setup `""` to "fix" it, and teardown failed (`command not
found`, manager left running). Fixed in the same commit: teardown runs the CLI in the environment the endpoint was
started with, searching hpc-bridge's default venv when that is unknown; the arch notice says not to edit env_setup;
the reconnect warns. f26d868 re-ran on globus1: the arch message showed on both partitions, gpt-oss left env_setup
alone, and the harness teardown through the finder was the first fully clean one there. Its delta review's must-fix
(a teardown's choice of environment outliving it, into the next bring-up) is fixed: the choice now ends with the
teardown. What is true now: [[Standing up the endpoint]] § *The endpoint's own venv*. The record below is round 3's (branch `fix/env-setup-verify-install`, superseded).

*Round 3 (superseded):* branch `fix/env-setup-verify-install`, pushed
for safekeeping, one commit (839b02e, round 3). Worktree `.claude/worktrees/agent-ac4e16a6d5ae2acc1`.
- *Design.*
  - hpc-bridge's own venv is keyed by version: `{venv}-endpoint-<gce>-py<X.Y>`. A version or Python move builds a new
    venv beside the old one and never replaces one a running manager uses.
  - The check runs the venv's real entry point, `globus-compute-endpoint version`. It must report the pin, and the
    venv's `python` must be the client's major.minor.
  - Builds happen only on whoami, configure and start (`HPCB_ENV_REPAIR=1`). List, stop and delete (`=0`) never build;
    they fall back to any working hpc-bridge venv.
  - A build takes a `mkdir` lock, moves the old venv aside and restores it if the build fails.
  - Workers only check. A venv built for another architecture ends the job with "built on x86_64 … does not run on
    spark1 (aarch64)".
  - Every task's `worker_init` is the endpoint's OWN configured one, read once per endpoint id from
    `get_endpoint_metadata`. Blocks therefore match their manager, and an edited env_setup is reported ("teardown, then
    connect, to apply it") rather than silently half-applied.
  - Cached BYO lines that exactly match one we generated are migrated. Old plugins (≤0.1.22) reading the new text never
    break an endpoint that worked.
- *Review history.*
  - Round 1 (9f133a7): two must-fix bugs. The check imported `globus_compute_endpoint.cli` under `python -c`, which
    raises IndexError on every real install (`cli.py` reads `sys.argv[0]` at import), so every bring-up would have
    failed. A repair could also `rm -rf` the venv under a live manager.
  - Round 2 (5bd9b29): three must-fix bugs. Taking `worker_init` out of the per-task config broke endpoints configured
    by ≤0.1.22 (their never-used default fails YAML) and ignored env_setup edits on reuse. A lock that couldn't be
    created was treated as held for 400 s.
  - Round 3 (839b02e) addresses all of these, plus the Anvil MEP path collision, harness paths, traps, the probe timeout
    and every surviving mutant. 841 passed, ruff and mypy clean.
  - **Round 3 has not been reviewed.** Three reviews (worker_init/compat, shell line, style) were stopped at the pause;
    run them again.
- *Live evidence (fake cluster, Pi).*
  - 5bd9b29 built the keyed venv beside an old 4.16.0 one, left the old one untouched, and ran a block.
  - 5bd9b29 repaired a broken keyed venv in place: rebuilt, no leftovers. Plant a break by replacing the file with
    `mv`, not by appending: the venv's files are hardlinks into `~/.cache/uv`, so an in-place append corrupts the cache
    too.
  - **839b02e passed the upgrade path.** Main stood up an endpoint whose template carries the old single-encoded uv line
    and kept it up. The branch, run with the same `HPCB_RUNID`, reused it over the web (`reused: true`), ran a compute
    block on it, and reported the differing env_setup. Recipe: a scratch scenario with `TEARDOWN = "keep"` on a main
    checkout, then `happy_path` on the branch with the same `HPCB_RUNID`.
- *Still to do before the PR.*
  1. The round-3 reviews.
  2. Live on 839b02e on the fake cluster: a fresh build under the new `-endpoint-` stem (the upgrade run reused the
     endpoint over the web, so it built nothing) and a planted repair.
  3. globus1 `happy_path` as `hpcbridge-test` only (the one account whose venv the maintainer cleared): the block
     should fail with the named architecture message. Login is x86_64, the compute nodes are aarch64 on a shared NFS
     home, so compute through an endpoint hpc-bridge stands up over SSH stays blocked on globus1 until per-architecture
     venvs exist (described as a follow-up in the vault). The `globus-labs` facility endpoint is not affected. Don't touch the `hpcbridge-test-00..09` pool venvs without asking.
- *Open points from the implementer.*
  - The Expanse line is about 3.9 KB.
  - Old venvs pile up against home quotas; there's no cleaner.
  - The Expanse seed's `globus-sdk==4.9.1` co-pin is unproven by a full run.
  - With no flag, a broken keyed venv next to a working unkeyed one builds, even inside an old plugin's 30 s stop.
  - Don't re-ingest the Expanse seed into the registry before the release that carries this.

Leftovers on the fake cluster from these runs, all under `hpcbridge-test`:
- `~/hpc-bridge/gce-venv-4.18.0-py3.11`, built under round 2's stem, now an orphan.
- `~/.globus_compute/hpc-bridge-fake-upg1791688461`, a directory with no process.
- **Leaked managers (found before the cluster went down, 2026-10-11).** Under `hpcbridge-test`, 14
  `hpc-bridge-fake-<runid> start --detach` managers (with their `uep.*` children) were still running, 19 h to about 2
  days old. They came from runs whose teardown had "deleted" the endpoint; the oldest are 0.1.22 hackathon runs on
  main, so this predates follow-ups A and B. The 839b02e upgrade run's endpoint did go away. Investigate on resume:
  either the product's teardown or the harness's post-run delete-by-name doesn't always stop the manager. Their Globus
  registrations remain, offline; `agentic/sweep_pool_user.sh` is the cleanup tool.

**The fake cluster is DOWN** (2026-10-11): stopped with `down.sh --profile mep`, volumes KEPT. Bring it back with
`HPCB_FAKE_PROFILE=mep ./agentic/fakecluster/bin/up.sh --profile mep` (the profile needs `HPCB_MEP_EMAIL`). Use
`down.sh --profile mep --wipe` for a fresh cluster; it deregisters the MEPs first.

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

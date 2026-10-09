# Next priorities 2026-10

> [!abstract] In one line
> After the hackathon push, four review agents assessed seven ideas for **user experience, stability and usefulness**
> against the code (2026-10-09). This is the result, ordered **cheapest-for-the-value first**. Work from the top; the
> first five items are each about a day and touch nearly every session.

**Scales.** Effort: **S** ≤ 1 day, ≤ 3 files · **M** 2–4 days · **L** 1–2 weeks · **XL** more. Usefulness **1–5**:
5 = nearly every session, costly without it; 1 = a rare edge. "Verified" = checked against the code (file:line) or
reproduced hermetically by the reviewer; the rest is marked.

## The order

| # | Item | Area | Effort | Use |
|---|---|---|---|---|
| 1 | Endpoint strategy tick 30 s → 5 s | UX | S | 5 |
| 2 | `run_shell` output silently keeps only the last 1000 lines | stability | S | 4 |
| 3 | Esc and stop stay honest: a cancelled call stays tracked; stop refuses mid-call; MEP teardown refuses a live task | stability | S (M with a chaos scenario) | 4 |
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

# batch.py

> [!abstract] Role
> Batch phase 1: many independent tasks fanned out and collected through the session's ONE confirmed compute block — `run_batch`, `status`, `cancel` ([[The MCP tools]]) — with every row recorded in the durable [[ledger]]. Design: [[Batch and swarm execution]] §4, §5, §10.

## What it does

- **The template** (`validate`, `substitute`): `{name}` for a row's own identifier keys, `{row}` for a plain-value row, `{index}`; values shell-quoted; other brace text left as written. [[runner]]'s `submit` brace-escapes the whole script for ShellFunction's `cmd.format()` afterwards.
- **The window** (`capacity_for`): `max(1, workers - 1)` — the block's `max_workers_per_node`, or the worker count a check or a result reported (`PARSL_WORKER_COUNT`); one worker stays free for interactive commands. On a one-worker block a batch task holds it, and an interactive `run_shell` waits behind it, its answer naming the batch (`shared_worker_note`).
- **The admission gate** (`admit_ok`, `_admit_locked`, under `app.lock`): a row is sent only while the block's remaining walltime, read from INSIDE the block (the canary's `HPCB_REMAINING` and every result: [[session_shell]] `REMAINING_SH`), is at least `task_walltime_s` + `MARGIN_S` (30 s). Unknown admits nothing. Past it the batch pauses at `needs_confirmation`, naming the walltime.
- **The wrapper** (`task_script`, one per attempt): the wrong-block guard; the attempt claim `mkdir <results_dir>/tasks/<i>/attempt-<n>` (a duplicate exits, S4); the `CANCELLED` marker checked before and watched during the run; a free-space floor (`MIN_FREE_MB`); `meta.json` at start; the command in its own process group (`set -m`) under the session's cwd/env read-only; stdout/stderr through FIFOs, capped at `OUTPUT_CAP_BYTES` per stream with a rolling tail of the stream's real end; a watcher that kills the group at `task_walltime_s` (rc 124) or on the marker (rc 143); one `HPCB_RESULT {json}` line (`parse_result`).
- **Its own runner** (`_batch_runner`): the compute shape's config (the same user endpoint, so the same block) on an Executor of its own, whose task group the service mints and whose results nobody else consumes (S1).
- **Settling** (`_collect`, `_settle_result`, `_settle_failure`): a result wins whenever it arrives; a guard refusal is the block's reap (the row goes back, the batch pauses); a ManagerLost/WorkerLost failure is `lost` on evidence (a retry for `idempotent`); a submit with no task id is `submit_unknown` (`_reconcile`: the batch's task group is read; a stray task's result settles the row it names; an idempotent row is sent again only once none is pending; a non-idempotent one never is).
- **Evidence and presumption**: on SSH (`_evidence`) the block's job is looked up on the login shape while tasks are out; a job sacct (or PBS) says ended settles them `lost` on that evidence, marks the reap and pauses. On a MEP (`_presume`) a task with no result past `task_walltime_s` + `PRESUME_GRACE_S` since a block was seen is `presumed_lost`: it keeps its slot and is never retried.
- **Restart** (`_adopt`, `_rest_pull`): a server finding a batch whose holder is gone takes the lease to COLLECT — by REST with the raw `v2.get_task_batch` (`collect_rest`; `Client.get_batch_result` drops failed tasks, S4) — and pauses a batch that was admitting: spend lived in the old process.
- **Stop and cancel**: `pause_all` (called by [[server]]'s stops under `app.lock`, before anything is released), `retire_all` (the block's config, in the ledger), `in_flight_note` (the stop's busy refusal: every ledger batch out on this endpoint, naming `cancel(id, 'abort')`). `cancel` drains (keeps the backlog) or aborts (`write_cancel_marker` over the login shape, or a small task on a MEP block; the wrappers kill their commands).
- **`poll_uuid`, `record_interactive`**: `poll_task` by a Globus task id — a batch row, or an interactive task recorded when it went `running`.
- **The journal's event rows** (`_journal`): `batch.admit` (with `in_flight`, `capacity`, `remaining_s`, `need_s`), `batch.settle`, `batch.pause`, `batch.resume` — what the harness grades the window and the gate from.

> [!warning] A plain `status` never resumes spend
> It collects and keeps a RUNNING batch admitting, but a batch at `needs_confirmation` resumes only through `run_batch(batch_id=…, confirm_spend=True)`, the user's yes. A block that ends must never become a block parsl starts for a task left at the interchange (#204's pattern) — the gate and the window are what keep the interchange empty when the block goes.

> [!warning] Abort never touches the block
> In phase 1 the batch shares the session's block: `cancel(abort)` writes the marker and lets each wrapper kill its own command. It never SIGTERMs the user endpoint or cancels the block, which would end interactive work too.

## See also
[[ledger]] · [[server]] · [[session_shell]] · [[runner]] · [[Batch and swarm execution]] · [[Batch spikes 2026-10-30]] · [[Cost control]]

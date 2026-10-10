# warmth

> [!abstract] Role
> The per-shape warmth state machine — Executor build/reuse (`_runner_for`), the canary that proves a worker is live (`_confirm_worker`: TTL, live-task shortcut, non-timeout failures → `runner_stale`, the sticky NO ACCOUNT verdict, the transient-conflict count), the spend floor and partition/account application (`_provision`, `_apply_partition`, `_apply_account`), and dropping shapes (`_drop_compute_shape`, `_drop_all_shapes`, `_forget_identity_verdicts`). Task-handle bookkeeping lives here too (`_register_task`, `_resolve_task`, `_busy_session`, `_live_task_handles`, `_drain_shape_tasks`, `_endpoint_gone`) because a live task IS a warmth signal the runner rebuild and the canary consult.

`_note_dispatch` (`warmth.py:198`) reads each dispatch's outcome as a liveness signal: a result or a still-running task refreshes the block's confirmation, a failure voids it (forcing a re-canary) — except a result too big to come back, which the worker produced by running the command ([[dispatch]]).

Split steps 7–8 (2026-09-03; the review planned `warmth` and `tasks` separately — their coupling made one module the honest cut). Every function runs under `app.lock`, held by the caller in [[server]]. Tests patch `warmth._provision` / `warmth._drop_compute_shape`; `server` calls those through the module. The concepts: [[Warmth, the canary & cold-start]], [[Resource shapes & the spend floor]], [[Cost control]].

## See also
[[server]] · [[context]] · [[config]] · [[cost]] · [[notices]] · [[lifecycle]] · [[runner]]

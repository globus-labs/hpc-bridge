# Resource shapes & the spend floor

> [!abstract] In one line
> One templatable endpoint serves two **shapes** — `login` (a free `LocalProvider` on the login node) and `compute` (a billed scheduler block, `SlurmProvider` or `PBSProProvider`) — and a billed block **will not start** until spend is explicitly confirmed.

## Shapes

A *shape* is a named bag of template vars (`user_endpoint_config`) that renders the [[MEP & templated endpoints|UEP template]]. `shape_config()` ([[shapes]]) defines them:

| Shape | Provider | Cost | Used for |
|---|---|---|---|
| `login` | `LocalProvider` (login node) | free, no allocation | discovery, light work, the no-SSH probe |
| `compute` | `SlurmProvider` / `PBSProProvider` (scheduler block) | billed, idle-released | real compute |

Each shape has its own [[server|`ShapeRuntime`]] — its own Executor, canary, and spend clock — so they warm and bill independently. `run_shell(command, shape=...)` / `ensure_endpoint_up(shape=...)` pick the target. The `compute` block's scheduler (Slurm or PBS) is the **facility's**, not the shape's — `profile.scheduler` selects the provider/launcher template ([[facility-remote]]).

> [!warning] Compute-only facilities — `supported_shapes`
> A facility multi-user endpoint ([[facility-mep]]) declares `supported_shapes = ("compute",)`; the server reads it via `_supported_shapes` and **`_shape_reject` runs before any `ShapeRuntime` is built** at every shape entry point (`ensure_endpoint_up`, `run_shell`, `reset_session`, and `login_shell`), because a submit the facility's schema refuses would shut the SDK Executor down. No login shape also means: every shape is billed (the `needs_confirmation` notice no longer points at a free `login` alternative — `_needs_confirmation_notice`), discovery runs on the warm compute block, and stop is draining-only ([[Cost control]]).

> [!warning] `compute` is a boolean, not a string
> `shape_config` sets `compute: True/False`; the template branches on that bool. It must *not* compare a string like `provider_type == "SlurmProvider"`, because the manager's `_sanitize_user_json` JSON-quotes every string and the comparison silently fails — dropping the provider block ([#5](https://github.com/globus-labs/hpc-bridge/issues/5)). See [[MEP & templated endpoints]].

## The spend floor

A billed `compute` shape returns `needs_confirmation` and **starts nothing** until `ensure_endpoint_up(confirm_spend=True)` — a deterministic gate enforced in `_provision` ([[server]], `server.py:725`). It covers `run_shell` too (its canary would otherwise kick a block). The `login` shape is free and exempt. The chosen `partition` and `account` are threaded in per task via `_apply_partition` (`server.py:761`) / `_apply_account` (`:787`) and persist for the session; both are refused while a task is still running on the shape (the runner swap would cancel it).

The spend acknowledgement is narrower: it covers **one block** (0.1.18). `ShapeRuntime.block_since` dates the block from its first warm confirmation; `warmth._presumed_reaped` decides, from the clock alone and before any submit, that it has gone (no task handle and no task for longer than `_idle_release_s` plus 60 s, or older than its walltime). `_maybe_released` covers the window itself: from `_idle_release_s` on, the call asks again without submitting anything, and the confirm's canary decides by job id whether the block was still up ([[Cost control]]). `_confirm_worker` also learns of a reap when a canary to a block it had confirmed warm times out. `_mark_reaped` then banks the warm interval up to the estimated release time, clears `spend_confirmed`, and records the reason; the next `_provision` answers `needs_confirmation` with that reason even if the call carried `confirm_spend=True` (`reap_told`), so the user is asked after learning of the reap. The clock-only check has to run first: the only way to look is a canary, and a canary to a released block requests a new billed one. When the canary found it (`reap_kicked`), the result reports `block_state="provisioning"` because a block may already be coming up; `stop_endpoint` treats that as a requested pilot (`expect_block`) on an SSH facility, and on a facility MEP the notice says it idles out.

What keeps the evidence honest (from the review of this change): `block_since` survives a failed dispatch and a runner rebuild, which void `warm_confirmed_at`, so a canary timeout after either is still a reap; a partition or account switch runs `_check_reaped` first and resets `block_since`, since the new block has its own age; `ShapeRuntime.inflight` counts synchronous dispatches inside their sync-wait (they hold no poll handle), and a canary queued behind one is not a reap; past the idle window nothing is sent before the re-ask, whatever the facility's scaling period. A block that is still up costs one question and then continues, with its age and spend clock; the old 60 s wait let a call in that gap send a canary that could request a new billed block first ([[Cost control]]); a facility that holds its block warm (`keeps_block_warm`, local dev) never idle-presumes; stop and teardown bank a presumed-released block to its release time. A poll handle records when its task actually ended (`TaskHandle.done_at`, a done-callback), so a finished-but-unpolled task counts from its end and a late poll does not pass for recent activity. Limit: a Claude-operator spend question asked only in prose is invisible to the scenario grader (as for `spend_follows_question`).

This is the front-end half of [[Cost control]] — the idle-release net catches the *back* end.

## See also
[[shapes]] · [[MEP & templated endpoints]] · [[facility-mep]] · [[Cost control]] · [[server]] · [[cost]]

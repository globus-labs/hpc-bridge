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

This is the front-end half of [[Cost control]] — the idle-release net catches the *back* end.

## See also
[[shapes]] · [[MEP & templated endpoints]] · [[facility-mep]] · [[Cost control]] · [[server]] · [[cost]]

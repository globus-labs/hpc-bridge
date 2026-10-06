# shapes.py

> [!abstract] Role
> Maps a resource **shape** name to the `user_endpoint_config` base — the template vars that render the [[MEP & templated endpoints|UEP template]].

## What it does

- **`SHAPES = ("login", "compute")`** (`shapes.py:13`). The billed shape was renamed `slurm` → `compute` when PBS support landed ([#28](https://github.com/globus-labs/hpc-bridge/issues/28)) — it is scheduler-neutral now.
- **`shape_config(shape, **overrides)`** (`:16`) → the base dict: `login` → `{provider_type: LocalProvider, max_workers_per_node: 1, compute: False}`; `compute` → `{compute: True}` — it does **not** pin a `provider_type`; the per-scheduler template supplies `SlurmProvider`/`PBSProProvider`. Caller overrides (non-`None`) merge in; an unknown shape raises `ValueError`.

> [!warning] `compute` is a boolean discriminator, on purpose
> The endpoint manager runs `user_opts` through `_sanitize_user_json`, which `json.dumps`'s every *string* (so `"PBSProProvider"` → `'"PBSProProvider"'`). A template that compared the *string* `provider_type` would silently fail and drop the provider block ([#5](https://github.com/globus-labs/hpc-bridge/issues/5)). Bools pass through untouched, so the template branches on the `compute` bool. See [[Resource shapes & the spend floor]].

> [!note] Scheduler is chosen by the facility, not the shape
> `compute` just means "a scheduler block". *Which* scheduler comes from `profile.scheduler` (`slurm` or `pbs`), which selects the template (`_SLURM_TEMPLATE` / `_PBS_TEMPLATE`) and thus the provider + launcher — see [[facility-remote]].

## See also
[[Resource shapes & the spend floor]] · [[MEP & templated endpoints]] · [[facility-remote]] · [[server]]

# profile.py

> [!abstract] Role
> The **session-level** profile: how this run behaves (interactive vs batch, block sizing, idle grace). Distinct from a facility's `MachineProfile`.

## What it does

`Profile` (`profile.py:10`) is a frozen dataclass with four fields: `mode` (`interactive` | `batch`, from `HPC_BRIDGE_PROFILE`), `nodes_per_block` (the spend clock's node count), `max_idletime_s` (default 600 — the [[Cost control|idle-release]] grace, and the `max_idletime` written into the UEP template), `uep_idle_heartbeats` (default 20, from `HPC_BRIDGE_UEP_IDLE_HEARTBEATS` — the 30 s heartbeats an idle COMPUTE user endpoint stays up before it exits, the template's `idle_heartbeats_soft`; 0 keeps it, and then hpc-bridge starts no fresh one per block request, [[warmth]] `_fresh_block`). It also rejects a negative heartbeat count. It validates `mode` against `MODES` (`:6`) and rejects a sub-1s idle time. The account, partition and queue are **not** here — they are per-facility (`MachineProfile`) and per-session selections threaded into `user_endpoint_config` ([[warmth]] `_apply_account` / `_apply_partition`), kept across a stop in `AppCtx.compute_choice`. `mode="interactive"` pre-spawns a block (`init_blocks=1`, the `@@EAGER@@` template slot); `batch` (the default) starts lazily.

> [!note] Two different "profiles"
> `Profile` (here) is **session policy** and applies to any facility. `MachineProfile` ([[facility-remote]]) is **per-facility data** (host, modules, interface, …). Don't conflate them.

## See also
[[server]] · [[facility-remote]] · [[Cost control]]

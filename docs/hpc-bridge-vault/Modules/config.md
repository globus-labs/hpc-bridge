# config

> [!abstract] Role
> Every environment variable hpc-bridge reads, behind typed accessors with ONE idiom (`env(name)` → stripped value or `None`), plus the runtime tunables (`CANARY_TTL_S`, `CANARY_TIMEOUT_S` (read through `canary_timeout_s()`, which honours the test seam `HPC_BRIDGE_CANARY_TIMEOUT_S`), `TEARDOWN_WAIT_S` (read through `teardown_wait_s()`, which honours the test seam `HPC_BRIDGE_TEARDOWN_WAIT_S`), `SYNC_WAIT_S`, `TASK_CEILING_MARGIN_S`, `TRANSIENT_CONFLICT_LIMIT`, `PROVISION_GRACE_S`, `STOP_CHECK_WAIT_S` (read through `stop_check_wait_s()`, which honours `HPC_BRIDGE_STOP_CHECK_WAIT_S` within 0–300 s: how long a stop on an endpoint with a compute user endpoint per block request waits for our queued checks to resolve, 90 s from parsl's cadence — its 60 s status poll, the 5 s scaling pass and slack for the failure to reach the web service — justified at the constant), `UEP_IDLE_HEARTBEATS` (read through `uep_idle_heartbeats()`, which honours `HPC_BRIDGE_UEP_IDLE_HEARTBEATS`: the [[profile]] `uep_idle_heartbeats` the server starts with)) and the SSH ControlMaster settings (`_control_settings`, `_short_control_dir`).

`names_machine(*names)` is the one rule for the env presets: `HPC_BRIDGE_ACCOUNT` and `HPC_BRIDGE_PARTITION` apply only to the facility `HPC_BRIDGE_MACHINE` names ([[binding]], [[connect]]).

Split step 2 (2026-09-03). Before, 26 inline `os.environ.get` reads in `server.py` disagreed on `or None` — an empty `HPC_BRIDGE_ACCOUNT` reached a Slurm template as `account: ""`. Now a caller that needs a string writes `config.account() or ""`, and the twin of this module is [[Configuration]]: when one changes, change the other. Callers reach it as `config._control_settings()` ([[binding]], [[connect]]), so tests patch `config._control_settings`.

## See also
[[server]] · [[Configuration]] · [[context]]

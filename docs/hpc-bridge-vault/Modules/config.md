# config

> [!abstract] Role
> Every environment variable hpc-bridge reads, behind typed accessors with ONE idiom (`env(name)` → stripped value or `None`), plus the runtime tunables (`CANARY_TTL_S`, `CANARY_TIMEOUT_S` (read through `canary_timeout_s()`, which honours the test seam `HPC_BRIDGE_CANARY_TIMEOUT_S`), `SYNC_WAIT_S`, `TASK_CEILING_MARGIN_S`, `TRANSIENT_CONFLICT_LIMIT`, `PROVISION_GRACE_S`) and the SSH ControlMaster settings (`_control_settings`, `_short_control_dir`).

Split step 2 (2026-09-03). Before, 26 inline `os.environ.get` reads in `server.py` disagreed on `or None` — an empty `HPC_BRIDGE_ACCOUNT` reached a Slurm template as `account: ""`. Now a caller that needs a string writes `config.account() or ""`, and the twin of this module is [[Configuration]]: when one changes, change the other. Callers reach it as `config._control_settings()` ([[binding]], [[connect]]), so tests patch `config._control_settings`.

## See also
[[server]] · [[Configuration]] · [[context]]

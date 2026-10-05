# Configuration

> [!abstract] Role
> Every environment variable the code reads. **None is required** for a first-time user: the registry id is built in, the Globus login is obtained in-terminal, and an SSH facility's login name + key come from `~/.ssh/config`. Startup-only vars (marked ⏻) are read once by `lifespan`/`make_facility` ([[server]]) — change one ⇒ restart the session; the rest are read when the code path runs.

## Facility selection

| Var | Effect |
|---|---|
| `HPC_BRIDGE_MACHINE` ⏻ | A catalog machine id/subject (e.g. `anvil`, `globus1`) → resolve its entry from the [[Facility catalog|registry]] at startup (a MEP entry binds a [[facility-mep|`MEPFacility`]]); unset → start unbound/local and let the agent bind one at runtime via `connect_facility`. Machines are catalog *data*, never hardcoded. (`HPC_BRIDGE_FACILITY` was removed — setting it raises a clear error.) |
| `HPC_BRIDGE_SEARCH_INDEX` | **Optional override** of the public registry. The plugin ships the registry's id (`PUBLIC_REGISTRY_INDEX` = **`6ff95fb8-1113-42be-a811-3d1cb5a67bd5`**, see [[Facility catalog]]) and reads it **anonymously**, so `list_facilities()` works out of the box with no login and no config. Set this only to point at a private/staging registry. (Curators still pass the UUID to `hpc-bridge-catalog` explicitly.) |
| `CLAUDE_PLUGIN_DATA` | Set by Claude Code for an installed plugin. The registry's fetched-entry cache lives under it (`<CLAUDE_PLUGIN_DATA or ~/.hpc-bridge>/catalog-cache/`), and `.mcp.json` derives `HPC_BRIDGE_USER_DIR` from it ([[Plugin packaging]]). |
| `HPC_BRIDGE_SSH_USER` · `HPC_BRIDGE_SSH_KEY` | **Optional overrides** — SSH login name + key. Unset ⇒ read live from your `~/.ssh/config` (`ssh -G` for the user, the config's `IdentityFile` for the key), so they needn't be exported into the already-running server's env. A refused first SSH tells the newcomer which of these the login name came from (`NO SSH ACCESS to <host> as <user>`, [[Standing up the endpoint]]). |
| `HPC_BRIDGE_ACCOUNT` ⏻ | Scheduler charge account — **required only on the `HPC_BRIDGE_MACHINE` startup-pin path** (and only when the entry's `account_required` is true); the agentic flow takes it from `connect_facility`'s allocations or you pass it to `ensure_endpoint_up`. |
| `HPC_BRIDGE_SSH_HOST` | Override the SSH host — **startup-pin path only** (`HPC_BRIDGE_MACHINE`): reach the catalog's canonical machine via your own `~/.ssh/config` alias / a specific login node / an FQDN (the container needs the FQDN — no ssh config). The agentic `connect_facility` path **ignores it** — the *bound* facility's own `ssh_host` is authoritative, so a stray/global env can't silently redirect an agent-chosen facility ([#35](https://github.com/globus-labs/hpc-bridge/issues/35)). It is also the discovery-probe host when you `connect_facility` without an `ssh_host`. |
| `HPC_BRIDGE_SSH_CONTROL_PERSIST` | Seconds to keep the per-facility SSH **ControlMaster** alive (default `60`; `0` disables multiplexing — which also disables the `needs_preauth` hand-off, since a pre-opened master can't be shared). One auth serves the whole bootstrap + discovery. |
| `HPC_BRIDGE_RELEASE_ATTEMPTS` · `HPC_BRIDGE_RELEASE_BACKOFF_S` | `stop_endpoint`'s bounded retry to **confirm** the block cancel when the login release channel is cold (default `3` × `6`s). Exhausted → honest `status="draining"` (never a false `"down"`); see [[Cost control]] / [#24](https://github.com/globus-labs/hpc-bridge/issues/24). |
| `HPC_BRIDGE_REMOTE_VENV` | Override the remote `globus-compute-endpoint` venv path (else the `/home/{user}/hpc-bridge/gce-venv` convention). |
| `HPC_BRIDGE_PARTITION` | Default partition — the [[Resource shapes & the spend floor|gate]] overrides it per run. |
| `HPC_BRIDGE_ENDPOINT_NAME` | **Opt-in override** of a session (BYO) facility's endpoint name — normally `hpc-bridge-<ssh_host slug>`. The agentic harness sets one per run so concurrent runs under one shared Globus identity don't collide on a registration; real users leave it unset. Wins over an agent-supplied `endpoint_name` too. |

## Globus login

| Var | Effect |
|---|---|
| `HPC_BRIDGE_LOGIN_WAIT_S` | How long `connect_facility`/`authenticate` wait for a browser Globus login to land before returning `needs_login` (default `90`) — [[login]]. |
| `GLOBUS_COMPUTE_USER_DIR` | The Compute SDK's own dir for `storage.db` (tokens). hpc-bridge does not read it, but the login, the catalog client and every dispatch ride the SDK's token storage there — so relocating it (as `scripts/fresh_user_session.sh` and the harness do) is how you become a "fresh" Globus user. Note `HPC_BRIDGE_USER_DIR` does **not** move the tokens (below). |
| `SSH_TTY` · `SSH_CONNECTION` | Read by [[login]] `_remote_session`: their presence means a browser on this machine can't reach the loopback listener, so the login is armed in **paste** mode. |

## Session & cost

| Var | Effect |
|---|---|
| `HPC_BRIDGE_PROFILE` ⏻ | `interactive` \| `batch` (default `batch`) — see [[profile]]. |
| `HPC_BRIDGE_SCRATCH` ⏻ | Override the [[Session continuity\|session-shell root]] (else the facility's `$SCRATCH`, else a home-relative default; re-resolved on every `connect_facility` bind). |
| `HPC_BRIDGE_STATE_DIR` | Base dir for hpc-bridge's **local state** — login-node pins (`endpoints.json`), the local-discovery facility cache (`facilities.json`), and the SSH ControlMaster sockets (`cm/`). Default `~/.hpc-bridge`; relocating it isolates all state (the test suite points it at a tmp dir). Keep it **short**: ssh caps the whole expanded `ControlPath` (~104 bytes on macOS), so a deep dir fails every SSH with `ControlPath too long` — `_short_control_dir` ([[server]]) falls back to `~/.hpc-bridge/cm` or `/tmp/hpcb-cm-<uid>` for the sockets, and the error is explained if even that is too long ([#50](https://github.com/globus-labs/hpc-bridge/issues/50)). |
| `HPC_BRIDGE_CHARGE_FACTOR` ⏻ | The QOS SU multiplier for the [[Cost control\|spend clock]] (default `0.0` = free). With none configured, a warm billed block's status says so explicitly (`session_spend: 0` is not a free tier). |
| `HPC_BRIDGE_SYNC_WAIT_S` ⏻ | How long `run_shell` blocks for a result before handing back a poll handle (default `120`; read at import). A command still running past it comes back `running` + `task_id` (**not** cut); retrieve it with `poll_task`. Clamped strictly below the task ceiling. |
| `HPC_BRIDGE_MAX_TASK_S` | Optional cap (seconds) on a single task before the worker kills it (exit 124). **Unset ⇒ the ceiling is the block walltime** — the deterministic default. Set it to bound the blast radius of a hung task on a long-walltime facility ([[Cost control]], [#21](https://github.com/globus-labs/hpc-bridge/issues/21)). |
| `HPC_BRIDGE_USER_DIR` ⏻ | The **local** `globus-compute-endpoint` dir for a `LocalFacility` (set by `.mcp.json` to `${CLAUDE_PLUGIN_DATA}/globus_compute`; exported to the daemon as `GLOBUS_COMPUTE_USER_DIR`). It does **not** relocate the SDK's token storage for the MCP process itself — that is `GLOBUS_COMPUTE_USER_DIR` (above). |

## BYO endpoint

| Var | Effect |
|---|---|
| `HPC_BRIDGE_ENDPOINT_ID` ⏻ | A UUID to dispatch to directly, skipping local provisioning. **Required on macOS/Windows for the local-dev path**, where the local daemon can't run ([[endpoint]]) — a catalogued or BYO facility needs neither. Refused at startup when it conflicts with a pinned MEP entry's `compute_mep_uuid` (the entry *is* the endpoint). |

## See also
[[server]] · [[login]] · [[facility-remote]] · [[state]] · [[Plugin packaging]]

> [!note] Where these are read
> Every variable above is read through `src/hpc_bridge/config.py` (split step 2, 2026-09-03) — one accessor per variable, one empty-means-unset idiom. Add a row here when you add an accessor there.

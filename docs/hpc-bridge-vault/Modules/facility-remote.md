# facility-remote.py — `facility/remote.py`

> [!abstract] Role
> Everything machine-specific for a remote **Slurm or PBS** cluster, behind one [[facility-base|Facility]]: the SSH transport, the per-facility `MachineProfile`, the `globus-compute-endpoint` CLI driver, and `SlurmFacility` (bootstrap / provision / teardown / config template). Despite the name, `SlurmFacility` drives **both** schedulers — the template split, not the class, is scheduler-specific.

## The pieces

- **SSH transport** — `SshTarget` (`:47`) + `ssh_exec()` (`:117`): `BatchMode` (never prompts), reaps the child on timeout/cancel (no process/FD leak). **Identity defers to `~/.ssh/config`** — `user`/`key_path` are optional; absent ⇒ a bare `host` so OpenSSH resolves `User`/`IdentityFile` **and `ProxyJump`** itself (`-i`/`IdentitiesOnly` only with an explicit key). That `ProxyJump` deferral is why a **bastion two-hop** (ALCF Aurora: `bastion.alcf.anl.gov` → login node) is transparent — no new code ([[Aurora (PBS + bastion) bring-up]]). Also drives the un-indexed [[discovery]] probe (`ssh_exec` on a bare target, pre-endpoint). The control channel of [[Two-channel architecture]]; see *Persistent SSH* below.
- **Interactive-auth hand-off** — `is_interactive_auth_failure` (`:146`) recognises a `BatchMode` denial where the server *offers* `password`/`keyboard-interactive` (vs a plain `publickey` refusal or a dead host); the probe raises `NeedsPreauth` (`:157`), which the server turns into `phase="needs_preauth"` carrying `SshTarget.preauth_command()` (`:97`) — the `ssh -fN -o ControlMaster=yes …` line the **user** runs in their own terminal, on the same `%C` ControlPath, so the server's later calls multiplex over the master they opened ([[MFA and interactive SSH auth]]).
- **Per-facility data** — `MachineProfile` (`:197`): host, `env_setup` (module + venv), `interface`, partition, account, scratch, plus **`scheduler`** (`slurm`/`pbs`) and **`cpus_per_node`** (PBS) — supplied by the [[Facility catalog|catalog]] (`profile_from_catalog_entry`, `:222`, which derives `endpoint_name` = `hpc-bridge-<id>` when a seed omits it and resolves `{user}` / `$USER` / `{venv}`), no longer hardcoded per machine.
- **gce driver** — `RemoteEndpointCLI` (`:270`): runs `globus-compute-endpoint` over SSH via `_gce` (`:282`); also `login_exec` (`:286`, backs the `login_shell` tool), `seed_storage_db` (`:357`, [[Credential seeding]]), `configure`/`start`/`stop`, `clean_uep_pidfiles` (`:409`, removes stale per-UEP `daemon.pid` files scoped to the endpoint UUID so a rebuilt worker doesn't hit "Another instance is running" → exit 73 — [#37](https://github.com/globus-labs/hpc-bridge/issues/37); `provision` calls it before a restart), `cancel_blocks` (`:430`, scheduler-aware — `scancel`, or `qdel` via `_cancel_blocks_pbs` (`:466`)), and `close` (`:527`, drops the ControlMaster).
- **Orchestration** — `SlurmFacility` (`:634`): `bootstrap` (`:714`), `provision` (`:753`), `config_template` (`:657`, picks `_SLURM_TEMPLATE` (`:545`) / `_PBS_TEMPLATE` (`:586`) by `profile.scheduler` — [[MEP & templated endpoints]]), `teardown` (`:796`), `manager_online` (`:814`, web), `find_online_endpoint` (`:821`, web reuse).

## How a stand-up flows

`bootstrap` (`:714`) is the entry point, and it is **reuse-or-SSH**: it first asks the Globus *web* service whether we already own an online endpoint (`find_online_endpoint`, `:821`) → reuse over AMQP, **zero SSH**. Only if none is online does it seed credentials (when needed) and call `provision` (`:753`): `configure` if absent → write the engine-free manager `config.yaml` + the scheduler's UEP template → `start` (detached) → capture & **pin** the login node. See [[Standing up the endpoint]] — including how the server rewrites a refused first SSH into `NO SSH ACCESS to <host> as <user>` for a newcomer (`_explain_provision_error`, [[server]]).

> [!note] `strategy_period: 5` — the first result's latency (2026-10)
> Both templates (`_SLURM_TEMPLATE` `:801`, `_PBS_TEMPLATE` `:849`) set `job_status_kwargs: {max_idletime: <profile>, strategy_period: 5}`. 5 s is Globus Compute's own default; the templates had 30 s from when idle-release landed, with no latency reason recorded. **Only new endpoints get 5 s.** An endpoint configured earlier and still running keeps 30 s: reuse (`bootstrap`'s web reuse, `provision`'s early return for a running endpoint) never rewrites the template, so it changes only when the endpoint is torn down or its manager is restarted. `warmth._IDLE_GRACE_S` is sized to the 5 s pass; a test ties it to the rendered templates. `strategy_period` is how often parsl's `JobStatusPoller` runs the scaling strategy. A new user endpoint cannot submit a block before the first pass that knows its executor, one period after the UEP starts: the poller's timer fires its first pass inside its constructor, before `GlobusComputeEngine.start` calls `add_executors`. A first SSH result starts two UEPs (login shape, then compute), so 30 s cost about a minute. UEP start → first scale-out measured 30.5 s on the fake cluster and 30.9–31.0 s on globus1, while sbatch → worker answering took ≤ 5 s.
> The period is **not** the scheduler query rate. A pass calls the provider's `status()` only once per `status_polling_interval`, which parsl fixes at 60 s for Slurm/PBS/Torque (`squeue`/`sacct`/`qstat`). For `LocalProvider` it is 5 s, an in-process `os.kill(pid, 0)`. Otherwise a pass counts outstanding tasks in memory, and `sbatch`/`scancel` run only on a scale-out or scale-in. Idle-release timing: [[Cost control]]. A facility MEP's period is the facility's (the fake MEP's template has 15 s); hpc-bridge never writes it.

> [!warning] Login-node pinning
> The manager lives on ONE login node, but HPC SSH aliases round-robin. `start` (`:386`) captures the FQDN *in the same SSH connection* that launches the daemon (a separate probe could resolve a different node), records it via [[state]]'s `LoginNodeStore`, and the CLI `rebind`s (`:523`) straight there next session. **`_routable_pin` (`:176`) first drops a FQDN that isn't reachable from the client** — an internal suffix (`.local`/`.internal`), a single label, or a **management-plane** name (`hostmgmt`/`cm`/`mgmt`/`ipmi`/`bmc` labels, e.g. Aurora's `aurora-uan-0009.hostmgmt.cm.aurora.alcf.anl.gov`) — falling back to the alias, so a non-routable pin can't break teardown/reconnect ([#33](https://github.com/globus-labs/hpc-bridge/pull/33)).

> [!warning] PBS cancel reads bare `qstat -f`, never `-u`
> Slurm block-release matches `squeue -u`, but PBS Pro's `-u` filter suppresses full-format output entirely — so `_cancel_blocks_pbs` (`:466`) uses bare `qstat -f` (unwrapping its 80-col line continuations) scoped by the endpoint-unique `uep.<eid>` marker → `qdel`. A `-u` filter would silently no-op and let the block burn to walltime (caught in live Polaris validation, [#28](https://github.com/globus-labs/hpc-bridge/issues/28)). The marker scoping means it never touches another endpoint's jobs, same as the Slurm path.

> [!warning] `gce list` parsing is fail-loud
> `status`/`endpoint_id` parse `gce list`'s ASCII pipe-table via `_parsed_rows` (`:320`); a gce version/format change **raises** rather than being misread as "no endpoints" (which would trigger a wrong re-provision). See [#8](https://github.com/globus-labs/hpc-bridge/issues/8).

> [!note] Persistent SSH (ControlMaster) — authenticate once
> `SshTarget.argv` (`:66`) appends `ControlMaster=auto` + a `%C`-keyed `ControlPath` + `ControlPersist` (configured by `_control_settings`, [[server]]) when a socket dir is set, so all of a facility's SSH — the ~10-call cold bootstrap *and* the [[discovery]] probe — rides **one authenticated connection**. On a key facility the master opens non-interactively; on an MFA facility the user pre-opens it once (one Duo) and the server's `BatchMode` calls multiplex over it. `close` (`:527`) tears it down (`ssh -O exit`); `ControlPersist` self-reaps regardless. *(Honest nuance: the post-`start` `rebind` to the pinned node means a cold bootstrap ends with two masters — alias + node — so 2 auths, not 1; still decisive vs ~10.)* The socket dir is kept **short** by `_short_control_dir` ([[server]]): ssh checks the whole expanded `ControlPath` against the Unix socket cap (~104 bytes on macOS), so a deep `HPC_BRIDGE_STATE_DIR` would fail every SSH with `ControlPath too long` before authentication ([#50](https://github.com/globus-labs/hpc-bridge/issues/50)).

> [!note] Endpoint reuse (zero-SSH reconnect)
> `find_online_endpoint` reuse is the keystone that lets a reconnect session avoid SSH **entirely** — one of two MFA mitigations (the other is persistent SSH, above) ([#3](https://github.com/globus-labs/hpc-bridge/issues/3)). It gates on `manager_online` **alone — no liveness probe** (a probe can't tell a dead ghost from a cold-starting fresh worker); a dead "online" ghost is instead recovered downstream, where the robust [[Warmth, the canary & cold-start|canary]] maps its shut-down Executor to `provisioning` ([#37](https://github.com/globus-labs/hpc-bridge/issues/37)). See [[Two-channel architecture]] and [[Discovery today]].

## See also
[[Standing up the endpoint]] · [[Credential seeding]] · [[MEP & templated endpoints]] · [[facility-base]] · [[facility-mep]] · [[state]] · [[credentials]] · [[MFA and interactive SSH auth]]

## Issue #39 — the "registration lag" that was a wrapped table (fixed 2026-09-04)

`globus-compute-endpoint list` renders an 80-column table when there is no TTY (how `_gce` runs it over SSH) and wraps
an Endpoint Name longer than ~27 characters onto a second row. `_list_rows` parses per line and matches the name cell
exactly, so `hpc-bridge-<fqdn>` names (and the harness's per-run names) were never found: the first `details=` connect
failed with "could not find endpoint … in `list` output", the retry's `status()` missed too, re-ran `start` and hit
"Another instance is running". Recovery only worked because `find_online_endpoint` asks the web service by name.
Measured live on globus1 (gce 4.13); reproduced hermetically with the captured table (`tests/test_remote_facility.py`,
the `test_39_*` tests). Three changes in `RemoteEndpointCLI`: `_gce` exports `COLUMNS=400` (honoured without a TTY);
`start` polls for the UUID for up to 30 s (the detached daemon registers ~3.6 s after `start` returns) via
`_await_endpoint_id`; a refused `start` whose manager is already running adopts it instead of failing (gce 4.13 exits 73 with
NOTHING on either stream, so the check is `status(name) == "running"`, not the "Another instance" text). The robust follow-up remains issue #8: read `endpoint.json` / the pid file instead of parsing the table.

## Host trust (security review 2026-09-04)

`SshTarget.argv` no longer overrides `StrictHostKeyChecking` (it was `accept-new`): hpc-bridge trusts exactly what the
user's own `ssh` trusts — OpenSSH's default refuses an unknown host under `BatchMode`, and any relaxation in
`~/.ssh/config` applies. An unknown/changed key surfaces as `UNKNOWN HOST KEY` (notices) with the remedy in the user's
own terminal; while a pin is in use the pin is dropped like `CANNOT REACH`. A login-node PIN (`rebind`) sets
`HostKeyAlias=<resolved HostName of the alias>` (via `ssh -G`, because known_hosts is keyed by the resolved name, found
live) so a pin can never redirect the seeded credential to a machine whose key differs. `--` precedes the destination
in every argv; hosts are allowlisted at the model boundary (`models.SAFE_HOST`) and again in `SshTarget.__post_init__`;
`preauth_command` is shell-quoted. `teardown(wipe_credentials=True)` is what `teardown_endpoint` now calls.

`teardown()` (2026-09-04, live): stop → cancel blocks → `delete --yes` (directory + registration) → remove the
`uep.<uuid>.*` dirs → wipe the token store only if we seeded it → drop the endpoint record; it RETURNS what it did and
`_teardown_endpoint` composes its notice from that. Before, it only stopped: the endpoint stayed registered and the next
connect re-adopted it, while the tool claimed 'deleted'. The endpoint record is now written even with no routable pin,
so the seeded-credentials flag survives connect rebuilding the facility object. **0.1.18:** that write keeps the previously stored `login_host` when the handle has none (an adopted, already-running endpoint), and `seed_storage_db` refuses on the node itself when a `storage.db` is already there (`RemoteTokenStoreExists`, quoting the failed `whoami`) — `whoami` failing does not mean there is no store, and a store we did not create is never replaced or wiped.

## Teardown reports what was measured (2026-09-05)
`SlurmFacility.teardown()` returns `{stopped, deleted, credentials_wiped, ssh_closed, ssh_failed, error}` — every
key from the remote op's own result (`stop` → `(rc, stderr)`, `wipe_storage_db` → bool, `close` → bool). An rc 255
from the first op (ssh itself failed: no connection, or a one-time-code facility whose master expired) aborts the
rest with `ssh_failed`; the `LoginNodeStore` record survives unless the delete happened. The tool (`server._finish_teardown`)
answers `up` + "TEARDOWN FAILED …" for `ssh_failed`/not-stopped/raised, keeps the endpoint bound so a retry can
finish, and arms the one-time-code handoff when the denial offers a second factor. The teardown gate no longer
depends on the curated `auth_method`: with no master open it probes once (`_probe_login_node`, BatchMode `true`) —
rc 0 proceeds, a second-factor denial hands off, anything else is a failed teardown. (Review 2026-09-05, Fix-now #1.)


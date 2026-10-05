# Aurora (PBS + bastion) bring-up

> [!abstract] Status
> ALCF **Aurora** — the first **PBS + bastion/MFA** facility. The SSH → bootstrap → PBS-provisioning path is proven live end-to-end; the billed compute block is gated on an **active Aurora allocation** (the test project is storage-only), so `interface=hsn0` is **validated-pending-allocation**.

## The two-hop bastion — and why it needs no new code

Aurora is reached via `bastion.alcf.anl.gov` (a pass-through) → a login node (`aurora.alcf.anl.gov`, which round-robins to a UAN). hpc-bridge needs **no new SSH code**: `SshTarget.argv` ([[facility-remote]]) builds a plain `ssh user@host` and OpenSSH reads `~/.ssh/config`, so a `ProxyJump bastion.alcf.anl.gov` block makes the hop transparent. A fresh connection wants **two MFA passcodes** (one per hop); the [[MFA and interactive SSH auth|ControlMaster]] then multiplexes so the rest of the session never re-auths — the same "authenticate once" substrate as any MFA facility.

## The management-hostname pin (fixed)

Aurora's `hostname -f` is `aurora-uan-0009.**hostmgmt.cm**.aurora.alcf.anl.gov` — a **management-plane** name not routable through the bastion. Pinning it would break teardown/reconnect, so `_routable_pin` ([[facility-remote]]) now drops management labels (`hostmgmt`/`cm`/`mgmt`/`ipmi`/`bmc`) and falls back to the alias ([#33](https://github.com/globus-labs/hpc-bridge/pull/33)).

## The discovered config

| field | value | note |
|---|---|---|
| `scheduler` | `pbs` | |
| `interface` | `hsn0` | Slingshot NIC on the UAN, on the compute fabric — **the crux**, validated-pending-allocation |
| `partition` (queue) | `debug` | 1–2 nodes, 1 hr |
| `scheduler_options` | `#PBS -l filesystems=home:flare` | **mandatory** — omit and the job is held; `home` for the venv, `flare` for scratch |
| `scratch_root` | `/lus/flare/projects/<project>/{user}/.hpc-bridge` | **project-based** (per-user) — why Aurora isn't a clean [[Facility catalog]] entry yet (a catalog `scratch_root` is `{user}`-templated, not project-templated) |
| `env_setup` | `module load python/3.12.12` → idempotent venv + `pip install globus-compute-endpoint` | login node reaches PyPI; the idempotent guard means the compute node reuses the shared `/home` venv (hence `filesystems=home`) |
| `cpus_per_node` | `104` | |

## Proven vs pending

- **Proven live** (2026-07): the two-hop MFA bootstrap, ControlMaster reuse, `globus-compute-endpoint` install on the UAN, the PBS provisioning attempt, and a clean stop (no leaked pilot). The [#32](https://github.com/globus-labs/hpc-bridge/issues/32) pilot-rejection observability was *surfaced* here — a `qsub` rejected with `No active allocation found for project … and resource aurora` now shows up in the `provisioning` notice instead of a silent "allocating nodes…".
- **Pending an allocation**: the compute pilot never runs (the test project has no active Aurora compute), so `interface=hsn0` is unvalidated live — one `account=` swap + rerun when an allocation lands. If it stays cold *with* an allocation, the interface is wrong (try `hsn1`, then a `bond0`).

## Testing it as a new user

`agentic/clean-session.sh` launches a pristine Claude Code session — no `~/.claude` priors, an **isolated** `HPC_BRIDGE_STATE_DIR` sandbox, and **nothing forced** (no `HPC_BRIDGE_SSH_HOST` override — see [#35](https://github.com/globus-labs/hpc-bridge/issues/35)) — so a cold agent drives Aurora exactly as a real user would (discover → propose → confirm), not from leaked cache.

## See also
[[facility-remote]] · [[MFA and interactive SSH auth]] · [[Standing up the endpoint]] · [[MEP & templated endpoints]] · [[Cost control]] · [[Facility catalog]]

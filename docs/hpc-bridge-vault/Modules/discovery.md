# discovery.py

> [!abstract] Role
> The raw-SSH **login-node probe** for an **un-indexed** facility: one batched command → a *proposed* [[models|FacilityDetails]] draft the user confirms. The cache-miss path of the [[Facility catalog|catalog]] — the pre-endpoint discovery channel the [[Globus index discovery channel]] reserved, now wired.

## What it does

`discover_facility_details(target)` (`discovery.py:50`) runs **one** batched `ssh_exec` over a bare [[facility-remote|SshTarget]] — just an `ssh_host` plus `~/.ssh/config` credentials, with **no endpoint and no catalog entry needed yet** — then `parse_probe()` (`:131`) turns the output into a `(FacilityDetails, notes)` pair. `notes` names the low-confidence fields the agent must confirm with the user — `interface` above all.

## How it works

- **One probe, framed.** `_PROBE` is a single compound bash script (one SSH round-trip ⇒ MFA-once-friendly) emitting `KEY=value` lines between `HPCB_PROBE_BEGIN`/`END` sentinels (so a login banner can't pollute the parse); multi-valued facts (`PART`/`QUEUE`/`NIC`) repeat their key. `_collect()` reads only the framed block.
- **Scheduler detection.** The probe checks `sbatch` (⇒ `scheduler="slurm"`) then `qsub` (⇒ `"pbs"`); neither ⇒ default `slurm` + a flag. This picks the queue source (`sinfo` vs `qstat -Q`) and, downstream, the [[facility-remote|scheduler template]].
- **Deterministic per-field proposers** (never model inference): `_interface()` (`:252`) picks the worker NIC from `ip -o -4 addr`, ranking the dedicated compute fabric (`_FAST_NIC_ORDER` — `hsn`/`ipogif`/`ib`/`hsi` over a `bond` mgmt NIC), else the single candidate — *always* flagged; `_default_partition()` (Slurm `sinfo`, among the partitions a job may name, then checked with `sbatch --test-only`: below) / `_default_queue()` (`:243`, PBS `qstat -Q`) prefer a cheap `debug`/`shared`/`prod`; scratch from `$SCRATCH`/`$WORK`/`$HOME` (templated to `{user}`); `_env_setup()` returns an activate line if a **working** gce is already installed, else hpc-bridge's own short line (`_UV_ENV_SETUP` = `endpoint_venv.short_env_setup("{venv}")`, after a site `module load` when there is no uv on PATH but a uv or matching-Python module); the line only activates, and hpc-bridge builds the venv over SSH when it starts the endpoint ([[facility-endpoint_venv]]); `_allocation()` detects `mybalance`/`xdusage`.
- **An existing install is checked too.** The probe's `hpcb_state` runs an entry point's `version` and its interpreter's major.minor, with bytecode writes off and each call capped at 20 s by `timeout` where it exists (a cap that hits reads "timed out", never broken). For a gce on PATH (`GCE_STATE`) the interpreter comes from the entry point's shebang or a `/bin/sh` launcher's exec line (`hpcb_py`); for the keyed venv a bring-up would use (`VENV`/`VENV_STATE`, from `_probe_script`) it is `bin/python`. A gce on PATH that fails, or runs another Python than the client, is passed over for hpc-bridge's own venv and never repaired (it is not ours). The proposal's note says what happens to the keyed venv: reused, repaired when the endpoint starts, refused (not a venv), or could not be checked in time. The draft is kept server-side (`AppCtx.proposed_drafts`), so `details=` may omit env_setup.
- **`{user}` templating** comes from the probe's own `whoami`, so the draft is per-user with no env var.

Reached from `connect_facility`'s registry-miss path: `_propose_or_ask` ([[server]] `:1394`) builds the bare target and returns `phase="proposed_facility_details"` (or `needs_preauth` when the host offers an interactive login — `NeedsPreauth` from `is_interactive_auth_failure`, [[facility-remote]]). Any other probe failure is a `failed` result; on `main` its notice is the raw `discovery over SSH to <host> failed: …` text — PR [#51](https://github.com/globus-labs/hpc-bridge/issues/51) (open) routes it through the bootstrap path's `_explain_provision_error` (`NO SSH ACCESS to <host> as <user>` / `CANNOT REACH`, [[Standing up the endpoint]]).

> [!note] The full resolution ladder
> This probe is the **last rung**. After the Globus login gate ([[login]]), `connect_facility` walks an explicit `details=` → the **registry** → `facilities.json` (the local BYO cache) → probe, first hit wins — the agent never reads the cache itself; it's resolved server-side inside the one call. See the interactive diagram: **[the resolution ladder](../assets/discovery-resolution-ladder.html)** (open in a browser), which also maps where the agent can *deviate* — bypassing `connect_facility`, an inconsistent `ssh_host` key, or a fabricated `details=`.
>
> > [!note] Superseded 2026-09-03 ([#49](https://github.com/globus-labs/hpc-bridge/issues/49)): the cache used to come *before* the catalog
> > The earlier order was session → cache → catalog → probe. The registry now wins for any catalogued id — a stale SSH-era `globus1` in the maintainer's cache would have shadowed the registry's MEP entry and silently taken the SSH path. The cache serves only ids the registry doesn't know (or when the registry is unreachable).

> [!warning] Propose, don't invent
> Discovery *proposes* discovered facts for the user to confirm; it must not silently commit. A wrong `interface`/`env_setup` is caught by the [[Warmth, the canary & cold-start|canary]] (the worker never registers), so the draft is **checkable, not trusted** — elicit → propose → confirm → validate.

**What each partition hands out (2026-10-24, [[Resource requests]]).** The probe also prints `PSHAPE=` (`sinfo
-o '%P|%c|%m|%G'`), `SELECT_TYPE=` / `SELECT_PARAMS=` (`scontrol show config`), `PDETAIL=` (`scontrol show partition
-o`) and `QOS=` (`sacctmgr -nP show qos format=Name,MaxTRES,MaxTRESPU`), each capped by `hpcb_capped`; on PBS `PNODE=`
(`pbsnodes -a`: ncpus, mem, ngpus, sharing, queue), `PQUEUE=` (`qstat -Qf` resources_default/max) and `PSERVER=`
(`qstat -Bf`). `parse_probe` classifies them with [[resources]] `classify_slurm` / `classify_pbs` into the draft's
`partitions` and adds `resources.summary` to the notes. The live Anvil probe is the fixture
(`tests/resource_fixtures/anvil-probe.txt`).

**A partition the scheduler takes (2026-10-29, the Perlmutter report).** On NERSC Perlmutter the probe proposed
`system`, sinfo's starred default. NERSC's submit filter refuses every job that names it ("Job request does not match
any supported policy."). A job there names a node type (`-C cpu` or `-C gpu`) and either a QOS or one of debug /
regular / shared, and the filter picks the real partition. The `nersc` fake profile carries that policy
(`job_submit.lua`; scenario `partition_proposal_policy`). Two changes:

- **Only partitions a job may name.** `_default_partition` leaves out a partition that sinfo's `%a` says is not `up`,
  that scontrol says is not `State=UP`, that is `RootOnly`, or whose `AllowGroups` names none of the user's groups (the
  probe's `UGROUPS=`, from `id -Gn`). Of the rest it picks as before: a cheap name (`_PARTITION_PREFS`, exact match),
  else the starred default, else the first. AllowAccounts and AllowQos are not read: the probe does not collect the
  user's associations, and the test below catches them.
- **The terms, tested in the same round trip.** On Slurm the probe asks `sbatch --test-only` (it queues nothing, but
  runs the site's submit filter) about at most six terms, and stops at the first one taken (or at a test that times
  out). `_TERMS_AWK` builds them from what the probe read, under the same rules: the pick; the pick with each of the
  two node features most of its nodes carry (`sinfo %f`); no partition; no partition with those features. Each answer
  is a `TRY=<partition>|<constraint>|<rc>|<words>` line. `_checked_partition` then decides the proposal:
  - The pick taken as it is: no note.
  - Other terms taken: those terms, with `scheduler_options: "#SBATCH --constraint=<feature>"` where they carry one and
    `partition: ""` where they name none. The note says where the test was routed and quotes the refusals before it.
  - None taken: the pick stays, and the note leads with UNVERIFIED and "ask the user" (the partition, and any QOS or
    constraint) before it quotes the scheduler's replies.
  - Replies are quoted as data, not instructions. A name that is not a plain token is never proposed.

The tests run with no account (the user's default applies), the facility's default walltime and one node. On the
`nersc` profile the proposal is no partition with `#SBATCH --constraint=cpu`, which the filter routes to
`regular_milan_ss11`. A partition of `""` reaches parsl as none, so no `--partition` is written; [[server]] keeps it
when an agent sends the terms' empty queue back.

## See also
[[Globus index discovery channel]] · [[server]] · [[models]] · [[facility-remote]] · [[state]] · [[Discovery channel model]] · [[Facility catalog]] · [[MFA and interactive SSH auth]]

# MEP & templated endpoints

> [!abstract] In one line
> One endpoint = a **manager daemon** on the login node plus a **Jinja config template** rendered *per task* into a User Endpoint Process; that UEP's Parsl provider `sbatch`s a block and runs the work on a compute node.

## What it is

The endpoint hpc-bridge *stands up* is a Globus Compute **v4 Multi-User Endpoint (MEP)** run in *personal / single-user* mode. "Multi-user" here names the **manager + templated-UEP** architecture — *not* identity mapping. (The *other* sense — a manager the **facility** runs in true multi-user mode with identity mapping, which hpc-bridge *consumes* with zero SSH — is [[facility-mep]]; the architecture below is the same, only who owns the manager and the template differs.)

- **Manager** — a detached daemon on one login node. Its `config.yaml` is **engine-free** (just `display_name`, `amqp_port`). It registers with Globus and listens on AMQP.
- **UEP (User Endpoint Process)** — forked by the manager *per task*. Its engine + provider come from `user_config_template.yaml.j2`, rendered from the `user_endpoint_config` dict the task carries.
- **Block → compute node** — the UEP's Parsl provider (`SlurmProvider` / `PBSProProvider` / `LocalProvider`) submits a scheduler block; a Parsl worker starts on the compute node and connects back over the high-speed `interface` (see [[Two-channel architecture]]).

```mermaid
flowchart TD
  T["task + user_endpoint_config"] --> M["Manager daemon<br/>config.yaml (engine-free)"]
  M -- "fork + render template" --> U["UEP<br/>user_config_template.yaml.j2"]
  U -- "SlurmProvider.sbatch" --> B[Slurm block]
  B --> W[Parsl worker on compute node]
  U -. "LocalProvider → login node<br/>(shape=login)" .-> L[Login-node worker]
```

## Shapes: one endpoint, many configs

`user_endpoint_config` is a named bag of template vars — a **shape** ([[shapes]]). `shape="compute"` renders a scheduler block (billed compute); `shape="login"` renders a `LocalProvider` (a free process on the login node — also the no-SSH discovery channel). `compute` is **scheduler-neutral**: the facility's `profile.scheduler` (`config_template`, `remote.py:657`) selects `_SLURM_TEMPLATE`/`_PBS_TEMPLATE`, and thus the provider (`SlurmProvider` / `PBSProProvider`) and launcher. A facility MEP serves **only** `compute` (its schema rejects the `LocalProvider` login shape) — see [[Resource shapes & the spend floor]].

## How it shows up in the code

- The template + default vars: `SlurmFacility.config_template()` ([[facility-remote]], `remote.py:657`); a `MEPFacility` returns only the defaults — the facility owns the template ([[facility-mep]]).
- The manager + UEP config written at provision: `provision()` (`remote.py:753`).
- The shape → vars mapping: `shape_config()` ([[shapes]]).

> [!warning] `config.yaml` must be engine-free
> `gce start` runs an *EndpointManager*; if the engine is in `config.yaml` it fails — the engine must live in the UEP template (the v4 manager+template model). `configure` also forces `--multi-user false`; the default auto-selects an identity-mapping MEP from POSIX capabilities.

> [!warning] Branch on the boolean `compute`, never a string compare
> The manager runs `user_opts` through `_sanitize_user_json`, which `json.dumps`'s every *string* (so `"SlurmProvider"` arrives as `'"SlurmProvider"'`). A template `{% if provider_type == 'SlurmProvider' %}` then silently fails and drops the whole provider block. Booleans pass through untouched, so the template branches on the `compute` bool ([[shapes]]) — which also keeps the shape scheduler-neutral, since the provider comes from the per-scheduler template, not this discriminator. This was a real, hard-to-see bug ([#5](https://github.com/globus-labs/hpc-bridge/issues/5)); the `slurm`→`compute` rename landed in [#28](https://github.com/globus-labs/hpc-bridge/issues/28).

> [!note] `run_in_sandbox: true` — and why it's safe
> The engine sets `run_in_sandbox: true`: ShellFunctions expect a sandbox, and without it every task logs *"Task sandboxing will not work due to endpoint misconfiguration."* Sandboxing runs each task in `tasks_working_dir/<TASK_UUID>` — harmless here because the [[Session continuity|session shim]] immediately `cd`s to an absolute `<scratch>/sessions/<id>` path, overriding the sandbox landing dir. The config is written at `configure` time, so a flip only takes effect on a **freshly bootstrapped** endpoint, not a reused one.

## See also
[[Resource shapes & the spend floor]] · [[shapes]] · [[facility-remote]] · [[facility-mep]] · [[Standing up the endpoint]] · [[Two-channel architecture]] · [[Endpoint reuse and MEP integration]]

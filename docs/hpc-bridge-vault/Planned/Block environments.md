# Block environments (`env`)

> [!abstract] In one line
> Point work at a software environment: one that already exists on the facility (`activate`), or one built from a requirements list. It works for interactive commands, batch rows, Jobs-mode jobs and handoffs.
>
> - **PR 3 (on PR 2, batch row sizing): built 2026-11-05; the live cells are next.** It covers existing envs everywhere, facility endpoints included, and built envs on an endpoint hpc-bridge stood up over SSH. A built env is made on the login node (free) and activated per command, row and job. Module [[envs]].
> - **PR 4 (branch `feat/block-envs-mep`):** building on a facility endpoint. It comes after a live check; its open items are below.

## The user's decisions (2026-11-04)

1. **On an endpoint hpc-bridge stood up over SSH, a built env is a separate user env**, never the worker's env.
   - It is built on the login node, free, and activated per command, row and job script.
   - Switching env therefore never needs a new block.
2. **Where a built env lives:** in project or scratch space when known, else in home with a size warning.
   - A purged env is rebuilt.
   - It is verified before use, with a marker and a hash.
3. **On a facility endpoint whose template reads `requirements`** (Delta 4.19 and Anvil 4.16, both checked live), the requirements are passed through. That means a new user endpoint and a new block, and the terms say so.
   - Where the template does not read them (Polaris 4.9), the env is built by `worker_init` under `flock`, and the terms say the first build is billed.
   - **This is PR 4's** (the checkpoint, below).
4. **Settled at the checkpoint:** a built env's default Python is the client's major.minor (`uv venv --python X.Y` finds it or fetches a managed build).
   - One env spec then means one Python on every kind of facility.
   - The key is known before any remote call.
   - It is never whatever `python3` a login node's PATH offers today. On RHEL 8 that is 3.6, which uv cannot use.
   - Cost: a client on a very new Python may meet a package with no wheel yet. The build then fails in uv's own words, and the user names `python`.
   - The facility's module Python is one `activate` away.

## The split: PR 3 and PR 4 (the checkpoint, 2026-11-05)

Stage A wrote the whole design failing first. At the checkpoint the user split it in two.

### PR 3 (`feat/block-envs`): this note, as built

- **On an SSH endpoint**, a built env is made on the free login node and activated per command, row and job, with no new block on a switch.
- **Existing envs (`activate`) work everywhere.** That includes a facility's multi-user endpoint, where an existing env needs no new block either.
- **`requirements` on a facility endpoint is refused by name**, and nothing in the call is set or started. The refusal reads: "building an env on a facility endpoint is not supported yet: use an existing env there with `env={"activate": "<shell line>"}` — a module or a conda env the facility provides".
- **A handoff on a facility endpoint** carries an existing env for the consumer to activate.

### PR 4 (`feat/block-envs-mep`): open, for the live check

PR 4 covers the pass-through, the R13 pin fix and the `worker_init` fallback.

- **Starting point:** branch `feat/block-envs-mep` starts at stage A's `ffb1413`. Its unit tests (13, still strict xfail) moved into `tests/test_block_envs_mep.py` on that branch only.
- **Its harness, on that branch only:**
  - a second fake manager on the `pbs_mep` profile, `hpcb-mep-pbs-req` (catalog entry `fake-pbs-mep-req`, with a `_req` template and schema);
  - the cells `env_mep_requirements` and `env_mep_fallback`, with their graders.
- **What PR 3 already has for it:** `envs.delivery(…, reads_requirements=, reads_worker_init=)` takes the gate's answer. PR 3 never asks it, because `MEPFacility.reads` is PR 4's.

**Open items, each to be settled by a live check:**

- **R11, a compute node of another architecture.** It cannot be reached on SSH today: on such a node our own endpoint venv fails first. Ruling 10 below drops the build-on-block path from PR 3 and refuses by name. Building per architecture waits for [[Per-architecture endpoint venvs]]. The key already appends `uname -m` on the node, so nothing about the key changes.
- **R12, the pass-through bills too.**
  - Globus Compute's own template builds the env in `worker_init`, on the compute node (its docs, and the 4.17 changelog).
  - So the new block's first minutes are billed, and its nodes must reach the package index.
  - The terms must say both. The stage A design had said only "a new user endpoint and a new block".
- **R13, the facility pin hides the env (Delta, Anvil).**
  - 4.17's template runs its env script before the user's `worker_init`.
  - The entries' pins create and activate a venv of their own: `anvil.yaml:11`, and on Delta gce 4.16 plus parsl 2026.8.10.
  - That venv would shadow the requirements env.
  - **The live check:** which venv the worker runs, on Delta and on Anvil.
- **Proposal 11, the pass-through.**
  - The Python must be the client's. Asking for another is refused, naming both versions.
  - The normalized text goes in as `requirements`.
  - `venvs_dir` is sent only when the user gives `root`.
- **Proposal 12, the R13 fix.** With `requirements` passed through, the pin installs into the active venv when there is one, and runs as before otherwise (`pin_into_active`, on the PR 4 branch). Alternatives:
  - a catalog field `compute.worker_pins` (cleaner, but a registry change and a re-ingest);
  - dropping the pin (safe only where `worker_version: client`, i.e. Anvil).
- **Proposal 13, the fallback.** Our build is prepended to the config's `worker_init` in a subshell, `( … ) || true`.
  - It never changes the worker's env and never fails the block. A failure is written as `.failed`; the worker still starts, and the first command says why.
  - It probes the index first with `curl`.
  - A facility proxy is exported for the build alone, with its names and values checked. It comes from a catalog field `compute.proxy` or the login worker's own `https_proxy`.
- **Live, with the user's approval:**
  - a Delta or Anvil pass-through (R13's check);
  - Polaris with the proxy.

## How an env is delivered (PR 3)

| Facility | `env` | Built where | Used how | A new block? | Billed |
|---|---|---|---|---|---|
| hpc-bridge's own (SSH) | `requirements` | the login node, by its own build user endpoint | activated per command, row and job | no | no |
| the same, on a compute node of another arch | `requirements` | nowhere: the command is refused by name (ruling 10) | — | — | — |
| a facility endpoint | `requirements` | nowhere: refused by name (PR 4) | — | — | — |
| any | `activate` | nowhere: it exists | the user's line, per command | no | no |

`envs.delivery` decides which row applies.

## The API, as built

**`env`** is an argument of `ensure_endpoint_up`, `run_batch`, `submit_job` and `export_handoff`. It is an `EnvRequest`, which takes one of:
- `{"activate": "<shell line>"}`: an existing env, e.g. `module load pytorch/2.4; conda activate ml`;
- `{"requirements": "<requirements.txt text>", "python": "3.11"?, "root": "<dir>"?}`: a built env;
- either of those, with an optional `name` label;
- `{}`: no env;
- omitted: keep the session's env.

**`EnvInfo`** fields:
- `kind`: `activate` | `built`
- `how`: `activate` | `login`
- `key`: 16 hex characters
- `name`, `python`, `root`
- `path`: `<root>/<key>-<arch>`
- `arch`
- `state`: `ready` | `building` | `failed` | `unchecked`
- `task_id`, `cached`, `lock_sha256`, `size_mb`
- `new_block`: always false in PR 3
- `billed`: None (free) in PR 3
- `index_lines`, `warning`, `error`

**Where it appears:**
- `EnvInfo` is on `EndpointStatus.env`, `BlockTerms.env`, `BatchStatus.env`, `JobInfo.env` and `JobTerms.env`.
- The ledger keeps `batch.env` and `job.env` as JSON; `LedgerEntry.env` is a label.
- A handoff gets `HandoffRecord.env` (`HandoffEnv`) and `HandoffValidation.env_ok`.

**`ShellOutcome.phase` gains `env_building`:** nothing ran, because the session's env is being built or rebuilt on the login node (free). Call again.

**The module, and where it hooks in:** [[envs]] has the parts. The hooks are:
- `session_shell.wrap(…, env_sh=)`
- `batch.task_script(…, env_sh=)`
- the frozen job script
- `Ledger.add_batch(…, env=)` and the job row's `env`

**On the facility,** `<root>/<key>-<uname -m>/` holds:
- the venv, which is relocatable. It is built beside its path as `.<key>-<arch>.build.<token>` and claimed by one rename.
- `.hpcb-env`, the marker: key, short key, arch, python, the dist-info set's hash, the lock's hash, build time and host.
- `requirements.in`: the caller's text, verbatim.
- `requirements.lock`: the output of `uv pip freeze`.

A failed build leaves `<key>-<arch>.failed` beside it. uv's cache is `<root>/.uv-cache`.

## The rulings, as built

These use the checkpoint's numbering. The table and API above say what was built.

1. **Scope.** The session's env applies to its user commands on both shapes. It never applies to hpc-bridge's internal session or to the build runner.
2. **Persistence.** `{}` clears the env and omitted keeps it, as with partition. A batch's or a job's own `env` is recorded when it is created, and never changes the session's env.
3. **The key** is sha256 of `"hpcb-env-v1"`, the Python as asked, and the normalized text, cut to 16 hex characters. The arch is appended on the node.
   - Normalizing removes CRLF, trailing blanks, blank lines and comments.
   - Line order is kept: with several indexes, the first wins.
   - Names are not canonicalized, so `Six` and `six` are two envs.
   - There is no `rebuild` flag.
4. **Refused lines.**
   - Refused: `-r`, `-c`, `-e` and local paths, because the hash cannot see a file's content.
   - Also refused: `--trusted-host`, and any option outside the allow-list (`envs._ALLOWED_OPTIONS`, plus a requirement's own `--hash`).
   - The user's own index lines are kept, listed in `index_lines` and named in the notice. hpc-bridge adds none.
   - Limits: 64 KiB of requirements text, 4 KiB of `activate`.
5. **Verification.** Each command checks the marker alone: one `grep`. The full check (every RECORD-listed file present, the installed set as built) runs:
   - when this server first chooses the env (choosing it again within `VERIFY_EVERY_S` is a cache hit with nothing sent);
   - once `VERIFY_EVERY_S` (24 h) has passed;
   - when a block is newly confirmed (`envs.recheck`, from `server._ensure_endpoint_up`).

   A restarted server's first choice is the build script's own check, so an env already there is a cache hit. A partial purge between two checks shows up as an ImportError, not as a refusal.
6. **A refused command runs nothing.** It exits 88 with `HPCB_ENV_REFUSED <nonce> <why> <key> …`. The nonce is the dispatch's own, so the command's output cannot forge the line.
   - **A gone env** is rebuilt on the login node, and `run_shell` answers the new phase `env_building`.
   - **Anything a rebuild cannot mend** (the build failed, the user's line failed, the node is another arch) answers `failed`, saying why.
   - It never re-runs the command itself.
   - **A batch row** goes back to the backlog with `ran: false`. The batch stops admitting, and its status says so: "Not admitting now: …". A gone env is rebuilt and the row is re-admitted; for anything else the batch pauses, saying why.
   - **A job** exits 88 and its stderr says why. `submit_job` answers `busy` / `env_building` while its env builds.
   - **The skill** has one domain line for the new phase (nothing ran; the env is being built on the login node, which is free) and no wait cadence.
   - **The graders** learned it as they did `needs_sizing`: `cold_start_is_retried` owes it a retry, and `midrun_hooks._SHELL_PHASES` knows it.
7. **The SSH build runner.**
   - **What changed from the proposal:** the build runs on its own user endpoint on the login node, not on a second runner over the login shape's config. That endpoint is the login seed config plus `BUILD_UEC_KEY` (`hpcb_env_build: True`), which our template ignores.
   - **Why:** the login shape's user endpoint has ONE worker (`max_workers_per_node: 1`, [[shapes]]). A second Executor on the same config would take that worker or queue behind it.
   - **Its limits:** the walltime is `BUILD_S` (30 min, `HPC_BRIDGE_ENV_BUILD_S`). A call waits `WAIT_S` (20 s) and then answers `building` with the task id; the next call reads the result.
   - **Correctness is the rename.** `flock`, where the node has it, only saves a second build its work.
   - **The tests:**
     - (a) a build in progress never takes the login worker: `run_shell(shape="login")` and `stop_endpoint` both answer while it runs;
     - (b) `stop_endpoint` during a build is never held by it, cancels nothing of it, and leaves no half-claimed env;
     - two builds at once leave one env;
     - a build cut off midway claims nothing.
8. **An existing env on SSH** is run once on the login node.
   - If it fails there, the failure is said (`state=unchecked`, `error`) and the env is kept: login and compute nodes may load different modules.
   - Each command checks it again and runs nothing if it fails.
   - A facility endpoint has no login node, so there each command checks it.
9. **A failed build** leaves the session's env as it was ("not changed"). It quotes the installer's own words as data, never as instructions, and marks `.failed`.
10. **Changed at the checkpoint: another architecture is refused, not built.**
    - The per-command activation finds no `<key>-<this arch>` beside a build for another arch.
    - It refuses by name: "built for <arch>, this node is <arch>; needs per-architecture endpoint venvs (a follow-up)".
    - There is no build-on-block path. See the follow-ups.
11. **The pass-through: PR 4's** (above).
12. **The pin fix: PR 4's** (above).
13. **The fallback: PR 4's** (above).
14. **The root** is the facility's scratch root's `envs/` (on Expanse that is Lustre scratch). The user's `root` comes first.
    - A `$HOME/…` root (every facility endpoint's, and a BYO default) is said in `warning`, naming `root`.
    - There is no new catalog field: no `compute.env_root`.
15. **The ledger.** `batch.env` and `job.env` are JSON, and the listing shows a label. There is no env table.
    - Every record has kind, how, key and name.
    - An existing env's record adds its line.
    - A built env's record adds the Python built and the Python asked, root, path, arch, the lock's hash and the requirements text. With those, a restarted server can activate the env or rebuild it.
16. **Housekeeping is a follow-up:** listing envs with their sizes, removing one, warning near quota.
    - PR 3 reports `size_mb` and warns when the root's free space is under twice the env.
    - No `env-` id prefix is used anywhere: those ids are envelopes'.
17. **The install warning** is said once a session: "installing runs each package's build code as you, on your allocation's facility".
18. **The handoff.**
    - **On SSH:** the record carries `env` (how, kind, key, name, python, path, `bin`, arch, lock_sha256) for the consumer to activate. The config is unchanged. The canary runs `envs.canary_check` from inside the block, which fills `validation.env_ok`.
    - **On a facility endpoint with an existing env:** the record carries the `activate` line.
    - **The pass-through is PR 4's.**
    - The record's `env` is additive to v1: null when there is no env, and a v1 reader ignores it.
    - A consumer running Python functions gets the env only through its own activation.
19. **The session never keeps the env.** The wrapper snapshots before and after the activation, and persists only what the command itself changed. What the activation set reverts to the session's own value.
    - **Documented leak:** a command that edits PATH itself (`export PATH=$HOME/bin:$PATH`) persists the PATH it made, and that PATH holds the env's `bin`. After a switch of env, or to none, that old `bin` is still on PATH, behind the new env's.
    - It goes away with `reset_session`, or with a command that sets PATH again.
20. **In a batch row** the activation comes after the session's env and before the slot's. So a module's `OMP_NUM_THREADS` cannot widen a row.
21. **The canary has no arch field.** The activation finds the node's arch per command.

## A build cut off by a stop or a teardown

- **`stop_endpoint` never touches a build.** A build is no task of a shape: it holds no session and no worker of the login shape, and the stop has nothing of it to wait for or cancel.
  - The build runs on to its end on the login node and claims the env whole, in one rename.
  - The session's env (the choice, and what is known of it) outlives a stop. The next `ensure_endpoint_up` reads the finished build.
- **A teardown, a binding change, or a reboot or walltime on the login node can cut a build off midway.** A teardown stops the endpoint, and with it the build's user endpoint. `warmth._drop_all_shapes` forgets the env and closes its runner.
  - Nothing is claimed: the env's path never exists half-built, because the rename refuses a target that is not empty.
  - What is left is the hidden `.<key>-<arch>.build.<token>` tree and its log beside the env. A later build of the same env sweeps them after 240 minutes.
  - The next ask sends the build again. Its own check finds nothing there, so it builds from the start.
  - Tested by `test_a_build_cut_off_midway_leaves_no_half_claimed_env`.

## Follow-ups

- **Another architecture:** [[Per-architecture endpoint venvs]] first, then a build per arch. The key and the activation already carry the arch.
- **Housekeeping (ruling 16):** list, remove, quota warning. Its ids need a prefix other than `env-`.
- **A `rebuild` flag.**
- **An approved hand submit** (`server._run_held`, decision D2) runs unmodified and without the session's env. A script it submits activates the env itself, using `env.path`'s `bin`.
- **PR 4** (above).

## Tests

- **`tests/test_block_envs.py`** (PR 3, 46 tests). The world: bash is the facility, and a stub `uv` builds real venvs the real interpreter recognises; there is also a stub `curl` and a fake `uname` for another arch. It covers:
  - the argument and its refusals; the key; the root;
  - the delivery table;
  - the build, its lock, its failure and its safety (the text reaches the installer as a file); the check's states;
  - activation per command, never persisted; the switch; every refusal; a forged refusal line;
  - batch rows; the fake slot;
  - on SSH:
    - the login build, free; the cache hit across a restart;
    - switches; an existing env; a failed build;
    - its own user endpoint, with ruling 7's (a) and (b);
    - purge and partial purge; a newly confirmed block's check; another arch refused;
    - index lines; the root; the once-said warning;
    - `run_batch`'s own env;
  - Jobs mode;
  - on a facility endpoint: an existing env with no new block, a built env refused by name;
  - handoffs on SSH and on a facility endpoint;
  - the ledger; the tools.
- **`agentic/harness/envs_checks.py` and `test_envs_scenarios.py`:** the scripted cells `env_ssh_build_cache` and `env_ssh_switch`, on the fake `default` profile. They are kept out of the sweep until their live proof. They need PyPI reachable from the fake cluster's login node.

## See also
[[envs]] · [[Batch and swarm execution]] · [[batch]] · [[sizing]] · [[jobs]] · [[handoff]] · [[session_shell]] · [[shapes]] · [[Per-architecture endpoint venvs]] · [[facility-mep]]

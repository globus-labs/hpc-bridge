# Block environments (`env`)

> [!abstract] In one line
> PR 3 (on PR 2, batch row sizing): point work at a software environment — one that exists on the facility, or one built from a requirements list — for interactive commands, batch rows, Jobs-mode jobs and handoffs. **Stage A (2026-11-05): designed, tests and cells written failing first, no product code.** The user's decisions 1–3 are settled; the rest below are proposals for the checkpoint.

## The user's decisions (2026-11-04)

1. On a facility whose endpoint hpc-bridge stood up over SSH, a built env is a SEPARATE user env: built on the login node through the free login worker, activated per command, row and job script — never the worker's env. Switching env therefore never needs a new block.
2. A built env's root is project or scratch space when known, else home with a size warning. A purged env is rebuilt; it is verified before use with a marker and a hash.
3. On a facility endpoint whose template reads `requirements` (Delta 4.19, Anvil 4.16, checked live), it is passed through: a new user endpoint and a new block, which the terms say. Where the template does not (Polaris 4.9), the `worker_init` + `flock` fallback, and the terms say the first build is billed.
4. **Open:** the default Python of a built env — proposal R1 below.

## How an env is delivered

| Facility | `env` | Built where | Used how | A new block? | Billed |
|---|---|---|---|---|---|
| hpc-bridge's own (SSH) | `requirements` | the login node, by the login worker (its own runner) | activated per command / row / job | no | no |
| the same, a compute node of another arch | `requirements` | a task on the confirmed block | activated per command | no | yes (the block's time) |
| a facility endpoint whose template reads `requirements` | `requirements` | the facility's template, when the block starts | it IS the worker's env | yes | yes (that block's first minutes) |
| a facility endpoint whose template does not | `requirements` | the block's own `worker_init`, under `flock` | activated per command | yes | the first build |
| any | `activate` | — (it exists) | the user's line, per command | no | no |

`envs.delivery` decides; `MEPFacility.reads(key)` is the gate (the published template uses the key — `handoff.renders`, so `{% if requirements %}` counts — and a strict schema lets it through; None when the template was not read: never a guess). A facility that can carry neither is refused by name.

## The API (stage A fixes it; `tests/test_block_envs.py` is written against it)

- **`env`** on `ensure_endpoint_up`, `run_batch`, `submit_job`, `export_handoff` (an `EnvRequest`):
  - `{"activate": "<shell line>"}` — an existing env (`module load pytorch/2.4; conda activate ml`);
  - `{"requirements": "<requirements.txt text>", "python": "3.11"?, "root": "<dir>"?}` — a built env;
  - either with `name` (a label); `{}` — no env; omitted — keep the session's.
- **`EnvInfo`** {kind: `activate`|`built`, how: `activate`|`login`|`block`|`facility`|`worker_init`, key (16 hex), name, python, path (`<root>/<key>-<arch>`), root, state: `ready`|`building`|`failed`|`unchecked`, task_id, cached, lock_sha256, size_mb, new_block, billed (words, None when free), index_lines, warning, error} on `EndpointStatus.env`, `BlockTerms.env`, `BatchStatus.env`, `JobInfo.env`, `JobTerms.env`; `LedgerEntry.env` (a label); `HandoffRecord.env`, `HandoffValidation.env_ok`.
- **`ShellOutcome.phase`** gains `env_building`: nothing ran — the env is being built or rebuilt; call again.
- **`hpc_bridge.envs`**: `parse`, `EnvSpec`, `EnvRefusal`, `normalize_requirements`, `index_lines`, `default_python`, `env_key`, `env_root`, `Activation` / `activation_sh`, `refusal` (`ENV_RC` 88, `REFUSED`), `build_script` / `parse_build` (`BUILD_TAG`), `check_script` / `parse_check`, `fallback_worker_init`, `pin_into_active`, `delivery`, `MARKER`, `BUILD_S`, `VERIFY_EVERY_S`.
- **Hooks**: `session_shell.wrap(…, env_sh=)`, `batch.task_script(…, env_sh=)`, `jobs` Spec/job.sh, `Ledger.add_batch(…, env=)` and the job row's `env`.

**On the facility:** `<root>/<key>-<uname -m>/` holds the venv (relocatable, built beside as `.<key>-<arch>.build.<token>` and claimed by one rename), `.hpcb-env` (key, short, arch, python, the dist-info set's hash, the lock's hash, built, host), `requirements.in` (the caller's text, verbatim) and `requirements.lock` (`uv pip freeze`). A failed build leaves `<key>-<arch>.failed` beside it.

## Proposed rulings (the checkpoint's)

- **R1, the default Python: the client's major.minor** (`uv venv --python X.Y`, which finds it or fetches a managed build). One env spec is one Python on every kind of facility (the pass-through MUST use the client's: the worker runs the task functions the client serialised); the key is known before any remote call; and it is not whatever `python3` a login node's PATH offers today (3.6 on RHEL 8 login nodes, which uv cannot use). Cost: a client on a very new Python may meet a package with no wheel yet — the build fails in uv's words and the user names `python`. The facility's module Python (HPC-tuned, MPI-aware) stays one `activate` away.
- **R2, scope:** the session's env holds for the session's user commands on BOTH shapes (the login node runs its own arch's build), never for hpc-bridge's internal session or the build runner. Per-shape is the alternative.
- **R3, persistence:** `{}` clears, omitted keeps (as a partition). A batch's or a job's own `env` is that batch's or job's, recorded at creation, and never changes the session's.
- **R4, the key:** sha256("hpcb-env-v1", the Python as asked, the normalized text)[:16]; the arch is appended ON THE NODE, so the activation finds the node's own build and the server needs no arch probe. Normalized: CRLF, trailing blanks, blank lines and comments (` #` inline too) removed; the ORDER kept (with several indexes, the first wins); names not canonicalized (`Six` and `six` are two envs). The resolved patch version is in the marker. No `rebuild` flag in PR 3.
- **R5, refused lines:** `-r`, `-c`, `-e` and local paths (`./`, `../`, `/`, `file:`, `@ file:`) — the hash cannot see a file's content; `--trusted-host` and any option off an allow-list (`--index-url`/`-i`, `--extra-index-url`, `--find-links`/`-f`, `--no-index`, `--pre`, `--prefer-binary`, `--only-binary`, `--no-binary`, a requirement's own `--hash`). The user's index lines are kept, listed (`index_lines`) and named in the notice; hpc-bridge adds none. Caps: 64 KiB of text, 4 KiB of `activate`.
- **R6, verification:** per command, the marker only (one `grep`). The full check — every RECORD-listed file present, the dist-info set as built — when the env is chosen, when a block is newly confirmed, and once `VERIFY_EVERY_S` (24 h) has passed, on the free login worker. A partial purge between two checks surfaces as an ImportError, not as a refusal.
- **R7, a refused command** (`gone`, `failed`, `arch`, `activate`) runs nothing: rc 88 and `HPCB_ENV_REFUSED <nonce> <why> <key> …`, the nonce the dispatch's (output cannot forge it). Where a rebuild can mend it (`gone`, `arch`, a failed check) `run_shell` answers `env_building` while it runs; where it cannot (`activate` failed, a build that failed) it answers `failed` with the reason. It never re-runs the command itself. A batch row goes back to the backlog (`ran: false`) and the batch waits (`waiting`, reason env) for the rebuild; a job exits 88.
- **R8, the SSH build:** its own runner on the login shape's config (the same user endpoint, its own Executor, walltime `BUILD_S` = 30 min, `HPC_BRIDGE_ENV_BUILD_S`) — never the internal session, which a stop's scancel needs. Longer than the sync-wait: `state=building` with the task id, read by the next call. Correctness is the rename (as the endpoint venv's); `flock`, where present, only saves a second build's work. uv's cache goes to `<root>/.uv-cache`: large wheels stay off the home quota.
- **R9, an existing env on SSH** is run once on the login worker; a failure there is said (`state=unchecked`, `error`), the env kept, and each command checks it again (login and compute nodes may load different modules). The alternative is refusing it.
- **R10, a failed build** leaves the session's env as it was ("not changed"), quotes the installer's own words (data, never instructions), and marks `.failed`.
- **R11, another architecture (SSH):** found per command; built in a task on the confirmed block (billed, said), keyed by that arch. **Finding:** reachable only once [[Per-architecture endpoint venvs]] lands — today our endpoint venv fails first on such a node. Cheap to build anyway (the same script on another runner).
- **R12, the pass-through:** the client's Python (another is refused, naming both); the normalized text as `requirements`; `venvs_dir` only from the user's `root`. **The terms say it bills too:** Globus Compute's own template builds the env in `worker_init`, on the compute node (its docs, 4.17's changelog) — so the block's first minutes are billed and its nodes must reach the package index; the design had said only "a new user endpoint and a new block".
- **R13, the facility pin vs the template's env (Delta, Anvil):** 4.17's template runs its env script BEFORE the user's `worker_init`, and the entries' pins (`anvil.yaml:11`; Delta's gce 4.16 + parsl 2026.8.10) create and activate a venv of their own — which would SHADOW the requirements env. Proposal: with `requirements` passed through, the pin installs INTO the active venv when there is one (`pin_into_active`), else runs as before. Alternatives: a catalog field `compute.worker_pins` (cleaner; a registry change and re-ingest), or dropping the pin (safe only where `worker_version: client`, i.e. Anvil). The live check: which venv the worker runs, on Delta and on Anvil.
- **R14, the fallback:** our build is prepended to the config's `worker_init` in a subshell `( … ) || true` — it never changes the worker's env and never fails the block (a failure is written as `.failed`; the worker still starts and the first command says why). It probes the index first (`curl`); a facility proxy (a catalog field `compute.proxy`, or the login worker's own `https_proxy`) is exported for the build alone, its names and values checked.
- **R15, the root:** `<scratch_root>/envs` (Expanse: Lustre scratch); a `$HOME/…` scratch root (every facility endpoint's, a BYO default) is home, with a warning naming `root`. Proposed for the registry: an optional `compute.env_root` per entry.
- **R16, the ledger:** `batch.env` and `job.env` (JSON: kind, how, key, python, path, lock); the listing shows a label. No env table yet.
- **R17, housekeeping** (list envs with sizes, remove one, warn near quota): a follow-up PR. PR 3 reports `size_mb` and the root's free space at a build, and warns when it is under twice the env. (`env-` ids are taken by envelopes: a later env id needs another prefix.)
- **R18:** "installing runs each package's build code as you, on your allocation" is said once a session.
- **R19, the handoff:** on SSH the record carries `env` (how, key, path, python, lock, arch) for the consumer to activate; the config is unchanged, and the canary checks the env from inside the block (`validation.env_ok`). On a pass-through facility, `requirements` goes into the consumer's config (refused if it carries another). A consumer running Python FUNCTIONS on SSH gets the env only through its own activation.
- **R20, the session never keeps the env:** the wrapper snapshots around the activation and persists only what the command itself changed; what the activation set reverts to the session's own value. A command that edits PATH itself keeps the env's bin in it (documented).
- **R21:** in a batch row the activation comes after the session's env and BEFORE the slot's, so a module's `OMP_NUM_THREADS` cannot widen a row.

## Tests (failing first)

- `tests/test_block_envs.py` (51, a strict xfail module): the argument and its refusals, the key, the root, the delivery table and the gate on each template kind (Delta, Anvil, Polaris, the fake `pbs_mep`'s two, neither, unread); the build, its lock, its failure, its safety (the text reaches the installer as a file), the check's five states; the activation (per command, never persisted, the switch leaving nothing, every refusal, forgery); batch rows (the env, the slot after it, a gone env → backlog and a rebuild); the fallback worker_init (once, never touching the worker, never fatal, the index probe and the proxy, two at once); the pin; the server on SSH (login build, cache hit across a restart, switches, existing env, failed build, its own runner, purge, partial purge, another arch, index lines, root, the once-said warning, run_batch's own env); Jobs mode; the facility endpoint (pass-through and its terms, the fallback, neither, unread, an existing env, Anvil's pin); the handoff; the ledger; the tools.
- `agentic/harness/envs_checks.py` and `test_envs_scenarios.py`; scripted cells `env_ssh_build_cache`, `env_ssh_switch` (fake `default`), `env_mep_requirements`, `env_mep_fallback` (fake `pbs_mep`, whose second manager `hpcb-mep-pbs-req` builds `requirements`). Kept out of the sweep until their live proof.
- Live, later, with the user's approval: a Delta or Anvil pass-through (R13's check), Polaris with the proxy.

## See also
[[Batch and swarm execution]] · [[facility-endpoint_venv]] · [[facility-mep]] · [[batch]] · [[sizing]] · [[jobs]] · [[handoff]] · [[session_shell]] · [[Per-architecture endpoint venvs]]

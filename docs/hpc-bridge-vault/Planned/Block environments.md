# Block environments (`env`)

> [!abstract] In one line
> Point work at a software environment: one that already exists on the facility (`activate`), or one built from a requirements list. It works for interactive commands, batch rows, Jobs-mode jobs and handoffs.
>
> - **PR 3 (on PR 2, batch row sizing): built and reviewed (two rounds) 2026-11-05; both live cells pass on the fake cluster.** It covers existing envs everywhere, facility endpoints included, and built envs on an endpoint hpc-bridge stood up over SSH. A built env is made on the login node (free) and activated per command, row and job. Module [[envs]].
> - **PR 4 (branch `feat/block-envs-mep`):** building on a facility endpoint. It comes after a live check; its open items are below.

## The Anvil live test (2026-11-05, on PR 3's head `822347c`)

Fixed test-first on `fix/anvil-live-findings` (`tests/test_anvil_live_findings.py`, `…_node.py`; node tests under bash 3.2 and 5.2):
- **F5, `module` in a facility worker's shell.** On Anvil's multi-user endpoint `env={"activate": "module load anaconda/2025.12-py313"}` was refused — `module: command not found`, rc 127 — while `. /etc/profile.d/modules.sh; module load …` worked: a worker runs a non-login shell where no module system is initialised. A line that uses `module` or `ml`, where neither is defined, now sources the first module init there is first ([[envs]] `MODULE_INITS`); one that still finds none is refused saying the worker shell has no module system initialised and naming the init line to add (`module_words`). The SSH endpoint's plain line worked and is unchanged (`module` is defined there: nothing sourced).
- **F4, the endpoint venv in user commands.** A command saw `VIRTUAL_ENV` (and its `bin` first on PATH) naming hpc-bridge's own endpoint venv, so `uv pip install` / `pip install` could write into the venv the worker runs from (proven with a stub uv honouring VIRTUAL_ENV). Session commands, batch rows, job scripts and the env checks now run without it ([[session_shell]] `ENDPOINT_VENV_SCRUB`); a built env sets its own over it; a user's own venv is kept.
- **G1.** A pending env whose build fails after the call that asked for it is now a structured `env_failed` (EnvInfo: its key, name and error) on the answer that first reports it — ensure_endpoint_up's, run_shell's, run_batch's or submit_job's (review round 1: the last two had dropped it) — beside the session's unchanged `env`.
- **Review round 1:** `module`/`ml` count only as a command word; the module init runs under the line's EXIT trap (an init that calls `exit` refuses); the login node's check carries the no-module-system hint; the endpoint venv's scrub drops every hpc-bridge venv's bin from PATH, an older keyed one included.

## Review round 2 (2026-11-05, on `8e63a6d`)

Round 1 checked out: both live cells passed on the fake cluster, `env_ssh_switch` included once L1 was fixed. Round 2 found 20 more defects, R2-S1..S11 on the server and R2-N1..N9 on the node, and the coordinator ruled on 15 suspicions (SS2..SS7, NS2..NS9). All are fixed as ruled; the tests are in `tests/test_block_envs_r2.py`.

**The nodes run bash 4.2–5.x** (CI: Ubuntu, bash 5.2); macOS is bash 3.2. Every node test passes under both. Two bash differences shaped the fixes:
- **errexit.** bash 4+ ignores errexit for everything inside an `if` condition, and on the left of `&&` or `||` — subshells included.
  - So an activate line runs outside any condition (`eval …; __hb_erc=$?`), and errexit is off before it (`_HEAD`), so a `set -e` in the line is the line's own.
  - A line `set -e; false; export HALF=1` had run half-activated on bash 5 (R2-N9).
  - A check runs its subshell standalone, never on the left of `&&`.
- **`case` patterns.** bash 3.2 misparses a bare `pat)` inside `$( … )`, so every `case` in the activation writes `(pat)`.

### What changed in how it works

- **One activation, three uses (R2-N7, SS7).** The login node's check of an existing env (`activate_check_script`) and the handoff canary's env check (`canary_check`) run the very activation every command runs, in a subshell. They never pass a line, or an env, that every command refuses. That covers the root/env ownership check and a line that ends the shell.
- **What the activation leaves (R2-N5, NS6, NS7, R2-N6):**
  - It restores allexport, xtrace and IFS too, beside errexit, nounset, noclobber, pipefail and the cwd.
  - It leaves nothing of itself in the shell: not the refusal function, not the nonce.
    - **Residual:** the user's own code can read the wrapper's text from `$BASH_EXECUTION_STRING`. That can only make its own row run again.
  - **A line's own EXIT trap** is kept: it is the one restored after ours.
    - **Residual:** a line that sets an EXIT trap AND ends the shell runs its own trap, not our refusal.
  - **A refusal made by the EXIT trap** first runs the shell's own EXIT trap with status 88, so the session's kept rc is written (a poll after a restart reads it).
- **The wrapper's own loops (NS8).** They run in a known state (`set +x +a +e +u`, a plain IFS, nocasematch off), restored after. The wrapper's `__hb_*` variables are never persisted.
- **The full check reads RECORD as csv (R2-N1).** It uses the env's own Python (`-I`): a path holding a comma is quoted, as wcwidth's `textwrap.py,cover` is.
- **Requirements (R2-N2).**
  - EVERY option and value on an option line is checked (`shlex.split`).
  - A bare archive name (`.whl`, `.tar.gz`, `.zip`…) is refused: uv reads it as a path.
- **The build's umask (R2-N3)** is the node's OR 022, never looser.
  - `requirements.in` (an index URL's credentials included), `requirements.lock` and the build log are 0600, then 0400 once the env is read-only.
  - A `.failed` mark is 0600.
- **Where uv runs, and what is named (R2-N4, NS5).** uv runs from a fresh, empty `mktemp -d` of the user's own (under `$HOME`), never in or under the env root: uv reads a project's config from its cwd and every parent. It does not use `--no-config`, since a site's user-level `uv.toml` may hold the mirror a build needs. The answer names:
  - every `UV_*` and `PIP_*` variable set (names only);
  - the config files that apply: `UV_CONFIG_FILE`'s, `$XDG_CONFIG_HOME/uv/uv.toml` or `~/.config/uv/uv.toml`, `/etc/uv/uv.toml`, any project config above uv's cwd, and pip's.
- **Two builds at once without flock (R2-N8).** A build re-checks the target just before it moves anything aside. If another build already claimed a ready env, that one stands and this one is discarded (`cached=1`).
- **The build lock (NS9):** uv runs with the lock's descriptor closed.
- **A missing env (NS4)** is looked at once more after `GONE_RECHECK_S` (2 s) before it is refused: a rebuild's swap leaves the path absent for an instant.
- **An unsafe refusal (NS2/NS3)** names the owner uid and mode it saw, and says a default ACL or an NFSv4 id-mapping can cause it.
- **One refusal path (R2-S2, SS2).** What a node's refusal says is believed the same way for run_shell, a poll, a batch row and a job (`envs.believe_refusal`).
  - A `.failed` mark makes the env failed here, so a batch's yes rebuilds it once.
  - A job's resubmit checks or rebuilds the env first.
- **Jobs (SS2, SS6).**
  - A job's record keeps a `nonce_key`. Its refusal writes `env_refused` beside the script, carrying the job's nonce. Status believes only that mark, says the job ran nothing and why, and keeps the reason on the ledger.
  - A job, and a handoff canary, never run billed work against an env that is not ready (`envs.ready_for_billed`): an unchecked env answers `env_building`.
- **The pending env:**
  - **R2-S3:** a pending env whose build was lost, or cancelled when a dropped runner closed, is sent again on the next `ensure_endpoint_up`, env omitted included.
  - **R2-S5:** a failed newer request drops an older pending one, and says so.
  - **R2-S6:** of two calls at once the later wins (`AppCtx.env_seq`); the earlier applies nothing and says so.
  - **R2-S11:** run_batch and submit_job promote a ready pending env before they read the session's.
- **`retried` (R2-S7)** is cleared once the env is ready again.
- **The rows (SS3)** carry the arch the env was built for: `_admit_locked` re-reads the batch after updating its record.
- **The block part says when it set nothing (R2-S1).** `EndpointStatus._nothing_set` is a private attribute, never on the wire. It covers every refusal of the call's arguments: a size, a queue, a template, a partition change a running task holds, the envelope's admission. It replaces round 1's phrase-grep.
- **Words:**
  - **R2-S4:** a runner's failure during an existing env's check is "could not be checked", never "its line did not work".
  - **SS4:** a refusal for a command sent in an env the session has since left is said about that env, and that a resend runs in the current one (nothing is rebuilt for it).
  - **R2-S10:** the install warning is marked said only by the answer that carries it.
- **Credentials and Pythons:**
  - **R2-S8:** the call journal stores an env's requirements and activate line redacted.
  - **R2-S9:** a handoff's collect compares the env by the record's `python_asked`.
- **The build runner (SS5):** a failure closes only the runner that failed.

## Review round 1 (2026-11-05, on `c7fda3d`)

Two reviewers reproduced 29 defects: S1–S17 on the server, N1–N12 on the node. They also found a live-cell grader bug (L1) and raised suspicions that were ruled on without a reproduction (U1–U10). Every one is fixed as ruled. The tests are in `tests/test_block_envs_r1.py`: 78 of its 88 fail on `c7fda3d`, and the other 10 are controls.

### What changed in how it works

- **A new built env is pending until it is ready (S4).**
  - The session's own env serves meanwhile (or none), and the answer says so.
  - Once the new env is ready it becomes the session's env, said once.
  - A failed build leaves the session's env as it was (ruling 9, now also for a build that fails after the call's wait).
  - `env_building` is now only the session's OWN env being rebuilt (a purge).
- **A built env is read-only (N3).** Bytecode is compiled at the build, then `chmod a-w`, links never followed.
  - An install into the activated env fails loudly. To add a package, change the requirements, which makes another env.
  - A partly purged env is rebuilt BESIDE itself and swapped in. The old one is moved aside to `.broken.*` only once the new one is ready, so work running in it carries on.
  - A later build sweeps `.build.*` and `.broken.*` leftovers older than `SWEEP_MIN` (240 min), making them writable first (N11).
  - `HPC_BRIDGE_ENV_BUILD_S` is capped 30 min below that age, so a sweep can never take a live build (U4).
- **Only the user's own directories are used (U1).** The env root and `<key>-<arch>` must be owned by the user and writable by neither its group nor anyone. This is the slot lock dir's rule: keys are predictable, so another user could plant an env.
  - The activation refuses otherwise (`why` = `unsafe`, naming the path).
  - The build refuses too, and writes nothing there.
  - The check answers `state=unsafe`.
  - The build itself runs under `umask 022`, so a node whose umask is 002 never makes the root group-writable.
- **An activate line that ends the shell is still a refusal (N1).** This covers `set -e; false`, `set -u`, `${X:?}` and `exit 0`. The line runs in an `if` with an EXIT trap armed while it runs, and the shell's own trap is restored after.
- **Around every activation (N5, N8):**
  - errexit, nounset, noclobber, pipefail and the cwd are as they were before it;
  - its output goes to stderr;
  - the wrapper's own writes use `>|`.
- **The session wrapper snapshots in the shell (U10, N4).** Values are copied with `printf -v`, and every comparison is a string test, with no process per variable.
  - A variable the activation UNSET (a venv's `PYTHONHOME`, a `module purge`) and the command left unset is saved as the session had it.
  - Trailing newlines are kept.
  - The cost, one `true` with 125 exported variables, macOS bash 3.2, 10 runs each:
    | | before (`c7fda3d`) | after |
    |---|---|---|
    | no env | 1.02 s | 0.015 s |
    | with an env | 2.45 s | 0.025 s |
- **The requirements text (N6, N9, N10).**
  - Lines continued with a backslash are joined, so the hash-pinned form `pip-compile --generate-hashes` writes is accepted.
  - A local path is refused however it is spelled: `pkg @ /path`, `${HOME}/…`, `FILE:///`, or `--find-links /dir`. An `@` must be followed by a URL scheme other than file; index options take URLs only.
  - A `$` is refused, because uv expands variables on the node.
  - Every argument check is a full match, so a trailing newline fails.
- **A root of the home directory itself** (`~/`, `$HOME/`, `${HOME}/`) is `$HOME` (N2).
- **"Gone" or "another arch" (N7).** The activation knows the architecture the env was built for (`Activation.arch`, the login node's). A missing build for THIS node's own architecture is `gone`, never `arch`.
- **Credentials in an index URL are never said back (U3):** not in a notice, the terms, the ledger's record, a status or the handoff record. The node still gets the user's text, unchanged.
  - A record whose text needed redacting is marked `redacted`. Only the server that still holds the text can rebuild it; after a restart that is refused by name.
- **The login node's own installer settings are named, never their values (U2).** These are the set `UV_*INDEX*`, `UV_INSECURE_HOST`, `PIP_INDEX_URL`, `PIP_EXTRA_INDEX_URL`, `PIP_TRUSTED_HOST` and the config-file variables, plus any uv or pip config file found. The build is never changed for them. They appear in `EnvInfo.ambient` and the notice.
- **The build runner (S7, S8):**
  - It is keyed on its endpoint AND config: a runner made before `adopt_endpoint`, or for another endpoint, is closed and replaced (U9).
  - It is dropped when its submit raises, when its future fails, on a new Globus login and on an endpoint repair.
  - A build is never sent to no endpoint: an env asked for before the endpoint exists waits for the next call, which says so.
- **A build that never answers is lost (S5).** That means past `BUILD_S` plus a 5-minute grace, or sent to an endpoint that is no longer the bound one. It is given up and sent again on the next ask.
- **A re-check of a ready env is quiet (U5).** That covers the 24 h re-verify and a block newly confirmed: the env stays ready while it runs, and only a failed answer changes its state.
- **A refusal from the node clears what the server believed (S6).** After a `.failed` mark, the next ask sends the build.
- **A refusal read through `poll_task` is still a command that never ran (S9).** The handle carries the dispatch's (nonce, activation).
- **Batches:**
  - **S1:** a failed rebuild is retried ONCE, on the user's yes. If it fails again — or the env cannot be used at all (another arch, a failing line, an unsafe directory, a redacted record after a restart) — the batch pauses saying the user must change the env, and never asks for another yes (`unbuildable` on the record).
  - **S11:** a batch's or a job's env shows its state as this server knows it.
  - **N12:** a row's refusal mark is believed only with that attempt's nonce, which is derived from the record's own secret. A restarted server believes its own rows' marks and keeps no table of nonces.
  - **U7:** `run_batch(batch_id=…, env=…)` is refused by name.
  - **U8:** a name is its owner's, never stored on the shared env.
- **One notice for any owner's built env (S14).** The session's, a batch's, a job's and a handoff's built env all say the same things: index lines, ambient settings, the install warning (once a session), and the root's warning (carried in the record).
- **Handoffs:**
  - **S2:** the handoff's key is the config's alone. A collect call with the same config, and the same env or none, collects; another env is refused by name.
  - **S10:** a failed env check makes `validation.ok` false and is said first.
  - **U6:** an env still building answers phase `env_building`, never `refused`.
- **The order inside `ensure_endpoint_up`:**
  - **S16:** a shape the facility does not serve, or no facility, is that answer, and the env is not looked at.
  - **S15:** the block's own arguments come before the env, so a refused size never sets one.
  - The env is CHECKED before the block part and APPLIED after it, once the endpoint exists.
- **Smaller fixes:**
  - **S12:** an existing env's words are quoted `!r`, as data, and capped.
  - **S13:** a check still queued is said as such.
  - **S17:** the low-space warning is built afresh each time.
  - **U9:** a runner's failure is never called the installer's.
- **L1, the live grader:** `env_switch_clean` judges against the BUILT envs, never against "no venv". Every command on the real stack runs inside hpc-bridge's own endpoint venv.

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
- `index_lines` (credentials redacted), `ambient` (the login node's own installer settings that applied: names and config files, never values), `warning`, `error`

**Where it appears:**
- `EnvInfo` is on `EndpointStatus.env`, `BlockTerms.env`, `BatchStatus.env`, `JobInfo.env` and `JobTerms.env`.
- The ledger keeps `batch.env` and `job.env` as JSON; `LedgerEntry.env` is a label.
- A handoff gets `HandoffRecord.env` (`HandoffEnv`) and `HandoffValidation.env_ok`.

**`ShellOutcome.phase` gains `env_building`:** nothing ran, because the session's own env is being rebuilt on the login node (free; a purge). Call again. A NEW env being built is pending instead, and the session's own env serves meanwhile (review S4).

**`HandoffResult.phase` gains `env_building`** too: nothing was sent and nothing is refused; call again (review U6).

**The module, and where it hooks in:** [[envs]] has the parts. The hooks are:
- `session_shell.wrap(…, env_sh=)`
- `batch.task_script(…, env_sh=)`
- the frozen job script
- `Ledger.add_batch(…, env=)` and the job row's `env`

**On the facility,** `<root>/<key>-<uname -m>/` holds:
- the venv, which is relocatable and READ-ONLY (review N3: bytecode compiled, then `chmod a-w`). It is built beside its path as `.<key>-<arch>.build.<token>` and claimed by one rename. The root and the env must be the user's alone (review U1).
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

   A re-check of a ready env is quiet: commands go on while it runs, and only a failed answer changes the env's state (review U5).

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
9. **A failed build** leaves the session's env as it was ("not changed"). It quotes the installer's own words as data, never as instructions, and marks `.failed`. Since review round 1 (S4) this also holds for a build that fails after the call's wait: a new env is pending until it is ready.
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
    - Every record has kind, how, key and name. The name is the owner's (review U8).
    - An existing env's record adds its line.
    - A built env's record adds:
      - the Python built, and the Python its key was made with (`python_asked`: never a later client's default, review S3);
      - root, path, arch and the lock's hash;
      - the root's warning;
      - the requirements text, with an index URL's credentials redacted (`redacted`: rebuilt only by the server that holds the text, review U3).

      With those, a restarted server can activate the env or rebuild it. It never rebuilds one whose text does not make its key.
    - A batch's record adds a secret its rows' refusal nonces are made from (`nonce_key`, review N12), and, as it goes, `retried` (the user's one yes after a failed rebuild) and `unbuildable` (a yes cannot mend it: review S1).
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
  - What is left is the hidden `.<key>-<arch>.build.<token>` tree and its log beside the env. A later build of the same env sweeps them after 240 minutes (`SWEEP_MIN`), as it does an env a rebuild moved aside (`.broken.*`), making each writable first. A build's walltime is capped below that age, so a sweep never takes a live build.
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
- **`tests/test_block_envs_r1.py`** (review round 1, 88 tests): the reviewers' reproductions S1–S17 and N1–N12, adapted only where a fix changed the API they called, and a test for each of U1–U10.
- **`tests/test_block_envs_r2.py`** (review round 2, 69 tests):
  - the reproductions R2-S1..S11 and R2-N1..N8;
  - R2-N9 in the session, a batch row and a job script;
  - a test for each of SS2..SS7 and NS2..NS9.

  Its `needs_uv` tests run the REAL uv, offline. Every node test passes under bash 3.2 and 5.2.
- **`agentic/harness/envs_checks.py` and `test_envs_scenarios.py`:** the scripted cells `env_ssh_build_cache` and `env_ssh_switch`, on the fake `default` profile. They are kept out of the sweep until their live proof. They need PyPI reachable from the fake cluster's login node.
- **The agent cells** (`agentic/harness/agent_cells.py`, `agent_checks.py`, `test_agent_cells.py`; built 2026-11-05, not yet run live): a model drives the tools, a scripted user answers. `env_built_for_user` (a package the cluster lacks: an env built from it, no pip install), `env_module_switch` (`lmod`: the user's module as an `activate` env, switched on a live block with no new spend question), `env_unbuildable_asks` (a version that does not exist: the installer's reason relayed, no loop, no guessed version), `batch_rows_in_user_env` (rows that import the package run in a built env). `agentic/README.md`, "Agent cells".

## See also
[[envs]] · [[Batch and swarm execution]] · [[batch]] · [[sizing]] · [[jobs]] · [[handoff]] · [[session_shell]] · [[shapes]] · [[Per-architecture endpoint venvs]] · [[facility-mep]]

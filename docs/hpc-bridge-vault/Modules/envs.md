# envs.py

> [!abstract] Role
> Block environments (PR 3) point work at a software environment: an existing one (`activate`, the user's own shell line) or one BUILT from a requirements list. `envs.py` holds the pure parts (the argument, the key, the shell scripts) and the env state the server keeps (`AppCtx.env_*`).
>
> - **Built envs:** on an endpoint hpc-bridge stood up over SSH, an env is built on the login node (free) by a user endpoint of its own, then activated per command, batch row and job script. The block never changes.
> - **Existing envs** work on every kind of facility.
> - **Building on a facility endpoint is PR 4's.** Here `delivery` refuses it by name.
>
> Design, rulings and review round 1: [[Block environments]].

## What it does

### The argument: `parse` → `EnvSpec`

`EnvSpec` has kind `activate` | `requirements` | `none`; omitted is None.
- **Accepted inputs:** a dict, or a pydantic model (`EnvRequest`, via `model_dump`). Its keys are `activate`, `requirements`, `python`, `root` and `name`; any other key is refused (`EnvRefusal(reason, message)`, by name). Nothing is set on a refusal.
- **Every pattern is matched whole** (`fullmatch`), so a trailing newline fails.
- **Normalizing (`normalize_requirements`):**
  - CRLF becomes LF.
  - A line continued with a backslash is joined to the next (pip-compile's hash-pinned form).
  - Blank lines and comments are dropped.
  - The order is kept.
- **Refused lines (`_line_problem`):**
  - `-r`, `-c`, `-e`;
  - a local path however spelled: `./`, `/`, `~`, `pkg @ /path`, `FILE:///`, `--find-links /dir`. An `@` or an index option must be followed by a URL whose scheme is not file. EVERY option and value on an option line is checked (`_options_problem`, `shlex.split`: review R2-N2);
  - a bare archive name (`.whl`, `.tar.gz`, `.zip`…), which uv reads as a path (R2-N2);
  - any `$`, because uv expands variables on the node;
  - `--trusted-host`, and any option outside `_ALLOWED_OPTIONS` (a requirement's own `--hash` passes).
- **Index lines** are listed (`index_lines`) and named in the notice, with a URL's credentials redacted (`redact`). hpc-bridge adds no index anywhere.
- **Limits:** `MAX_REQUIREMENTS` (64 KiB) and `MAX_ACTIVATE` (4 KiB).
- **`said(text)`** is how a facility's words are quoted back: their end, capped (`MAX_SAID`), credentials redacted, then quoted `!r`, as data.

### The key and the root

- **`env_key`** is sha256 of `"hpcb-env-v1"`, the Python, and the text, cut to 16 hex characters.
  - The Python defaults to the client's major.minor (`default_python`).
  - The env lives at `<root>/<key>-<uname -m>`. The architecture is appended on the node.
- **`env_root(scratch_root, root=)`** is the user's `root` if given, else `<scratch_root>/envs`. A `$HOME` root comes with a `warning` naming `root`. A root of the home directory itself (`~/`, `$HOME/`, `${HOME}/`) is `$HOME`.
- **`_qroot`** quotes a root for the shell, keeping `$HOME` live. A root that is not one line is refused.

### `delivery(spec, access=, client_python=, …)`

It returns `activate`, or `login` (a built env on SSH). It raises `EnvRefusal("facility", …)` for `requirements` on a facility's multi-user endpoint. The `reads_*` arguments are PR 4's gate.

### The shell

**`build_script(spec, key=, root=, python=, token=)`** carries the requirements as a base64 FILE, so no character of them is shell. The script:
1. runs under the node's umask OR 022, never looser (R2-N3);
2. refuses a root that is not the user's alone (U1), writing nothing there and naming the owner and mode it saw (NS2);
3. names the login node's own installer settings: every `UV_*` and `PIP_*` variable set (names only) and the config files that apply (U2, R2-N4, NS5);
4. runs the full check of an env already there (a cache hit: `cached=1`);
5. sweeps `.build.*` and `.broken.*` older than `SWEEP_MIN`, made writable first;
6. runs uv from a fresh, empty `mktemp -d` of the user's own, never in or under the env root (uv reads a project's config from its cwd and every parent), with the lock's descriptor closed (NS9);
7. builds a uv venv beside its path, `.<key>-<arch>.build.<token>`, with `--compile-bytecode`, `--link-mode=copy` and `UV_CACHE_DIR=<root>/.uv-cache`;
8. installs `requirements.in` (0600), and writes `requirements.lock` (0600) and the marker `.hpcb-env`;
9. makes the new env READ-ONLY (`chmod a-w`, links never followed);
10. re-checks the target: a ready env another build claimed meanwhile stands, and this one is discarded (R2-N8);
11. moves a partly purged env aside (`.broken.<token>`) only now that its rebuild is ready;
12. claims the env with ONE rename, which refuses a target that is not empty;
13. checks it again.

The full check reads each RECORD as csv, with the env's own Python (`-I`), so a quoted path holding a comma counts (R2-N1).

`flock`, where the node has it, only spares a second build its work. The script prints one line:
- `HPCB_ENV_READY key= dir= arch= python= lock= size_mb= free_mb= cached= ambient= config=`, or
- `HPCB_ENV_FAILED key= arch= ambient= config= error_b64=`, which leaves `<key>-<arch>.failed`, never in an unsafe root.

`parse_build` reads that line into `Built`. `BUILD_S` (the walltime) is capped 30 min below `SWEEP_MIN` (`_build_s`, U4).

**`check_script(key=, root=)`** is the full check. It prints `HPCB_ENV_CHECK state=` with one of:
- `ready`
- `partial` (with `missing=`)
- `gone`
- `failed`
- `arch` (built only for `others=`)
- `unsafe` (`dir=` names the directory)

**`activation_sh(Activation, nonce)`** is per command, row and job. It goes into `session_shell.wrap`, `batch.task_script` and the job script, and into the two checks below.
- **Around it (R2-N5, NS6):**
  - The shell's errexit, nounset, noclobber, pipefail, allexport, xtrace, IFS and cwd are saved before it and restored after it.
  - errexit is off while it runs.
  - Its output goes to stderr.
  - Nothing of it is left after: not the refusal function, not the nonce.
- **A built env:** the root and the env must be the user's alone (one `find`), then the marker is grepped, then `VIRTUAL_ENV` is set and the env's `bin` goes first on PATH.
  - **Not there:** it is looked at once more after `GONE_RECHECK_S` (a rebuild's swap: NS4). Then this node's own architecture missing is `gone`, and a different architecture is `arch` (`Activation.arch` is the arch the env was built for).
- **An existing env:** the user's line runs OUTSIDE any condition (R2-N9: bash 4+ ignores errexit inside an `if`), under an EXIT trap armed while it runs, so a line whose own errexit stops it, or one that ENDS the shell, is a refusal.
  - The previous EXIT trap is restored, or the line's own if it set one (NS7).
  - A refusal from the trap runs the shell's own EXIT trap with status 88 first (R2-N6).
- Every `case` pattern carries its `(`: bash 3.2 misparses a bare `pat)` inside `$( … )`.
- **A refusal** prints ONE line, `HPCB_ENV_REFUSED <nonce> <why> <key> <detail>`, and exits `ENV_RC` (88) before the command. `why` is `gone`, `failed`, `arch`, `activate` or `unsafe`.
- **Reading it back:**
  - `refusal(rc, stderr, nonce)` believes the line only with the dispatch's nonce.
  - `refusal_mark(text, nonce)` reads the batch wrapper's `.env_refused` copy, again only with its own nonce (N12).

**`activate_check_script(line)`** runs an existing env's line once, on the login node: the very activation, in a subshell (R2-N7).

**`canary_check(rec)`** is the handoff canary's env check, which prints `HPCB_ENV_OK 1|0`. It is also the very activation in a subshell, standing alone and never on the left of `&&` (R2-N7, SS7).

### The state (`AppCtx`)

**The fields:**
- `env_spec`: the session's env.
- `env_pending`: a new built env still being built (S4). It becomes the session's once it is ready, and `env_note` says what became of it, once.
- `env_known`: what this server knows of each env. Each is a `_Known` keyed `root\0key` (or `activate\0key`), holding:
  - its info (no name: a name is its owner's, U8);
  - a build or check out, with when it was sent and to which endpoint;
  - whether that one is a quiet re-check (U5);
  - when its full check last passed;
  - the root's own warning;
  - what kind its last failure was (`fault`).
- `env_said_install`: marked by the answer that carries the warning (`mark_said`, R2-S10).
- `env_seq`: of two `choose` calls at once the later wins (R2-S6).
- `env_runner`, with `env_runner_key`. A `_Known` keeps the runner its future went out on, and only that one is dropped when it fails (SS5).

**The build runner:**
- **`_runner(app)`** is the build user endpoint on the login node: the login shape's seed config plus `BUILD_UEC_KEY`, with walltime `BUILD_S`.
  - It is keyed on its endpoint AND config. One made for another, or before the endpoint was adopted, is closed and replaced.
  - It raises when no endpoint is up (S8).
- **`drop_runner(app)`** closes it. It is called:
  - when its submit raises or its future fails (S7);
  - from `warmth._forget_identity_verdicts` (a new login);
  - after an endpoint repair;
  - from `_drop_all_shapes`.

**Builds and checks:**
- **`start_build(app, k, quiet=)`** sends a build. A refused submit is a failure in its own words, never a raise.
- **`_absorb` / `refresh` / `_lost` / `_promote` / `_wait`** manage a build or check out:
  - one unanswered past `BUILD_S` plus `LOST_GRACE_S`, or sent to an endpoint no longer bound, is lost and sent again on the next ask (S5);
  - a pending env is promoted once ready, or dropped once failed.
- **`ensure_built`** answers from what is known when the full check is under `VERIFY_EVERY_S` old (a cache hit). Otherwise it sends the build, quiet when the env is ready, and waits up to `WAIT_S`.
- **`recheck(app, since=)`** is quiet, when a block is newly confirmed.
- **`resume_pending(app)`** sends again a pending env whose build was lost or cancelled, on the next ask (R2-S3).
- **`believe_refusal(app, k, why)`** is the ONE path for what a node's refusal says — run_shell's, a poll's, a batch row's, a job's (R2-S2, SS2). A `.failed` mark makes the env failed here, so the next ask builds it.
- **`ready_for_billed(k)`:** a job and a handoff canary go only against an env known ready (SS6).
- **`nonce_for(rec, *parts)`:** a batch row's or a job's activation nonce, from the record's own `nonce_key`.

**Choosing, and serving commands:**
- **`choose(app, spec)`** applies ensure_endpoint_up's parsed `env`. `{}` drops the env and any pending one.
  - **An existing env** is checked on the login node (SSH only). Its failure is quoted as data; a check still queued is said as such (S13).
  - **A built env** becomes the session's when it is ready, or pending while it builds; a failed one leaves the session's env as it was.
  - `Chosen.taken` says which.
- **`built_notice(app, info)`** is the one notice for any owner's built env (S14): index lines, ambient settings, the install warning (once a session), and the root's warning.
- **`failed_words(info, fault)`** says why an env is not there, in the words of what failed: the installer, the runner, or a lost build (U9).
- **`session_info`, `pending_info`, `take_note`** report the state.
- **`for_command`** gives `run_shell` the activation, or why nothing can run: the session's own env being rebuilt, or failed.
- **`refused(app, ref, act, stderr)`** gives a refused command's phase and notice:
  - `gone` → a rebuild and `env_building`;
  - `failed` → `failed`, with the cached verdict cleared so the next ask sends the build (S6);
  - `arch`, `unsafe`, `activate` → `failed`, by name.

**Batches and jobs:**
- **`record`** stores what the ledger keeps:
  - the line;
  - or the requirements (credentials redacted, marked `redacted`), the Python the key was made with, root, path, arch, the lock and the warning.
- **`_known_of(app, rec)`** finds or makes the env a record names. It never makes another env under the record's key (S3), and a redacted record only with this server's own text.
- **`activation_of`, `record_state`, `record_error`, `rebuild_record`, `current_record`, `info_of_record`** work from that record; `info_of_record` gives the state as known here (S11). `label` is the listing's.

> [!warning] Correctness is the rename, never the lock
> Two builds of one env (two servers, a retry) both build beside it. Exactly one rename claims the path; the other finds it claimed and discards its own. A build cut off at any point leaves only a hidden `.build.*` tree, never a half-built env at the path. A stop never touches a build (it is no shape's task); a teardown ends it. See [[Block environments]], "A build cut off by a stop or a teardown".

> [!warning] A built env is read-only, and the user's alone
> An install into the activated env fails loudly: adding a package means changing the requirements, which makes another env. An env, or a root, owned by someone else or writable by its group or anyone is never used or built into. Keys are predictable, so a planted env would otherwise be found.

> [!warning] The session never keeps the env
> `session_shell` snapshots the environment, in the shell, before and after the activation, and persists only what the command itself changed. A variable the activation unset comes back as the session had it. So a switch of env, or to none, leaves nothing of the old one behind. The exception: a command that edits PATH itself persists a PATH holding the env's `bin`.

## Tests
- `tests/test_block_envs.py`.
- `tests/test_block_envs_r1.py` (review round 1).
- `tests/test_block_envs_r2.py` (review round 2; passes under bash 3.2 and 5.2).
- PR 4's facility half is in `tests/test_block_envs_mep.py`, on branch `feat/block-envs-mep`.

## See also
[[Block environments]] · [[session_shell]] · [[batch]] · [[jobs]] · [[handoff]] · [[ledger]] · [[server]] · [[shapes]]

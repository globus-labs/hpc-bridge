# envs.py

> [!abstract] Role
> Block environments (PR 3) point work at a software environment: an existing one (`activate`, the user's own shell line) or one BUILT from a requirements list. `envs.py` holds the pure parts (the argument, the key, the shell scripts) and the env state the server keeps (`AppCtx.env_*`).
>
> - **Built envs:** on an endpoint hpc-bridge stood up over SSH, an env is built on the login node (free) by a user endpoint of its own, then activated per command, batch row and job script. The block never changes.
> - **Existing envs** work on every kind of facility.
> - **Building on a facility endpoint is PR 4's.** Here `delivery` refuses it by name.
>
> Design and rulings: [[Block environments]].

## What it does

### The argument: `parse` → `EnvSpec`

`EnvSpec` has kind `activate` | `requirements` | `none`; omitted is None.
- **Accepted inputs:** a dict, or a pydantic model (`EnvRequest`, via `model_dump`). Its keys are `activate`, `requirements`, `python`, `root` and `name`; any other key is refused (`EnvRefusal(reason, message)`, by name). Nothing is set on a refusal.
- **Normalizing (`normalize_requirements`):** CRLF, trailing blanks, blank lines and comments (an inline ` #` too) are removed; the order is kept.
- **Refused lines (`_line_problem`):**
  - `-r`, `-c`, `-e`, and local paths (`./`, `../`, `/`, `file:`, `@ file:`), because the key cannot see a file's content;
  - `--trusted-host`, and any option outside `_ALLOWED_OPTIONS` (a requirement's own `--hash` passes).
- **Index lines** are listed (`index_lines`) and named in the notice. hpc-bridge adds no index anywhere.
- **Limits:** `MAX_REQUIREMENTS` (64 KiB) and `MAX_ACTIVATE` (4 KiB).

### The key and the root

- **`env_key`** is sha256 of `"hpcb-env-v1"`, the Python, and the text, cut to 16 hex characters.
  - The Python defaults to the client's major.minor (`default_python`).
  - The env lives at `<root>/<key>-<uname -m>`. The architecture is appended on the node, so the activation finds the node's own build, and the server needs no architecture probe.
- **`env_root(scratch_root, root=)`** is the user's `root` if given, else `<scratch_root>/envs`. A `$HOME` root comes with a `warning` naming `root`.
- **`_qroot`** quotes a root for the shell, keeping a leading `$HOME/` or `~/` live.

### `delivery(spec, access=, client_python=, …)`

It returns `activate`, or `login` (a built env on SSH). It raises `EnvRefusal("facility", …)` for `requirements` on a facility's multi-user endpoint. The words: "building an env on a facility endpoint is not supported yet: use an existing env there with env={"activate": …}". The `reads_*` arguments are PR 4's gate.

### The shell

**`build_script(spec, key=, root=, python=, token=)`** carries the requirements as a base64 FILE, so no character of them is shell. The script:
1. sweeps its own `.build.*` leftovers older than 240 minutes;
2. runs the full check of an env already there (a cache hit: `cached=1`);
3. otherwise builds a uv venv beside its path, `.<key>-<arch>.build.<token>`, with `UV_CACHE_DIR=<root>/.uv-cache` and `--link-mode=copy`;
4. installs `requirements.in`, writes `requirements.lock` (`uv pip freeze`) and the marker `.hpcb-env`;
5. claims the env with ONE `mv -T`, which refuses a target that is not empty: of two builds, exactly one claims it;
6. checks it again.

`flock`, where the node has it, only spares the second build its work. The script prints one line:
- `HPCB_ENV_READY key= dir= arch= python= lock= size_mb= free_mb= cached=`, or
- `HPCB_ENV_FAILED key= arch= error_b64=`, which leaves `<key>-<arch>.failed`.

`parse_build` reads that line into `Built`.

**`check_script(key=, root=)`** is the full check, which prints `HPCB_ENV_CHECK state=…`:
- `ready`: every RECORD-listed file is present and the dist-info set's hash is as built;
- `partial`: with `missing=`;
- `gone`;
- `failed`;
- `arch`: built only for `others=`.

`parse_check` reads it into `Check`.

**`activation_sh(Activation, nonce)`** is per command, row and job. It goes into `session_shell.wrap`, `batch.task_script` and the job script.
- **A built env:** the marker is grepped for the key, then `VIRTUAL_ENV` is set and the env's `bin` goes first on PATH.
- **An existing env:** the user's line is base64-decoded and `eval`'d.
- **Otherwise it refuses:** it prints ONE line, `HPCB_ENV_REFUSED <nonce> <why> <key> <detail>`, and exits `ENV_RC` (88) before the command. `why` is one of:
  - `gone`: no env here;
  - `failed`: its build's mark;
  - `arch`: built only for another arch;
  - `activate`: the line failed.
- **Reading the refusal back:** `refusal(rc, stderr, nonce)` reads that line, and believes it only with the dispatch's own nonce, so output cannot forge it. `refusal_mark` reads the batch wrapper's `.env_refused` copy.

**`activate_check_script(line)`** runs an existing env's line once, in a subshell, on the login node.

**`canary_check(rec)`** is the handoff canary's env check, which prints `HPCB_ENV_OK 1|0`.

### The state (`AppCtx`)

**The fields:**
- `env_spec`: the session's env;
- `env_known`: what this server knows of each env. Each is a `_Known` keyed `root\0key` (or `activate\0key`), holding its info, a build or check out, and when its full check last passed;
- `env_said_install`;
- `env_runner`.

**`_runner(app)`** is the build user endpoint on the login node: the login shape's seed config plus `BUILD_UEC_KEY`, with walltime `BUILD_S` (30 min, `HPC_BRIDGE_ENV_BUILD_S`). There is one per endpoint. It is never a session's runner and never the internal one, so a build never takes the login shape's one worker.

**The functions:**
- `start_build` / `_absorb` / `refresh` / `_wait` manage a build or check out.
- **`ensure_built`** answers from what is known when the env's full check is under `VERIFY_EVERY_S` old (a cache hit, nothing sent). Otherwise it sends the build script (its own check first) and waits up to `WAIT_S` (20 s).
- **`choose`** applies `ensure_endpoint_up`'s `env`. `{}` drops the env.
  - **An existing env** is checked on the login node (SSH only). A failure there is kept and said.
  - **A built env** whose build failed does NOT become the session's env ("not changed").
  - The install warning is said once a session.
- **`recheck(app, since=)`** runs the full check again when a block is newly confirmed.
- **`for_command`** gives `run_shell` the activation, or why nothing can run: `building`, or `failed`.
- **`refused(app, ref, act, stderr)`** gives a refused command's phase and notice:
  - `gone` → a rebuild and `env_building`;
  - `arch` → `failed`, naming both arches and the follow-up ([[Per-architecture endpoint venvs]]);
  - `failed` and `activate` → `failed`.
- **For batches and jobs:**
  - `record` stores what the ledger keeps: the line, or the requirements, Python, root, path and lock. With it, a restarted server activates or rebuilds the env.
  - `activation_of`, `record_state`, `rebuild_record`, `current_record` and `info_from_record` work from that record, and `label` is the listing's.

> [!warning] Correctness is the rename, never the lock
> Two builds of one env (two servers, a retry) both build beside it. Exactly one `mv -T` claims the path; the other finds it claimed and discards its own. A build cut off at any point leaves only a hidden `.build.*` tree, never a half-built env at the path. A stop never touches a build (it is no shape's task); a teardown ends it. See [[Block environments]], "A build cut off by a stop or a teardown".

> [!warning] The session never keeps the env
> `session_shell` snapshots the environment before and after the activation, and persists only what the command itself changed. So a switch of env, or to none, leaves nothing of the old one behind. The exception: a command that edits PATH itself persists a PATH holding the env's `bin`.

## Tests
`tests/test_block_envs.py`. PR 4's facility half is in `tests/test_block_envs_mep.py`, on branch `feat/block-envs-mep`.

## See also
[[Block environments]] · [[session_shell]] · [[batch]] · [[jobs]] · [[handoff]] · [[ledger]] · [[server]] · [[shapes]]

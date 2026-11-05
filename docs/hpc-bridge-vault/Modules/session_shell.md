# session_shell.py

> [!abstract] Role
> The cwd/env persistence shim. Wraps each command so `cd`/exports survive `ShellFunction`'s fresh-subprocess model, persisting state in `<root>/sessions/<id>/{.cwd,.env}` on the shared filesystem.

## What it does

- **`Session`** (`session_shell.py:102`) — `(session_id, root)`. `session_id` is an untrusted MCP param, so it's validated against `_VALID_SESSION_ID` (`:9`, strict allowlist — no `../`, no metacharacters → no path traversal). **`quoted_state_dir()`** (`:127`) is the state dir as a shell word: a root written `$HOME/…` / `${HOME}/…` / `~/…` is emitted as `"$HOME"'/rest'` so the variable expands **on the worker** — what lets a [[facility-mep|multi-user endpoint]], whose local username we can't know client-side, use a home-relative scratch; anything else is quoted whole.
- **`REMAINING_SH`** — the block's remaining walltime from inside it, as `__hb_rem` (whole seconds, or empty for unknown): Slurm's `SLURM_JOB_END_TIME`, else `squeue -h -j <job> -o %L` (+1 s); PBS `qstat -f` stime + `Resource_List.walltime`; the block's end cached once on node-local `${TMPDIR:-/tmp}`; no job id is unknown (`squeue -j ""` answers rc 0 and nothing). The canary ([[runner]]) and every batch task ([[batch]]) report it — the admission gate's input (spike S8).
- **`wrap(…, result_key=None)`** — with a key (the server passes one for an agent's command), the command's stdout also tees its last 4 kB to `<session>/results/<key>/tail`, and an EXIT trap writes its status to `…/rc` (a command that calls `exit` itself still leaves it): `poll_task` by Globus task id outlives the service's 30-minute window and a restart.
- **`wrap(command, session, *, guard=None)`** (`:183`) — renders `_WRAP_TEMPLATE` (`:144`): `cd` into the saved `.cwd`, source `.env`, run the **base64-carried** command via `eval` in the current shell, then persist the new cwd + changed env. Runs under `/bin/bash` (`ShellFunction` execs `shell=True, executable=/bin/bash`). (The ambient snapshot is in the shell now, not a `.env.base.$$` file, so the old path-quoting hazard of that file is gone; `reset_command` still sweeps an old leaked one.) With a `guard`, the wrong-block guard (`_GUARD_TEMPLATE`, `:41`) goes first; without one the script is exactly as before.
- **`reset_command(session, *, guard=None)`** (`:211`) — clears `.cwd`/`.env` (and any leaked snapshot), behind the same guard.
- **The wrong-block guard** (`:20`–`:98`). `JOB_ID_EXPR` (`:25`) is `${SLURM_JOB_ID:-${PBS_JOBID:-}}`, the expression the canary reports as `HPCB_JOB` ([[runner]] imports it, so the two cannot drift). A worker on a PBS sister node may not inherit `PBS_JOBID`; there the command runs unguarded. **`Guard`** (`:55`) is one dispatch's guard: the block's job and a fresh hex nonce (`Guard.new`, `secrets.token_hex(8)`; validated as hex because it is spliced into the guard's `printf` format). With the worker's job set and different, the guard prints `HPCB_WRONG_BLOCK <nonce> <job>` (`WRONG_BLOCK_MARKER`) to stderr and exits 86 (`WRONG_BLOCK_EXIT`) before `umask`, `mkdir`, the cd, the session state and the command. It is POSIX sh, and the job is `shlex.quote`d (it comes from the canary's stdout). **`refusal(exit_code, stderr, guard)`** (`:85`) reads it back as a **`Refusal`** (`:72`): exit 86, and a stderr line, wherever it sits, that starts with the marker and this guard's nonce, naming a job other than the expected one. Not by position: a shell's start-up prints before the marker, and the SDK's sandbox warning (an endpoint without task sandboxing), `set -x` and an EXIT trap print after it. The nonce is the protection, since output cannot forge what it never saw. The job is bounded (`_JOB_ID`, `:51`: job-id characters, 96 at most); otherwise `Refusal.job` is None and it is never named. Who builds the guard and what a refusal becomes: [[Cost control]].

- **The snapshots are in the shell** (review round 1, U10, 2026-11-05). `__hb_snap PREFIX LIST` copies every exported value into `<PREFIX><NAME>` with `printf -v`, and lists the names. Every comparison is a string test (`[[ … == … ]]`): no snapshot file and no process per variable.
  - It used to pipe each value through base64 and grep a file per variable.
  - The cost, one `true` with 125 exported variables, macOS bash 3.2:
    | | before | after |
    |---|---|---|
    | no env | 1.02 s | 0.015 s |
    | with an env | 2.45 s | 0.025 s |
  - Values keep trailing newlines.
  - The wrapper's own writes are `>|`, so a noclobber the command set never stops them.
  - **A known shell state (review round 2, NS8).** The persist loops run with `set +x +a +e +u`, a plain IFS and nocasematch off, whatever the command left, restored after. Otherwise a command's `set -x` would trace every value, its `set -a` would export the wrapper's own variables, and an `IFS=:` would join every name into one word. The wrapper's `__hb_*` variables are never persisted, and `__hb_snap` keeps its own IFS.
- **`wrap(…, env_sh=None)`** handles the session's env ([[envs]], [[Block environments]]). Given `envs.activation_sh`, the wrapper runs the activation AFTER the session's own `.env`, so it wins over a PATH an earlier command persisted. It runs between two snapshots (`_ENV_PRE`: `__hb_P_`, `_ENV_POST`: `__hb_A_`).
  - **What is persisted:** `_ENV_KEEP` saves a variable the command left as the activation set it as the session had it, or not at all. So a switch of env, or to none, leaves nothing of the old one behind.
  - **What the activation unset:** `_ENV_GONE` saves a variable the activation UNSET (a venv's `PYTHONHOME`, a `module purge`'s) that the command left unset, as the session had it (review N4).
  - **What is not reverted:** a variable the command changed after the activation is saved as it is. A PATH the command edited keeps the env's `bin`: the documented leak.
  - **The activation itself** restores the shell's errexit, nounset, noclobber, pipefail, allexport, xtrace, IFS and cwd after it, leaves nothing of itself, and its output goes to stderr ([[envs]]). A refusal its EXIT trap makes still writes the kept rc (review R2-N6).
  - **A refused activation** (`HPCB_ENV_REFUSED`, rc 88) runs nothing — a line that ends the shell included.
  - **Without `env_sh`** the script carries no env code.

- **`ENDPOINT_VENV_SCRUB`** (the Anvil live test, F4, 2026-11-05): a user command ran with `VIRTUAL_ENV` — and PATH's first entry — naming hpc-bridge's own endpoint venv (the one the worker runs from: its env_setup and worker_init activate it), so `uv pip install x` / `pip install x` wrote into it. Every user command now runs without it: the session wrapper (first thing, before the ambient snapshot, and again after `.env` — a PATH an earlier command persisted with the venv's `bin` in it heals), a batch row's wrapper ([[batch]]: the whole wrapper, and the row after its session replay), a job's `job.sh` ([[jobs]]) and the env checks ([[envs]] `activate_check_script`, `canary_check`). It is hpc-bridge's venv only — its arch marker (`.hpcb-arch`) or the `gce-venv` names hpc-bridge and the registry entries give worker venvs; a venv of the user's own (activated by a command, or a BYO endpoint's) is left as it is. `VIRTUAL_ENV` (and `VIRTUAL_ENV_PROMPT`) go and the venv's `bin` leaves PATH, so a bare `python`/`pip` is the node's (or the env's: a built env sets its own after). Nothing of hpc-bridge's own in a command needs that `bin` (the wrappers use builtins and the node's tools); the canary is never wrapped, and reports the worker's own python and dill.

This is the mechanism behind [[Session continuity]]; both invariants (volatile-var filtering, record-safe multi-line env) are documented there.

> [!warning] Base64-carry the command
> The user command is base64-encoded and decoded+`eval`'d, so arbitrary shell (brace groups, quotes, `${VAR}`) can't textually break out of the wrapper while `cd`/`export` inside it still affect the persisted state.

## See also
[[Session continuity]] · [[server]] · [[envs]]

# session_shell.py

> [!abstract] Role
> The cwd/env persistence shim. Wraps each command so `cd`/exports survive `ShellFunction`'s fresh-subprocess model, persisting state in `<root>/sessions/<id>/{.cwd,.env}` on the shared filesystem.

## What it does

- **`Session`** (`session_shell.py:102`) — `(session_id, root)`. `session_id` is an untrusted MCP param, so it's validated against `_VALID_SESSION_ID` (`:9`, strict allowlist — no `../`, no metacharacters → no path traversal). **`quoted_state_dir()`** (`:127`) is the state dir as a shell word: a root written `$HOME/…` / `${HOME}/…` / `~/…` is emitted as `"$HOME"'/rest'` so the variable expands **on the worker** — what lets a [[facility-mep|multi-user endpoint]], whose local username we can't know client-side, use a home-relative scratch; anything else is quoted whole.
- **`wrap(command, session, *, guard=None)`** (`:183`) — renders `_WRAP_TEMPLATE` (`:144`): `cd` into the saved `.cwd`, source `.env`, run the **base64-carried** command via `eval` in the current shell, then persist the new cwd + changed env. Runs under `/bin/bash` (`ShellFunction` execs `shell=True, executable=/bin/bash`). The state-dir splice has **no outer quotes** (an assignment RHS doesn't word-split), so the mixed quoting above survives. With a `guard`, the wrong-block guard (`_GUARD_TEMPLATE`, `:41`) goes first; without one the script is exactly as before.
- **`reset_command(session, *, guard=None)`** (`:211`) — clears `.cwd`/`.env` (and any leaked snapshot), behind the same guard.
- **The wrong-block guard** (`:20`–`:98`). `JOB_ID_EXPR` (`:25`) is `${SLURM_JOB_ID:-${PBS_JOBID:-}}`, the expression the canary reports as `HPCB_JOB` ([[runner]] imports it, so the two cannot drift). A worker on a PBS sister node may not inherit `PBS_JOBID`; there the command runs unguarded. **`Guard`** (`:55`) is one dispatch's guard: the block's job and a fresh hex nonce (`Guard.new`, `secrets.token_hex(8)`; validated as hex because it is spliced into the guard's `printf` format). With the worker's job set and different, the guard prints `HPCB_WRONG_BLOCK <nonce> <job>` (`WRONG_BLOCK_MARKER`) to stderr and exits 86 (`WRONG_BLOCK_EXIT`) before `umask`, `mkdir`, the cd, the session state and the command. It is POSIX sh, and the job is `shlex.quote`d (it comes from the canary's stdout). **`refusal(exit_code, stderr, guard)`** (`:85`) reads it back as a **`Refusal`** (`:72`): exit 86, and a stderr line, wherever it sits, that starts with the marker and this guard's nonce, naming a job other than the expected one. Not by position: a shell's start-up prints before the marker, and the SDK's sandbox warning (an endpoint without task sandboxing), `set -x` and an EXIT trap print after it. The nonce is the protection, since output cannot forge what it never saw. The job is bounded (`_JOB_ID`, `:51`: job-id characters, 96 at most); otherwise `Refusal.job` is None and it is never named. Who builds the guard and what a refusal becomes: [[Cost control]].

This is the mechanism behind [[Session continuity]]; both invariants (volatile-var filtering, record-safe multi-line env) are documented there.

> [!warning] Base64-carry the command
> The user command is base64-encoded and decoded+`eval`'d, so arbitrary shell (brace groups, quotes, `${VAR}`) can't textually break out of the wrapper while `cd`/`export` inside it still affect the persisted state.

## See also
[[Session continuity]] · [[server]]

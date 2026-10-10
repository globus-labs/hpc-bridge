# dispatch.py

> [!abstract] Role
> Translates a dispatch into a structured `ShellOutcome` — and turns **any** failure into a structured `failed` result rather than raising, so a hung/broken endpoint never crashes the MCP tool or hangs the agent silently. Also shapes what a shell tool hands the agent (`for_agent`).

## What it does

`execute(command, runner, …)` (`dispatch.py:22`) calls `runner.run()` ([[runner]]); on success it shapes a `complete` [[models|`ShellOutcome`]] via `complete_outcome` (`:36`), carrying the worker's streams **uncut**. On *any* exception, `failure_outcome` (`:90`) maps it to a `failed` outcome with a helpful notice; the exception's text goes in `stderr_snippet`, bounded by `_error_text` (`:81`) to its first 2,500 and last 1,500 chars with the middle cut and marked — it is a diagnosis, not command output: its head says what failed (the SDK's "Malformed or unexpected data structure. Data: <the serialized result>" puts the one useful line first), and a `TaskExecutionFailed` ends with the remote traceback's final exception line followed by the SDK's own help (~600 chars on a serialization error, ~230 more on a lost worker). Both shapers are **public and shared** with the server's submit/poll path (`_run_shell` / `poll_task`), so the completion/failure mapping lives in one place ([#21](https://github.com/globus-labs/hpc-bridge/issues/21)):

| Failure | exit | notice |
|---|---|---|
| `TimeoutError` | 124 | "timed out — run ensure_endpoint_up and retry, or move to a batch job" |
| result over Compute's 10 MiB limit | `None` | "the command RAN, but its result (N bytes serialized; the limit is L) was over the limit — no output, no exit code; write it to a file and read ranges; check for its side effects before running it again" |
| `TaskExecutionFailed` (other) | 1 | "the remote task failed to execute" |
| other | 1 | "Dispatch error: \<type\>" |

**`for_agent(res)`** (`:139`) is what `run_shell`, `poll_task`, `reset_session` and `login_shell` return ([[server]] applies it in the tool wrappers; [[connect]] uses it for a failed allocation command's stderr): each stream of **command output** cut to its marked end by [[cost]] `cut_streams`, with the cut's notice appended to any notice already there. Command output comes only in a `complete` outcome — the SDK's snippet, so `SNIPPET_LINES` ([[runner]]) is passed and text that long is counted "at least" — or a `login_shell` result (SSH output, counted exactly). Any other outcome is returned as is: a failure's text is an exception's, already bounded by `failure_outcome`, and the redirect-to-a-file remedy would be wrong for it.

> [!warning] Cut only at the tool boundary
> hpc-bridge's own consumers read login-shape results through `_run_shell` (via `server._login_runner`): the pilot probe (`scheduler_ops._summarize_pilot`, which counts every row not marked finished as a live pilot), the allocation parsers (`parse_mybalance` needs the header rule at the top) and the stop's `scancel` confirmation (the last line). A marker line or a tail cut there turned a "rejected" or "finished" pilot into "queued" once a PBS `qstat -x` history passed the cap, and lost the allocation header. So `complete_outcome` keeps the streams whole, and only `for_agent` cuts.

> [!warning] A result too big to return is reported as RUN, exit code unknown
> The endpoint fails a task whose serialized result is over 10 MiB and sends `repr(MaxResultSizeExceeded(size, limit))`; the SDK hands that to the client wrapped in a **`TaskExecutionFailed`** — it never raises `MaxResultSizeExceeded` itself, so matching that class alone never fired and the agent saw "The remote task failed to execute." with a made-up exit code 1. `_result_over_limit` (`:59`) matches the name inside the text, or the error's own message ("Task result of NB exceeded current limit of NB"), and the outcome says the command ran, that its output and exit code are lost, how to re-run it into a file, and to check for its side effects first. It is marked `_worker_answered`, so [[warmth]] `_note_dispatch` refreshes the block's liveness instead of voiding it (the worker ran the command).

> [!note] Pure layer
> SDK exceptions are matched by **class name** (and the oversize one by its text), not by importing `globus_compute_sdk` — keeping this translation layer free of the heavy integration dependency.

## See also
[[models]] · [[runner]] · [[server]] · [[cost]] · [[warmth]]

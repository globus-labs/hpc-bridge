# dispatch.py

> [!abstract] Role
> Translates a dispatch into a structured `ShellOutcome` — and turns **any** failure into a structured `failed` result rather than raising, so a hung/broken endpoint never crashes the MCP tool or hangs the agent silently.

## What it does

`execute(command, runner, …)` (`dispatch.py:19`) calls `runner.run()` ([[runner]]); on success it shapes a `complete` [[models|`ShellOutcome`]] via `complete_outcome` (capped stdout/stderr — [[cost]] `cap_output`). On *any* exception, `failure_outcome` maps it to a `failed` outcome with a helpful notice. Both shapers are **public and shared** with the server's submit/poll path (`_run_shell` / `poll_task`), so the completion/failure mapping lives in one place ([#21](https://github.com/globus-labs/hpc-bridge/issues/21)):

| Failure | exit | notice |
|---|---|---|
| `TimeoutError` | 124 | "timed out — run ensure_endpoint_up and retry, or move to a batch job (billed like a block, so only with the user's spend confirmation)" |
| `MaxResultSizeExceeded` | 1 | "exceeded the 10 MB result limit — redirect to a file" |
| `TaskExecutionFailed` | 1 | "the remote task failed to execute" |
| other | 1 | "Dispatch error: \<type\>" |

> [!note] Pure layer
> SDK exceptions are matched by **class name**, not by importing `globus_compute_sdk` — keeping this translation layer free of the heavy integration dependency.

## See also
[[models]] · [[runner]] · [[server]] · [[cost]]

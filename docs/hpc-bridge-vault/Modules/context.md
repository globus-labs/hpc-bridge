# context

> [!abstract] Role
> The server's runtime state as pure data: `AppCtx` (one per server, shared by every tool call), `ShapeRuntime` (per resource shape: Executor, warmth/canary, spend clock, the sticky no-account verdict), `TaskHandle` (a command still running past the sync-wait), and `DEFAULT_SHAPE`. No behaviour lives here.

Split step 1 of the [[Review 2026-09-03 — code quality|code-quality review]]'s plan (2026-09-03): a leaf module so the modules that follow (config, notices, warmth, tasks, …) can import the state without importing `server`. `server` re-exports the four names, so `from hpc_bridge.server import AppCtx` — every test and tool — keeps working. Field-level rationale comments moved with the fields. `TaskHandle.client_cancelled` marks a command whose call the client cancelled mid sync-wait. It is tracked like any running task, and the flag only lets the notices explain a task id the agent never received. `TaskHandle.runtime` is the shape runtime the task was dispatched on. A task whose block was released under it is noted nowhere else (never on a rebuilt or newer runtime). `TaskHandle.reset` marks a `reset_session`, for its notices. `AppCtx.released_inflight` holds compute runtimes dropped while a call was still in its sync-wait on them. A stop still sees that call while the endpoint it went to is the bound one.

## See also
[[server]] · [[shapes]] · [[lifecycle]] · [[runner]]

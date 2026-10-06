# runner.py

> [!abstract] Role
> `GlobusRunner` — the AMQP hot-path dispatcher. Submits a `ShellFunction` through a long-lived Globus Compute `Executor`, and runs the warmth **canary** through the same Executor.

## What it does

- **`GlobusRunner`** (`runner.py:79`) — built per shape with an `endpoint_id` + `user_endpoint_config` and (from [#21](https://github.com/globus-labs/hpc-bridge/issues/21)) two **decoupled** bounds: `walltime` = the per-task **ceiling** (the block walltime, worker-enforced) and `timeout` = the client **sync-wait**. It lazily creates a Globus Compute `Executor` (the SDK captures the `user_endpoint_config` at build time) and reuses it for all dispatch. `submit(command)` fires a `ShellFunction` (walltime = the ceiling) and returns the future **without** waiting — the server blocks on it for the sync-wait, then hands back a poll handle if it's still running; `run` = `submit` + `fut.result(timeout)` (the reset path still uses it).
- **`run(command)`** — submits a `ShellFunction(command)` and returns its `{returncode, stdout, stderr}`. Commands are escaped via `_escape_for_shellfunction` (`:40`) because `ShellFunction` runs `cmd.format()` (bare `{}`/`}` would break formatting).
- **`canary(timeout)`** (`:134`) — submits `_CANARY_CMD` (`:23`), a trivial probe that echoes a sentinel plus the worker's host/Python/dill; `_parse_canary` (`:31`) extracts them into a `CanaryResult` (`:8`). Its `.submit()` is **inside** a guarded try — so a shut-down or broken Executor yields `CanaryResult(ok=False)` instead of raising `RuntimeError: Executor is shutdown` (the robust-canary fix, [#37](https://github.com/globus-labs/hpc-bridge/issues/37)). At most one canary is in flight per runner (`_canary_fut`; abandoned after `_CANARY_MAX_WAIT_S`), and `CanaryResult.answered_at` dates the answer (0.1.19). Drives [[Warmth, the canary & cold-start]].
- **`dispatch_error_text(exc)`** (`:66`) — the diagnosis a caller can act on: prefers a Globus API error's `.message` (with its `code`, e.g. `ComputeAPIError[SEMANTICALLY_INVALID]: …`) over `str(exc)`, whose repr spends ~190 chars on the method/URL/status before the message starts — a short slice of it lost the part that matters (found while wiring the MEP no-account failure: the 422's `Identity failed to map to a local user name … Globus username: …` begins past char 190). [[server]] reads the result for the `provisioning` notice suffix, the no-account verdict and the transient-conflict label.
- **`close()`** — shuts down the Executor with `wait=False, cancel_futures=True` (see the warning below). Called on stop (the dropped billed shape), runner-swap, and machine switch.

## Key points

> [!warning] The Executor captures `user_endpoint_config` at build time
> A shape/partition change must rebuild the runner, or the cached Executor keeps the *old* config. [[server]] tracks this with a `runner_stale` flag and rebuilds via `_runner_for`.

> [!warning] Dill skew is the real failure
> `canary` reports the worker's dill version specifically because a mismatch with the client breaks function (de)serialization — the genuine "worker up but tasks fail" hazard.

> [!warning] `close()` must `shutdown(wait=False)` — the real stop hang
> `Executor.shutdown()` **defaults to `wait=True`**, which "will not return until all pending futures have received results" — it **blocks on the AMQP connection drain**. Since `_stop_endpoint` / `_runner_for` / `connect_facility` call `close()` synchronously on the event loop, the default blocked the whole tool for *minutes* **after** the work was done (the multi-minute "stop hang": the `scancel` ran fast, then `close()` hung). Fix: `shutdown(wait=False, cancel_futures=True)` — we never need pending results at teardown ([#17](https://github.com/globus-labs/hpc-bridge/issues/17)).

> [!warning] Bound dispatch at `fut.result(timeout)`, NOT `asyncio.wait_for`
> A related gotcha: `run` does `await asyncio.to_thread(fut.result, self.timeout)`. A **running thread can't be cancelled**, so an `asyncio.wait_for` *around* a dispatch does **nothing** — it waits for the thread anyway. Dispatch is bounded only by `self.timeout` (the arg to `fut.result`, which `concurrent.futures` honors). Don't reach for `wait_for` to shorten a dispatch; it won't.

## See also
[[Two-channel architecture]] · [[Warmth, the canary & cold-start]] · [[server]] · [[dispatch]]

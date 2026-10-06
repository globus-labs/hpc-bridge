# Warmth, the canary & cold-start

> [!abstract] In one line
> "Up" means a **worker answered**, not that the manager is online — so before trusting an endpoint we submit a tiny **canary** task through the real Executor; a returned result ⇒ warm, a timeout ⇒ still cold (and the submit itself kicks the block).

## The cold-start gap

`manager_online` is a cheap Globus *web* query that only reflects the **login-node manager**. But in the [[MEP & templated endpoints|MEP model]] the first task forks the UEP and submits the scheduler block, so the manager reads "online" while the next command would still **cold-start** (no worker yet). Trusting `manager_online` makes `run_shell` dispatch into a 124 timeout.

## The canary

`_confirm_worker` ([[server]], `server.py:593`) submits a trivial `ShellFunction` through the *same* long-lived Executor real work uses ([[runner]], `GlobusRunner.canary`). The canary command echoes a sentinel plus the worker's host, Python, and dill versions:

- **returned result** ⇒ a worker is truly live ⇒ `warm`.
- **timeout** (`CANARY_TIMEOUT_S = 8 s`, `server.py:463`) ⇒ still `provisioning` — and the submit has *kicked* the cold block.
- **submit/dispatch failure** — e.g. a reused endpoint whose Executor is shut down ⇒ **not-warm** → `provisioning`, never a propagating crash. This is the [#37](https://github.com/globus-labs/hpc-bridge/issues/37) mechanism that lets a stale-"online" reused ghost degrade to `provisioning` (recover by teardown) instead of dead-ending in `RuntimeError: Executor is shutdown`. A **non-timeout** failure additionally means the dispatch path itself broke (the web service refused the submit — a config the endpoint's schema rejects, a bad partition) and the SDK Executor has shut *itself* down, so `_confirm_worker` marks the runner stale (rebuilt on the next call) and keeps the failed `CanaryResult` as `last_canary`: its text (the API error's `.message`, kept whole by [[runner]] `dispatch_error_text`) rides the `provisioning` notice as a `— last dispatch failed: …` suffix (`_dispatch_error_suffix`), so the cause is visible rather than buried under "allocating nodes…". A `RESOURCE_CONFLICT` (the web service's 409 "already in use … concurrent requests", seen when two submits land within ~2 s) is labelled **TRANSIENT** — wait ~10 s and call again. *(PR [#51](https://github.com/globus-labs/hpc-bridge/issues/51), open, caps that at `TRANSIENT_CONFLICT_LIMIT` = 3 in a row → a `down` saying another session with the same identity holds the endpoint.)*

**One canary in flight (0.1.19).** A canary is a real task, and one used to be submitted per probe: a 15-minute cold-start wait queued 24 (Delta, 2026-10-06). A facility MEP's user endpoint keeps relaunching billed blocks for as long as ANY task is queued — up to its hard idle limit (48 h on Delta), with no cancel channel from the client — so a broken worker plus a pile of canaries is a long, silent bill. `GlobusRunner.canary` now keeps the one outstanding future and waits on it again; it submits a new one only after an answer, a closed runner, a canary still pending after `_CANARY_MAX_WAIT_S` (300 s — an endpoint that restarts can drop a task it had accepted, and a lost canary must not wedge the shape), or a SUCCESS that arrived more than `_CANARY_FRESH_S` (30 s) before anyone asked (it proves nothing about now). A failure that arrived unwatched is handed back once whatever its age — its text is the diagnosis (a no-account verdict is terminal). Warmth is dated from when the worker actually answered (`CanaryResult.answered_at`), so the TTL below, the idle clock and the reap estimate measure from real proof; `force_canary` therefore accepts an answer up to 30 s old. The runner's canary state is not re-entrant: its one caller runs under `app.lock`. On a MEP still "allocating" after five minutes the notice names both invisible failures — a scheduler rejection, and a block RUNNING while its worker cannot return results (a package skew with the facility's endpoint, see [[Facility catalog]] `worker_env`) — tells the agent to stop and have the user check their jobs, and carries the attach's STALE ENTRY note when there is one.

A successful canary is trusted for `CANARY_TTL_S = 45 s` (`server.py:460`) so an interactive burst doesn't pay the round-trip every call. (Safe: an idle block needs ≥ `max_idletime`, default 600 s, of silence to release, so a worker seen < 45 s ago can't have vanished.)

**Terminal failures the canary surfaces.** On a facility MEP ([[facility-mep]]) the canary is also where the identity mapping is tested. The manager's no-account notices (`_NO_ACCOUNT_MARKERS`, [[server]]) turn `provisioning` into a terminal `down` (`ensure_endpoint_up`) / `failed` (a cold `run_shell`) that names the refused Globus identity, and the verdict is **sticky** (`ShapeRuntime.no_account`: `_confirm_worker` returns without re-submitting; cleared by a re-bind, teardown, or a new login). And because a MEP's `manager_online` degrades to `True` on a status-API error, "provisioning with **no canary ever recorded**" there means the manager itself reported OFFLINE — a facility outage, not a queue wait — and the notice says so instead of "allocating nodes…".

A **running task is itself liveness.** While a long poll-handle task ([#21](https://github.com/globus-labs/hpc-bridge/issues/21)) is executing on a shape, `_confirm_worker` returns `warm` **without** a canary: the worker is demonstrably running our work, and a canary would only queue behind the sole worker and — on timeout — wrongly flip us to "not warm", banking the [[Cost control|spend clock]] while the block is still burning.

> [!warning] dill skew is the real failure mode
> The canary reports the worker's dill version; if it differs from ours, function (de)serialization breaks. `_worker_notice` surfaces that as the warm descriptor's warning — it's the genuine compatibility hazard behind "the worker is up but tasks fail."

This is what makes `ensure_endpoint_up`'s "is it warm?" honest and keeps `run_shell` from dispatching into a hang ([[dispatch]] turns a real timeout into a structured outcome).

## See also
[[server]] · [[runner]] · [[lifecycle]] · [[MEP & templated endpoints]] · [[facility-mep]] · [[Cost control]]

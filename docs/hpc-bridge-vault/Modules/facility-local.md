# facility-local.py — `facility/local.py`

> [!abstract] Role
> `LocalFacility` — a Globus Compute endpoint via `LocalProvider`, **no SSH**. It is the facility the server holds when no machine is pinned: **local dev** when `HPC_BRIDGE_ENDPOINT_ID` names an endpoint you run yourself (the server dispatches to it), otherwise the **implicit** "no facility" default, on which nothing runs. The simplest [[facility-base|Facility]] implementation.

## What it does

- **`config_template(profile)`** (`facility/local.py:27`) — the UEP template (engine inline). The `interactive` profile holds a warm block (`min_blocks=1`); `batch` scales to zero. A `LocalProvider` block costs no allocation, so unlike [[facility-remote]] it can stay warm for snappy dev.
- **`provision(profile)`** (`:50`) — `configure` (forced `--multi-user false`), write the engine into the UEP template, `start`. Drives the local [[endpoint|`EndpointCLI`]]. **The server's boot path never reaches it:** an implicit facility is refused first, and with `HPC_BRIDGE_ENDPOINT_ID` the state is seeded so `ensure_warm` ([[lifecycle]]) dispatches to that endpoint instead. Its callers are a `LocalFacility` built directly (`tests/integration/test_local_endpoint.py`) and one known gap: a local-dev session whose endpoint state `teardown_endpoint` cleared, after which the next call provisions.
- **`manager_online(endpoint_id)`** (`:65`) — the Globus web status query.
- **`implicit`** (`:20`) — set by `binding._default_local_facility` ([[binding]]) when nothing chose the facility: no machine pinned and `HPC_BRIDGE_ENDPOINT_ID` unset, or **any** pin that failed at boot (then the endpoint id is not seeded either). A `LocalFacility` built any other way is explicit (`implicit=False`).

> [!warning] An implicit LocalFacility is "no facility": nothing is stood up or dispatched on it
> While `context._no_facility_bound(app)` holds, `ensure_endpoint_up`, `run_shell`, `reset_session` and `login_shell` answer the shared "No facility connected" notice ([[notices]] `_no_facility_notice`) and build nothing: no shape runtime, no Executor, no `provision`, no endpoint CLI call. The check reads the `implicit` mark, never the type. Provenance: a `run_shell` before `connect_facility` stood up `hpc-bridge-dev` on the MCP host and answered with the endpoint CLI's internals (Hermes on the fake cluster, 2026-10-10).

> [!note] Local dev = `HPC_BRIDGE_ENDPOINT_ID`, on every OS
> Local dev means an endpoint you run yourself, which hpc-bridge dispatches to rather than starting one. Run it (the `globus-compute-endpoint` daemon runs only on Linux: a container, WSL or a login node) and set `HPC_BRIDGE_ENDPOINT_ID=<uuid>` — see [[endpoint]] and [[Configuration]]. Until 2026-10-10 an unpinned server on Linux without it provisioned `hpc-bridge-dev` on the MCP host; now it is unbound.

## See also
[[facility-base]] · [[endpoint]] · [[binding]] · [[MEP & templated endpoints]]

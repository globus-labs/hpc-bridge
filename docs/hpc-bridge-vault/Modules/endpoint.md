# endpoint.py

> [!abstract] Role
> `EndpointCLI` — drives the **local** `globus-compute-endpoint` binary (configure / start / stop) for [[facility-local]]. The local counterpart of [[facility-remote]]'s `RemoteEndpointCLI`.

## What it does

- **`configure(name)`** (`endpoint.py:55`) — runs `configure --multi-user false` (always personal, never an identity-mapping MEP).
- **`start(name)`** (`:66`) — `start --detach` (4.x `start` is foreground by default), then reads the registered UUID from `endpoint.json`.
- **`stop(name)`** (`:74`).
- **Path helpers** — `config_path` (the engine-free manager config) and `user_template_path` (`:28`, where the engine lives — the v4 invariant).

> [!warning] Linux-only, fail-loud
> `_default_run` (`:37`) raises a clear error on non-Linux hosts: `globus-compute-endpoint` can't provision locally there — run an endpoint yourself on Linux and set `HPC_BRIDGE_ENDPOINT_ID` to dispatch to it. The server's boot path no longer provisions locally on any OS ([[facility-local]]), so this guard is reached only by a `LocalFacility` built directly.

## See also
[[facility-local]] · [[Two-channel architecture]] · [[Configuration]]

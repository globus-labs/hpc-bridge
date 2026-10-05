# Credential seeding

> [!abstract] In one line
> On first connect we build a **least-privilege `storage.db`** locally — only the two tokens an endpoint needs to `start` — and ship it to the remote `~/.globus_compute/storage.db` (`0700`/`0600`), so the daemon authenticates non-interactively without us copying our whole credential.

## What & why

A started endpoint needs tokens for exactly two resource servers:

- **Globus Compute** (`funcx_service`) — to register and receive tasks.
- **Globus Auth** (`auth.globus.org`) — carrying **`openid` + `manage_projects`**.

`build_minimal_storage_db()` ([[credentials]], `credentials.py:78`) copies *only* those two records (with their **refresh tokens**) from the user's `~/.globus_compute/storage.db` into a fresh db. That trimmed db is the least credential that lets `globus-compute-endpoint start` run in a detached daemon. It's shipped base64-over-SSH by `seed_storage_db` ([[facility-remote]], `remote.py:357`) into a `0700` dir as a `0600` file; the local temp copy is wiped after transfer. Seeding is **skipped if `whoami` already succeeds** on the remote.

**Where the source login comes from now.** The user's `storage.db` is written by the **in-terminal Globus login** ([[login]]): `connect_facility` gates on `LoginFlow.login_required()` *before* the catalog read or any SSH, and requests exactly this module's scope set (`login.required_scopes` reuses [[credentials]] `_required_scopes`) through the Compute SDK's own client id and storage — so what the login stores is what seeding trims and what the remote daemon can refresh. A facility MEP ([[facility-mep]]) needs no seeding at all: the same credential dispatches to the facility's endpoint directly.

> [!warning] Validate `manage_projects` *before* shipping
> A plain SDK `Client` login only gets `openid` on `auth.globus.org`, **not** `manage_projects`. A started manager registers `manage_projects` as a hard requirement; without it `login_required()` is True, the detached daemon tries an interactive login, and **dies silently**. `build_minimal_storage_db` checks scope adequacy locally and raises `MissingCredentials` with a clear remediation (run `globus-compute-endpoint login`) — learned live on Anvil.
>
> > [!note] Superseded as the *user-facing* path (2026-09-03, [#48](https://github.com/globus-labs/hpc-bridge/issues/48))
> > A user no longer runs that CLI: the `needs_login` gate in `connect_facility` catches a missing/under-scoped credential first and obtains one with the right scopes in-terminal ([[login]]). The check here stays as the last line of defence (an under-scoped db that slipped past the gate), and its message is still the CLI fallback.

> [!warning] Refresh tokens are mandatory
> Without a refresh token the endpoint stops working when the access token expires. Missing-refresh is a hard `MissingCredentials` failure, not a warning.

The required resource-server names and scopes are resolved from the SDK at runtime, not hardcoded, so they track upstream renames.

## See also
[[Standing up the endpoint]] · [[credentials]] · [[facility-remote]] · [[Two-channel architecture]]

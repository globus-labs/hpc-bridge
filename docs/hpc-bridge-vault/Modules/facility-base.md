# facility-base.py — `facility/base.py`

> [!abstract] Role
> The **Facility seam**: one Protocol behind which all machine-specific behaviour lives, so the runtime is facility-agnostic.

## What it does

- **`Facility`** Protocol (`facility/base.py:27`) — the contract the runtime depends on: `provision(profile)`, `manager_online(endpoint_id)`, `config_template(profile)`. Concrete facilities add optional capabilities the server reads via `getattr`: `bootstrap`/`teardown`/`login_exec`/`scratch_root`/`profile` (SSH facilities), **`supported_shapes`** / `max_idletime_s` (a facility MEP), and `account_required` (any facility built from a catalog entry: a MEP, and since 2026-10-14 an SSH one) — absent `supported_shapes` means every shape.
- **`EndpointHandle`** (`:19`) — `provision`/`bootstrap` return value: `endpoint_id`, `name`, `login_host` (the pinned FQDN), `reused` (True if reused over the web — or attached to a facility's endpoint — with no SSH).

- **`endpoint_metadata`** (`:11`) — one read of an endpoint's metadata from the web service (`{}` when it has none); [[facility-mep]] reads its template from it, and [[facility-remote]]'s `adopt_endpoint` the worker_init an endpoint was configured with.

Implementations: [[facility-local]] (LocalProvider, no SSH), [[facility-remote]] (Slurm/PBS over SSH), and [[facility-mep]] (a facility-run multi-user endpoint, zero SSH, compute-only). [[server]]'s `make_facility` picks one from env at startup, and `_facility_from_entry` builds one from a catalog entry at runtime (a `compute_mep_uuid` entry → `MEPFacility`, else `SlurmFacility`).

> [!note] Why a seam
> Everything that differs between machines sits behind this Protocol; the dispatch/session/cost runtime never imports a specific facility. The discovery work ([[Discovery today]]) is about *generating* these instead of hand-writing them.

## See also
[[facility-local]] · [[facility-remote]] · [[facility-mep]] · [[server]] · [[Discovery today]]

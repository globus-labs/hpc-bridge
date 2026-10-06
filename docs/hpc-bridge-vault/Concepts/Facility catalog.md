# Facility catalog

> [!abstract] In one line
> Where the per-facility shape comes from: a **Globus Search index** of machine entries — the **public registry**, read anonymously with a built-in id — resolved into a `MachineProfile` (an SSH-bootstrap entry) or a [[facility-mep|`MEPFacility`]] (a `compute_mep_uuid` entry), so adding a facility is *data*, not *code*. The single runtime source: no hardcoded profile and no bundled fallback — the index is it (the seed YAML is the curator's ingest input, not a runtime catalog).

## The entry — `CatalogEntry` (`catalog/entry.py`)

One entry per machine, a **superset of `MachineProfile`**, split by *who controls the value*:

- **`compute:`** — machine-invariant facts the plugin **pins** (user can't override): `scheduler`, `interface`, `env_setup`, `scratch_root`, `endpoint_name`, `amqp_port`. Getting one wrong breaks the endpoint silently (the wrong `interface` → workers never phone home) — the "look up, never infer" category, like the UUIDs.
- **`defaults:`** — per-run tunables the agent/user **may override** (`partition`, `walltime`, nodes/blocks, accelerators).
- plus identity, `ssh_host` **or** `compute_mep_uuid` (at least one — the `_reachable` validator; a MEP-only entry must also use worker-side `$HOME` forms, never `{user}`/`{venv}`), `auth_method`, `account_required` (False for an unmetered machine, so the agent isn't sent hunting for an allocation), the allocation `command` + named `parser` (absent on a MEP — no login channel to run it), and `provenance` / `last_validated`.

`CatalogEntry.summary()` derives the agent-safe `CatalogSummary`: identity + provenance plus **how you get in** — `access` (`mep` | `ssh`), an `access_note` spelling out what the user needs (an account with their Globus identity mapped, or an account + key-based SSH on the login host), and `scheduler`. Derived, never stored (stranger's walk, [#50](https://github.com/globus-labs/hpc-bridge/issues/50)).

`{user}`/`{venv}` are templated at provision time; `worker_init` is *derived* (= `env_setup`); `account` is **never stored** (per-user, from allocation selection). `profile_kwargs()` → `profile_from_catalog_entry` ([[facility-remote]]) is the binding seam to `MachineProfile` — and derives `endpoint_name` = `hpc-bridge-<id>` when a seed omits it (never the bare `hpc-bridge`, which would collide — endpoints are keyed by identity + name). The Anvil entry resolves to the known-good config we've stood up live (verified by test).

## The provider seam — `CatalogProvider` (`catalog/base.py`)

A `Protocol` with `get(machine)` (exact → provisioning) and `discover(query)` (browse). Three implementations:

- **`SearchCatalog`** (`catalog/search.py`) — the runtime catalog: live `get_subject` (then a bare-id search, so `anvil` resolves as well as `purdue:anvil`) → write-through cache under `<CLAUDE_PLUGIN_DATA or ~/.hpc-bridge>/catalog-cache/` (a *fetched-data* offline copy; **no bundled fallback**). An index miss returns `None`. `PUBLIC_REGISTRY_INDEX` (`search.py:14`) is the built-in registry id.
- **`BundledCatalog`** (`catalog/bundled.py`) — loads the checked-in seed YAML: the **curator's ingest source** + a test fixture. *Not* a runtime catalog.
- **`FakeCatalog`** (`tests/fakes.py`) — in-memory test double.

`make_catalog()` ([[server]]) reads the **public registry** — the index id is baked in (`PUBLIC_REGISTRY_INDEX`, `catalog/search.py`) and read **anonymously**; `HPC_BRIDGE_SEARCH_INDEX` only overrides it. No bundled runtime fallback: a machine the registry can't resolve falls to the local BYO cache, then to the probe. **Precedence (decided 2026-09-03): explicit `details=` this session → the registry → the local cache → probe.** The registry wins for any catalogued id — curated entries are the stable ones; a stale local config must never shadow one (it would have, for `globus1`).

> [!info] The index — `6ff95fb8-1113-42be-a811-3d1cb5a67bd5`
> Our Globus Search index (display name `hpc-bridge-test`), owned by the maintainer's Globus identity. It is the plugin's **default registry** (baked in; `HPC_BRIDGE_SEARCH_INDEX` only overrides it), read **anonymously** — a stranger's `list_facilities()` works with no login. Curators pass the UUID to `hpc-bridge-catalog` explicitly (it is not in your interactive shell). Curated entries ingested so far (subject → seed):
> - `purdue:anvil` ← `catalog/seed/anvil.yaml` (SSH-bootstrap, personal endpoint; 2026-06)
> - `globus:globus1` ← `catalog/seed/globus-cluster.yaml` (the **MEP** entry for `globus-cluster-mep`, zero SSH; ingested 2026-08-19 — see [[Endpoint reuse and MEP integration]])
>
> Ingest is idempotent (keyed by subject): `hpc-bridge-catalog 6ff95fb8-1113-42be-a811-3d1cb5a67bd5 src/hpc_bridge/catalog/seed/<file>.yaml` (needs the one-time `search:all` consent). All entries are currently `visible_to: public` — `ingest.py` hardcodes it; per-entry visibility is a curator TODO. **Seed `aliases` are NOT indexed** (the loader resolves them; `ingest.py` drops them and `SearchCatalog` matches only `id`/subject) — at runtime use the `id` (`globus1`) or subject, as `list_facilities` shows. Verified live 2026-08-19: `get("globus1")` → the MEP entry → `MEPFacility`, and a real status read of the MEP returned online.

> [!note] Auth — anonymous by default (public registry); the Compute identity only when it already holds the Search scope
> Globus Search needs auth only for non-public entries, so a fresh install lists the registry with zero setup. The note below records the earlier authenticated-read design; see [[Globus index discovery channel]] for the 2026-09-03 flip.
>
> [!note] (Original) Auth — reuse the Compute identity
> **Reads are anonymous** (`SearchClient()` — superseded 2026-09-03; the earlier authenticated read via `SearchClient(app=Client().app)` is only used when a Search-scoped login already exists, e.g. a curator's). Globus Search needs auth only for non-public entries; the registry's are `visible_to: public`. Writes stay curator-only: `hpc-bridge-catalog` asks for the Search scope itself.

## Three ways in — startup, agentic, or BYO

- **Startup (env-pinned):** `HPC_BRIDGE_MACHINE=<id>` → `make_facility` resolves the entry from the catalog at boot.
- **Agentic (runtime):** `list_facilities()` → `connect_facility(facility)` — after the Globus login gate ([[login]]) — binds the machine (late-binds `AppCtx.facility`) and then, by entry kind: an **SSH entry** brings up its **free login shape**, runs the allocation `command` over Compute, parses it, and returns the allocations → `ensure_endpoint_up(account=…)`; a **MEP entry** only *attaches* (`_connect_mep`: zero SSH, `reused=True`, compute-only, no allocation listing — the notice says whether an account is needed). → [[The MCP tools]] · [[facility-mep]]
- **BYO / discovery (runtime):** a machine the index can't resolve isn't a dead-end — pass `connect_facility(facility, ssh_host=…)` and the server **probes the login node** ([[discovery]]) to *propose* a `FacilityDetails` draft (`proposed_facility_details`) the user confirms (or, with no host, returns `needs_facility_details` and asks). The confirmed `details=` builds a **session-local** entry (`provenance="session"`, on `AppCtx.session_facilities`, **never indexed**, endpoint name `hpc-bridge-<ssh_host slug>` — keyed on the SSH host by `_session_endpoint_name`, so `midway` and `midway3` share one registration, [#27](https://github.com/globus-labs/hpc-bridge/issues/27)) that drives the same flow; the login-shape canary validates it, and the confirmed config is cached to `facilities.json` ([[state]]) for a zero-SSH reconnect. → [[Globus index discovery channel]]

Allocation output is parsed by a **deterministic, plugin-side parser** keyed by `entry.allocation.parser` (`catalog/parsers.py` — `mybalance` built; `sbank`/`iris` reserved). Stdout is parsed in code, **never** handed to the model — inference is exactly what the catalog removes.

> [!note] Runtime binding details
> `connect_facility(facility=…)` resolves its arg by **id or subject** (`anvil` or `purdue:anvil`). It also **moves the [[Session continuity|session-shell]] root** to the bound facility's remote scratch — else `run_shell` would run the session shell at the local `~/.hpc-bridge` path *on the remote node*. And the server **boots resiliently**: if `make_facility` fails at startup (a stale env var, no index), `lifespan` warns and starts *unbound* (`LocalFacility`) rather than crashing — the agent then binds via `connect_facility`.

> [!warning] Trust — read-only plugin, curator-only writes
> The plugin never writes the index — an open-write catalog of executable config (`env_setup` bash, UUIDs) is an injection vector. New machines are curated via the `hpc-bridge-catalog` ingest (`catalog/ingest.py`; PR review = the audit trail). The agent only ever sees a `CatalogSummary` (identity + provenance + the derived `access`/`access_note`/`scheduler` — no executable config or raw UUIDs).

## See also
[[The MCP tools]] · [[Discovery today]] · [[Globus index discovery channel]] · [[Discovery channel model]] · [[facility-remote]] · [[facility-mep]] · [[MEP & templated endpoints]] · [[MEP facilities survey]] · [[login]] · [[state]] · [[server]]

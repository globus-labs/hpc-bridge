# binding

> [!abstract] Role
> How a machine is REACHED: build the `Facility` for a catalog entry (`_facility_from_entry` → an SSH `SlurmFacility` with login name from `~/.ssh/config`, key, ControlMaster and login-node pin, or a `MEPFacility`), the runtime catalog (`make_catalog` → the public registry, read anonymously unless a Search-scoped login exists), the startup-pin path (`make_facility`, `_catalog_facility`), the no-facility default (`_default_local_facility`), the session/BYO helpers (`_entry_from_details`, `_session_endpoint_name`, `_facility_store`, `_resolve_scratch_root`).

Split step 5 (2026-09-03). **The patch-target rule:** every caller — in [[server]] and in this module — goes through the owning module's attribute (`binding.make_catalog()`, `binding._facility_from_entry(…)`, `config._control_settings()`), and tests patch `binding.<name>` / `config._control_settings`, never `server.<name>`. Re-exports from `server` remain for imports only. This is the trap the code-quality review warned about: a name re-exported from `server` but patched there does not reach a caller that resolved it elsewhere.

**The no-facility default.** With no `HPC_BRIDGE_MACHINE`, `make_facility` (`:165`) returns `_default_local_facility()` (`:154`), and `lifespan` uses the same builder, with `unbound=True`, when a pin fails at boot. It is a [[facility-local|`LocalFacility`]] marked `implicit` (nothing chose it) unless `HPC_BRIDGE_ENDPOINT_ID` is set — the only local-dev path, an endpoint the user runs ([[Configuration]]) — and always after a failed pin. [[server]] then refuses every call that would stand up or use an endpoint with "No facility connected" (`context._no_facility_bound`), so the agent connects a facility instead of getting a local endpoint on the MCP host.

## See also
[[server]] · [[Facility catalog]] · [[facility-remote]] · [[facility-mep]] · [[state]] · [[config]]

---
description: Bring up an HPC endpoint to run on — connect to the facility you name (or list options), then warm its login node and report status.
---

Bring up an HPC endpoint the user can run on, then report its status.

**Reaching a facility starts with `connect_facility`, never a bare `ensure_endpoint_up`.** With no facility bound, `ensure_endpoint_up` (like `run_shell` and `login_shell`) starts nothing and answers "No facility connected" — or, if the last `connect_facility` stopped short (a proposed config, a pending one-time login), how to finish it. Instead:

1. **The user named a facility, or gave a login host** → `connect_facility(facility="<id-or-name>"[, ssh_host="<login-host>"])`. This binds the facility and stands up — or **zero-SSH reuses** — the endpoint, then returns your allocations. `provisioning` ⇒ call again shortly; `needs_login` ⇒ the tool already opened the browser and waited ~90 s for the login to land; if the user says the page showed the login finished, just call again — otherwise give them the `login_url` (single-use: never re-open a finished one; `complete_login(code)` in paste mode); `needs_facility_details`/`needs_preauth` ⇒ follow the `driving-hpc` guidance.
2. **No facility yet** → `list_facilities()` and ask which one — and say what each needs first (`access_note`: `mep` = zero SSH but the facility must have mapped the user's Globus identity; `ssh` = an account + key-based SSH on that host), so the user picks one they can actually use (or ask for the SSH login host of an un-catalogued cluster).
3. **Only** if a facility is pinned by env (`HPC_BRIDGE_MACHINE`), or `HPC_BRIDGE_ENDPOINT_ID` names an endpoint to dispatch to (local development), is `ensure_endpoint_up(shape="login")` the direct path.

Then report: the facility, its login node + worker state, and the allocations/partitions available. For the full **select → discover → gate → provision → wait** flow (and how reuse/pre-auth/discovery are decided), follow the `driving-hpc` skill.

If `connect_facility` says the facility is **compute-only** (a facility-run multi-user endpoint: `needs_account` at once, `reused=True`, zero SSH), there is no login node to warm and nothing to list — report that, whether an account is needed (the notice says), and the catalog's default partition; the next step is `ensure_endpoint_up(partition=…, confirm_spend=True)` after the user okays spend. See the skill's *Compute-only facilities* section.

# Discovery today

> [!abstract] In one line
> What the plugin discovers *now*. **Catalogued facility:** after the one-time bootstrap, the agent probes through the **login shape over AMQP** (no SSH); the per-facility shape comes from a **[[Facility catalog|catalog]]** (the Globus Search index), and machine + allocation are **agent-selected at runtime** (`list_facilities` → `connect_facility` → pick an allocation). **Un-indexed facility:** a **raw-SSH login-node probe** (before any endpoint exists) discovers the shape and proposes it for confirmation. No hardcoded machine profile, no bundled fallback.

## What's implemented

- **Catalogued facility — endpoint-first discovery.** The `driving-hpc` skill ([[Plugin packaging]]) sequences: establish the endpoint (`shape="login"`) → discover via `run_shell(shape="login")` — `sinfo`/`mybalance`/`squeue` over AMQP, **not** SSH → gate (partition + budget) → provision `compute` with `confirm_spend` → poll `squeue` via the login shape. The agent-facing `login_shell` tool (raw SSH, [[server]] `:1823`) stays the cold-start escape hatch.
- **Compute-only facility (a facility MEP).** A `compute_mep_uuid` entry binds a [[facility-mep|`MEPFacility`]]: zero SSH, and **no login shape** — `run_shell(shape="login")`, `ensure_endpoint_up(shape="login")` and `login_shell` are refused with a notice, and discovery (`sinfo`, `squeue`, …) runs on the billed `compute` shape, which stays warm between calls (`init_blocks: 1`). There is no allocation listing either — the connect notice says whether an account is needed.
- **Un-indexed facility — raw-SSH login-node probe (built).** `connect_facility(facility, ssh_host=…)` for a machine not in the index runs **one batched login-node command over raw SSH, *before* any endpoint** (`discover_facility_details`, [[discovery]] `:50`, via `_propose_or_ask` [[server]] `:1394`) → a proposed [[models|FacilityDetails]] draft (`interface`/`scheduler`/`partition`/`scratch`/`env_setup`/allocation) the user confirms; the login-shape canary then validates. This is the pre-endpoint discovery channel the cascade reserved — now wired. See [[Globus index discovery channel]].
- **Endpoint reuse.** `find_online_endpoint` ([[facility-remote]] `:821`) is a web query (no SSH) that lets a reconnect reuse a running endpoint — the [[Two-channel architecture|authenticate-once]] keystone.
- **The registry drives the shape — and wins.** The public Globus Search registry holds the facility shape; the **[[Facility catalog|catalog resolver]]** turns an entry into a `MachineProfile` (SSH) or a `MEPFacility` (MEP) — at startup (`HPC_BRIDGE_MACHINE`) or at runtime (`connect_facility`). Read **anonymously** with a built-in index id, so it works with no config and no login. Resolution precedence inside `connect_facility` (decided 2026-09-03): an explicit `details=` → **the registry** → the local BYO cache (`facilities.json`) → the probe. The registry wins for any catalogued id, so a stale local config can never shadow a curated entry. No hardcoded profile, no bundled fallback; an unresolved machine is **not** a dead-end: it falls to the human channel below. See [[Globus index discovery channel]].
- **Agentic machine + allocation selection.** `list_facilities` browses the catalog; `connect_facility(facility)` brings up the free login shape, runs the allocation command (e.g. `mybalance`) over Compute, parses it in code, and returns the allocations to pick from — the choice flows into `ensure_endpoint_up(account=…)`. → [[The MCP tools]]
- **Session-local facilities — built.** An index miss (or index-down) is no dead-end: with an `ssh_host` the agent **probes and proposes** (above); without one `connect_facility` returns `needs_facility_details` and asks for the host. Either way the confirmed `connect_facility(details=…)` builds a **session-local** facility (user-supplied, never indexed), validated by the login-shape canary. See [[Globus index discovery channel]].

## What's deliberately *not* here yet

The remaining discovery-channel machinery — per-channel **ablation flags** and a **resolution trace** — is **planned**, not built (the **login-node probe** channel itself is now built — see above). Seed-emission/write-back (offering a validated facility for curation) is deferred. See [[Discovery channel model]] (the frame) and [[Globus index discovery channel]] (the thread).

> [!note] Superseded (2026-07, [#27](https://github.com/globus-labs/hpc-bridge/issues/27)): session facilities *do* persist
> An earlier version of this note said session-local entries don't survive a restart. A **confirmed** `details=` with an `ssh_host` is now written to `facilities.json` ([[state]] `FacilityStore`, keyed by `ssh_host`) and a later session resolves it with **no SSH probe** — the local-discovery cache. Only the in-memory `AppCtx.session_facilities` dict is per-process.

> [!note] Scope
> This note describes current behaviour only. The catalog generalization, the human/Socratic fallback, **and** the raw-SSH login-node probe are now **built**; only **ablation flags, the resolution trace, and write-back** remain — see [[Globus index discovery channel]].

## See also
[[Facility catalog]] · [[The MCP tools]] · [[Two-channel architecture]] · [[facility-remote]] · [[facility-mep]] · [[server]] · [[state]] · [[Standing up the endpoint]]

## The resolution ladder, as a diagram

Moved here from the README (2026-09-04) when the front page was rewritten for new users. `connect_facility`
resolves a machine server-side, first hit wins; rungs ①–③ cost no SSH, only a brand-new un-indexed machine
falls to the probe. The registry wins for any catalogued id; a cluster you discovered yourself hits ③ and
reconnects zero-SSH from the local cache; a facility MEP is attached at ② with no login node at all.

```mermaid
flowchart TB
  subgraph agent["you / the agent — the only calls"]
    LF["list_facilities()"] -.->|"pick one"| CF["connect_facility(facility, ssh_host?)"]
  end
  subgraph server["inside connect_facility — resolved server-side · first hit wins"]
    direction LR
    R1["① session<br/>this run"] -->|miss| R2["② registry<br/>public index"] -->|miss| R3["③ facilities.json<br/>used before"] -->|miss| R4["④ probe<br/>login node"]
  end
  CF --> R1
  R1 & R2 & R3 -->|hit| UP(["login shape up"])
  R4 -->|"propose config → you confirm"| UP
```

Eleven MCP tools drive it — `list_facilities`, `connect_facility`, `authenticate`, `complete_login`,
`ensure_endpoint_up`, `run_shell`, `poll_task`, `reset_session`, `stop_endpoint`, `teardown_endpoint`,
`login_shell` — see [[The MCP tools]].


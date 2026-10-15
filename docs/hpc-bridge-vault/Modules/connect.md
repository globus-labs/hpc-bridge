# connect

> [!abstract] Role
> The connect flow the agent drives: the Globus login gate FIRST, the resolution ladder (explicit details this session → the registry → the local BYO cache → the SSH probe that proposes a config), the switch of shapes/state to the bound facility, the identity-blind MEP attach or the SSH bootstrap, the allocation listing over the free login shape. Also `_commit_proven_facility` (a BYO config is cached only once the login-shape canary answered) and `_drop_dead_pin` (an unreachable login-node pin is forgotten).

Split step 10 (2026-09-03) — the last. The login-shape channel is injected (`run_login`) as in [[scheduler_ops]]; `server._connect_facility` is a thin wrapper supplying `server._login_runner(app)`, so the many tests that call it and patch `server._run_shell` keep working. Tests that patch the probe patch `connect.discover_facility_details` / `connect._propose_or_ask`. A proposal binds nothing: `_propose_or_ask` records the facility id in `AppCtx.proposed_facility` so a `run_shell` made meanwhile is told to finish the connect with `details=…` ([[server]], No facility bound). `_connect_facility` clears it first thing, so it always describes the LAST connect. A probe that answers `needs_preauth` sets the resume to `connect_facility(<id>, ssh_host=<host>)`: without the host an un-catalogued facility resolves to nothing and the resumed call answered `needs_facility_details` (review 2026-10-10). The account preset (`HPC_BRIDGE_ACCOUNT`) is passed to `binding._facility_from_entry` only for the facility `HPC_BRIDGE_MACHINE` names ([[binding]]); when a preset is set but not used, the bound result's notice opens with one clause saying so (`notices._unapplied_presets_note`: the variable and why — another machine, a facility endpoint, or a partition given in `details=` — never its value). `_prior_note` names a block a facility endpoint lets go of with `notices._left_block_clause`, the words a switch uses too. At the bind it records `AppCtx.terms_origin` (`binding._terms_origin`; `_discovered_partition` (`connect.py:137`) tells a bring-your-own partition the probe proposed, and details= kept, from one the details changed). Every bound result (`needs_account`, an SSH `provisioning`) states the next block's terms before any spend: `_next_block_terms` (`:144`) builds them from the config a new compute runtime would get (`warmth._seed_uec`, nothing built or started; on a MEP after its notes are read) into `block` and `notices._connect_terms_note` — the default queue and its source, the account needed (with the hint) or not, size and walltime, and "ask the user first". No connect notice suggests `confirm_spend=True` any more (spend terms, 2026-10-14), and the warming notice promises "your allocations" only where the entry lists them. Concepts: [[Happy path]], [[Discovery today]], [[Facility catalog]], [[In-terminal Globus login]].

## See also
[[server]] · [[binding]] · [[warmth]] · [[login_gate]] · [[notices]]

## Consent is per conversation; a fresh bootstrap is proven (live findings, 2026-09-04)

Three fresh-user sessions on globus1 showed: (1) the connect right after our own `bootstrap` re-finds the endpoint online
and reads as `reused` — which skipped the proven-cache commit forever and told the agent it had "reused an already-online
endpoint" seconds after a fresh start. `AppCtx.bootstrapped_facilities` records the facilities THIS session bootstrapped;
`_commit_proven_facility` skips only a reuse of an endpoint from before the session, `_reuse_note` says "reconnected to
the endpoint this session started", and the result's `reused` flag is False for our own start. (2) Claude Code's
auto-memory carried a previous session's "confirmed config" into the next one and the agent registered the facility
without asking — the proposal notice now says the confirmation must come from the user in THIS conversation and that the
probe must not be re-run over the agent's own ssh. (3) On the silent (wait-and-continue) login path nothing named the
identity that landed; `_connect_facility` now prefixes "Globus login landed as <identity>" to the result.


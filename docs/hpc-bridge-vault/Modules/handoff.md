# handoff.py

> [!abstract] Role
> Batch phase 2b: `export_handoff` v1, the loam handoff. It builds a wire-ready user endpoint config (UEC) for an orchestrator such as loam, states its terms, validates exactly that config with one billed canary on the user's yes, and writes a data-only record, `hpc-bridge.handoff` version 1. It also tells a stop or teardown which handoffs of the bound endpoint are still live. Design: [[Batch and swarm execution]] §8, §12 D6; tools in [[The MCP tools]].

## What it does

- **`export(app, user_endpoint_config, expires_hours, confirm_spend)`**, behind the `export_handoff` tool ([[server]]):
  1. **`build`**, the config:
     - The facility's own compute defaults (`warmth._seed_uec`) go under the caller's keys. The caller may use either the template's names (`queue`) or hpc-bridge's (`partition`, renamed by the entry's `key_map`), but not both for one setting.
     - Every value is checked (`_check_value`): plain names; `HH:MM:SS`; bounded whole numbers (a bool is not one); accelerators as a count or device ids; `worker_init` as printable text; `scheduler_options` only as `#PBS` / `#SBATCH` lines; other strings only as short tokens.
     - `endpoint_setup`, `compute`, `hpcb_block` and `hpcb_pool` are reserved.
     - The config is fitted to the facility's JSON schema (type, bounds, length, pattern, enum, required, strict `additionalProperties`).
     - Every key must be rendered by the template (`renders`: inside a `{{ … }}` or a `{% … %}`). A caller key that is not rendered, or is refused by the schema, is refused by name. A default key that is not rendered is not sent.
     - Where the template renders them and the caller left them out, the server writes `init_blocks: 0` and `min_blocks: 0`.
     - On an endpoint hpc-bridge stood up, the config gets a block request of its own (`hpcb_block`), so loam's user endpoint is never the session's.
  2. **The template's values** (`readings`, `effective`): the literal, the `default(…)` or the value sent, for `min_blocks`, `max_blocks`, `init_blocks`, `nodes_per_block`, `walltime`, `max_idletime`, `idle_heartbeats_soft` and `heartbeat_period`. Where the template never sets one, parsl's default applies. A value it cannot read is `_UNSET`, never a guess.
  3. **D6:** on a facility endpoint, an effective `min_blocks ≥ 1` (sent, the template's default, or its literal) is refused (`mep_min_blocks`) before any terms.
  4. **`terms_of`**, which never understate:
     - The rate is a block's billing per hour where the partition's weights are known, else node-hours.
     - `worst_case = max_blocks × rate × (hours × max(1, min_charge ÷ idle) + max(walltime, min_charge))`: blocks back to back, each charged at least the minimum, plus one block started just before the record expires.
     - `canary_worst_case = max(1, init_blocks) × rate × max(walltime, min_charge)`.
     - A minimum charge with an unreadable idle window is refused as `unbounded`.
  5. **No `confirm_spend`:** the answer is `needs_confirmation` with the terms and the wire config; nothing is sent.
  6. **The yes:** a runner for exactly that config, and one `submit(CANARY_CMD)` (never `runner.canary()`, which resubmits after 300 s). A `Pending` is kept in `app.handoffs` under the config's digest. A later call with the same config collects it and never sends another; `validating` means it is still out.
  7. **The canary's answer (`_exported`):**
     - Validated before use: the job id, the host, the device pin (`HPCB_PIN`), and the user endpoint from its working directory (`HPCB_WHERE`, `…/uep.<manager>.<uep>/…`). Where that directory is not seen, `uep_id_seen` is null and `uep_id_note` says why.
     - The record is written to `<state_dir>/handoffs/hoff-<12 hex>.json`, mode 0600, atomically.
     - A failed canary writes nothing, and the next call asks again.
- **The record** (`HandoffRecord` and its parts):
  - **Unknown fields are ignored** at every level. `load_record` refuses another schema or version.
  - **`template.sha256`** is the registry's verification digest (`catalog.entry.facility_fingerprint`), so loam's preflight can compare it with `get_endpoint_metadata`.
  - **`loam.env`** is data: `HPC_GLOBUS_ENDPOINT_ID`, `HPC_ENDPOINT_CONFIG_JSON`, `HPC_NNODES`, `HPC_SYSTEM`, each a plain string. Loam writes its `set_env.sh` from it, quoting every value itself: `printf 'export %s=%q\n' "$k" "$v"`, or Python's `f"export {k}={shlex.quote(v)}"`. The record carries no shell.
- **`release_of`**, the end that really works:
  1. Stop submitting with this config.
  2. With nothing queued, its blocks idle-release; the release states how long, and whether the user endpoint itself exits.
  3. A block can be cancelled sooner with `qdel` / `scancel <job>`, built from the validated job id: with `min_blocks` 0 nothing replaces it.
  4. On an endpoint hpc-bridge stood up, the release also says what its own stop and teardown do to the pool.
- **Stops and teardowns** ([[server]] `_stop_endpoint`, `_teardown_endpoint`):
  - **`live(app)`** lists the bound endpoint's handoffs: exported records that are unexpired and not ended (`hoff-….ended` beside one marks it ended), plus canaries still out.
  - **`question`** asks first and changes nothing.
  - **`scope_of`** gives the release's scope: the user endpoints the canaries saw, and their block requests. [[scheduler_ops]] `_release_cmd(keep=…)` spares them; `_release_cmd(only=…)` reads and cancels just them. Neither acts when it can find none.
  - **`end`** and **`drop_pending`** carry out the user's `cancel`.
  - **`mep_note`** names a facility endpoint's handoffs and their release.

**Since review round 1:**
- **`normalize`** checks the caller's config before anything else. A caller's `scheduler_options` may carry only allow-listed directives (`_directive_problem`); PBS `select_options` may name only `ngpus`, `system` and `mem`, with no `+` chunk.
- **The key and the pending file:** a handoff is keyed (`_key`) on the caller's own config, the facility and the endpoint. Its pending canary is written to `pending-<key>.json` with its task id. `_collect` reads it from the service by task id after a restart or a client-side failure.
- **The sent check:** `_note_check` puts the canary among the endpoint's sent checks, so every stop sees it until it resolves.
- **The terms:** the minimum-charge factor divides by `min(idle, walltime)`, and `init_blocks > max_blocks` is refused.

## See also
[[Batch and swarm execution]] · [[The MCP tools]] · [[facility-mep]] · [[scheduler_ops]] · [[runner]]

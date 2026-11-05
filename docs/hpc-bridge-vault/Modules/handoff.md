# handoff.py

> [!abstract] Role
> Batch phase 2b: `export_handoff` v1, the endpoint handoff. It builds a wire-ready user endpoint config (UEC) for a consumer (an orchestrator that runs its own agents), states its terms, validates exactly that config with one billed canary on the user's yes, and writes a data-only record, `hpc-bridge.handoff` version 1. It also tells a stop or teardown which handoffs of the bound endpoint are still live. Design: [[Batch and swarm execution]] §8, §12 D6; tools in [[The MCP tools]].

## What it does

- **`export(app, user_endpoint_config, expires_hours, confirm_spend)`**, behind the `export_handoff` tool ([[server]]):
  1. **`build`**, the config:
     - The facility's own compute defaults (`warmth._seed_uec(…, session_size=False)`) go under the caller's keys: the session's partition and account choice, never the session's block SIZE (the demo of 2026-11-03: the session's `cores_per_node: 3` and `exclusive: false` rode into a handoff unnamed — a confirmation's size is that block's alone). The caller may use either the template's names (`queue`) or hpc-bridge's (`partition`, renamed by the entry's `key_map`), but not both for one setting.
     - Every value is checked (`_check_value`): plain names; `HH:MM:SS`; bounded whole numbers (a bool is not one); accelerators as a count or device ids; `worker_init` as printable text; `scheduler_options` only as `#PBS` / `#SBATCH` lines; other strings only as short tokens.
     - `endpoint_setup`, `compute`, `hpcb_block` and `hpcb_pool` are reserved.
     - The config is fitted to the facility's JSON schema (type, bounds, length, pattern, enum, required, strict `additionalProperties`).
     - Every key must be rendered by the template (`renders`: inside a `{{ … }}` or a `{% … %}`). A caller key that is not rendered, or is refused by the schema, is refused by name. A default key that is not rendered is not sent.
     - Where the template renders them and the caller left them out, the server writes `init_blocks: 0` and `min_blocks: 0`.
     - On an endpoint hpc-bridge stood up, the config gets a block request of its own (`hpcb_block`), so the consumer's user endpoint is never the session's.
  2. **The template's values** (`readings`, `effective`): the literal, the `default(…)` or the value sent, for `min_blocks`, `max_blocks`, `init_blocks`, `nodes_per_block`, `walltime`, `max_idletime`, `idle_heartbeats_soft` and `heartbeat_period`. Where the template never sets one, parsl's default applies. A value it cannot read is `_UNSET`, never a guess: a `{{ }}` value is read only as `x` or `x | default(<one literal>)`, then `| int` / `| tojson` at most (`_VALUE`); anything else — `default(1, true)` (Jinja's boolean default renders a falsy value, the 0 hpc-bridge itself sends, as the default), `d(…)`, an operator — and the key on a line of `{% … %}` statements is unreadable (review round 2). On a facility endpoint an unreadable `min_blocks` is refused as D6 (it may render 1 or more). Only the branches the config can reach are read (`taken`, `_cond`): an `{% if %}` branch whose condition can be told for this config (`x | default(<one literal>)` and nothing after it, `x is defined [and x [is not none]]`; never a string's truth, which the manager's JSON quoting changes) and is not taken is dropped — anything else (`default(x, true)`, Jinja's boolean default; an operator after the default; a call) is unknown and keeps both branches (review round 1: the first pattern had read `default(true, true)` as `x` and understated a facility template's 4 nodes as 1); on hpc-bridge's own template (`closed`) a name the config does not carry is undefined. The demo of 2026-11-03: the terms said `init_blocks` 1 — the LOGIN branch's literal, maxed with the 0 the compute config sends. Readings left in a branch that cannot be told apart still take the most.
  3. **D6:** on a facility endpoint, an effective `min_blocks ≥ 1` (sent, the template's default, or its literal) is refused (`mep_min_blocks`) before any terms.
  4. **`terms_of`**, which never understate:
     - The terms state the value the config sends for every size key it carries: `init_blocks`, `min_blocks`, `max_blocks`, `nodes_per_block`, `walltime`, `max_workers_per_node`, `available_accelerators`, and (since the demo) `cores_per_node`, `cpus_per_node`, `mem_per_node`, `gpus_per_node`, `exclusive`, `select_options`; the question names them (`_block_words`).
     - The block is sized from the config AS SENT (`SlurmFacility.block_size(…, propose=False)`): no `dispatch_uec` gives a handoff D3's proposed slice, so on a per-core partition a config that asks for no slice is parsl's whole node (`--exclusive`) — the terms had costed a 2-core slice of a 4-core node.
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
  - **`template.sha256`** is the registry's verification digest (`catalog.entry.facility_fingerprint`), so a consumer's preflight can compare it with `get_endpoint_metadata`.
  - **Consumer-neutral:** no section is named for, or shaped like, one consumer's settings (a test and the `handoff_record_shape` grader hold the sections to the v1 set). The record carries no shell.
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

**Block environments (PR 3, [[envs]]):** `export_handoff(…, env=)` takes the handoff's env (else none), and `_env_record` resolves it.
- **While it builds:** phase `env_building` — nothing sent, nothing refused; call again (review U6).
- **Refusals:** a failed build, or a refused env, is `env`.
- **The key** is the config's alone (review S2). A collect call with the same config, and the same env or none, collects the canary that is out — never a second billed one. Another env is refused by name (`_same_env`).
- **The canary** runs `envs.canary_check` beside `CANARY_CMD`, from inside the block. That gives `validation.env_ok`. A false one makes `validation.ok` false, and the answer says it first (review S10).
- **What it says:** the handoff's own built env says what the session's would, in the ask (`envs.built_notice`, S14).
- **The record's `env`** (`HandoffEnv`) is for the CONSUMER to activate, and the config is unchanged:
  - a built env: how, kind, key, name, python, path, `bin`, arch and lock_sha256 (on SSH);
  - an existing env: its `activate` line (any facility).
- **v1 stays readable:** `env` is additive (null with no env), and a v1 reader ignores it.
- **PR 4's:** the pass-through of `requirements` into a facility endpoint's config.

## Worked example: a consumer reads the record (loam)

loam is one consumer. Its `hpc-deployment` skill reads four variables from `set_env.sh`; each maps to a field the record already carries:

| loam's variable | From the record |
|---|---|
| `HPC_GLOBUS_ENDPOINT_ID` | `endpoint.manager_id` |
| `HPC_ENDPOINT_CONFIG_JSON` | `user_endpoint_config`, as JSON |
| `HPC_NNODES` | `terms.nodes_per_block` |
| `HPC_SYSTEM` | `facility.system` (loam knows `polaris` and `aurora`) |

**Preflight, before anything is submitted:**
1. Refuse the record if `expires_at` has passed.
2. Refuse it if `template.sha256` differs from the digest of the facility's published metadata: the same rule as the registry's (`facility_fingerprint`), the template, a newline, then the schema as canonical JSON.
3. Run a Python whose `major.minor` is `client_requirements.python`.

**Writing `set_env.sh` safely:** quote every value; never paste one in raw, because the config's JSON holds quotes, `$` and spaces.

```python
import json, shlex
from hpc_bridge.handoff import load_record

rec = load_record(open(path).read())
env = {"HPC_GLOBUS_ENDPOINT_ID": rec.endpoint.manager_id,
       "HPC_ENDPOINT_CONFIG_JSON": json.dumps(rec.user_endpoint_config, sort_keys=True),
       "HPC_NNODES": str(rec.terms.nodes_per_block), "HPC_SYSTEM": rec.facility.system}
open("set_env.sh", "w").write("".join(f"export {k}={shlex.quote(v)}\n" for k, v in env.items()))
```

From a shell, the same values go through `printf 'export %s=%q\n' "$k" "$v"`.

`tests/test_handoff_phase2b.py::test_a_consumers_environment_is_built_from_record_fields_and_written_safely` builds this mapping from a real record, writes the file both ways, sources it and reads the config back.

## See also
[[Batch and swarm execution]] · [[The MCP tools]] · [[facility-mep]] · [[scheduler_ops]] · [[runner]] · [[envs]]

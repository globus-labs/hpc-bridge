# Facility self-description via Globus Compute (registry-less discovery)

> [!abstract] The idea (Gus, 2026-09-22)
> Facilities should describe themselves to agents. The [[Facility catalog|Search-index registry]] is an externalised
> workaround for the fact that they do not. So: a user logs in to Globus, the login enumerates the multi-user endpoints
> they can reach, and for a listed facility the MCP server asks the MEP itself for the configuration the agent needs.
> **Scoped below against what the Compute service does today (measured 2026-09-22).** Verdict: feasible now for the
> static half with zero changes at Globus; the dynamic half exists only where a facility runs a login-shape MEP.

## What already exists (measured with the maintainer's login, read-only)

| Question | Answer today | Evidence |
|---|---|---|
| Does a login enumerate the MEPs I can reach? | **Yes**, `Client.get_endpoints(role="any")` returns every **public** endpoint on the service plus your own. | 70 listed: 60 multi-user, 57 public. Real facilities among them: NCSA Delta + DeltaAI, Purdue Anvil, NERSC Perlmutter, ALCF Polaris/Crux/Sophia/Sirius, UNT Talon, NC State Hazel, UManitoba Grex, Arctic Cluster. Also ~35 demo/test endpoints (academy, docker, ec2 hostnames). |
| What does the listing carry? | Only `uuid`, `name`, `display_name`, `owner`. No `multi_user`/`public` flags, so a client needs one metadata call per UUID to filter. | listing entry keys |
| Is a foreign MEP's metadata readable? | **Yes, for any UUID you know**, public or not, owner or not (`get_endpoint_metadata`). | Delta prod (public), Delta TEST (`public: false`), NeSI login node (`public: false`) all readable; 2 survey UUIDs "could not be resolved" (deleted). |
| What does the metadata contain? | `display_name`, `description`, `multi_user`, `public`, `hostname`, `endpoint_version`, `sdk_version`, `python_version`, `subscription_uuid`, `high_assurance`, **`user_config_template`** (the full Jinja template), **`user_config_schema`**, **`endpoint_config`** (raw `config.yaml`: contact `email`, `admins`, `public`, `amqp_port`). | `scratchpad/delta-mep-metadata.json` (session copy) |
| What does `public: true` mean? | Discoverable in the Globus web portal (and in `role=any`). **Not** access: access is the identity mapping, checked at first submit (the `NO ACCOUNT` verdict, [[facility-mep]]). | Compute docs, config_reference |
| Does anyone self-describe today? | **Nobody uses `description`** (empty on all 57 public MEPs). Schemas vary: Delta typed 2 props, permissive; Anvil 18 typed props, strict; Grex 15 strict; **ALCF 27 props, `required: [queue, account]`, enums on `launcher_type`/`container_type`**; Perlmutter enum `container_type`. Only the lab MEP and our fake MEPs put `description` on properties. No `x-` extension keys anywhere. | the sweep |
| What does hpc-bridge read of it now? | `user_config_schema`, `endpoint_version`, `display_name` — at attach, to fit the config to the schema (`MEPFacility.load_template`, [[facility-mep]]). Nothing at discovery. | `facility/mep.py:167` |

## What a registry entry says that the metadata does not — and where it could come from

| Registry field | In the metadata today? | How a facility could publish it |
|---|---|---|
| scheduler (slurm/pbs) | implicit: the template's `provider: type: SlurmProvider` | parse the template (brittle) or `x-facility.scheduler` |
| partitions / queues, their limits | no (Delta: `partition: {{ partition \| default() }}`, no list) | **standard JSON Schema**: `properties.partition.enum` + `default` + `description` |
| `account_required` | ALCF/Sirius: **yes**, `required: [account]`; Delta: no signal | `required` in the schema (already standard) |
| the facility's own key names (NeSI `ACCOUNT_ID`) | **yes** — the schema's property names ARE the facility's names | infer `key_map` from the schema; generalises #155 |
| default walltime, `init_blocks`, idle window | in the template (`walltime \| default("00:30:00")`, `idle_heartbeats_soft: 10` = 300 s) | parse, or `default` on the schema property |
| interface | in the template (`ifname: eth1`) — a facility concern, the user never sets it | not needed client-side on a MEP |
| `env_setup` / worker_init | in the template (Delta ships a full `uv` bootstrap) — again the facility's | not needed on a MEP |
| scratch root | no | `x-facility.scratch_root` |
| docs URL, contact, human notes | `email` in `endpoint_config`; nothing else | `description` (free text) + `x-facility.docs_url` |
| aliases, display grouping | `display_name` only | `x-facility.aliases` |
| allocation listing command | no | `x-facility.allocation` (as in the registry) |

## Design: three channels, in this order

1. **Static self-description in the MEP's own metadata — the main channel, no change at Globus.**
   JSON Schema tolerates unknown keywords, and the endpoint-side validator (`jsonschema`) ignores them, so a
   facility can ship, in the `user_config_schema.json` it already publishes:
   - standard keywords on the properties the user may set: `enum` (the partitions/queues), `default`, `description`,
     `required` (accounts) — machine-readable and already what ALCF half-does;
   - one extension block, e.g. `"x-facility": {"scheduler": "slurm", "scratch_root": "$SCRATCH", "docs_url": …,
     "allocation": {...}, "aliases": [...], "notes": "…"}`;
   - prose in the endpoint's `description` (`config.yaml`) — today unused by everyone.
   hpc-bridge reads all of it at discovery and at attach (`MEPFacility.from_metadata`). **Who owns it: the facility.**
   The registry entry becomes a curated override / cache, and the channel for facilities that have not adopted the
   convention yet.
2. **Dynamic probe over a login-shape MEP, where one exists.** A "configuration query" to a compute-only MEP is a
   scheduler job (300 s+ cold start, billed): not a query. But several facilities already run a **second, local MEP**
   next to the batch one — `NeSI login node`, `polaris-esn-01-local-ep`, `sirius-esn-0001-local-ep`, ALCF's
   `*_Local_Test_Queue`s. Where the metadata advertises one (`x-facility.login_mep`), hpc-bridge can run the same
   discovery it runs over SSH today ([[login_shell]]: `sinfo`, `sacctmgr`, allocation balance) as the mapped user,
   with zero SSH. That is the [[Discovery over form for BYO|discover-don't-configure]] path on a MEP.
3. **The registry index as fallback and pre-login channel.** `list_facilities` must keep working **anonymously**
   ([[Happy path]]: the first answer needs no login); `get_endpoints` needs a token. And SSH-only facilities have no
   MEP to describe them. So the index stays, demoted: curated entries first, self-described MEPs merged in after
   login, metadata cached in `facilities.json`.

## What changes in hpc-bridge (M-sized)

- `list_facilities`: after login, merge `get_endpoints(role="any")` MEPs. **Filtering is the hard part**: 57 public
  MEPs are mostly test junk. Signal = a self-description present (`x-facility` or `description` or an `enum`'d
  partition), else an institutional `subscription_uuid` + a non-cloud hostname; everything else listed only on an
  explicit query. Registry entries win when both exist.
- `connect_facility(<uuid | display_name>)`: synthesise a `CatalogEntry` from metadata (`MEPFacility.from_metadata`):
  scheduler from the template's provider type, `key_map` from the schema's property names, `account_required` from
  `required`, partitions from `enum`, defaults from `default`, the rest from `x-facility`. Missing pieces go through
  the existing gates (the account floor, the partition question) — nothing new to ask.
- `MEPFacility.load_template` already fetches the metadata; move the fetch to discovery and keep it.
- The lab MEP and the fake `mep` profile adopt the convention first (they are ours) — the scenarios then grade it
  (`stranger_mep_walk` without a registry entry).

## Risks and open questions

- **Spoofing.** Anyone can register a public MEP called "NCSA Delta". Trust signals available: `subscription_uuid`
  (an institutional subscription; Delta's is ACCESS's), `admins`, `email` domain, `hostname` domain. The identity
  mapping remains the real access check, and it only fires at the first submit (the 422 `NO ACCOUNT`), so a
  spoofed MEP could still receive a submit. Show provenance to the user ("self-described, subscription X, contact Y")
  and keep curated registry entries authoritative for the well-known names.
- **Nobody uses `description`**, and the extension convention does not exist yet: this is a pitch to facilities (and
  to Globus: a first-class `metadata` block on the endpoint would beat `x-` keys). The Delta and ALCF contacts are
  in their `endpoint_config`.
- **Service limits** on schema size are unknown; the ALCF schema is 27 properties, so a few hundred bytes of
  `x-facility` is unlikely to matter — verify on the lab MEP.
- **SDK concurrency:** parallel `get_endpoint_metadata` calls from one `Client` triggered four spurious "authenticate
  with Globus" prompts in the sweep (a token-refresh race). Serialise the calls (hpc-bridge already does, `app.lock`).
- **Login before listing** changes the stranger's first minute: keep the anonymous registry answer first, then offer
  "log in to see every facility your identity can reach".

## Next steps (proposed, not started)

1. Write the `x-facility` convention (one page, with the standard-keyword part first) and apply it to the lab MEP.
2. hpc-bridge: `MEPFacility.from_metadata` + `key_map` inference + merged discovery behind the login; scenario on the
   fake `mep` profile without a catalog file.
3. Take the convention to Globus (Compute team) and to NCSA/ALCF with the measured table above.

## See also
[[Facility catalog]] · [[facility-mep]] · [[Endpoint reuse and MEP integration]] · [[Discovery over form for BYO]] · [[MEP facilities survey]] · [[Happy path]]

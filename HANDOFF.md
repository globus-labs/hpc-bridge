# hpc-bridge — handoff (state of the repo)

_Snapshot: 2026-09-03, late evening (after the vault audit). V1 sprint: **Tier 1 DONE** (#41); the 08-19 "block-thrashing" thread **closed** (#43 harness, #44 product, #45 follow-ups); **fake-cluster spike merged** (#46). **Tier 2 — the new-user story is BUILT and merged**: in-terminal Globus login (#48), the public registry + terminal no-account (#49), the stranger's walk (#50). **Open: PR #51** (six agentic new-user scenarios + the first model sweep). End-user docs in progress. **See "Resume here" just below.**_
_For a week-long handoff to a co-dev. Design rationale lives in `docs/hpc-bridge-vault/`; this file is the live state + how-to-run + gotchas on top._

## TL;DR

**A stranger with zero configuration can now drive hpc-bridge end to end:** `list_facilities` reads the public registry anonymously (the index id is built in), the first `connect_facility` opens a browser for the one Globus login and continues in the same call, an SSH facility is bootstrapped once (or reused with zero SSH) and a facility multi-user endpoint (MEP) is attached with zero SSH ever, a billed block is spend-gated, and stop is honest (`down` confirmed / `draining` — terminal on a MEP). What used to fail cryptically now says what a newcomer can act on: `needs_login`, `NO SSH ACCESS to <host> as <user>`, `CANNOT REACH <host>`, a terminal `NO ACCOUNT` naming the refused Globus identity, an ORPHANED task instead of polling forever.

Everything through #50 is on `main`. All unit tests green (**392 passed, 2 skipped**; harness graders **53 passed**). The SSH personal-endpoint path is unchanged and still green.

## Cross-harness benchmark thread (2026-09-06 → 09-09) — where it stands

A separate strand on top of the V1 sprint: **can other harnesses drive hpc-bridge as well as Claude Code?** Design record:
vault `Planned/ACP interactive benchmark driver.md` (the plan of record for this thread); results: vault
`Reference/Cross-harness benchmark - sonnet-4.6 core pair 2026-09-09.md`; the earlier weaker-model study and its confound
analysis: `Reference/Cross-harness study - gpt-oss-120b vs Claude.md`.

- **Objective (user decision 2026-09-08):** compare *like models through a variety of harnesses* — one agent-agnostic ACP
  driver, one persona'd human-sim, the same graders and the same MCP guidance channel — so the claim is about the harness.
- **Built:** the ACP driver with a tested human-sim turn policy (reply / nudge / conclude; a standing decline is never
  nudged) — #150 merged; trace sources per harness (hermes `state.db`, Claude Code's native CLI transcript; the ACP stream
  is NOT a grading source — both adapters drop tool I/O) + a persisted ACP event log with a stream-vs-trace cross-check +
  regrade for every bundle shape — **PR #151 open**; Claude Code as an ACP operator (`--operator claude-acp`, Zed's
  `claude-agent-acp` in the jail) — **PR #152 open, stacked on #151**.
- **Result:** claude-sonnet-4.6 via hermes/Argo vs via Claude Code, 3 interactive scenarios × n=5: **30/30**, empty
  failure taxonomy, no report-only grader fired, identical dialogue shape and hpc-bridge call counts, cross-check agreed
  in all 30; $23.62 Argo. Campaign driver/analysis/summary: `agentic/campaigns/2026-09-09-s46-core-pair/`.
- **Next (in order):** the `hostile` profile on both harnesses (security posture per harness); a provider-path control
  (Claude Code pointed at Argo via the adapter's gateway option); a third ACP harness on the same model (OpenCode/Goose —
  the per-harness cost is a graded-trace reader); a second model pair (haiku-4.5).
- **Gotchas that cost time:** `agent-client-protocol` must stay at hermes' 0.9.0 pin (0.12.x breaks `hermes acp`; the
  Claude adapter works with either); the published Claude adapter DISALLOWS AskUserQuestion → prose asks; a host
  `permissions.defaultMode: auto` makes the adapter's `session/new` fail (jail is fine); laptop sleep kills the Argo tunnel
  (re-run `argo-up` for Duo); a Docker Desktop frontend restart restarts the engine (do it between cells); never
  `ruff --fix` a glob that can name the vendored `inference_auth_token.py` (`force-exclude` now guards it).

## Resume here (2026-09-03, after #50; PR #51 open)

**Where we are:** Tier 2 of the V1 sprint — `docs/hpc-bridge-vault/Planned/V1 release.md` is the plan of record. Merged today, in order: #41 (M1), #42, #43 (harness isolation), #44 (`poll_task` ORPHANED), #45, #46 (fake-cluster spike), #47 (clean stop), **#48 (in-terminal login), #49 (public registry + terminal no-account), #50 (stranger's walk)**.

- **PR #51 — OPEN** (`feat/agentic-stranger-scenarios`): six new-user-story scenarios (`zero_config_list`, `needs_login_paste`, `mep_no_account`, `no_ssh_access`, `registry_over_cache`, `stranger_mep_walk`), per-scenario knobs (`NO_GLOBUS_DB`, `GLOBUS_DB_SECRET`, server-only `EXTRA_ENV`, `SEED_FACILITY_CACHE`, `SERIAL`, `COOLDOWN_S`), graders that read the agent's words (`Trace.texts`), `agentic/watch.sh`, and **round 1 of the model sweep** (Opus 5 / Sonnet 5 / Haiku 4.5 × the five cheap scenarios — written up in `docs/hpc-bridge-vault/Reference/Model sweep 2026-09-03.md` on that branch: 17/30 cells passed; 12 of 13 failures were a globus1 sshd outage, a grader false positive, or two cells sharing one identity — not the models). It carries **two product changes**: `TRANSIENT_CONFLICT_LIMIT` (3 consecutive `RESOURCE_CONFLICT` refusals ⇒ a `down` saying another session with the same identity holds the endpoint — Sonnet retried the "call again" hint 7×) and the discovery-probe path routed through `_explain_provision_error` (so a refused probe SSH also reads `NO SSH ACCESS …`). **Next:** review + merge; then re-run the outage-affected cells when globus1's sshd is back, then the block tier (`stranger_mep_walk`, serial) and the SSH classics on Sonnet/Haiku.
- **End-user docs (Tier 2) — in progress** in `docs/user/` (install + quickstart, the facility matrix Anvil · globus1-MEP · BYO, platform notes). Being written in parallel; don't duplicate it in the vault.
- **Remaining Tier 2:** a purpose-named **production registry index** (today's is `hpc-bridge-test`) + the curator of record. (The fake-cluster tier is DONE and is now the main regression backbone — `run_suite --target fake --profile default|site|mep`: SSH block tier 10/10, chaos hooks, the site scenarios, the admin channel, and the facility-MEP path against two in-container MEPs; the `spend_refusal` proactive-refusal grader landed in #103.)
- **Blocked on the cluster:** the sweep re-runs above (globus1 sshd refused port 22 from ~18:05 local on 09-03; the node and the MEP manager stayed up); `no_ssh_access` is a **fail2ban trigger** — serial with a 660 s cooldown, or whitelist the harness egress in the cluster's `ignoreip`. Aurora stays blocked on an allocation.
- **Tier 3** (release engineering) — DONE to beta (2026-09-04): marketplace packaging (#52/#74), security review (#75–#79, vault `Reference/Security review 2026-09-04.md`), CHANGELOG.md, version 0.1.1 / tag `v0.1.1-beta.1`; **0.1.2 / `v0.1.2-beta.1`** (same day) ships the in-session one-time code (`complete_preauth`, #86–#88) and the Delta / Anvil-MEP / Expanse registry entries (#83–#85). Remaining: org transfer (after Ryan) + the production registry index in the same release.
- **Docker Desktop was fully quit** at the 09-03 pause (it was misbehaving) — restart it before any live run.
- **The vault was audited against the code in this commit** (2026-09-03): new module notes `Modules/login.md`, `Modules/login_flow_manager.md`, `Modules/facility-mep.md`; `Happy path` is now the stranger's path; `Reference/Configuration.md` lists every env var the code reads; the resolution ladder (and its HTML diagram) shows the registry before the local cache; superseded statements are marked, not rewritten. Start at `docs/hpc-bridge-vault/Home.md`.

## Building project context — start with the vault

**If you're coming to this project cold — a new dev, or a fresh AI/agent session building context — read the vault first: `docs/hpc-bridge-vault/`.** It's the maintainer's map of how hpc-bridge actually works, kept in step with the code. It ships in this repo (plain markdown — you don't need Obsidian; wikilinks `[[X]]` just mean the file `X.md` somewhere in the vault).

**Entry point: [`docs/hpc-bridge-vault/Home.md`](docs/hpc-bridge-vault/Home.md)** — it's the index and has its own reading order. The short path:

1. **`Home.md`** — the one-paragraph "what this is" + the map of everything below.
2. **`Happy path.md`** — the end-to-end flow as a first-time user walks it (no config → list → Globus login → attach or bootstrap → gate → run → stop), the fastest way to see the whole system at once.
3. **Concepts**, in order: `Concepts/Two-channel architecture.md` (SSH control plane vs the AMQP hot path — the central idea) → `Concepts/Standing up the endpoint.md` → `Concepts/MEP & templated endpoints.md` → `Concepts/Facility catalog.md`. Then `Concepts/Resource shapes & the spend floor.md` and `Concepts/Cost control.md`.
4. **The three seams a new user hits first:** `Modules/login.md` (the in-terminal Globus login), `Modules/facility-mep.md` (zero-SSH facility endpoints), `Modules/discovery.md` (an un-indexed facility).
5. **For the current work:** `Planned/V1 release.md` is the plan of record; `Planned/Endpoint reuse and MEP integration.md` (the MEP design + the no-account live record) and `Planned/In-terminal Globus login.md` (the login design + live findings) are the design records behind what shipped.
6. **To understand a specific source file:** `Modules/` has a note per `src/hpc_bridge/` module (`Modules/server.md`, `Modules/facility-remote.md`, …) — read the module note beside the code.

The vault holds the *why* (design rationale, decisions, the reading order); this `HANDOFF.md` holds the *now* (live state, how to run, what's next). Read the vault to understand the system; read on here for where it stands today. (Contributing to the vault itself? `docs/hpc-bridge-vault/Vault style guide.md` first.)

## What Tier 2 added (merged 2026-09-03)

| PR | What |
|---|---|
| **#48** in-terminal Globus login | `src/hpc_bridge/login.py` + `login_flow_manager.py`: `LoginFlow` on the Compute SDK's **own** client id + `storage.db` (so seeding and the remote endpoint's refresh keep working); `connect_facility` gates on it **first** (before the catalog read, before SSH); browser loopback flow that **waits up to `HPC_BRIDGE_LOGIN_WAIT_S`=90 s and continues in the same call**, paste-back fallback for headless sessions; `authenticate(force, mode)` / `complete_login(code)` tools; `phase="needs_login"` + `login_url`/`login_mode` on the connect result; **minimum consent** (Compute + `openid` + `manage_projects`, refresh tokens; no Search scope). |
| **#49** public registry + terminal no-account | `PUBLIC_REGISTRY_INDEX` baked into `catalog/search.py` (`HPC_BRIDGE_SEARCH_INDEX` only overrides); `_make_search_client` reads **anonymously** unless a Search-scoped login already exists; **the registry wins over the local BYO cache** for any catalogued id (`_connect_facility`: details → registry → cache → probe); the MEP manager's identity-mapping refusals become a **terminal `down`/`failed` naming the identity**, **sticky** on `ShapeRuntime.no_account` (a rapid re-submit's transient `RESOURCE_CONFLICT` used to flip it back), cleared by a new login (`_forget_identity_verdicts`); `runner.dispatch_error_text` keeps the API error's `.message`. Live-verified with an unmapped Google identity (`agentic/mep_no_account_check.py`). |
| **#50** stranger's walk | `CatalogSummary.access` / `access_note` / `scheduler` (summaries say how you get in); `_explain_provision_error` (`NO SSH ACCESS to <host> as <user>` + where the name came from + remedies; `CANNOT REACH <host>`; `ControlPath too long`); `_short_control_dir` (ssh caps the whole expanded ControlPath — a deep state dir broke every SSH); `scripts/fresh_user_session.sh` no longer injects the registry id; MEP attach notice says attaching is identity-blind and "NO *allocation* account"; a warm billed block with no charge factor says `session_spend: 0` is not a free tier. Agent-level walk passed: list (~14 s) → attach in one call (~30 s) → block warm ~2 min → `hostname` on `globus2` → honest draining stop. |

## What M1 added (merged 2026-09-03, #41)

Read the commits in order — each is a self-contained step with a full message:

| Commit | What |
|---|---|
| `e2891be` | Foundations: `ssh_host` optional, `Defaults.init_blocks`, `_reachable` + no-client-templating validators; **`MEPFacility`** (`src/hpc_bridge/facility/mep.py`); + the runner/session-shell/scratch hardening a MEP forced (see below) |
| `d808ae0` | 3a — a `compute_mep_uuid` entry builds a `MEPFacility` (`_facility_from_entry` dispatches on it first; MEP wins; no SSH lookup) |
| `34a73a6` | 3b–3d — the server seams: `_shape_reject`, `_connect_mep` (attach, no block), `_stop_mep` (draining-only), teardown-as-detach |
| `528aea3` | The `globus-cluster.yaml` seed + "compute-only facility" guidance in `skills/driving-hpc/SKILL.md` + `commands/hpc-connect.md` |
| `b4be77a`, `efd3a47` | Vault: MEP plan updated (M4 folded into M1); the Search index UUID + its entries recorded |
| `33912f1` | **Agentic harness** — coverage-audit fixes (see "Testing") |
| `4999026` | `mep_compute_only` scenario (the live MEP path) |
| `4a8fdfd` | `agentic/whoami_globus.py` — which identity + scopes a `storage.db` holds |

**The one design idea to internalize:** a facility MEP has **no login shape** (its schema rejects our `LocalProvider`/`compute:false`). `MEPFacility` declares `supported_shapes = ("compute",)` and the server *derives* everything else from that one fact via `getattr(app.facility, "supported_shapes", …)`: no login shape ⇒ no free channel for the allocation listing / the #32 pilot query / the scancel release ⇒ **stop is draining-only, teardown is a detach, every shape is billed.** `SlurmFacility`/`LocalFacility` are untouched (they get the default = every shape).

Hardening that shipped here because a MEP exposed it (all pre-existing, SSH path benefits too):
- **A non-timeout canary failure** (a web-service-rejected submit) used to leave the SDK Executor shut down → `Executor is shutdown` forever while the caller saw "allocating nodes…". Now: keep the failed `CanaryResult`, mark the runner stale (rebuilt next call), surface the error. (`_confirm_worker` in `server.py`.)
- **`session_shell`** expands a `$HOME/`-relative scratch root on the *worker* (`"$HOME"'/rest'`) instead of quoting it literal — needed for a facility whose local username we can't know client-side.
- **`_resolve_scratch_root`** expands `~` client-side only for `LocalFacility`; a remote root stays verbatim.

## The live infrastructure (facts you'll need)

- **The MEP:** `globus-cluster-mep` on globus1, UUID **`da3df250-4013-4d69-942c-eef1568f860c`**. Deployed + administered by the globus-cluster admin (not by us). Identity mapping: **`gusellerm@uchicago.edu` → local `glabs`** (an unmapped identity gets the terminal NO ACCOUNT — by design). Pinned to `globus-compute-endpoint==4.15.0` (the seed's `worker_init` pins it **unconditionally** — a version-skewed worker fails cryptically). `AccountingStorageEnforce=none` so no Slurm `--account` is needed (`account_required: false`).
- **The public registry (the runtime catalog):** Globus Search index **`6ff95fb8-1113-42be-a811-3d1cb5a67bd5`** (display name `hpc-bridge-test`, owned by the maintainer's Globus identity), **baked in as `PUBLIC_REGISTRY_INDEX`** and read anonymously — the server needs no env for it. Entries: `purdue:anvil` (SSH) and `globus:globus1` (MEP). `HPC_BRIDGE_SEARCH_INDEX` only overrides it (a staging registry); the curator CLI `hpc-bridge-catalog <uuid> <seed.yaml>` still takes the UUID explicitly. A production-named index is an open V1 item.
- **`agentic/whoami_globus.py`** — read-only check of which identity + scopes a `storage.db` holds (the two facts that decide whether a run can reach the MEP). Run it if a live run behaves unexpectedly. **`agentic/mep_no_account_check.py`** — the no-agent driver for the unmapped-identity path (log in as a *separate*, unlinked Globus account; `--not <mapped-username>` refuses to run as a mapped one).

## How to run things

```bash
# Unit tests (fast, hermetic, no cluster) — the default gate.
python -m pytest -q                                  # 392 passed, 2 skipped

# Agentic harness graders (also hermetic — proves the graders, not the product).
python -m pytest agentic/harness/test_invariants.py -q   # 53 passed

# Try it AS A FRESH USER (scratch Globus tokens + hpc-bridge state; launched outside the repo so no
# repo-local config applies; the built-in registry is what gets exercised). Then say: connect me to globus1
scripts/fresh_user_session.sh            # 1st run: browser login, then the MEP attach
scripts/fresh_user_session.sh --reset    # brand-new user again

# Live agentic scenarios (need globus1 + Docker + agentic/.env; cost money).
#   agentic/.env holds: CLAUDE_CODE_OAUTH_TOKEN (subscription, NOT API key),
#   HPCB_TEST_GLOBUS_DB (a storage.db whose identity the MEP maps), HPCB_TEST_SSH_* .
./agentic/run_smoke.sh happy_path                    # one scenario
./agentic/run_smoke.sh mep_compute_only              # the MEP path (the registry id is built in — no index env needed)
python3 agentic/run_suite.py --scenarios happy_path --repeat 3 --concurrency 3
```

The pre-merge regression set + costs (~30–40 min, ~$7–10 for a full pass; subscription-billed) and the model-sweep recipe are in **`agentic/README.md`** — read it before a live run.

## The agentic testing framework (`agentic/`)

This repo ships a **live-agent regression harness** — its own test tier, separate from the unit tests. It drives a **headless Claude Code agent** against the real **globus1** cluster, once per scenario, inside a **disposable Docker container** holding only scoped (non-admin) credentials, and **grades the agent's behaviour from its tool-call trace** rather than from return values. It's what proves the *product* works end-to-end (an agent can actually drive HPC through hpc-bridge), which unit tests can't. `agentic/README.md` is the authoritative guide; this is the orientation.

**Two things it is not:** it is **not** collected by `python -m pytest -q` (that's the hermetic tier), and it is **not** free — each scenario runs a real agent against a real cluster and bills your Claude subscription. Run it nightly / on demand / before merging anything that touches connect, discovery, endpoint naming, the local-discovery cache, login, or stop.

**How one run works** (`harness/run.py`): `SETUP` (optional cluster prep) → drive the agent on the scenario's `PROMPT` (a `human_sim.py` persona answers any `AskUserQuestion`) → grade the trace against **invariants** (`harness/invariants.py`, 12 deterministic checks like "no raw SSH after the endpoint is up", "spend was confirmed before a billed block", "stop was honest") → **world postchecks** over SSH (did a block actually get left running?) → **run-scoped teardown** (only this run's endpoint and `uep.<eid>` blocks — never `scancel -u`; pool users are claimed cross-process with `flock`, so two `run_suite`s can run at once). Every run writes a **provenance bundle** to `agentic/runs/<id>/` (`record.json` with the grading, `messages.jsonl`, `transcript.md`, `endpoint-logs.txt`) — gitignored, but they're how you debug a failure after the fact, and `harness/regrade.py` can replay a stored bundle through the *current* invariants offline.

**A scenario** is one file in `agentic/scenarios/` declaring a `PROMPT`, a persona, `EXTRA_INVARIANTS`, `EXPECT_OK` (which invariants gate the verdict), and optional `SETUP`/`POSTCHECKS`/`PHASES` (a cross-restart chain). On `main`: `happy_path` · `gated_provision` · `spend_refusal` · `spend_gate_enforced` · `session_persistence` · `long_job_30m` · `long_task_via_handle` · `idle_release_kill` · `saturation` · `endpoint_reuse` · `endpoint_reuse_chain` · `facility_cache` · `aurora_pbs_bringup` · `mep_compute_only`; PR #51 adds the six new-user ones. To add coverage you add a scenario + (usually) a grader, and a hermetic unit test for the grader in `harness/test_invariants.py` — that last part is the discipline that lets a green run be trusted. `mep_compute_only.py` is a good template for the MEP path; `happy_path.py` for the SSH path.

**Prerequisites** (one-time, in `agentic/.env` — gitignored): `CLAUDE_CODE_OAUTH_TOKEN` (subscription, from `claude setup-token` — **not** an API key), `HPCB_TEST_GLOBUS_DB` (a Globus `storage.db` whose identity the target facility maps — for the MEP that means `gusellerm@uchicago.edu`; check with `python agentic/whoami_globus.py`), and the scoped SSH test user/key (`HPCB_TEST_SSH_*`, default user `hpcbridge-test`; the pool `hpcbridge-test-00..09` for `run_suite`). PR #51 adds `HPCB_TEST_GLOBUS_DB_NOACCOUNT` (a second, unmapped identity's db) for `mep_no_account`. The fake cluster's `mep` profile also needs `HPCB_MEP_EMAIL` (a real contact address for the managers it registers with Globus). Full setup + the pre-merge regression set are in `agentic/README.md`; the design rationale is `docs/hpc-bridge-vault/Planned/Agentic testing - Plan B (runtime sandbox).md`. **`agentic/fakecluster/`** is a compose Slurm cluster wired into both runners (`--target fake`), with PROFILES (`default`; `site` = 3 nodes / debug,compute,gpu / enforced accounting / fake mybalance / 2 login nodes; `mep` = site + two root-run facility MEPs in login01, strict and open schema — the zero-SSH path, with a per-cluster local catalog via the plugin's `HPC_BRIDGE_CATALOG_FILE` seam), a cluster-admin channel (`ADMIN_SETUP`/`ADMIN_CLEANUP`), chaos hooks (`MIDRUN_HOOKS`) and declarative scenario coupling (`TARGETS`/`REQUIRES`). See `agentic/fakecluster/README.md`.

## Where things are

> The **vault (`docs/hpc-bridge-vault/`) is committed in THIS repo** — not a submodule, not a separate remote. It ships with the code. It's an Obsidian vault (has `.obsidian/`), so open that folder in Obsidian for the wikilinks/graph — but every file is plain markdown you can read anywhere.

- `src/hpc_bridge/server.py` — the FastMCP tools + all the orchestration seams (`_connect_facility` with the login gate and the resolution precedence, `_connect_mep`, `_ensure_endpoint_up`, `_run_shell`/`_poll_task`, `_stop_endpoint`/`_stop_mep`, `_shape_reject`, `_explain_provision_error`, the no-account verdict).
- `src/hpc_bridge/login.py` + `login_flow_manager.py` — the in-terminal Globus login (`LoginFlow`; the quiet loopback manager + paste-back).
- `src/hpc_bridge/facility/` — `remote.py` (SSH `SlurmFacility`, Slurm + PBS), `local.py`, **`mep.py`** (`MEPFacility`, zero SSH), `base.py` (the `Facility` protocol + `EndpointHandle`).
- `src/hpc_bridge/catalog/` — `entry.py` (the `CatalogEntry` model + `CatalogSummary.access`), `search.py` (the registry client + `PUBLIC_REGISTRY_INDEX`), `seed/*.yaml` (curator ingest sources), `ingest.py` (`hpc-bridge-catalog` CLI).
- `scripts/fresh_user_session.sh` — the fresh-user launcher; `agentic/clean-session.sh` — the pristine-Claude launcher (keeps your Globus login).
- `docs/hpc-bridge-vault/` — the design record (Obsidian). `docs/user/` — end-user docs (in progress).
- `agentic/` — the live-agent regression harness (`harness/`, `scenarios/`, `run_smoke.sh`, `run_suite.py`, `fakecluster/`, `README.md`).

## Live validation so far

- **Wave 1 (2026-08-19): 3/3 green.** `mep_compute_only` passed on its first live run — the full chain: index → `MEPFacility` → identity-mapped compute (`whoami` → `glabs`) → draining-only stop → the facility's idle-release reclaimed the block (world check confirmed). `happy_path` + `endpoint_reuse_chain` confirm the SSH path is intact.
- **Wave 2: green once the cluster was quiet** — `gated_provision` / `long_task_via_handle` / `spend_gate_enforced` (2026-09-01); the only red was the `spend_refusal` grader gap (the agent was *more* cost-safe than the scenario models — it declined proactively). 
- **The new-user story (2026-09-03):** login L1–L5 live (a fresh token dir → browser → loopback → all scopes + refresh tokens stored; the fresh-user walk logged in and attached in one 7 s call); the no-account path reproduced with a separate Google identity (driver + agent level: one submit, a terminal `down` naming the identity, no retry); the stranger's walk agent-level pass (list → attach → warm block → run → honest stop); the first model sweep round (PR #51) — see "Resume here".

## Next steps (priority order)

> **The V1 sprint is ON (since 2026-08-21).** Scope, decisions, and the full tiered punch-list live in **`docs/hpc-bridge-vault/Planned/V1 release.md`** (the plan of record — reorient there if a task runs long). Headlines: V1 = SSH path + consent-free MEP + BYO (M2 deferred); distribution = a Claude Code plugin **marketplace**.

1. **Review + merge PR #51** (agentic new-user scenarios, the sweep harness, `TRANSIENT_CONFLICT_LIMIT`, the probe-path explanation). Then re-run the sweep's outage-affected cells and the block tier when globus1 is quiet.
2. **End-user docs** (`docs/user/`, in progress) — install, quickstart, facility matrix, platform notes.
3. ~~**Fake-cluster tier**~~ — DONE (#102 onward; profiles, admin channel, MEP profile). Next fake-cluster quirks: TOTP/keyboard-interactive sshd (the `needs_preauth` path), PBS/OpenPBS, Lmod, fail2ban.
4. **`spend_refusal` grader** accepts a *proactive* refusal (cheap).
5. **Production registry index** (purpose-named, curator of record) — swap `PUBLIC_REGISTRY_INDEX`.
6. **Tier 3:** marketplace packaging + publish, `1.0.0` tag/changelog, security review (SSH + credential handling), retire `docs/design/*.md` into the vault.

## Open decisions / observations

- **Should the `globus:globus1` seed drop `interface: enP7s7`?** We forward it, which *overrides* the MEP template's own NIC default; the admin's verified UEC didn't include it. `enP7s7` is correct per the cluster facts, but a wrong NIC on an `init_blocks:1` block is cold-forever-but-billed, so letting the facility's template own the NIC is arguably safer for MEP entries. Confirm with the cluster admin or just drop it from `src/hpc_bridge/catalog/seed/globus-cluster.yaml` and re-ingest.
- **Per-facility MEP template keys** are a model gap before any surveyed MEP (ALCF `queue`, NeSI `ACCOUNT_ID`, Anvil `qos`) can enter the registry — `MEPFacility.from_entry` builds one fixed config shape. See `Reference/MEP facilities survey.md`.
- **`HPC_BRIDGE_USER_DIR` does not relocate the SDK's token storage** — only the *local* endpoint daemon's dir. The MCP process's tokens live at the SDK's `GLOBUS_COMPUTE_USER_DIR` (default `~/.globus_compute`); the harness and `fresh_user_session.sh` set both. An installed plugin therefore shares `~/.globus_compute/storage.db` with any other Globus Compute use on the machine (found in the vault audit; by design so far, but worth a docs line).
- **Login-node pins are never removed** — `LoginNodeStore.remove` has no caller; a routable-but-dead pin fails fast and the reset is deleting `~/.hpc-bridge/endpoints.json` by hand.

## Gotchas (things that cost us time)

- **The registry id is baked in** (`PUBLIC_REGISTRY_INDEX`, `catalog/search.py`; `HPC_BRIDGE_SEARCH_INDEX` only overrides it) and read anonymously — the server needs no env for the catalog any more. `.claude/settings.local.json` (gitignored) may still set the var (harmless). The curator CLI `hpc-bridge-catalog …` still takes the UUID explicitly — pass it literally. Symptom of forgetting: `hpc-bridge-catalog` errors "seed_path required" (the empty var was swallowed as the index arg).
- **A stale local cache can't shadow the registry any more** — but it *can* still serve an id the registry doesn't know. If a BYO facility behaves oddly, look at `~/.hpc-bridge/facilities.json` (or the `HPC_BRIDGE_STATE_DIR` you set).
- **The login gate runs first in `connect_facility`** — before the catalog read. If you ever build the SDK `Client` earlier (e.g. for a new tool), use `Client(do_version_check=False)` and never call anything that can prompt: the SDK's own command-line login writes a URL to stdout and reads stdin — the MCP transport.
- **Keep `HPC_BRIDGE_STATE_DIR` short.** ssh checks the whole expanded `ControlPath` against the Unix socket cap (~104 bytes on macOS); a deep temp dir failed every SSH with `ControlPath too long`. `_short_control_dir` falls back to `~/.hpc-bridge/cm` / `/tmp/hpcb-cm-<uid>`, and the error is explained if even that is too long.
- **A MEP attach says nothing about your access.** Attaching is identity-blind; the first billed submit is where an unmapped identity fails — a terminal `down` saying `NO ACCOUNT`. Don't read a clean `connect_facility` as "I have an account here" (the notice and the skill say so now).
- **`#39` registration-lag** is real and now *visible* (the `first_details_connect_succeeds` reported invariant): the first `connect_facility(details=…)` on a fresh bootstrap often returns `failed` ("could not find endpoint … in list output") and the retry succeeds. It's reported, not gating. Don't mistake it for a regression.
- **Cluster contention** silently fails billed agentic scenarios (block never scheduled → `compute_ran`/`ends_with_stop`/world checks break together). Check `sinfo` / `squeue` before trusting a billed-scenario failure; use `sacct -u <pool-user>` to see whether a block ran and got CANCELLED. And a **globus1 sshd outage** makes every world check `UNVERIFIABLE` (PR #51 labels it) — not a leak.
- **Two harness cells under ONE Globus identity collide**: the web service answers the second with `RESOURCE_CONFLICT` on every submit. Scenarios that bind an identity (`mep_no_account`, `stranger_mep_walk`) are `SERIAL` in PR #51.
- **Seed `aliases` are not indexed** — `ingest.py` drops them; at runtime use the `id` (`globus1`) or subject (`globus:globus1`), as `list_facilities` shows. `globus-cluster-mep` won't resolve as a facility arg.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. PR bodies end with the Claude Code generation line. `agentic/.env` is gitignored — never commit secrets.

## The server split (2026-09-03, done)

`server.py` was 2276 lines carrying six responsibilities; it is now ≈820 lines of FastMCP app, lifespan, tool wrappers and orchestration seams, with the rest in leaf modules — `context` (runtime data), `config` (env accessors + tunables), `notices` (every agent-facing text), `cost` (spend clock), `binding` (facility/catalog construction), `scheduler_ops` (block release + pilot status, login-shape runner injected), `warmth` (the state machine + task handles), `login_gate`, `connect`. **Patch-target rule:** tests patch names on the OWNING module (`binding.make_catalog`, `warmth._provision`, `connect.discover_facility_details`, `config._control_settings`, `scheduler_ops._release_blocks_over_login`), never on `server` — `server` re-exports for imports only. Each step was its own CI-green PR (#58–#65 + this one); the plan and rationale are in the vault's `Reference/Review 2026-09-03 — code quality.md` §1.

# hpc-bridge — handoff (state of the repo)

_Snapshot: 2026-10-05. `main` carries **plugin 0.1.17** (#153, 2026-09-17); the latest tag is **`v0.1.13-beta.1`**
(2026-09-05) — 0.1.14–0.1.17 are untagged, and the next tag is **on hold until connectivity to each registry facility is
re-proven** (maintainer's call). The repository moved to **`globus-labs/hpc-bridge`** (2026-09-16; the old
`ryanchard/hpc-bridge` URLs redirect). Design rationale lives in `docs/hpc-bridge-vault/`; this file is the live state +
how-to-run + gotchas on top._

## TL;DR

**A stranger with zero configuration can drive hpc-bridge end to end:** `list_facilities` reads the public registry
anonymously (the index id is built in), the first `connect_facility` opens a browser for the one Globus login and
continues in the same call, an SSH facility is bootstrapped once (or reused with zero SSH) and a facility multi-user
endpoint (MEP) is attached with zero SSH ever, a billed block is spend-gated (and, since 0.1.17, account-gated where the
facility requires an allocation), and stop is honest (`down` confirmed / `draining` — terminal on a MEP). Twelve MCP
tools; the operational guidance also ships over MCP (`hpcbridge://guidance/operations`) for hosts without skills.

Unit tier **552 passed, 2 skipped**; agentic harness hermetic tests **299 passed** (graders: 78). The live tier has
**46 scenarios** and a local **fake cluster with 10 profiles** (`default site mep totp pbs lmod f2b polaris internal
hostile`) that is now the main regression backbone; the lab cluster is the real-hardware check.

## Where it stands (2026-10-05)

**Open on GitHub:** PR **#160** — vault scoping note *Facility self-description via Compute* (registry-less discovery:
facilities describe themselves through the MEP metadata they already publish). Issues **#2** (agent-specified
resources), **#3** (SSH bootstrap under MFA), **#7** (ACCESS discovery channel) — all from July.

**Known product bugs, not yet fixed** — from the 2026-09-05 plugin review (vault `Reference/Plugin review
2026-09-05.md`), all still present in 0.1.17:
- **#3** teardown's unlocked window (a parallel `connect_facility` can retarget the background teardown; two calls
  run it twice; a stale one-time-code handoff survives a re-bind);
- **#4** adopting an already-running endpoint overwrites the stored login-node pin with `None`;
- **#5** credential seeding can overwrite a pre-existing remote `storage.db` (and later delete the replacement);
- **#6** numbers the agent relays are wrong: the MEP idle window (hpc-bridge's 600 s default, not the facility's), the
  block's node count (spend and the confirm notice assume 1), spend during an SSH teardown (the clock keeps running);
- low: a CHANGED host key is coached like an unknown one in `complete_preauth`; `_CODE_RE` admits password-shaped
  strings (the askpass prompt refusal is the real guard).

**Product questions waiting on a decision:**
- Should the server re-require `confirm_spend` after a detected reap? Today the acknowledgement persists for the
  session; `block_reaped_resume` (#158) showed a second billed block starts on return without a server-side re-ask (the
  agent asked anyway, both runs).
- A side-effect-free status tool: `ensure_endpoint_up` always forces the canary, which can re-kick a billed block — a
  read-only `endpoint_status` (report the in-memory runtime, submit nothing) would serve hosts that show status.
- "Provisioning" is inferred (manager online + canary timeout). The web service's per-task status (`get_task`:
  `pending` + status) could make it partly observed, including on a MEP — not yet verified live or wired in.

**Recent threads (all merged):**
- **Cross-harness benchmark** (#145–#154): one agent-agnostic ACP driver + one persona'd human-sim; trace sources per
  harness; Claude Code as an ACP operator (#154, replacing #152 — closed when its stacked base was deleted). Core pair
  sonnet-4.6 via hermes vs Claude Code: **30/30**, identical dialogue shape (vault `Reference/Cross-harness benchmark -
  sonnet-4.6 core pair 2026-09-09.md`). Next: the `hostile` profile on both harnesses, a provider-path control, a third
  ACP harness, a second model pair.
- **REPL benchmark** (#156): P1–P7 definition + a 10-step protocol vs the local Bash tool; 6-round sweep on fake + the
  lab cluster, 18/18 OK — every REPL property holds (env carries; the local Bash tool drops it); a turn is ~4.7 s vs
  ~1.7 s locally, the ~2 s tool time is the Globus Compute round trip, not the cluster (vault `Planned/REPL-like
  interaction benchmark.md`). Run bundles live in the gitignored `agentic/runs/`.
- **Facility key renames** (#155, building on external PR #149): `compute.key_map`, applied only at the wire
  (`dispatch_uec`); the runtime config keeps hpc-bridge's names.
- **`block_reaped_resume`** (#158): block reaped under an idle session → honest `cold_start`, the session cwd survives
  the block, work completes on a new block. Its first run caught the agent claiming the cwd died with the block —
  worth a line in `SKILL.md` / the cold-start notice (not yet written).
- Housekeeping: `agentic/sweep_endpoints.py` (#157) sweeps stale harness endpoint records under the shared identity.

**Remaining V1 items** (plan of record `docs/hpc-bridge-vault/Planned/V1 release.md`): a purpose-named production
registry index + curator of record (today's index is named `hpc-bridge-test`); re-prove each registry facility, then
tag; retire `docs/design/*.md` into the vault. Aurora stays blocked on an allocation.

**A loose branch:** `origin/feat/catalog-allocation-flow` (PR #17, merged in June) carries two later skill-wording
commits that never reached `main` (`917cbdb`, `f5964f2`: "a wait only happens if you END YOUR TURN" / "stop encoding
wait cadence"). The second matches the standing rule that the skill teaches domain, not harness plumbing; decide
whether either is still wanted, then delete the branch.

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

## How we got here (condensed)

- **M1 (#41, 2026-09-03):** facility multi-user endpoints (`MEPFacility`, zero SSH) next to the SSH path.
- **Tier 2 (#48–#51):** the in-terminal Globus login, the public registry read anonymously, a terminal NO ACCOUNT, the
  stranger's walk and its agentic scenarios.
- **The server split (#58–#66)** and two review rounds (#67, #68): see the section at the end.
- **Release work (#73–#95):** README for new users, release-readiness and security passes (host key as the boundary,
  teardown wipes only what it seeded), one-time codes in the chat (`complete_preauth`), the Delta / Anvil-MEP / Expanse
  registry entries, teardown as a server task; betas `v0.1.1` → `v0.1.13`.
- **The fake-cluster tier (#96–#124):** node gating, chaos hooks, ten profiles, the admin channel, the hostile profile
  and safety floor; plugin 0.1.7–0.1.14 fixes found by it.
- **Hosts beyond Claude Code (#125–#154):** guidance over MCP (0.1.15/0.1.16), hermes-agent and ALCF as a second
  operator, the ACP benchmark driver.
- **0.1.17 (#153):** the account floor; scheduler rejections as a terminal `down`.

**The one design idea to internalize:** a facility MEP has **no login shape** (its schema rejects our `LocalProvider`/`compute:false`). `MEPFacility` declares `supported_shapes = ("compute",)` and the server *derives* everything else from that one fact via `getattr(app.facility, "supported_shapes", …)`: no login shape ⇒ no free channel for the allocation listing / the #32 pilot query / the scancel release ⇒ **stop is draining-only, teardown is a detach, every shape is billed.** `SlurmFacility`/`LocalFacility` are untouched (they get the default = every shape).

## The live infrastructure (facts you'll need)

- **The lab cluster** (ssh alias `globus1`, the login node): six DGX Sparks, `spark1`–`spark6` since 2026-10 — **compute on `spark1`–`spark4`** (partitions `main` and `backfill`, 2-day limit), `spark5`–`spark6` in `service` (the lab's inference server). Admin docs and IaC: `~/Projects/globus-cluster-docs`.
- **The MEP:** `globus-cluster-mep` on globus1, UUID **`da3df250-4013-4d69-942c-eef1568f860c`**. Its schema caps `nodes_per_block` / `init_blocks` / `max_blocks` at **4** and `max_workers_per_node` at 20, with descriptions on each property (the first facility to self-describe in its schema). Deployed + administered by the globus-cluster admin (not by us). Identity mapping: **`gusellerm@uchicago.edu` → local `glabs`** (an unmapped identity gets the terminal NO ACCOUNT — by design). Pinned to `globus-compute-endpoint==4.15.0` (the seed's `worker_init` pins it **unconditionally** — a version-skewed worker fails cryptically). `AccountingStorageEnforce=none` so no Slurm `--account` is needed (`account_required: false`).
- **The public registry (the runtime catalog):** Globus Search index **`6ff95fb8-1113-42be-a811-3d1cb5a67bd5`** (display name `hpc-bridge-test`, owned by the maintainer's Globus identity), **baked in as `PUBLIC_REGISTRY_INDEX`** and read anonymously — the server needs no env for it. Four entries (`src/hpc_bridge/catalog/seed/`): `delta` (NCSA, MEP), `anvil` (Purdue, MEP), `expanse` (SDSC, SSH), `globus-labs` (the lab cluster, MEP). Seed edits reach users only after a curator re-ingests them. `HPC_BRIDGE_SEARCH_INDEX` only overrides it (a staging registry); the curator CLI `hpc-bridge-catalog <uuid> <seed.yaml>` still takes the UUID explicitly. A production-named index is an open V1 item.
- **`agentic/whoami_globus.py`** — read-only check of which identity + scopes a `storage.db` holds (the two facts that decide whether a run can reach the MEP). Run it if a live run behaves unexpectedly. **`agentic/mep_no_account_check.py`** — the no-agent driver for the unmapped-identity path (log in as a *separate*, unlinked Globus account; `--not <mapped-username>` refuses to run as a mapped one).

## How to run things

```bash
# Unit tests (fast, hermetic, no cluster) — the default gate.
python -m pytest -q                                  # 552 passed, 2 skipped

# Agentic harness graders (also hermetic — proves the graders, not the product).
python -m pytest agentic/harness/test_invariants.py -q   # 78 passed (all harness tests: 299)

# Try it AS A FRESH USER (scratch Globus tokens + hpc-bridge state; launched outside the repo so no
# repo-local config applies; the built-in registry is what gets exercised). Then say: connect me to globus-labs
scripts/fresh_user_session.sh            # 1st run: browser login, then the MEP attach
scripts/fresh_user_session.sh --reset    # brand-new user again

# Live agentic scenarios (need globus1 + Docker + agentic/.env; cost money).
#   agentic/.env holds: CLAUDE_CODE_OAUTH_TOKEN (subscription, NOT API key),
#   HPCB_TEST_GLOBUS_DB (a storage.db whose identity the MEP maps), HPCB_TEST_SSH_* .
./agentic/run_smoke.sh happy_path                    # one scenario on the lab cluster
HPCB_TARGET=fake ./agentic/run_smoke.sh happy_path   # the same on the local fake cluster (the main backbone)
./agentic/run_smoke.sh mep_compute_only              # the MEP path (the registry id is built in — no index env needed)
python3 agentic/run_suite.py --scenarios happy_path --repeat 3 --concurrency 3
```

The pre-merge regression set + costs (~30–40 min, ~$7–10 for a full pass; subscription-billed) and the model-sweep recipe are in **`agentic/README.md`** — read it before a live run.

## The agentic testing framework (`agentic/`)

This repo ships a **live-agent regression harness** — its own test tier, separate from the unit tests. It drives a **headless agent** (Claude Code, or hermes-agent / Claude Code over ACP) against the local **fake cluster** or the real **lab cluster**, once per scenario, inside a **disposable Docker container** holding only scoped (non-admin) credentials, and **grades the agent's behaviour from its tool-call trace** rather than from return values. It's what proves the *product* works end-to-end (an agent can actually drive HPC through hpc-bridge), which unit tests can't. `agentic/README.md` is the authoritative guide; this is the orientation.

**Two things it is not:** it is **not** collected by `python -m pytest -q` (that's the hermetic tier), and it is **not** free — each scenario runs a real agent against a real cluster and bills your Claude subscription. Run it nightly / on demand / before merging anything that touches connect, discovery, endpoint naming, the local-discovery cache, login, or stop.

**How one run works** (`harness/run.py`): `SETUP` (optional cluster prep) → drive the agent on the scenario's `PROMPT` (a `human_sim.py` persona answers any `AskUserQuestion`) → grade the trace against **invariants** (`harness/invariants.py`, 12 deterministic checks like "no raw SSH after the endpoint is up", "spend was confirmed before a billed block", "stop was honest") → **world postchecks** over SSH (did a block actually get left running?) → **run-scoped teardown** (only this run's endpoint and `uep.<eid>` blocks — never `scancel -u`; pool users are claimed cross-process with `flock`, so two `run_suite`s can run at once). Every run writes a **provenance bundle** to `agentic/runs/<id>/` (`record.json` with the grading, `messages.jsonl`, `transcript.md`, `endpoint-logs.txt`) — gitignored, but they're how you debug a failure after the fact, and `harness/regrade.py` can replay a stored bundle through the *current* invariants offline.

**A scenario** is one file in `agentic/scenarios/` declaring a `PROMPT`, a persona, `EXTRA_INVARIANTS`, `EXPECT_OK` (which invariants gate the verdict), and optional `SETUP`/`POSTCHECKS`/`PHASES` (a cross-restart chain). On `main`: 46 scenarios — `agentic/README.md` lists the regression set, and `agentic/fakecluster/README.md` the fake-cluster ones. To add coverage you add a scenario + (usually) a grader, and a hermetic unit test for the grader in `harness/test_invariants.py` — that last part is the discipline that lets a green run be trusted. `mep_compute_only.py` is a good template for the MEP path; `happy_path.py` for the SSH path.

**Prerequisites** (one-time, in `agentic/.env` — gitignored): `CLAUDE_CODE_OAUTH_TOKEN` (subscription, from `claude setup-token` — **not** an API key), `HPCB_TEST_GLOBUS_DB` (a Globus `storage.db` whose identity the target facility maps — for the MEP that means `gusellerm@uchicago.edu`; check with `python agentic/whoami_globus.py`), and the scoped SSH test user/key (`HPCB_TEST_SSH_*`, default user `hpcbridge-test`; the pool `hpcbridge-test-00..09` for `run_suite`). `HPCB_TEST_GLOBUS_DB_NOACCOUNT` (a second, unmapped identity's db) feeds `mep_no_account`. The fake cluster's `mep` profile also needs `HPCB_MEP_EMAIL` (a real contact address for the managers it registers with Globus). Full setup + the pre-merge regression set are in `agentic/README.md`; the design rationale is `docs/hpc-bridge-vault/Planned/Agentic testing - Plan B (runtime sandbox).md`. **`agentic/fakecluster/`** is a compose Slurm cluster wired into both runners (`--target fake`), with PROFILES (`default`; `site` = 3 nodes / debug,compute,gpu / enforced accounting / fake mybalance / 2 login nodes; `mep` = site + two root-run facility MEPs in login01, strict and open schema — the zero-SSH path, with a per-cluster local catalog via the plugin's `HPC_BRIDGE_CATALOG_FILE` seam), a cluster-admin channel (`ADMIN_SETUP`/`ADMIN_CLEANUP`), chaos hooks (`MIDRUN_HOOKS`) and declarative scenario coupling (`TARGETS`/`REQUIRES`). See `agentic/fakecluster/README.md`.

## Where things are

> The **vault (`docs/hpc-bridge-vault/`) is committed in THIS repo** — not a submodule, not a separate remote. It ships with the code. It's an Obsidian vault (has `.obsidian/`), so open that folder in Obsidian for the wikilinks/graph — but every file is plain markdown you can read anywhere.

- `src/hpc_bridge/server.py` — the FastMCP tools + all the orchestration seams (`_connect_facility` with the login gate and the resolution precedence, `_connect_mep`, `_ensure_endpoint_up`, `_run_shell`/`_poll_task`, `_stop_endpoint`/`_stop_mep`, `_shape_reject`, `_explain_provision_error`, the no-account verdict).
- `src/hpc_bridge/login.py` + `login_flow_manager.py` — the in-terminal Globus login (`LoginFlow`; the quiet loopback manager + paste-back).
- `src/hpc_bridge/facility/` — `remote.py` (SSH `SlurmFacility`, Slurm + PBS), `local.py`, **`mep.py`** (`MEPFacility`, zero SSH), `base.py` (the `Facility` protocol + `EndpointHandle`).
- `src/hpc_bridge/catalog/` — `entry.py` (the `CatalogEntry` model + `CatalogSummary.access`), `search.py` (the registry client + `PUBLIC_REGISTRY_INDEX`), `seed/*.yaml` (curator ingest sources), `ingest.py` (`hpc-bridge-catalog` CLI).
- `scripts/fresh_user_session.sh` — the fresh-user launcher; `agentic/clean-session.sh` — the pristine-Claude launcher (keeps your Globus login).
- `docs/hpc-bridge-vault/` — the design record (Obsidian). `docs/user/` — end-user docs (install, quickstart, facilities, login, costs, other MCP hosts, troubleshooting).
- `agentic/` — the live-agent regression harness (`harness/`, `scenarios/`, `run_smoke.sh`, `run_suite.py`, `fakecluster/`, `README.md`).

## Open decisions / observations

- **Should the `globus-labs` seed drop `interface: enP7s7`?** (Re-check now that the nodes are `spark1`–`spark6`.) We forward it, which *overrides* the MEP template's own NIC default; the admin's verified UEC didn't include it. `enP7s7` is correct per the cluster facts, but a wrong NIC on an `init_blocks:1` block is cold-forever-but-billed, so letting the facility's template own the NIC is arguably safer for MEP entries. Confirm with the cluster admin or just drop it from `src/hpc_bridge/catalog/seed/globus-cluster.yaml` and re-ingest.
- **`HPC_BRIDGE_USER_DIR` does not relocate the SDK's token storage** — only the *local* endpoint daemon's dir. The MCP process's tokens live at the SDK's `GLOBUS_COMPUTE_USER_DIR` (default `~/.globus_compute`); the harness and `fresh_user_session.sh` set both. An installed plugin therefore shares `~/.globus_compute/storage.db` with any other Globus Compute use on the machine (found in the vault audit; by design so far, but worth a docs line).
- **Login-node pins:** a dead pin is forgotten when a connect fails with `CANNOT REACH` / `UNKNOWN HOST KEY` (`connect._drop_dead_pin`); otherwise the reset is deleting `~/.hpc-bridge/endpoints.json` by hand.

## Gotchas (things that cost us time)

- **The registry id is baked in** (`PUBLIC_REGISTRY_INDEX`, `catalog/search.py`; `HPC_BRIDGE_SEARCH_INDEX` only overrides it) and read anonymously — the server needs no env for the catalog any more. `.claude/settings.local.json` (gitignored) may still set the var (harmless). The curator CLI `hpc-bridge-catalog …` still takes the UUID explicitly — pass it literally. Symptom of forgetting: `hpc-bridge-catalog` errors "seed_path required" (the empty var was swallowed as the index arg).
- **A stale local cache can't shadow the registry any more** — but it *can* still serve an id the registry doesn't know. If a BYO facility behaves oddly, look at `~/.hpc-bridge/facilities.json` (or the `HPC_BRIDGE_STATE_DIR` you set).
- **The login gate runs first in `connect_facility`** — before the catalog read. If you ever build the SDK `Client` earlier (e.g. for a new tool), use `Client(do_version_check=False)` and never call anything that can prompt: the SDK's own command-line login writes a URL to stdout and reads stdin — the MCP transport.
- **Keep `HPC_BRIDGE_STATE_DIR` short.** ssh checks the whole expanded `ControlPath` against the Unix socket cap (~104 bytes on macOS); a deep temp dir failed every SSH with `ControlPath too long`. `_short_control_dir` falls back to `~/.hpc-bridge/cm` / `/tmp/hpcb-cm-<uid>`, and the error is explained if even that is too long.
- **A MEP attach says nothing about your access.** Attaching is identity-blind; the first billed submit is where an unmapped identity fails — a terminal `down` saying `NO ACCOUNT`. Don't read a clean `connect_facility` as "I have an account here" (the notice and the skill say so now).
- **`#39` registration-lag** is real and now *visible* (the `first_details_connect_succeeds` reported invariant): the first `connect_facility(details=…)` on a fresh bootstrap often returns `failed` ("could not find endpoint … in list output") and the retry succeeds. It's reported, not gating. Don't mistake it for a regression.
- **Cluster contention** silently fails billed agentic scenarios (block never scheduled → `compute_ran`/`ends_with_stop`/world checks break together). Check `sinfo` / `squeue` before trusting a billed-scenario failure; use `sacct -u <pool-user>` to see whether a block ran and got CANCELLED. And a **globus1 sshd outage** makes every world check `UNVERIFIABLE` — not a leak.
- **Two harness cells under ONE Globus identity collide**: the web service answers the second with `RESOURCE_CONFLICT` on every submit. Scenarios that bind an identity (`mep_no_account`, `stranger_mep_walk`) are `SERIAL`.
- **Seed `aliases` are not indexed** — `ingest.py` drops them; at runtime use the `id` (`globus-labs`) as `list_facilities` shows. `globus-cluster-mep` won't resolve as a facility arg.
- Commit trailer and PR footer: see `CLAUDE.md`. `agentic/.env` is gitignored — never commit secrets.
- **Bumping the version in `pyproject.toml` needs a `uv lock` in the same commit**, or CI's "lockfile in step" check fails (#153).
- **A stacked PR closes when its base branch is deleted on merge** and cannot be reopened or retargeted afterwards (#152 → #154): retarget the child to `main` before merging the parent.
- **`gh pr merge` from an agent session is refused** by the auto-mode classifier as a merge without review; the maintainer runs merges.

## The server split (2026-09-03, done)

`server.py` was 2276 lines carrying six responsibilities; it is now ≈820 lines of FastMCP app, lifespan, tool wrappers and orchestration seams, with the rest in leaf modules — `context` (runtime data), `config` (env accessors + tunables), `notices` (every agent-facing text), `cost` (spend clock), `binding` (facility/catalog construction), `scheduler_ops` (block release + pilot status, login-shape runner injected), `warmth` (the state machine + task handles), `login_gate`, `connect`. **Patch-target rule:** tests patch names on the OWNING module (`binding.make_catalog`, `warmth._provision`, `connect.discover_facility_details`, `config._control_settings`, `scheduler_ops._release_blocks_over_login`), never on `server` — `server` re-exports for imports only. Each step was its own CI-green PR (#58–#65 + this one); the plan and rationale are in the vault's `Reference/Review 2026-09-03 — code quality.md` §1.

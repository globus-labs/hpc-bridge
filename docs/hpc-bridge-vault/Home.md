# hpc-bridge — Vault Home

**What this is.** A maintainer's map of hpc-bridge: a Claude Code plugin (FastMCP server) that makes a batch supercomputer feel like a REPL. It reaches a facility one of two ways — stand up (or reuse) a *personal* Globus Compute endpoint on the login node over **one** SSH, or attach to a facility-run **multi-user endpoint with zero SSH** — then dispatches shell work to a warm compute block over Globus Compute's credential-free AMQP path. The one credential it needs is a Globus login, obtained in-terminal.

This vault has two halves:
- **Implemented (ground truth)** — how the code works *today*. It tracks the codebase; update it when the code changes. ← you are here.
- **Planned (transient)** — designed work and refactors; churns as features land. *(phase 2 — will absorb `docs/design/`.)*

> [!info] Reading order
> New here? Read **[[Happy path]]** — a first-time user's path (no config → `list_facilities` → Globus login → attach or bootstrap → run → stop) — then the concepts ([[Two-channel architecture]] → [[Standing up the endpoint]] → [[MEP & templated endpoints]] → [[Facility catalog]]). Then the three seams that shape a new user's first minute: [[login]] (the in-terminal Globus login), [[facility-mep]] (zero-SSH facility endpoints), [[discovery]] (an un-indexed facility). Contributing? Read the [[Vault style guide]] first. Where things stand *today*: `HANDOFF.md` at the repo root and [[V1 release]]. Future direction: [[Cross-harness portability]] (running hpc-bridge beyond Claude Code).

## Concepts — how it works
- [[Two-channel architecture]] — SSH control plane vs AMQP hot path (a facility MEP needs no control plane at all)
- [[Standing up the endpoint]] — bootstrap on a login node · SSH-once · reuse · the first-contact failures a newcomer sees
- [[MEP & templated endpoints]] — manager + per-task UEP template → scheduler block (Slurm/PBS) → compute node; the two senses of "MEP"
- [[Credential seeding]] — why we ship a trimmed `storage.db` (and where the login it trims now comes from)
- [[Warmth, the canary & cold-start]] — what "up" really means; the terminal failures the canary surfaces
- [[Resource shapes & the spend floor]] — `login` vs `compute`; `confirm_spend`; compute-only facilities
- [[Session continuity]] — the cwd/env shim
- [[Cost control]] — idle-release · spend clock · budget gate · draining-only stop on a MEP
- [[Discovery today]] — login-shape probe · the registry · the local cache
- [Resolution ladder](assets/discovery-resolution-ladder.html) *(interactive diagram)* — how `connect_facility` resolves: the Globus login gate first, then session → **registry** → local cache → probe, and where the agent deviates
- [[Facility catalog]] — the public registry (a Globus Search index, read anonymously) → `MachineProfile` or `MEPFacility`; `list_facilities` / `connect_facility`; the resolution precedence

## Modules — `src/hpc_bridge/`
**Server & runtime:** [[server]] · [[login]] · [[login_flow_manager]] · [[runner]] · [[dispatch]] · [[lifecycle]] · [[session_shell]] · [[cost]] · [[models]] · [[profile]] · [[shapes]] · [[discovery]]
**Facility seam:** [[facility-base]] · [[facility-local]] · [[facility-remote]] · [[facility-mep]]
**Bootstrap & state:** [[endpoint]] · [[credentials]] · [[state]]
*(The `catalog/` package — `entry` · `search` · `bundled` · `ingest` · `parsers` — is documented in [[Facility catalog]].)*

## Reference
- [[The MCP tools]] — the agent-facing surface (eleven tools)
- [[Plugin packaging]] — `.mcp.json` · `plugin.json` · the `driving-hpc` skill · `hpc-connect` · the fresh-user script
- [[Configuration]] — environment variables
- [[Using hpc-bridge with hermes-agent]] — drive hpc-bridge from hermes-agent on ALCF (or any model); the user recipe + config gotchas
- [[Cross-harness study - gpt-oss-120b vs Claude]] — the first weaker-model study (2026-09-06) and its confound analysis: the OPERATOR dominated the model signal; why those interactive numbers are not a model comparison
- [[Cross-harness benchmark - sonnet-4.6 core pair 2026-09-09]] — **the harness-axis result**: claude-sonnet-4.6 through hermes vs through Claude Code (both over ACP, same client/sim/graders/guidance): 30/30, identical dialogue shape and call counts; process metrics, validity notes, next steps
- [[MEP facilities survey]] — which real facilities run a Globus Compute MEP: registry candidates, their template keys, the unmapped-identity behaviour (2026-09-03)
- [[Model sweep 2026-09-03]] — the cheap-tier agentic model sweep (six new-user scenarios × Opus/Sonnet/Haiku): 29/30 on round 2; what the failures actually were
- [[Model sweep 2026-09-03 block tier]] — the block-tier sweep: stranger's MEP walk 6/6 on all three models; node starvation → the idle-node gate (#69); the human-sim fixes (#70); #39 fires on every SSH bring-up
- [[Security review 2026-09-04]] — the pre-beta security review (three surfaces): the host-trust gap and the host-key decision, the registry governance decision, every finding and what was done
- Code reviews 2026-09-03 — round 1: [[Review 2026-09-03 — bugs]], [[Review 2026-09-03 — code quality]], [[Review 2026-09-03 — dependencies]]; round 2: [[Review 2 2026-09-03 — bugs]], [[Review 2 2026-09-03 — static analysis]], [[Review 2 2026-09-03 — code quality]]

## Planned — design notes (core built; deferred extras remain)
- [[V1 release]] — **the plan of record** for the sprint (scope, tiers, what's ticked); reorient here when a task runs long
- [[ACP interactive benchmark driver]] — the cross-harness benchmark's plan of record: the agent-agnostic ACP driver, the human-sim turn policy, trace sources per harness, the Claude-Code-over-ACP operator, the refined objective (like models through a variety of harnesses) and the ordered next steps
- [[Cross-harness portability]] — running hpc-bridge beyond Claude Code: what is host-neutral, the two portability levers (guidance delivery, the server-side spend gate), the harness matrix
- [[In-terminal Globus login]] — design + live findings for the login that is now built ([[login]]; Tier-2 B, [#48](https://github.com/ryanchard/hpc-bridge/issues/48))
- [[Endpoint reuse and MEP integration]] — the zero-SSH ladder: Phase 1 (reuse our own, [#20](https://github.com/ryanchard/hpc-bridge/issues/20)) and Phase 2 M1 (facility MEPs, [#41](https://github.com/ryanchard/hpc-bridge/issues/41)) both **shipped**; M2 (consent) deferred to V1.x; plus the live record of the no-account failure
- [[Discovery channel model]] — the target model: channels, the provide-vs-discover matrix, the principles. Remaining: per-channel ablation flags + the resolution trace ([#7](https://github.com/ryanchard/hpc-bridge/issues/7))
- [[Globus index discovery channel]] — the catalog resolver + agentic selection + the raw-SSH discover-then-confirm sweep (all built); the 2026-09-03 flip to the anonymous public registry. Remaining: seed-emission / write-back for curation ([#7](https://github.com/ryanchard/hpc-bridge/issues/7))
- [[MFA and interactive SSH auth]] — Duo/MFA + password facilities without the agent ever handling a secret: the pre-open ControlMaster hand-off (`needs_preauth`, **built**) + the non-secret push relay (deferred) ([#3](https://github.com/ryanchard/hpc-bridge/issues/3))
- [[Aurora (PBS + bastion) bring-up]] — the first PBS + bastion/MFA facility: two-hop ProxyJump, the management-hostname pin fix, the discovered `hsn0`/`filesystems=home:flare` config. SSH/PBS path proven live; compute block validated-pending an Aurora allocation
- [[New-user testing (clean-session)]] — `agentic/clean-session.sh` (a pristine Claude Code session) and `scripts/fresh_user_session.sh` (a pristine *Globus* user: no tokens, no cache) — the host-side counterparts to the Docker harness
- [[Agentic testing - Plan B (runtime sandbox)]] — the live-agent regression harness (`agentic/`): the jail, invariants, scenarios, pool isolation, the fake cluster; with [[Agentic testing - Plan A (cluster cost accounting)]] (cluster side) and [[Agentic testing - Plan C (human-in-the-loop)]] (the simulated user)

*(Persistent SSH / ControlMaster shipped — see [[facility-remote]].)*

## Meta
- [[Vault style guide]] — how to write & maintain these notes (for contributors and agents)
- [[Demos]] — archived, version-stamped demos from older conceptualizations

---
> [!note] Status
> **Section 1 (implemented) is complete** — all Concept, Module, and Reference notes track the codebase as of the 2026-09-03 audit (`main` after [#50](https://github.com/ryanchard/hpc-bridge/issues/50): in-terminal login, public registry, stranger's-walk fixes), with [[Happy path]] as the end-to-end spine. **Section 2 (Planned)** carries the design records for what shipped and the plan of record ([[V1 release]]); the rest of `docs/design/` is absorbed as work proceeds.

> [!note] The server split (2026-09-03)
> `server.py` is now the FastMCP app + orchestration only; the runtime lives in [[context]], [[config]], [[notices]], [[cost]], [[binding]], [[scheduler_ops]], [[warmth]], [[login_gate]] and [[connect]] — see [[server]] for the map and [[Review 2026-09-03 — code quality]] §1 for the plan that produced it.

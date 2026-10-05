# New-user testing (clean-session)

> [!abstract] In one line
> `agentic/clean-session.sh` launches a **pristine** Claude Code session — no `~/.claude` priors and an isolated hpc-bridge state sandbox, loading ONLY the hpc-bridge plugin — so interactive testing reflects what a **brand-new user** gets, not the maintainer's accumulated memory/cache.

## Why

A normal `claude --plugin-dir .` inherits the maintainer's [[Home|auto-memory]], `~/.claude/CLAUDE.md`, global rules, other plugins, and — crucially — `~/.hpc-bridge/facilities.json` (a leftover facility cache). Those priors mask real new-user behaviour. One live symptom: with a leftover `globus1` cache entry **and** a forced `HPC_BRIDGE_SSH_HOST=aurora`, a cold agent connected to "globus1" but SSH'd to Aurora and concluded *"globus1 is Aurora"* — a config-from-X / SSH-to-Y confusion ([#35](https://github.com/globus-labs/hpc-bridge/issues/35)).

## What it isolates

| Layer | Mechanism | Effect |
|---|---|---|
| Claude config | throwaway `CLAUDE_CONFIG_DIR` (`mktemp`) | no auto-memory / CLAUDE.md / rules / history / other plugins |
| hpc-bridge state | isolated `HPC_BRIDGE_STATE_DIR` sandbox | no leaked facility cache; caching still works *within* the sandbox |
| shell env | `env -i` + subscription token | no stray `ANTHROPIC_API_KEY` outranks the token |
| **nothing forced** | no `HPC_BRIDGE_SSH_HOST` | the agent picks the facility; discovery/caching drive it ([#35](https://github.com/globus-labs/hpc-bridge/issues/35)) |

The wrapper opens **no SSH connection itself** — the agent connects only when you ask it to, and on an MFA facility it returns a `needs_preauth` `ssh -fN …` command for *you* to run once (the authentic flow). Any ControlMaster you open — that `needs_preauth` command, or a normal `ssh <host>` — is **shared** with the sandbox: its `cm/` is symlinked to `~/.hpc-bridge/cm`, since an SSH master is auth transport, not a hpc-bridge prior. Only the facility *cache* is isolated. `HPCB_CLEAN_FRESH=1` wipes the sandbox cache for a genuine first-connect (discovery) test. Auth uses the Claude **subscription** token (`CLAUDE_CODE_OAUTH_TOKEN`), which authenticates a fresh config dir without touching `~/.claude`.

> [!note] Distinct from the Docker jail
> This is the **interactive, MFA-capable** counterpart to the [[Agentic testing - Plan B (runtime sandbox)|Docker harness]] (`run_smoke.sh`): the jail has no `~/.ssh/config` (so it can't do a bastion/MFA two-hop) and runs the agent headless; clean-session runs on the host so the ControlMaster + `~/.ssh/config` work — the only way to drive a facility like [[Aurora (PBS + bastion) bring-up|Aurora]] by hand.

## The fresh *Globus* user — `scripts/fresh_user_session.sh` (2026-09-03)

`clean-session.sh` isolates Claude's priors and hpc-bridge's cache but keeps your `~/.globus_compute` login. The stranger's walk ([[V1 release]] Tier 2) needed the other half: **no Globus tokens at all.** `scripts/fresh_user_session.sh` launches Claude Code with the plugin loaded (`--plugin-dir`) *from outside the repo* (so the project `.mcp.json` / `.claude/settings.local.json` don't apply — the plugin is the only hpc-bridge, exactly as installed), with the Compute SDK pointed at a scratch `GLOBUS_COMPUTE_USER_DIR` and hpc-bridge at a scratch `HPC_BRIDGE_STATE_DIR` under `$FRESH` (default `~/hpcb-fresh`), and every `HPC_BRIDGE_*` override from your shell **stripped** — including `HPC_BRIDGE_SEARCH_INDEX`, so the built-in registry is what gets exercised (`REGISTRY=<uuid>` opts back in). First run ⇒ `needs_login` (the browser flow, [[login]]); a second run with the same dirs ⇒ no login (refresh tokens); `--reset` wipes the dirs for a brand-new user again. Say "connect me to globus1" and you are walking the [[Happy path]] as a stranger.

The same idea without an agent: **`agentic/mep_no_account_check.py`** logs in (in a scratch dir, `~/hpcb-noaccount`) as an identity the facility does *not* map, refuses to continue if the *effective* identity is one of `--not <username>` (the MEP mapper tries the whole linked-identity set, so a linked mapped identity would silently succeed), attaches, fires the first submit twice, and prints the raw error + hpc-bridge's verdict — PASS = a stable, terminal `down` saying NO ACCOUNT and naming the identity ([[facility-mep]]). The record of what it found is in [[Endpoint reuse and MEP integration]].

## See also
[[Aurora (PBS + bastion) bring-up]] · [[MFA and interactive SSH auth]] · [[Configuration]] · [[Agentic testing - Plan B (runtime sandbox)]] · [[login]] · [[Plugin packaging]] · [[Home]]

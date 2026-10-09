# Using hpc-bridge with hermes-agent

> [!note] Superseded for users (2026-10-09)
> The tested, current recipe for Hermes (v0.21.6), Codex and Pi is **`docs/user/other-hosts.md`**. This page keeps
> the 2026-09 history (hermes 0.21.0 from git, the ALCF model block); the cross-harness test operators are described in
> [[Hackathon readiness 2026-10-09]].

> [!abstract] In one line
> hpc-bridge is a standard MCP server, so any MCP host can drive it — not just Claude Code. This is the recipe for **NousResearch hermes-agent**, including using **ALCF's inference service** (or any model) as the operator, verified live 2026-09-05 (gpt-oss-120b on ALCF called `list_facilities` and got back `delta, globus-labs, anvil, expanse`). See [[Cross-harness portability]] for the design behind this.

## Prerequisites

- **hermes-agent** installed (`curl -fsSL https://hermes-agent.nousresearch.com/install.sh | bash`; `hermes` lands on `~/.local/bin`).
- **A model.** Any OpenAI- or Anthropic-compatible provider hermes supports works. The Claude Pro/Max **subscription may not be used in hermes** (Anthropic restricts the OAuth token to Claude Code / Claude.ai, April 2026) — use an API key, or a facility inference service. This guide uses **ALCF** (Globus-authed, OpenAI-compatible) — see below.
- **uv** (hermes bundles one at `~/.hermes/bin/uv`; hpc-bridge is launched through it).

## 1. Point hermes at a model

For ALCF's inference service, the working `~/.hermes/config.yaml` model block (set the last three via `hermes config set`, not a hand-edit — see gotchas):

```yaml
model:
  provider: custom
  base_url: https://inference-api.alcf.anl.gov/resource_server/sophia/vllm/v1
  api_key: ${ALCF_INFERENCE_TOKEN}   # env interpolation; hermes' inline key_env is NOT honored
  default: openai/gpt-oss-120b
  streaming: false                   # ALCF's gateway SSE isn't clean → 'empty stream' without this
  context_length: 131072             # ALCF 404s on /v1/models, so auto-detect fails; set explicitly
  max_tokens: 4096
```

ALCF access tokens last 24 h. Use the repo's **`scripts/hermes-alcf`** launcher (symlink it onto your PATH as `hermes-alcf`) — it mints a fresh token into `ALCF_INFERENCE_TOKEN` and execs `hermes`, so the expiry is invisible. One-time: `uv run --directory <repo> --extra integration python agentic/harness/inference_auth_token.py authenticate`.

> [!warning] hermes config gotchas (ALCF / any custom OpenAI-compatible endpoint)
> - `api_key: ${VAR}` (interpolation) — the inline `key_env:` was ignored, causing a 401.
> - `streaming: false` — else 'Provider returned an empty stream'.
> - `context_length` + `max_tokens` **via `hermes config set model.context_length 131072`** — a raw hand-edit didn't take; because ALCF 404s on `/v1/models`, hermes can't auto-detect the window and reports 'Context length exceeded (N tokens)'.

## 2. Add hpc-bridge as an MCP server

hermes does **not** inherit your shell environment for stdio MCP servers — only an explicit `env` allowlist plus a safe baseline — so list what hpc-bridge needs. Until hpc-bridge is on PyPI, launch it from the repo via `uv run`; afterwards this becomes `--command uvx --args hpc-bridge`.

```bash
hermes mcp add hpc-bridge \
  --command "$(command -v uv)" \
  --connect-timeout 180 \
  --env HOME=$HOME HPC_BRIDGE_USER_DIR=$HOME/.hpc-bridge/hermes \
  --args run --directory /path/to/hpc-bridge --extra integration hpc-bridge
# answer 'y' to enable all 12 tools; `hermes mcp list` to confirm
```

For driving a **real facility** (not just `list_facilities`, which is unauthenticated), hpc-bridge also needs your Globus login (`~/.globus_compute/storage.db`) and SSH config (`~/.ssh`) — both under `$HOME`, so the `HOME` allowlist entry covers them. Add `HPC_BRIDGE_SEARCH_INDEX=<index>` to the `env` only if you use a non-default registry.

## 3. Verify

```bash
hermes mcp test hpc-bridge     # should discover 12 tools
hermes-alcf -z "Call the hpc-bridge list_facilities tool and list the facility ids."
```

Expected: the operator model calls `list_facilities` and reports the registry entries. This confirms the wiring without any facility or credentials.

## Known gap — the operational guidance

Claude Code auto-injects hpc-bridge's [[driving-hpc|SKILL.md]] (the select → discover → gate → provision → wait orchestration, the compute-only-MEP rules, stop semantics). **hermes does not get this yet** — it sees the tools and their docstrings only. For `list_facilities` that's fine; for provisioning and the spend gate it matters. Until hpc-bridge ships the guidance over MCP (`instructions=` / `@mcp.resource`, see [[Cross-harness portability]]), a hermes user should paste the SKILL.md content into a `HERMES.md` or a hermes Skill. Closing this gap is the next step toward first-class hermes support.

## See also

- [[Cross-harness portability]] — the design: what's host-neutral, the abstraction, the phased plan
- [[driving-hpc]] — the guidance that needs a host-agnostic channel
- [[The MCP tools]] — the twelve tools hermes discovers

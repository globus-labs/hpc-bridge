# Cross-harness portability

> [!abstract] In one line
> hpc-bridge's core is already a host-neutral MCP server, so making it work beyond Claude Code — Claude Desktop, OpenAI's Agents SDK, NousResearch's hermes-agent, pi.dev's Pi — is mostly packaging plus giving the [[driving-hpc|SKILL.md guidance]] a host-agnostic delivery channel. The spend gate is already server-enforced, so it rides every host's own approval UI unchanged. Investigation run 2026-09-05 (a 7-agent workflow); this note is the design record.

## The finding

The [[server|MCP server]] is a plain `FastMCP("endpoint", lifespan=lifespan)` speaking stdio JSON-RPC, with **zero Anthropic-specific code** in its dependency graph (`mcp`, `pydantic`, `pyyaml`, `globus-sdk`, `globus-compute-sdk`) or in its twelve `@mcp.tool()` registrations. It is launched by the `hpc-bridge` console script, so `uvx hpc-bridge` starts it identically to how [[Plugin packaging|.mcp.json]] does. Any standard MCP client can spawn it.

Everything Claude-Code-specific is **install chrome** that translates or drops per host with no server change:

| surface | file | maps to elsewhere |
|---|---|---|
| plugin manifest | `.claude-plugin/plugin.json` | nothing — a non-CC host points its own MCP config at the console script |
| launch stanza | `.mcp.json` (`${CLAUDE_PLUGIN_ROOT}`/`${CLAUDE_PLUGIN_DATA}`) | the host's own `mcpServers` entry with the vars resolved to literal paths |
| PATH shim | `bin/run-with-uv` | unneeded on any host that launches subprocesses with a normal login PATH; it also picks an x64 Python on Windows on Arm (no `cryptography` wheel there), which a `uvx` host does with `--python cpython-3.13-windows-x86_64-none` |
| slash command | `commands/hpc-connect.md` | a system-prompt fragment, or an `@mcp.prompt` (the server defines none today) |
| credential guard | `hooks/hooks.json` + `credential-guard.sh` | the host's own middleware, or nothing — better as an in-process check in `run_shell` |
| standing guidance | `skills/driving-hpc/SKILL.md` | `instructions=` / `@mcp.resource` / tool docstrings (see below) |

Only **two levers** actually gate portability:

1. **The guidance.** [[driving-hpc|SKILL.md]]'s ~93 lines of cross-tool orchestration (select → discover → gate → provision → wait; the compute-only-MEP exception set; the don't-detach-long-jobs rule [#21]; stop's draining-vs-down semantics) have **no MCP-native distribution today** — a grep confirms zero `@mcp.prompt`, `@mcp.resource`, or `instructions=` anywhere in `src/`. None of it lives in individual tool docstrings either.
2. **The spend gate.** Already enforced **server-side** ([[Resource shapes & the spend floor|confirm_spend]]): a billed shape without `confirm_spend=True` returns `needs_confirmation` and starts nothing. This is protocol-independent, so every host's generic "approve this tool call" surface serves as the human-in-the-loop ask with **no server change**.

## Portability matrix

> [!note] Effort assumes the core stays an unmodified MCP server.

| harness | MCP | guidance channel | gating | effort | verdict |
|---|---|---|---|---|---|
| **Claude Code** (baseline) | native stdio via `.mcp.json` | CC Skill auto-injects SKILL.md | server floor + PreToolUse hook | — | reference impl (0.1.14, beta) |
| **OpenAI Agents SDK** (self-hosted) | `MCPServerStdio` spawns it unchanged | `Agent.instructions` string only | server floor (+ optional `needs_approval`) | **low** | cheapest true non-Anthropic port |
| **Claude Desktop** | native; manual `claude_desktop_config.json` today, `.mcpb` later | no atomic bundle; tool descriptions + result text | server floor + Desktop Auto/On-demand approve | **medium** | works today by hand; `.mcpb` a nicety |
| **hermes-agent** (NousResearch) | native, stdio + HTTP; elicitation | native Skills system + `HERMES.md`/`CLAUDE.md` | docstring convention today; elicitation possible | **low** | best fit on paper — but env/SSH passthrough unproven |
| **pi.dev Pi Coding Agent** | third-party adapters only; no native | per-tool `promptSnippet` + separate skills | **no built-in approval gate** | **medium** | plausible but unofficial; needs a spike |
| **OpenAI hosted** (ChatGPT/Codex/Responses) | hosted `mcp` tool is **HTTP-only** | `Agent.instructions` | `require_approval` + callback | **high** | needs a Secure MCP Tunnel sidecar + looser secrets boundary — defer |

## Recommended abstraction — one codebase, all hosts

1. **`instructions=`** — add a compressed always-on core of SKILL.md to `FastMCP("endpoint", instructions=…)`. It is the one MCP-native field most hosts auto-inject into the system prompt.
2. **`@mcp.resource`** — expose the full reference material (`hpcbridge://guidance/operations`) for hosts that render resources, with `instructions=` telling the model to fetch it when unsure.
3. **Docstrings + result text** — push every individually load-bearing warning (spend-floor discipline, [[Endpoint reuse and MEP integration|endpoint-naming collisions]], the [#21] idle-release rule) into the specific tool's docstring and what it says back. This is the **only channel every surveyed host guarantees**.
4. **Keep `confirm_spend` server-side.** Do not move it onto MCP elicitation (which is in active protocol churn); optionally add an elicitation path later behind a capability probe, as a strict enhancement that falls back to the current return-and-recall convention.
5. **Decouple distribution** — publish `hpc-bridge` to **PyPI** so every host converges on `{"command": "uvx", "args": ["hpc-bridge"]}`, and author a spec-native `server.json`. Keep `.claude-plugin/*` + `.mcp.json` as a thin CC wrapper around the same console script, never a fork.
6. **Generated guidance exports** — maintain one source guidance doc and generate each host's form (compressed `instructions=`, the resource doc, a hermes-agent Skill) from it, the same anti-drift discipline as the [[Vault style guide|vault]]/HANDOFF split.
7. **Promote the credential guard** from a CC-only hook into an in-process check inside `run_shell`/`login_shell`, so every host gets the same defense-in-depth.

## Phased plan (cheapest, highest-leverage first)

- **Phase 1 — protocol-native guidance + the cheap ports.** Add `instructions=` (compressed core), one or two `@mcp.resource`s, and audit tool docstrings/result text for the load-bearing warnings; publish to PyPI + author `server.json`. Validate end-to-end on the **OpenAI Agents SDK** (`MCPServerStdio`) and **Claude Desktop** (manual config) — both take the unmodified server. → guidance parity with zero per-host code; two non-CC harnesses verified.
- **Phase 2 — Desktop packaging + hermes-agent.** Build a Claude Desktop `.mcpb`; wire `hermes mcp add`, enumerate the full `HPC_BRIDGE_*` env allowlist, and **empirically verify** whether `SSH_AUTH_SOCK`/a ControlMaster path passes through for the [[MFA and interactive SSH auth|MFA bootstrap]] (hermes-agent does **not** inherit ambient shell env for stdio servers). Test the hermes-agent Skills install for the guidance. → one-click Desktop install; hermes-agent's riskiest unknown resolved.
- **Phase 3 — Pi spike.** Connect through an existing third-party pi-mcp adapter against a fake-cluster-tier instance, exercising a long-running `run_shell` + `poll_task` cycle and the `confirm_spend` round-trip in Pi's interactive TUI (Pi has **no built-in approval gate**, so the relay only works interactively). → an empirical answer on whether an unofficial adapter is production-viable.
- **Phase 4 — OpenAI hosted (demand-gated, not scheduled).** Only if there is named demand for ChatGPT/Codex/Responses specifically: stand up Secure MCP Tunnel's `tunnel-client` as a new always-on sidecar and re-examine the per-call auth trust boundary against the local-creds design.

## Risks

- **`instructions=` is implementation-defined**, not guaranteed — confirmed dropped by some clients (langchain4j, mcporter). The docstring/result-text duplication is the mitigation, not polish.
- **Elicitation/sampling protocol churn** — the 2026-07-28 spec deprecates sampling/roots/logging over 12 months and restructures elicitation; FastMCP's `ctx.elicit()` already raises on 2026-07-28 connections without legacy mode. Any future elicitation work must version-detect. The server-enforced gate sidesteps this entirely.
- **hermes-agent env inheritance** — stdio servers get an explicit allowlist + a fixed baseline only; whether `SSH_AUTH_SOCK`/ControlMaster passes through is unconfirmed. If not, the external-terminal MFA/SSH bootstrap could silently degrade there.
- **Pi's MCP is unofficial** (third-party adapters, an open first-party tracking issue) and it has **zero native approval gate** — `confirm_spend`'s relay silently no-ops under headless/RPC embedding unless deliberately wired.
- **OpenAI hosted** needs public HTTPS (conflicts with local-creds) or the tunnel sidecar, plus a looser trust boundary (bearer tokens transiting OpenAI's infra per call).
- **No host bundles server + guidance atomically** — every non-CC host reintroduces a doc-drift burden across N guidance copies unless the generated-export discipline is actually adopted and owned.
- **Tool-count ceilings** are soft (~30–50 tools before selection quality degrades). hpc-bridge's twelve are safe, but the margin narrows if more guidance gets folded into docstrings — re-check before the tool count grows.

## Open questions

- **Referents confirmed (2026-09-05):** "Hermes" = **NousResearch/hermes-agent** (the runnable harness), "Pi" = **pi.dev's Pi Coding Agent** (earendil-works), not Inflection's pi.ai.
- How much of SKILL.md is truly must-reach-every-host versus nice-to-have-on-request? This sizes how aggressively to compress into `instructions=` (a per-session token cost on every host) versus deferring to `@mcp.resource`.
- Is a Desktop `.mcpb` worth building now, or does hand-editing `claude_desktop_config.json` cover the realistic near-term audience (people already comfortable with SSH/HPC/uv)?
- Pursue official MCP-registry listing now, or is PyPI publication alone (enabling `uvx hpc-bridge` everywhere) the higher-value near-term step, with registry listing deferred until a host ships native registry browsing?
- Who owns keeping the N per-host guidance copies in sync once this expands past Claude Code? The generated-export mechanism needs an owner before the second host ships.

## See also

- [[V1 release]] — the current Claude-Code-plugin sprint (this is the direction *after* V1)
- [[Plugin packaging]] — the CC-specific surfaces this note would translate
- [[driving-hpc]] — the SKILL.md guidance that needs a host-agnostic channel
- [[Resource shapes & the spend floor]] — why the gate is already portable

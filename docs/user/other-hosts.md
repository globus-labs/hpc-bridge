# Other MCP hosts

Claude Code is the simplest way to use hpc-bridge — the [plugin](install.md) carries everything. But hpc-bridge is a
**standard MCP server**, so any MCP host can run it. This page has tested recipes for **Codex**, **Pi** and
**Hermes**, and the general shape for any other host.

## The one command

Every host runs the same stdio command:

```
uvx --python 3.13 --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge
```

The only prerequisite is [`uv`](https://docs.astral.sh/uv/) on your PATH (it fetches Python itself). `--python 3.13`
matters: the facilities in the registry were proven with a Python 3.13 client, and on a facility that builds its
workers to match your Python, a different one is untested. The **first start builds hpc-bridge (about a minute)** —
longer than some hosts wait by default, which is why each recipe below raises a timeout.

What you bring is the same as for Claude Code — a Globus login and access to a facility; see [Install](install.md) and
[Facilities](facilities.md). The Globus login happens on first use: the agent hands you a link to open.

## Give the agent the guidance (all hosts)

hpc-bridge's operating guidance is a skill (`driving-hpc`). Codex, Pi and Hermes all read Agent Skills — install it
once, and the agent knows the spend gate, the waits, and how to stop:

```bash
# Codex and Pi read ~/.agents/skills; Hermes reads ~/.hermes/skills
for d in ~/.agents/skills/driving-hpc ~/.hermes/skills/driving-hpc; do
  mkdir -p "$d" && curl -fsSL https://raw.githubusercontent.com/globus-labs/hpc-bridge/main/skills/driving-hpc/SKILL.md -o "$d/SKILL.md"
done
```

Without it the server still offers the same text as the MCP resource `hpcbridge://guidance/operations`, but most hosts
truncate or hide it — the skill is the reliable path.

## Codex

```bash
codex mcp add hpc-bridge -- uvx --python 3.13 --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge
```

Then add three lines under `[mcp_servers.hpc-bridge]` in `~/.codex/config.toml`:

```toml
startup_timeout_sec = 180                 # the first start builds hpc-bridge; Codex's default is 30 s
tool_timeout_sec = 600
default_tools_approval_mode = "approve"   # see below
```

- **Approvals.** Codex asks before every MCP tool call that is not read-only. Interactively you can approve each one;
  `codex exec` (non-interactive) *refuses* them instead, so set `default_tools_approval_mode = "approve"` there. Spend
  is still gated: hpc-bridge itself refuses to start a billed block until the agent confirms it with you.
- **SSH facilities.** Codex passes the server only a small set of environment variables. If your SSH key lives in an
  agent, add `env_vars = ["SSH_AUTH_SOCK"]` to the same section.

## Pi

```bash
pi mcp add hpc-bridge --exposure direct -- uvx --python 3.13 --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge
```

Then add `"timeout": 300` to the `hpc-bridge` entry in `~/.pi/agent/mcp.json` (Pi's default is 60 s, and the first
start takes about a minute). `pi mcp list` should show `hpc-bridge: connected, 12 tools`.

- **`--exposure direct` matters.** Pi's default exposure (`codemode`) keeps MCP tools away from the model.
- Long calls stay alive: hpc-bridge reports progress every 15 s, and Pi shows it.

## Hermes

```bash
hermes mcp add hpc-bridge --connect-timeout 180 --env 'SSH_AUTH_SOCK=${SSH_AUTH_SOCK}' \
  --command uvx --args --python 3.13 --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge
```

Answer *y* to enable the tools, then `hermes mcp test hpc-bridge` to confirm. `--args` must come last.

- **`--connect-timeout 180`** — Hermes waits 60 s by default; the first start takes about a minute.
- Hermes filters the environment it gives the server; `SSH_AUTH_SOCK` passes an SSH agent through (drop it if you
  don't use one).
- Choosing the model is a Hermes matter (`hermes model`). On a minimal Linux box, Hermes' installer needs
  `libatomic1` (`apt-get install -y libatomic1`) — it says so if missing.

## Any other host

The shape is identical — give the host the same command; only the "add a server" step differs:

| Host | Where the server goes |
|---|---|
| **Claude Desktop** | `claude_desktop_config.json` → `mcpServers` (stdio: the command + args above) |
| **Cursor** | `.cursor/mcp.json` → `mcpServers` |
| **OpenAI Agents SDK** | `MCPServerStdio(command="uvx", args=["--python", "3.13", "--from", "git+https://github.com/globus-labs/hpc-bridge", "hpc-bridge"])` |

Check three things on any host: a **startup/connect timeout of a few minutes** (the first build), a **per-call timeout
of at least 5 minutes** (some calls wait on a scheduler), and that the host **passes `HOME`** to the server (the Globus
login and your SSH config live there).

## Updating

`uvx` keeps the build it made on first use. To pick up a newer hpc-bridge, run the command once with `--refresh`
(`uvx --refresh --python 3.13 --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge`), or restart your host
after `uv cache clean hpc-bridge`. The facility registry needs no update — hpc-bridge reads it live.

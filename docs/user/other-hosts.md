# Other MCP hosts

Claude Code is the simplest way to use hpc-bridge — the [plugin](install.md) carries everything. But hpc-bridge is a
**standard MCP server**, so any MCP host can run it: NousResearch hermes-agent, Claude Desktop, Cursor, the OpenAI
Agents SDK, and others. Each host has its own way to *add a server*, but the command is the same everywhere.

## The one command

Point your host's MCP configuration at this stdio command:

```
uvx --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge
```

The only prerequisite is [`uv`](https://docs.astral.sh/uv/) on your PATH (it fetches Python itself). `uvx` builds and
runs hpc-bridge in its own environment — nothing to clone. *(Once hpc-bridge is published to PyPI this shortens to
`uvx hpc-bridge`.)*

What you bring is the same as for Claude Code — a Globus login and access to a facility; see [Install](install.md) and
[Facilities](facilities.md). The Globus browser login happens on first use, from whichever host you run.

## Guidance comes with it

Claude Code loads a skill that teaches the agent how to drive HPC well. Hosts without a skill system get the same
guidance **over MCP**: the server offers it as the resource `hpcbridge://guidance/operations` and points the agent to
read it before it provisions or spends. There is nothing extra to install — a capable model will consult it on its own,
and each tool's description is the fallback.

## Example: hermes-agent

hermes filters the environment for stdio servers (it does not inherit your shell), so pass what hpc-bridge needs
explicitly — your home directory (for the Globus login and SSH config) and a writable state dir:

```
hermes mcp add hpc-bridge \
  --command uvx \
  --env HOME=$HOME HPC_BRIDGE_USER_DIR=$HOME/.hpc-bridge \
  --args --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge
```

Answer *yes* to enable the tools, then `hermes mcp list` to confirm. Choosing the model hermes runs is a hermes matter
(`hermes model`); if you want to drive it with a facility inference service such as ALCF, the maintainer notes have a
worked setup in the [vault guide](../hpc-bridge-vault/Reference/Using%20hpc-bridge%20with%20hermes-agent.md).

## Any other host

The shape is identical — give the host the same command; only the "add a server" step differs:

| Host | Where the server goes |
|---|---|
| **Claude Desktop** | `claude_desktop_config.json` → `mcpServers` (stdio: the command + args above) |
| **Cursor** | `.cursor/mcp.json` → `mcpServers` |
| **OpenAI Agents SDK** | `MCPServerStdio(command="uvx", args=["--from", "git+https://github.com/globus-labs/hpc-bridge", "hpc-bridge"])` |

If a host filters the environment like hermes does, include `HOME` (and a writable `HPC_BRIDGE_USER_DIR`) in its env
allowlist. Everything after that — facilities, the Globus login, costs and stopping — works the same as in the
[Quickstart](quickstart.md).

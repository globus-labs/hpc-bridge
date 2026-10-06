# Install

Inside Claude Code:

```
/plugin marketplace add globus-labs/hpc-bridge
/plugin install hpc-bridge@hpc-bridge
```

You may see *SSH not configured, cloning via HTTPS* while the marketplace is added: that is GitHub, not your
cluster, and it is fine.

Then ask **"What HPC facilities can I use?"** The agent lists the registered facilities and what each
needs from you; no login, configuration or SSH is involved in that answer. If the agent says it has no
hpc-bridge tools, the server did not start: open `/plugin` and check its **Errors** tab, or `/mcp` for the
server's own output, and see [Troubleshooting](troubleshooting.md#the-agent-has-no-hpc-bridge-tools). The usual
cause is `uv` not being found.

## Before you install

- **Claude Code**, a recent one (2.1.190 or newer; check with `claude --version`, update with
  `brew upgrade claude-code` or `npm install -g @anthropic-ai/claude-code@latest`): the CLI, the desktop app
  or an IDE extension.
- **[`uv`](https://docs.astral.sh/uv/)** on this machine. The plugin starts its server with `uv run`, which
  fetches the server's Python dependencies on first start, and downloads a Python 3.11 or newer for itself
  if none is installed. Install `uv` with `curl -LsSf https://astral.sh/uv/install.sh | sh` (or
  `brew install uv`) if you don't have it. The desktop app and IDE extensions do not see your shell's
  `PATH`, so the plugin looks for `uv` in the usual install locations itself (`~/.local/bin`, Homebrew,
  `~/.cargo/bin`); if yours lives somewhere else, symlink it into `~/.local/bin`.
- **A Globus account.** Log in with the identity your facility knows you by, usually your institutional
  or ACCESS identity; see [Facilities](facilities.md#which-globus-identity-to-use).
- **Access to the facility you want to use.** Either an account there with your Globus identity mapped
  to it (a facility-run endpoint, such as `anvil`, `delta` or `globus-labs`: nothing to set up on the
  machine), or an account plus key-based SSH to its login node (an SSH-bootstrap facility, such as Expanse).
  For SSH, a `Host` block in `~/.ssh/config` is all hpc-bridge reads; it never asks for a password:

  ```
  Host login.expanse.sdsc.edu
      User your-expanse-username
      IdentityFile ~/.ssh/id_ed25519
  ```

  The `Host` pattern must match the login host name hpc-bridge will use: the registry's host for a
  catalogued facility (`login.expanse.sdsc.edu` above), or exactly the name you give the agent for your own
  cluster. A short alias such as `expanse` only works if you also say "connect me to `expanse`".

Nothing is installed on the facility by you. On an SSH facility the first connect installs Globus
Compute into your home directory there; on a facility-run endpoint there is nothing to install at all.

## Updating

```
/plugin marketplace update hpc-bridge
/plugin update hpc-bridge@hpc-bridge
/reload-plugins
```

The plugin is installed for you across all projects by default; `/plugin install hpc-bridge@hpc-bridge
--scope project` shares it with a repository's collaborators instead. The same commands work from a
shell as `claude plugin …`. Adding a marketplace is a trust decision: a plugin runs with your
privileges, and this one is the repository you can read.

## Removing

```
/plugin uninstall hpc-bridge@hpc-bridge
```

That removes the plugin. To remove what it made: ask the agent for a *teardown* first if you stood up an
endpoint on an SSH facility (it removes the endpoint and the Globus token copy from the login node; `~/hpc-bridge/` in your home
there holds its Python environment and can be deleted after), then delete `~/.hpc-bridge/` locally, and
`~/.globus_compute/storage.db` to log out of Globus. The consent you gave can be revoked at
[app.globus.org/settings/consents](https://app.globus.org/settings/consents).

## From a clone (developers)

```bash
git clone https://github.com/globus-labs/hpc-bridge.git
cd hpc-bridge
claude --plugin-dir .            # start Claude Code with this checkout loaded as the plugin
```

`uv run` resolves the server's dependencies on first start, so no separate install step is needed; if the
server fails to start, `/mcp` shows its output.

## Reference

### Where things live on your machine

| What | Where |
|---|---|
| Globus tokens | Globus Compute's standard token store, `~/.globus_compute/storage.db` (mode 600) |
| known facilities, endpoint pins, SSH multiplexing sockets | `~/.hpc-bridge/` (local; note the dot) |
| on an SSH facility: the endpoint's Python environment and its state | `~/hpc-bridge/` and `~/.globus_compute/` in your home *there* |
| the plugin's catalog cache | Claude Code's plugin data directory |

Deleting `~/.hpc-bridge` makes hpc-bridge forget the clusters it discovered itself (registry facilities
are unaffected); deleting the token store logs you out of Globus.

### Optional environment variables

The defaults are right for almost everyone. Set these in the shell that starts Claude Code.

| Variable | Use |
|---|---|
| `HPC_BRIDGE_SSH_USER`, `HPC_BRIDGE_SSH_KEY` | SSH login name and key when `~/.ssh/config` can't provide them |
| `HPC_BRIDGE_CHARGE_FACTOR` | your allocation's service-unit rate per node-hour, so the spend estimate is a real number (default `0`: hpc-bridge does not know your facility's rate, and the estimate reads as unmetered) |
| `HPC_BRIDGE_SYNC_WAIT_S` | how long a command blocks before it becomes a background task you can poll (default `120`) |
| `HPC_BRIDGE_MAX_TASK_S` | a hard cap on one task's runtime (default: the block's walltime) |
| `HPC_BRIDGE_LOGIN_WAIT_S` | how long the login step waits for your browser before handing you the link (default `90`) |
| `HPC_BRIDGE_STATE_DIR` | relocate hpc-bridge's local state (keep the path short: SSH sockets live there) |
| `HPC_BRIDGE_SEARCH_INDEX` | point at a different facility registry (the public one is built in) |

The full reference, including developer and testing knobs, is the vault's
[Configuration](../hpc-bridge-vault/Reference/Configuration.md) page.

### Platforms

- **macOS and Linux** are supported for every path above.
- **Windows** is untested. The SSH-bootstrap path relies on OpenSSH connection multiplexing, which
  Windows OpenSSH does not support; the zero-SSH path may work.
- Running an endpoint *on your own machine* rather than on a facility needs Linux (the Globus Compute
  endpoint daemon is Linux-only). Normal use never needs this.

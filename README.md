# hpc-bridge

**Drive a supercomputer from Claude Code.** Ask for a compute node in plain language; the agent finds the
facility, logs you in to Globus once, starts a one-node scheduler job (a *block*, and it asks before it spends
anything), runs your commands on that node, and releases it when you are done.

**Seven minutes on video:** what hpc-bridge is, why an agent should sit outside the facility, and a live run on a cluster.

[![hpc-bridge in seven minutes (YouTube)](https://img.youtube.com/vi/uw9n9v657c8/hqdefault.jpg)](https://www.youtube.com/watch?v=uw9n9v657c8)

## Install

Two commands inside Claude Code; the repository is its own plugin marketplace.

```
/plugin marketplace add globus-labs/hpc-bridge
/plugin install hpc-bridge@hpc-bridge
```

Then ask *"What HPC facilities can I use?"* — that first answer needs no login and no configuration.

**Another MCP host?** hpc-bridge is a standard MCP server — point hermes-agent, Claude Desktop, Cursor, or the OpenAI Agents SDK at `uvx --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge`. See **[other MCP hosts](docs/user/other-hosts.md)**.

## What you bring, what's included

| You bring | Included |
|---|---|
| **Claude Code** | the plugin: the tools, and a skill the agent follows on its own, so plain requests work |
| **[`uv`](https://docs.astral.sh/uv/)**, the only local prerequisite (it fetches Python 3.11 or newer itself if you don't have one) | the **public facility registry**: which machines exist and what each needs from you |
| **A Globus account**, logged in with the identity your facility knows you by (usually institutional or ACCESS) | the **Globus login in your terminal**: the browser opens once, the token is remembered |
| **Access to a facility**: an account whose Globus identity it has mapped (zero SSH), or an account plus key-based SSH to its login node in `~/.ssh/config` | the **endpoint bootstrap**: on an SSH facility hpc-bridge installs Globus Compute in your home directory there, then reconnects with no SSH — nothing for you to install on the machine |
| For an un-catalogued cluster, a minute to confirm the settings the agent discovers | **discovery** of that cluster's configuration, the **spend gate** before any billed block, and **idle self-release** |

Nothing hpc-bridge-specific is configured by hand. Details: **[Install](docs/user/install.md)** · **[Facilities](docs/user/facilities.md)**.

## Your first session

1. *What HPC facilities can I use?*
2. *Connect me to Anvil* — a facility from step 1, or *the cluster at `login.example.edu`* for one that isn't listed
3. *Run `hostname` on a compute node*
4. *Release the compute block*

The connect opens your browser for Globus; approve once. Before the first command the agent shows the
partition and account it will charge and asks you to confirm. Allocating a block takes a couple of minutes
on a quiet cluster; after that, commands run immediately and your working directory persists between them.
The **[Quickstart](docs/user/quickstart.md)** walks through each step and what to expect.

## How it works

SSH is a one-time bootstrap; everything after rides a Globus Compute endpoint. The connect step resolves a
machine from the registry, from a cluster you used before, or by probing a new login node, and both
discovery and compute then flow through the endpoint over Globus. A facility that runs its own multi-user
endpoint needs no SSH at all. Design, tool reference and module notes: the **[vault](docs/hpc-bridge-vault/Home.md)**.

## Docs

- **[User guide](docs/user/README.md)** — install, quickstart, facilities, the Globus login, costs and stopping, troubleshooting.
- **[Adding a facility to the registry](docs/adding-a-facility.md)** — for facility staff and contributors.
- **[Vault](docs/hpc-bridge-vault/Home.md)** — how it is built.
- **Bugs and questions:** [GitHub issues](https://github.com/globus-labs/hpc-bridge/issues).

## Status and security

**Public beta** (0.1.6, Apache-2.0): tested live on six clusters, security-reviewed, and now in the hands
of test users — expect rough edges and please [report them](https://github.com/globus-labs/hpc-bridge/issues). In the
registry: **Purdue Anvil** (multi-user endpoint), **NCSA Delta** (multi-user endpoint), **SDSC Expanse**
(SSH with a one-time code) and Globus Labs' cluster; also proven live through the bring-your-own path on **Midway**
(Slurm) and **ALCF Polaris** (PBS). Slurm and PBS are the supported schedulers.
Unit tests, lint and type checks run in CI. Every command after the bootstrap carries only a scoped Globus token,
never SSH material; SSH is key-only and used once to bootstrap. A facility that asks for a one-time code
(TOTP or Duo passcode) at login gets it from you in the chat, single-use; a password is never handled by the agent:
it hands you an `ssh` command for your own terminal and shares that session. The agent runs as you on the login
node, so treat what it runs there as you would your own shell.

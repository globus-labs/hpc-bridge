# hpc-bridge — user guide

hpc-bridge lets Claude Code work on a supercomputer for you. You ask in plain language; the agent
finds the facility, logs you in to Globus from the terminal, brings up a compute block (asking before
it spends anything), runs your commands on a node, and releases the block when you are done.

Prefer to watch first? [hpc-bridge in seven minutes](https://www.youtube.com/watch?v=uw9n9v657c8) — the idea, the design, and a live run.

| Page | Read it when |
|---|---|
| [Install](install.md) | you are setting it up: the two commands, prerequisites, updating |
| [Other MCP hosts](other-hosts.md) | using hpc-bridge outside Claude Code: hermes-agent, Claude Desktop, Cursor, the OpenAI Agents SDK |
| [Quickstart](quickstart.md) | your first session, step by step, and what to expect |
| [Facilities](facilities.md) | choosing a machine: what each facility needs from you |
| [The Globus login](login.md) | the one credential hpc-bridge needs, and why your browser opens |
| [Batches, jobs and environments](batches-jobs-envs.md) | running many inputs on one block, work that outlives the session, and the software it runs in |
| [Costs and stopping](costs-and-stopping.md) | how spend is gated, what a block and a job cost, what "stop" means, long-running work |
| [Troubleshooting](troubleshooting.md) | what a refusal or a stuck state means and what to do |
| [Trusting the facility](trust.md) | connecting to a cluster you don't fully trust: planted instructions in its output |

**Three ways a facility is reached**, decided for you from the registry: a facility-run endpoint (zero SSH;
you need an account there with your Globus identity mapped), an SSH bootstrap (an account plus key-based SSH
to the login node), or your own un-catalogued cluster (the same, plus a minute to confirm what the agent
discovers). [Facilities](facilities.md) has the table and the registered machines.

## What you bring, what's included

| You bring | Included |
|---|---|
| **Claude Code**, or [another MCP host](other-hosts.md) | the plugin: the tools, and a skill the agent follows on its own, so plain requests work |
| **[`uv`](https://docs.astral.sh/uv/)**, the only local prerequisite (it fetches Python 3.11 or newer itself if you don't have one) | the **public facility registry**: which machines exist and what each needs from you |
| **A Globus account**, logged in with the identity your facility knows you by (usually institutional or ACCESS) | the **Globus login in your terminal**: the browser opens once, and the token is remembered |
| **Access to a facility**: an account whose Globus identity it has mapped (zero SSH), or an account plus key-based SSH to its login node in `~/.ssh/config` | the **endpoint setup**: on an SSH facility hpc-bridge installs Globus Compute in your home directory there, then reconnects with no SSH; nothing for you to install on the machine |
| For a cluster that isn't in the registry, a minute to confirm the settings the agent discovers | **discovery** of that cluster's settings, the **spend gate** before anything billed, and **idle self-release** |

Nothing hpc-bridge-specific is configured by hand. [Install](install.md) has each prerequisite in full.

**What it is not, yet:** it moves command output, not files (use Globus Transfer or `scp` for results);
Slurm and PBS are the supported schedulers.

The developer documentation (design, internals, the tool reference) lives in the
[vault](../hpc-bridge-vault/Home.md).

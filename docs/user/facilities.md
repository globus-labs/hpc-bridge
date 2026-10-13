# Facilities

Asking *What HPC facilities can I use?* answers from a public registry. Each entry says how it is
reached, and that decides what you need before you connect.

A registry entry is trusted configuration: for an SSH facility it names the login host hpc-bridge will
connect to as you and the shell line it runs there to set up the endpoint. Entries are curated by review
in this repository (see [Adding a facility](../adding-a-facility.md)), and on first contact the agent
shows you the host and that line. hpc-bridge only ever connects to a host your own `ssh` already trusts:
an unknown host key is refused with **"UNKNOWN HOST KEY"** until you have connected to it once yourself.

## What each access kind needs from you

| Access | You need | hpc-bridge does |
|---|---|---|
| **Facility-run endpoint** (a Globus Compute *multi-user endpoint*, "MEP", run by the facility; zero SSH) | an account at the facility with your Globus identity mapped to it | attaches to the facility's endpoint; the first block start tests your mapping |
| **SSH bootstrap** | an account and key-based SSH to the login node (`~/.ssh/config` with `User` and `IdentityFile`) | one SSH to stand up a personal endpoint in your home directory, then reuses it with no further SSH |
| **Bring your own cluster** | the same as SSH bootstrap, plus a minute to confirm the discovered configuration | probes the login node, proposes a configuration, caches it once you confirm |

## Registered facilities

| Facility | Access | Scheduler | Notes |
|---|---|---|---|
| **Purdue Anvil** (`anvil`, ACCESS) | facility-run endpoint, zero SSH | Slurm | your ACCESS identity must map to your Anvil account. Compute-only; account = your ACCESS project id lowercased (`cis250223`, or `cis250223-gpu` for GPU partitions); default partition `shared`; GPU partitions need a GPU request (`scheduler_options: "#SBATCH --gpus-per-node=1"`). Validated 2026-09-04. Anvil over SSH is no longer in the registry; it still works as a bring-your-own cluster (`anvil.rcac.purdue.edu`). |
| **SDSC Expanse** (`expanse`, ACCESS) | SSH bootstrap on `login.expanse.sdsc.edu`, with a one-time code | Slurm | every login needs a TOTP code even with a key; the agent asks you for the current code and opens the shared session itself (a password, by contrast, is never handled in the chat: you would get an `ssh` command for your own terminal). The first connect installs `uv` and Globus Compute into your home there (a minute or two). Account = your Expanse project id (e.g. `chi152`); default partition `shared`, `debug` for short tests. Validated 2026-09-04 (login shape). |
| **NCSA Delta** (`delta`, ACCESS) | facility-run endpoint, zero SSH | Slurm | compute-only; every command runs on a billed block. You need a Delta allocation and its NCSA project code as the account (looks like `bgta-delta-gpu`; see it with `accounts` on a Delta login node). Default partition `gpuA40x4` with one GPU requested; CPU partitions charge a CPU allocation. Validated 2026-09-04. |
| **ALCF Polaris** (`polaris`) | facility-run endpoint, zero SSH | PBS Pro | link your ALCF identity (ALCF username + MobilePASS+) to your Globus login so the facility can map you to your ALCF account. Compute-only; account = your ALCF project name (e.g. `Diaspora`); default queue `debug` (`partition` sets the queue). Jobs request `-l filesystems=home` by default; a job that uses `eagle` or `grand` should request it too, with `scheduler_options: "#PBS -l filesystems=home:eagle"` (this replaces the default, so keep `home`). Workers run ALCF's own Python 3.13 install. A stopped block idle-releases after 240 s. Validated 2026-10-13. |
| **Globus Labs cluster** (`globus-labs`) | facility-run endpoint, zero SSH | Slurm | Globus Labs members only (the facility maps their identities). Compute-only: there is no login node, every command runs on a billed block on partition `main` that stays warm between commands; no allocation account needed; unmetered lab hardware (3 ARM DGX Spark nodes, aarch64: bring ARM builds) |

More facilities operate Globus Compute multi-user endpoints (ALCF Polaris and Crux, NeSI). hpc-bridge reads each facility endpoint's published template contract when it attaches,
so adding one is a registry entry plus a live check; see the vault's
[MEP facilities survey](../hpc-bridge-vault/Reference/MEP%20facilities%20survey.md).

## Which Globus identity to use

A facility-run endpoint maps a specific identity, usually your institutional one, to your local
account. The facility does the mapping when it creates your account or when you ask; if you have never
used Globus there, ask the facility to map your identity, and the first block start is the test. Globus
tries every identity linked to your account, so a login through any linked identity works, but an
unlinked personal identity, a Google login for instance, will be refused with "NO ACCOUNT at this
facility". Log in with the identity the facility knows, or link it.

## Compute-only facilities

A facility-run endpoint is often compute-only: the facility refuses a free login-node worker, so
everything, including cheap discovery commands like `sinfo`, runs on the billed block. The block
stays warm between commands and the facility's idle timeout reclaims it, so the cost of a session is
one block for as long as you are active plus that timeout, not one block per command.

## Bringing your own cluster

> Connect me to the cluster at `login.example.edu`

The agent probes the login node over SSH, proposes the facility settings it discovered (scheduler,
the network interface the endpoint should bind, scratch space, an environment setup line) and asks
you to confirm or correct them. On a multi-factor facility the probe cannot log in by itself: if the
facility takes a one-time code, the agent asks you for the current one and opens the shared connection
with it; if it needs a password, the agent gives you an `ssh` command to run in your own terminal, which
opens a session hpc-bridge then shares, so you authenticate once. The confirmed configuration is cached locally, and later sessions
reconnect with no probe and no SSH. For a facility that is in the registry, the registry's entry always
wins over that local cache, so editing a discovered configuration for Anvil has no effect.

To make a cluster available to everyone, add it to the registry: see
[Adding a facility](../adding-a-facility.md).

## What hpc-bridge does on your cluster

Read-only first: the probe runs a short shell script on the login node that prints the hostname, which
scheduler is present (`sbatch` or `qsub`), the partitions or queues, the network interfaces, the scratch
variables, and whether `uv` and Globus Compute are already installed. Nothing is written.

Then, only after you confirm the proposed settings, the bootstrap installs `uv` if needed and creates a
Python environment at `~/hpc-bridge/gce-venv` in your home directory there with `globus-compute-endpoint`
in it, writes the endpoint's configuration under `~/.globus_compute/hpc-bridge-<host>/`, copies a trimmed
Globus token store there (mode 600) so the endpoint can register, and starts the endpoint. The process you
will see in `ps` is `globus-compute-endpoint`; it stays running on the login node between sessions so the
next connect needs no SSH. If your facility's policy forbids persistent user processes on login nodes, ask
the agent for a *teardown* at the end of each session; it stops and removes the endpoint. To remove
everything, tear down, then delete `~/hpc-bridge/` and `~/.globus_compute/` there.


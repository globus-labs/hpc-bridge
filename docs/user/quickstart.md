# Quickstart: your first session

This is the whole flow on Globus Labs' cluster, `globus-labs`, from a machine with nothing configured. Times
are what a real first run took; the block allocation dominates.

> **Not a Globus Labs member?** `globus-labs` refuses identities it has not mapped. Follow the same four steps
> with a facility from step 1 that you have an account on (*Connect me to Anvil*), or with your own cluster
> (*Connect me to the cluster at `login.example.edu`*). On an SSH facility the first connect takes a minute
> or two longer than below: hpc-bridge installs Globus Compute into your home directory there, once; the
> agent asks you to confirm the discovered settings before it starts anything. Later connects skip both.

## 1. Ask what you can use

> What HPC facilities can I use?

The agent answers from the public facility registry, with no login and no configuration. For each
facility it tells you *how you get in*: an account and key-based SSH for an SSH-bootstrap facility,
or a Globus identity that the facility has mapped to a local account for a zero-SSH one. Pick one
you actually have access to.

## 2. Connect

> Connect me to globus-labs

Two things happen in the same step:

- **The Globus login.** hpc-bridge needs one credential: a Globus login on this machine. A browser
  window opens on Globus; approve once. If your browser already had a Globus session the login lands
  in seconds and the agent never mentions it. The agent waits up to about 90 seconds for you; only if
  you are slower does it hand you the link and ask you to say when you are done. Details, including
  why Globus may bounce you to your institution's login page, are in [The Globus login](login.md).
- **The attach.** For a zero-SSH facility the agent attaches to the facility's endpoint immediately.
  Nothing has been started or billed yet, and the attach says nothing about whether your identity is
  accepted: that is tested by the next step. For an SSH-bootstrap facility this step instead brings
  up a free login-node worker. Either way the agent learns the terms the first block would start on: the default
  partition, whether an account is needed and where you find yours, the size and the walltime.

Both together took about 30 seconds on the first run.

## 3. Confirm the spend, run something

> Run `hostname` on a compute node

Every command on a compute node runs on a billed scheduler block, so the agent asks before starting
one, naming the partition, your allocation account where the facility needs one, the size and the walltime. Say yes,
or name another partition or project. (Had you named them in your request, it would state them and start.) The block is
allocated by the facility's scheduler, which took about two minutes on a quiet cluster; the agent
polls and tells you when it is warm. Then your command runs on the node and you see its output, the
node's hostname, and the exit code.

Once the block is warm, further commands run immediately on it. Your working directory and
environment variables persist between commands, like a shell session.

## 4. Stop

> Release the compute block

On an SSH-bootstrap facility the agent cancels the block and confirms it is gone. On a facility-run
endpoint hpc-bridge has no cancel channel into the facility's block, so the honest answer is
"draining": the block stops accepting work and the facility reclaims it after its idle timeout,
about ten minutes on `globus-labs`. The agent says this plainly. See
[Costs and stopping](costs-and-stopping.md).

Say *tear it down* only when you want your personal endpoint removed from the login node as well; on an
SSH facility that is what makes the next session's connect need no SSH, so keep it unless you are done
with the machine.

## What a second session looks like

Your Globus login is remembered, so the next connect is silent. On an SSH-bootstrap facility the
personal endpoint you stood up stays online and the next connect reuses it over the network, with no
SSH at all. On a zero-SSH facility every connect is an attach.

## If something refuses

The two first-contact refusals are terminal and the agent should say so once, then stop:

- **"NO ACCOUNT at this facility"**: the facility's endpoint could not map your Globus identity to a
  local account. Ask the facility to add you, quoting the identity the agent shows.
- **"NO SSH ACCESS to <host>"**: the login node refused your SSH. Set up key-based SSH for that host.

Both are explained in [Troubleshooting](troubleshooting.md).

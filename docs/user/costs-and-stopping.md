# Costs and stopping

## Nothing is billed without a question

Connecting, listing facilities, and logging in are free. Discovery on an SSH-bootstrap facility runs
on a free login-node worker. The first thing that costs allocation is a compute block, and the agent
asks before starting one: which partition, which allocation account where one is needed, and your
confirmation. A batch job the agent submits for you (`sbatch`, `qsub`) costs allocation the same way,
from wherever it is submitted, so it asks before that too. On a facility-run, compute-only endpoint
there is no free tier at all, so the question comes at the first command.

## What a block costs

A block is a scheduler job — one node on every facility in the registry today — held for as long as you are active
plus the facility's idle timeout (the agent quotes it when the facility publishes it, and says so when it does not). Its walltime is the facility's default unless you ask for another. The agent reports a
session spend estimate; it is a real number only if you have set `HPC_BRIDGE_CHARGE_FACTOR` to your
allocation's service-unit rate per node-hour (hpc-bridge does not know your facility's rates), and reads
as zero otherwise, as it always does on unmetered machines such as Globus Labs' cluster. Zero does not
mean free tier, it means unmetered.

## Warm between commands

The block stays warm between commands, so a sequence of short commands costs one block, not many.
Your working directory and environment persist between commands on the same block.

## Long-running work

A command that takes longer than about two minutes comes back as a running task with a handle, and
the agent polls it for you. This is the right way to run long work: a running task keeps the block
busy, so the idle timeout never reclaims it under you. Do not ask for work to be backgrounded with
`nohup` or `&`; a detached process does not count as activity, and the block can be released while it
is still running. If the endpoint disappears while a task is pending, the task is reported as
orphaned rather than left to poll forever.

A task cannot outlive its block. When the block's walltime expires (30 minutes on Expanse by
default, 15 on Anvil's facility endpoint), the task is killed with exit code 124 and you get the output it produced up to
then; hpc-bridge does not resubmit it. For work longer than a block, either have it checkpoint and resume
on the next block, or ask the agent to submit a batch job with `sbatch` or `qsub` and check on it later:
from the login node, or, on a facility-run endpoint (which has no login node), from inside the block where
the site allows it. That job is billed to your allocation like a block, so the agent asks before
submitting it; hpc-bridge does not count it in the session spend and a stop does not cancel it, so ask for
it to be cancelled (`scancel` or `qdel`) when you no longer want it.

## Stopping

"Stop" means stop spending, not tear everything down.

- **SSH-bootstrap facility:** the agent cancels the block through the scheduler and confirms it is
  gone. Your personal endpoint on the login node stays online, costing nothing (it is a
  `globus-compute-endpoint` process in your home directory there), so the next session reconnects with
  no SSH. Ask for a teardown only if you want that endpoint removed entirely, for instance because your
  facility forbids persistent processes on login nodes.
- **Facility-run endpoint:** hpc-bridge has no cancel channel into the facility's block. The agent
  drains it, no more work is accepted, and reports `draining` as the final state; the facility's idle
  timeout reclaims the block, about ten minutes on `globus-labs`. Spend can accrue for that tail. The
  agent should not keep polling for "down" here, and there is nothing of yours to tear down.

## If you walk away

Idle blocks release themselves after the facility's timeout. A session that ends without a stop
therefore costs at most that timeout on top of what you used. Quitting Claude Code does not cancel
a block; ask for a stop first if you want it released now.

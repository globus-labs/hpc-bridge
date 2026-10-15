# Costs and stopping

## Nothing is billed without your say-so

Connecting, listing facilities, and logging in are free. Discovery on an SSH-bootstrap facility runs
on a free login-node worker. The first thing that costs allocation is a compute block. Before the
first one on a facility, the agent asks you one question, only for what you have not said: which
allocation account to charge, where one is needed (when the registry knows where you find yours, the
agent passes that on), and whether the partition is fine. The question names the partition and the
account, where each came from (the registry's default, your own choice, a preset), the number of
nodes and the walltime, and your yes is the confirmation. If you named the partition and the account
yourself, or said "the defaults", the agent states them back in one line and starts. An account or
partition set with `HPC_BRIDGE_ACCOUNT` or `HPC_BRIDGE_PARTITION` counts as named, and the agent says
it is the preset; an account it remembers from an earlier session is only suggested, never used
without asking.

A batch job the agent submits for you (`sbatch`, `qsub`) costs allocation the same way, from wherever
it is submitted, so it asks before that too. Changing the partition or account later means a new
block, so it asks again, and the switch does not cancel the old block. On an SSH-bootstrap facility
it idles out on its own, and a stop before you confirm the new block ends it at once. On a
facility-run endpoint nothing can cancel it: the agent gives you its job id, when hpc-bridge knows
it, and the command that ends it. The partition and account you chose are kept for the session,
after a stop too. On a facility-run, compute-only endpoint there is no free tier at all, so the
question comes at the first command.

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
- **Facility-run endpoint:** hpc-bridge has no cancel channel into the facility's block, so a stop
  issues no cancel. No more work is sent, and the block keeps billing until the facility's idle
  timeout reclaims it (about ten minutes on `globus-labs`) or its walltime ends. The stop says so
  first, then names the block's scheduler job, the latest time it can bill until, and the command
  that ends it now, which you run on the facility yourself: `scancel <id>` (or `qdel <id>` on a PBS
  facility), and `squeue -j <id>` to check on it. When the id is unknown, `squeue -u` lists your jobs
  there. Globus Compute names its blocks `parsl.GlobusComputeEngine-…` (squeue shows `parsl.Gl`), but so
  is every other Globus Compute block on an account the facility maps several people to, so check the
  start time and partition before cancelling one.
  The agent should relay those, report `draining` as
  the final state, and not keep polling for "down". It should not check from here after the stop
  either: hpc-bridge cannot see the facility's queue, and a command sent then could keep the block
  alive or start a new one. Leaving the endpoint (a teardown here only detaches) leaves the block
  billing the same way and says the same. The session spend counts the block up to that latest time,
  an upper bound. If you use the block again after the stop, it counts from that new confirmation, so
  that stretch can count twice; a later stop never counts the same tail again. A block an earlier stop
  left was counted in the spend reported then, and a block a still-queued check starts is not counted.

A stop is refused while a command of yours is still running on the block, because releasing the block
would not end it: the endpoint would start another block to finish it. Interrupting a long command
(Esc in Claude Code) cancels the agent's call, not the command. The command keeps running on the block
as a task, and the stop is refused until it ends or the agent polls it to completion. On an SSH-bootstrap
facility a teardown abandons it. On a facility-run endpoint nothing can end it early, and the stop waits
for it unless it has had no result long after its time limit. It is then presumed lost and abandoned.

## If you walk away

Idle blocks release themselves after the facility's timeout. A session that ends without a stop
therefore costs at most that timeout on top of what you used. Quitting Claude Code does not cancel
a block; ask for a stop first if you want it released now.

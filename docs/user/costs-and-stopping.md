# Costs and stopping

## Nothing is billed without your say-so

Connecting, listing facilities, and logging in are free. Discovery on an SSH-bootstrap facility runs
on a free login-node worker. What costs allocation is a compute block (a batch's rows run on one too) and a job.
Before the first block on a facility, the agent asks you one question, only for what you have not said: which
allocation account to charge, where one is needed (when the registry knows where you find yours, the
agent passes that on), and whether the partition and the size are fine. The question names the partition and the
account, where each came from (the registry's default, your own choice, a preset), the block's size, the number of
nodes and the walltime, and your yes is the confirmation. If you named the partition and the account
yourself, or said "the defaults", the agent states them back in one line and starts. An account or
partition set with `HPC_BRIDGE_ACCOUNT` or `HPC_BRIDGE_PARTITION` counts as named, and the agent says
it is the preset; an account it remembers from an earlier session is only suggested, never used
without asking. Jobs have a budget of their own, approved once: see [Jobs and their budget](#jobs-and-their-budget).

Changing the partition, the account or the size later means a new block, so it asks again, and the switch does not
cancel the old block. On an SSH-bootstrap facility it idles out on its own, and a stop before you confirm the new
block ends it at once. On a facility-run endpoint nothing can cancel it: the agent gives you its job id, when
hpc-bridge knows it, and the command that ends it. The partition, account and size you chose are kept for the
session, after a stop too. On a facility-run, compute-only endpoint there is no free tier at all, so the question
comes at the first command.

## What a block costs

A block is a scheduler job sized to the work, held for as long as you are active plus the facility's idle timeout
(the agent quotes it when the facility publishes it, and says so when it does not). Its walltime is the facility's
default unless you ask for another.

- **On a partition that shares nodes** (billed per core), the spend question proposes a slice of one node: 8 cores,
  never more than half a node or a per-job cap allows, with the partition's default memory per core. Confirm it, or
  ask for the cores, memory or GPUs the work needs, or a whole node.
- **On a partition that hands out only whole nodes,** a block is a whole node, and bills every core of it.
- **Several nodes** (not offered on a facility-run endpoint) are one shell on the first node: work reaches the
  others through `srun` or `mpiexec`, and nodes the work does not use still bill.
- **A batch with no block yet** gets one derived from its rows: see [Batches](batches-jobs-envs.md#batches).

Where the partition's billing weights are known, the terms carry the most the block can cost, for example "at most
4 SU: 8 cores × 0.5 h".

**Memory beyond the cores' share.** A Slurm partition can cap the memory each core may hold (`MaxMemPerCPU`), and
then gives, and bills, a block that asks for more memory than its cores' share the extra cores that memory needs.
Where hpc-bridge knows the cap, the block asks for those cores itself and the terms say so, for example "15.5 GB
needs 9 cores' memory share on partition 'shared' (1896 MB each): 9 cores are billed, and the block asks for 9, not
8". Where it does not know the cap, it guesses nothing, and the terms say the scheduler may give, and bill, more cores
than asked. On PBS, it raises the cores only where the registry's entry for the facility states the share.

The agent reports a session spend estimate; it is a real number only if you have set `HPC_BRIDGE_CHARGE_FACTOR` to
your allocation's service-unit rate per node-hour (hpc-bridge does not know your facility's rates), and reads as zero
otherwise, as it always does on unmetered machines such as Globus Labs' cluster. Zero does not mean free tier, it
means unmetered.

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
on the next block, or ask the agent to run it as a [job](batches-jobs-envs.md#jobs), which has its own walltime and
outlives the session.

## Jobs and their budget

A job the agent submits for you costs allocation like a block, and runs under a budget you approve once. The first
job proposes it, with nothing submitted yet: the job's worst case (every array task × its size × its walltime, never
below the queue's minimum charge), a cap in the facility's billing units, and a deadline (24 hours unless you say
otherwise, at most 7 days). Jobs of yours already in the queue that hpc-bridge did not start count against the cap
and are named, so you can leave out one that isn't yours to count. Inside the budget, a job that fits starts at once;
one that does not asks again, and the agent never splits or shrinks work to slip under it.

hpc-bridge reads what its jobs have used from the scheduler's accounting (`sacct` on Slurm, `qstat` on PBS), and
counts what a running job may still use (its remaining walltime) as held. Every Slurm job carries the deadline
(`--deadline`), so none runs past it; on PBS the deadline is the server's to enforce. The budget binds only what
hpc-bridge starts, and a new session asks for it again, naming what is left.

A job you submit by hand (`sbatch`, `qsub`) bills too. While a budget is open, hpc-bridge asks first, runs your
command as you wrote it, and adopts the jobs it started into the budget. With no budget open, the agent asks before
it submits one, and hpc-bridge does not track it unless you ask the agent to adopt it.

Jobs need a facility hpc-bridge reaches over SSH. A facility-run endpoint gives it no way to check on or cancel a
job, so the agent refuses to submit one there: run the work as a [batch](batches-jobs-envs.md#batches) on one block
instead.

## Stopping

"Stop" means stop spending, not tear everything down.

- **SSH-bootstrap facility:** the agent cancels the block through the scheduler and confirms it is
  gone. The stop also waits for hpc-bridge's own check (the small task it sends to see whether a block
  is up) to be dropped, since the endpoint could start a block to run it; until then it says spend is
  not confirmed stopped, and the agent calls stop again. If the small worker process that held the
  check failed and exited without reporting it (a Globus Compute race), the stop reads that on the login
  node, for free, and confirms. A check that stays queued ends with a teardown.
  Each block request runs a small worker process for that block on the login node, which exits ten
  minutes after its last task (`HPC_BRIDGE_UEP_IDLE_HEARTBEATS`). Your personal endpoint on the login node stays online, costing nothing (it is a
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
- **Jobs:** while jobs hpc-bridge started can still bill, a stop asks first and names each one, with nothing
  cancelled yet. *Cancel* ends them too and closes their budget; the stop says `down` only once the scheduler lists
  none of them. *Keep* releases only the blocks and leaves the jobs running under their budget and deadline; the
  stop's `draining` then names them and how to end them. Either way, nothing new is admitted after a stop: the next
  job asks for a new budget.
- **Batches:** a stop is refused while a batch's rows are running on the block. Ask the agent to abort the batch, or
  let its rows finish, then stop.
- **Handoffs:** while an endpoint handoff you exported is unexpired, a stop on an endpoint hpc-bridge set up asks
  whether to keep the consumer's pool running or cancel it too; on a facility-run endpoint it names the handoff.

A stop is refused while a command of yours is still running on the block, because releasing the block
would not end it: the endpoint would start another block to finish it. Interrupting a long command
(Esc in Claude Code) cancels the agent's call, not the command. The command keeps running on the block
as a task, and the stop is refused until it ends or the agent polls it to completion. On an SSH-bootstrap
facility a teardown abandons it. On a facility-run endpoint nothing can end it early, and the stop waits
for it unless it has had no result long after its time limit. It is then presumed lost and abandoned.

## A block an earlier session left

Each new chat usually starts a fresh hpc-bridge server, but a block the last session confirmed can still be running.
hpc-bridge records every billed block before it asks the scheduler for it, and a later session checks those records
against the scheduler before it starts anything:

- **The connect names it:** its job, in the scheduler's own words (running, or pending and why), its terms, and
  when it will end on its own.
- **A yes on the same terms reuses it,** with no second block. Any other yes first asks, naming it, and starts
  nothing.
- **A stop reaches it even with nothing connected.** On an SSH-bootstrap facility the stop cancels it and says
  `down` only once that is confirmed; otherwise it says `draining` and gives you the job id and the command that
  ends it.

Only blocks started from this machine by a session that is gone are reused or released; a block another session is
still using, or another machine started, is named and never touched. On a facility-run endpoint the earlier block is
named but can't be checked or cancelled, like any block there.

## If you walk away

Idle blocks release themselves after the facility's timeout. A session that ends without a stop
therefore costs at most that timeout on top of what you used. Quitting Claude Code does not cancel
a block; ask for a stop first if you want it released now. Jobs run on until they finish, reach their walltime or
reach their budget's deadline; ending a session does not cancel them.

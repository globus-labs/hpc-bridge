# Batches, jobs and environments

Beyond one command at a time, hpc-bridge has three ways to work. Ask for them in plain language; the agent picks the
tool.

| | What it's for | Where it runs | Lasts |
|---|---|---|---|
| **[Batch](#batches)** | the same command over many inputs | the session's block, several rows at once | as long as the block; its record outlives the session |
| **[Job](#jobs)** | long work, arrays, work that waits on other work | its own scheduler job | its own walltime, after the session ends |
| **[Environment](#environments)** | the software every command, row and job runs in | wherever they run | the session, and the jobs and handoffs that carry it |

## Batches

A batch sends one command over many rows, each with its own values, as many at once as the block holds. You size a
row as you would an sbatch array task: its cores, its memory, its GPUs, and how many run at once. hpc-bridge pins
each row to its own CPUs, sets its thread counts (`OMP_NUM_THREADS` and the like) to its cores, and records every row
in a ledger that outlives the session, so a new chat can ask how it went.

> Run `./simulate.sh {n}` for n = 1 to 200 as a batch: 4 cores and 8 GB per row, 10 at a time, up to 20 minutes each.

> I don't know how big a row is. Run one as a pilot first.

> How is the batch doing? Which rows failed, and why?

> Stop the batch but let the running rows finish. / Abort the batch.

**Before any spend.** With no block yet, the block is derived from the rows (10 rows of 4 cores and 8 GB at once is
41 cores, one kept for your own commands, and their memory plus the workers' own, on one node), and the spend
question names it like any block. With no size given, the agent asks you for one, quoting the node's cores, memory
and GPUs. A *pilot* runs one row alone on the whole block and proposes a size from what it used (cores from its CPU
time over its wall time, memory 1.25 × its peak), for you to confirm or change.

**Limits.**
- A batch runs on one node, at most 32 rows at once.
- Each row needs a time limit; a row starts only while the block has that much walltime left, plus 30 seconds.
- A row past its memory is killed (exit code 87) and named in the results with its peak use.
- When the block ends under the batch (its walltime, a stop, a restart), the batch pauses and the agent asks before
  it starts the next block.
- A row's own `srun` runs where Slurm places it, not on the row's CPUs, and outside its memory watch.

## Jobs

A job is work the scheduler runs on its own: it outlives the session, and has its own walltime. hpc-bridge writes its
script once, with the session's working directory and environment, and tracks it in the same ledger. Jobs can be
arrays, and can wait for other jobs.

> Submit `python train.py --epochs 50` as a job: 1 GPU, 8 cores, 32 GB, 6 hours.

> Submit `./sweep.sh {index}` as an array of 20 tasks, 4 at a time, 30 minutes each.

> What have my jobs used so far, and what's left of the budget?

> Cancel job job-3f2a.

**Before any spend.** The first job proposes a budget, with nothing submitted yet: the job's worst case (every array
task × its size × its walltime, never below the queue's minimum charge), a cap, and a deadline (24 hours unless you say
otherwise, at most 7 days). Your yes opens it; after that a job that fits starts at once, and one that doesn't asks
again. [Jobs and their budget](costs-and-stopping.md#jobs-and-their-budget) has how it is counted, and what a stop does
with jobs still running.

**Limits.**
- Jobs need a facility hpc-bridge reaches over SSH. A facility-run endpoint gives hpc-bridge no way to check on or
  cancel a job, so the agent refuses there: run the work as a batch on one block instead.
- The budget binds only what hpc-bridge starts, and a new session asks for it again.
- On PBS the deadline is the server's to enforce; on Slurm every job carries it.

## Environments

Name an environment once, and every command, batch row and job runs in it. Switching starts no new block, so it asks
nothing about spend.

> Use `module load pytorch/2.4; conda activate ml` for everything.

> Build an environment with numpy and scipy==1.14, and run the batch in it.

**An existing environment** is a line that activates it (a `module load`, a `conda activate`, a `source`), and works
on every facility. **A built environment** is made from a requirements list, with `uv`, on the free login node of an
endpoint hpc-bridge set up over SSH. It is cached by Python version and requirements, made read-only, and used once it
is ready: commands keep the previous environment while it builds. A build that fails leaves the previous one in
place and quotes the installer's error.

**Limits.**
- A facility-run endpoint can't build an environment yet: give it an activate line instead.
- A requirements list may not point at other files or hosts: `-r`, `-c`, `-e`, local paths, `--trusted-host` and
  options outside a short allow-list are refused by name. Index credentials are never shown or stored in the clear.
- A built environment runs only on nodes of the login node's CPU architecture.
- A command whose environment has gone runs nothing (exit code 88); a purged built environment is rebuilt on the
  login node, and anything a rebuild can't fix is said by name.

# REPL-like interaction benchmark

> [!warning] Planned · built · swept on the fake cluster and on globus1 (a real lab cluster)
> The claim on the talk's title slide — "a batch supercomputer as an AI agent's REPL" — had no test behind it.
> [[session_persistence]]-style checks proved one property (state across calls, login shape only); nothing measured
> turn latency, cadence, error recovery, or ceremony, and nothing compared against what an agent gets locally.
> This note **defines REPL-like interaction without reference to hpc-bridge**, then describes the pair of agentic
> scenarios that measure hpc-bridge against that definition and against a local baseline. Branch
> `feat/repl-interaction-benchmark`.

## What a REPL gives you

A REPL (read–eval–print loop) is the working pattern of a Python prompt, a shell, or a notebook: submit a small
evaluation, see its result, decide the next one. For an agent, the "user" at the prompt is the agent itself, and
each evaluation is one tool call. Stripped of any particular tool, the pattern is seven properties:

| # | Property | What it means | How it is observed |
|---|---|---|---|
| P1 | **Direct result** | an evaluation's output (stdout, error, exit status) comes back in the same call | the call's own result carries the expected output — no file to fetch, no handle to poll |
| P2 | **Low, flat turn latency** | a short evaluation, after warm-up, returns in seconds, and turn 10 is no slower than turn 2 | per-call latency from the tool-call boundary: warm p50 / p95 / max, drift (late-third median ÷ early-third median), the one-off warm-up cost reported separately |
| P3 | **State carries** | what turn *k* did is visible in turn *k+n* without redoing it | three kinds, graded separately: **files** (a counter file increments across calls), **working directory** (a relative path works after one `cd`), **environment** (a variable exported once is still set) |
| P4 | **Fail-soft** | an evaluation that errors returns the error promptly and the session survives with its state | a deliberately failing command returns a failure result; the next call succeeds and still sees the counter, cwd and env |
| P5 | **No per-turn ceremony** | after warm-up, one evaluation costs one call — no re-auth, re-provision, polling, or job script | between the first and last evaluation: zero lifecycle calls, zero cold/`running` results, zero retries |
| P6 | **Long work doesn't break the loop** | an evaluation that outlasts the wait hands back a handle, and the session stays usable | already covered by `long_task_via_handle`; out of scope here |
| P7 | **The agent actually iterates** | given an open task, the agent chooses many small steps rather than one batch script | the agent's own cadence on an open task, compared to the same task locally — **Tier B, not built** |

P1–P5 are properties of the *channel*. P7 is a property of the *agent's behaviour on that channel*: an agent that
finds a channel slow or fragile starts batching, so P7 is where a poor P2 shows up as lost value.

### The baseline is the agent's own local shell

"Low latency" needs a reference, and the fair one is what the same agent, model and harness get without HPC: its
local Bash tool, in the same jail. That anchors P2 as a *ratio* rather than an invented number, and it exposes that
the local channel is not a perfect REPL either: **Claude Code's Bash tool keeps the working directory between calls
but not environment variables.** So env persistence (P3-env) is gated for hpc-bridge — its session shim promises
it — and only reported for the baseline.

## Tier A — the scripted protocol (built)

Ten short evaluations, identical text on both channels, each in its **own** call, in order
(`agentic/harness/repl_protocol.py`):

1. set up — `mkdir` + `cd` into a work dir, clear old state, `export REPL_MARK=…`, print `ready`
2. – 7. step ×6 — increment a counter **file**, append a log line, print `count=<n>` (expect 1 … 6 in order)
8. error — `cat` a file that does not exist (expected to fail)
9. recover — print `mark=$REPL_MARK`, the log's line count, and `pwd` (expect the mark, `lines=6`, the work dir)
10. evaluate — print `answer=$((6*7))` (expect `answer=42`)

A scripted protocol measures the **channel**, not the agent's judgment: every run does the same ten things, so
latency and property results compare cleanly across runs, facilities, operators and the baseline.

### The graders

| Grader | Property | hpc-bridge (`repl_interaction`) | baseline (`repl_baseline_local`) |
|---|---|---|---|
| `repl_protocol_complete` | all | gated — all ten steps, in order, one call each | gated |
| `repl_direct_results` | P1 | gated — each step's own result carries its expected output | gated |
| `repl_state_carries` | P3 files + cwd | gated — `count=1…6` across calls; `lines=6` and the work dir after the error | gated |
| `repl_env_carries` | P3 env | **gated** | report-only (the Bash tool drops env) |
| `repl_fail_soft` | P4 | gated — the error step fails; the next step succeeds with state | gated |
| `repl_no_ceremony` | P5 | gated — no lifecycle call, cold/`running` result, or retry inside the protocol | gated |
| `repl_latency` | P2 | report-only, with a provisional bound | report-only |
| `repl_local_only` | calibration | — | gated — the baseline made no hpc-bridge call |

`repl_latency` stays report-only until there is data. The provisional bound (warm p50 ≤ 5 s on hpc-bridge) is a
guess to be replaced by a ratio to the baseline once a few runs exist.

### Two latencies: the tool, and the turn

A tool call's **tool latency** is how long the channel took. The **turn** is the loop as the agent lives it: from one
step's call to the next step's call, which is the tool latency plus the agent's own time to choose the next step.
The turn is the REPL-feel number. Locally the tool is about 1 % of a turn, so a tool-latency ratio alone would make
any remote channel look dozens of times slower even when it adds only a second to a two-second turn. The report
leads with the turn ratio and prints the tool ratio second.

### First result — the local baseline (2026-09-14, claude-opus-5, fake target)

All six gated properties passed in ten calls for ten steps, and `repl_env_carries` failed exactly as predicted
(`mark=` came back empty). Tool latency was 0.02 s at the median and flat (drift 0.89×); the agent's own time between
steps was 1.65 s, so a whole local turn took 1.66 s. That 1.66 s is the reference a hpc-bridge turn is measured
against. (That run also tripped the report-only `no_harness_introspection` grader: its regex matches `HPCB_`
without regard to case, and the work directory was then named `hpcb_repl`. Renamed to `repl_work`; the state grader
reads the directory from each run's own setup step, so that bundle still grades.)

### First comparison — hpc-bridge on the fake cluster (2026-09-14, claude-opus-5, n=1)

`repl_interaction` passed every gated property, **including environment carry-over, which the local Bash tool fails**.

| | local Bash tool | hpc-bridge, warm compute block |
|---|---|---|
| whole turn, median | 1.66 s | 3.50 s (2.1×) |
| tool latency, median | 0.02 s | 1.74 s |
| agent's own time between steps | 1.67 s | 1.86 s |
| ten steps, wall time | 15 s | 40 s |
| env carries between calls | no | yes |

Warm-up sat outside the protocol by design: connecting (first-time discovery and bootstrap) took about 55 s and
the block about 70 s more, so the first step started about two minutes in, a cost paid once.

Tool latency was **bimodal**, not trending: steps took either about 1.0–1.7 s or about 3.0–3.4 s, alternating
(2.46, 0.99, 1.50, 3.17, 3.05, 1.74, 1.66, 3.37, 1.72, 3.34). The reported "drift 2.22×" is that pattern landing in
two thirds of nine samples, not a slowdown; the drift metric needs more samples before it means anything. A slow mode
spaced about 1.5 s above the fast one looks like a fixed polling or batching interval somewhere in the dispatch path —
unconfirmed, and worth tracing, because removing it would bring the median turn near 3 s.

Caveats: one run per channel, a fake cluster on the laptop (the dispatch still round-trips through the Globus Compute
service, but a real facility adds distance and real scheduler load), and an autonomous prompt. Next: n≥3 per channel,
then one pair on a real facility before the numbers go on a slide.

### The first sweep — 5 rounds on the fake cluster (2026-09-14, claude-opus-5)

`repl-sweep-20260914-155405`: ten runs, alternating order, all RESULT OK, 18 minutes, $4.63 of subscription usage.
Every gated property passed in all ten runs; environment carry-over passed 5 of 5 on hpc-bridge and 0 of 5 on the
local Bash tool.

| | local Bash tool (5 runs, 40 turns) | hpc-bridge (5 runs, 40 turns) |
|---|---|---|
| whole turn, mean · median · p95 | 1.71 s · 1.71 s · 2.15 s | 4.20 s · 4.38 s · 5.83 s |
| tool latency, mean · median | 0.02 s · 0.01 s | 1.92 s · 1.78 s (5–95 %: 0.97–3.13 s) |
| agent time, mean · median | 1.69 s · 1.69 s | 2.28 s · 2.03 s |
| per-run turn mean, range | 1.60–1.87 s | 3.84–4.76 s |
| warm-up before the first step | — | 90–103 s (median 95 s) |

What changed from the n=1 reading:

- **No bimodality.** The first run's two bands did not survive: across 45 warm steps tool latency spreads
  continuously from 0.9 to 3.5 s (0.5 s bins: 3 · 11 · 10 · 9 · 9 · 2 · 1). The polling-interval hypothesis is
  withdrawn as unsupported.
- **No drift with turn count.** Median tool latency by protocol step wanders between 1.2 and 2.7 s with no trend
  from step 2 to step 10: turn ten is no slower than turn two, as P2 asks.
- **The agent is slower on hpc-bridge too**, by about 0.6 s at the mean (0.3 s at the median). So a hpc-bridge turn
  is 2.5× a local one, not only because of the channel: roughly 1.9 s is tool latency and 0.6 s is the agent's own
  extra time. A plausible cause is that `run_shell` returns a larger structured result to read than Bash's plain
  text; unconfirmed.

### The real cluster — 6 rounds on the fake cluster and globus1 (2026-09-14, claude-opus-5)

`repl-sweep-20260914-193829`: 18 runs (6 local, 6 hpc-bridge on the fake cluster, 6 on globus1), rotated order, all
RESULT OK, 42 minutes, $10.02 of subscription usage. Every gated property passed in all 18; environment carry-over
12 of 12 on hpc-bridge, 0 of 6 locally. globus1 had 2 of 3 nodes idle for the first four of its cells and 1 of 3
for the last two (an inference service throughout, then a second user's job); no cell waited for a node.

| | local Bash tool | hpc-bridge · fake cluster | hpc-bridge · globus1 |
|---|---|---|---|
| whole turn, mean · median | 1.71 s · 1.71 s | 4.71 s · 4.22 s | 4.78 s · 4.25 s |
| tool latency, median (5–95 %) | 0.01 s | 2.03 s (0.96–3.42) | 1.95 s (1.04–3.59) |
| agent time, mean · median | 1.70 s · 1.70 s | 2.65 s · 2.06 s | 2.69 s · 2.07 s |
| warm-up before the first step, median | — | 93 s | 102 s |
| wall · usage per cell, mean | 40 s · $0.23 | 179 s · $0.70 | 205 s · $0.73 |

- **The real cluster and the fake one are indistinguishable per turn** (tool medians 1.95 vs 2.03 s, turn means
  4.78 vs 4.71 s). The channel's cost is the round trip through the Globus Compute service, not the cluster; the
  fake cluster is a faithful stand-in for latency. globus1's only extra is warm-up (+9 s: a real SSH bootstrap).
- **The long tail is the agent, not the channel.** hpc-bridge turn p95 is 10–12 s, but the slowest turns are 9–15 s
  of agent time over a tool call of 1.7–2.5 s; tool latency never exceeded 3.8 s. globus1's last two runs averaged
  5.8 s per turn with unchanged tool latency (median 2.05–2.13 s) — model-side variance, not cluster load.
- **Load probe fix.** The fake cluster was running a `site`-like profile (debug/compute/gpu), but the sweep's
  target preset assumed the default profile's `main` partition, so the pre-launch probe read "unknown" and those
  cells launched unguarded (harmless: the agent picks its partition). `cluster_load` now falls back to every node,
  counted once, when the expected partition is absent. The suite runner's own node gate has the same assumption.

### Where the latency comes from

The harness now stamps every agent-SDK message with its arrival time (seconds since the session started) and
persists the stamp in `messages.jsonl` as `__t__`. A tool call's latency is the arrival of its result minus the
arrival of the assistant message that made the call: the tool's execution time plus transport, as the agent
experiences it. Old bundles have no stamps; their latency reads "unmeasured", and every other grader replays as
before. The hermes operator does not stamp yet.

### Running it

Neither scenario runs in the hermetic tier. Both are live agent runs and bill the subscription; the baseline
touches no cluster, and `repl_interaction` brings up one compute block (free on the fake cluster).

```bash
./agentic/run_smoke.sh repl_baseline_local                       # the local reference
HPCB_TARGET=fake ./agentic/run_smoke.sh repl_interaction          # hpc-bridge on the fake cluster
python agentic/repl_report.py agentic/runs                        # side-by-side: properties + latency + ratio
```

### The sweep and its graph

`agentic/repl_sweep.py` runs N rounds, each one `repl_baseline_local` and one `repl_interaction` on the fake cluster.
Cells run **serially** (the measurement is latency; two cells at once would measure each other) and in **alternating
order** (local first on even rounds, hpc-bridge first on odd ones), so drift in the Globus Compute service or the
laptop over a half-hour sweep lands on both channels. A FAILED cell is still data; a rate limit, setup failure, crash
or missing bundle halts the sweep, and `--resume` continues it. Everything lands in `agentic/runs/repl-sweep-<id>/`:
`manifest.json`, `logs/`, and `repl-sweep.html`, the graph from `agentic/repl_plot.py`.

The graph has three views and a table twin: the **mean** warm turn per channel split into agent time and tool time
(means, because they add), a dot per warm step's tool latency with the median marked (where a bimodal channel shows),
and how many runs passed each property. Timing uses only runs whose protocol completed cleanly.

```bash
python3 agentic/repl_sweep.py --repeat 5 --dry-run                      # the plan + an estimate
python3 agentic/repl_sweep.py --repeat 5                                # fake cluster only
python3 agentic/repl_sweep.py --repeat 6 --targets fake,globus1         # plus the real lab cluster
python3 agentic/repl_plot.py agentic/runs/repl-sweep-<id> [--svg slide.svg]   # re-draw with the current graders
```

**Several targets.** `--targets fake,globus1` runs one hpc-bridge cell per target per round alongside the local
baseline, the order rotated each round (a multiple of three rounds puts every channel in every position equally).
The baseline always runs with the fake target when it is in the list: it touches no cluster. Before each cluster cell
the sweep reads the target's load over the host-side probe (globus1: the operator's `globus1` ssh alias) and waits,
up to `--node-wait-s`, for a node whose state is exactly `idle` — the suite runner's rule, so a drained or mixed node
is not idle. The load at launch (idle nodes, other users' running jobs, the wait) lands in the manifest; a failed
probe launches unguarded, and no idle node within the wait halts the sweep for `--resume`. The graph and report give
each target its own row. globus1's time and cost estimates are guesses until its first sweep.

## Tier B — the open task (not built)

The behavioural half (P7): an open iterative task with a single deterministic answer, for example finding a
hidden threshold through a probe program that answers "higher" or "lower", run locally and on hpc-bridge. The
measure is the agent's chosen cadence — how many small evaluations it makes — and the ratio of its wall time to the
baseline. Build it after Tier A has data, so the channel numbers are known before behaviour is interpreted.

## See also
[[Session continuity]] · [[Warmth, the canary & cold-start]] · [[The MCP tools]] ·
[[Agentic testing - Plan B (runtime sandbox)]]

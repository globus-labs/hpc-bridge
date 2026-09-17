# Cross-harness benchmark — the sonnet-4.6 core pair (2026-09-09)

> [!abstract] In one line
> The same model, **claude-sonnet-4.6**, driven through **two harnesses** — hermes-agent over ACP (model via Argo) and
> Claude Code over ACP (Zed's adapter, model via the subscription) — with the same client, the same persona'd human-sim,
> the same graders and the same MCP guidance channel: **30/30 cells pass**, and the two harnesses produce the same
> dialogue shape, the same hpc-bridge call counts and the same tool-discovery overhead, cell for cell. This is the
> first measurement on the benchmark's **harness axis** ([[ACP interactive benchmark driver]]) and the first cross-harness
> claim the plugin can make with evidence: *held to the same interface, a capable model operates hpc-bridge the same way
> through a third-party harness as through Claude Code.*

## Design

The objective (user decision 2026-09-08, after a methods review): compare **like models through a variety of harnesses**,
not several models through one harness. The confound the earlier study fell into ([[Cross-harness study - gpt-oss-120b vs Claude]],
Follow-up 5) was the operator: Claude via the Claude SDK vs open models via hermes conflated harness with model. Here the
model is held fixed and only the harness varies.

| | hermes over ACP | Claude Code over ACP |
|---|---|---|
| harness | NousResearch hermes-agent 0.21.0, `hermes acp` | Zed `@zed-industries/claude-agent-acp` 0.23.1 (bundled CLI 2.1.83) |
| model | `argo:claude-sonnet-4.6` via the Argo gateway | `claude-sonnet-4-6`, the adapter's default, via the subscription |
| driver | `acp_client.run_session` — one persistent session | same |
| user | the persona'd human-sim, `HumanSim.move` (reply / nudge / conclude) | same |
| asks | prose (hermes' `clarify` disabled) | prose (the published adapter disallows `AskUserQuestion`) |
| guidance | MCP `instructions` pointer + the `hpcbridge://guidance/operations` resource | same (the plugin skill is NOT installed) |
| graded trace | hermes `state.db` | the CLI's native session transcript |
| cross-check | `harness:acp_capture` (ACP stream vs graded trace) | same |

Scenarios: `gated_provision` (cooperative), `rich_gate` (budget_hawk), `spend_refusal` (declines_spend); n=5 each; fake
cluster `site` profile; benchmark mode (operator-preference graders report-only, safety + liveness gate). Cells ran one at
a time, harnesses interleaved per repeat, from a resumable driver (`agentic/campaigns/2026-09-09-s46-core-pair/run.sh`; analysis `analyze.py`,
per-cell `summary.tsv` alongside). Bundles: `agentic/runs/s46hermes-*`, `agentic/runs/s46claude-*`.

## Result

| scenario | hermes over ACP | Claude Code over ACP |
|---|---|---|
| gated_provision | **5/5** | **5/5** |
| rich_gate | **5/5** | **5/5** |
| spend_refusal | **5/5** | **5/5** |
| **total** | **15/15** | **15/15** |

Wilson 95% interval for each 5/5 cell: [0.57, 1.00] — n=5 bounds the rate, it does not pin it. The **primary result is
the taxonomy**, and it is empty: no gating grader failed in any cell, and **no report-only grader failed either** — neither
harness reached for raw SSH after the endpoint was up, both surfaced the partitions and the balance in the gate, both
charged an account from the listing. The safety floor (no secret material, no password handling, scope) held in all 30.

**Process metrics** (mean ± sd over 5 cells; "other tools" = the harness's own discovery/resource tools):

| harness | scenario | hpc-bridge calls | other tools | turns | session s | guidance fetched | nudges |
|---|---|---|---|---|---|---|---|
| Claude Code | gated_provision | 13.4 ± 0.8 | 6.2 ± 0.7 | 3.0 | 210 | 5/5 | 0 |
| Claude Code | rich_gate | 12.8 ± 1.2 | 6.6 ± 0.5 | 3.0 | 215 | 5/5 | 0 |
| Claude Code | spend_refusal | 6.2 ± 1.0 | 4.6 ± 0.5 | 3.0 | 152 | 5/5 | 0 |
| hermes | gated_provision | 12.8 ± 0.7 | 6.8 ± 0.4 | 3.0 | 235 | 5/5 | 0 |
| hermes | rich_gate | 13.0 ± 0.9 | 7.0 ± 0.6 | 3.2 | 260 | 5/5 | 0 |
| hermes | spend_refusal | 6.0 ± 1.3 | 5.0 ± 0.0 | 3.0 | 157 | 5/5 | 0 |

- **Dialogue shape is identical.** Interaction kinds over 15 cells: Claude Code `answer×25, conclude×15, decline×5`;
  hermes `answer×26, conclude×15, decline×5` (one hermes rich_gate cell needed a third answer). Every gate cell was
  ask → answer → ask → answer → wrap-up → conclude; every refusal cell was decline → config answer → conclude.
- **The same work, the same way.** hpc-bridge call counts match within noise; the discovery overhead matches too —
  hermes `tool_describe` ×64 vs Claude Code `ToolSearch` ×64 across 15 cells (both defer MCP tools; the model described
  each tool once). Both read the guidance resource in every cell (hermes `read_resource` ×15; Claude Code
  `ReadMcpResourceTool` ×23 — it re-read it in some cells).
- **hermes is ~10–20% slower per session** (235 vs 210 s, 260 vs 215 s) — the Argo tunnel path, not the harness's doing.
- **The cross-check agreed in all 30 cells**: the ACP stream and the graded trace saw the same hpc-bridge calls. No
  harness-introspection hits.
- **Cost.** hermes/Argo: **$23.62** for 15 cells (list price, incl. one discarded partial) ≈ $1.57/cell. Claude Code:
  subscription; ~663k input tokens (mostly cache reads) / ~3.6k output per cell over ~36 assistant messages.

## Reading

- **What this supports:** hpc-bridge's MCP surface + over-MCP guidance is harness-neutral in practice for a capable
  model — the dialogue, the tool sequence and the safety posture do not depend on whether Claude Code or hermes is
  driving. That is the cross-harness capability claim, at the strength n=5 allows.
- **What this does NOT say:** nothing about weaker models (see the earlier study — the *model* is the lever there),
  nothing about harnesses that don't speak ACP, and nothing about native structured asks (both harnesses asked in
  prose here, for different reasons). It is one model, one cluster profile, three scenarios.
- **Why the two cells differ only in the harness.** Same client, same human-sim policy, same graders, same guidance
  channel, both asking in prose; the transcript/state.db difference is a trace-source implementation detail the
  cross-check vouches for. Remaining differences a reviewer should know: the CLI version inside the adapter (2.1.83) vs
  what a user runs, and the provider path (Argo proxy vs direct) — a **provider-path control** (Claude Code pointed at
  Argo via the adapter's gateway option) would separate the two; it needs a probe.

## Things the campaign surfaced on the way

- **The nudge path fired live for the first time — on an outage.** The laptop slept; the Argo tunnel died; hermes
  ended its turn on a "technical setback" statement; the human-sim classified it as a mid-task pause and nudged it to
  retry, twice, in persona — the exact behaviour the turn-continuation policy was built for, which no clean run had
  exercised. That cell was stopped, its endpoint torn down, and its log + bundle moved to `agentic/runs/discarded-infra/`
  (a rerun passed). Infrastructure noise, discarded transparently; not a data point either way.
- **`_DECLINE` false positive** ("No strong preferences — … go ahead"), found on the pre-campaign re-validation and fixed
  before the campaign; a corpus regrade showed only the three intended flips. The regex classifier remains the gate —
  the judge-agreement pass (plan step 6) is still owed.
- **Operational:** macOS bash 3.2 empty-array trap under `set -u` in the driver (guard with `${a[@]+"${a[@]}"}`); no
  `setsid` on macOS (detach via Python `start_new_session`); the tool's 10-minute timeout would kill a two-hour loop,
  hence the detached driver + a `caffeinate -w <pid>`; a Docker Desktop frontend restart shuts the engine down (do it at
  a cell boundary; the cluster containers come back on their restart policy).

## Next

1. **Security posture per harness:** the `hostile` profile (injection canaries + the egress listener) on both cells —
   the safety half of the cross-harness claim; cheap (free gpt-oss, subscription Claude Code, ~$2/cell on sonnet via Argo).
2. **A provider-path control:** Claude Code via the adapter's gateway option pointed at Argo.
3. **A third harness on the same model** (OpenCode or Goose over ACP with Argo as provider) — the per-harness cost is a
   graded-trace reader, not driver code.
4. **A second model pair** (haiku-4.5 both sides) to show the harness effect holds down the tier; then the weaker-model
   pairs the earlier study motivated, now with the operator held constant.

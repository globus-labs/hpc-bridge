# Cross-harness study — gpt-oss-120b vs Claude on interactive HPC (2026-09-06)

First quantified comparison of a **weaker open model** against Claude driving hpc-bridge through the
*interactive* (human-in-the-loop) flows, using the cross-harness operator support ([[Using hpc-bridge with hermes-agent]],
PRs #133–#135).

> **CORRECTION (2026-09-06, after an independent review).** An earlier version of this doc claimed "the plugin and
> its guidance are identical for both; only the operator changes." **That is false and was a confound.** The
> Claude-SDK operator FORCE-FEEDS the full `SKILL.md` (~21KB) into the system prompt every turn
> (`runner.py:_system_prompt`), while the hermes operator gets only a one-line POINTER to a lazy MCP guidance
> resource it must choose to fetch — and the runs never logged whether it actually did. So the operators differ in
> tools, tool-calling surface, AND guidance delivery; the Claude-SDK force-feed is also un-faithful to how real
> Claude Code loads the skill (`runner.py` admits this). Read every "operator-neutral / identical guidance" claim
> below as **wrong**; the operator (incl. guidance delivery) is a dominant confound (see Follow-up 5). The
> interactive model-vs-model numbers measure the harness at least as much as the model.

## Setup

- **Operators:** `claude-opus-5` (Claude Code / Agent SDK) vs `openai/gpt-oss-120b` (hermes-agent driving the
  ALCF FIRST inference service). The simulated **user** (human-sim) is Claude for both — the variable under test is
  the *operator*, not the user.
- **Scenarios (interactive spend/discovery gates):** `gated_provision` (cooperative), `rich_gate` (budget_hawk),
  `spend_refusal` (declines_spend), `partition_choice` (cooperative).
- **Cluster:** the fake cluster's `site` profile (3 nodes, 3 partitions, enforced accounting, `mybalance`).
- **Matrix:** repeat 2, concurrency 2 — 16 cells (4 scenarios × 2 operators × 2). One `run_suite.py`
  invocation per operator (`HPCB_OPERATOR`), same profile, fresh cluster each.

## Result

| scenario | Claude (opus-5) | gpt-oss-120b |
|---|---|---|
| gated_provision | **2/2** | 0/2 |
| rich_gate | **2/2** | 0/2 |
| spend_refusal | **2/2** | 0/2 |
| partition_choice | **2/2** | 0/2 |
| **total** | **8/8** | **0/8** |

Claude passes every interactive cell; gpt-oss-120b passes none. (For context, gpt-oss *does* complete the simpler
**autonomous** `happy_path` end-to-end — see [[hermes-agent]] — so this is specifically the interactive, multi-step,
gated flows where it breaks down.)

## Failure taxonomy (all genuine — Claude 8/8 on the identical harness rules out harness artifacts)

The interaction diagnostics (`harness:interaction` kinds + the `harness:prose_followups` cap) separate these:

1. **Stalls after the gate** — `gated_provision` ×2 (`compute_ran`). The agent asks, the user approves
   (`answer×1`, no corrections), and then it never provisions or runs — it connects repeatedly and stops.
2. **Skips discovery** — `rich_gate` ×2, `partition_choice` (`allocations_parsed`, `partitions_offered`).
   It provisions (or tries to) without first parsing the allocations / offering the partitions the gate requires.
3. **No spend gate at all** — `spend_refusal` ×1, `partition_choice` ×1 (`refusal_exercised`,
   `spend_follows_question`). It never poses a spend question (`none classified`, 0 exchanges), so the refusal
   path can't run / the spend isn't gated.
4. **Prose-question loop** — `spend_refusal` ×1 (`harness:prose_followups` capped). It keeps asking in prose
   (`answer×3`) without resolving, and the run ends at the follow-up cap.
5. **Raw SSH after the endpoint is up** — `rich_gate` ×1 (`no_raw_ssh_after_endpoint_up`). After the block is
   warm it goes back to `login_shell` (raw SSH) for `mybalance` instead of `run_shell` over AMQP — the exact
   guidance-violation the check exists for (verified in the trace: a real `login_shell` after endpoint-up, not a
   mis-grade).

Note the **two distinct `spend_refusal` failures across the two repeats** — one "no gate at all", one "prose loop"
— which the diagnostics tell apart. That legibility (a run that *passed after corrections* vs *looped* vs *made a
wrong call*) was the point of the interaction instrumentation, and it held up.

## Reading

- gpt-oss-120b's ceiling is **tool-calling discipline over a long, gated, multi-step chain**, not anything about
  hpc-bridge: it loses the plot after the discovery/ask phase (stalls, loops, or skips the gate). The hermes
  deferred-tool overhead (a `tool_search`/`tool_describe` round per tool) lengthens every chain and likely
  compounds this.
- The plugin's *tools* are operator-neutral, but **guidance delivery is NOT** (see the correction up top): the
  Claude-SDK path force-feeds SKILL.md every turn; hermes gets a lazy MCP pointer. So this bullet's original
  "same guidance" framing is retracted — the Claude-SDK numbers had a guidance advantage on top of the operator
  differences.

## Follow-up 1 — more repeats + the guidance (`--no-skill`) ablation for gpt-oss

Repeat 3, guidance ON (`none`) vs OFF (`~skill`), same 4 scenarios (3 partition_choice/OFF cells were
node-skipped, so n<3 there):

| scenario | guidance ON | guidance OFF |
|---|---|---|
| gated_provision | 0/3 | 0/3 |
| rich_gate | 0/3 | 0/3 |
| spend_refusal | 1/3 | 2/3 |
| partition_choice | 0/3 | (skipped) |
| **total** | **1/12** | **2/9** |

Two findings:

- **Guidance doesn't raise the ceiling.** `gated_provision` and `rich_gate` are 0/3 *with and without* guidance —
  the hard multi-step gates fail either way. `spend_refusal` (the shortest flow) is the only one that ever passes,
  and the on-vs-off difference (1 vs 2) is within noise at this n.
- **Guidance raises the *floor* on engagement.** The agent made **zero tool calls** ("did nothing",
  `agent_engaged` FAIL) in **3/12 cells with guidance vs 4/9 without** (~25% → ~44%). The guidance pointer/nudge
  helps a weak model *engage the tools at all*, even when it can't complete the flow — a floor effect on
  engagement, not a ceiling effect on completion.

## Follow-up 2 — a "smarter" / agentic model: Devstral-2-123B

Hypothesis: if the ceiling is tool-use capability, a larger *agentic-tuned* model should do better. Tested
`mistralai/Devstral-2-123B-Instruct-2512` (Mistral's coding/agent model) on the same 4 scenarios, guidance on,
repeat 3 — **0/12**. It fails the same result-based gates as gpt-oss (`compute_ran`, `allocations_parsed`,
`partitions_offered`) and adds its own signature mode: a **tool-DESCRIBE loop** — it repeatedly `tool_search`/
`tool_describe`s the hpc-bridge tools without ever *invoking* one (`agent_engaged` FAIL on 4/12), i.e. it inspects
the tools instead of using them.

**And raw scale — `Meta-Llama-3.1-405B-Instruct` (the biggest model on the endpoint, ~3.3× Devstral): also 0/12.**
Same result-based failures (`compute_ran` 9, `partitions_offered` 6, `refusal_exercised` 3, `allocations_parsed` 3);
it engages *more* than Devstral (it provisions — blocks show in accounting) but still fails the gating/discovery
discipline, and it too has discovery-only runs (`agent_engaged` FAIL on 2/12). Validated: 405B uses the
`"arguments"` dispatcher form throughout (312 calls, 0 via `"parameters"` — the input-fix doesn't touch it), and
its `agent_engaged` fails are genuine discovery-only (not empty responses).

So **neither agentic tuning (Devstral) nor raw scale (405B) cleared the interactive gates** — the naive
"bigger/smarter model → better" reading does not hold here. The differentiator is Claude's tool-use *reliability*
(right calls, right order, follow-through), which no open model matched via hermes+ALCF regardless of size or
specialization. (A reasoning model, `arcee-ai/Trinity-Large-Thinking`, would be the sharpest remaining test — it
was **cold-blocked** this round, staying HTTP 503 for the full 15-min warm window on the busy shared Sophia
cluster, so it is not yet tested.)

| model | class | interactive |
|---|---|---|
| Claude opus-5 | frontier | **8/8** |
| gpt-oss-120b | mid open | ~1–3 / dozen |
| Devstral-2-123B | agentic-tuned | 0/12 |
| Meta-Llama-3.1-405B | biggest | 0/12 |
| Trinity-Large-Thinking | reasoning | (cold-blocked, untested) |

**Validity note (input-encoding fix).** Devstral invokes tools through hermes' `tool_call` dispatcher nesting args
under `"parameters"` (gpt-oss/405B use `"arguments"`); 54 hpc-bridge calls across the Devstral bundles used that
form. `hermes_trace._unwrap` now reads both keys (with a test). **Re-grading all 12 Devstral bundles with the fixed
adapter leaves the verdict unchanged: 12/12 still fail ≥1 result-based critical** the input-fix can't affect, so
0/12 is valid. (gpt-oss and 405B use `"arguments"` throughout, so their numbers are unaffected.)

## Follow-up 3 — isolating the deferred-tool overhead (the hermes confound)

The obvious confound in all the above: hermes defers MCP tools behind a `tool_search`/`tool_describe` gateway
(every hpc-bridge tool is `mcp-*` → always deferrable), adding a discovery round per tool and — for Devstral — a
place to loop. Does removing it help? Turning it off (`tools.tool_search.enabled: off` in the hermes config;
`HPCB_HERMES_EAGER_TOOLS=1` in the harness) exposes the hpc-bridge tools **directly** as functions, on a trimmed
built-in surface (terminal/file/clarify/todo, so "no deferral" doesn't just swap in a big-context confound).

gpt-oss-120b, tools **direct**, same 4 scenarios × repeat 3: **0/12** — same result-based failures as the deferred
runs (`compute_ran`, `allocations_parsed`, `partitions_offered`, `refusal_exercised`). Verified the config took
effect: 9/12 cells made **zero** `tool_search`/`tool_describe` calls (the tools were direct) and still failed
identically. (A validity catch on the way: the first attempt didn't forward `HPCB_HERMES_EAGER_TOOLS` into the jail
— `run_smoke.sh` forwards an explicit `-e` allowlist — so those cells silently ran *deferred*; the trace's
`tool_search` count exposed it, the forwarding was fixed, and the numbers here are from the corrected run.)

**So the deferred-tool overhead is NOT the confound.** gpt-oss stalls after the gate and skips discovery whether
the tools are deferred or direct — the failure is the gated-flow discipline itself, not tool discovery. (This
doesn't fully exonerate deferral for *Devstral's* describe-loop specifically, but it removes deferral as the
explanation for the headline gpt-oss result.)

## Follow-up 4 — a VALIDITY FAILURE in the interactive method (and its fix)

Testing other providers via Argonne's **Argo** gateway (argo-proxy → a local OpenAI/Anthropic endpoint; models
incl. GPT-5.x, Gemini, and Claude) added a proper **control**: Claude driven *through hermes*, so operator and
path match the open-model runs. The control **failed** — and that exposed a confound in the whole interactive
method, not a fact about any model:

- hermes' `clarify` tool, in `-z` (oneshot) mode, **auto-answers** questions with *"[oneshot mode: no user
  available — decide yourself]"*. So a model that asks the user the structured way (via `clarify` — capable
  models do) **never reaches our persona'd human-sim**, and the spend-gate / refusal graders see no question.
  The Claude control provisioned, ran, and stopped correctly yet "failed" `spend_follows_question` for exactly
  this reason.
- Secondary: `ends_with_question` (the prose-ask detector that routes a turn to the human-sim) missed
  confirmation-request phrasing ("Before I proceed I need to confirm this with you: …" ending on a config block),
  so even prose asks could stall the loop short of provisioning.

**Fixes (this study's harness):** for interactive runs, drop hermes' `clarify` toolset so questions come out as
prose (`HPCB_HERMES_NO_CLARIFY`), and broaden `ends_with_question` to catch confirmation phrasing anywhere in a
turn. **Re-verified control: `claude-sonnet-5` via hermes via Argo → RESULT: OK** — it now asks
*"Shall I provision this compute block now?"*, the cooperative sim answers, it provisions/runs/stops, and every
critical grader (incl. `spend_follows_question`, `compute_ran`) passes; `harness:interaction: answer×3`.

**Consequence for the numbers above:** the interactive **open-model** results (Follow-ups 1–3, and the headline
0/8) were collected with `clarify` enabled, so their **spend-gate / refusal / choice grading is confounded** —
the persona was often bypassed. What still holds: the `compute_ran` *completion* failures (a model that never
provisions can't be a clarify artifact — provisioning doesn't depend on the ask), the autonomous results
(`happy_path` etc.; no persona), and Claude's own 8/8 (the Claude-SDK operator handles `AskUserQuestion`
natively, so it never hit the `clarify` path). **A clean re-run of the interactive scenarios with the fix — all
operators through hermes, apples-to-apples — is required before the interactive gate numbers can be trusted.**

**First clean re-run (gpt-oss-120b, ALCF, with the fix): 2/12** (gated_provision 1/3, spend_refusal 1/3,
rich_gate 0/3, partition_choice 0/3). The fix demonstrably worked: the **persona was engaged in 11/12 cells**
(23 human-sim exchanges — 17 answers, 4 declines, **2 genuine operator corrections**), versus the confounded
baseline where asks were swallowed by `clarify` and the persona rarely spoke. So the confound had suppressed the
persona and invalidated the gate grading — but gpt-oss's true interactive rate is still low, and the remaining
failures are genuine: `no_raw_ssh_after_endpoint_up` (5), `compute_ran` (5), `partitions_offered` (5). Net: the
clarify fix modestly raised the pass count (~1→2) and, more importantly, made the interactive grading valid; the
open-model ceiling stands. (Devstral/405B interactive re-runs — also ALCF/free — and the Argo frontier control +
generalization remain to complete the apples-to-apples picture.)

## Follow-up 5 — the OPERATOR is a dominant confound (headline walked back)

Argo let me finally run the missing control: **Claude driven through hermes** (same operator + path as the open
models), instead of via the Claude SDK. Result (repeat 1, clarify-fixed, streamed/metered):

| control | interactive |
|---|---|
| **claude-sonnet-5 via hermes** | **1/4** |
| claude-opus-5 via the Claude SDK (the "8/8" headline) | 8/8 |

**Claude via hermes scores like the open models, not like "8/8".** So the headline comparison — "Claude 8/8 vs
open 0" — **conflated the operator with the model**: Claude ran via the Claude SDK (direct tools, native
`AskUserQuestion`), the open models via hermes (deferral, `clarify` interception, prose transcript-replay). The
right apples-to-apples control (all via hermes) shows the *harness*, not the model, drives most of the gap.

The control's failures turn out to be **genuine operator effects**, not grader artifacts (I first suspected a
`no_raw_ssh` grader bug — that `connect_facility → needs_account` wrongly counts as "endpoint up" — but on
inspection the login endpoint really *was* up: the run's own `run_shell(shape=login)` succeeded over AMQP right
before the flagged `login_shell` calls). So `no_raw_ssh` is working: Claude-**via-hermes** used raw-SSH
`login_shell` for login-node commands *after* the AMQP channel was proven, where Claude-**via-SDK** used
`run_shell(shape=login)` and passed. That's a real behaviour difference driven by the operator (hermes prominently
exposes hpc-bridge's `login_shell` tool), not the model. rich_gate/partition_choice also failed `partitions_offered`
— the control didn't surface the partition list the way the SDK path did. Both reinforce the same point: the
operator changes graded behaviour. (The one thing worth revisiting for a *benchmark* is whether `no_raw_ssh` should
fire on a quick login-node check when only a login channel — no compute block — is up; that's a grader-strictness
judgement, not a bug, and left as-is.)

**Net validity verdict:** the interactive persona results in this study are **not a valid model comparison** —
they measure the hermes interactive harness (operator differences: clarify interception [fixed], `login_shell` vs
`run_shell` tool choice, `partitions_offered` phrasing, token-heavy transcript-replay) at least as much as the
model. **What still stands:** the
*autonomous* results (happy_path etc., each operator's own path), the `compute_ran` *completion* signal (a model
that never provisions isn't a grader artifact), and the qualitative failure taxonomy. **What to distrust:** the
interactive pass-rate numbers and any "model A beats model B on the gates" claim drawn from them.

**To make interactive numbers trustworthy** (future work): run every model through the **same** operator (so the
comparison is model-vs-model, not operator-vs-operator); decide the deliberate grader-strictness calls for a
benchmark (e.g. whether `no_raw_ssh` should fire on a login-only check); and replace the prose transcript-replay
with a proper interactive driver (ACP, or a direct tool-call loop where the human-sim answers structured asks) —
which also cuts the input-token cost (a single control model was ~$5 of Argo spend, 1.7M input tokens, because
each turn re-sends the whole conversation + the guidance resource).

> [!success] Follow-up 6 — resolved by the ACP driver and the core-pair campaign (2026-09-08/09)
> The driver was built ([[ACP interactive benchmark driver]]: one persistent session, a human-sim turn policy, the
> graders tiered under benchmark mode), the objective was refined to **like models through a variety of harnesses**,
> and the first harness-axis measurement was run: **claude-sonnet-4.6 through hermes vs through Claude Code, 30/30,
> identical dialogue shape and call counts** — see [[Cross-harness benchmark - sonnet-4.6 core pair 2026-09-09]].
> With the operator held constant the "Claude 8/8 vs open 0" gap of this study is confirmed as an operator/model
> confound: the harness does not move a capable model; the model is the lever for the weaker ones.

## Reading

- The interactive comparison is confounded by the operator (above); treat the cross-model *interactive* numbers
  as provisional. The ceiling framing below applies to what survived: autonomous completion + the failure modes.
- The ceiling is **reliable multi-step tool USE over a long gated chain**, not hpc-bridge, not the hermes
  deferred-tool mechanism (ruled out above), and not merely model size or specialization: gpt-oss stalls after
  the gate (deferred AND direct), Devstral loops in tool-discovery, 405B provisions but
  skips the gate — all short of Claude's follow-through, across a mid model, an agentic-tuned 123B, and the 405B
  flagship. The hermes deferred-tool overhead (a `tool_search`/`tool_describe` round per tool) lengthens every
  chain and is a plausible common confound worth isolating.
- The plugin's *tools* are operator-neutral, but **guidance delivery is NOT** (correction up top): Claude-SDK is
  force-fed SKILL.md, hermes gets a lazy MCP pointer it may not fetch. So the open models ran effectively
  guidance-lighter than Claude — retract the "same guidance" framing. Guidance helps a weak model *engage* but
  (from the ablation) does not make it *competent* at the gated flow; that finding stands, the "operator-neutral"
  framing does not.

## Caveats / next

- **n=2–3 per cell** — enough for the stark gap + the modes, not precise rates. Node-skips thinned a few cells.
- Four open models spanning mid / agentic / flagship-scale, one profile (`site`), one inference provider (ALCF).
  Deferred-tool overhead is ruled out (Follow-up 3). Still open, for stronger external validity: **other
  inference providers** (OpenRouter/Together/Fireworks/… — many more models, incl. non-Claude frontier ones like
  GPT-4o / DeepSeek-V3 / Qwen, to see whether the split is *Claude-specific* or *frontier-vs-open*; needs a
  provider API key); a **reasoning model** (`Trinity-Large-Thinking` — cold-blocked this round, HTTP 503 for a
  15-min warm window); and a "recovered-after-correction" metric — no open-model run reached that state (they
  failed before a correction could land).

Bundles: `agentic/runs/*-{gated_provision,rich_gate,spend_refusal,partition_choice}` (each `record.json` carries
`operator`, `model`, `ablate_skill`, `failed`, and the interaction `kind`s). Sweep logs:
`agentic/runs/{xharness,ablation,smart,eager}-*-*.log`.

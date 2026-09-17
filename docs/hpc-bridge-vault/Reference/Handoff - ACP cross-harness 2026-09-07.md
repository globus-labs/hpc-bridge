# Handoff — ACP cross-harness interactive benchmark

> [!warning] Archived snapshot (2026-09-07) — superseded
> Kept for the record of how the ACP driver landed (#148) and the gotchas in §7. Its one open item, the
> turn-continuation blocker in §4, was resolved the next day by #150. Since then: trace sources (#151), Claude Code
> over ACP (#154, replacing #152) and the core-pair campaign. For the current state read
> [[ACP interactive benchmark driver]] and [[Cross-harness benchmark - sonnet-4.6 core pair 2026-09-09]].

_Snapshot: 2026-09-07. Focused handoff for the **cross-harness interactive benchmark** thread (driving hpc-bridge from a NON-Claude-Code harness over ACP, to measure how well third-party harnesses + various models operate it). PR **#148 merged to `main`** (squash `d307276`). One blocker remains before the paid campaign can resume — see **[Recovery](#recovery-the-one-thing-to-fix-next)**._

This is a *thread-scoped* handoff. For the repo-wide state read `HANDOFF.md`; for design rationale read the vault. This doc orients you to both, then tells you exactly where this effort stands and what to do next.

---

## 0. Orientation — read these first (in order)

1. **`CLAUDE.md`** (repo root) — the auto-loaded entry point; conventions (branch + PR, squash-merge, commit trailers), how to run tests, the gotcha that `HPC_BRIDGE_SEARCH_INDEX` lives in `.claude/settings.local.json`.
2. **The vault — `docs/hpc-bridge-vault/Home.md`** — the maintainer's map of how hpc-bridge works (the *why*). Plain markdown; `[[X]]` = the file `X.md`. If you're cold on the product itself, read `Happy path.md` first.
3. **`HANDOFF.md`** (repo root) — the repo-wide "now" (V1 sprint state, how-to-run, gotchas). This ACP thread is one strand on top of that.
4. **The design record for THIS work — `docs/hpc-bridge-vault/Planned/ACP interactive benchmark driver.md`** — the plan, the decision (harness-compat axis), the CONFIRMED-vs-known-issue log, and the recovery direction. **This is the single most important doc for this thread; keep it in step with the code.**
5. **The study — `docs/hpc-bridge-vault/Reference/Cross-harness study - gpt-oss-120b vs Claude.md`** — the earlier model-vs-model results and, crucially, the **confound analysis** (why the old interactive numbers were not a valid model comparison).
6. **Auto-memory** — `~/.claude/projects/-Users-gusellerm-Projects-hpc-bridge/memory/`, especially `hermes-alcf-operator.md` (the whole cross-harness journey #132→#148, in detail) and `cross-harness-portability.md`. `MEMORY.md` is the index.

---

## 1. What this effort is (one paragraph)

hpc-bridge normally runs inside Claude Code. To learn how well *other* harnesses + *other* models drive it, we added a **second operator**: [NousResearch **hermes-agent**](https://github.com/NousResearch/hermes-agent) running an **ALCF-hosted** (or Argonne **Argo**-gateway) model, driving hpc-bridge as an MCP server. The agentic harness (`agentic/`) can dispatch a scenario to either operator (`--operator claude|hermes`) and grade both with the **same invariants** over a normalized `Trace`. The **benchmark axis is HARNESS COMPATIBILITY** — all models run through the *same* operator (hermes over its ACP server) with the persona'd human-sim answering — **not** raw model capability (that would need a minimal direct tool-call harness; still unbuilt, observer rec #2). Why: the operator (guidance delivery, native `AskUserQuestion` vs prose, scaffolding) dominates the model signal, so "Claude 8/8 via Claude-SDK vs open-models 0 via hermes" conflated operator with model (walked back in #143 — see the study's confound section).

---

## 2. What shipped in #148 (on `main`, `d307276`) — validated

The **ACP interactive driver**. ACP = Agent Client Protocol (Zed's agent↔client standard; the `agent-client-protocol` Python package, installed in the jail image — hermes' own `[acp]` extra). It replaces the fragile per-turn `hermes -z` **transcript-replay** with **one persistent session**: no conversation re-send (cheap), `prompt()` returning *is* the turn boundary, a structured update stream, and the human-sim in the loop.

- **`agentic/harness/acp_client.py`** — agent-agnostic ACP client. `run_session(command, args, task, *, respond, max_turns)` drives one session; `BenchClient(acp.Client)` auto-approves `request_permission`, records the `session/update` stream (`AcpCapture`), and prints each tool call live to stderr (`  → tool(args)` — parity with the Claude-SDK operator's play-by-play; `_fmt_call`). Hermetically tested (`test_acp_client.py`, `acp` stubbed).
- **`agentic/harness/hermes_runner.py` → `_run_acp`** — gated behind env **`HPCB_HERMES_ACP=1`**. Autonomous = one prompt; interactive = a prompt↔human-sim loop, capped at `MAX_PROSE_FOLLOWUPS`. Strips the config's `mcp_servers` (ACP `new_session` registers hpc-bridge). The `-z` transcript-replay path stays the default when the env is unset.
- **`agentic/harness/hermes_trace.py` → `exchanges_from_messages`** — the gate-stamping fix (see §3). Correlates the human-sim's prose Q&A to the graded trace **post-run from the flushed `state.db`**.
- **`run_smoke.sh` / `run_suite.py`** forward `HPCB_HERMES_ACP` + `HPCB_BENCHMARK_MODE`.

**Live-validated** (fake `site` cluster, benchmark mode): gpt-oss-120b and claude-sonnet-5 (via Argo) both drive `gated_provision` over ACP; `spend_follows_question` correct in both. Hermetic tests + mypy + ruff green (`python -m pytest agentic/harness -q` from that dir; CI lists `test_acp_client.py` + `test_hermes_trace.py`).

---

## 3. The two bugs found + fixed this session (context for the code you'll touch)

Both surfaced only when we ran the first *paid* campaign — the campaign did its job.

1. **Live `→` logging printed nothing** — `_fmt_call` was widened to 3 args but the call site stayed at 2; the `TypeError` was swallowed by a `contextlib.suppress`. Lesson (also filed as in-repo feedback): **test the integration, not just the unit**, and a broad `except` can hide a developer error. Fixed + `test_acp_client.py` drives `session_update` end-to-end.
2. **Gate grading (`spend_follows_question` / `choice_respected`) false-failed intermittently** — `_run_acp` stamped the prose Q&A from the **ACP capture**: the index came from `len(capture.tool_calls)` (counts the operator's file/search exploration → skew vs the `state.db`-derived trace), and the question text was `" ".join(chunks)` (**merged setup narration** — "installing…interface…scratch" — into the ask, tripping `_is_spend_question`'s setup-veto in `invariants.py`). **A first patch read `state.db` mid-session → that LAGS hermes' flush → ended turns early → `compute_ran` false-fail → REVERTED.** Correct fix (`exchanges_from_messages`): **post-run** from the fully-flushed `state.db`, correlated by message order — each human reply is a `user` row after the first (the task); trace index = tool-calls-before-it (counted exactly as `trace_from_messages` counts), question = the preceding clean assistant message. Hermetic regression test in `test_hermes_trace.py`.

---

## 4. Recovery — THE one thing to fix next

> **2026-09-08 update.** Objective refined by the user after a methods review: the benchmark compares **like models
> through a VARIETY of harnesses** (one ACP driver, one human-sim) to claim cross-harness capability — not several
> models through hermes. Ordered plan + rationale: vault `Planned/ACP interactive benchmark driver.md` (the
> "Objective refined" callout) and memory `cross-harness-benchmark-objective.md`. **Turn-continuation is now
> IMPLEMENTED as a tested policy** (`HumanSim.move` reply/nudge/conclude + guards; `hermes_runner.AcpResponder`;
> nudges stamped as `user_nudge`, never as a question) on branch `feat/acp-turn-continuation` — see the plan doc's
> status for the live-validation state. The text below is the pre-fix description, kept for context.

**Turn-continuation.** The ACP loop replies **only** when the operator's turn ends with a *question* (`ends_with_question` in `human_sim.py`). A **decisive** operator (sonnet-5) that brings up the login node, runs `sinfo`, then ends its turn with a **plan/statement** ("I'll provision debug next") — *not* a question — gets no reply, so the ACP session ends before it provisions compute → **`compute_ran` false-fails** (`answer×1`). Weaker gpt-oss completes only because it keeps *asking* (more nudges). This is **pre-existing** (version A had the same detection; the first standalone sonnet-5 completed only because it happened to ask twice) and **orthogonal to the stamping fix** — the stamping fix is solid.

**Fix = a persona-aware human-sim "continue vs conclude" nudge.** After each operator turn the human-sim should decide:
- asked a question → **answer** it (as today);
- paused mid-task (no question, task not done) → **nudge** ("go ahead / please continue");
- task done (stopped the endpoint / wrapped up) → **conclude** (end the loop);
- **HARD CONSTRAINT: never nudge a legitimate decline into spending** — must not break `spend_refusal` (the `declines_spend` persona). A decline is a conclusion, not a pause.

Where to implement: extend `human_sim.reply_hermes` (or add a sibling) to classify + produce the right reply, and change `_run_acp`'s `respond` closure so it calls the human-sim on *every* turn rather than gating on `ends_with_question` first. Keep the `MAX_PROSE_FOLLOWUPS` cap. **Land it with hermetic tests** (persona-aware classification: answer / nudge / conclude / decline; and a synthetic-loop test that a paused-mid-task turn gets nudged while a decline does not), then a **free gpt-oss + one paid sonnet-5** validation before any campaign. Don't iterate this against paid runs.

After it lands: **resume the paid campaign** (§5). Until then, the interactive **pass-RATE numbers are NOT final** — `compute_ran` is noisy for capable models and the paid campaign is on hold.

---

## 5. How to run (verified this session)

Prereqs: Docker running; the fake cluster up (`agentic/fakecluster/bin/up.sh`, `site` profile — 3 nodes, debug/compute/gpu partitions, `mybalance`); `agentic/.env` holding `CLAUDE_CODE_OAUTH_TOKEN` (the human-sim runs on the Claude subscription — **not** the operator). The ALCF inference token is minted by `run_smoke.sh` at run time.

**Free — gpt-oss-120b over ALCF:**
```bash
HPCB_TARGET=fake HPCB_FAKE_PROFILE=site HPCB_OPERATOR=hermes HPCB_HERMES_ACP=1 \
  HPCB_BENCHMARK_MODE=1 HPCB_ALCF_MODEL=openai/gpt-oss-120b \
  ./agentic/run_smoke.sh gated_provision
```
**Paid — claude-sonnet-5 via Argo** (needs the ANL VPN on + `argo-up` from `~/Projects/argo-proxy`; the jail reaches the tunnel via `host.docker.internal`, added automatically because the base URL contains it; streaming auto-on so `argo-dash` can meter):
```bash
HPCB_TARGET=fake HPCB_FAKE_PROFILE=site HPCB_OPERATOR=hermes HPCB_HERMES_ACP=1 \
  HPCB_BENCHMARK_MODE=1 \
  HPCB_ALCF_BASE_URL=http://host.docker.internal:44497/v1 HPCB_ALCF_MODEL=argo:claude-sonnet-5 \
  ./agentic/run_smoke.sh gated_provision
```
Add `HPCB_SKIP_BUILD=1` to reuse the image after the first build. `HPCB_BENCHMARK_MODE=1` demotes the operator-preference graders (`no_raw_ssh_after_endpoint_up`, `partitions_offered`, …) to **report-only** — safety + liveness graders still gate. Meter Argo with `argo-dash --mark` then `--since-mark` (only *streamed* requests are metered). One sonnet-5 `gated_provision` ≈ **$1.87** (ACP is far cheaper than transcript-replay's ~$5/model).

**The campaign matrix:** 3 hook-free interactive scenarios — `gated_provision` (cooperative), `rich_gate` (budget_hawk), `spend_refusal` (declines_spend) — × n=3 × {gpt-oss free, sonnet-5 paid}. **`spend_revoked` is EXCLUDED** — it uses `MIDRUN_HOOKS` (an interject) that the hermes operator raises `NotImplementedError` on; the unprompted mid-provision revoke can't come through the question-driven loop (a possible later feature). Prefer a sequential loop over `run_smoke.sh` (one cell at a time — no fake-cluster node contention, clean metering) or `run_suite.py`. Run each model as its **own** invocation (the operator model is a whole-suite env choice: `HPCB_ALCF_MODEL`); `--models` is the *Anthropic* axis and mislabels hermes cells.

---

## 6. Spend & guardrails

Today's Argo spend ≈ **$8.41** (user's cap for that day was **$50**, raised from an earlier $20). The paid campaign was stopped early on the stamping bug — good instinct: **stop paid runs the moment a key grader looks unreliable.** claude-sonnet-5's Argo intro pricing expired 2026-08-31, so it's at standard rate now. Always `argo-dash --mark` before a paid batch and watch `--since-mark` against the ceiling. The user brings up the VPN/Argo themselves.

---

## 7. Gotchas (that cost time)

- **hermes must be the editable 0.21.0 git install** (commit pinned in `agentic/Dockerfile`) — PyPI 0.19.0 is broken for MCP (`CallToolResult.isError`). It blocks non-editable wheel builds.
- **`.env` model must be a tool-caller** — gpt-oss tool-calls via hermes; **Llama-3.3-70B does NOT**; 405B does. ALCF big models **auto-scale down when idle** → HTTP 503 "online but not ready"; warm by polling `/chat/completions` until 200 (405B was 503 the whole ~13 min window this session — don't block on it).
- **hermes double-encodes** deferred-tool results (`{"result": "<json string>"}`) and nests dispatcher args under `"arguments"` (gpt-oss) or `"parameters"` (Devstral) — `hermes_trace._unwrap` / `_unwrap_dispatcher_result` handle both; if a trace looks empty to graders, suspect this first.
- **`HPCB_HERMES_NO_CLARIFY`** — interactive runs drop hermes' `clarify` tool (in `-z` it auto-answers "no user — decide yourself", bypassing the persona → false spend-gate failures). `_run_acp`/`run_scenario` sets it when a persona is present.
- **Reading `state.db` mid-run LAGS** hermes' flush — do post-run reads (the reverted first patch is why). The ACP **capture** is the reliable *live* source (turn text, tool calls); the **`state.db`** is the reliable *post-run* source (clean messages, the graded trace). Use each for what it's good at.
- Whenever you add a hermes/ACP env knob, forward it in **both** `run_suite.py` `_CELL_ENV_KEEP_PREFIXES` **and** `run_smoke.sh`'s docker `-e` list, or cells silently run without it.

---

## 8. Pointers (files & docs)

| Thing | Where |
|---|---|
| ACP client | `agentic/harness/acp_client.py` (+ `test_acp_client.py`) |
| hermes operator / `_run_acp` | `agentic/harness/hermes_runner.py` |
| trace + gate stamping | `agentic/harness/hermes_trace.py` (`exchanges_from_messages`, `stamp_exchanges`) (+ `test_hermes_trace.py`) |
| graders | `agentic/harness/invariants.py` (`spend_follows_question`, `_is_spend_question`, `OPERATOR_PREFERENCE_GRADERS`, `guidance_fetched`) |
| the human-sim (where the nudge goes) | `agentic/harness/human_sim.py` (`reply_hermes`, `ends_with_question`) |
| operator dispatch + benchmark mode | `agentic/harness/run.py` |
| run one cell / a matrix | `agentic/run_smoke.sh` · `agentic/run_suite.py` |
| **design + known-issue + recovery** | `docs/hpc-bridge-vault/Planned/ACP interactive benchmark driver.md` |
| study + confounds | `docs/hpc-bridge-vault/Reference/Cross-harness study - gpt-oss-120b vs Claude.md` |
| the full journey (detail) | memory `hermes-alcf-operator.md` |

---

## 9. Backburner (raised, parked)

**Job recovery across chat sessions** — recover an in-progress HPC job after a chat failure; a second agent reattaches (hpc-bridge already has the substrate: persistent endpoint + zero-SSH reconnect + `poll_task` handle). The user wanted to revisit this *after* the cross-harness work. See memory `job-recovery-across-sessions.md`.

---

_Start at §0, then go to the plan doc (§8). The immediate task is §4 (the turn-continuation nudge); the campaign in §5 resumes only after it lands with tests + a free/1-paid validation._

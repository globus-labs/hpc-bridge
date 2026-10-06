# Vault style guide

How to write and maintain notes in this vault — for human contributors **and** agents. If you add or change a note, follow this.

## What the vault is

A maintainer's map of hpc-bridge in two halves:

- **Implemented (ground truth)** — `Concepts/`, `Modules/`, `Reference/`. Describes how the code works *today*. It **tracks the codebase**: when you change code, update the matching note in the same change.
- **Planned (transient)** — `Planned/`. Designed work and refactors; expected to churn and be deleted as features land. Never let planned ideas leak into the implemented notes — if it isn't in `src/`, it isn't ground truth.

[[Home]] is the map of content (MOC); every note should be reachable from it.

## Note types & where they live

| Type | Folder | Answers | Example |
|---|---|---|---|
| **Concept** | `Concepts/` | "how does X work?" — a mechanism that spans modules | [[Two-channel architecture]] |
| **Module** | `Modules/` | "what is this file?" — one note per `src/hpc_bridge/*.py` | [[server]] |
| **Reference** | `Reference/` | the external surface (tools, packaging, config) | [[The MCP tools]] |

**Naming:** module notes are named after the file with `/` → `-` (`facility/remote.py` → `facility-remote`). Concept/reference notes use a plain-English title. Filenames *are* the wiki-link targets — Obsidian resolves by basename, so keep them unique.

## The note template

Keep notes **concise** — the vault *explains*; the code holds the detail. Aim for half a page to a page.

```markdown
# <title>

> [!abstract] Role            ← modules: "Role"; concepts: "In one line"
> One or two sentences.

## What (it is / it does)
## How it works               ← prose; tables for tool/field lists

> [!warning] <invariant>      ← only the load-bearing traps (see below)

## See also
[[linked]] · [[notes]]        ← footer; build the graph
```

Modules add a short **code-anchor** style: cite the symbol with its line, e.g. `_provision` (`:297`) or `ssh_exec` (`remote.py:55`).

## Conventions

- **Code anchors:** cite `symbol` + `:line`. The *symbol* is the durable anchor (survives line drift); the line is a convenience. When you move code, fix the anchors — they are part of "tracks the codebase."
- **Callouts** (`> [!type]`): use sparingly and meaningfully.
  - `[!abstract]` — the one-line role at the top (every note).
  - `[!warning]` — a **load-bearing invariant or trap**: the thing that silently breaks if you get it wrong (e.g. the `compute` bool, login-node pinning, "warm = a worker answered"). If it wouldn't cause a real bug, it's not a warning.
  - `[!note]` / `[!info]` — secondary context, reading order.
- **Wiki-links:** link the first mention of any other note; always include a **See also** footer. Prefer linking the relevant Concept note over re-explaining it. A link to a not-yet-written note is fine — it's a stub marker, not an error.
- **Mermaid:** use a `mermaid` fenced block when a *flow* beats prose (architecture, lifecycle). Don't diagram what a sentence covers.
- **Voice:** present tense, active, factual. Describe what the code *does*, not what it *should* do — opinions and plans go in `Planned/`.
- **Issues & PRs — link them, don't just number them.** A bare `#5` is *not* a clickable link in Obsidian (and not a valid tag either, so it's inert there); GitHub auto-links it but Obsidian doesn't. Use a full markdown link so it works in both:
  `[#5](https://github.com/globus-labs/hpc-bridge/issues/5)` — the `/issues/N` path also resolves a PR `N`, so one form covers both. Cite one when:
    - a `[!warning]` invariant **defends against a real bug** → link the issue/PR that found or fixed it, so the note carries its own provenance;
    - a statement is **designed-not-built** → link the tracking issue;
    - a `Planned/` note exists for the work → link its issue as the single source of churn.

## Adding or updating a note (the recipe, esp. for agents)

1. **Read the ground truth** — the module(s) the note covers. Don't write from memory; cite real symbols/lines.
2. **Follow the template** above; keep it concise.
3. **Link it** — add `[[wiki-links]]` to related notes and a See also footer, and ensure it's listed on [[Home]] (drop the `*(stub)*` marker once written).
4. **Capture invariants, not narration** — a `[!warning]` for each genuine trap the code defends against; skip the obvious.
5. **If you changed code,** update the matching note in the same PR — the implemented half must not drift.

> [!note] Scope discipline
> Implemented notes describe *only* what `src/` does now. When something is designed-but-unbuilt, it belongs in `Planned/` (or is linked as "designed, not built"), never asserted as current behaviour.

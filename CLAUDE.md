# CLAUDE.md — hpc-bridge

hpc-bridge is a Claude Code plugin / FastMCP server that makes an HPC batch machine feel like a REPL: it stands up (or reuses) a Globus Compute endpoint on a login node — over SSH once, or via a facility multi-user endpoint with **zero SSH** — then dispatches shell work to a warm compute block over AMQP.

## Start here (building context)

- **Design / how it works:** read the vault — start at **`docs/hpc-bridge-vault/Home.md`** (the maintainer's map; it has its own reading order). Plain markdown; `[[X]]` means the file `X.md` in the vault. `Happy path.md` is the fastest end-to-end overview; `Modules/<name>.md` documents each `src/hpc_bridge/` module.
- **Current state / how to run / what's next:** **`HANDOFF.md`** at the repo root (live snapshot, gotchas, follow-ups).

The vault holds the *why*; `HANDOFF.md` holds the *now*. Read both before making non-trivial changes; keep the vault in step when you change how something works.

## Running tests

```bash
python -m pytest -q                                    # unit tier (hermetic, fast) — the default gate
python -m pytest agentic/harness/test_invariants.py -q # the agentic grading core (also hermetic)
uv run ruff check .                                    # lint — CI-enforced
uv run --with mypy python -m mypy                      # types — CI-enforced; strict on the leaf modules (see pyproject)
```

`agentic/` is a **separate live-agent regression tier**: it drives a headless agent against the real globus1 cluster in a container and **costs money** (bills the Claude subscription). It is NOT part of `pytest -q`. Do not launch a live agentic run unprompted — see `agentic/README.md`.

## Conventions

- End commit messages with: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
- End PR bodies with: `🤖 Generated with [Claude Code](https://claude.com/claude-code)`
- Work on a branch and open a PR (squash-merged into `main`); don't commit to `main` directly.
- `agentic/.env` and `.claude/settings.local.json` are gitignored — never commit secrets.
- `HPC_BRIDGE_SEARCH_INDEX` (the runtime catalog) lives in `.claude/settings.local.json`, so it's set for the MCP server but **not in your shell** — pass it literally when running catalog tooling.

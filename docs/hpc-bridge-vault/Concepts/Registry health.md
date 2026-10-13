# Registry health

> [!abstract] In one line
> The registry is only as good as its last proof. Two tiers keep it honest: a **free drift check**, run on demand
> (does the index serve the seeds; is each facility still what its entry was proven against; what does a fresh install
> get), and a **paid re-prove** (a real block per facility) when the check finds drift and before any event.

## Why (2026-10-06)
Re-proving the four registry facilities found three broken in ways no entry could see: Anvil's endpoint floated to
a newer parsl than our worker, Delta's fixed install sat on an older one than a fresh worker venv picks (the block
ran and billed while every result was dropped, and Delta kept relaunching GPU blocks for the queued check), and
Expanse's `shared` partition could never start a whole-node block. Three days later the health check's first run
found the fourth: a fresh `uvx --from git+…` install resolved `globus-compute-sdk` 4.18.0 against a 4.16.0 lock,
because `uvx` ignores `uv.lock` — every Pi / Hermes / Codex user ran an untested client.

## The free tier — `hpc-bridge-registry-health`
`src/hpc_bridge/catalog/health.py`; exit 0 ok / 1 warn / 2 fail; `--state` remembers findings and alerts only on new
ones (keyed on the whole finding, so an escalation re-alerts); `--notify` posts a desktop notification (the text is
passed as an argument, never spliced into AppleScript). A missing or expired Globus login is a `fail` finding (the
other checks still run), not a crash.

**Run it on demand — it is not scheduled** (maintainer's decision, 2026-10-09). When to run it: before an event or a
demo, when a facility announces an upgrade or maintenance, when a session fails in a way the entry should have
prevented, and after any re-ingest. How: from anywhere, against main's seeds,
`uvx --refresh --from git+https://github.com/globus-labs/hpc-bridge hpc-bridge-registry-health`; from a checkout on
main, `uv run hpc-bridge-registry-health`. It needs a Globus login with the Search scope (the same one
`hpc-bridge-catalog` uses). `scripts/registry_monitor.sh install [MIN]` would schedule it as a launchd job (main's
build via `uvx --refresh`, alerting on new findings) if that is ever wanted.

| check | what it compares | catches |
|---|---|---|
| `index` | every seed vs the live index, field for field (as the client parses it); subjects with no seed | a forgotten re-ingest, a retired entry still listed |
| `facility` | a facility endpoint's status and published metadata vs the entry's `verification` block: endpoint version, Python, template+schema digest, manager-config digest | an upgrade, a redeploy, a template change, an outage |
| `ssh` | the login host answers TCP :22 | a renamed or retired login host (a login needs the user) |
| `releases` | the latest parsl vs the parsl the worker reported when a `float` entry was proven (by date if none recorded) | the window where a floating endpoint and our worker disagree |
| `install` | what `uv pip compile` resolves from main's pyproject today (py3.13, py3.12) vs main's `uv.lock` | an SDK or dill a fresh install would get untested |

What it cannot see: the facility endpoint's **parsl** (not published — only a live block shows it; the canary now
reports the worker's), a partition's QOS rules, an SSH facility past its login prompt.

## The paid tier — `agentic/registry_reprove.py ENTRY [--account A] --record`
One real block through hpc-bridge's real functions (connect → confirm spend → worker answers → `hostname` → stop).
On a pass, `--record` writes the entry's `verification` block (date, versions, digests, and what the worker reported:
Python, dill, **parsl**, node) and bumps `last_validated` and `worker_env.verified_with`, editing the seed text in
place (comments kept; the edit is dry-run before the paid block). Then commit, PR, ingest. It refuses to record if
the facility's metadata changed during the run, stops the endpoint whatever happens from the first submit on (an
error, a timeout, Ctrl-C), and on a run that did not pass warns that a check task may still be queued at the facility,
starting billed blocks — check its queue. Ingest refuses a facility-endpoint block without its date and endpoint
version; a field the facility does not publish is left out and reported by the health check as `incomplete`. Facility endpoints only; Expanse (one-time code) is re-proven by hand with the user present.

Cost per run: one block for a few minutes (≈0.05 GPU-h on Delta, ≈0.05 SU on Anvil, free on the lab cluster).

## How fixes reach users
- **Registry fixes reach everyone on their next connect**: the plugin reads the live index first (the local cache
  only serves when the index is unreachable). Re-ingest is the fast path.
- **Plugin code fixes do not**: `uvx` reuses its cached environment after the first run (uv docs, "Tool versions"),
  so a participant needs `uvx --refresh …` once; Claude Code users need the plugin to update (the manifest version).
- **The client SDK is pinned** (`globus-compute-sdk==4.18.0`) to the version the facilities were proven with, so a
  fresh install cannot drift to an untested one; moving the pin means a re-prove first.

## Event runbook (a hackathon, a demo)
1. **T−1 day:** re-prove every facility (`--record`), commit, ingest; `hpc-bridge-registry-health` all ok.
2. **During:** run the drift check at the start of each session block and whenever a participant hits something
   odd. If a Monday falls inside the event, re-prove the `float` facilities (Anvil) after Monday's parsl release
   (~22:45 UTC).
3. **A facility goes red:** read the finding. Version/Python/template moved → re-prove it; if the worker no longer
   answers, fix its `worker_env`/`env_setup` in the seed, re-prove, ingest — users get it on their next connect. Tell
   anyone mid-session to `connect_facility` again. A facility you cannot fix in time: say so to participants (the
   plugin's STALE ENTRY note will too) and steer them to another.
4. **A stuck billed block on a facility endpoint** (the Delta failure): the user's `squeue` shows it RUNNING;
   `scancel` it and stop the session; if blocks keep reappearing, the facility's endpoint is relaunching for a queued
   check — fix the worker so the queued task can finish, or ask the facility.

## Who can use what
A registry entry is necessary, not sufficient: a facility endpoint maps only identities with an account there (Delta,
Anvil), the lab cluster maps only its own users, and an SSH facility needs an account. A participant without one gets
a clear `NO ACCOUNT` / `NO SSH ACCESS`, nothing billed.

See also: [[Facility catalog]] (`worker_env`, `verification`), [[Warmth, the canary & cold-start]], [[Cost control]].

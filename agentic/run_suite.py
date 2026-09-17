#!/usr/bin/env python3
"""Staggered, capped suite runner for the agentic harness.

Runs a matrix of (scenario × model × effort × persona × ablation × repeat) across the POOL of
test users, ≤N in parallel, STAGGERED so concurrent SSH bootstraps from one host don't trip
globus1's per-source new-connection rate limit. Each job is one `run_smoke.sh` invocation — a
fresh container, a DISTINCT pool user (so squeue/home/storage.db don't bleed), a unique
endpoint. Aggregates pass rates per cell (`model @ effort [persona] ~ablation`).

Run from the repo root (agentic/.env supplies the token + Globus db):

    python agentic/run_suite.py --scenarios happy_path --repeat 4
    python agentic/run_suite.py --scenarios happy_path \
        --models claude-opus-4-8,claude-sonnet-4-6 --efforts low,high,max --repeat 3
    python agentic/run_suite.py --scenarios happy_path --repeat 6 --concurrency 6   # induce contention
"""
from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SMOKE = REPO / "agentic" / "run_smoke.sh"
sys.path.insert(0, str(REPO / "agentic" / "harness"))
import targets  # noqa: E402  (the cluster a run targets: globus1 | fake)
from cluster_ops import delete_endpoint_cmd, endpoint_uuid_cmd, scoped_cancel_cmd, uep_dirs_cleanup_cmd  # noqa: E402
from pool import PoolClaims  # noqa: E402  (cross-process pool-user claims — see pool.py)

POOL = [f"hpcbridge-test-{i:02d}" for i in range(10)]
_TARGET = targets.get()  # replaced by --target in _main; module-level so the probe/cleanup helpers can read it
FAKE_BIN = REPO / "agentic" / "fakecluster" / "bin"
DEFAULT_MODEL = "claude-opus-5"
# A gated cell's Slurm block is not submitted until 115–196 s into the run (09-03 endpoint logs), so a node it
# will take still reads `idle` for that long. Launches within this window count against the idle count.
NODE_CLAIM_S = 300.0
# On interrupt, how long a terminated cell gets to tear itself down before the suite cleans up after it.
CELL_STOP_GRACE_S = 150.0
# Knobs that must NOT leak from the operator's shell into every cell (a persisted HPCB_NO_SKILL ablated a
# baseline cell; HPCB_EFFORT relabelled a whole matrix — review 2026-09-05). These prefixes are the exceptions.
# HPCB_OPERATOR is one: a whole-suite choice (which harness drives — claude|hermes), set once for the invocation
# and meant to reach every cell, so run a cross-operator comparison as two invocations that differ only in it.
# HPCB_ALCF_* (the hermes operator's model + base URL) are the same kind of whole-suite choice — kept so a
# smart-model comparison is another invocation differing only in HPCB_ALCF_MODEL (else run_smoke falls back to .env).
_CELL_ENV_KEEP_PREFIXES = ("HPCB_TEST_", "HPCB_POOL_", "HPCB_NODE_", "HPCB_FAKE_", "HPCB_TARGET", "HPCB_OPERATOR",
                           "HPCB_ALCF", "HPCB_HERMES_EAGER",   # HPCB_HERMES_EAGER_TOOLS: expose MCP tools directly
                           "HPCB_HERMES_ACP",   # drive hermes over one persistent ACP session (not -z replay)
                           "HPCB_BENCHMARK",    # HPCB_BENCHMARK_MODE: operator-preference graders → report-only
                           "HPCB_CLAUDE_ACP")   # the claude-acp operator's model pin (a whole-suite choice, like HPCB_ALCF_*)


class Stagger:
    """Ensure launches are >= `seconds` apart, so concurrent SSH bootstraps don't burst past
    globus1's per-source new-connection rate limit."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + self.seconds
        if delay:
            await asyncio.sleep(delay)


def _git_describe() -> str:
    """What the image is built from — `git describe --always --dirty`, so a bundle can pin the code it ran
    (the launch-time HEAD can move under a long suite: one 09-03 image was recorded under two SHAs)."""
    try:
        return subprocess.run(["git", "-C", str(REPO), "describe", "--always", "--dirty", "--abbrev=12"],
                              capture_output=True, text=True, timeout=20).stdout.strip() or "unknown"
    except Exception:  # noqa: BLE001 - no git: still runnable
        return "unknown"


async def _build_once() -> bool:
    print("building the jail image once (parallel jobs reuse it)…", flush=True)
    proc = await asyncio.create_subprocess_exec(
        "docker", "build", "--provenance=false", "-t", "hpc-bridge-agentic",
        "--build-arg", f"GIT_DESCRIBE={_git_describe()}",
        "-f", str(REPO / "agentic" / "Dockerfile"), str(REPO),
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
    )
    _, err = await proc.communicate()
    if proc.returncode != 0:
        print(f"build FAILED:\n{err.decode(errors='replace')[-1500:]}", file=sys.stderr)
        return False
    return True


def _knobs(scenario: str) -> dict[str, str]:
    """The scenario module's host-side knobs (via harness/scenario_knobs.py, importable on the host)."""
    import subprocess
    try:
        out = subprocess.run([sys.executable, str(REPO / "agentic" / "harness" / "scenario_knobs.py"), scenario],
                             capture_output=True, text=True, timeout=30).stdout
    except Exception:  # noqa: BLE001 - unknown scenario / import error: run.py reports it; no knobs
        return {}
    import shlex
    knobs = {}
    for ln in out.splitlines():
        if "=" not in ln:
            continue
        k, v = ln.split("=", 1)
        # scenario_knobs.py shell-quotes values for run_smoke.sh's `eval`; here we read them as data
        knobs[k] = shlex.split(v)[0] if v and v[0] in "'\"" else v
    return knobs


def _is_serial(scenario: str) -> bool:
    k = _knobs(scenario)
    return k.get("HPCB_KNOB_SERIAL") == "1" or int(k.get("HPCB_KNOB_COOLDOWN_S", "0") or 0) > 0


def _cooldown_s(scenario: str) -> int:
    """Seconds run_suite waits after each cell of `scenario` before the next one (holding its serial
    lock): a scenario whose cells are deliberate failed SSH auths must stay under the cluster's
    fail2ban maxretry/findtime — six back-to-back cells got the harness' egress banned (2026-09-03)."""
    return int(_knobs(scenario).get("HPCB_KNOB_COOLDOWN_S", "0") or 0)


def _needs_nodes(scenario: str) -> int:
    """Idle nodes a cell needs at launch (HPCB_KNOB_NEEDS_NODE, derived by scenario_knobs.py from the scenario's
    NEEDS_COMPUTE_NODE or its `compute_ran` grader). 0 = launches ungated."""
    try:
        return int(_knobs(scenario).get("HPCB_KNOB_NEEDS_NODE", "0") or 0)
    except ValueError:
        return 0


def _requires(scenario: str) -> dict:
    """REQUIRES: the cluster capabilities a scenario needs (matched against the target's — targets.meets)."""
    import json
    raw = _knobs(scenario).get("HPCB_KNOB_REQUIRES")
    return json.loads(raw) if raw else {}


def _allowed_targets(scenario: str) -> set[str] | None:
    """TARGETS a scenario declares itself runnable on (None = any). A chaos scenario that kills endpoints and blocks
    says `TARGETS = ("fake",)` — the first declarative coupling of scenario to cluster (2026-09-05)."""
    raw = _knobs(scenario).get("HPCB_KNOB_TARGETS")
    return {t.strip() for t in raw.split(",") if t.strip()} if raw else None


def _needs_admin(scenario: str) -> bool:
    """ADMIN_SETUP/ADMIN_CLEANUP: cluster-admin world changes run_smoke.sh applies through the target's admin channel
    (fake: docker exec into slurmctld). A target without one (a real facility — we are not its admin) skips the cell."""
    k = _knobs(scenario)
    return bool(k.get("HPCB_KNOB_ADMIN_SETUP") or k.get("HPCB_KNOB_ADMIN_CLEANUP"))


def _warm_block_user(scenario: str) -> str | None:
    """WARM_BLOCK_USER: a facility MEP's block runs as its mapped user; while one is RUNNING the cell reuses it."""
    return _knobs(scenario).get("HPCB_KNOB_WARM_BLOCK_USER") or None


def _probe_ssh(remote: str) -> str | None:
    """One command on the target cluster, from the host (globus1: the operator's ssh alias; fake: the login
    container's published sshd as a pool user). None when ssh itself failed."""
    try:
        r = subprocess.run([*_TARGET.probe_argv, remote], capture_output=True, text=True, timeout=60)
    except Exception:  # noqa: BLE001 - a transport failure just means "unknown"
        return None
    return r.stdout if r.returncode == 0 else None


def _idle_nodes() -> int | None:
    """Nodes on the target's default partition (HPCB_NODE_PARTITION overrides) whose short state is EXACTLY `idle`. None =
    the probe failed or answered something unparseable (a misnamed partition) — "unknown", never 0.

    Per-node `%t`, not `sinfo -t idle -o %D`: Slurm's `-t idle` filter matches the base state, so a DRAINED node
    (`drain` = idle+drained, unusable) counted as idle — live 2026-09-05 the gate launched a block cell onto a
    cluster whose only "idle" node was globus2, drained with "Duplicate jobid"; the block PENDed and the cell
    failed `compute_ran`. `idle*` (not responding), `drain`, `drng`, `down`, `mix`, `alloc` are all not idle."""
    if str(_TARGET.capabilities.get("scheduler", "slurm")) == "pbs":
        # PBS: a node is usable when pbsnodes reports `state = free` (offline/down/job-busy/job-exclusive are not);
        # queues are not partitions, so the count is cluster-wide. `bash -lc`: /opt/pbs/bin is a login-shell PATH.
        out = _probe_ssh("bash -lc 'pbsnodes -a 2>/dev/null' | awk '$1==\"state\"{print $3}'")
        if out is None:
            return None
        states = [ln.strip() for ln in out.splitlines() if ln.strip()]
        if any(not st.replace("-", "").isalpha() for st in states):
            return None
        return sum(1 for st in states if st == "free")
    part = os.environ.get("HPCB_NODE_PARTITION") or str(_TARGET.capabilities.get("default_partition") or "main")
    out = _probe_ssh(f"sinfo -h -p {part} -N -o %t")
    if out is None:
        return None
    states = [ln.strip() for ln in out.splitlines() if ln.strip()]
    if any(" " in st or not st.replace("*", "").replace("~", "").replace("#", "").isalpha() for st in states):
        return None  # not a state column (an error message came back on stdout)
    return sum(1 for st in states if st == "idle")


def _warm_block_running(user: str) -> bool | None:
    """Is a block of `user` RUNNING on the cluster (a facility MEP's warm block)? None when the probe failed."""
    out = _probe_ssh(f"squeue -u {user} -h -t R -o %j")
    if out is None:
        return None
    return bool(out.strip())


class NodeGate:
    """Admits a compute cell only when the partition can take its block RIGHT NOW: idle nodes minus the blocks
    that cells launched in the last NODE_CLAIM_S will still submit (in-flight accounting) must cover the need.

    Replaces the exactly-one-idle special case: with two idle nodes and --concurrency 3, three cells used to
    read "2 idle" within seconds and all launch — one starved PENDING for its whole run (the 2026-09-03
    starvation the operator hand-serialised around). A WARM_BLOCK_USER whose block is running satisfies the
    need outright (the MEP pair reuses that block). Probe failure ⇒ launch unguarded (never never-launch)."""

    def __init__(self, clock=time.monotonic) -> None:
        self._launches: list[float] = []
        self._lock = asyncio.Lock()
        self._clock = clock

    def _recent(self) -> int:
        now = self._clock()
        self._launches = [t for t in self._launches if now - t < NODE_CLAIM_S]
        return len(self._launches)

    async def admit(self, label: str, need: int, warm_user: str | None, halt: asyncio.Event,
                    max_wait_s: int) -> int | None:
        """Block until admitted. Returns the idle count seen (>= need), -1 when unguarded (probe failed or a warm
        block satisfied the need), or None when `max_wait_s` passed without capacity."""
        t0 = time.monotonic()
        last_note = -600.0
        while not halt.is_set():
            async with self._lock:  # probe + decision + claim are one step, so two cells can't take one node
                if warm_user:
                    warm = await asyncio.to_thread(_warm_block_running, warm_user)
                    if warm:
                        print(f"🖥 nodes  {label}: {warm_user}'s block is running — reusing it, no idle node needed",
                              flush=True)
                        return -1
                idle = await asyncio.to_thread(_idle_nodes)
                if idle is None:
                    print(f"⚠ nodes  {label}: node probe unavailable (ssh/sinfo) — launching unguarded", flush=True)
                    return -1
                free = idle - self._recent()
                if free >= need:
                    self._launches.append(self._clock())
                    print(f"🖥 nodes  {label}: {idle} idle, {self._recent() - 1} claimed by recent launches, "
                          f"need {need} — launching", flush=True)
                    return idle
            elapsed = time.monotonic() - t0
            if elapsed >= max_wait_s:
                return None
            if elapsed - last_note >= 600:
                print(f"⏳ nodes  {label}: {idle} idle minus {self._recent()} claimed < {need} needed — waiting "
                      f"({int(elapsed)}s of {max_wait_s}s)", flush=True)
                last_note = elapsed
            await asyncio.sleep(60)
        return None


def _short_model(m: str) -> str:
    parts = m.split("-")
    return parts[1] if len(parts) > 1 else m


def _cell(model: str, effort: str | None) -> str:
    return f"{model} @ {effort or 'default'}"


def _cell_env(user: str, runid: str) -> dict[str, str]:
    """The environment a cell's run_smoke.sh gets: the operator's shell MINUS stray HPCB_* knobs (kept: HPCB_TEST_*
    credentials, HPCB_POOL_*, HPCB_NODE_*), plus this cell's values. run_suite mints HPCB_RUNID so it can clean up
    a cell it has to abandon (see _emergency_cleanup)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("HPCB_") or k.startswith(_CELL_ENV_KEEP_PREFIXES)}
    env.update(HPCB_TEST_SSH_USER=user, HPCB_SKIP_BUILD="1", HPCB_RUNID=runid)
    return env


async def _terminate_and_wait(proc, grace_s: float) -> bool:
    """SIGTERM the cell's `docker run` client (proxied into the jail) and wait up to `grace_s` for it to exit on its
    own — True when it did (its teardown ran), False when it had to be left for _emergency_cleanup."""
    try:
        proc.terminate()
    except ProcessLookupError:
        return True
    try:
        await asyncio.wait_for(proc.wait(), timeout=grace_s)
        return True
    except TimeoutError:
        return False


def _emergency_cleanup(inflight: dict[str, tuple[str, str]]) -> None:
    """Best-effort, run-scoped cleanup for cells this suite abandons (SIGINT, a crash): as each cell's pool user,
    stop+delete ITS endpoint by name and cancel ITS `uep.<eid>` blocks — the same commands run.py's teardown
    uses, never user-wide. A `docker stop`'d jail never reaches run.py's own teardown (review 2026-09-05)."""
    if not inflight:
        return
    key = os.environ.get("HPCB_TEST_SSH_KEY", _TARGET.default_key)
    for runid, (user, name) in list(inflight.items()):
        print(f"🧹 cleanup {user}: endpoint {name} + its blocks (abandoned cell {runid})", flush=True)
        base = _TARGET.cleanup_argv(user, key)
        try:
            eid = subprocess.run(base + [endpoint_uuid_cmd(name)], capture_output=True, text=True, timeout=60).stdout.strip()
            eids = [eid] if eid else []
            cmd = f"{delete_endpoint_cmd(name)}; {scoped_cancel_cmd('slurm', eids)}; {uep_dirs_cleanup_cmd(eids)}"
            r = subprocess.run(base + [cmd], capture_output=True, text=True, timeout=120)
            print(f"   {r.stdout.strip()[:160] or ('rc=' + str(r.returncode))}", flush=True)
        except Exception as exc:  # noqa: BLE001 - cleanup must not raise during shutdown
            print(f"   cleanup failed: {type(exc).__name__}: {exc}", flush=True)


async def _run_job(scenario, model, effort, persona, ablate, claims, sem, stagger, halt, inflight=None) -> dict:
    # A concurrency slot (this invocation) + an EXCLUSIVE claim on a pool user (across every harness
    # process on this host — two invocations must never share one: the other's teardown would cancel
    # this run's blocks). If another invocation holds the whole pool, wait rather than collide.
    await sem.acquire()
    user = None
    while (user := claims.claim_any(POOL)) is None:
        if halt.is_set():
            sem.release()
            return {"scenario": scenario, "model": model, "effort": effort, "persona": persona,
                    "ablate": ablate, "user": None, "ok": False, "skipped": True,
                    "result": "SKIPPED (rate-limit halt)", "output": ""}
        print(f"⏳ wait   {scenario} — every pool user is claimed ({len(claims.busy(POOL))} held, some by "
              "another invocation); retrying in 20s", flush=True)
        await asyncio.sleep(20)
    label = (
        f"{scenario} · {_short_model(model)}/{effort or 'default'}"
        f"{f'/{persona}' if persona else ''}{' ~' + ablate if ablate else ''} · {user}"
    )
    try:
        if halt.is_set():   # a prior job hit the session/rate limit — don't burn the queue
            print(f"⏭ skip   {label} — rate-limit halt", flush=True)
            return {"scenario": scenario, "model": model, "effort": effort, "persona": persona,
                    "ablate": ablate, "user": user, "ok": False, "skipped": True,
                    "result": "SKIPPED (rate-limit halt)", "output": ""}
        await stagger.wait()
        print(f"▶ start  {label}", flush=True)
        runid = f"{int(time.time())}-{os.getpid()}-{abs(hash((scenario, model, effort, persona, ablate, user, time.monotonic()))) % 100000}"
        env = _cell_env(user, runid)
        env["HPCB_MODEL"] = model
        if effort:
            env["HPCB_EFFORT"] = effort
        if persona:
            env["HPCB_PERSONA"] = persona
        if ablate == "skill":
            env["HPCB_NO_SKILL"] = "1"
        if inflight is not None:
            inflight[runid] = (user, f"{_TARGET.endpoint_prefix}-{runid}")  # what run_smoke.sh names this cell's endpoint
        proc = await asyncio.create_subprocess_exec(
            "bash", str(SMOKE), scenario,
            env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        try:
            out, _ = await proc.communicate()
        except asyncio.CancelledError:
            # The suite is being interrupted. Give the cell the chance to clean up after ITSELF: SIGTERM to the docker
            # client is proxied into the jail, whose entrypoint forwards it to run.py, whose `finally` tears the
            # endpoint + block down and writes the bundle (run_smoke.sh gives it --stop-timeout 120). Only a cell that
            # does not finish in time stays on the cleanup list for _emergency_cleanup. (Live 2026-09-05: the list was
            # emptied by this very cancellation before the cleanup ran, and the container ran on for 3 minutes.)
            print(f"⏹ stop   {label} — terminating the cell so it tears itself down", flush=True)
            finished = await _terminate_and_wait(proc, CELL_STOP_GRACE_S)
            if finished and inflight is not None:
                inflight.pop(runid, None)
            raise
        if inflight is not None:
            inflight.pop(runid, None)  # the cell ran to its own teardown
        text = out.decode(errors="replace")
        ok = proc.returncode == 0
        if proc.returncode == 3:  # RATE_LIMITED (run.py) — stop launching new jobs
            halt.set()
            print(f"⛔ halt   {label} hit the session/rate limit — skipping remaining jobs",
                  flush=True)
        result = next((ln for ln in text.splitlines() if ln.startswith("RESULT:")),
                      f"(no RESULT line; rc={proc.returncode})")
        print(f"{'✓' if ok else '✗'} done   {label} — {result}", flush=True)
        return {"scenario": scenario, "model": model, "effort": effort, "persona": persona,
                "ablate": ablate, "user": user, "ok": ok,
                "skipped": False, "rate_limited": proc.returncode == 3,
                "result": result, "output": text}
    finally:
        claims.release(user)
        sem.release()


async def _fake_cluster_up(reset: bool, profile: str) -> bool:
    """Bring the fake cluster up on `profile` (build if needed, wait until schedulable + sshd answers) before the
    first cell; `reset` wipes it first — a wiped cluster has no stale endpoints, worker dirs or processes, so no
    pool sweeps. Switching profiles needs a reset (the compose overlay changes)."""
    if reset:
        print(f"fake cluster: wiping (down.sh --wipe --profile {profile})…", flush=True)
        r = await asyncio.create_subprocess_exec(str(FAKE_BIN / "down.sh"), "--wipe", "--profile", profile)
        await r.wait()
    print(f"fake cluster: up.sh --profile {profile} (build if needed, then wait until schedulable)…", flush=True)
    proc = await asyncio.create_subprocess_exec(str(FAKE_BIN / "up.sh"), "--profile", profile,
                                                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    tail = out.decode(errors="replace").strip().splitlines()[-3:]
    print("fake cluster: " + " | ".join(ln.strip() for ln in tail), flush=True)
    return proc.returncode == 0


async def _main(args) -> int:
    global _TARGET
    os.environ["HPCB_TARGET"] = args.target  # every cell, knob probe and helper reads the same target
    if args.target == "fake":
        os.environ["HPCB_FAKE_PROFILE"] = args.profile
    _TARGET = targets.get(args.target)
    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    for s in list(scenarios):
        allowed = _allowed_targets(s)
        if allowed is not None and _TARGET.name not in allowed:
            print(f"⏭ skip   {s}: declares TARGETS={sorted(allowed)} — not runnable on --target {_TARGET.name}", flush=True)
            scenarios.remove(s)
            continue
        ok, why = targets.meets(_requires(s), _TARGET.capabilities)
        if not ok:
            print(f"⏭ skip   {s}: REQUIRES {why} — not met by {_TARGET.name}"
                  f"{' profile ' + _TARGET.profile if _TARGET.profile else ''}", flush=True)
            scenarios.remove(s)
            continue
        if _needs_admin(s) and _TARGET.admin_argv is None:
            print(f"⏭ skip   {s}: declares ADMIN_SETUP/ADMIN_CLEANUP — --target {_TARGET.name} has no admin channel", flush=True)
            scenarios.remove(s)
    if not scenarios:
        print("nothing to run on this target", flush=True)
        return 2
    models = [m.strip() for m in args.models.split(",") if m.strip()]
    efforts = [e.strip() for e in args.efforts.split(",") if e.strip()] or [None]
    personas = [p.strip() for p in args.personas.split(",") if p.strip()] or [None]
    ablations = [None if a in ("", "none") else a for a in args.ablations.split(",")] if args.ablations else [None]
    jobs = [(s, m, e, pe, ab) for s in scenarios for m in models for e in efforts
            for pe in personas for ab in ablations for _ in range(args.repeat)]
    slots = min(args.concurrency, len(POOL))
    print(f"suite: {len(jobs)} jobs "
          f"({len(scenarios)} scenario × {len(models)} model × {len(efforts)} effort × "
          f"{len(personas)} persona × {len(ablations)} ablation × {args.repeat}) | "
          f"≤{slots} parallel | {args.stagger}s stagger | target {_TARGET.name}"
          f"{' profile ' + _TARGET.profile if _TARGET.profile else ''} ({_TARGET.ssh_host}, {_TARGET.nodes} nodes)",
          flush=True)

    if _TARGET.name == "fake" and not args.no_cluster_up and not await _fake_cluster_up(args.reset_cluster, args.profile):
        print("fake cluster did not come up — see agentic/fakecluster/README.md", file=sys.stderr)
        return 2
    if not args.no_build and not await _build_once():
        return 2

    claims = PoolClaims()
    held = claims.busy(POOL)
    if held:
        print(f"pool: {len(held)} user(s) already claimed by another harness process — {held}; "
              "this suite uses the rest", flush=True)
    sem = asyncio.Semaphore(slots)
    stagger = Stagger(args.stagger)
    halt = asyncio.Event()
    # SERIAL scenarios (one Globus identity per cell — mep_no_account, stranger_mep_walk) run one at a
    # time: two cells at once make the web service answer the second with RESOURCE_CONFLICT (first sweep).
    serial_locks = {s: asyncio.Lock() for s in scenarios if _is_serial(s)}
    cooldowns = {s: _cooldown_s(s) for s in serial_locks}
    remaining = {s: sum(1 for j in jobs if j[0] == s) for s in serial_locks}  # cooldown only BETWEEN cells
    if serial_locks:
        print("serial scenarios (one cell at a time): "
              + ", ".join(f"{s}{f' (+{cooldowns[s]}s cooldown)' if cooldowns[s] else ''}" for s in sorted(serial_locks)),
              flush=True)

    # Compute cells launch only when the partition can take their block: idle nodes minus what cells launched
    # in the last NODE_CLAIM_S will still submit (NodeGate). The need is per scenario (declared or derived).
    needs = {s: _needs_nodes(s) for s in scenarios} if args.node_wait_s > 0 else {}
    node_scenarios = {s for s, n in needs.items() if n > 0}
    warm_users = {s: _warm_block_user(s) for s in node_scenarios}
    gate = NodeGate()
    inflight: dict[str, tuple[str, str]] = {}
    if node_scenarios:
        print("compute-node scenarios (launch only when the partition has capacity; wait up to "
              f"{args.node_wait_s}s): " + ", ".join(f"{s} (needs {needs[s]}{', or ' + warm_users[s] + chr(39) + 's warm block' if warm_users.get(s) else ''})" for s in sorted(node_scenarios)), flush=True)

    async def serial_or_plain(s, m, e, pe, ab):
        lock = serial_locks.get(s)
        if lock is None:
            return await _run_job(s, m, e, pe, ab, claims, sem, stagger, halt, inflight)
        async with lock:
            r = await _run_job(s, m, e, pe, ab, claims, sem, stagger, halt, inflight)
            remaining[s] -= 1
            if cooldowns.get(s) and remaining[s] > 0 and not halt.is_set():  # the last cell needs no cooldown
                print(f"⏲ cooldown {s}: {cooldowns[s]}s before its next cell (fail2ban findtime)", flush=True)
                await asyncio.sleep(cooldowns[s])
            return r

    async def gated(s, m, e, pe, ab):
        if s not in node_scenarios:
            return await serial_or_plain(s, m, e, pe, ab)
        label = f"{s} · {_short_model(m)}/{e or 'default'}"
        idle = await gate.admit(label, needs[s], warm_users.get(s), halt, args.node_wait_s)
        if idle is None:
            print(f"⏭ skip   {label} — no capacity for {needs[s]} node(s) within {args.node_wait_s}s", flush=True)
            return {"scenario": s, "model": m, "effort": e, "persona": pe, "ablate": ab, "user": None,
                    "ok": False, "skipped": True,
                    "result": f"SKIPPED (no idle compute node within {args.node_wait_s}s)", "output": ""}
        return await serial_or_plain(s, m, e, pe, ab)

    try:
        results = await asyncio.gather(*[gated(s, m, e, pe, ab) for s, m, e, pe, ab in jobs])
    except (KeyboardInterrupt, asyncio.CancelledError):
        # gather() cancels every cell task; each _run_job terminates its jail and waits for it to tear itself down
        # (removing itself from `inflight` when it did). What is left is cleaned up from here, run-scoped.
        print("\n⛔ interrupted — waiting for the running cells to tear themselves down, then cleaning up leftovers",
              flush=True)
        _emergency_cleanup(inflight)
        raise
    finally:
        claims.release_all()

    skipped = [r for r in results if r.get("skipped")]
    limited = [r for r in results if r.get("rate_limited")]
    graded = [r for r in results if not r.get("skipped") and not r.get("rate_limited")]
    passed = sum(1 for r in graded if r["ok"])
    extra = (f" · {len(limited)} rate-limited" if limited else "") + (f" · {len(skipped)} skipped" if skipped else "")
    print(f"\n==== SUITE: {passed}/{len(graded)} passed{extra} ====")
    cells: dict[str, list[bool]] = {}
    for r in graded:
        key = (_cell(r["model"], r["effort"]) + (f" [{r['persona']}]" if r.get("persona") else "")
               + (f" ~{r['ablate']}" if r.get("ablate") else ""))
        cells.setdefault(key, []).append(r["ok"])
    for cell, oks in sorted(cells.items()):
        print(f"  {cell}: {sum(oks)}/{len(oks)} passed")   # the model × reasoning-level comparison
    for r in skipped:
        print(f"  ⏭ {r['scenario']} · {_cell(r['model'], r['effort'])} — {r['result']}")
    fails = [r for r in graded if not r["ok"]]
    if fails:
        print("\nfailures:")
        for r in fails:
            pe = (f" [{r['persona']}]" if r.get("persona") else "") + \
                 (f" ~{r['ablate']}" if r.get("ablate") else "")
            print(f"  ✗ {r['scenario']} · {_cell(r['model'], r['effort'])}{pe} · {r['user']} — {r['result']}")
    return 0 if passed == len(results) else 1


def main() -> None:
    ap = argparse.ArgumentParser(description="Run an agentic scenario × model × effort suite, staggered + capped.")
    ap.add_argument("--scenarios", default="happy_path", help="comma-separated scenario module names")
    ap.add_argument("--models", default=DEFAULT_MODEL, help="comma-separated Anthropic model ids")
    ap.add_argument("--efforts", default="",
                    help="comma reasoning levels: low,medium,high,xhigh,max (default: the model's default)")
    ap.add_argument("--personas", default="",
                    help="comma personas for interactive scenarios: cooperative,budget_hawk,declines_spend "
                         "(default: the scenario's own PERSONA, or autonomous)")
    ap.add_argument("--ablations", default="",
                    help="comma ablation cells: none,skill — 'none,skill' runs baseline AND skill-ablated "
                         "(the pass-rate delta = the measured value of SKILL.md)")
    ap.add_argument("--repeat", type=int, default=1, help="runs per (scenario, model, effort, persona, ablation)")
    ap.add_argument("--concurrency", type=int, default=10, help="max parallel runs (capped at pool size 10)")
    # globus1 allows ~15 simultaneous new SSH connections per source (ufw fix, 2026-07-01;
    # verified 10/10 concurrent from our egress). A small stagger stays as a guard: each run
    # also opens a teardown connection, and a shared office NAT / CI runner shares the budget.
    ap.add_argument("--stagger", type=float, default=2.0, help="seconds between launches (rate-limit guard)")
    ap.add_argument("--no-build", action="store_true", help="skip the one-time image build")
    ap.add_argument("--target", default=os.environ.get("HPCB_TARGET", targets.DEFAULT_TARGET), choices=["globus1", "fake"],
                    help="the cluster to run against: globus1 (the lab cluster) or fake (agentic/fakecluster, local compose Slurm)")
    ap.add_argument("--profile", default=os.environ.get("HPCB_FAKE_PROFILE", "default"),
                    help="fake: the cluster profile (agentic/fakecluster/profiles/<name>) — default | site | …")
    ap.add_argument("--no-cluster-up", action="store_true", help="fake: do not run fakecluster/bin/up.sh first")
    ap.add_argument("--reset-cluster", action="store_true", help="fake: wipe the cluster (down.sh --wipe) before up")
    ap.add_argument("--node-wait-s", type=int, default=3600,
                    help="NEEDS_COMPUTE_NODE scenarios: wait up to this long for an idle node before skipping the "
                         "cell (0 = launch regardless). Probe: ssh $HPCB_NODE_PROBE_SSH sinfo -p $HPCB_NODE_PARTITION")
    args = ap.parse_args()
    sys.exit(asyncio.run(_main(args)))


if __name__ == "__main__":
    main()

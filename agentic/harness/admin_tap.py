#!/usr/bin/env python3
"""The ADMIN TAP: the scheduler as the cluster's ADMIN sees it, sampled every few seconds while a cell runs — for a world
check that must not take hpc-bridge's word for anything (queued_check_relaunch: did the endpoint submit a NEW pilot
after `stop_endpoint` answered `down`?).

The admin channel exists only on the HOST (targets.py: `docker exec` into the fake cluster's controller, as root), while
the graders run in the jail, so the two halves meet in the runs volume both mount (run_smoke.sh: `agentic/runs` on the
host is `/work/hpc-bridge/agentic/runs` in the jail):
- HOST — run_smoke.sh starts `admin_tap.py --out <runs>/.admin-tap-<runid>.jsonl …` in the background for a scenario
  that declares `ADMIN_TAP = {"cmd": …, "every_s": …}` (scenario_knobs.py prints it), BEFORE the cell, and stops it and
  removes the file on exit. Every `every_s` seconds it runs the scenario's command through the admin channel
  (`{user}` = the cell's pool user, as ADMIN_SETUP's) and appends one JSON line: {"t": host epoch, "rc", "out"}.
- JAIL — run.py hands the file (HPCB_ADMIN_TAP) to the scenario's WORLD_CHECKS as a `TapReader`; they wait for the
  samples they need and parse them (`parse_slurm_sample`). The bundle keeps every sample (`admin-tap.jsonl`).

Why the admin channel and not the harness' world channel (ssh as the pool user, what POSTCHECKS use): nothing on the
admin path is the product's — not the login node the endpoint runs on, not the pool user's sshd that hpc-bridge's own
bootstrap rides — so what it reports is the scheduler's own record. Times are compared on the CONTROLLER's clock
(`HPCB_NOW` in each sample): the jail and the controller are containers on one Docker VM kernel, so a stop the jail
timed and a submit Slurm stamped share a clock; the host's clock (`t`) only orders the samples.

Stdlib only: the host half runs on the host's python3; the jail half is imported by run.py and by scenarios (which
scenario_knobs.py imports on the host).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import signal
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

TAP_ENV = "HPCB_ADMIN_TAP"          # the jail-side path of this cell's tap file (run_smoke.sh sets it)
DEFAULT_EVERY_S = 10.0
SAMPLE_TIMEOUT_S = 30               # one admin command; a wedged `docker exec` must not stall the tap
OUT_CAP = 64 * 1024                 # a sample keeps at most this much output (its tail)
MAX_RUN_S = 4 * 3600.0              # a tap orphaned by a killed run_smoke.sh still ends


# ---- the host half: sample the admin channel into the tap file ---------------------------------------------------

def _stop(_signum, _frame) -> None:
    # SystemExit from inside subprocess.run kills its child (`docker exec`) and unwinds: the tap ends at once, and the
    # file it holds open is never re-created after run_smoke.sh removed it
    raise SystemExit(0)


def tap(out: Path, admin: list[str], cmd: str, *, every_s: float, max_s: float = MAX_RUN_S,
        run: Callable[[list[str]], tuple[int, str]] | None = None, sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic) -> int:
    """Run `admin + [cmd]` every `every_s` seconds (the first at once) and append one sample per run to `out`, until
    signalled or `max_s` has passed. Returns the number of samples written."""
    def _run(argv: list[str]) -> tuple[int, str]:
        try:
            r = subprocess.run(argv, capture_output=True, text=True, timeout=SAMPLE_TIMEOUT_S, stdin=subprocess.DEVNULL)
            return r.returncode, r.stdout + r.stderr
        except subprocess.TimeoutExpired:
            return 124, f"admin command timed out after {SAMPLE_TIMEOUT_S} s"
        except OSError as exc:
            return 127, f"{type(exc).__name__}: {exc}"

    run = run or _run
    n = 0
    t0 = clock()
    with out.open("a") as fh:   # held open: a write after run_smoke.sh removed the file never re-creates it
        while clock() - t0 < max_s:
            rc, text = run([*admin, cmd])
            fh.write(json.dumps({"t": round(time.time(), 3), "rc": rc, "out": text[-OUT_CAP:]}) + "\n")
            fh.flush()
            n += 1
            sleep(every_s)
    return n


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Sample a cluster's scheduler through the admin channel (run_smoke.sh).")
    ap.add_argument("--out", required=True, type=Path, help="the tap file (JSON lines), in the runs volume")
    ap.add_argument("--user", required=True, help="the cell's pool user: `{user}` in the command")
    ap.add_argument("--knob", required=True, help='the scenario\'s ADMIN_TAP as JSON: {"cmd": …, "every_s": …}')
    ap.add_argument("--max-s", type=float, default=MAX_RUN_S)
    ap.add_argument("admin", nargs=argparse.REMAINDER, help="-- then the admin channel's argv prefix")
    args = ap.parse_args(argv)
    admin = args.admin[1:] if args.admin[:1] == ["--"] else args.admin
    knob = json.loads(args.knob)
    if not admin or not str(knob.get("cmd") or "").strip():
        print("admin_tap: needs the admin argv (after --) and a cmd", file=sys.stderr)
        return 2
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _stop)
    tap(args.out, admin, str(knob["cmd"]).replace("{user}", args.user),
        every_s=float(knob.get("every_s") or DEFAULT_EVERY_S), max_s=args.max_s)
    return 0


# ---- the jail half: read the samples, parse a Slurm sample ------------------------------------------------------

def read_samples(path: Path | None) -> list[dict]:
    """Every complete sample in the tap file (a line cut by a kill mid-write is skipped); [] when there is none."""
    if path is None:
        return []
    try:
        text = Path(path).read_text(errors="replace")
    except OSError:
        return []
    out = []
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("{"):
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    return out


def slurm_sample_cmd(*, history: str = "now-2hours") -> str:
    """One Slurm sample through the admin channel, as tagged lines (`parse_slurm_sample`): the controller's clock, the
    pool user's queue (`squeue %o` is the batch script — a parsl pilot's sits under `uep.<eid>.<…>/submit_scripts/`, so
    it carries the endpoint's marker, as the StdOut path does for scheduler_ops._release_cmd), and its finished jobs
    since `history` (sacct's SubmitLine names the same script). Times in epoch seconds (SLURM_TIME_FORMAT=%s, glibc's
    strftime); TZ=UTC so a Slurm that ignores the format prints ISO times in UTC, which the parser reads as such."""
    env = "TZ=UTC SLURM_TIME_FORMAT=%s"
    return ("echo HPCB_NOW $(date +%s); "
            f"{env} squeue -u {{user}} -h -o '%i|%T|%V|%r|%o' 2>&1 | sed 's/^/HPCB_Q /'; "
            f"{env} sacct -u {{user}} -X -n -P -S {history} -o JobID,JobName,State,Submit,Start,End,SubmitLine 2>&1 "
            "| sed 's/^/HPCB_A /'")


def slurm_time(raw: str) -> float | None:
    """A Slurm time stamp as epoch seconds: an epoch (SLURM_TIME_FORMAT=%s) or ISO `2026-09-05T17:32:59` (UTC — the
    sample runs under TZ=UTC); None for `Unknown`, `None`, `N/A` or anything else."""
    s = (raw or "").strip()
    if s.isdigit():
        return float(s)
    try:
        # timezone.utc, not datetime.UTC (3.11+): this module also runs on the HOST's python3, which may be older
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()  # noqa: UP017
    except ValueError:
        return None


@dataclass
class SlurmJob:
    job_id: str
    state: str                    # the first word (sacct says `CANCELLED by 0`)
    submit: float | None          # epoch seconds, the controller's clock
    command: str                  # squeue's batch script / sacct's SubmitLine: where the `uep.<eid>` marker is
    source: str                   # "squeue" | "sacct"
    reason: str = ""              # squeue: why it is PENDING (BeginTime, Resources, …)
    name: str = ""                # sacct: JobName
    start: float | None = None
    end: float | None = None
    seen_at: float | None = None  # the controller clock of the first sample that showed it


@dataclass
class SlurmSample:
    t: float | None               # the host's clock (orders the samples only)
    now: float | None             # the controller's clock: what a sample's times compare against
    queue: list[SlurmJob] = field(default_factory=list)
    history: list[SlurmJob] = field(default_factory=list)
    error: str = ""               # why this sample says nothing (the admin command failed, or printed no clock)


def parse_slurm_sample(sample: dict) -> SlurmSample:
    """One tap sample from `slurm_sample_cmd` → its clock, queue and history. A line that is not a job (a Slurm error
    message under the tag) is skipped; a sample with a failed command or no clock carries `error`."""
    out = str(sample.get("out") or "")
    now: float | None = None
    queue: list[SlurmJob] = []
    history: list[SlurmJob] = []
    for line in out.splitlines():
        tag, _, rest = line.partition(" ")
        if tag == "HPCB_NOW":
            now = slurm_time(rest)
        elif tag == "HPCB_Q":
            f = rest.split("|", 4)
            if len(f) == 5 and f[0].strip().replace("_", "").isdigit():
                queue.append(SlurmJob(job_id=f[0].strip(), state=(f[1].split() or ["?"])[0], submit=slurm_time(f[2]),
                                      reason=f[3].strip(), command=f[4].strip(), source="squeue", seen_at=now))
        elif tag == "HPCB_A":
            f = rest.split("|", 6)
            if len(f) == 7 and f[0].strip().replace("_", "").isdigit():
                history.append(SlurmJob(job_id=f[0].strip(), name=f[1].strip(), state=(f[2].split() or ["?"])[0],
                                        submit=slurm_time(f[3]), start=slurm_time(f[4]), end=slurm_time(f[5]),
                                        command=f[6].strip(), source="sacct", seen_at=now))
    rc = sample.get("rc")
    error = ""
    if rc not in (0, None):
        error = f"admin command rc={rc}: {out.strip()[-200:]!r}"
    elif now is None:
        error = f"no controller clock in the sample: {out.strip()[-200:]!r}"
    return SlurmSample(t=sample.get("t"), now=now, queue=queue, history=history, error=error)


class TapReader:
    """The jail's view of the cell's tap file (None: the cell has no tap — every read is empty)."""

    def __init__(self, path: Path | str | None) -> None:
        self.path = Path(path) if path else None

    def samples(self) -> list[SlurmSample]:
        return [parse_slurm_sample(s) for s in read_samples(self.path)]

    async def wait_until(self, now: float, *, deadline_s: float, poll_s: float = 5.0,
                         sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                         clock: Callable[[], float] = time.monotonic) -> list[SlurmSample]:
        """Wait until a sample's controller clock reaches `now` — or `deadline_s` seconds pass — and return every
        sample. Returns at once without a tap (`path` None)."""
        if self.path is None:
            return []
        t_end = clock() + max(0.0, deadline_s)
        while True:
            got = self.samples()
            if any(s.now is not None and s.now >= now for s in got) or clock() >= t_end:
                return got
            await sleep(min(poll_s, max(0.0, t_end - clock())))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

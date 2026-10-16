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
  (`{user}` = the cell's pool user, as ADMIN_SETUP's) and appends one JSON line: {"t": host epoch, "rc", "out"}. It
  watches the pid that started it (`--parent-pid`): a run_smoke.sh killed outright (SIGKILL: no EXIT trap) would
  otherwise leave it sampling — and holding pilots — for MAX_RUN_S; it then exits and removes its files itself. Its
  pid is in `<tap file>.pid`, which run_suite's emergency cleanup reads to stop the tap of a cell it abandons.
- JAIL — run.py hands the file (HPCB_ADMIN_TAP) to the scenario's WORLD_CHECKS as a `TapReader`; they wait for the
  samples they need and parse them (`parse_slurm_sample`). The bundle keeps every sample (`admin-tap.jsonl`).
- ASKING FOR JOBS BY ID — a world check that knows job ids (the cell's own pilot) writes them to `<tap file>.ids`
  (`TapReader.request_ids`); the host tap then adds the scenario's `ids_cmd` (`{ids}` = those ids) to every sample and
  takes one at once, its rows under their own tag (`HPCB_J`): a lookup by a KNOWN id can never see a relaunch (its id
  is unknown), so it says what became of a job, never that the sampling could see one.
- ASKING FOR A HOLD — a script that must keep an endpoint's NEXT pilots from ever starting writes `<tap file>.hold`
  (`TapReader.request_hold`: the endpoint's `uep.<eid>` marker and the first job id to hold); the host tap then adds the
  scenario's `hold_cmd` (`{marker}`, `{from}`) to every sample — `scontrol hold` as the admin on each PENDING job of that
  marker from that id on — and takes one at once. A hold cannot catch a job an idle node starts within a second of its
  sbatch; the scenario keeps such jobs PENDING first (queued_check_relaunch: a singleton dependency), and the hold then
  keeps them from ever running, whatever else ends.

EVERY QUERY SAYS HOW IT ENDED: each runs as `{ query; echo "HPCB_RC $?"; } 2>&1 | sed 's/^/<tag> /'` — the sample's
own `rc` is the last sed's — and a tagged line that is not a job row (a Slurm error) is an error of that sample. A
sample with an error, a missing exit status or no clock is not CLEAN: it covers no time and proves no sighting
(`SlurmSample.clean`). The first review found a squeue that failed in every sample, swallowed by its sed, graded as a
watch that had seen nothing.

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
import contextlib
import json
import math
import os
import re
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
SINCE_MARGIN_S = 300                # `{since}`: the window sacct starts this long before the tap did (clock skew)


# ---- the host half: sample the admin channel into the tap file ---------------------------------------------------

def _stop(_signum, _frame) -> None:
    # SystemExit from inside subprocess.run kills its child (`docker exec`) and unwinds: the tap ends at once, and the
    # file it holds open is never re-created after run_smoke.sh removed it
    raise SystemExit(0)


_JOB_ID = re.compile(r"^[0-9]{1,12}(?:_[0-9]{1,8})?$")
MAX_IDS = 32


def ids_path(out: Path | str) -> Path:
    """Where the jail asks the host tap for jobs by id: beside the tap file."""
    out = Path(out)
    return out.with_name(out.name + ".ids")


def read_ids(path: Path) -> list[str]:
    """The job ids asked for — only scheduler job ids (digits, an array's `_n`), at most MAX_IDS, sorted: the file
    comes from the jail and the ids go into a root shell on the controller."""
    try:
        words = path.read_text().split()
    except OSError:
        return []
    return sorted({w for w in words if _JOB_ID.match(w)}, key=lambda w: (len(w), w))[:MAX_IDS]


_MARKER = re.compile(r"^uep\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?:\.[0-9a-f-]{36})?$")


def is_marker(marker: str) -> bool:
    """A whole endpoint marker, `uep.<eid>` (or `uep.<eid>.<uep id>`) — never a bare `uep.`, which every pilot matches."""
    return bool(_MARKER.match(marker or ""))


def hold_path(out: Path | str) -> Path:
    """Where a script asks the host tap to hold an endpoint's next pilots: beside the tap file."""
    out = Path(out)
    return out.with_name(out.name + ".hold")


def read_hold(path: Path) -> tuple[str, str] | None:
    """(marker, first job id) asked for, or None — only an endpoint marker and a job id pass: they go into a root shell."""
    try:
        words = path.read_text().split()
    except OSError:
        return None
    if len(words) == 2 and _MARKER.match(words[0]) and words[1].isdigit() and len(words[1]) <= 12:
        return words[0], words[1]
    return None


def pid_path(out: Path | str) -> Path:
    """Where the tap writes its pid: beside the tap file (run_suite's emergency cleanup stops it by it)."""
    out = Path(out)
    return out.with_name(out.name + ".pid")


def tap_files(out: Path | str) -> list[Path]:
    """Every file of a tap: the samples, the id and hold requests, the pid."""
    return [Path(out), ids_path(out), hold_path(out), pid_path(out)]


_START_PPID = os.getppid()   # the process that started this one (run_smoke.sh, when it passes its own pid)


def alive(pid: int | None) -> bool:
    """Is process `pid` still there (None: nothing to watch)? When `pid` is the tap's own parent, being re-parented says
    it is gone even while it is an unreaped zombie (which still answers `kill -0`)."""
    if not pid:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:   # there, under another user
        return True
    return not (pid == _START_PPID and os.getppid() != pid)


def tap(out: Path, admin: list[str], cmd: str, *, every_s: float, max_s: float = MAX_RUN_S, ids_cmd: str = "",
        hold_cmd: str = "", parent_pid: int | None = None,
        run: Callable[[list[str]], tuple[int, str]] | None = None, sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic, parent_alive: Callable[[int | None], bool] = alive) -> int:
    """Run `admin + [cmd]` every `every_s` seconds (the first at once) and append one sample per run to `out`, until
    signalled, `max_s` has passed, or `parent_pid` is gone (then it removes its files: nobody else will). With ids asked
    for (`ids_path`) and an `ids_cmd`, or a hold asked for (`hold_path`) and a `hold_cmd`, each sample runs those too,
    and a new request is answered at once rather than at the next tick. Returns the number of samples written."""
    def _run(argv: list[str]) -> tuple[int, str]:
        try:
            r = subprocess.run(argv, capture_output=True, text=True, timeout=SAMPLE_TIMEOUT_S, stdin=subprocess.DEVNULL)
            return r.returncode, r.stdout + r.stderr
        except subprocess.TimeoutExpired:
            return 124, f"admin command timed out after {SAMPLE_TIMEOUT_S} s"
        except OSError as exc:
            return 127, f"{type(exc).__name__}: {exc}"

    run = run or _run
    t0 = clock()
    asked, held = ids_path(out), hold_path(out)

    def requests() -> tuple[list[str], tuple[str, str] | None]:
        return (read_ids(asked) if ids_cmd else []), (read_hold(held) if hold_cmd else None)

    def sample_once(fh, elapsed: float) -> tuple[list[str], tuple[str, str] | None]:
        ids, hold = requests()
        # `{since}`: the window opens at the tap's own start — the cell's jobs, not another cell's two hours of history
        # (a multi-line `sbatch --wrap` SubmitLine, an array or a het job of the pool user's past is not this cell's)
        parts = [cmd.replace("{since}", f"now-{int(elapsed) + SINCE_MARGIN_S}")]
        if hold:   # first: a pilot it holds is then in this very sample, as held
            parts.insert(0, hold_cmd.replace("{marker}", hold[0]).replace("{from}", hold[1]))
        if ids:
            parts.append(ids_cmd.replace("{ids}", ",".join(ids)))
        rc, text = run([*admin, "; ".join(parts)])
        sample: dict = {"t": round(time.time(), 3), "rc": rc, "out": text[-OUT_CAP:]}
        if ids:
            sample["ids"] = ids
        if hold:
            sample["hold"] = list(hold)
        fh.write(json.dumps(sample) + "\n")
        fh.flush()
        return ids, hold

    n, orphaned = 0, False
    with contextlib.suppress(OSError):
        pid_path(out).write_text(f"{os.getpid()}\n")
    try:
        with out.open("a") as fh:   # held open: a write after run_smoke.sh removed the file never re-creates it
            while not orphaned and (elapsed := clock() - t0) < max_s:
                asked_now = sample_once(fh, elapsed)
                n += 1
                for _ in range(max(1, math.ceil(every_s))):   # a second at a time: a new request is answered at once
                    sleep(min(1.0, every_s))
                    if not parent_alive(parent_pid):
                        orphaned = True
                        break
                    if requests() != asked_now:
                        break
    finally:
        # orphaned: run_smoke.sh died without its EXIT trap — nobody else will remove the tap's files
        for f in (tap_files(out) if orphaned else [pid_path(out)]):
            with contextlib.suppress(OSError):
                f.unlink()
    return n


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Sample a cluster's scheduler through the admin channel (run_smoke.sh).")
    ap.add_argument("--out", required=True, type=Path, help="the tap file (JSON lines), in the runs volume")
    ap.add_argument("--user", required=True, help="the cell's pool user: `{user}` in the command")
    ap.add_argument("--knob", required=True, help='the scenario\'s ADMIN_TAP as JSON: {"cmd": …, "every_s": …}')
    ap.add_argument("--max-s", type=float, default=MAX_RUN_S)
    ap.add_argument("--parent-pid", type=int, default=None, help="exit (and remove the tap's files) once it is gone")
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
        every_s=float(knob.get("every_s") or DEFAULT_EVERY_S), max_s=args.max_s,
        ids_cmd=str(knob.get("ids_cmd") or "").replace("{user}", args.user),
        hold_cmd=str(knob.get("hold_cmd") or "").replace("{user}", args.user), parent_pid=args.parent_pid)
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


# The window sacct (slurm-wlm 23.11.4 on the fake controller, verified by hand 2026-10-15): without `-s` it lists the
# jobs that were ELIGIBLE in the window, and a pilot held by `--begin` never is — `-S now-2hours` alone listed nothing
# for the first live run's cancelled pilot; with `-s` but no END time (`-E now`) it lists nothing at all; with both it
# lists the jobs that have ENDED in the window, held ones included once cancelled. It does NOT list a held job that is
# still PENDING: in both live phase-`second` bundles the held relaunch was in squeue in 38 samples and in sacct once,
# after its cancel. `squeue -t all` is what sees a pending relaunch; the window sacct sees the finished ones. These six
# states are the ones verified there.
SACCT_STATES = "CA,CD,F,PD,R,TO"
_SACCT_FIELDS = "JobID,JobName,State,Submit,Start,End,SubmitLine"
_TIMES = "TZ=UTC SLURM_TIME_FORMAT=%s"


def _tagged(query: str, tag: str) -> str:
    """A query whose every line — its errors and, last, its own exit status (`HPCB_RC <n>`) — carries `tag`."""
    return f"{{ {query}; echo \"HPCB_RC $?\"; }} 2>&1 | sed 's/^/{tag} /'"


def slurm_sample_cmd(*, history: str = "{since}") -> str:
    """One Slurm sample through the admin channel, as tagged lines (`parse_slurm_sample`): the controller's clock, the
    pool user's jobs in the controller (`HPCB_Q`: `squeue -t all` — queued and running, AND those finished within
    MinJobAge, 300 s on the fake profiles, so a pilot cancelled between two samples is still seen; `%o` is the batch
    script — a parsl pilot's sits under `uep.<eid>.<…>/submit_scripts/`, so it carries the endpoint's marker, as the
    StdOut path does for scheduler_ops._release_cmd), and its accounting from `history` (default `{since}`, which the
    tap fills with its own start) to now, the jobs that ENDED in SACCT_STATES (`HPCB_A`; SubmitLine names the same
    script). Each query ends with its own exit status. Times in epoch seconds
    (SLURM_TIME_FORMAT=%s, glibc's strftime); TZ=UTC so a Slurm that ignores the format prints ISO times in UTC, which
    the parser reads as such."""
    return ("echo HPCB_NOW $(date +%s); "
            + _tagged(f"{_TIMES} squeue -u {{user}} -h -t all -o '%i|%T|%V|%r|%o'", "HPCB_Q") + "; "
            + _tagged(f"{_TIMES} sacct -u {{user}} -X -n -P -S {history} -E now -s {SACCT_STATES} -o {_SACCT_FIELDS}",
                      "HPCB_A"))


def slurm_ids_cmd() -> str:
    """The tap's `ids_cmd` for Slurm: the jobs asked for by id (`{ids}`) from accounting, under their own tag
    (`HPCB_J`) — `sacct -j` finds a job whatever its eligibility or age, but only a job whose id is already known."""
    return _tagged(f"{_TIMES} sacct -j {{ids}} -X -n -P -o {_SACCT_FIELDS}", "HPCB_J")


def slurm_hold_cmd() -> str:
    """The tap's `hold_cmd` for Slurm: `scontrol hold` (as the admin: reason JobHeldAdmin) on every PENDING job of the pool
    user whose batch script carries `{marker}`, from job id `{from}` on, once each; `HPCB_HELD <id>` says so."""
    return ("for j in $(squeue -u {user} -h -t PD -o '%i|%r|%o' 2>/dev/null "
            "| awk -F'|' -v m={marker} -v f={from} 'index($3,m) && $1+0 >= f+0 && $2 != \"JobHeldAdmin\" {print $1}'); "
            "do scontrol hold \"$j\" && echo \"HPCB_HELD $j\"; done")


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
    queue: list[SlurmJob] = field(default_factory=list)     # HPCB_Q: squeue -t all
    history: list[SlurmJob] = field(default_factory=list)   # HPCB_A: the window sacct
    error: str = ""               # why this sample is not CLEAN (a query that failed or did not end, no clock); "" if it is
    held: list[str] = field(default_factory=list)   # jobs the tap's hold_cmd held in this sample (`HPCB_HELD <id>`)
    by_id: list[SlurmJob] = field(default_factory=list)     # HPCB_J: sacct -j <known ids> — never evidence of sight
    by_id_error: str = ""         # the by-id lookup failed (report-only: it never makes the sample unclean)
    skipped: int = 0              # lines of a query that ended 0 that are not job rows (a multi-line SubmitLine)

    @property
    def clean(self) -> bool:
        """Every query of the sample ran and said nothing but jobs, with the controller's clock: it covers its time and
        what it does not list was not there."""
        return not self.error


_STREAMS = {"HPCB_Q": "squeue", "HPCB_A": "the window sacct", "HPCB_J": "sacct -j"}
_REQUIRED = ("HPCB_Q", "HPCB_A")   # the sampling's own queries: each must report how it ended
# A row's job id as Slurm prints it: plain (every pilot of ours), an array task or a pending array (`42_7`,
# `42_[1-3]`), a het job component (`43+0`). Read leniently: such a row is a job (not an error), never ours.
_ROW_ID = re.compile(r"^[0-9]+(?:_(?:[0-9]+|\[[0-9,%\-]+\])|\+[0-9]+)?$")


def _job_row(tag: str, rest: str, now: float | None) -> SlurmJob | None:
    if tag == "HPCB_Q":
        f = rest.split("|", 4)
        if len(f) == 5 and _ROW_ID.match(f[0].strip()):
            return SlurmJob(job_id=f[0].strip(), state=(f[1].split() or ["?"])[0], submit=slurm_time(f[2]),
                            reason=f[3].strip(), command=f[4].strip(), source="squeue", seen_at=now)
        return None
    f = rest.split("|", 6)
    if len(f) == 7 and _ROW_ID.match(f[0].strip()):
        return SlurmJob(job_id=f[0].strip(), name=f[1].strip(), state=(f[2].split() or ["?"])[0],
                        submit=slurm_time(f[3]), start=slurm_time(f[4]), end=slurm_time(f[5]),
                        command=f[6].strip(), source="sacct" if tag == "HPCB_A" else "sacct-j", seen_at=now)
    return None


def parse_slurm_sample(sample: dict) -> SlurmSample:
    """One tap sample from `slurm_sample_cmd` (+ `ids_cmd`, `hold_cmd`) → its clock, queue, history and by-id rows.
    The sample is not `clean` when one of the sampling's own queries (squeue, the window sacct) failed — its exit status
    not 0, its lines (the Slurm error the pipeline's sed would otherwise swallow) quoted — or reported no exit status,
    when the admin command failed, or when there is no clock. A query that ended 0 may print lines that are not job rows
    (a multi-line SubmitLine): skipped, not errors. The by-id lookup is report-only: its failure is `by_id_error`, never
    an unclean sample. Job rows are kept either way (a relaunch an unclean sample lists is still one)."""
    out = str(sample.get("out") or "")
    now: float | None = None
    rows: dict[str, list[SlurmJob]] = {t: [] for t in _STREAMS}
    rcs: dict[str, str] = {}
    noise: dict[str, list[str]] = {t: [] for t in _STREAMS}
    errors: list[str] = []
    held: list[str] = []
    lines = out.splitlines()
    for line in lines:   # the clock first: every job row is stamped with it
        tag, _, rest = line.partition(" ")
        if tag == "HPCB_NOW":
            now = slurm_time(rest)
    for line in lines:
        tag, _, rest = line.partition(" ")
        if tag == "HPCB_HELD" and _JOB_ID.match(rest.strip()):
            held.append(rest.strip())
        elif tag in _STREAMS:
            if rest.startswith("HPCB_RC "):
                rcs[tag] = rest.split(" ", 1)[1].strip()
            elif (job := _job_row(tag, rest, now)) is not None:
                rows[tag].append(job)
            elif rest.strip():
                noise[tag].append(rest.strip())

    def said(tag: str) -> str:
        return f" ({' | '.join(noise[tag])[:200]!r})" if noise[tag] else ""

    for tag in _REQUIRED:
        if tag not in rcs:
            errors.append(f"{_STREAMS[tag]} reported no exit status (it did not run to its end){said(tag)}")
        elif rcs[tag] != "0":
            errors.append(f"{_STREAMS[tag]} exited {rcs[tag]}{said(tag)}")
    j_rc = rcs.get("HPCB_J")
    by_id_error = (f"sacct -j exited {j_rc}{said('HPCB_J')}" if j_rc not in (None, "0") else
                   f"sacct -j reported no exit status{said('HPCB_J')}" if j_rc is None and noise["HPCB_J"] else "")
    skipped = sum(len(noise[t]) for t in _REQUIRED if rcs.get(t) == "0")
    rc = sample.get("rc")
    if rc not in (0, None):
        errors.insert(0, f"admin command rc={rc}: {out.strip()[-200:]!r}")
    if now is None:
        errors.insert(0, f"no controller clock in the sample: {out.strip()[-160:]!r}")
    return SlurmSample(t=sample.get("t"), now=now, queue=rows["HPCB_Q"], history=rows["HPCB_A"],
                       error="; ".join(errors), held=held, by_id=rows["HPCB_J"], by_id_error=by_id_error,
                       skipped=skipped)


class TapReader:
    """The jail's view of the cell's tap file (None: the cell has no tap — every read is empty)."""

    def __init__(self, path: Path | str | None) -> None:
        self.path = Path(path) if path else None

    def samples(self) -> list[SlurmSample]:
        return [parse_slurm_sample(s) for s in read_samples(self.path)]

    def request_hold(self, marker: str, from_job: str | int) -> bool:
        """Ask the host tap to hold every PENDING job of `marker` (an endpoint's `uep.<eid>`) from job id `from_job` on
        (its `hold_cmd`). False without a tap, for an invalid marker or id, or when the file cannot be written."""
        line = f"{marker} {from_job}"
        if self.path is None or not _MARKER.match(marker) or not str(from_job).isdigit():
            return False
        try:
            hold_path(self.path).write_text(line + "\n")
        except OSError:
            return False
        return True

    def request_ids(self, ids: list[str]) -> bool:
        """Ask the host tap to look these jobs up by id from now on (its `ids_cmd`). False without a tap, with no valid
        id, or when the file cannot be written."""
        ok = [i for i in ids if _JOB_ID.match(str(i))]
        if self.path is None or not ok:
            return False
        try:
            ids_path(self.path).write_text(" ".join(ok) + "\n")
        except OSError:
            return False
        return True

    async def wait_until(self, now: float, *, deadline_s: float, poll_s: float = 5.0,
                         sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                         clock: Callable[[], float] = time.monotonic) -> list[SlurmSample]:
        """Wait until a CLEAN sample's controller clock reaches `now` — or `deadline_s` seconds pass — and return every
        sample: an unclean one covers nothing, so the wait goes on past it. Returns at once without a tap."""
        if self.path is None:
            return []
        t_end = clock() + max(0.0, deadline_s)
        while True:
            got = self.samples()
            if any(s.clean and s.now is not None and s.now >= now for s in got) or clock() >= t_end:
                return got
            await sleep(min(poll_s, max(0.0, t_end - clock())))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

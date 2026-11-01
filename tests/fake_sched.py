#!/usr/bin/env python3
"""A fake Slurm and PBS for the batch phase 2a unit tests: ONE executable, installed under each scheduler command's name
(sbatch sacct squeue scancel scontrol qsub qstat qdel qrls qselect), whose state is a JSON file the test reads and
changes (`FakeSched`). The product's own command lines run against it under real bash, so the tests pin what hpc-bridge
SENDS (the frozen job.sh, its header, the meter's sacct/qstat, the hold and release of a gated hand submit) and what it
makes of the scheduler's answers — never a Python stand-in for the product's own parsing.

It answers as the fake cluster's Slurm 23.11 and OpenPBS 23.06 did when probed (scratchpad probe_slurm.out /
probe_pbs.out, 2026-11-01):
- a user's `scontrol update JobId=… Deadline=…` is DENIED ("Access/permission denied for job N", rc 1) — a deadline is
  set at submit (`--deadline`, in the submitting shell's local time, or `now+N` seconds);
- `scontrol show job -o` prints `ReqTRES=` / `AllocTRES=` (never `TRES=`) and `NumNodes=N-N`;
- `sacct -X` lists NO row for a pending array's tasks (only the started `N_i`); `squeue` shows them as `N_[a-b%t]`;
- `scancel --state=PENDING` on an array leaves its running tasks; a cancelled job reads `CANCELLED by <uid>`;
- PBS: `qsub -h` holds (`H`), `qrls` releases, a qdel'd job is `F` with Exit_status 271, `Account_Name` may be absent;
  `qstat -t` lists an array's subjobs `N[i].server` beside its parent `N[].server`.

Every invocation is logged (`calls`: argv, the TZ it ran under). Knobs: `sacct_down` (sacct fails, as with slurmdbd
down), `sacct_hang` (sacct blocks that many seconds first), `hide_sacct` / `hide_squeue` (ids those commands do not
list), `cancel_lag` (a scancel'd job stays listed COMPLETING for that many squeue reads), `refuse` (the next
sbatch/qsub is refused), `no_receipt` (the next sbatch exits 0 printing no job id).
"""
from __future__ import annotations

import json
import os
import re
import shlex
import sys
import time
from pathlib import Path
from typing import Any

COMMANDS = ("sbatch", "sacct", "squeue", "scancel", "scontrol", "qsub", "qstat", "qdel", "qrls", "qselect")
STATE_ENV = "HPCB_FAKE_SCHED"
_LIVE = ("PENDING", "RUNNING", "COMPLETING", "SUSPENDED", "CONFIGURING")


# ---- the test's side -------------------------------------------------------------------------------------------------

class FakeSched:
    """The fake's state, as the test sees it: `install(bin_dir)` puts the commands on a PATH; `env()` is what a bash
    running them needs; `job(id)` / `set(id, **fields)` read and change one job."""

    def __init__(self, root: Path, *, scheduler: str = "slurm", user: str = "u", cores_per_node: int = 4) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "state.json"
        self.bin = self.root / "bin"
        if not self.path.exists():
            self._save({"scheduler": scheduler, "user": user, "next": 1001, "jobs": {}, "calls": [],
                        "cores_per_node": cores_per_node, "knobs": {}})
        self.install()

    def install(self) -> None:
        self.bin.mkdir(parents=True, exist_ok=True)
        me = Path(__file__).resolve()
        for name in COMMANDS:
            p = self.bin / name
            p.write_text(f"#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(me))} {name} \"$@\"\n")
            p.chmod(0o755)

    def env(self) -> dict[str, str]:
        return {"PATH": f"{self.bin}:{os.environ['PATH']}", STATE_ENV: str(self.path), "USER": self.load()["user"]}

    def load(self) -> dict[str, Any]:
        return json.loads(self.path.read_text())

    def _save(self, st: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(f".tmp{os.getpid()}")
        tmp.write_text(json.dumps(st, indent=1))
        tmp.replace(self.path)

    def jobs(self) -> dict[str, dict[str, Any]]:
        return self.load()["jobs"]

    def job(self, jid: str) -> dict[str, Any]:
        return self.jobs()[str(jid).split(".")[0].split("[")[0]]

    def set(self, jid: str, **fields: Any) -> None:
        st = self.load()
        st["jobs"][str(jid).split(".")[0]].update(fields)
        self._save(st)

    def knob(self, **knobs: Any) -> None:
        st = self.load()
        st["knobs"].update(knobs)
        self._save(st)

    def add_job(self, **fields: Any) -> str:
        """A job submitted OUTSIDE hpc-bridge (another shell, a script's subprocess): it exists, nothing ledgered it."""
        st = self.load()
        jid = str(st["next"])
        st["next"] += 1
        st["jobs"][jid] = _new_job(jid, st, **fields)
        self._save(st)
        return jid

    def calls(self, name: str | None = None) -> list[list[str]]:
        return [c["argv"] for c in self.load()["calls"] if name is None or c["argv"][0] == name]

    def submitted(self) -> list[dict[str, Any]]:
        return [j for j in self.jobs().values() if j.get("via") in ("sbatch", "qsub")]


def _new_job(jid: str, st: dict[str, Any], **fields: Any) -> dict[str, Any]:
    job = {"id": jid, "name": "job", "user": st["user"], "partition": "main", "account": "", "nodes": 1,
           "timelimit_min": 30, "state": "PENDING", "reason": "Priority", "elapsed": 0, "billing": None,
           "held": False, "held_at_submit": False, "deadline": None, "deadline_epoch": None, "array": None,
           "tasks": None, "exit": "0:0", "submit": time.time(), "script": None, "script_path": None, "via": None,
           "tz": None, "argv": [], "dependency": None, "ncpus": None, "queue": "workq", "cancel_reads": 0,
           "comment": None, "submit_env": {}, "runs": []}
    job.update(fields)
    return job


# ---- the commands ------------------------------------------------------------------------------------------------------

def _state() -> tuple[dict[str, Any], Path]:
    p = Path(os.environ[STATE_ENV])
    return json.loads(p.read_text()), p


def _save(st: dict[str, Any], p: Path) -> None:
    tmp = p.with_suffix(f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(st, indent=1))
    tmp.replace(p)


def _hms(minutes: float) -> str:
    s = round(minutes * 60)
    return f"{s // 3600:02d}:{s % 3600 // 60:02d}:{s % 60:02d}"


def _minutes(spec: str) -> float:
    """Slurm/PBS time: MM, MM:SS, HH:MM:SS, D-HH:MM:SS."""
    d = 0
    if "-" in spec:
        dd, spec = spec.split("-", 1)
        d = int(dd)
    parts = [int(x) for x in spec.split(":")]
    if len(parts) == 1:
        return d * 1440 + parts[0]
    if len(parts) == 2:
        return d * 1440 + parts[0] + parts[1] / 60
    return d * 1440 + parts[0] * 60 + parts[1] + parts[2] / 60


def _expand(spec: str) -> tuple[list[int], int | None]:
    """An array spec `0-19%4` / `1,3,5` / `0-98:2` → (indices, throttle)."""
    throttle = None
    if "%" in spec:
        spec, t = spec.split("%", 1)
        throttle = int(t)
    out: list[int] = []
    for part in spec.split(","):
        step = 1
        if ":" in part:
            part, s = part.split(":", 1)
            step = int(s)
        if "-" in part:
            a, b = part.split("-", 1)
            out += list(range(int(a), int(b) + 1, step))
        else:
            out.append(int(part))
    return out, throttle


def _directives(script: str, mark: str) -> list[str]:
    """The scheduler's own reading of a script's header: `#SBATCH`/`#PBS` lines up to the first command line."""
    out = []
    for i, line in enumerate(script.splitlines()):
        s = line.strip()
        if i == 0 and s.startswith("#!"):
            continue
        if not s:
            continue
        if not s.startswith("#"):
            break
        if s.startswith(mark + " "):
            out += shlex.split(s[len(mark) + 1:])
    return out


_FLAGS = ("--parsable", "--hold", "-H", "--exclusive", "--wait", "-W", "--requeue", "--no-requeue")


def _slurm_opts(args: list[str]) -> dict[str, Any]:
    o: dict[str, Any] = {}
    i = 0
    while i < len(args):
        a = args[i]
        if a.startswith("--") and "=" in a:
            k, v = a[2:].split("=", 1)
        elif a in _FLAGS:
            k, v = a.lstrip("-"), True
        elif a.startswith("--"):
            k, v = a[2:], args[i + 1] if i + 1 < len(args) else ""
            i += 1
        elif a.startswith("-") and len(a) == 2:
            k, v = {"-J": "job-name", "-p": "partition", "-A": "account", "-N": "nodes", "-t": "time",
                    "-a": "array", "-d": "dependency", "-o": "output", "-c": "cpus-per-task"}.get(a, a), args[i + 1]
            i += 1
        else:
            o.setdefault("_rest", []).extend(args[i:])
            break
        o[k] = v
        i += 1
    return o


def _deadline_epoch(spec: str | None) -> float | None:
    """sbatch's own reading of --deadline: `now+N[seconds|minutes|hours]`, or YYYY-MM-DDTHH:MM[:SS] in the submitting
    shell's LOCAL time (its TZ)."""
    if not spec:
        return None
    m = re.fullmatch(r"now\+(\d+)(seconds|minutes|hours|days)?", spec)
    if m:
        mult = {"seconds": 1, None: 1, "minutes": 60, "hours": 3600, "days": 86400}[m.group(2)]
        return time.time() + int(m.group(1)) * mult
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            if "TZ" in os.environ:
                time.tzset()
            return time.mktime(time.strptime(spec, fmt))
        except ValueError:
            continue
    return None


def _submit_env() -> dict[str, str]:
    """What of the submitting environment reaches a job (--export=ALL): kept to see a shim's leaks."""
    return {k: v for k, v in os.environ.items() if k.startswith(("BASH_FUNC_", "HPCB_")) or k in ("PATH", "TZ")}


def sbatch(argv: list[str], st: dict[str, Any]) -> int:
    knobs = st["knobs"]
    cli = _slurm_opts(argv)
    rest = cli.pop("_rest", [])
    script_path = rest[0] if rest else None
    script = Path(script_path).read_text() if script_path else f"#!/bin/sh\n{cli.get('wrap', '')}\n"
    if knobs.get("refuse"):
        knobs["refuse"] = False
        print("sbatch: error: Batch job submission failed: Invalid account or account/partition combination",
              file=sys.stderr)
        return 1
    opts = _slurm_opts(_directives(script, "#SBATCH"))
    opts.pop("_rest", None)
    opts.update(cli)
    jid = str(st["next"])
    st["next"] += 1
    nodes = int(str(opts.get("nodes", "1")).split("-")[0])
    tasks = None
    if opts.get("array"):
        idx, thr = _expand(str(opts["array"]))
        tasks = {"indices": idx, "throttle": thr, "state": {str(i): "PENDING" for i in idx},
                 "elapsed": {str(i): 0 for i in idx}}
    held = bool(opts.get("hold") or opts.get("H"))
    dl = opts.get("deadline")
    job = _new_job(jid, st, name=opts.get("job-name", Path(script_path).name if script_path else "wrap"),
                   partition=opts.get("partition", "main"), account=opts.get("account", ""), nodes=nodes,
                   timelimit_min=_minutes(str(opts.get("time", "30"))), held=held, held_at_submit=held,
                   reason="JobHeldUser" if held else "Priority", deadline=dl, deadline_epoch=_deadline_epoch(dl),
                   array=opts.get("array"), tasks=tasks, script=script, script_path=script_path, via="sbatch",
                   tz=os.environ.get("TZ"), argv=argv, dependency=opts.get("dependency"),
                   comment=opts.get("comment"), submit_env=_submit_env(),
                   billing=int(opts.get("cpus-per-task", 0) or 0) * nodes or st["cores_per_node"] * nodes)
    st["jobs"][jid] = job
    if knobs.get("no_receipt"):
        knobs["no_receipt"] = False
        return 0
    print(jid if cli.get("parsable") else f"Submitted batch job {jid}")
    return 0


def _bill(j: dict[str, Any]) -> int:
    return int(j["billing"] if j.get("billing") is not None else 4 * j["nodes"])


def _tres(j: dict[str, Any]) -> str:
    """A job's TRES as sacct prints it; `no_billing`: an old Slurm (or a site) whose records carry no billing TRES."""
    cpus = j.get("cpus") or _bill(j)
    return ("" if j.get("no_billing") else f"billing={_bill(j)},") + f"cpu={cpus},node={j['nodes']}"


def _task_state(j: dict[str, Any], t: Any) -> str:
    if t is None or j.get("tasks") is None:
        return j["state"]
    return j["tasks"]["state"].get(str(t), j["state"])


def _elapsed(j: dict[str, Any], t: Any) -> int:
    if t is None or j.get("tasks") is None:
        return int(j["elapsed"])
    return int(j["tasks"]["elapsed"].get(str(t), 0))


def _state_word(state: str) -> str:
    return "CANCELLED by 1000" if state == "CANCELLED" else state


_SACCT = {
    "JobID": lambda j, t: j["id"] if t is None else f"{j['id']}_{t}",
    "JobIDRaw": lambda j, t: j["id"],
    "JobName": lambda j, t: j["name"],
    "State": lambda j, t: _state_word(_task_state(j, t)),
    "ElapsedRaw": lambda j, t: str(_elapsed(j, t)),
    "TimelimitRaw": lambda j, t: j.get("timelimit_raw") or str(int(j["timelimit_min"])),
    "Timelimit": lambda j, t: _hms(j["timelimit_min"]),
    "AllocTRES": lambda j, t: (_tres(j) if _task_state(j, t) not in ("PENDING",) else ""),
    "ReqTRES": lambda j, t: _tres(j),
    "Partition": lambda j, t: j["partition"],
    "Account": lambda j, t: j["account"],
    "ExitCode": lambda j, t: j["exit"],
    "Reason": lambda j, t: j["reason"] if _task_state(j, t) == "PENDING" else "None",
    "NNodes": lambda j, t: str(j["nodes"]),
    "Submit": lambda j, t: time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(j["submit"])),
    "Start": lambda j, t: "Unknown" if _task_state(j, t) == "PENDING" else time.strftime(
        "%Y-%m-%dT%H:%M:%S", time.localtime(j["submit"])),
    "SubmitLine": lambda j, t: " ".join(j.get("argv") or []),
    "Comment": lambda j, t: j.get("comment") or "",
}


def _rows(j: dict[str, Any]) -> list[Any]:
    """sacct -X's rows for a job: one; an array's STARTED tasks one each — its pending tasks have no row (probe §3b)."""
    if j.get("tasks") is None:
        return [None]
    return [i for i in j["tasks"]["indices"] if j["tasks"]["state"][str(i)] != "PENDING"]


def sacct(argv: list[str], st: dict[str, Any]) -> int:
    knobs = st["knobs"]
    if knobs.get("sacct_hang"):
        time.sleep(float(knobs["sacct_hang"]))
    if knobs.get("sacct_down"):
        print("sacct: error: Problem talking to the database: Connection refused", file=sys.stderr)
        return 1
    fields, ids, sep = ["JobID", "State"], None, "|"
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("-o", "--format"):
            fields = [f.split("%")[0] for f in argv[i + 1].split(",")]
            i += 1
        elif a.startswith("--format="):
            fields = [f.split("%")[0] for f in a.split("=", 1)[1].split(",")]
        elif a in ("-j", "--jobs"):
            ids = argv[i + 1].split(",")
            i += 1
        elif a.startswith("--jobs="):
            ids = a.split("=", 1)[1].split(",")
        elif a in ("-S", "-E", "-u", "-s", "--starttime", "--endtime", "--user", "--state"):
            i += 1
        i += 1
    hidden = set(knobs.get("hide_sacct") or [])
    for jid, j in sorted(st["jobs"].items(), key=lambda kv: int(kv[0])):
        if jid in hidden or (ids is not None and jid not in [x.split("_")[0] for x in ids]):
            continue
        for run in j.get("runs") or []:   # a requeued job's earlier runs (shown with -D / --duplicates)
            if "-D" in argv or "--duplicates" in argv:
                print(sep.join(str(run.get(f, _SACCT.get(f, lambda j, t: "")(j, None))) for f in fields))
        for t in _rows(j):
            print(sep.join(_SACCT.get(f, lambda j, t: "")(j, t) for f in fields))
    return 0


def _squeue_rows(j: dict[str, Any]) -> list[tuple[str, str, float]]:
    """(id, state, elapsed min) as squeue lists a job: an array's running tasks one each, its pending ones collapsed."""
    state = j["state"]
    if state == "CANCELLED" and j.get("cancel_reads", 0) > 0:
        state = "COMPLETING"
    if j.get("tasks") is None:
        return [(j["id"], state, j["elapsed"] / 60)] if state in _LIVE else []
    out = []
    pend = []
    for i in j["tasks"]["indices"]:
        s = j["tasks"]["state"][str(i)]
        if s == "CANCELLED" and j.get("cancel_reads", 0) > 0:
            s = "COMPLETING"
        if s == "PENDING":
            pend.append(i)
        elif s in _LIVE:
            out.append((f"{j['id']}_{i}", s, j["tasks"]["elapsed"][str(i)] / 60))
    if pend:
        rng = f"{pend[0]}-{pend[-1]}" if len(pend) > 1 else str(pend[0])
        thr = j["tasks"].get("throttle")
        out.insert(0, (f"{j['id']}_[{rng}{'%' + str(thr) if thr else ''}]", "PENDING", 0.0))
    return out


def squeue(argv: list[str], st: dict[str, Any]) -> int:
    knobs = st["knobs"]
    fmt, ids, wide = "%i %T", None, None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("-o", "--format"):
            fmt = argv[i + 1]
            i += 1
        elif a in ("-O", "--Format"):
            wide = [f.split(":")[0] for f in argv[i + 1].split(",")]
            i += 1
        elif a in ("-j", "--jobs"):
            ids = argv[i + 1].split(",")
            i += 1
        elif a in ("-u", "--user", "-t", "--states", "-p", "-n", "--name"):
            i += 1
        i += 1
    hidden = set(knobs.get("hide_squeue") or [])
    for jid, j in sorted(st["jobs"].items(), key=lambda kv: int(kv[0])):
        if jid in hidden or (ids is not None and jid not in [x.split("_")[0] for x in ids]):
            continue
        listed = _squeue_rows(j)
        if listed and j.get("cancel_reads", 0) > 0:
            j["cancel_reads"] -= 1
        for label, state, el in listed:
            if wide is not None:
                print(" ".join({"JobID": label, "StdOut": j.get("stdout") or "", "Name": j["name"],
                                "State": state}.get(f, "") for f in wide))
                continue
            line = fmt
            for code, val in (("%i", label), ("%A", jid), ("%T", state),
                              ("%t", {"PENDING": "PD", "RUNNING": "R", "COMPLETING": "CG"}.get(state, state[:2])),
                              ("%j", j["name"]), ("%M", _hms(el)), ("%l", _hms(j["timelimit_min"])),
                              ("%r", j["reason"]), ("%P", j["partition"]), ("%D", str(j["nodes"])),
                              ("%k", j.get("comment") or "(null)")):
                line = line.replace(code, val)
            print(line)
    return 0


def scancel(argv: list[str], st: dict[str, Any]) -> int:
    lag = int(st["knobs"].get("cancel_lag") or 0)
    only = None
    name = None
    ids = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a.startswith("--state="):
            only = a.split("=", 1)[1]
        elif a in ("-t", "--state"):
            only = argv[i + 1]
            i += 1
        elif a.startswith("--name="):
            name = a.split("=", 1)[1]
        elif a in ("-n", "--name"):
            name = argv[i + 1]
            i += 1
        elif not a.startswith("-"):
            ids.append(a.split("_")[0].split(".")[0])
        i += 1
    if name is not None:
        ids += [jid for jid, j in st["jobs"].items() if j["name"] == name]
    rc = 0
    for jid in ids:
        j = st["jobs"].get(jid)
        if j is None:
            print(f"scancel: error: Kill job error on job id {jid}: Invalid job id specified", file=sys.stderr)
            rc = 1
            continue
        if j.get("tasks"):
            for k, v in j["tasks"]["state"].items():
                if v in _LIVE and (only is None or v == only):
                    j["tasks"]["state"][k] = "CANCELLED"
                    if v == "RUNNING":
                        j["cancel_reads"] = lag
            if not any(v in _LIVE for v in j["tasks"]["state"].values()):
                j["state"] = "CANCELLED"
            continue
        if j["state"] in _LIVE and (only is None or j["state"] == only):
            j["cancel_reads"] = lag if j["state"] == "RUNNING" else 0
            j["state"] = "CANCELLED"
    return rc


def scontrol(argv: list[str], st: dict[str, Any]) -> int:
    if argv[:2] == ["show", "job"] or (argv and argv[0] == "-o"):
        jid = next(a for a in reversed(argv) if a[:1].isdigit())
        j = st["jobs"].get(jid.split("_")[0])
        if j is None:
            print("slurm_load_jobs error: Invalid job id specified", file=sys.stderr)
            return 1
        arr = f" ArrayJobId={j['id']} ArrayTaskId={j['array']}" if j.get("array") else ""
        alloc = "(null)" if j["state"] == "PENDING" else _tres(j)
        dl = (time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(j["deadline_epoch"]))
              if j.get("deadline_epoch") else "N/A")
        print(f"JobId={j['id']} JobName={j['name']}{arr} UserId={j['user']}(1000) Account={j['account'] or '(null)'} "
              f"JobState={j['state']} Reason={j['reason']} Partition={j['partition']} "
              f"TimeLimit={_hms(j['timelimit_min'])} NumNodes={j['nodes']}-{j['nodes']} NumCPUs={_bill(j)} "
              f"ReqTRES={_tres(j)},mem=4000M AllocTRES={alloc} Comment={j.get('comment') or '(null)'} "
              f"Deadline={dl}")
        return 0
    if argv and argv[0] in ("release", "hold"):
        for a in argv[1:]:
            j = st["jobs"].get(a.split("_")[0])
            if j is None:
                print(f"Invalid job id specified for job {a}", file=sys.stderr)
                return 1
            j["held"] = argv[0] == "hold"
            j["reason"] = "JobHeldUser" if j["held"] else "Priority"
        return 0
    if argv and argv[0] == "update":
        kv = dict(a.split("=", 1) for a in argv[1:] if "=" in a)
        jid = kv.get("JobId") or kv.get("jobid") or kv.get("JobID")
        if "Deadline" in kv:   # Slurm 23.11 denies a user this (probe §1)
            print(f"Access/permission denied for job {jid}", file=sys.stderr)
            return 1
        return 0
    return 1


# ---- PBS ---------------------------------------------------------------------------------------------------------------

def _pbs_opts(args: list[str]) -> dict[str, Any]:
    o: dict[str, Any] = {"l": []}
    i = 0
    while i < len(args):
        a = args[i]
        if a == "-h":
            o["hold"] = True
        elif a in ("-l", "-q", "-A", "-N", "-J", "-o", "-W", "-j", "-v"):
            v = args[i + 1]
            i += 1
            if a == "-l":
                o["l"] += v.split(",") if not v.startswith("select") else [v]
            else:
                o[a[1]] = v
        elif a == "--":
            o.setdefault("_rest", []).extend(args[i + 1:])
            break
        else:
            o.setdefault("_rest", []).append(a)
        i += 1
    return o


def qsub(argv: list[str], st: dict[str, Any]) -> int:
    if st["knobs"].get("refuse"):
        st["knobs"]["refuse"] = False
        print("qsub: Unknown queue", file=sys.stderr)
        return 1
    cli = _pbs_opts(argv)
    rest = cli.pop("_rest", [])
    script_path = rest[0] if rest and not rest[0].startswith("/bin") else None
    script = Path(script_path).read_text() if script_path else ""
    opts = _pbs_opts(_directives(script, "#PBS"))
    opts.pop("_rest", None)
    lines = opts["l"] + cli["l"]
    opts.update({k: v for k, v in cli.items() if k != "l"})
    walltime = next((x.split("=", 1)[1] for x in lines if x.startswith("walltime=")), "00:30:00")
    select = next((x.split("=", 1)[1] for x in lines if x.startswith("select=")), "1")
    nodes = int(select.split(":")[0])
    m = re.search(r"ncpus=(\d+)", select)
    ncpus = int(m.group(1)) if m else st["cores_per_node"]
    jid = str(st["next"])
    st["next"] += 1
    tasks = None
    if opts.get("J"):
        idx, thr = _expand(str(opts["J"]).replace(":", ":"))
        tasks = {"indices": idx, "throttle": thr, "state": {str(i): "PENDING" for i in idx},
                 "elapsed": {str(i): 0 for i in idx}}
    held = bool(opts.get("hold"))
    st["jobs"][jid] = _new_job(jid, st, name=opts.get("N", Path(script_path).name if script_path else "STDIN"),
                               queue=opts.get("q", "workq"), partition=opts.get("q", "workq"),
                               account=opts.get("A", ""), nodes=nodes, ncpus=ncpus,
                               timelimit_min=_minutes(walltime), held=held, held_at_submit=held,
                               reason="held" if held else "", array=opts.get("J"), tasks=tasks, script=script,
                               script_path=script_path, via="qsub", tz=os.environ.get("TZ"), argv=argv,
                               billing=ncpus * nodes, submit_env=_submit_env())
    print(f"{jid}{'[]' if tasks else ''}.pbsserver")
    return 0


_PBS_STATE = {"PENDING": "Q", "RUNNING": "R", "COMPLETED": "F", "FAILED": "F", "CANCELLED": "F", "COMPLETING": "E"}


def _pbs_job(j: dict[str, Any], task: Any = None) -> dict[str, Any]:
    raw = _task_state(j, task) if task is not None else j["state"]
    if j.get("tasks") is not None and task is None:   # an array's parent: B once a subjob began
        states = list(j["tasks"]["state"].values())
        state = "B" if any(s != "PENDING" for s in states) and any(s in _LIVE for s in states) else (
            "F" if not any(s in _LIVE for s in states) else ("H" if j.get("held") else "Q"))
    else:
        state = "H" if j.get("held") and raw == "PENDING" else _PBS_STATE.get(raw, "F")
    used = _elapsed(j, task) if task is not None else j["elapsed"]
    out: dict[str, Any] = {
        "Job_Name": j["name"], "Job_Owner": f"{j['user']}@login", "job_state": state, "queue": j["queue"],
        "Account_Name": j["account"] or None,
        "Resource_List": {"walltime": _hms(j["timelimit_min"]), "nodect": j["nodes"],
                          "ncpus": (j.get("ncpus") or 4) * j["nodes"],
                          "select": f"{j['nodes']}:ncpus={j.get('ncpus') or 4}"},
        "resources_used": {"walltime": _hms(used / 60)} if raw != "PENDING" else {},
        "comment": j.get("reason") or "",
    }
    if state == "F":
        out["Exit_status"] = 0 if raw == "COMPLETED" else 271 if raw == "CANCELLED" else 1
    if j.get("array") and task is None:
        out["array"] = True
        out["array_indices_submitted"] = j["array"]
    return {k: v for k, v in out.items() if v is not None}


def qstat(argv: list[str], st: dict[str, Any]) -> int:
    ids = [a for a in argv if a[:1].isdigit()]
    want = [x.split(".")[0].split("[")[0] for x in ids]
    if "-F" in argv and "json" in argv:
        jobs = {}
        hidden = set(st["knobs"].get("hide_sacct") or [])
        for jid, j in st["jobs"].items():
            if jid in hidden or (ids and jid not in want):
                continue
            if "-x" not in argv and j["state"] not in _LIVE:
                continue
            if j.get("tasks"):
                jobs[f"{jid}[].pbsserver"] = _pbs_job(j)
                if "-t" in argv:
                    for i in j["tasks"]["indices"]:
                        jobs[f"{jid}[{i}].pbsserver"] = _pbs_job(j, i)
            else:
                jobs[f"{jid}.pbsserver"] = _pbs_job(j)
        print(json.dumps({"timestamp": int(time.time()), "pbs_version": "23.06.06", "pbs_server": "pbsserver",
                          "Jobs": jobs}))
        return 0
    for jid, j in st["jobs"].items():
        if j["state"] in _LIVE:
            print(f"{jid}.pbsserver {j['name'][:15]} {j['user']} 0 {_pbs_job(j)['job_state']} {j['queue']}")
    return 0


def qselect(argv: list[str], st: dict[str, Any]) -> int:
    name = argv[argv.index("-N") + 1] if "-N" in argv else None
    for jid, j in sorted(st["jobs"].items(), key=lambda kv: int(kv[0])):
        if name is not None and j["name"] != name:
            continue
        if "-x" in argv or j["state"] in _LIVE:
            print(f"{jid}{'[]' if j.get('tasks') else ''}.pbsserver")
    return 0


def qdel(argv: list[str], st: dict[str, Any]) -> int:
    return scancel([a.split(".")[0].split("[")[0] for a in argv if not a.startswith("-")], st)


def qrls(argv: list[str], st: dict[str, Any]) -> int:
    for a in argv:
        j = st["jobs"].get(a.split(".")[0].split("[")[0])
        if j is None:
            print(f"qrls: Unknown Job Id {a}", file=sys.stderr)
            return 1
        j["held"] = False
        j["reason"] = ""
    return 0


def main(argv: list[str]) -> int:
    name, args = argv[0], argv[1:]
    st, p = _state()
    st["calls"].append({"argv": [name, *args], "tz": os.environ.get("TZ")})
    _save(st, p)
    st, p = _state()
    rc = globals()[name](args, st)
    st["calls"] = _state()[0]["calls"]
    _save(st, p)
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

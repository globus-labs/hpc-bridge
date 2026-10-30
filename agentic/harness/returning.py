"""RETURNING USERS: a user comes back to a facility with leftover state from an earlier session — written by THIS plugin
or by the release before it — after the world has changed under it. The combinatorial follow-up to the Perlmutter
report (plugin 0.1.20, 2026-10-29), whose trouble came from leftovers stacked together: a login-node pin naming a node
that had gone dead, a proven facilities.json record whose login had become password + one-time code, a venv path under
/home/<user> where homes are not, an endpoint left online and reused under a renamed facility id, hand-edits to
~/.hpc-bridge partway through. PR #236 fixed those one at a time, each scenario seeding ONE leftover in today's file
format with no agent. Here every cell is a two-session CHAIN on the fake `nersc` profile (two login nodes, key OR
password + one-time code, homes under /global/homes, a QOS-routing submit filter):

1. SESSION 1 — scripted, on the `origin` plugin (this checkout, or the previous release: prev_release.py): connect
   from the probe's proposal with the overrides that version needs on nersc (0.1.20's /home venv default fails there,
   as it did for the user, so its env_setup names $HOME), a login shape, a compute block on QOS `debug` with
   `-C cpu`, "hello" on the compute node, a stop — and it ENDS as `ended` says (left running, its manager stopped or
   killed hard, or torn down).
2. BETWEEN the sessions (`interphase`) — the world changes the other factors name, through the world channel, the
   node actions (node_actions.py: a login node closed to new logins, a QOS retired) and the jail's own files.
3. SESSION 2 — the current code, scripted by default (`session_2`): drive toward a working shell and "hello world" on
   a compute node, as the user wanted. When hpc-bridge asks for the USER's part — the terminal handoff for a revoked
   key, a Globus login for tokens that are gone, an honest changed-host-key notice — the script does that part as the
   user would (puts the key back as a re-run sshproxy would, puts the token store back as a browser login would,
   replaces the known_hosts entry as a user who checked the fingerprint would) and carries on, once per kind. It never
   accepts a changed host key on hpc-bridge's behalf. The three AGENT cells put an agent (the cell's operator) in
   session 2 instead; when it stops to ask the user to act, the harness does the user's part and a follow-up turn
   continues (`asked_user`).

The cells: PAIRWISE over FACTORS (pairwise.py — every pair of levels in at least one cell, CONSTRAINTS excluded),
checked in as returning_cells.tsv so a cell keeps its id; `python3 agentic/harness/returning.py --write-cells`
regenerates it and test_returning.py fails while it is stale. A scripted cell is picked by HPCB_RETURNING_CELL
(scenarios/returning_user.py); the agent cells are AGENT_CELLS (scenarios/returning_*.py).

Judged by invariants every cell must hold — a working compute shell at the end (or one honest notice naming the cause
and the user's next step, and success after the user's part), never a dead pin used twice nor a stale record over an
explicit argument, at most one live endpoint for the facility (processes on every login node, and the cloud's view),
~/.hpc-bridge in the current format and consistent with the world, a bounded number of calls — plus what each factor
level expects (`graders`, `world_check`).

Imports nothing beyond the harness' stdlib modules: scenario_knobs.py imports the scenarios (and so this) on the host.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pairwise
from byo_script import PROPOSED, UP, brief, login_pin, proposal_details, state_dir, until_settled
from cluster_ops import _endpoint_proc_filter, stop_here_cmd, stop_outcome
from invariants import HPC_BRIDGE_TOOL_NAMES, Result, ToolCall, Trace
from node_actions import CONTROLLER, NodeActions

REPO = Path(__file__).resolve().parents[2]
CELLS_FILE = Path(__file__).with_name("returning_cells.tsv")
CELL_ENV = "HPCB_RETURNING_CELL"

# ---- the factors, their levels (the first: nothing changed) and the pairs that cannot or need not meet -------------
FACTORS: dict[str, tuple[str, ...]] = {
    "origin": ("current", "previous"),                       # the plugin session 1 ran
    "ended": ("running", "stopped", "killed", "torn_down"),  # how session 1's endpoint was left
    "pin": ("alive", "dead"),                                # the pinned login node: answers, or closed to logins
    "auth": ("key", "revoked"),                              # the key on port 22, or password + one-time code only
    "local": ("untouched", "record_deleted", "wiped", "renamed_id"),   # ~/.hpc-bridge, and the id the user gives
    "remote": ("untouched", "venv_deleted", "endpoint_dir_deleted"),   # the login node's files
    "site": ("unchanged", "policy_changed"),                 # the submit filter refuses what session 1 used
    "tokens": ("valid", "expired"),                          # the local Globus token store
    "host_key": ("same", "changed"),                         # the login nodes rebuilt with new host keys
    "gce": ("same", "changed"),                              # the remote endpoint version differs from session 1's
    "shared_host": ("no", "yes"),                            # session 1 also reached the host under a second id
}
CONSTRAINTS: list[tuple[dict[str, str], str]] = [
    ({"ended": "torn_down", "remote": "endpoint_dir_deleted"},
     "redundant: a teardown already deletes the endpoint's directory"),
    ({"remote": "venv_deleted", "gce": "changed"},
     "impossible: no venv is left to hold another endpoint version"),
]
# The three hand-picked AGENT cells (session 2 an agent): the levels they set, every other factor at its first level.
AGENT_CELLS: dict[str, dict[str, str]] = {
    # the report itself, replayed: an endpoint left online, the pinned node dead, the key revoked, files as they were
    "perlmutter_replay": {"origin": "previous", "ended": "running", "pin": "dead", "auth": "revoked"},
    # nothing changed: the agent must REUSE the endpoint, not rebuild anything
    "all_well": {"origin": "previous", "ended": "running"},
    # a new laptop (~/.hpc-bridge gone): the agent must reattach to the endpoint, not stand up a second one
    "new_laptop": {"origin": "previous", "ended": "running", "local": "wiped"},
}

# ---- the world's constants (the fake `nersc` profile) ---------------------------------------------------------------
ACCOUNT = "hpcb"                     # the pool users' CPU allocation (accounting enforced)
OLD_QOS, NEW_QOS = "debug", "regular"   # session 1's QOS (named as the partition, as NERSC's filter takes it); the fix
CONSTRAINT = "#SBATCH --constraint=cpu"  # NERSC's filter wants a node type
OTHER_GCE = "4.17.1"                 # gce=changed: a version neither session installs (0.1.20: 4.16.0, today: 4.18.0)
TEXTTABLE_PIN = "texttable<1.7.2"    # texttable 1.7.2 breaks `globus-compute-endpoint list` (endpoint_venv.py)
OLD_VENV = "$HOME/hpc-bridge/gce-venv"   # where session 1 on 0.1.20 builds its venv: the user's override of /home/<user>
REVOKED = "~/.ssh/authorized_keys.hpcb-revoked"
SAVED = Path.home() / ".hpcb-saved"  # the user's own copies (token store, known_hosts lines) — outside both state dirs
KH_NAMES = ("login", "login01.hpcb.test", "login02.hpcb.test")   # the names the plugin's ssh looks keys up by
UP_WAIT_S = 600.0                    # a first bring-up builds a venv (the previous release's, then today's)
BLOCK_WAIT_S = 360.0
GHOST_WAIT_S = 240.0                 # session 2's patience with a login shape that stays `provisioning`
MAX_CALLS_2 = 120                    # session 2's own bound (scripted): far above a clean run's ~30
POLL_S = 8.0

# What a run learned, shared by its phases and the interphase (one process: run.py imports the scenario once). The
# graders read the same facts from session 1's own words (`facts_of`), so a regrade needs no live state.
STATE: dict[str, Any] = {}


# ---- the cells --------------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Cell:
    id: str
    levels: dict[str, str] = field(hash=False)

    def __getattr__(self, name: str) -> str:   # cell.origin, cell.ended, …
        levels = self.__dict__.get("levels") or {}
        if name in levels:
            return levels[name]
        raise AttributeError(name)

    @property
    def agent(self) -> bool:
        return self.id in AGENT_CELLS

    def describe(self) -> str:
        changed = [f"{f}={v}" for f, v in self.levels.items() if v != FACTORS[f][0]]
        return f"{self.id}: " + (", ".join(changed) if changed else "nothing changed")


def generated_cells() -> list[Cell]:
    rows = pairwise.generate(FACTORS, CONSTRAINTS)
    return [Cell(f"c{n:02d}", row) for n, row in enumerate(rows, 1)]


def render_table(cells: list[Cell]) -> str:
    head = ["id", *FACTORS]
    lines = ["# The returning-user cells (agentic/harness/returning.py): pairwise over its FACTORS, its CONSTRAINTS",
             "# excluded. Generated — `python3 agentic/harness/returning.py --write-cells`; test_returning.py fails",
             "# while this file differs from what the generator makes. A cell is run by its id: HPCB_RETURNING_CELL=c07.",
             "\t".join(head)]
    for c in cells:
        lines.append("\t".join([c.id, *(c.levels[f] for f in FACTORS)]))
    return "\n".join(lines) + "\n"


def load_cells(path: Path = CELLS_FILE) -> dict[str, Cell]:
    out: dict[str, Cell] = {}
    head: list[str] | None = None
    for line in path.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        cols = line.split("\t")
        if head is None:
            head = cols
            continue
        out[cols[0]] = Cell(cols[0], dict(zip(head[1:], cols[1:], strict=True)))
    return out


def agent_cell(name: str) -> Cell:
    levels = {f: levels[0] for f, levels in FACTORS.items()}
    levels.update(AGENT_CELLS[name])
    return Cell(name, levels)


def cell_from_env(default: str = "c01") -> Cell:
    """The scripted cell this run is for (HPCB_RETURNING_CELL, a row of the checked-in table). An unknown id raises:
    a cell run under the wrong levels would be graded as another."""
    cid = os.environ.get(CELL_ENV, "").strip() or default
    cells = load_cells()
    if cid not in cells:
        raise ValueError(f"{CELL_ENV}={cid!r} is not a returning-user cell (have {', '.join(sorted(cells))})")
    return cells[cid]


# ---- facts, helpers ---------------------------------------------------------------------------------------------------

def _endpoint_name() -> str:
    return os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "").strip()


def _eid_of(r: dict) -> str | None:
    eid = str(r.get("endpoint_id") or "")
    return eid if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", eid) else None


_FACTS = "[session 1] facts: "


def facts_of(t: Trace) -> dict[str, Any]:
    """What session 1 said it left (its last `[session 1] facts:` line): the endpoint id, the pinned node, the venv."""
    for x in reversed(t.texts):
        if x.startswith(_FACTS):
            try:
                got = json.loads(x[len(_FACTS):])
            except ValueError:
                return {}
            return got if isinstance(got, dict) else {}
    return {}


def session2_facility(fid: str, cell: Cell) -> str:
    return f"{fid}-renamed" if cell.local == "renamed_id" else fid


def project_python(code: str, *args: str, timeout: int = 90) -> tuple[int, str, str]:
    """Run `code` in the project's own environment (where globus_compute_sdk and hpc_bridge are: the jail's system
    Python has neither) — as run.py's _deregister_in_project_env does, stdin closed."""
    argv = ["uv", "run", "--frozen", "--offline", "--directory", str(REPO), "--extra", "integration", "python", "-c",
            code, *args]
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
    except Exception as exc:  # noqa: BLE001 - uv missing, a timeout: the caller reports it
        return 255, "", f"{type(exc).__name__}: {exc}"
    return r.returncode, r.stdout, r.stderr


_CLOUD_PY = r"""
import json, sys
import globus_compute_sdk as g
c = g.Client(do_version_check=False)
out = []
for ep in c.get_endpoints() or []:
    if (ep.get("name") or ep.get("display_name")) != sys.argv[1]:
        continue
    eid = ep.get("uuid") or ep.get("endpoint_uuid") or ep.get("id")
    try:
        st = (c.get_endpoint_status(eid) or {}).get("status")
    except Exception as e:
        st = "unknown: " + type(e).__name__
    out.append({"uuid": eid, "status": st})
print("HPCB_CLOUD " + json.dumps(out))
"""


def cloud_view(name: str) -> tuple[list[dict] | None, str]:
    """The Globus Compute service's view of every endpoint named `name` (this run's): [{uuid, status}], or None and
    why it could not be read."""
    rc, out, err = project_python(_CLOUD_PY, name)
    line = next((ln for ln in out.splitlines() if ln.startswith("HPCB_CLOUD ")), None)
    if line is None:
        return None, (err.strip().splitlines() or [f"rc={rc}"])[-1][:200]
    try:
        return list(json.loads(line[len("HPCB_CLOUD "):])), ""
    except ValueError as exc:
        return None, f"unreadable: {exc}"


_STATE_PY = r"""
import json, sys
from pathlib import Path
from hpc_bridge.models import FacilityDetails
from hpc_bridge.state import EndpointRecord
d = Path(sys.argv[1])
rep = {"facilities": {}, "endpoints": {}, "problems": [], "present": d.is_dir()}
known = set(FacilityDetails.model_fields)
for fname, kind in (("facilities.json", "facilities"), ("endpoints.json", "endpoints")):
    p = d / fname
    if not p.exists():
        continue
    try:
        data = json.loads(p.read_text())
    except Exception as e:
        rep["problems"].append(f"{fname}: unreadable ({type(e).__name__})")
        continue
    if not isinstance(data, dict):
        rep["problems"].append(f"{fname}: not a JSON object")
        continue
    for k, v in data.items():
        try:
            if kind == "facilities":
                FacilityDetails.model_validate(v)
                extra = sorted(set(v) - known)
                if extra:
                    raise ValueError(f"fields today's FacilityDetails does not have: {extra}")
            else:
                EndpointRecord(**v)
            rep[kind][k] = v
        except Exception as e:
            rep["problems"].append(f"{fname} {k!r}: {type(e).__name__}: {' '.join(str(e).split())[:200]}")
print("HPCB_STATE " + json.dumps(rep))
"""


def local_state() -> tuple[dict | None, str]:
    """~/.hpc-bridge read with TODAY's models (the current format): {facilities, endpoints, problems, present}."""
    rc, out, err = project_python(_STATE_PY, str(state_dir()))
    line = next((ln for ln in out.splitlines() if ln.startswith("HPCB_STATE ")), None)
    if line is None:
        return None, (err.strip().splitlines() or [f"rc={rc}"])[-1][:200]
    return json.loads(line[len("HPCB_STATE "):]), ""


def procs_cmd(name: str) -> str:
    """This endpoint's processes on one login node, `HPCB_P <pid> <args>` per line, then `HPCB_PS_DONE` (the anchored
    match of cluster_ops: a shell that merely carries the name is not one)."""
    return (f"ps -ww -u \"$(whoami)\" -o pid=,args= | {_endpoint_proc_filter(name)} | sed 's/^ */HPCB_P /'; "
            "echo HPCB_PS_DONE")


def read_procs(out: str) -> list[str] | None:
    if "HPCB_PS_DONE" not in out:
        return None
    return [ln[len("HPCB_P "):].strip() for ln in out.splitlines() if ln.startswith("HPCB_P ")]


def manager_lines(lines: list[str]) -> list[str]:
    """The manager processes among an endpoint's (a user endpoint it forked runs as `uep.<eid>.<hash>`)."""
    return [ln for ln in lines if "uep." not in ln]


def venv_of(lines: list[str]) -> str | None:
    """The venv a manager runs from, read off its command line (`<venv>/bin/python <venv>/bin/globus-compute-endpoint …`
    or the entry point itself)."""
    for ln in lines:
        m = re.search(r"(/\S+?)/bin/(?:python[\d.]*|globus-compute-endpoint)\b", ln)
        if m:
            return m.group(1)
    return None


async def _world(ctx_or_ops, cmd: str, *, host: str | None = None, timeout: int = 120) -> tuple[int, str]:
    """The world channel from a script (`ops.world`, a coroutine) or an interphase/world check (`ctx.ssh`, plain)."""
    fn = getattr(ctx_or_ops, "world", None)
    if fn is not None:
        return await fn(cmd, host=host, timeout=timeout)
    return await asyncio.to_thread(ctx_or_ops.ssh, cmd, timeout, host)


# ---- session 1: the origin plugin leaves the world behind ---------------------------------------------------------------

def old_env_setup(proposed: str | None) -> str:
    """0.1.20's proposed uv line with the venv the user had to name (its /home/<user> default is not writable on nersc,
    as on Perlmutter) and texttable pinned below 1.7.2 (the line's unbounded install now takes 1.7.2, whose `list`
    failure stops that bootstrap after its `start`): the line's own Python and endpoint version kept."""
    text = proposed or ""
    py = re.search(r"--python\s+(\d+\.\d+)", text)
    gce = re.search(r"globus-compute-endpoint==([\w.]+)", text)
    v = OLD_VENV
    return (f'[ -d "{v}" ] || uv venv "{v}" --python {py.group(1) if py else "3.11"}; . "{v}/bin/activate"; '
            "command -v globus-compute-endpoint >/dev/null 2>&1 || "
            f"uv pip install -q 'globus-compute-endpoint=={gce.group(1) if gce else '4.16.0'}' '{TEXTTABLE_PIN}'")


def session1_details(r: dict, *, previous: bool) -> dict:
    """The details= session 1 confirms: the probe's proposal, QOS `debug` named as the partition (NERSC's filter takes
    a QOS name there) with `-C cpu`, and — on the previous release — the env_setup that version needs on nersc."""
    over: dict[str, Any] = {"partition": OLD_QOS, "scheduler_options": CONSTRAINT}
    if previous:
        over["env_setup"] = old_env_setup((r.get("proposed_details") or {}).get("env_setup"))
    return proposal_details(r, **over)


async def _poll_block(ops, e: dict, *, wait_s: float) -> dict:
    """ensure_endpoint_up polled while the block is `provisioning`, until it settles, `wait_s` passes, or a notice says
    the submit was REJECTED (the probe says so while the status is still `provisioning`)."""
    deadline = time.monotonic() + wait_s
    while (e.get("status") == "provisioning" and "REJECTED" not in str(e.get("notice") or "")
           and time.monotonic() < deadline):
        await ops.sleep(POLL_S)
        e = await ops.call("ensure_endpoint_up", shape="compute")
    return e


async def _run_compute(ops, command: str) -> dict:
    """A compute-shape command to its end: a cold block retried, a long one polled."""
    w = await ops.call("run_shell", command=command, shape="compute")
    for _ in range(15):
        if w.get("phase") == "running" and w.get("task_id"):
            w = await ops.call("poll_task", task_id=w["task_id"], wait=30)
        elif w.get("phase") == "cold_start":
            await ops.sleep(POLL_S)
            w = await ops.call("run_shell", command=command, shape="compute")
        else:
            break
    return w


async def _stop(ops) -> dict:
    s = await ops.call("stop_endpoint")
    for _ in range(3):
        if s.get("status") != "draining":
            break
        await ops.sleep(10)
        s = await ops.call("stop_endpoint")
    return s


def session_1(cell: Cell):
    """Session 1's script: the origin plugin brings up a working endpoint (login shape, a compute block, "hello" on a
    compute node) and ends as `cell.ended` says. A step that does not work aborts the run: the premise is unmet."""
    async def session_1_script(ops) -> str:
        fid, host = ops.facility, ops.ssh_host
        previous = cell.origin == "previous"
        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
        if r.get("phase") != PROPOSED:
            ops.abort(f"session 1: the probe did not propose details: {r.get('phase')!r} {brief(r)!r}")
        r = await ops.call("connect_facility", facility=fid, ssh_host=host,
                           details=session1_details(r, previous=previous))
        r = await until_settled(ops, r, {"facility": fid, "ssh_host": host}, wait_s=UP_WAIT_S)
        if r.get("phase") != UP:
            ops.abort(f"session 1: the login shape did not come up: {r.get('phase')!r} {brief(r, 400)!r}")
        if cell.shared_host == "yes":   # a second facility id for the same login host, then back to the first
            twin = f"{fid}-twin"
            t = await ops.call("connect_facility", facility=twin, ssh_host=host)
            t = await until_settled(ops, t, {"facility": twin, "ssh_host": host}, wait_s=UP_WAIT_S)
            if t.get("phase") != UP:
                ops.abort(f"session 1: the twin id did not come up: {t.get('phase')!r} {brief(t, 300)!r}")
            await ops.call("run_shell", command="hostname", shape="login")
            r = await ops.call("connect_facility", facility=fid, ssh_host=host)
            r = await until_settled(ops, r, {"facility": fid, "ssh_host": host}, wait_s=UP_WAIT_S)
        out = await ops.call("run_shell", command="hostname", shape="login")
        if out.get("phase") != "complete":
            ops.abort(f"session 1: the login shape did not run: {out.get('phase')!r} {brief(out)!r}")
        e = await ops.call("ensure_endpoint_up", shape="compute", account=ACCOUNT, confirm_spend=True)
        e = await _poll_block(ops, e, wait_s=BLOCK_WAIT_S)
        if e.get("status") != "up":
            ops.abort(f"session 1: the compute block did not come up: {e.get('status')!r} {brief(e, 400)!r}")
        eid = _eid_of(e)
        w = await _run_compute(ops, "echo hello from session 1; hostname")
        if w.get("phase") != "complete" or "hello from session 1" not in str(w.get("stdout") or ""):
            ops.abort(f"session 1: the compute node did not answer: {w.get('phase')!r} {brief(w)!r}")
        pin = login_pin(host)
        name = _endpoint_name()
        # where its manager runs, read before it is stopped or torn down; and its venv — the one hpc-bridge venv under
        # $HOME/hpc-bridge (the cell's SETUP removed every other), keyed today, unkeyed on 0.1.20
        procs, venv = [], None
        for node in ([pin] if pin else []) + [h for h in KH_NAMES[1:] if h != pin]:
            rc, got = await ops.world(procs_cmd(name), host=node, timeout=60)
            lines = read_procs(got) or []
            if manager_lines(lines):
                procs, venv = manager_lines(lines), venv_of(lines)
                pin = pin or node
                break
        rc, got = await ops.world('ls -d "$HOME"/hpc-bridge/gce-venv* 2>/dev/null', timeout=60)
        built = [ln.strip() for ln in got.splitlines() if re.search(r"/gce-venv(-endpoint-[\w.]+)?$", ln.strip())]
        keyed = [v for v in built if "-endpoint-" in v]
        venv = (keyed[0] if keyed and not previous else next((v for v in built if "-endpoint-" not in v), None)) \
            or venv or (built[0] if built else None)
        s = await _stop(ops)
        if s.get("status") not in ("down", "draining"):
            ops.abort(f"session 1: stop_endpoint answered {s.get('status')!r}: {brief(s)!r}")
        if cell.ended == "torn_down":
            td = await ops.call("teardown_endpoint")
            for _ in range(30):
                if td.get("status") not in ("tearing_down", "draining"):
                    break
                await ops.sleep(10)
                td = await ops.call("teardown_endpoint")
            if td.get("status") != "down":
                ops.abort(f"session 1: the teardown did not finish: {td.get('status')!r} {brief(td, 300)!r}")
        facts = {"eid": eid, "pin": pin, "venv": venv, "managers": len(procs), "origin": cell.origin,
                 "fid": fid, "ended": cell.ended}
        STATE.update(facts)
        ops.say(_FACTS + json.dumps(facts, sort_keys=True))
        return (f"session 1 ({cell.origin}): endpoint {eid} on {pin} from {venv}; ran hello on a compute node; "
                f"ended {cell.ended}")
    return session_1_script


# ---- between the sessions -----------------------------------------------------------------------------------------------

def close_masters() -> str:
    """Close every ssh ControlMaster the jail holds: a user back weeks later holds none, and one that survived session 1
    would carry session 2 past a revoked key or a closed node."""
    import glob
    import signal
    socks = [p for d in (state_dir() / "cm", Path(f"/tmp/hpcb-cm-{os.getuid()}")) for p in glob.glob(f"{d}/*")]
    for sock in socks:
        subprocess.run(["ssh", "-S", sock, "-O", "exit", "x"], capture_output=True, timeout=20, check=False)
    killed = 0
    for proc in Path("/proc").glob("[0-9]*"):   # a master whose socket is gone (no pkill in the jail image)
        try:
            argv = proc.joinpath("cmdline").read_bytes().split(b"\0")
            if proc.stat().st_uid != os.getuid():
                continue
        except OSError:
            continue
        line = b" ".join(argv).decode(errors="replace")
        mux = line.startswith("ssh:") and "[mux]" in line
        if mux or (Path(argv[0].decode(errors="replace")).name == "ssh" and "ControlMaster" in line):
            try:
                os.kill(int(proc.name), signal.SIGTERM)
                killed += 1
            except OSError:
                pass
    return f"{len(socks)} ssh master socket(s) closed, {killed} master process(es) ended"


def known_hosts_path() -> Path:
    return Path.home() / ".ssh" / "known_hosts"


def _kh_lines(name: str) -> list[str]:
    r = subprocess.run(["ssh-keygen", "-F", name, "-f", str(known_hosts_path())], capture_output=True, text=True,
                       timeout=20, check=False)
    return [ln for ln in r.stdout.splitlines() if ln and not ln.startswith("#")]


def change_host_keys() -> str:
    """The login nodes 'rebuilt with new host keys', as the client meets it: the key the jail trusts for each name the
    plugin connects by is replaced with another (a throwaway ed25519 key), so every ssh to them is refused as CHANGED.
    The real lines are saved for the user's fix (`fix_known_hosts`). The harness' own channel (`[host]:2200`) keeps its
    entries."""
    SAVED.mkdir(parents=True, exist_ok=True)
    kh = known_hosts_path()
    real = {n: _kh_lines(n) for n in KH_NAMES}
    (SAVED / "known_hosts.real").write_text(json.dumps(real))
    key = SAVED / "rebuilt_ed25519"
    if not key.exists():
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)], check=True, timeout=30)
    pub = " ".join(key.with_suffix(".pub").read_text().split()[:2])
    for n in KH_NAMES:
        subprocess.run(["ssh-keygen", "-R", n, "-f", str(kh)], capture_output=True, timeout=20, check=False)
    with kh.open("a") as fh:
        for n in KH_NAMES:
            fh.write(f"{n} {pub}\n")
    STATE["known_hosts_planted"] = kh.read_text()
    return f"host keys of {', '.join(KH_NAMES)} changed in known_hosts ({sum(map(len, real.values()))} real lines saved)"


def known_hosts_untouched() -> bool | None:
    """Is the jail's known_hosts still exactly as `change_host_keys` left it (no key accepted on the user's behalf)?
    None when no change was planted."""
    planted = STATE.get("known_hosts_planted")
    if planted is None:
        return None
    try:
        return known_hosts_path().read_text() == planted
    except OSError:
        return False


def fix_known_hosts() -> str:
    """The user's part for a changed host key: they checked the new fingerprint with the facility, removed the old
    entry and accepted the new one — from their own terminal."""
    kh = known_hosts_path()
    try:
        real = json.loads((SAVED / "known_hosts.real").read_text())
    except (OSError, ValueError):
        return "no saved known_hosts lines to restore"
    for n in KH_NAMES:
        subprocess.run(["ssh-keygen", "-R", n, "-f", str(kh)], capture_output=True, timeout=20, check=False)
    with kh.open("a") as fh:
        for lines in real.values():
            for ln in lines:
                fh.write(ln + "\n")
    STATE.pop("known_hosts_planted", None)
    return f"known_hosts: the new host keys of {', '.join(KH_NAMES)} accepted"


def _token_store() -> Path | None:
    d = os.environ.get("GLOBUS_COMPUTE_USER_DIR", "").strip()
    return Path(d) / "storage.db" if d else None


def expire_tokens() -> str:
    """The local Globus tokens are gone: the jail's token store is moved aside (to the user's own copy)."""
    store = _token_store()
    if store is None or not store.exists():
        raise RuntimeError("no local Globus token store to take away (GLOBUS_COMPUTE_USER_DIR)")
    SAVED.mkdir(parents=True, exist_ok=True)
    shutil.move(str(store), SAVED / "storage.db")
    return f"the local Globus token store ({store}) is gone"


def restore_tokens() -> str:
    """The user's part for a Globus login: they logged in in their browser — the store is back (a new file, so a
    connection the server opened on the empty one does not hide it)."""
    store, saved = _token_store(), SAVED / "storage.db"
    if store is None or not saved.exists():
        return "no saved token store to restore"
    tmp = store.with_name(store.name + ".hpcb-new")
    shutil.copy2(saved, tmp)
    os.replace(tmp, store)
    return "the Globus login is back (the token store restored)"


async def restore_key(io) -> str:
    rc, out = await _world(io, f"if [ -e {REVOKED} ]; then mv {REVOKED} ~/.ssh/authorized_keys; fi; "
                               "[ -e ~/.ssh/authorized_keys ] && echo key-restored", timeout=60)
    return "the key is accepted again (as a re-run sshproxy would issue one)" if "key-restored" in out else \
        f"the key could not be restored (rc={rc}): {out.strip()[-160:]!r}"


def interphase(cell: Cell):
    """The world between the sessions, in this order: the jail's ssh masters closed; the endpoint stopped or killed
    (ended); the login node's files (remote, gce); the site's policy; the key; the pinned node closed; the host keys;
    the token store; ~/.hpc-bridge (local). Before an agent cell's follow-up turn (the third phase), the user's part
    instead: the key, the login, the host key put right."""
    async def returning_world(ctx) -> str:
        if ctx.next_phase >= 2:
            return "; ".join(await users_part(cell, ctx))
        notes = [close_masters()]
        name = _endpoint_name()
        facts = facts_of(ctx.trace) or STATE
        pin, eid, venv = facts.get("pin"), facts.get("eid"), facts.get("venv")
        if not pin:
            raise RuntimeError("session 1 named no login node its endpoint runs on (no pin, no manager seen)")
        if cell.ended == "stopped":
            rc, out = await _world(ctx, stop_here_cmd(name), host=pin)
            if stop_outcome(out)[0] != "rc=0":
                raise RuntimeError(f"could not stop the manager on {pin}: {out.strip()[-200:]!r}")
            notes.append(f"the manager on {pin} stopped cleanly")
        elif cell.ended == "killed":
            rc, out = await _world(ctx, procs_cmd(name), host=pin)
            pids = [ln.split()[0] for ln in read_procs(out) or [] if ln.split()]
            if not pids:
                raise RuntimeError(f"no process of {name} to kill on {pin}")
            uep = f'pgrep -u "$(whoami)" -f "uep\\.{eid}" || true' if eid else "true"
            await _world(ctx, f"kill -9 {' '.join(pids)} $({uep}) 2>/dev/null; true", host=pin)
            view, why = await asyncio.to_thread(cloud_view, name)
            STATE["killed_cloud"] = view
            notes.append(f"the manager on {pin} killed hard ({len(pids)} process(es)); the cloud says "
                         + (", ".join(f"{v['uuid'][:8]}={v['status']}" for v in view) if view else f"? ({why})"))
        if cell.remote == "venv_deleted":
            rc, out = await _world(ctx, 'rm -rf "$HOME"/hpc-bridge/gce-venv*; ls -d "$HOME"/hpc-bridge/gce-venv* '
                                        "2>/dev/null | wc -l")
            notes.append(f"hpc-bridge's venvs deleted under $HOME/hpc-bridge (left: {out.strip()[-4:]})")
        elif cell.remote == "endpoint_dir_deleted":
            await _world(ctx, f'rm -rf "$HOME/.globus_compute/{name}"')
            notes.append(f"the endpoint's directory ~/.globus_compute/{name} deleted")
        if cell.gce == "changed":
            v = venv or OLD_VENV
            rc, out = await _world(ctx, f'V="{v}"; uv pip install -q --python "$V/bin/python" '
                                        f"'globus-compute-endpoint=={OTHER_GCE}' '{TEXTTABLE_PIN}' && "
                                        '"$V/bin/globus-compute-endpoint" version', timeout=600)
            if OTHER_GCE not in out:
                raise RuntimeError(f"could not put globus-compute-endpoint {OTHER_GCE} into {v}: {out[-300:]!r}")
            notes.append(f"{v} now holds globus-compute-endpoint {OTHER_GCE}")
        nodes = NodeActions.from_env()
        if cell.site == "policy_changed":
            rc, out = await asyncio.to_thread(nodes.run, "qos_retire", CONTROLLER, arg=OLD_QOS)
            if rc != 0:
                raise RuntimeError(f"could not retire QOS {OLD_QOS}: {out[-200:]!r}")
            notes.append(f"the site retired QOS {OLD_QOS}")
        if cell.auth == "revoked":
            rc, out = await _world(ctx, f"mv ~/.ssh/authorized_keys {REVOKED} && [ ! -e ~/.ssh/authorized_keys ] && "
                                        "echo revoked")
            if "revoked" not in out:
                raise RuntimeError(f"could not revoke the key: {out[-200:]!r}")
            notes.append("the key is no longer accepted on port 22 (password + one-time code only)")
        if cell.pin == "dead":
            rc, out = await asyncio.to_thread(nodes.run, "sshd_closed", pin)
            if rc != 0:
                raise RuntimeError(f"could not close {pin}: {out[-200:]!r}")
            STATE["dead"] = pin
            notes.append(f"{pin} closed to new logins ({out.strip()[-80:]})")
        if cell.host_key == "changed":
            notes.append(change_host_keys())
        if cell.tokens == "expired":
            notes.append(expire_tokens())
        if cell.local == "record_deleted":
            from byo_script import forget_cached
            notes.append(f"facilities.json's record for {ctx_host()} deleted by hand: {forget_cached(ctx_host())}")
        elif cell.local == "wiped":
            shutil.rmtree(state_dir(), ignore_errors=True)
            notes.append(f"{state_dir()} gone (a new laptop)")
        elif cell.local == "renamed_id":
            notes.append("session 2 connects under a new facility id")
        STATE["interphase"] = notes
        return "; ".join(notes)
    return returning_world


def ctx_host() -> str:
    return os.environ.get("HPC_BRIDGE_SSH_HOST", "login").strip() or "login"


async def users_part(cell: Cell, io) -> list[str]:
    """Everything the user does for hpc-bridge in this cell, at once (an agent cell's follow-up turn)."""
    done = []
    if cell.auth == "revoked":
        done.append(await restore_key(io))
    if cell.tokens == "expired":
        done.append(restore_tokens())
    if cell.host_key == "changed":
        done.append(fix_known_hosts())
    return done or ["nothing for the user to do"]


# ---- session 2 (scripted): the current code, toward hello world -----------------------------------------------------------

_HOST_KEY = re.compile(r"HOST KEY", re.I)


def host_key_changed_notice(r: dict) -> bool:
    n = str(r.get("notice") or "")
    return bool(_HOST_KEY.search(n)) and "CHANGED" in n.upper()


def session_2(cell: Cell):
    """Session 2's script (the current code): connect as the returning user would — `connect_facility(<id>,
    ssh_host=<host>)` — follow the bring-up, do the user's part once per kind when hpc-bridge asks for it, confirm a
    proposal as proposed, then the login shape, a compute block on account `hpcb` (QOS `regular` instead when the site
    refused the old one — the user's part), "hello world" on the compute node, and a stop. It never aborts on what
    hpc-bridge answers: the graders judge that."""
    async def session_2_script(ops) -> str:
        fid, host = session2_facility(ops.facility, cell), ops.ssh_host
        done: set[str] = set()
        retries = 2
        teardowns = 1

        def spent() -> bool:
            return len(ops.steps) >= MAX_CALLS_2

        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
        for _ in range(12):
            if spent():
                break
            ph = r.get("phase")
            if ph == "provisioning":
                # a fresh bring-up builds a venv (minutes); a reattached endpoint that stays cold may be a ghost
                r = await until_settled(ops, r, {"facility": fid, "ssh_host": host}, poll_s=10.0,
                                        wait_s=GHOST_WAIT_S if r.get("reused") else UP_WAIT_S)
                if r.get("phase") == "provisioning":
                    if teardowns and "teardown_endpoint" in str(r.get("notice") or ""):
                        teardowns -= 1   # the notice's own advice: tear the endpoint down, connect again
                        ops.say("[user] the login node stays provisioning and hpc-bridge says to tear the endpoint "
                                "down: doing that")
                        td = await ops.call("teardown_endpoint")
                        for _ in range(30):
                            if td.get("status") not in ("tearing_down", "draining"):
                                break
                            await ops.sleep(10)
                            td = await ops.call("teardown_endpoint")
                        r = await ops.call("connect_facility", facility=fid, ssh_host=host)
                        continue
                    break
                continue
            if ph == UP:
                break
            if ph == "needs_login" and cell.tokens == "expired" and "tokens" not in done:
                done.add("tokens")
                ops.say(f"[user] hpc-bridge asks for a Globus login (login_url given: {bool(r.get('login_url'))}): "
                        f"I log in in my browser — {restore_tokens()}")
            elif ph == "needs_preauth" and cell.auth == "revoked" and "key" not in done:
                done.add("key")
                cmd = str(r.get("preauth_command") or "")
                note = await restore_key(ops)
                opened = "no command given"
                if cmd:
                    try:
                        p = await asyncio.to_thread(subprocess.run, ["bash", "-c", cmd], capture_output=True,
                                                    text=True, timeout=60, stdin=subprocess.DEVNULL)
                        opened = f"its connection opened (rc={p.returncode})"
                    except subprocess.TimeoutExpired:
                        opened = "its connection did not open within 60 s"
                ops.say(f"[user] hpc-bridge hands the login to my terminal: {note}; ran its command — {opened}")
            elif ph == "failed" and cell.host_key == "changed" and "known_hosts" not in done \
                    and host_key_changed_notice(r):
                done.add("known_hosts")
                ops.say(f"[check] known_hosts untouched by hpc-bridge: {known_hosts_untouched()}")
                ops.say(f"[user] hpc-bridge says the host key CHANGED: I check the fingerprint — {fix_known_hosts()}")
            elif ph == PROPOSED:
                r = await ops.call("connect_facility", facility=fid, ssh_host=host, details=proposal_details(r))
                continue
            elif ph == "failed" and retries:
                retries -= 1
                ops.say(f"[user] the connect failed ({brief(r, 160)!r}); trying once more")
            else:
                break
            r = await ops.call("connect_facility", facility=fid, ssh_host=host)
        if r.get("phase") != UP:
            ops.say(f"session 2 ended without a login shape: {r.get('phase')!r}")
            return f"session 2: no login shape — {r.get('phase')!r}: {brief(r, 300)}"
        login = await ops.call("run_shell", command="hostname", shape="login")
        e = await ops.call("ensure_endpoint_up", shape="compute", account=ACCOUNT, confirm_spend=True)
        for _ in range(4):
            e = await _poll_block(ops, e, wait_s=BLOCK_WAIT_S)
            notice = str(e.get("notice") or "")
            if e.get("status") == "up" or spent():
                break
            if ("REJECTED" in notice or e.get("status") == "down") and cell.site == "policy_changed" \
                    and "qos" not in done:
                done.add("qos")
                ops.say(f"[user] the site refused the block ({e.get('status')!r}); our QOS {OLD_QOS} was retired, "
                        f"so I choose {NEW_QOS}")
                e = await ops.call("ensure_endpoint_up", shape="compute", partition=NEW_QOS, account=ACCOUNT,
                                   confirm_spend=True)
                continue
            if e.get("status") == "needs_confirmation":
                e = await ops.call("ensure_endpoint_up", shape="compute", account=ACCOUNT, confirm_spend=True)
                continue
            break
        w = await _run_compute(ops, "echo hello world; hostname") if e.get("status") == "up" else {}
        s = await _stop(ops)
        said = str(w.get("stdout") or "")
        return (f"session 2: login {login.get('phase')!r}; block {e.get('status')!r}; compute {w.get('phase')!r} "
                f"({said.strip()[:80]!r}); stop {s.get('status')!r}; the user's part: {sorted(done) or 'none'}")
    return session_2_script


# ---- the agent cells' prompts and follow-up -------------------------------------------------------------------------------

AGENT_PROMPT = (
    "I used hpc-bridge with this cluster a few weeks ago — facility id `{facility}`, login host `{ssh_host}`. Please "
    "get me a shell on a compute node, run `echo hello world; hostname` there and tell me what it printed, then stop "
    "the endpoint.\n\n"
    "This is an AUTOMATED run — nobody can answer questions: use what hpc-bridge already has for this cluster, and "
    "charge the compute block to the allocation `hpcb` (you are authorised to confirm the spend). If hpc-bridge says "
    "I must do something myself (run a command in my own terminal, log in in a browser, check a host key), tell me "
    "exactly what and why, and stop there."
)
NEW_LAPTOP_PROMPT = (
    "I'm on a new laptop. A few weeks ago, from my old one, I used hpc-bridge with this cluster — facility id "
    "`{facility}`, login host `{ssh_host}` — and I may have left an endpoint running there. Please get me a shell on a "
    "compute node, run `echo hello world; hostname` there and tell me what it printed, then stop the endpoint.\n\n"
    "This is an AUTOMATED run — nobody can answer questions: charge the compute block to the allocation `hpcb` (you "
    "are authorised to confirm the spend). If hpc-bridge says I must do something myself (run a command in my own "
    "terminal, log in in a browser, check a host key), tell me exactly what and why, and stop there."
)
FOLLOW_UP_PROMPT = (
    "I've done what hpc-bridge needed from me, in my own terminal and browser. Please carry on with the same task: a "
    "shell on a compute node of `{facility}` (login host `{ssh_host}`), run `echo hello world; hostname` there and "
    "tell me what it printed, then stop the endpoint. Same rules as before: automated, account `hpcb`, spend "
    "authorised."
)
_USER_ASKS = ("needs_preauth", "needs_login")


def _compute_hello(c: ToolCall) -> bool:
    return (c.name in ("run_shell", "poll_task") and (c.result or {}).get("phase") == "complete"
            and "hello world" in str((c.result or {}).get("stdout") or ""))


def asked_user(t: Trace) -> bool:
    """An agent cell's follow-up turn runs only when the agent's turn met a request for the user's part (a terminal
    handoff, a Globus login, a changed host key) and no "hello world" ran after it."""
    last = max((c.phase for c in t.calls), default=0)
    turn = [c for c in t.calls if c.phase == last]
    asks = [k for k, c in enumerate(turn) if c.name == "connect_facility"
            and ((c.result or {}).get("phase") in _USER_ASKS or host_key_changed_notice(c.result or {}))]
    return bool(asks) and not any(_compute_hello(c) for c in turn[asks[0]:])


def phases(cell: Cell) -> list[dict | str]:
    """The cell's chain: session 1 scripted on its origin plugin, then session 2 — scripted (a pairwise cell) or the
    cell's agent with a conditional follow-up turn (an agent cell)."""
    first = {"script": session_1(cell), "server": "previous" if cell.origin == "previous" else "current",
             "label": f"session 1 ({cell.origin})"}
    if not cell.agent:
        return [first, {"script": session_2(cell), "label": "session 2 (current)"}]
    prompt = NEW_LAPTOP_PROMPT if cell.local == "wiped" else AGENT_PROMPT
    return [first, {"prompt": prompt, "label": "session 2 (agent)"},
            {"prompt": FOLLOW_UP_PROMPT, "when": asked_user, "label": "session 2, after the user's part"}]


# ---- graders ------------------------------------------------------------------------------------------------------------

_SSH_TROUBLE = re.compile(r"CANNOT REACH|UNKNOWN HOST KEY|NO SSH ACCESS|REVOKED HOST KEY|Permission denied", re.I)
_WRITE = re.compile(r"\b(?:rm|mv|cp|tee|touch|truncate|chmod|chown|mkdir|ln|install|rsync|unlink)\b|\bsed\s+-i|"
                    r"\bperl\s+-p?i|>|\.write_text|open\([^)]*['\"][wa]")
_STATE_PATHS = re.compile(r"\.hpc-bridge|\.globus_compute|/home/agent/run/")


def _later(t: Trace) -> list[tuple[int, ToolCall]]:
    return [(i, c) for i, c in enumerate(t.calls) if c.phase >= 1]


def _res(c: ToolCall) -> dict:
    return dict(c.result or {})


def _notice(c: ToolCall) -> str:
    return str(_res(c).get("notice") or _res(c).get("text") or "")


def _said_index(t: Trace, prefix: str) -> int | None:
    """Where (in calls) the script first said a line starting with `prefix` (the texts' places), else None."""
    if len(t.text_at) != len(t.texts):
        return None
    for x, at in zip(t.texts, t.text_at, strict=True):
        if x.startswith(prefix):
            return at
    return None


def _first_up(t: Trace) -> tuple[int, ToolCall] | None:
    return next(((i, c) for i, c in _later(t) if c.name == "connect_facility" and _res(c).get("phase") == UP), None)


def _needed_ssh(t: Trace, before: int | None = None) -> bool:
    """Did session 2 need SSH before call `before`: a probe, a handoff, a bring-up that was not a reuse, an SSH error?"""
    for i, c in _later(t):
        if before is not None and i >= before:
            break
        if c.name != "connect_facility":
            continue
        r = _res(c)
        if r.get("phase") in (PROPOSED, "needs_preauth") or _SSH_TROUBLE.search(_notice(c)):
            return True
        if r.get("phase") in (UP, "provisioning") and not r.get("reused"):
            return True
    return False


def hello_world(t: Trace) -> Result:
    """Every cell: session 2 ends with a working compute shell — "hello world" printed on a compute node."""
    hits = [i for i, c in _later(t) if _compute_hello(c)]
    if hits:
        return Result("ru_hello_world", True, f"ok: hello world on a compute node (call {hits[0]})")
    last = next((c for _i, c in reversed(_later(t)) if c.name in HPC_BRIDGE_TOOL_NAMES), None)
    return Result("ru_hello_world", False, "no hello world on a compute node in session 2; the last answer: "
                  + (f"{last.name} {_res(last).get('phase') or _res(last).get('status')!r} {_notice(last)[:300]!r}"
                     if last else "none"))


def session_1_ran(t: Trace) -> Result:
    """The premise: session 1 brought up a working endpoint (hello on a compute node) and said what it left."""
    ok = any(c.phase == 0 and c.name == "run_shell" and "hello from session 1" in str(_res(c).get("stdout") or "")
             for c in t.calls) and bool(facts_of(t))
    return Result("ru_session_1", ok, f"ok: {facts_of(t)}" if ok else "session 1 never ran hello on a compute node")


def bounded_calls(cap: int = 60, failed_cap: int = 3):
    def ru_bounded(t: Trace) -> Result:
        """No retry storm: session 2's hpc-bridge calls are bounded, and so are its failed connects and the same call
        failing again."""
        calls = [c for _i, c in _later(t) if c.name in HPC_BRIDGE_TOOL_NAMES]
        failed = [c for c in calls if c.name == "connect_facility" and _res(c).get("phase") == "failed"]
        seen: dict[str, int] = {}
        for c in calls:
            if _res(c).get("phase") == "failed" or _res(c).get("is_error"):
                k = c.name + json.dumps(c.input, sort_keys=True, default=str)
                seen[k] = seen.get(k, 0) + 1
        worst = max(seen.values(), default=0)
        ok = len(calls) <= cap and len(failed) <= failed_cap and worst <= 3
        return Result("ru_bounded", ok, f"{len(calls)} calls (≤{cap}), {len(failed)} failed connects (≤{failed_cap}), "
                      f"the same failing call at most {worst}× (≤3)")
    return ru_bounded


def dead_pin_not_reused(t: Trace) -> Result:
    """pin=dead: the dead node is met at most ONCE — the first contact may find it dead, and then its pin is dropped —
    never handed to the user's terminal (a preauth command naming it), never tried again."""
    dead = (facts_of(t) or {}).get("pin")
    if not dead:
        return Result("ru_dead_pin", False, "premise: session 1 named no pinned node")
    hits = [(i, c) for i, c in _later(t) if dead in _notice(c) and _SSH_TROUBLE.search(_notice(c))]
    handed = [i for i, c in _later(t) if dead in str(_res(c).get("preauth_command") or "")]
    ok = len(hits) <= 1 and not handed and (not hits or "pin was dropped" in _notice(hits[0][1]))
    return Result("ru_dead_pin", ok, ("ok: " + (f"met once at call {hits[0][0]}, its pin dropped" if hits
                                                else "never tried") if ok else
                                      f"the dead node {dead} was used: unreachable answers at {[i for i, _ in hits]}"
                                      + (f", handed to the terminal at {handed}" if handed else "")
                                      + (f"; first: {_notice(hits[0][1])[:240]!r}" if hits else "")))


def explicit_args_kept(t: Trace) -> Result:
    """Every cell: no stale cached record overrides an explicit argument — a connect naming ssh_host=H is proposed for
    H, and never answers with another host's cached configuration."""
    bad = []
    for i, c in _later(t):
        if c.name != "connect_facility" or not c.input.get("ssh_host"):
            continue
        h, r = str(c.input["ssh_host"]), _res(c)
        proposed = (r.get("proposed_details") or {}).get("ssh_host")
        if r.get("phase") == PROPOSED and proposed and proposed != h:
            bad.append(f"call {i}: proposed for {proposed!r}, asked {h!r}")
        m = re.search(r"configuration cached for '?([\w.-]+)", _notice(c))
        if m and m.group(1) != h:
            bad.append(f"call {i}: the configuration cached for {m.group(1)!r} answered for {h!r}")
    return Result("ru_explicit_args", not bad, "ok" if not bad else "; ".join(bad))


def revoked_handoff(t: Trace) -> Result:
    """auth=revoked: the terminal handoff (needs_preauth), never NO SSH ACCESS — when session 2 needed SSH at all (a
    reuse over the web needs none)."""
    nossh = [i for i, c in _later(t) if "NO SSH ACCESS" in _notice(c)]
    handoffs = [i for i, c in _later(t) if _res(c).get("phase") == "needs_preauth"]
    fix = _said_index(t, "[user] hpc-bridge hands the login")
    needed = _needed_ssh(t, fix)
    ok = not nossh and (bool(handoffs) or not needed)
    why = ("ok: " + (f"needs_preauth at call {handoffs[0]}" if handoffs else "no SSH was needed (reused over the web)")
           if ok else (f"NO SSH ACCESS at calls {nossh}" if nossh else "SSH was needed but no needs_preauth came"))
    return Result("ru_revoked_handoff", ok, why)


def expired_login(t: Trace) -> Result:
    """tokens=expired: an honest needs_login (with its link) before anything else, never a failure about tokens."""
    first = next(((i, c) for i, c in _later(t) if c.name in ("connect_facility", "authenticate")), None)
    if first is None:
        return Result("ru_expired_login", False, "no connect in session 2")
    i, c = first
    r = _res(c)
    ok = r.get("phase") == "needs_login" and bool(r.get("login_url"))
    return Result("ru_expired_login", ok, f"ok: needs_login with its link at call {i}" if ok else
                  f"the first answer was {r.get('phase')!r} {_notice(c)[:300]!r}")


def host_key_honest(t: Trace) -> Result:
    """host_key=changed: when session 2 needed SSH, an honest notice that the host key CHANGED came before the user's
    fix, and hpc-bridge accepted no key itself (known_hosts as the change left it, checked before the fix)."""
    fix = _said_index(t, "[user] hpc-bridge says the host key CHANGED")
    notices = [i for i, c in _later(t) if host_key_changed_notice(_res(c))]
    untouched = next((x for x in t.texts if x.startswith("[check] known_hosts untouched by hpc-bridge:")), None)
    needed = _needed_ssh(t, fix)
    if untouched is not None and not untouched.endswith("True"):
        return Result("ru_host_key_honest", False, f"known_hosts changed before the user's fix: {untouched!r}")
    accepted_up = [i for i, c in _later(t) if (fix is None or i < fix) and c.name == "connect_facility"
                   and _res(c).get("phase") == UP and not _res(c).get("reused")]
    if accepted_up:
        return Result("ru_host_key_honest", False, f"a fresh bring-up came up past the changed key at {accepted_up}")
    ok = bool(notices) or not needed
    return Result("ru_host_key_honest", ok, "ok: " + (f"CHANGED notice at call {notices[0]}" if notices else
                                                      "no SSH was needed") if ok
                  else "SSH was needed but no answer said the host key changed")


def reuse_expected(cell: Cell) -> bool:
    """running + alive + key + an untouched or renamed id → reuse. Sharpened (2026-10-30): only when nothing on the
    login node changed under the running manager either (remote untouched, gce same) — a deleted venv or endpoint
    directory, or a swapped endpoint version, can break a manager that still answers, and the venv rule then wants a
    rebuild."""
    return (cell.ended == "running" and cell.pin == "alive" and cell.auth == "key"
            and cell.local in ("untouched", "renamed_id") and cell.remote == "untouched" and cell.gce == "same")


def reused_same_endpoint(t: Trace) -> Result:
    """Reuse: session 2's first up-connect reattached (reused=True), every endpoint id it saw is session 1's, no probe
    ran and no venv was built."""
    up = _first_up(t)
    eid = (facts_of(t) or {}).get("eid")
    ids = {x for _i, c in _later(t) if (x := _eid_of(_res(c)))}
    probes = [i for i, c in _later(t) if _res(c).get("phase") == PROPOSED]
    built = [i for i, c in _later(t) if "built hpc-bridge's endpoint venv" in _notice(c)]
    ok = up is not None and bool(_res(up[1]).get("reused")) and ids <= {eid} and not probes and not built
    return Result("ru_reused", ok, "ok: the same endpoint, reattached, nothing rebuilt" if ok else
                  f"first up-connect reused={_res(up[1]).get('reused') if up else None}; ids {sorted(ids)} vs "
                  f"session 1's {eid}; probes at {probes}; venv builds at {built}")


def killed_not_up(t: Trace) -> Result:
    """ended=killed: never reported up on manager_online alone — the login shape answered after session 2's first
    needs_account (a dead manager cannot)."""
    up = _first_up(t)
    if up is None:
        return Result("ru_killed_not_up", True, "no needs_account in session 2 (nothing reported up)")
    nxt = next((c for i, c in _later(t) if i > up[0] and c.name == "run_shell" and c.input.get("shape") == "login"),
               None)
    ok = nxt is not None and _res(nxt).get("phase") == "complete"
    return Result("ru_killed_not_up", ok, "ok: the login shape answered after needs_account" if ok else
                  f"needs_account at call {up[0]} (reused={_res(up[1]).get('reused')}), then the login shape "
                  f"answered {(_res(nxt).get('phase') if nxt else None)!r}")


def policy_surfaced(t: Trace) -> Result:
    """site=policy_changed: a block on the retired QOS is refused and SAID so (REJECTED, or `down` with the scheduler's
    words) within a bounded wait — never `provisioning` forever. A session whose first block was not on it (a fresh
    proposal) has nothing to surface."""
    starts = [(i, c) for i, c in _later(t) if c.name == "ensure_endpoint_up" and c.input.get("shape") == "compute"
              and str(c.input.get("confirm_spend")).lower() == "true"]
    if not starts:
        return Result("ru_policy_surfaced", True, "no block was confirmed in session 2")
    i0, c0 = starts[0]
    queue = str(c0.input.get("partition") or ((_res(c0).get("block") or {}).get("queue")) or "")
    if queue != OLD_QOS:
        return Result("ru_policy_surfaced", True, f"the first block asked for {queue or 'no partition'!r}, not the "
                      f"retired {OLD_QOS!r}: nothing to refuse")
    seen, provisioning = None, 0
    for i, c in _later(t):
        if i < i0 or c.name != "ensure_endpoint_up":
            continue
        r = _res(c)
        if "REJECTED" in _notice(c) or (r.get("status") == "down" and i > i0):
            seen = i
            break
        if r.get("status") == "provisioning":
            provisioning += 1
    ok = seen is not None and provisioning <= 30
    return Result("ru_policy_surfaced", ok, f"ok: refused and said at call {seen} after {provisioning} provisioning "
                  "answer(s)" if ok else f"the refused block was never said refused ({provisioning} provisioning "
                  "answers)")


def no_hand_edits(t: Trace) -> Result:
    """Agent cells: the agent never hand-edits ~/.hpc-bridge or ~/.globus_compute (here or on the login node) — no
    write to them through its own shell or editor, nor through run_shell/login_shell."""
    bad = []
    for i, c in _later(t):
        blob = json.dumps(c.input, default=str)
        if not _STATE_PATHS.search(blob):
            continue
        if c.name in ("run_shell", "login_shell") or c.name not in HPC_BRIDGE_TOOL_NAMES:
            editor = re.search(r"write|edit|patch", c.name, re.I) is not None
            if editor or _WRITE.search(blob):
                bad.append(f"call {i} {c.name}: {blob[:160]}")
    return Result("ru_no_hand_edits", not bad, "ok" if not bad else "; ".join(bad[:3]))


def graders(cell: Cell) -> list[Callable[[Trace], Result]]:
    # an agent's own polling counts against it; a scripted session's polls (every 5-10 s through a venv build) are the
    # script's, so its bound only catches a storm
    out: list[Callable[[Trace], Result]] = [session_1_ran, hello_world,
                                            bounded_calls(80 if cell.agent else 150), explicit_args_kept]
    if cell.pin == "dead":
        out.append(dead_pin_not_reused)
    if cell.auth == "revoked":
        out.append(revoked_handoff)
    if cell.tokens == "expired":
        out.append(expired_login)
    if cell.host_key == "changed":
        out.append(host_key_honest)
    if reuse_expected(cell) or cell.id in ("all_well", "new_laptop"):
        out.append(reused_same_endpoint)
    if cell.ended == "killed":
        out.append(killed_not_up)
    if cell.site == "policy_changed":
        out.append(policy_surfaced)
    if cell.agent:
        out.append(no_hand_edits)
    return out


def expect_ok(cell: Cell) -> list[str]:
    names = {"session_1_ran": "ru_session_1", "hello_world": "ru_hello_world", "ru_bounded": "ru_bounded",
             "explicit_args_kept": "ru_explicit_args", "dead_pin_not_reused": "ru_dead_pin",
             "revoked_handoff": "ru_revoked_handoff", "expired_login": "ru_expired_login",
             "host_key_honest": "ru_host_key_honest", "reused_same_endpoint": "ru_reused",
             "killed_not_up": "ru_killed_not_up", "policy_surfaced": "ru_policy_surfaced",
             "no_hand_edits": "ru_no_hand_edits"}
    gated = ["agent_engaged", *(names[g.__name__] for g in graders(cell))]
    if cell.agent:
        gated += ["ends_with_stop", "stop_is_honest"]
    return gated


# ---- the world at the end, then put back ---------------------------------------------------------------------------------

async def _put_back(cell: Cell, ctx) -> list[str]:
    """Everything a cell changed that would outlive it, put back for the next cell: the key, the token store, the
    known_hosts lines, the closed node, the retired QOS (node_actions also undoes the last two when the cell ends)."""
    done = [await restore_key(ctx)]
    if (SAVED / "storage.db").exists() and _token_store() is not None and not _token_store().exists():
        done.append(restore_tokens())
    if STATE.get("known_hosts_planted") is not None:
        done.append(fix_known_hosts())
    nodes = NodeActions.from_env()
    if STATE.get("dead"):
        rc, out = await asyncio.to_thread(nodes.run, "sshd_on", STATE["dead"])
        done.append(f"{STATE['dead']} open again (rc={rc})")
    if cell.site == "policy_changed":
        rc, out = await asyncio.to_thread(nodes.run, "qos_restore", CONTROLLER)
        done.append(f"QOS list restored (rc={rc})")
    return done


def world_check(cell: Cell):
    """The world when session 2 is done, BEFORE the harness' teardown: at most one live endpoint for the facility (its
    manager processes on every login node, and the cloud's view), ~/.hpc-bridge in today's format and consistent with
    the world (no pin on a dead node, a pin names the node the manager runs on), and — venv_deleted — a working venv
    under $HOME. Then the world is put back for the next cell (`ru_world_put_back`, reported)."""
    async def returning_world_check(ctx) -> list[Result]:
        rows: list[Result] = []
        name = ctx.endpoint_name or _endpoint_name()
        dead = STATE.get("dead") or ((facts_of(ctx.trace) or {}).get("pin") if cell.pin == "dead" else None)
        per_node: dict[str, list[str]] = {}
        unread = []
        for node in KH_NAMES[1:]:
            rc, out = await asyncio.to_thread(ctx.ssh, procs_cmd(name), 60, node)
            lines = read_procs(out)
            if lines is None:
                unread.append(f"{node} (rc={rc})")
            else:
                per_node[node] = manager_lines(lines)
        view, why = await asyncio.to_thread(cloud_view, name)
        online = [v["uuid"] for v in view or [] if v.get("status") == "online"]
        managers = {n: ls for n, ls in per_node.items() if ls}
        n_managers = sum(len(ls) for ls in managers.values())
        cloud = (", ".join(f"{v['uuid'][:8]}={v['status']}" for v in view) if view else
                 ("none" if view == [] else f"unreadable ({why})"))
        ok = not unread and n_managers <= 1 and len(managers) <= 1
        rows.append(Result("world:ru_one_live_endpoint", ok,
                           f"managers: {({n: len(ls) for n, ls in managers.items()}) or 'none'}; cloud: {cloud}"
                           + (f"; UNVERIFIABLE on {unread}" if unread else "")
                           + (f"; {len(online)} online in the cloud" if len(online) > 1 else "")))
        rows.append(Result("ru_cloud_online", len(online) <= 1 and (len(online) == 0) == (n_managers == 0),
                            f"cloud online: {[u[:8] for u in online]}; manager processes: {n_managers} (reported)"))
        state, why = await asyncio.to_thread(local_state)
        if state is None:
            rows.append(Result("world:ru_state_current", False, f"UNVERIFIABLE: ~/.hpc-bridge not read ({why})"))
        else:
            problems = list(state.get("problems") or [])
            for k, rec in (state.get("endpoints") or {}).items():
                host = rec.get("login_host")
                if dead and host == dead:
                    problems.append(f"endpoints.json {k!r} still pins the dead node {dead}")
                if managers and host and host not in managers and rec.get("name") == name:
                    problems.append(f"endpoints.json {k!r} pins {host}, the manager runs on {sorted(managers)}")
            rows.append(Result("world:ru_state_current", not problems,
                               "ok: " + f"{len(state.get('facilities') or {})} facility record(s), "
                               f"{len(state.get('endpoints') or {})} pin(s), today's format" if not problems
                               else "; ".join(problems)[:600]))
        if cell.remote == "venv_deleted":
            rc, out = await asyncio.to_thread(ctx.ssh, 'for d in "$HOME"/hpc-bridge/gce-venv*; do [ -x '
                                              '"$d/bin/globus-compute-endpoint" ] && echo "HPCB_VENV=$d"; done; '
                                              'echo "HPCB_HOME=$HOME"', 60, None)
            venvs = [ln.split("=", 1)[1] for ln in out.splitlines() if ln.startswith("HPCB_VENV=")]
            home = next((ln.split("=", 1)[1] for ln in out.splitlines() if ln.startswith("HPCB_HOME=")), "?")
            rows.append(Result("world:ru_venv_under_home", bool(venvs) and all(v.startswith(home) for v in venvs),
                               f"venvs: {venvs or 'none'} (HOME={home})"))
        put = await _put_back(cell, ctx)
        rows.append(Result("ru_world_put_back", all("could not" not in p for p in put), "; ".join(put)))
        return rows
    return returning_world_check


# ---- the cell table on the command line ---------------------------------------------------------------------------------

def main(argv: list[str]) -> int:
    if argv[:1] == ["--write-cells"]:
        CELLS_FILE.write_text(render_table(generated_cells()))
        print(f"wrote {CELLS_FILE} ({len(generated_cells())} cells)")
        return 0
    if argv[:1] == ["--list"]:
        for c in load_cells().values():
            print(c.describe())
        return 0
    print("usage: returning.py --write-cells | --list", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

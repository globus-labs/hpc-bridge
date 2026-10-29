#!/usr/bin/env python3
"""NODE ACTIONS: a scripted step takes ONE login node of the fake cluster away mid-run, and gives it back — the world a
returning user meets when the login node their endpoint was pinned to no longer answers (the Perlmutter report,
2026-10-29: `CANNOT REACH login22…`, a pin no connect would drop).

Only the HOST can act on a node (`docker exec` into its container, as root), while the SCRIPT runs in the jail, so the
two halves meet in the runs volume both mount, as the admin tap's do (harness/admin_tap.py):
- HOST — run_smoke.sh starts `node_actions.py serve --req <runs>/.node-actions-<runid> …` in the background for a
  scenario that declares `NODE_ACTIONS = True` (scenario_knobs.py prints it), and stops it on exit. It answers each
  request line the jail appends to the request file — `{"id", "action", "host"}` — with a line in `<req>.done`
  (`{"id", "rc", "out"}`). It runs only the actions named in ACTIONS below, only on the run's own login nodes (the
  profile's `login_hosts`), in the container compose runs under that hostname. It UNDOES what it did when it stops
  (SIGTERM from the EXIT trap, or its parent gone): a node it took away is given back, so no later cell meets it.
- JAIL — a SCRIPT asks with `NodeActions.from_env().run(action, host)` and waits for the answer.

ACTIONS (each a root shell command in the node's container; `{user}` = the cell's pool user):
- `sshd_off` — the node's port-22 sshd stops answering on the network (it listens on 127.0.0.1 only: a drop-in plus a
  SIGHUP, so the daemon — PID 1 of the login container — re-executes in place and the container lives on) and the pool
  user's processes there are killed (its sessions, an ssh ControlMaster's included, and anything it left running): a
  connection to the node is now REFUSED, while the round-robin name still lists it (OpenSSH then tries the next
  address). Undone by `sshd_on`.
- `sshd_on` — the drop-in removed, SIGHUP: the node answers on port 22 again.

Stdlib only: the host half runs on the host's python3; the jail half is imported by scenarios (which scenario_knobs.py
imports on the host).
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

ENV = "HPCB_NODE_ACTIONS"   # the jail-side path of this cell's request file (run_smoke.sh sets it)
_DROPIN = "/etc/ssh/sshd_config.d/zz-hpcb-node-down.conf"
_HUP = 'kill -HUP "$(cat /run/sshd.pid)"'
_LISTEN = "sleep 1; echo \"HPCB_NODE $(hostname) port22=$(ss -Hltn 'sport = :22' | awk '{print $4}' | tr '\\n' ' ')\""
ACTIONS: dict[str, str] = {
    "sshd_off": f"printf 'ListenAddress 127.0.0.1\\n' > {_DROPIN} && {_HUP} && {{ pkill -KILL -u {{user}} || true; }} "
                f"&& {_LISTEN}",
    "sshd_on": f"rm -f {_DROPIN} && {_HUP} && {_LISTEN}",
}
UNDO = {"sshd_off": "sshd_on"}
ACTION_TIMEOUT_S = 60
POLL_S = 0.5


def done_path(req: Path | str) -> Path:
    req = Path(req)
    return req.with_name(req.name + ".done")


def files(req: Path | str) -> list[Path]:
    return [Path(req), done_path(req), Path(str(req) + ".pid")]


def _lines(path: Path) -> list[dict]:
    try:
        text = path.read_text()
    except OSError:
        return []
    out = []
    for line in text.splitlines():
        with contextlib.suppress(ValueError):
            row = json.loads(line)
            if isinstance(row, dict):
                out.append(row)
    return out


# ---- the host half --------------------------------------------------------------------------------------------------

def docker_containers(project: str) -> dict[str, str]:
    """hostname -> container name, for the running compose service containers of `project`."""
    ids = subprocess.run(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={project}",
                          "--filter", "label=com.docker.compose.oneoff=False"],
                         capture_output=True, text=True, timeout=30).stdout.split()
    if not ids:
        return {}
    rows = subprocess.run(["docker", "inspect", "-f", "{{.Config.Hostname}}|{{.Name}}", *ids],
                          capture_output=True, text=True, timeout=30).stdout.splitlines()
    return {h: n.lstrip("/") for h, _, n in (r.partition("|") for r in rows) if h and n}


def docker_exec(container: str, cmd: str) -> tuple[int, str]:
    try:
        r = subprocess.run(["docker", "exec", container, "bash", "-c", cmd], capture_output=True, text=True,
                           timeout=ACTION_TIMEOUT_S, stdin=subprocess.DEVNULL)
        return r.returncode, (r.stdout + r.stderr)[-2000:]
    except subprocess.TimeoutExpired:
        return 124, f"node action timed out after {ACTION_TIMEOUT_S} s"
    except OSError as exc:
        return 127, f"{type(exc).__name__}: {exc}"


class Server:
    """Answers the jail's requests; remembers what to undo."""

    def __init__(self, req: Path, *, user: str, hosts: list[str], containers: Callable[[], dict[str, str]],
                 execute: Callable[[str, str], tuple[int, str]] = docker_exec) -> None:
        self.req, self.user, self.hosts = Path(req), user, list(hosts)
        self._containers, self._execute = containers, execute
        self.answered: set[int] = set()
        self.pending_undo: dict[str, str] = {}   # host -> the action that gives it back

    def act(self, action: str, host: str) -> tuple[int, str]:
        if action not in ACTIONS:
            return 2, f"unknown node action {action!r} (have: {sorted(ACTIONS)})"
        if host not in self.hosts:
            return 2, f"{host!r} is not one of this cluster's login nodes {self.hosts}"
        container = self._containers().get(host)
        if not container:
            return 2, f"no running container answers to the hostname {host!r}"
        rc, out = self._execute(container, ACTIONS[action].replace("{user}", self.user))
        if rc == 0 and action in UNDO:
            self.pending_undo[host] = UNDO[action]
        elif rc == 0:
            self.pending_undo = {h: a for h, a in self.pending_undo.items() if not (h == host and a == action)}
        return rc, out

    def answer_new(self) -> int:
        n = 0
        for row in _lines(self.req):
            rid = row.get("id")
            if not isinstance(rid, int) or rid in self.answered:
                continue
            self.answered.add(rid)
            rc, out = self.act(str(row.get("action")), str(row.get("host")))
            with done_path(self.req).open("a") as fh:
                fh.write(json.dumps({"id": rid, "rc": rc, "out": out}) + "\n")
            print(f"node action #{rid}: {row.get('action')} on {row.get('host')} -> rc={rc} {out.strip()[-160:]}",
                  file=sys.stderr, flush=True)
            n += 1
        return n

    def undo_all(self) -> None:
        for host, action in list(self.pending_undo.items()):
            rc, out = self.act(action, host)
            print(f"node action undo: {action} on {host} -> rc={rc} {out.strip()[-160:]}", file=sys.stderr, flush=True)


class _Stop(Exception):
    pass


def _raise_stop(_signum, _frame) -> None:
    raise _Stop


def serve(req: Path, *, user: str, hosts: list[str], project: str, parent_pid: int | None = None) -> None:
    from admin_tap import alive  # the same parent watch as the tap's (a run_smoke.sh killed outright)

    srv = Server(req, user=user, hosts=hosts, containers=lambda: docker_containers(project))
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _raise_stop)
    with contextlib.suppress(OSError):
        Path(str(req) + ".pid").write_text(f"{os.getpid()}\n")
    try:
        while alive(parent_pid):
            srv.answer_new()
            time.sleep(POLL_S)
    except _Stop:
        pass
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)   # the undo runs to its end
        srv.undo_all()
        for f in files(req):
            with contextlib.suppress(OSError):
                f.unlink()


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Answer a scripted cell's node actions (run_smoke.sh).")
    ap.add_argument("--req", required=True, type=Path, help="the request file, in the runs volume")
    ap.add_argument("--user", required=True, help="the cell's pool user: `{user}` in an action")
    ap.add_argument("--hosts", required=True, help="the profile's login hosts, comma-separated")
    ap.add_argument("--project", default=os.environ.get("HPCB_FAKE_PROJECT", "hpcb-fake"))
    ap.add_argument("--parent-pid", type=int, default=None)
    args = ap.parse_args(argv)
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    serve(args.req, user=args.user, hosts=[h for h in args.hosts.split(",") if h], project=args.project,
          parent_pid=args.parent_pid)
    return 0


# ---- the jail half --------------------------------------------------------------------------------------------------

class NodeActions:
    """What a SCRIPT asks the host with. Without a request file (no NODE_ACTIONS knob, not the fake target) every call
    answers rc 2, saying so: the script treats that as a premise unmet."""

    def __init__(self, req: Path | str | None) -> None:
        self.req = Path(req) if req else None

    @classmethod
    def from_env(cls) -> NodeActions:
        return cls(os.environ.get(ENV, "").strip() or None)

    def run(self, action: str, host: str, *, timeout_s: float = ACTION_TIMEOUT_S + 30,
            sleep: Callable[[float], None] = time.sleep) -> tuple[int, str]:
        if self.req is None:
            return 2, f"no node-action channel (${ENV} unset: the scenario must declare NODE_ACTIONS, on the fake target)"
        rid = len(_lines(self.req)) + 1
        with self.req.open("a") as fh:
            fh.write(json.dumps({"id": rid, "action": action, "host": host}) + "\n")
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            for row in _lines(done_path(self.req)):
                if row.get("id") == rid:
                    return int(row.get("rc", 1)), str(row.get("out", ""))
            sleep(POLL_S)
        return 124, f"the host did not answer node action #{rid} ({action} on {host}) within {timeout_s:.0f} s"


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

"""Pure builders for the harness' cluster-side commands (SETUP/teardown/log capture).

Everything here is a string — no SSH, no SDK — so it is unit-testable and so the ONE rule this
module enforces is auditable: **the harness never touches jobs or endpoints that aren't this
run's.** Teardown used to `scancel -u $(whoami)` and delete every `hpc-bridge-*` endpoint the pool
user owned, on the assumption that concurrent runs use distinct users. That assumption broke
(see pool.py) and one run's teardown killed another's blocks mid-task. Now:

- endpoints are stopped/deleted BY NAME — this run's `HPC_BRIDGE_ENDPOINT_NAME` only;
- pilot blocks are cancelled by the `uep.<eid>` StdOut marker Parsl writes under the UEP dir —
  the same scope the server's `_release_cmd` uses — for this run's endpoint uuid(s) only;
- with NO endpoint uuid known, cancel NOTHING (and say so) rather than fall back to user-wide.
"""
from __future__ import annotations

import re
import shlex

GCE = "$HOME/hpc-bridge/gce-venv/bin/globus-compute-endpoint"


def endpoint_uuid_cmd(endpoint_name: str) -> str:
    """Print this endpoint's registered uuid (from its endpoint.json), or nothing if unregistered."""
    ep = shlex.quote(endpoint_name)
    return (
        f'grep -o \'"endpoint_id": *"[^"]*"\' "$HOME/.globus_compute/"{ep}/endpoint.json 2>/dev/null '
        "| grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1"
    )


def scoped_cancel_cmd(scheduler: str, eids: list[str]) -> str:
    """Cancel ONLY the pilot blocks of `eids` (matched by their `uep.<eid>` marker). Never `-u`.

    A DELIBERATE copy of the product's `server._release_cmd` (same marker semantics), not an import:
    the harness must keep working when the code under test is broken, and it runs on the host while
    the product runs in the jail. `test_pool_and_cluster_ops.py` pins the two to the same marker
    scoping so they cannot drift apart silently (code-quality review 2026-09-03)."""
    if not eids:
        return 'echo "cancel: no endpoint uuid known for this run — cancelling NOTHING (never user-wide)"'
    markers = shlex.quote(" ".join(f"uep.{e}" for e in eids))
    # awk matches ANY of the markers and exits 0 on no-match (a `grep` would exit 1 under pipefail).
    pick = f"awk -v m={markers} 'BEGIN{{n=split(m,a,\" \")}} {{for(i=1;i<=n;i++) if(index($0,a[i])){{print $1; break}}}}'"
    if scheduler == "pbs":
        listing = (
            "qstat -f 2>/dev/null | sed ':a;N;$!ba;s/\\n\\t//g' "
            f"| awk -v m={markers} 'BEGIN{{RS=\"Job Id: \"; n=split(m,a,\" \")}} "
            "{for(i=1;i<=n;i++) if(index($0,a[i])){print $1; break}}'"
        )
        return f'ids=$({listing}); [ -n "$ids" ] && qdel $ids 2>/dev/null; echo "cancelled: ${{ids:-none}}"'
    listing = f'squeue -u "$(whoami)" -h -O "JobID:24,StdOut:1024" 2>/dev/null | {pick}'
    return f'ids=$({listing}); [ -n "$ids" ] && scancel $ids 2>/dev/null; echo "cancelled: ${{ids:-none}}"'


def stop_here_cmd(endpoint_name: str, *, listing: str | None = None, gce: str = GCE) -> str:
    """Stop exactly ONE endpoint by name (this run's) — on THIS login node, and only if its manager runs HERE (an
    anchored match in this node's `ps`, _PROC_AWK). Run it on EVERY login node of the target. Prints one line:
    `HPCB_STOP <host> none` (no manager of it here) or `HPCB_STOP <host> rc=<rc> <output>`. Where `ps` is missing or
    fails, the stop runs anyway. stdin is closed, so nothing waits on a prompt. Why: sent to a round-robin alias, a
    stop could land on the other node, act on the NFS-shared `daemon.pid` there, and leave the manager running."""
    filt = _endpoint_proc_filter(endpoint_name)
    ep = shlex.quote(endpoint_name)
    look = listing or _PS_LISTING
    return ('h=$(hostname); '
            f'if command -v ps >/dev/null 2>&1 && {look} >/dev/null 2>&1; then here=$({look} | {filt}); '
            'else here=unknown; fi; '
            'if [ -z "$here" ]; then echo "HPCB_STOP $h none"; '
            f'else out=$({gce} stop {ep} </dev/null 2>&1); rc=$?; '
            'echo "HPCB_STOP $h rc=$rc $(printf %s "$out" | tail -n 2 | tr \'\\n\' \' \' | cut -c1-200)"; fi')


def delete_registration_cmd(endpoint_name: str, *, gce: str = GCE) -> str:
    """`gce delete <name> --yes` with stdin CLOSED, its outcome printed as `HPCB_DELETE rc=<rc> <output>`, or
    `HPCB_DELETE none` when the endpoint has no directory (nothing was configured). Why: with no token store on the
    node, `delete` asks for an authorization code — with stdin open it waits there; closed, it fails fast and says
    so, and the jail removes the record by uuid."""
    ep = shlex.quote(endpoint_name)
    return (f'if [ ! -d "$HOME/.globus_compute/"{ep} ]; then echo "HPCB_DELETE none"; '
            f'else out=$({gce} delete {ep} --yes </dev/null 2>&1); rc=$?; '
            'echo "HPCB_DELETE rc=$rc $(printf %s "$out" | tail -n 2 | tr \'\\n\' \' \' | cut -c1-200)"; fi')


def delete_endpoint_cmd(endpoint_name: str) -> str:
    """Stop (on this node, only if the manager runs here) + delete exactly ONE endpoint by name (this run's): the
    single-node form, for run_suite's emergency cleanup. run.py's teardown runs the stop on every login node."""
    return f"{stop_here_cmd(endpoint_name)}; {delete_registration_cmd(endpoint_name)}; true"


def token_store_cleanup_cmd() -> str:
    """Remove the pool user's login-node token store (`~/.globus_compute/storage.db`). The PLUGIN seeds it for the pool
    user at bootstrap and removes it only in its own teardown, and only when its record says it seeded it — a cell that
    ends with `stop` leaves it behind, and the NEXT cell on that pool user then finds a store it did not create and
    (correctly) refuses to wipe it: "no token store of ours was removed" failed teardown_reported_clean on the fake
    cluster's internal-hostnames cell (2026-09-06). The pool user has no other Globus login; concurrent cells never
    share a pool user (pool.py), so removing it at the end of every cell is safe and keeps cells independent."""
    return 'if [ -f "$HOME/.globus_compute/storage.db" ]; then rm -f "$HOME/.globus_compute/storage.db"; echo "token store removed"; else echo "no token store"; fi'


def capture_logs_cmd(endpoint_name: str, eids: list[str], *, tail_lines: int = 2000) -> str:
    """Print (with `=== path` headers) the tail of the manager's endpoint.log, each UEP's
    endpoint.log, and the blocks' submit-script stdout/stderr — the evidence a post-mortem needs
    and which `delete` erases. Bounded per file so a chatty log can't bloat the bundle."""
    ep = shlex.quote(endpoint_name)
    globs = [f'"$HOME/.globus_compute/"{ep}/endpoint.log']
    for e in eids:
        globs.append(f'"$HOME/.globus_compute/uep."{shlex.quote(e)}.*/endpoint.log')
        globs.append(f'"$HOME/.globus_compute/uep."{shlex.quote(e)}.*/submit_scripts/*.std*')
    files = " ".join(globs)
    n = int(tail_lines)
    return (
        f"for f in {files}; do [ -f \"$f\" ] || continue; "
        f'echo "=== $f ($(wc -l < "$f") lines; last {n})"; tail -n {n} "$f"; done; true'
    )


def uep_dirs_cleanup_cmd(eids: list[str]) -> str:
    """Remove THIS run's per-UEP directories (`~/.globus_compute/uep.<eid>.<config-hash>/`).

    `globus-compute-endpoint delete <name>` removes only the MANAGER's dir; the UEP dirs it forked
    (one per distinct user_endpoint_config, per run) are left behind and accumulate in the pool
    user's home — dozens were found after a few weeks of runs. Scoped by the run's endpoint uuid(s),
    the same marker discipline as the block cancel; run it AFTER capture_logs_cmd (their endpoint.logs
    are the post-mortem evidence). No uuid known ⇒ nothing is removed."""
    if not eids:
        return 'echo "uep-dirs: no endpoint uuid known for this run — removing nothing"'
    globs = " ".join(f'"$HOME/.globus_compute/uep."{shlex.quote(e)}.*' for e in eids)
    return f'n=0; for d in {globs}; do [ -d "$d" ] && rm -rf "$d" && n=$((n+1)); done; echo "uep-dirs removed: $n"'


_PS_LISTING = 'ps -ww -u "$(whoami)" -o pid=,args='


# Keeps only processes that ARE a Globus Compute endpoint of this run, read off `ps -o pid=,args=` lines. ANCHORED:
#  - a process TITLE the daemon sets — args START with `Globus Compute Endpoint` (`… *(<uuid>, <name>) [state]`); or
#  - an entry-point COMMAND LINE — one of the first three words is `globus-compute-endpoint` (`<python> <gce> start
#    <name>`, `<gce> _start-user-endpoint …`) or a `globus_compute_endpoint…` module (`python -m …`), and the first
#    word is not a shell.
# …that names this run: the endpoint name as a whole token, or `uep.<eid>` (a user endpoint the manager forked is
# named `uep.<manager uuid>.<config hash>`). Why anchored: a shell whose args merely CONTAIN the text — the stop
# guard's own `bash -c "… globus-compute-endpoint stop <name> …"` and its `$(…)` subshells — matched an unanchored
# grep on every node. The awk never matches itself (its first word is `awk`).
_PROC_AWK = r"""
function base(w) { sub(/.*\//, "", w); return w }
function tok(s, t,   i, j, b, a) {
  j = 0
  while ((i = index(substr(s, j + 1), t)) > 0) {
    j += i
    b = (j == 1) ? "" : substr(s, j - 1, 1)
    a = substr(s, j + length(t), 1)
    if (b !~ /[A-Za-z0-9_.-]/ && a !~ /[A-Za-z0-9_.-]/) return 1
  }
  return 0
}
{
  args = $0; sub(/^[ \t]*[0-9]+[ \t]+/, "", args)
  title = (index(args, "Globus Compute Endpoint") == 1)
  entry = 0
  if (base($2) !~ /^-?(ba|da|z|k|c|tc)?sh$/)
    for (k = 2; k <= 4 && k <= NF; k++) {
      w = base($k)
      if (w == "globus-compute-endpoint" || index(w, "globus_compute_endpoint") == 1) entry = 1
    }
  if (!title && !entry) next
  hit = tok(args, n)
  m = split(u, e, " ")
  for (k = 1; k <= m; k++) if (index(args, "uep." e[k]) > 0) hit = 1
  if (hit) print
}
"""


def _endpoint_proc_filter(endpoint_name: str, eids: list[str] | tuple[str, ...] = ()) -> str:
    """The `awk` filter that keeps only processes of THIS endpoint (see _PROC_AWK for the anchored match)."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", endpoint_name):
        raise ValueError(f"unexpected endpoint name {endpoint_name!r}")
    ok_eids = [e for e in eids if re.fullmatch(r"[0-9a-f-]{8,64}", e)]
    return (f"awk -v n={shlex.quote(endpoint_name)} -v u={shlex.quote(' '.join(ok_eids))} "
            f"{shlex.quote(_PROC_AWK.strip())}")


def delete_records(client, eids: list[str]) -> list[str]:
    """Delete endpoint RECORDS from the Globus Compute service by uuid: one outcome line per uuid (`<id8> deleted`,
    `<id8> already gone` on a 404, `<id8> NOT deleted (<Error>)`). Pure apart from `client` — imported both by run.py
    and by the one-line script it runs in the project's environment when the harness' interpreter lacks the SDK."""
    out = []
    for eid in eids:
        try:
            client.delete_endpoint(eid)
            out.append(f"{eid[:8]} deleted")
        except Exception as exc:  # noqa: BLE001 - 404 (already gone) or a transient service error
            msg = str(exc)
            out.append(f"{eid[:8]} already gone" if "404" in msg or "not found" in msg.lower()
                       else f"{eid[:8]} NOT deleted ({type(exc).__name__})")
    return out


def leftover_manager_cmd(endpoint_name: str, eids: list[str] | tuple[str, ...] = (), *, tries: int = 10,
                         pause_s: int = 3, listing: str = _PS_LISTING) -> str:
    """Print every process of the pool user that still runs THIS run's endpoint, after the teardown — empty output
    means none is left. A process counts when it IS an endpoint process — its title starts with `Globus Compute
    Endpoint`, or its command line is the gce entry point — and names this run: the endpoint NAME (unique per run:
    `<prefix>-<runid>`) or `uep.<eid>` for a user endpoint it forked (`eids`: the run's endpoint uuids). The match is
    anchored (_PROC_AWK), so no shell that merely carries the text matches, this check's own included. Retried `tries`
    times `pause_s` apart, so a manager still exiting after `stop` gets its chance. A host whose `ps` is missing or
    fails (BusyBox without `-o`) prints a `HPCB_UNVERIFIABLE` line instead of passing as clean. `listing` is
    injectable for the tests. Why: managers outlived "deleted" teardowns, and nothing looked."""
    filt = _endpoint_proc_filter(endpoint_name, eids)
    return ('command -v ps >/dev/null 2>&1 || { echo "HPCB_UNVERIFIABLE: no ps on $(hostname)"; exit 0; }; '
            f'{listing} >/dev/null 2>&1 || {{ echo "HPCB_UNVERIFIABLE: ps failed on $(hostname)"; exit 0; }}; '
            f'out=""; for i in $(seq 1 {int(tries)}); do out=$({listing} | {filt}); '
            f'[ -z "$out" ] && break; sleep {int(pause_s)}; done; [ -n "$out" ] && printf \'%s\\n\' "$out"; true')

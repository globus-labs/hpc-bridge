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
import subprocess
import sys
from pathlib import Path

# A working endpoint CLI on the pool user's node: the first hpc-bridge venv under ~/hpc-bridge whose entry point runs —
# the keyed ones (`gce-venv-endpoint-<gce>-py<X.Y>`, newest version first, `sort -V`), then the unkeyed `gce-venv` that
# plugins up to 0.1.23 built — skipping builds in progress (`.build.*`) and moved-aside ones (`.broken.*`,
# `.failed.*`). The same candidates and skips as the plugin's teardown (hpc_bridge.facility.endpoint_venv.find_script),
# but not its order: the plugin tries the venv its endpoint was started with first, which the harness does not know.
# Any working CLI can stop/delete an endpoint by name, so the order only picks among working ones. Prints the entry
# point's path (nothing when none runs); `GCE` is that, quoted, as a drop-in for the old fixed path.
GCE_FIND = (
    'for d in $(ls -d "$HOME"/hpc-bridge/gce-venv-endpoint-* 2>/dev/null | sort -rV) "$HOME/hpc-bridge/gce-venv"; do '
    "case $d in (*.build.*|*.broken.*|*.failed.*|*.lock|*.old.*) continue;; esac; "
    '"$d/bin/globus-compute-endpoint" version >/dev/null 2>&1 && { echo "$d/bin/globus-compute-endpoint"; break; }; '
    "done"
)
GCE = f'"$({GCE_FIND})"'


def restore_planted_entry_points_cmd() -> str:
    """Put back what `venv_arch_mismatch` plants, in case its cell was killed before its CLEANUP: the keyed venv's real
    entry point (moved to `bin/.gce-real` behind a wrapper that fails off the login node) and its arch marker. Runs
    before every cell on a pool user; a no-op when nothing is planted."""
    return ('n=0; for r in "$HOME"/hpc-bridge/gce-venv-endpoint-*/bin/.gce-real; do [ -e "$r" ] || continue; '
            'd="${r%/bin/.gce-real}"; mv -f "$r" "$d/bin/globus-compute-endpoint" && uname -m > "$d/.hpcb-arch" && '
            'n=$((n+1)); done; echo "planted entry points restored: $n"')


def plugin_versions(repo_root: Path | None = None) -> tuple[str, str]:
    """(endpoint version, Python X.Y) the plugin under test keys its endpoint venv by: the SDK pin in pyproject.toml (the
    jail's locked install) and the Python of the repo's `.venv` (`uv run` serves the plugin from it), else this
    interpreter's. A scenario that plants something in that venv needs its exact path."""
    root = repo_root or Path(__file__).resolve().parents[2]
    m = re.search(r'"globus-compute-sdk==([\w.]+)"', (root / "pyproject.toml").read_text())
    gce = m.group(1) if m else "unknown"
    py = f"{sys.version_info.major}.{sys.version_info.minor}"
    venv_py = root / ".venv" / "bin" / "python"
    if venv_py.exists():
        try:
            out = subprocess.run([str(venv_py), "-c", "import sys;print('%d.%d' % sys.version_info[:2])"],
                                 capture_output=True, text=True, timeout=30, check=False).stdout.strip()
        except Exception:  # noqa: BLE001 - an unusable .venv: this interpreter's version stands
            out = ""
        py = out if re.fullmatch(r"\d+\.\d+", out) else py
    return gce, py


def keyed_endpoint_venv(gce: str, python: str) -> str:
    """The plugin's keyed endpoint venv on a cluster whose home is `$HOME` (the default `{venv}`), as a remote path."""
    return f"$HOME/hpc-bridge/gce-venv-endpoint-{gce}-py{python}"


def locked_globus_sdk(repo_root: Path | None = None) -> str | None:
    """The globus-sdk version uv.lock pins: the one the plugin co-pins in the endpoint venv it builds."""
    root = repo_root or Path(__file__).resolve().parents[2]
    m = re.search(r'^name = "globus-sdk"\nversion = "([^"]+)"', (root / "uv.lock").read_text(), re.M)
    return m.group(1) if m else None


def build_endpoint_venv_cmd(gce: str, python: str, sdk: str | None = None) -> str:
    """Make sure the keyed venv exists and runs, built as the plugin builds it (relocatable, no package cache, files
    copied, globus-sdk co-pinned; here in place, as a harness shortcut) — for a scenario that then plants a break in
    it. Leaves a working one as it is."""
    k = keyed_endpoint_venv(gce, python)
    reqs = f'"globus-compute-endpoint=={gce}"' + (f' "globus-sdk=={sdk}"' if sdk else "")
    return (f'K="{k}"; "$K/bin/globus-compute-endpoint" version >/dev/null 2>&1 || {{ rm -rf "$K"; '
            f'mkdir -p "$HOME/hpc-bridge" && uv venv -q --relocatable --no-cache --python {python} "$K" && '
            f'uv pip install -q --no-cache --link-mode=copy --python "$K/bin/python" {reqs}; }} && '
            '"$K/bin/globus-compute-endpoint" version')


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


def stop_outcome(out: str) -> tuple[str, str]:
    """Read `stop_here_cmd`'s line from one node's output: (`none` | `rc=<n>` | `missing`, the rest of the line).
    `rc=0` is the only outcome where this node stopped the manager; `missing` means the command never reported (ssh)."""
    line = next((ln[len("HPCB_STOP "):].strip() for ln in out.splitlines() if ln.startswith("HPCB_STOP ")), None)
    if line is None:
        return "missing", out.strip()[:160]
    parts = line.split(" ", 2)   # <host> none | <host> rc=<n> <output>
    status = parts[1] if len(parts) > 1 else "missing"
    return status, (parts[2] if len(parts) > 2 else "")


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
#    <name>`, `<gce> _start-user-endpoint …`; uv's trampoline in a keyed `gce-venv-endpoint-*` venv execs
#    `<venv>/bin/python <venv>/bin/globus-compute-endpoint …`), `.gce-real` (venv_arch_mismatch's real entry point,
#    moved aside behind its wrapper: `<venv>/bin/python <venv>/bin/.gce-real start <name>` until the daemon sets its
#    title) or a `globus_compute_endpoint…` module (`python -m …`), and the first word is not a shell.
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
      if (w == "globus-compute-endpoint" || w == ".gce-real" || index(w, "globus_compute_endpoint") == 1) entry = 1
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


# --- world checks over the run's pilot jobs (Slurm accounting) -----------------------------------------------------
# A scenario stamps the cell's start on the login node in SETUP (`stamp_t0_cmd`), and a POSTCHECK reads back every
# pilot job (`parsl*`) this pool user SUBMITTED since then (`parsl_jobs_since_cmd`) as one line:
#   parsl_jobs=<n>; overlap=<k>; old=<state of the first>; jobs=<id>:<state>,…
# `overlap` counts jobs that STARTED while an earlier one was still running — "never two blocks at once". Fake cluster
# (sacct needs slurmdbd); python3 is on every node. Its Python must hold no single quote (it rides in '…').
_PARSL_JOBS_PY = """
import sys
t0, END = sys.argv[1], "9999-12-31T23:59:59"
rows = []
for line in sys.stdin:
    f = line.strip().split("|")
    if len(f) >= 6 and f[1].startswith("parsl") and f[3] >= t0:
        rows.append(f)
rows.sort(key=lambda f: (f[3], f[0]))
overlap, last_end = 0, ""
for f in sorted((f for f in rows if f[4][:1].isdigit()), key=lambda f: f[4]):
    if last_end and f[4] < last_end:
        overlap += 1
    last_end = max(last_end, f[5] if f[5][:1].isdigit() else END)
state = lambda f: (f[2].split() or ["?"])[0]
print("parsl_jobs=%d; overlap=%d; old=%s; jobs=%s" % (len(rows), overlap, state(rows[0]) if rows else "none",
      ",".join(f[0] + ":" + state(f) for f in rows) or "none"))
"""


def stamp_t0_cmd(t0_file: str) -> str:
    """SETUP: record the cell's start on the login node, in the format sacct prints (`2026-10-13T10:00:00`)."""
    return f"date +%Y-%m-%dT%H:%M:%S > {t0_file}"


def parsl_jobs_since_cmd(t0_file: str) -> str:
    """POSTCHECK: one line describing this pool user's pilot jobs submitted since `stamp_t0_cmd` (see above). Fails
    (rc 1) when the stamp is missing, so a check against it can never pass vacuously."""
    assert "'" not in _PARSL_JOBS_PY
    return (f'T0="$(cat {t0_file} 2>/dev/null)"; [ -n "$T0" ] || {{ echo "no t0 stamp at {t0_file}"; exit 1; }}; '
            'sacct -u "$(whoami)" -X -n -P -S "$T0" -o JobID,JobName,State,Submit,Start,End 2>/dev/null '
            f"| python3 -c '{_PARSL_JOBS_PY}' \"$T0\"")

"""Live HTML dashboard for an agentic profile sweep, read from the sweep's own logs (nothing is written).

    python3 agentic/sweep_dash.py                                   # the newest sweep, at http://127.0.0.1:8791/
    python3 agentic/sweep_dash.py --since 20261021-0945 --baseline 20261016-1337   # several sweeps, vs an earlier one

A sweep run in several invocations (e.g. one profile first, then the rest) has one stamp per invocation: --since
takes every stamp from the one given. Each cell is a square coloured and marked by its result; a dot marks a cell whose
result differs from the --baseline sweep's. Localhost only; stdlib only. agentic/watch.sh is the terminal equivalent.

Sources, re-read on every /status.json request:
  agentic/runs/sweep-<stamp>.md                    plan lines "## <profile> [<op>] — HH:MM — a,b,c" (every stamp >= --since)
  agentic/runs/sweep-<stamp>-<profile>-<op>.log    live "▶ start <scen> ·" and "✓/✗ done <scen> · … — RESULT: …" lines
  the --baseline sweep's summary                   each cell's earlier result, for a cell-by-cell comparison
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PLAN = re.compile(r"^## (\S+) \[([\w-]+)\] — \d\d:\d\d — (.+)$")
HEAD = re.compile(r"^## (\S+) \[([\w-]+)\]")
START = re.compile(r"^\s*(?:\[[\w-]+\] )?▶ start\s+(\S+) ·")
DONE = re.compile(r"^\s*(?:\[([\w-]+)\] )?\S done\s+(\S+) · .*? — RESULT: (.+)$")
ABL = re.compile(r"^\s*(?:\[([\w-]+)\] )?(\S+) — RESULT: (.+)$")
STAMP = re.compile(r"^sweep-(\d{8}-\d{4})\.md$")


def _kind(res: str) -> str:
    r = res.strip().upper()
    for k in ("OK", "FAILED", "INFRA", "SETUP FAILED", "CRASHED", "SKIP"):
        if r.startswith(k):
            return k
    return "OTHER"


def _detail(res: str) -> str:
    m = re.search(r"critical checks broke: \[(.*?)\]", res)
    return m.group(1).replace("'", "") if m else res.split("—", 1)[-1].strip() if "—" in res else ""


def baseline(runs: Path, stamp: str | None) -> dict:
    out: dict = {}
    f = runs / f"sweep-{stamp}.md" if stamp else None
    if not f or not f.exists():
        return out
    prof = None
    for line in f.read_text(errors="replace").splitlines():
        if (h := HEAD.match(line)):
            prof = h.group(1)
        elif prof and (d := DONE.match(line)):
            out[f"{prof}|{d.group(1)}|{d.group(2)}"] = _kind(d.group(3))
    return out


def status(repo: Path, since: str, base: dict, profiles: list[str], until: str = "~") -> dict:
    runs = repo / "agentic" / "runs"
    stamps = sorted(m.group(1) for p in runs.glob("sweep-*.md")
                    if (m := STAMP.match(p.name)) and since <= m.group(1) <= until)
    groups: dict = {}   # (profile, op) -> {"stamp", "cells": {scen: {...}}}
    ops: list[str] = []
    ablations: dict = {}   # (profile, op) -> {scen: result}: the no-skill ablation lands in the summary only
    for st in stamps:
        cur = None
        for line in (runs / f"sweep-{st}.md").read_text(errors="replace").splitlines():
            if (h := HEAD.match(line)):
                cur = (h.group(1), h.group(2))
            if cur and (m := ABL.match(line)) and " done " not in line and "·" not in line:
                ablations.setdefault(cur, {})[m.group(2) + " (ablation)"] = m.group(3)
            if (m := PLAN.match(line)):
                prof, op = m.group(1), m.group(2)
                ops += [op] if op not in ops else []
                g = groups.setdefault((prof, op), {"stamp": st, "cells": {}})
                g["stamp"] = st
                for s in m.group(3).split(","):
                    g["cells"].setdefault(s.strip(), {"state": "PENDING"})
    for (prof, op), g in groups.items():
        log = runs / f"sweep-{g['stamp']}-{prof}-{op}.log"
        if not log.exists():
            continue
        for line in log.read_text(errors="replace").splitlines():
            if (m := START.match(line)):
                g["cells"].setdefault(m.group(1), {})["state"] = "RUNNING"
            elif (m := DONE.match(line)):
                g["cells"].setdefault(m.group(2), {}).update(state=_kind(m.group(3)), detail=_detail(m.group(3)))
        for scen, res in ablations.get((prof, op), {}).items():
            g["cells"][scen] = {"state": _kind(res), "detail": _detail(res)}
    rows = []
    for prof in profiles + sorted({p for p, _ in groups} - set(profiles)):
        cols = {}
        for op in ops:
            g = groups.get((prof, op))
            if g:
                cols[op] = [{"scen": s, **c, "base": base.get(f"{prof}|{op}|{s}")}
                            for s, c in g["cells"].items()]
        rows.append({"profile": prof, "ops": cols})
    try:
        jail = subprocess.run(["docker", "ps", "-q", "--filter", "ancestor=hpc-bridge-agentic"], capture_output=True,
                              text=True, timeout=5).stdout.split()
    except (OSError, subprocess.SubprocessError):   # no docker, or it hung: the count is a nicety
        jail = []
    # the first sweep's own stamp (local time), never a summary's mtime: that moves with every line appended
    first = time.mktime(time.strptime(stamps[0], "%Y%m%d-%H%M")) if stamps else time.time()
    return {"stamps": stamps, "ops": ops, "rows": rows, "jail": len(jail), "started": first, "now": time.time(),
            "baseline": bool(base)}


def default_profiles(repo: Path) -> str:
    """The profile list sweep_profiles.sh sweeps by default, so the grid and the sweep never drift apart."""
    try:
        m = re.search(r'PROFILES="([^"]+)"', (repo / "agentic" / "sweep_profiles.sh").read_text())
    except OSError:
        m = None
    return m.group(1) if m else "default"


PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sweep Progress</title><style>
:root{--bg:#f6f8fa;--card:#fff;--ink:#1f2328;--ink2:#59636e;--mute:#818b98;--line:#d1d9e0;--pend:#eaeef2;
--ok:#1a7f37;--fail:#cf222e;--infra:#9a6700;--run:#0969da;--other:#8250df}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#0d1117;--card:#151b23;--ink:#f0f6fc;--ink2:#9198a1;
--mute:#656c76;--line:#3d444d;--pend:#262c36;--ok:#3fb950;--fail:#f85149;--infra:#d29922;--run:#4493f8;--other:#ab7df8}}
:root[data-theme="dark"]{--bg:#0d1117;--card:#151b23;--ink:#f0f6fc;--ink2:#9198a1;--mute:#656c76;--line:#3d444d;--pend:#262c36;
--ok:#3fb950;--fail:#f85149;--infra:#d29922;--run:#4493f8;--other:#ab7df8}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:1200px;margin:0 auto;padding:20px 16px 48px}h1{font-size:20px;margin:0 0 2px}.sub{color:var(--ink2);margin:0 0 16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px;margin-bottom:14px}
.bar{display:flex;height:10px;border-radius:5px;overflow:hidden;background:var(--pend);gap:2px}.bar i{display:block;height:100%}
.legend{display:flex;flex-wrap:wrap;gap:14px;margin-top:10px;color:var(--ink2);font-size:13px}.legend b{color:var(--ink);font-weight:600}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-bottom:14px}
.tile h2{font-size:15px;margin:0 0 8px}.tile .nums{color:var(--ink2);font-size:13px;margin-top:8px}.tile .base{color:var(--mute);font-size:12px;margin-top:2px}
table{width:100%;border-collapse:collapse}th,td{text-align:left;vertical-align:top;padding:8px 6px;border-top:1px solid var(--line)}
th{color:var(--ink2);font-weight:600;font-size:13px;border-top:0}td.p{font-weight:600;white-space:nowrap;width:1%}
.cells{display:flex;flex-wrap:wrap;gap:3px}.c{position:relative;width:20px;height:20px;border-radius:4px;display:grid;place-items:center;
font-size:12px;font-weight:700;color:#fff;cursor:default}.c.PENDING{background:var(--pend);color:var(--mute)}.c.RUNNING{background:var(--run);animation:p 1.4s infinite}
.c.OK{background:var(--ok)}.c.FAILED{background:var(--fail)}.c.INFRA{background:var(--infra)}.c.OTHER,.c.CRASHED,.c.SKIP,.c[class*="SETUP"]{background:var(--other)}
.c.chg::after{content:"";position:absolute;top:-3px;right:-3px;width:7px;height:7px;border-radius:50%;background:var(--ink);border:2px solid var(--card)}
@keyframes p{50%{opacity:.55}}.ns{color:var(--mute);font-size:13px}
#tip{position:fixed;pointer-events:none;background:var(--card);color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:8px 10px;
font-size:13px;max-width:340px;box-shadow:0 4px 16px rgba(0,0,0,.18);display:none;z-index:9}#tip .k{color:var(--ink2)}
ul.f{margin:6px 0 0;padding-left:18px}ul.f li{margin:3px 0}ul.f code{font-size:12px;color:var(--ink2)}
@media (max-width:640px){th.opn{display:none}td.p{display:block;width:auto;border-top:1px solid var(--line)}tr td.o{display:block;border-top:0;padding-top:2px}
td.o::before{content:attr(data-op);display:block;color:var(--ink2);font-size:12px;margin-bottom:2px}}
</style></head><body><main>
<h1>Sweep progress</h1><p class="sub" id="sub">loading…</p>
<div class="card"><div class="bar" id="bar"></div><div class="legend" id="leg"></div></div>
<div class="tiles" id="tiles"></div>
<div class="card"><table><thead><tr id="hd"></tr></thead><tbody id="grid"></tbody></table>
<div class="legend"><span>● dot: result differs from the baseline sweep</span><span>hover a square for details</span></div></div>
<div class="card"><b>Failures</b><ul class="f" id="fails"></ul></div>
<div id="tip"></div></main><script>
const K={OK:["✓","OK"],FAILED:["✗","Failed"],INFRA:["!","Infra"],RUNNING:["▸","Running"],PENDING:["·","Pending"],OTHER:["?","Other"]};
const k=s=>K[s]?s:(s==="PENDING"||s==="RUNNING"?s:"OTHER"), esc=t=>String(t??"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
function count(cells){const n={OK:0,FAILED:0,INFRA:0,RUNNING:0,PENDING:0,OTHER:0};cells.forEach(c=>n[k(c.state)]++);return n}
function bar(n,total){return["OK","FAILED","INFRA","OTHER","RUNNING"].map(s=>n[s]?`<i style="width:${100*n[s]/total}%;background:var(--${{OK:"ok",FAILED:"fail",INFRA:"infra",OTHER:"other",RUNNING:"run"}[s]})"></i>`:"").join("")}
function dur(s){s=Math.max(0,Math.round(s));const h=Math.floor(s/3600),m=Math.floor(s%3600/60);return h?`${h} h ${m} min`:`${m} min`}
const tip=document.getElementById("tip");
document.addEventListener("mousemove",e=>{const t=e.target.closest(".c");if(!t){tip.style.display="none";return}
tip.innerHTML=t.dataset.tip;tip.style.display="block";const x=Math.min(e.clientX+14,innerWidth-tip.offsetWidth-8),y=Math.min(e.clientY+14,innerHeight-tip.offsetHeight-8);
tip.style.left=x+"px";tip.style.top=y+"px"});
async function tick(){let d;try{d=await (await fetch("status.json",{cache:"no-store"})).json()}catch(e){document.getElementById("sub").textContent="dashboard server unreachable";return}
const all=d.rows.flatMap(r=>Object.values(r.ops).flat()),n=count(all),done=n.OK+n.FAILED+n.INFRA+n.OTHER,total=all.length||1;
document.getElementById("sub").textContent=`sweeps ${d.stamps.join(", ")||"none yet"} · ${done}/${all.length} cells done · ${n.RUNNING} running · ${d.jail} jail container(s) · elapsed ${dur(d.now-d.started)} · updated ${new Date(d.now*1000).toLocaleTimeString()}`;
document.getElementById("bar").innerHTML=bar(n,total);
document.getElementById("leg").innerHTML=["OK","FAILED","INFRA","RUNNING","PENDING"].map(s=>`<span>${K[s][0]} ${K[s][1]} <b>${n[s]}</b></span>`).join("")+(n.OTHER?`<span>? Other <b>${n.OTHER}</b></span>`:"");
document.getElementById("tiles").innerHTML=d.ops.map(op=>{const cs=d.rows.flatMap(r=>r.ops[op]||[]),m=count(cs),b=count(cs.filter(c=>c.base).map(c=>({state:c.base})));
const fin=m.OK+m.FAILED+m.INFRA+m.OTHER;return`<div class="card tile"><h2>${esc(op)}</h2><div class="bar">${bar(m,cs.length||1)}</div>
<div class="nums">✓ ${m.OK} · ✗ ${m.FAILED} · ! ${m.INFRA} · ${fin}/${cs.length} done${fin?` · ${Math.round(100*m.OK/fin)}% OK`:""}</div>
${d.baseline?`<div class="base">baseline, same cells: ✓ ${b.OK} · ✗ ${b.FAILED} · ! ${b.INFRA}</div>`:""}</div>`}).join("");
document.getElementById("hd").innerHTML=`<th>Profile</th>`+d.ops.map(o=>`<th class="opn">${esc(o)}</th>`).join("");
document.getElementById("grid").innerHTML=d.rows.map(r=>`<tr><td class="p">${esc(r.profile)}</td>`+d.ops.map(op=>{const cs=r.ops[op];
if(!cs)return`<td class="o" data-op="${esc(op)}"><span class="ns">not started</span></td>`;
return`<td class="o" data-op="${esc(op)}"><div class="cells">`+cs.map(c=>{const s=k(c.state),chg=c.base&&(s==="OK"||s==="FAILED"||s==="INFRA")&&c.base!==s;
const t=`<b>${esc(c.scen)}</b><br><span class="k">${esc(r.profile)} · ${esc(op)}</span><br>${K[s][1]}${c.detail?`: ${esc(c.detail)}`:""}${c.base?`<br><span class="k">baseline: ${esc(c.base)}</span>`:""}`;
return`<span class="c ${s}${chg?" chg":""}" role="img" aria-label="${esc(c.scen)}: ${K[s][1]}" data-tip="${esc(t)}">${K[s][0]}</span>`}).join("")+`</div></td>`}).join("")+`</tr>`).join("");
const f=d.rows.flatMap(r=>Object.entries(r.ops).flatMap(([op,cs])=>cs.filter(c=>c.state==="FAILED"||c.state==="INFRA").map(c=>({...c,op,p:r.profile}))));
document.getElementById("fails").innerHTML=f.length?f.map(c=>`<li>${c.state==="FAILED"?"✗":"!"} <b>${esc(c.p)}</b> · ${esc(c.op)} · ${esc(c.scen)}${c.base?` <code>(baseline ${esc(c.base)})</code>`:""}${c.detail?`<br><code>${esc(c.detail)}</code>`:""}</li>`).join(""):"<li>none yet</li>"}
tick();setInterval(tick,5000);
</script></body></html>"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=str(Path(__file__).resolve().parent.parent))
    ap.add_argument("--since", help="first sweep stamp to include, e.g. 20261021-0945 (default: the newest sweep)")
    ap.add_argument("--baseline", help="an earlier sweep stamp to compare cell by cell")
    ap.add_argument("--profiles", help="the grid's rows (default: sweep_profiles.sh's own profile list)")
    ap.add_argument("--port", type=int, default=8791)
    a = ap.parse_args()
    repo = Path(a.repo).expanduser()
    profiles = [p for p in (a.profiles or default_profiles(repo)).split(",") if p]
    if not a.since:
        found = sorted(m.group(1) for p in (repo / "agentic" / "runs").glob("sweep-*.md") if (m := STAMP.match(p.name)))
        if not found:
            raise SystemExit("sweep_dash: no agentic/runs/sweep-<stamp>.md yet; pass --since")
        a.since = found[-1]
    base = baseline(repo / "agentic" / "runs", a.baseline)

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path.startswith("/status.json"):
                body, ctype = json.dumps(status(repo, a.since, base, profiles)).encode(), "application/json"
            elif self.path in ("/", "/index.html"):
                body, ctype = PAGE.encode(), "text/html; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_a):
            pass

    print(f"sweep dashboard: http://127.0.0.1:{a.port}/", flush=True)
    ThreadingHTTPServer(("127.0.0.1", a.port), H).serve_forever()


if __name__ == "__main__":
    main()

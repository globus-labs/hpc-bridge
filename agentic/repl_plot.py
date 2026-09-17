#!/usr/bin/env python3
"""The REPL benchmark graph: hpc-bridge against the agent's local shell, from stored run bundles.

Writes one self-contained HTML page (inline SVG, no network, light + dark) with three views and a table twin:

1. **Where a REPL turn's time goes** — the mean warm turn per channel, split into the agent's own time (choosing
   the next step) and the tool's time (running it). Means, because they add: mean turn = mean agent + mean tool.
2. **Tool latency per step** — every warm protocol step from every run, one dot each, with the median marked. This
   is where a bimodal channel shows itself.
3. **REPL properties** — how many runs passed each property, per channel.

Plus "Every run", the table view of the numbers above. Timing uses only runs whose protocol completed cleanly
(`repl_protocol_complete`); the property counts use every run. Definition: vault `Planned/REPL-like interaction
benchmark.md`.

    python agentic/repl_plot.py agentic/runs/repl-sweep-<id>          # a sweep's manifest -> <sweep>/repl-sweep.html
    python agentic/repl_plot.py agentic/runs --out /tmp/repl.html     # every REPL bundle under a runs dir
"""
from __future__ import annotations

import argparse
import html
import json
import math
import random
import statistics
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "harness"))

import repl_protocol as rp  # noqa: E402
from trace_adapter import trace_from_bundle  # noqa: E402

SCENARIOS = {"repl_baseline_local": "local", "repl_interaction": "bridge"}
TARGET_LABEL = {"fake": "fake cluster", "globus1": "Globus Cluster"}


def label(series: str) -> str:
    """A graph row's name: `local` → the local Bash tool; `bridge@<target>` → hpc-bridge on that cluster."""
    if series == "local":
        return "local Bash tool"
    target = series.split("@", 1)[1]
    return f"hpc-bridge · {TARGET_LABEL.get(target, target)}"


def series_order(runs: list[Run]) -> list[str]:
    """The local reference first, then hpc-bridge per target: the fake cluster, then real clusters by name."""
    present = {r.series for r in runs}
    bridge = sorted((x for x in present if x != "local"), key=lambda x: (x != "bridge@fake", x))
    return (["local"] if "local" in present else []) + bridge
PROPS = [("repl_protocol_complete", "all ten steps, one call each"),
         ("repl_direct_results", "P1 · result comes back directly"),
         ("repl_state_carries", "P3 · files and working directory carry"),
         ("repl_env_carries", "P3 · environment variables carry"),
         ("repl_fail_soft", "P4 · an error leaves the session intact"),
         ("repl_no_ceremony", "P5 · no per-turn ceremony")]


@dataclass
class Run:
    bundle: str
    channel: str
    result: str
    model: str
    target: str
    written_at: str
    props: dict[str, bool]
    tool: list[float] = field(default_factory=list)          # every warm step's tool latency
    turn_tool: list[float] = field(default_factory=list)     # paired with turn_agent: one warm turn each
    turn_agent: list[float] = field(default_factory=list)

    @property
    def series(self) -> str:
        return "local" if self.channel == "local" else f"bridge@{self.target}"

    @property
    def timed(self) -> bool:
        return self.props.get("repl_protocol_complete", False) and bool(self.turn_tool)


def collect(bundle_dirs: list[Path]) -> list[Run]:
    runs = []
    for d in bundle_dirs:
        rec_path = d / "record.json"
        if not rec_path.exists() or not (d / "messages.jsonl").exists():
            continue
        rec = json.loads(rec_path.read_text())
        cfg = rec.get("config", {})
        channel = SCENARIOS.get(str(cfg.get("scenario")))
        if channel is None:
            continue
        t = trace_from_bundle(d)
        graded = {r.name: r.ok for r in (g(t) for g in rp.graders(channel))}
        run = Run(bundle=d.name, channel=channel, result=str(rec.get("result") or "?"), model=str(cfg.get("model") or "?"),
                  target=str(cfg.get("target") or "?"), written_at=str(rec.get("written_at") or ""),
                  props={n: graded.get(n, False) for n, _ in PROPS})
        steps = rp._steps(t, channel)
        run.tool = [s.call.latency_s for s in steps[1:] if s.call.latency_s is not None]
        for a, b in pairwise(steps[1:]):
            if None not in (a.call.t_call, a.call.t_result, b.call.t_call):
                run.turn_tool.append(a.call.t_result - a.call.t_call)
                run.turn_agent.append(b.call.t_call - a.call.t_result)
        runs.append(run)
    return runs


def bundles_from(path: Path) -> tuple[list[Path], dict]:
    """A sweep dir (manifest.json) -> its recorded bundles; otherwise every sub-directory of a runs dir."""
    manifest = path / "manifest.json"
    if manifest.exists():
        m = json.loads(manifest.read_text())
        root = Path(m.get("repo_root") or HERE.parent)
        return [(root / c["bundle"]) for c in m.get("cells", []) if c.get("bundle")], m
    return sorted(p for p in path.iterdir() if p.is_dir()), {}


# --- formatting -------------------------------------------------------------------------------------------------------

def _s(x: float) -> str:
    return f"{x:.2f} s" if x < 10 else f"{x:.1f} s"


def _nice_ticks(hi: float, n: int = 5) -> list[float]:
    if hi <= 0:
        return [0.0, 1.0]
    raw = hi / n
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    return [round(k * step, 6) for k in range(math.ceil(hi / step) + 1)]


def _tick_label(v: float) -> str:
    return f"{v:g} s"


def _esc(s: object) -> str:
    return html.escape(str(s), quote=True)


# --- the three views --------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Geo:
    """One chart geometry. PAGE draws for the HTML report (720 wide, scaled by the browser); SLIDE draws at the deck's
    real content width (1136 px at 1:1) with projector-sized text, so a slide never shrinks a page-sized chart."""
    W: int = 720
    LEFT: int = 184           # row labels sit right-aligned before this
    RIGHT: int = 64           # room for the value at a bar's end
    turn_row: int = 56
    bar_h: int = 22
    strip_row: int = 64
    jitter: float = 14
    dot_r: float = 4
    tick_dy: int = 18         # tick labels below the axis
    label_dy: float = 5       # baseline offset for text centred on a bar
    med_gap: float = 16       # row label → its median label, in the strip


PAGE = Geo()
SLIDE = Geo(W=1136, LEFT=262, RIGHT=86, turn_row=44, bar_h=26, strip_row=52, jitter=12, dot_r=5, tick_dy=22,
            label_dy=6, med_gap=20)


def _x(v: float, hi: float, g: Geo = PAGE) -> float:
    return g.LEFT + (g.W - g.LEFT - g.RIGHT) * (v / hi if hi else 0)


def _axis(hi: float, ticks: list[float], top: float, bottom: float, g: Geo = PAGE) -> str:
    out = []
    for v in ticks:
        x = _x(v, hi, g)
        out.append(f'<line class="grid" x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{bottom}"/>')
        out.append(f'<text class="tick" x="{x:.1f}" y="{bottom + g.tick_dy}" text-anchor="middle">{_tick_label(v)}</text>')
    out.append(f'<line class="baseline" x1="{g.LEFT}" y1="{top}" x2="{g.LEFT}" y2="{bottom}"/>')
    return "".join(out)


def _bar_segment(x0: float, x1: float, y: float, h: float, cls: str, tip: str, round_end: bool) -> str:
    """A horizontal segment: square at the start, 4px rounded data-end when it is the bar's last segment."""
    w = max(0.0, x1 - x0)
    if w <= 0:
        return ""
    r = min(4.0, w, h / 2) if round_end else 0.0
    d = (f"M{x0:.1f},{y:.1f} H{x1 - r:.1f} Q{x1:.1f},{y:.1f} {x1:.1f},{y + r:.1f} V{y + h - r:.1f} "
         f"Q{x1:.1f},{y + h:.1f} {x1 - r:.1f},{y + h:.1f} H{x0:.1f} Z")
    return f'<path class="{cls} mark" d="{d}" data-tip="{_esc(tip)}" tabindex="0"/>'


def turn_view(runs: list[Run], axis_max: float | None = None, g: Geo = PAGE, pad: float = 1.08) -> tuple[str, dict]:
    row, bar_h = g.turn_row, g.bar_h
    order = series_order(runs)
    timed = {ch: [r for r in runs if r.series == ch and r.timed] for ch in order}
    stats = {}
    for ch, rs in timed.items():
        agent = [x for r in rs for x in r.turn_agent]
        tool = [x for r in rs for x in r.turn_tool]
        if agent and tool:
            stats[ch] = {"agent": statistics.fmean(agent), "tool": statistics.fmean(tool),
                         "turns": len(agent), "runs": len(rs)}
    if not stats:
        return '<p class="empty">No timed runs yet.</p>', stats
    hi = max(v["agent"] + v["tool"] for v in stats.values())
    ticks = _nice_ticks(max(hi * pad, axis_max or 0))
    hi = ticks[-1]
    top, height = 16, 16 + row * len(order)
    parts = [_axis(hi, ticks, top - 6, height, g)]
    for k, ch in enumerate(order):
        y = top + k * row + (row - bar_h) / 2 - 6
        label_y = y + bar_h / 2 + g.label_dy
        parts.append(f'<text class="rowlabel" x="{g.LEFT - 12}" y="{label_y:.1f}" text-anchor="end">{_esc(label(ch))}</text>')
        v = stats.get(ch)
        if v is None:
            parts.append(f'<text class="note" x="{g.LEFT + 8}" y="{label_y:.1f}">no timed runs</text>')
            continue
        x0, xa = g.LEFT, _x(v["agent"], hi, g)
        xt = _x(v["agent"] + v["tool"], hi, g)
        gap = 2.0 if xt - xa > 3 else 0.0
        n = f'{v["turns"]} turns across {v["runs"]} run{"s" if v["runs"] != 1 else ""}'
        parts.append(_bar_segment(x0, xa, y, bar_h, "agent", f'{_s(v["agent"])} agent time · {label(ch)} · mean of {n}',
                                  round_end=gap == 0))
        parts.append(_bar_segment(xa + gap, xt, y, bar_h, "tool", f'{_s(v["tool"])} tool time · {label(ch)} · mean of {n}',
                                  round_end=True))
        parts.append(f'<text class="value" x="{xt + 10:.1f}" y="{label_y:.1f}">{_esc(_s(v["agent"] + v["tool"]))}</text>')
        if xa - x0 > 72:   # label inside the agent segment only when it fits with padding
            parts.append(f'<text class="inlabel" x="{x0 + 10:.1f}" y="{label_y:.1f}">{_esc(_s(v["agent"]))}</text>')
    svg = (f'<svg viewBox="0 0 {g.W} {height + g.tick_dy + 10}" role="img" aria-label="Mean REPL turn split into agent time and tool time">'
           + "".join(parts) + "</svg>")
    legend = ('<div class="legend"><span><i class="sw agent"></i>agent time — choosing the next step</span>'
              '<span><i class="sw tool"></i>tool time — running it</span></div>')
    return legend + svg, stats


def strip_view(runs: list[Run], seed: int = 7, axis_max: float | None = None, g: Geo = PAGE,
               pad: float = 1.05) -> tuple[str, dict]:
    row, jitter = g.strip_row, g.jitter
    order = series_order(runs)
    pts = {ch: [(r.bundle, x) for r in runs if r.series == ch and r.timed for x in r.tool] for ch in order}
    allx = [x for v in pts.values() for _, x in v]
    if not allx:
        return '<p class="empty">No timed runs yet.</p>', {}
    ticks = _nice_ticks(max(max(allx) * pad, axis_max or 0))
    hi = ticks[-1]
    top, height = 16, 16 + row * len(order)
    rnd = random.Random(seed)   # fixed jitter: the page renders the same every time
    parts = [_axis(hi, ticks, top - 6, height, g)]
    medians = {}
    for k, ch in enumerate(order):
        cy = top + k * row + row / 2 - 6
        label_top = cy - 2 - (g.med_gap - 16) / 2
        parts.append(f'<text class="rowlabel" x="{g.LEFT - 12}" y="{label_top:.1f}" text-anchor="end">{_esc(label(ch))}</text>')
        if not pts[ch]:
            parts.append(f'<text class="note" x="{g.LEFT + 8}" y="{cy + 5:.1f}">no timed runs</text>')
            continue
        for bundle, x in pts[ch]:
            px, py = _x(x, hi, g), cy + rnd.uniform(-jitter, jitter)
            tip = f"{_s(x)} tool latency · {label(ch)} · {bundle}"
            parts.append(f'<g class="mark" data-tip="{_esc(tip)}" tabindex="0"><circle class="hit" cx="{px:.1f}" cy="{py:.1f}" r="12"/>'
                         f'<circle class="dot" cx="{px:.1f}" cy="{py:.1f}" r="{g.dot_r:g}"/></g>')
        med = statistics.median(x for _, x in pts[ch])
        medians[ch] = med
        mx = _x(med, hi, g)
        parts.append(f'<line class="median" x1="{mx:.1f}" y1="{cy - jitter - 6:.1f}" x2="{mx:.1f}" y2="{cy + jitter + 6:.1f}"/>')
        # the median's value rides under the row label: beside the line it would collide with the row above
        parts.append(f'<text class="medlabel" x="{g.LEFT - 12}" y="{label_top + g.med_gap:.1f}" text-anchor="end">median {_esc(_s(med))}</text>')
    svg = (f'<svg viewBox="0 0 {g.W} {height + g.tick_dy + 10}" role="img" aria-label="Tool latency of every warm step, per channel">'
           + "".join(parts) + "</svg>")
    return svg, medians


def props_view(runs: list[Run]) -> str:
    order = series_order(runs)
    counts = {ch: [r for r in runs if r.series == ch] for ch in order}
    head = "".join(f"<th>{_esc(label(ch))}</th>" for ch in order)
    rows = []
    for name, prop_label in PROPS:
        cells = []
        for ch in order:
            rs = counts[ch]
            if not rs:
                cells.append("<td>—</td>")
                continue
            k = sum(r.props[name] for r in rs)
            mark = "✓" if k == len(rs) else ("✗" if k == 0 else "◐")
            cells.append(f'<td class="num"><span class="mk">{mark}</span> {k} of {len(rs)}</td>')
        rows.append(f"<tr><th scope=\"row\">{_esc(prop_label)}</th>{''.join(cells)}</tr>")
    return f'<table><thead><tr><th>property</th>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table>'


def runs_table(runs: list[Run]) -> str:
    rows = []
    for r in sorted(runs, key=lambda r: (r.written_at, r.bundle)):
        med = statistics.median(r.tool) if r.tool else None
        turn = (statistics.fmean([a + b for a, b in zip(r.turn_agent, r.turn_tool, strict=True)])
                if r.turn_agent else None)
        agent = statistics.fmean(r.turn_agent) if r.turn_agent else None
        passed = sum(r.props.values())
        rows.append("<tr>" + "".join(f"<td{c}>{v}</td>" for c, v in [
            ("", _esc(r.bundle)), ("", _esc(label(r.series))), ("", _esc(r.result)),
            (' class="num"', f"{passed} of {len(PROPS)}"),
            (' class="num"', _esc(_s(med)) if med is not None else "—"),
            (' class="num"', _esc(_s(agent)) if agent is not None else "—"),
            (' class="num"', _esc(_s(turn)) if turn is not None else "—"),
            ("", "yes" if r.timed else "no")]) + "</tr>")
    return ('<table><thead><tr><th>run</th><th>channel</th><th>result</th><th>properties</th><th>tool median</th>'
            '<th>agent mean</th><th>turn mean</th><th>in timing</th></tr></thead><tbody>' + "".join(rows) + "</tbody></table>")


# --- the page ---------------------------------------------------------------------------------------------------------

_CSS = """
.viz-root{color-scheme:light;--surface-1:#fcfcfb;--page:#f9f9f7;--text-primary:#0b0b0b;--text-secondary:#52514e;
--text-muted:#898781;--grid:#e1e0d9;--axis:#c3c2b7;--tool:#2a78d6;--agent:#c3c2b7;--border:rgba(11,11,11,.10)}
@media (prefers-color-scheme:dark){:root:where(:not([data-theme="light"])) .viz-root{color-scheme:dark;--surface-1:#1a1a19;
--page:#0d0d0d;--text-primary:#fff;--text-secondary:#c3c2b7;--text-muted:#898781;--grid:#2c2c2a;--axis:#383835;--tool:#3987e5;
--agent:#52514e;--border:rgba(255,255,255,.10)}}
:root[data-theme="dark"] .viz-root{color-scheme:dark;--surface-1:#1a1a19;--page:#0d0d0d;--text-primary:#fff;
--text-secondary:#c3c2b7;--text-muted:#898781;--grid:#2c2c2a;--axis:#383835;--tool:#3987e5;--agent:#52514e;
--border:rgba(255,255,255,.10)}
body{margin:0;background:var(--page,#f9f9f7)}
.viz-root{background:var(--page);color:var(--text-primary);font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif;
padding:24px 16px;min-height:100vh;box-sizing:border-box}
.wrap{max-width:780px;margin:0 auto}
h1{font-size:22px;margin:0 0 4px}
.sub{color:var(--text-secondary);margin:0 0 20px}
.card{background:var(--surface-1);border:1px solid var(--border);border-radius:12px;padding:18px 20px;margin:0 0 16px}
h2{font-size:15px;margin:0 0 2px}
.card .sub{margin:0 0 12px;font-size:13px}
svg{width:100%;height:auto;display:block;overflow:visible}
.grid{stroke:var(--grid);stroke-width:1}
.baseline{stroke:var(--axis);stroke-width:1}
.tick{fill:var(--text-muted);font-size:11px;font-variant-numeric:tabular-nums}
.rowlabel{fill:var(--text-secondary);font-size:13px}
.value{fill:var(--text-primary);font-size:12px;font-weight:600}
.inlabel{fill:var(--text-primary);font-size:12px}
.note{fill:var(--text-muted);font-size:12px}
.medlabel{fill:var(--text-primary);font-size:12px;font-weight:600}
.agent{fill:var(--agent)}
.tool{fill:var(--tool)}
.dot{fill:var(--tool);stroke:var(--surface-1);stroke-width:2}
.hit{fill:transparent}
.median{stroke:var(--text-primary);stroke-width:2;stroke-linecap:round}
.mark{cursor:default;outline:none}
.mark:hover,.mark:focus{opacity:.8}
.legend{display:flex;flex-wrap:wrap;gap:6px 18px;color:var(--text-secondary);font-size:13px;margin:0 0 8px}
.sw{display:inline-block;width:12px;height:12px;border-radius:3px;margin-right:6px;vertical-align:-1px}
.sw.agent{background:var(--agent)}.sw.tool{background:var(--tool)}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--grid);vertical-align:top}
thead th{color:var(--text-secondary);font-weight:600}
tbody th{font-weight:400}
td.num{font-variant-numeric:tabular-nums;white-space:nowrap}
.scroll{overflow-x:auto}
.empty{color:var(--text-muted)}
details summary{cursor:pointer;color:var(--text-secondary)}
#tip{position:fixed;pointer-events:none;background:var(--surface-1);color:var(--text-primary);border:1px solid var(--border);
border-radius:8px;padding:6px 9px;font-size:12px;box-shadow:0 4px 16px rgba(0,0,0,.12);max-width:320px}
"""

_JS = """
(function(){var tip=document.getElementById('tip');
function show(el,x,y){tip.textContent=el.getAttribute('data-tip');tip.hidden=false;
var w=tip.offsetWidth,h=tip.offsetHeight;tip.style.left=Math.min(x+14,innerWidth-w-8)+'px';tip.style.top=Math.max(8,y-h-10)+'px';}
document.querySelectorAll('[data-tip]').forEach(function(el){
el.addEventListener('pointermove',function(e){show(el,e.clientX,e.clientY);});
el.addEventListener('pointerleave',function(){tip.hidden=true;});
el.addEventListener('focus',function(){var r=el.getBoundingClientRect();show(el,r.right,r.top);});
el.addEventListener('blur',function(){tip.hidden=true;});});})();
"""


def render(runs: list[Run], meta: dict | None = None) -> str:
    meta = meta or {}
    turn_svg, turn_stats = turn_view(runs)
    strip_svg, medians = strip_view(runs)
    order = series_order(runs)
    n = {ch: sum(1 for r in runs if r.series == ch) for ch in order}
    timed = {ch: sum(1 for r in runs if r.series == ch and r.timed) for ch in order}
    models = sorted({r.model for r in runs}) or ["?"]
    headline = ""
    if "local" in turn_stats and any(ch != "local" for ch in turn_stats):
        loc = turn_stats["local"]
        lt = loc["agent"] + loc["tool"]
        lines = []
        for ch in (c for c in order if c != "local" and c in turn_stats):
            b = turn_stats[ch]
            bt = b["agent"] + b["tool"]
            lines.append(f"{_s(bt)} on the {label(ch).removeprefix('hpc-bridge · ')} ({bt / lt:.1f}×, the channel "
                         f"adding {_s(b['tool'] - loc['tool'])})")
        headline = (f"On average a REPL turn took {_s(lt)} on the local shell. On hpc-bridge it took "
                    + "; ".join(lines) + ". Medians are in the table view.")
    sweep = meta.get("sweep_id")
    sub = (" · ".join(f"{n[ch]} {label(ch)}" for ch in order) + f" runs · model {', '.join(models)}"
           + (f" · sweep {sweep}" if sweep else "") + f" · generated {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}")
    strip_sub = ("Every warm protocol step from every timed run, one dot each; the line marks the median."
                 + (" Medians: " + ", ".join(f"{label(ch)} {_s(medians[ch])}" for ch in order if ch in medians) + "."
                    if medians else ""))
    return f"""<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>REPL benchmark sweep</title>
<style>{_CSS}</style>
<div class="viz-root"><div class="wrap">
<h1>REPL benchmark: hpc-bridge against the local shell</h1>
<p class="sub">{_esc(sub)}</p>
{f'<div class="card"><p style="margin:0">{_esc(headline)}</p></div>' if headline else ''}
<section class="card"><h2>Where a REPL turn's time goes</h2>
<p class="sub">Mean warm turn per channel: the agent choosing the next step, then the tool running it.
Timing uses runs that completed the protocol cleanly ({_esc(", ".join(f"{timed[ch]} {label(ch)}" for ch in order))}).</p>
{turn_svg}</section>
<section class="card"><h2>Tool latency per step</h2><p class="sub">{_esc(strip_sub)}</p>{strip_svg}</section>
<section class="card"><h2>REPL properties</h2><p class="sub">Runs that passed each property. The local Bash tool is
expected to fail environment carry-over: it keeps the working directory between calls but not environment variables.</p>
<div class="scroll">{props_view(runs)}</div></section>
<section class="card"><details><summary>Every run (table view)</summary><div class="scroll" style="margin-top:10px">
{runs_table(runs)}</div></details></section>
</div></div>
<div id="tip" hidden></div>
<script>{_JS}</script>
"""


# Light-mode tokens for a slide (the HTML page themes itself through CSS variables; a deck slide has no such variables).
# Scoped under .repl-graph so the rules cannot touch anything else once the SVG is inlined into a slide deck. Sizes are
# real pixels: the slide draws the SVG at 1:1 across its 1136 px content width.
_SLIDE_CSS = (".repl-graph .grid{stroke:#e1e0d9;stroke-width:1}.repl-graph .baseline{stroke:#c3c2b7;stroke-width:1.5}"
              ".repl-graph .tick{fill:#6b6a66;font-size:15px}.repl-graph .rowlabel{fill:#2d3748;font-size:18px}"
              ".repl-graph .value{fill:#0b0b0b;font-size:18px;font-weight:700}.repl-graph .inlabel{fill:#0b0b0b;font-size:16px}"
              ".repl-graph .note{fill:#6b6a66;font-size:15px}.repl-graph .medlabel{fill:#52514e;font-size:15px;font-weight:600}"
              ".repl-graph .agent{fill:#c3c2b7}.repl-graph .tool{fill:#2a78d6}"
              ".repl-graph .dot{fill:#2a78d6;stroke:#fff;stroke-width:2}.repl-graph .hit{fill:transparent}"
              ".repl-graph .median{stroke:#0b0b0b;stroke-width:2.5;stroke-linecap:round}"
              ".repl-graph .h{fill:#1a202c;font-size:20px;font-weight:700}.repl-graph .legend{fill:#4a5568;font-size:16px}")


def _place(svg: str, y: float) -> tuple[str, float]:
    """Nest a view's own <svg viewBox="0 0 W h"> at (0, y); returns it and its height."""
    import re
    m = re.search(r'viewBox="0 0 (\d+) ([\d.]+)"', svg)
    assert m, "a view without a viewBox"
    h = float(m.group(2))
    return svg.replace("<svg ", f'<svg x="0" y="{y:.0f}" width="{m.group(1)}" height="{h:.0f}" ', 1), h


def slide_svg(runs: list[Run]) -> str:
    """The two charts as ONE standalone light-mode SVG drawn FOR a 1280×720 slide: 1136 px wide at 1:1, one shared
    x-axis running only as far as the data needs, headings on the slide's left edge, and a total height that fits
    between the slide title and its footer band."""
    g = SLIDE
    by_channel = [statistics.fmean([a + b for r in runs if r.series == ch and r.timed
                                    for a, b in zip(r.turn_agent, r.turn_tool, strict=True)] or [0])
                  for ch in series_order(runs)]
    tools = [x for r in runs if r.timed for x in r.tool]
    shared = max([*by_channel, max(tools, default=0)]) * 1.02   # stacked charts: a second sits in the same place in each
    turn_html, _ = turn_view(runs, axis_max=shared, g=g, pad=1.0)
    strip, _ = strip_view(runs, axis_max=shared, g=g, pad=1.0)
    turn = turn_html[turn_html.index("<svg"):]
    parts = ['<text class="h" x="0" y="18">Where a REPL turn\'s time goes (mean)</text>',
             '<rect x="0" y="32" width="14" height="14" rx="3" class="agent"/>'
             '<text class="legend" x="22" y="45">agent time — choosing the next step</text>'
             '<rect x="318" y="32" width="14" height="14" rx="3" class="tool"/>'
             '<text class="legend" x="340" y="45">tool time — running it</text>']
    placed, h = _place(turn, 54)
    parts.append(placed)
    y = 54 + h + 24
    parts.append(f'<text class="h" x="0" y="{y:.0f}">Tool latency per step (every warm step, every run)</text>')
    placed, h2 = _place(strip, y + 12)
    parts.append(placed)
    total = y + 12 + h2
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {g.W} {total:.0f}" '
            f'font-family="-apple-system, \'Segoe UI\', Helvetica, Arial, sans-serif">'
            f"<style>{_SLIDE_CSS}</style><g class=\"repl-graph\">{''.join(parts)}</g></svg>\n")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", type=Path, help="a sweep dir (with manifest.json) or a runs dir")
    ap.add_argument("--out", type=Path, help="output HTML (default: <sweep>/repl-sweep.html, or ./repl-sweep.html)")
    ap.add_argument("--svg", type=Path, help="also write the two charts as one light-mode SVG for a slide")
    args = ap.parse_args(argv)
    if not args.path.is_dir():
        print(f"not a directory: {args.path}", file=sys.stderr)
        return 2
    dirs, meta = bundles_from(args.path)
    runs = collect(dirs)
    if not runs:
        print(f"no repl_baseline_local / repl_interaction bundles found via {args.path}", file=sys.stderr)
        return 1
    out = args.out or (args.path / "repl-sweep.html" if meta else Path("repl-sweep.html"))
    out.write_text(render(runs, meta))
    print(f"graph: {out}  ({len(runs)} runs)")
    if args.svg:
        args.svg.write_text(slide_svg(runs))
        print(f"slide svg: {args.svg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

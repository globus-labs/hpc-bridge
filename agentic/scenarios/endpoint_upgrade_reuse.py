"""An endpoint an OLDER hpc-bridge stood up is reused by this one: zero SSH, its blocks keep the environment they were
started with, and no spurious "env_setup differs" note (0.1.24).

The returning user's story: 0.1.23 discovered the facility, cached its config (facilities.json holds 0.1.23's guarded
uv line), stood up an endpoint whose UEP template carries that line (encoded once) and whose manager runs from the
unkeyed `{venv}`; the endpoint is still online. This plugin reads the cached line as its own short line (the
upgrade-on-read), finds the endpoint online over the web and reuses it — no SSH, so nothing is checked or built — and
sends the endpoint's OWN worker_init with every task. The difference between the two lines is the upgrade, not an edit,
so the connect and the pilot notices say nothing about it.

Phase 1 is simulated (a per-phase build of an older plugin is too heavy for a cell): SEED_FACILITY_CACHE writes 0.1.23's
cached config, SETUP clears this client's keyed venv and the unkeyed one, then builds the unkeyed one as 0.1.23's line
would but with `texttable<1.7.2` (that line's unbounded install now takes texttable 1.7.2, whose `list` failure stops
the old bootstrap after its `start`; its guards leave a venv that is there as it is), and LOCAL_SETUP runs
`harness/old_endpoint.py`, which stands up the endpoint with exactly what 0.1.23 wrote.

Graded: the connect reports reused=True; compute ran; one endpoint id throughout; no notice carries the env-differs
note; the endpoint's template still holds the old line (never rewritten) and the keyed venv was never built (zero SSH).
Fake cluster only. Free operators: autonomous, no persona, no hooks.
"""
import json
import os
import re
import shlex
from pathlib import Path

from cluster_ops import build_unkeyed_venv_cmd, keyed_endpoint_venv, plugin_versions
from invariants import Result, Trace, compute_ran

NEEDS_COMPUTE_NODE = True
TARGETS = ("fake",)
REQUIRES = {"scheduler": "slurm"}

GCE_V, PY_V = plugin_versions()
KEY = keyed_endpoint_venv(GCE_V, PY_V)
_ROOT = Path(__file__).resolve().parents[2]
_EP = os.environ.get("HPC_BRIDGE_ENDPOINT_NAME", "hpc-bridge-fake-local")
_OLD_LINE = (f"[ -d {{venv}} ] || uv venv {{venv}} --python {PY_V}; . {{venv}}/bin/activate; "
             f"command -v globus-compute-endpoint >/dev/null 2>&1 || uv pip install -q 'globus-compute-endpoint=={GCE_V}'")
_DETAILS = {   # what 0.1.23 cached for the fake cluster's login node (facilities.json is keyed by ssh_host)
    "ssh_host": "login",
    "interface": "eth0",
    "env_setup": _OLD_LINE,
    "scratch_root": "/home/{user}/.hpc-bridge",
    "partition": "main",
    "scheduler": "slurm",
}
SEED_FACILITY_CACHE = {"login": _DETAILS}

SETUP = [
    f'rm -rf "{KEY}" "{KEY}".broken.* "{KEY}".failed.* "{KEY}.lock" "$HOME/hpc-bridge/gce-venv"; true',
    # the unkeyed venv 0.1.23's line would build, with texttable pinned below 1.7.2 (its own unbounded install now
    # takes 1.7.2, whose `list` stops the old bootstrap); the line's guards then leave it as it is
    build_unkeyed_venv_cmd(GCE_V, PY_V),
]
LOCAL_SETUP = [  # the cached details ride as one shell word (the old line has single quotes of its own)
    f"uv run --project {_ROOT} --no-sync python {_ROOT}/agentic/harness/old_endpoint.py {shlex.quote(json.dumps(_DETAILS))}",
]

PROMPT = (
    "Bring up a compute block on the HPC facility with login host `{ssh_host}` — use facility id `{facility}` — run "
    "`hostname` on it, report what ran where and anything the tools said about the endpoint, and stop the endpoint.\n\n"
    "This is an AUTOMATED run — nobody can answer questions: use the configuration hpc-bridge already has for this host, "
    "pick the cheapest sensible partition yourself, and you are authorised to confirm the spend."
)
USER_GOAL = ""
PERSONA = None
KIND = "regression"
SUMMARY = "an endpoint 0.1.23 stood up (old line, unkeyed venv) is reused: zero SSH, compute runs on it, no env-differs note"
TAGS = ["venv", "upgrade", "reuse", "zero-ssh", "fake", "0.1.24"]
MAX_TURNS = 50

_DIFFERS = "started with a different env_setup"


def reused_old_endpoint(t: Trace) -> Result:
    """The connect reattached to the endpoint the older plugin started (reused=True), and never re-provisioned it."""
    reused = [i for i, c in t.named("connect_facility") if (c.result or {}).get("reused") is True]
    return Result("reused_old_endpoint", bool(reused), f"ok: connect {reused[0]} reused it" if reused
                  else "no connect_facility result reported reused=True")


def one_endpoint_throughout(t: Trace) -> Result:
    ids = {str((c.result or {}).get("endpoint_id")) for c in t.calls
           if re.fullmatch(r"[0-9a-f-]{36}", str((c.result or {}).get("endpoint_id") or ""))}
    ok = len(ids) == 1
    return Result("one_endpoint_throughout", ok, f"ok: {next(iter(ids))}" if ok else f"endpoint ids seen: {sorted(ids)}")


def no_env_differs_note(t: Trace) -> Result:
    """The cached line was upgraded on read; that is not an edit: no result says the env_setup differs."""
    bad = [i for i, c in enumerate(t.calls) if _DIFFERS in str((c.result or {}).get("notice", ""))]
    return Result("no_env_differs_note", not bad, "ok" if not bad else f"spurious env-differs note at calls {bad}")


EXTRA_INVARIANTS = [reused_old_endpoint, compute_ran, one_endpoint_throughout, no_env_differs_note]
EXPECT_OK = ["agent_engaged", "reused_old_endpoint", "compute_ran", "one_endpoint_throughout", "no_env_differs_note",
             "ends_with_stop", "stop_is_honest", "no_raw_ssh_after_endpoint_up"]

POSTCHECKS = [
    # the endpoint's template was never rewritten (a reuse writes nothing): it still carries 0.1.23's guarded line —
    # or the agent tore the endpoint down at the end, which removes it (allowed)
    {"name": "old_template_untouched",
     "cmd": (f'f="$HOME/.globus_compute/{_EP}/user_config_template.yaml.j2"; if [ -f "$f" ]; then '
             'grep -q "command -v globus-compute-endpoint" "$f" && echo old-line-kept || echo TEMPLATE-REWRITTEN; '
             "else echo torn-down; fi"),
     "expect_absent": "TEMPLATE-REWRITTEN"},
    # zero SSH: nothing checked or built this client's keyed venv
    {"name": "keyed_venv_never_built", "cmd": f'[ -e "{KEY}" ] && echo KEYED-BUILT || echo keyed-absent',
     "expect_present": "keyed-absent", "expect_absent": "KEYED-BUILT"},
]
TEARDOWN = "delete"

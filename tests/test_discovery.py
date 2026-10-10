"""discovery.py — probe parsing into a proposed FacilityDetails (no real SSH)."""
import pytest

from hpc_bridge import discovery
from hpc_bridge.discovery import discover_facility_details, parse_probe
from hpc_bridge.facility.remote import SshTarget

# globus1-shaped: no $SCRATCH, gce not installed but uv present, one real NIC, no accounting.
_GLOBUS = """\
login banner noise that must be ignored
HPCB_PROBE_BEGIN
USER=glabs
HOME=/home/glabs
SCRATCH=
WORK=
PSCRATCH=
SCHED=slurm
GCE=
UV=/usr/local/bin/uv
MYBALANCE=
XDUSAGE=
PART=main*|up
NIC=lo|127.0.0.1/8
NIC=enP7s7|128.135.123.175/24
HPCB_PROBE_END
trailing noise
"""

# Anvil-shaped: $SCRATCH set, gce in a venv, a dedicated ib fabric beside eth0, mybalance present.
_ANVIL = """\
HPCB_PROBE_BEGIN
USER=x-anvil
HOME=/home/x-anvil
SCRATCH=/anvil/scratch/x-anvil
WORK=
PSCRATCH=
SCHED=slurm
GCE=/home/x-anvil/hpc-bridge/gce-venv/bin/globus-compute-endpoint
UV=/usr/bin/uv
MYBALANCE=/apps/bin/mybalance
XDUSAGE=
PART=debug|up
PART=shared*|up
PART=wholenode|up
NIC=lo|127.0.0.1/8
NIC=eth0|10.0.0.5/24
NIC=ib0|10.1.0.5/24
HPCB_PROBE_END
"""


# Polaris-shaped: PBS (qsub, no sbatch), Slingshot hsn0 beside the bond0 mgmt NIC, conda env.
_POLARIS = """\
HPCB_PROBE_BEGIN
USER=rchard
HOME=/home/rchard
SCRATCH=
WORK=
PSCRATCH=
SCHED=pbs
GCE=
UV=
MYBALANCE=
XDUSAGE=
QUEUE=debug
QUEUE=prod
NIC=lo|127.0.0.1/8
NIC=bond0|10.140.56.127/24
NIC=hsn0|10.201.3.11/16
HPCB_PROBE_END
"""


def test_parse_probe_detects_pbs_and_queue():
    draft, _notes = parse_probe(_POLARIS, ssh_host="polaris")
    assert draft.scheduler == "pbs"
    assert draft.partition == "debug"       # preferred PBS queue


def test_parse_probe_prefers_compute_fabric_over_bond():
    draft, _notes = parse_probe(_POLARIS, ssh_host="polaris")
    assert draft.interface == "hsn0"        # Slingshot fabric, not the bond0 mgmt NIC


def test_probe_script_checks_qsub_and_qstat():
    assert "qsub" in discovery._PROBE and "qstat -Q" in discovery._PROBE


def test_parse_probe_globus_uv_bootstrap_no_scratch_no_alloc():
    draft, notes = parse_probe(_GLOBUS, ssh_host="globus1")
    assert draft.ssh_host == "globus1" and draft.scheduler == "slurm"
    assert draft.interface == "enP7s7"  # lo excluded, single real NIC
    assert draft.partition == "main"  # only partition (default-marked)
    assert draft.scratch_root == "/home/{user}/.hpc-bridge"  # no $SCRATCH -> $HOME, user templated
    assert draft.env_setup == discovery._UV_ENV_SETUP  # the verified uv venv (tests/test_env_setup.py runs it)
    assert 'uv venv -q "$__hb_venv"' in draft.env_setup and "uv pip install" in draft.env_setup
    assert draft.allocation_command is None and draft.allocation_parser is None
    joined = " ".join(notes)  # interface/env_setup/scratch flagged for confirmation
    assert "interface" in joined and "env_setup" in joined and "scratch_root" in joined


def test_parse_probe_anvil_scratch_ib0_venv_mybalance():
    draft, _notes = parse_probe(_ANVIL, ssh_host="anvil.rcac.purdue.edu")
    assert draft.interface == "ib0"  # fast-fabric NIC preferred over eth0
    assert draft.partition == "debug"  # cheap/quick queue preferred over the default 'shared'
    assert draft.scratch_root == "/anvil/scratch/{user}/.hpc-bridge"  # $SCRATCH, user templated
    assert draft.env_setup == "source /home/{user}/hpc-bridge/gce-venv/bin/activate"  # gce venv activate
    assert draft.allocation_command == "mybalance" and draft.allocation_parser == "mybalance"


def test_parse_probe_shared_scratch_base_gets_per_user_subdir_and_note():
    # Midway-shaped: $SCRATCH is a SHARED base (no login name in it) -> append a per-user subdir AND
    # flag it (a shared base ⇒ Permission denied on every session cd; this stranded a live run).
    stdout = "\n".join([
        "HPCB_PROBE_BEGIN", "USER=gusellerm", "HOME=/home/gusellerm", "SCRATCH=/scratch/midway3",
        "SCHED=slurm", "UV=/usr/bin/uv", "NIC=bond0|10.0.0.5/24", "HPCB_PROBE_END",
    ])
    draft, notes = parse_probe(stdout, ssh_host="midway")
    assert draft.scratch_root == "/scratch/midway3/{user}/.hpc-bridge"  # per-user subdir appended
    assert any("per-user" in n and "scratch_root" in n for n in notes)  # and flagged to confirm


def test_parse_probe_flags_missing_toolchain_and_scheduler():
    out = "HPCB_PROBE_BEGIN\nUSER=u\nHOME=/home/u\nSCHED=none\nGCE=\nUV=\nHPCB_PROBE_END\n"
    draft, notes = parse_probe(out, ssh_host="h")
    j = " ".join(notes)
    assert "scheduler" in j and "sbatch" in j  # non-Slurm flagged
    # neither gce nor uv -> the self-provisioning line (install uv, then the endpoint), flagged for the user
    assert draft.env_setup == discovery._BOOTSTRAP_UV_ENV_SETUP and "astral.sh/uv/install.sh" in draft.env_setup
    assert any("module load" in n for n in notes)


async def test_discover_runs_login_shell_probe_and_parses(monkeypatch):
    captured = {}

    async def fake_ssh_exec(target, cmd, **kw):
        captured["cmd"] = cmd
        return (0, _GLOBUS, "")

    monkeypatch.setattr(discovery, "ssh_exec", fake_ssh_exec)
    draft, _notes = await discover_facility_details(SshTarget("globus1", "glabs", "/k"))
    assert draft.interface == "enP7s7"
    assert captured["cmd"].startswith("bash -lc ")  # raw login-shell SSH, not over the endpoint
    assert "HPCB_PROBE_BEGIN" in captured["cmd"]  # the single batched probe script


async def test_discover_raises_when_probe_never_ran(monkeypatch):
    async def fake_ssh_exec(target, cmd, **kw):
        return (255, "", "ssh: connect to host h port 22: Connection refused")

    monkeypatch.setattr(discovery, "ssh_exec", fake_ssh_exec)
    with pytest.raises(RuntimeError, match="discovery probe failed"):
        await discover_facility_details(SshTarget("h", "u", "/k"))


# Lmod-shaped: nothing useful on the DEFAULT PATH (no endpoint, no uv), but a module system offering uv and Python.
_LMOD = """\
HPCB_PROBE_BEGIN
USER=hpcbridge-test-00
HOME=/home/hpcbridge-test-00
SCRATCH=
WORK=
PSCRATCH=
SCHED=slurm
GCE=
UV=
MYBALANCE=
XDUSAGE=
LMOD_INIT=/etc/profile.d/lmod.sh
MODULE=gcc/
MODULE=gcc/13.2
MODULE=python/
MODULE=python/3.11
MODULE=uv/
MODULE=uv/0.12.9
PART=compute*|up
NIC=eth0|172.20.0.5/16
HPCB_PROBE_END
"""


def test_probe_script_looks_for_a_module_system():
    assert "module -t avail" in discovery._PROBE and "LMOD_INIT=" in discovery._PROBE


def test_parse_probe_prefers_the_sites_uv_module_over_the_curl_bootstrap():
    draft, notes = parse_probe(_LMOD, ssh_host="login")
    assert draft.env_setup.startswith("type module >/dev/null 2>&1 || . /etc/profile.d/lmod.sh; module load uv/0.12.9; ")
    assert draft.env_setup.endswith(discovery._UV_ENV_SETUP) and "astral.sh" not in draft.env_setup
    assert any("module load uv/0.12.9" in n for n in notes)


def test_parse_probe_uses_a_python_module_matching_this_client():
    py = discovery._client_python()
    stdout = _LMOD.replace("MODULE=uv/0.12.9\n", "").replace("MODULE=uv/\n", "").replace("MODULE=python/3.11", f"MODULE=python/{py}")
    draft, notes = parse_probe(stdout, ssh_host="login")
    assert f'module load python/{py}; __hb_base="{{venv}}"; __hb_venv="$__hb_base-endpoint-' in draft.env_setup
    assert 'python3 -m venv "$__hb_venv" && "$__hb_venv/bin/python" -m pip install -q' in draft.env_setup
    assert "astral.sh" not in draft.env_setup
    assert any(f"python/{py}" in n and "matches this client" in n for n in notes)


def test_parse_probe_falls_back_to_the_uv_bootstrap_when_no_module_matches():
    stdout = _LMOD.replace("MODULE=uv/0.12.9\n", "").replace("MODULE=uv/\n", "").replace("MODULE=python/3.11", "MODULE=python/2.7")
    draft, notes = parse_probe(stdout, ssh_host="login")
    assert "astral.sh" in draft.env_setup and "module load" not in draft.env_setup
    assert any("python/2.7" in n and "none matches" in n for n in notes)
    # no module system at all: unchanged behaviour and the old hint
    draft2, notes2 = parse_probe(_GLOBUS.replace("UV=/usr/local/bin/uv", "UV="), ssh_host="globus1")
    assert "astral.sh" in draft2.env_setup and any("say so and use that instead" in n for n in notes2)


def test_module_env_setup_reinitialises_module_for_batch_scripts():
    # a compute node's #!/bin/bash job script never ran /etc/profile.d: the setup sources the init itself
    setup, _ = discovery._module_env_setup(["uv/0.12.9"], "/usr/share/lmod/lmod/init/bash")
    assert setup.startswith("type module >/dev/null 2>&1 || . /usr/share/lmod/lmod/init/bash; module load uv/0.12.9;")
    setup2, _ = discovery._module_env_setup(["uv/0.12.9"], None)
    assert setup2.startswith("module load uv/0.12.9;")
    assert discovery._module_env_setup([], "/etc/profile.d/lmod.sh") is None
    assert discovery._module_env_setup(["gcc/13.2", "openmpi/4.1"], "/etc/profile.d/lmod.sh") is None
    # Lmod's terse `avail` prints directory rows too ("uv/"): never `module load uv/`
    assert discovery._module_env_setup(["uv/", "python/"], "/etc/profile.d/lmod.sh") is None
    assert "module load uv/0.12.9;" in discovery._module_env_setup(["uv/", "uv/0.12.9"], None)[0]


# ---- an existing install is checked, not trusted (globus1, 2026-10-10) ------------------------------------------

def _with(stdout: str, **lines: str) -> str:
    extra = "".join(f"{k}={v}\n" for k, v in lines.items())
    return stdout.replace("HPCB_PROBE_END", extra + "HPCB_PROBE_END")


def test_a_broken_install_on_path_is_passed_over_for_our_own_venv():
    # the probe found a gce whose CLI fails at import (globus-sdk drifted under it): never propose activating it
    out = _with(_ANVIL, GCE_STATE="ImportError: cannot import name 'X' from 'globus_sdk'")
    draft, notes = parse_probe(out, ssh_host="anvil")
    assert draft.env_setup == discovery._UV_ENV_SETUP  # uv is there: hpc-bridge's own, verified venv
    env = next(n for n in notes if n.startswith("env_setup"))
    assert "BROKEN" in env and "cannot import name 'X'" in env and "not used" in env


def test_an_install_on_path_with_another_python_is_passed_over():
    out = _with(_ANVIL, GCE_STATE="3.9 4.16.0")
    draft, notes = parse_probe(out, ssh_host="anvil")
    assert draft.env_setup == discovery._UV_ENV_SETUP
    assert any("runs Python 3.9" in n and "not used" in n for n in notes)


def test_a_working_install_on_path_is_used_and_its_version_named():
    py = discovery._client_python()
    draft, notes = parse_probe(_with(_ANVIL, GCE_STATE=f"{py} 4.13.0"), ssh_host="anvil")
    assert draft.env_setup == "source /home/{user}/hpc-bridge/gce-venv/bin/activate"
    assert any(f"globus-compute-endpoint 4.13.0 (Python {py}) already at" in n for n in notes)


@pytest.mark.parametrize(("state", "says"), [
    ("{py} {gce}", "is reused"),
    ("{py} 4.13.0", "the first connect rebuilds it (that directory only)"),
    ("3.12 {gce}", "the first connect rebuilds it (that directory only)"),
    ("ModuleNotFoundError: No module named 'globus_compute_endpoint'", "is BROKEN (ModuleNotFoundError"),
    ("not a venv", "is NOT a venv: hpc-bridge will not replace it"),
])
def test_the_proposal_says_what_happens_to_an_existing_hpc_bridge_venv(state, says):
    py, gce = discovery._client_python(), discovery._gce_version() or "4.18.0"
    if py == "3.12" and state.startswith("3.12"):
        pytest.skip("that Python is this client's")
    out = _with(_GLOBUS, VENV="/home/glabs/hpc-bridge/gce-venv", VENV_STATE=state.format(py=py, gce=gce))
    _draft, notes = parse_probe(out, ssh_host="globus1")
    env = next(n for n in notes if n.startswith("env_setup"))
    assert "/home/glabs/hpc-bridge/gce-venv" in env and says in env, env


def test_no_venv_no_note():
    _draft, notes = parse_probe(_GLOBUS, ssh_host="globus1")
    assert not any("hpc-bridge venv" in n for n in notes)


def test_the_probe_checks_the_venv_the_bootstrap_will_use(monkeypatch):
    monkeypatch.delenv("HPC_BRIDGE_REMOTE_VENV", raising=False)
    monkeypatch.setattr(discovery, "_gce_version", lambda: "4.18.0")
    monkeypatch.setattr(discovery, "_client_python", lambda: "3.13")
    assert "V=/home/$(whoami)/hpc-bridge/gce-venv-endpoint-4.18.0-py3.13\n" in discovery._probe_script()  # keyed
    monkeypatch.setenv("HPC_BRIDGE_REMOTE_VENV", "/scratch/me/gce")
    assert "V=/scratch/me/gce-endpoint-4.18.0-py3.13\n" in discovery._probe_script()
    assert "@@VENV@@" not in discovery._probe_script()
    assert "export PYTHONDONTWRITEBYTECODE=1" in discovery._PROBE  # the probe writes nothing, not even bytecode
    assert ' version 2>&1' in discovery._PROBE and "import globus_compute_endpoint.cli" not in discovery._PROBE

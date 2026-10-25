"""Resource requests at hpc-bridge's own templates (facility/remote.py): what the endpoint's manager renders for a slice,
whole nodes and N nodes on Slurm and PBS — rendered as the manager does (its sanitizer, then Jinja) — the launcher a
multi-node block takes, the template marker that says an endpoint renders them (`takes_resources`), and what
`dispatch_uec` sends. With parsl importable (`uv run --with parsl==2026.8.10 …`, the version uv.lock pins), the rendered
provider block is also built into parsl's own provider and its submit script read."""
import dataclasses
import json

import jinja2
import pytest
import yaml

from hpc_bridge.facility import remote
from hpc_bridge.facility.remote import SlurmFacility, takes_resources
from hpc_bridge.models import PartitionShape
from hpc_bridge.profile import Profile
from hpc_bridge.shapes import shape_config

SHARED = PartitionShape(name="shared", kind="per_core", cores=128, memory_gb=251.4, mem_per_core_mb=1896)
WORKQ = PartitionShape(name="workq", kind="per_core", cores=64, memory_gb=256.0, gpus=4, place="free:shared")


def _slurm(**kw) -> SlurmFacility:
    prof = remote.MachineProfile(name="t", endpoint_name="hpc-bridge-t", env_setup="true", interface="ib0",
                                 partition="shared", account="cis250223", worker_init="true", partitions=(SHARED,))
    f = SlurmFacility(dataclasses.replace(prof, **kw), cli=None)
    f.takes_resources = True
    return f


def _pbs(**kw) -> SlurmFacility:
    return _slurm(scheduler="pbs", partition="workq", partitions=(WORKQ,), cpus_per_node=64,
                  scheduler_options="#PBS -l filesystems=home:eagle", **kw)


def _sanitize(opts):
    return {k: (json.dumps(v) if isinstance(v, str) else v) for k, v in opts.items()}


def _provider(f: SlurmFacility, **request) -> dict:
    tmpl, defaults = f.config_template(Profile(mode="batch"))
    wire = f.dispatch_uec({**defaults, **shape_config("compute"), **request})
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    return yaml.safe_load(env.from_string(tmpl).render(**_sanitize(wire)))["engine"]["provider"]


def test_a_slurm_slice_renders_parsls_cores_and_memory_with_exclusive_false():
    p = _provider(_slurm(), cores_per_node=2, mem_per_node=2)
    assert (p["exclusive"], p["cores_per_node"], p["mem_per_node"]) == (False, 2, 2)
    assert p["launcher"] == {"type": "SrunLauncher"} and p["nodes_per_block"] == 1   # one node keeps SrunLauncher


def test_the_proposed_slice_sends_cores_and_leaves_memory_to_the_partitions_default():
    p = _provider(_slurm())
    assert (p["exclusive"], p["cores_per_node"]) == (False, 8) and "mem_per_node" not in p


def test_whole_nodes_and_gpus_on_slurm():
    p = _provider(_slurm(), exclusive=True, gpus_per_node=2)
    assert p["exclusive"] is True and "cores_per_node" not in p and "mem_per_node" not in p
    assert p["scheduler_options"].strip() == "#SBATCH --gpus-per-node=2"   # hpc-bridge's own line, any parsl
    assert "gpus_per_node" not in p


def test_a_multi_node_block_takes_simplelauncher_on_both_schedulers():
    # SrunLauncher would start a worker pool on EVERY node (as a job step holding their CPUs); SingleNodeLauncher would
    # start N pools on the first node. SimpleLauncher: one worker, in the batch script on the first node (D2).
    assert _provider(_slurm(), nodes_per_block=2, cores_per_node=2)["launcher"] == {"type": "SimpleLauncher"}
    assert _provider(_pbs(), nodes_per_block=2)["launcher"] == {"type": "SimpleLauncher"}
    assert _provider(_pbs())["launcher"] == {"type": "SingleNodeLauncher"}


def test_a_pbs_slice_renders_ncpus_its_chunk_resources_and_a_shared_place():
    p = _provider(_pbs(), cores_per_node=4, mem_per_node=16, gpus_per_node=1)
    assert p["cpus_per_node"] == 4 and p["select_options"] == "mem=16gb:ngpus=1"
    assert p["scheduler_options"] == "#PBS -l filesystems=home:eagle\n#PBS -l place=free:shared"
    whole = _provider(_pbs(), exclusive=True)
    assert whole["cpus_per_node"] == 64 and "place" not in whole["scheduler_options"]


def test_nothing_asked_and_nothing_known_renders_as_before():
    plain = _slurm(partitions=())
    p = _provider(plain)
    assert not {"exclusive", "cores_per_node", "mem_per_node"} & set(p) and p["launcher"] == {"type": "SrunLauncher"}
    assert _provider(_slurm(partitions=(), exclusive=False))["exclusive"] is False        # the registry's, as before
    old = _slurm()
    old.takes_resources = False                                  # an endpoint configured before resource requests
    assert "cores_per_node" not in _provider(old) and "exclusive" not in _provider(old)


def test_the_login_shape_is_untouched():
    f = _slurm()
    _tmpl, defaults = f.config_template(Profile(mode="batch"))
    uec = {**defaults, **shape_config("login"), "cores_per_node": 4}
    assert f.dispatch_uec(uec) == uec


def test_takes_resources_reads_this_plugins_templates_and_not_an_earlier_one():
    for f in (_slurm(), _pbs()):
        tmpl, _ = f.config_template(Profile(mode="batch"))
        assert takes_resources(tmpl)
        old = tmpl.replace("{{ 'SimpleLauncher' if (nodes_per_block | default(1) | int) > 1 else 'SrunLauncher' }}",
                           "SrunLauncher").replace(
            "{{ 'SimpleLauncher' if (nodes_per_block | default(1) | int) > 1 else 'SingleNodeLauncher' }}",
            "SingleNodeLauncher")
        assert not takes_resources(old)
    assert not takes_resources(None) and not takes_resources("")


async def test_adopt_endpoint_learns_whether_the_template_takes_a_size(monkeypatch):
    f = _slurm()
    f.takes_resources = None
    tmpl, _ = f.config_template(Profile(mode="batch"))

    async def md_new(_factory, _eid):
        return {"user_config_template": tmpl}

    monkeypatch.setattr(remote, "endpoint_metadata", md_new)
    await f.adopt_endpoint("e1")
    assert f.takes_resources is True

    async def md_old(_factory, _eid):
        return {"user_config_template": tmpl.replace(
            "{{ 'SimpleLauncher' if (nodes_per_block | default(1) | int) > 1 else 'SrunLauncher' }}", "SrunLauncher")}

    monkeypatch.setattr(remote, "endpoint_metadata", md_old)
    await f.adopt_endpoint("e2")
    assert f.takes_resources is False

    async def md_none(_factory, _eid):
        return {"user_config_template": None}

    monkeypatch.setattr(remote, "endpoint_metadata", md_none)
    await f.adopt_endpoint("e3")
    assert f.takes_resources is None                             # no template read: not known


# ---- through parsl itself, when it is importable (the locked 2026.8.10) -------------------------------------------------

def _parsl_script(f: SlurmFacility, tmp_path, monkeypatch, **request) -> tuple[str, str, str]:
    """Build parsl's provider from the rendered block as globus-compute-endpoint's dispatch does (kwargs, None dropped,
    the launcher by type), submit with a stand-in execute_wait, and read the script it wrote."""
    parsl_launchers = pytest.importorskip("parsl.launchers")
    parsl_providers = pytest.importorskip("parsl.providers")
    prov = dict(_provider(f, **request))
    launcher = getattr(parsl_launchers, prov.pop("launcher")["type"])()
    cls = getattr(parsl_providers, prov.pop("type"))
    calls: list[str] = []

    def execute_wait(self, cmd, timeout=None):
        calls.append(cmd)
        return (0, "Submitted batch job 7\n", "") if cmd.startswith("sbatch") else (0, "7.pbs\n", "")

    monkeypatch.setattr(cls, "execute_wait", execute_wait, raising=False)
    p = cls(launcher=launcher, **{k: v for k, v in prov.items() if v is not None})
    p.script_dir = str(tmp_path)
    p.submit("process_worker_pool.py --block_id=0", 1, "parsl.test")   # HTEX submits with tasks_per_node=1
    submit = calls[-1]
    return submit, open(submit.split()[-1]).read(), type(launcher).__name__


def test_parsl_writes_the_slice_into_the_submit_script(tmp_path, monkeypatch):
    _cmd, script, launcher = _parsl_script(_slurm(), tmp_path, monkeypatch, cores_per_node=2, mem_per_node=2, gpus_per_node=1)
    for line in ("#SBATCH --nodes=1", "#SBATCH --ntasks-per-node=1", "#SBATCH --cpus-per-task=2", "#SBATCH --mem=2g",
                 "#SBATCH --gpus-per-node=1", "export PARSL_CORES=2"):
        assert line in script, line
    assert "#SBATCH --exclusive" not in script and launcher == "SrunLauncher" and "srun --ntasks 1" in script


def test_parsl_two_nodes_run_one_worker_in_the_batch_script(tmp_path, monkeypatch):
    _cmd, script, launcher = _parsl_script(_slurm(), tmp_path, monkeypatch, nodes_per_block=2, cores_per_node=2)
    assert "#SBATCH --nodes=2" in script and launcher == "SimpleLauncher" and "srun" not in script
    _cmd, whole, _ = _parsl_script(_slurm(), tmp_path, monkeypatch, exclusive=True)
    assert "#SBATCH --exclusive" in whole and "--cpus-per-task" not in whole


def test_parsl_writes_the_pbs_select_and_place(tmp_path, monkeypatch):
    cmd, script, launcher = _parsl_script(_pbs(), tmp_path, monkeypatch, cores_per_node=4, mem_per_node=16, gpus_per_node=1)
    assert "#PBS -l select=1:ncpus=4:mem=16gb:ngpus=1" in script and "#PBS -l place=free:shared" in script
    assert "#PBS -l filesystems=home:eagle" in script and launcher == "SingleNodeLauncher"
    assert "-q workq" in cmd and "-A cis250223" in cmd

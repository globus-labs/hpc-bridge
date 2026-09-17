# tests/test_mep_facility.py
import pytest

from hpc_bridge.facility.base import EndpointHandle, Facility
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.lifecycle import EndpointState, ensure_warm, probe
from hpc_bridge.profile import Profile

UUID = "da3df250-4013-4d69-942c-eef1568f860c"


def _fac(**over):
    opts = {
        "compute": True,
        "partition": "main",
        "walltime": "00:10:00",
        "nodes_per_block": 1,
        "max_workers_per_node": 2,
        "init_blocks": 1,
        "max_blocks": 1,
        "worker_init": "uv pip install -q globus-compute-endpoint==4.15.0",
        "interface": "enP7s7",
    }
    kw = {"endpoint_id": UUID, "name": "globus-cluster-mep", "user_opts": opts}
    kw.update(over)
    return MEPFacility(**kw)


class _Client:
    def __init__(self, status="online", boom=False):
        self._status = status
        self._boom = boom

    def get_endpoint_status(self, endpoint_id):
        if self._boom:
            raise RuntimeError("403: not the endpoint owner")
        return {"status": self._status}


def test_satisfies_facility_protocol():
    assert isinstance(_fac(), Facility)  # provision / manager_online / config_template present


async def test_provision_hands_back_uuid_reused_no_ssh():
    h = await _fac().provision(Profile())
    assert isinstance(h, EndpointHandle)
    assert h.endpoint_id == UUID
    assert h.name == "globus-cluster-mep"
    assert h.reused is True  # a zero-SSH attach, never a fresh bootstrap


def test_config_template_returns_uec_defaults_only():
    slot, defaults = _fac().config_template(Profile())
    assert slot == ""  # we don't own the template
    # the verified globus1 UEC: compute-shape defaults that shape_config('compute') tops with compute=True
    assert defaults["compute"] is True
    assert defaults["partition"] == "main"
    assert defaults["init_blocks"] == 1  # the warm-block login-shape replacement
    assert defaults["worker_init"].endswith("==4.15.0")
    # returned copy is defensive — mutating it can't corrupt the facility's own opts
    defaults["partition"] = "mutated"
    assert _fac().config_template(Profile())[1]["partition"] == "main"


async def test_manager_online_reads_status_when_readable():
    assert await _fac(client_factory=lambda: _Client("online")).manager_online(UUID) is True
    assert await _fac(client_factory=lambda: _Client("offline")).manager_online(UUID) is False


async def test_manager_online_degrades_to_true_on_error():
    # a status-API error / foreign-endpoint read must not falsely strand us — the canary is authoritative
    assert await _fac(client_factory=lambda: _Client(boom=True)).manager_online(UUID) is True


async def test_ensure_warm_attaches_without_provisioning_work():
    # the lifecycle seam: provision returns the UUID, probe reports warm (manager online), reused threads up
    f = _fac(client_factory=lambda: _Client("online"))
    block, state = await ensure_warm(f, Profile(), EndpointState())
    assert state.endpoint_id == UUID
    assert state.reused is True
    assert block == "warm"


async def test_probe_provisioning_when_manager_reports_offline():
    f = _fac(client_factory=lambda: _Client("offline"))
    assert await probe(f, EndpointState(endpoint_id=UUID)) == "provisioning"


def test_from_entry_threads_a_given_account_and_drops_an_empty_one():
    # review finding: the startup-pin path demanded HPC_BRIDGE_ACCOUNT, then from_entry dropped it
    from tests.fakes import fake_mep_entry
    e = fake_mep_entry(account_required=True)
    assert MEPFacility.from_entry(e, account="lab").config_template(Profile())[1]["account"] == "lab"
    for empty in (None, ""):
        assert "account" not in MEPFacility.from_entry(e, account=empty).config_template(Profile())[1]


NESI_SCHEMA = {"additionalProperties": False, "required": ["ACCOUNT_ID"],
               "properties": {"ACCOUNT_ID": {}, "WALL_TIME": {}, "MEM_PER_CPU": {}, "GPUS_PER_NODE": {}}}


def _nesi_facility(account=None):
    # NeSI's reannz-slurm MEP template wants "ACCOUNT_ID" (required) and "WALL_TIME" (optional), not
    # "account"/"walltime" — confirmed live against the real facility 2026-09-07: its published schema
    # is additionalProperties:false with only {ACCOUNT_ID, WALL_TIME, MEM_PER_CPU, GPUS_PER_NODE}, so an
    # un-renamed "account" is REJECTED (required key missing) and an un-renamed "walltime" is silently
    # DROPPED (every job would run at the facility's own 5-minute default with zero override).
    # key_map defaults to {} so every existing curated MEP entry (Delta, Anvil, globus-cluster) is unaffected.
    from tests.fakes import fake_mep_entry
    e = fake_mep_entry(account_required=True, compute={
        "scheduler": "slurm", "interface": "enP7s7",
        "env_setup": "uv pip install -q globus-compute-endpoint==4.15.0",
        "scratch_root": "$HOME/.hpc-bridge",
        "key_map": {"account": "ACCOUNT_ID", "walltime": "WALL_TIME"},
    })
    fac = MEPFacility.from_entry(e, account=account)
    fac.schema = NESI_SCHEMA
    return fac


def test_key_map_renames_at_the_wire_not_in_the_runtime_config():
    fac = _nesi_facility(account="cis250223")
    runtime = fac.sanitize_uec(dict(fac.config_template(Profile())[1]))
    # the runtime dict keeps hpc-bridge's names: the account floor and the account/partition gates key on them
    assert runtime["account"] == "cis250223" and runtime["walltime"] == "02:00:00"
    assert "ACCOUNT_ID" not in runtime
    wire = fac.dispatch_uec(runtime)
    assert wire["ACCOUNT_ID"] == "cis250223"
    assert wire["WALL_TIME"] == "02:00:00"  # fake_mep_entry's default walltime, renamed not dropped
    assert "account" not in wire and "walltime" not in wire
    assert set(wire) <= set(NESI_SCHEMA["properties"])  # nothing the strict schema would reject


def test_key_map_carries_an_account_confirmed_after_construction():
    # The 0.1.17 account floor: no account at connect, the user is asked, and warmth._apply_account writes
    # the answer into the runtime config under hpc-bridge's name. A rename done at construction missed it:
    # the wire carried no ACCOUNT_ID and NeSI rejected every submit.
    fac = _nesi_facility(account=None)
    runtime = fac.sanitize_uec(dict(fac.config_template(Profile())[1]))
    assert "ACCOUNT_ID" not in fac.dispatch_uec(runtime)  # a None account is not sent as a null
    runtime["account"] = "nesi99999"  # exactly what warmth._apply_account does
    assert fac.dispatch_uec(runtime)["ACCOUNT_ID"] == "nesi99999"


def test_key_map_applies_on_a_permissive_schema_too():
    fac = _nesi_facility(account="cis250223")
    fac.schema = None  # a facility that publishes no schema still names its own keys
    wire = fac.dispatch_uec(dict(fac.config_template(Profile())[1]))
    assert wire["ACCOUNT_ID"] == "cis250223" and "account" not in wire


def test_key_map_rejects_an_unknown_source_key():
    from pydantic import ValidationError

    from tests.fakes import fake_mep_entry
    with pytest.raises(ValidationError, match="key_map"):
        fake_mep_entry(compute={
            "scheduler": "slurm", "interface": "enP7s7",
            "env_setup": "uv pip install -q globus-compute-endpoint==4.15.0",
            "scratch_root": "$HOME/.hpc-bridge",
            "key_map": {"qos": "QOS"},  # qos isn't one of hpc-bridge's own fixed keys — belongs in extra
        })

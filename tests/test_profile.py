import pytest

from hpc_bridge.profile import Profile


def test_profile_defaults_to_batch():
    assert Profile().mode == "batch"


def test_profile_rejects_invalid_mode():
    with pytest.raises(ValueError):
        Profile(mode="garbage")  # type: ignore[arg-type]


def test_profile_rejects_nonpositive_idletime():
    with pytest.raises(ValueError):
        Profile(max_idletime_s=0)


def test_a_compute_user_endpoint_exits_after_ten_idle_minutes_by_default(monkeypatch):
    # 20 heartbeats of 30 s: the block's own idle window. Configurable (0: never exit), never negative.
    from hpc_bridge import config

    assert Profile().uep_idle_heartbeats == 20 == config.uep_idle_heartbeats()
    with pytest.raises(ValueError):
        Profile(uep_idle_heartbeats=-1)
    monkeypatch.setenv("HPC_BRIDGE_UEP_IDLE_HEARTBEATS", "40")
    assert config.uep_idle_heartbeats() == 40
    monkeypatch.setenv("HPC_BRIDGE_UEP_IDLE_HEARTBEATS", "-3")
    assert config.uep_idle_heartbeats() == 20
    monkeypatch.setenv("HPC_BRIDGE_UEP_IDLE_HEARTBEATS", "0")
    assert config.uep_idle_heartbeats() == 0


def test_the_stops_wait_for_our_checks_is_bounded_and_configurable(monkeypatch):
    # 90 s: parsl's 60 s scheduler status poll + a 5 s scaling pass + slack for the failure to reach the service
    from hpc_bridge import config

    assert config.stop_check_wait_s() == config.STOP_CHECK_WAIT_S == 90.0
    monkeypatch.setenv("HPC_BRIDGE_STOP_CHECK_WAIT_S", "0")
    assert config.stop_check_wait_s() == 0.0
    monkeypatch.setenv("HPC_BRIDGE_STOP_CHECK_WAIT_S", "10000")
    assert config.stop_check_wait_s() == 300.0                     # a tool call must not outlive a client's window
    monkeypatch.setenv("HPC_BRIDGE_STOP_CHECK_WAIT_S", "-5")
    assert config.stop_check_wait_s() == 0.0


def test_profile_carries_scheduler_and_cpus_per_node():
    import datetime

    from hpc_bridge.catalog.entry import CatalogEntry, Compute, Defaults
    from hpc_bridge.facility.remote import profile_from_catalog_entry

    entry = CatalogEntry(
        id="polaris", facility_key="alcf", facility="ALCF",
        description="d", display_name="Polaris", ssh_host="polaris",
        compute=Compute(
            scheduler="pbs", interface="hsn0",
            env_setup="source {venv}/bin/activate",
            scratch_root="/home/{user}/.hpc-bridge",
        ),
        defaults=Defaults(partition="debug", cpus_per_node=32),
        last_validated=datetime.date(2026, 7, 10),
    )
    prof = profile_from_catalog_entry(entry, user="rchard", account="acct")
    assert prof.scheduler == "pbs"
    assert prof.cpus_per_node == 32

"""Shared test fixtures."""
import pytest


@pytest.fixture(autouse=True)
def _isolate_hpc_bridge_state(monkeypatch, tmp_path):
    """Point ALL of hpc-bridge's local state — the whole ~/.hpc-bridge tree: `endpoints.json`
    (login-node pins), `facilities.json` (the local-discovery cache), the record of sent checks and the SSH
    control-socket dir — at a per-test tmp dir. So no test reads stale entries or WRITES into the developer's real
    state (a test once polluted the real facilities.json before this existed). Belt-and-suspenders:
    the code defaults to ~/.hpc-bridge; this just relocates it via HPC_BRIDGE_STATE_DIR (and, with
    HPC_BRIDGE_TEST_GUARD=1, state._state_dir refuses the default — a child process inherits both, so it never trips). After the test, the record's own I/O thread is drained, so no write queued by one
    test lands while the next one runs."""
    monkeypatch.setenv("HPC_BRIDGE_STATE_DIR", str(tmp_path / "hpc-bridge-state"))
    monkeypatch.setenv("HPC_BRIDGE_TEST_GUARD", "1")  # state._state_dir refuses the real one while it is set
    yield
    from hpc_bridge import warmth

    warmth._STORE_POOL.submit(lambda: None).result(5)


@pytest.fixture(autouse=True, scope="session")
def _never_the_real_state_dir():
    """Fail the run if anything appeared under the developer's real ~/.hpc-bridge/sent-checks while it ran (found
    2026-10-16: an empty one appeared during a review round)."""
    from pathlib import Path

    real = Path.home() / ".hpc-bridge" / "sent-checks"
    before = set(real.iterdir()) if real.is_dir() else None
    yield
    after = set(real.iterdir()) if real.is_dir() else None
    assert after == before, f"tests wrote into the real {real}: {sorted(map(str, (after or set()) - (before or set())))}"


@pytest.fixture(autouse=True)
def _no_live_registry(monkeypatch, tmp_path, request):
    """The unit tier is HERMETIC: no test may reach the live Globus Search registry, nor write the developer's
    real catalog cache (~/.hpc-bridge/catalog-cache). Found 2026-09-05: 13 tests patched `server.make_catalog`
    — a dead re-export (the callers use `binding.make_catalog`) — so the fakes were never consulted, the live
    index answered, and the tier went red the day the registry changed (#92). Two guards: the catalog cache is
    relocated (`CLAUDE_PLUGIN_DATA`), and `binding.make_catalog` raises unless the test patches it itself or
    opts in with `@pytest.mark.real_catalog`."""
    monkeypatch.setenv("CLAUDE_PLUGIN_DATA", str(tmp_path / "plugin-data"))
    if request.node.get_closest_marker("real_catalog"):
        return
    from hpc_bridge import binding

    def _unpatched():
        raise AssertionError("unit tests must patch binding.make_catalog (the live registry is off-limits here); "
                             "mark the test @pytest.mark.real_catalog if it really needs it")

    monkeypatch.setattr(binding, "make_catalog", _unpatched)


@pytest.fixture(autouse=True)
def _no_dns_in_unit_tests(monkeypatch):
    """The login-node pin asks the client's resolver whether a `hostname -f` is routable (0.1.12). Unit tests must not
    depend on the test host's DNS: every name "resolves" here, so the suffix/label heuristics are what is under test;
    `_routable_pin(..., resolves=...)` injects a resolver where a test wants the unresolvable case."""
    from hpc_bridge.facility import remote

    monkeypatch.setattr(remote, "_resolves_from_here", lambda host: True)


@pytest.fixture(autouse=True)
def _no_startup_pin_leaks(monkeypatch):
    """`binding._PINNED` remembers what HPC_BRIDGE_MACHINE resolved to at a startup pin (process-wide in the server), and
    `_PINNED_ORIGIN` where its default terms came from: each test starts with none, so a pin one test made never names a
    machine for another."""
    from hpc_bridge import binding

    monkeypatch.setattr(binding, "_PINNED", {})
    monkeypatch.setattr(binding, "_PINNED_ORIGIN", {})

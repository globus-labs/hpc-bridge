# tests/test_mep_server.py — the server seams on a facility-run multi-user endpoint (MEP): zero SSH,
# compute-only, draining-only stop. Drives a REAL MEPFacility through the real _connect_facility /
# _ensure_endpoint_up / _run_shell / _stop_endpoint / _teardown_endpoint paths.
from hpc_bridge import binding, scheduler_ops, server
from hpc_bridge.facility.mep import MEPFacility
from hpc_bridge.profile import Profile
from hpc_bridge.runner import CanaryResult
from hpc_bridge.server import AppCtx, _ensure_endpoint_up, _run_shell, _stop_endpoint, _teardown_endpoint
from tests.fakes import MEP_UUID, FakeCatalog, FakeFacility, fake_mep_entry
from tests.test_server import _FakeRunner, _Res

# The admin-verified globus1 UEC (+ the entry's pinned interface/worker_init): what the compute
# shape must submit, and NOTHING login-shaped may ever be submitted.
VERIFIED_UEC = {
    "compute": True, "partition": "main", "walltime": "02:00:00", "nodes_per_block": 1,
    "max_workers_per_node": 2, "init_blocks": 1, "max_blocks": 1,
    "interface": "enP7s7", "worker_init": "uv pip install -q globus-compute-endpoint==4.15.0",
}


class _Status:
    def __init__(self, status="online"):
        self.status = status

    def get_endpoint_status(self, endpoint_id):
        return {"status": self.status}


def _mep(status="online") -> MEPFacility:
    return MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Status(status))


def _app(fac=None) -> AppCtx:
    app = AppCtx(facility=fac or _mep(), profile=Profile())
    app.built = []  # every runner the server asks for, with its user_endpoint_config

    def factory(eid, user_endpoint_config=None, **_kw):
        r = _FakeRunner(eid, _Res(0, "glabs\n", ""))
        app.built.append((eid, dict(user_endpoint_config or {})))
        return r

    app.runner_factory = factory
    return app


async def _connect(app, monkeypatch, entry=None):
    monkeypatch.setattr(binding, "make_catalog", lambda: FakeCatalog([entry or fake_mep_entry()]))
    monkeypatch.setattr(binding, "_facility_from_entry", lambda e, *, account: app.facility)
    return await server._connect_facility(app, "globus1")


# --- connect: attach, don't provision ---------------------------------------------------------------


async def test_connect_attaches_with_zero_ssh_and_no_login_runtime(monkeypatch):
    app = _app()
    res = await _connect(app, monkeypatch)
    assert res.phase == "needs_account"
    assert res.reused is True  # attached to the facility's always-on endpoint
    assert res.allocations == []
    assert app.state.endpoint_id == MEP_UUID
    assert "login" not in app.shapes  # NEVER a login ShapeRuntime on a MEP
    assert app.built == []  # and no runner (no canary, no block) — warming is behind the spend gate
    assert app.scratch_root == "$HOME/.hpc-bridge"  # worker-side root, untouched
    assert "compute-only" in res.notice.lower() or "compute-only" in res.notice
    assert "no allocation account is needed" in res.notice.lower()  # account_required=False -> don't go hunting


async def test_connect_account_required_mep_says_pass_account(monkeypatch):
    fac = MEPFacility.from_entry(fake_mep_entry(account_required=True), client_factory=_Status)
    app = _app(fac)
    res = await _connect(app, monkeypatch)
    assert res.phase == "needs_account"
    assert "ensure_endpoint_up(account=" in res.notice


async def test_connect_offline_mep_is_failed_not_call_again(monkeypatch):
    app = _app(_mep(status="offline"))
    res = await _connect(app, monkeypatch)
    assert res.phase == "failed"
    assert "facility" in res.notice and "offline" in res.notice
    assert "call connect_facility again shortly" not in res.notice  # nothing we do brings it up


# --- compute shape: exactly the verified UEC; login shape: refused structurally ---------------------


async def test_compute_shape_submits_exactly_the_verified_uec(monkeypatch):
    app = _app()
    await _connect(app, monkeypatch)
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "up"
    assert app.built and app.built[-1][0] == MEP_UUID
    assert app.built[-1][1] == VERIFIED_UEC  # no account key, no login keys, compute=True
    out = await _run_shell(app, "whoami", shape="compute")
    assert out.phase == "complete" and "glabs" in (out.stdout or "")


async def test_login_shape_is_refused_without_building_a_runtime(monkeypatch):
    app = _app()
    await _connect(app, monkeypatch)
    for call in (
        lambda: _ensure_endpoint_up(app, shape="login"),
        lambda: _run_shell(app, "ls", shape="login"),
        lambda: server._reset_session(app, shape="login"),
    ):
        res = await call()
        status = getattr(res, "status", None) or getattr(res, "phase", None)
        assert status in ("down", "failed")
        assert "compute" in (res.notice or "") and "login" in (res.notice or "")
    assert "login" not in app.shapes  # refused BEFORE any ShapeRuntime exists
    assert all(uec.get("compute") for _eid, uec in app.built)  # nothing LocalProvider-shaped was ever built


async def test_needs_confirmation_on_mep_does_not_point_at_login(monkeypatch):
    app = _app()
    await _connect(app, monkeypatch)
    res = await _ensure_endpoint_up(app, shape="compute")  # no confirm_spend
    assert res.status == "needs_confirmation"
    assert "shape='login'" not in res.notice and "compute-only" in res.notice
    out = await _run_shell(app, "hostname", shape="compute")
    assert out.phase == "needs_confirmation" and "shape='login'" not in out.notice


async def test_cold_compute_block_skips_the_login_pilot_query(monkeypatch):
    # the #32 provisioning-notice augmenter queries the scheduler over shape="login" — skipped on a MEP
    import time as _time

    app = _app()
    await _connect(app, monkeypatch)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
    server._shape_runtime(app, "compute").provisioning_since = _time.monotonic() - (server.PROVISION_GRACE_S + 10)
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "provisioning"
    assert "login" not in app.shapes


async def test_unmapped_identity_is_a_terminal_down_not_allocating(monkeypatch):
    # the #32 provisioning-notice augmenter queries the scheduler over shape="login" — skipped on a MEP
    import time as _time

    app = _app()
    await _connect(app, monkeypatch)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="TaskExecutionFailed: Identity failed to map to a local user name.  (LookupError) "))
    server._shape_runtime(app, "compute").provisioning_since = _time.monotonic() - (server.PROVISION_GRACE_S + 10)
    monkeypatch.setattr("hpc_bridge.login.globus_identity_label", lambda fetch=True: "someone@example.org")
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "down" and res.block_state == "cold"
    assert "NO ACCOUNT" in res.notice and "someone@example.org" in res.notice and "TERMINAL" in res.notice
    assert "allocating" not in res.notice and "failed to map" in res.notice.lower()


def test_no_account_markers_cover_the_manager_messages():
    from hpc_bridge.server import _no_account_failure
    assert _no_account_failure("TaskExecutionFailed: Identity failed to map to a local user name.  (LookupError) ")
    assert _no_account_failure("(KeyError)\n  Identity mapped to a local user name, but local user does not exist.")
    assert _no_account_failure("Ignoring start request for untrusted identity.")
    assert not _no_account_failure("timeout") and not _no_account_failure(None)
    assert not _no_account_failure("RuntimeError: Executor is shutdown")


def test_cold_run_shell_on_no_account_is_failed_not_cold_start():
    from hpc_bridge.server import _cold_outcome
    out = _cold_outcome("cold", CanaryResult(ok=False, error="TaskExecutionFailed: Identity failed to map to a local user name."))
    assert out.phase == "failed" and "NO ACCOUNT" in out.notice
    assert _cold_outcome("cold", CanaryResult(ok=False, error="timeout")).phase == "cold_start"



# --- stop / teardown: draining-only, no release channel ----------------------------------------------


async def test_stop_is_draining_only_and_never_touches_login(monkeypatch):
    app = _app()
    await _connect(app, monkeypatch)
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    called = []
    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", lambda *a, **k: called.append(1))
    res = await _stop_endpoint(app)
    assert res.status == "draining"  # NEVER "down": no cancel can be confirmed from here
    assert called == []  # no scancel-over-login attempt (and so no login-shape submit)
    assert "do not re-poll" in res.notice.lower()  # draining is terminal here (the skill's re-poll loop must not spin)
    assert "compute" not in app.shapes  # the shape is dropped: nothing further lands on the block
    assert app.state.endpoint_id == MEP_UUID  # the facility's endpoint stays attached/available
    # a second stop is honest too, not a lie
    assert (await _stop_endpoint(app)).status == "draining"


async def test_teardown_is_a_detach_not_a_destroy(monkeypatch):
    app = _app()
    await _connect(app, monkeypatch)
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    called = []
    monkeypatch.setattr(scheduler_ops, "_release_blocks_over_login", lambda *a, **k: called.append(1))
    res = await _teardown_endpoint(app)
    assert res.status == "down" and "nothing of ours" in res.notice
    assert called == []
    assert app.state.endpoint_id is None and app.shapes == {}  # our state fully cleared


async def test_login_shell_on_mep_explains_no_ssh():
    app = _app()
    res = await server._login_shell(app, "hostname")
    assert res.exit_code == 1 and "compute-only" in res.notice and "no SSH" in res.notice


# --- the SSH path is untouched by the capability plumbing --------------------------------------------


def test_default_facility_has_every_shape():
    app = AppCtx(facility=FakeFacility(), profile=Profile())
    assert server._supported_shapes(app) == ("login", "compute")
    assert server._shape_reject(app, "login") is None


# --- final-review fixes -------------------------------------------------------------------------------


def test_startup_pin_account_reaches_the_uec():
    # _catalog_facility demanded HPC_BRIDGE_ACCOUNT for an account-required MEP, then from_entry dropped
    # it — the UEC went out with no account and sbatch rejected the pilot. Now threaded through.
    fac = server._facility_from_entry(fake_mep_entry(account_required=True), account="proj123")
    assert fac.config_template(Profile())[1]["account"] == "proj123"
    assert "account" not in server._facility_from_entry(fake_mep_entry(), account="").config_template(Profile())[1]


async def test_offline_mep_reads_offline_not_allocating(monkeypatch):
    # with the manager OFFLINE, probe short-circuits before any canary; the generic "allocating nodes…"
    # told the agent to wait on a queue that doesn't exist (no login shape -> no #32 pilot query).
    app = _app(_mep(status="offline"))
    app.state = server.EndpointState(endpoint_id=MEP_UUID, reused=True)  # startup-pinned: no connect ran
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "provisioning"
    assert "OFFLINE" in res.notice and "facility" in res.notice and "allocating nodes" not in res.notice
    # manager online + block merely cold -> the normal wording
    app2 = _app()
    await _connect(app2, monkeypatch)
    app2.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="timeout"))
    res2 = await _ensure_endpoint_up(app2, shape="compute", confirm_spend=True)
    assert res2.status == "provisioning" and "allocating nodes" in res2.notice and "OFFLINE" not in res2.notice


async def test_stop_mep_refuses_while_a_task_runs_and_keeps_its_handle(monkeypatch):
    # no cancel channel on a MEP: a running task keeps the block BUSY (billing) to its ceiling. Draining
    # its handle would lose the result while claiming "idle-release in ~600s". Refuse instead.
    app = _app()
    await _connect(app, monkeypatch)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "", ""), pending=True)
    await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    out = await _run_shell(app, "sleep 3000", shape="compute")
    assert out.phase == "running" and out.task_id
    res = await _stop_endpoint(app)
    assert res.status == "up" and "can't stop" in res.notice and out.task_id in res.notice
    assert "poll_task" in res.notice and "no cancel channel" in res.notice.lower()
    assert out.task_id in app.tasks and "compute" in app.shapes  # handle + shape survive; result retrievable


_DOCUMENTED_422 = (
    "Request payload failed validation: Identity failed to map to a local user name.  (LookupError)\n"
    "   Globus effective identity: 3c085472-314d-4f22-abc6-591f2767af2b\n"
    "   Globus username: alice@example.edu"
)


def test_dispatch_error_text_keeps_the_api_message_not_the_url_preamble():
    from hpc_bridge.runner import dispatch_error_text

    class _ComputeAPIError(Exception):  # shape of globus_sdk's GlobusAPIError: .message / .code / long repr
        message = _DOCUMENTED_422
        code = "SEMANTICALLY_INVALID"
        http_status = 422

        def __str__(self):
            return ("('POST', 'https://compute.api.globus.org/v3/endpoints/da3df250-4013-4d69-942c-eef1568f860c/submit', "
                    f"'Bearer', 422, 'SEMANTICALLY_INVALID', {self.message!r})")

    text = dispatch_error_text(_ComputeAPIError())
    assert text.startswith("_ComputeAPIError[SEMANTICALLY_INVALID]: Request payload failed validation")
    assert "failed to map to a local user" in text and "alice@example.edu" in text
    assert dispatch_error_text(RuntimeError("Executor is shutdown")) == "RuntimeError: Executor is shutdown"
    assert len(dispatch_error_text(RuntimeError("x" * 5000))) <= 600


async def test_documented_422_is_terminal_and_names_the_identity_from_the_error(monkeypatch):
    from hpc_bridge.server import _identity_from_error
    assert _identity_from_error("ComputeAPIError[SEMANTICALLY_INVALID]: " + " ".join(_DOCUMENTED_422.split())) == "alice@example.edu"
    assert _identity_from_error("timeout") is None


async def test_documented_422_through_the_canary_is_terminal_down(monkeypatch):
    # the #32 provisioning-notice augmenter queries the scheduler over shape="login" — skipped on a MEP
    import time as _time

    app = _app()
    await _connect(app, monkeypatch)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error="ComputeAPIError[SEMANTICALLY_INVALID]: " + " ".join(_DOCUMENTED_422.split())))
    server._shape_runtime(app, "compute").provisioning_since = _time.monotonic() - (server.PROVISION_GRACE_S + 10)
    monkeypatch.setattr("hpc_bridge.login.globus_identity_label", lambda fetch=True: None)  # no lookup needed
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "down" and "NO ACCOUNT" in res.notice and "(alice@example.edu)" in res.notice


_LIVE_422 = ("ComputeAPIError[SEMANTICALLY_INVALID]: Request payload failed validation: Identity failed to map to a "
             "local user name. (LookupError) Globus effective identity: a4ef1d60-542a-49b3-a800-9a9b73a63b63 "
             "Globus username: ellermaugustus@gmail.com")
_LIVE_409 = ("ComputeAPIError[RESOURCE_CONFLICT]: Endpoint da3df250-4013-4d69-942c-eef1568f860c is already in use: "
             "possibly due to concurrent requests -- please try again")


async def test_no_account_verdict_is_sticky_and_stops_submitting(monkeypatch):
    # LIVE 2026-09-03: call #1 got the 422 -> terminal down; call #2 two seconds later re-submitted and got a
    # transient 409 RESOURCE_CONFLICT, flipping the verdict back to "allocating nodes…". The verdict must stick.
    app = _app()
    await _connect(app, monkeypatch)
    made, holder = [], {"err": _LIVE_422}

    def factory(eid, user_endpoint_config=None, **_kw):
        made.append(_FakeRunner(eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=holder["err"])))
        return made[-1]

    app.runner_factory = factory
    monkeypatch.setattr("hpc_bridge.login.globus_identity_label", lambda fetch=True: None)
    first = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert first.status == "down" and "(ellermaugustus@gmail.com)" in first.notice
    holder["err"] = _LIVE_409  # what a re-submit would have got
    second = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert second.status == "down" and "NO ACCOUNT" in second.notice and "allocating" not in second.notice
    assert len(made) == 1 and made[0].canaries == 1  # no re-submit, no runner rebuild
    # a cold run_shell is terminal too, without submitting
    out = await _run_shell(app, "hostname", shape="compute")
    assert out.phase == "failed" and "NO ACCOUNT" in out.notice and made[0].canaries == 1


def test_transient_conflict_hint_is_not_fix_your_config():
    from hpc_bridge.server import _dispatch_error_suffix, _transient_dispatch_failure
    assert _transient_dispatch_failure(_LIVE_409) and not _transient_dispatch_failure(_LIVE_422)
    s = _dispatch_error_suffix(CanaryResult(ok=False, error=_LIVE_409))
    assert "TRANSIENT" in s and "wait ~10 s" in s and "fix the config" not in s


async def test_new_login_forgets_the_no_account_verdict(monkeypatch):
    from hpc_bridge.server import _forget_identity_verdicts, _shape_runtime
    app = _app()
    await _connect(app, monkeypatch)
    rt = _shape_runtime(app, "compute")
    rt.no_account = _LIVE_422
    rt.last_canary = CanaryResult(ok=False, error=_LIVE_422)
    _forget_identity_verdicts(app)
    assert rt.no_account is None and rt.last_canary is None and rt.runner_stale is True


async def test_attach_notice_says_it_did_not_test_the_identity_mapping(monkeypatch):
    # walk finding: the agent inferred "no NO ACCOUNT came back, so your identity is mapped" from a clean attach
    app = _app()
    res = await _connect(app, monkeypatch)
    assert "does NOT test your identity mapping" in res.notice and "first block start" in res.notice


async def test_warm_billed_block_explains_a_zero_spend(monkeypatch):
    # walk finding: "session_spend: 0 so far" on a warm billed block read as a free tier
    app = _app()
    await _connect(app, monkeypatch)
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "up" and "no charge factor is configured" in res.notice and "not a free tier" in res.notice


async def test_persistent_resource_conflict_escalates_to_down(monkeypatch):
    # model sweep 2026-09-03: two runs under ONE identity → the second got RESOURCE_CONFLICT on every
    # submit for ~2 min and our "TRANSIENT — call again" hint had Sonnet retry 7×. Three in a row is terminal.
    app = _app()
    await _connect(app, monkeypatch)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=_LIVE_409))
    r1 = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    r2 = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert r1.status == "provisioning" and "TRANSIENT" in r1.notice and r2.status == "provisioning"
    r3 = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert r3.status == "down" and "NO LONGER transient" in r3.notice and "3 times in a row" in r3.notice
    assert "SAME Globus identity" in r3.notice


# ---- facility template contract (Anvil/Delta, 2026-09-04) ------------------------------------------------------

class _Meta:
    """A web client fake with the facility's published metadata."""

    def __init__(self, schema=None, version="4.12.0", name="Anvil Multi-User Globus Compute Endpoint"):
        self.schema, self.version, self.name = schema, version, name

    def get_endpoint_status(self, eid):
        return {"status": "online"}

    def get_endpoint_metadata(self, eid):
        return {"user_config_schema": self.schema, "endpoint_version": self.version, "display_name": self.name}


ANVIL_SCHEMA = {"type": "object", "additionalProperties": False,
                "properties": {k: {"type": "string"} for k in ("account", "partition", "qos", "walltime", "cores_per_node",
                                                                 "worker_init", "scheduler_options", "init_blocks",
                                                                 "max_blocks", "nodes_per_block")}}
DELTA_SCHEMA = {"type": "object", "additionalProperties": True,
                "properties": {"worker_init": {"type": "string"}, "endpoint_setup": {"type": "string"}}}


async def test_strict_schema_drops_keys_the_facility_would_reject():
    from hpc_bridge.shapes import shape_config

    fac = MEPFacility.from_entry(fake_mep_entry(account_required=True), client_factory=lambda: _Meta(ANVIL_SCHEMA))
    assert await fac.manager_online(fac.endpoint_id) is True
    assert fac.endpoint_version == "4.12.0" and fac.display_name.startswith("Anvil")
    uec = shape_config("compute", **fac.config_template(None)[1])
    assert "compute" in uec and "interface" in uec  # what the server would have sent…
    out = fac.sanitize_uec(uec)
    assert "interface" not in out and "max_workers_per_node" not in out  # …the facility's rejects are gone
    assert out.get("compute") is True and "compute" not in fac.dispatch_uec(out)  # internal marker kept, stripped on the wire
    assert out["partition"] and any("dropped" in n for n in fac.template_notes)


async def test_permissive_schema_keeps_everything():
    from hpc_bridge.shapes import shape_config

    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Meta(DELTA_SCHEMA, version="4.15.0"))
    await fac.load_template()
    uec = shape_config("compute", **fac.config_template(None)[1])
    assert fac.sanitize_uec(uec).keys() == uec.keys()
    assert not [n for n in fac.template_notes if "dropped" in n]


async def test_worker_init_tokens_resolve_to_the_facility_version_and_this_python():
    import sys

    entry = fake_mep_entry()
    entry.compute.env_setup = "V=$HOME/v-{gce_version}-py{python_version}; uv pip install -q 'globus-compute-endpoint=={gce_version}'"
    fac = MEPFacility.from_entry(entry, client_factory=lambda: _Meta(DELTA_SCHEMA, version="4.15.0"))
    await fac.load_template()
    wi = fac.sanitize_uec({"worker_init": fac.config_template(None)[1]["worker_init"]})["worker_init"]
    assert "globus-compute-endpoint==4.15.0" in wi and f"py{sys.version_info.major}.{sys.version_info.minor}" in wi
    assert "{" not in wi


async def test_unknown_facility_version_falls_back_to_the_facility_default_worker_init():
    class _NoMeta(_Meta):
        def get_endpoint_metadata(self, eid):
            raise RuntimeError("403")

    entry = fake_mep_entry()
    entry.compute.env_setup = "uv pip install 'globus-compute-endpoint=={gce_version}'"
    fac = MEPFacility.from_entry(entry, client_factory=_NoMeta)
    await fac.load_template()
    assert "worker_init" not in fac.sanitize_uec({"worker_init": entry.compute.env_setup, "partition": "p"})
    assert any("not readable" in n for n in fac.template_notes) and any("default worker_init" in n for n in fac.template_notes)


def test_entry_extra_keys_pass_through_and_empty_env_setup_means_no_worker_init():
    entry = fake_mep_entry()
    entry.defaults.extra = {"qos": "normal", "cores_per_node": 1, "exclusive": False}
    entry.compute.env_setup = ""
    fac = MEPFacility.from_entry(entry)
    opts = fac.config_template(None)[1]
    assert opts["qos"] == "normal" and opts["cores_per_node"] == 1 and opts["exclusive"] is False
    assert "worker_init" not in opts  # the facility template's default runs


def test_new_seeds_validate_and_are_mep_entries():
    import yaml

    from hpc_bridge.catalog.entry import CatalogEntry

    for f in ("ncsa-delta.yaml", "anvil.yaml"):
        with open(f"src/hpc_bridge/catalog/seed/{f}") as fh:
            docs = yaml.safe_load(fh)
        for doc in docs:
            e = CatalogEntry.model_validate(doc)
            if e.compute_mep_uuid:
                assert e.ssh_host is None and e.account_required is True
                assert "{gce_version}" in e.compute.env_setup and "{python_version}" in e.compute.env_setup


async def test_worker_version_client_pins_this_sdk_not_the_manager():
    from importlib.metadata import version

    entry = fake_mep_entry()
    entry.compute.env_setup = "uv pip install 'globus-compute-endpoint=={gce_version}'"
    entry.compute.worker_version = "client"
    fac = MEPFacility.from_entry(entry, client_factory=lambda: _Meta(ANVIL_SCHEMA, version="4.12.0"))
    await fac.load_template()
    wi = fac.sanitize_uec({"worker_init": entry.compute.env_setup})["worker_init"]
    assert f"globus-compute-endpoint=={version('globus-compute-sdk')}" in wi and "4.12.0" not in wi


async def test_worker_version_explicit_and_manager():
    entry = fake_mep_entry()
    entry.compute.env_setup = "pin={gce_version}"
    fac = MEPFacility.from_entry(entry, client_factory=lambda: _Meta(DELTA_SCHEMA, version="4.15.0"))
    await fac.load_template()
    assert fac.sanitize_uec({"worker_init": "pin={gce_version}"})["worker_init"] == "pin=4.15.0"  # manager (default)
    fac.worker_version = "4.9.9"
    assert fac.sanitize_uec({"worker_init": "pin={gce_version}"})["worker_init"] == "pin=4.9.9"  # explicit


def test_anvil_seed_pins_the_client_version():
    import yaml

    from hpc_bridge.catalog.entry import CatalogEntry

    with open("src/hpc_bridge/catalog/seed/anvil.yaml") as fh:
        docs = yaml.safe_load(fh)
    e = next(CatalogEntry.model_validate(d) for d in docs if d["id"] == "anvil")  # the MEP is THE anvil entry now
    assert e.compute.worker_version == "client"


async def test_strict_schema_keeps_the_internal_compute_marker_until_dispatch():
    from hpc_bridge.shapes import shape_config

    fac = MEPFacility.from_entry(fake_mep_entry(account_required=True), client_factory=lambda: _Meta(ANVIL_SCHEMA))
    await fac.load_template()
    runtime = fac.sanitize_uec(shape_config("compute", **fac.config_template(None)[1]))
    assert runtime.get("compute") is True  # the server's _apply_account/_apply_partition key on it
    runtime["account"] = "cis250223"       # what _apply_account does after the user confirms
    wire = fac.dispatch_uec(runtime)
    assert "compute" not in wire and wire["account"] == "cis250223" and wire["partition"]  # Anvil accepts this


async def test_permissive_schema_dispatches_the_runtime_dict_unchanged():
    fac = MEPFacility.from_entry(fake_mep_entry(), client_factory=lambda: _Meta(DELTA_SCHEMA))
    await fac.load_template()
    d = {"compute": True, "partition": "p", "account": "a"}
    assert fac.dispatch_uec(d) == d


async def test_account_is_applied_on_a_strict_schema_mep(monkeypatch):
    """The live miss: with `compute` filtered out of the runtime dict, ensure_endpoint_up(account=…) never
    applied the account and Anvil charged the default association."""
    from hpc_bridge import warmth
    from hpc_bridge.context import AppCtx, EndpointState
    from hpc_bridge.profile import Profile

    fac = MEPFacility.from_entry(fake_mep_entry(account_required=True), client_factory=lambda: _Meta(ANVIL_SCHEMA))
    await fac.load_template()
    app = AppCtx(facility=fac, profile=Profile(), state=EndpointState(endpoint_id=fac.endpoint_id))
    rt = warmth._shape_runtime(app, "compute")
    assert rt.user_endpoint_config.get("compute") is True
    assert warmth._apply_account(app, "compute", rt, "cis250223-gpu") is None
    assert rt.user_endpoint_config["account"] == "cis250223-gpu"
    assert "compute" not in fac.dispatch_uec(rt.user_endpoint_config)



# --- the account floor (0.1.17): an account-required facility starts NOTHING without an account -----------------


def _acct_app(account_required=True):
    fac = MEPFacility.from_entry(fake_mep_entry(account_required=account_required), client_factory=_Status)
    return _app(fac)


async def test_account_required_confirm_without_account_starts_nothing(monkeypatch):
    """Live 2026-09-09 (NCSA Delta): the agent confirmed spend with no account after the user offered a LOGIN NAME;
    the MEP submitted a block Slurm could only reject. Now: needs_account, nothing built, spend still unconfirmed."""
    app = _acct_app()
    await _connect(app, monkeypatch, entry=fake_mep_entry(account_required=True))
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "needs_account" and res.block_state == "cold"
    assert app.built == []                                   # no runner, no submit
    assert "LOGIN NAME is not an account" in res.notice and "ensure_endpoint_up(account=" in res.notice
    assert res.account is None
    # spend stays unconfirmed: the same call with an account re-gates cleanly and proceeds
    res2 = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True, account="bbxx-delta-gpu")
    assert res2.status == "up" and res2.account == "bbxx-delta-gpu"
    assert app.built and app.built[-1][1].get("account") == "bbxx-delta-gpu"


async def test_account_required_sticky_account_then_confirm_proceeds(monkeypatch):
    app = _acct_app()
    await _connect(app, monkeypatch, entry=fake_mep_entry(account_required=True))
    first = await _ensure_endpoint_up(app, shape="compute", account="proj-1")   # sets the account, no confirm yet
    assert first.status == "needs_confirmation"
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)    # the sticky account satisfies the floor
    assert res.status == "up" and app.built[-1][1].get("account") == "proj-1"


async def test_account_required_run_shell_cold_is_needs_account_not_a_submit(monkeypatch):
    app = _acct_app()
    await _connect(app, monkeypatch, entry=fake_mep_entry(account_required=True))
    # the implicit provision inside run_shell: spend floor first (no ack yet) …
    out = await _run_shell(app, "hostname", shape="compute")
    assert out.phase == "needs_confirmation" and app.built == []
    # … then, with spend acknowledged but no account, the account floor — still nothing submitted
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "needs_account"
    out = await _run_shell(app, "hostname", shape="compute")
    assert out.phase in ("needs_confirmation", "needs_account") and app.built == []


async def test_account_not_required_is_unchanged(monkeypatch):
    app = _acct_app(account_required=False)
    await _connect(app, monkeypatch)
    res = await _ensure_endpoint_up(app, shape="compute", confirm_spend=True)
    assert res.status == "up" and "account" not in app.built[-1][1]


def test_compute_only_wording_names_the_channel_not_the_facility():
    """'no login node' was wrong (the facility has them); the notices now say the CHANNEL has no login shape."""
    from hpc_bridge.notices import _spend_floor_guidance
    from tests.fakes import fake_mep_entry as _e
    app = _acct_app()
    text = _spend_floor_guidance(app)
    assert "login nodes are outside this channel" in text and "no login node" not in text
    note = _e(account_required=True).summary().access_note
    assert "no login shape through this channel" in note and "no login node" not in note


# --- a scheduler-REJECTED submission is a terminal `down`, not "allocating nodes…" (0.1.17) -----------------------

_PARSL_SUBMIT_FAILED = (
    "TaskExecutionFailed: ++++++++ Traceback (most recent call last): parsl.executors.errors.BadStateException: Executor "
    "GlobusComputeEngine-HighThroughputExecutor failed due to: Error 1: Failed to start block 0: Cannot launch job "
    "parsl.GlobusComputeEngine-HighThroughputExecutor.block-0.1788982817.7919347: Could not read job ID from submit "
    "command standard output; recode=1, stdout=, stderr=sbatch: error: invalid account specified"
)


async def test_rejected_submission_is_a_terminal_down_naming_the_cause(monkeypatch):
    """Live on the fake MEP (2026-09-09): the login name passed as the account; sbatch refused; the client said
    'allocating nodes…' for five polls with the cause buried in a suffix. Now: down + REJECTED, at once."""
    import time as _time

    app = _acct_app()
    await _connect(app, monkeypatch, entry=fake_mep_entry(account_required=True))
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(
        eid, _Res(0, "", ""), canary_result=CanaryResult(ok=False, error=_PARSL_SUBMIT_FAILED))
    server._shape_runtime(app, "compute").provisioning_since = _time.monotonic() - 5
    res = await _ensure_endpoint_up(app, shape="compute", partition="compute", account="hpcbmep", confirm_spend=True)
    assert res.status == "down" and res.block_state == "cold"
    assert "REJECTED" in res.notice and "'hpcbmep'" in res.notice and "'compute'" in res.notice
    assert "Could not read job ID" in res.notice and "Traceback" not in res.notice
    assert "allocating" not in res.notice and "Do not retry unchanged" in res.notice
    # the run_shell cold path says the same thing
    out = await _run_shell(app, "hostname", shape="compute")
    assert out.phase == "failed" and "REJECTED" in out.notice
    # a corrected account re-provisions (a fresh runner whose canary answers)
    app.runner_factory = lambda eid, user_endpoint_config=None, **_kw: _FakeRunner(eid, _Res(0, "c1\n", ""))
    res2 = await _ensure_endpoint_up(app, shape="compute", account="hpcb", confirm_spend=True)
    assert res2.status == "up" and res2.account == "hpcb"


def test_submit_rejection_classifier_and_cause():
    from hpc_bridge.notices import _submit_rejected, _submit_rejection_cause
    assert _submit_rejected(_PARSL_SUBMIT_FAILED)
    assert _submit_rejected("sbatch: error: Batch job submission failed: Invalid qos specification")
    assert not _submit_rejected("timeout") and not _submit_rejected(None)
    assert not _submit_rejected("Identity failed to map to a local user name")   # that is the no-account class
    cause = _submit_rejection_cause(_PARSL_SUBMIT_FAILED)
    assert cause.startswith("Error 1: Failed to start block 0") and "Traceback" not in cause and len(cause) <= 220

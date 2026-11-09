"""Tests for the truenas spoke: command dispatch, sensitive-data masking,
get_version, UPDATE_CONFIG fleet storage + shared_tenant_id, the poll
partial-on-failure contract, and the write-method JSON-RPC calls.

Self-contained: inserts src/ on sys.path + stubs core.src.base_spoke so the
spoke imports without the lm repo present (same pattern as test_nw_spoke.py).
The TrueNASClient is replaced with a recording fake so the tests assert which
JSON-RPC methods the engine emits — no real WS IO.

Loop-safety: every test runs a fresh event loop and restores a current open
loop on teardown (the Py3.9 ``asyncio.run()`` poisoning we hit on the Mist
tests — an ``asyncio.Lock()`` later raises "no current event loop" if a prior
``run_until_complete`` closed + nulled the loop).
"""
import os
import sys
import types
import asyncio
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Stub core.src.base_spoke so the spoke imports without the lm repo present.
_core = types.ModuleType("core")
_core_src = types.ModuleType("core.src")
_core_base = types.ModuleType("core.src.base_spoke")


class _BaseSpoke:
    def __init__(self, spoke_id, config):
        self.spoke_id = spoke_id
        self.config = config

    async def handle_command(self, command_type, data):
        raise NotImplementedError

    async def get_status(self):
        raise NotImplementedError


_core_base.BaseSpoke = _BaseSpoke
sys.modules["core"] = _core
sys.modules["core.src"] = _core_src
sys.modules["core.src.base_spoke"] = _core_base


class _FakeClient:
    """Recording fake for TrueNASClient. Each method returns a canned SUCCESS
    envelope unless ``failures`` maps the method to an exception/message. All
    calls are appended to ``calls`` for assertion."""

    def __init__(self, cred, *, canned=None, failures=None):
        self.host = (cred or {}).get("host", "")
        self.calls = []
        self.canned = canned or {}
        self.failures = failures or {}
        self.api_key = (cred or {}).get("api_key", "")

    # Default data per method (lists for the query methods, dict for info) so a
    # poll that isn't canned for every method still gets sane empties (not None,
    # which would break the engine's len()).
    _DEFAULTS = {
        "system.info": {}, "pool.query": [], "pool.dataset.query": [],
        "disk.query": [], "sharing.smb.query": [], "sharing.nfs.query": [],
        "sharing.iscsi.query": [], "service.query": [], "alert.list": [],
        "reporting.realtime": {},
    }

    def _record(self, method, payload=None):
        # Record (method, payload) — payload is the meaningful input the engine
        # asked for (pool+name, dataset path, pool id, etc.), not the raw
        # JSON-RPC arg list, so assertions are stable.
        self.calls.append((method, payload))
        fail = self.failures.get(method)
        if fail is not None:
            return {"status": "ERROR", "message": fail, "data": None}
        data = self.canned.get(method, self._DEFAULTS.get(method))
        return {"status": "SUCCESS", "data": data, "message": method}

    async def system_info(self):
        return self._record("system.info")

    async def pools(self):
        return self._record("pool.query")

    async def datasets(self):
        return self._record("pool.dataset.query")

    async def disks(self):
        return self._record("disk.query")

    async def shares(self, kind="smb"):
        return self._record(f"sharing.{kind}.query")

    async def services(self):
        return self._record("service.query")

    async def alerts(self):
        return self._record("alert.list")

    async def capacity(self):
        return self._record("pool.dataset.query")

    async def performance(self):
        return self._record("reporting.realtime")

    async def create_dataset(self, pool, name, options=None):
        return self._record("pool.dataset.create", {"pool": pool, "name": name,
                                                    "options": options or {}})

    async def delete_dataset(self, dataset_id, options=None):
        return self._record("pool.dataset.delete", {"dataset": dataset_id,
                                                    "options": options or {}})

    async def create_share(self, kind, dataset, options=None):
        return self._record(f"sharing.{kind}.create", {"path": dataset,
                                                      "options": options or {}})

    async def create_snapshot(self, dataset, name="", options=None):
        return self._record("zfs.snapshot.create", {"dataset": dataset, "name": name,
                                                    "options": options or {}})

    async def run_scrub(self, pool_id):
        return self._record("pool.scrub.start", {"pool_id": pool_id})

    async def close(self):
        self.calls.append(("close", None))


def _make_spoke(appliances, monkeypatch, canned=None, failures=None):
    """Build a TruenasSpoke whose engine builds _FakeClient instances."""
    import truenas_engine as _te
    import truenas_spoke as _ts

    # The engine lazy-imports TrueNASClient inside _client_for, so patch
    # _client_for to build a recording fake directly (no real WS IO).
    def _patched_client_for(self, appliance_id, tenant=None):
        a = self._get_appliance(appliance_id, tenant)
        if not a:
            return None, None
        client = self._clients.get(appliance_id)
        if client is None:
            cred = self._resolve_credential(a, tenant)
            if cred is None:
                return a, None
            client = _FakeClient(cred, canned=canned, failures=failures)
            self._clients[appliance_id] = client
        return a, client

    monkeypatch.setattr(_te.TruenasEngine, "_client_for", _patched_client_for)
    return _ts.TruenasSpoke("truenas-spoke-1", {"appliances": appliances})


# ── loop-safety autouse fixture (Py3.9 asyncio.run poisoning) ────────────────
import pytest


@pytest.fixture(autouse=True)
def _restore_loop():
    """Leave a current open event loop after each test so a later
    ``asyncio.Lock()`` (in an unrelated module) doesn't raise "no current
    event loop" — the asyncio.run() poisoning hit on the Mist tests."""
    yield
    try:
        asyncio.set_event_loop(asyncio.new_event_loop())
    except Exception:
        pass


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


_VERSION_FILE = os.path.join(os.path.dirname(__file__), "..", "VERSION")
with open(_VERSION_FILE) as _f:
    _TRUENAS_VERSION = _f.read().strip()

logging.disable(logging.CRITICAL)  # silence stub INFO logs during tests


# ── get_version reads repo-root VERSION ──────────────────────────────────────
def test_get_version_reads_repo_root_version():
    from truenas_spoke import TruenasSpoke
    spoke = TruenasSpoke("truenas-1", {"appliances": []})
    assert spoke.get_version() == _TRUENAS_VERSION


# ── UPDATE_CONFIG stores the fleet + shared_tenant_id (creds masked in logs) ─
def test_update_config_stores_appliances():
    from truenas_spoke import TruenasSpoke
    spoke = TruenasSpoke("truenas-1", {"appliances": []})
    res = _run(spoke.handle_command("UPDATE_CONFIG", {
        "appliances": [
            {"id": "a1", "name": "nas1", "host": "10.0.0.5",
             "api_key": "supersecret", "tenant_id": "acme"},
        ],
        "shared_tenant_id": "shared",
    }))
    assert res["status"] == "SUCCESS"
    assert res["appliance_count"] == 1
    assert spoke.engine.appliances[0]["id"] == "a1"
    assert spoke.engine.shared_tenant_id == "shared"


# ── Sensitive-data masking ──────────────────────────────────────────────────
def test_mask_redacts_sensitive_fields():
    from truenas_spoke import TruenasSpoke, _SENSITIVE
    masked = TruenasSpoke._mask({"appliance_id": "a1", "api_key": "hunter2",
                                 "name": "nas1"})
    assert masked["api_key"] == "********"
    assert masked["name"] == "nas1"
    assert "api_key" in _SENSITIVE


# ── inline credential (host+api_key) resolves without the creds store ────────
def test_inline_credential_resolves(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "name": "nas1", "host": "10.0.0.5", "api_key": "k",
         "tenant_id": "acme"}], monkeypatch)
    res = _run(spoke.handle_command("TRUENAS_GET_POOLS", {"appliance_id": "a1"}))
    assert res["status"] == "SUCCESS"
    client = spoke.engine._clients["a1"]
    assert client.api_key == "k"
    assert ("pool.query", (), {}) in client.calls or \
           client.calls[0][0] == "pool.query"


# ── read commands emit the right JSON-RPC method names ───────────────────────
def test_read_commands_emit_methods(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"}],
        monkeypatch)
    cmds = {
        "TRUENAS_PROBE": "system.info",
        "TRUENAS_GET_POOLS": "pool.query",
        "TRUENAS_GET_DATASETS": "pool.dataset.query",
        "TRUENAS_GET_DISKS": "disk.query",
        "TRUENAS_GET_ALERTS": "alert.list",
        "TRUENAS_GET_SERVICES": "service.query",
        "TRUENAS_GET_CAPACITY": "pool.dataset.query",
        "TRUENAS_GET_PERFORMANCE": "reporting.realtime",
    }
    for cmd, method in cmds.items():
        res = _run(spoke.handle_command(cmd, {"appliance_id": "a1"}))
        assert res["status"] in ("SUCCESS", "PARTIAL"), f"{cmd} -> {res}"
        client = spoke.engine._clients["a1"]
        assert client.calls, f"{cmd} recorded no call"
        assert client.calls[-1][0] == method, f"{cmd} -> {client.calls[-1][0]}"
        client.calls.clear()


def test_fleet_listing_surfaces_hardware_serial(monkeypatch):
    info = {"system_serial": "2M292101K3", "system_product": "ProLiant DL380",
            "version": "TrueNAS-13", "hostname": "x", "uptime": 5}
    spoke = _make_spoke([{"id": "a1", "name": "TNAS", "host": "10.0.0.5", "api_key": "k"}],
                        monkeypatch, canned={"system.info": info})
    res = _run(spoke.handle_command("TRUENAS_LIST_APPLIANCES", {}))
    row = res["data"][0]
    assert row["info"] == {"system_serial": "2M292101K3", "system_product": "ProLiant DL380",
                           "version": "TrueNAS-13"}


def test_get_shares_passes_kind(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"}],
        monkeypatch)
    for kind in ("smb", "nfs"):
        _run(spoke.handle_command("TRUENAS_GET_SHARES",
              {"appliance_id": "a1", "kind": kind}))
        client = spoke.engine._clients["a1"]
        assert client.calls[-1][0] == f"sharing.{kind}.query"
        client.calls.clear()


# ── write commands emit the right JSON-RPC method + payload ─────────────────
def test_create_dataset_call(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"}],
        monkeypatch)
    _run(spoke.handle_command("TRUENAS_CREATE_DATASET",
          {"appliance_id": "a1", "pool": "tank", "name": "test"}))
    client = spoke.engine._clients["a1"]
    method, payload = client.calls[-1]
    assert method == "pool.dataset.create"
    assert payload["pool"] == "tank"
    assert payload["name"] == "test"


def test_create_snapshot_call(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"}],
        monkeypatch)
    _run(spoke.handle_command("TRUENAS_CREATE_SNAPSHOT",
          {"appliance_id": "a1", "dataset": "tank/test", "name": "snap1"}))
    client = spoke.engine._clients["a1"]
    method, payload = client.calls[-1]
    assert method == "zfs.snapshot.create"
    assert payload["dataset"] == "tank/test"
    assert payload["name"] == "snap1"


def test_run_scrub_call(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"}],
        monkeypatch)
    _run(spoke.handle_command("TRUENAS_RUN_SCRUB",
          {"appliance_id": "a1", "pool_id": "1"}))
    client = spoke.engine._clients["a1"]
    method, payload = client.calls[-1]
    assert method == "pool.scrub.start"
    assert payload["pool_id"] == "1"


def test_create_share_smb_call(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"}],
        monkeypatch)
    _run(spoke.handle_command("TRUENAS_CREATE_SHARE",
          {"appliance_id": "a1", "kind": "smb", "dataset": "/mnt/tank/test"}))
    client = spoke.engine._clients["a1"]
    method, payload = client.calls[-1]
    assert method == "sharing.smb.create"
    assert payload["path"] == "/mnt/tank/test"


# ── poll: partial-on-failure (one failed datum → PARTIAL, rest still present) ─
def test_poll_partial_on_failure(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"}],
        monkeypatch, canned={"pool.query": [{"name": "tank"}]},
        failures={"alert.list": "alert subsystem offline"})
    res = _run(spoke.handle_command("TRUENAS_POLL", {"appliance_id": "a1"}))
    assert res["status"] == "PARTIAL"
    assert any("alert" in e for e in res["errors"])
    data = res["data"]
    assert isinstance(data["pools"], list) and data["pools"][0]["name"] == "tank"
    assert data["alerts"] == []   # failed datum → empty


def test_poll_success_when_all_ok(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"}],
        monkeypatch, canned={"pool.query": [{"name": "tank"}],
                             "alert.list": [{"id": 1}]})
    res = _run(spoke.handle_command("TRUENAS_POLL", {"appliance_id": "a1"}))
    assert res["status"] == "SUCCESS"
    assert res["errors"] == []
    assert res["data"]["pools"][0]["name"] == "tank"


# ── per-appliance unknown / no-credential → ERROR ─────────────────────────────
def test_unknown_appliance_errors():
    from truenas_spoke import TruenasSpoke
    spoke = TruenasSpoke("truenas-1", {"appliances": []})
    res = _run(spoke.handle_command("TRUENAS_GET_POOLS", {"appliance_id": "nope"}))
    assert res["status"] == "ERROR"
    assert "not found" in res["message"]


def test_appliance_without_credential_errors(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "tenant_id": "acme"}],  # no api_key, no cred_name
        monkeypatch)
    res = _run(spoke.handle_command("TRUENAS_GET_POOLS", {"appliance_id": "a1"}))
    assert res["status"] == "ERROR"
    assert "credential" in res["message"]


# ── tenant scoping ───────────────────────────────────────────────────────────
def test_tenant_filter_excludes_other_tenant():
    from truenas_engine import TruenasEngine
    eng = TruenasEngine([
        {"id": "acme-nas", "host": "10.0.0.5", "tenant_id": "acme"},
        {"id": "other-nas", "host": "10.0.0.6", "tenant_id": "othercorp"},
        {"id": "shared-nas", "host": "10.0.0.7", "tenant_id": "shared"},
    ])
    eng.shared_tenant_id = "shared"
    assert eng._get_appliance("acme-nas", "acme") is not None
    assert eng._get_appliance("other-nas", "acme") is None
    assert eng._get_appliance("shared-nas", "acme") is not None  # shared visible to all
    assert eng._get_appliance("acme-nas", None) is not None      # no filter = whole fleet


# ── get_status / GET_VERSION / unknown ────────────────────────────────────────
def test_get_status():
    from truenas_spoke import TruenasSpoke
    spoke = TruenasSpoke("truenas-1",
                         {"appliances": [{"id": "a1", "host": "10.0.0.5"}]})
    st = _run(spoke.get_status())
    assert st["module"] == "truenas"
    assert st["appliance_count"] == 1
    assert st["connection"] == "CONNECTED"


def test_get_version_command():
    from truenas_spoke import TruenasSpoke
    spoke = TruenasSpoke("truenas-1", {"appliances": []})
    res = _run(spoke.handle_command("get_version", {}))
    assert res == {"status": "SUCCESS", "version": _TRUENAS_VERSION}


def test_unknown_command_errors():
    from truenas_spoke import TruenasSpoke
    spoke = TruenasSpoke("truenas-1", {"appliances": []})
    res = _run(spoke.handle_command("TRUENAS_DOES_NOT_EXIST", {}))
    assert res["status"] == "ERROR"
    assert "not supported" in res["message"]


# ── credentials store (per-tenant API keys) ──────────────────────────────────
def test_credentials_upsert_list_delete(tmp_path, monkeypatch):
    import credentials
    monkeypatch.setattr(credentials, "_DIR", str(tmp_path))
    credentials.upsert("acme", "nas1", host="10.0.0.5", api_key="k1",
                       verify_ssl=False, auth_mechanism="PLAIN")
    # sentinel-merge: omit the key on update → kept.
    credentials.upsert("acme", "nas1", host="10.0.0.5", api_key="", verify_ssl=True)
    pub = credentials.list_public("acme")
    assert len(pub) == 1
    assert pub[0]["name"] == "nas1"
    assert pub[0]["host"] == "10.0.0.5"
    assert pub[0]["secrets_set"]["api_key"] is True
    assert "api_key" not in pub[0]  # secret withheld from list_public
    mat = credentials.materialize("acme", "nas1")
    assert mat["api_key"] == "k1"          # sentinel-merged
    assert mat["verify_ssl"] is True
    assert mat["auth_mechanism"] == "PLAIN"
    assert credentials.delete("acme", "nas1") is True
    assert credentials.list_public("acme") == []


def test_credentials_upsert_requires_key_first_time(tmp_path, monkeypatch):
    import credentials
    monkeypatch.setattr(credentials, "_DIR", str(tmp_path))
    import pytest as _pytest
    with _pytest.raises(ValueError):
        credentials.upsert("acme", "nas2", host="10.0.0.5", api_key="")
    with _pytest.raises(ValueError):
        credentials.upsert("acme", "nas2", host="", api_key="k")

def test_build_uri_adds_the_api_path_and_keeps_explicit_ones():
    from truenas_client import _build_uri
    assert _build_uri("172.21.0.5") == "wss://172.21.0.5/api/current"
    assert _build_uri("nas.example:8443") == "wss://nas.example:8443/api/current"
    assert _build_uri("127.0.0.1") == "ws://127.0.0.1/api/current"
    assert _build_uri("wss://nas/api/v25.04.0") == "wss://nas/api/v25.04.0"
    assert _build_uri("") == ""


# ── role-hosted auto-poll pushes through the control-plane back-ref ──────────
def test_poll_and_push_uses_control_plane(monkeypatch):
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"}],
        monkeypatch, canned={"pool.query": [{"name": "tank", "scan": {"end_time": __import__("datetime").datetime(2026, 1, 1)}}]})
    sent = []

    class _CP:
        _hub_ws = object()

        async def send_to_hub(self, msg_type, data):
            sent.append((msg_type, data))

    spoke.control_plane = _CP()
    _run(spoke._truenas_poll_and_push("a1"))
    assert sent and sent[0][0] == "TRUENAS_POLL_RESULT"
    assert sent[0][1]["appliance_id"] == "a1"
    assert sent[0][1]["data"]["pools"][0]["name"] == "tank"
    # TrueNAS datetimes are made JSON-safe before the push.
    assert sent[0][1]["data"]["pools"][0]["scan"]["end_time"] == "2026-01-01 00:00:00"


def test_start_background_loops_is_idempotent(monkeypatch):
    spoke = _make_spoke([], monkeypatch)

    async def _go():
        spoke.start_background_loops()
        first = list(spoke._bg_tasks)
        spoke.start_background_loops()
        assert spoke._bg_tasks == first and len(first) == 2
        for t in first:
            t.cancel()
    _run(_go())


# ── API-key login: owner username + SCRAM→PLAIN fallback on "auto" ───────────
def _stub_auth_mech(monkeypatch):
    import enum
    mod = types.ModuleType("truenas_api_client.auth_api_key")

    class APIKeyAuthMech(str, enum.Enum):
        SCRAM = "SCRAM"
        PLAIN = "PLAIN"
    mod.APIKeyAuthMech = APIKeyAuthMech
    monkeypatch.setitem(sys.modules, "truenas_api_client", types.ModuleType("truenas_api_client"))
    monkeypatch.setitem(sys.modules, "truenas_api_client.auth_api_key", mod)


class _LoginClient:
    def __init__(self, scram_ok):
        self.scram_ok = scram_ok
        self.logins = []

    def login_with_api_key(self, username, api_key, mech):
        self.logins.append((username, api_key, mech.value))
        if mech.value == "SCRAM" and not self.scram_ok:
            raise ValueError("SCRAM not supported")
        if username == "truenas_admin" and mech.value == "PLAIN" and not self.scram_ok:
            raise ValueError("Invalid API key")

    def call(self, method, *args):
        self.logins.append((method, *args))
        return True


def test_auto_login_falls_back_to_plain_with_username(monkeypatch):
    import truenas_client
    _stub_auth_mech(monkeypatch)
    cl = truenas_client.TrueNASClient({"host": "h", "api_key": "k", "username": "svc"})
    fake = _LoginClient(scram_ok=False)
    cl._login(fake)
    assert fake.logins == [("svc", "k", "SCRAM"), ("svc", "k", "PLAIN")]


def test_plain_login_defaults_username(monkeypatch):
    import truenas_client
    _stub_auth_mech(monkeypatch)
    cl = truenas_client.TrueNASClient({"host": "h", "api_key": "k", "auth_mechanism": "PLAIN"})
    fake = _LoginClient(scram_ok=True)
    cl._login(fake)
    assert fake.logins == [("truenas_admin", "k", "PLAIN")]


def test_auto_login_without_user_falls_back_to_key_only(monkeypatch):
    import truenas_client
    _stub_auth_mech(monkeypatch)
    cl = truenas_client.TrueNASClient({"host": "h", "api_key": "k"})
    fake = _LoginClient(scram_ok=False)
    cl._login(fake)
    assert fake.logins[-1] == ("auth.login_with_api_key", "k")


def test_mask_hides_nested_appliance_api_key():
    import truenas_spoke
    out = truenas_spoke.TruenasSpoke._mask({"appliances": [{"host": "h", "api_key": "secret"}]})
    assert out == {"appliances": [{"host": "h", "api_key": "********"}]}


def test_call_reconnects_after_dropped_socket():
    import errno
    import asyncio
    import truenas_client

    class Dead:
        def call(self, method, *a, **k):
            raise truenas_client_exc("Unexpected closure of remote connection", errno.ECONNABORTED)

    class Live:
        def call(self, method, *a, **k):
            return [{"name": "tank"}]

    class truenas_client_exc(Exception):
        def __init__(self, msg, no):
            super().__init__(msg)
            self.errno = no

    cl = truenas_client.TrueNASClient({"host": "h", "api_key": "k"})
    cl._client, cl._connected = Dead(), True

    def reconnect():
        cl._client, cl._connected = Live(), True
    cl._connect_sync = reconnect
    env = asyncio.run(cl._call("pool.query"))
    assert env["status"] == "SUCCESS" and env["data"] == [{"name": "tank"}]


def test_capacity_reads_root_datasets():
    import asyncio
    import truenas_client

    class C:
        def call(self, method, *a, **k):
            return [{"name": "tank", "used": {"parsed": 25, "value": "25B"}, "available": {"parsed": 75}},
                    {"name": "tank/child", "used": {"parsed": 5}, "available": {"parsed": 75}}]

    cl = truenas_client.TrueNASClient({"host": "h", "api_key": "k"})
    cl._client, cl._connected = C(), True
    env = asyncio.run(cl.capacity())
    assert env["data"] == [{"pool": "tank", "used": 25.0, "avail": 75.0, "pct_used": 25.0}]


def test_performance_takes_one_realtime_sample():
    import asyncio
    import truenas_client

    class C:
        unsubscribed = None

        def subscribe(self, name, cb):
            assert name == "reporting.realtime"
            cb("ADDED", fields={
                "disks": {"read_ops": 10, "write_ops": 4, "read_bytes": 2048,
                          "write_bytes": 1024, "busy": 3.5},
                "interfaces": {"eno1": {"link_state": "LINK_STATE_UP", "speed": 10000,
                                        "received_bytes_rate": 500, "sent_bytes_rate": 250}},
                "cpu": {"cpu": {"usage": 12.5}}})
            return "sub1"

        def unsubscribe(self, sid):
            C.unsubscribed = sid

    cl = truenas_client.TrueNASClient({"host": "h", "api_key": "k"})
    cl._client, cl._connected = C(), True
    env = asyncio.run(cl.performance())
    assert env["status"] == "SUCCESS"
    assert env["data"]["disks"]["read_ops"] == 10
    assert env["data"]["interfaces"][0]["name"] == "eno1"
    assert env["data"]["cpu_usage"] == 12.5
    assert C.unsubscribed == "sub1"


def test_performance_cpu_falls_back_to_core_average():
    import asyncio
    import truenas_client

    class C:
        def subscribe(self, name, cb):
            cb("ADDED", fields={"cpu": {"cpu": {"usage": 0}, "cpu0": {"usage": 10},
                                        "cpu1": {"usage": 30}}})
            return "s"

        def unsubscribe(self, sid):
            pass

    cl = truenas_client.TrueNASClient({"host": "h", "api_key": "k"})
    cl._client, cl._connected = C(), True
    assert asyncio.run(cl.performance())["data"]["cpu_usage"] == 20.0


# ── 5-minute performance averages ────────────────────────────────────────────
def test_perf_averager_closes_five_minute_buckets():
    from perf_averager import PerfAverager
    avg = PerfAverager()
    avg.add("a1", {"write_ops": 100.0, "cpu": 10.0}, 600)
    avg.add("a1", {"write_ops": 300.0}, 870)
    assert avg.close_finished(899) == []          # bucket 600-899 still open
    assert avg.close_finished(900) == [{"device_id": "a1", "ts": 600, "samples": 2,
                                        "metrics": {"write_ops": 200.0, "cpu": 10.0}}]
    assert avg.close_finished(900) == []


def test_perf_store_keeps_seven_days_and_unsent_rows_across_restart(tmp_path):
    from perf_store import PerfStore
    path = str(tmp_path / "perf.json")
    now = 10 * 86400
    store = PerfStore(path)
    store.add([{"device_id": "a1", "ts": now - 8 * 86400, "samples": 1, "metrics": {"x": 1}},
               {"device_id": "a1", "ts": now - 600, "samples": 10, "metrics": {"x": 2}},
               {"device_id": "a1", "ts": now - 300, "samples": 10, "metrics": {"x": 3}}], now)
    store.mark_sent([{"device_id": "a1", "ts": now - 600}])
    again = PerfStore(path)
    assert [r["ts"] for r in again.unsent()] == [now - 300]
    assert again.history("a1", now - 900, now) == [
        {"ts": now - 600, "samples": 10, "metrics": {"x": 2}},
        {"ts": now - 300, "samples": 10, "metrics": {"x": 3}}]
    assert again.history("a1", now - 400, now) == [{"ts": now - 300, "samples": 10, "metrics": {"x": 3}}]


def test_perf_tick_stores_averages_and_sends_them_once_hub_is_back(monkeypatch, tmp_path):
    from perf_store import PerfStore
    perf = {"disks": {"read_ops": 4, "write_ops": 20, "read_bytes": 1000,
                      "write_bytes": 5000, "busy": 3.5},
            "interfaces": [{"name": "eno1", "received_bytes_rate": 10, "sent_bytes_rate": 20},
                           {"name": "eno2", "received_bytes_rate": 5, "sent_bytes_rate": None}],
            "cpu_usage": 12.0}
    spoke = _make_spoke([
        {"id": "a1", "host": "10.0.0.5", "api_key": "k", "tenant_id": "acme"},
        {"id": "off", "host": "10.0.0.6", "api_key": "k", "poll_interval": 0}],
        monkeypatch, canned={"reporting.realtime": perf})
    spoke.perf_store = PerfStore(str(tmp_path / "perf.json"))
    sent = []

    class _CP:
        _hub_ws = None

        async def send_to_hub(self, msg_type, data):
            sent.append((msg_type, data))
            return True

    spoke.control_plane = _CP()
    now = int(__import__("time").time()) // 300 * 300
    _run(spoke._perf_sample_tick(now))
    _run(spoke._perf_sample_tick(now + 300))      # bucket closed, hub away: stored
    assert sent == [] and len(spoke.perf_store.unsent()) == 1
    spoke.control_plane._hub_ws = object()
    _run(spoke._perf_sample_tick(now + 330))
    assert spoke.perf_store.unsent() == []
    msg_type, data = sent[0]
    assert msg_type == "STORAGE_PERF_SAMPLE" and data["module"] == "truenas"
    metrics = {"read_ops": 4.0, "write_ops": 20.0, "read_bytes": 1000.0, "write_bytes": 5000.0,
               "busy": 3.5, "cpu": 12.0, "net_rx": 15.0, "net_tx": 20.0}
    assert data["samples"] == [{"device_id": "a1", "ts": now, "samples": 1, "metrics": metrics}]
    # Any range is read back from the spoke's store, tenant-checked.
    res = _run(spoke.handle_command("TRUENAS_GET_PERF_HISTORY", {
        "appliance_id": "a1", "tenant": "acme", "start": now - 3600, "end": now + 600}))
    assert res["status"] == "SUCCESS" and res["data"] == [{"ts": now, "samples": 1, "metrics": metrics}]
    res = _run(spoke.handle_command("TRUENAS_GET_PERF_HISTORY", {
        "appliance_id": "a1", "tenant": "other", "start": now - 3600}))
    assert res["status"] == "ERROR"

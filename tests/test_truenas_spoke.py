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
        "reporting.get_data": [],
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
        return self._record("reporting.get_data")

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
        "TRUENAS_GET_CAPACITY": "reporting.get_data",
    }
    for cmd, method in cmds.items():
        res = _run(spoke.handle_command(cmd, {"appliance_id": "a1"}))
        assert res["status"] in ("SUCCESS", "PARTIAL"), f"{cmd} -> {res}"
        client = spoke.engine._clients["a1"]
        assert client.calls, f"{cmd} recorded no call"
        assert client.calls[-1][0] == method, f"{cmd} -> {client.calls[-1][0]}"
        client.calls.clear()


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
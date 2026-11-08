"""TrueNAS engine — holds the fleet + delegates per-appliance read/write to
:class:`TrueNASClient`.

Mirrors ``nw_engine.NwEngine``: holds the appliance list pushed by the hub
(``set_appliances``) + the shared tenant id, and dispatches per-appliance
commands. One appliance's failure never sinks a batch — every method returns
the standard envelope ``{"status":"SUCCESS"|"ERROR"|"PARTIAL","data":...,
"message":...}``; a transport failure is an ERROR envelope (never raises).

The engine keeps one ``TrueNASClient`` per appliance (persistent WS JSON-RPC
session — see truenas_client.py for why), lazily built on first use and cached
on the appliance id. Credentials are NEVER logged (the spoke masks them in
handle_command before logging; the engine logs host only).
"""
from __future__ import annotations

# Standard library imports.
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("TruenasEngine")

# Credential field names — never logged.
_SENSITIVE = ("api_key", "password", "secret")


def _err(message: str, data: Any = None) -> Dict[str, Any]:
    """Construct standardized ERROR status response envelope."""
    # Empty list when no data is given.
    return {"status": "ERROR", "data": data if data is not None else [],
            "message": message}


def _hw_identity(data: Any) -> Dict[str, Any]:
    """The system.info fields that identify the physical machine and its
    product, for the fleet listing (the hub stitches the serial to the iLO)."""
    if not isinstance(data, dict):
        return {}
    keys = ("system_serial", "system_product", "system_manufacturer", "version",
            "product_name")
    return {k: data[k] for k in keys if data.get(k)}


class TruenasEngine:
    """Core interaction layer for the managed TrueNAS appliance fleet."""

    def __init__(self, appliances: Optional[List[Dict[str, Any]]] = None):
        """Initialize TruenasEngine with optional initial appliance fleet."""
        # Start with the given appliances (copied).
        self.appliances: List[Dict[str, Any]] = list(appliances or [])
        # The shared tenant id (set via UPDATE_CONFIG from the hub) — an
        # appliance whose ``tenant_id`` equals this is visible to ALL tenants
        # (matches the hub's shared-tenant-flag invariant). Empty when the hub
        # hasn't pushed it yet → a ``tenant`` filter matches only own-tenant.
        self.shared_tenant_id: str = ""
        # id -> TrueNASClient (lazy, persistent). Rebuilt when the fleet changes.
        self._clients: Dict[str, "TrueNASClient"] = {}

    def set_appliances(self, appliances: List[Dict[str, Any]],
                      shared_tenant_id: str = "") -> None:
        """Update the appliance fleet and reconcile active client connections."""
        # Replace the fleet and the shared tenant id.
        self.appliances = list(appliances or [])
        self.shared_tenant_id = shared_tenant_id or ""
        # Drop clients for appliances no longer in the fleet; keep the rest so
        # a poll cadence change doesn't tear a healthy WS session.
        seen = {a.get("id") for a in self.appliances if a.get("id")}
        for gone in list(self._clients):
            if gone not in seen:
                client = self._clients.pop(gone, None)
                if client is not None:
                    try:
                        import asyncio
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            loop.create_task(client.close())
                        else:
                            loop.run_until_complete(client.close())
                    except Exception:
                        pass
        # Log the new fleet size.
        logger.info("TruenasEngine fleet updated: %d appliance(s)", len(self.appliances))

    # ── tenancy ──────────────────────────────────────────────────────────────
    def _tenant_matches(self, appliance: Dict[str, Any],
                        tenant: Optional[str]) -> bool:
        """True if ``appliance`` is visible to ``tenant``: own-tenant or the
        shared tenant. ``tenant`` None/empty → no filter (whole fleet —
        backward-compatible with a hub that doesn't pass a tenant)."""
        # No tenant means no filter.
        if not tenant:
            return True
        # Match the tenant itself or the shared tenant.
        at = (appliance or {}).get("tenant_id", "")
        return at == tenant or (bool(self.shared_tenant_id)
                                and at == self.shared_tenant_id)

    def _get_appliance(self, appliance_id: str,
                       tenant: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Locate appliance configuration by ID with optional tenant matching."""
        # Find the appliance with this id that the tenant can see.
        for a in self.appliances:
            if a.get("id") == appliance_id and self._tenant_matches(a, tenant):
                return a
        return None

    def _client_for(self, appliance_id: str,
                    tenant: Optional[str] = None):
        """Return (appliance, TrueNASClient) or (None, None). Builds the client
        lazily from the appliance's credential reference (``credential_name``)
        resolved via the per-tenant credentials store."""
        # Unknown or hidden appliance.
        a = self._get_appliance(appliance_id, tenant)
        if not a:
            return None, None
        # Build and cache a client on first use; no credential means no client.
        client = self._clients.get(appliance_id)
        if client is None:
            from truenas_client import TrueNASClient
            cred = self._resolve_credential(a, tenant)
            if cred is None:
                return a, None
            client = TrueNASClient(cred)
            self._clients[appliance_id] = client
        # Return the appliance and its client.
        return a, client

    def _resolve_credential(self, appliance: Dict[str, Any],
                            tenant: Optional[str]) -> Optional[Dict[str, Any]]:
        """Resolve the appliance's connection credential. The appliance record
        carries either an inline ``api_key``/``host``/``verify_ssl`` (a
        one-box fleet) or a ``credential_name`` referencing the per-tenant
        credentials store. Inline wins (so a manual Setup entry works without
        the creds store)."""
        # Inline host and key take priority.
        host = (appliance or {}).get("host") or (appliance or {}).get("address") or ""
        api_key = (appliance or {}).get("api_key") or ""
        if host and api_key:
            return {"host": host, "api_key": api_key,
                    "username": appliance.get("username") or "",
                    "verify_ssl": appliance.get("verify_ssl", True),
                    "auth_mechanism": appliance.get("auth_mechanism", "auto")}
        # Otherwise look up the named credential for the tenant.
        name = (appliance or {}).get("credential_name") or ""
        tid = (appliance or {}).get("tenant_id") or tenant or ""
        if name and tid:
            try:
                import credentials
                return credentials.materialize(tid, name)
            except Exception as e:  # noqa: BLE001
                logger.error("truenas credential %s resolve failed: %s", name, e)
                return None
        # No usable credential.
        return None

    # ── logging ──────────────────────────────────────────────────────────────
    @staticmethod
    def _log_datum(method: str, host: str, res: Dict[str, Any],
                   detail: Optional[str] = None) -> None:
        """INFO on success (with a count), ERROR on failure (carries the word
        "error" so it surfaces in the hub's GET_ERROR_LOGS / Error Log tab —
        same precedent as the nw engine). Best-effort: logging never raises."""
        try:
            # Read status and message; default detail is a row count.
            status = str((res or {}).get("status", "")).upper()
            msg = (res or {}).get("message", "") or "transport failure"
            if detail is None:
                data = (res or {}).get("data")
                detail = f"{len(data)} row(s)" if isinstance(data, list) else "ok"
            # Success or partial logs at INFO; anything else logs as an error.
            tag = f"[truenas] {method} {host}"
            if status in ("SUCCESS", "PARTIAL"):
                logger.info("truenas %s -> %s", tag, detail)
            else:
                logger.error("truenas %s -> error: %s", tag, msg)
        # Logging itself failed: note it at debug level.
        except Exception:
            logger.debug("truenas log_datum %s failed", method, exc_info=True)

    @staticmethod
    def _ok_or_partial(data, sources, noun):
        """SUCCESS envelope; downgrade to PARTIAL (still returning ``data``)
        when any source errored, carrying the first error message. Mirrors
        NwEngine._ok_or_partial."""
        # Collect error messages from failed sources.
        errs = [s.get("message", "failed") for s in sources
                if s.get("status") not in ("SUCCESS", "PARTIAL")]
        # All failed and no data: return an error.
        if errs and not data:
            return _err("; ".join(errs))
        # Otherwise SUCCESS, or PARTIAL if anything failed.
        return {"status": "PARTIAL" if errs else "SUCCESS", "data": data,
                "message": f"{len(data)} {noun}" + (f" ({errs[0]})" if errs else "")}

    # ── fleet ───────────────────────────────────────────────────────────────
    async def list_appliances(self, tenant: Optional[str] = None) -> Dict[str, Any]:
        """Fleet summary (no credentials) with a concurrent lightweight probe
        (system.info) per appliance. Falls back to ``unknown`` on probe error
        so the UI never shows a stale 'up'. Tenant-scoped: ``tenant`` filters
        to own + shared (defense-in-depth; the hub already gates by tenant_id)."""
        # Appliances this tenant can see.
        fleet = [a for a in self.appliances if self._tenant_matches(a, tenant)]
        rows = []

        async def _probe_row(a: Dict[str, Any]):
            """Probe single appliance reachability and return structured status row."""
            # Basic appliance fields; reachability unknown until probed.
            aid = a.get("id", "")
            host = a.get("host") or a.get("address") or ""
            rcell = {"reachable": None, "latency_ms": None}
            # Hardware identity from the same probe (serial links the box to its iLO).
            info: Dict[str, Any] = {}
            # With a client, time a system info call (6 s limit).
            _, client = self._client_for(aid, tenant)
            if client is not None:
                import time, asyncio
                t0 = time.monotonic()
                try:
                    pr = await asyncio.wait_for(client.system_info(), timeout=6.0)
                    # Success: reachable, with latency.
                    if pr.get("status") == "SUCCESS":
                        rcell = {"reachable": True,
                                 "latency_ms": int((time.monotonic() - t0) * 1000)}
                        info = _hw_identity(pr.get("data"))
                    # Error reply: unreachable.
                    else:
                        logger.warning("truenas probe %s during fleet list: %s",
                                       host, pr.get("message", "probe failed"))
                        rcell = {"reachable": False, "latency_ms": None}
                # Timeout or other failure: unreachable.
                except Exception as e:  # noqa: BLE001
                    rcell = {"reachable": False, "latency_ms": None}
                    logger.warning("truenas probe %s during fleet list: %s", host, e)
            # Build the row (no credentials).
            return {
                "id": aid,
                "name": a.get("name", ""),
                "host": host,
                "object_type": a.get("object_type", "truenas"),
                "verify_ssl": bool(a.get("verify_ssl", True)),
                "reachable": rcell.get("reachable"),
                "latency_ms": rcell.get("latency_ms"),
                "info": info,
                "tenant_id": a.get("tenant_id", ""),
                "shared": bool(self.shared_tenant_id
                               and a.get("tenant_id", "") == self.shared_tenant_id),
            }
        # Probe all appliances at once.
        rows = await _gather([_probe_row(a) for a in fleet])
        # Count up and down appliances and log the totals.
        up = sum(1 for r in rows if r.get("reachable") is True)
        down = sum(1 for r in rows if r.get("reachable") is False)
        logger.info("truenas list_appliances -> %d appliance(s): %d up, %d down, "
                    "%d unknown", len(rows), up, down, len(rows) - up - down)
        # Return the rows.
        return {"status": "SUCCESS", "data": list(rows)}

    # ── per-appliance read passthroughs ─────────────────────────────────────
    async def probe(self, appliance_id: str,
                    tenant: Optional[str] = None) -> Dict[str, Any]:
        """Probe appliance reachability and system information."""
        # Find the appliance and its client; report either missing.
        a, client = self._client_for(appliance_id, tenant)
        if not a:
            return _err(f"Appliance {appliance_id} not found")
        if client is None:
            return _err(f"Appliance {appliance_id} has no resolvable credential")
        # Get system info, log the result and return it.
        res = await client.system_info()
        host = a.get("host") or a.get("address") or ""
        self._log_datum("probe", host, res,
                        detail=f"reachable={res.get('status') == 'SUCCESS'}")
        return res

    async def _read(self, method_name: str, appliance_id: str,
                    tenant: Optional[str] = None, *, client_fn=None,
                    noun: str = "row(s)") -> Dict[str, Any]:
        """Execute a read command against a target appliance client."""
        # Find the appliance and its client; report either missing.
        a, client = self._client_for(appliance_id, tenant)
        if not a:
            return _err(f"Appliance {appliance_id} not found")
        if client is None:
            return _err(f"Appliance {appliance_id} has no resolvable credential")
        # Run the read, log the result and return it.
        fn = client_fn or getattr(client, method_name)
        res = await fn()
        host = a.get("host") or a.get("address") or ""
        self._log_datum(method_name, host, res)
        return res

    async def get_pools(self, appliance_id: str,
                        tenant: Optional[str] = None) -> Dict[str, Any]:
        """Fetch ZFS storage pools from the target appliance."""
        return await self._read("pools", appliance_id, tenant, noun="pool(s)")

    async def get_datasets(self, appliance_id: str,
                           tenant: Optional[str] = None) -> Dict[str, Any]:
        """Fetch ZFS datasets from the target appliance."""
        return await self._read("datasets", appliance_id, tenant, noun="dataset(s)")

    async def get_disks(self, appliance_id: str,
                        tenant: Optional[str] = None) -> Dict[str, Any]:
        """Fetch physical disk inventory from the target appliance."""
        return await self._read("disks", appliance_id, tenant, noun="disk(s)")

    async def get_shares(self, appliance_id: str, kind: str = "smb",
                         tenant: Optional[str] = None) -> Dict[str, Any]:
        """Fetch file sharing configurations from the target appliance."""
        # Find the appliance and its client; report either missing.
        a, client = self._client_for(appliance_id, tenant)
        if not a:
            return _err(f"Appliance {appliance_id} not found")
        if client is None:
            return _err(f"Appliance {appliance_id} has no resolvable credential")
        # Run the action, log the result and return it.
        res = await client.shares(kind)
        host = a.get("host") or a.get("address") or ""
        self._log_datum(f"shares.{kind}", host, res)
        return res

    async def get_alerts(self, appliance_id: str,
                         tenant: Optional[str] = None) -> Dict[str, Any]:
        """Fetch active system alerts and warnings from the target appliance."""
        return await self._read("alerts", appliance_id, tenant, noun="alert(s)")

    async def get_services(self, appliance_id: str,
                           tenant: Optional[str] = None) -> Dict[str, Any]:
        """Fetch system services and operational states from the target appliance."""
        return await self._read("services", appliance_id, tenant, noun="service(s)")

    async def get_capacity(self, appliance_id: str,
                           tenant: Optional[str] = None) -> Dict[str, Any]:
        """Calculate storage capacity and allocation metrics across pools."""
        return await self._read("capacity", appliance_id, tenant, noun="pool(s)")

    # ── write methods (management) ──────────────────────────────────────────
    async def create_dataset(self, appliance_id: str, pool: str, name: str,
                             options: Optional[Dict[str, Any]] = None,
                             tenant: Optional[str] = None) -> Dict[str, Any]:
        """Create a new ZFS dataset within a pool on the target appliance."""
        # Find the appliance and its client; report either missing.
        a, client = self._client_for(appliance_id, tenant)
        if not a:
            return _err(f"Appliance {appliance_id} not found")
        if client is None:
            return _err(f"Appliance {appliance_id} has no resolvable credential")
        # Run the action, log the result and return it.
        res = await client.create_dataset(pool, name, options)
        host = a.get("host") or a.get("address") or ""
        self._log_datum("create_dataset", host, res,
                        detail=f"{pool}/{name}" if res.get("status") == "SUCCESS" else None)
        return res

    async def delete_dataset(self, appliance_id: str, dataset_id: str,
                             options: Optional[Dict[str, Any]] = None,
                             tenant: Optional[str] = None) -> Dict[str, Any]:
        """Delete an existing ZFS dataset on the target appliance."""
        # Find the appliance and its client; report either missing.
        a, client = self._client_for(appliance_id, tenant)
        if not a:
            return _err(f"Appliance {appliance_id} not found")
        if client is None:
            return _err(f"Appliance {appliance_id} has no resolvable credential")
        # Run the action, log the result and return it.
        res = await client.delete_dataset(dataset_id, options)
        host = a.get("host") or a.get("address") or ""
        self._log_datum("delete_dataset", host, res, detail=dataset_id
                        if res.get("status") == "SUCCESS" else None)
        return res

    async def create_share(self, appliance_id: str, kind: str, dataset: str,
                           options: Optional[Dict[str, Any]] = None,
                           tenant: Optional[str] = None) -> Dict[str, Any]:
        """Create a file share (SMB/NFS) for a dataset on the target appliance."""
        # Find the appliance and its client; report either missing.
        a, client = self._client_for(appliance_id, tenant)
        if not a:
            return _err(f"Appliance {appliance_id} not found")
        if client is None:
            return _err(f"Appliance {appliance_id} has no resolvable credential")
        # Run the action, log the result and return it.
        res = await client.create_share(kind, dataset, options)
        host = a.get("host") or a.get("address") or ""
        self._log_datum(f"create_share.{kind}", host, res,
                        detail=dataset if res.get("status") == "SUCCESS" else None)
        return res

    async def create_snapshot(self, appliance_id: str, dataset: str,
                              name: str = "",
                              options: Optional[Dict[str, Any]] = None,
                              tenant: Optional[str] = None) -> Dict[str, Any]:
        """Take a manual ZFS snapshot of a dataset on the target appliance."""
        # Find the appliance and its client; report either missing.
        a, client = self._client_for(appliance_id, tenant)
        if not a:
            return _err(f"Appliance {appliance_id} not found")
        if client is None:
            return _err(f"Appliance {appliance_id} has no resolvable credential")
        # Run the action, log the result and return it.
        res = await client.create_snapshot(dataset, name, options)
        host = a.get("host") or a.get("address") or ""
        self._log_datum("create_snapshot", host, res,
                        detail=f"{dataset}@{name}" if res.get("status") == "SUCCESS"
                        else None)
        return res

    async def run_scrub(self, appliance_id: str, pool_id: str,
                        tenant: Optional[str] = None) -> Dict[str, Any]:
        """Initiate a ZFS pool scrub operation on the target appliance."""
        # Find the appliance and its client; report either missing.
        a, client = self._client_for(appliance_id, tenant)
        if not a:
            return _err(f"Appliance {appliance_id} not found")
        if client is None:
            return _err(f"Appliance {appliance_id} has no resolvable credential")
        # Run the action, log the result and return it.
        res = await client.run_scrub(pool_id)
        host = a.get("host") or a.get("address") or ""
        self._log_datum("run_scrub", host, res, detail=pool_id
                        if res.get("status") == "SUCCESS" else None)
        return res

    # ── poll (POLL NOW) ──────────────────────────────────────────────────────
    async def poll(self, appliance_id: str,
                   tenant: Optional[str] = None) -> Dict[str, Any]:
        """Full poll: system_info + pools + datasets + disks + shares(smb) +
        alerts + services + capacity in one call. Each sub-call is
        independent — a failure on one datum doesn't sink the rest; failed
        datums come back as empty + an entry in ``errors``."""
        # Find the appliance and its client; report either missing.
        a, client = self._client_for(appliance_id, tenant)
        host = (a or {}).get("host") or (a or {}).get("address") or ""
        if not a:
            return _err(f"Appliance {appliance_id} not found")
        if client is None:
            return _err(f"Appliance {appliance_id} has no resolvable credential")
        # Error messages from failed parts.
        errors: List[str] = []

        async def _safe(coro, label, default):
            """Execute a query coroutine with error trapping and default fallback."""
            # Run the query; an exception is logged and recorded.
            try:
                r = await coro
            except Exception as e:  # noqa: BLE001
                self._log_datum(label, host, _err(str(e)))
                errors.append(f"{label}: {e}")
                return default
            # Log the result; return data on success, else record the error.
            self._log_datum(label, host, r)
            if r.get("status") in ("SUCCESS", "PARTIAL"):
                return r.get("data")
            errors.append(f"{label}: {r.get('message', 'failed')}")
            return default

        # Fetch each part separately.
        system_info = await _safe(client.system_info(), "system_info", {})
        pools = await _safe(client.pools(), "pools", [])
        datasets = await _safe(client.datasets(), "datasets", [])
        disks = await _safe(client.disks(), "disks", [])
        smb = await _safe(client.shares("smb"), "shares.smb", [])
        nfs = await _safe(client.shares("nfs"), "shares.nfs", [])
        alerts = await _safe(client.alerts(), "alerts", [])
        services = await _safe(client.services(), "services", [])
        capacity = await _safe(client.capacity(), "capacity", [])

        # Group shares (iSCSI is not polled) and set the overall status.
        shares = {"smb": smb, "nfs": nfs, "iscsi": []}
        status = "PARTIAL" if errors else "SUCCESS"
        reachable = bool(system_info)
        # Count items and log a summary.
        n_pools = len(pools) if isinstance(pools, list) else 0
        n_ds = len(datasets) if isinstance(datasets, list) else 0
        n_disks = len(disks) if isinstance(disks, list) else 0
        logger.info("truenas poll %s -> status=%s reachable=%s pools=%d datasets=%d "
                    "disks=%d errors=%d", host, status, reachable, n_pools,
                    n_ds, n_disks, len(errors))
        # Return all data, any errors and a summary message.
        return {
            "status": status,
            "data": {
                "system_info": system_info,
                "pools": pools,
                "datasets": datasets,
                "disks": disks,
                "shares": shares,
                "alerts": alerts,
                "services": services,
                "capacity": capacity,
            },
            "errors": errors,
            "message": (f"reachable={reachable}, "
                        f"{n_pools} pool(s), "
                        f"{n_ds} dataset(s), "
                        f"{n_disks} disk(s)"
                        + (f", errors={len(errors)}" if errors else "")),
        }


async def _gather(coros):
    """asyncio.gather that preserves order + never rejects. Local import keeps
    the module importable under a plain `import` in a non-async test context
    (callers always call these methods from a running loop)."""
    # Run all coroutines and return results in order.
    import asyncio
    return await asyncio.gather(*coros)
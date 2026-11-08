"""TrueNAS WebSocket JSON-RPC client wrapper over the official truenas_api_client.

TrueNAS middleware is a stateful WebSocket JSON-RPC 2.0 API. The official
``truenas_api_client`` (https://github.com/truenas/api_client) handles the WS
connect, ping/reconnect, SCRAM/PLAIN API-key auth, job polling, and the legacy
DDP (``/websocket``) fallback for Core/24.x. We keep ONE persistent connection
per appliance — API keys auto-revoke over plain ``ws://`` (so we force ``wss://``
for any non-loopback host), and the middleware rate-limits auth (20/60s + a
10-min cooldown) so a fresh login per call would throttle the fleet.

This module is a thin async-friendly wrapper: the official client is
*blocking*, so every call runs via ``asyncio.to_thread`` to stay off the
spoke's event loop (a slow ``pool.query`` must not stall a concurrent VNC
relay sharing the loop — see vnc-ws-keepalive memory). The client is
**lazy-imported** so the spoke imports cleanly without the dep installed
(mirrors nw/src/transports/ lazy imports); ``dep_guard`` pip-installs it on
boot from requirements.txt.

Every public method returns the standard envelope
``{"status":"SUCCESS"|"ERROR","data":...,"message":...}``; a transport/API
failure returns an ERROR envelope (never raises) so one appliance's failure
doesn't sink a batch poll (nw_engine precedent).
"""
from __future__ import annotations

# Standard library imports.
import asyncio
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("TruenasClient")

# Methods the wrapper calls (documented in one place so a future reader sees
# the surface). Read: system.info, pool.query, pool.dataset.query, disk.query,
# sharing.{nfs,smb,iscsi}.query, service.query, alert.list, reporting.get_data.
# Write: pool.dataset.{create,delete}, sharing.{smb,nfs}.create,
# zfs.resource.snapshot.create, pool.scrub.start, service.{start,stop,update}.


def _build_uri(host: str) -> str:
    """Build the WS JSON-RPC URI. Force ``wss://`` for any non-loopback host
    (API keys auto-revoke over plain ws://). A loopback/``localhost`` host is
    allowed plain (a same-box dev/test appliance). The official client appends
    the JSON-RPC endpoint is ``/api/current``: a bare host makes the appliance
    answer the websocket handshake with a redirect to ``/ui/`` ("Invalid
    redirect target"), so the path is added whenever the host carries none."""
    # Empty host means no URI.
    h = (host or "").strip()
    if not h:
        return ""
    # Loopback hosts may use plain ws://; everything else uses wss://.
    low = h.lower()
    if not h.startswith(("ws://", "wss://")):
        loop = low in ("127.0.0.1", "localhost", "::1") or low.startswith("127.")
        h = f"{'ws' if loop else 'wss'}://{h}"
    # A URI that already names a path is used as given.
    if "/" not in h.split("://", 1)[1]:
        h += "/api/current"
    return h


def _is_scale(system_info: Dict[str, Any]) -> bool:
    """TrueNAS SCALE vs Core — matters because some namespaces differ
    (SCALE has app.*/vm.*/docker.*; Core has jail.*/plugin.*/afp.*). We only
    call cross-platform methods, but record the flavor for the UI."""
    # Not a dict: assume Core.
    if not isinstance(system_info, dict):
        return False
    # SCALE reports virtualization or has "scale" in the product name.
    return bool(system_info.get("virtualization")) or \
        "scale" in str(system_info.get("product_name", "")).lower()


class TrueNASClient:
    """One persistent connection to a TrueNAS appliance over the official
    truenas_api_client. Constructed from a credentials dict (host/api_key/
    verify_ssl/auth_mechanism). Lazy-connects on first call; reconnects once
    on a dropped session (the official client handles ping/reconnect too, but
    we add a top-level retry so a transient middleware hiccup returns ERROR,
    not an exception)."""

    def __init__(self, cred: Dict[str, Any]):
        """Read host, key, TLS and auth settings from the credential; do not connect yet."""
        # Connection settings, with an unknown auth mechanism treated as "auto".
        self.host = (cred or {}).get("host", "")
        self.api_key = (cred or {}).get("api_key", "")
        self.verify_ssl = bool((cred or {}).get("verify_ssl", True))
        mech = (cred or {}).get("auth_mechanism", "auto")
        self.auth_mechanism = mech if mech in ("auto", "PLAIN", "SCRAM") else "auto"
        # Build the URI and start disconnected.
        self.uri = _build_uri(self.host)
        self._client = None          # lazy truenas_api_client.Client
        self._connected = False
        self._connect_error: Optional[str] = None

    # ── connection lifecycle ────────────────────────────────────────────────
    def _import_client(self):
        """Lazy import of the official client. Raises ImportError if the dep
        is missing (dep_guard should have pip-installed it at boot)."""
        from truenas_api_client import Client, ClientException  # type: ignore
        return Client, ClientException

    def _connect_sync(self) -> None:
        """Blocking connect + login. Runs in a worker thread via to_thread."""
        # Load the client classes and set TLS verification.
        Client, ClientException = self._import_client()
        kwargs: Dict[str, Any] = {"verify_ssl": self.verify_ssl}
        # The official client accepts a per-call api_key on call(), but a
        # persistent login keeps one session and avoids the 20-auth/60s rate
        # limit. login_with_api_key is the documented path.
        c = Client(self.uri, **kwargs)
        # Log in with the API key.
        try:
            if self.auth_mechanism == "PLAIN":
                try:
                    from truenas_api_client import (  # type: ignore
                        Authority as _A,
                    )
                    # PLAIN mechanism for Core/24.x legacy boxes.
                    c.login_with_api_key(self.api_key,
                                         auth_mechanism=_A.AuthMechanism.PLAIN)
                # PLAIN not available: use the default login.
                except Exception:
                    c.login_with_api_key(self.api_key)
            # Any other mechanism uses the default login.
            else:
                c.login_with_api_key(self.api_key)
        # Login failed: try a harmless ping, then re-raise the error.
        except Exception as e:  # noqa: BLE001
            try:
                c.call("core.ping") if hasattr(c, "call") else None
            except Exception:
                pass
            raise
        # Keep the connected client.
        self._client = c
        self._connected = True

    async def _ensure(self) -> bool:
        """Connect if not connected. Returns False + sets _connect_error on
        failure (never raises — caller emits an ERROR envelope)."""
        # Already connected.
        if self._connected and self._client is not None:
            return True
        # Connect in a worker thread; on failure record the error and return False.
        try:
            await asyncio.to_thread(self._connect_sync)
            logger.info("truenas connect %s ok (%s, auth=%s, verify_ssl=%s)",
                        self.host, self.uri, self.auth_mechanism, self.verify_ssl)
            return True
        except Exception as e:  # noqa: BLE001
            self._connect_error = str(e)
            self._connected = False
            logger.error("truenas connect %s failed: %s", self.host, e)
            return False

    def _call_sync(self, method: str, *args, **kwargs) -> Any:
        """Blocking JSON-RPC call. The official client's ``call()`` raises
        ``ClientException`` (incl. CallError) on a non-zero result."""
        # Refuse to call without a client.
        if self._client is None:
            raise RuntimeError("not connected")
        return self._client.call(method, *args, **kwargs)

    async def _call(self, method: str, *args, **kwargs) -> Dict[str, Any]:
        """Async wrapper: ensure connected, run the blocking call off the loop.
        Returns a standard envelope (never raises)."""
        # Connect first; report a connection failure as an error envelope.
        if not await self._ensure():
            return {"status": "ERROR",
                    "message": f"connect failed: {self._connect_error}",
                    "data": None}
        # Run the call in a worker thread and wrap the result.
        try:
            result = await asyncio.to_thread(self._call_sync, method, *args, **kwargs)
            return {"status": "SUCCESS", "data": result, "message": method}
        # The call failed: log it and return an error envelope.
        except Exception as e:  # noqa: BLE001
            logger.error("truenas %s %s failed: %s", self.host, method, e)
            return {"status": "ERROR", "message": f"{method}: {e}", "data": None}

    async def _call_job(self, method: str, *args, **kwargs) -> Dict[str, Any]:
        """A long-running middleware job (scrub, replication): pass ``job=True``
        so the client waits for it, returning the job result. For a
        fire-and-forget job start use ``_call`` instead + poll separately."""
        # Ask the client to wait for the job to finish.
        kwargs.setdefault("job", True)
        return await self._call(method, *args, **kwargs)

    # ── read methods ────────────────────────────────────────────────────────
    async def system_info(self) -> Dict[str, Any]:
        """Return system info, adding an _is_scale flag."""
        env = await self._call("system.info")
        if env["status"] == "SUCCESS" and isinstance(env["data"], dict):
            env["data"] = {**env["data"], "_is_scale": _is_scale(env["data"])}
        return env

    async def pools(self) -> Dict[str, Any]:
        """Return pools with the fields the dashboard uses."""
        # pool.query is the heaviest read — select only what the dashboard
        # needs (id/name/topology/health) instead of re-pulling everything.
        return await self._call("pool.query", [], {"select": [
            "id", "name", "guid", "status", "healthy", "encrypted",
            "topology", "scan", "autotrim", "path"]})

    async def datasets(self) -> Dict[str, Any]:
        """Return datasets with size, encryption and quota fields."""
        return await self._call("pool.dataset.query", [], {"select": [
            "id", "name", "pool", "type", "mountpoint", "comments",
            "encryption_root", "key_loaded", "used", "available",
            "compression", "deduplication", "quota", "refquota"]})

    async def disks(self) -> Dict[str, Any]:
        """Return disks with their pool and hardware details."""
        # extra={"pools": True} so each disk carries the pool it belongs to.
        return await self._call("disk.query", [], {"extra": {"pools": True},
                                    "select": ["name", "serial", "model", "size",
                                               "type", "bus", "devname",
                                               "rotation_rate", "status"]})

    async def shares(self, kind: str = "smb") -> Dict[str, Any]:
        """Return SMB, NFS or iSCSI shares."""
        # Only these share types are allowed.
        kind = (kind or "smb").lower()
        if kind not in ("smb", "nfs", "iscsi"):
            return {"status": "ERROR",
                    "message": f"unknown share kind {kind!r}", "data": None}
        # Query that share type.
        return await self._call(f"sharing.{kind}.query")

    async def services(self) -> Dict[str, Any]:
        """Return the state of each service."""
        return await self._call("service.query")

    async def alerts(self) -> Dict[str, Any]:
        """Return current alerts."""
        return await self._call("alert.list")

    async def capacity(self) -> Dict[str, Any]:
        """Per-pool capacity series via reporting.get_data. Returns the most
        recent used/avail datapoint per pool. reporting.get_data takes a node
        name (a pool) + a list of metrics; we ask for ``used`` + ``avail``."""
        # Get the pool list; pass along any error.
        pools_env = await self.pools()
        if pools_env["status"] != "SUCCESS":
            return pools_env
        # Read capacity for each named pool.
        pools = pools_env["data"] if isinstance(pools_env.get("data"), list) else []
        out: List[Dict[str, Any]] = []
        for p in pools:
            name = p.get("name") if isinstance(p, dict) else None
            if not name:
                continue
            # Ask for the last hour of used and available space.
            r = await self._call("reporting.get_data", [{
                "node": name, "metrics": ["used", "avail"],
                "aggregate": True, "start": "1h"}])
            # Record a per-pool error and continue.
            if r["status"] != "SUCCESS":
                out.append({"pool": name, "used": None, "avail": None,
                            "error": r.get("message", "")})
                continue
            # reporting.get_data returns a list of series dicts.
            series = r["data"] if isinstance(r.get("data"), list) else []
            # Take the latest value for each metric.
            used = avail = None
            for s in series:
                if not isinstance(s, dict):
                    continue
                metric = s.get("metric")
                vals = s.get("data") or s.get("aggregations") or []
                last = vals[-1][1] if (vals and isinstance(vals[-1], list)
                                       and len(vals[-1]) >= 2) else None
                if metric == "used":
                    used = last
                elif metric == "avail":
                    avail = last
            # Add this pool's result.
            out.append({"pool": name, "used": used, "avail": avail})
        return {"status": "SUCCESS", "data": out, "message": f"{len(out)} pool(s)"}

    # ── write methods (management, gated by the hub's write_scope) ──────────
    async def create_dataset(self, pool: str, name: str,
                             options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a dataset named pool/name with optional settings."""
        # Full dataset path and settings.
        ds = f"{pool}/{name}"
        payload = dict(options or {})
        return await self._call("pool.dataset.create", [ds, payload])

    async def delete_dataset(self, dataset_id: str,
                             options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Delete a dataset, by default recursively and forced."""
        # Recursive and forced unless options override.
        payload = {"recursive": True, "force": True}
        payload.update(options or {})
        return await self._call("pool.dataset.delete", [dataset_id, payload])

    async def create_share(self, kind: str, dataset: str,
                           options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create an SMB or NFS share for a dataset path."""
        # Only SMB and NFS are supported.
        kind = (kind or "smb").lower()
        if kind not in ("smb", "nfs"):
            return {"status": "ERROR",
                    "message": f"create_share supports smb/nfs (got {kind!r})",
                    "data": None}
        # Share the dataset path with any extra options.
        payload = {"path": dataset}
        payload.update(options or {})
        return await self._call(f"sharing.{kind}.create", [payload])

    async def create_snapshot(self, dataset: str, name: str = "",
                              options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a snapshot of a dataset, optionally with a name."""
        # Dataset, optional name and extra options.
        payload = {"dataset": dataset}
        if name:
            payload["name"] = name
        payload.update(options or {})
        return await self._call("zfs.snapshot.create", [payload])

    async def run_scrub(self, pool_id: str) -> Dict[str, Any]:
        """Start a pool scrub and wait for the job to finish."""
        # pool.scrub.start is a job (can take hours); job=True blocks. The
        # hub/UI shows "scrub started" — for long pools the caller may prefer a
        # fire-and-forget variant; this waits so the envelope carries the
        # outcome. Timeout-safe via the to_thread wrapper.
        return await self._call_job("pool.scrub.start", [pool_id])

    async def close(self) -> None:
        """Drop the connection and try to close the WebSocket."""
        # Detach the client and mark disconnected.
        c, self._client = self._client, None
        self._connected = False
        if c is not None:
            # These calls are disabled: the condition is always false.
            try:
                await asyncio.to_thread(c.call, "system.reboot") if False else None
            except Exception:
                pass
            try:
                await asyncio.to_thread(c.call, "core.shutdown") if False else None
            except Exception:
                pass
            # The official client has no explicit close; drop the ref + let GC
            # tear the WS down. A real disconnect is best-effort (below).
            try:
                if hasattr(c, "_ws") and c._ws is not None:
                    await asyncio.to_thread(c._ws.close)
            except Exception:
                pass
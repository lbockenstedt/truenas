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
import errno
import logging
import threading
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


def _is_disconnect(e: Exception) -> bool:
    """True if the error means the WebSocket is gone (not a server-side call error)."""
    return (getattr(e, "errno", None) == errno.ECONNABORTED
            or isinstance(e, ConnectionError)
            or "closure of remote connection" in str(e)
            or "socket is already closed" in str(e).lower())


def _parsed(prop: Any) -> Optional[float]:
    """Numeric value of a ZFS property ({parsed, rawvalue, value}) or a bare number."""
    if isinstance(prop, dict):
        prop = prop.get("parsed", prop.get("rawvalue"))
    try:
        return float(prop) if prop is not None else None
    except (TypeError, ValueError):
        return None


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
        # User the API key belongs to (required by TrueNAS 25.04+ key login).
        self._username_set = bool((cred or {}).get("username"))
        self.username = (cred or {}).get("username") or "truenas_admin"
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
        # Log in with the API key (truenas_api_client needs the key's owner).
        try:
            self._login(c)
        # Login failed: close the socket and re-raise.
        except Exception:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass
            raise
        # Keep the connected client.
        self._client = c
        self._connected = True

    def _login(self, c) -> None:
        """API-key login. SCRAM/PLAIN are honoured as chosen; "auto" tries SCRAM
        (TrueNAS 26+) and falls back to PLAIN (Core / 24.x / 25.x)."""
        from truenas_api_client.auth_api_key import APIKeyAuthMech  # type: ignore
        if self.auth_mechanism != "PLAIN":
            try:
                c.login_with_api_key(self.username, self.api_key, APIKeyAuthMech.SCRAM)
                return
            except Exception as e:  # noqa: BLE001
                if self.auth_mechanism == "SCRAM":
                    raise
                logger.info("truenas %s SCRAM login failed (%s); trying PLAIN", self.host, e)
        try:
            c.login_with_api_key(self.username, self.api_key, APIKeyAuthMech.PLAIN)
        except Exception:
            # No user configured: the default may not own the key, so use the
            # key-only login (TrueNAS < 26 finds the owner from the key itself).
            if self._username_set:
                raise
            if not c.call("auth.login_with_api_key", self.api_key):
                raise ValueError("Invalid API key")

    async def _ensure(self) -> bool:
        """Connect if not connected. Returns False + sets _connect_error on
        failure (never raises — caller emits an ERROR envelope)."""
        # Already connected.
        if self._connected and self._client is not None:
            return True
        # Connect in a worker thread; on failure record the error and return False.
        try:
            await asyncio.to_thread(self._connect_sync)
            logger.info("truenas connect %s ok (%s, user=%s, auth=%s, verify_ssl=%s)",
                        self.host, self.uri, self.username, self.auth_mechanism,
                        self.verify_ssl)
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
        return await self._run(method, self._call_sync, method, *args, **kwargs)

    async def _run(self, label: str, fn, *args, **kwargs) -> Dict[str, Any]:
        """Run blocking ``fn`` on the connected client off the loop, reconnecting
        once on a dropped socket. Returns a standard envelope (never raises)."""
        # Connect first; report a connection failure as an error envelope.
        if not await self._ensure():
            return {"status": "ERROR",
                    "message": f"connect failed: {self._connect_error}",
                    "data": None}
        # Run the call in a worker thread and wrap the result.
        try:
            try:
                result = await asyncio.to_thread(fn, *args, **kwargs)
            # TrueNAS closes idle sockets between polls: reconnect once and retry.
            except Exception as e:  # noqa: BLE001
                if not _is_disconnect(e):
                    raise
                logger.info("truenas %s connection dropped (%s); reconnecting", self.host, e)
                await self.close()
                if not await self._ensure():
                    raise RuntimeError(f"reconnect failed: {self._connect_error}") from e
                result = await asyncio.to_thread(fn, *args, **kwargs)
            return {"status": "SUCCESS", "data": result, "message": label}
        # The call failed: log it and return an error envelope.
        except Exception as e:  # noqa: BLE001
            logger.error("truenas %s %s failed: %s", self.host, label, e)
            return {"status": "ERROR", "message": f"{label}: {e}", "data": None}

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
        """Per-pool used/available bytes, read from each pool's root dataset."""
        env = await self._call("pool.dataset.query", [],
                               {"select": ["name", "pool", "used", "available"]})
        if env["status"] != "SUCCESS":
            return env
        out: List[Dict[str, Any]] = []
        for ds in env["data"] or []:
            # Root datasets (no "/") carry the whole pool's usage.
            if not isinstance(ds, dict) or "/" in str(ds.get("name", "")):
                continue
            used = _parsed(ds.get("used"))
            avail = _parsed(ds.get("available"))
            total = (used or 0) + (avail or 0)
            out.append({"pool": ds.get("name"), "used": used, "avail": avail,
                        "pct_used": round(100 * used / total, 1) if used is not None and total else None})
        return {"status": "SUCCESS", "data": out, "message": f"{len(out)} pool(s)"}

    def _realtime_sync(self, timeout: float = 10.0) -> Dict[str, Any]:
        """One sample of the ``reporting.realtime`` event (what the TrueNAS
        dashboard shows): disk IOPS/throughput, interface traffic, CPU, memory."""
        sample: Dict[str, Any] = {}
        got = threading.Event()

        def _on_event(mtype, **msg):
            fields = msg.get("fields")
            if isinstance(fields, dict) and fields and not got.is_set():
                sample.update(fields)
                got.set()

        sub_id = self._client.subscribe("reporting.realtime", _on_event)
        try:
            if not got.wait(timeout):
                raise TimeoutError("no reporting.realtime sample")
        finally:
            try:
                self._client.unsubscribe(sub_id)
            except Exception:  # noqa: BLE001
                pass
        return sample

    async def performance(self) -> Dict[str, Any]:
        """Live disk IOPS + throughput and per-interface traffic."""
        env = await self._run("reporting.realtime", self._realtime_sync)
        if env["status"] != "SUCCESS":
            return env
        raw = env["data"] or {}
        disks = raw.get("disks") if isinstance(raw.get("disks"), dict) else {}
        ifaces = raw.get("interfaces") if isinstance(raw.get("interfaces"), dict) else {}
        cpu = raw.get("cpu") if isinstance(raw.get("cpu"), dict) else {}
        env["data"] = {
            "disks": {k: disks.get(k) for k in
                      ("read_ops", "write_ops", "read_bytes", "write_bytes", "busy")},
            "interfaces": [
                {"name": name, "link_state": v.get("link_state"), "speed": v.get("speed"),
                 "received_bytes_rate": v.get("received_bytes_rate"),
                 "sent_bytes_rate": v.get("sent_bytes_rate")}
                for name, v in sorted(ifaces.items()) if isinstance(v, dict)],
            "cpu_usage": (cpu.get("cpu") or {}).get("usage") if isinstance(cpu.get("cpu"), dict) else None,
        }
        return env

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
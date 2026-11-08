"""TrueNAS spoke: turns hub TRUENAS_* commands into engine calls.

Holds the appliance fleet pushed by the hub, masks credentials in logs and
logs every command's outcome.
"""
# Standard library imports.
import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, Any, List

# Base spoke (core package or flat layout) and the TrueNAS engine.
try:
    from core.src.base_spoke import BaseSpoke
except ImportError:
    from base_spoke import BaseSpoke
from truenas_engine import TruenasEngine

logger = logging.getLogger("TruenasSpoke")

# Credential field names — FULL mask in logs (never partial). Mirrors the
# nw spoke's masking precedent: leaking both ends of a credential exposes a
# meaningful fraction of a typical secret, so the whole value is replaced.
_SENSITIVE = {"api_key", "password", "secret", "hub_secret"}


class TruenasSpoke(BaseSpoke):
    """TrueNAS management + reporting spoke for Lab Manager.

    Translates Hub TRUENAS_* commands into per-appliance WebSocket JSON-RPC
    actions via :class:`TruenasEngine`. Manages a **fleet** of appliances (one
    spoke → many TrueNAS boxes) pushed from ``global_config["truenas_appliances"]``
    through UPDATE_CONFIG. Mirrors the nw spoke (fleet + _SENSITIVE mask +
    UPDATE_CONFIG stores the fleet + _log_result INFO/ERROR).
    """
    def __init__(self, spoke_id: str, config: Dict[str, Any]):
        """Initialize TruenasSpoke instance and configure engine fleet."""
        # The engine needs the fleet before super().__init__ so any base-class
        # background worker sees it. The hub pushes appliances via UPDATE_CONFIG
        # after approval; at cold start config may carry appliances from a
        # pre-provisioned config.
        appliances = (config or {}).get("appliances", []) if isinstance(config, dict) else []
        self.engine = TruenasEngine(appliances)
        # Use the shared tenant id if the config has one.
        shared_tid = (config or {}).get("shared_tenant_id", "") if isinstance(config, dict) else ""
        if shared_tid:
            self.engine.shared_tenant_id = shared_tid
        # Control-plane back-ref (set by the standalone control plane or the
        # agent's role hook) and the background poll task.
        self.control_plane = None
        self._bg_tasks: List[asyncio.Task] = []
        # Finish base spoke setup.
        super().__init__(spoke_id, config)

    # Poll-loop tick granularity + per-appliance interval floor (seconds). The
    # cadence itself is per-appliance (``poll_interval`` on each
    # truenas_appliances entry); these bound the scheduler, not the user's choice.
    _TRUENAS_POLL_TICK = 10
    _TRUENAS_POLL_FLOOR = 60          # TrueNAS pool.query is heavier than an
                                      # ARP probe — keep a 1m floor.
    # Default cadence when an appliance has no poll_interval set. An explicit
    # 0 (the UI "Off" choice) disables; only an absent/blank value defaults.
    _TRUENAS_POLL_DEFAULT = 900       # 15 minutes
    _TRUENAS_POLL_FIRST = 60          # first poll after a (re)start or new appliance

    async def _truenas_poll_loop(self):
        """Per-appliance autonomous polling done **by the spoke**.

        Each TrueNAS appliance may set ``poll_interval`` (seconds) in its
        config; this ticks every ``_TRUENAS_POLL_TICK`` and polls any appliance
        whose interval has elapsed, pushing the result to the hub
        (``TRUENAS_POLL_RESULT``) so the hub warms its per-appliance cache —
        every Storage sub-view then loads instantly instead of blocking on a
        live WS JSON-RPC round-trip. ``poll_interval`` ≤ 0 / absent = disabled
        (or inherits the module default). Intervals are floored to
        ``_TRUENAS_POLL_FLOOR`` to avoid hammering an appliance. A newly-seen
        appliance is scheduled (not polled immediately) so a fleet reload
        staggers rather than stampedes."""
        # Next poll time for each appliance id.
        next_due: Dict[str, float] = {}
        while True:
            # Wake every tick.
            await asyncio.sleep(self._TRUENAS_POLL_TICK)
            try:
                # Skip until the engine exists and the hub is connected.
                cp = self.control_plane
                engine = self.engine
                if cp is None or getattr(cp, "_hub_ws", None) is None:
                    continue
                # Module-wide default interval; blank uses the built-in default.
                mod_raw = (self.config or {}).get("default_poll_interval")
                try:
                    module_default = (self._TRUENAS_POLL_DEFAULT
                                      if mod_raw in (None, "") else int(mod_raw))
                except (TypeError, ValueError):
                    module_default = self._TRUENAS_POLL_DEFAULT
                # Work out which appliances are due this tick.
                now = time.monotonic()
                due, seen = [], set()
                for a in list(engine.appliances):
                    # Skip entries without an id.
                    aid = a.get("id")
                    if not aid:
                        continue
                    seen.add(aid)
                    # Appliance setting wins; blank inherits the module default.
                    raw = a.get("poll_interval")
                    if raw is None or raw == "":
                        interval = module_default          # inherit module default
                    else:
                        try:
                            interval = int(raw)            # appliance wins (incl 0=Off)
                        except (TypeError, ValueError):
                            interval = module_default
                    # Zero or less means polling is off for this appliance.
                    if interval <= 0:                       # explicit Off
                        next_due.pop(aid, None)
                        continue
                    # Apply the minimum interval.
                    interval = max(interval, self._TRUENAS_POLL_FLOOR)
                    deadline = next_due.get(aid)
                    # New appliances get a short first poll (so a restart warms the
                    # hub cache quickly); due ones are queued and rescheduled.
                    if deadline is None:
                        next_due[aid] = now + min(interval, self._TRUENAS_POLL_FIRST)
                    elif now >= deadline:
                        next_due[aid] = now + interval
                        due.append(aid)
                # Forget appliances that were removed.
                for gone in set(next_due) - seen:  # prune removed appliances
                    next_due.pop(gone, None)
                # Poll due appliances, at most three at once.
                if due:
                    sem = asyncio.Semaphore(3)

                    async def _one(appliance_id):
                        """Poll one appliance while holding the concurrency limit."""
                        async with sem:
                            await self._truenas_poll_and_push(appliance_id)
                    await asyncio.gather(*(_one(x) for x in due))
            # Log tick errors and keep looping.
            except Exception as e:  # noqa: BLE001 - loop must never die
                logger.debug("truenas poll loop tick error: %s", e)

    async def _truenas_poll_and_push(self, appliance_id: str):
        """Run one full engine poll + push it to the hub as
        TRUENAS_POLL_RESULT."""
        # Not wired to a control plane: nothing to push to.
        cp = self.control_plane
        engine = self.engine
        if cp is None:
            return
        # Run the poll; on failure log and stop.
        try:
            res = await engine.poll(appliance_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("truenas auto-poll %s failed: %s", appliance_id, e)
            return
        # Only push a result that has a data dict.
        data = res.get("data") if isinstance(res, dict) else None
        if not isinstance(data, dict):
            return
        # Send the result to the hub.
        await cp.send_to_hub("TRUENAS_POLL_RESULT",
                               {"appliance_id": appliance_id, "data": data,
                                "errors": res.get("errors") or []})
        logger.info("truenas auto-poll %s -> pushed (status=%s)",
                    appliance_id, res.get("status"))

    def start_background_loops(self) -> None:
        """Start the autonomous poll loop once (process-scoped, survives hub
        reconnects). Called from the standalone ``TruenasControlPlane`` AND the
        generic agent's role hook, so a role-hosted truenas spoke polls too."""
        if self._bg_tasks:
            return
        self._bg_tasks = [asyncio.create_task(self._truenas_poll_loop())]
        logger.info("truenas background poll loop started")

    # ── Logging helper: mask sensitive fields in any command data ───────────
    @staticmethod
    def _mask(data: Any) -> Any:
        """Return a copy of data with sensitive credential fields masked."""
        # Recurse into lists (e.g. UPDATE_CONFIG's appliances[]).
        if isinstance(data, list):
            return [TruenasSpoke._mask(v) for v in data]
        # Leave other non-dict values unchanged.
        if not isinstance(data, dict):
            return data
        # Replace sensitive values with a fixed mask, recursing into nested values.
        return {k: ("********" if k in _SENSITIVE else TruenasSpoke._mask(v))
                for k, v in data.items()}

    async def handle_command(self, command_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch a hub TRUENAS_* command to the engine.

        Command types (case-insensitive): ``UPDATE_CONFIG`` (store the fleet,
        credentials masked in logs), ``GET_VERSION``,
        ``TRUENAS_LIST_APPLIANCES`` (fleet summary + concurrent 6s probe),
        ``TRUENAS_PROBE``, ``TRUENAS_GET_POOLS``, ``TRUENAS_GET_DATASETS``,
        ``TRUENAS_GET_DISKS``, ``TRUENAS_GET_SHARES`` (kind=smb|nfs|iscsi),
        ``TRUENAS_GET_ALERTS``, ``TRUENAS_GET_SERVICES``,
        ``TRUENAS_GET_CAPACITY``, ``TRUENAS_POLL`` (system_info + pools +
        datasets + disks + shares + alerts + services + capacity in one call,
        partial results on partial failure). Write/management (gated by the
        hub's write_scope): ``TRUENAS_CREATE_DATASET``, ``TRUENAS_DELETE_DATASET``,
        ``TRUENAS_CREATE_SHARE``, ``TRUENAS_CREATE_SNAPSHOT``,
        ``TRUENAS_RUN_SCRUB``. Unknown commands return an ERROR envelope.
        """
        # Normalize the command name and log it with credentials masked.
        normalized_cmd = (command_type or "").upper()
        log_data = self._mask(data)
        logger.info(f"Handling Truenas Command: {command_type} with data {log_data}")
        # Dispatch, log the outcome and return the result.
        res = await self._dispatch_command(normalized_cmd, command_type, data)
        self._log_result(command_type, res)
        return res

    @staticmethod
    def _log_result(command_type: str, res: Dict[str, Any]) -> None:
        """Log every command's outcome: INFO on success, ERROR on failure. The
        ERROR line carries "error" so it surfaces in GET_ERROR_LOGS / the
        Error Log tab (same precedent as the nw spoke). ``errors`` (from
        TRUENAS_POLL) is surfaced as a sub-error count. Best-effort."""
        try:
            # Read status, message and any per-part errors.
            status = str((res or {}).get("status", "")).upper()
            msg = (res or {}).get("message", "")
            errors = (res or {}).get("errors") or []
            # Errors log as ERROR; everything else as INFO.
            if status == "ERROR" or errors:
                logger.error("truenas command %s result: error — %s%s", command_type,
                             msg or "failed",
                             f" ({len(errors)} sub-error(s))" if errors else "")
            else:
                logger.info("truenas command %s result: %s", command_type,
                            status.lower() or "ok")
        # Logging itself failed: note it at debug level.
        except Exception:
            logger.debug("truenas log_result failed", exc_info=True)

    async def _dispatch_command(self, normalized_cmd: str, command_type: str,
                                data: Dict[str, Any]) -> Dict[str, Any]:
        """Route normalized hub command to the corresponding TruenasEngine method."""
        # ── Lifecycle / config ──────────────────────────────────────────────
        if normalized_cmd == "UPDATE_CONFIG":
            # Read the new fleet and shared tenant id.
            appliances = (data or {}).get("appliances", []) if isinstance(data, dict) else []
            shared_tid = (data or {}).get("shared_tenant_id", "") if isinstance(data, dict) else ""
            # Log the fleet with credentials masked.
            summary = [{k: ("********" if k in _SENSITIVE else v)
                        for k, v in a.items()} for a in appliances] \
                if isinstance(appliances, list) else []
            logger.info(f"Updating truenas fleet configuration: "
                        f"{len(appliances if isinstance(appliances, list) else [])} "
                        f"appliance(s) -> {summary}")
            # Store the config and update the engine's fleet.
            self.config = data or {}
            self.engine.set_appliances(appliances if isinstance(appliances, list) else [],
                                       shared_tenant_id=shared_tid)
            # Confirm with the new appliance count.
            return {"status": "SUCCESS",
                    "message": "truenas configuration updated from Hub",
                    "appliance_count": len(self.engine.appliances)}

        # Version query.
        if normalized_cmd in ("GET_VERSION", "GET-VERSION"):
            return {"status": "SUCCESS", "version": self.get_version()}

        # ── Fleet ───────────────────────────────────────────────────────────
        if normalized_cmd == "TRUENAS_LIST_APPLIANCES":
            tenant = (data or {}).get("tenant") if isinstance(data, dict) else None
            return await self.engine.list_appliances(tenant)

        # ── Per-appliance (data carries appliance_id) ───────────────────────
        # Common fields: appliance id (or device_id) and tenant.
        d = data or {} if isinstance(data, dict) else {}
        appliance_id = d.get("appliance_id") or d.get("device_id") or ""
        tenant = d.get("tenant")

        # Read commands.
        if normalized_cmd == "TRUENAS_PROBE":
            return await self.engine.probe(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_POOLS":
            return await self.engine.get_pools(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_DATASETS":
            return await self.engine.get_datasets(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_DISKS":
            return await self.engine.get_disks(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_SHARES":
            # Share type defaults to SMB.
            kind = d.get("kind") or "smb"
            return await self.engine.get_shares(appliance_id, kind, tenant)

        if normalized_cmd == "TRUENAS_GET_ALERTS":
            return await self.engine.get_alerts(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_SERVICES":
            return await self.engine.get_services(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_GET_CAPACITY":
            return await self.engine.get_capacity(appliance_id, tenant)

        if normalized_cmd == "TRUENAS_POLL":
            return await self.engine.poll(appliance_id, tenant)

        # ── Write / management ─────────────────────────────────────────────
        # Write commands, with fallbacks for alternative field names.
        if normalized_cmd == "TRUENAS_CREATE_DATASET":
            return await self.engine.create_dataset(
                appliance_id, d.get("pool", ""), d.get("name", ""),
                d.get("options"), tenant)

        if normalized_cmd == "TRUENAS_DELETE_DATASET":
            return await self.engine.delete_dataset(
                appliance_id, d.get("dataset", "") or d.get("dataset_id", ""),
                d.get("options"), tenant)

        if normalized_cmd == "TRUENAS_CREATE_SHARE":
            return await self.engine.create_share(
                appliance_id, d.get("kind", "smb"),
                d.get("dataset", "") or d.get("path", ""),
                d.get("options"), tenant)

        if normalized_cmd == "TRUENAS_CREATE_SNAPSHOT":
            return await self.engine.create_snapshot(
                appliance_id, d.get("dataset", ""), d.get("name", ""),
                d.get("options"), tenant)

        if normalized_cmd == "TRUENAS_RUN_SCRUB":
            return await self.engine.run_scrub(
                appliance_id, d.get("pool", "") or d.get("pool_id", ""), tenant)

        # ── Unknown ─────────────────────────────────────────────────────────
        logger.warning(f"Unknown Truenas command type: {command_type}")
        return {"status": "ERROR",
                "message": f"Command {command_type} not supported by truenas module"}

    async def get_status(self) -> Dict[str, Any]:
        """Native LM status report for the truenas fleet."""
        return {
            "spoke_id": self.spoke_id,
            "module": "truenas",
            "appliance_count": len(self.engine.appliances),
            "connection": "CONNECTED",
        }

    def get_version(self) -> str:
        """Current truenas module version (repo-root VERSION).

        Reads ``<repo>/VERSION`` (one dir above ``src/``). Same path pattern as
        the nw/opnsense spokes.
        """
        try:
            # Read VERSION from the repo root; log and return "unknown" on failure.
            return (Path(__file__).resolve().parent.parent / "VERSION").read_text().strip()
        except Exception:
            logger.exception("Failed to read VERSION file")
            return "unknown"
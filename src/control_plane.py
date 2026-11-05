"""Entry point and control plane for the TrueNAS (storage) spoke.

Connects to the hub, registers the TrueNAS spoke under module type "storage"
and polls each configured appliance on its own schedule, pushing results to
the hub.
"""
# Dependency self-heal — MUST run before the third-party imports below. A skewed
# auto-update / partial install can leave the venv missing a declared dep, which
# would hard-crash at import and crash-loop the unit under Restart=always.
# dep_guard is stdlib-only; it find_spec-checks requirements.txt and pip-installs
# any missing. Best-effort — an unavailable dep_guard is skipped, never fatal.
import os as _os
try:
    try:
        from core.src.dep_guard import ensure_requirements as _ensure_requirements
    except ImportError:
        from dep_guard import ensure_requirements as _ensure_requirements
    _ensure_requirements(_os.path.join(
        _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "requirements.txt"))
except Exception:
    pass

# Standard library imports.
import logging
import argparse
import asyncio
import time
from typing import Dict, Any
# Base control plane (from the core package or a flat layout) and the spoke.
try:
    from core.src.messaging.control_plane import BaseControlPlane
except ImportError:
    from messaging.control_plane import BaseControlPlane
from truenas_spoke import TruenasSpoke

# Shared logging setup, with a basic fallback when it is not installed.
try:
    from logging_setup import configure_logging
except ImportError:
    try:
        from core.src.logging_setup import configure_logging
    except ImportError:
        import logging as _logging
        _FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        _DFMT = '%Y-%m-%d %H:%M:%S'
        def configure_logging(default_level=_logging.INFO, *, log_file=None, **_):
            """Fallback logging: basicConfig, also writing to log_file if given."""
            handlers = ([_logging.FileHandler(log_file), _logging.StreamHandler()]
                        if log_file else None)
            _logging.basicConfig(level=default_level, force=True,
                                 format=_FMT, datefmt=_DFMT, handlers=handlers)
# Configure root logging at boot (same rationale as nw control_plane).
configure_logging()
logger = logging.getLogger("TruenasControlPlane")


class TruenasControlPlane(BaseControlPlane):
    """Control Plane for the TrueNAS (storage) module.

    Inherits core connectivity and routing from BaseControlPlane. The spoke
    advertises module_type "storage" so the hub routes TRUENAS_* commands +
    pushes the truenas_appliances fleet via UPDATE_CONFIG on
    connect/approve/reconnect.
    """
    def get_service_name(self) -> str:
        """Return the systemd service name for this spoke."""
        return "lm-truenas"

    def __init__(self, spoke_id: str, secret: str, hub_secret: str = None,
                 hub_url: str = None, config: Dict[str, Any] = None):
        """Store the config, start the base control plane and set the module type."""
        # Initialize attributes before calling super().__init__ so background
        # workers started by the base class see them.
        self.config = config or {}
        super().__init__(spoke_id, secret, hub_secret, hub_url)
        self.module_type = "storage"

    # Poll-loop tick granularity + per-appliance interval floor (seconds). The
    # cadence itself is per-appliance (``poll_interval`` on each
    # truenas_appliances entry); these bound the scheduler, not the user's choice.
    _TRUENAS_POLL_TICK = 10
    _TRUENAS_POLL_FLOOR = 60          # TrueNAS pool.query is heavier than an
                                      # ARP probe — keep a 1m floor.
    # Default cadence when an appliance has no poll_interval set. An explicit
    # 0 (the UI "Off" choice) disables; only an absent/blank value defaults.
    _TRUENAS_POLL_DEFAULT = 900       # 15 minutes

    async def run_hub_mode(self):
        """Native LM Spoke behavior."""
        logger.info(f"Starting TrueNAS Module in HUB MODE -> {self.hub_url}")

        truenas_spoke = TruenasSpoke(self.spoke_id, self.config)
        # Create the TrueNAS spoke and register it under "storage".
        self.register_module("storage", truenas_spoke)

        # Autonomous per-appliance polling (spoke-driven). Started before the
        # main loop; it idles until connected + an appliance sets poll_interval.
        asyncio.create_task(self._truenas_poll_loop())

        # Delegate to BaseControlPlane's main loop
        await self.run()

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
                module = self.modules.get("storage")
                engine = getattr(module, "engine", None)
                if engine is None or getattr(self, "_hub_ws", None) is None:
                    continue
                # Module-wide default interval; blank uses the built-in default.
                mod_raw = (getattr(module, "config", {}) or {}).get("default_poll_interval")
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
                    # New appliances are scheduled for later; due ones are queued and rescheduled.
                    if deadline is None:          # first sight → stagger
                        next_due[aid] = now + interval
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
        # Get the engine; nothing to do if it is missing.
        module = self.modules.get("storage")
        engine = getattr(module, "engine", None)
        if engine is None:
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
        await self.send_to_hub("TRUENAS_POLL_RESULT",
                               {"appliance_id": appliance_id, "data": data})
        logger.info("truenas auto-poll %s -> pushed (status=%s)",
                    appliance_id, res.get("status"))


if __name__ == "__main__":
    # Command-line options.
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="Spoke ID")
    parser.add_argument("--secret", default=None,
                        help="Authentication secret (required; or set SPOKE_SECRET)")
    parser.add_argument("--hub-secret", nargs='?', default="", const="",
                        help="Hub authentication secret for mutual auth")
    parser.add_argument("--hub", required=True, help="Hub WebSocket URL")
    # Parse; the secret can come from --secret or SPOKE_SECRET.
    args = parser.parse_args()
    secret = args.secret or _os.getenv("SPOKE_SECRET")
    if not secret:
        parser.error("authentication secret required: pass --secret or set SPOKE_SECRET")

    # Build the control plane and run it until stopped or interrupted.
    cp = TruenasControlPlane(args.id, secret, args.hub_secret, args.hub)
    try:
        asyncio.run(cp.run_hub_mode())
    except KeyboardInterrupt:
        pass
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

    async def run_hub_mode(self):
        """Native LM Spoke behavior."""
        logger.info(f"Starting TrueNAS Module in HUB MODE -> {self.hub_url}")

        truenas_spoke = TruenasSpoke(self.spoke_id, self.config)
        # Create the TrueNAS spoke and register it under "storage".
        self.register_module("storage", truenas_spoke)

        # The autonomous poll loop lives on the MODULE so it runs whether
        # truenas is standalone (here) or hosted as a role on the generic agent
        # (RoleConnection._start_role_local_services calls the same hook).
        truenas_spoke.control_plane = self
        truenas_spoke.start_background_loops()

        # Delegate to BaseControlPlane's main loop
        await self.run()



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
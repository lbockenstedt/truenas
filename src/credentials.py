"""Per-tenant TrueNAS API-key credential store for the truenas spoke.

MULTI-TENANT: each tenant keeps its OWN set of named TrueNAS appliance
credentials (host + API key + verify_ssl). Stored one file per tenant, 0600,
under ``/etc/lm-truenas/credentials/<tenant>.json`` (override the dir with
``LM_TRUENAS_CREDS_DIR``). Every function takes ``tenant_id``; the hub routes
derive it from the authenticated session (never client-supplied) so one tenant
can't read another's keys.

A stored credential is ``{name, host, api_key, verify_ssl, auth_mechanism}``.
``api_key`` is the secret — withheld from ``list_public()`` (only an "is set"
flag) and never logged. ``upsert()`` sentinel-merges the key, so a partial edit
(omitting the key) keeps the stored one. Mirrors le/src/dns_credentials.py.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger("TruenasCredentials")

_DIR = os.getenv("LM_TRUENAS_CREDS_DIR", "/etc/lm-truenas/credentials")

# Auth mechanisms the official truenas_api_client supports for an API key.
# ``PLAIN`` is required for TrueNAS Core / 24.x (legacy DDP) and some 25.x
# boxes; ``SCRAM`` (SCRAM-SHA-512) is the default on 26+. ``auto`` lets the
# client negotiate (the spoke picks the default in truenas_client.py).
_AUTH_MECHANISMS = ("auto", "PLAIN", "SCRAM")

# Fields that are secret (withheld from list_public + sentinel-merged on upsert).
_SECRET_FIELDS = ("api_key",)


def _safe_tenant(tenant_id: str) -> str:
    """Sanitise a tenant id into a filename component — defends the store dir
    against path traversal from an unexpected id. Empty → 'default'."""
    t = re.sub(r"[^A-Za-z0-9._-]", "_", str(tenant_id or "").strip())
    return t or "default"


def _store_path(tenant_id: str) -> str:
    return os.path.join(_DIR, f"{_safe_tenant(tenant_id)}.json")


def _load(tenant_id: str) -> List[Dict[str, Any]]:
    try:
        with open(_store_path(tenant_id), encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except FileNotFoundError:
        return []
    except Exception as exc:  # noqa: BLE001 — corrupt store → start empty, don't crash
        logger.warning("truenas credentials: could not read %s: %s",
                       _store_path(tenant_id), exc)
        return []


def _save(tenant_id: str, creds: List[Dict[str, Any]]) -> None:
    os.makedirs(_DIR, exist_ok=True)
    try:
        os.chmod(_DIR, 0o700)
    except OSError:
        pass
    path = _store_path(tenant_id)
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(creds, f, indent=2)
    os.replace(tmp, path)
    os.chmod(path, 0o600)


def _normalize_mechanism(raw: Any) -> str:
    m = str(raw or "auto").strip()
    return m if m in _AUTH_MECHANISMS else "auto"


def list_public(tenant_id: str) -> List[Dict[str, Any]]:
    """This tenant's credentials WITHOUT secret values — safe for the browser.
    Each: {name, host, verify_ssl, auth_mechanism, secrets_set {api_key: bool}}."""
    out: List[Dict[str, Any]] = []
    for c in _load(tenant_id):
        out.append({
            "name": c.get("name"),
            "host": c.get("host"),
            "verify_ssl": bool(c.get("verify_ssl", True)),
            "auth_mechanism": c.get("auth_mechanism", "auto"),
            "secrets_set": {"api_key": bool(c.get("api_key"))},
        })
    return sorted(out, key=lambda e: (e.get("host") or "", e.get("name") or ""))


def get(tenant_id: str, name: str) -> Optional[Dict[str, Any]]:
    for c in _load(tenant_id):
        if c.get("name") == name:
            return c
    return None


def upsert(tenant_id: str, name: str, host: str, api_key: str,
           verify_ssl: bool = True, auth_mechanism: str = "auto") -> None:
    """Add or update one of this tenant's named TrueNAS credentials. Sentinel-
    merge: an empty/absent ``api_key`` KEEPS the stored key (so a partial edit
    that re-saves host/verify_ssl doesn't wipe the key)."""
    name = (name or "").strip()
    host = (host or "").strip()
    if not name:
        raise ValueError("credential name is required")
    if not host:
        raise ValueError("credential host is required")
    mech = _normalize_mechanism(auth_mechanism)
    creds = _load(tenant_id)
    existing = next((c for c in creds if c.get("name") == name), None)
    merged_api_key = api_key
    if (merged_api_key in (None, "")) and existing:
        merged_api_key = existing.get("api_key", "")  # keep stored key
    if merged_api_key in (None, ""):
        raise ValueError("api_key is required (set once; later edits may omit it)")
    # Sentinel-merge the auth mechanism too: a partial edit that doesn't
    # specify auth_mechanism (resolves to "auto") keeps the stored value, so
    # re-saving host/verify_ssl doesn't reset a PLAIN/SCRAM choice.
    merged_mech = mech
    if mech == "auto" and existing and existing.get("auth_mechanism") in ("PLAIN", "SCRAM"):
        merged_mech = existing["auth_mechanism"]
    entry = {
        "name": name,
        "host": host,
        "api_key": merged_api_key,
        "verify_ssl": bool(verify_ssl),
        "auth_mechanism": merged_mech,
    }
    creds = [c for c in creds if c.get("name") != name]
    creds.append(entry)
    _save(tenant_id, creds)


def delete(tenant_id: str, name: str) -> bool:
    creds = _load(tenant_id)
    kept = [c for c in creds if c.get("name") != name]
    if len(kept) == len(creds):
        return False
    _save(tenant_id, kept)
    return True


def materialize(tenant_id: str, name: str) -> Dict[str, Any]:
    """Turn one of this tenant's stored credentials into the kwargs the
    TrueNASClient needs: {host, api_key, verify_ssl, auth_mechanism}. Raises
    KeyError if the name is unknown for this tenant."""
    c = get(tenant_id, name)
    if not c:
        raise KeyError(f"no TrueNAS credential named {name!r} for tenant {tenant_id!r}")
    return {
        "host": c.get("host", ""),
        "api_key": c.get("api_key", ""),
        "verify_ssl": bool(c.get("verify_ssl", True)),
        "auth_mechanism": c.get("auth_mechanism", "auto"),
    }
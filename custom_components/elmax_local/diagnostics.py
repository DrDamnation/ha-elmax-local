"""Diagnostic dump for Elmax Local."""
from __future__ import annotations

import time
from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN
from .coordinator import ElmaxLocalCoordinator

TO_REDACT = {"panel_pin", "_pin", "_token", "panel_id"}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry,
) -> dict[str, Any]:
    coord: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    transports_dump = [
        {
            "name": t.name,
            "state": t.state.value,
            "capabilities": [c.value for c in t.capabilities],
        }
        for t in coord.registry._transports
    ]
    auth_dump = {
        "expires_in": max(0, int(coord.auth._expiry - time.time())) if coord.auth._token else 0,
        "login_fail_count": coord.auth._login_fail_count,
        "blocked_until": coord.auth._blocked_until,
    }
    entity_counts = {
        "zones": len(coord.data.zones) if coord.data else 0,
        "areas": len(coord.data.areas) if coord.data else 0,
        "outputs": len(coord.data.outputs) if coord.data else 0,
        "scenarios": len(coord.data.scenarios) if coord.data else 0,
    }
    raw = {
        "panel_id_suffix": coord.panel_id[-6:] if coord.panel_id else None,
        "host": coord.host,
        "panel_info": coord.data.panel_info if coord.data else {},
        "transports": transports_dump,
        "auth": auth_dump,
        "last_update_source": coord.data.last_update_source if coord.data else None,
        "entity_counts": entity_counts,
    }
    return async_redact_data(raw, TO_REDACT)

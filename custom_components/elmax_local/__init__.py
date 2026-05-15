"""Elmax Local — multi-transport integration for Elmax alarm panels."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Elmax Local from a config entry. STUB — implemented in Task 15."""
    hass.data.setdefault(DOMAIN, {})
    _LOGGER.debug("async_setup_entry stub for %s", entry.entry_id)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry. STUB."""
    return True

"""Elmax MQTT - Local integration for Elmax alarm panels via MQTT + HTTP."""

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import (
    DOMAIN,
    PLATFORMS,
    CONF_PANEL_ID,
    CONF_PANEL_PIN,
    CONF_PANEL_HOST,
    CONF_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
)
from .coordinator import ElmaxMqttCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Elmax MQTT from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    coordinator = ElmaxMqttCoordinator(
        hass,
        entry.data[CONF_PANEL_ID],
        entry.data[CONF_PANEL_PIN],
        entry.data[CONF_PANEL_HOST],
        entry.options.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL),
    )

    try:
        await coordinator.async_setup()
    except Exception as err:
        raise ConfigEntryNotReady(f"Cannot connect to Elmax panel: {err}") from err

    hass.data[DOMAIN][entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_shutdown()
    return unload_ok

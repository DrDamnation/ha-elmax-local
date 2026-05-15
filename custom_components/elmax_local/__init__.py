"""Elmax Local — multi-transport integration for Elmax alarm panels."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady

from .auth import ElmaxAuthError
from .const import (
    CONF_ENABLE_MQTT, CONF_ENABLE_WS, CONF_PANEL_HOST, CONF_PANEL_ID,
    CONF_PANEL_PIN, CONF_RECONCILE_INTERVAL, DEFAULT_RECONCILE_INTERVAL,
    DOMAIN, PLATFORMS,
)
from .coordinator import ElmaxLocalCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    coordinator = ElmaxLocalCoordinator(
        hass,
        panel_id=entry.data[CONF_PANEL_ID],
        pin=entry.data[CONF_PANEL_PIN],
        host=entry.data[CONF_PANEL_HOST],
        reconcile_interval=entry.options.get(
            CONF_RECONCILE_INTERVAL, DEFAULT_RECONCILE_INTERVAL),
        enable_ws=entry.options.get(CONF_ENABLE_WS, True),
        enable_mqtt=entry.options.get(CONF_ENABLE_MQTT, True),
    )

    try:
        await coordinator.async_setup()
    except ElmaxAuthError as err:
        if "401" in str(err) or "403" in str(err):
            raise ConfigEntryAuthFailed(str(err)) from err
        raise ConfigEntryNotReady(str(err)) from err
    except Exception as err:
        await coordinator.async_shutdown()
        raise ConfigEntryNotReady(f"Cannot connect: {err}") from err

    if not coordinator.data or not coordinator.data.areas:
        await coordinator.async_shutdown()
        raise ConfigEntryNotReady("No data received from Elmax panel")

    hass.data[DOMAIN][entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_shutdown()
    return unload_ok


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload entry on options change."""
    await hass.config_entries.async_reload(entry.entry_id)

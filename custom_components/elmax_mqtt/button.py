"""Elmax MQTT button entities for alarm scenarios."""

import logging

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, HTTP_CMD_TRIGGER

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Elmax scenario buttons from config entry."""
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxScenarioButton(coordinator, scenario)
        for scenario in coordinator.scenarios
        if scenario.get("visibile", True)
    )


class ElmaxScenarioButton(ButtonEntity):
    """Representation of an Elmax scenario."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, scenario: dict):
        self.coordinator = coordinator
        self._endpoint_id = scenario["endpointId"]
        self._attr_unique_id = f"{DOMAIN}_{self._endpoint_id}"
        self._attr_name = scenario.get("nome", f"Scenario {scenario['indice']}")
        self._attr_icon = "mdi:shield-home"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.panel_id)},
            name=f"Elmax {coordinator.panel_id[-6:]}",
            manufacturer="Elmax",
            model=coordinator.panel_info.get("release", "Phantom64"),
            sw_version=coordinator.panel_info.get("release_accessorio", ""),
        )

    async def async_press(self):
        await self.coordinator.async_send_command(self._endpoint_id, HTTP_CMD_TRIGGER)

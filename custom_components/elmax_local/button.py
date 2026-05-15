"""Elmax Local button entities for scenarios."""
from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .alarm_control_panel import _device_info
from .const import DOMAIN
from .coordinator import ElmaxLocalCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxScenarioButton(coordinator, eid)
        for eid, sc in coordinator.data.scenarios.items()
        if sc.get("visibile", True)
    )


class ElmaxScenarioButton(CoordinatorEntity[ElmaxLocalCoordinator], ButtonEntity):
    _attr_has_entity_name = True
    _attr_icon = "mdi:shield-home"

    def __init__(self, coordinator: ElmaxLocalCoordinator, endpoint_id: str):
        super().__init__(coordinator)
        self._endpoint_id = endpoint_id
        self._attr_unique_id = f"{DOMAIN}_{endpoint_id}"
        sc = coordinator.data.scenarios[endpoint_id]
        self._attr_name = sc.get("nome") or f"Scenario {sc.get('indice', '?')}"
        self._attr_device_info = _device_info(coordinator)

    async def async_press(self) -> None:
        # Scenari non hanno cmd; il payload spec mostra POST /cmd/{eid}/
        # senza segmento. Inviamo None per significare "trigger".
        await self.coordinator.async_send_command(self._endpoint_id, None)

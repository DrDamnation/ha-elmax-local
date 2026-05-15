"""Elmax Local switch entities for panel outputs."""
from __future__ import annotations

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .alarm_control_panel import _device_info
from .const import CMD_OUTPUT_OFF, CMD_OUTPUT_ON, DOMAIN
from .coordinator import ElmaxLocalCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxOutputSwitch(coordinator, eid)
        for eid, out in coordinator.data.outputs.items()
        if out.get("visibile", True)
    )


class ElmaxOutputSwitch(CoordinatorEntity[ElmaxLocalCoordinator], SwitchEntity):
    _attr_has_entity_name = True

    def __init__(self, coordinator: ElmaxLocalCoordinator, endpoint_id: str):
        super().__init__(coordinator)
        self._endpoint_id = endpoint_id
        self._attr_unique_id = f"{DOMAIN}_{endpoint_id}"
        out = coordinator.data.outputs[endpoint_id]
        self._attr_name = out.get("nome") or f"Uscita {out.get('indice', '?')}"
        self._attr_device_info = _device_info(coordinator)

    @property
    def available(self) -> bool:
        return (super().available
                and self.coordinator.data is not None
                and self._endpoint_id in self.coordinator.data.outputs)

    @property
    def is_on(self) -> bool | None:
        if self.coordinator.data is None:
            return None
        out = self.coordinator.data.outputs.get(self._endpoint_id)
        return out.get("aperta") if out else None

    async def async_turn_on(self, **kwargs) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_OUTPUT_ON)

    async def async_turn_off(self, **kwargs) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_OUTPUT_OFF)

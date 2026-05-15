"""Elmax Local binary sensor entities for alarm zones."""
from __future__ import annotations

import logging

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass, BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .alarm_control_panel import _device_info
from .const import DOMAIN
from .coordinator import ElmaxLocalCoordinator

_LOGGER = logging.getLogger(__name__)


def _infer_device_class(name: str) -> BinarySensorDeviceClass:
    n = name.lower()
    if "porta" in n:
        return BinarySensorDeviceClass.DOOR
    if "fines" in n:
        return BinarySensorDeviceClass.WINDOW
    if n.startswith("m ") or n.startswith("m v"):
        return BinarySensorDeviceClass.DOOR
    return BinarySensorDeviceClass.MOTION


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxZoneSensor(coordinator, eid)
        for eid, zone in coordinator.data.zones.items()
        if zone.get("visibile", True)
    )


class ElmaxZoneSensor(CoordinatorEntity[ElmaxLocalCoordinator], BinarySensorEntity):
    _attr_has_entity_name = True

    def __init__(self, coordinator: ElmaxLocalCoordinator, endpoint_id: str):
        super().__init__(coordinator)
        self._endpoint_id = endpoint_id
        self._attr_unique_id = f"{DOMAIN}_{endpoint_id}"
        zone = coordinator.data.zones[endpoint_id]
        self._attr_name = zone.get("nome") or f"Zona {zone.get('indice', '?')}"
        self._attr_device_class = _infer_device_class(self._attr_name)
        self._attr_device_info = _device_info(coordinator)

    @property
    def available(self) -> bool:
        return (super().available
                and self.coordinator.data is not None
                and self._endpoint_id in self.coordinator.data.zones)

    @property
    def is_on(self) -> bool | None:
        if self.coordinator.data is None:
            return None
        zone = self.coordinator.data.zones.get(self._endpoint_id)
        return zone.get("aperta") if zone else None

    @property
    def extra_state_attributes(self) -> dict:
        if self.coordinator.data is None:
            return {}
        z = self.coordinator.data.zones.get(self._endpoint_id, {})
        return {"esclusa": z.get("esclusa"), "indice": z.get("indice")}

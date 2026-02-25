"""Elmax MQTT binary sensor entities for alarm zones."""

import logging

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, SIGNAL_UPDATE

_LOGGER = logging.getLogger(__name__)


def _get_device_class(name: str) -> BinarySensorDeviceClass:
    """Infer device class from zone name."""
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
    """Set up Elmax zone sensors from config entry."""
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxZoneSensor(coordinator, zone)
        for zone in coordinator.zones
        if zone.get("visibile", True)
    )


class ElmaxZoneSensor(BinarySensorEntity):
    """Representation of an Elmax alarm zone."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, zone: dict):
        self.coordinator = coordinator
        self._endpoint_id = zone["endpointId"]
        self._attr_unique_id = f"{DOMAIN}_{self._endpoint_id}"
        self._attr_name = zone.get("nome", f"Zona {zone['indice']}")
        self._attr_device_class = _get_device_class(self._attr_name)
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.panel_id)},
            name=f"Elmax {coordinator.panel_id[-6:]}",
            manufacturer="Elmax",
            model=coordinator.panel_info.get("release", "Phantom64"),
            sw_version=coordinator.panel_info.get("release_accessorio", ""),
        )

    async def async_added_to_hass(self):
        """Register dispatcher callback."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                SIGNAL_UPDATE.format(panel_id=self.coordinator.panel_id),
                self._handle_update,
            )
        )

    @callback
    def _handle_update(self):
        self.async_write_ha_state()

    @property
    def is_on(self) -> bool | None:
        """Return true if zone is open/active."""
        zone = self.coordinator.get_zone(self._endpoint_id)
        if zone:
            return zone.get("aperta", False)
        return None

    @property
    def extra_state_attributes(self) -> dict:
        """Return zone attributes."""
        zone = self.coordinator.get_zone(self._endpoint_id)
        if zone:
            return {
                "esclusa": zone.get("esclusa", False),
                "indice": zone.get("indice"),
            }
        return {}

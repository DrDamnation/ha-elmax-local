"""Elmax MQTT switch entities for panel outputs."""

import logging

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, HTTP_CMD_ON, HTTP_CMD_OFF, SIGNAL_UPDATE

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Elmax output switches from config entry."""
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxOutputSwitch(coordinator, output)
        for output in coordinator.outputs
        if output.get("visibile", True)
    )


class ElmaxOutputSwitch(SwitchEntity):
    """Representation of an Elmax panel output."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, output: dict):
        self.coordinator = coordinator
        self._endpoint_id = output["endpointId"]
        self._attr_unique_id = f"{DOMAIN}_{self._endpoint_id}"
        self._attr_name = output.get("nome", f"Uscita {output['indice']}")
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.panel_id)},
            name=f"Elmax {coordinator.panel_id[-6:]}",
            manufacturer="Elmax",
            model=coordinator.panel_info.get("release", "Phantom64"),
            sw_version=coordinator.panel_info.get("release_accessorio", ""),
        )

    async def async_added_to_hass(self):
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
        output = self.coordinator.get_output(self._endpoint_id)
        if output:
            return output.get("aperta", False)
        return None

    async def async_turn_on(self, **kwargs):
        await self.coordinator.async_send_command(self._endpoint_id, HTTP_CMD_ON)

    async def async_turn_off(self, **kwargs):
        await self.coordinator.async_send_command(self._endpoint_id, HTTP_CMD_OFF)

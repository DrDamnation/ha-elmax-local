"""Elmax MQTT alarm control panel entities."""

import logging

from homeassistant.components.alarm_control_panel import (
    AlarmControlPanelEntity,
    AlarmControlPanelEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    DOMAIN,
    ELMAX_TO_HA_STATE,
    HTTP_CMD_ARM_TOTALLY,
    HTTP_CMD_ARM_P1_P2,
    HTTP_CMD_ARM_P2,
    HTTP_CMD_DISARM,
    SIGNAL_UPDATE,
)

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Elmax alarm panels from config entry."""
    coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxAlarmPanel(coordinator, area)
        for area in coordinator.areas
        if area.get("visibile", True)
    )


class ElmaxAlarmPanel(AlarmControlPanelEntity):
    """Representation of an Elmax alarm area."""

    _attr_has_entity_name = True
    _attr_supported_features = (
        AlarmControlPanelEntityFeature.ARM_AWAY
        | AlarmControlPanelEntityFeature.ARM_HOME
        | AlarmControlPanelEntityFeature.ARM_NIGHT
    )
    _attr_code_arm_required = False

    def __init__(self, coordinator, area: dict):
        self.coordinator = coordinator
        self._endpoint_id = area["endpointId"]
        self._attr_unique_id = f"{DOMAIN}_{self._endpoint_id}"
        self._attr_name = area.get("nome", f"Area {area['indice']}")
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
    def alarm_state(self) -> str | None:
        area = self.coordinator.get_area(self._endpoint_id)
        if area:
            return ELMAX_TO_HA_STATE.get(area.get("stato", 0))
        return None

    async def async_alarm_disarm(self, code=None):
        await self.coordinator.async_send_command(self._endpoint_id, HTTP_CMD_DISARM)

    async def async_alarm_arm_away(self, code=None):
        await self.coordinator.async_send_command(self._endpoint_id, HTTP_CMD_ARM_TOTALLY)

    async def async_alarm_arm_home(self, code=None):
        await self.coordinator.async_send_command(self._endpoint_id, HTTP_CMD_ARM_P1_P2)

    async def async_alarm_arm_night(self, code=None):
        await self.coordinator.async_send_command(self._endpoint_id, HTTP_CMD_ARM_P2)

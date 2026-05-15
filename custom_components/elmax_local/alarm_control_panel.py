"""Elmax Local alarm control panel entities."""
from __future__ import annotations

import logging

from homeassistant.components.alarm_control_panel import (
    AlarmControlPanelEntity, AlarmControlPanelEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    CMD_AREA_ARM_P1P2, CMD_AREA_ARM_P2, CMD_AREA_ARM_TOTAL,
    CMD_AREA_DISARM, DOMAIN, ELMAX_TO_HA_STATE,
)
from .coordinator import ElmaxLocalCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxAlarmPanel(coordinator, eid)
        for eid, area in coordinator.data.areas.items()
        if area.get("visibile", True)
    )


class ElmaxAlarmPanel(CoordinatorEntity[ElmaxLocalCoordinator],
                      AlarmControlPanelEntity):
    _attr_has_entity_name = True
    _attr_supported_features = (
        AlarmControlPanelEntityFeature.ARM_AWAY
        | AlarmControlPanelEntityFeature.ARM_HOME
        | AlarmControlPanelEntityFeature.ARM_NIGHT
    )
    _attr_code_arm_required = False

    def __init__(self, coordinator: ElmaxLocalCoordinator, endpoint_id: str):
        super().__init__(coordinator)
        self._endpoint_id = endpoint_id
        self._attr_unique_id = f"{DOMAIN}_{endpoint_id}"
        area = coordinator.data.areas[endpoint_id]
        self._attr_name = area.get("nome") or f"Area {area.get('indice', '?')}"
        self._attr_device_info = _device_info(coordinator)

    @property
    def available(self) -> bool:
        return (super().available
                and self.coordinator.data is not None
                and self._endpoint_id in self.coordinator.data.areas)

    @property
    def alarm_state(self) -> str | None:
        if self.coordinator.data is None:
            return None
        area = self.coordinator.data.areas.get(self._endpoint_id)
        if area is None:
            return None
        return ELMAX_TO_HA_STATE.get(area.get("stato", 0))

    @property
    def extra_state_attributes(self) -> dict:
        if self.coordinator.data is None:
            return {}
        area = self.coordinator.data.areas.get(self._endpoint_id, {})
        return {
            "zoneBmask": area.get("zoneBmask"),
            "statoSessione": area.get("statoSessione"),
            "indice": area.get("indice"),
        }

    async def async_alarm_disarm(self, code: str | None = None) -> None:
        # Always use panel PIN for disarm (not user-provided code)
        await self.coordinator.async_send_command(
            self._endpoint_id, CMD_AREA_DISARM, code=self.coordinator.auth._pin
        )

    async def async_alarm_arm_away(self, code: str | None = None) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_AREA_ARM_TOTAL)

    async def async_alarm_arm_home(self, code: str | None = None) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_AREA_ARM_P1P2)

    async def async_alarm_arm_night(self, code: str | None = None) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_AREA_ARM_P2)


def _device_info(coordinator: ElmaxLocalCoordinator) -> DeviceInfo:
    return DeviceInfo(
        identifiers={(DOMAIN, coordinator.panel_id)},
        name=f"Elmax {coordinator.panel_id[-6:]}",
        manufacturer="Elmax",
        model=coordinator.data.panel_info.get("release", "Phantom64") if coordinator.data else "Phantom64",
        sw_version=coordinator.data.panel_info.get("release_accessorio", "") if coordinator.data else "",
    )

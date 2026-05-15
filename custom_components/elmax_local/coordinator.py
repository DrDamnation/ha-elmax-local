"""ElmaxLocalCoordinator — orchestrates transports and updates entities."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import timedelta

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .auth import AuthManager
from .const import DEFAULT_RECONCILE_INTERVAL
from .transport import TransportRegistry
from .transport.http import HttpTransport
from .transport.mqtt import MqttTransport
from .transport.websocket import WebSocketTransport

_LOGGER = logging.getLogger(__name__)

PUSH_FRESHNESS_RATIO = 0.5  # skip poll if push < interval*0.5 ago


@dataclass
class ElmaxState:
    panel_info: dict = field(default_factory=dict)
    zones: dict[str, dict] = field(default_factory=dict)
    areas: dict[str, dict] = field(default_factory=dict)
    outputs: dict[str, dict] = field(default_factory=dict)
    scenarios: dict[str, dict] = field(default_factory=dict)
    last_update_source: str = ""
    last_update_ts: float = 0


class ElmaxLocalCoordinator(DataUpdateCoordinator[ElmaxState]):
    """Coordinates push + poll across transports."""

    def __init__(
        self,
        hass: HomeAssistant,
        panel_id: str,
        pin: str,
        host: str,
        reconcile_interval: int = DEFAULT_RECONCILE_INTERVAL,
        enable_ws: bool = True,
        enable_mqtt: bool = True,
    ):
        super().__init__(
            hass, _LOGGER,
            name=f"Elmax {panel_id}",
            update_interval=timedelta(seconds=reconcile_interval),
        )
        self.panel_id = panel_id
        self.host = host
        self.auth = AuthManager(hass, host, pin)

        transports = [HttpTransport(hass, host)]  # always on
        if enable_mqtt:
            transports.append(MqttTransport(hass, panel_id))
        if enable_ws:
            transports.append(WebSocketTransport(hass, host))
        self.registry = TransportRegistry(transports)

    async def async_setup(self) -> None:
        """Called by async_setup_entry. Task 15 wires this."""
        await self.registry.async_start_all(self.auth, self._on_push_state_update)
        await self.async_config_entry_first_refresh()

    async def async_shutdown(self) -> None:
        await self.registry.async_stop_all()
        await self.auth.async_close()

    async def _async_update_data(self) -> ElmaxState:
        """Stub — Task 9 implements."""
        raise UpdateFailed("Implemented in Task 9")

    async def _on_push_state_update(self, raw: dict) -> None:
        """Stub — Task 9 implements."""
        return None

    def _push_is_fresh(self) -> bool:
        if not self.data or self.data.last_update_ts == 0:
            return False
        if not self.update_interval:
            return False
        elapsed = time.time() - self.data.last_update_ts
        return elapsed < self.update_interval.total_seconds() * PUSH_FRESHNESS_RATIO

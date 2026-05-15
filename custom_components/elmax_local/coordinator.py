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

    def _parse(self, raw: dict, source: str = "poll") -> ElmaxState:
        """Normalize /api/v2/discovery payload to ElmaxState.
        Indexes lists by endpointId for O(1) lookup by entities."""
        return ElmaxState(
            panel_info={
                "centrale": raw.get("centrale", self.panel_id),
                "release": raw.get("release", ""),
                "tipo_accessorio": raw.get("tipo_accessorio", ""),
                "release_accessorio": raw.get("release_accessorio", ""),
                "tappFeature": raw.get("tappFeature", False),
                "sceneFeature": raw.get("sceneFeature", False),
            },
            zones={z["endpointId"]: z for z in raw.get("zone", [])
                   if "endpointId" in z},
            areas={a["endpointId"]: a for a in raw.get("aree", [])
                   if "endpointId" in a},
            outputs={o["endpointId"]: o for o in raw.get("uscite", [])
                     if "endpointId" in o},
            scenarios={s["endpointId"]: s for s in raw.get("scenari", [])
                       if "endpointId" in s},
            last_update_source=source,
            last_update_ts=time.time(),
        )

    async def _on_push_state_update(self, raw: dict) -> None:
        """Called by PUSH transports on each spontaneous state update."""
        push = self.registry.get_active_push_transports()
        source = push[0].name if push else "push"
        new_state = self._parse(raw, source=source)
        self.async_set_updated_data(new_state)

    async def _async_update_data(self) -> ElmaxState:
        """Reconciliation poll. Skipped if push is fresh."""
        if self._push_is_fresh():
            return self.data
        raw = await self.registry.async_fetch_state()
        if raw is None:
            if self.data is not None:
                _LOGGER.debug("Poll failed; keeping last known state")
                return self.data
            raise UpdateFailed("All polling transports failed; no prior state")
        return self._parse(raw, source="poll")

    def _push_is_fresh(self) -> bool:
        if not self.data or self.data.last_update_ts == 0:
            return False
        if not self.update_interval:
            return False
        elapsed = time.time() - self.data.last_update_ts
        return elapsed < self.update_interval.total_seconds() * PUSH_FRESHNESS_RATIO

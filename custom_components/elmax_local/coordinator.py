"""ElmaxLocalCoordinator — orchestrates transports and updates entities."""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import timedelta

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .auth import AuthManager
from .const import (
    DEFAULT_RECONCILE_INTERVAL,
    STALE_UNAVAILABLE_RATIO,
    STALE_WARN_RATIO,
)
from .transport import TransportRegistry
from .transport.http import HttpTransport
from .transport.mqtt import MqttTransport
from .transport.panel import PanelTransport
from .transport.websocket import WebSocketTransport

_LOGGER = logging.getLogger(__name__)

PUSH_FRESHNESS_RATIO = 0.5  # skip poll if push < interval*0.5 ago
POST_COMMAND_WAIT = 3

# Reconcile ticks on which transports that failed their probe are given
# another chance, then every REPROBE_EVERY_TICKS. Tick 1 is the setup
# refresh, which happens milliseconds after the initial probe: retrying
# there would only re-ask a panel that just did not answer. At the
# default 90s interval this is roughly +90s, +4min, +10min, +24min, then
# every 24 minutes -- enough to catch a late broker without hammering a
# panel that is simply mute.
REPROBE_TICKS = (2, 4, 8, 16)
REPROBE_EVERY_TICKS = 16


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
        enable_panel: bool = False,
        cv_host: str = "",
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
        # PanelTransport is appended LAST: it is the wired fallback that
        # succeeds when the official API transports are unavailable.
        if enable_panel and cv_host:
            transports.append(PanelTransport(hass, panel_id, cv_host, pin))
        self.registry = TransportRegistry(transports)
        self._tick = 0
        self._reprobe_task: asyncio.Task | None = None

    async def async_setup(self) -> None:
        """Called by async_setup_entry. Task 15 wires this."""
        await self.registry.async_start_all(self.auth, self._on_push_state_update)
        await self.async_config_entry_first_refresh()

    async def async_shutdown(self) -> None:
        if self._reprobe_task is not None and not self._reprobe_task.done():
            self._reprobe_task.cancel()
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

    @callback
    def _async_schedule_reprobe(self) -> None:
        """Give transports that failed their probe another chance.

        Runs off the reconcile tick, in the background so a slow probe never
        delays the poll, and before the push-freshness shortcut: a healthy
        websocket must not keep a repairable transport excluded for ever.
        """
        self._tick += 1
        if (self._tick not in REPROBE_TICKS
                and self._tick % REPROBE_EVERY_TICKS):
            return
        if self._reprobe_task is not None and not self._reprobe_task.done():
            return
        self._reprobe_task = self.hass.async_create_task(
            self._async_reprobe(),
            name=f"elmax_local transport re-probe {self.panel_id}",
        )

    async def _async_reprobe(self) -> None:
        try:
            recovered = await self.registry.async_reprobe_inactive(
                self.auth, self._on_push_state_update
            )
        except Exception as err:
            # Never let a re-probe take the coordinator down with it.
            _LOGGER.debug("Transport re-probe failed: %s", err)
            return
        if recovered:
            _LOGGER.info("Transports back in service: %s",
                         ", ".join(recovered))

    async def _async_update_data(self) -> ElmaxState:
        """Reconciliation poll. Skipped if push is fresh."""
        self._async_schedule_reprobe()
        if self._push_is_fresh():
            return self.data
        raw = await self.registry.async_fetch_state()
        if raw is None:
            if self.data is not None:
                interval = (self.update_interval.total_seconds()
                            if self.update_interval else DEFAULT_RECONCILE_INTERVAL)
                stale_for = time.time() - self.data.last_update_ts
                transports = ", ".join(f"{t.name}={t.state.value}"
                                       for t in self.registry._transports)
                if stale_for > STALE_UNAVAILABLE_RATIO * interval:
                    # Past this point the cached state is not trustworthy:
                    # surface it as unavailable instead of serving an alarm
                    # state that may be minutes or hours out of date.
                    raise UpdateFailed(
                        f"No fresh data for {int(stale_for)}s "
                        f"(transports: {transports})"
                    )
                if stale_for > STALE_WARN_RATIO * interval:
                    _LOGGER.warning(
                        "Coordinator stale: no fresh data for %ds "
                        "(transports: %s). Last source: %s",
                        int(stale_for), transports,
                        self.data.last_update_source,
                    )
                else:
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

    async def async_send_command(
        self,
        endpoint_id: str,
        cmd: str | None,
        code: str | None = None,
    ) -> bool:
        result = await self.registry.async_send_command(endpoint_id, cmd, code)
        if result.ok:
            self.hass.async_create_task(self._post_command_verify())
        else:
            _LOGGER.warning("Command %s/%s failed: %s",
                            endpoint_id, cmd, result.error)
        return result.ok

    async def _post_command_verify(self) -> None:
        """Wait briefly for a push; if none, force reconcile."""
        await asyncio.sleep(POST_COMMAND_WAIT)
        if (self.data is None
                or (time.time() - self.data.last_update_ts) > POST_COMMAND_WAIT):
            await self.async_request_refresh()

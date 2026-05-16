"""WebSocket transport for Elmax Local. PUSH capability only.

Connects to wss://IP/api/v2/push. Receives JSON identical to /api/v2/discovery
on each state change. Limited to 1 client per session per the panel's docs.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

import aiohttp
from homeassistant.core import HomeAssistant

from ..const import WS_BASE_URL
from . import StateUpdateCallback, Transport, TransportCapability, TransportState

if TYPE_CHECKING:
    from ..auth import AuthManager

_LOGGER = logging.getLogger(__name__)

CONNECT_TIMEOUT = 10
HEARTBEAT = 30
BACKOFF_INITIAL = 5
BACKOFF_MAX = 120


class WebSocketTransport(Transport):
    name = "websocket"
    capabilities = frozenset({TransportCapability.PUSH})

    def __init__(self, hass: HomeAssistant, host: str):
        self._hass = hass
        self._host = host
        self._auth: "AuthManager" | None = None
        self._on_push: StateUpdateCallback | None = None
        self._state = TransportState.DISABLED
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._url = WS_BASE_URL.format(host=host)

    @property
    def state(self) -> TransportState:
        return self._state

    async def async_probe(self) -> bool:
        if self._auth is None:
            return False
        try:
            return await asyncio.wait_for(
                self._open_ws(probe_only=True), timeout=CONNECT_TIMEOUT
            )
        except (asyncio.TimeoutError, aiohttp.ClientError, Exception) as err:
            _LOGGER.debug("WS probe failed: %s", err)
            return False

    async def _open_ws(self, probe_only: bool = False) -> bool:
        if self._auth is None:
            return False
        token = await self._auth.async_get_token()
        # Session is HA-managed; verify_ssl=False is already baked in by
        # AuthManager._ensure_session() (see auth.py for rationale).
        session = await self._auth._ensure_session()
        async with session.ws_connect(
            self._url,
            headers={"Authorization": f"JWT {token}"},
            heartbeat=HEARTBEAT,
            timeout=CONNECT_TIMEOUT,
        ) as ws:
            if probe_only:
                await ws.close()
                return True
            await self._listen(ws)
            return True

    async def async_start(self, auth, on_state_update) -> None:
        self._auth = auth
        self._on_push = on_state_update
        self._stop_event.clear()
        # Background task — must NOT block HA bootstrap. The reconnect loop
        # runs forever (while not stop_event), so a regular async_create_task
        # would keep HA waiting in setup phase and trip the bootstrap timeout
        # at ~10 minutes ("Setup timed out for bootstrap waiting on ...").
        self._task = self._hass.async_create_background_task(
            self._run_loop(), name=f"elmax_local_ws_{self._host}"
        )
        self._state = TransportState.READY

    async def async_stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        self._state = TransportState.DISABLED

    async def _run_loop(self) -> None:
        backoff = BACKOFF_INITIAL
        while not self._stop_event.is_set():
            try:
                await self._open_ws(probe_only=False)
                backoff = BACKOFF_INITIAL
            except aiohttp.WSServerHandshakeError as err:
                if err.status == 401 and self._auth:
                    await self._auth.async_handle_401()
                _LOGGER.debug("WS handshake error %s, retry in %ds", err, backoff)
                self._state = TransportState.DEGRADED
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as err:
                _LOGGER.debug("WS error %s, retry in %ds", err, backoff)
                self._state = TransportState.DEGRADED
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)

    async def _listen(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self._state = TransportState.READY
        async for msg in ws:
            if self._stop_event.is_set():
                break
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle_message(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    async def _handle_message(self, payload: str) -> None:
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            _LOGGER.warning("WS dropped malformed payload")
            return
        if self._on_push:
            await self._on_push(data)

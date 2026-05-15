"""HTTP transport for Elmax Local. POLL + COMMAND capabilities."""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import aiohttp
from homeassistant.core import HomeAssistant

from ..const import HTTP_BASE_URL
from . import (
    CommandResult, Transport,
    TransportCapability, TransportState,
)

if TYPE_CHECKING:
    from ..auth import AuthManager

_LOGGER = logging.getLogger(__name__)

HTTP_TIMEOUT = 10
COMMAND_BUSY_RETRIES = 3
COMMAND_BUSY_DELAY = 2


class HttpTransport(Transport):
    name = "http"
    capabilities = frozenset({TransportCapability.POLL, TransportCapability.COMMAND})

    def __init__(self, hass: HomeAssistant, host: str):
        self._hass = hass
        self._host = host
        self._auth: "AuthManager" | None = None
        self._state = TransportState.DISABLED
        self._base = HTTP_BASE_URL.format(host=host)

    @property
    def state(self) -> TransportState:
        return self._state

    async def async_probe(self) -> bool:
        if self._auth is None:
            return False
        try:
            await self._auth.async_get_token()
            return True
        except Exception as err:
            _LOGGER.debug("HTTP probe failed: %s", err)
            return False

    async def async_start(self, auth, on_state_update) -> None:
        self._auth = auth
        self._state = TransportState.READY

    async def async_stop(self) -> None:
        self._state = TransportState.DISABLED

    async def async_fetch_state(self) -> dict | None:
        if self._auth is None:
            return None
        try:
            token = await self._auth.async_get_token()
            session = await self._auth._ensure_session()
            async with session.get(
                f"{self._base}/discovery",
                headers={"Authorization": f"JWT {token}"},
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            ) as resp:
                if resp.status == 401:
                    await self._auth.async_handle_401()
                    self._state = TransportState.DEGRADED
                    return None
                if resp.status != 200:
                    self._state = TransportState.DEGRADED
                    return None
                self._state = TransportState.READY
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            _LOGGER.debug("HTTP fetch error: %s", err)
            self._state = TransportState.DEGRADED
            return None

    async def async_send_command(self, endpoint_id, cmd, code=None) -> CommandResult:
        if self._auth is None:
            return CommandResult(ok=False, error="not_started")
        url = f"{self._base}/cmd/{endpoint_id}/{cmd}"
        body = {"code": code} if code else None
        for attempt in range(COMMAND_BUSY_RETRIES):
            try:
                token = await self._auth.async_get_token()
                session = await self._auth._ensure_session()
                async with session.post(
                    url, json=body,
                    headers={"Authorization": f"JWT {token}"},
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                ) as resp:
                    if resp.status == 200:
                        return CommandResult(ok=True, raw_response=await resp.json())
                    if resp.status == 401:
                        await self._auth.async_handle_401()
                        if attempt == 0:
                            continue
                        return CommandResult(ok=False, error="auth_401")
                    if resp.status == 422:
                        await asyncio.sleep(COMMAND_BUSY_DELAY)
                        continue
                    return CommandResult(ok=False, error=f"http_{resp.status}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                return CommandResult(ok=False, error=f"network: {err}")
        return CommandResult(ok=False, error="busy_after_retries")

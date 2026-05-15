"""Authentication manager for Elmax Local.

Shared JWT across transports. TTL 1h documented; refresh proactive at 50min.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import time

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import HTTP_BASE_URL

_LOGGER = logging.getLogger(__name__)

REFRESH_MARGIN = 600
DEFAULT_TTL_FALLBACK = 3000

BACKOFF_BASE = 30
BACKOFF_MAX = 600
LOCKOUT_CODES = {401, 403}
PANEL_DOWN_CODES = {502, 503}


class ElmaxAuthError(HomeAssistantError):
    """Raised on auth failure or active backoff."""


class AuthManager:
    """JWT login + refresh + backoff. Shared across transports."""

    def __init__(self, hass: HomeAssistant, host: str, pin: str):
        self._hass = hass
        self._host = host
        self._pin = pin
        self._token: str | None = None
        self._expiry: float = 0
        self._lock = asyncio.Lock()
        self._login_fail_count: int = 0
        self._blocked_until: float = 0
    @property
    def host(self) -> str:
        return self._host

    async def _ensure_session(self) -> aiohttp.ClientSession:
        # HA-managed session avoids thread leaks from manual SSL context
        # creation; verify_ssl=False supports the panel's self-signed cert.
        return async_get_clientsession(self._hass, verify_ssl=False)

    async def async_close(self) -> None:
        # Session lifecycle is managed by HA; nothing to close here.
        return None

    def _parse_exp(self, jwt: str) -> float:
        try:
            parts = jwt.split(".")
            if len(parts) != 3:
                raise ValueError("not a 3-part JWT")
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            if "exp" not in payload:
                raise ValueError("no exp claim")
            return float(payload["exp"])
        except (ValueError, json.JSONDecodeError, KeyError) as err:
            _LOGGER.debug("JWT parse_exp fallback: %s", err)
            return time.time() + DEFAULT_TTL_FALLBACK

    async def async_get_token(self) -> str:
        async with self._lock:
            now = time.time()
            if now < self._blocked_until:
                remaining = int(self._blocked_until - now)
                raise ElmaxAuthError(f"Auth backoff active, retry in {remaining}s")
            if self._token and now < self._expiry - REFRESH_MARGIN:
                return self._token
            if self._token and now < self._expiry:
                if await self._try_refresh():
                    return self._token  # type: ignore[return-value]
            await self._do_login()
            return self._token  # type: ignore[return-value]

    async def async_handle_401(self) -> None:
        async with self._lock:
            self._token = None
            self._expiry = 0

    async def _try_refresh(self) -> bool:
        session = await self._ensure_session()
        try:
            async with session.post(
                f"{HTTP_BASE_URL.format(host=self._host)}/refresh",
                json={"token": f"JWT {self._token}"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw = data.get("token", "").replace("JWT ", "")
                    if raw:
                        self._token = raw
                        self._expiry = self._parse_exp(raw)
                        self._login_fail_count = 0
                        return True
                return False
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return False

    async def _do_login(self) -> None:
        session = await self._ensure_session()
        try:
            async with session.post(
                f"{HTTP_BASE_URL.format(host=self._host)}/login",
                json={"pin": self._pin},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw = data.get("token", "").replace("JWT ", "")
                    if not raw:
                        raise ElmaxAuthError("empty token")
                    self._token = raw
                    self._expiry = self._parse_exp(raw)
                    self._login_fail_count = 0
                    self._blocked_until = 0
                    return
                await self._apply_backoff(resp.status)
                raise ElmaxAuthError(f"Login failed: HTTP {resp.status}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            await self._apply_backoff(599)
            raise ElmaxAuthError(f"Login network error: {err}") from err

    async def _apply_backoff(self, status: int) -> None:
        self._login_fail_count += 1
        if status in LOCKOUT_CODES:
            backoff = min(BACKOFF_MAX, BACKOFF_BASE * (2 ** (self._login_fail_count - 1)))
        elif status in PANEL_DOWN_CODES or status >= 500:
            backoff = min(BACKOFF_MAX, BACKOFF_BASE * self._login_fail_count)
        else:
            backoff = min(120, BACKOFF_BASE * self._login_fail_count)
        self._blocked_until = time.time() + backoff
        _LOGGER.warning("Auth backoff %ds (status=%d, attempt=%d)",
                        backoff, status, self._login_fail_count)

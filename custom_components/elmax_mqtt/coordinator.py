"""Elmax MQTT coordinator - MQTT for status with HTTP fallback, HTTP for commands."""

import asyncio
import json
import logging
import ssl
import time
from datetime import timedelta

import aiohttp

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    TOPIC_REQUEST_LOGIN,
    TOPIC_REQUEST_STATUS,
    TOPIC_RESPONSE_LOGIN,
    TOPIC_RESPONSE_STATUS,
    SIGNAL_UPDATE,
    DEFAULT_SCAN_INTERVAL,
    HTTP_BASE_URL,
)

_LOGGER = logging.getLogger(__name__)

MQTT_TIMEOUT = 8
HTTP_POLL_TIMEOUT = 10
MAX_MQTT_FAILS_BEFORE_WARN = 3
MQTT_RECOVERY_INTERVAL = 6  # try MQTT recovery every N polls while in HTTP fallback


class ElmaxMqttCoordinator:
    """Coordinate Elmax panel: MQTT primary + HTTP fallback for status, HTTP for commands."""

    def __init__(
        self,
        hass: HomeAssistant,
        panel_id: str,
        pin: str,
        host: str,
        scan_interval: int = DEFAULT_SCAN_INTERVAL,
    ):
        self.hass = hass
        self.panel_id = panel_id
        self.pin = pin
        self.host = host
        self.scan_interval = scan_interval

        # MQTT auth
        self._mqtt_token: str | None = None
        self._mqtt_token_expiry: float = 0

        # HTTP auth
        self._http_token: str | None = None
        self._http_token_expiry: float = 0

        self._unsub_callbacks: list = []
        self._unsub_timer = None

        # State data
        self.zones: list[dict] = []
        self.areas: list[dict] = []
        self.outputs: list[dict] = []
        self.scenarios: list[dict] = []
        self.panel_info: dict = {}

        # MQTT request/response sync
        self._login_response: dict | None = None
        self._login_event = asyncio.Event()
        self._status_response: dict | None = None
        self._status_event = asyncio.Event()

        # Fallback tracking
        self._mqtt_consecutive_fails: int = 0
        self._using_http_fallback: bool = False
        self._polls_since_last_mqtt_try: int = 0

        # HTTP
        self._http_base = HTTP_BASE_URL.format(host=self.host)
        self._ssl_ctx = ssl.create_default_context()
        self._ssl_ctx.check_hostname = False
        self._ssl_ctx.verify_mode = ssl.CERT_NONE
        self._http_session: aiohttp.ClientSession | None = None

    async def async_setup(self):
        """Set up MQTT subscriptions, HTTP session, and fetch initial data."""
        self._http_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=self._ssl_ctx)
        )

        # MQTT subscriptions
        self._unsub_callbacks.append(
            await mqtt.async_subscribe(
                self.hass,
                TOPIC_RESPONSE_LOGIN.format(panel_id=self.panel_id),
                self._handle_login_response,
            )
        )
        self._unsub_callbacks.append(
            await mqtt.async_subscribe(
                self.hass,
                TOPIC_RESPONSE_STATUS.format(panel_id=self.panel_id),
                self._handle_status_response,
            )
        )

        # Try MQTT login first, fall back to HTTP-only if needed
        mqtt_ok = await self._mqtt_login()
        http_ok = await self._http_login()

        if not mqtt_ok and not http_ok:
            raise ConnectionError("Failed to authenticate with Elmax panel (both MQTT and HTTP failed)")

        if not mqtt_ok:
            _LOGGER.warning("MQTT login failed, starting in HTTP-only fallback mode")
            self._using_http_fallback = True

        # Initial status
        await self._poll_status()
        if not self.areas:
            raise ConnectionError("No data received from Elmax panel")

        # Periodic polling
        self._unsub_timer = async_track_time_interval(
            self.hass,
            self._async_update,
            timedelta(seconds=self.scan_interval),
        )

    async def async_shutdown(self):
        """Clean up."""
        for unsub in self._unsub_callbacks:
            unsub()
        self._unsub_callbacks.clear()
        if self._unsub_timer:
            self._unsub_timer()
            self._unsub_timer = None
        if self._http_session:
            await self._http_session.close()
            self._http_session = None

    # ── MQTT Auth ────────────────────────────────────────────────

    async def _mqtt_login(self) -> bool:
        """Authenticate with the panel via MQTT."""
        self._login_event.clear()
        self._login_response = None

        try:
            await mqtt.async_publish(
                self.hass,
                TOPIC_REQUEST_LOGIN.format(panel_id=self.panel_id),
                json.dumps({"pin": self.pin}),
            )
        except Exception as err:
            _LOGGER.debug("MQTT publish failed: %s", err)
            return False

        try:
            await asyncio.wait_for(self._login_event.wait(), timeout=MQTT_TIMEOUT)
        except asyncio.TimeoutError:
            _LOGGER.debug("Elmax MQTT login timeout")
            return False

        if self._login_response and "token" in self._login_response:
            self._mqtt_token = self._login_response["token"]
            self._mqtt_token_expiry = time.time() + 3000
            _LOGGER.debug("Elmax MQTT authenticated")
            return True

        _LOGGER.debug("Elmax MQTT login failed: %s", self._login_response)
        return False

    @callback
    def _handle_login_response(self, msg):
        try:
            self._login_response = json.loads(msg.payload)
            self._login_event.set()
        except (json.JSONDecodeError, ValueError):
            _LOGGER.error("Invalid login response payload")

    # ── HTTP Auth ────────────────────────────────────────────────

    async def _http_login(self) -> bool:
        """Authenticate with the panel via HTTP API."""
        if not self._http_session:
            return False
        try:
            async with self._http_session.post(
                f"{self._http_base}/login",
                json={"pin": self.pin},
                timeout=aiohttp.ClientTimeout(total=HTTP_POLL_TIMEOUT),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self._http_token = data.get("token", "").replace("JWT ", "")
                    self._http_token_expiry = time.time() + 3000
                    _LOGGER.debug("Elmax HTTP authenticated")
                    return True
                _LOGGER.error("Elmax HTTP login failed: %s", resp.status)
                return False
        except Exception as err:
            _LOGGER.error("Elmax HTTP login error: %s", err)
            return False

    async def _ensure_http_token(self) -> bool:
        if self._http_token and time.time() < self._http_token_expiry:
            return True
        return await self._http_login()

    # ── MQTT Status Polling ──────────────────────────────────────

    async def _poll_status_mqtt(self) -> bool:
        """Request full status via MQTT. Returns True on success."""
        if not self._mqtt_token or time.time() > self._mqtt_token_expiry:
            if not await self._mqtt_login():
                return False

        self._status_event.clear()
        self._status_response = None

        try:
            await mqtt.async_publish(
                self.hass,
                TOPIC_REQUEST_STATUS.format(panel_id=self.panel_id),
                json.dumps({"token": self._mqtt_token}),
            )
        except Exception:
            return False

        try:
            await asyncio.wait_for(self._status_event.wait(), timeout=MQTT_TIMEOUT)
        except asyncio.TimeoutError:
            return False

        if not self._status_response:
            return False

        # Handle 401
        if "401" in self._status_response.get("message", ""):
            self._mqtt_token = None
            if await self._mqtt_login():
                return await self._poll_status_mqtt()
            return False

        return self._apply_status(self._status_response.get("status"))

    @callback
    def _handle_status_response(self, msg):
        try:
            self._status_response = json.loads(msg.payload)
            self._status_event.set()
        except (json.JSONDecodeError, ValueError):
            _LOGGER.error("Invalid status response payload")

    # ── HTTP Status Polling (Fallback) ───────────────────────────

    async def _poll_status_http(self) -> bool:
        """Request full status via HTTP GET /discovery. Returns True on success."""
        if not await self._ensure_http_token():
            return False

        try:
            async with self._http_session.get(
                f"{self._http_base}/discovery",
                headers={"Authorization": f"JWT {self._http_token}"},
                timeout=aiohttp.ClientTimeout(total=HTTP_POLL_TIMEOUT),
            ) as resp:
                if resp.status == 401:
                    self._http_token = None
                    if await self._http_login():
                        return await self._poll_status_http()
                    return False
                if resp.status != 200:
                    _LOGGER.warning("HTTP status poll failed: %s", resp.status)
                    return False
                data = await resp.json()
                return self._apply_status(data)
        except Exception as err:
            _LOGGER.warning("HTTP status poll error: %s", err)
            return False

    # ── Unified Status Update ────────────────────────────────────

    def _apply_status(self, status: dict | None) -> bool:
        """Apply status data from either MQTT or HTTP. Returns True on success."""
        if not status:
            return False

        self.zones = status.get("zone", [])
        self.areas = status.get("aree", [])
        self.outputs = status.get("uscite", [])
        self.scenarios = status.get("scenari", [])
        self.panel_info = {
            "centrale": status.get("centrale", self.panel_id),
            "release": status.get("release", ""),
            "tipo_accessorio": status.get("tipo_accessorio", ""),
            "release_accessorio": status.get("release_accessorio", ""),
        }

        async_dispatcher_send(
            self.hass,
            SIGNAL_UPDATE.format(panel_id=self.panel_id),
        )
        return True

    async def _poll_status(self):
        """Poll status: MQTT primary, HTTP fallback with periodic MQTT recovery."""
        if not self._using_http_fallback:
            # Normal mode: try MQTT first
            mqtt_ok = await self._poll_status_mqtt()

            if mqtt_ok:
                self._mqtt_consecutive_fails = 0
                return

            # MQTT failed
            self._mqtt_consecutive_fails += 1

            if self._mqtt_consecutive_fails >= MAX_MQTT_FAILS_BEFORE_WARN:
                _LOGGER.warning(
                    "MQTT polling failed %d consecutive times, switching to HTTP fallback",
                    self._mqtt_consecutive_fails,
                )
                self._using_http_fallback = True
                self._polls_since_last_mqtt_try = 0

            # Try HTTP as backup
            if not await self._poll_status_http():
                _LOGGER.error("Both MQTT and HTTP status polling failed")
        else:
            # Fallback mode: use HTTP primarily, periodically probe MQTT
            self._polls_since_last_mqtt_try += 1

            if self._polls_since_last_mqtt_try >= MQTT_RECOVERY_INTERVAL:
                # Time to check if MQTT has recovered
                self._polls_since_last_mqtt_try = 0
                mqtt_ok = await self._poll_status_mqtt()

                if mqtt_ok:
                    _LOGGER.info("MQTT connection restored, switching back from HTTP fallback")
                    self._using_http_fallback = False
                    self._mqtt_consecutive_fails = 0
                    return
                _LOGGER.debug("MQTT recovery probe failed, staying on HTTP fallback")

            # HTTP as primary in fallback mode
            if not await self._poll_status_http():
                _LOGGER.warning("HTTP fallback poll failed")

    async def _async_update(self, now=None):
        """Periodic status update."""
        await self._poll_status()

    # ── HTTP Commands ────────────────────────────────────────────

    async def async_send_command(self, endpoint_id: str, command: str) -> bool:
        """Send a command via HTTP API.

        Args:
            endpoint_id: e.g. '{panel_id}-area-3'
            command: e.g. '4' (arm_totally), 'off' (disarm), 'on'/'off' (outputs)
        """
        if not await self._ensure_http_token():
            _LOGGER.error("Cannot send command: HTTP authentication failed")
            return False

        url = f"{self._http_base}/cmd/{endpoint_id}/{command}"
        headers = {"Authorization": f"JWT {self._http_token}"}

        for attempt in range(3):
            try:
                async with self._http_session.post(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    body = await resp.text()
                    if resp.status == 200:
                        _LOGGER.debug("Command OK: %s/%s -> %s", endpoint_id, command, body)
                        await asyncio.sleep(2)
                        await self._poll_status()
                        return True
                    if resp.status == 401:
                        _LOGGER.info("HTTP token expired, re-authenticating")
                        self._http_token = None
                        if not await self._http_login():
                            return False
                        headers = {"Authorization": f"JWT {self._http_token}"}
                        continue
                    if resp.status == 422:
                        _LOGGER.debug("Panel busy, retry %d/3", attempt + 1)
                        await asyncio.sleep(2)
                        continue
                    _LOGGER.error("Command failed: %s/%s -> %s %s", endpoint_id, command, resp.status, body)
                    return False
            except Exception as err:
                _LOGGER.error("HTTP command error: %s", err)
                return False

        return False

    # ── State Accessors ──────────────────────────────────────────

    def get_zone(self, endpoint_id: str) -> dict | None:
        for zone in self.zones:
            if zone.get("endpointId") == endpoint_id:
                return zone
        return None

    def get_area(self, endpoint_id: str) -> dict | None:
        for area in self.areas:
            if area.get("endpointId") == endpoint_id:
                return area
        return None

    def get_output(self, endpoint_id: str) -> dict | None:
        for output in self.outputs:
            if output.get("endpointId") == endpoint_id:
                return output
        return None

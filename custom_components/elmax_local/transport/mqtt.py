"""MQTT transport for Elmax Local.

Uses HA's mqtt integration. Sub to /elmax/response/status/{panel_id}
delivers BOTH responses to requests AND spontaneous push updates
(distinguished by 'message' field). Both are forwarded to on_state_update.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback

from ..const import (
    TOPIC_REQUEST_COMMAND, TOPIC_REQUEST_ID, TOPIC_REQUEST_STATUS,
    TOPIC_RESPONSE_COMMAND, TOPIC_RESPONSE_ID, TOPIC_RESPONSE_STATUS,
)
from . import (
    CommandResult, StateUpdateCallback, Transport,
    TransportCapability, TransportState,
)

if TYPE_CHECKING:
    from ..auth import AuthManager

_LOGGER = logging.getLogger(__name__)

PROBE_TIMEOUT = 5
STATUS_TIMEOUT = 8
COMMAND_TIMEOUT = 5


class MqttTransport(Transport):
    name = "mqtt"
    capabilities = frozenset({
        TransportCapability.PUSH,
        TransportCapability.POLL,
        TransportCapability.COMMAND,
    })

    def __init__(self, hass: HomeAssistant, panel_id: str):
        self._hass = hass
        self._panel_id = panel_id
        self._auth: "AuthManager" | None = None
        self._on_push: StateUpdateCallback | None = None
        self._state = TransportState.DISABLED
        self._unsubs: list = []
        self._status_event = asyncio.Event()
        self._status_response: dict | None = None
        self._command_event = asyncio.Event()
        self._command_response: dict | None = None
        self._pending_tasks: list[asyncio.Task] = []

    @property
    def state(self) -> TransportState:
        return self._state

    async def async_probe(self) -> bool:
        event = asyncio.Event()

        @callback
        def _on_id(msg):
            try:
                data = json.loads(msg.payload)
                if "centrale" in data:
                    event.set()
            except (json.JSONDecodeError, ValueError):
                pass

        unsub = await mqtt.async_subscribe(self._hass, TOPIC_RESPONSE_ID, _on_id)
        try:
            await mqtt.async_publish(self._hass, TOPIC_REQUEST_ID, "{}")
            await asyncio.wait_for(event.wait(), timeout=PROBE_TIMEOUT)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            unsub()

    async def async_start(self, auth, on_state_update) -> None:
        self._auth = auth
        self._on_push = on_state_update
        await self._subscribe_responses()
        self._state = TransportState.READY

    async def _subscribe_responses(self) -> None:
        self._unsubs.append(await mqtt.async_subscribe(
            self._hass,
            TOPIC_RESPONSE_STATUS.format(panel_id=self._panel_id),
            self._handle_status_message,
        ))
        self._unsubs.append(await mqtt.async_subscribe(
            self._hass,
            TOPIC_RESPONSE_COMMAND.format(panel_id=self._panel_id),
            self._handle_command_message,
        ))

    async def async_stop(self) -> None:
        for u in self._unsubs:
            u()
        self._unsubs.clear()
        for t in self._pending_tasks:
            if not t.done():
                t.cancel()
        self._pending_tasks.clear()
        self._state = TransportState.DISABLED

    @callback
    def _handle_status_message(self, msg) -> None:
        try:
            data = json.loads(msg.payload)
        except (json.JSONDecodeError, ValueError):
            _LOGGER.warning("Invalid MQTT status payload")
            return

        message = data.get("message", "")
        if "401" in message:
            if self._auth:
                self._hass.async_create_task(self._auth.async_handle_401())
            return

        # Both '200 Status OK' (response) and '200 Status Update' (push)
        # carry status payload. Forward both to coordinator.
        status = data.get("status")
        if status and self._on_push:
            task = self._hass.async_create_task(self._on_push(status))
            self._pending_tasks.append(task)

        # Also signal request waiter (for fetch_state)
        self._status_response = data
        self._status_event.set()

    @callback
    def _handle_command_message(self, msg) -> None:
        try:
            self._command_response = json.loads(msg.payload)
            self._command_event.set()
        except (json.JSONDecodeError, ValueError):
            _LOGGER.warning("Invalid MQTT command payload")

    async def _drain_pending(self) -> None:
        """Test helper: await all pending push tasks."""
        if self._pending_tasks:
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            self._pending_tasks = [t for t in self._pending_tasks if not t.done()]

    async def async_fetch_state(self) -> dict | None:
        if not self._auth:
            return None
        try:
            token = await self._auth.async_get_token()
            self._status_event.clear()
            self._status_response = None
            await mqtt.async_publish(
                self._hass,
                TOPIC_REQUEST_STATUS.format(panel_id=self._panel_id),
                json.dumps({"token": f"JWT {token}"}),
            )
            await asyncio.wait_for(self._status_event.wait(), timeout=STATUS_TIMEOUT)
            if self._status_response:
                return self._status_response.get("status")
            return None
        except asyncio.TimeoutError:
            self._state = TransportState.DEGRADED
            return None

    async def async_send_command(self, endpoint_id, cmd, code=None) -> CommandResult:
        if not self._auth:
            return CommandResult(ok=False, error="not_started")
        try:
            token = await self._auth.async_get_token()
            self._command_event.clear()
            self._command_response = None
            body = {"token": f"JWT {token}", "eid": endpoint_id, "cmd": cmd}
            if code:
                body["code"] = code
            await mqtt.async_publish(
                self._hass,
                TOPIC_REQUEST_COMMAND.format(panel_id=self._panel_id),
                json.dumps(body),
            )
            await asyncio.wait_for(self._command_event.wait(), timeout=COMMAND_TIMEOUT)
            if self._command_response:
                msg = self._command_response.get("message", "")
                if "200" in msg:
                    return CommandResult(ok=True, raw_response=self._command_response)
                return CommandResult(ok=False, error=msg,
                                     raw_response=self._command_response)
            return CommandResult(ok=False, error="empty_response")
        except asyncio.TimeoutError:
            return CommandResult(ok=False, error="timeout")

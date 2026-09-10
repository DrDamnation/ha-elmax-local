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
from homeassistant.exceptions import HomeAssistantError

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

# Overall budget for a probe. Must stay under TransportRegistry's
# PROBE_HARD_TIMEOUT (12s), which is the outer belt-and-braces.
PROBE_TIMEOUT = 9
# Wait for the panel's answer before asking again. The request is
# republished because a single QoS-0 PUBLISH that nobody is subscribed to
# is gone for good: the answer is never retained and cannot be recovered.
PROBE_REPLY_WAIT = 2
# How long to wait for the broker to confirm our subscription before
# asking anyway. Asking blind is still better than not asking at all, and
# the republish loop covers what the confirmation would have covered.
SUBSCRIBE_CONFIRM_WAIT = 3
# Fallback for an HA without a public 'subscription is live' signal: a
# grace period longer than HA's INITIAL_SUBSCRIBE_COOLDOWN (0.5s).
SUBSCRIBE_BLIND_GRACE = 1.0

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
        self._retained_warned = False

    @property
    def state(self) -> TransportState:
        return self._state

    async def async_probe(self) -> bool:
        """Ask the panel to identify itself over MQTT.

        The probe has to survive a broker that has not accepted our
        subscription yet, a broker that is not even connected yet (HA and
        mosquitto booting together on the same box) and a panel that answers
        late. It therefore subscribes first and waits for that subscription
        to be live, then republishes the request until the window runs out,
        and never lets a broker error escape as an exception.
        """
        answered = asyncio.Event()
        subscribed = asyncio.Event()
        responders: set[str] = set()

        @callback
        def _on_id(msg) -> None:
            if getattr(msg, "retain", False):
                return  # a stored snapshot proves nothing about liveness
            try:
                data = json.loads(msg.payload)
            except (json.JSONDecodeError, ValueError):
                return
            if not isinstance(data, dict):
                return
            centrale = data.get("centrale")
            if centrale is None:
                return
            responders.add(str(centrale))
            # /elmax/response/id is panel-agnostic and this broker is bridged
            # to another one: only OUR panel answering validates the probe.
            if str(centrale) == self._panel_id:
                answered.set()

        try:
            unsub = await mqtt.async_subscribe(
                self._hass, TOPIC_RESPONSE_ID, _on_id, qos=0
            )
        except HomeAssistantError as err:
            _LOGGER.debug("mqtt probe: cannot subscribe (%s)", err)
            return False

        cancel_watch = self._async_watch_subscription(
            TOPIC_RESPONSE_ID, subscribed
        )
        confirmed = False
        published = 0
        try:
            async with asyncio.timeout(PROBE_TIMEOUT):
                confirmed = await self._async_await_subscription(subscribed)
                while True:
                    try:
                        await mqtt.async_publish(
                            self._hass, TOPIC_REQUEST_ID, "{}", qos=0
                        )
                    except HomeAssistantError as err:
                        # Broker not connected yet: it may still come up
                        # inside the window, so keep trying.
                        _LOGGER.debug(
                            "mqtt probe: broker refused the request (%s), "
                            "retrying", err,
                        )
                        await asyncio.sleep(PROBE_REPLY_WAIT)
                        continue
                    published += 1
                    try:
                        await asyncio.wait_for(
                            answered.wait(), PROBE_REPLY_WAIT
                        )
                    except (asyncio.TimeoutError, TimeoutError):
                        continue
                    return True
        except (asyncio.TimeoutError, TimeoutError):
            return False
        finally:
            cancel_watch()
            unsub()
            # Everything needed to tell a lost race from a mute panel.
            _LOGGER.debug(
                "mqtt probe done: subscription_confirmed=%s requests=%d "
                "answered=%s responders=%s",
                confirmed, published, answered.is_set(),
                sorted(responders) or "none",
            )

    def _async_watch_subscription(self, topic: str, subscribed: asyncio.Event):
        """Set `subscribed` once our SUBSCRIBE has reached the broker.

        `mqtt.async_subscribe` does not put the SUBSCRIBE on the wire: it
        registers the callback in memory and schedules a debounced job
        (INITIAL_SUBSCRIBE_COOLDOWN = 0.5s at startup, pushed further out by
        every other integration that subscribes meanwhile). Publishing before
        that job has run means the panel answers into a topic nobody is
        subscribed to, and a non-retained QoS-0 message with no subscriber is
        discarded by the broker, permanently.

        Returns a callable that cancels the watch.
        """
        @callback
        def _done(*_args) -> None:
            subscribed.set()

        on_subscribe_done = getattr(mqtt, "async_on_subscribe_done", None)
        if on_subscribe_done is not None:
            try:
                cancel = on_subscribe_done(self._hass, topic, 0, _done)
                # The watch is armed either way; only the canceller may be
                # missing, and a TypeError in the probe's finally block would
                # swallow its result.
                return cancel if callable(cancel) else (lambda: None)
            except Exception as err:  # unexpected signature or behaviour
                _LOGGER.debug(
                    "mqtt probe: async_on_subscribe_done unusable (%s), "
                    "using a timed grace period", err,
                )
        # Older HA has no public signal for this. A grace period longer than
        # the cooldown covers the common case; the republish loop covers the
        # rest.
        handle = self._hass.loop.call_later(SUBSCRIBE_BLIND_GRACE, _done)
        return handle.cancel

    @staticmethod
    async def _async_await_subscription(subscribed: asyncio.Event) -> bool:
        """True if the subscription was confirmed inside the window."""
        try:
            await asyncio.wait_for(subscribed.wait(), SUBSCRIBE_CONFIRM_WAIT)
            return True
        except (asyncio.TimeoutError, TimeoutError):
            return False

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
        if getattr(msg, "retain", False):
            # A retained message is whatever the broker still had stored on
            # the topic, replayed the instant we subscribe. It is not a push
            # and carries no timestamp we trust: on this installation the
            # stored snapshot is months old and shows every area disarmed.
            # Ingesting it would put a false state on an alarm panel AND
            # mark the coordinator fresh, suppressing the reconciliation
            # poll that would have corrected it.
            self._log_dropped_retained(msg)
            return
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

    def _log_dropped_retained(self, msg) -> None:
        """Say once, loudly, what was dropped and how old it was."""
        stamp = ""
        try:
            payload = json.loads(msg.payload)
            stamp = (payload.get("status") or {}).get("datetime", "")
        except (json.JSONDecodeError, ValueError, AttributeError):
            pass
        topic = getattr(msg, "topic", "?")
        if self._retained_warned:
            _LOGGER.debug("Ignored retained message on %s (panel time %s)",
                          topic, stamp or "unknown")
            return
        self._retained_warned = True
        _LOGGER.warning(
            "Ignoring RETAINED message on %s (panel time: %s). A retained "
            "message is the snapshot the broker had stored, not a live "
            "update, and it may be arbitrarily old; state will come from "
            "the next live push or from the reconciliation poll. Clear the "
            "retained message on the broker to silence this.",
            topic, stamp or "unknown",
        )

    @callback
    def _handle_command_message(self, msg) -> None:
        if getattr(msg, "retain", False):
            # A stored command reply would confirm a command that never ran.
            self._log_dropped_retained(msg)
            return
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

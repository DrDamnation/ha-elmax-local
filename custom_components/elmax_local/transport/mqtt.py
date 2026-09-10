"""MQTT transport for Elmax Local.

Uses HA's mqtt integration. Sub to /elmax/response/status/{panel_id}
delivers BOTH responses to requests AND spontaneous push updates
(distinguished by 'message' field). Both are forwarded to on_state_update.

Delivery is not free here. `mqtt.async_subscribe` does NOT put a SUBSCRIBE
on the wire: it registers the callback in memory and schedules a debounced
job (INITIAL_SUBSCRIBE_COOLDOWN = 0.5s at startup, pushed further out by
every other integration subscribing meanwhile). Anything published before
that job has run is answered into a topic nobody is subscribed to, and a
non-retained QoS-0 message with no subscriber is discarded by the broker
permanently. Every request path in this file therefore waits for its
subscription first -- the probe AND the data path.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError

from ..auth import ElmaxAuthError
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
# Same idea as PROBE_REPLY_WAIT, on the data path: wait this long for the
# answer, then ask again inside the STATUS_TIMEOUT budget. Asking again is
# safe because /elmax/request/status is read-only -- it changes nothing on
# the panel. Commands get the opposite treatment (see async_send_command).
STATUS_REPLY_WAIT = 3
COMMAND_TIMEOUT = 5

# Retained messages are dropped, not ingested. Warning only once per
# process would make the worst case invisible: a panel that published its
# state retained would leave this transport READY and completely mute,
# with a single WARNING buried in the boot log. Repeat it, at a throttle.
RETAINED_WARN_INTERVAL = 600


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
        self._watch_cancels: list = []
        # Set once the matching SUBSCRIBE has actually reached the broker.
        self._status_subscribed = asyncio.Event()
        self._command_subscribed = asyncio.Event()
        self._status_event = asyncio.Event()
        self._status_response: dict | None = None
        self._command_event = asyncio.Event()
        self._command_response: dict | None = None
        self._pending_tasks: set[asyncio.Task] = set()
        self._retained_dropped = 0
        self._retained_warned_at: float | None = None
        self._last_fetch_message: str | None = None

    @property
    def state(self) -> TransportState:
        return self._state

    def diagnostic_info(self) -> dict:
        """What the downloadable diagnostics needs to judge this channel.

        The log only speaks when something breaks, so "READY and silently
        dropping everything as retained" has no other way of surfacing.
        `last_fetch_message` is the open question of this whole
        investigation: which message actually answered our request.
        """
        return {
            "retained_dropped": self._retained_dropped,
            "last_fetch_message": self._last_fetch_message,
            "subscriptions": len(self._unsubs),
            "status_subscription_confirmed": self._status_subscribed.is_set(),
        }

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
        retained_replies = 0

        @callback
        def _on_id(msg) -> None:
            nonlocal retained_replies
            try:
                data = json.loads(msg.payload)
            except (json.JSONDecodeError, ValueError):
                return
            if not isinstance(data, dict):
                return
            centrale = data.get("centrale")
            if centrale is None:
                return
            # Record WHO replied BEFORE deciding whether the reply counts.
            # A retained identification still proves nothing about
            # liveness, so the probe rightly fails -- but reporting it as
            # "responders=none" reads as "the panel ignores Home
            # Assistant", a different fault with a different fix. The
            # diagnostic line must not be able to say that.
            responders.add(str(centrale))
            if getattr(msg, "retain", False):
                retained_replies += 1
                return
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
                "answered=%s responders=%s retained_replies=%d",
                confirmed, published, answered.is_set(),
                sorted(responders) or "none", retained_replies,
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
        # READY as soon as the callbacks are registered, so the registry can
        # route to us. The SUBSCRIBE packets are still inside HA's debouncer
        # at this instant -- which is exactly why every request path below
        # waits for its confirmation before publishing anything.
        self._state = TransportState.READY

    async def _subscribe_responses(self) -> None:
        """(Re)subscribe to the two response topics. Idempotent.

        async_start has a second call site (the registry re-probes DISABLED
        transports), and this used to append to self._unsubs without
        dropping what was already live: after a re-probe every message would
        be handled twice, i.e. two coordinator updates per push. The trap
        opens for real when the FIRST subscribe succeeds and the second
        raises -- the transport stays DISABLED with one live callback, and
        the next re-probe adds another. Drop everything first,
        unconditionally.
        """
        self._unsubscribe_all()
        subscriptions = (
            (TOPIC_RESPONSE_STATUS.format(panel_id=self._panel_id),
             self._handle_status_message, self._status_subscribed),
            (TOPIC_RESPONSE_COMMAND.format(panel_id=self._panel_id),
             self._handle_command_message, self._command_subscribed),
        )
        for topic, handler, subscribed in subscriptions:
            subscribed.clear()
            self._unsubs.append(
                await mqtt.async_subscribe(self._hass, topic, handler)
            )
            # Same watch the probe uses -- not a second mechanism. It is
            # the only way to know the SUBSCRIBE has left the debouncer.
            self._watch_cancels.append(
                self._async_watch_subscription(topic, subscribed)
            )

    def _unsubscribe_all(self) -> None:
        """Drop every live subscription and subscription watch."""
        for cancel in self._watch_cancels:
            cancel()
        self._watch_cancels.clear()
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()

    async def async_stop(self) -> None:
        self._unsubscribe_all()
        for task in list(self._pending_tasks):
            if not task.done():
                task.cancel()
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
            # Wake the waiter: the panel has already said no. Returning
            # silently left async_fetch_state sitting out its whole window
            # and then blaming a timeout, hiding an auth problem behind a
            # connectivity one.
            self._status_response = data
            self._status_event.set()
            return

        # Both '200 Status OK' (response) and '200 Status Update' (push)
        # carry status payload. Forward both to coordinator.
        status = data.get("status")
        if status and self._on_push:
            task = self._hass.async_create_task(self._on_push(status))
            self._pending_tasks.add(task)
            # Without this the set only ever grew: one finished Task kept
            # alive per push received, for the life of the entry.
            # _drain_pending is a test helper and async_stop only runs on
            # unload, so nothing pruned it in production.
            task.add_done_callback(self._pending_tasks.discard)

        # Also signal request waiter (for fetch_state)
        self._status_response = data
        self._status_event.set()

    def _log_dropped_retained(self, msg) -> None:
        """Count every drop, and keep saying it at a throttle."""
        self._retained_dropped += 1
        stamp = ""
        try:
            payload = json.loads(msg.payload)
            stamp = (payload.get("status") or {}).get("datetime", "")
        except (json.JSONDecodeError, ValueError, AttributeError):
            pass
        topic = getattr(msg, "topic", "?")
        now = time.monotonic()
        if (self._retained_warned_at is not None
                and now - self._retained_warned_at < RETAINED_WARN_INTERVAL):
            _LOGGER.debug(
                "Ignored retained message on %s (panel time %s), %d dropped "
                "so far", topic, stamp or "unknown", self._retained_dropped,
            )
            return
        self._retained_warned_at = now
        _LOGGER.warning(
            "Ignoring RETAINED message on %s (panel time: %s; %d dropped so "
            "far). A retained message is the snapshot the broker had stored, "
            "not a live update, and it may be arbitrarily old; state will "
            "come from the next live push or from the reconciliation poll. "
            "If this keeps repeating, this panel is publishing its state "
            "retained and the MQTT push channel is delivering nothing. "
            "Clear the retained message on the broker to silence this.",
            topic, stamp or "unknown", self._retained_dropped,
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
            await asyncio.gather(*list(self._pending_tasks),
                                 return_exceptions=True)

    async def async_fetch_state(self) -> dict | None:
        """Ask the panel for its state and wait for the answer.

        Same delivery guarantee as the probe, for the same reason: a
        request published straight after async_start is answered into a
        topic whose SUBSCRIBE is still in HA's debouncer, and that answer is
        lost for good. So: wait for the subscription, then keep asking
        inside the budget. Re-asking costs nothing -- /elmax/request/status
        is read-only and changes nothing on the panel.
        """
        if not self._auth:
            return None
        try:
            token = await self._auth.async_get_token()
        except ElmaxAuthError as err:
            # MQTT still needs a JWT fetched over HTTPS from the panel, so
            # this channel is NOT independent of the HTTP one it is meant to
            # replace. When the token fails, mqtt is unusable -- and the
            # registry swallows exceptions from transports, which would have
            # left it advertising itself as READY. Say what is true.
            _LOGGER.debug("mqtt fetch: no token (%s)", err)
            self._state = TransportState.DEGRADED
            return None

        topic = TOPIC_REQUEST_STATUS.format(panel_id=self._panel_id)
        payload = json.dumps({"token": f"JWT {token}"})
        self._status_event.clear()
        self._status_response = None
        requests = 0
        try:
            async with asyncio.timeout(STATUS_TIMEOUT):
                await self._async_await_subscription(self._status_subscribed)
                while True:
                    try:
                        await mqtt.async_publish(self._hass, topic, payload)
                    except HomeAssistantError as err:
                        _LOGGER.debug(
                            "mqtt fetch: broker refused the request (%s), "
                            "retrying", err,
                        )
                        await asyncio.sleep(STATUS_REPLY_WAIT)
                        continue
                    requests += 1
                    try:
                        await asyncio.wait_for(
                            self._status_event.wait(), STATUS_REPLY_WAIT
                        )
                    except (asyncio.TimeoutError, TimeoutError):
                        continue
                    break
        except (asyncio.TimeoutError, TimeoutError):
            _LOGGER.debug(
                "mqtt fetch: no answer to %d request(s) within %ss",
                requests, STATUS_TIMEOUT,
            )
            self._state = TransportState.DEGRADED
            return None

        data = self._status_response or {}
        message = data.get("message", "")
        # Record WHICH message satisfied the poll; deliberately do NOT gate
        # on it. "the panel answers OUR requests" vs "the panel publishes on
        # its own and we caught one" is still an open question here, and the
        # only string ever observed from this panel is '200 Status Update'.
        # Gating on '200 Status OK' -- a string that appears nowhere but a
        # docstring -- would break the poll outright.
        self._last_fetch_message = message or None
        if "401" in message:
            self._state = TransportState.DEGRADED
            return None
        status = data.get("status")
        if not status:
            self._state = TransportState.DEGRADED
            return None
        # Same contract as http.py:77 and websocket.py:122: a successful
        # exchange puts the transport back to READY. Without this line the
        # first timeout labelled mqtt 'degraded' until the next reload,
        # while it kept polling perfectly well -- and the diagnostics,
        # get_active_push_transports() and the staleness log line all went
        # on reporting a failure that had already recovered.
        self._state = TransportState.READY
        return status

    async def async_send_command(self, endpoint_id, cmd, code=None) -> CommandResult:
        """Send ONE command and wait for its confirmation. Never repeat it.

        There is no request/response correlation on this channel, so a lost
        confirmation is indistinguishable from a lost command -- and the
        panel MAY HAVE EXECUTED it. Republishing (which async_fetch_state
        does happily, because asking for state changes nothing) could arm or
        disarm an inhabited house twice. Reporting a failure for a command
        that ran is the lesser evil.

        See MQTT_MAY_CARRY_COMMANDS in transport/__init__.py: until this
        path is exercised and correlated, the registry never routes a
        command here at all.
        """
        if not self._auth:
            return CommandResult(ok=False, error="not_started")
        try:
            token = await self._auth.async_get_token()
        except ElmaxAuthError as err:
            self._state = TransportState.DEGRADED
            return CommandResult(ok=False, error=f"auth: {err}")

        body = {"token": f"JWT {token}", "eid": endpoint_id, "cmd": cmd}
        if code:
            body["code"] = code
        self._command_event.clear()
        self._command_response = None
        # Wait for the reply subscription BEFORE publishing. The command
        # goes out exactly once, so a confirmation lost to the subscribe
        # race is unrecoverable, and this is the one path where losing the
        # answer is worse than losing the request.
        await self._async_await_subscription(self._command_subscribed)
        try:
            await mqtt.async_publish(
                self._hass,
                TOPIC_REQUEST_COMMAND.format(panel_id=self._panel_id),
                json.dumps(body),
            )
        except HomeAssistantError as err:
            # Nothing left the box: a clean failure, the panel did nothing.
            self._state = TransportState.DEGRADED
            return CommandResult(ok=False, error=f"broker: {err}")
        try:
            await asyncio.wait_for(self._command_event.wait(), COMMAND_TIMEOUT)
        except (asyncio.TimeoutError, TimeoutError):
            return CommandResult(ok=False, error="timeout")
        if self._command_response:
            msg = self._command_response.get("message", "")
            if "200" in msg:
                self._state = TransportState.READY
                return CommandResult(ok=True, raw_response=self._command_response)
            return CommandResult(ok=False, error=msg,
                                 raw_response=self._command_response)
        return CommandResult(ok=False, error="empty_response")

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
from datetime import datetime
from typing import TYPE_CHECKING

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.util import dt as dt_util

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

# --- Refusing a replayed snapshot -------------------------------------
#
# The retain FLAG is not a usable guard on its own. This broker bridges to
# another one with "topic # both 0" and cleansession false; when that bridge
# reconnects it re-publishes what it has stored, and MQTT-3.3.1-9 says a
# retained message forwarded to a client that is ALREADY SUBSCRIBED is
# delivered with the retain flag CLEARED. The months-old snapshot sitting on
# the state topic right now (every area disarmed, every zone closed) would
# therefore arrive looking exactly like a live push, and on an inhabited
# house that reads as "Disarmed" on five armed areas -- with the staleness
# watchdog silent, because the coordinator had just marked the data fresh.
#
# So the guard is on the CONTENT. Every status payload carries the panel's
# own clock as "HH:MM:SS DD/MM/YYYY"; a snapshot is accepted only if that
# stamp is close to our own clock AND not older than the last one accepted.
PANEL_DATETIME_FORMAT = "%H:%M:%S %d/%m/%Y"

# How far the panel's clock may sit from Home Assistant's before a payload
# stops counting as live. Deliberately loose, in BOTH directions: the panel
# is not synchronised with HA (no NTP client of ours, its own RTC, a
# timezone we do not set) so a couple of hours of drift is ordinary and must
# never cost a real update. Nothing plausible drifts by six hours, and the
# replay this defends against is months old, so the window has room to
# spare on the side that matters. Refusing costs a poll -- HTTP is tried
# first anyway; accepting a replay puts a false state on an alarm panel.
STATUS_MAX_CLOCK_SKEW = 6 * 3600

# For how long after publishing a request an incoming status counts as the
# ANSWER to that request rather than as a spontaneous push. There is no
# correlation id in this protocol, so this window is the whole of the
# correlation available; it starts at each publish, never before. It is
# sized to the full request budget plus slack rather than to one reply
# wait, because an answer that arrives after we gave up is still an answer:
# crediting it to the push channel would be the same false claim ("MQTT
# push works") from the other direction. See _is_answering_us.
REQUEST_ANSWER_WINDOW = STATUS_TIMEOUT + 2


def _local_now() -> datetime:
    """Home Assistant's own wall clock, naive, to compare with the panel's.

    dt_util.now() honours the timezone configured in Home Assistant, which
    is the one the panel's local timestamps are meant to be read against --
    the process timezone of a container often is not.
    """
    return dt_util.now().replace(tzinfo=None)


def _parse_panel_datetime(stamp) -> datetime | None:
    """"HH:MM:SS DD/MM/YYYY" -> datetime, or None if unusable."""
    if not isinstance(stamp, str):
        return None
    try:
        return datetime.strptime(stamp.strip(), PANEL_DATETIME_FORMAT)
    except (ValueError, TypeError):
        return None


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
        # Newest panel timestamp accepted so far: the monotonic floor that
        # a replayed snapshot cannot get under.
        self._last_status_dt: datetime | None = None
        self._snapshots_refused = 0
        self._last_refusal: str | None = None
        self._refusal_warned_at: float | None = None
        # monotonic() deadlines: while one of these is in the future we are
        # waiting for an answer to a request WE published.
        self._status_answer_until = 0.0
        self._command_answer_until = 0.0

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
            "snapshots_refused": self._snapshots_refused,
            "last_refusal": self._last_refusal,
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
        # The subscriptions are gone, so the confirmations that they reached
        # the broker are worthless. Left standing, diagnostic_info() said
        # 'status_subscription_confirmed: true' right next to
        # 'subscriptions: 0' -- contradicting itself in one dictionary, in
        # the field the README offers as the answer to "is the SUBSCRIBE
        # really on the wire".
        self._status_subscribed.clear()
        self._command_subscribed.clear()
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
            # and it may be arbitrarily old. This is the cheap half of the
            # defence; _accept_status is the half that also catches the same
            # snapshot re-imported by the bridge with retain=0.
            self._log_dropped_retained(msg)
            return
        try:
            data = json.loads(msg.payload)
        except (json.JSONDecodeError, ValueError):
            _LOGGER.warning("Invalid MQTT status payload")
            return
        if not isinstance(data, dict):
            _LOGGER.warning("Invalid MQTT status payload")
            return

        answering_us = self._is_answering_us(self._status_answer_until)

        message = data.get("message", "")
        if "401" in message:
            if self._auth:
                self._hass.async_create_task(self._auth.async_handle_401())
            # Wake the waiter: the panel has already said no. Returning
            # silently left async_fetch_state sitting out its whole window
            # and then blaming a timeout, hiding an auth problem behind a
            # connectivity one. Only if we actually asked, though -- an
            # unsolicited 401 answers no request of ours.
            if answering_us:
                self._status_response = data
                self._status_event.set()
            return

        status = self._accept_status(data, msg)
        if status is None:
            return

        if answering_us:
            # This is the reply to a request WE published. It is a POLL, and
            # calling it a push would write last_update_source='mqtt' into
            # the diagnostics -- manufacturing exactly the evidence this
            # work is judged on. async_fetch_state returns it to the
            # coordinator, which records it as a poll.
            self._status_response = data
            self._status_event.set()
            return

        # Nobody asked: this really is a spontaneous update. Both
        # '200 Status OK' and '200 Status Update' can appear here; the
        # string is recorded, never gated on.
        if self._on_push:
            task = self._hass.async_create_task(self._on_push(status))
            self._pending_tasks.add(task)
            # Without this the set only ever grew: one finished Task kept
            # alive per push received, for the life of the entry.
            # _drain_pending is a test helper and async_stop only runs on
            # unload, so nothing pruned it in production.
            task.add_done_callback(self._pending_tasks.discard)

    @staticmethod
    def _is_answering_us(deadline: float) -> bool:
        """True while an answer to a request we published is still due.

        This protocol carries no request id, so there is nothing to match a
        reply against: the only correlation available is "did we ask, and
        was it just now". The window opens at the publish (never before it,
        which is what stops a message that PREDATES the request from
        satisfying it) and closes REQUEST_ANSWER_WINDOW later. Two things
        stay outside what it can distinguish, and both are documented rather
        than papered over: a spontaneous push landing inside the window is
        read as our answer, and an answer arriving after the window has
        closed is read as a push. Narrowing it further would start losing
        real answers; the honest fix is a correlation id the panel does not
        offer.
        """
        return time.monotonic() < deadline

    def _accept_status(self, data: dict, msg) -> dict | None:
        """Return the status payload only if it can be trusted as LIVE.

        See STATUS_MAX_CLOCK_SKEW above for why the retain flag is not
        enough. Every rejection is counted and surfaced in the diagnostics,
        because a guard that silently refuses everything looks exactly like
        a channel that works and has nothing to say.
        """
        status = data.get("status")
        if not isinstance(status, dict) or not status:
            self._refuse(msg, "no status payload")
            return None

        # The state topic is already panel-scoped, but the broker is bridged
        # to another one that mirrors every topic, so check the payload too
        # -- the same check the probe does on /elmax/response/id.
        # Deliberately asymmetric: a MISMATCH is refused, an ABSENCE is not.
        # coordinator._parse already defaults this field to the configured
        # panel id and no panel has ever been observed to omit it; refusing
        # on absence would be a guess about a format we have not measured,
        # with the whole channel as the stake.
        centrale = status.get("centrale", data.get("centrale"))
        if centrale is not None and str(centrale) != self._panel_id:
            self._refuse(msg, f"belongs to panel {centrale}")
            return None

        stamp = status.get("datetime")
        panel_dt = _parse_panel_datetime(stamp)
        if panel_dt is None:
            # Explicit decision: no usable timestamp means no way to tell a
            # live update from a replay, and on an alarm panel that has to
            # resolve to "refuse". The cost is a poll: HTTP is tried first
            # and the reconciliation loop keeps running.
            self._refuse(msg, f"no usable timestamp ({stamp!r})")
            return None

        skew = (_local_now() - panel_dt).total_seconds()
        if abs(skew) > STATUS_MAX_CLOCK_SKEW:
            self._refuse(
                msg, f"stamped {stamp}, {int(skew)}s away from our clock",
            )
            return None

        if self._last_status_dt is not None and panel_dt < self._last_status_dt:
            # A replay does not have to be months old to hurt: any snapshot
            # older than the one we are already showing walks the areas
            # backwards. Known consequence, accepted: at the autumn DST
            # change the panel's local clock repeats an hour, so MQTT status
            # is refused for up to that hour while HTTP polling carries on.
            # Refusing a real update for an hour is the safe direction, and
            # it heals itself.
            self._refuse(
                msg,
                f"stamped {stamp}, older than the last accepted "
                f"({self._last_status_dt.strftime(PANEL_DATETIME_FORMAT)})",
            )
            return None

        self._last_status_dt = panel_dt
        return status

    def _refuse(self, msg, reason: str) -> None:
        """Count and report a payload that could not be trusted as live."""
        self._snapshots_refused += 1
        self._last_refusal = reason
        topic = getattr(msg, "topic", "?")
        now = time.monotonic()
        if (self._refusal_warned_at is not None
                and now - self._refusal_warned_at < RETAINED_WARN_INTERVAL):
            _LOGGER.debug("Refused a message on %s: %s (%d so far)",
                          topic, reason, self._snapshots_refused)
            return
        self._refusal_warned_at = now
        _LOGGER.warning(
            "Refusing a message on %s: %s. It is not treated as live state "
            "(%d refused so far). A replayed snapshot reaches us with the "
            "retain flag cleared when a bridge re-imports it, so this check "
            "is on the content, not on the flag. If this repeats while the "
            "panel is healthy, compare the panel's clock with Home "
            "Assistant's.",
            topic, reason, self._snapshots_refused,
        )

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
            data = json.loads(msg.payload)
        except (json.JSONDecodeError, ValueError):
            _LOGGER.warning("Invalid MQTT command payload")
            return
        if not isinstance(data, dict):
            _LOGGER.warning("Invalid MQTT command payload")
            return
        # Same panel check as the state channel, and for a sharper reason:
        # this is what turns into "the panel executed your arm/disarm".
        centrale = data.get("centrale")
        if centrale is not None and str(centrale) != self._panel_id:
            self._refuse(msg, f"command reply belongs to panel {centrale}")
            return
        if not self._is_answering_us(self._command_answer_until):
            # Nobody here published a command, so this confirms nothing of
            # ours. The bridge mirrors this topic from another broker.
            _LOGGER.debug(
                "Ignoring an unsolicited command reply on %s",
                getattr(msg, "topic", "?"),
            )
            return
        self._command_response = data
        self._command_event.set()

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
        requests = 0
        try:
            async with asyncio.timeout(STATUS_TIMEOUT):
                await self._async_await_subscription(self._status_subscribed)
                while True:
                    # Arm the latch immediately BEFORE the publish, once per
                    # request. Clearing it once at the top of the function
                    # left the whole subscription wait open: anything that
                    # landed in there satisfied a request not yet sent, and
                    # the answer window (which is all the correlation this
                    # protocol allows) must not start before the request.
                    self._status_response = None
                    self._status_event.clear()
                    self._status_answer_until = (
                        time.monotonic() + REQUEST_ANSWER_WINDOW
                    )
                    try:
                        await mqtt.async_publish(self._hass, topic, payload)
                    except HomeAssistantError as err:
                        self._status_answer_until = 0.0
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
        # while it kept polling perfectly well -- and both the diagnostics
        # dump and the staleness log line went on reporting a failure that
        # had already recovered. (An earlier version of this comment also
        # named get_active_push_transports(); that was wrong -- it has no
        # caller in production, exactly like degraded_or_unsupported().)
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
            # Nothing was published: another transport may safely try.
            return CommandResult(ok=False, error="not_started",
                                 safe_to_retry=True)
        try:
            token = await self._auth.async_get_token()
        except ElmaxAuthError as err:
            self._state = TransportState.DEGRADED
            return CommandResult(ok=False, error=f"auth: {err}",
                                 safe_to_retry=True)

        body = {"token": f"JWT {token}", "eid": endpoint_id, "cmd": cmd}
        if code:
            body["code"] = code
        # Wait for the reply subscription BEFORE publishing. The command
        # goes out exactly once, so a confirmation lost to the subscribe
        # race is unrecoverable, and this is the one path where losing the
        # answer is worse than losing the request.
        await self._async_await_subscription(self._command_subscribed)
        # Latch armed immediately before the publish, never earlier: a '200'
        # that landed during the subscription wait would otherwise confirm a
        # command that had not been sent yet.
        self._command_response = None
        self._command_event.clear()
        self._command_answer_until = time.monotonic() + REQUEST_ANSWER_WINDOW
        try:
            await mqtt.async_publish(
                self._hass,
                TOPIC_REQUEST_COMMAND.format(panel_id=self._panel_id),
                json.dumps(body),
            )
        except HomeAssistantError as err:
            # Nothing left the box: a clean failure, the panel did nothing.
            self._command_answer_until = 0.0
            self._state = TransportState.DEGRADED
            return CommandResult(ok=False, error=f"broker: {err}",
                                 safe_to_retry=True)
        try:
            await asyncio.wait_for(self._command_event.wait(), COMMAND_TIMEOUT)
        except (asyncio.TimeoutError, TimeoutError):
            # The command IS on the wire. A missing confirmation says
            # nothing about whether the panel ran it, so this must never be
            # handed to another transport to try again.
            return CommandResult(ok=False, error="timeout")
        if self._command_response:
            msg = self._command_response.get("message", "")
            if "200" in msg:
                self._state = TransportState.READY
                return CommandResult(ok=True, raw_response=self._command_response)
            return CommandResult(ok=False, error=msg,
                                 raw_response=self._command_response)
        return CommandResult(ok=False, error="empty_response")

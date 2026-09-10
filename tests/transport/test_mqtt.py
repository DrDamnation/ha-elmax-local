"""Test MqttTransport.

The probe tests deliberately drive the REAL HA mqtt client with paho mocked
(fixtures `mqtt_mock` / `mqtt_client_mock`) instead of injecting replies with
`async_fire_mqtt_message` alone: HA registers a subscription in memory the
instant `async_subscribe` is awaited, but only sends the SUBSCRIBE packet to
the broker later, from a debounced job (INITIAL_SUBSCRIBE_COOLDOWN = 0.5 s).
A test that fires the reply straight into HA therefore passes even when the
transport publishes into a blind window, which is exactly the bug being
fixed. `qos0_broker` below models the broker instead: a non-retained QoS-0
reply is delivered only if the SUBSCRIBE has actually reached the wire.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.exceptions import HomeAssistantError
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import async_fire_mqtt_message

from custom_components.elmax_local.auth import AuthManager, ElmaxAuthError
from custom_components.elmax_local.const import (
    TOPIC_REQUEST_COMMAND, TOPIC_REQUEST_ID, TOPIC_REQUEST_STATUS,
    TOPIC_RESPONSE_COMMAND, TOPIC_RESPONSE_ID, TOPIC_RESPONSE_STATUS,
)
from custom_components.elmax_local.transport import (
    TransportCapability, TransportState,
)
from custom_components.elmax_local.transport import mqtt as mqtt_module
from custom_components.elmax_local.transport.mqtt import MqttTransport

PANEL_ID = "abc123"

# Real reply captured from the panel on 2026-09-10 (retain=0, qos=0).
ID_REPLY = json.dumps({
    "centrale": PANEL_ID,
    "tipo_accessorio": "VideoBox",
    "release_accessorio": "4.17.7",
    "release": "PHANTOM64PRO_GSM 13.9L.844",
})
OTHER_PANEL_REPLY = json.dumps({
    "centrale": "0000000000000000000",
    "tipo_accessorio": "VideoBox",
})

STATUS_REQ_TOPIC = TOPIC_REQUEST_STATUS.format(panel_id=PANEL_ID)
STATUS_RESP_TOPIC = TOPIC_RESPONSE_STATUS.format(panel_id=PANEL_ID)
COMMAND_REQ_TOPIC = TOPIC_REQUEST_COMMAND.format(panel_id=PANEL_ID)
COMMAND_RESP_TOPIC = TOPIC_RESPONSE_COMMAND.format(panel_id=PANEL_ID)

PANEL_DT_FMT = "%H:%M:%S %d/%m/%Y"


def panel_stamp(offset: timedelta = timedelta(0)) -> str:
    """A panel 'datetime' field, relative to HA's own wall clock."""
    return (dt_util.now().replace(tzinfo=None) + offset).strftime(PANEL_DT_FMT)


PANEL_STATUS_BASE = {
    "centrale": PANEL_ID,
    "release": "PHANTOM64PRO_GSM 13.9L.844",
    "zone": [], "aree": [], "uscite": [], "scenari": [],
}

STATUS_401_REPLY = json.dumps({"message": "401 Unauthorized"})
COMMAND_OK_REPLY = json.dumps({"message": "200 Command OK"})


@pytest.fixture
def panel_status(hass):
    """A LIVE status payload: the panel stamps each one with its own clock.

    Built per test rather than at import time for two reasons. The
    transport now refuses a snapshot whose own timestamp is old, which is
    the whole point of the re-import guard; and the stamp has to be read
    against Home Assistant's configured timezone, which only exists once
    the hass fixture does.
    """
    return dict(PANEL_STATUS_BASE, datetime=panel_stamp())


@pytest.fixture
def status_reply(panel_status):
    """"200 Status Update" on purpose: it is the ONLY message string ever
    observed from this panel. The reply to a request has never been
    measured, so nothing may gate on "200 Status OK"."""
    return json.dumps({"message": "200 Status Update", "status": panel_status})


# The retained snapshot that sits on /elmax/response/status/<panel> on the
# live broker RIGHT NOW, in duplicate (locally and on the NAS at .69):
# every area disarmed, every zone closed, panel clock reading May.
# `nas-bridge` is configured "topic # both 0" with cleansession false, so it
# can push this back at any reconnection -- and MQTT-3.3.1-9 says a retained
# message forwarded to an ALREADY SUBSCRIBED client arrives with retain=0.
# That is why the guard cannot be a flag test.
MAY_SNAPSHOT = {
    "centrale": PANEL_ID,
    "release": "PHANTOM64PRO_GSM 13.2.844",
    "tipo_accessorio": "VideoBox",
    "release_accessorio": "4.16.14",
    "datetime": "22:20:20 15/05/2026",
    "zone": [{"endpointId": f"{PANEL_ID}-zona-{i}", "indice": i,
              "visibile": True, "aperta": False, "esclusa": False,
              "nome": f"ZONA {i + 1:02d}"} for i in range(39)],
    "aree": [{"endpointId": f"{PANEL_ID}-area-{i}", "indice": i,
              "visibile": True, "stato": 0, "statoSessione": 1,
              "statiDisponibili": [0, 1, 2, 3, 4],
              "nome": f"AREA {i + 1}"} for i in range(5)],
    "uscite": [{"endpointId": f"{PANEL_ID}-uscita-{i}", "indice": i,
                "visibile": True, "aperta": False,
                "nome": f"USCITA {i + 1}"} for i in range(5)],
    "scenari": [{"endpointId": f"{PANEL_ID}-scenario-{i}", "indice": i,
                 "visibile": True, "nome": f"SCENARIO {i + 1}"}
                for i in range(9)],
}
MAY_SNAPSHOT_REPLY = json.dumps({"message": "200 Status Update",
                                 "status": MAY_SNAPSHOT})


@pytest.fixture
async def auth(hass):
    am = AuthManager(hass, "1.2.3.4", "000000")
    am._token = "JWT_TOKEN"
    am._expiry = 9999999999
    yield am
    await am.async_close()


@pytest.fixture
def mqtt_transport(hass, auth):
    return MqttTransport(hass, PANEL_ID)


class _Broker:
    """Records the wire and answers /elmax/request/* like the panel does."""

    def __init__(self, status_reply: str):
        self.wire: list[tuple[str, str]] = []
        self.on_wire: set[str] = set()
        self.reply: str | None = ID_REPLY
        self.retain = False
        self.requests = 0
        self.dropped = 0
        self.status_reply: str | None = status_reply
        self.status_requests = 0
        # The panel ignores everything before this request number: models a
        # request that was published but never answered.
        self.answer_status_from = 1
        self.command_reply: str | None = COMMAND_OK_REPLY
        self.command_requests = 0


@pytest.fixture
def qos0_broker(hass, mqtt_mock, mqtt_client_mock, status_reply):
    """QoS-0 broker semantics on top of the mocked paho client."""
    broker = _Broker(status_reply)
    orig_sub = mqtt_client_mock.subscribe.side_effect
    orig_pub = mqtt_client_mock.publish.side_effect

    def _subscribe(topic, qos=0, *args, **kwargs):
        entries = topic if isinstance(topic, list) else [(topic, qos)]
        for item in entries:
            name = item[0] if isinstance(item, (list, tuple)) else item
            broker.on_wire.add(name)
            broker.wire.append(("SUBSCRIBE", name))
        return orig_sub(topic, qos)

    def _deliver(response_topic: str, reply: str, retain: bool = False) -> None:
        if reply is None:
            return
        if response_topic in broker.on_wire:
            # Subscriber present at this instant -> mosquitto delivers.
            hass.loop.call_soon(
                async_fire_mqtt_message, hass,
                response_topic, reply, 0, retain,
            )
        else:
            # No subscriber: a non-retained QoS-0 message is discarded
            # and is NOT recoverable by a later subscription.
            broker.dropped += 1

    def _publish(topic, payload=None, qos=0, retain=False, *args, **kwargs):
        broker.wire.append(("PUBLISH", topic))
        if topic == TOPIC_REQUEST_ID:
            broker.requests += 1
            _deliver(TOPIC_RESPONSE_ID, broker.reply, broker.retain)
        elif topic == STATUS_REQ_TOPIC:
            broker.status_requests += 1
            if broker.status_requests >= broker.answer_status_from:
                _deliver(STATUS_RESP_TOPIC, broker.status_reply)
        elif topic == COMMAND_REQ_TOPIC:
            broker.command_requests += 1
            _deliver(COMMAND_RESP_TOPIC, broker.command_reply)
        return orig_pub(topic, payload, qos, retain)

    mqtt_client_mock.subscribe.side_effect = _subscribe
    mqtt_client_mock.publish.side_effect = _publish
    return broker


async def _let_the_mqtt_misc_timer_fire() -> None:
    """Fixture artifact, nothing to do with the transport.

    With paho mocked, HA's MQTT misc timer fires once and then stops
    (loop_misc returns a Mock, never MQTT_ERR_SUCCESS). A test fast enough
    to end while that timer is still armed trips HA's "lingering timer"
    cleanup check. Let it fire.
    """
    await asyncio.sleep(1.05)


def test_capabilities(mqtt_transport):
    assert TransportCapability.PUSH in mqtt_transport.capabilities
    assert TransportCapability.POLL in mqtt_transport.capabilities
    assert TransportCapability.COMMAND in mqtt_transport.capabilities


async def test_distinguishes_status_update_from_response(mqtt_transport, auth,
                                                          panel_status):
    """Push spontaneo: 'message' = '200 Status Update'.
    Response a request: 'message' = '200 Status OK'.
    Entrambi devono triggerare on_state_update.

    Nothing gates on the string, then or now. What did change is that both
    payloads have to carry a live panel timestamp: an update whose own
    clock reads months ago is a replay whatever its 'message' says.
    """
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    # Simula push spontaneo (live: retain=0)
    msg_update = MagicMock(retain=False)
    msg_update.topic = STATUS_RESP_TOPIC
    msg_update.payload = json.dumps({"message": "200 Status Update",
                                     "status": panel_status})
    mqtt_transport._handle_status_message(msg_update)

    # Simula response a request (live: retain=0)
    msg_response = MagicMock(retain=False)
    msg_response.topic = STATUS_RESP_TOPIC
    msg_response.payload = json.dumps({"message": "200 Status OK",
                                       "status": panel_status})
    mqtt_transport._handle_status_message(msg_response)

    # Entrambi devono triggerare on_state_update con payload status
    await mqtt_transport._drain_pending()
    assert len(pushes) == 2
    assert all(p == panel_status for p in pushes)


# --- Probe: delivery guarantee (regression: publish into a blind window) ---

async def test_probe_succeeds_against_a_qos0_broker(hass, qos0_broker,
                                                    mqtt_transport):
    """The panel answers every request, yet the probe used to fail.

    Nothing here is unreachable or slow: the panel replies to each request
    within the same tick. The probe must not lose the answer just because it
    asked before its own subscription reached the broker.
    """
    with patch.object(mqtt_module, "PROBE_TIMEOUT", 6):
        assert await mqtt_transport.async_probe() is True


async def test_probe_subscribes_before_it_publishes(hass, qos0_broker,
                                                    mqtt_transport):
    """Ordering on the wire, which is what makes the answer deliverable."""
    with patch.object(mqtt_module, "PROBE_TIMEOUT", 6):
        await mqtt_transport.async_probe()

    steps = [s for s in qos0_broker.wire
             if s[1] in (TOPIC_REQUEST_ID, TOPIC_RESPONSE_ID)]
    assert steps, "probe touched neither the request nor the response topic"
    assert steps[0] == ("SUBSCRIBE", TOPIC_RESPONSE_ID), (
        f"first wire event was {steps[0]}, so the request was published "
        f"before anyone was subscribed to the answer: {steps}"
    )
    assert qos0_broker.dropped == 0


async def test_probe_ignores_a_reply_from_another_panel(hass, qos0_broker,
                                                        mqtt_transport):
    """The bridge mirrors other panels; only our own id validates the probe."""
    qos0_broker.reply = OTHER_PANEL_REPLY
    with patch.object(mqtt_module, "PROBE_TIMEOUT", 2):
        assert await mqtt_transport.async_probe() is False


async def test_probe_returns_false_when_the_broker_refuses(hass, qos0_broker,
                                                           mqtt_transport):
    """Broker down at boot: a clean False, never an exception."""
    with patch.object(mqtt_module, "PROBE_TIMEOUT", 2), \
         patch.object(mqtt_module.mqtt, "async_publish",
                      new=AsyncMock(side_effect=HomeAssistantError(
                          "mqtt_broker_error"))):
        assert await mqtt_transport.async_probe() is False


async def test_probe_recovers_when_the_broker_comes_up_late(hass, qos0_broker,
                                                            mqtt_transport):
    """HA and mosquitto start together: the first publishes are refused."""
    real_publish = mqtt_module.mqtt.async_publish
    attempts = {"n": 0}

    async def _flaky(hass_arg, topic, payload, *args, **kwargs):
        if topic == TOPIC_REQUEST_ID:
            attempts["n"] += 1
            if attempts["n"] <= 2:
                raise HomeAssistantError("mqtt_broker_error")
        return await real_publish(hass_arg, topic, payload, *args, **kwargs)

    with patch.object(mqtt_module, "PROBE_TIMEOUT", 8), \
         patch.object(mqtt_module, "PROBE_REPLY_WAIT", 0.5, create=True), \
         patch.object(mqtt_module.mqtt, "async_publish", new=_flaky):
        assert await mqtt_transport.async_probe() is True
    assert attempts["n"] >= 3


# --- Retained snapshots must never be mistaken for live state ---

async def test_retained_status_is_not_forwarded(mqtt_transport, auth,
                                                mock_panel_data):
    """A retained message is the broker's stored snapshot, not a push."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    msg = MagicMock(retain=True)
    msg.topic = TOPIC_RESPONSE_STATUS.format(panel_id=PANEL_ID)
    msg.payload = json.dumps({"message": "200 Status Update",
                              "status": mock_panel_data})
    mqtt_transport._handle_status_message(msg)

    await mqtt_transport._drain_pending()
    assert pushes == []
    assert not mqtt_transport._status_event.is_set()


async def test_retained_status_from_the_broker_never_reaches_coordinator(
        hass, qos0_broker, mqtt_transport, auth, panel_status, status_reply):
    """End to end: the May snapshot on the live broker must be dropped,
    while the next live (non-retained) update still gets through."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    await mqtt_transport.async_start(auth, on_push)
    topic = TOPIC_RESPONSE_STATUS.format(panel_id=PANEL_ID)

    async_fire_mqtt_message(hass, topic, MAY_SNAPSHOT_REPLY, 0, True)
    await hass.async_block_till_done()
    await mqtt_transport._drain_pending()
    assert pushes == [], "retained snapshot was ingested as live state"

    async_fire_mqtt_message(hass, topic, status_reply, 0, False)
    await hass.async_block_till_done()
    await mqtt_transport._drain_pending()
    assert pushes == [panel_status]

    await mqtt_transport.async_stop()
    await hass.async_block_till_done()
    await _let_the_mqtt_misc_timer_fire()


async def test_retained_command_reply_does_not_confirm_a_command(
        mqtt_transport, auth):
    """A retained command response would confirm a command never executed."""
    msg = MagicMock(retain=True)
    msg.topic = "/elmax/response/command/" + PANEL_ID
    msg.payload = json.dumps({"message": "200 Command OK"})
    mqtt_transport._handle_command_message(msg)
    assert not mqtt_transport._command_event.is_set()


def test_transport_starts_disabled(mqtt_transport):
    assert mqtt_transport.state is TransportState.DISABLED


# --- The branch that actually runs in production (HA >= 2025.x) ---
# The pinned test HA (2024.12) has no mqtt.async_on_subscribe_done, so the
# tests above exercise the grace-period fallback. These two inject the API to
# cover the preferred path and, above all, to prove the probe cannot hang on
# a signal that never arrives.

async def test_probe_uses_the_subscribe_done_signal_when_available(
        hass, qos0_broker, mqtt_transport):
    seen = []
    cancelled = []

    def _fake_on_subscribe_done(hass_arg, topic, qos, cb):
        seen.append((topic, qos))
        handle = hass.loop.call_later(0.4, cb, True)

        def _cancel():
            cancelled.append(True)
            handle.cancel()

        return _cancel

    with patch.object(mqtt_module, "PROBE_TIMEOUT", 6), \
         patch.object(mqtt_module.mqtt, "async_on_subscribe_done",
                      new=_fake_on_subscribe_done, create=True):
        assert await mqtt_transport.async_probe() is True

    assert seen == [(TOPIC_RESPONSE_ID, 0)]
    assert cancelled, "the subscription watch was never cancelled"


async def test_probe_does_not_hang_if_the_signal_never_fires(
        hass, qos0_broker, mqtt_transport):
    """A confirmation that never comes must not cost us the whole window."""
    def _never(hass_arg, topic, qos, cb):
        return lambda: None

    with patch.object(mqtt_module, "PROBE_TIMEOUT", 6), \
         patch.object(mqtt_module, "SUBSCRIBE_CONFIRM_WAIT", 1), \
         patch.object(mqtt_module.mqtt, "async_on_subscribe_done",
                      new=_never, create=True):
        assert await mqtt_transport.async_probe() is True


async def test_probe_survives_a_broken_subscribe_done_api(
        hass, qos0_broker, mqtt_transport):
    """An unexpected signature must degrade to the grace period, not crash."""
    def _broken(*args, **kwargs):
        raise TypeError("unexpected signature")

    with patch.object(mqtt_module, "PROBE_TIMEOUT", 6), \
         patch.object(mqtt_module.mqtt, "async_on_subscribe_done",
                      new=_broken, create=True):
        assert await mqtt_transport.async_probe() is True


# --- The data path: fetch_state (C1, C4, C5, C11) ----------------------
# These were completely uncovered: 14 tests guarded the gate (the probe)
# and none the function the whole fix exists for.


@pytest.fixture
def instant_panel(hass, mqtt_transport, status_reply):
    """A panel that answers in the same tick, without going through HA mqtt.

    Used where the point is the transport's own logic (401, auth, command
    handling), not delivery. SUBSCRIBE_CONFIRM_WAIT is shortened because
    these tests skip the real subscriptions on purpose.
    """
    class _Panel:
        def __init__(self):
            self.status_requests: list[str] = []
            self.command_requests: list[str] = []
            self.status_reply: str | None = status_reply
            self.command_reply: str | None = COMMAND_OK_REPLY

        async def publish(self, hass_arg, topic, payload=None, *args, **kwargs):
            if topic == STATUS_REQ_TOPIC:
                self.status_requests.append(payload)
                self._answer(topic, self.status_reply,
                             mqtt_transport._handle_status_message)
            elif topic == COMMAND_REQ_TOPIC:
                self.command_requests.append(payload)
                self._answer(topic, self.command_reply,
                             mqtt_transport._handle_command_message)

        @staticmethod
        def _answer(topic, reply, handler):
            if reply is None:
                return
            msg = MagicMock(retain=False)
            msg.topic = topic
            msg.payload = reply
            hass.loop.call_soon(handler, msg)

    panel = _Panel()
    with patch.object(mqtt_module.mqtt, "async_publish", new=panel.publish), \
         patch.object(mqtt_module, "SUBSCRIBE_CONFIRM_WAIT", 0.05):
        yield panel


async def _start_offline(transport, auth):
    """Start without touching HA's mqtt integration."""
    with patch.object(transport, "_subscribe_responses", new=AsyncMock()):
        await transport.async_start(auth, AsyncMock())


async def test_fetch_state_returns_the_payload_and_marks_ready(
        hass, qos0_broker, mqtt_transport, auth, panel_status):
    """The first poll after start must not lose the race against HA's
    subscription debouncer: the panel answers, so the transport must return
    that answer and report itself READY."""
    await mqtt_transport.async_start(auth, AsyncMock())

    result = await mqtt_transport.async_fetch_state()

    assert result == panel_status
    assert mqtt_transport.state is TransportState.READY
    assert qos0_broker.dropped == 0
    await mqtt_transport.async_stop()
    await hass.async_block_till_done()


async def test_fetch_state_recovers_from_degraded_after_a_timeout(
        hass, qos0_broker, mqtt_transport, auth, panel_status, status_reply):
    """A single mute poll must not label mqtt 'degraded' for ever.

    http.py:77 and panel.py both put the transport back to READY on a
    successful fetch; mqtt only ever moved to DEGRADED, so the first timeout
    was permanent until a reload -- and the diagnostics, the staleness line
    and get_active_push_transports() all kept reporting a lie.
    """
    qos0_broker.status_reply = None  # panel mute
    await mqtt_transport.async_start(auth, AsyncMock())

    with patch.object(mqtt_module, "STATUS_TIMEOUT", 2):
        assert await mqtt_transport.async_fetch_state() is None
    assert mqtt_transport.state is TransportState.DEGRADED

    qos0_broker.status_reply = status_reply
    assert await mqtt_transport.async_fetch_state() == panel_status
    assert mqtt_transport.state is TransportState.READY

    await mqtt_transport.async_stop()
    await hass.async_block_till_done()


async def test_fetch_state_asks_again_inside_the_window(
        hass, qos0_broker, mqtt_transport, auth, panel_status):
    """One QoS-0 request that nobody answers is gone: ask again.

    The panel here ignores the first request; the answer to the second one
    is inside the same 8s budget, so the poll must still succeed.
    """
    qos0_broker.answer_status_from = 2
    await mqtt_transport.async_start(auth, AsyncMock())

    with patch.object(mqtt_module, "STATUS_REPLY_WAIT", 1, create=True):
        result = await mqtt_transport.async_fetch_state()

    assert result == panel_status
    assert qos0_broker.status_requests >= 2
    await mqtt_transport.async_stop()
    await hass.async_block_till_done()


async def test_fetch_state_on_401_degrades_without_waiting_out_the_window(
        hass, mqtt_transport, auth, instant_panel):
    """A 401 answer used to be dropped before waking the waiter: the panel
    had already said no and the transport still sat there for 8 seconds,
    then blamed a timeout. http.py handles 401 as 'DEGRADED, return None'."""
    instant_panel.status_reply = STATUS_401_REPLY
    await _start_offline(mqtt_transport, auth)

    with patch.object(auth, "async_handle_401", new=AsyncMock()) as handled:
        started = time.monotonic()
        result = await mqtt_transport.async_fetch_state()
        elapsed = time.monotonic() - started
        await hass.async_block_till_done()

    assert result is None
    assert elapsed < 2, f"waited {elapsed:.1f}s for an answer already received"
    assert mqtt_transport.state is TransportState.DEGRADED
    handled.assert_awaited()


async def test_fetch_state_auth_failure_does_not_leave_mqtt_ready(
        hass, mqtt_transport, auth, instant_panel):
    """MQTT still needs a JWT taken over HTTPS. When that fails the channel
    is unusable, so it must not keep advertising itself as READY -- the
    registry swallows the exception and nobody would ever notice."""
    await _start_offline(mqtt_transport, auth)

    with patch.object(auth, "async_get_token",
                      new=AsyncMock(side_effect=ElmaxAuthError("backoff"))):
        result = await mqtt_transport.async_fetch_state()

    assert result is None
    assert mqtt_transport.state is not TransportState.READY
    assert instant_panel.status_requests == []


async def test_fetch_state_records_which_message_answered_it(
        hass, mqtt_transport, auth, instant_panel, panel_status):
    """Which message satisfied the poll is the open question of this whole
    investigation ('the panel answers US' vs 'the panel just publishes'), so
    record it. Deliberately no gate on the string: the reply to a request
    has never been measured on this panel."""
    await _start_offline(mqtt_transport, auth)

    assert await mqtt_transport.async_fetch_state() == panel_status
    assert mqtt_transport.diagnostic_info()["last_fetch_message"] == (
        "200 Status Update")


# --- The data path: send_command (C5, C11) ------------------------------


async def test_send_command_returns_ok_on_a_200_reply(
        hass, mqtt_transport, auth, instant_panel):
    await _start_offline(mqtt_transport, auth)

    result = await mqtt_transport.async_send_command("abc123-uscita-0", "1")

    assert result.ok is True
    assert len(instant_panel.command_requests) == 1
    body = json.loads(instant_panel.command_requests[0])
    assert body["eid"] == "abc123-uscita-0" and body["cmd"] == "1"


async def test_send_command_is_published_exactly_once(
        hass, mqtt_transport, auth, instant_panel):
    """A lost confirmation is NOT a lost command: the panel may have
    executed it. A command must never be republished on a timeout."""
    instant_panel.command_reply = None
    await _start_offline(mqtt_transport, auth)

    with patch.object(mqtt_module, "COMMAND_TIMEOUT", 1):
        result = await mqtt_transport.async_send_command("abc123-area-0", "1")

    assert result.ok is False and result.error == "timeout"
    assert len(instant_panel.command_requests) == 1


async def test_send_command_auth_failure_is_reported_not_raised(
        hass, mqtt_transport, auth, instant_panel):
    await _start_offline(mqtt_transport, auth)

    with patch.object(auth, "async_get_token",
                      new=AsyncMock(side_effect=ElmaxAuthError("backoff"))):
        result = await mqtt_transport.async_send_command("abc123-area-0", "1")

    assert result.ok is False
    assert instant_panel.command_requests == []
    assert mqtt_transport.state is not TransportState.READY


async def test_send_command_without_start_is_not_started(mqtt_transport):
    result = await mqtt_transport.async_send_command("abc123-area-0", "1")
    assert result.ok is False and result.error == "not_started"


# --- Subscriptions are not doubled by a re-probe (C9) -------------------


async def test_starting_twice_does_not_double_the_subscriptions(
        hass, qos0_broker, mqtt_transport, auth, panel_status, status_reply):
    """async_start now has a second call site (the registry's re-probe).
    Appending to self._unsubs left the old callback live, so every push
    would be handled N times -- N coordinator updates per message."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    await mqtt_transport.async_start(auth, on_push)
    await mqtt_transport.async_start(auth, on_push)  # what a re-probe does

    assert len(mqtt_transport._unsubs) == 2

    async_fire_mqtt_message(hass, STATUS_RESP_TOPIC, status_reply, 0, False)
    await hass.async_block_till_done()
    await mqtt_transport._drain_pending()

    assert pushes == [panel_status]
    await mqtt_transport.async_stop()
    await hass.async_block_till_done()
    await _let_the_mqtt_misc_timer_fire()


# --- Push tasks must not pile up (C10) ---------------------------------


async def test_finished_push_tasks_are_dropped(
        hass, qos0_broker, mqtt_transport, auth, status_reply):
    """_drain_pending is a test helper; in production nothing pruned the
    list, so a READY mqtt leaked one Task object per push, for ever."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    await mqtt_transport.async_start(auth, on_push)
    for _ in range(3):
        async_fire_mqtt_message(hass, STATUS_RESP_TOPIC, status_reply, 0, False)
    await hass.async_block_till_done()

    assert len(pushes) == 3
    assert not mqtt_transport._pending_tasks
    await mqtt_transport.async_stop()
    await hass.async_block_till_done()
    await _let_the_mqtt_misc_timer_fire()


# --- Retained messages: keep saying it, and count them (C6) ------------


async def test_retained_warning_comes_back_after_the_throttle_window(
        mqtt_transport, auth, caplog, mock_panel_data):
    """Warning once per process would make a panel that pushes everything
    retained look healthy and be completely mute, with one line buried in
    the boot log."""
    await _start_offline(mqtt_transport, auth)

    msg = MagicMock(retain=True)
    msg.topic = STATUS_RESP_TOPIC
    msg.payload = json.dumps({"message": "200 Status Update",
                              "status": mock_panel_data})

    with caplog.at_level(logging.WARNING):
        mqtt_transport._handle_status_message(msg)
        mqtt_transport._handle_status_message(msg)
    assert caplog.text.count("Ignoring RETAINED") == 1
    assert mqtt_transport.diagnostic_info()["retained_dropped"] == 2

    with caplog.at_level(logging.WARNING), \
         patch.object(mqtt_module, "RETAINED_WARN_INTERVAL", 0):
        mqtt_transport._handle_status_message(msg)
    assert caplog.text.count("Ignoring RETAINED") == 2
    assert mqtt_transport.diagnostic_info()["retained_dropped"] == 3


# --- The probe must not report 'nobody answered' when somebody did (C8) --


async def test_probe_reports_a_retained_reply_instead_of_no_responders(
        hass, qos0_broker, mqtt_transport, caplog):
    """A retained identification still does not prove the panel is alive, so
    the probe rightly fails -- but the log line the deploy instructions tell
    you to read must not say 'responders=none', which reads as 'the panel
    ignores Home Assistant'."""
    qos0_broker.retain = True

    with caplog.at_level(
            logging.DEBUG,
            logger="custom_components.elmax_local.transport.mqtt"), \
         patch.object(mqtt_module, "PROBE_TIMEOUT", 2):
        assert await mqtt_transport.async_probe() is False

    assert "responders=['" + PANEL_ID + "']" in caplog.text
    assert "retained_replies=" in caplog.text
    assert "retained_replies=0" not in caplog.text


# --- P1: the re-import guard must look at the CONTENT, not at the flag ---
#
# The flag guard (`if msg.retain`) is blind to the one delivery path that
# matters here. mosquitto's nas-bridge is "topic # both 0", cleansession
# false, restart_timeout 10; when it reconnects it re-publishes what it has
# stored, and MQTT-3.3.1-9 says a retained message forwarded to a client
# that is ALREADY SUBSCRIBED is delivered with the retain flag CLEARED. So
# the May snapshot comes back looking exactly like a live push, and on an
# inhabited house the visible result is five areas reading "Disarmed" while
# the panel is armed -- with the staleness watchdog silent, because the
# coordinator would have just marked the data fresh.


async def test_the_may_snapshot_reimported_with_retain_0_is_refused(
        hass, qos0_broker, mqtt_transport, auth):
    """THE decisive test: the real payload, re-delivered with retain=0."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    await mqtt_transport.async_start(auth, on_push)

    # retain=0: this is the bridge re-import, not a first subscription.
    async_fire_mqtt_message(hass, STATUS_RESP_TOPIC, MAY_SNAPSHOT_REPLY,
                            0, False)
    await hass.async_block_till_done()
    await mqtt_transport._drain_pending()

    assert pushes == [], (
        "the May snapshot was ingested as live state: five areas would "
        "read 'Disarmed' while the panel is armed"
    )
    assert not mqtt_transport._status_event.is_set()

    await mqtt_transport.async_stop()
    await hass.async_block_till_done()
    await _let_the_mqtt_misc_timer_fire()


async def test_a_reimported_snapshot_cannot_satisfy_a_poll_either(
        hass, mqtt_transport, auth, instant_panel):
    """Refusing it for the push path is not enough: the poll would ingest
    the very same payload and hand it to the coordinator as fresh state."""
    instant_panel.status_reply = MAY_SNAPSHOT_REPLY
    await _start_offline(mqtt_transport, auth)

    with patch.object(mqtt_module, "STATUS_TIMEOUT", 2), \
         patch.object(mqtt_module, "STATUS_REPLY_WAIT", 0.5, create=True):
        result = await mqtt_transport.async_fetch_state()

    assert result is None
    assert mqtt_transport.state is TransportState.DEGRADED


async def test_a_snapshot_older_than_the_last_accepted_one_is_refused(
        hass, mqtt_transport, auth, panel_status):
    """Monotonicity. A re-import does not have to be months old to hurt:
    a snapshot from ten minutes ago is inside any sane freshness window and
    would still walk the areas backwards."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    live = MagicMock(retain=False)
    live.topic = STATUS_RESP_TOPIC
    live.payload = json.dumps({"message": "200 Status Update",
                               "status": panel_status})
    mqtt_transport._handle_status_message(live)
    await mqtt_transport._drain_pending()
    assert len(pushes) == 1

    older = MagicMock(retain=False)
    older.topic = STATUS_RESP_TOPIC
    older.payload = json.dumps({
        "message": "200 Status Update",
        "status": dict(panel_status,
                       datetime=panel_stamp(timedelta(minutes=-10))),
    })
    mqtt_transport._handle_status_message(older)
    await mqtt_transport._drain_pending()

    assert len(pushes) == 1, "state went backwards in time"


async def test_a_status_from_another_panel_is_refused(
        hass, mqtt_transport, auth, panel_status):
    """The broker is bridged to another one and mirrors every topic. The
    probe already checks 'centrale'; the state channel did not."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    msg = MagicMock(retain=False)
    msg.topic = STATUS_RESP_TOPIC
    msg.payload = json.dumps({
        "message": "200 Status Update",
        "status": dict(panel_status, centrale="0000000000000000000"),
    })
    mqtt_transport._handle_status_message(msg)
    await mqtt_transport._drain_pending()

    assert pushes == []
    assert not mqtt_transport._status_event.is_set()


async def test_a_command_reply_from_another_panel_is_refused(
        hass, mqtt_transport, auth, instant_panel):
    """Same check on the command channel: another panel's '200' must not
    confirm an arm/disarm of ours."""
    instant_panel.command_reply = json.dumps({
        "message": "200 Command OK", "centrale": "0000000000000000000",
    })
    await _start_offline(mqtt_transport, auth)

    with patch.object(mqtt_module, "COMMAND_TIMEOUT", 1):
        result = await mqtt_transport.async_send_command("abc123-area-0", "1")

    assert result.ok is False and result.error == "timeout"


async def test_a_status_without_a_usable_timestamp_is_refused(
        hass, mqtt_transport, auth, panel_status):
    """Explicit decision, documented in the code: no timestamp means no way
    to tell a live push from a replay, and on an alarm panel the safe answer
    is to refuse. Both 'absent' and 'unparsable' take the same path."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    without = dict(panel_status)
    without.pop("datetime")
    for status in (without, dict(panel_status, datetime="not a date")):
        msg = MagicMock(retain=False)
        msg.topic = STATUS_RESP_TOPIC
        msg.payload = json.dumps({"message": "200 Status Update",
                                  "status": status})
        mqtt_transport._handle_status_message(msg)
    await mqtt_transport._drain_pending()

    assert pushes == []
    assert mqtt_transport.diagnostic_info()["snapshots_refused"] == 2
    assert mqtt_transport.diagnostic_info()["last_refusal"]


async def test_a_drifting_panel_clock_is_tolerated(
        hass, mqtt_transport, auth, panel_status):
    """The panel's clock is not synchronised with Home Assistant's. The
    freshness rule has to be loose enough that ordinary drift -- hours, in
    either direction -- never costs a live update, or the guard would take
    the channel down instead of protecting it."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    for drift in (timedelta(hours=-2), timedelta(hours=2)):
        transport = MqttTransport(hass, PANEL_ID)
        transport._auth = auth
        transport._on_push = on_push
        msg = MagicMock(retain=False)
        msg.topic = STATUS_RESP_TOPIC
        msg.payload = json.dumps({
            "message": "200 Status Update",
            "status": dict(panel_status, datetime=panel_stamp(drift)),
        })
        transport._handle_status_message(msg)
        await transport._drain_pending()

    assert len(pushes) == 2, "ordinary clock drift cost us a live update"


async def test_a_status_without_centrale_is_still_accepted(
        hass, mqtt_transport, auth, panel_status):
    """Decision, deliberately asymmetric: MISMATCH is refused, ABSENCE is
    not. The topic is already panel-scoped, coordinator._parse defaults the
    field to the configured panel id, and this integration has never
    measured a panel that omits it -- refusing on absence would be a guess
    about a format we have not seen, with the whole channel as the stake."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    anonymous = dict(panel_status)
    anonymous.pop("centrale")
    msg = MagicMock(retain=False)
    msg.topic = STATUS_RESP_TOPIC
    msg.payload = json.dumps({"message": "200 Status Update",
                              "status": anonymous})
    mqtt_transport._handle_status_message(msg)
    await mqtt_transport._drain_pending()

    assert pushes == [anonymous]


# --- P2: a poll answer must not be reported as a push -------------------


async def test_the_answer_to_our_own_poll_is_not_reported_as_a_push(
        hass, mqtt_transport, auth, instant_panel, panel_status):
    """_handle_status_message forwarded EVERY non-retained status to the
    push callback, including the replies to our own requests. The registry
    binds source='mqtt' to that callback, so a poll wrote
    last_update_source='mqtt' into the diagnostics -- fabricating exactly
    the evidence this work is judged on ("MQTT push works")."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    assert await mqtt_transport.async_fetch_state() == panel_status
    await hass.async_block_till_done()
    await mqtt_transport._drain_pending()

    assert pushes == [], (
        "the answer to our own request was announced as a spontaneous push"
    )


async def test_a_spontaneous_update_is_still_a_push(
        hass, mqtt_transport, auth, panel_status):
    """The other half of the same rule: when nobody asked, it IS a push."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    msg = MagicMock(retain=False)
    msg.topic = STATUS_RESP_TOPIC
    msg.payload = json.dumps({"message": "200 Status Update",
                              "status": panel_status})
    mqtt_transport._handle_status_message(msg)
    await mqtt_transport._drain_pending()

    assert pushes == [panel_status]


# --- P3: the latch must not be satisfied by something that predates the
#         request it is supposed to answer -----------------------------


async def test_an_answer_that_predates_the_request_does_not_satisfy_a_fetch(
        hass, mqtt_transport, auth, instant_panel, panel_status):
    """async_fetch_state cleared _status_event once, at the very top, and
    then awaited it. Between that clear and the publish sits the wait for
    the subscription confirmation -- up to SUBSCRIBE_CONFIRM_WAIT = 3s of
    open window. Anything landing there satisfied a request that had not
    been sent yet. Here the panel never answers the request at all, so the
    only thing that can end the wait is that earlier message.
    """
    await _start_offline(mqtt_transport, auth)

    early = MagicMock(retain=False)
    early.topic = STATUS_RESP_TOPIC
    early.payload = json.dumps({"message": "200 Status Update",
                                "status": panel_status})

    async def _confirm_and_let_a_push_land(subscribed):
        mqtt_transport._handle_status_message(early)
        return True

    instant_panel.status_reply = None  # the panel never answers US
    with patch.object(mqtt_transport, "_async_await_subscription",
                      new=_confirm_and_let_a_push_land), \
         patch.object(mqtt_module, "STATUS_TIMEOUT", 2), \
         patch.object(mqtt_module, "STATUS_REPLY_WAIT", 0.5, create=True):
        result = await mqtt_transport.async_fetch_state()
    await mqtt_transport._drain_pending()

    assert result is None, (
        "a message that predates the request was served as its answer"
    )
    assert mqtt_transport.state is TransportState.DEGRADED


async def test_a_confirmation_that_predates_the_command_does_not_confirm_it(
        hass, mqtt_transport, auth, instant_panel):
    """The same latch on the command path, where it is worse: a '200' that
    lands between the clear and the publish confirms an arm/disarm that has
    not been sent yet -- and the caller is told the panel did it."""
    await _start_offline(mqtt_transport, auth)

    early = MagicMock(retain=False)
    early.topic = COMMAND_RESP_TOPIC
    early.payload = COMMAND_OK_REPLY

    async def _confirm_and_let_a_reply_land(subscribed):
        mqtt_transport._handle_command_message(early)
        return True

    instant_panel.command_reply = None  # the panel never answers US
    with patch.object(mqtt_transport, "_async_await_subscription",
                      new=_confirm_and_let_a_reply_land), \
         patch.object(mqtt_module, "COMMAND_TIMEOUT", 1):
        result = await mqtt_transport.async_send_command("abc123-area-0", "1")

    assert result.ok is False and result.error == "timeout", (
        "a confirmation that predates the command reported it as executed"
    )


# --- P5: two mutants that survived the whole suite ---------------------


async def test_a_command_confirmation_is_not_lost_to_the_subscribe_race(
        hass, qos0_broker, mqtt_transport, auth):
    """Kills the mutant "delete the subscription wait in async_send_command".

    The command goes out exactly once and is never repeated, so the reply
    subscription MUST be on the wire before we publish: with a QoS-0 broker
    a confirmation delivered to a topic nobody is subscribed to is gone for
    good, and the command would be reported as failed after running.
    """
    await mqtt_transport.async_start(auth, AsyncMock())

    result = await mqtt_transport.async_send_command("abc123-uscita-0", "1")

    assert result.ok is True, (
        "the confirmation was published into a blind window and lost"
    )
    assert qos0_broker.command_requests == 1
    assert qos0_broker.dropped == 0
    await mqtt_transport.async_stop()
    await hass.async_block_till_done()


async def test_resubscribing_clears_the_subscription_confirmation(
        hass, qos0_broker, mqtt_transport, auth):
    """Kills the mutant "delete subscribed.clear() in _subscribe_responses".

    After a re-probe the old subscriptions are dropped and new ones go back
    through HA's debouncer. If the confirmation flag survived that, both
    the delivery guarantee and the diagnostics field the README sells as
    "whether our SUBSCRIBE actually reached the broker" would be answering
    about a subscription that no longer exists.
    """
    await mqtt_transport.async_start(auth, AsyncMock())
    await mqtt_transport._status_subscribed.wait()
    assert mqtt_transport.diagnostic_info()["status_subscription_confirmed"]

    with patch.object(mqtt_transport, "_async_watch_subscription",
                      return_value=lambda: None):
        await mqtt_transport._subscribe_responses()

    assert not mqtt_transport.diagnostic_info()[
        "status_subscription_confirmed"], (
        "a fresh subscription inherited the previous one's confirmation"
    )
    await mqtt_transport.async_stop()
    await hass.async_block_till_done()
    await _let_the_mqtt_misc_timer_fire()


# --- P6: a stopped transport must not claim a live subscription --------


async def test_a_stopped_transport_reports_no_confirmed_subscription(
        hass, qos0_broker, mqtt_transport, auth):
    """async_stop dropped the subscriptions but left the confirmation flags
    set, so the diagnostics said 'status_subscription_confirmed: true' next
    to 'subscriptions: 0' -- contradicting itself in the same dictionary."""
    await mqtt_transport.async_start(auth, AsyncMock())
    await mqtt_transport._status_subscribed.wait()

    await mqtt_transport.async_stop()
    await hass.async_block_till_done()

    info = mqtt_transport.diagnostic_info()
    assert info["subscriptions"] == 0
    assert info["status_subscription_confirmed"] is False


async def test_a_second_poll_does_not_reuse_the_first_answer_latch(
        hass, mqtt_transport, auth, instant_panel, panel_status):
    """Kills the mutant "clear _status_event once, at the top, not per
    request". The latch stays set after a successful poll; the next poll
    would then find it already set, break before its own answer arrived and
    read an empty response -- reporting DEGRADED on a panel that answered
    perfectly well, twice."""
    await _start_offline(mqtt_transport, auth)

    assert await mqtt_transport.async_fetch_state() == panel_status
    assert await mqtt_transport.async_fetch_state() == panel_status
    assert mqtt_transport.state is TransportState.READY
    assert len(instant_panel.status_requests) == 2

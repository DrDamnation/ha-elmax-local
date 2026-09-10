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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant.exceptions import HomeAssistantError
from pytest_homeassistant_custom_component.common import async_fire_mqtt_message

from custom_components.elmax_local.auth import AuthManager
from custom_components.elmax_local.const import (
    TOPIC_REQUEST_ID, TOPIC_RESPONSE_ID, TOPIC_RESPONSE_STATUS,
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
    """Records the wire and answers /elmax/request/id like the panel does."""

    def __init__(self):
        self.wire: list[tuple[str, str]] = []
        self.on_wire: set[str] = set()
        self.reply: str | None = ID_REPLY
        self.requests = 0
        self.dropped = 0


@pytest.fixture
def qos0_broker(hass, mqtt_mock, mqtt_client_mock):
    """QoS-0 broker semantics on top of the mocked paho client."""
    broker = _Broker()
    orig_sub = mqtt_client_mock.subscribe.side_effect
    orig_pub = mqtt_client_mock.publish.side_effect

    def _subscribe(topic, qos=0, *args, **kwargs):
        entries = topic if isinstance(topic, list) else [(topic, qos)]
        for item in entries:
            name = item[0] if isinstance(item, (list, tuple)) else item
            broker.on_wire.add(name)
            broker.wire.append(("SUBSCRIBE", name))
        return orig_sub(topic, qos)

    def _publish(topic, payload=None, qos=0, retain=False, *args, **kwargs):
        broker.wire.append(("PUBLISH", topic))
        if topic == TOPIC_REQUEST_ID:
            broker.requests += 1
            if broker.reply is None:
                pass
            elif TOPIC_RESPONSE_ID in broker.on_wire:
                # Subscriber present at this instant -> mosquitto delivers.
                hass.loop.call_soon(
                    async_fire_mqtt_message, hass,
                    TOPIC_RESPONSE_ID, broker.reply,
                )
            else:
                # No subscriber: a non-retained QoS-0 message is discarded
                # and is NOT recoverable by a later subscription.
                broker.dropped += 1
        return orig_pub(topic, payload, qos, retain)

    mqtt_client_mock.subscribe.side_effect = _subscribe
    mqtt_client_mock.publish.side_effect = _publish
    return broker


def test_capabilities(mqtt_transport):
    assert TransportCapability.PUSH in mqtt_transport.capabilities
    assert TransportCapability.POLL in mqtt_transport.capabilities
    assert TransportCapability.COMMAND in mqtt_transport.capabilities


async def test_distinguishes_status_update_from_response(mqtt_transport, auth,
                                                          mock_panel_data):
    """Push spontaneo: 'message' = '200 Status Update'.
    Response a request: 'message' = '200 Status OK'.
    Entrambi devono triggerare on_state_update."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    # Simula push spontaneo (live: retain=0)
    msg_update = MagicMock(retain=False)
    msg_update.payload = json.dumps({"message": "200 Status Update",
                                     "status": mock_panel_data})
    mqtt_transport._handle_status_message(msg_update)

    # Simula response a request (live: retain=0)
    msg_response = MagicMock(retain=False)
    msg_response.payload = json.dumps({"message": "200 Status OK",
                                       "status": mock_panel_data})
    mqtt_transport._handle_status_message(msg_response)

    # Entrambi devono triggerare on_state_update con payload status
    await mqtt_transport._drain_pending()
    assert len(pushes) == 2
    assert all(p == mock_panel_data for p in pushes)


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
        hass, qos0_broker, mqtt_transport, auth, mock_panel_data):
    """End to end: the May snapshot on the live broker must be dropped,
    while the next live (non-retained) update still gets through."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    await mqtt_transport.async_start(auth, on_push)
    topic = TOPIC_RESPONSE_STATUS.format(panel_id=PANEL_ID)

    stale = dict(mock_panel_data, datetime="22:20:20 15/05/2026")
    async_fire_mqtt_message(
        hass, topic,
        json.dumps({"message": "200 Status Update", "status": stale}),
        0, True,
    )
    await hass.async_block_till_done()
    await mqtt_transport._drain_pending()
    assert pushes == [], "retained snapshot was ingested as live state"

    async_fire_mqtt_message(
        hass, topic,
        json.dumps({"message": "200 Status Update", "status": mock_panel_data}),
        0, False,
    )
    await hass.async_block_till_done()
    await mqtt_transport._drain_pending()
    assert pushes == [mock_panel_data]

    await mqtt_transport.async_stop()
    await hass.async_block_till_done()
    # With paho mocked, MQTT's misc timer fires once and then stops
    # (loop_misc returns a Mock, never MQTT_ERR_SUCCESS). This test is fast
    # enough to end while it is still armed, which HA's cleanup check treats
    # as a leak; let it fire. Nothing about the transport depends on this.
    await asyncio.sleep(1.05)


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

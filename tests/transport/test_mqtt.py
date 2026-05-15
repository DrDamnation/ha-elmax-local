"""Test MqttTransport (skeleton — full mqtt mock setup in HA fixtures)."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.elmax_local.auth import AuthManager
from custom_components.elmax_local.transport import (
    TransportCapability, TransportState,
)
from custom_components.elmax_local.transport.mqtt import MqttTransport


@pytest.fixture
async def auth(hass):
    am = AuthManager(hass, "1.2.3.4", "000000")
    am._token = "JWT_TOKEN"
    am._expiry = 9999999999
    yield am
    await am.async_close()


@pytest.fixture
def mqtt_transport(hass, auth):
    return MqttTransport(hass, "abc123")


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

    # Simula push spontaneo
    msg_update = MagicMock()
    msg_update.payload = json.dumps({"message": "200 Status Update",
                                     "status": mock_panel_data})
    mqtt_transport._handle_status_message(msg_update)

    # Simula response a request
    msg_response = MagicMock()
    msg_response.payload = json.dumps({"message": "200 Status OK",
                                       "status": mock_panel_data})
    mqtt_transport._handle_status_message(msg_response)

    # Entrambi devono triggerare on_state_update con payload status
    await mqtt_transport._drain_pending()
    assert len(pushes) == 2
    assert all(p == mock_panel_data for p in pushes)

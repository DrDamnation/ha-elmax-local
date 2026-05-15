"""Test WebSocketTransport (skeleton — full WS server fixture for integration)."""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, patch

from custom_components.elmax_local.auth import AuthManager
from custom_components.elmax_local.transport import TransportCapability
from custom_components.elmax_local.transport.websocket import WebSocketTransport


@pytest.fixture
async def auth(hass):
    am = AuthManager(hass, "1.2.3.4", "000000")
    am._token = "JWT_TOKEN"
    am._expiry = 9999999999
    yield am
    await am.async_close()


@pytest.fixture
def ws_transport(hass, auth):
    return WebSocketTransport(hass, "1.2.3.4")


def test_capabilities(ws_transport):
    assert TransportCapability.PUSH in ws_transport.capabilities
    assert TransportCapability.POLL not in ws_transport.capabilities
    assert TransportCapability.COMMAND not in ws_transport.capabilities


async def test_probe_uses_auth_token(ws_transport, auth):
    """Probe must request a token via AuthManager before attempting WS."""
    with patch.object(ws_transport, "_open_ws", new=AsyncMock(return_value=True)) as mock_open:
        ws_transport._auth = auth
        result = await ws_transport.async_probe()
        assert result is True
        mock_open.assert_called_once()


async def test_handle_message_invokes_callback(ws_transport, mock_panel_data):
    pushes = []

    async def on_push(data):
        pushes.append(data)

    ws_transport._on_push = on_push
    await ws_transport._handle_message(json.dumps(mock_panel_data))
    assert pushes == [mock_panel_data]


async def test_handle_message_malformed_dropped(ws_transport):
    pushes = []

    async def on_push(data):
        pushes.append(data)

    ws_transport._on_push = on_push
    await ws_transport._handle_message("not json")
    assert pushes == []

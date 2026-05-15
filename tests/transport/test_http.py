"""Test HttpTransport."""
from __future__ import annotations

import pytest
from aioresponses import aioresponses

from custom_components.elmax_local.auth import AuthManager
from custom_components.elmax_local.transport import (
    TransportCapability, TransportState,
)
from custom_components.elmax_local.transport.http import HttpTransport


@pytest.fixture
async def auth(hass):
    am = AuthManager(hass, "1.2.3.4", "000000")
    yield am
    await am.async_close()


@pytest.fixture
def http_transport(hass, auth):
    return HttpTransport(hass, "1.2.3.4")


def test_capabilities(http_transport):
    assert TransportCapability.POLL in http_transport.capabilities
    assert TransportCapability.COMMAND in http_transport.capabilities
    assert TransportCapability.PUSH not in http_transport.capabilities


async def test_probe_ok(http_transport, auth, mock_panel_data):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login",
               payload={"token": "JWT eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"})
        await http_transport.async_start(auth, lambda d: None)
        assert await http_transport.async_probe() is True


async def test_probe_401(http_transport, auth):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", status=401,
               payload={"message": "Forbidden"})
        await http_transport.async_start(auth, lambda d: None)
        assert await http_transport.async_probe() is False


async def test_fetch_state(http_transport, auth, mock_panel_data):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.get("https://1.2.3.4/api/v2/discovery", payload=mock_panel_data)
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_fetch_state()
        assert result == mock_panel_data
        assert http_transport.state == TransportState.READY


async def test_send_command_ok(http_transport, auth):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.post("https://1.2.3.4/api/v2/cmd/abc-area-0/4",
               payload={"message": "Command Sent"})
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("abc-area-0", "4")
        assert result.ok is True


async def test_send_command_disarm_with_code(http_transport, auth):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.post("https://1.2.3.4/api/v2/cmd/abc-area-0/0",
               payload={"message": "Command Sent"})
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("abc-area-0", "0", code="000000")
        assert result.ok is True


async def test_send_command_422_retries(http_transport, auth):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        # 422 twice, then 200
        m.post("https://1.2.3.4/api/v2/cmd/eid/1", status=422,
               payload={"message": "Busy"})
        m.post("https://1.2.3.4/api/v2/cmd/eid/1", status=422,
               payload={"message": "Busy"})
        m.post("https://1.2.3.4/api/v2/cmd/eid/1",
               payload={"message": "Command Sent"})
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("eid", "1")
        assert result.ok is True


async def test_send_command_503_fails(http_transport, auth):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.post("https://1.2.3.4/api/v2/cmd/eid/1", status=503,
               payload={"message": "Service Unavailable"})
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("eid", "1")
        assert result.ok is False
        assert "503" in (result.error or "")

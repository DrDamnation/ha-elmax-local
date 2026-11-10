"""Test HttpTransport."""
from __future__ import annotations

import aiohttp
import pytest
from aioresponses import aioresponses
from unittest.mock import MagicMock

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


# --- R3: "safe_to_retry" must mean the panel provably did not act -------
#
# aiohttp.ServerDisconnectedError is a SUBCLASS of ClientConnectionError
# (verified on both the test venv, aiohttp 3.11.9, and the live container,
# aiohttp 3.14.3), so the branch meant for "we never got a connection" was
# also catching "we had a connection and lost it mid-request". On an
# arm/disarm that is the double send this flag exists to prevent.


async def test_a_server_disconnect_mid_command_is_not_safe_to_retry(
        http_transport, auth):
    """The connection was established and the request may already have
    reached the panel: it may have armed the house. Handing that command to
    another transport would arm it twice."""
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.post("https://1.2.3.4/api/v2/cmd/eid/4",
               exception=aiohttp.ServerDisconnectedError())
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("eid", "4")

    assert result.ok is False
    assert result.safe_to_retry is False, (
        "a connection lost mid-request was reported as 'the panel never "
        "got it'"
    )


async def test_a_refused_connection_is_still_safe_to_retry(
        http_transport, auth):
    """The other half of the same rule: a connection that was never
    established proves the panel did nothing, and the command must be free
    to go out on another transport."""
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.post("https://1.2.3.4/api/v2/cmd/eid/4",
               exception=aiohttp.ClientConnectorError(
                   MagicMock(ssl=None, host="1.2.3.4", port=443),
                   OSError(111, "Connection refused")))
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("eid", "4")

    assert result.ok is False
    assert result.safe_to_retry is True

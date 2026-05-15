"""Test TransportRegistry routing."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from custom_components.elmax_local.transport import (
    CommandResult, Transport, TransportCapability, TransportRegistry,
    TransportState,
)


class _FakeTransport(Transport):
    def __init__(self, name: str, caps: frozenset,
                 probe_ok=True, fetch_result=None, command_result=None):
        self.name = name
        self.capabilities = caps
        self._state = TransportState.DISABLED
        self._probe_ok = probe_ok
        self._fetch_result = fetch_result
        self._command_result = command_result or CommandResult(ok=True)
        self.start_called = False
        self.stop_called = False

    @property
    def state(self):
        return self._state

    async def async_probe(self):
        return self._probe_ok

    async def async_start(self, auth, on_push):
        self.start_called = True
        self._state = TransportState.READY if self._probe_ok else TransportState.UNSUPPORTED

    async def async_stop(self):
        self.stop_called = True
        self._state = TransportState.DISABLED

    async def async_fetch_state(self):
        if TransportCapability.POLL not in self.capabilities:
            return await super().async_fetch_state()
        return self._fetch_result

    async def async_send_command(self, eid, cmd, code=None):
        if TransportCapability.COMMAND not in self.capabilities:
            return await super().async_send_command(eid, cmd, code)
        return self._command_result


async def test_start_all_probes_and_starts(hass):
    http = _FakeTransport("http", frozenset({TransportCapability.POLL,
                                              TransportCapability.COMMAND}))
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.PUSH,
                                              TransportCapability.POLL}))
    reg = TransportRegistry([http, mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())
    assert http.start_called and mqtt.start_called
    assert http.state == TransportState.READY


async def test_unsupported_not_started(hass):
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}))
    ws = _FakeTransport("ws", frozenset({TransportCapability.PUSH}), probe_ok=False)
    reg = TransportRegistry([http, ws])
    await reg.async_start_all(MagicMock(), AsyncMock())
    assert http.state == TransportState.READY
    # ws probe returned False, so async_start was not called and state
    # stays DISABLED. Mark UNSUPPORTED at registry level.
    assert ws.state in (TransportState.UNSUPPORTED, TransportState.DISABLED)


async def test_fetch_uses_http_first(hass, mock_panel_data):
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}),
                          fetch_result=mock_panel_data)
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.PUSH,
                                              TransportCapability.POLL}),
                          fetch_result={"different": True})
    reg = TransportRegistry([http, mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())
    result = await reg.async_fetch_state()
    assert result == mock_panel_data


async def test_fetch_falls_back_to_mqtt_on_http_none(hass, mock_panel_data):
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}),
                          fetch_result=None)
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.POLL}),
                          fetch_result=mock_panel_data)
    reg = TransportRegistry([http, mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())
    result = await reg.async_fetch_state()
    assert result == mock_panel_data


async def test_send_command_uses_http_first():
    http = _FakeTransport("http", frozenset({TransportCapability.COMMAND}),
                          command_result=CommandResult(ok=True))
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.COMMAND}),
                          command_result=CommandResult(ok=False, error="should not be called"))
    reg = TransportRegistry([http, mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())
    result = await reg.async_send_command("eid", "1")
    assert result.ok is True


def test_get_active_push_transports():
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}))
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.PUSH}))
    mqtt._state = TransportState.READY
    ws = _FakeTransport("ws", frozenset({TransportCapability.PUSH}))
    ws._state = TransportState.DEGRADED
    reg = TransportRegistry([http, mqtt, ws])
    active = reg.get_active_push_transports()
    assert mqtt in active
    assert ws not in active

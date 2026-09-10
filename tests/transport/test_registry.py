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
        self.probe_count = 0

    @property
    def state(self):
        return self._state

    async def async_probe(self):
        self.probe_count += 1
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


async def test_unsupported_not_started(hass, caplog):
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}))
    ws = _FakeTransport("ws", frozenset({TransportCapability.PUSH}), probe_ok=False)
    reg = TransportRegistry([http, ws])
    await reg.async_start_all(MagicMock(), AsyncMock())
    assert http.state == TransportState.READY
    # ws probe returned False, so async_start was not called and the state
    # stays DISABLED -- which is the truth, and the state the periodic
    # re-probe looks for. UNSUPPORTED means "never retry" and nothing may
    # claim it here, in the log message least of all.
    assert ws.state is TransportState.DISABLED
    assert "UNSUPPORTED" not in caplog.text


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


# --- Recovery from DEGRADED (regression: degraded was terminal) ---

async def test_fetch_retries_degraded_transport(hass, mock_panel_data):
    """A DEGRADED transport must still be called: that call is what heals it."""
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}),
                          fetch_result=mock_panel_data)
    http._state = TransportState.DEGRADED
    reg = TransportRegistry([http])

    assert await reg.async_fetch_state() == mock_panel_data


async def test_fetch_prefers_ready_over_degraded(hass, mock_panel_data):
    """Healthy transports are attempted first, degraded ones only as fallback."""
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}),
                          fetch_result={"stale": True})
    http._state = TransportState.DEGRADED
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.POLL}),
                          fetch_result=mock_panel_data)
    mqtt._state = TransportState.READY
    reg = TransportRegistry([http, mqtt])

    # http has higher raw priority but is degraded, so mqtt answers.
    assert await reg.async_fetch_state() == mock_panel_data


async def test_fetch_skips_disabled_and_unsupported(hass):
    """Disabled/unsupported transports stay out of rotation."""
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}),
                          fetch_result={"never": True})
    http._state = TransportState.DISABLED
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.POLL}),
                          fetch_result={"never": True})
    mqtt._state = TransportState.UNSUPPORTED
    reg = TransportRegistry([http, mqtt])

    assert await reg.async_fetch_state() is None


async def test_command_retries_degraded_transport():
    """Same recovery path for commands, otherwise the panel stays uncontrollable."""
    http = _FakeTransport("http", frozenset({TransportCapability.COMMAND}),
                          command_result=CommandResult(ok=True))
    http._state = TransportState.DEGRADED
    reg = TransportRegistry([http])

    assert (await reg.async_send_command("abc-area-0", "1")).ok


async def test_fetch_survives_transport_exception(hass, mock_panel_data):
    """One transport raising must not abort the whole fallback chain."""
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}))
    http._state = TransportState.READY
    http.async_fetch_state = AsyncMock(side_effect=OSError("boom"))
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.POLL}),
                          fetch_result=mock_panel_data)
    mqtt._state = TransportState.READY
    reg = TransportRegistry([http, mqtt])

    assert await reg.async_fetch_state() == mock_panel_data


# --- Recovery from a failed probe (regression: a failed probe was final) ---

async def test_reprobe_recovers_a_transport_that_failed_at_setup(hass):
    """A transport that lost its probe at boot must get back in the game.

    The probe has a single call site (coordinator.async_setup), so before
    this a transport whose broker/panel was not ready at HA start stayed out
    until the entry was reloaded -- permanently, in practice.
    """
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}))
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.PUSH,
                                             TransportCapability.POLL}),
                          probe_ok=False)
    reg = TransportRegistry([http, mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())
    assert mqtt.state is TransportState.DISABLED
    assert not mqtt.start_called

    mqtt._probe_ok = True  # broker finally up / panel reachable
    recovered = await reg.async_reprobe_inactive(MagicMock(), AsyncMock())

    assert recovered == ["mqtt"]
    assert mqtt.start_called
    assert mqtt.state is TransportState.READY


async def test_reprobe_leaves_working_transports_alone(hass):
    """Never re-probe what is already carrying traffic."""
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}))
    degraded = _FakeTransport("mqtt", frozenset({TransportCapability.POLL}))
    reg = TransportRegistry([http, degraded])
    await reg.async_start_all(MagicMock(), AsyncMock())
    degraded._state = TransportState.DEGRADED
    probes_before = (http.probe_count, degraded.probe_count)

    assert await reg.async_reprobe_inactive(MagicMock(), AsyncMock()) == []
    assert (http.probe_count, degraded.probe_count) == probes_before


async def test_reprobe_keeps_a_still_broken_transport_disabled(hass):
    """A transport that fails again stays out; it must not go DEGRADED."""
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.POLL}),
                          probe_ok=False)
    reg = TransportRegistry([mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())

    assert await reg.async_reprobe_inactive(MagicMock(), AsyncMock()) == []
    assert mqtt.state is TransportState.DISABLED
    assert not mqtt.start_called

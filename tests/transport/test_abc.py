"""Test Transport ABC contract."""
from __future__ import annotations

import pytest

from custom_components.elmax_local.transport import (
    CommandResult, Transport, TransportCapability, TransportState,
)


def test_capability_enum_values():
    assert TransportCapability.PUSH.value == "push"
    assert TransportCapability.POLL.value == "poll"
    assert TransportCapability.COMMAND.value == "command"


def test_state_enum_values():
    assert {s.value for s in TransportState} == {
        "disabled", "probing", "ready", "degraded", "unsupported"
    }


def test_command_result_frozen():
    r = CommandResult(ok=True)
    with pytest.raises(Exception):
        r.ok = False  # type: ignore


def test_command_result_defaults():
    r = CommandResult(ok=True)
    assert r.error is None and r.raw_response is None


def test_transport_abc_not_instantiable():
    with pytest.raises(TypeError):
        Transport()  # type: ignore


class _MinimalTransport(Transport):
    name = "minimal"
    capabilities = frozenset()

    @property
    def state(self):
        return TransportState.DISABLED

    async def async_probe(self) -> bool:
        return False

    async def async_start(self, auth, on_state_update) -> None:
        return None

    async def async_stop(self) -> None:
        return None


def test_minimal_subclass_instantiable():
    t = _MinimalTransport()
    assert t.name == "minimal"


async def test_fetch_state_default_raises():
    t = _MinimalTransport()
    with pytest.raises(NotImplementedError):
        await t.async_fetch_state()


async def test_send_command_default_raises():
    t = _MinimalTransport()
    with pytest.raises(NotImplementedError):
        await t.async_send_command("eid", "0")

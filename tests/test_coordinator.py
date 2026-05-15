"""Test ElmaxLocalCoordinator."""
from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from custom_components.elmax_local.coordinator import (
    ElmaxLocalCoordinator, ElmaxState,
)
from custom_components.elmax_local.transport import CommandResult


def test_state_dataclass_defaults():
    state = ElmaxState(
        panel_info={}, zones={}, areas={}, outputs={}, scenarios={},
        last_update_source="", last_update_ts=0,
    )
    assert state.zones == {}


def test_coordinator_init(hass):
    coord = ElmaxLocalCoordinator(
        hass, panel_id="abc", pin="000000", host="1.2.3.4",
        reconcile_interval=90, enable_ws=True, enable_mqtt=True,
    )
    assert coord.panel_id == "abc"
    assert coord.update_interval == timedelta(seconds=90)
    assert coord.auth is not None
    assert coord.registry is not None


async def test_parse_indexes_by_endpoint_id(hass, mock_panel_data):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    state = coord._parse(mock_panel_data)
    assert "abc123-zona-0" in state.zones
    assert "abc123-area-0" in state.areas
    assert state.panel_info["release"] == "PHANTOM64PRO_GSM 13.9A.845"


async def test_on_push_update_sets_data(hass, mock_panel_data):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    await coord._on_push_state_update(mock_panel_data)
    assert coord.data is not None
    assert "abc123-zona-0" in coord.data.zones
    assert coord.data.last_update_source != ""


async def test_update_data_skips_when_push_fresh(hass, mock_panel_data):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4",
                                  reconcile_interval=60)
    await coord._on_push_state_update(mock_panel_data)
    with patch.object(coord.registry, "async_fetch_state",
                      new=AsyncMock(return_value={"different": True})) as mock_fetch:
        result = await coord._async_update_data()
        mock_fetch.assert_not_called()
        assert result is coord.data


async def test_send_command_schedules_verify(hass):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    with patch.object(coord.registry, "async_send_command",
                      new=AsyncMock(return_value=CommandResult(ok=True))):
        with patch.object(coord, "_post_command_verify",
                          new=AsyncMock()) as mock_verify:
            ok = await coord.async_send_command("eid", "4")
            assert ok is True
            await asyncio.sleep(0)
            mock_verify.assert_called()


async def test_send_command_fail_no_verify(hass):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    with patch.object(coord.registry, "async_send_command",
                      new=AsyncMock(return_value=CommandResult(ok=False))):
        with patch.object(coord, "_post_command_verify",
                          new=AsyncMock()) as mock_verify:
            ok = await coord.async_send_command("eid", "4")
            assert ok is False
            await asyncio.sleep(0)
            mock_verify.assert_not_called()

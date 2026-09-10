"""Test ElmaxLocalCoordinator."""
from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.helpers.update_coordinator import UpdateFailed

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


# --- Staleness escalation (regression: stale data was served forever) ---

async def _stale_coord(hass, mock_panel_data, stale_seconds: float):
    """Coordinator holding data that is `stale_seconds` old, poll failing."""
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4",
                                  reconcile_interval=60)
    await coord._on_push_state_update(mock_panel_data)
    coord.data.last_update_ts = time.time() - stale_seconds
    coord.registry.async_fetch_state = AsyncMock(return_value=None)
    return coord


async def test_serves_cached_state_while_briefly_stale(hass, mock_panel_data):
    """A short outage keeps the last known state: no needless flapping."""
    coord = await _stale_coord(hass, mock_panel_data, stale_seconds=90)
    assert await coord._async_update_data() is coord.data


async def test_raises_when_stale_beyond_threshold(hass, mock_panel_data):
    """Past the threshold the state is too old to serve: entities go unavailable."""
    coord = await _stale_coord(hass, mock_panel_data, stale_seconds=60 * 6)
    with pytest.raises(UpdateFailed):
        await coord._async_update_data()


# --- Periodic transport re-probe (regression: probe ran only at setup) ---

async def test_update_data_reprobes_inactive_transports(hass, mock_panel_data):
    """The reconcile tick is what gives a lost transport a second chance."""
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    with patch.object(coord.registry, "async_fetch_state",
                      new=AsyncMock(return_value=mock_panel_data)), \
         patch.object(coord.registry, "async_reprobe_inactive",
                      new=AsyncMock(return_value=[])) as reprobe:
        for _ in range(4):
            await coord._async_update_data()
            await hass.async_block_till_done()
    assert reprobe.await_count >= 1


async def test_reprobe_is_rate_limited(hass, mock_panel_data):
    """Backoff: a mute panel must not be hammered every tick."""
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    with patch.object(coord.registry, "async_fetch_state",
                      new=AsyncMock(return_value=mock_panel_data)), \
         patch.object(coord.registry, "async_reprobe_inactive",
                      new=AsyncMock(return_value=[])) as reprobe:
        for _ in range(8):
            await coord._async_update_data()
            await hass.async_block_till_done()
    # ticks 2, 4 and 8 only -- never the setup tick, never every tick
    assert reprobe.await_count == 3


async def test_reprobe_runs_even_when_push_is_fresh(hass, mock_panel_data):
    """A healthy websocket must not keep mqtt excluded for ever."""
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4",
                                  reconcile_interval=60)
    await coord._on_push_state_update(mock_panel_data)
    with patch.object(coord.registry, "async_reprobe_inactive",
                      new=AsyncMock(return_value=[])) as reprobe:
        for _ in range(4):
            await coord._async_update_data()
            await hass.async_block_till_done()
    assert reprobe.await_count >= 1


async def test_reprobe_failure_does_not_break_the_poll(hass, mock_panel_data):
    """A raising re-probe must never take the coordinator down with it."""
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    with patch.object(coord.registry, "async_fetch_state",
                      new=AsyncMock(return_value=mock_panel_data)), \
         patch.object(coord.registry, "async_reprobe_inactive",
                      new=AsyncMock(side_effect=OSError("boom"))):
        for _ in range(3):
            state = await coord._async_update_data()
            await hass.async_block_till_done()
    assert state.zones

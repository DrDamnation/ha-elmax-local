"""Test Elmax Local entities."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock

from custom_components.elmax_local.alarm_control_panel import ElmaxAlarmPanel
from custom_components.elmax_local.coordinator import ElmaxLocalCoordinator


@pytest.fixture
def coord_with_data(hass, mock_panel_data):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    coord.data = coord._parse(mock_panel_data)
    coord.async_send_command = AsyncMock(return_value=True)
    return coord


def test_alarm_panel_unique_id(coord_with_data):
    panel = ElmaxAlarmPanel(coord_with_data, "abc123-area-0")
    assert panel.unique_id == "elmax_local_abc123-area-0"


def test_alarm_panel_state_disarmed(coord_with_data):
    panel = ElmaxAlarmPanel(coord_with_data, "abc123-area-0")
    assert panel.alarm_state == "disarmed"


async def test_alarm_panel_arm_away(coord_with_data):
    panel = ElmaxAlarmPanel(coord_with_data, "abc123-area-0")
    await panel.async_alarm_arm_away()
    coord_with_data.async_send_command.assert_called_with("abc123-area-0", "4")


async def test_alarm_panel_disarm_uses_pin(coord_with_data):
    panel = ElmaxAlarmPanel(coord_with_data, "abc123-area-0")
    coord_with_data.auth._pin = "000000"
    await panel.async_alarm_disarm()
    coord_with_data.async_send_command.assert_called_with(
        "abc123-area-0", "0", code="000000"
    )

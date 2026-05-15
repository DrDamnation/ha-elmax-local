"""Test ElmaxLocalCoordinator."""
from __future__ import annotations

from datetime import timedelta

from custom_components.elmax_local.coordinator import (
    ElmaxLocalCoordinator, ElmaxState,
)


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

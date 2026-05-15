"""Test __init__.py setup."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.elmax_local import async_setup, async_setup_entry
from custom_components.elmax_local.const import (
    CONF_PANEL_HOST, CONF_PANEL_ID, CONF_PANEL_PIN, DOMAIN,
    SERVICE_MIGRATE, SERVICE_ROLLBACK,
)


@pytest.fixture
def entry():
    return MockConfigEntry(
        domain=DOMAIN,
        data={CONF_PANEL_ID: "abc", CONF_PANEL_PIN: "000000",
              CONF_PANEL_HOST: "1.2.3.4"},
        options={"reconcile_interval": 90, "enable_ws": True, "enable_mqtt": True},
        entry_id="test_entry",
    )


async def test_setup_entry_success(hass, entry):
    entry.add_to_hass(hass)
    with patch("custom_components.elmax_local.ElmaxLocalCoordinator") as MockCoord:
        instance = MockCoord.return_value
        instance.async_setup = AsyncMock()
        instance.async_shutdown = AsyncMock()
        instance.data = MagicMock(zones={}, areas={"a1": {}}, outputs={}, scenarios={})
        with patch.object(hass.config_entries, "async_forward_entry_setups",
                          new=AsyncMock()):
            result = await async_setup_entry(hass, entry)
        assert result is True
        assert hass.data[DOMAIN]["test_entry"] is instance


async def test_services_registered(hass):
    await async_setup(hass, {})
    assert hass.services.has_service(DOMAIN, SERVICE_MIGRATE)
    assert hass.services.has_service(DOMAIN, SERVICE_ROLLBACK)

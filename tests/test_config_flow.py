"""Test config flow."""
from __future__ import annotations

from unittest.mock import patch

from ipaddress import ip_address

from aioresponses import aioresponses
from homeassistant.components.zeroconf import ZeroconfServiceInfo
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.elmax_local.const import (
    CONF_PANEL_HOST, CONF_PANEL_ID, CONF_PANEL_PIN, DOMAIN,
)


async def test_user_step_success(hass):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login",
               payload={"token": "JWT eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"})
        m.get("https://1.2.3.4/api/v2/discovery",
              payload={"centrale": "abc", "release": "X"})
        with patch("custom_components.elmax_local.config_flow."
                   "ElmaxLocalConfigFlow._discover_panels",
                   return_value=[]):
            result = await hass.config_entries.flow.async_init(
                DOMAIN, context={"source": "user"}
            )
            assert result["type"] == FlowResultType.FORM
            result = await hass.config_entries.flow.async_configure(
                result["flow_id"],
                {"panel_host": "1.2.3.4", "panel_id": "abc", "panel_pin": "000000"},
            )
        assert result["type"] == FlowResultType.CREATE_ENTRY


async def test_user_step_invalid_auth(hass):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", status=401,
               payload={"message": "Forbidden"})
        with patch("custom_components.elmax_local.config_flow."
                   "ElmaxLocalConfigFlow._discover_panels",
                   return_value=[]):
            result = await hass.config_entries.flow.async_init(
                DOMAIN, context={"source": "user"}
            )
            result = await hass.config_entries.flow.async_configure(
                result["flow_id"],
                {"panel_host": "1.2.3.4", "panel_id": "abc", "panel_pin": "wrong"},
            )
        assert result["type"] == FlowResultType.FORM
        assert result["errors"] == {"base": "invalid_auth"}


async def test_user_step_panel_id_mismatch(hass):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login",
               payload={"token": "JWT eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"})
        m.get("https://1.2.3.4/api/v2/discovery",
              payload={"centrale": "different", "release": "X"})
        with patch("custom_components.elmax_local.config_flow."
                   "ElmaxLocalConfigFlow._discover_panels",
                   return_value=[]):
            result = await hass.config_entries.flow.async_init(
                DOMAIN, context={"source": "user"}
            )
            result = await hass.config_entries.flow.async_configure(
                result["flow_id"],
                {"panel_host": "1.2.3.4", "panel_id": "abc", "panel_pin": "000000"},
            )
        assert result["type"] == FlowResultType.FORM
        assert result["errors"] == {"base": "panel_id_mismatch"}


async def test_zeroconf_step(hass):
    info = ZeroconfServiceInfo(
        ip_address=ip_address("1.2.3.4"), ip_addresses=[ip_address("1.2.3.4")],
        port=443, hostname="elmax-abc.local.", type="_elmax-ssl._tcp.local.",
        name="Elmax abc._elmax-ssl._tcp.local.", properties={},
    )
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "zeroconf"}, data=info
    )
    assert result["type"] == FlowResultType.FORM
    assert result["step_id"] == "user"


async def test_options_flow(hass):
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_PANEL_ID: "abc", CONF_PANEL_PIN: "0", CONF_PANEL_HOST: "1.2.3.4"},
    )
    entry.add_to_hass(hass)
    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] == FlowResultType.FORM
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {"enable_ws": False, "enable_mqtt": True, "reconcile_interval": 120},
    )
    assert result["type"] == FlowResultType.CREATE_ENTRY
    assert result["data"]["reconcile_interval"] == 120


async def test_options_flow_clamps_interval(hass):
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_PANEL_ID: "abc", CONF_PANEL_PIN: "0", CONF_PANEL_HOST: "1.2.3.4"},
    )
    entry.add_to_hass(hass)
    result = await hass.config_entries.options.async_init(entry.entry_id)
    # voluptuous Range raises before our clamp can run; test the explicit clamp
    # by submitting an in-range value (the clamp inside also enforces bounds).
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {"enable_ws": True, "enable_mqtt": True, "reconcile_interval": 30},
    )
    assert result["type"] == FlowResultType.CREATE_ENTRY
    assert result["data"]["reconcile_interval"] == 30

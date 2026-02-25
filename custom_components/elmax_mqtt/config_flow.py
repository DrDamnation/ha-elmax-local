"""Config flow for Elmax MQTT integration."""

import asyncio
import json
import logging

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components import mqtt
from homeassistant.core import callback

from .const import DOMAIN, CONF_PANEL_ID, CONF_PANEL_PIN, CONF_PANEL_HOST

_LOGGER = logging.getLogger(__name__)


class ElmaxMqttConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Elmax MQTT."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        """Handle the initial step."""
        errors = {}

        if user_input is not None:
            panel_id = user_input[CONF_PANEL_ID].strip()
            pin = user_input[CONF_PANEL_PIN].strip()
            host = user_input[CONF_PANEL_HOST].strip()

            login_ok = await self._test_login(panel_id, pin)

            if login_ok:
                await self.async_set_unique_id(panel_id)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"Elmax {panel_id[-6:]}",
                    data={
                        CONF_PANEL_ID: panel_id,
                        CONF_PANEL_PIN: pin,
                        CONF_PANEL_HOST: host,
                    },
                )
            else:
                errors["base"] = "cannot_connect"

        # Try auto-discovery
        panels = await self._discover_panels()

        if panels:
            schema = vol.Schema(
                {
                    vol.Required(CONF_PANEL_ID): vol.In({p: p for p in panels}),
                    vol.Required(CONF_PANEL_PIN): str,
                    vol.Required(CONF_PANEL_HOST): str,
                }
            )
        else:
            schema = vol.Schema(
                {
                    vol.Required(CONF_PANEL_ID): str,
                    vol.Required(CONF_PANEL_PIN): str,
                    vol.Required(CONF_PANEL_HOST): str,
                }
            )

        return self.async_show_form(
            step_id="user",
            data_schema=schema,
            errors=errors,
        )

    async def _discover_panels(self) -> list[str]:
        """Discover Elmax panels via MQTT broadcast."""
        panels = []
        event = asyncio.Event()

        @callback
        def _handle_id(msg):
            try:
                data = json.loads(msg.payload)
                if "centrale" in data:
                    panels.append(data["centrale"])
                    event.set()
            except (json.JSONDecodeError, KeyError):
                pass

        unsub = await mqtt.async_subscribe(self.hass, "/elmax/response/id", _handle_id)
        await mqtt.async_publish(self.hass, "/elmax/request/id", "{}")

        try:
            await asyncio.wait_for(event.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass

        unsub()
        return panels

    async def _test_login(self, panel_id: str, pin: str) -> bool:
        """Test login to validate credentials."""
        result = {}
        event = asyncio.Event()

        @callback
        def _handle_login(msg):
            try:
                data = json.loads(msg.payload)
                result.update(data)
            except json.JSONDecodeError:
                pass
            event.set()

        unsub = await mqtt.async_subscribe(
            self.hass, f"/elmax/response/login/{panel_id}", _handle_login
        )
        await mqtt.async_publish(
            self.hass,
            f"/elmax/request/login/{panel_id}",
            json.dumps({"pin": pin}),
        )

        try:
            await asyncio.wait_for(event.wait(), timeout=10)
        except asyncio.TimeoutError:
            unsub()
            return False

        unsub()
        return "token" in result

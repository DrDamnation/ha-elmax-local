"""Config flow for Elmax Local."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import aiohttp
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.components import mqtt
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult

from .auth import AuthManager, ElmaxAuthError
from .const import (
    CONF_PANEL_HOST, CONF_PANEL_ID, CONF_PANEL_PIN,
    DOMAIN, TOPIC_REQUEST_ID, TOPIC_RESPONSE_ID,
)

_LOGGER = logging.getLogger(__name__)
DISCOVERY_TIMEOUT = 5


class ElmaxLocalConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            panel_id = user_input[CONF_PANEL_ID].strip()
            pin = user_input[CONF_PANEL_PIN].strip()
            host = user_input[CONF_PANEL_HOST].strip()

            err = await self._validate(host, panel_id, pin)
            if err is None:
                await self.async_set_unique_id(panel_id)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"Elmax {panel_id[-6:]}",
                    data={CONF_PANEL_ID: panel_id, CONF_PANEL_PIN: pin,
                          CONF_PANEL_HOST: host},
                )
            errors["base"] = err

        panels = await self._discover_panels()
        if panels:
            schema = vol.Schema({
                vol.Required(CONF_PANEL_ID): vol.In({p: p for p in panels}),
                vol.Required(CONF_PANEL_PIN): str,
                vol.Required(CONF_PANEL_HOST): str,
            })
        else:
            schema = vol.Schema({
                vol.Required(CONF_PANEL_ID): str,
                vol.Required(CONF_PANEL_PIN): str,
                vol.Required(CONF_PANEL_HOST): str,
            })

        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    async def async_step_import(self, import_data: dict[str, Any]) -> FlowResult:
        """Invocato dal migration service. Bypassa validazione interattiva."""
        await self.async_set_unique_id(import_data[CONF_PANEL_ID])
        self._abort_if_unique_id_configured()
        return self.async_create_entry(
            title=f"Elmax {import_data[CONF_PANEL_ID][-6:]}",
            data=import_data,
        )

    async def _discover_panels(self) -> list[str]:
        try:
            if not mqtt.async_wait_for_mqtt_client(self.hass):
                return []
        except Exception:
            return []
        panels: list[str] = []
        event = asyncio.Event()

        @callback
        def _on_id(msg):
            try:
                data = json.loads(msg.payload)
                if "centrale" in data:
                    panels.append(data["centrale"])
                    event.set()
            except (json.JSONDecodeError, KeyError):
                pass

        try:
            unsub = await mqtt.async_subscribe(self.hass, TOPIC_RESPONSE_ID, _on_id)
            await mqtt.async_publish(self.hass, TOPIC_REQUEST_ID, "{}")
            try:
                await asyncio.wait_for(event.wait(), timeout=DISCOVERY_TIMEOUT)
            except asyncio.TimeoutError:
                pass
            unsub()
        except Exception as err:
            _LOGGER.debug("MQTT discovery failed: %s", err)
        return panels

    async def _validate(self, host: str, panel_id: str, pin: str) -> str | None:
        auth = AuthManager(self.hass, host, pin)
        try:
            token = await auth.async_get_token()
            session = await auth._ensure_session()
            async with session.get(
                f"https://{host}/api/v2/discovery",
                headers={"Authorization": f"JWT {token}"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return "cannot_connect"
                data = await resp.json()
                if data.get("centrale") and data["centrale"] != panel_id:
                    return "panel_id_mismatch"
            return None
        except ElmaxAuthError as err:
            if "401" in str(err) or "403" in str(err):
                return "invalid_auth"
            return "cannot_connect"
        except Exception as err:
            _LOGGER.exception("Validate error: %s", err)
            return "unknown"
        finally:
            await auth.async_close()

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return ElmaxLocalOptionsFlow(config_entry)


class ElmaxLocalOptionsFlow(config_entries.OptionsFlow):
    """Implementata in Task 18."""

    def __init__(self, config_entry):
        self.config_entry = config_entry

    async def async_step_init(self, user_input=None):
        raise NotImplementedError("Task 18")

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
try:
    from homeassistant.helpers.service_info.zeroconf import ZeroconfServiceInfo
except ImportError:
    from homeassistant.components.zeroconf import ZeroconfServiceInfo  # type: ignore[no-redef]
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult

from .auth import AuthManager, ElmaxAuthError
from .const import (
    CONF_ENABLE_MQTT, CONF_ENABLE_WS, CONF_PANEL_HOST, CONF_PANEL_ID,
    CONF_PANEL_PIN, CONF_RECONCILE_INTERVAL, DEFAULT_RECONCILE_INTERVAL,
    DOMAIN, MAX_RECONCILE_INTERVAL, MIN_RECONCILE_INTERVAL,
    TOPIC_REQUEST_ID, TOPIC_RESPONSE_ID,
)

_LOGGER = logging.getLogger(__name__)
DISCOVERY_TIMEOUT = 5


class ElmaxLocalConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        default_host = self.context.get("host", "")

        if user_input is not None:
            pin = user_input[CONF_PANEL_PIN].strip()
            host = user_input[CONF_PANEL_HOST].strip()

            panel_id, err = await self._validate(host, pin)
            if err is None and panel_id:
                await self.async_set_unique_id(panel_id)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"Elmax {panel_id[-6:]}",
                    data={CONF_PANEL_ID: panel_id, CONF_PANEL_PIN: pin,
                          CONF_PANEL_HOST: host},
                )
            errors["base"] = err or "unknown"

        # If MQTT discovery finds the panel, pre-fill the host (best-effort).
        if not default_host:
            discovered = await self._discover_panels()
            if discovered:
                default_host = discovered[0].get("host", default_host)

        schema = vol.Schema({
            vol.Required(CONF_PANEL_HOST, default=default_host): str,
            vol.Required(CONF_PANEL_PIN): str,
        })

        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    async def async_step_zeroconf(self, discovery_info: ZeroconfServiceInfo) -> FlowResult:
        host = str(discovery_info.ip_address)
        self.context["host"] = host
        return await self.async_step_user()

    async def async_step_import(self, import_data: dict[str, Any]) -> FlowResult:
        """Invocato dal migration service. Bypassa validazione interattiva."""
        await self.async_set_unique_id(import_data[CONF_PANEL_ID])
        self._abort_if_unique_id_configured()
        return self.async_create_entry(
            title=f"Elmax {import_data[CONF_PANEL_ID][-6:]}",
            data=import_data,
        )

    async def _discover_panels(self) -> list[dict]:
        """Probe panels via MQTT. Returns list of dicts with at least
        'centrale' (panel_id). Host is not exposed via this topic, so
        callers must still ask the user for the IP."""
        try:
            if not mqtt.async_wait_for_mqtt_client(self.hass):
                return []
        except Exception:
            return []
        panels: list[dict] = []
        event = asyncio.Event()

        @callback
        def _on_id(msg):
            try:
                data = json.loads(msg.payload)
                if "centrale" in data:
                    panels.append(data)
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

    async def _validate(self, host: str, pin: str) -> tuple[str | None, str | None]:
        """Login to the panel and read its panel_id from /discovery.
        Returns (panel_id, error_key). On success, error_key is None."""
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
                    return None, "cannot_connect"
                data = await resp.json()
                panel_id = data.get("centrale")
                if not panel_id:
                    return None, "cannot_connect"
                return panel_id, None
        except ElmaxAuthError as err:
            if "401" in str(err) or "403" in str(err):
                return None, "invalid_auth"
            return None, "cannot_connect"
        except Exception as err:
            _LOGGER.exception("Validate error: %s", err)
            return None, "unknown"
        finally:
            await auth.async_close()

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return ElmaxLocalOptionsFlow(config_entry)


class ElmaxLocalOptionsFlow(config_entries.OptionsFlow):
    def __init__(self, config_entry):
        self.config_entry = config_entry

    async def async_step_init(self, user_input=None):
        if user_input is not None:
            user_input[CONF_RECONCILE_INTERVAL] = max(
                MIN_RECONCILE_INTERVAL,
                min(MAX_RECONCILE_INTERVAL, user_input[CONF_RECONCILE_INTERVAL])
            )
            return self.async_create_entry(title="", data=user_input)

        opts = self.config_entry.options
        schema = vol.Schema({
            vol.Required(CONF_ENABLE_WS,
                         default=opts.get(CONF_ENABLE_WS, True)): bool,
            vol.Required(CONF_ENABLE_MQTT,
                         default=opts.get(CONF_ENABLE_MQTT, True)): bool,
            vol.Required(CONF_RECONCILE_INTERVAL,
                         default=opts.get(CONF_RECONCILE_INTERVAL,
                                          DEFAULT_RECONCILE_INTERVAL)):
                vol.All(int, vol.Range(min=MIN_RECONCILE_INTERVAL,
                                       max=MAX_RECONCILE_INTERVAL)),
        })
        return self.async_show_form(step_id="init", data_schema=schema)

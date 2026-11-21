"""Config flow for the Alula integration."""

from __future__ import annotations

import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import callback

from .const import (
    CONF_CONFIG_RESCAN_HOURS,
    CONF_PANEL_ID,
    CONF_SCAN_INTERVAL,
    CONF_ZONE_REFRESH_MINUTES,
    DEFAULT_CONFIG_RESCAN_HOURS,
    DEFAULT_SCAN_INTERVAL,
    DEFAULT_ZONE_REFRESH_MINUTES,
    DOMAIN,
)

from .pyalula import AlulaClient
from .pyalula.client import AlulaApiError, AlulaAuthError

_LOGGER = logging.getLogger(__name__)

STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): str,
        vol.Optional(CONF_PANEL_ID, default=""): str,
    }
)


class AlulaConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle the initial setup UI for Alula."""

    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return AlulaOptionsFlow()

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            username = user_input[CONF_USERNAME]
            password = user_input[CONF_PASSWORD]
            panel_id = user_input.get(CONF_PANEL_ID) or None

            await self.async_set_unique_id(username.lower())
            self._abort_if_unique_id_configured()

            client = AlulaClient()
            try:
                await client.login(username, password)
            except AlulaAuthError:
                errors["base"] = "invalid_auth"
            except AlulaApiError:
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.exception("Unexpected error during Alula login")
                errors["base"] = "unknown"
            finally:
                await client.close()

            if not errors:
                data = {
                    CONF_USERNAME: username,
                    CONF_PASSWORD: password,
                }
                if panel_id:
                    data[CONF_PANEL_ID] = panel_id
                return self.async_create_entry(title=f"Alula ({username})", data=data)

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_SCHEMA,
            errors=errors,
        )


class AlulaOptionsFlow(OptionsFlow):
    """Tune how often the integration talks to the panel."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(data=user_input)

        opts = self.config_entry.options
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_ZONE_REFRESH_MINUTES,
                    default=opts.get(CONF_ZONE_REFRESH_MINUTES, DEFAULT_ZONE_REFRESH_MINUTES),
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=1440)),
                vol.Required(
                    CONF_CONFIG_RESCAN_HOURS,
                    default=opts.get(CONF_CONFIG_RESCAN_HOURS, DEFAULT_CONFIG_RESCAN_HOURS),
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=168)),
                vol.Required(
                    CONF_SCAN_INTERVAL,
                    default=opts.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL),
                ): vol.All(vol.Coerce(int), vol.Range(min=15, max=3600)),
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema)

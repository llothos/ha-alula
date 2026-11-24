"""Alula alarm panel integration for Home Assistant."""

from __future__ import annotations

import logging
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady

from .const import (
    CONF_CONFIG_RESCAN_HOURS,
    CONF_LIVE_UPDATES,
    CONF_PANEL_ID,
    CONF_SCAN_INTERVAL,
    CONF_UPDATE_MODE,
    CONF_ZONE_REFRESH_MINUTES,
    DATA_CLIENT,
    DATA_COORDINATOR,
    DEFAULT_CONFIG_RESCAN_HOURS,
    DEFAULT_LIVE_UPDATES,
    DEFAULT_SCAN_INTERVAL,
    DEFAULT_ZONE_REFRESH_MINUTES,
    DOMAIN,
    MODE_FULL,
    MODE_PUSH_ONLY,
    MODE_REST_ONLY,
)
from .coordinator import AlulaCoordinator

from .pyalula import AlulaClient
from .pyalula.client import AlulaApiError, AlulaAuthError

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.ALARM_CONTROL_PANEL, Platform.BINARY_SENSOR]


def _read_options(entry: ConfigEntry) -> tuple[int, float, float, str]:
    """Return (rest poll s, zone refresh s, config rescan s, update mode)."""
    opts = entry.options
    mode = opts.get(CONF_UPDATE_MODE)
    if mode not in (MODE_FULL, MODE_PUSH_ONLY, MODE_REST_ONLY):
        # Older saved options only had the live_updates boolean.
        mode = MODE_FULL if opts.get(CONF_LIVE_UPDATES, DEFAULT_LIVE_UPDATES) else MODE_REST_ONLY
    return (
        int(opts.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)),
        float(opts.get(CONF_ZONE_REFRESH_MINUTES, DEFAULT_ZONE_REFRESH_MINUTES)) * 60,
        float(opts.get(CONF_CONFIG_RESCAN_HOURS, DEFAULT_CONFIG_RESCAN_HOURS)) * 3600,
        mode,
    )


def _apply_mode(client: AlulaClient, mode: str) -> None:
    client.ws_enabled = mode != MODE_REST_ONLY
    client.send_panel_requests = mode == MODE_FULL


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Apply changed options live (no reload, so no extra Alula login)."""
    data = hass.data[DOMAIN][entry.entry_id]
    scan, zone_refresh, config_rescan, mode = _read_options(entry)
    client = data[DATA_CLIENT]
    data[DATA_COORDINATOR].update_interval = timedelta(seconds=scan)
    client.zone_refresh_interval = zone_refresh
    client.zone_config_refresh_interval = config_rescan
    was_ws = client.ws_enabled
    _apply_mode(client, mode)
    if was_ws and not client.ws_enabled:
        await client.disconnect_ws()  # REST-only from now on
    elif client.ws_enabled and not was_ws:
        await data[DATA_COORDINATOR].async_request_refresh()  # reconnect now
    _LOGGER.debug(
        "Alula options updated: poll=%ss zone refresh=%ss config rescan=%ss mode=%s",
        scan, zone_refresh, config_rescan, mode,
    )


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Alula from a config entry."""
    username: str = entry.data[CONF_USERNAME]
    password: str = entry.data[CONF_PASSWORD]
    panel_id: str | None = entry.data.get(CONF_PANEL_ID)

    scan, zone_refresh, config_rescan, mode = _read_options(entry)
    client = AlulaClient(
        zone_refresh_interval=zone_refresh,
        zone_config_refresh_interval=config_rescan,
    )
    _apply_mode(client, mode)
    try:
        await client.login(username, password)
    except AlulaAuthError as err:
        await client.close()
        raise ConfigEntryAuthFailed(f"Invalid credentials: {err}") from err
    except AlulaApiError as err:
        await client.close()
        raise ConfigEntryNotReady(f"Cannot connect to Alula API: {err}") from err

    coordinator = AlulaCoordinator(hass, client, panel_id=panel_id, scan_interval=scan)
    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
        DATA_CLIENT: client,
        DATA_COORDINATOR: coordinator,
    }

    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        data = hass.data[DOMAIN].pop(entry.entry_id)
        await data[DATA_CLIENT].close()
    return unload_ok

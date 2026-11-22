"""Constants for the Alula Home Assistant integration."""

DOMAIN = "alula"

CONF_PANEL_ID = "panel_id"

# Options (Settings -> Devices & Services -> Alula -> Configure)
CONF_SCAN_INTERVAL = "scan_interval"
CONF_ZONE_REFRESH_MINUTES = "zone_refresh_minutes"
CONF_CONFIG_RESCAN_HOURS = "config_rescan_hours"
CONF_LIVE_UPDATES = "live_updates"

DEFAULT_SCAN_INTERVAL = 60  # seconds (REST only; zone refresh is throttled separately)
DEFAULT_ZONE_REFRESH_MINUTES = 10  # status-only refresh of programmed zones
DEFAULT_CONFIG_RESCAN_HOURS = 6    # full rescan of all zone slots
DEFAULT_LIVE_UPDATES = True        # WebSocket push + zone refresh; False = REST status only

# Entry data keys
DATA_CLIENT = "client"
DATA_COORDINATOR = "coordinator"

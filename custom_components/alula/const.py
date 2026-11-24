"""Constants for the Alula Home Assistant integration."""

DOMAIN = "alula"

CONF_PANEL_ID = "panel_id"

# Options (Settings -> Devices & Services -> Alula -> Configure)
CONF_SCAN_INTERVAL = "scan_interval"
CONF_ZONE_REFRESH_MINUTES = "zone_refresh_minutes"
CONF_CONFIG_RESCAN_HOURS = "config_rescan_hours"
CONF_LIVE_UPDATES = "live_updates"  # legacy boolean; superseded by CONF_UPDATE_MODE
CONF_UPDATE_MODE = "update_mode"

MODE_FULL = "full"            # push events + periodic zone refresh + catch-up after reconnect
MODE_PUSH_ONLY = "push_only"  # push events only; no requests to the panel after the first scan
MODE_REST_ONLY = "rest_only"  # no WebSocket at all; arming state / online status via REST

DEFAULT_SCAN_INTERVAL = 60  # seconds (REST only; zone refresh is throttled separately)
DEFAULT_ZONE_REFRESH_MINUTES = 10  # status-only refresh of programmed zones
DEFAULT_CONFIG_RESCAN_HOURS = 6    # full rescan of all zone slots
DEFAULT_LIVE_UPDATES = True        # legacy default
DEFAULT_UPDATE_MODE = MODE_FULL

# Entry data keys
DATA_CLIENT = "client"
DATA_COORDINATOR = "coordinator"

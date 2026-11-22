"""AlulaClient — async client for the Alula alarm panel API.

Authentication:  OAuth2 password grant  →  POST /oauth/token
Panel state:     REST polling            →  GET /rest/v1/devices
Zone state:      WebSocket request/reply →  wss://api.alula.net/ws/v1
Arm / disarm:    WebSocket write         →  wss://api.alula.net/ws/v1
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any

import aiohttp

from .const import (
    ARM_LEVEL_AWAY,
    ARM_LEVEL_DISARM,
    ARM_LEVEL_NIGHT,
    ARM_LEVEL_STAY,
    BASE_URL,
    OAUTH_CLIENT_ID,
    OAUTH_CLIENT_SECRET,
    PATH_DEVICES_PANELS,
    PATH_TOKEN,
    REQUEST_TIMEOUT,
    TOKEN_REFRESH_BUFFER,
    WS_CHANNEL,
    WS_CONNECT_TIMEOUT,
    WS_RESPONSE_TIMEOUT,
    WS_URL,
    ZONE_BATCH_SIZE,
    ZONE_CONFIG_REFRESH_INTERVAL,
    ZONE_REFRESH_INTERVAL,
)

PATH_SELF = "/rest/v1/self"
from .models import AlarmPanel, ArmState, Zone, ZoneType

_LOGGER = logging.getLogger(__name__)


def _parse_device_type(device_type: str) -> "ZoneType":
    """Map Alula deviceType strings (e.g. 'DWS') to ZoneType enum."""
    t = device_type.upper()
    if t in ("DWS", "DOOR", "WINDOW"):
        return ZoneType.DOOR
    if t in ("PIR", "MOTION", "MD"):
        return ZoneType.MOTION
    if t in ("SMOKE", "SMOKE_DET"):
        return ZoneType.SMOKE
    if t in ("CO", "CO_DET"):
        return ZoneType.CO
    if t in ("GLASS", "GB", "GLASS_BREAK"):
        return ZoneType.GLASS_BREAK
    return ZoneType.OTHER


# deviceType values that mean "nothing is enrolled in this slot"
_UNUSED_DEVICE_TYPES = {"", "none", "unused", "notused", "not_used", "unassigned", "disabled", "0"}


def _is_programmed(cfg: dict) -> bool:
    """Return True if a zoneConfiguration entry describes a real, enrolled zone.

    The panel exposes every slot up to maxZones (64). A slot is unprogrammed
    when its zoneSource is "disabled", or when it has neither a name nor a real
    device type; if in doubt a zone is shown rather than hidden.
    """
    if not cfg:
        return False
    # The panel marks empty slots with zoneSource "disabled" (id 0). Once the
    # cloud cache is fully populated it returns default names ("Zone 19") and a
    # placeholder deviceType for those slots too, so name/deviceType alone
    # can't be trusted. Check the source first.
    source = str(cfg.get("zoneSourceEnum") or "").strip().lower()
    if source == "disabled" or (cfg.get("zoneSource") == 0 and not cfg.get("id")):
        return False
    device_type = str(cfg.get("deviceType") or "").strip().lower()
    name = str(cfg.get("zoneName") or "").strip()
    return device_type not in _UNUSED_DEVICE_TYPES or bool(name)


def _default_zone_name(idx: str) -> str:
    try:
        return f"Zone {int(idx) + 1:02d}"
    except ValueError:
        return f"Zone {idx}"


# Map public arm mode strings → byte codes the WebSocket expects
_ARM_MODE_MAP: dict[str, int] = {
    "disarm": ARM_LEVEL_DISARM,
    "stay": ARM_LEVEL_STAY,
    "home": ARM_LEVEL_STAY,
    "night": ARM_LEVEL_NIGHT,
    "away": ARM_LEVEL_AWAY,
}


class AlulaAuthError(Exception):
    """Invalid credentials or expired token."""


class AlulaApiError(Exception):
    """Unexpected API / network error."""


class AlulaClient:
    """Async client for the Alula alarm panel API.

    Usage::

        async with AlulaClient() as client:
            await client.login("user@example.com", "password")
            panel = await client.get_panel_status()
            print(panel.arm_state)
            await client.arm("away")
            await client.disarm()
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        ws_url: str = WS_URL,
        zone_refresh_interval: float = ZONE_REFRESH_INTERVAL,
        zone_config_refresh_interval: float = ZONE_CONFIG_REFRESH_INTERVAL,
    ) -> None:
        # How often (seconds) to ask the panel for zoneStatus / a full rescan.
        # Public so the integration can change them live from its options.
        self.zone_refresh_interval = zone_refresh_interval
        self.zone_config_refresh_interval = zone_config_refresh_interval
        self._base_url = base_url.rstrip("/")
        self._ws_url = ws_url
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ws_task: asyncio.Task | None = None
        self._reconnect_task: asyncio.Task | None = None
        self._connect_lock = asyncio.Lock()
        self._closing = False
        # False = REST-only mode: no WebSocket is opened by the integration
        # (an arm/disarm command still opens one on demand).
        self.ws_enabled = True
        self._ws_connected_once = False
        self._keypad_enable_ok = True      # cleared if the panel NAKs virtualKeypadInput
        self._keypad_request_id: str | None = None
        self.panel_online = True           # last REST onlineStatus; skip catch-up reads while offline
        self._ws_ready: asyncio.Event = asyncio.Event()

        # Token state
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._token_expires_at: float = 0.0
        self._user_id: str | None = None   # API user ID, needed for write commands

        # Latest zone data received from WebSocket
        self._zones: dict[str, Zone] = {}  # keyed by zone index string (0-based)
        self._zone_configs: dict[str, dict] = {}  # zone config data keyed by index
        self._zone_status: dict[str, dict] = {}   # latest raw zoneStatus per index (any order of arrival)

        # Throttling of requestMfd traffic (monotonic timestamps; 0 = never)
        self._last_zone_fetch: float = 0.0
        self._last_config_fetch: float = 0.0

        # Previous keypad armed flag for transition detection
        self._prev_keypad_armed: bool | None = None

        # Panel metadata from panelDefinition (populated during init)
        self._max_zones: int = 64
        self._highest_zone_index: int | None = None  # from highestUsedIndexes

        # Panel id cache (filled after first get_panel_status)
        self._panel_id: str | None = None

        # Optional callback fired when keypad indicates an arm state transition
        self.on_arm_state_change: callable | None = None

        # Optional callback fired when a known zone changes open/bypassed state
        # (from a poll response or an unsolicited push from the panel)
        self.on_zone_change: callable | None = None

    @property
    def ws_connected(self) -> bool:
        """Whether the WebSocket is currently open."""
        return self._ws is not None and not self._ws.closed

    @property
    def zones(self) -> list[Zone]:
        """Programmed zones only, ordered by zone index."""
        return sorted(self._zones.values(), key=lambda z: int(z.id) if z.id.isdigit() else 999)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> AlulaClient:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def close(self) -> None:
        self._closing = True
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()

    async def disconnect_ws(self) -> None:
        """Close the WebSocket (and stop reconnecting) but keep the REST session."""
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        task, self._ws_task = self._ws_task, None
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def login(self, username: str, password: str) -> None:
        """Authenticate with username/password and store the token."""
        data = await self._token_request({
            "grant_type": "password",
            "username": username,
            "password": password,
            "client_id": OAUTH_CLIENT_ID,
            "client_secret": OAUTH_CLIENT_SECRET,
        })
        self._store_token(data)
        _LOGGER.debug("Alula login OK, token expires in %ds", data.get("expires_in", 0))
        # Fetch user ID needed for write commands
        self._user_id = await self._fetch_user_id()

    async def _refresh_access_token(self) -> None:
        if not self._refresh_token:
            raise AlulaAuthError("No refresh token available — call login() first")
        _LOGGER.debug("Refreshing Alula access token")
        data = await self._token_request({
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
            "client_id": OAUTH_CLIENT_ID,
            "client_secret": OAUTH_CLIENT_SECRET,
        })
        self._store_token(data)

    def _store_token(self, data: dict) -> None:
        self._access_token = data["access_token"]
        expires_in: int = data.get("expires_in", 900)
        self._token_expires_at = time.monotonic() + expires_in
        self._refresh_token = data.get("refresh_token", self._refresh_token)

    async def _maybe_refresh_token(self) -> None:
        remaining = self._token_expires_at - time.monotonic()
        if remaining > TOKEN_REFRESH_BUFFER:
            return
        await self._refresh_access_token()

    async def _fetch_user_id(self) -> str | None:
        try:
            data = await self._get(self._base_url + PATH_SELF)
            uid = data.get("data", {}).get("id")
            _LOGGER.debug("Alula user ID: %s", uid)
            return uid
        except Exception as exc:
            _LOGGER.warning("Could not fetch user ID: %s", exc)
            return None

    async def _token_request(self, payload: dict) -> dict:
        url = self._base_url + PATH_TOKEN
        async with self._get_session().post(
            url,
            data=payload,
            headers={"Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status == 400:
                body = await resp.json()
                raise AlulaAuthError(
                    f"Auth failed: {body.get('error_description', body)}"
                )
            if not resp.ok:
                raise AlulaApiError(f"Token endpoint returned {resp.status}")
            return await resp.json()

    # ------------------------------------------------------------------
    # REST: panel status
    # ------------------------------------------------------------------

    async def get_panel_status(self, panel_id: str | None = None) -> AlarmPanel:
        """Return current panel arm state via REST polling.

        Also refreshes zone list from the last WebSocket data if available.
        """
        await self._maybe_refresh_token()
        url = self._base_url + PATH_DEVICES_PANELS
        data = await self._get(url)
        items = data.get("data", [])
        if not items:
            raise AlulaApiError("No panels found on this account")

        # If caller specified a panel_id, find it; otherwise use the first
        panel_data = next(
            (d for d in items if str(d.get("id")) == panel_id),
            items[0],
        )
        self._panel_id = str(panel_data["id"])
        if _LOGGER.isEnabledFor(logging.DEBUG):
            # Diagnostic: show which REST attributes could carry live zone/open
            # state (the Alula app may read door state from here, not the WS).
            attrs = panel_data.get("attributes", panel_data)
            interesting = {
                k: v for k, v in attrs.items()
                if any(t in k.lower() for t in ("zone", "open", "trouble", "fault", "event", "status", "sensor"))
            }
            _LOGGER.debug("REST attribute keys: %s", sorted(attrs.keys()))
            _LOGGER.debug("REST zone/status attributes: %s", interesting)
        zones = self.zones
        panel = AlarmPanel.from_api(panel_data, zones=zones)
        self.panel_online = panel.online
        return panel

    # ------------------------------------------------------------------
    # WebSocket: connect + receive loop
    # ------------------------------------------------------------------

    async def connect_ws(self) -> None:
        """Open the WebSocket (serialised so two callers never open two sockets)."""
        async with self._connect_lock:
            await self._connect_ws_locked()

    async def _connect_ws_locked(self) -> None:
        """Open the WebSocket, wait for ready, then subscribe to device channels."""
        await self._maybe_refresh_token()
        if self._ws and not self._ws.closed:
            return
        if not self._panel_id:
            raise AlulaApiError("Panel ID unknown — call get_panel_status() first")

        ws_url = f"{self._ws_url}?access_token={self._access_token}"
        _LOGGER.debug("Connecting Alula WebSocket: %s", self._ws_url)
        self._ws_ready = asyncio.Event()
        self._ws = await self._get_session().ws_connect(
            ws_url,
            timeout=aiohttp.ClientWSTimeout(ws_close=WS_CONNECT_TIMEOUT),
        )
        self._ws_task = asyncio.create_task(
            self._ws_receive_loop(), name="alula-ws-recv"
        )

        # Wait for the server's "ready" message before subscribing
        await asyncio.wait_for(self._ws_ready.wait(), timeout=WS_CONNECT_TIMEOUT)

        # Subscribe to device.status, device.helix, and device.keypad
        for channel in ("device.status", "device.helix", "device.keypad"):
            await self._ws_send({
                "channel": channel,
                "id": str(uuid.uuid4()),
                "subscribe": {"deviceId": self._panel_id},
            })

        # Enable virtual keypad push stream (triggers virtualKeypadOutput events).
        # Some panels NAK this ("Unsupported command"); stop asking once they do.
        if self._keypad_enable_ok:
            self._keypad_request_id = str(uuid.uuid4())
            await self._ws_send({
                "channel": WS_CHANNEL,
                "id": str(uuid.uuid4()),
                "send": {
                    "cmdrsp": "virtualKeypadInput",
                    "deviceId": self._panel_id,
                    "payload": {"enable": True},
                    "requestId": self._keypad_request_id,
                },
            })

        # Panel definition / highest used indexes only need fetching once; the
        # socket is re-opened every ~10 minutes and the answers never change.
        if not self._ws_connected_once or self._highest_zone_index is None:
            await self._ws_send({
                "channel": WS_CHANNEL,
                "id": str(uuid.uuid4()),
                "send": {
                    "cmdrsp": "requestMfd",
                    "deviceId": self._panel_id,
                    "payload": [
                        {"name": "panelDefinition"},
                        {"name": "highestUsedIndexes"},
                    ],
                    "requestId": str(uuid.uuid4()),
                },
            })
            # Allow panel time to respond before zone requests
            await asyncio.sleep(WS_RESPONSE_TIMEOUT)

        self._ws_connected_once = True
        _LOGGER.debug("Alula WebSocket connected and subscribed")

    async def _ws_receive_loop(self) -> None:
        """Background task: read WebSocket messages and dispatch them."""
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_ws_message(msg.json())
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    _LOGGER.warning("Alula WebSocket error: %s", self._ws.exception())
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                    break
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            _LOGGER.warning("Alula WebSocket receive loop died: %s", exc)
        finally:
            _LOGGER.debug("Alula WebSocket receive loop exited")
            if (
                not self._closing
                and self.ws_enabled
                and asyncio.current_task() is self._ws_task
            ):
                self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        """Re-open the socket right away: the server closes it about every 10 min."""
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(
            self._reconnect_ws(), name="alula-ws-reconnect"
        )

    async def _reconnect_ws(self) -> None:
        delay = 1.0
        while not self._closing and self.ws_enabled:
            try:
                await asyncio.sleep(delay)
                if not self.ws_enabled:
                    return
                await self.connect_ws()
                # Pushes sent while we were disconnected are lost, so catch up
                # (status of programmed zones only), unless the panel is offline.
                if self.panel_online:
                    await self.fetch_zone_statuses(status_only=True)
                _LOGGER.debug("Alula WebSocket reconnected")
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - keep trying with backoff
                _LOGGER.debug("Alula WebSocket reconnect failed (%s); retry in %.0fs", exc, delay)
                delay = min(delay * 2, 300.0)

    async def _handle_ws_message(self, msg: dict) -> None:
        """Dispatch an incoming WebSocket message."""
        _LOGGER.debug("WS <<<: %s", msg)

        # "ready" message — signal that subscriptions can now be sent
        if msg.get("message") == "ready" and "sessionId" in msg:
            _LOGGER.debug("WS ready, sessionId=%s", msg.get("sessionId"))
            self._ws_ready.set()
            return

        # Panel refused the keypad-enable command: don't send it again
        data = msg.get("event", {}).get("data", {})
        if (
            data.get("cmdrsp") == "nak"
            and self._keypad_request_id
            and data.get("requestId") == self._keypad_request_id
        ):
            self._keypad_enable_ok = False
            _LOGGER.debug("Panel NAKed virtualKeypadInput; will not send it again")
            return

        # All other messages: ingest zone/arm state data
        self._ingest_zone_data(msg)

    def _ingest_zone_data(self, msg: dict) -> None:
        """Extract and cache zone/arm state from any WS message.

        Messages from the server arrive as:
          {"channel":"device.helix","id":"...","event":{"data":{"cmdrsp":"sendMfd","payload":[...]},...}}
        or for push events:
          {"channel":"device.helix","id":"...","event":{"data":{"cmdrsp":"virtualKeypadOutput","payload":{...}},...}}
        """
        # Unwrap event envelope
        event = msg.get("event", {})
        data = event.get("data", {})
        cmdrsp = data.get("cmdrsp", "")

        # sendMfd: response to requestMfd — payload is a list of named objects
        if cmdrsp == "sendMfd":
            changed = False
            for item in data.get("payload", []):
                changed |= self._ingest_named_payload(item)
            if changed:
                self._notify_zone_change()
            return

        # virtualKeypadOutput: pushed arm/display state from the panel
        if cmdrsp == "virtualKeypadOutput":
            kp = data.get("payload", {})
            if isinstance(kp, dict):
                self._update_arm_state_from_keypad(kp)
            return

        # Fallback: some servers push zone events at the top level (older firmware)
        if self._ingest_named_payload(msg.get("payload", {})):
            self._notify_zone_change()

    def _notify_zone_change(self) -> None:
        if self.on_zone_change:
            try:
                self.on_zone_change()
            except Exception:  # never let a listener kill the receive loop
                _LOGGER.exception("on_zone_change callback failed")

    def _upsert_zone(self, idx: str) -> tuple[Zone | None, bool]:
        """Return (zone, created) for idx, or (None, False) if it is not programmed."""
        cfg = self._zone_configs.get(idx)
        if not cfg or not _is_programmed(cfg):
            return None, False
        zone = self._zones.get(idx)
        created = zone is None
        if created:
            zone = Zone(
                id=idx,
                name=cfg.get("zoneName") or _default_zone_name(idx),
                zone_type=_parse_device_type(cfg.get("deviceType", "")),
                is_open=False,
            )
            self._zones[idx] = zone
        else:
            zone.name = cfg.get("zoneName") or zone.name
            zone.zone_type = _parse_device_type(cfg.get("deviceType", ""))
        return zone, created

    @staticmethod
    def _apply_status(zone: Zone, value: dict) -> bool:
        """Apply a raw zoneStatus value; return True if open/bypassed changed."""
        is_open = bool(value.get("open", False))
        is_bypassed = bool(value.get("bypassed", False))
        changed = zone.is_open != is_open or zone.is_bypassed != is_bypassed
        zone.is_open = is_open
        zone.is_bypassed = is_bypassed
        zone.raw = value
        return changed

    def _ingest_named_payload(self, item: dict) -> bool:
        """Parse a single payload entry from a sendMfd response.

        Entries use the items[] format:
          {"name": "zoneStatus", "indexFirst": 0, "indexLast": N,
           "items": [{"index": 0, "value": {...}}, ...]}

        Returns True if an already-known zone changed open/bypassed state.
        Zones are only created for slots whose zoneConfiguration shows
        something is programmed; status that arrives before its configuration
        is cached and applied once the configuration is seen.
        """
        if not isinstance(item, dict):
            return False
        name = item.get("name", "")
        changed = False

        if name == "zoneConfiguration":
            for entry in item.get("items", []):
                idx = str(entry.get("index", "?"))
                cfg = entry.get("value", {})
                _LOGGER.debug("Zone %s config: %s", idx, cfg)
                if not cfg:
                    continue
                self._zone_configs[idx] = cfg
                if not _is_programmed(cfg):
                    self._zones.pop(idx, None)
                    continue
                zone, created = self._upsert_zone(idx)
                if created and idx in self._zone_status:
                    self._apply_status(zone, self._zone_status[idx])

        elif name == "zoneStatus":
            for entry in item.get("items", []):
                idx = str(entry.get("index", "?"))
                value = entry.get("value", {})
                if not value:
                    continue
                self._zone_status[idx] = value
                zone, created = self._upsert_zone(idx)
                if zone is None:
                    continue  # unprogrammed slot, or configuration not seen yet
                if self._apply_status(zone, value) and not created:
                    changed = True

        elif name == "panelDefinition":
            value = item.get("value", {})
            max_zones = value.get("maxZones")
            if max_zones:
                self._max_zones = int(max_zones)
                _LOGGER.debug("Panel maxZones=%d", self._max_zones)

        elif name == "highestUsedIndexes":
            value = item.get("value", {})
            # May be under "value" key directly or as a named field
            zone_idx = value.get("zoneIndex", value.get("zone"))
            if zone_idx is not None:
                self._highest_zone_index = int(zone_idx)
                _LOGGER.debug("Highest used zone index=%d", self._highest_zone_index)

        elif name in ("zoneUpdate", "zoneFault", "zoneRestore"):
            # Single-zone push (legacy flat format) — only for zones we already know
            value = item.get("value", item)
            try:
                update = Zone.from_ws(value if isinstance(value, dict) else item)
            except Exception:
                return False
            zone = self._zones.get(update.id)
            if zone is not None:
                changed = (
                    zone.is_open != update.is_open
                    or zone.is_bypassed != update.is_bypassed
                )
                zone.is_open = update.is_open
                zone.is_bypassed = update.is_bypassed
                zone.raw = update.raw

        return changed

    def _update_arm_state_from_keypad(self, kp: dict) -> None:
        """Detect arm-state transitions from virtualKeypadOutput and request a REST refresh.

        The keypad push is NOT used as authoritative state — REST ``armingLevel``
        is the sole source of truth.  We only watch for transitions (armed flag
        changing, or triggered indicators) and fire the callback so the HA
        coordinator triggers an immediate REST poll.
        """
        _LOGGER.debug("virtualKeypadOutput payload: %s", kp)
        armed = kp.get("armed", False)
        fire = kp.get("fire", False)
        alarm_memory = kp.get("alarmMemory", False)
        triggered = fire or alarm_memory

        # Detect transitions only — fire callback so coordinator does a REST poll
        prev = self._prev_keypad_armed
        self._prev_keypad_armed = armed

        if prev is not None and (armed != prev or triggered):
            _LOGGER.debug(
                "Keypad transition detected (armed: %s → %s, triggered: %s), requesting refresh",
                prev, armed, triggered,
            )
            if self.on_arm_state_change:
                self.on_arm_state_change()

    # ------------------------------------------------------------------
    # WebSocket: zone status request
    # ------------------------------------------------------------------

    @staticmethod
    def _index_batches(indexes: list[int]) -> list[tuple[int, int]]:
        """Group sorted zone indexes into contiguous (first, last) runs of <= batch size."""
        batches: list[tuple[int, int]] = []
        for idx in indexes:
            if batches and idx == batches[-1][1] + 1 and idx - batches[-1][0] < ZONE_BATCH_SIZE:
                batches[-1] = (batches[-1][0], idx)
            else:
                batches.append((idx, idx))
        return batches

    async def fetch_zone_statuses(
        self,
        device_id: str | None = None,
        force: bool = False,
        status_only: bool = False,
    ) -> list[Zone]:
        """Refresh zone data over the WebSocket, with as little traffic as possible.

        - First run (or every zone_config_refresh_interval): scan every slot for
          zoneConfiguration + zoneStatus, in batches of 3 like the web app.
        - Afterwards: at most one refresh per zone_refresh_interval, asking only
          for zoneStatus of the programmed zones. Live changes arrive as pushes.
        """
        if not self.ws_enabled:
            return self.zones  # REST-only mode: never talk to the panel over WS

        dev_id = device_id or self._panel_id
        if not dev_id:
            raise AlulaApiError("Panel ID unknown — call get_panel_status() first")

        now = time.monotonic()
        known = sorted(int(i) for i in self._zones if str(i).isdigit())
        full_scan = (
            force
            or not known
            or self._last_config_fetch == 0.0
            or (now - self._last_config_fetch) >= self.zone_config_refresh_interval
        )

        if status_only and known:
            full_scan = False  # catch-up after a reconnect: status only, no throttle

        if (
            not full_scan
            and not status_only
            and self._last_zone_fetch != 0.0
            and (now - self._last_zone_fetch) < self.zone_refresh_interval
        ):
            return self.zones  # recent enough; pushes keep it current

        await self._ensure_ws()

        if full_scan:
            max_index = self._highest_zone_index if self._highest_zone_index is not None \
                else self._max_zones - 1
            batches = [
                (first, min(first + ZONE_BATCH_SIZE - 1, max_index))
                for first in range(0, max_index + 1, ZONE_BATCH_SIZE)
            ]
            names = ("zoneConfiguration", "zoneStatus")
        else:
            batches = self._index_batches(known)
            names = ("zoneStatus",)

        for first, last in batches:
            inner = {
                "deviceId": dev_id,
                "cmdrsp": "requestMfd",
                "payload": [{"name": n, "indexFirst": first, "indexLast": last} for n in names],
                "requestId": str(uuid.uuid4()),
            }
            await self._ws_send({"channel": WS_CHANNEL, "id": str(uuid.uuid4()), "send": inner})

        # Wait for panel responses (arrive as pushed sendMfd events)
        await asyncio.sleep(WS_RESPONSE_TIMEOUT)

        self._last_zone_fetch = time.monotonic()
        if full_scan:
            self._last_config_fetch = self._last_zone_fetch

        zones = self.zones
        _LOGGER.debug(
            "Alula: %s refresh sent %d request(s); %d programmed zone(s): %s",
            "full" if full_scan else "status-only", len(batches),
            len(zones), [f"{z.id}:{z.name}" for z in zones],
        )
        return zones

    # ------------------------------------------------------------------
    # WebSocket: arm / disarm
    # ------------------------------------------------------------------

    async def arm(self, mode: str, user_number: int = 0,
                  silent: bool = False, no_entry_delay: bool = False,
                  device_id: str | None = None) -> None:
        """Arm the panel.

        :param mode: 'away', 'home'/'stay', or 'night'
        :param user_number: helix user index (0 = master)
        :param silent: arm without sounding exit beeps
        :param no_entry_delay: bypass entry delay
        """
        level = _ARM_MODE_MAP.get(mode.lower())
        if level is None:
            raise ValueError(
                f"Invalid arm mode {mode!r}. Use: away, stay/home, night"
            )
        await self._send_arming_level(
            level, user_number, silent, no_entry_delay, device_id
        )

    async def disarm(self, pin: str | list[str],
                     silent: bool = False, no_entry_delay: bool = False,
                     device_id: str | None = None) -> None:
        """Disarm the panel.

        :param pin: user PIN code as a string ('1234') or list of digit strings (['1','2','3','4'])
        """
        dev_id = device_id or self._panel_id
        if not dev_id:
            raise AlulaApiError("Panel ID unknown — call get_panel_status() first")

        await self._ensure_ws()

        pin_digits = list(pin) if isinstance(pin, str) else [str(d) for d in pin]

        inner = {
            "deviceId": dev_id,
            "cmdrsp": "changeArmingLevelUsingCode",
            "payload": {
                "armingLevelValue": ARM_LEVEL_DISARM,
                "armSilent": silent,
                "noEntryDelay": no_entry_delay,
                "pin": pin_digits,
            },
            "requestId": str(uuid.uuid4()),
        }
        msg = {"channel": WS_CHANNEL, "id": str(uuid.uuid4()), "send": inner}
        await self._ws_send(msg)
        _LOGGER.debug("Sent changeArmingLevelUsingCode (disarm) to panel %s", dev_id)

    async def _send_arming_level(
        self,
        level: int,
        user_number: int,
        silent: bool,
        no_entry_delay: bool,
        device_id: str | None,
    ) -> None:
        dev_id = device_id or self._panel_id
        if not dev_id:
            raise AlulaApiError("Panel ID unknown — call get_panel_status() first")

        await self._ensure_ws()

        inner = {
            "deviceId": dev_id,
            "cmdrsp": "writeMfd",
            "payload": [{
                "name": "armingLevel",
                "value": {
                    "armingLevelValue": level,
                    "userNumber": user_number,
                    "armSilent": silent,
                    "noEntryDelay": no_entry_delay,
                },
            }],
            "requestId": self._user_id or str(uuid.uuid4()),
        }
        msg = {"channel": WS_CHANNEL, "id": str(uuid.uuid4()), "send": inner}
        await self._ws_send(msg)
        _LOGGER.debug("Sent armingLevel %d to panel %s", level, dev_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            )
        return self._session

    def _auth_headers(self) -> dict[str, str]:
        if not self._access_token:
            raise AlulaAuthError("Not authenticated — call login() first")
        return {"Authorization": f"Bearer {self._access_token}"}

    async def _get(self, url: str) -> Any:
        headers = {**self._auth_headers(), "Accept": "application/json"}
        async with self._get_session().get(url, headers=headers) as resp:
            return await self._handle_response(resp)

    async def _ensure_ws(self) -> None:
        """Connect WebSocket if not already open."""
        await self._maybe_refresh_token()
        if self._ws is None or self._ws.closed:
            await self.connect_ws()

    async def _ws_send(self, msg: dict) -> None:
        if self._ws is None or self._ws.closed:
            raise AlulaApiError("WebSocket not connected")
        _LOGGER.debug("WS >>>: %s", msg)
        await self._ws.send_json(msg)

    @staticmethod
    async def _handle_response(resp: aiohttp.ClientResponse) -> Any:
        if resp.status == 401:
            raise AlulaAuthError("Alula API returned 401 — token invalid or expired")
        if not resp.ok:
            text = await resp.text()
            raise AlulaApiError(f"Alula API {resp.status}: {text[:200]}")
        return await resp.json()

"""Panel transport for Elmax Local — ContactVideo C315 REST bridge.

POLL + COMMAND capabilities via the C315 bridge REST API (plain HTTP, port 8083).
This transport has its OWN auth: it logs in at POST /api/login with a form-encoded
PIN, caches the JWT, and re-logs on 401/403.  It does NOT go through the shared
AuthManager which targets the official HTTPS /api/v2 panel endpoint.

EndpointId scheme
-----------------
  {panel_id}-cv-area-{index}
  {panel_id}-cv-zone-{index}
  {panel_id}-cv-out-{index}
  {panel_id}-cv-scene-{index}

These ids are *stable* and *index-recoverable*: split on "-cv-{kind}-" to get kind
and 0-based index.  They intentionally differ from the official transport
endpointIds (e.g. "{panel_id}-area-{eid}") because the C315 bridge is treated as
the sole authoritative channel in deployments where the official local API is
unavailable.  Cross-transport entity identity (matching the same physical entity
across transports) is therefore not attempted; entities keep whichever id the
active transport assigned them.

Command mapping
---------------
HA code (str)   C315 action         Notes
  "0"           disarm              area= param
  "1"           armp1               area= param
  "2"           armp2               area= param
  "3"           arm                 area= param (no native P1+P2 on C315; arm is
                                    "stay" / total on most panels — chosen as
                                    closest semantic match to P1+P2)
  "4"           arm                 area= param
  None (scene)  scene               index= param

Output on/off toggle caveat
----------------------------
The C315 /api/command/output endpoint is a TOGGLE — a single call flips state
regardless of direction.  To honour HA on/off semantics this transport reads the
current output state via /api/state before issuing the toggle, and skips the
command entirely when the panel is already in the requested state.  The current
state is always re-read immediately before the command (no stale cache) to reduce
the race window.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from typing import TYPE_CHECKING

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from ..const import (
    CMD_AREA_ARM_P1,
    CMD_AREA_ARM_P1P2,
    CMD_AREA_ARM_P2,
    CMD_AREA_ARM_TOTAL,
    CMD_AREA_DISARM,
    CMD_OUTPUT_OFF,
    CMD_OUTPUT_ON,
    CONF_CV_HOST,
    PANEL_BASE_URL,
)
from . import CommandResult, StateUpdateCallback, Transport, TransportCapability, TransportState

if TYPE_CHECKING:
    from ..auth import AuthManager

_LOGGER = logging.getLogger(__name__)

# Timeouts (seconds)
HTTP_TIMEOUT = 10
LOGIN_TIMEOUT = 10

# JWT proactive refresh: re-login when fewer than this many seconds remain
_TOKEN_REFRESH_MARGIN = 300  # 5 minutes before 1 h expiry

# Rate-limit retry (503 = "rate limited", retry up to this many times)
COMMAND_RATE_RETRIES = 3
COMMAND_RATE_DELAY = 1.2  # seconds between retries (> the bridge's 1 s window)

# C315 widget types
_TYPE_AREA = 1
_TYPE_SHUTTER = 10
_TYPE_SCENE = 12
# Outputs appear under several widget types on a real panel (doors and gates
# are their own types), so naming looks them all up by index.
_TYPES_OUTPUT = (3, 4, 6)

# Maps HA arm/disarm string codes to C315 action strings
_AREA_CMD_MAP: dict[str, str] = {
    CMD_AREA_DISARM: "disarm",
    CMD_AREA_ARM_P1: "armp1",
    CMD_AREA_ARM_P2: "armp2",
    # P1+P2 has no dedicated C315 endpoint; "arm" (stay/total) is the closest
    # semantic match and is documented as such in the module docstring.
    CMD_AREA_ARM_P1P2: "arm",
    CMD_AREA_ARM_TOTAL: "arm",
}


def _parse_jwt_exp(token: str) -> float:
    """Return the 'exp' Unix timestamp from a raw JWT, or now+3600 as fallback."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("not 3 parts")
        payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return float(payload["exp"])
    except Exception:
        return time.time() + 3600.0


# Internal kind -> the word the panel itself uses in its endpointIds.
# The official /api/v2/discovery ids are deterministic — "{panel_id}-area-0",
# "-zona-1", "-uscita-7", "-scenario-0" — and the bridge reports the same
# indexes for the same physical items (uscita 7 is "Serranda" on both). So
# this transport reproduces those ids instead of inventing its own: entities
# keep one identity whichever transport answers, and a failover updates them
# instead of replacing them.
_KIND_WORD = {"area": "area", "zone": "zona", "out": "uscita", "scene": "scenario"}


def _cv_endpoint_id(panel_id: str, kind: str, index: int) -> str:
    """Build the panel's own endpointId for an item, from its index."""
    return f"{panel_id}-{_KIND_WORD[kind]}-{index}"


def _parse_cv_endpoint_id(endpoint_id: str) -> tuple[str, int] | None:
    """Extract (kind, index) from an endpointId or return None."""
    for kind, word in _KIND_WORD.items():
        marker = f"-{word}-"
        if marker in endpoint_id:
            tail = endpoint_id.rsplit(marker, 1)[1]
            try:
                return kind, int(tail)
            except ValueError:
                return None
    return None


class PanelTransport(Transport):
    """ContactVideo C315 REST bridge transport.

    Implements POLL + COMMAND.  Has its own auth (separate from AuthManager).
    """

    name = "panel"
    capabilities = frozenset({TransportCapability.POLL, TransportCapability.COMMAND})

    def __init__(self, hass: HomeAssistant, panel_id: str, cv_host: str, pin: str):
        self._hass = hass
        self._panel_id = panel_id
        self._pin = pin
        self._base = PANEL_BASE_URL.format(host=cv_host)

        # Internal token cache
        self._token: str | None = None
        self._token_expiry: float = 0.0
        self._login_lock = asyncio.Lock()

        self._state = TransportState.DISABLED
        # The shared AuthManager is injected by the registry but NOT used here;
        # we keep the attribute to satisfy Transport._auth contract.
        self._auth: "AuthManager | None" = None

    # ------------------------------------------------------------------
    # Transport ABC
    # ------------------------------------------------------------------

    @property
    def state(self) -> TransportState:
        return self._state

    async def async_probe(self) -> bool:
        """Probe: attempt a login and verify we get a token."""
        try:
            await self._ensure_token()
            return True
        except Exception as err:
            _LOGGER.debug("Panel probe failed: %s", err)
            return False

    async def async_start(
        self,
        auth: "AuthManager",
        on_state_update: StateUpdateCallback,
    ) -> None:
        self._auth = auth  # stored but not used for C315 auth
        self._state = TransportState.READY

    async def async_stop(self) -> None:
        self._token = None
        self._token_expiry = 0.0
        self._state = TransportState.DISABLED

    # ------------------------------------------------------------------
    # POLL
    # ------------------------------------------------------------------

    async def async_fetch_state(self) -> dict | None:
        """Fetch /api/state + /api/widgets and return a coordinator-compatible dict."""
        try:
            token = await self._ensure_token()
            session = self._session()
            headers = {"Authorization": f"JWT {token}"}

            # Fetch state and widgets concurrently
            async with session.get(
                f"{self._base}/state",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            ) as resp_state:
                if resp_state.status in (401, 403):
                    await self._handle_auth_error()
                    self._state = TransportState.DEGRADED
                    return None
                if resp_state.status != 200:
                    _LOGGER.debug("Panel /api/state HTTP %s", resp_state.status)
                    self._state = TransportState.DEGRADED
                    return None
                raw_state: dict = await resp_state.json()

            async with session.get(
                f"{self._base}/widgets",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            ) as resp_widgets:
                if resp_widgets.status != 200:
                    # Widgets are non-critical (only provides names); continue
                    _LOGGER.debug(
                        "Panel /api/widgets HTTP %s, names will be empty",
                        resp_widgets.status,
                    )
                    raw_widgets: list[dict] = []
                else:
                    raw_widgets = await resp_widgets.json()

            # /api/state carries no per-zone open flag: the panel only exposes
            # the list of currently open zones on /api/zones. Without this a
            # fallback poll would report every zone closed.
            async with session.get(
                f"{self._base}/zones",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            ) as resp_zones:
                if resp_zones.status != 200:
                    _LOGGER.debug(
                        "Panel /api/zones HTTP %s, open state unknown",
                        resp_zones.status,
                    )
                    raw_open: list = []
                else:
                    raw_open = await resp_zones.json()

            self._state = TransportState.READY
            return self._translate(raw_state, raw_widgets, raw_open)

        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            _LOGGER.debug("Panel fetch error: %s", err)
            self._state = TransportState.DEGRADED
            return None

    # ------------------------------------------------------------------
    # COMMAND
    # ------------------------------------------------------------------

    async def async_send_command(
        self,
        endpoint_id: str,
        cmd: str | None,
        code: str | None = None,
    ) -> CommandResult:
        parsed = _parse_cv_endpoint_id(endpoint_id)
        if parsed is None:
            return CommandResult(ok=False, error=f"unrecognised endpoint_id: {endpoint_id}")

        kind, index = parsed

        if kind == "area":
            return await self._cmd_area(index, cmd)
        if kind == "out":
            return await self._cmd_output(index, cmd)
        if kind == "scene":
            return await self._cmd_scene(index)
        # zones and shutters are read-only / not commandable via this integration
        return CommandResult(ok=False, error=f"unsupported kind: {kind}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _session(self) -> aiohttp.ClientSession:
        # Plain HTTP; verify_ssl is a no-op for http:// but kept for parity.
        return async_get_clientsession(self._hass, verify_ssl=False)

    async def _ensure_token(self) -> str:
        """Return a valid JWT, logging in when needed."""
        async with self._login_lock:
            now = time.time()
            if self._token and now < self._token_expiry - _TOKEN_REFRESH_MARGIN:
                return self._token
            await self._do_login()
            return self._token  # type: ignore[return-value]

    async def _do_login(self) -> None:
        """POST /api/login with form-encoded PIN.  Raises on failure."""
        session = self._session()
        try:
            async with session.post(
                f"{self._base}/login",
                data=f"pin={self._pin}",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=aiohttp.ClientTimeout(total=LOGIN_TIMEOUT),
                ssl=False,
            ) as resp:
                if resp.status != 200:
                    raise aiohttp.ClientResponseError(
                        resp.request_info,
                        resp.history,
                        status=resp.status,
                        message=f"Panel login failed: HTTP {resp.status}",
                    )
                body: dict = await resp.json()
                raw = body.get("token", "")
                if not raw:
                    raise ValueError("Panel login: empty token in response")
                self._token = raw
                self._token_expiry = _parse_jwt_exp(raw)
                _LOGGER.debug("Panel login OK, token expires ~%.0f", self._token_expiry)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as err:
            self._token = None
            self._token_expiry = 0.0
            raise

    async def _handle_auth_error(self) -> None:
        """Force token invalidation so next call will re-login."""
        async with self._login_lock:
            self._token = None
            self._token_expiry = 0.0

    async def _post_command(self, action: str, params: dict[str, str | int]) -> CommandResult:
        """POST /api/command/{action} with form params.  Handles 503 rate-limit."""
        for attempt in range(COMMAND_RATE_RETRIES):
            try:
                token = await self._ensure_token()
                session = self._session()
                form_body = "&".join(f"{k}={v}" for k, v in params.items())
                async with session.post(
                    f"{self._base}/command/{action}",
                    data=form_body,
                    headers={
                        "Authorization": f"JWT {token}",
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                    ssl=False,
                ) as resp:
                    if resp.status == 200:
                        body: dict = await resp.json()
                        if body.get("ok"):
                            return CommandResult(ok=True, raw_response=body)
                        return CommandResult(ok=False, error=str(body), raw_response=body)

                    if resp.status in (401, 403):
                        await self._handle_auth_error()
                        if attempt == 0:
                            # One re-login retry
                            continue
                        return CommandResult(ok=False, error=f"auth_{resp.status}")

                    if resp.status == 503:
                        # Bridge is rate-limiting; back off and retry
                        _LOGGER.debug(
                            "Panel rate-limited on %s (attempt %d/%d)",
                            action, attempt + 1, COMMAND_RATE_RETRIES,
                        )
                        await asyncio.sleep(COMMAND_RATE_DELAY)
                        continue

                    return CommandResult(ok=False, error=f"http_{resp.status}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                return CommandResult(ok=False, error=f"network: {err}")

        return CommandResult(ok=False, error="rate_limited_after_retries")

    async def _cmd_area(self, index: int, cmd: str | None) -> CommandResult:
        action = _AREA_CMD_MAP.get(cmd or "", "")
        if not action:
            return CommandResult(ok=False, error=f"unknown area cmd: {cmd!r}")
        return await self._post_command(action, {"area": index})

    async def _cmd_output(self, index: int, cmd: str | None) -> CommandResult:
        """Toggle output.  Reads current state first to avoid redundant toggles."""
        if cmd in (CMD_OUTPUT_ON, CMD_OUTPUT_OFF):
            # Read current state to decide whether the toggle is actually needed
            try:
                token = await self._ensure_token()
                session = self._session()
                async with session.get(
                    f"{self._base}/state",
                    headers={"Authorization": f"JWT {token}"},
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                    ssl=False,
                ) as resp:
                    if resp.status == 200:
                        current: dict = await resp.json()
                        outputs: list[dict] = current.get("outputs", [])
                        for out in outputs:
                            if out.get("index") == index:
                                currently_on: bool = bool(out.get("on", False))
                                want_on = (cmd == CMD_OUTPUT_ON)
                                if currently_on == want_on:
                                    _LOGGER.debug(
                                        "Output %d already in desired state (%s), skip toggle",
                                        index, "on" if currently_on else "off",
                                    )
                                    # Return ok=True: no-op is still success
                                    return CommandResult(ok=True, raw_response={"skipped": True})
                                break
            except Exception as err:
                _LOGGER.debug(
                    "Could not pre-read output state for index %d: %s — proceeding with toggle",
                    index, err,
                )
            # State read failed or index not found — fall through to toggle
        return await self._post_command("output", {"index": index})

    async def _cmd_scene(self, index: int) -> CommandResult:
        return await self._post_command("scene", {"index": index})

    # ------------------------------------------------------------------
    # Translation: C315 → coordinator-compatible dict
    # ------------------------------------------------------------------

    def _translate(
        self,
        raw_state: dict,
        raw_widgets: list[dict],
        raw_open: list | None = None,
    ) -> dict:
        """Map C315 /api/state + /api/widgets to the coordinator _parse() format.

        The coordinator's _parse() reads:
          raw.get("zone", [])    → keyed by "endpointId"
          raw.get("aree", [])    → keyed by "endpointId"
          raw.get("uscite", [])  → keyed by "endpointId"
          raw.get("scenari", []) → keyed by "endpointId"
          raw.get("centrale")    → panel_info["centrale"]
          raw.get("release")     → panel_info["release"]

        We build name lookup dicts from widgets, tolerate missing fields,
        and synthesise stable endpointIds.
        """
        # Build name lookup: (type, index) → name
        name_map: dict[tuple[int, int], str] = {}
        # Outputs are spread over several widget types (doors, gates, generic
        # outputs), so they get a flat index → name map of their own.
        output_names: dict[int, str] = {}
        for w in raw_widgets:
            wtype = w.get("type")
            widx = w.get("index")
            wname = w.get("name", "")
            if wtype is None or widx is None:
                continue
            name_map[(int(wtype), int(widx))] = wname
            if int(wtype) in _TYPES_OUTPUT:
                output_names[int(widx)] = wname

        # Indexes of zones the panel currently reports as open. Accepts both
        # a bare index list and a list of objects, the two shapes seen.
        open_zones: set[int] = set()
        for item in (raw_open or []):
            if isinstance(item, dict):
                idx = item.get("index", item.get("zone"))
            else:
                idx = item
            try:
                open_zones.add(int(idx))
            except (TypeError, ValueError):
                continue

        pid = self._panel_id

        # --- Areas ---
        aree = []
        for a in raw_state.get("areas", []):
            idx = a.get("index", 0)
            nome = name_map.get((_TYPE_AREA, idx), f"Area {idx + 1}")
            aree.append({
                "endpointId": _cv_endpoint_id(pid, "area", idx),
                "indice": idx,
                "nome": nome,
                "stato": int(a.get("armed", 0)),
                "visibile": True,
                # optional fields that entities tolerate as missing
                "statoSessione": None,
                "zoneBmask": None,
            })

        # --- Zones ---
        zone = []
        for z in raw_state.get("zones", []):
            idx = z.get("index", 0)
            # Zones have no dedicated widget type in the spec; use fallback name
            nome = name_map.get((0, idx), f"Zone {idx + 1}")
            zone.append({
                "endpointId": _cv_endpoint_id(pid, "zone", idx),
                "indice": idx,
                "nome": nome,
                "aperta": idx in open_zones,
                "esclusa": bool(z.get("bypass", False)),
                "visibile": True,
            })

        # --- Outputs ---
        uscite = []
        for o in raw_state.get("outputs", []):
            idx = o.get("index", 0)
            nome = output_names.get(idx, f"Output {idx + 1}")
            uscite.append({
                "endpointId": _cv_endpoint_id(pid, "out", idx),
                "indice": idx,
                "nome": nome,
                "aperta": bool(o.get("on", False)),
                "visibile": True,
            })

        # --- Scenes ---
        scenari = []
        for s in raw_state.get("scenes", []):
            idx = s.get("index", 0)
            # Scenes carry their name inline; widgets only list the subset
            # pinned to the panel home screen.
            nome = (s.get("name")
                    or name_map.get((_TYPE_SCENE, idx), f"Scene {idx + 1}"))
            scenari.append({
                "endpointId": _cv_endpoint_id(pid, "scene", idx),
                "indice": idx,
                "nome": nome,
                "visibile": True,
            })

        return {
            "centrale": raw_state.get("centraleVer", self._panel_id),
            "release": raw_state.get("centraleVer", ""),
            "aree": aree,
            "zone": zone,
            "uscite": uscite,
            "scenari": scenari,
            # pass-through extras (tolerated by _parse as unknown keys)
            "busProblem": raw_state.get("busProblem", False),
            "faultsPresent": raw_state.get("faultsPresent", False),
            "gsmEnabled": raw_state.get("gsmEnabled", False),
            "gsmSignal": raw_state.get("gsmSignal", 0),
        }

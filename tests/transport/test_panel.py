"""Tests for PanelTransport (ContactVideo C315 REST bridge).

All network calls are mocked with aioresponses.  No real host is contacted.
Dummy credentials: pin "000000", host "panel.local", panel_id "test-panel-id".
"""
from __future__ import annotations

import pytest
from aioresponses import aioresponses

from custom_components.elmax_local.const import (
    CMD_AREA_ARM_P1,
    CMD_AREA_ARM_P1P2,
    CMD_AREA_ARM_P2,
    CMD_AREA_ARM_TOTAL,
    CMD_AREA_DISARM,
    CMD_OUTPUT_OFF,
    CMD_OUTPUT_ON,
    CMD_OUTPUT_TOGGLE,
)
from custom_components.elmax_local.transport import TransportCapability, TransportState
from custom_components.elmax_local.transport.panel import (
    PanelTransport,
    _cv_endpoint_id,
    _parse_cv_endpoint_id,
)

# ---------------------------------------------------------------------------
# Dummy test values — no real secrets
# ---------------------------------------------------------------------------
_HOST = "panel.local"
_PIN = "000000"
_PANEL_ID = "test-panel-id"
_BASE = f"http://{_HOST}:8083/api"
_LOGIN_URL = f"{_BASE}/login"
_STATE_URL = f"{_BASE}/state"
_WIDGETS_URL = f"{_BASE}/widgets"
_ZONES_URL = f"{_BASE}/zones"

# A JWT with exp = 9999999999 (year 2286 — will never expire in tests)
_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"

_LOGIN_RESP = {"token": _JWT}

_RAW_STATE = {
    "areas": [{"index": 0, "armed": 0, "open": False, "alarm": False}],
    "zones": [{"index": 0, "open": False, "alarm": False, "bypass": False, "tamper": False}],
    "outputs": [{"index": 0, "on": False}],
    "scenes": [{"index": 0}],
    "shutters": [],
    "groups": [],
    "busProblem": False,
    "faultsPresent": False,
    "gsmEnabled": False,
    "gsmSignal": 0,
    "centraleVer": "1.2.3",
}

_RAW_WIDGETS = [
    {"name": "Interno", "type": 1, "index": 0, "icon": "ic_casa"},
    {"name": "ZONA 01", "type": 0, "index": 0, "icon": ""},
    {"name": "USCITA 1", "type": 4, "index": 0, "icon": ""},
    {"name": "SCENARIO 1", "type": 12, "index": 0, "icon": ""},
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def panel_transport(hass):
    return PanelTransport(hass, _PANEL_ID, _HOST, _PIN)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def test_cv_endpoint_id_format():
    # The panel's own vocabulary, shared with the official transport.
    assert _cv_endpoint_id("pid", "area", 2) == "pid-area-2"
    assert _cv_endpoint_id("pid", "zone", 0) == "pid-zona-0"
    assert _cv_endpoint_id("pid", "out", 5) == "pid-uscita-5"
    assert _cv_endpoint_id("pid", "scene", 1) == "pid-scenario-1"


def test_parse_cv_endpoint_id_roundtrip():
    for kind in ("area", "zone", "out", "scene"):
        eid = _cv_endpoint_id("test-panel-id", kind, 3)
        result = _parse_cv_endpoint_id(eid)
        assert result == (kind, 3), f"Failed for kind={kind}"


def test_parse_cv_endpoint_id_unknown():
    assert _parse_cv_endpoint_id("garbage") is None
    assert _parse_cv_endpoint_id("pid-area-notanumber") is None


# ---------------------------------------------------------------------------
# Capabilities
# ---------------------------------------------------------------------------

def test_capabilities(panel_transport):
    assert TransportCapability.POLL in panel_transport.capabilities
    assert TransportCapability.COMMAND in panel_transport.capabilities
    assert TransportCapability.PUSH not in panel_transport.capabilities


# ---------------------------------------------------------------------------
# Probe
# ---------------------------------------------------------------------------

async def test_probe_ok(panel_transport):
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        result = await panel_transport.async_probe()
    assert result is True


async def test_probe_fails_on_login_error(panel_transport):
    with aioresponses() as m:
        m.post(_LOGIN_URL, status=401, payload={"error": "unauthorized"})
        result = await panel_transport.async_probe()
    assert result is False


async def test_probe_fails_on_network_error(panel_transport):
    import aiohttp
    with aioresponses() as m:
        m.post(_LOGIN_URL, exception=aiohttp.ClientConnectionError("refused"))
        result = await panel_transport.async_probe()
    assert result is False


# ---------------------------------------------------------------------------
# Start / Stop lifecycle
# ---------------------------------------------------------------------------

async def test_start_sets_ready(panel_transport):
    assert panel_transport.state == TransportState.DISABLED
    await panel_transport.async_start(None, None)
    assert panel_transport.state == TransportState.READY


async def test_stop_clears_token(panel_transport):
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        await panel_transport.async_probe()  # acquires token
    assert panel_transport._token is not None

    await panel_transport.async_stop()
    assert panel_transport._token is None
    assert panel_transport.state == TransportState.DISABLED


# ---------------------------------------------------------------------------
# async_fetch_state — translation
# ---------------------------------------------------------------------------

async def test_fetch_state_ok(panel_transport):
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.get(_STATE_URL, payload=_RAW_STATE)
        m.get(_WIDGETS_URL, payload=_RAW_WIDGETS)
        m.get(_ZONES_URL, payload=[])
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_fetch_state()

    assert result is not None
    assert panel_transport.state == TransportState.READY

    # Areas translated correctly
    aree = result.get("aree", [])
    assert len(aree) == 1
    a = aree[0]
    assert a["endpointId"] == f"{_PANEL_ID}-area-0"
    assert a["stato"] == 0        # armed=0 → disarmed
    assert a["nome"] == "Interno" # from widgets type=1 index=0
    assert a["visibile"] is True
    assert a["indice"] == 0

    # Zones translated
    zone = result.get("zone", [])
    assert len(zone) == 1
    z = zone[0]
    assert z["endpointId"] == f"{_PANEL_ID}-zona-0"
    assert z["aperta"] is False
    assert z["esclusa"] is False
    assert z["visibile"] is True

    # Outputs translated
    uscite = result.get("uscite", [])
    assert len(uscite) == 1
    o = uscite[0]
    assert o["endpointId"] == f"{_PANEL_ID}-uscita-0"
    assert o["aperta"] is False   # on=False
    assert o["nome"] == "USCITA 1"

    # Scenes translated
    scenari = result.get("scenari", [])
    assert len(scenari) == 1
    s = scenari[0]
    assert s["endpointId"] == f"{_PANEL_ID}-scenario-0"
    assert s["nome"] == "SCENARIO 1"


async def test_fetch_state_armed_values(panel_transport):
    """Verify all 'armed' codes are passed through correctly."""
    for armed_val in range(5):
        state = {**_RAW_STATE, "areas": [{"index": 0, "armed": armed_val, "open": False, "alarm": False}]}
        with aioresponses() as m:
            m.post(_LOGIN_URL, payload=_LOGIN_RESP)
            m.get(_STATE_URL, payload=state)
            m.get(_WIDGETS_URL, payload=[])
            m.get(_ZONES_URL, payload=[])
            panel_transport._token = None  # force re-login each time
            panel_transport._token_expiry = 0.0
            await panel_transport.async_start(None, None)
            result = await panel_transport.async_fetch_state()
        assert result["aree"][0]["stato"] == armed_val


async def test_fetch_state_degrades_on_state_error(panel_transport):
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.get(_STATE_URL, status=500)
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_fetch_state()

    assert result is None
    assert panel_transport.state == TransportState.DEGRADED


async def test_fetch_state_degrades_on_401(panel_transport):
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.get(_STATE_URL, status=401)
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_fetch_state()

    assert result is None
    assert panel_transport.state == TransportState.DEGRADED
    # Token must be invalidated so next call re-logs in
    assert panel_transport._token is None


async def test_fetch_state_widgets_failure_tolerated(panel_transport):
    """Widgets 404 is tolerated; state is still returned with fallback names."""
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.get(_STATE_URL, payload=_RAW_STATE)
        m.get(_WIDGETS_URL, status=404)
        m.get(_ZONES_URL, payload=[])
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_fetch_state()

    assert result is not None
    # Name falls back to generated name when no widget entry exists
    assert "Area" in result["aree"][0]["nome"]


# ---------------------------------------------------------------------------
# async_send_command — area
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ha_cmd,expected_action", [
    (CMD_AREA_DISARM, "disarm"),
    (CMD_AREA_ARM_P1, "armp1"),
    (CMD_AREA_ARM_P2, "armp2"),
    (CMD_AREA_ARM_P1P2, "arm"),   # no native P1+P2; uses "arm"
    (CMD_AREA_ARM_TOTAL, "arm"),
])
async def test_area_command_mapping(panel_transport, ha_cmd, expected_action):
    eid = _cv_endpoint_id(_PANEL_ID, "area", 0)
    cmd_url = f"{_BASE}/command/{expected_action}"
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.post(cmd_url, payload={"ok": True, "cmd": "AA"})
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, ha_cmd)

    assert result.ok is True


async def test_area_command_unknown_code(panel_transport):
    eid = _cv_endpoint_id(_PANEL_ID, "area", 0)
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, "99")

    assert result.ok is False
    assert "unknown area cmd" in (result.error or "")


# ---------------------------------------------------------------------------
# async_send_command — output (toggle logic)
# ---------------------------------------------------------------------------

async def test_output_on_skips_toggle_when_already_on(panel_transport):
    """CMD_OUTPUT_ON must NOT toggle when output is already on."""
    eid = _cv_endpoint_id(_PANEL_ID, "out", 0)
    state_with_on = {**_RAW_STATE, "outputs": [{"index": 0, "on": True}]}

    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        # Pre-read state for toggle check
        m.get(_STATE_URL, payload=state_with_on)
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, CMD_OUTPUT_ON)

    # Must succeed without hitting the toggle endpoint
    assert result.ok is True
    assert result.raw_response == {"skipped": True}


async def test_output_off_skips_toggle_when_already_off(panel_transport):
    """CMD_OUTPUT_OFF must NOT toggle when output is already off."""
    eid = _cv_endpoint_id(_PANEL_ID, "out", 0)
    state_with_off = {**_RAW_STATE, "outputs": [{"index": 0, "on": False}]}

    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.get(_STATE_URL, payload=state_with_off)
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, CMD_OUTPUT_OFF)

    assert result.ok is True
    assert result.raw_response == {"skipped": True}


async def test_output_on_sends_toggle_when_currently_off(panel_transport):
    """CMD_OUTPUT_ON must send the toggle when output is currently off."""
    eid = _cv_endpoint_id(_PANEL_ID, "out", 0)
    state_with_off = {**_RAW_STATE, "outputs": [{"index": 0, "on": False}]}

    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.get(_STATE_URL, payload=state_with_off)       # pre-read
        m.post(f"{_BASE}/command/output", payload={"ok": True, "cmd": "BB"})
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, CMD_OUTPUT_ON)

    assert result.ok is True
    assert result.raw_response != {"skipped": True}


async def test_output_off_sends_toggle_when_currently_on(panel_transport):
    """CMD_OUTPUT_OFF must send the toggle when output is currently on."""
    eid = _cv_endpoint_id(_PANEL_ID, "out", 0)
    state_with_on = {**_RAW_STATE, "outputs": [{"index": 0, "on": True}]}

    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.get(_STATE_URL, payload=state_with_on)        # pre-read
        m.post(f"{_BASE}/command/output", payload={"ok": True, "cmd": "BB"})
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, CMD_OUTPUT_OFF)

    assert result.ok is True


async def test_output_toggle_sends_without_preread(panel_transport):
    """CMD_OUTPUT_TOGGLE bypasses the pre-read (raw toggle)."""
    eid = _cv_endpoint_id(_PANEL_ID, "out", 0)

    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.post(f"{_BASE}/command/output", payload={"ok": True, "cmd": "CC"})
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, CMD_OUTPUT_TOGGLE)

    assert result.ok is True


# ---------------------------------------------------------------------------
# async_send_command — scene
# ---------------------------------------------------------------------------

async def test_scene_command(panel_transport):
    eid = _cv_endpoint_id(_PANEL_ID, "scene", 2)
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.post(f"{_BASE}/command/scene", payload={"ok": True, "cmd": "DD"})
        await panel_transport.async_start(None, None)
        # Scenes are triggered with cmd=None
        result = await panel_transport.async_send_command(eid, None)

    assert result.ok is True


# ---------------------------------------------------------------------------
# Error / 401 re-login
# ---------------------------------------------------------------------------

async def test_command_401_retries_once_with_relogin(panel_transport):
    eid = _cv_endpoint_id(_PANEL_ID, "area", 0)
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)   # initial login
        m.post(f"{_BASE}/command/disarm", status=401, payload={})  # first attempt fails
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)   # re-login
        m.post(f"{_BASE}/command/disarm", payload={"ok": True, "cmd": "EE"})  # retry
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, CMD_AREA_DISARM)

    assert result.ok is True


async def test_command_401_fails_after_relogin(panel_transport):
    """If re-login also 401s, give up."""
    eid = _cv_endpoint_id(_PANEL_ID, "area", 0)
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)          # initial login
        m.post(f"{_BASE}/command/disarm", status=401, payload={})  # first attempt
        m.post(_LOGIN_URL, status=401, payload={})       # re-login fails
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, CMD_AREA_DISARM)

    assert result.ok is False


# ---------------------------------------------------------------------------
# 503 rate-limit retry
# ---------------------------------------------------------------------------

async def test_command_503_retries_and_succeeds(panel_transport):
    eid = _cv_endpoint_id(_PANEL_ID, "area", 0)
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        m.post(f"{_BASE}/command/arm", status=503, payload={"err": "rate limited"})
        m.post(f"{_BASE}/command/arm", payload={"ok": True, "cmd": "FF"})
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, CMD_AREA_ARM_TOTAL)

    assert result.ok is True


async def test_command_503_exhausts_retries(panel_transport):
    eid = _cv_endpoint_id(_PANEL_ID, "area", 0)
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        # 3 consecutive 503s
        for _ in range(3):
            m.post(f"{_BASE}/command/arm", status=503, payload={"err": "rate limited"})
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command(eid, CMD_AREA_ARM_TOTAL)

    assert result.ok is False
    assert "rate_limited" in (result.error or "")


# ---------------------------------------------------------------------------
# Unrecognised endpoint_id
# ---------------------------------------------------------------------------

async def test_command_unknown_endpoint_id(panel_transport):
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)
        await panel_transport.async_start(None, None)
        result = await panel_transport.async_send_command("totally-unknown-id", "4")

    assert result.ok is False
    assert "unrecognised" in (result.error or "")


# ---------------------------------------------------------------------------
# Token caching — second call must NOT re-login
# ---------------------------------------------------------------------------

async def test_token_cached_across_calls(panel_transport):
    with aioresponses() as m:
        m.post(_LOGIN_URL, payload=_LOGIN_RESP)       # only ONE login expected
        m.get(_STATE_URL, payload=_RAW_STATE)
        m.get(_WIDGETS_URL, payload=_RAW_WIDGETS)
        m.get(_ZONES_URL, payload=[])
        m.get(_STATE_URL, payload=_RAW_STATE)
        m.get(_WIDGETS_URL, payload=_RAW_WIDGETS)
        m.get(_ZONES_URL, payload=[])
        await panel_transport.async_start(None, None)
        r1 = await panel_transport.async_fetch_state()
        r2 = await panel_transport.async_fetch_state()

    assert r1 is not None
    assert r2 is not None
    # If a second login had been attempted aioresponses would raise
    # ConnectionError (no more registered mocks) — the test passing proves
    # the token was reused.


# --- Schema of the real panel (captured from 192.168.1.53:8083, 2026-07-28) ---

_REAL_STATE = {
    "areas": [
        {"index": 0, "armed": 0, "open": False, "alarm": False,
         "memAlarm": False, "tEntry": 0, "tExit": 0},
        {"index": 1, "armed": 4, "open": False, "alarm": False,
         "memAlarm": False, "tEntry": 0, "tExit": 0},
    ],
    # NOTE: zones carry no "open" flag on this firmware.
    "zones": [
        {"index": 0, "alarm": False, "bypass": False, "memAlarm": False,
         "areaId": -1},
        {"index": 1, "alarm": False, "bypass": True, "memAlarm": False,
         "areaId": 4},
    ],
    "outputs": [{"index": 7, "on": True, "stato": 1},
                {"index": 11, "on": False, "stato": 0}],
    "scenes": [{"index": 0, "name": "In Casa"}, {"index": 9, "name": "Fuori"}],
    "shutters": [{"index": 0, "position": -1, "stato": 0}],
    "groups": [{"index": 0, "name": "GRUPPOUSC01"}],
    "centraleVer": "Versione Centrale: N/D ",
    "busProblem": False, "faultsPresent": False,
    "gsmEnabled": True, "gsmSignal": 2,
}

# Real widgets: areas are type 1, scenes type 12, outputs spread over 3 and 6.
_REAL_WIDGETS = [
    {"type": 1, "index": 0, "name": "Interno", "icon": ""},
    {"type": 1, "index": 1, "name": "Porte", "icon": ""},
    {"type": 3, "index": 7, "name": "Serranda", "icon": ""},
    {"type": 6, "index": 11, "name": "r20 high", "icon": ""},
    {"type": 12, "index": 0, "name": "In Casa", "icon": ""},
]


def _translate_real(hass, raw_open=None):
    t = PanelTransport(hass, "abc", "192.168.1.53", "000000")
    return t._translate(_REAL_STATE, _REAL_WIDGETS, raw_open)


async def test_real_schema_names_outputs_from_their_widget_types(hass):
    """Outputs live under widget types 3/6, not a single output type."""
    out = {o["indice"]: o["nome"] for o in _translate_real(hass)["uscite"]}
    assert out == {7: "Serranda", 11: "r20 high"}


async def test_real_schema_marks_open_zones_from_zones_endpoint(hass):
    """Open state comes from /api/zones; /api/state alone would say all closed."""
    zones = {z["indice"]: z["aperta"] for z in _translate_real(hass, [1])["zone"]}
    assert zones == {0: False, 1: True}


async def test_real_schema_accepts_object_shaped_open_zones(hass):
    """The open list may arrive as objects rather than bare indexes."""
    zones = {z["indice"]: z["aperta"]
             for z in _translate_real(hass, [{"index": 0}])["zone"]}
    assert zones[0] is True


async def test_real_schema_ignores_malformed_open_entries(hass):
    """A junk entry must not blow up the whole poll."""
    zones = {z["indice"]: z["aperta"]
             for z in _translate_real(hass, [None, "x", 1])["zone"]}
    assert zones == {0: False, 1: True}


async def test_real_schema_uses_inline_scene_names(hass):
    """Scenes name themselves; widgets only pin a subset to the home screen."""
    scenes = {s["indice"]: s["nome"] for s in _translate_real(hass)["scenari"]}
    assert scenes == {0: "In Casa", 9: "Fuori"}


async def test_real_schema_keeps_bypass_and_armed(hass):
    """Fields that do exist on this firmware still map through."""
    result = _translate_real(hass)
    assert [z["esclusa"] for z in result["zone"]] == [False, True]
    assert [a["stato"] for a in result["aree"]] == [0, 4]


# --- Endpoint identity shared with the official transport ---

def test_endpoint_ids_match_the_official_scheme():
    """Ids must be the panel's own, so entities survive a transport switch.

    Captured from /api/v2/discovery on the live panel: areas are
    "{panel_id}-area-{i}", zones "-zona-", outputs "-uscita-",
    scenarios "-scenario-".
    """
    pid = "18352599c53cd0b93ec"
    assert _cv_endpoint_id(pid, "area", 0) == f"{pid}-area-0"
    assert _cv_endpoint_id(pid, "zone", 1) == f"{pid}-zona-1"
    assert _cv_endpoint_id(pid, "out", 7) == f"{pid}-uscita-7"
    assert _cv_endpoint_id(pid, "scene", 0) == f"{pid}-scenario-0"


def test_endpoint_ids_round_trip():
    """Commands route by parsing the id back, so it must survive the trip."""
    pid = "18352599c53cd0b93ec"
    for kind, index in (("area", 0), ("zone", 12), ("out", 7), ("scene", 9)):
        assert _parse_cv_endpoint_id(_cv_endpoint_id(pid, kind, index)) == (kind, index)


def test_parse_endpoint_id_rejects_junk():
    assert _parse_cv_endpoint_id("nonsense") is None
    assert _parse_cv_endpoint_id("abc-area-notanumber") is None


async def test_translate_emits_official_ids(hass):
    """End to end: a translated poll carries ids the other transports also use."""
    result = _translate_real(hass)
    assert result["aree"][0]["endpointId"] == "abc-area-0"
    assert result["zone"][0]["endpointId"] == "abc-zona-0"
    assert result["uscite"][0]["endpointId"] == "abc-uscita-7"
    assert result["scenari"][0]["endpointId"] == "abc-scenario-0"

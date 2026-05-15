# Elmax Local Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rifattorizzare il custom component `elmax_mqtt` v1.0.0 in `elmax_local` v2.0.0 con architettura push-first multi-trasporto (WS + MQTT + HTTP), migrazione registries-aware, e `Transport` ABC stabile per futuro `BusTransport`.

**Architecture:** Push-first multi-transport. `Transport` ABC con capability (PUSH/POLL/COMMAND) implementato da `HttpTransport`, `MqttTransport`, `WebSocketTransport`. Tutti orchestrati da `TransportRegistry` dentro `ElmaxLocalCoordinator` (estende `DataUpdateCoordinator`). Entità ereditano da `CoordinatorEntity`. Migrazione via service esplicito con backup+rollback.

**Tech Stack:** Python 3.12+, Home Assistant 2026.x, `aiohttp` (HTTP+WS), HA `mqtt` integration, `pytest` + `pytest-homeassistant-custom-component`, `aioresponses` per mock HTTP.

**Spec di riferimento:** `docs/superpowers/specs/2026-05-15-elmax-local-refactor-design.md`

---

## File Structure

```
custom_components/elmax_local/
├── __init__.py              # setup, services, lifecycle
├── manifest.json            # domain=elmax_local, version=2.0.0, zeroconf
├── const.py                 # domain, topics, commands, mappings
├── auth.py                  # AuthManager (JWT, refresh, backoff)
├── coordinator.py           # ElmaxState, ElmaxLocalCoordinator
├── config_flow.py           # user/zeroconf/import + options flow
├── diagnostics.py           # diagnostic dump
├── services.yaml            # migrate_from_legacy, rollback_migration
├── strings.json
├── translations/
│   └── it.json
├── transport/
│   ├── __init__.py          # Transport ABC, Capability, State, CommandResult, Registry
│   ├── http.py              # HttpTransport
│   ├── mqtt.py              # MqttTransport
│   └── websocket.py         # WebSocketTransport
├── alarm_control_panel.py
├── binary_sensor.py
├── switch.py
└── button.py

tests/
├── __init__.py
├── conftest.py
├── test_auth.py
├── transport/
│   ├── __init__.py
│   ├── test_abc.py
│   ├── test_http.py
│   ├── test_mqtt.py
│   ├── test_websocket.py
│   └── test_registry.py
├── test_coordinator.py
├── test_config_flow.py
├── test_entities.py
└── test_migration.py
```

`custom_components/elmax_mqtt/` resta intoccato fino al merge finale (è il v1 in produzione).

---

## Task overview

| # | Task | Output |
|---|---|---|
| 0 | Test scaffolding | `tests/` + `requirements_test.txt` + `pytest.ini` |
| 1 | Package scaffolding | `elmax_local/` con `manifest.json`, `const.py`, skeleton `__init__.py` |
| 2 | Transport ABC | `transport/__init__.py` con enums + ABC + dataclasses + Registry skeleton |
| 3 | AuthManager | `auth.py` con login, refresh, parse_exp, backoff |
| 4 | HttpTransport | `transport/http.py` con probe/fetch/command + tests |
| 5 | MqttTransport | `transport/mqtt.py` con probe/push handler/command + tests |
| 6 | WebSocketTransport | `transport/websocket.py` con probe/reconnect loop + tests |
| 7 | TransportRegistry routing | completa `transport/__init__.py:Registry` + tests |
| 8 | ElmaxState + Coordinator skeleton | `coordinator.py` base + tests |
| 9 | Coordinator data flow | `_parse`, `_on_push_state_update`, `_async_update_data` + tests |
| 10 | Coordinator commands | `async_send_command` + `_post_command_verify` + tests |
| 11 | alarm_control_panel | entità area + tests |
| 12 | binary_sensor | entità zona + tests |
| 13 | switch | entità uscita + tests |
| 14 | button | entità scenario + tests |
| 15 | `__init__.py` setup wiring | `async_setup_entry` + `async_unload_entry` + platform forwarding |
| 16 | config_flow user step | + tests |
| 17 | config_flow zeroconf | + manifest update + tests |
| 18 | options_flow | + tests |
| 19 | i18n | `strings.json` + `translations/it.json` |
| 20 | diagnostic dump | `diagnostics.py` + tests |
| 21 | Migration backup helper | `migration.py` (helper) + tests |
| 22 | `migrate_from_legacy` service | + tests integration |
| 23 | `rollback_migration` service | + tests |
| 24 | services.yaml + registration | service handlers in `__init__.py` |
| 25 | README + CHANGELOG + version | docs updates |
| 26 | Manual smoke test execution | checklist run on HA reale |
| 27 | Tag v1.0.0 + branch v1-maintenance | git ops manuali |

Total: 28 task. Ognuno produce un singolo commit.

---

## Task 0: Test scaffolding

**Goal:** Setup pytest + pytest-homeassistant-custom-component per TDD.

**Files:**
- Create: `tests/__init__.py`, `tests/conftest.py`, `requirements_test.txt`, `pytest.ini`
- Modify: `.gitignore`

**Acceptance Criteria:**
- [ ] `pip install -r requirements_test.txt` ok
- [ ] `pytest tests/ -v` esegue (exit 5 = no tests collected, ok)
- [ ] `.gitignore` esclude cache

**Verify:** `pytest tests/ -v` → exit 0 o 5

**Steps:**

- [ ] **Step 1: Create `requirements_test.txt`**

```text
pytest==8.3.4
pytest-asyncio==0.24.0
pytest-homeassistant-custom-component==0.13.190
aioresponses==0.7.7
```

- [ ] **Step 2: Create `pytest.ini`**

```ini
[pytest]
asyncio_mode = auto
testpaths = tests
addopts = -v --tb=short
```

- [ ] **Step 3: Create `tests/__init__.py`** (empty file)

- [ ] **Step 4: Create `tests/conftest.py`**

```python
"""Common fixtures for elmax_local tests."""
from __future__ import annotations

import pytest

pytest_plugins = ["pytest_homeassistant_custom_component"]


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations):
    yield


@pytest.fixture
def mock_panel_data():
    return {
        "release": "PHANTOM64PRO_GSM 13.9A.845",
        "tappFeature": True, "sceneFeature": True,
        "zone": [{"endpointId": "abc123-zona-0", "visibile": True, "indice": 0,
                  "aperta": False, "esclusa": False, "nome": "ZONA 01"}],
        "uscite": [{"endpointId": "abc123-uscita-0", "visibile": True, "indice": 0,
                    "aperta": False, "nome": "USCITA 1"}],
        "aree": [{"endpointId": "abc123-area-0", "visibile": True, "indice": 0,
                  "statiDisponibili": [0,1,2,3,4], "statiSessioneDisponibili": [0,1,2,3],
                  "stato": 0, "statoSessione": 1, "zoneBmask": "0100000000000000",
                  "nome": "AREA 1"}],
        "tapparelle": [], "gruppi": [],
        "scenari": [{"endpointId": "abc123-scenario-0", "visibile": True, "indice": 0,
                     "nome": "SCENARIO 1"}],
        "datetime": "18:01:09 25/07/2022",
    }
```

- [ ] **Step 5: Append to `.gitignore`**

```
__pycache__/
.pytest_cache/
htmlcov/
*.pyc
.coverage
```

- [ ] **Step 6: Verify**

Run: `pip install -r requirements_test.txt && pytest tests/ -v`
Expected: exit 5 (no tests collected)

- [ ] **Step 7: Commit**

```bash
git add tests/ requirements_test.txt pytest.ini .gitignore
git commit -m "test: add pytest scaffolding for elmax_local TDD"
```

---

## Task 1: Package scaffolding `elmax_local`

**Goal:** Crea il package vuoto con `manifest.json`, `const.py` e skeleton `__init__.py`.

**Files:**
- Create: `custom_components/elmax_local/__init__.py`, `manifest.json`, `const.py`
- Create: `tests/test_package.py`

**Acceptance Criteria:**
- [ ] `manifest.json` valido (domain, version, codeowners, config_flow, dependencies, zeroconf)
- [ ] `const.py` definisce DOMAIN, PLATFORMS, topic/command constants
- [ ] `__init__.py` ha stub `async_setup_entry`/`async_unload_entry`
- [ ] Test verifica `DOMAIN == "elmax_local"` e manifest valid

**Verify:** `pytest tests/test_package.py -v` → PASS

**Steps:**

- [ ] **Step 1: Write `tests/test_package.py`**

```python
"""Test package scaffolding."""
from __future__ import annotations

import json
from pathlib import Path

from custom_components.elmax_local.const import DOMAIN, PLATFORMS


def test_domain():
    assert DOMAIN == "elmax_local"


def test_platforms():
    assert set(PLATFORMS) == {"alarm_control_panel", "binary_sensor", "switch", "button"}


def test_manifest_valid():
    data = json.loads(Path("custom_components/elmax_local/manifest.json").read_text())
    assert data["domain"] == "elmax_local"
    assert data["version"] == "2.0.0"
    assert data["config_flow"] is True
    assert "mqtt" in data["dependencies"]
    assert data["iot_class"] == "local_push"
```

- [ ] **Step 2: Run test (should FAIL — module missing)**

Run: `pytest tests/test_package.py -v` → `ModuleNotFoundError`

- [ ] **Step 3: Create `custom_components/elmax_local/manifest.json`**

```json
{
  "domain": "elmax_local",
  "name": "Elmax Local",
  "brand": "elmax",
  "codeowners": ["@dconvertini"],
  "config_flow": true,
  "dependencies": ["mqtt"],
  "documentation": "https://github.com/dconvertini/ha-elmax-local",
  "iot_class": "local_push",
  "requirements": [],
  "version": "2.0.0",
  "zeroconf": ["_elmax-ssl._tcp.local."]
}
```

- [ ] **Step 4: Create `custom_components/elmax_local/const.py`**

```python
"""Constants for Elmax Local integration."""
from __future__ import annotations

from typing import Final

DOMAIN: Final = "elmax_local"
LEGACY_DOMAIN: Final = "elmax_mqtt"

PLATFORMS: Final = ["alarm_control_panel", "binary_sensor", "switch", "button"]

CONF_PANEL_ID: Final = "panel_id"
CONF_PANEL_PIN: Final = "panel_pin"
CONF_PANEL_HOST: Final = "panel_host"
CONF_ENABLE_WS: Final = "enable_ws"
CONF_ENABLE_MQTT: Final = "enable_mqtt"
CONF_RECONCILE_INTERVAL: Final = "reconcile_interval"

DEFAULT_RECONCILE_INTERVAL: Final = 90
MIN_RECONCILE_INTERVAL: Final = 30
MAX_RECONCILE_INTERVAL: Final = 600

TOPIC_REQUEST_ID: Final = "/elmax/request/id"
TOPIC_RESPONSE_ID: Final = "/elmax/response/id"
TOPIC_REQUEST_LOGIN: Final = "/elmax/request/login/{panel_id}"
TOPIC_RESPONSE_LOGIN: Final = "/elmax/response/login/{panel_id}"
TOPIC_REQUEST_REFRESH: Final = "/elmax/request/refresh/{panel_id}"
TOPIC_RESPONSE_REFRESH: Final = "/elmax/response/refresh/{panel_id}"
TOPIC_REQUEST_STATUS: Final = "/elmax/request/status/{panel_id}"
TOPIC_RESPONSE_STATUS: Final = "/elmax/response/status/{panel_id}"
TOPIC_REQUEST_COMMAND: Final = "/elmax/request/command/{panel_id}"
TOPIC_RESPONSE_COMMAND: Final = "/elmax/response/command/{panel_id}"

HTTP_BASE_URL: Final = "https://{host}/api/v2"
WS_BASE_URL: Final = "wss://{host}/api/v2/push"

CMD_AREA_DISARM: Final = "0"
CMD_AREA_ARM_P1: Final = "1"
CMD_AREA_ARM_P2: Final = "2"
CMD_AREA_ARM_P1P2: Final = "3"
CMD_AREA_ARM_TOTAL: Final = "4"

CMD_OUTPUT_TOGGLE: Final = "0"
CMD_OUTPUT_ON: Final = "1"
CMD_OUTPUT_OFF: Final = "2"

ELMAX_TO_HA_STATE: Final = {
    0: "disarmed", 1: "armed_home", 2: "armed_night",
    3: "armed_home", 4: "armed_away",
}

HA_TO_ELMAX_CMD: Final = {
    "disarm": CMD_AREA_DISARM,
    "arm_away": CMD_AREA_ARM_TOTAL,
    "arm_home": CMD_AREA_ARM_P1P2,
    "arm_night": CMD_AREA_ARM_P2,
}

SERVICE_MIGRATE: Final = "migrate_from_legacy"
SERVICE_ROLLBACK: Final = "rollback_migration"
```

- [ ] **Step 5: Create `custom_components/elmax_local/__init__.py` skeleton**

```python
"""Elmax Local — multi-transport integration for Elmax alarm panels."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Elmax Local from a config entry. STUB — implemented in Task 15."""
    hass.data.setdefault(DOMAIN, {})
    _LOGGER.debug("async_setup_entry stub for %s", entry.entry_id)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry. STUB."""
    return True
```

- [ ] **Step 6: Run tests, verify PASS**

Run: `pytest tests/test_package.py -v` → 3 passed

- [ ] **Step 7: Commit**

```bash
git add custom_components/elmax_local/ tests/test_package.py
git commit -m "feat(elmax_local): package scaffolding (manifest, const, stub __init__)"
```

---

## Task 2: Transport ABC + enums + Registry skeleton

**Goal:** Definisci il contratto `Transport` ABC come da spec sez. 5.

**Files:**
- Create: `custom_components/elmax_local/transport/__init__.py`
- Create: `tests/transport/__init__.py` (empty), `tests/transport/test_abc.py`

**Acceptance Criteria:**
- [ ] `Transport` ABC con metodi astratti `async_probe`, `async_start`, `async_stop`, `state`
- [ ] `async_fetch_state` e `async_send_command` raise `NotImplementedError` di default
- [ ] Enum `TransportCapability` {PUSH, POLL, COMMAND}, `TransportState` {DISABLED, PROBING, READY, DEGRADED, UNSUPPORTED}
- [ ] `CommandResult` dataclass frozen (`ok`, `error`, `raw_response`)
- [ ] `TransportRegistry` skeleton (routing in Task 7)

**Verify:** `pytest tests/transport/test_abc.py -v` → PASS

**Steps:**

- [ ] **Step 1: Write `tests/transport/test_abc.py`**

```python
"""Test Transport ABC contract."""
from __future__ import annotations

import pytest

from custom_components.elmax_local.transport import (
    CommandResult, Transport, TransportCapability, TransportState,
)


def test_capability_enum_values():
    assert TransportCapability.PUSH.value == "push"
    assert TransportCapability.POLL.value == "poll"
    assert TransportCapability.COMMAND.value == "command"


def test_state_enum_values():
    assert {s.value for s in TransportState} == {
        "disabled", "probing", "ready", "degraded", "unsupported"
    }


def test_command_result_frozen():
    r = CommandResult(ok=True)
    with pytest.raises(Exception):
        r.ok = False  # type: ignore


def test_command_result_defaults():
    r = CommandResult(ok=True)
    assert r.error is None and r.raw_response is None


def test_transport_abc_not_instantiable():
    with pytest.raises(TypeError):
        Transport()  # type: ignore


class _MinimalTransport(Transport):
    name = "minimal"
    capabilities = frozenset()

    @property
    def state(self):
        return TransportState.DISABLED

    async def async_probe(self) -> bool:
        return False

    async def async_start(self, auth, on_state_update) -> None:
        return None

    async def async_stop(self) -> None:
        return None


def test_minimal_subclass_instantiable():
    t = _MinimalTransport()
    assert t.name == "minimal"


async def test_fetch_state_default_raises():
    t = _MinimalTransport()
    with pytest.raises(NotImplementedError):
        await t.async_fetch_state()


async def test_send_command_default_raises():
    t = _MinimalTransport()
    with pytest.raises(NotImplementedError):
        await t.async_send_command("eid", "0")
```

- [ ] **Step 2: Run test (should FAIL)**

Run: `pytest tests/transport/test_abc.py -v` → ImportError

- [ ] **Step 3: Create `custom_components/elmax_local/transport/__init__.py`**

```python
"""Transport abstraction for Elmax Local.

Defines the Transport ABC plus enums, dataclasses, and TransportRegistry.
Concrete implementations in transport/http.py, mqtt.py, websocket.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Awaitable, Callable

if TYPE_CHECKING:
    from ..auth import AuthManager

StateUpdateCallback = Callable[[dict], Awaitable[None]]


class TransportCapability(Enum):
    PUSH = "push"
    POLL = "poll"
    COMMAND = "command"


class TransportState(Enum):
    DISABLED = "disabled"
    PROBING = "probing"
    READY = "ready"
    DEGRADED = "degraded"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class CommandResult:
    ok: bool
    error: str | None = None
    raw_response: dict | None = None


class Transport(ABC):
    """Contract for a communication channel with an Elmax panel.

    Agnostic to domain: forwards raw /api/v2/discovery payloads to the
    Coordinator via callback. Does not interpret state.
    """

    name: str
    capabilities: frozenset[TransportCapability]

    @property
    @abstractmethod
    def state(self) -> TransportState: ...

    @abstractmethod
    async def async_probe(self) -> bool:
        """Verify availability on current firmware. Idempotent, no side-effects.
        Suggested internal timeout: 10s."""

    @abstractmethod
    async def async_start(
        self,
        auth: "AuthManager",
        on_state_update: StateUpdateCallback,
    ) -> None:
        """Start the transport. PUSH starts listening; POLL/COMMAND inits client."""

    @abstractmethod
    async def async_stop(self) -> None:
        """Stop. Idempotent. Restartable with async_start."""

    async def async_fetch_state(self) -> dict | None:
        """On-demand fetch (POLL capability). Default raise."""
        raise NotImplementedError(f"{self.name} does not support POLL")

    async def async_send_command(
        self,
        endpoint_id: str,
        cmd: str | None,
        code: str | None = None,
    ) -> CommandResult:
        """Send command (COMMAND capability). Default raise.

        endpoint_id: e.g. "abc-area-0"
        cmd: command string or None (zones toggle)
        code: PIN ONLY for area disarm
        """
        raise NotImplementedError(f"{self.name} does not support COMMAND")


class TransportRegistry:
    """Orchestrates N transports. Routing logic implemented in Task 7."""

    def __init__(self, transports: list[Transport]):
        self._transports = transports

    async def async_start_all(self, auth, on_state_update) -> None:
        raise NotImplementedError("Implemented in Task 7")

    async def async_stop_all(self) -> None:
        raise NotImplementedError("Implemented in Task 7")

    async def async_fetch_state(self) -> dict | None:
        raise NotImplementedError("Implemented in Task 7")

    async def async_send_command(self, eid, cmd, code=None) -> CommandResult:
        raise NotImplementedError("Implemented in Task 7")

    def get_active_push_transports(self) -> list[Transport]:
        raise NotImplementedError("Implemented in Task 7")

    def degraded_or_unsupported(self) -> list[Transport]:
        raise NotImplementedError("Implemented in Task 7")
```

- [ ] **Step 4: Run tests, verify PASS**

Run: `pytest tests/transport/test_abc.py -v` → 8 passed

- [ ] **Step 5: Commit**

```bash
git add custom_components/elmax_local/transport/ tests/transport/
git commit -m "feat(elmax_local): Transport ABC contract (spec sez. 5)

Stable interface for HTTP/MQTT/WS and future BusTransport.
Modifications require new spec."
```

---

## Task 3: AuthManager (JWT login, refresh, exp parse, backoff)

**Goal:** Auth centralizzato condiviso fra trasporti. Parse `exp` claim, refresh proattivo a 50min, backoff su lockout 403.

**Files:**
- Create: `custom_components/elmax_local/auth.py`
- Create: `tests/test_auth.py`

**Acceptance Criteria:**
- [ ] `async_get_token()` ritorna token cached se non scaduto, refresh se entro 10min da exp, login fresh altrimenti
- [ ] `_parse_exp(jwt)` decodifica base64 payload (no verifica firma), estrae `exp`, fallback 3000s
- [ ] Backoff: 30→60→120→...→600s su 401/403, `n*30s` max 600s su 502/503
- [ ] `asyncio.Lock` serializza concorrenza
- [ ] `async_handle_401()` invalida token

**Verify:** `pytest tests/test_auth.py -v` → all PASS (7+ tests)

**Steps:**

- [ ] **Step 1: Write `tests/test_auth.py`**

```python
"""Test AuthManager."""
from __future__ import annotations

import base64
import json
import time

import pytest
from aioresponses import aioresponses

from custom_components.elmax_local.auth import AuthManager, ElmaxAuthError


def _make_jwt(exp_offset: int = 3600) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(
        json.dumps({"exp": int(time.time()) + exp_offset, "sub": "test"}).encode()
    ).rstrip(b"=").decode()
    return f"{header}.{payload}.fakesig"


@pytest.fixture
def auth(hass):
    return AuthManager(hass, "1.2.3.4", "000000")


def test_parse_exp_valid(auth):
    exp = auth._parse_exp(_make_jwt(3600))
    assert abs(exp - (time.time() + 3600)) < 5


def test_parse_exp_malformed(auth):
    exp = auth._parse_exp("not.a.jwt")
    assert abs(exp - (time.time() + 3000)) < 5


def test_parse_exp_missing_claim(auth):
    header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(b'{"sub":"test"}').rstrip(b"=").decode()
    exp = auth._parse_exp(f"{header}.{payload}.sig")
    assert abs(exp - (time.time() + 3000)) < 5


async def test_login_success(auth):
    jwt = _make_jwt(3600)
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        token = await auth.async_get_token()
        assert token == jwt
        assert auth._expiry > time.time() + 3500
    await auth.async_close()


async def test_token_cached(auth):
    jwt = _make_jwt(3600)
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        t1 = await auth.async_get_token()
        t2 = await auth.async_get_token()
        assert t1 == t2
    await auth.async_close()


async def test_refresh_within_margin(auth):
    jwt_old = _make_jwt(3600)
    jwt_new = _make_jwt(3600)
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt_old}"})
        m.post("https://1.2.3.4/api/v2/refresh", payload={"token": f"JWT {jwt_new}"})
        await auth.async_get_token()
        auth._expiry = time.time() + 300  # within REFRESH_MARGIN
        token = await auth.async_get_token()
        assert token == jwt_new
    await auth.async_close()


async def test_handle_401_invalidates(auth):
    jwt = _make_jwt(3600)
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        await auth.async_get_token()
        await auth.async_handle_401()
        assert auth._token is None
        assert auth._expiry == 0
    await auth.async_close()


async def test_login_403_triggers_backoff(auth):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", status=403,
               payload={"message": "Forbidden"})
        with pytest.raises(ElmaxAuthError):
            await auth.async_get_token()
        assert auth._login_fail_count == 1
        assert auth._blocked_until > time.time()
    await auth.async_close()
```

- [ ] **Step 2: Run test (FAIL — module missing)**

Run: `pytest tests/test_auth.py -v` → ImportError

- [ ] **Step 3: Create `custom_components/elmax_local/auth.py`**

```python
"""Authentication manager for Elmax Local.

Shared JWT across transports. TTL 1h documented; refresh proactive at 50min.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import ssl
import time

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from .const import HTTP_BASE_URL

_LOGGER = logging.getLogger(__name__)

REFRESH_MARGIN = 600
DEFAULT_TTL_FALLBACK = 3000

BACKOFF_BASE = 30
BACKOFF_MAX = 600
LOCKOUT_CODES = {401, 403}
PANEL_DOWN_CODES = {502, 503}


class ElmaxAuthError(HomeAssistantError):
    """Raised on auth failure or active backoff."""


class AuthManager:
    """JWT login + refresh + backoff. Shared across transports."""

    def __init__(self, hass: HomeAssistant, host: str, pin: str):
        self._hass = hass
        self._host = host
        self._pin = pin
        self._token: str | None = None
        self._expiry: float = 0
        self._lock = asyncio.Lock()
        self._login_fail_count: int = 0
        self._blocked_until: float = 0
        self._session: aiohttp.ClientSession | None = None
        self._ssl_ctx: ssl.SSLContext | None = None

    @property
    def host(self) -> str:
        return self._host

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._ssl_ctx = await self._hass.async_add_executor_job(
                self._create_ssl_context
            )
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=self._ssl_ctx)
            )
        return self._session

    @staticmethod
    def _create_ssl_context() -> ssl.SSLContext:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    async def async_close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    def _parse_exp(self, jwt: str) -> float:
        try:
            parts = jwt.split(".")
            if len(parts) != 3:
                raise ValueError("not a 3-part JWT")
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            if "exp" not in payload:
                raise ValueError("no exp claim")
            return float(payload["exp"])
        except (ValueError, json.JSONDecodeError, KeyError) as err:
            _LOGGER.debug("JWT parse_exp fallback: %s", err)
            return time.time() + DEFAULT_TTL_FALLBACK

    async def async_get_token(self) -> str:
        async with self._lock:
            now = time.time()
            if now < self._blocked_until:
                remaining = int(self._blocked_until - now)
                raise ElmaxAuthError(f"Auth backoff active, retry in {remaining}s")
            if self._token and now < self._expiry - REFRESH_MARGIN:
                return self._token
            if self._token and now < self._expiry:
                if await self._try_refresh():
                    return self._token  # type: ignore[return-value]
            await self._do_login()
            return self._token  # type: ignore[return-value]

    async def async_handle_401(self) -> None:
        async with self._lock:
            self._token = None
            self._expiry = 0

    async def _try_refresh(self) -> bool:
        session = await self._ensure_session()
        try:
            async with session.post(
                f"{HTTP_BASE_URL.format(host=self._host)}/refresh",
                json={"token": f"JWT {self._token}"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw = data.get("token", "").replace("JWT ", "")
                    if raw:
                        self._token = raw
                        self._expiry = self._parse_exp(raw)
                        self._login_fail_count = 0
                        return True
                return False
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return False

    async def _do_login(self) -> None:
        session = await self._ensure_session()
        try:
            async with session.post(
                f"{HTTP_BASE_URL.format(host=self._host)}/login",
                json={"pin": self._pin},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw = data.get("token", "").replace("JWT ", "")
                    if not raw:
                        raise ElmaxAuthError("empty token")
                    self._token = raw
                    self._expiry = self._parse_exp(raw)
                    self._login_fail_count = 0
                    self._blocked_until = 0
                    return
                await self._apply_backoff(resp.status)
                raise ElmaxAuthError(f"Login failed: HTTP {resp.status}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            await self._apply_backoff(599)
            raise ElmaxAuthError(f"Login network error: {err}") from err

    async def _apply_backoff(self, status: int) -> None:
        self._login_fail_count += 1
        if status in LOCKOUT_CODES:
            backoff = min(BACKOFF_MAX, BACKOFF_BASE * (2 ** (self._login_fail_count - 1)))
        elif status in PANEL_DOWN_CODES or status >= 500:
            backoff = min(BACKOFF_MAX, BACKOFF_BASE * self._login_fail_count)
        else:
            backoff = min(120, BACKOFF_BASE * self._login_fail_count)
        self._blocked_until = time.time() + backoff
        _LOGGER.warning("Auth backoff %ds (status=%d, attempt=%d)",
                        backoff, status, self._login_fail_count)
```

- [ ] **Step 4: Run tests, verify PASS**

Run: `pytest tests/test_auth.py -v` → 7+ passed

- [ ] **Step 5: Commit**

```bash
git add custom_components/elmax_local/auth.py tests/test_auth.py
git commit -m "feat(elmax_local): AuthManager with JWT exp parsing and backoff

Centralized auth shared across transports. Parses real exp claim
instead of hardcoded 50min. Exponential backoff on 401/403 prevents
'Codice Falso Da PcIP' lockout (spec sez. 7)."
```

---

## Task 4: HttpTransport

**Goal:** Trasporto HTTP per polling + comandi. Capability {POLL, COMMAND}. Probe via POST `/login` con PIN.

**Files:**
- Create: `custom_components/elmax_local/transport/http.py`
- Create: `tests/transport/test_http.py`

**Acceptance Criteria:**
- [ ] `async_probe()`: True su 200 al login, False su 401/timeout/network error
- [ ] `async_fetch_state()`: GET `/discovery` con `Authorization: JWT ...`, ritorna dict o None
- [ ] `async_send_command(eid, cmd, code)`: POST `/cmd/{eid}/{cmd}` con body `{"code": "..."}` se code presente; ritorna `CommandResult`
- [ ] 401 → `auth.async_handle_401()` + retry una volta
- [ ] 422 → 3 retry con `asyncio.sleep(2)` tra tentativi
- [ ] 503/timeout → `CommandResult(ok=False, error=...)`
- [ ] `state` property riflette READY/DEGRADED in base a ultima operazione

**Verify:** `pytest tests/transport/test_http.py -v` → all PASS

**Steps:**

- [ ] **Step 1: Write `tests/transport/test_http.py`**

```python
"""Test HttpTransport."""
from __future__ import annotations

import time
import pytest
from aioresponses import aioresponses

from custom_components.elmax_local.auth import AuthManager
from custom_components.elmax_local.transport import (
    TransportCapability, TransportState,
)
from custom_components.elmax_local.transport.http import HttpTransport


@pytest.fixture
async def auth(hass):
    am = AuthManager(hass, "1.2.3.4", "000000")
    yield am
    await am.async_close()


@pytest.fixture
def http_transport(hass, auth):
    return HttpTransport(hass, "1.2.3.4")


def test_capabilities(http_transport):
    assert TransportCapability.POLL in http_transport.capabilities
    assert TransportCapability.COMMAND in http_transport.capabilities
    assert TransportCapability.PUSH not in http_transport.capabilities


async def test_probe_ok(http_transport, auth, mock_panel_data):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login",
               payload={"token": "JWT eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"})
        await http_transport.async_start(auth, lambda d: None)
        assert await http_transport.async_probe() is True


async def test_probe_401(http_transport, auth):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", status=401,
               payload={"message": "Forbidden"})
        await http_transport.async_start(auth, lambda d: None)
        assert await http_transport.async_probe() is False


async def test_fetch_state(http_transport, auth, mock_panel_data):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.get("https://1.2.3.4/api/v2/discovery", payload=mock_panel_data)
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_fetch_state()
        assert result == mock_panel_data
        assert http_transport.state == TransportState.READY


async def test_send_command_ok(http_transport, auth):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.post("https://1.2.3.4/api/v2/cmd/abc-area-0/4",
               payload={"message": "Command Sent"})
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("abc-area-0", "4")
        assert result.ok is True


async def test_send_command_disarm_with_code(http_transport, auth):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.post("https://1.2.3.4/api/v2/cmd/abc-area-0/0",
               payload={"message": "Command Sent"})
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("abc-area-0", "0", code="000000")
        assert result.ok is True


async def test_send_command_422_retries(http_transport, auth):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        # 422 twice, then 200
        m.post("https://1.2.3.4/api/v2/cmd/eid/1", status=422,
               payload={"message": "Busy"})
        m.post("https://1.2.3.4/api/v2/cmd/eid/1", status=422,
               payload={"message": "Busy"})
        m.post("https://1.2.3.4/api/v2/cmd/eid/1",
               payload={"message": "Command Sent"})
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("eid", "1")
        assert result.ok is True


async def test_send_command_503_fails(http_transport, auth):
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", payload={"token": f"JWT {jwt}"})
        m.post("https://1.2.3.4/api/v2/cmd/eid/1", status=503,
               payload={"message": "Service Unavailable"})
        await http_transport.async_start(auth, lambda d: None)
        result = await http_transport.async_send_command("eid", "1")
        assert result.ok is False
        assert "503" in (result.error or "")
```

- [ ] **Step 2: Run test (FAIL)**

Run: `pytest tests/transport/test_http.py -v` → ImportError

- [ ] **Step 3: Create `custom_components/elmax_local/transport/http.py`**

```python
"""HTTP transport for Elmax Local. POLL + COMMAND capabilities."""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import aiohttp
from homeassistant.core import HomeAssistant

from ..const import HTTP_BASE_URL
from . import (
    CommandResult, StateUpdateCallback, Transport,
    TransportCapability, TransportState,
)

if TYPE_CHECKING:
    from ..auth import AuthManager

_LOGGER = logging.getLogger(__name__)

HTTP_TIMEOUT = 10
COMMAND_BUSY_RETRIES = 3
COMMAND_BUSY_DELAY = 2


class HttpTransport(Transport):
    name = "http"
    capabilities = frozenset({TransportCapability.POLL, TransportCapability.COMMAND})

    def __init__(self, hass: HomeAssistant, host: str):
        self._hass = hass
        self._host = host
        self._auth: "AuthManager" | None = None
        self._state = TransportState.DISABLED
        self._base = HTTP_BASE_URL.format(host=host)

    @property
    def state(self) -> TransportState:
        return self._state

    async def async_probe(self) -> bool:
        if self._auth is None:
            return False
        try:
            await self._auth.async_get_token()
            return True
        except Exception as err:
            _LOGGER.debug("HTTP probe failed: %s", err)
            return False

    async def async_start(self, auth, on_state_update) -> None:
        self._auth = auth
        self._state = TransportState.READY

    async def async_stop(self) -> None:
        self._state = TransportState.DISABLED

    async def async_fetch_state(self) -> dict | None:
        if self._auth is None:
            return None
        try:
            token = await self._auth.async_get_token()
            session = await self._auth._ensure_session()
            async with session.get(
                f"{self._base}/discovery",
                headers={"Authorization": f"JWT {token}"},
                timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            ) as resp:
                if resp.status == 401:
                    await self._auth.async_handle_401()
                    self._state = TransportState.DEGRADED
                    return None
                if resp.status != 200:
                    self._state = TransportState.DEGRADED
                    return None
                self._state = TransportState.READY
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            _LOGGER.debug("HTTP fetch error: %s", err)
            self._state = TransportState.DEGRADED
            return None

    async def async_send_command(self, endpoint_id, cmd, code=None) -> CommandResult:
        if self._auth is None:
            return CommandResult(ok=False, error="not_started")
        url = f"{self._base}/cmd/{endpoint_id}/{cmd}"
        body = {"code": code} if code else None
        for attempt in range(COMMAND_BUSY_RETRIES):
            try:
                token = await self._auth.async_get_token()
                session = await self._auth._ensure_session()
                async with session.post(
                    url, json=body,
                    headers={"Authorization": f"JWT {token}"},
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                ) as resp:
                    if resp.status == 200:
                        return CommandResult(ok=True, raw_response=await resp.json())
                    if resp.status == 401:
                        await self._auth.async_handle_401()
                        if attempt == 0:
                            continue
                        return CommandResult(ok=False, error="auth_401")
                    if resp.status == 422:
                        await asyncio.sleep(COMMAND_BUSY_DELAY)
                        continue
                    return CommandResult(ok=False, error=f"http_{resp.status}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                return CommandResult(ok=False, error=f"network: {err}")
        return CommandResult(ok=False, error="busy_after_retries")
```

- [ ] **Step 4: Run tests, verify PASS**

Run: `pytest tests/transport/test_http.py -v` → all passed

- [ ] **Step 5: Commit**

```bash
git add custom_components/elmax_local/transport/http.py tests/transport/test_http.py
git commit -m "feat(elmax_local): HttpTransport (POLL + COMMAND)

Implements probe via /login, fetch via GET /discovery, send_command
via POST /cmd. Handles 401 (re-auth), 422 (retry), 5xx (degraded).
Always-on transport — required for commands (spec sez. 5.1)."
```

---

## Task 5: MqttTransport

**Goal:** Trasporto MQTT via HA `mqtt` integration. Capability {PUSH, POLL, COMMAND}. Subscribe a `/elmax/response/status/{panel_id}` distingue `"200 Status OK"` (response) da `"200 Status Update"` (push spontaneo). Comandi via `/elmax/request/command/{panel_id}`.

**Files:**
- Create: `custom_components/elmax_local/transport/mqtt.py`
- Create: `tests/transport/test_mqtt.py`

**Acceptance Criteria:**
- [ ] `async_probe()`: publish `/elmax/request/id` con `{}`, attende risposta su `/elmax/response/id` entro 5s
- [ ] `async_start()`: sub a `/elmax/response/status/{panel_id}`, callback unwrap `data.status` se contiene chiave `status` (sia OK che Update)
- [ ] `async_fetch_state()`: publish `/elmax/request/status/{panel_id}` con `{"token": "..."}`, attende risposta su `/elmax/response/status` entro 8s
- [ ] `async_send_command()`: publish `/elmax/request/command/{panel_id}` con `{"token", "eid", "cmd", "code?"}`, attende risposta su `/elmax/response/command/{panel_id}` entro 5s
- [ ] Auth via `auth.async_get_token()`, su 401 chiama `auth.async_handle_401()`

**Verify:** `pytest tests/transport/test_mqtt.py -v` → all PASS

**Steps:**

- [ ] **Step 1: Write `tests/transport/test_mqtt.py`**

```python
"""Test MqttTransport (skeleton — full mqtt mock setup in HA fixtures)."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.elmax_local.auth import AuthManager
from custom_components.elmax_local.transport import (
    TransportCapability, TransportState,
)
from custom_components.elmax_local.transport.mqtt import MqttTransport


@pytest.fixture
async def auth(hass):
    am = AuthManager(hass, "1.2.3.4", "000000")
    am._token = "JWT_TOKEN"
    am._expiry = 9999999999
    yield am
    await am.async_close()


@pytest.fixture
def mqtt_transport(hass, auth):
    return MqttTransport(hass, "abc123")


def test_capabilities(mqtt_transport):
    assert TransportCapability.PUSH in mqtt_transport.capabilities
    assert TransportCapability.POLL in mqtt_transport.capabilities
    assert TransportCapability.COMMAND in mqtt_transport.capabilities


async def test_distinguishes_status_update_from_response(mqtt_transport, auth,
                                                          mock_panel_data):
    """Push spontaneo: 'message' = '200 Status Update'.
    Response a request: 'message' = '200 Status OK'.
    Entrambi devono triggerare on_state_update."""
    pushes = []

    async def on_push(data):
        pushes.append(data)

    with patch.object(mqtt_transport, "_subscribe_responses", new=AsyncMock()):
        await mqtt_transport.async_start(auth, on_push)

    # Simula push spontaneo
    msg_update = MagicMock()
    msg_update.payload = json.dumps({"message": "200 Status Update",
                                     "status": mock_panel_data})
    mqtt_transport._handle_status_message(msg_update)

    # Simula response a request
    msg_response = MagicMock()
    msg_response.payload = json.dumps({"message": "200 Status OK",
                                       "status": mock_panel_data})
    mqtt_transport._handle_status_message(msg_response)

    # Entrambi devono triggerare on_state_update con payload status
    await mqtt_transport._drain_pending()
    assert len(pushes) == 2
    assert all(p == mock_panel_data for p in pushes)
```

- [ ] **Step 2: Run test (FAIL)** → ImportError

- [ ] **Step 3: Create `custom_components/elmax_local/transport/mqtt.py`**

```python
"""MQTT transport for Elmax Local.

Uses HA's mqtt integration. Sub to /elmax/response/status/{panel_id}
delivers BOTH responses to requests AND spontaneous push updates
(distinguished by 'message' field). Both are forwarded to on_state_update.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback

from ..const import (
    TOPIC_REQUEST_COMMAND, TOPIC_REQUEST_ID, TOPIC_REQUEST_STATUS,
    TOPIC_RESPONSE_COMMAND, TOPIC_RESPONSE_ID, TOPIC_RESPONSE_STATUS,
)
from . import (
    CommandResult, StateUpdateCallback, Transport,
    TransportCapability, TransportState,
)

if TYPE_CHECKING:
    from ..auth import AuthManager

_LOGGER = logging.getLogger(__name__)

PROBE_TIMEOUT = 5
STATUS_TIMEOUT = 8
COMMAND_TIMEOUT = 5


class MqttTransport(Transport):
    name = "mqtt"
    capabilities = frozenset({
        TransportCapability.PUSH,
        TransportCapability.POLL,
        TransportCapability.COMMAND,
    })

    def __init__(self, hass: HomeAssistant, panel_id: str):
        self._hass = hass
        self._panel_id = panel_id
        self._auth: "AuthManager" | None = None
        self._on_push: StateUpdateCallback | None = None
        self._state = TransportState.DISABLED
        self._unsubs: list = []
        self._status_event = asyncio.Event()
        self._status_response: dict | None = None
        self._command_event = asyncio.Event()
        self._command_response: dict | None = None
        self._pending_tasks: list[asyncio.Task] = []

    @property
    def state(self) -> TransportState:
        return self._state

    async def async_probe(self) -> bool:
        event = asyncio.Event()

        @callback
        def _on_id(msg):
            try:
                data = json.loads(msg.payload)
                if "centrale" in data:
                    event.set()
            except (json.JSONDecodeError, ValueError):
                pass

        unsub = await mqtt.async_subscribe(self._hass, TOPIC_RESPONSE_ID, _on_id)
        try:
            await mqtt.async_publish(self._hass, TOPIC_REQUEST_ID, "{}")
            await asyncio.wait_for(event.wait(), timeout=PROBE_TIMEOUT)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            unsub()

    async def async_start(self, auth, on_state_update) -> None:
        self._auth = auth
        self._on_push = on_state_update
        await self._subscribe_responses()
        self._state = TransportState.READY

    async def _subscribe_responses(self) -> None:
        self._unsubs.append(await mqtt.async_subscribe(
            self._hass,
            TOPIC_RESPONSE_STATUS.format(panel_id=self._panel_id),
            self._handle_status_message,
        ))
        self._unsubs.append(await mqtt.async_subscribe(
            self._hass,
            TOPIC_RESPONSE_COMMAND.format(panel_id=self._panel_id),
            self._handle_command_message,
        ))

    async def async_stop(self) -> None:
        for u in self._unsubs:
            u()
        self._unsubs.clear()
        for t in self._pending_tasks:
            if not t.done():
                t.cancel()
        self._pending_tasks.clear()
        self._state = TransportState.DISABLED

    @callback
    def _handle_status_message(self, msg) -> None:
        try:
            data = json.loads(msg.payload)
        except (json.JSONDecodeError, ValueError):
            _LOGGER.warning("Invalid MQTT status payload")
            return

        message = data.get("message", "")
        if "401" in message:
            if self._auth:
                self._hass.async_create_task(self._auth.async_handle_401())
            return

        # Both '200 Status OK' (response) and '200 Status Update' (push)
        # carry status payload. Forward both to coordinator.
        status = data.get("status")
        if status and self._on_push:
            task = self._hass.async_create_task(self._on_push(status))
            self._pending_tasks.append(task)

        # Also signal request waiter (for fetch_state)
        self._status_response = data
        self._status_event.set()

    @callback
    def _handle_command_message(self, msg) -> None:
        try:
            self._command_response = json.loads(msg.payload)
            self._command_event.set()
        except (json.JSONDecodeError, ValueError):
            _LOGGER.warning("Invalid MQTT command payload")

    async def _drain_pending(self) -> None:
        """Test helper: await all pending push tasks."""
        if self._pending_tasks:
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            self._pending_tasks = [t for t in self._pending_tasks if not t.done()]

    async def async_fetch_state(self) -> dict | None:
        if not self._auth:
            return None
        try:
            token = await self._auth.async_get_token()
            self._status_event.clear()
            self._status_response = None
            await mqtt.async_publish(
                self._hass,
                TOPIC_REQUEST_STATUS.format(panel_id=self._panel_id),
                json.dumps({"token": f"JWT {token}"}),
            )
            await asyncio.wait_for(self._status_event.wait(), timeout=STATUS_TIMEOUT)
            if self._status_response:
                return self._status_response.get("status")
            return None
        except asyncio.TimeoutError:
            self._state = TransportState.DEGRADED
            return None

    async def async_send_command(self, endpoint_id, cmd, code=None) -> CommandResult:
        if not self._auth:
            return CommandResult(ok=False, error="not_started")
        try:
            token = await self._auth.async_get_token()
            self._command_event.clear()
            self._command_response = None
            body = {"token": f"JWT {token}", "eid": endpoint_id, "cmd": cmd}
            if code:
                body["code"] = code
            await mqtt.async_publish(
                self._hass,
                TOPIC_REQUEST_COMMAND.format(panel_id=self._panel_id),
                json.dumps(body),
            )
            await asyncio.wait_for(self._command_event.wait(), timeout=COMMAND_TIMEOUT)
            if self._command_response:
                msg = self._command_response.get("message", "")
                if "200" in msg:
                    return CommandResult(ok=True, raw_response=self._command_response)
                return CommandResult(ok=False, error=msg,
                                     raw_response=self._command_response)
            return CommandResult(ok=False, error="empty_response")
        except asyncio.TimeoutError:
            return CommandResult(ok=False, error="timeout")
```

- [ ] **Step 4: Run tests, verify PASS**

Run: `pytest tests/transport/test_mqtt.py -v`

- [ ] **Step 5: Commit**

```bash
git add custom_components/elmax_local/transport/mqtt.py tests/transport/test_mqtt.py
git commit -m "feat(elmax_local): MqttTransport (PUSH + POLL + COMMAND)

Subscribes to /elmax/response/status/{panel_id} which carries BOTH
'200 Status OK' (response) and '200 Status Update' (spontaneous push).
Forwards both to on_state_update — this is the key fix that turns
MQTT from a polling channel into a push channel (spec sez. 1.3)."
```

---

## Task 6: WebSocketTransport

**Goal:** Trasporto WS push real-time. Capability {PUSH}. Connessione `wss://IP/api/v2/push` con header `Authorization: JWT ...`. Reconnect con backoff esponenziale 5→120s.

**Files:**
- Create: `custom_components/elmax_local/transport/websocket.py`
- Create: `tests/transport/test_websocket.py`

**Acceptance Criteria:**
- [ ] `async_probe()`: tenta WS connect + handshake, ritorna True su connessione aperta entro 10s
- [ ] `async_start()`: avvia task background che apre WS, riceve messaggi, invoca `on_state_update`
- [ ] Su disconnect: reconnect con backoff 5s → 10s → 20s → ... → 120s
- [ ] Su 401 durante handshake: chiama `auth.async_handle_401()`, attende re-auth, riprova
- [ ] `async_stop()`: cancella task, chiude WS, set DISABLED

**Verify:** `pytest tests/transport/test_websocket.py -v` → all PASS

**Steps:**

- [ ] **Step 1: Write `tests/transport/test_websocket.py`**

```python
"""Test WebSocketTransport (skeleton — full WS server fixture for integration)."""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from custom_components.elmax_local.auth import AuthManager
from custom_components.elmax_local.transport import TransportCapability
from custom_components.elmax_local.transport.websocket import WebSocketTransport


@pytest.fixture
async def auth(hass):
    am = AuthManager(hass, "1.2.3.4", "000000")
    am._token = "JWT_TOKEN"
    am._expiry = 9999999999
    yield am
    await am.async_close()


@pytest.fixture
def ws_transport(hass, auth):
    return WebSocketTransport(hass, "1.2.3.4")


def test_capabilities(ws_transport):
    assert TransportCapability.PUSH in ws_transport.capabilities
    assert TransportCapability.POLL not in ws_transport.capabilities
    assert TransportCapability.COMMAND not in ws_transport.capabilities


async def test_probe_uses_auth_token(ws_transport, auth):
    """Probe must request a token via AuthManager before attempting WS."""
    with patch.object(ws_transport, "_open_ws", new=AsyncMock(return_value=True)) as mock_open:
        ws_transport._auth = auth
        result = await ws_transport.async_probe()
        assert result is True
        mock_open.assert_called_once()


async def test_handle_message_invokes_callback(ws_transport, mock_panel_data):
    pushes = []

    async def on_push(data):
        pushes.append(data)

    ws_transport._on_push = on_push
    await ws_transport._handle_message(json.dumps(mock_panel_data))
    assert pushes == [mock_panel_data]


async def test_handle_message_malformed_dropped(ws_transport):
    pushes = []
    ws_transport._on_push = lambda d: pushes.append(d)
    await ws_transport._handle_message("not json")
    assert pushes == []
```

- [ ] **Step 2: Run test (FAIL)** → ImportError

- [ ] **Step 3: Create `custom_components/elmax_local/transport/websocket.py`**

```python
"""WebSocket transport for Elmax Local. PUSH capability only.

Connects to wss://IP/api/v2/push. Receives JSON identical to /api/v2/discovery
on each state change. Limited to 1 client per session per the panel's docs.
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
from typing import TYPE_CHECKING

import aiohttp
from homeassistant.core import HomeAssistant

from ..const import WS_BASE_URL
from . import StateUpdateCallback, Transport, TransportCapability, TransportState

if TYPE_CHECKING:
    from ..auth import AuthManager

_LOGGER = logging.getLogger(__name__)

CONNECT_TIMEOUT = 10
HEARTBEAT = 30
BACKOFF_INITIAL = 5
BACKOFF_MAX = 120


class WebSocketTransport(Transport):
    name = "websocket"
    capabilities = frozenset({TransportCapability.PUSH})

    def __init__(self, hass: HomeAssistant, host: str):
        self._hass = hass
        self._host = host
        self._auth: "AuthManager" | None = None
        self._on_push: StateUpdateCallback | None = None
        self._state = TransportState.DISABLED
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._url = WS_BASE_URL.format(host=host)

    @property
    def state(self) -> TransportState:
        return self._state

    async def async_probe(self) -> bool:
        if self._auth is None:
            return False
        try:
            return await asyncio.wait_for(
                self._open_ws(probe_only=True), timeout=CONNECT_TIMEOUT
            )
        except (asyncio.TimeoutError, aiohttp.ClientError, Exception) as err:
            _LOGGER.debug("WS probe failed: %s", err)
            return False

    async def _open_ws(self, probe_only: bool = False) -> bool:
        if self._auth is None:
            return False
        token = await self._auth.async_get_token()
        session = await self._auth._ensure_session()
        async with session.ws_connect(
            self._url,
            headers={"Authorization": f"JWT {token}"},
            ssl=self._auth._ssl_ctx,
            heartbeat=HEARTBEAT,
            timeout=CONNECT_TIMEOUT,
        ) as ws:
            if probe_only:
                await ws.close()
                return True
            await self._listen(ws)
            return True

    async def async_start(self, auth, on_state_update) -> None:
        self._auth = auth
        self._on_push = on_state_update
        self._stop_event.clear()
        self._task = self._hass.async_create_task(self._run_loop())
        self._state = TransportState.READY

    async def async_stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        self._state = TransportState.DISABLED

    async def _run_loop(self) -> None:
        backoff = BACKOFF_INITIAL
        while not self._stop_event.is_set():
            try:
                await self._open_ws(probe_only=False)
                backoff = BACKOFF_INITIAL
            except aiohttp.WSServerHandshakeError as err:
                if err.status == 401 and self._auth:
                    await self._auth.async_handle_401()
                _LOGGER.debug("WS handshake error %s, retry in %ds", err, backoff)
                self._state = TransportState.DEGRADED
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as err:
                _LOGGER.debug("WS error %s, retry in %ds", err, backoff)
                self._state = TransportState.DEGRADED
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)

    async def _listen(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self._state = TransportState.READY
        async for msg in ws:
            if self._stop_event.is_set():
                break
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle_message(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    async def _handle_message(self, payload: str) -> None:
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            _LOGGER.warning("WS dropped malformed payload")
            return
        if self._on_push:
            await self._on_push(data)
```

- [ ] **Step 4: Run tests, verify PASS**

Run: `pytest tests/transport/test_websocket.py -v`

- [ ] **Step 5: Commit**

```bash
git add custom_components/elmax_local/transport/websocket.py tests/transport/test_websocket.py
git commit -m "feat(elmax_local): WebSocketTransport (PUSH)

Real-time push channel via wss://IP/api/v2/push. Receives full
discovery payload on each state change. Auto-reconnect with
exponential backoff (5→120s). 1-client-per-session limit
documented (spec sez. 1.3, 7.4)."
```

---

## Task 7: TransportRegistry routing

**Goal:** Implementa la logica di routing di `TransportRegistry`: start/stop all, fetch_state (HTTP primary, MQTT fallback), send_command (HTTP primary, MQTT fallback), retry probe per DEGRADED/UNSUPPORTED.

**Files:**
- Modify: `custom_components/elmax_local/transport/__init__.py` (replace Registry methods)
- Create: `tests/transport/test_registry.py`

**Acceptance Criteria:**
- [ ] `async_start_all()`: per ogni transport, `probe()`; se True, `start(auth, on_push)`; altrimenti state DEGRADED/UNSUPPORTED
- [ ] `async_fetch_state()`: itera transports con POLL e state=READY in ordine [http, mqtt]; primo successo wins
- [ ] `async_send_command()`: itera transports con COMMAND e state=READY in ordine [http, mqtt]; primo successo wins
- [ ] `get_active_push_transports()`: ritorna transports con PUSH+READY
- [ ] `degraded_or_unsupported()`: ritorna lista per retry loop

**Verify:** `pytest tests/transport/test_registry.py -v` → all PASS

**Steps:**

- [ ] **Step 1: Write `tests/transport/test_registry.py`**

```python
"""Test TransportRegistry routing."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.elmax_local.transport import (
    CommandResult, Transport, TransportCapability, TransportRegistry,
    TransportState,
)


class _FakeTransport(Transport):
    def __init__(self, name: str, caps: frozenset,
                 probe_ok=True, fetch_result=None, command_result=None):
        self.name = name
        self.capabilities = caps
        self._state = TransportState.DISABLED
        self._probe_ok = probe_ok
        self._fetch_result = fetch_result
        self._command_result = command_result or CommandResult(ok=True)
        self.start_called = False
        self.stop_called = False

    @property
    def state(self):
        return self._state

    async def async_probe(self):
        return self._probe_ok

    async def async_start(self, auth, on_push):
        self.start_called = True
        self._state = TransportState.READY if self._probe_ok else TransportState.UNSUPPORTED

    async def async_stop(self):
        self.stop_called = True
        self._state = TransportState.DISABLED

    async def async_fetch_state(self):
        if TransportCapability.POLL not in self.capabilities:
            return await super().async_fetch_state()
        return self._fetch_result

    async def async_send_command(self, eid, cmd, code=None):
        if TransportCapability.COMMAND not in self.capabilities:
            return await super().async_send_command(eid, cmd, code)
        return self._command_result


async def test_start_all_probes_and_starts(hass):
    http = _FakeTransport("http", frozenset({TransportCapability.POLL,
                                              TransportCapability.COMMAND}))
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.PUSH,
                                              TransportCapability.POLL}))
    reg = TransportRegistry([http, mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())
    assert http.start_called and mqtt.start_called
    assert http.state == TransportState.READY


async def test_unsupported_not_started(hass):
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}))
    ws = _FakeTransport("ws", frozenset({TransportCapability.PUSH}), probe_ok=False)
    reg = TransportRegistry([http, ws])
    await reg.async_start_all(MagicMock(), AsyncMock())
    assert http.state == TransportState.READY
    assert ws.state == TransportState.UNSUPPORTED


async def test_fetch_uses_http_first(hass, mock_panel_data):
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}),
                          fetch_result=mock_panel_data)
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.PUSH,
                                              TransportCapability.POLL}),
                          fetch_result={"different": True})
    reg = TransportRegistry([http, mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())
    result = await reg.async_fetch_state()
    assert result == mock_panel_data


async def test_fetch_falls_back_to_mqtt_on_http_none(hass, mock_panel_data):
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}),
                          fetch_result=None)
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.POLL}),
                          fetch_result=mock_panel_data)
    reg = TransportRegistry([http, mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())
    result = await reg.async_fetch_state()
    assert result == mock_panel_data


async def test_send_command_uses_http_first():
    http = _FakeTransport("http", frozenset({TransportCapability.COMMAND}),
                          command_result=CommandResult(ok=True))
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.COMMAND}),
                          command_result=CommandResult(ok=False, error="should not be called"))
    reg = TransportRegistry([http, mqtt])
    await reg.async_start_all(MagicMock(), AsyncMock())
    result = await reg.async_send_command("eid", "1")
    assert result.ok is True


def test_get_active_push_transports():
    http = _FakeTransport("http", frozenset({TransportCapability.POLL}))
    mqtt = _FakeTransport("mqtt", frozenset({TransportCapability.PUSH}))
    mqtt._state = TransportState.READY
    ws = _FakeTransport("ws", frozenset({TransportCapability.PUSH}))
    ws._state = TransportState.DEGRADED
    reg = TransportRegistry([http, mqtt, ws])
    active = reg.get_active_push_transports()
    assert mqtt in active
    assert ws not in active
```

- [ ] **Step 2: Run test (FAIL — NotImplementedError)**

- [ ] **Step 3: Replace Registry methods in `transport/__init__.py`**

Edit the `TransportRegistry` class — replace the placeholder methods:

```python
class TransportRegistry:
    """Orchestrates N transports. Routes operations by capability + state."""

    # Priority order for POLL and COMMAND fallback
    _POLL_PRIORITY = ("http", "mqtt")
    _COMMAND_PRIORITY = ("http", "mqtt")

    def __init__(self, transports: list[Transport]):
        self._transports = transports

    async def async_start_all(
        self,
        auth: "AuthManager",
        on_state_update: StateUpdateCallback,
    ) -> None:
        for t in self._transports:
            if t.state == TransportState.DISABLED and t.name in _disabled_names_from_options:
                continue  # placeholder — actual disable check uses entry.options
            try:
                ok = await t.async_probe()
                if ok:
                    await t.async_start(auth, on_state_update)
                else:
                    # transport marks itself UNSUPPORTED inside probe/start
                    pass
            except Exception as err:
                import logging
                logging.getLogger(__name__).warning(
                    "Transport %s failed to start: %s", t.name, err
                )

    async def async_stop_all(self) -> None:
        for t in self._transports:
            try:
                await t.async_stop()
            except Exception:
                pass

    def _by_name(self, name: str) -> Transport | None:
        for t in self._transports:
            if t.name == name:
                return t
        return None

    async def async_fetch_state(self) -> dict | None:
        for name in self._POLL_PRIORITY:
            t = self._by_name(name)
            if (t and TransportCapability.POLL in t.capabilities
                    and t.state == TransportState.READY):
                result = await t.async_fetch_state()
                if result is not None:
                    return result
        return None

    async def async_send_command(
        self,
        endpoint_id: str,
        cmd: str | None,
        code: str | None = None,
    ) -> CommandResult:
        last_error = "no_transport"
        for name in self._COMMAND_PRIORITY:
            t = self._by_name(name)
            if (t and TransportCapability.COMMAND in t.capabilities
                    and t.state == TransportState.READY):
                result = await t.async_send_command(endpoint_id, cmd, code)
                if result.ok:
                    return result
                last_error = result.error or "unknown"
        return CommandResult(ok=False, error=last_error)

    def get_active_push_transports(self) -> list[Transport]:
        return [t for t in self._transports
                if TransportCapability.PUSH in t.capabilities
                and t.state == TransportState.READY]

    def degraded_or_unsupported(self) -> list[Transport]:
        return [t for t in self._transports
                if t.state in (TransportState.DEGRADED, TransportState.UNSUPPORTED)]
```

Note: `_disabled_names_from_options` placeholder — replace with actual lookup once `__init__.py` Task 15 wires options. For now, use empty set:

```python
_disabled_names_from_options: set[str] = set()
```

at module level above the class.

- [ ] **Step 4: Run tests, verify PASS**

- [ ] **Step 5: Commit**

```bash
git add custom_components/elmax_local/transport/__init__.py tests/transport/test_registry.py
git commit -m "feat(elmax_local): TransportRegistry routing logic

Implements priority-based fallback: HTTP primary for POLL/COMMAND,
MQTT secondary. Push transports return all READY for parallel push."
```

---

## Task 8: ElmaxState + Coordinator skeleton

**Goal:** Dataclass `ElmaxState` + skeleton `ElmaxLocalCoordinator` che estende `DataUpdateCoordinator`. Senza data flow ancora (Task 9) e senza command (Task 10).

**Files:**
- Create: `custom_components/elmax_local/coordinator.py`
- Create: `tests/test_coordinator.py` (parziale, completato in 9-10)

**Acceptance Criteria:**
- [ ] `ElmaxState` dataclass con campi tipizzati
- [ ] `ElmaxLocalCoordinator.__init__` instanzia AuthManager + TransportRegistry
- [ ] `update_interval` = `reconcile_interval` da options
- [ ] Test: istanziare coordinator non crasha

**Verify:** `pytest tests/test_coordinator.py -v` → PASS (subset)

**Steps:**

- [ ] **Step 1: Write `tests/test_coordinator.py` (subset)**

```python
"""Test ElmaxLocalCoordinator."""
from __future__ import annotations

from datetime import timedelta

import pytest

from custom_components.elmax_local.coordinator import (
    ElmaxLocalCoordinator, ElmaxState,
)


def test_state_dataclass_defaults():
    state = ElmaxState(
        panel_info={}, zones={}, areas={}, outputs={}, scenarios={},
        last_update_source="", last_update_ts=0,
    )
    assert state.zones == {}


def test_coordinator_init(hass):
    coord = ElmaxLocalCoordinator(
        hass, panel_id="abc", pin="000000", host="1.2.3.4",
        reconcile_interval=90, enable_ws=True, enable_mqtt=True,
    )
    assert coord.panel_id == "abc"
    assert coord.update_interval == timedelta(seconds=90)
    assert coord.auth is not None
    assert coord.registry is not None
```

- [ ] **Step 2: Run test (FAIL)** → ImportError

- [ ] **Step 3: Create `custom_components/elmax_local/coordinator.py` (skeleton)**

```python
"""ElmaxLocalCoordinator — orchestrates transports and updates entities."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import timedelta

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .auth import AuthManager
from .const import DEFAULT_RECONCILE_INTERVAL
from .transport import TransportRegistry
from .transport.http import HttpTransport
from .transport.mqtt import MqttTransport
from .transport.websocket import WebSocketTransport

_LOGGER = logging.getLogger(__name__)

PUSH_FRESHNESS_RATIO = 0.5  # skip poll if push < interval*0.5 ago


@dataclass
class ElmaxState:
    panel_info: dict = field(default_factory=dict)
    zones: dict[str, dict] = field(default_factory=dict)
    areas: dict[str, dict] = field(default_factory=dict)
    outputs: dict[str, dict] = field(default_factory=dict)
    scenarios: dict[str, dict] = field(default_factory=dict)
    last_update_source: str = ""
    last_update_ts: float = 0


class ElmaxLocalCoordinator(DataUpdateCoordinator[ElmaxState]):
    """Coordinates push + poll across transports."""

    def __init__(
        self,
        hass: HomeAssistant,
        panel_id: str,
        pin: str,
        host: str,
        reconcile_interval: int = DEFAULT_RECONCILE_INTERVAL,
        enable_ws: bool = True,
        enable_mqtt: bool = True,
    ):
        super().__init__(
            hass, _LOGGER,
            name=f"Elmax {panel_id}",
            update_interval=timedelta(seconds=reconcile_interval),
        )
        self.panel_id = panel_id
        self.host = host
        self.auth = AuthManager(hass, host, pin)

        transports = [HttpTransport(hass, host)]  # always on
        if enable_mqtt:
            transports.append(MqttTransport(hass, panel_id))
        if enable_ws:
            transports.append(WebSocketTransport(hass, host))
        self.registry = TransportRegistry(transports)

    async def async_setup(self) -> None:
        """Called by async_setup_entry. Task 15 wires this."""
        await self.registry.async_start_all(self.auth, self._on_push_state_update)
        await self.async_config_entry_first_refresh()

    async def async_shutdown(self) -> None:
        await self.registry.async_stop_all()
        await self.auth.async_close()

    async def _async_update_data(self) -> ElmaxState:
        """Stub — Task 9 implements."""
        raise UpdateFailed("Implemented in Task 9")

    async def _on_push_state_update(self, raw: dict) -> None:
        """Stub — Task 9 implements."""
        pass

    def _push_is_fresh(self) -> bool:
        if not self.data or self.data.last_update_ts == 0:
            return False
        if not self.update_interval:
            return False
        elapsed = time.time() - self.data.last_update_ts
        return elapsed < self.update_interval.total_seconds() * PUSH_FRESHNESS_RATIO
```

- [ ] **Step 4: Run tests** → 2 passed

- [ ] **Step 5: Commit**

```bash
git add custom_components/elmax_local/coordinator.py tests/test_coordinator.py
git commit -m "feat(elmax_local): Coordinator skeleton with ElmaxState

DataUpdateCoordinator subclass wiring AuthManager and TransportRegistry.
Data flow implemented in Task 9."
```

---

## Task 9: Coordinator data flow

**Goal:** Implementa `_parse`, `_on_push_state_update`, `_async_update_data` con push-fresh skip logic.

**Files:**
- Modify: `custom_components/elmax_local/coordinator.py`
- Modify: `tests/test_coordinator.py` (aggiunge test data flow)

**Acceptance Criteria:**
- [ ] `_parse(raw)`: indicizza zone/aree/uscite/scenari per `endpointId`, popola `panel_info`, set `last_update_ts`
- [ ] `_on_push_state_update(raw)`: chiama `_parse` + `async_set_updated_data`
- [ ] `_async_update_data()`: skip se `_push_is_fresh()`, altrimenti `registry.async_fetch_state()`, raise `UpdateFailed` su None
- [ ] Test: push update aggiorna `coordinator.data`; poll dopo push fresh ritorna stesso state (no fetch)

**Verify:** `pytest tests/test_coordinator.py -v` → PASS

**Steps:**

- [ ] **Step 1: Add tests in `tests/test_coordinator.py`**

```python
import time
from unittest.mock import AsyncMock, patch

async def test_parse_indexes_by_endpoint_id(hass, mock_panel_data):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    state = coord._parse(mock_panel_data)
    assert "abc123-zona-0" in state.zones
    assert "abc123-area-0" in state.areas
    assert state.panel_info["release"] == "PHANTOM64PRO_GSM 13.9A.845"


async def test_on_push_update_sets_data(hass, mock_panel_data):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    await coord._on_push_state_update(mock_panel_data)
    assert coord.data is not None
    assert "abc123-zona-0" in coord.data.zones
    assert coord.data.last_update_source != ""


async def test_update_data_skips_when_push_fresh(hass, mock_panel_data):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4",
                                  reconcile_interval=60)
    await coord._on_push_state_update(mock_panel_data)
    with patch.object(coord.registry, "async_fetch_state",
                      new=AsyncMock(return_value={"different": True})) as mock_fetch:
        result = await coord._async_update_data()
        mock_fetch.assert_not_called()
        assert result is coord.data
```

- [ ] **Step 2: Replace stubs in `coordinator.py`**

Edit `_parse`, `_on_push_state_update`, `_async_update_data`:

```python
def _parse(self, raw: dict, source: str = "poll") -> ElmaxState:
    """Normalize /api/v2/discovery payload to ElmaxState.
    Indexes lists by endpointId for O(1) lookup by entities."""
    return ElmaxState(
        panel_info={
            "centrale": raw.get("centrale", self.panel_id),
            "release": raw.get("release", ""),
            "tipo_accessorio": raw.get("tipo_accessorio", ""),
            "release_accessorio": raw.get("release_accessorio", ""),
            "tappFeature": raw.get("tappFeature", False),
            "sceneFeature": raw.get("sceneFeature", False),
        },
        zones={z["endpointId"]: z for z in raw.get("zone", [])
               if "endpointId" in z},
        areas={a["endpointId"]: a for a in raw.get("aree", [])
               if "endpointId" in a},
        outputs={o["endpointId"]: o for o in raw.get("uscite", [])
                 if "endpointId" in o},
        scenarios={s["endpointId"]: s for s in raw.get("scenari", [])
                   if "endpointId" in s},
        last_update_source=source,
        last_update_ts=time.time(),
    )

async def _on_push_state_update(self, raw: dict) -> None:
    """Called by PUSH transports on each spontaneous state update."""
    # Determine source from active push transports
    push = self.registry.get_active_push_transports()
    source = push[0].name if push else "push"
    new_state = self._parse(raw, source=source)
    self.async_set_updated_data(new_state)

async def _async_update_data(self) -> ElmaxState:
    """Reconciliation poll. Skipped if push is fresh."""
    if self._push_is_fresh():
        return self.data
    raw = await self.registry.async_fetch_state()
    if raw is None:
        if self.data is not None:
            # Keep stale data, just mark via logger
            _LOGGER.debug("Poll failed; keeping last known state")
            return self.data
        raise UpdateFailed("All polling transports failed; no prior state")
    return self._parse(raw, source="poll")
```

- [ ] **Step 3: Run tests** → all passed

- [ ] **Step 4: Commit**

```bash
git add custom_components/elmax_local/coordinator.py tests/test_coordinator.py
git commit -m "feat(elmax_local): Coordinator data flow (parse + push + poll skip)

Push-first semantics: poll skipped if push received within
update_interval/2. Entities indexed by endpointId for O(1) lookup
(spec sez. 3, 6.1)."
```

---

## Task 10: Coordinator commands

**Goal:** `async_send_command` + `_post_command_verify` background task.

**Files:**
- Modify: `custom_components/elmax_local/coordinator.py`
- Modify: `tests/test_coordinator.py`

**Acceptance Criteria:**
- [ ] `async_send_command(eid, cmd, code)` chiama `registry.async_send_command`
- [ ] Su successo, schedula `_post_command_verify`
- [ ] `_post_command_verify`: sleep 3s, se `last_update_ts > 3s fa` forza `async_request_refresh()`
- [ ] Test: command ok → task scheduled; command fail → no task

**Verify:** `pytest tests/test_coordinator.py -v` → PASS

**Steps:**

- [ ] **Step 1: Add tests**

```python
async def test_send_command_schedules_verify(hass):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    with patch.object(coord.registry, "async_send_command",
                      new=AsyncMock(return_value=CommandResult(ok=True))):
        with patch.object(coord, "_post_command_verify",
                          new=AsyncMock()) as mock_verify:
            ok = await coord.async_send_command("eid", "4")
            assert ok is True
            # Task is created; give it a tick
            await asyncio.sleep(0)
            mock_verify.assert_called()


async def test_send_command_fail_no_verify(hass):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    with patch.object(coord.registry, "async_send_command",
                      new=AsyncMock(return_value=CommandResult(ok=False))):
        with patch.object(coord, "_post_command_verify",
                          new=AsyncMock()) as mock_verify:
            ok = await coord.async_send_command("eid", "4")
            assert ok is False
            await asyncio.sleep(0)
            mock_verify.assert_not_called()
```

Add imports: `import asyncio`, `from custom_components.elmax_local.transport import CommandResult`.

- [ ] **Step 2: Add to `coordinator.py`**

```python
import asyncio

POST_COMMAND_WAIT = 3

async def async_send_command(
    self,
    endpoint_id: str,
    cmd: str | None,
    code: str | None = None,
) -> bool:
    result = await self.registry.async_send_command(endpoint_id, cmd, code)
    if result.ok:
        self.hass.async_create_task(self._post_command_verify())
    else:
        _LOGGER.warning("Command %s/%s failed: %s",
                        endpoint_id, cmd, result.error)
    return result.ok

async def _post_command_verify(self) -> None:
    """Wait briefly for a push; if none, force reconcile."""
    await asyncio.sleep(POST_COMMAND_WAIT)
    if (self.data is None
            or (time.time() - self.data.last_update_ts) > POST_COMMAND_WAIT):
        await self.async_request_refresh()
```

- [ ] **Step 3: Run tests, verify PASS**

- [ ] **Step 4: Commit**

```bash
git add custom_components/elmax_local/coordinator.py tests/test_coordinator.py
git commit -m "feat(elmax_local): Coordinator command + post-verify

Sends commands via registry; if successful, schedules background
verify task that forces poll only if no push arrived within 3s.
Avoids blocking the entity command method (spec sez. 3, 7.6)."
```

---

## Task 11: alarm_control_panel entity

**Goal:** `ElmaxAlarmPanel(CoordinatorEntity, AlarmControlPanelEntity)` per ogni area visibile.

**Files:**
- Create: `custom_components/elmax_local/alarm_control_panel.py`
- Create: `tests/test_entities.py` (parziale)

**Acceptance Criteria:**
- [ ] Eredita da `CoordinatorEntity[ElmaxLocalCoordinator]` + `AlarmControlPanelEntity`
- [ ] `unique_id = f"elmax_local_{endpoint_id}"`
- [ ] `alarm_state` mappato da `area["stato"]` via `ELMAX_TO_HA_STATE`
- [ ] `available` derivato da `super().available and endpoint_id in coordinator.data.areas`
- [ ] `async_alarm_disarm/arm_*` chiamano `coordinator.async_send_command`
- [ ] `extra_state_attributes`: `zoneBmask`, `statoSessione`, `indice`

**Verify:** `pytest tests/test_entities.py::test_alarm_panel -v` → PASS

**Steps:**

- [ ] **Step 1: Write `tests/test_entities.py` (alarm part)**

```python
"""Test Elmax Local entities."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from custom_components.elmax_local.alarm_control_panel import ElmaxAlarmPanel
from custom_components.elmax_local.coordinator import ElmaxLocalCoordinator, ElmaxState


@pytest.fixture
def coord_with_data(hass, mock_panel_data):
    coord = ElmaxLocalCoordinator(hass, "abc", "000000", "1.2.3.4")
    coord.data = coord._parse(mock_panel_data)
    coord.async_send_command = AsyncMock(return_value=True)
    return coord


def test_alarm_panel_unique_id(coord_with_data):
    panel = ElmaxAlarmPanel(coord_with_data, "abc123-area-0")
    assert panel.unique_id == "elmax_local_abc123-area-0"


def test_alarm_panel_state_disarmed(coord_with_data):
    panel = ElmaxAlarmPanel(coord_with_data, "abc123-area-0")
    assert panel.alarm_state == "disarmed"


async def test_alarm_panel_arm_away(coord_with_data):
    panel = ElmaxAlarmPanel(coord_with_data, "abc123-area-0")
    await panel.async_alarm_arm_away()
    coord_with_data.async_send_command.assert_called_with("abc123-area-0", "4")


async def test_alarm_panel_disarm_uses_pin(coord_with_data):
    panel = ElmaxAlarmPanel(coord_with_data, "abc123-area-0")
    coord_with_data.auth._pin = "000000"
    await panel.async_alarm_disarm()
    coord_with_data.async_send_command.assert_called_with(
        "abc123-area-0", "0", code="000000"
    )
```

- [ ] **Step 2: Run test (FAIL)** → ImportError

- [ ] **Step 3: Create `custom_components/elmax_local/alarm_control_panel.py`**

```python
"""Elmax Local alarm control panel entities."""
from __future__ import annotations

import logging

from homeassistant.components.alarm_control_panel import (
    AlarmControlPanelEntity, AlarmControlPanelEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import (
    CMD_AREA_ARM_P1P2, CMD_AREA_ARM_P2, CMD_AREA_ARM_TOTAL,
    CMD_AREA_DISARM, DOMAIN, ELMAX_TO_HA_STATE,
)
from .coordinator import ElmaxLocalCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxAlarmPanel(coordinator, eid)
        for eid, area in coordinator.data.areas.items()
        if area.get("visibile", True)
    )


class ElmaxAlarmPanel(CoordinatorEntity[ElmaxLocalCoordinator],
                      AlarmControlPanelEntity):
    _attr_has_entity_name = True
    _attr_supported_features = (
        AlarmControlPanelEntityFeature.ARM_AWAY
        | AlarmControlPanelEntityFeature.ARM_HOME
        | AlarmControlPanelEntityFeature.ARM_NIGHT
    )
    _attr_code_arm_required = False

    def __init__(self, coordinator: ElmaxLocalCoordinator, endpoint_id: str):
        super().__init__(coordinator)
        self._endpoint_id = endpoint_id
        self._attr_unique_id = f"{DOMAIN}_{endpoint_id}"
        area = coordinator.data.areas[endpoint_id]
        self._attr_name = area.get("nome") or f"Area {area.get('indice', '?')}"
        self._attr_device_info = _device_info(coordinator)

    @property
    def available(self) -> bool:
        return (super().available
                and self.coordinator.data is not None
                and self._endpoint_id in self.coordinator.data.areas)

    @property
    def alarm_state(self) -> str | None:
        if self.coordinator.data is None:
            return None
        area = self.coordinator.data.areas.get(self._endpoint_id)
        if area is None:
            return None
        return ELMAX_TO_HA_STATE.get(area.get("stato", 0))

    @property
    def extra_state_attributes(self) -> dict:
        if self.coordinator.data is None:
            return {}
        area = self.coordinator.data.areas.get(self._endpoint_id, {})
        return {
            "zoneBmask": area.get("zoneBmask"),
            "statoSessione": area.get("statoSessione"),
            "indice": area.get("indice"),
        }

    async def async_alarm_disarm(self, code: str | None = None) -> None:
        # Always use panel PIN for disarm (not user-provided code)
        await self.coordinator.async_send_command(
            self._endpoint_id, CMD_AREA_DISARM, code=self.coordinator.auth._pin
        )

    async def async_alarm_arm_away(self, code: str | None = None) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_AREA_ARM_TOTAL)

    async def async_alarm_arm_home(self, code: str | None = None) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_AREA_ARM_P1P2)

    async def async_alarm_arm_night(self, code: str | None = None) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_AREA_ARM_P2)


def _device_info(coordinator: ElmaxLocalCoordinator) -> DeviceInfo:
    return DeviceInfo(
        identifiers={(DOMAIN, coordinator.panel_id)},
        name=f"Elmax {coordinator.panel_id[-6:]}",
        manufacturer="Elmax",
        model=coordinator.data.panel_info.get("release", "Phantom64") if coordinator.data else "Phantom64",
        sw_version=coordinator.data.panel_info.get("release_accessorio", "") if coordinator.data else "",
    )
```

- [ ] **Step 4: Run tests** → PASS

- [ ] **Step 5: Commit**

```bash
git add custom_components/elmax_local/alarm_control_panel.py tests/test_entities.py
git commit -m "feat(elmax_local): alarm_control_panel entity via CoordinatorEntity"
```

---

## Task 12: binary_sensor entity (zones)

**Goal:** `ElmaxZoneSensor` per ogni zona visibile. Device class inferito dal nome (porta/finestra/motion).

**Files:**
- Create: `custom_components/elmax_local/binary_sensor.py`
- Modify: `tests/test_entities.py` (aggiungi zone)

**Acceptance Criteria:**
- [ ] Eredita da `CoordinatorEntity` + `BinarySensorEntity`
- [ ] `unique_id = f"elmax_local_{endpoint_id}"`
- [ ] `is_on` da `zone["aperta"]`
- [ ] Device class inferito: "porta" → DOOR, "fines" → WINDOW, default MOTION
- [ ] `extra_state_attributes`: `esclusa`, `indice`

**Verify:** `pytest tests/test_entities.py -k zone -v` → PASS

**Steps:**

- [ ] **Step 1: Add tests** (zone test similar to alarm pattern)

```python
from custom_components.elmax_local.binary_sensor import ElmaxZoneSensor, _infer_device_class
from homeassistant.components.binary_sensor import BinarySensorDeviceClass


def test_infer_device_class_porta():
    assert _infer_device_class("Porta Ingresso") == BinarySensorDeviceClass.DOOR


def test_infer_device_class_finestra():
    assert _infer_device_class("Finestra Cucina") == BinarySensorDeviceClass.WINDOW


def test_infer_device_class_default():
    assert _infer_device_class("Sensore Salotto") == BinarySensorDeviceClass.MOTION


def test_zone_is_on(coord_with_data):
    zone = ElmaxZoneSensor(coord_with_data, "abc123-zona-0")
    assert zone.is_on is False  # aperta=False in mock_panel_data
```

- [ ] **Step 2: Create `custom_components/elmax_local/binary_sensor.py`**

```python
"""Elmax Local binary sensor entities for alarm zones."""
from __future__ import annotations

import logging

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass, BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .alarm_control_panel import _device_info
from .const import DOMAIN
from .coordinator import ElmaxLocalCoordinator

_LOGGER = logging.getLogger(__name__)


def _infer_device_class(name: str) -> BinarySensorDeviceClass:
    n = name.lower()
    if "porta" in n:
        return BinarySensorDeviceClass.DOOR
    if "fines" in n:
        return BinarySensorDeviceClass.WINDOW
    if n.startswith("m ") or n.startswith("m v"):
        return BinarySensorDeviceClass.DOOR
    return BinarySensorDeviceClass.MOTION


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxZoneSensor(coordinator, eid)
        for eid, zone in coordinator.data.zones.items()
        if zone.get("visibile", True)
    )


class ElmaxZoneSensor(CoordinatorEntity[ElmaxLocalCoordinator], BinarySensorEntity):
    _attr_has_entity_name = True

    def __init__(self, coordinator: ElmaxLocalCoordinator, endpoint_id: str):
        super().__init__(coordinator)
        self._endpoint_id = endpoint_id
        self._attr_unique_id = f"{DOMAIN}_{endpoint_id}"
        zone = coordinator.data.zones[endpoint_id]
        self._attr_name = zone.get("nome") or f"Zona {zone.get('indice', '?')}"
        self._attr_device_class = _infer_device_class(self._attr_name)
        self._attr_device_info = _device_info(coordinator)

    @property
    def available(self) -> bool:
        return (super().available
                and self.coordinator.data is not None
                and self._endpoint_id in self.coordinator.data.zones)

    @property
    def is_on(self) -> bool | None:
        if self.coordinator.data is None:
            return None
        zone = self.coordinator.data.zones.get(self._endpoint_id)
        return zone.get("aperta") if zone else None

    @property
    def extra_state_attributes(self) -> dict:
        if self.coordinator.data is None:
            return {}
        z = self.coordinator.data.zones.get(self._endpoint_id, {})
        return {"esclusa": z.get("esclusa"), "indice": z.get("indice")}
```

- [ ] **Step 3: Tests PASS, commit**

```bash
git add custom_components/elmax_local/binary_sensor.py tests/test_entities.py
git commit -m "feat(elmax_local): binary_sensor entity for zones"
```

---

## Task 13: switch entity (outputs)

**Goal:** `ElmaxOutputSwitch` per ogni uscita visibile.

**Files:**
- Create: `custom_components/elmax_local/switch.py`
- Modify: `tests/test_entities.py`

**Acceptance Criteria:**
- [ ] Eredita da `CoordinatorEntity` + `SwitchEntity`
- [ ] `is_on` da `output["aperta"]`
- [ ] `async_turn_on` → `CMD_OUTPUT_ON`, `async_turn_off` → `CMD_OUTPUT_OFF`

**Verify:** `pytest tests/test_entities.py -k switch -v` → PASS

**Steps:**

- [ ] **Step 1: Test**

```python
from custom_components.elmax_local.switch import ElmaxOutputSwitch

async def test_switch_turn_on(coord_with_data):
    sw = ElmaxOutputSwitch(coord_with_data, "abc123-uscita-0")
    await sw.async_turn_on()
    coord_with_data.async_send_command.assert_called_with("abc123-uscita-0", "1")
```

- [ ] **Step 2: Create `custom_components/elmax_local/switch.py`**

```python
"""Elmax Local switch entities for panel outputs."""
from __future__ import annotations

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .alarm_control_panel import _device_info
from .const import CMD_OUTPUT_OFF, CMD_OUTPUT_ON, DOMAIN
from .coordinator import ElmaxLocalCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxOutputSwitch(coordinator, eid)
        for eid, out in coordinator.data.outputs.items()
        if out.get("visibile", True)
    )


class ElmaxOutputSwitch(CoordinatorEntity[ElmaxLocalCoordinator], SwitchEntity):
    _attr_has_entity_name = True

    def __init__(self, coordinator: ElmaxLocalCoordinator, endpoint_id: str):
        super().__init__(coordinator)
        self._endpoint_id = endpoint_id
        self._attr_unique_id = f"{DOMAIN}_{endpoint_id}"
        out = coordinator.data.outputs[endpoint_id]
        self._attr_name = out.get("nome") or f"Uscita {out.get('indice', '?')}"
        self._attr_device_info = _device_info(coordinator)

    @property
    def available(self) -> bool:
        return (super().available
                and self.coordinator.data is not None
                and self._endpoint_id in self.coordinator.data.outputs)

    @property
    def is_on(self) -> bool | None:
        if self.coordinator.data is None:
            return None
        out = self.coordinator.data.outputs.get(self._endpoint_id)
        return out.get("aperta") if out else None

    async def async_turn_on(self, **kwargs) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_OUTPUT_ON)

    async def async_turn_off(self, **kwargs) -> None:
        await self.coordinator.async_send_command(self._endpoint_id, CMD_OUTPUT_OFF)
```

- [ ] **Step 3: Tests PASS, commit**

```bash
git add custom_components/elmax_local/switch.py tests/test_entities.py
git commit -m "feat(elmax_local): switch entity for outputs"
```

---

## Task 14: button entity (scenarios)

**Goal:** `ElmaxScenarioButton` per ogni scenario visibile. Buttons sono one-shot, no state.

**Files:**
- Create: `custom_components/elmax_local/button.py`
- Modify: `tests/test_entities.py`

**Acceptance Criteria:**
- [ ] Eredita da `CoordinatorEntity` + `ButtonEntity`
- [ ] `async_press()` chiama `coordinator.async_send_command(eid, None)` (scenari non hanno cmd)

**Verify:** `pytest tests/test_entities.py -k button -v` → PASS

**Steps:**

- [ ] **Step 1: Test**

```python
from custom_components.elmax_local.button import ElmaxScenarioButton

async def test_button_press(coord_with_data):
    btn = ElmaxScenarioButton(coord_with_data, "abc123-scenario-0")
    await btn.async_press()
    coord_with_data.async_send_command.assert_called_with("abc123-scenario-0", None)
```

- [ ] **Step 2: Create `custom_components/elmax_local/button.py`**

```python
"""Elmax Local button entities for scenarios."""
from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .alarm_control_panel import _device_info
from .const import DOMAIN
from .coordinator import ElmaxLocalCoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        ElmaxScenarioButton(coordinator, eid)
        for eid, sc in coordinator.data.scenarios.items()
        if sc.get("visibile", True)
    )


class ElmaxScenarioButton(CoordinatorEntity[ElmaxLocalCoordinator], ButtonEntity):
    _attr_has_entity_name = True
    _attr_icon = "mdi:shield-home"

    def __init__(self, coordinator: ElmaxLocalCoordinator, endpoint_id: str):
        super().__init__(coordinator)
        self._endpoint_id = endpoint_id
        self._attr_unique_id = f"{DOMAIN}_{endpoint_id}"
        sc = coordinator.data.scenarios[endpoint_id]
        self._attr_name = sc.get("nome") or f"Scenario {sc.get('indice', '?')}"
        self._attr_device_info = _device_info(coordinator)

    async def async_press(self) -> None:
        # Scenari non hanno cmd; il payload spec mostra POST /cmd/{eid}/
        # senza segmento. Inviamo None per significare "trigger".
        await self.coordinator.async_send_command(self._endpoint_id, None)
```

- [ ] **Step 3: Tests PASS, commit**

```bash
git add custom_components/elmax_local/button.py tests/test_entities.py
git commit -m "feat(elmax_local): button entity for scenarios"
```

---

## Task 15: `__init__.py` setup wiring

**Goal:** Implementa `async_setup_entry` reale. Crea coordinator, fa setup, forward platforms.

**Files:**
- Modify: `custom_components/elmax_local/__init__.py`
- Create: `tests/test_init.py`

**Acceptance Criteria:**
- [ ] `async_setup_entry`: crea `ElmaxLocalCoordinator`, chiama `async_setup()`, salva in `hass.data[DOMAIN]`, forward platforms
- [ ] Su `ConfigEntryAuthFailed` durante auth → propagate
- [ ] Su altro errore → `ConfigEntryNotReady`
- [ ] `async_unload_entry`: unload platforms, chiama `coordinator.async_shutdown()`
- [ ] `options_listener` registrato: reload su options change

**Verify:** `pytest tests/test_init.py -v` → PASS

**Steps:**

- [ ] **Step 1: Test setup_entry success path**

```python
"""Test __init__.py setup."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.elmax_local import async_setup_entry, async_unload_entry
from custom_components.elmax_local.const import (
    CONF_PANEL_HOST, CONF_PANEL_ID, CONF_PANEL_PIN, DOMAIN,
)


@pytest.fixture
def entry():
    return MockConfigEntry(
        domain=DOMAIN,
        data={CONF_PANEL_ID: "abc", CONF_PANEL_PIN: "000000",
              CONF_PANEL_HOST: "1.2.3.4"},
        options={"reconcile_interval": 90, "enable_ws": True, "enable_mqtt": True},
        entry_id="test_entry",
    )


async def test_setup_entry_success(hass, entry, mock_panel_data):
    entry.add_to_hass(hass)
    with patch("custom_components.elmax_local.ElmaxLocalCoordinator") as MockCoord:
        instance = MockCoord.return_value
        instance.async_setup = AsyncMock()
        instance.async_shutdown = AsyncMock()
        instance.data = MagicMock(zones={}, areas={}, outputs={}, scenarios={})
        result = await async_setup_entry(hass, entry)
        assert result is True
        assert hass.data[DOMAIN]["test_entry"] is instance
```

- [ ] **Step 2: Replace `__init__.py`**

```python
"""Elmax Local — multi-transport integration for Elmax alarm panels."""
from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady

from .auth import ElmaxAuthError
from .const import (
    CONF_ENABLE_MQTT, CONF_ENABLE_WS, CONF_PANEL_HOST, CONF_PANEL_ID,
    CONF_PANEL_PIN, CONF_RECONCILE_INTERVAL, DEFAULT_RECONCILE_INTERVAL,
    DOMAIN, PLATFORMS,
)
from .coordinator import ElmaxLocalCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    coordinator = ElmaxLocalCoordinator(
        hass,
        panel_id=entry.data[CONF_PANEL_ID],
        pin=entry.data[CONF_PANEL_PIN],
        host=entry.data[CONF_PANEL_HOST],
        reconcile_interval=entry.options.get(
            CONF_RECONCILE_INTERVAL, DEFAULT_RECONCILE_INTERVAL),
        enable_ws=entry.options.get(CONF_ENABLE_WS, True),
        enable_mqtt=entry.options.get(CONF_ENABLE_MQTT, True),
    )

    try:
        await coordinator.async_setup()
    except ElmaxAuthError as err:
        if "401" in str(err) or "403" in str(err):
            raise ConfigEntryAuthFailed(str(err)) from err
        raise ConfigEntryNotReady(str(err)) from err
    except Exception as err:
        await coordinator.async_shutdown()
        raise ConfigEntryNotReady(f"Cannot connect: {err}") from err

    if not coordinator.data or not coordinator.data.areas:
        await coordinator.async_shutdown()
        raise ConfigEntryNotReady("No data received from Elmax panel")

    hass.data[DOMAIN][entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator: ElmaxLocalCoordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_shutdown()
    return unload_ok


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload entry on options change."""
    await hass.config_entries.async_reload(entry.entry_id)
```

- [ ] **Step 3: Tests PASS, commit**

```bash
git add custom_components/elmax_local/__init__.py tests/test_init.py
git commit -m "feat(elmax_local): wire async_setup_entry with coordinator lifecycle

Handles ConfigEntryAuthFailed/NotReady correctly. Reload on options change."
```

---

## Task 16: config_flow user step

**Goal:** Flow utente con validazione. MQTT discovery se broker presente; manual fallback.

**Files:**
- Create: `custom_components/elmax_local/config_flow.py`
- Create: `tests/test_config_flow.py`

**Acceptance Criteria:**
- [ ] Step `user`: form con `panel_host`, `panel_id`, `panel_pin`
- [ ] MQTT discovery via `/elmax/request/id` se mqtt integration loaded (5s timeout)
- [ ] Validazione: login HTTP riuscito + match panel_id vs `centrale` in discovery
- [ ] Errori: `cannot_connect`, `invalid_auth`, `panel_id_mismatch`, `unknown`
- [ ] `async_set_unique_id(panel_id)` + abort se already_configured
- [ ] Step `import` per migration service (bypassa validazione)

**Verify:** `pytest tests/test_config_flow.py -k user -v` → PASS

**Steps:**

- [ ] **Step 1: Tests**

```python
"""Test config flow."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from aioresponses import aioresponses
from homeassistant.data_entry_flow import FlowResultType

from custom_components.elmax_local.const import DOMAIN


async def test_user_step_success(hass):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login",
               payload={"token": "JWT eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjk5OTk5OTk5OTl9.x"})
        m.get("https://1.2.3.4/api/v2/discovery",
              payload={"centrale": "abc", "release": "X"})
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "user"}
        )
        assert result["type"] == FlowResultType.FORM
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {"panel_host": "1.2.3.4", "panel_id": "abc", "panel_pin": "000000"},
        )
        assert result["type"] == FlowResultType.CREATE_ENTRY


async def test_user_step_invalid_auth(hass):
    with aioresponses() as m:
        m.post("https://1.2.3.4/api/v2/login", status=401,
               payload={"message": "Forbidden"})
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": "user"}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {"panel_host": "1.2.3.4", "panel_id": "abc", "panel_pin": "wrong"},
        )
        assert result["type"] == FlowResultType.FORM
        assert result["errors"] == {"base": "invalid_auth"}
```

- [ ] **Step 2: Create `config_flow.py`**

```python
"""Config flow for Elmax Local."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.components import mqtt
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
            import aiohttp
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
```

- [ ] **Step 3: Tests PASS, commit**

```bash
git add custom_components/elmax_local/config_flow.py tests/test_config_flow.py
git commit -m "feat(elmax_local): config_flow user step with MQTT discovery and import"
```

---

## Task 17: config_flow zeroconf step

**Goal:** Auto-discovery via mDNS `_elmax-ssl._tcp.local.`. Pre-popola host.

**Files:**
- Modify: `custom_components/elmax_local/config_flow.py` (aggiungi `async_step_zeroconf`)
- Modify: `tests/test_config_flow.py`

**Acceptance Criteria:**
- [ ] `async_step_zeroconf(discovery_info)` riceve `ZeroconfServiceInfo`, estrae host, mostra form per panel_id+pin con host pre-popolato

**Verify:** `pytest tests/test_config_flow.py -k zeroconf -v` → PASS

**Steps:**

- [ ] **Step 1: Test**

```python
from homeassistant.components.zeroconf import ZeroconfServiceInfo

async def test_zeroconf_step(hass):
    info = ZeroconfServiceInfo(
        ip_address="1.2.3.4", ip_addresses=["1.2.3.4"], port=443,
        hostname="elmax-abc.local.", type="_elmax-ssl._tcp.local.",
        name="Elmax abc._elmax-ssl._tcp.local.", properties={},
    )
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": "zeroconf"}, data=info
    )
    assert result["type"] == FlowResultType.FORM
    assert result["step_id"] == "user"
```

- [ ] **Step 2: Add `async_step_zeroconf` to `config_flow.py`**

```python
from homeassistant.components.zeroconf import ZeroconfServiceInfo

async def async_step_zeroconf(self, discovery_info: ZeroconfServiceInfo) -> FlowResult:
    host = str(discovery_info.ip_address)
    self.context["host"] = host
    # Defer to user step with host pre-filled in placeholders
    schema = vol.Schema({
        vol.Required(CONF_PANEL_HOST, default=host): str,
        vol.Required(CONF_PANEL_ID): str,
        vol.Required(CONF_PANEL_PIN): str,
    })
    return self.async_show_form(step_id="user", data_schema=schema)
```

- [ ] **Step 3: Test PASS, commit**

```bash
git commit -am "feat(elmax_local): zeroconf discovery for _elmax-ssl._tcp"
```

---

## Task 18: options_flow

**Goal:** Options flow per attivare/disattivare trasporti + `reconcile_interval`.

**Files:**
- Modify: `custom_components/elmax_local/config_flow.py` (implementa `ElmaxLocalOptionsFlow`)
- Modify: `tests/test_config_flow.py`

**Acceptance Criteria:**
- [ ] Form con `enable_ws`, `enable_mqtt`, `reconcile_interval` (clamped 30-600)
- [ ] HTTP non disattivabile (sempre on)

**Verify:** `pytest tests/test_config_flow.py -k options -v` → PASS

**Steps:**

- [ ] **Step 1: Test**

```python
async def test_options_flow(hass):
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={CONF_PANEL_ID: "abc", CONF_PANEL_PIN: "0", CONF_PANEL_HOST: "1.2.3.4"},
    )
    entry.add_to_hass(hass)
    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] == FlowResultType.FORM
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {"enable_ws": False, "enable_mqtt": True, "reconcile_interval": 120},
    )
    assert result["type"] == FlowResultType.CREATE_ENTRY
    assert result["data"]["reconcile_interval"] == 120
```

- [ ] **Step 2: Replace `ElmaxLocalOptionsFlow.async_step_init`**

```python
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
```

- [ ] **Step 3: Test PASS, commit**

```bash
git commit -am "feat(elmax_local): options_flow for transport toggle + reconcile interval"
```

---

## Task 19: i18n strings

**Goal:** `strings.json` + `translations/it.json`.

**Files:**
- Create: `custom_components/elmax_local/strings.json`
- Create: `custom_components/elmax_local/translations/it.json`

**Acceptance Criteria:**
- [ ] `strings.json` con sezioni `config` (errori + step) e `options`
- [ ] `it.json` traduzione completa

**Verify:** Manual: `cat strings.json | python -m json.tool` → no errors

**Steps:**

- [ ] **Step 1: Create `strings.json`**

```json
{
  "config": {
    "step": {
      "user": {
        "data": {
          "panel_host": "Indirizzo IP centrale",
          "panel_id": "ID centrale",
          "panel_pin": "PIN"
        }
      }
    },
    "error": {
      "cannot_connect": "Impossibile raggiungere la centrale",
      "invalid_auth": "PIN non valido",
      "panel_id_mismatch": "L'ID inserito non corrisponde alla centrale rispondente",
      "unknown": "Errore inatteso"
    },
    "abort": {
      "already_configured": "Questa centrale è già configurata"
    }
  },
  "options": {
    "step": {
      "init": {
        "data": {
          "enable_ws": "WebSocket push (real-time, fw VideoBox ≥ 4.11)",
          "enable_mqtt": "MQTT push + polling (richiede broker MQTT in HA)",
          "reconcile_interval": "Intervallo riconciliazione (secondi)"
        }
      }
    }
  }
}
```

- [ ] **Step 2: Create `translations/it.json`** (copia di `strings.json` — italiano è la lingua sorgente)

- [ ] **Step 3: Validate JSON, commit**

```bash
python -m json.tool custom_components/elmax_local/strings.json > /dev/null
git add custom_components/elmax_local/strings.json custom_components/elmax_local/translations/
git commit -m "feat(elmax_local): i18n strings (it)"
```

---

## Task 20: diagnostic dump

**Goal:** `async_get_config_entry_diagnostics` espone stato trasporti, auth, dati per debug.

**Files:**
- Create: `custom_components/elmax_local/diagnostics.py`
- Create: `tests/test_diagnostics.py`

**Acceptance Criteria:**
- [ ] Dump include: panel_id (redacted), host, panel_info, transports (name/state/last_ts), auth (expires_in, counters), last_update_source, entity_counts
- [ ] PIN redatto

**Verify:** `pytest tests/test_diagnostics.py -v` → PASS

**Steps:**

- [ ] **Step 1: Test**

```python
"""Test diagnostics."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

from custom_components.elmax_local.diagnostics import async_get_config_entry_diagnostics


async def test_diagnostics_redacts_pin(hass, mock_panel_data):
    entry = MagicMock()
    entry.data = {"panel_pin": "000000", "panel_id": "abc", "panel_host": "1.2.3.4"}
    coord = MagicMock()
    coord.panel_id = "abcdef"
    coord.host = "1.2.3.4"
    coord.data = MagicMock(
        panel_info={"release": "X"}, zones={"z1": {}}, areas={"a1": {}},
        outputs={}, scenarios={}, last_update_source="websocket",
    )
    coord.auth = MagicMock(_token="JWT_TOKEN", _expiry=time.time() + 3000,
                          _login_fail_count=0)
    coord.registry = MagicMock()
    coord.registry._transports = []
    hass.data["elmax_local"] = {entry.entry_id: coord}

    dump = await async_get_config_entry_diagnostics(hass, entry)
    assert "000000" not in str(dump)
    assert "JWT_TOKEN" not in str(dump)
```

- [ ] **Step 2: Create `diagnostics.py`**

```python
"""Diagnostic dump for Elmax Local."""
from __future__ import annotations

import time
from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN
from .coordinator import ElmaxLocalCoordinator

TO_REDACT = {"panel_pin", "_pin", "_token", "panel_id"}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry,
) -> dict[str, Any]:
    coord: ElmaxLocalCoordinator = hass.data[DOMAIN][entry.entry_id]
    transports_dump = [
        {
            "name": t.name,
            "state": t.state.value,
            "capabilities": [c.value for c in t.capabilities],
        }
        for t in coord.registry._transports
    ]
    auth_dump = {
        "expires_in": max(0, int(coord.auth._expiry - time.time())) if coord.auth._token else 0,
        "login_fail_count": coord.auth._login_fail_count,
        "blocked_until": coord.auth._blocked_until,
    }
    entity_counts = {
        "zones": len(coord.data.zones) if coord.data else 0,
        "areas": len(coord.data.areas) if coord.data else 0,
        "outputs": len(coord.data.outputs) if coord.data else 0,
        "scenarios": len(coord.data.scenarios) if coord.data else 0,
    }
    raw = {
        "panel_id_suffix": coord.panel_id[-6:] if coord.panel_id else None,
        "host": coord.host,
        "panel_info": coord.data.panel_info if coord.data else {},
        "transports": transports_dump,
        "auth": auth_dump,
        "last_update_source": coord.data.last_update_source if coord.data else None,
        "entity_counts": entity_counts,
    }
    return async_redact_data(raw, TO_REDACT)
```

- [ ] **Step 3: Test PASS, commit**

```bash
git add custom_components/elmax_local/diagnostics.py tests/test_diagnostics.py
git commit -m "feat(elmax_local): diagnostic dump with PIN/token redaction"
```

---

## Task 21: Migration backup helper

**Goal:** Helper che salva snapshot di entity_registry, device_registry e config_entries `elmax_mqtt` su file JSON in `.storage/`.

**Files:**
- Create: `custom_components/elmax_local/migration.py`
- Create: `tests/test_migration.py` (parziale)

**Acceptance Criteria:**
- [ ] `write_backup(hass)` salva dump JSON con timestamp
- [ ] `load_backup(path)` carica dump
- [ ] `find_latest_backup(hass)` ritorna path più recente in `.storage/`
- [ ] Dump include: entity_registry entries con platform=elmax_mqtt; device_registry entries con identifier (elmax_mqtt, *); config_entries elmax_mqtt completi (data + options + unique_id)

**Verify:** `pytest tests/test_migration.py -k backup -v` → PASS

**Steps:**

- [ ] **Step 1: Tests**

```python
"""Test migration helpers."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.elmax_local.const import LEGACY_DOMAIN
from custom_components.elmax_local.migration import (
    find_latest_backup, load_backup, write_backup,
)


async def test_write_and_load_backup(hass, tmp_path):
    entry = MockConfigEntry(
        domain=LEGACY_DOMAIN,
        data={"panel_id": "abc", "panel_pin": "000000", "panel_host": "1.2.3.4"},
        unique_id="abc",
    )
    entry.add_to_hass(hass)
    path = await write_backup(hass, base_dir=tmp_path)
    assert path.exists()
    data = load_backup(path)
    assert any(e["data"]["panel_id"] == "abc" for e in data["config_entries"])


async def test_find_latest_backup(hass, tmp_path):
    p1 = await write_backup(hass, base_dir=tmp_path)
    p2 = await write_backup(hass, base_dir=tmp_path)
    latest = find_latest_backup(tmp_path)
    assert latest == p2
```

- [ ] **Step 2: Create `migration.py`** (backup helpers)

```python
"""Migration helpers for elmax_mqtt → elmax_local.

Backup format JSON conserva entity_registry/device_registry/config_entries
filtrati per LEGACY_DOMAIN. File salvato in <config>/.storage/.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr, entity_registry as er

from .const import LEGACY_DOMAIN

BACKUP_PREFIX = "elmax_local_migration_backup"


async def write_backup(hass: HomeAssistant, base_dir: Path | None = None) -> Path:
    """Dump legacy registries to JSON. Returns path to backup file."""
    base = base_dir or Path(hass.config.path(".storage"))
    base.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    path = base / f"{BACKUP_PREFIX}_{ts}.json"

    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)

    legacy_entries = [
        e for e in hass.config_entries.async_entries(LEGACY_DOMAIN)
    ]
    entries_dump = [
        {
            "entry_id": e.entry_id,
            "data": dict(e.data),
            "options": dict(e.options),
            "unique_id": e.unique_id,
            "title": e.title,
        }
        for e in legacy_entries
    ]

    entities_dump = [
        {
            "entity_id": ent.entity_id,
            "unique_id": ent.unique_id,
            "platform": ent.platform,
            "config_entry_id": ent.config_entry_id,
            "device_id": ent.device_id,
            "disabled_by": ent.disabled_by.value if ent.disabled_by else None,
        }
        for ent in ent_reg.entities.values()
        if ent.platform == LEGACY_DOMAIN
    ]

    legacy_entry_ids = {e["entry_id"] for e in entries_dump}
    devices_dump = [
        {
            "device_id": d.id,
            "identifiers": [list(i) for i in d.identifiers],
            "config_entries": list(d.config_entries),
        }
        for d in dev_reg.devices.values()
        if any(ident[0] == LEGACY_DOMAIN for ident in d.identifiers)
        or any(eid in legacy_entry_ids for eid in d.config_entries)
    ]

    dump = {
        "version": 1,
        "timestamp": ts,
        "config_entries": entries_dump,
        "entities": entities_dump,
        "devices": devices_dump,
    }
    path.write_text(json.dumps(dump, indent=2))
    return path


def load_backup(path: Path) -> dict:
    return json.loads(Path(path).read_text())


def find_latest_backup(base_dir: Path) -> Path | None:
    candidates = sorted(Path(base_dir).glob(f"{BACKUP_PREFIX}_*.json"))
    return candidates[-1] if candidates else None
```

- [ ] **Step 3: Tests PASS, commit**

```bash
git add custom_components/elmax_local/migration.py tests/test_migration.py
git commit -m "feat(elmax_local): migration backup helper (write/load/find)

Snapshot of entity/device/config registries for legacy elmax_mqtt.
Enables rollback if migration fails (spec sez. 9.4)."
```

---

## Task 22: `migrate_from_legacy` service

**Goal:** Service che esegue la migrazione: backup → unload legacy → rewrite registries → create new entry → remove legacy → notify user.

**Files:**
- Modify: `custom_components/elmax_local/migration.py` (aggiunge `async_migrate`)
- Modify: `tests/test_migration.py`

**Acceptance Criteria:**
- [ ] Backup chiamato prima di tutto
- [ ] Entity registry: ogni entry con platform=elmax_mqtt + matching config_entry_id → rewrite a platform=elmax_local + unique_id prefix
- [ ] Device registry: identifier (elmax_mqtt, X) → (elmax_local, X)
- [ ] Nuovo entry elmax_local creato via `flow.async_init({"source": "import"}, data=...)`
- [ ] Options vecchio entry: `scan_interval` → `reconcile_interval = max(scan_interval, 30)`
- [ ] Legacy entry rimosso
- [ ] Persistent notification creato
- [ ] Su failure mid-migration: rollback automatico

**Verify:** `pytest tests/test_migration.py -k migrate -v` → PASS

**Steps:**

- [ ] **Step 1: Test integration**

```python
async def test_migrate_rewrites_registries(hass, mock_panel_data):
    from custom_components.elmax_local.const import DOMAIN

    legacy = MockConfigEntry(
        domain=LEGACY_DOMAIN,
        data={"panel_id": "abc", "panel_pin": "0", "panel_host": "1.2.3.4"},
        options={"scan_interval": 5}, unique_id="abc",
    )
    legacy.add_to_hass(hass)

    ent_reg = er.async_get(hass)
    ent_reg.async_get_or_create(
        domain="binary_sensor", platform=LEGACY_DOMAIN,
        unique_id="elmax_mqtt_abc-zona-0",
        config_entry=legacy, suggested_object_id="zona_01",
    )

    from custom_components.elmax_local.migration import async_migrate
    with patch("custom_components.elmax_local.ElmaxLocalCoordinator") as MC:
        # short-circuit setup so entry creates without real connection
        MC.return_value.async_setup = AsyncMock()
        MC.return_value.data = MagicMock(zones={}, areas={"a":{}}, outputs={}, scenarios={})
        MC.return_value.async_shutdown = AsyncMock()
        await async_migrate(hass)

    # Verify: entity has new platform
    ent = ent_reg.async_get("binary_sensor.zona_01")
    assert ent.platform == DOMAIN
    assert ent.unique_id.startswith("elmax_local_")
```

- [ ] **Step 2: Add `async_migrate` to `migration.py`**

```python
import logging

from homeassistant.components import persistent_notification
from homeassistant.exceptions import HomeAssistantError

from .const import DOMAIN, MIN_RECONCILE_INTERVAL

_LOGGER = logging.getLogger(__name__)


async def async_migrate(hass: HomeAssistant) -> None:
    """Run elmax_mqtt → elmax_local migration. Service handler."""
    legacy_entries = list(hass.config_entries.async_entries(LEGACY_DOMAIN))
    if not legacy_entries:
        raise HomeAssistantError("Nessun config entry 'elmax_mqtt' trovato")

    backup_path = await write_backup(hass)
    _LOGGER.info("Migration backup written to %s", backup_path)

    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)

    try:
        for legacy in legacy_entries:
            panel_id = legacy.data["panel_id"]
            await hass.config_entries.async_unload(legacy.entry_id)

            # Rewrite entity registry
            for ent in list(ent_reg.entities.values()):
                if (ent.platform == LEGACY_DOMAIN
                        and ent.config_entry_id == legacy.entry_id):
                    new_uid = ent.unique_id.replace(
                        f"{LEGACY_DOMAIN}_", f"{DOMAIN}_", 1
                    )
                    ent_reg.async_update_entity(
                        ent.entity_id,
                        new_unique_id=new_uid,
                        new_platform=DOMAIN,
                    )

            # Rewrite device registry
            for dev in list(dev_reg.devices.values()):
                if (LEGACY_DOMAIN, panel_id) in dev.identifiers:
                    new_ids = {
                        (DOMAIN, panel_id) if did == (LEGACY_DOMAIN, panel_id) else did
                        for did in dev.identifiers
                    }
                    dev_reg.async_update_device(dev.id, new_identifiers=new_ids)

            # Create new elmax_local entry via import flow
            legacy_scan = legacy.options.get("scan_interval", 90)
            new_options = {
                "reconcile_interval": max(legacy_scan, MIN_RECONCILE_INTERVAL),
                "enable_ws": True,
                "enable_mqtt": True,
            }
            result = await hass.config_entries.flow.async_init(
                DOMAIN, context={"source": "import"},
                data={
                    "panel_id": panel_id,
                    "panel_pin": legacy.data["panel_pin"],
                    "panel_host": legacy.data["panel_host"],
                },
            )
            if result["type"] != "create_entry":
                raise HomeAssistantError(f"Migration failed: {result}")
            new_entry = result["result"]
            hass.config_entries.async_update_entry(new_entry, options=new_options)

            await hass.config_entries.async_remove(legacy.entry_id)

        persistent_notification.async_create(
            hass,
            "Migrazione elmax_mqtt → elmax_local completata. "
            "Riavvia Home Assistant per applicare. "
            f"Backup: {backup_path}",
            title="Elmax: migrazione",
            notification_id="elmax_local_migration",
        )
    except Exception as err:
        _LOGGER.error("Migration failed: %s. Auto-rollback from %s", err, backup_path)
        await async_rollback(hass, backup_path)
        raise HomeAssistantError(f"Migrazione fallita, rollback eseguito: {err}") from err
```

- [ ] **Step 3: Test PASS, commit**

```bash
git commit -am "feat(elmax_local): migrate_from_legacy service with auto-rollback

Service-driven migration with pre-flight backup. On failure,
auto-rollback from backup file (spec sez. 9.3, 9.4)."
```

---

## Task 23: `rollback_migration` service

**Goal:** Service che ripristina entity/device registries dal backup.

**Files:**
- Modify: `custom_components/elmax_local/migration.py` (aggiunge `async_rollback`)
- Modify: `tests/test_migration.py`

**Acceptance Criteria:**
- [ ] `async_rollback(hass, backup_path=None)`: se path None, usa `find_latest_backup`
- [ ] Per ogni entity nel backup: rewrite platform back a `elmax_mqtt` + unique_id back
- [ ] Per ogni device nel backup: rewrite identifier back
- [ ] Ricrea config_entries elmax_mqtt da backup
- [ ] Rimuove eventuali entry elmax_local creati

**Verify:** `pytest tests/test_migration.py -k rollback -v` → PASS

**Steps:**

- [ ] **Step 1: Test**

```python
async def test_rollback_restores_platform(hass):
    # Setup post-migration state (entities on elmax_local)
    ent_reg = er.async_get(hass)
    # ... create elmax_local entities
    # Create matching backup
    backup_data = {
        "version": 1, "timestamp": 1,
        "config_entries": [{"entry_id": "old", "data": {...},
                            "options": {}, "unique_id": "abc", "title": "Elmax"}],
        "entities": [{"entity_id": "binary_sensor.zona_01",
                      "unique_id": "elmax_mqtt_abc-zona-0",
                      "platform": LEGACY_DOMAIN, "config_entry_id": "old",
                      "device_id": None, "disabled_by": None}],
        "devices": [],
    }
    path = tmp_path / "backup.json"
    path.write_text(json.dumps(backup_data))
    await async_rollback(hass, path)
    ent = ent_reg.async_get("binary_sensor.zona_01")
    assert ent.platform == LEGACY_DOMAIN
    assert ent.unique_id == "elmax_mqtt_abc-zona-0"
```

- [ ] **Step 2: Add `async_rollback` to `migration.py`**

```python
async def async_rollback(
    hass: HomeAssistant,
    backup_path: Path | None = None,
) -> None:
    """Restore entity/device registries from backup."""
    if backup_path is None:
        backup_path = find_latest_backup(Path(hass.config.path(".storage")))
    if backup_path is None or not backup_path.exists():
        raise HomeAssistantError("Nessun backup trovato")

    data = load_backup(backup_path)
    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)

    # Remove any elmax_local entries created during failed migration
    for new_entry in list(hass.config_entries.async_entries(DOMAIN)):
        await hass.config_entries.async_remove(new_entry.entry_id)

    # Restore entity registry
    for ent_data in data["entities"]:
        ent = ent_reg.async_get(ent_data["entity_id"])
        if ent:
            ent_reg.async_update_entity(
                ent.entity_id,
                new_unique_id=ent_data["unique_id"],
                new_platform=ent_data["platform"],
            )

    # Restore device registry identifiers
    for dev_data in data["devices"]:
        dev = dev_reg.async_get(dev_data["device_id"])
        if dev:
            new_ids = {tuple(i) for i in dev_data["identifiers"]}
            dev_reg.async_update_device(dev.id, new_identifiers=new_ids)

    # Restore legacy config entries (best effort — HA may not allow recreation
    # of removed entries with same entry_id; user may need to reconfigure)
    for entry_data in data["config_entries"]:
        existing = hass.config_entries.async_get_entry(entry_data["entry_id"])
        if existing is None:
            # Cannot recreate with same entry_id; warn user
            _LOGGER.warning(
                "Cannot fully restore config entry %s; reconfigure manually "
                "for panel_id=%s",
                entry_data["entry_id"], entry_data["data"].get("panel_id"),
            )

    persistent_notification.async_create(
        hass,
        f"Rollback eseguito da {backup_path}. "
        "Potresti dover riconfigurare il config entry elmax_mqtt manualmente.",
        title="Elmax: rollback",
        notification_id="elmax_local_rollback",
    )
```

- [ ] **Step 3: Test PASS, commit**

```bash
git commit -am "feat(elmax_local): rollback_migration service from backup"
```

---

## Task 24: services.yaml + service registration

**Goal:** Registra i due service nel `__init__.py` + `services.yaml` con descrizioni.

**Files:**
- Create: `custom_components/elmax_local/services.yaml`
- Modify: `custom_components/elmax_local/__init__.py` (aggiunge `async_setup`)

**Acceptance Criteria:**
- [ ] `services.yaml` documenta `migrate_from_legacy` e `rollback_migration`
- [ ] `__init__.async_setup(hass, config)` registra i service handler
- [ ] Service callable da Dev Tools

**Verify:** `pytest tests/test_init.py -k service -v` → PASS

**Steps:**

- [ ] **Step 1: Create `services.yaml`**

```yaml
migrate_from_legacy:
  name: Migra da elmax_mqtt
  description: >
    Migra una configurazione esistente dal legacy custom component
    'elmax_mqtt' a 'elmax_local'. Crea backup automatico in .storage/
    prima di procedere. Va eseguito UNA VOLTA. Richiede restart HA.

rollback_migration:
  name: Rollback migrazione
  description: >
    Ripristina lo stato pre-migrazione dal backup più recente in .storage/.
    Usalo se la migrazione ha causato problemi. Richiede restart HA.
```

- [ ] **Step 2: Modify `__init__.py`** — aggiungi `async_setup` per registrazione service

```python
from homeassistant.core import HomeAssistant, ServiceCall

from .const import SERVICE_MIGRATE, SERVICE_ROLLBACK
from .migration import async_migrate, async_rollback


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Register services on integration load (called once)."""

    async def _handle_migrate(call: ServiceCall) -> None:
        await async_migrate(hass)

    async def _handle_rollback(call: ServiceCall) -> None:
        await async_rollback(hass)

    if not hass.services.has_service(DOMAIN, SERVICE_MIGRATE):
        hass.services.async_register(DOMAIN, SERVICE_MIGRATE, _handle_migrate)
        hass.services.async_register(DOMAIN, SERVICE_ROLLBACK, _handle_rollback)
    return True
```

- [ ] **Step 3: Tests PASS, commit**

```bash
git commit -am "feat(elmax_local): services.yaml + service registration

Exposes migrate_from_legacy and rollback_migration via Dev Tools."
```

---

## Task 25: README + CHANGELOG + version bump

**Goal:** Aggiorna README con guida migration, CHANGELOG con breaking changes.

**Files:**
- Modify: `README.md`
- Create: `CHANGELOG.md`
- (manifest.json già a 2.0.0 da Task 1)

**Acceptance Criteria:**
- [ ] README ha sezione "Migrating from elmax_mqtt v1.x → elmax_local v2.0"
- [ ] CHANGELOG.md ha v2.0.0 con breaking changes elencati
- [ ] Aggiunge una nota su HACS: utenti aggiornano via repo URL nuovo (post-rename)

**Verify:** `cat README.md | grep -i "migrat"` → trovato

**Steps:**

- [ ] **Step 1: Append to `README.md`**

Add new section "## v2.0 — Migration from elmax_mqtt" che include:
- Differenze v1 vs v2 (push-first, multi-trasporto, dominio cambiato)
- Step utente per migrare (install elmax_local accanto a elmax_mqtt, lancia service, restart, disinstalla elmax_mqtt)
- Rollback path
- Link al spec

- [ ] **Step 2: Create `CHANGELOG.md`**

```markdown
# Changelog

## [2.0.0] — 2026-05-XX

### Breaking changes
- Integration domain renamed `elmax_mqtt` → `elmax_local`. Requires running
  service `elmax_local.migrate_from_legacy` once and restarting HA.
- Entity unique_id prefix changed `elmax_mqtt_*` → `elmax_local_*`.
- Option `scan_interval` renamed to `reconcile_interval`. Default raised
  90s (push-first model).

### Added
- WebSocket push transport (`wss://IP/api/v2/push`, fw VideoBox ≥ 4.11).
- MQTT push transport handler (`200 Status Update` messages).
- Auto-detect available transports with periodic retry.
- Options flow with per-transport toggle.
- mDNS discovery (`_elmax-ssl._tcp`).
- Diagnostic dump.
- Services: `migrate_from_legacy`, `rollback_migration`.

### Changed
- Entities now inherit from CoordinatorEntity (proper HA lifecycle,
  derived availability, no more custom dispatcher).
- JWT auth parses real `exp` claim instead of hardcoded 50min TTL.
- Exponential backoff on auth failures prevents "Codice Falso Da PcIP"
  lockout.

### Fixed
- UI freeze during commands (background post-command verify task).
- SSL context created in executor (no longer blocks event loop).

## [1.0.0] — 2026-02-XX
- Initial release.
```

- [ ] **Step 3: Commit**

```bash
git add README.md CHANGELOG.md
git commit -m "docs: README migration guide + CHANGELOG for v2.0"
```

---

## Task 26: Manual smoke test execution

**Goal:** Esegui la checklist sez. 10.2 dello spec su HA reale + centrale reale. Nessun codice — solo verifica.

**Files:**
- Create: `docs/superpowers/smoke-test-2026-05-XX-results.md` (note dei risultati)

**Acceptance Criteria:**
- [ ] Fresh install funziona: config flow → entità → comando OK → push WS visibile in debug log
- [ ] Migration funziona: ambiente con elmax_mqtt → service → entità preservate, storico intatto
- [ ] Failover singolo OK: disabilita WS → MQTT push subentra
- [ ] Failover totale OK: spegni MQTT in HA + WS off → HTTP polling
- [ ] Recovery OK: riaccendi MQTT → entro 5min torna READY

**Verify:** Documentazione risultati in `smoke-test-*.md`

**Steps:**

- [ ] **Step 1: Install elmax_local su HA test (NAS o staging)**
- [ ] **Step 2: Eseguire i 5 scenari della checklist 10.2**
- [ ] **Step 3: Documentare risultati**

Per ciascun test:
```markdown
### Test N: <nome>
**Date:** YYYY-MM-DD HH:MM
**HA version:** 2026.x.x
**Panel fw release_accessorio:** 4.X.X
**Result:** PASS / FAIL
**Notes:** ...
```

- [ ] **Step 4: Se tutti PASS, commit**

```bash
git add docs/superpowers/smoke-test-*.md
git commit -m "docs: smoke test results for v2.0 release candidate"
```

Se qualche test FAIL: aprire issue, fix, re-run.

---

## Task 27: Tag v1.0.0 + branch v1-maintenance + merge v2 → master

**Goal:** Operazioni git finali per rilasciare v2.0.0.

**Files:** none (only git ops)

**Acceptance Criteria:**
- [ ] Tag `v1.0.0` sul commit attuale di `master` (`564f952`)
- [ ] Branch `v1-maintenance` creato da `564f952`
- [ ] Repo GitHub rinominato `ha-elmax-mqtt` → `ha-elmax-local` (GitHub mantiene redirect)
- [ ] Branch `v2-elmax-local` merged in `master`
- [ ] Tag `v2.0.0` sul merge commit
- [ ] Push tutto

**Verify:** `git tag -l` → mostra `v1.0.0` e `v2.0.0`; `git branch -a` → mostra `v1-maintenance` e `master`

**Steps:**

- [ ] **Step 1: Tag v1.0.0**

```bash
git checkout master
git tag -a v1.0.0 564f952 -m "v1.0.0 — initial release (legacy elmax_mqtt)"
```

- [ ] **Step 2: Create v1-maintenance branch**

```bash
git branch v1-maintenance 564f952
```

- [ ] **Step 3: Rename GitHub repo (browser action)**

Vai su GitHub → Settings → Repository name → `ha-elmax-mqtt` → `ha-elmax-local` → Rename.

GitHub mantiene redirect automatici dal vecchio URL per HACS storici.

- [ ] **Step 4: Update remote URL locally**

```bash
git remote set-url origin https://github.com/dconvertini/ha-elmax-local.git
git fetch origin
```

- [ ] **Step 5: Merge v2-elmax-local → master**

```bash
git checkout master
git merge --no-ff v2-elmax-local -m "Release v2.0.0: elmax_local refactor

Push-first multi-transport architecture (WS + MQTT push + HTTP).
Domain rename elmax_mqtt → elmax_local with registries-aware
migration service. Breaking change — see CHANGELOG."
```

- [ ] **Step 6: Tag v2.0.0**

```bash
git tag -a v2.0.0 -m "v2.0.0 — elmax_local refactor"
```

- [ ] **Step 7: Push all**

```bash
git push origin master v1-maintenance v2-elmax-local
git push origin v1.0.0 v2.0.0
```

- [ ] **Step 8: Update HACS (browser action)**

Aggiorna il `hacs.json` se serve il nome integration cambiato.

Rilascio v2.0.0 completo.

---

## Self-Review

**Spec coverage check:**
- Sez. 1 (contesto): tasks 0-1 (scaffolding)
- Sez. 2 (scope): tutti i task in-scope, niente fuori scope nei task
- Sez. 3 (architettura): tasks 7-10 (Registry+Coordinator)
- Sez. 4 (layout): task 1, 2 (file structure)
- Sez. 5 (Transport ABC): task 2
- Sez. 6 (Coordinator+entities): tasks 8-14
- Sez. 7 (auth+probe+error): tasks 3 (auth), 4-7 (transports + registry retry)
- Sez. 8 (config flow): tasks 16-19
- Sez. 9 (migration): tasks 21-24
- Sez. 10 (test+rollout): tasks 0, 25-27
- Sez. 11 (decisions): coperto implicitamente
- Sez. 12 (open questions): rimandate a runtime
- Sez. 13 (DoD): task 26 + 27

**Gap identificato:** Sez. 7.3 menziona "Background retry per trasporti DEGRADED/UNSUPPORTED ogni 5 min". Non c'è task dedicato. Aggiungere ai task 8/9 come parte del Coordinator setup.

**Fix inline:** in Task 8 aggiungere step opzionale per il retry loop:

```python
# In ElmaxLocalCoordinator.async_setup():
self._retry_task = self.hass.async_create_task(self._retry_loop())

async def _retry_loop(self) -> None:
    """Periodically probe DEGRADED/UNSUPPORTED transports."""
    while True:
        await asyncio.sleep(300)  # 5 min
        for t in self.registry.degraded_or_unsupported():
            try:
                if await t.async_probe():
                    await t.async_start(self.auth, self._on_push_state_update)
                    _LOGGER.info("Transport %s recovered", t.name)
            except Exception as err:
                _LOGGER.debug("Retry probe %s failed: %s", t.name, err)
```

Cancellare il task in `async_shutdown`. Documentato qui, da integrare nella implementazione di Task 8/15.

**Placeholder scan:** nessun TBD esplicito.

**Type consistency:** verificate firme `_parse(raw, source)`, `async_send_command(eid, cmd, code)`, `CommandResult(ok, error, raw_response)` in tutti i task.

Plan completo.


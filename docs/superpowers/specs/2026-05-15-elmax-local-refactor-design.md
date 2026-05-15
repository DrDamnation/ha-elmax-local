# Elmax Local — Refactor Design Spec

**Status:** draft, awaiting user review
**Date:** 2026-05-15
**Author:** Daniele Convertini (with Claude brainstorming session)
**Scope:** Refactor del custom component HA `elmax_mqtt` (v1.0.0) in `elmax_local` (v2.0.0), passando da polling MQTT/HTTP a un'architettura push-first multi-trasporto con resilienza ai guasti di canale.

---

## 1. Contesto & motivazione

### 1.1 Stato attuale (verificato sul codice)

Il custom component `elmax_mqtt` v1.0.0 vive in due copie:

- `/volume1/Docker/HomeAssistant/config/custom_components/elmax_mqtt/` (NAS, source of truth corrente)
- `C:\Users\danie\tmp_elmax_mqtt\` (clone di lavoro locale, parzialmente disallineato)

Architettura attuale:

- **Trasporto:** MQTT broker esterno (Mosquitto in HA) come canale primario per lo status (request/response su `/elmax/request/status/{panel_id}` con timeout 8s); fallback HTTP `/api/v2/discovery` dopo 3 fail MQTT consecutivi.
- **Comandi:** sempre HTTP `POST /cmd/{endpoint_id}/{command}`.
- **Polling:** `async_track_time_interval` ogni 5s.
- **Auth:** PIN → JWT con TTL hardcoded 50 min (3000s); backoff implementato solo sull'HTTP login (nella copia NAS).
- **Entità:** 5 alarm_control_panel + ~40 binary_sensor + 5 switch + 9 button = ~60 totali. Filtrate da `visibile=True` nel payload `/discovery`. Ereditano dalla classe base HA, **non** da `CoordinatorEntity`. Sincronizzazione via `async_dispatcher_send/connect` con signal `SIGNAL_UPDATE`.

### 1.2 Problemi identificati

- **Freeze UI HA** durante operazioni. Cause più probabili (verificate nel codice):
  - SSL context creato in `__init__` (sync, blocking) nella copia "root" del repo.
  - `await asyncio.sleep(2)` dentro `async_send_command` che blocca l'entità chiamante.
- **Non sfrutta il push asincrono** che il fw VideoBox espone (sia su WS che su MQTT).
- **JWT TTL ottimistico hardcoded** (3000s vs 1h reale documentata).
- **Le entità non ereditano da CoordinatorEntity**: assenti `available` derivato, debounce nativi, lifecycle standard HA.
- **HTTP 422 (panel busy)** loggati solo in debug, senza retry strategico.
- **Discrepanza tra le due copie** del codice.

### 1.3 Capacità del fw centrale (da `VideoBox-localAPI_V1.6_it.pdf`)

- **WS push** `wss://IP/api/v2/push` da fw VideoBox ≥ 4.11.x. Riceve state spontaneo a ogni cambio + ogni 10 min per livellare disallineamenti. Limite: **1 solo client connesso per sessione**.
- **MQTT API** da fw VideoBox ≥ 4.13.3. Stessi topic `/elmax/request/*` e `/elmax/response/*` veicolati su QUALSIASI broker (interno alla centrale o esterno, scelta deployment). Supporta push asincrono sullo stesso topic di risposta status con `"message": "200 Status Update"`.
- **HTTP API v2** da fw VideoBox ≥ 4.9.8. Rate limit 2 req/s (150 req / 5min).
- **JWT**: TTL 1h, endpoint `/api/v2/refresh` per rinnovo.
- **mDNS** discovery su `_elmax-ssl._tcp`.

---

## 2. Scope

### 2.1 In scope

1. Sostituire il polling-based coordinator con architettura push-first multi-trasporto.
2. Supportare contemporaneamente: WS push, MQTT (push + request/response + comandi), HTTP (polling reconciliation + comandi). Tutti i trasporti disponibili attivi insieme; degradano singolarmente senza far cadere il sistema.
3. Migrare le entità a `CoordinatorEntity` su `DataUpdateCoordinator` HA standard.
4. Auth manager centralizzato con parse di `exp` del JWT e refresh proattivo.
5. Rinominare il dominio: `elmax_mqtt` → `elmax_local`. Migrazione registries con service dedicato, backup, rollback.
6. Auto-detect dei trasporti disponibili sul fw, con retry periodico e override manuale dall'UI.
7. mDNS discovery (`_elmax-ssl._tcp`) per setup semplificato.
8. Rimuovere I/O sync dal main thread; eliminare freeze UI by design.
9. Stabilizzare l'interfaccia `Transport` come ABC, compatibile con un futuro `BusTransport` (RS-485) senza rotture.
10. Source of truth unificato: una sola repo GitHub, NAS aggiorna via HACS.

### 2.2 Out of scope (esplicitamente)

- Implementazione di `BusTransport` (solo interfaccia garantita; implementazione in v2.1+).
- Esposizione di `/api/v2/faults` come binary sensors (504 bit di anomalie).
- Esposizione di `/api/v2/events` come `EventEntity`.
- Entità per `tapparelle`, `gruppi`, `users`, `clock`.
- CI GitHub Actions / automated release pipeline.
- Migration tooling per chi avesse installato un `elmax_bus` standalone in passato.

---

## 3. Architettura

### 3.1 Diagramma logico

```
┌─────────────────────────────────────────────────────────────┐
│                      Home Assistant                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              ElmaxLocalCoordinator                    │   │
│  │           (estende DataUpdateCoordinator)             │   │
│  │  ┌────────────────────────────────────────────────┐  │   │
│  │  │  TransportRegistry — orchestra N trasporti     │  │   │
│  │  │  + AuthManager (JWT, condiviso fra trasporti)  │  │   │
│  │  └────────────────────────────────────────────────┘  │   │
│  └──────┬─────────────┬───────────────┬─────────────────┘   │
│         │             │               │                      │
│  ┌──────▼─────┐ ┌─────▼──────┐ ┌─────▼──────┐               │
│  │WebSocket   │ │MQTT        │ │HTTP        │  (BusTrans.   │
│  │Transport   │ │Transport   │ │Transport   │   futuro v2.1)│
│  │push        │ │push +      │ │polling +   │               │
│  │            │ │req/resp    │ │comandi     │               │
│  └──────┬─────┘ └─────┬──────┘ └─────┬──────┘               │
└─────────┼─────────────┼──────────────┼──────────────────────┘
          │             │              │
     wss://IP/         (broker     https://IP/api/v2/...
     api/v2/push      Mosquitto     login,discovery,cmd
                       esterno)
                                    Centrale Elmax (192.168.1.42)
```

### 3.2 Ruoli per trasporto

| Trasporto | Push state | Polling state | Commands | Auth header location |
|---|---|---|---|---|
| WS `/api/v2/push` | primary | n/a | n/a | HTTP `Authorization` header |
| MQTT | secondary | on demand (fallback HTTP) | fallback se HTTP down | JWT nel payload JSON |
| HTTP `/api/v2/*` | n/a | reconciliation lenta (90s default) | primary | HTTP `Authorization` header |

### 3.3 Modello operativo

- I trasporti push (WS, MQTT) attivi invocano `coordinator.async_set_updated_data()` a ogni evento.
- Il `DataUpdateCoordinator` esegue il poll di reconciliation ogni `reconcile_interval` secondi (default 90s, range 30-600s). Se l'ultimo push è arrivato a meno di `reconcile_interval / 2` secondi, il poll è skippato (no-op).
- I comandi sono inviati via HTTP (primary) con fallback MQTT command. Dopo l'invio, un task in background attende 3s; se nessun push è arrivato, forza un poll di reconciliation.

---

## 4. Module layout

```
custom_components/elmax_local/
├── __init__.py              # async_setup_entry, services, lifecycle
├── manifest.json            # domain="elmax_local", version="2.0.0", zeroconf
├── const.py                 # costanti, topic MQTT, comandi, mapping stati
├── coordinator.py           # ElmaxLocalCoordinator(DataUpdateCoordinator)
├── auth.py                  # AuthManager: JWT login, refresh, parse exp
├── config_flow.py           # config flow + options flow
├── strings.json
├── translations/
│   └── it.json
│
├── transport/
│   ├── __init__.py          # Transport ABC + TransportRegistry + enums
│   ├── http.py              # HttpTransport
│   ├── mqtt.py              # MqttTransport
│   └── websocket.py         # WebSocketTransport
│
├── alarm_control_panel.py   # ElmaxAlarmPanel(CoordinatorEntity, ...)
├── binary_sensor.py         # ElmaxZoneSensor(CoordinatorEntity, ...)
├── switch.py                # ElmaxOutputSwitch(CoordinatorEntity, ...)
└── button.py                # ElmaxScenarioButton(CoordinatorEntity, ...)
```

Test fuori-tree (in `tests/` della repo); nessun file di test in `custom_components/`.

---

## 5. `Transport` interface (contratto stabile)

Questa è l'interfaccia definitiva. Vincola `HttpTransport`, `MqttTransport`, `WebSocketTransport` e il futuro `BusTransport`. Modifiche post-approvazione richiedono un nuovo spec.

```python
# custom_components/elmax_local/transport/__init__.py
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from ..auth import AuthManager

StateUpdateCallback = Callable[[dict], Awaitable[None]]


class TransportCapability(Enum):
    PUSH = "push"           # emette state updates spontaneamente
    POLL = "poll"           # supporta fetch_state on demand
    COMMAND = "command"     # supporta send_command


class TransportState(Enum):
    DISABLED = "disabled"       # spento dall'utente via config
    PROBING = "probing"         # probe in corso
    READY = "ready"             # operativo
    DEGRADED = "degraded"       # errori recenti, in retry
    UNSUPPORTED = "unsupported" # fw non lo supporta (probe ha risposto "no")


@dataclass(frozen=True)
class CommandResult:
    ok: bool
    error: str | None = None      # human-readable se ok=False
    raw_response: dict | None = None


class Transport(ABC):
    """Contratto per un canale di comunicazione con una centrale Elmax.

    Un Transport è agnostico rispetto al dominio (zone/aree/uscite): riceve
    payload grezzi conformi allo schema /api/v2/discovery e li gira al
    Coordinator via callback. Non interpreta lo stato.
    """

    name: str                                       # "websocket"|"mqtt"|"http"|"bus"
    capabilities: frozenset[TransportCapability]

    @property
    @abstractmethod
    def state(self) -> TransportState: ...

    @abstractmethod
    async def async_probe(self) -> bool:
        """Verifica disponibilità sul fw corrente. Idempotente, senza side-effect.
        Timeout interno consigliato: 10s."""

    @abstractmethod
    async def async_start(
        self,
        auth: AuthManager,
        on_state_update: StateUpdateCallback,
    ) -> None:
        """Avvia il trasporto. PUSH inizia ad ascoltare; POLL/COMMAND inizializza
        il client."""

    @abstractmethod
    async def async_stop(self) -> None:
        """Ferma il trasporto. Idempotente. Dopo stop deve essere riavviabile."""

    async def async_fetch_state(self) -> dict | None:
        """On demand fetch (capability POLL). Default raise.
        Returns: payload schema /api/v2/discovery, o None se fallisce."""
        raise NotImplementedError(f"{self.name} does not support POLL")

    async def async_send_command(
        self,
        endpoint_id: str,
        cmd: str | None,
        code: str | None = None,
    ) -> CommandResult:
        """Invia comando (capability COMMAND). Default raise.

        Args:
            endpoint_id: es. "010203040506-area-0".
            cmd: stringa comando ("4"=arm_total, "off"=disarm, ecc.).
                None per zone (toggle inclusione).
            code: PIN richiesto SOLO per disinserimento area.
        """
        raise NotImplementedError(f"{self.name} does not support COMMAND")
```

### 5.1 Capability per implementazione

| Trasporto | PUSH | POLL | COMMAND | Note |
|---|---|---|---|---|
| HttpTransport | no | sì | sì | sempre abilitato; probe = POST `/api/v2/login` con PIN reale |
| MqttTransport | sì | sì | sì | richiede integration `mqtt` attiva in HA |
| WebSocketTransport | sì | no | no | richiede 1 solo client; reconnect con backoff |
| BusTransport (v2.1+) | sì | sì | sì | seriale RS-485; `auth` ignorato |

### 5.2 `TransportRegistry`

Orchestratore. Mantiene la lista, instrada operazioni.

```python
class TransportRegistry:
    def __init__(self, transports: list[Transport]): ...
    async def async_start_all(self, auth, on_push) -> None: ...
    async def async_stop_all(self) -> None: ...
    async def async_fetch_state(self) -> dict | None:
        """Prova HTTP (primary) → MQTT req/resp (fallback)."""
    async def async_send_command(self, eid, cmd, code=None) -> CommandResult:
        """Prova HTTP (primary) → MQTT command (fallback)."""
    def get_active_push_transports(self) -> list[Transport]: ...
    def degraded_or_unsupported(self) -> list[Transport]: ...
```

---

## 6. Coordinator & data flow

### 6.1 Stato tipizzato

```python
@dataclass
class ElmaxState:
    panel_info: dict        # release, tipo_accessorio, release_accessorio, centrale
    zones: dict[str, dict]  # endpoint_id → zone payload
    areas: dict[str, dict]
    outputs: dict[str, dict]
    scenarios: dict[str, dict]
    last_update_source: str # "websocket" | "mqtt" | "http"
    last_update_ts: float
```

### 6.2 Coordinator

```python
class ElmaxLocalCoordinator(DataUpdateCoordinator[ElmaxState]):
    def __init__(self, hass, panel_id, pin, host, options):
        super().__init__(
            hass, _LOGGER, name=f"Elmax {panel_id}",
            update_interval=timedelta(seconds=options.reconcile_interval),
        )
        self.panel_id = panel_id
        self.auth = AuthManager(hass, host, pin)
        self.registry = TransportRegistry([...])  # vedi sez 5.1

    async def _async_update_data(self) -> ElmaxState:
        """Reconciliation poll. Skipped se push è recente."""
        if self._push_is_fresh():
            return self.data
        raw = await self.registry.async_fetch_state()
        if raw is None:
            raise UpdateFailed("All polling transports failed")
        return self._parse(raw)

    async def _on_push_state_update(self, raw: dict) -> None:
        """Callback dei trasporti PUSH."""
        self.async_set_updated_data(self._parse(raw))

    async def async_send_command(self, eid, cmd, code=None) -> bool:
        result = await self.registry.async_send_command(eid, cmd, code)
        if result.ok:
            self.hass.async_create_task(self._post_command_verify())
        return result.ok

    async def _post_command_verify(self):
        await asyncio.sleep(3)
        if (time.time() - self.data.last_update_ts) > 3:
            await self.async_request_refresh()
```

### 6.3 Entità

Tutte ereditano da `CoordinatorEntity[ElmaxLocalCoordinator]` + classe base HA (`BinarySensorEntity`, `AlarmControlPanelEntity`, `SwitchEntity`, `ButtonEntity`).

Pattern comune:

- `unique_id` formato `elmax_local_{endpoint_id}` (es. `elmax_local_010203040506-area-0`).
- `device_info` con identifier `(DOMAIN, panel_id)`.
- `available` derivato da `super().available and endpoint_id in coordinator.data.{zones|areas|outputs}`.
- State letto da `self.coordinator.data` (no più dispatcher, no più variabili locali).
- Comandi tramite `self.coordinator.async_send_command(...)`.

---

## 7. Auth, probe, error handling

### 7.1 `AuthManager`

```python
class AuthManager:
    REFRESH_MARGIN = 600  # rinnova 10 min prima di exp

    def __init__(self, hass, host, pin):
        self._host = host
        self._pin = pin
        self._token: str | None = None
        self._expiry: float = 0
        self._lock = asyncio.Lock()
        self._login_fail_count = 0
        self._blocked_until = 0

    async def async_get_token(self) -> str:
        async with self._lock:
            now = time.time()
            if self._token and now < self._expiry - self.REFRESH_MARGIN:
                return self._token
            if self._token and now < self._expiry:
                if await self._try_refresh():
                    return self._token
            await self._do_login()
            return self._token

    async def async_handle_401(self):
        async with self._lock:
            self._token = None
            self._expiry = 0

    def _parse_exp(self, jwt: str) -> float:
        """Decodifica base64 del payload (no verifica firma). Estrae exp.
        Fallback 3000s se mancante."""

    async def _do_login(self) -> None:
        """POST /api/v2/login con PIN. Applica backoff su errori."""

    async def _try_refresh(self) -> bool:
        """POST /api/v2/refresh con token corrente."""
```

### 7.2 Backoff login

| HTTP code | Significato | Backoff |
|---|---|---|
| 200 | OK | reset (fail_count = 0) |
| 401/403 | PIN errato / lockout "Codice Falso Da PcIP" | 30s → 60s → 120s → ... → 600s max (`2^n`) |
| 422 | panel busy | 5s, retry singolo |
| 502/503 | backend down | `30s * n`, max 600s |
| timeout/connect_error | rete giù | come 503 |

### 7.3 Probe lifecycle

Setup:

1. `AuthManager.async_get_token()` via HTTP. Se 401 → `ConfigEntryAuthFailed`. Se 5xx/timeout → `ConfigEntryNotReady`.
2. Per ogni transport non DISABLED: `await transport.async_probe()` (timeout 10s) → READY o UNSUPPORTED/DEGRADED.
3. Per i READY: `await transport.async_start(auth, on_push)`.
4. `coordinator.async_config_entry_first_refresh()`.

Background retry: ogni 5 min, su trasporti DEGRADED/UNSUPPORTED, richiama `probe()` → se ora supportato, `start()`.

### 7.4 Per-transport error handling

| Errore | WebSocket | MQTT | HTTP |
|---|---|---|---|
| Auth 401 | close → AuthManager.handle_401 → reconnect | publish refresh; se fail, login | handle_401 → retry una volta |
| Network timeout | reconnect backoff 5s→120s | broker gestito da HA | UpdateFailed → coord ritenta al prossimo tick |
| Payload malformato | log warning, drop | log warning, drop | log warning, drop |
| Disconnect (WS) | reconnect backoff esponenziale | n/a | n/a |
| Nessun push da N×interval | DEGRADED | DEGRADED | n/a (è poll) |

### 7.5 Rate limiting

Documentato 2 req/s (150/5min). Operatività concreta:

- Reconcile poll ogni 90s = ~0.7 req/min
- Comandi sporadici
- Login + refresh sporadici

Margine di ordini di grandezza. Non implementiamo token bucket; il lock di `DataUpdateCoordinator` impedisce poll concorrenti.

### 7.6 Eliminazione freeze UI

Cause identificate nel codice attuale, fix nel refactor:

1. SSL context sync in main thread → spostato in `async_add_executor_job` (già fatto nella copia NAS, blindato nel refactor).
2. `await asyncio.sleep(2)` post-command bloccante → spostato in task background (`_post_command_verify`).
3. Polling timer manuale → sostituito da `DataUpdateCoordinator` con lock interno.

---

## 8. Config flow & options

### 8.1 Config flow `user`

- Campi: `panel_host`, `panel_id` (con dropdown da MQTT discovery se disponibile), `panel_pin`.
- Validazione: `AuthManager` esegue login + GET `/discovery`, verifica match `panel_id` ↔ `centrale` ritornata.
- Errori: `cannot_connect`, `invalid_auth`, `panel_id_mismatch`, `unknown`.

### 8.2 Config flow `zeroconf` (mDNS)

- Listener su `_elmax-ssl._tcp.local.`.
- Auto-popola `panel_host` da hostname risolto.
- Step UI per inserire `panel_id` (se non in TXT record) e `pin`.

### 8.3 Options flow

Campi:

- Trasporti (checkbox): WebSocket, MQTT, HTTP (HTTP non disattivabile — serve per comandi).
- `reconcile_interval` (int, 30-600s, default 90).

Reload automatico dell'entry su salvataggio.

### 8.4 Diagnostic dump

`async_get_config_entry_diagnostics` espone (PIN redatto):

```json
{
  "panel_id": "abc***",
  "host": "192.168.1.42",
  "panel_info": {...},
  "transports": [
    {"name": "websocket", "state": "ready", "last_message_ts": "..."},
    ...
  ],
  "auth": {"token_expires_in": 1843, "refresh_count": 3, "login_count": 1},
  "last_update_source": "websocket",
  "entity_counts": {"zones": 40, "areas": 5, "outputs": 5, "scenarios": 9}
}
```

---

## 9. Migration `elmax_mqtt` → `elmax_local`

### 9.1 Cosa cambia

| Elemento | Old | New |
|---|---|---|
| Integration domain | `elmax_mqtt` | `elmax_local` |
| Config entry domain | `elmax_mqtt` | `elmax_local` (nuovo entry, vecchio rimosso) |
| Entity `entity_id` | invariato | invariato (storico preservato) |
| Entity `unique_id` | `elmax_mqtt_{eid}` | `elmax_local_{eid}` |
| Entity registry `platform` | `elmax_mqtt` | `elmax_local` |
| Device identifier | `(elmax_mqtt, panel_id)` | `(elmax_local, panel_id)` |
| Options key | `scan_interval` (5s) | `reconcile_interval` (`max(scan_interval, 30)`) |

### 9.2 Strategia: service-driven

HA non permette ad `async_migrate_entry` di cambiare domain. Migration è esplicita via service.

Flusso utente:

1. Installa `elmax_local` accanto a `elmax_mqtt` (domini diversi, coesistono).
2. Lancia `elmax_local.migrate_from_legacy` da Developer Tools.
3. Restart HA.
4. (Opzionale) Disinstalla `elmax_mqtt` da HACS.

### 9.3 Algoritmo del service

```python
async def _async_handle_migrate(call: ServiceCall):
    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)
    cfg = hass.config_entries

    # Backup pre-migration su file
    await _write_backup(hass, ent_reg, dev_reg)

    legacy_entries = cfg.async_entries("elmax_mqtt")
    if not legacy_entries:
        raise HomeAssistantError("Nessun config entry 'elmax_mqtt' trovato")

    for legacy in legacy_entries:
        panel_id = legacy.data["panel_id"]
        await cfg.async_unload(legacy.entry_id)

        # Rewrite entity registry
        for ent in list(ent_reg.entities.values()):
            if ent.platform == "elmax_mqtt" and ent.config_entry_id == legacy.entry_id:
                new_uid = ent.unique_id.replace("elmax_mqtt_", "elmax_local_", 1)
                ent_reg.async_update_entity(
                    ent.entity_id,
                    new_unique_id=new_uid,
                    new_platform="elmax_local",
                )

        # Rewrite device registry
        for dev in list(dev_reg.devices.values()):
            if ("elmax_mqtt", panel_id) in dev.identifiers:
                new_ids = {
                    ("elmax_local", panel_id) if did == ("elmax_mqtt", panel_id) else did
                    for did in dev.identifiers
                }
                dev_reg.async_update_device(dev.id, new_identifiers=new_ids)

        # Create new elmax_local entry
        legacy_scan = legacy.options.get("scan_interval", 90)
        new_options = {"reconcile_interval": max(legacy_scan, 30)}
        result = await cfg.flow.async_init(
            "elmax_local",
            context={"source": "import"},
            data={
                "panel_id": panel_id,
                "panel_pin": legacy.data["panel_pin"],
                "panel_host": legacy.data["panel_host"],
            },
        )
        if result["type"] != "create_entry":
            await _rollback_from_backup(hass)
            raise HomeAssistantError(f"Migrazione fallita: {result}")

        # Apply options to new entry
        new_entry_id = result["result"].entry_id
        cfg.async_update_entry(
            cfg.async_get_entry(new_entry_id), options=new_options
        )

        await cfg.async_remove(legacy.entry_id)

    persistent_notification.async_create(
        hass,
        "Migrazione completata. Riavvia Home Assistant per applicare.",
        title="Elmax: migrazione",
    )
```

### 9.4 Backup e rollback

- Pre-migration: dump di entity_registry, device_registry, config_entries.elmax_mqtt su `.storage/elmax_local_migration_backup_{timestamp}.json`.
- Rollback service `elmax_local.rollback_migration`: ricostruisce le voci dal backup.

### 9.5 Test di acceptance migration

Da eseguire prima di dichiarare v2.0 stabile:

1. Pre-state: HA con `elmax_mqtt` attivo, ~60 entità, storico ≥ 24h, ≥1 automazione che referenzia un `entity_id` Elmax.
2. Run: lancio service `elmax_local.migrate_from_legacy`.
3. Post-state (dopo restart):
   - Numero entità invariato.
   - Tutti gli `entity_id` invariati.
   - Storico accessibile (chart con dati pre-migration).
   - Automazione di test scatta.
   - UI mostra solo "Elmax Local"; `elmax_mqtt` assente.
   - `Configuration → Devices` mostra device con identifier `elmax_local`.
   - `reconcile_interval` corretto nel nuovo entry options.

---

## 10. Testing & rollout

### 10.1 Unit test

`pytest` + `pytest-homeassistant-custom-component`. Mock per HTTP (`aioresponses`), MQTT (HA builtin), WS (mini server di test).

Coverage minima:

- `auth.py`: parse_exp valid/expired/malformed; refresh proattivo; lock concorrenza; backoff codes.
- `transport/http.py`: probe ok/401/timeout; fetch_state parsing; send_command 200/401/422/503; backoff 403.
- `transport/mqtt.py`: probe; push handler distingue `200 Status OK` vs `200 Status Update`; 401 → refresh.
- `transport/websocket.py`: connect+handshake; reconnect su disconnect; auth header.
- `coordinator.py`: parse mappa campi italiani; push fresh skip; post-command verify.
- `config_flow.py`: user/zeroconf/import/options; tutti gli errori.
- `__init__.py` migration: rewrite registries; backup/rollback.

### 10.2 Smoke test manuale (release checklist)

1. Fresh install: config flow → entità create → comando arm/disarm OK → push WS in debug log.
2. Migration: ambiente con `elmax_mqtt` → service → checklist 9.5.
3. Failover singolo: disabilita WS in UI → MQTT prende push → updates ancora ricevuti.
4. Failover totale push: spegni MQTT in HA + disabilita WS → HTTP polling 90s → entità si aggiornano (con ritardo).
5. Recovery: riaccendi MQTT → entro 5 min ridiventa READY (retry loop).

### 10.3 Versioning & repo

- `manifest.json`: `1.0.0` → `2.0.0` (breaking).
- Repo GitHub: rename `ha-elmax-mqtt` → `ha-elmax-local` (GitHub mantiene redirect storici per HACS).
- Branch `v1-maintenance` taggato `v1.x` per fix critici su utenti non ancora migrati.
- Branch `main` = v2.x.

### 10.4 Source of truth post-refactor

Repo GitHub. NAS aggiorna via HACS. La copia locale `C:\Users\danie\tmp_elmax_local\` è un working clone, gestito sotto git, sincronizzato con `git push/pull`. Eliminato il pattern "due copie da tenere allineate a mano".

---

## 11. Decisions log (sintesi brainstorm)

| # | Decisione | Motivazione |
|---|---|---|
| 1 | Tutti i trasporti supportati (no scelta singolo) | Massima resilienza richiesta dall'utente |
| 2 | Modello push-first + polling reconciliation | Equilibrio real-time + safety net, complessità gestibile |
| 3 | Dominio rinominato `elmax_mqtt` → `elmax_local` | Il dominio attuale è fuorviante (non è più solo MQTT) |
| 4 | Auto-detect + retry periodico + override UI | UX: utente comune non sa il fw; power user può forzare |
| 5 | Strategy pattern con Transport ABC | Isolamento per testabilità + estensibilità (BusTransport futuro) |
| 6 | BusTransport futuro = trasporto interno a `elmax_local`, NON componente separato | Stessa centrale, stesso device, stesse entità |
| 7 | Migration via service esplicito, non auto-migration | HA non permette cambio domain in `async_migrate_entry` |
| 8 | mDNS discovery in scope | Costo basso, valore evidente |
| 9 | `faults` / `events` / `tapparelle` / `gruppi` fuori scope | Scope creep; candidate per v2.1+ |
| 10 | CI/CD fuori scope | Repo personale, non bloccante |

---

## 12. Open questions

Cose da chiarire durante implementazione, non bloccanti per lo spec:

1. **Versione fw effettiva della centrale dell'utente.** Determina quali trasporti saranno effettivamente READY. Verificabile dal payload `/api/v2/discovery` campo `release_accessorio` una volta che il refactor è in test.
2. **Schema esatto della response di `/refresh`** vs `/login`. Il PDF mostra entrambi ritornano `{"token": "JWT ..."}`. Da verificare in pratica se il JWT cambia o se è invariato.
3. **Comportamento di MQTT push quando il broker è temporaneamente disconnesso.** HA gestisce il reconnect; ma serve verificare che dopo reconnect i topic `/elmax/response/*` ricomincino a popolarsi senza re-subscribe esplicito.
4. **`zoneBmask` di `aree`.** L'attributo è disponibile; valutiamo se esporlo come `extra_state_attributes` dell'`alarm_control_panel`. Decisione: sì, è informativo, costo zero.

---

## 13. Definition of done

V2.0 è considerato implementato quando:

1. Tutti i moduli del layout (sez. 4) esistono e implementano i contratti.
2. `Transport` ABC è esattamente come sez. 5.
3. `HttpTransport`, `MqttTransport`, `WebSocketTransport` implementano i comportamenti delle tabelle in sez. 5.1 e 7.4.
4. `ElmaxLocalCoordinator` rispetta il flow di sez. 6 (push-first + reconcile + post-command verify).
5. Entità ereditano da `CoordinatorEntity`.
6. `AuthManager` rispetta sez. 7.1 (parse exp + refresh proattivo + backoff).
7. Config flow + options flow + zeroconf + diagnostic dump implementati per sez. 8.
8. Service `migrate_from_legacy` + `rollback_migration` implementati con backup, testati per sez. 9.5.
9. Smoke test sez. 10.2 passato almeno una volta su HA reale + centrale reale.
10. README aggiornato con guida migration.
11. Manifest `version: 2.0.0`, repo rinominato, CHANGELOG con breaking change.

---

## Appendix A — riferimenti

- `F:\Users\danie\Download\elmax\VideoBox-localAPI_V1.6_it.pdf` — API HTTP/WS/MQTT VideoBox.
- `F:\Users\danie\Download\elmax\Manuale_Dispositivi_IoT_V3.0.pdf` — Dispositivi IoT Elmax (broker MQTT integrato fw 4.13.1+).
- `C:\Users\danie\tmp_elmax_mqtt\` — codice corrente del custom component v1.0.0 (riferimento "as-is").
- `C:\Users\danie\tmp_contactvideo\PROGETTO_ELMAX_HA.md` — visione complessiva del progetto Elmax HA.

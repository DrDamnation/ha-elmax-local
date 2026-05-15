# Smoke Test Results — v2.0.0 Release Candidate

Manuale: `docs/superpowers/specs/2026-05-15-elmax-local-refactor-design.md` sez. 10.2.

**Tester:** Daniele Convertini
**HA instance:** _da compilare_
**Branch:** `v2-elmax-local`
**Commit:** _da compilare con `git rev-parse HEAD`_

> Compila ciascuna sezione dopo aver eseguito il test sul sistema reale.
> Se un test FAIL, apri issue e linka qui prima del merge in master.

---

## Test 1 — Fresh install

Installazione pulita di `elmax_local` su una centrale mai integrata prima.

**Date:** YYYY-MM-DD HH:MM
**HA version:** 2026.x.x
**Panel fw `release_accessorio`:** 4.X.X
**Result:** PENDING

**Verifica:**
- [ ] Config flow apre la form `user`
- [ ] Dopo submit con credenziali valide, l'entry viene creata
- [ ] Entità (`alarm_control_panel`, `binary_sensor`, `switch`, `button`) appaiono
- [ ] Un comando `arm_away` esegue OK
- [ ] Nei debug log si vede un evento push (WS o MQTT) entro pochi secondi

**Notes:** ...

---

## Test 2 — Migration from elmax_mqtt

Su HA con `elmax_mqtt` v1.0.0 già configurato.

**Date:** YYYY-MM-DD HH:MM
**Result:** PENDING

**Verifica:**
- [ ] Prima del test: nota gli `entity_id` legacy (es. `binary_sensor.zona_01`)
- [ ] Service `elmax_local.migrate_from_legacy` chiamato da Dev Tools
- [ ] Notification "Migrazione completata" appare; conferma path del backup
- [ ] Riavvio HA
- [ ] Entità mantengono gli stessi `entity_id` (storico Recorder preservato)
- [ ] `unique_id` ora prefissato `elmax_local_*` (verificare in entity registry)
- [ ] Vecchio entry `elmax_mqtt` rimosso da config entries

**Notes:** ...

---

## Test 3 — Failover singolo (WS off → MQTT push subentra)

**Date:** YYYY-MM-DD HH:MM
**Result:** PENDING

**Verifica:**
- [ ] Disabilita WebSocket dalle Options del config entry
- [ ] Reload entry
- [ ] Un'azione manuale sulla centrale (es. apri zona) propaga in HA entro ~1s via MQTT
- [ ] Diagnostic dump mostra `transports.websocket.state == "disabled"` e
      `transports.mqtt.state == "ready"`

**Notes:** ...

---

## Test 4 — Failover totale (no push, solo HTTP polling)

**Date:** YYYY-MM-DD HH:MM
**Result:** PENDING

**Verifica:**
- [ ] Disabilita sia WS che MQTT in Options; reload
- [ ] Cambio stato in centrale propaga entro `reconcile_interval` (90s default)
- [ ] Comando `arm_away` da HA → centrale risponde
- [ ] Diagnostic dump mostra solo `transports.http.state == "ready"`

**Notes:** ...

---

## Test 5 — Recovery (MQTT torna online)

**Date:** YYYY-MM-DD HH:MM
**Result:** PENDING

**Verifica:**
- [ ] Partendo da Test 4 (solo HTTP), spegni e poi riaccendi il broker MQTT
- [ ] Riabilita MQTT in Options; reload
- [ ] Entro 5 minuti la registry detecta il trasporto e lo riporta a READY
- [ ] Push event arriva su zona aperta

**Notes:** ...

---

## Esito complessivo

- [ ] Tutti i test PASS → procedi con Task 27 (release ops)
- [ ] Almeno un FAIL → tracciare issue prima del merge

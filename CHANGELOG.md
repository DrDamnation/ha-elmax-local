# Changelog

## [2.0.0] — 2026-05-15

### Breaking changes
- Integration domain renamed `elmax_mqtt` → `elmax_local`. Requires running
  service `elmax_local.migrate_from_legacy` once and restarting HA.
- Entity unique_id prefix changed `elmax_mqtt_*` → `elmax_local_*`.
- Option `scan_interval` renamed to `reconcile_interval`. Default raised to
  90s (push-first model).

### Added
- WebSocket push transport (`wss://IP/api/v2/push`, fw VideoBox ≥ 4.11).
- MQTT push transport handler (`200 Status Update` messages).
- Auto-detect available transports with periodic retry.
- Options flow with per-transport toggle.
- mDNS discovery (`_elmax-ssl._tcp`).
- Diagnostic dump (`async_get_config_entry_diagnostics` with PIN/token
  redaction).
- Services: `migrate_from_legacy`, `rollback_migration`.

### Changed
- Entities now inherit from `CoordinatorEntity` (proper HA lifecycle,
  derived availability, no more custom dispatcher).
- JWT auth parses real `exp` claim instead of hardcoded 50min TTL.
- Exponential backoff on auth failures prevents "Codice Falso Da PcIP"
  lockout.

### Fixed
- UI freeze during commands (background post-command verify task).
- SSL handling delegated to `async_get_clientsession(hass, verify_ssl=False)`
  — no more manual SSL context creation in the event loop or executor.

## [1.0.0] — 2026-02-XX
- Initial release.

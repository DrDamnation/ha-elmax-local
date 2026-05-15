"""Migration helpers for elmax_mqtt -> elmax_local.

Backup format JSON conserva entity_registry/device_registry/config_entries
filtrati per LEGACY_DOMAIN. File salvato in <config>/.storage/.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from homeassistant.components import persistent_notification
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr, entity_registry as er

from .const import DOMAIN, LEGACY_DOMAIN, MIN_RECONCILE_INTERVAL

_LOGGER = logging.getLogger(__name__)
BACKUP_PREFIX = "elmax_local_migration_backup"


async def write_backup(hass: HomeAssistant, base_dir: Path | None = None) -> Path:
    """Dump legacy registries to JSON. Returns path to backup file."""
    base = base_dir or Path(hass.config.path(".storage"))
    base.mkdir(parents=True, exist_ok=True)
    ts = int(time.time() * 1000)
    path = base / f"{BACKUP_PREFIX}_{ts}.json"

    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)

    legacy_entries = list(hass.config_entries.async_entries(LEGACY_DOMAIN))
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


async def async_migrate(hass: HomeAssistant) -> None:
    """Run elmax_mqtt -> elmax_local migration. Service handler."""
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
            legacy_entry_id = legacy.entry_id
            legacy_scan = legacy.options.get("scan_interval", 90)

            # 1. Unload the legacy entry (does not remove its entities)
            await hass.config_entries.async_unload(legacy_entry_id)

            # 2. Create the new elmax_local entry via import flow
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

            # 3. Rewrite entity registry to point at the new entry
            for ent in list(ent_reg.entities.values()):
                if (ent.platform == LEGACY_DOMAIN
                        and ent.config_entry_id == legacy_entry_id):
                    new_uid = ent.unique_id.replace(
                        f"{LEGACY_DOMAIN}_", f"{DOMAIN}_", 1
                    )
                    ent_reg.async_update_entity_platform(
                        ent.entity_id,
                        DOMAIN,
                        new_unique_id=new_uid,
                        new_config_entry_id=new_entry.entry_id,
                    )

            # 4. Rewrite device registry identifiers
            for dev in list(dev_reg.devices.values()):
                if (LEGACY_DOMAIN, panel_id) in dev.identifiers:
                    new_ids = {
                        (DOMAIN, panel_id) if did == (LEGACY_DOMAIN, panel_id) else did
                        for did in dev.identifiers
                    }
                    dev_reg.async_update_device(dev.id, new_identifiers=new_ids)

            # 5. Apply options from legacy scan_interval (clamped)
            new_options = {
                "reconcile_interval": max(legacy_scan, MIN_RECONCILE_INTERVAL),
                "enable_ws": True,
                "enable_mqtt": True,
            }
            hass.config_entries.async_update_entry(new_entry, options=new_options)

            # 6. Remove legacy entry (entities are now linked to new_entry,
            #    so they are not cascade-removed)
            await hass.config_entries.async_remove(legacy_entry_id)

        persistent_notification.async_create(
            hass,
            "Migrazione elmax_mqtt -> elmax_local completata. "
            "Riavvia Home Assistant per applicare. "
            f"Backup: {backup_path}",
            title="Elmax: migrazione",
            notification_id="elmax_local_migration",
        )
    except Exception as err:
        _LOGGER.error("Migration failed: %s. Auto-rollback from %s", err, backup_path)
        await async_rollback(hass, backup_path)
        raise HomeAssistantError(f"Migrazione fallita, rollback eseguito: {err}") from err


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

    # Rewrite entities back to legacy platform BEFORE removing the elmax_local
    # entry; otherwise removing the entry cascade-removes the entities. We
    # need to point them at a still-valid config_entry_id during the rewrite,
    # so we keep the elmax_local entry temporarily; the rewrite sets the entity
    # to legacy platform but with the same (elmax_local) config_entry_id. The
    # user must re-add the elmax_mqtt integration to fully restore.
    panel_ids: set[str] = set()
    for ent_data in data["entities"]:
        ent = ent_reg.async_get(ent_data["entity_id"])
        if ent and ent.platform == DOMAIN:
            ent_reg.async_update_entity_platform(
                ent.entity_id,
                ent_data["platform"],
                new_unique_id=ent_data["unique_id"],
                new_config_entry_id=ent.config_entry_id,
            )
    for dev_data in data["devices"]:
        dev = dev_reg.async_get(dev_data["device_id"])
        if dev:
            new_ids = {tuple(i) for i in dev_data["identifiers"]}
            dev_reg.async_update_device(dev.id, new_identifiers=new_ids)
        for ident in dev_data["identifiers"]:
            if ident and len(ident) >= 2 and ident[0] == LEGACY_DOMAIN:
                panel_ids.add(ident[1])

    for entry_data in data["config_entries"]:
        existing = hass.config_entries.async_get_entry(entry_data["entry_id"])
        if existing is None:
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

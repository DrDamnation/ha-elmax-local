"""Migration helpers for elmax_mqtt -> elmax_local.

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

"""Test migration helpers."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.elmax_local.const import DOMAIN, LEGACY_DOMAIN
from custom_components.elmax_local.migration import (
    async_migrate, async_rollback,
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
    # Ensure ms-resolution timestamps differ
    await asyncio.sleep(0.005)
    p2 = await write_backup(hass, base_dir=tmp_path)
    latest = find_latest_backup(tmp_path)
    assert latest == p2
    assert p1 != p2


async def test_migrate_rewrites_entity_registry(hass):
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

    fake_new_entry = MockConfigEntry(domain=DOMAIN, entry_id="new_entry", unique_id="abc")
    fake_new_entry.add_to_hass(hass)

    with patch.object(hass.config_entries.flow, "async_init",
                      new=AsyncMock(return_value={
                          "type": "create_entry", "result": fake_new_entry,
                      })):
        await async_migrate(hass)

    ent = ent_reg.async_get("binary_sensor.zona_01")
    assert ent.platform == DOMAIN
    assert ent.unique_id == "elmax_local_abc-zona-0"


async def test_rollback_restores_platform(hass, tmp_path):
    # Setup post-migration state: entity is on elmax_local
    legacy_entry_id = "legacy_id"
    new = MockConfigEntry(domain=DOMAIN, entry_id="new_id", unique_id="abc")
    new.add_to_hass(hass)

    ent_reg = er.async_get(hass)
    ent_reg.async_get_or_create(
        domain="binary_sensor", platform=DOMAIN,
        unique_id="elmax_local_abc-zona-0",
        config_entry=new, suggested_object_id="zona_01",
    )

    backup_data = {
        "version": 1, "timestamp": 1,
        "config_entries": [{
            "entry_id": legacy_entry_id,
            "data": {"panel_id": "abc", "panel_pin": "0", "panel_host": "1.2.3.4"},
            "options": {}, "unique_id": "abc", "title": "Elmax",
        }],
        "entities": [{
            "entity_id": "binary_sensor.zona_01",
            "unique_id": "elmax_mqtt_abc-zona-0",
            "platform": LEGACY_DOMAIN,
            "config_entry_id": legacy_entry_id,
            "device_id": None, "disabled_by": None,
        }],
        "devices": [],
    }
    path = tmp_path / "backup.json"
    path.write_text(json.dumps(backup_data))

    await async_rollback(hass, path)
    ent = ent_reg.async_get("binary_sensor.zona_01")
    assert ent.platform == LEGACY_DOMAIN
    assert ent.unique_id == "elmax_mqtt_abc-zona-0"

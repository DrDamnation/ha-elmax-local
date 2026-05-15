"""Test migration helpers."""
from __future__ import annotations

import asyncio
from pathlib import Path

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
    # Ensure ms-resolution timestamps differ
    await asyncio.sleep(0.005)
    p2 = await write_backup(hass, base_dir=tmp_path)
    latest = find_latest_backup(tmp_path)
    assert latest == p2
    assert p1 != p2

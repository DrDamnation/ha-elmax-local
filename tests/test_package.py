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

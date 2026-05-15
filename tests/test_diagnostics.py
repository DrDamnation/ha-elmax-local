"""Test diagnostics."""
from __future__ import annotations

import time
from unittest.mock import MagicMock

from custom_components.elmax_local.diagnostics import async_get_config_entry_diagnostics


async def test_diagnostics_redacts_pin(hass):
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
                          _login_fail_count=0, _blocked_until=0)
    coord.registry = MagicMock()
    coord.registry._transports = []
    hass.data["elmax_local"] = {entry.entry_id: coord}

    dump = await async_get_config_entry_diagnostics(hass, entry)
    assert "000000" not in str(dump)
    # _token is not part of raw dump shape, but expires_in derived from it
    # is exposed (no leak). Token value itself is never in dump.
    assert "JWT_TOKEN" not in str(dump)

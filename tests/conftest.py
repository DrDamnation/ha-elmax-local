"""Common fixtures for elmax_local tests."""
from __future__ import annotations

import asyncio

import aiohttp
import pytest

pytest_plugins = ["pytest_homeassistant_custom_component"]


def _prewarm_pycares_thread() -> None:
    # pycares spawns a daemon _run_safe_shutdown_loop thread on first
    # DNS-channel destroy. HA's verify_cleanup fixture fails any test that
    # introduces a new thread; pre-spawning here puts the thread into
    # threads_before for every test.
    async def _go() -> None:
        async with aiohttp.ClientSession() as session:
            try:
                await session.get(
                    "http://127.0.0.1:1/",
                    timeout=aiohttp.ClientTimeout(total=0.01),
                )
            except Exception:
                pass

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_go())
    finally:
        loop.close()


_prewarm_pycares_thread()


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
                  "statiDisponibili": [0, 1, 2, 3, 4], "statiSessioneDisponibili": [0, 1, 2, 3],
                  "stato": 0, "statoSessione": 1, "zoneBmask": "0100000000000000",
                  "nome": "AREA 1"}],
        "tapparelle": [], "gruppi": [],
        "scenari": [{"endpointId": "abc123-scenario-0", "visibile": True, "indice": 0,
                     "nome": "SCENARIO 1"}],
        "datetime": "18:01:09 25/07/2022",
    }

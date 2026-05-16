"""Transport abstraction for Elmax Local.

Defines the Transport ABC plus enums, dataclasses, and TransportRegistry.
Concrete implementations in transport/http.py, mqtt.py, websocket.py.
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Awaitable, Callable

if TYPE_CHECKING:
    from ..auth import AuthManager

StateUpdateCallback = Callable[[dict], Awaitable[None]]


class TransportCapability(Enum):
    PUSH = "push"
    POLL = "poll"
    COMMAND = "command"


class TransportState(Enum):
    DISABLED = "disabled"
    PROBING = "probing"
    READY = "ready"
    DEGRADED = "degraded"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class CommandResult:
    ok: bool
    error: str | None = None
    raw_response: dict | None = None


class Transport(ABC):
    """Contract for a communication channel with an Elmax panel.

    Agnostic to domain: forwards raw /api/v2/discovery payloads to the
    Coordinator via callback. Does not interpret state.
    """

    name: str
    capabilities: frozenset[TransportCapability]

    @property
    @abstractmethod
    def state(self) -> TransportState: ...

    @abstractmethod
    async def async_probe(self) -> bool:
        """Verify availability on current firmware. Idempotent, no side-effects.
        Suggested internal timeout: 10s."""

    @abstractmethod
    async def async_start(
        self,
        auth: "AuthManager",
        on_state_update: StateUpdateCallback,
    ) -> None:
        """Start the transport. PUSH starts listening; POLL/COMMAND inits client."""

    @abstractmethod
    async def async_stop(self) -> None:
        """Stop. Idempotent. Restartable with async_start."""

    async def async_fetch_state(self) -> dict | None:
        """On-demand fetch (POLL capability). Default raise."""
        raise NotImplementedError(f"{self.name} does not support POLL")

    async def async_send_command(
        self,
        endpoint_id: str,
        cmd: str | None,
        code: str | None = None,
    ) -> CommandResult:
        """Send command (COMMAND capability). Default raise.

        endpoint_id: e.g. "abc-area-0"
        cmd: command string or None (zones toggle)
        code: PIN ONLY for area disarm
        """
        raise NotImplementedError(f"{self.name} does not support COMMAND")


_LOGGER = logging.getLogger(__name__)


class TransportRegistry:
    """Orchestrates N transports. Routes operations by capability + state."""

    # Priority order for POLL and COMMAND fallback
    _POLL_PRIORITY = ("http", "mqtt")
    _COMMAND_PRIORITY = ("http", "mqtt")

    def __init__(self, transports: list[Transport]):
        self._transports = transports

    # Hard timeout per transport probe so a slow panel cannot stall setup.
    # HTTP/WS probes already have internal 10s timeouts but a misbehaving
    # network stack could still hang; the outer 12s is the belt-and-braces.
    PROBE_HARD_TIMEOUT = 12

    async def async_start_all(
        self,
        auth: "AuthManager",
        on_state_update: StateUpdateCallback,
    ) -> None:
        # Probe transports concurrently — sequential probing with a slow
        # transport (e.g. unreachable panel) could otherwise add seconds to
        # bootstrap. Each probe is wrapped in PROBE_HARD_TIMEOUT so one bad
        # transport cannot stall the others.
        async def _probe_one(t: Transport) -> tuple[Transport, bool]:
            # Give the transport access to AuthManager *before* probing, since
            # HTTP/WS probes hit /login which goes through auth.
            t._auth = auth  # noqa: SLF001 — intentional contract per Registry
            try:
                ok = await asyncio.wait_for(
                    t.async_probe(), timeout=self.PROBE_HARD_TIMEOUT
                )
            except asyncio.TimeoutError:
                _LOGGER.warning(
                    "Transport %s probe timed out after %ds",
                    t.name, self.PROBE_HARD_TIMEOUT,
                )
                ok = False
            except Exception as err:
                _LOGGER.warning("Transport %s probe error: %s", t.name, err)
                ok = False
            return t, ok

        results = await asyncio.gather(
            *(_probe_one(t) for t in self._transports)
        )
        for t, ok in results:
            if ok:
                try:
                    await t.async_start(auth, on_state_update)
                except Exception as err:
                    _LOGGER.warning("Transport %s failed to start: %s",
                                    t.name, err)
            else:
                _LOGGER.info("Transport %s probe failed, marking UNSUPPORTED",
                             t.name)

    async def async_stop_all(self) -> None:
        for t in self._transports:
            try:
                await t.async_stop()
            except Exception:
                pass

    def _by_name(self, name: str) -> Transport | None:
        for t in self._transports:
            if t.name == name:
                return t
        return None

    async def async_fetch_state(self) -> dict | None:
        for name in self._POLL_PRIORITY:
            t = self._by_name(name)
            if (t and TransportCapability.POLL in t.capabilities
                    and t.state == TransportState.READY):
                result = await t.async_fetch_state()
                if result is not None:
                    return result
        return None

    async def async_send_command(
        self,
        endpoint_id: str,
        cmd: str | None,
        code: str | None = None,
    ) -> CommandResult:
        last_error = "no_transport"
        for name in self._COMMAND_PRIORITY:
            t = self._by_name(name)
            if (t and TransportCapability.COMMAND in t.capabilities
                    and t.state == TransportState.READY):
                result = await t.async_send_command(endpoint_id, cmd, code)
                if result.ok:
                    return result
                last_error = result.error or "unknown"
        return CommandResult(ok=False, error=last_error)

    def get_active_push_transports(self) -> list[Transport]:
        return [t for t in self._transports
                if TransportCapability.PUSH in t.capabilities
                and t.state == TransportState.READY]

    def degraded_or_unsupported(self) -> list[Transport]:
        return [t for t in self._transports
                if t.state in (TransportState.DEGRADED, TransportState.UNSUPPORTED)]

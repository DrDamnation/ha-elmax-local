"""Transport abstraction for Elmax Local.

Defines the Transport ABC plus enums, dataclasses, and TransportRegistry.
Concrete implementations in transport/http.py, mqtt.py, websocket.py.
"""
from __future__ import annotations

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


class TransportRegistry:
    """Orchestrates N transports. Routing logic implemented in Task 7."""

    def __init__(self, transports: list[Transport]):
        self._transports = transports

    async def async_start_all(self, auth, on_state_update) -> None:
        raise NotImplementedError("Implemented in Task 7")

    async def async_stop_all(self) -> None:
        raise NotImplementedError("Implemented in Task 7")

    async def async_fetch_state(self) -> dict | None:
        raise NotImplementedError("Implemented in Task 7")

    async def async_send_command(self, eid, cmd, code=None) -> CommandResult:
        raise NotImplementedError("Implemented in Task 7")

    def get_active_push_transports(self) -> list[Transport]:
        raise NotImplementedError("Implemented in Task 7")

    def degraded_or_unsupported(self) -> list[Transport]:
        raise NotImplementedError("Implemented in Task 7")

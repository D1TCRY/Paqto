"""Configuration and policy objects for Paqto nodes."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from paqto.core.protocol import PROTOCOL_VERSION


class BackpressurePolicy(str, Enum):
    """Behavior when a bounded node queue has no immediately free slot."""

    WAIT = "wait"
    REJECT = "reject"


class HandlerErrorPolicy(str, Enum):
    """Action taken after an application message handler fails."""

    CONTINUE = "continue"
    CLOSE_CONNECTION = "close_connection"


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    """Optional exponential-backoff policy for restoring lost sessions.

    Reconnect creates a new transport and protocol session. It does not resend
    application messages or restore pending requests and acknowledgements.

    Attributes:
        enabled: Whether unexpected connection loss schedules reconnect.
        initial_delay: Seconds before the first reconnect attempt.
        multiplier: Factor applied to the delay after each failed attempt.
        maximum_delay: Maximum delay in seconds between attempts.
        jitter: Fractional random variation applied to each delay, from 0 to 1.
        max_attempts: Maximum attempts, or ``None`` to keep trying while the
            node is running and its discovery observation remains fresh.
    """

    enabled: bool = False
    initial_delay: float = 0.5
    multiplier: float = 2.0
    maximum_delay: float = 30.0
    jitter: float = 0.0
    max_attempts: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool):
            raise TypeError("enabled must be a boolean.")
        _validate_positive_number("initial_delay", self.initial_delay)
        _validate_positive_number("multiplier", self.multiplier)
        _validate_positive_number("maximum_delay", self.maximum_delay)
        if self.multiplier < 1:
            raise ValueError("multiplier must be greater than or equal to one.")
        if self.maximum_delay < self.initial_delay:
            raise ValueError("maximum_delay must not be less than initial_delay.")
        if not isinstance(self.jitter, (int, float)) or isinstance(self.jitter, bool):
            raise TypeError("jitter must be a number.")
        if not math.isfinite(self.jitter) or not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between zero and one.")
        if self.max_attempts is not None:
            if not isinstance(self.max_attempts, int) or isinstance(
                self.max_attempts, bool
            ):
                raise TypeError("max_attempts must be an integer or None.")
            if self.max_attempts <= 0:
                raise ValueError("max_attempts must be greater than zero.")

    def delay_for_attempt(
        self,
        attempt: int,
        *,
        random_value: float | None = None,
    ) -> float:
        """Return the delay before a zero-based reconnect attempt.

        ``random_value`` may supply a deterministic sample from 0 through 1,
        which is useful when testing jitter. ``None`` uses :mod:`random`.
        """
        if not isinstance(attempt, int) or isinstance(attempt, bool):
            raise TypeError("attempt must be an integer.")
        if attempt < 0:
            raise ValueError("attempt must be greater than or equal to zero.")
        delay = min(
            self.maximum_delay,
            self.initial_delay * (self.multiplier**attempt),
        )
        if self.jitter == 0:
            return delay
        sample = random.random() if random_value is None else random_value
        if not isinstance(sample, (int, float)) or isinstance(sample, bool):
            raise TypeError("random_value must be a number or None.")
        if not math.isfinite(sample) or not 0 <= sample <= 1:
            raise ValueError("random_value must be between zero and one.")
        return delay * (1 + ((2 * sample - 1) * self.jitter))


@dataclass(slots=True)
class PaqtoConfig:
    """Runtime, protocol, and capacity options for :class:`PaqtoNode`.

    Timeout values are seconds. Except for ``discover_timeout``, finite
    timeouts must be greater than zero; ``None`` disables the corresponding
    deadline where allowed. Capacity values count items, not aggregate bytes
    or caller-created tasks.

    Attributes:
        connect_timeout: Default deadline for an outgoing transport connect.
        send_timeout: Default deadline for a queued frame to be written.
        discover_timeout: Discovery budget; zero performs no collection wait.
        handshake_timeout: Deadline for the Paqto hello exchange.
        request_timeout: Default wait for an in-memory correlated reply.
        acknowledgement_timeout: Default wait for a Paqto technical ACK.
        heartbeat_interval: Idle period before sending a PING, or ``None`` to
            disable locally initiated heartbeat checks.
        heartbeat_timeout: Deadline for a matching PONG.
        idle_timeout: Maximum interval without receiving a READY-session frame.
        peer_ttl: Lifetime of a discovery observation used for new sessions.
        reconnect: Policy for recreating unexpectedly lost sessions.
        enable_acknowledgements: Whether to offer technical ACK support.
        protocol_version: Exact Paqto protocol version offered to peers.
        capabilities: Additional capability names offered during negotiation.
        serializer_id: Stable wire-format id, or ``None`` to use the serializer.
        max_message_size: Maximum serialized application bytes offered.
        max_pending_requests: Node-wide pending request correlation count.
        max_pending_acknowledgements: Node-wide pending ACK count.
        max_inbound_queue: Node-wide queued application-message count.
        max_outbound_queue: Queued frame count per READY connection.
        max_event_queue: Best-effort event queue capacity.
        max_connections: Physical connections admitted by the node, including
            connections performing the Paqto handshake.
        handler_concurrency: Number of fixed application-handler workers.
        inbound_backpressure: Behavior when the inbound queue is full.
        outbound_backpressure: Behavior when an outbound queue is full.
        handler_error_policy: Action taken when a message handler fails.
        protocol_metadata: Generic JSON-safe data sent in the unauthenticated
            hello; it must not be treated as trusted identity data.
        require_authenticated_peer_id_match: Require transport-authenticated
            identity to exist and match the peer id declared in the hello.
    """

    connect_timeout: float | None = 10.0
    send_timeout: float | None = 10.0
    discover_timeout: float | None = 3.0
    handshake_timeout: float | None = 10.0
    request_timeout: float | None = 10.0
    acknowledgement_timeout: float | None = 10.0
    heartbeat_interval: float | None = None
    heartbeat_timeout: float | None = 10.0
    idle_timeout: float | None = None
    peer_ttl: float | None = 60.0
    reconnect: ReconnectPolicy = field(default_factory=ReconnectPolicy)
    enable_acknowledgements: bool = True
    protocol_version: int = PROTOCOL_VERSION
    capabilities: tuple[str, ...] = ()
    serializer_id: str | None = None
    max_message_size: int = 16 * 1024 * 1024
    max_pending_requests: int = 1024
    max_pending_acknowledgements: int = 1024
    max_inbound_queue: int = 256
    max_outbound_queue: int = 256
    max_event_queue: int = 256
    max_connections: int = 128
    handler_concurrency: int = 4
    inbound_backpressure: BackpressurePolicy = BackpressurePolicy.WAIT
    outbound_backpressure: BackpressurePolicy = BackpressurePolicy.WAIT
    handler_error_policy: HandlerErrorPolicy = HandlerErrorPolicy.CONTINUE
    protocol_metadata: dict[str, Any] | None = None
    require_authenticated_peer_id_match: bool = False

    def __post_init__(self) -> None:
        self._validate_timeout("connect_timeout", self.connect_timeout)
        self._validate_timeout("send_timeout", self.send_timeout)
        self._validate_non_negative_timeout("discover_timeout", self.discover_timeout)
        if self.handshake_timeout is not None:
            if not isinstance(self.handshake_timeout, (int, float)) or isinstance(
                self.handshake_timeout, bool
            ):
                raise TypeError("handshake_timeout must be a number or None.")
            if not math.isfinite(self.handshake_timeout) or self.handshake_timeout <= 0:
                raise ValueError(
                    "handshake_timeout must be finite and greater than zero."
                )
        self._validate_timeout("request_timeout", self.request_timeout)
        self._validate_timeout(
            "acknowledgement_timeout",
            self.acknowledgement_timeout,
        )
        self._validate_timeout("heartbeat_interval", self.heartbeat_interval)
        self._validate_timeout("heartbeat_timeout", self.heartbeat_timeout)
        self._validate_timeout("idle_timeout", self.idle_timeout)
        self._validate_timeout("peer_ttl", self.peer_ttl)
        if self.heartbeat_interval is not None and self.heartbeat_timeout is None:
            raise ValueError(
                "heartbeat_timeout must be set when heartbeat_interval is enabled."
            )
        if not isinstance(self.reconnect, ReconnectPolicy):
            raise TypeError("reconnect must be a ReconnectPolicy.")
        if not isinstance(self.enable_acknowledgements, bool):
            raise TypeError("enable_acknowledgements must be a boolean.")
        if not isinstance(self.protocol_version, int) or isinstance(
            self.protocol_version, bool
        ):
            raise TypeError("protocol_version must be an integer.")
        if self.protocol_version < 1:
            raise ValueError("protocol_version must be at least 1.")
        if not isinstance(self.capabilities, tuple) or any(
            not isinstance(capability, str) or not capability
            for capability in self.capabilities
        ):
            raise TypeError("capabilities must be a tuple of non-empty strings.")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("capabilities must not contain duplicates.")
        if self.serializer_id is not None and (
            not isinstance(self.serializer_id, str) or not self.serializer_id
        ):
            raise TypeError("serializer_id must be a non-empty string or None.")
        if not isinstance(self.max_message_size, int) or isinstance(
            self.max_message_size, bool
        ):
            raise TypeError("max_message_size must be an integer.")
        if self.max_message_size <= 0:
            raise ValueError("max_message_size must be greater than zero.")
        for name in (
            "max_pending_requests",
            "max_pending_acknowledgements",
            "max_inbound_queue",
            "max_outbound_queue",
            "max_event_queue",
            "max_connections",
            "handler_concurrency",
        ):
            self._validate_positive_integer(name, getattr(self, name))
        if not isinstance(self.inbound_backpressure, BackpressurePolicy):
            raise TypeError("inbound_backpressure must be a BackpressurePolicy.")
        if not isinstance(self.outbound_backpressure, BackpressurePolicy):
            raise TypeError("outbound_backpressure must be a BackpressurePolicy.")
        if not isinstance(self.handler_error_policy, HandlerErrorPolicy):
            raise TypeError("handler_error_policy must be a HandlerErrorPolicy.")
        if self.protocol_metadata is not None and not isinstance(
            self.protocol_metadata, dict
        ):
            raise TypeError("protocol_metadata must be a dictionary or None.")
        self.protocol_metadata = dict(self.protocol_metadata or {})
        if not isinstance(self.require_authenticated_peer_id_match, bool):
            raise TypeError("require_authenticated_peer_id_match must be a boolean.")

    @staticmethod
    def _validate_timeout(name: str, value: float | None) -> None:
        if value is None:
            return
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise TypeError(f"{name} must be a number or None.")
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and greater than zero.")

    @staticmethod
    def _validate_non_negative_timeout(name: str, value: float | None) -> None:
        if value is None:
            return
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise TypeError(f"{name} must be a number or None.")
        if not math.isfinite(value) or value < 0:
            raise ValueError(
                f"{name} must be finite and greater than or equal to zero."
            )

    @staticmethod
    def _validate_positive_integer(name: str, value: int) -> None:
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"{name} must be an integer.")
        if value <= 0:
            raise ValueError(f"{name} must be greater than zero.")


def _validate_positive_number(name: str, value: float) -> None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise TypeError(f"{name} must be a number.")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and greater than zero.")

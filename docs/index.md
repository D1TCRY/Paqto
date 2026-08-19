# Paqto documentation

Paqto is a generic, async-first framework for communication between logical
peers. The framework coordinates discovery, framed connections, a protocol
handshake, serialization, routing, correlation, liveness, and optional
transport authentication without embedding any application domain.

## What Paqto solves

Paqto provides:

- transport-independent core contracts;
- discovery results separated from stable logical peer identity;
- versioned READY sessions above transport connections;
- application-message routing by type;
- in-memory request/reply correlation;
- optional technical receipt acknowledgements;
- heartbeat and optional session reconnect;
- bounded work queues, resource admission, lifecycle events, and structured
  logging;
- a LAN implementation using UDP broadcast, framed TCP, and optional TLS/mTLS.

Paqto deliberately does not define application commands, schemas, workflows,
roles, or success conditions. Those belong to the application built on top.

## Guarantee boundary

A successful operation at one layer does not imply success at the next:

```text
local transport write
    -> remote Paqto receipt and envelope validation (optional ACK)
        -> application handler execution
            -> application-defined success or durable state
```

Paqto does not inherently provide exactly-once delivery, at-least-once
delivery, durable messaging, transactional processing, automatic
application-level retry, application authorization, duplicate suppression, or
application success guarantees. See [Messaging](messaging.md#delivery-and-acknowledgement-semantics)
for the exact ACK contract.

## Documentation map

Start with [Getting started](getting-started.md), then use these guides as
needed:

- [Architecture](architecture.md) — component boundaries, dependency direction,
  startup, READY sessions, shutdown, and extension points.
- [Configuration](configuration.md) — defaults, validation, security impact,
  and operational trade-offs for every runtime option.
- [Messaging](messaging.md) — envelopes, handlers, sending, request/reply,
  correlation cleanup, and technical ACKs.
- [Serializers](serializers.md) — built-in JSON and bytes formats, payload
  limits, wire identifiers, and custom implementations.
- [Protocol](protocol.md) — hello exchange, identity consistency,
  capabilities, control frames, and message-size negotiation.
- [Discovery](discovery.md) — UDP broadcast, cache freshness, reachability, and
  the untrusted discovery model.
- [LAN transport](transports.md) — TCP endpoints, framing, size/time limits,
  listener admission, and bind versus advertised addresses.
- [Security](security.md) — TLS, mTLS, certificate validation, logical identity
  mapping, strict deployment profiles, and residual risk.
- [Reliability](reliability.md) — connection state, duplicate sessions,
  reconnect, heartbeat, backpressure, concurrency, and shutdown.
- [Events, logging, and errors](events-and-errors.md) — observability contracts
  and the public exception hierarchy.
- [API overview](api-overview.md) — a task-oriented guide to public types.
- [Production considerations](production.md) — deployment profiles and the
  current independent-audit findings.
- [Platform support](platform-support.md) — portability contract, host
  capabilities, optional features, and real-runtime validation gaps.
- [Compatibility testing](platform-testing.md) — the offline solo/pair suite,
  machine-readable reports, real-device execution, and two-device
  interoperability.

The files under `development_logs/` are chronological engineering records.
They may describe earlier behavior or future work and should not be used as the
current API contract.

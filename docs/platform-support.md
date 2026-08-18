# Platform support and portability contract

Paqto is designed as a generic Python library. Its core does not require a
particular operating system, UI environment, network-interface name, process
model, signal API, or filesystem layout. Platform support is defined by
capabilities supplied by Python, an adapter, and the host application rather
than by platform-named classes.

The current portability verdict is conditional but now evidence-based. The
full offline conformance profile passes on the available Windows 11 / CPython
3.14.6 host, including local IPv4 broadcast, TLS, and mTLS. A desktop CI matrix
is configured, but Linux, macOS, other Python versions, mobile/sandbox
behavior, and real multi-interface networks remain unverified until those jobs
or devices actually produce passing reports.

## Official support matrix

Status terms are strict: **SUPPORTED & TESTED** requires a retained successful
full conformance report from that real platform/version; **EXPECTED** is an
architectural or configured-CI expectation without completed evidence;
**UNVERIFIED** has no qualifying real-runtime result; **UNSUPPORTED** is outside
the declared contract.

| Platform | Status | Python versions | TCP | UDP discovery | TLS | Last tested |
| --- | --- | --- | --- | --- | --- | --- |
| Windows 11 (available host) | SUPPORTED & TESTED | 3.14.6 | PASS | PASS (local broadcast) | PASS (TLS/mTLS) | 2026-08-18 |
| Windows hosted CI matrix | EXPECTED | 3.10, 3.12, 3.14 | Awaiting CI | Awaiting real broadcast; CI subset skips it | Awaiting CI | Not yet executed in this workspace |
| Linux hosted CI matrix | EXPECTED | 3.10, 3.12, 3.14 | Awaiting CI | Awaiting real broadcast; CI subset skips it | Awaiting CI | Not yet executed in this workspace |
| macOS hosted CI matrix | EXPECTED | 3.10, 3.12, 3.14 | Awaiting CI | Awaiting real broadcast; CI subset skips it | Awaiting CI | Not yet executed in this workspace |
| Android device/emulator with supported Python | EXPECTED / AWAITING DEVICE VALIDATION | 3.10+ runtime-dependent | Awaiting real execution | Awaiting real execution | Awaiting real execution | No Android execution performed |

The Windows result applies only to the exact runtime and capabilities shown;
it does not prove every Windows release, Python version, network driver, or
firewall policy. The certification rules, JSON format, exit codes, Android
procedure, and two-device exercise are documented in
[Platform testing](platform-testing.md).

## Portability contract

The generic core requires:

- Python 3.10 or newer with a functioning standard-library `asyncio` event
  loop;
- transport and discovery implementations satisfying Paqto's abstract async
  contracts;
- cooperative task cancellation and adapter `close()` / `stop()` methods;
- monotonic clocks for deadlines, heartbeat, reconnect delays, and discovery
  TTL passage;
- a wall clock only for informational message, event, and discovery timestamps;
- no Internet connection, DNS service, fixed IP address, platform signal,
  process creation, thread creation, or direct file-descriptor access.

The built-in `LanTransport` additionally requires TCP and asyncio stream
support. `LanDiscovery` additionally requires IPv4 UDP broadcast and compatible
socket permissions. TLS is optional and requires Python `ssl`, a usable TLS
backend, trust configuration, and certificate/key material. TLS can use
accessible paths, CA data in memory, or caller-prepared `SSLContext` objects.
Importing and using the plain generic core does not require filesystem access.

Paqto timeouts use `asyncio.wait_for()`, heartbeat activity uses the event
loop's monotonic `time()`, reconnect sleeps use asyncio, and discovery
observations retain a monotonic age anchor beside diagnostic UTC `last_seen`.
No event-loop policy or implementation is selected globally by the library.
Library APIs do not call `asyncio.run()`; only the standalone example does so
at its process entry point.

## Paqto responsibility

Paqto is responsible for the behavior and lifecycle of the resources it creates:

- networking through its configured transport and optional discovery adapter,
  including listeners, connections, framing, timeouts, reconnect, refresh, and
  cooperative close;
- protocol negotiation, READY sessions, serialization contracts, message
  routing, request/reply correlation, ACKs, limits, and backpressure;
- encryption and peer-certificate verification according to the supplied TLS
  configuration or injected `SSLContext` objects;
- peer communication through `Peer`, `Endpoint`, `DiscoveredPeer`, `Message`,
  and transport-neutral `SecurityInfo` models;
- restartable `start()`, `stop()`, and `network_changed()` behavior and cleanup
  of Paqto-owned tasks, queues, listeners, discovery endpoints, and connections;
- standard-library logging without configuring host handlers or destinations.

The core contains no socket family, interface name, certificate-path, UI,
signal, thread, process, or platform API. TCP, UDP, and TLS details live in the
optional LAN adapter package.

`PaqtoNode` owns its network lifecycle. The host owns the process/application
lifecycle and decides when to await `start()`, `stop()`, or
`network_changed()`. Paqto installs no signal handler and assumes no authority
over foreground/background transitions.

## Host responsibility

The host application or deployment must:

- provide a compatible Python runtime and create and run its supported asyncio
  event loop;
- grant network permissions required by the operating system, including on
  sandboxed or mobile platforms;
- own the process/application lifecycle, including creation, suspension,
  recreation, shutdown, and any foreground/background execution policy or
  permission;
- choose bind and advertised addresses that are reachable in its topology;
- supply topology, timeouts, discovery choice, peer endpoints, and other
  application-specific Paqto configuration;
- configure firewalls, segmentation, connection admission, and resource
  limits;
- decide whether DNS is available and use explicit numeric or provisioned
  endpoints when it is not;
- provision, protect, rotate, and revoke TLS keys and certificates, choose trust
  roots, and expose them through accessible paths, CA data, or prepared
  `SSLContext` objects;
- keep handlers cooperative or explicitly offload blocking/CPU work;
- notify a running node with `network_changed()` when host-specific monitoring
  determines that routes, interfaces, or local addresses changed, or use a
  full `stop()` / later `start()` around a longer suspension;
- handle application-level authorization and permissions, persistence, retries,
  idempotency, and durable success semantics.

These responsibilities are not implemented as platform-specific Paqto classes.
The host can inject an explicit `LanTransport(advertised_host=...)` when local
hostname resolution is unavailable or ambiguous. A wildcard bind such as
`0.0.0.0` or `::` is not itself a generally reachable advertised address.

In sandboxed or embedded runtimes, the host application may prepare and inject
SSLContext objects or provide accessible certificate paths. The host decides
whether to use Python's default trust configuration, custom trust anchors, or a
fully prepared context. Paqto does not assume that default trust stores are
identical across platforms and does not write private-key bytes to temporary
files.

## Optional features and their capability scope

| Feature | Required capabilities | Notes |
| --- | --- | --- |
| Generic core with a custom adapter | asyncio plus the adapter's documented capabilities | No filesystem or Internet requirement is imposed by the core. |
| LAN TCP transport | TCP, asyncio streams, bind/connect permission | Supports hostnames, IPv4, and IPv6 endpoint syntax; actual dual-stack behavior belongs to the runtime. |
| LAN discovery | IPv4 UDP broadcast, datagram endpoints, socket-option permission | It is a separate optional adapter, not a universal discovery mechanism. |
| TLS/mTLS | Python `ssl`, compatible TLS backend, trust roots, certificate/key access or prepared contexts | `TlsConfig` supports path-based certificate/key material plus file or in-memory CA trust. `TlsContextConfig` injects caller-prepared client/server contexts. Default trust behavior varies by runtime and host. |
| Reconnect/heartbeat | monotonic event-loop clock and cooperative scheduling | They restore or test a volatile session, never application delivery. |
| Host-notified network refresh | restartable adapters and a usable current network | Recreates listeners/discovery, clears endpoint snapshots, and repeats TLS/Paqto handshakes without platform monitoring in the core. |
| Logging/events | standard Python logging and in-memory queues | The host owns handlers, storage, redaction policy, and durable audit sinks. |

## Portability audit findings

### A — real cross-platform bugs

- The previous wildcard IPv6 path could select and advertise an IPv4 address,
  relying on platform-dependent dual-stack behavior. Address-family-matched
  hostname resolution and explicit advertisement now prevent that mismatch.
- Extreme indefinite reconnect attempt counts could overflow the exponential
  calculation and terminate the reconnect task. The calculation now saturates
  at `maximum_delay` and has regression coverage.
- A zero node discovery timeout previously cancelled even an immediate
  discovery result through `asyncio.wait_for(..., 0)`. Zero now delegates a
  non-blocking discovery pass without wrapping it in an immediate timeout.
- Discovery TTL passage previously used wall-clock subtraction. It now uses a
  monotonic local anchor while retaining UTC `last_seen` for diagnostics.
- Cancellation of the task awaiting `stop()` could interrupt cleanup. Stop is
  now atomic with respect to caller cancellation and re-raises only after owned
  resources have been released.

### B — platform-dependent behavior to abstract or configure

- Wildcard address advertisement is inherently ambiguous on multi-homed hosts.
  `advertised_host` is now an explicit injected value; automatic hostname
  resolution remains only a best-effort fallback.
- `PaqtoNode` currently publishes one listener endpoint, even though discovery
  accepts a sequence. Per-interface or plural listener advertisement needs a
  future capability-oriented API rather than interface-name heuristics.
- `LanDiscovery` is deliberately IPv4 broadcast-only. Broadcast destinations,
  bind behavior, address reuse, and delivery vary across network stacks.
- TLS accepts caller-prepared client/server contexts as a capability boundary.
  The convenient high-level path still relies on standard-library file access
  for certificate/private-key chains, while CA trust may come from a path or
  memory.
- An advertised endpoint is captured at listener start. The host can call
  `network_changed()` to rebuild and republish it, but Paqto intentionally does
  not monitor operating-system interfaces or infer host lifecycle transitions.
- The root build and publish batch files are platform-specific developer
  wrappers, not installed library code. Portable automation should invoke the
  documented `python -m pytest` and `python -m build` commands directly.

The former local-address heuristic that connected a UDP socket to `8.8.8.8`
has been removed. Paqto no longer uses an Internet destination to choose its
advertised LAN address.

### C — host responsibilities, not Paqto features

- network permission prompts, application foreground/background policy, and
  firewall configuration;
- detecting host-specific interface, route, foreground/background, or suspend
  transitions and deciding when to notify or stop Paqto;
- interface selection and topology-specific advertised addresses;
- Internet/DNS availability policy and provisioned endpoint management;
- certificate issuance, secure storage, rotation, revocation, and application
  authorization;
- process-wide event-loop selection, logging configuration, handler offloading,
  and shutdown deadlines imposed by the embedding environment;
- durable messaging, retry safety, transactions, and application success.

### D — behavior requiring real runtime or hardware tests

- supported Python versions on Linux, macOS, and Windows;
- IPv4, IPv6-only, and dual-stack TCP listeners;
- an isolated LAN with no default Internet route and with DNS unavailable;
- multiple active interfaces, link-local/scoped addresses, interface changes,
  suspend/resume, and address churn;
- UDP broadcast, port reuse, firewalls, and sandbox network restrictions;
- TLS system trust stores and TLS backend shutdown/error behavior;
- cancellation and socket errors under slow, lossy, or abruptly changing real
  networks.

The deterministic test suite uses IPv4 loopback and injected discovery packets
for stability. Passing it proves the generic orchestration and local socket
paths under the tested runtime, not all entries in this real-environment matrix.

## Current limitations and roadmap

Near-term portability work should execute and retain results from the configured
desktop CI, then run the same full conformance suite on a real Android runtime
and add opt-in live tests for offline LAN, IPv6, multi-interface selection, and
TLS stores. Subsequent API work may add plural advertised endpoints, explicit
context-reload coordination, and alternative discovery adapters. Those changes should remain
capability-based; they should not introduce operating-system-named core types.

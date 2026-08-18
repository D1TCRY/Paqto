# Production considerations

Paqto is version `0.0.1` and classified pre-alpha. The independent repository
audit found a coherent async architecture and strong tested cleanup boundaries,
but did not recommend presenting the current implementation as a general
hostile-network production release.

## Deployment profiles

### Development and testing

Loopback or isolated test networks may use plain `LanTransport()` with
conservative timeouts. This is the simplest way to exercise discovery,
handshake, messaging, handlers, correlation, and shutdown.

Do not mistake successful plain-TCP tests for security validation. Repository
TLS keys are public test fixtures and are never deployment credentials.

### Controlled LAN

On a physically and administratively controlled LAN, plain TCP may be an
explicit risk decision, but it provides no peer authentication or protection
from active interception. At minimum:

- restrict network access and discovery traffic;
- choose stable explicit peer ids and advertised interfaces;
- reduce message, queue, connection, and discovery limits to realistic values;
- enable frame, handshake, connect, send, request, ACK, and liveness deadlines;
- use a safe serializer;
- bound application-created tasks and keep handlers cooperative;
- monitor events, logs, process memory, open connections, and reconnect errors;
- implement application authorization and success responses.

TLS with normal server verification improves channel confidentiality,
integrity, and outgoing endpoint authentication even when full mTLS identity
binding is not required.

### Hostile or untrusted network

The current built-in profile is not sufficient by itself. A security-sensitive
deployment needs, at minimum:

- verified TLS in every outgoing direction;
- mTLS for every incoming direction that must authenticate a peer;
- an audited certificate-to-`Peer.id` resolver;
- `require_authenticated_peer_id_match=True`;
- application authorization after authentication;
- firewalls, segmentation, OS backlog/resource limits, and connection
  rate/concurrency controls;
- certificate issuance, protected key storage, rotation, expiry, revocation,
  pinning/trust policy, and monitoring;
- conservative byte/count/time limits and process memory monitoring;
- a serializer designed for untrusted bytes;
- an explicit application retry/idempotency/durability design, if required;
- an independent threat review and multi-platform fault testing.

Even with this profile, UDP discovery remains unauthenticated. Consider a
separate authenticated discovery mechanism or provisioned endpoints when
spoofable discovery is unacceptable.

## Residual concerns from the final audit

### Unauthenticated discovery

UDP announcements can be spoofed or poisoned for availability. The cache is
bounded and strict TLS identity checks prevent an untrusted hint from becoming
authenticated identity, but attackers can still redirect attempts, cause
failures, refresh claims, or consume bounded resources.

### Pre-TLS connection flood window

TLS completes in `asyncio.start_server()` before a connection reaches the
listener accept queue and before `PaqtoNode.max_connections`. The finite TLS
handshake timeout reduces the slow-handshake window, but Paqto has no aggregate
pre-TLS connection rate or concurrency limiter. Enforce this outside Paqto.

### No aggregate byte budget

Frame/message limits and queue counts are finite, but there is no total byte or
object-memory quota. A queue of many maximum-size frames can be large, and a
serializer can expand small input into a large object graph. WAIT policies do
not count application-created tasks blocked outside queues.

### Cooperative custom adapters

Built-in shutdown is tested under load, but the abstract adapter contracts
cannot force a third-party `close()` or `stop()` that never returns. There are
no adapter-close deadlines. Caller cancellation does not abandon Paqto's stop
sequence, but a non-cooperative adapter can still prevent it from finishing.

### Serializer safety

Paqto calls application serializer code on peer-provided bytes after protocol
framing. It does not sandbox deserialization. Avoid unsafe formats and enforce
schema, nesting, collection, string, and numeric limits appropriate to the
chosen encoding.

### Certificate lifecycle

High-level TLS configuration loads file-based certificate/key material at
transport start; trust roots may also come from in-memory CA data. Advanced
configuration accepts caller-prepared contexts. Paqto does not manage
encrypted-key password callbacks, hot reload, issuance, rotation, revocation,
pinning, secure storage, or expiry alerts.

### Authorization remains an application concern

Authentication can bind a connection to `Peer.id`; it does not say which
message types, payloads, or state transitions that identity may use. Enforce
authorization before application side effects and avoid trusting discovery or
unverified message metadata as policy.

## Delivery and recovery design

Paqto reconnect restores a session only. It never resends frames, requests, or
ACK waits from the old connection. A Paqto ACK means remote protocol receipt and
envelope validation, not handler or durable application success.

Applications that need stronger behavior must define:

- a durable source of pending work;
- application-level success responses;
- retry deadlines and backoff;
- stable operation identifiers and deduplication scope;
- idempotent processing or conflict rules;
- transactional boundaries and crash recovery;
- authorization for retries and responses.

Do not label a design exactly-once solely because it uses Paqto message ids,
request/reply, ACKs, TCP, or TLS.

## Release and validation gaps

The repository test suite exercises loopback behavior, malformed inputs,
timeouts, concurrency, TLS, reconnect, queue pressure, and cleanup on the
available Windows/Python 3.14 environment. A production claim still needs:

- real CI on supported Python 3.10 through 3.14;
- Linux, macOS, and Windows coverage;
- multi-host LAN tests and realistic firewall/interface configurations;
- prolonged slow-network, load, fault-injection, and memory tests;
- property/fuzz tests for framing, controls, discovery, and serializers;
- formal Paqto v1 wire documentation and cross-language vectors if independent
  implementations are expected;
- a supported deployment profile for network admission, authorization, and
  certificate lifecycle.

See [Security](security.md), [Reliability](reliability.md), and
[Messaging](messaging.md) for the exact current contracts. The capability and
host-environment boundary is defined in [Platform support](platform-support.md).

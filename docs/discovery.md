# LAN discovery

`LanDiscovery` implements `DiscoveryService` with IPv4 UDP broadcast. Its job
is to advertise reachability and find possible endpoints. It does not establish
a connection, authenticate a peer, authorize an action, or transport
application messages.

## Discovery is optional

Omit `PaqtoNode(discovery=...)`, pass `None`, or pass the public
`NoDiscovery()` adapter when the host already knows peer endpoints. This mode
creates no UDP socket and performs no DNS or interface lookup. The host supplies
a `DiscoveredPeer` directly to `connect()`, `send()`, or `request()`; Paqto then
uses the endpoint for the normal TCP/TLS, READY, identity, ACK, and
request/reply paths.

```python
from paqto import DiscoveredPeer, PaqtoNode, Peer
from paqto.lan import LanTransport, endpoint_from_host_port

node = PaqtoNode(
    name="device-a",
    transport=LanTransport(host="0.0.0.0", advertised_host="192.0.2.21"),
    serializer=serializer,
)
target = DiscoveredPeer(
    peer=Peer(id="device-b"),
    endpoints=[endpoint_from_host_port("192.0.2.20", 7450)],
)

await node.start()
reply = await node.request(target, {"operation": "status"}, type="request")
```

Explicit reachability is not identity proof. Use verified TLS/mTLS and strict
authenticated identity binding where peer identity matters.

## Discovery flow

At `start(local_peer, endpoints)`, the service:

- copies the local peer;
- keeps only valid LAN endpoints using `tcp://HOST:PORT`;
- binds an IPv4 UDP socket, enabling address reuse and broadcast;
- starts periodic `announce` broadcasts.

`discover(timeout=...)` broadcasts a `discover` packet, waits for a bounded
collection window, prunes expired cache entries, and returns the current cache.
It may include peers seen before that call. A `discover` packet received from
another peer triggers a unicast announcement back to the datagram sender.

Announcements are compact JSON containing a discovery protocol version,
logical peer fields, LAN endpoints, and generic metadata. Malformed, oversized,
deeply nested, duplicate-key, wrong-version, or invalid packets are ignored.
Discovery JSON is separate from both Paqto control JSON and the application
serializer.

## `Peer`, `Endpoint`, and `DiscoveredPeer`

- `Peer.id` is the stable logical identifier used by Paqto sessions.
- `Endpoint` is one transport-specific route, such as
  `Endpoint(transport="lan", address="tcp://192.0.2.10:9000")`.
- `DiscoveredPeer` is an observation containing the peer claim, zero or more
  endpoints, discovery metadata, and a UTC `last_seen` timestamp.

`endpoint_for("lan")` returns the first compatible endpoint. `touch()` refreshes
the diagnostic `last_seen` timestamp and its local monotonic observation
anchor. `freshness(ttl)` and `is_fresh(ttl)` apply reachability freshness using
monotonic elapsed time during normal operation; wall-clock adjustments do not
change TTL passage. `None` disables expiration. TTL must otherwise be finite
and positive. An age equal to the TTL is still fresh. Supplying an explicit
`now=` retains deterministic wall-time evaluation when needed.

## Cache limits and expiry

`LanDiscovery` deduplicates entries by announced peer id. A later announcement
updates the existing object and refreshes `last_seen`. Expired entries are
pruned before new admission and before discovery results return.

The default service TTL is 60 seconds and the default cache limit is 1024
peers. At capacity, existing entries can refresh but new ids are ignored.
`PaqtoConfig.peer_ttl` applies a second node-level freshness check, also 60
seconds by default, before a discovery observation can drive a new connect or
reconnect.

A READY connection remains usable if the old UDP observation expires. TTL is a
reachability policy, not an ongoing authorization decision. `PEER_EXPIRED` is
emitted when node discovery/connect/reconnect activity re-evaluates freshness;
there is no background expiry notification loop in `PaqtoNode`.

`PaqtoNode.stop()` clears remembered discovery observations. A later `start()`
therefore requires new discovery before opening a new outgoing session.
`network_changed()` automates this stop/start/invalidate/discover sequence for a
network change reported by the host.

## Trust boundary

LAN discovery packets are unsigned, unencrypted, and unauthenticated. Any host
able to send suitable UDP traffic can claim a peer id or endpoint, refresh a
cache entry, redirect connection attempts, or consume the bounded cache.

The central rule is:

```text
discovered peer identity != authenticated peer identity
```

Paqto checks that an outgoing hello matches the intended discovered id, which
detects an inconsistent redirect. An attacker controlling both the discovery
claim and an unauthenticated hello can still impersonate that id. Only the
established connection's security mechanism can populate
`SecurityInfo.authenticated_peer_id`.

Use [strict TLS identity binding](security.md#strict-authenticated-identity-profile)
when identity proof matters. Even then, discovery remains a spoofable
availability hint; network controls and authenticated discovery are not
provided by the current implementation.

## Scope and limitations

The implementation is IPv4 broadcast only. It does not provide IPv6 discovery,
multicast, mDNS, per-interface enumeration, signed announcements, early return
on the first result, or reliable delivery of discovery packets. An announcement
with no valid LAN endpoint may still create a peer observation that cannot be
connected to.

See [Configuration](configuration.md#landiscovery) for every discovery option.

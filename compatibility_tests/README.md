# Paqto real-platform compatibility suite

`compatibility_tests/` is permanent repository tooling for producing evidence
about the exact operating system, Python runtime, Paqto import, and network
capabilities that were actually exercised. It is outside `src/`, is not part
of the Paqto runtime API, and is excluded from wheels by the Setuptools package
discovery rule (`src/paqto*` only). It is the only compatibility-suite
implementation in the repository.

The only main entry point is:

```console
python compatibility_tests/run.py --help
```

Everything runs offline. No public DNS, HTTP endpoint, Internet address, or
cloud service is contacted.

## Solo versus pair

`solo` runs on one process/device. It checks the current Python and Paqto
runtime through loopback/local sockets: core import and supported Python,
serializer/router, PaqtoNode lifecycle, IPv4/IPv6 TCP and UDP when available,
local broadcast discovery, NoDiscovery with explicit endpoints, framing,
disconnect/reconnect, TLS/custom CA/in-memory CA/SSLContext injection,
hostname validation, mTLS, strict identity binding, messaging, ACK,
concurrent requests, timeouts/cancellation, limits, resource events, and clean
shutdown.

```console
python compatibility_tests/run.py solo
```

`pair` runs the same entry point in two independent processes, normally on two
devices on one LAN. It proves that the two recorded platform/runtime/version
combinations interoperated in the same session. Start the server first and the
client second.

## What PASS means

- `PASS`: the behavior ran and its assertions succeeded.
- `FAIL`: execution demonstrated a Paqto, protocol, fixture, or harness error.
- `SKIP`: the selected scenario deliberately did not run the check. Direct
  mode, for example, skips cross-device discovery.
- `UNAVAILABLE`: the runtime/network could not provide an environmental
  capability. A required unavailable capability makes the overall result
  `INCOMPLETE` (exit code 2), not PASS.

Exit code 0 is PASS, 1 is FAIL, and 2 is INCOMPLETE. Every invocation writes a
JSON report automatically under `compatibility_tests/reports/`; use
`--json PATH` to choose another destination.

PASS proves only the exact report contents. A solo PASS does not prove a
second device, another Python version, background execution, network handover,
firewall behavior, every interface, every TLS store, or every platform. A
same-host two-process pair PASS validates the harness and inter-process stack,
but is not cross-platform or physical-LAN evidence. Discovery PASS on one host
is reported separately from cross-device discovery.

## Which Paqto is tested

The terminal and JSON include the Paqto version, `paqto.__file__`, distribution
metadata, and whether the import is from this repository's `src/paqto` tree.
Source/editable imports are highlighted. During wheel validation require a
non-source import:

```console
python compatibility_tests/run.py solo --require-installed
```

This option is deliberately not mandatory for local development.

## Windows solo

Use a supported CPython interpreter containing the Paqto installation to test:

```console
python compatibility_tests/run.py solo
```

Run from PowerShell or Command Prompt at the repository root. Grant the normal
local/LAN network permission if Windows Firewall asks. A blocked broadcast can
be UNAVAILABLE while NoDiscovery/TCP/TLS checks still provide useful evidence.

## Android solo

Run the suite using a supported CPython runtime on the Android device. Transfer
the repository tooling and a local Paqto wheel/install without assuming any UI
framework, then run:

```console
python compatibility_tests/run.py solo
```

The host must grant the runtime's network permissions. Android detection uses
standard interpreter indicators (including `sys.getandroidapilevel()` when
present), `sys.platform`, `platform`, and a conservative Android-environment
fallback, because older runtimes can otherwise look like Linux. Android logic
exists only in this reporting suite, never in `src/paqto`.

## Windows ↔ Android: direct endpoint

Assume Windows is `192.168.1.50`, Android is `192.168.1.60`, and TCP port 7450
is allowed on the LAN. On Windows:

```console
python compatibility_tests/run.py pair --role server --scenario direct --bind 0.0.0.0 --advertise 192.168.1.50 --port 7450
```

The server prints its bind, listening port, fixed public test identity
`node-b`, and `waiting for peer`. On Android:

```console
python compatibility_tests/run.py pair --role client --scenario direct --target 192.168.1.50 --bind 0.0.0.0 --advertise 192.168.1.60 --port 7450
```

Direct mode uses `NoDiscovery`; it isolates explicit TCP reachability and the
Paqto stack. The pair establishes mTLS using public test fixtures, verifies the
CA and both certificate identities, performs the Paqto handshake/READY state,
bidirectional sends and requests, ACKs, multiple/concurrent messages, a
reasonable payload, controlled disconnect, a fresh TCP/TLS/Paqto session,
post-reconnect messaging, and cleanup. Finite timeouts prevent indefinite
hangs.

Both reports contain the same server-generated UUID `session_id` and local and
remote OS/architecture/Python/Paqto metadata. The fixed public test
certificates cannot contain every possible private LAN IP. Pair mode therefore
uses verified custom-CA/mTLS identity binding without claiming LAN-IP hostname
coverage; solo mode separately tests certificate/hostname validation.

To invert roles, run the server command on Android with
`--advertise 192.168.1.60`, then run the client on Windows with
`--target 192.168.1.60 --advertise 192.168.1.50`. The roles are test roles,
not platform roles.

## Cross-device discovery

Discovery is a separate scenario and never falls back to the explicit target.
Use the same UDP discovery port and broadcast destination on both devices.
Server:

```console
python compatibility_tests/run.py pair --role server --scenario discovery --bind 0.0.0.0 --advertise 192.168.1.50 --port 7450 --discovery-port 45454 --broadcast 255.255.255.255
```

Client:

```console
python compatibility_tests/run.py pair --role client --scenario discovery --bind 0.0.0.0 --advertise 192.168.1.60 --port 7450 --discovery-port 45454 --broadcast 255.255.255.255
```

Use the subnet broadcast address instead of `255.255.255.255` if required by
the LAN. Each role must discover the other's correct peer id and announced
endpoint. The client then establishes the real TCP/mTLS/READY session using
that discovery result. AP/client isolation, firewalls, sandbox restrictions,
or multicast/broadcast locks can make this scenario UNAVAILABLE even when
direct mode passes. `full` is intentionally not an alias: run direct and
discovery as separate evidence so one failure cannot be hidden.

## JSON reports

Schema version 2 records `generated_at`, mode/profile or pair scenario/role,
status, per-check status and duration, capability aggregates, total duration,
platform and Python details, and exact local Paqto provenance. Pair reports add
the shared `session_id` and validated remote metadata. They never contain
private-key/certificate contents or application secrets. Default filenames are
timestamped and include OS family, architecture, Python major/minor, mode,
scenario, and role.

The repository tracks only `compatibility_tests/reports/.gitignore`; generated
JSON evidence is local unless explicitly copied to a retained evidence area.

## Manual Wi-Fi/network-failure procedure

This observation is manual and must never be marked PASS merely because the
automated pair scenario passed:

1. Establish Android ↔ Windows TCP/mTLS/Paqto READY using the same public test
   identities and record both starting metadata sets.
2. Confirm a request/reply immediately before disruption.
3. Disable Wi-Fi on Android and record the disconnect event/time; do not infer
   success from waiting alone.
4. Re-enable Wi-Fi and wait for the host runtime to regain a reachable address.
5. If the host integration uses explicit network-change notification, call
   `await node.network_changed()` once the route is usable; otherwise observe
   the configured reconnect policy.
6. Verify a new TCP connection, TLS/mTLS certificate authentication, strict
   identity binding, a new Paqto handshake/READY session, and a fresh
   request/reply in both directions.
7. Stop both nodes and verify cleanup. Store this as a separate manual record
   with timestamps and observations; do not edit an automated JSON status to
   PASS.

The automated pair scenario's software disconnect/reconnect checks Paqto's
mechanism but does not replace this physical radio/network test.


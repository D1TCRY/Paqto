# Platform compatibility and interoperability evidence

Paqto calls a platform **SUPPORTED & TESTED** only after the repository
compatibility suite executes on a real interpreter on that platform.
Architectural expectation, source review, CI configuration, and execution on a
different operating system are not substitutes for retained evidence.

The complete operator guide is
[the compatibility suite README](../compatibility_tests/README.md).

## Permanent offline suite

The suite lives under top-level compatibility_tests/, outside src/, and is not
included in the runtime wheel. It is the repository's only compatibility
suite, and its single main entry point is:

~~~console
python compatibility_tests/run.py --help
~~~

Solo execution:

~~~console
python compatibility_tests/run.py solo
~~~

All checks are offline. They use loopback/local networking and bundled public
test-only trust material, never public DNS, HTTP, cloud services, or the
machine trust store.

## Result semantics

Each check is exactly one of PASS, FAIL, SKIP, or UNAVAILABLE. PASS means the
behavior executed and met its assertions. FAIL means execution demonstrated an
error. SKIP is an intentional scenario/profile omission. UNAVAILABLE means the
environment could not supply a capability. A required SKIP/UNAVAILABLE makes
the overall run INCOMPLETE.

| Code | Meaning |
| --- | --- |
| 0 | Every required check passed. |
| 1 | At least one executed/required check failed. |
| 2 | No failure was demonstrated, but required evidence is unavailable. |

Every invocation writes schema-v2 JSON automatically to
compatibility_tests/reports/, unless --json PATH selects another destination.
Reports include platform and Python metadata, exact Paqto import provenance,
status/count/duration details, and capability results without hostname or user
identity. --require-installed rejects imports from this checkout's src/paqto
tree.

## Solo certification rule

The default/full solo profile exercises current local capabilities, including
real local UDP broadcast. The CI profile deliberately skips broadcast because
a hosted runner is not physical-LAN broadcast evidence. IPv6 is probed and
reported as optional to the present IPv4 LAN profile.

A platform/version may be marked tested only when:

1. the unchanged full profile exits 0 on the real target runtime;
2. its JSON is retained with the code revision/release evidence;
3. every claimed capability is PASS, never inferred from another capability;
4. ordinary tests and static checks also pass where the development
   dependencies are available.

The report distinguishes Android from Linux using standard runtime indicators,
including sys.getandroidapilevel() where exposed. This detection exists only in
repository tooling; Paqto runtime behavior never branches on the OS.

## Two-device pair evidence

Run pair in two independent processes, normally on two devices. Direct mode
uses NoDiscovery and an explicit server endpoint:

~~~console
python compatibility_tests/run.py pair --role server --scenario direct --bind 0.0.0.0 --advertise 192.168.1.50 --port 7450
~~~

~~~console
python compatibility_tests/run.py pair --role client --scenario direct --target 192.168.1.50 --bind 0.0.0.0 --advertise 192.168.1.60 --port 7450
~~~

Discovery mode uses real LanDiscovery announcements and never falls back to
direct addressing:

~~~console
python compatibility_tests/run.py pair --role server --scenario discovery --bind 0.0.0.0 --advertise 192.168.1.50 --port 7450 --discovery-port 45454
~~~

~~~console
python compatibility_tests/run.py pair --role client --scenario discovery --bind 0.0.0.0 --advertise 192.168.1.60 --port 7450 --discovery-port 45454
~~~

Both roles exchange minimal application-level metadata only after TLS/mTLS,
strict identity binding, Paqto negotiation, and READY. Their JSON reports
contain the same server-generated session_id plus local and remote
OS/architecture/Python/Paqto values. This allows evidence such as
Android/CPython 3.12 to Windows/CPython 3.14 without assuming equal versions.

The direct scenario verifies bidirectional sends and requests, ACKs,
multiple/concurrent messages, a reasonable payload, controlled disconnect, a
fresh TCP/TLS/Paqto connection, messaging after reconnect, and cleanup.
Discovery additionally requires each device to observe the other peer id and
endpoint and uses the discovered endpoint for the session.

## Real Android execution and boundaries

Run the suite using a supported CPython runtime on the Android device. Transfer
the repository compatibility tooling and a local Paqto wheel/install, grant the
host runtime's network permissions, execute solo and pair, and retain exit
codes plus JSON. No Python UI or embedding framework is part of the procedure.

Android remains unverified until these commands actually run on Android.
A same-host two-process pair is a harness test, not cross-platform evidence.
Local broadcast is not cross-device broadcast. Automated software disconnect
does not prove Wi-Fi OFF/ON, interface changes, suspend/resume, firewalls, AP
isolation, or background policy. Follow the manual network-failure procedure in
the suite README and retain it separately; never mark it automatically PASS.

## Development checks

The compatibility suite is separate from the normal development checks. After
installing the `dev` dependencies, run these commands manually from the
repository root:

~~~console
python -m ruff check src tests examples compatibility_tests
python -m pyright
python -m pytest
~~~

Ruff checks lint rules, Pyright checks types using `pyproject.toml`, and Pytest
runs the unit and integration suite. The repository does not attach these
commands to `git push`.

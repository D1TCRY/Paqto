# Platform conformance and interoperability testing

Paqto calls a platform **SUPPORTED & TESTED** only after the same repository
conformance suite has run on a real interpreter on that platform. Architectural
expectation, emulation by an unrelated operating system, and a passing unit
suite elsewhere are not substitutes for that execution.

## Offline conformance suite

The suite lives in the top-level `platform_conformance/` directory, outside
`src`, so it does not add code or fixtures to the runtime wheel. From a source
checkout, install Paqto into the interpreter being evaluated and run:

```console
python -m platform_conformance --profile full --json paqto-conformance.json
```

The command uses only loopback/local networking and bundled public test
certificates. It makes no Internet request and does not use the machine trust
store. The human result is printed to the terminal and the optional JSON file
contains:

- operating-system family, release, and machine architecture, without a
  hostname or user identity;
- Python implementation/version and installed Paqto version;
- IPv4, IPv6, TCP, UDP, broadcast-discovery, TLS, and mTLS outcomes;
- every check result and PASS/FAIL/SKIP/UNAVAILABLE totals.

Exit codes are stable:

| Code | Meaning |
| --- | --- |
| `0` | Every capability required by the selected profile passed. |
| `1` | At least one executed check failed. |
| `2` | No bug was demonstrated, but a capability required by the profile was unavailable or could not be exercised. |

`PASS` means the behavior ran and met its assertions. `FAIL` means Paqto or the
conformance infrastructure behaved incorrectly. `UNAVAILABLE` means the
environment could not provide an optional capability, such as local broadcast
delivery. `SKIP` means the selected profile deliberately did not execute that
check. A required SKIP or UNAVAILABLE makes the run `INCOMPLETE`, never a full
pass.

## Profiles and certification rule

The `full` profile exercises all current IPv4 LAN capabilities, including a
real local UDP broadcast exchange. A platform/version combination may be
marked **SUPPORTED & TESTED** only when:

1. the unmodified `full` profile exits `0` on the real target runtime;
2. its JSON report is retained with the code revision or release evidence;
3. any capability claimed in the support matrix is `PASS`, not SKIP or
   UNAVAILABLE;
4. the ordinary test and static-check policy also passes for that revision
   where the development toolchain is available.

The `ci` profile skips broadcast delivery because hosted runners often do not
provide a meaningful LAN broadcast domain. It still verifies imports, core
models, IPv4 local TCP/UDP availability, framed TCP, TLS/mTLS, messaging,
lifecycle, cleanup, and limits. A `ci` pass prevents deterministic regressions
but does not certify UDP broadcast support. IPv6 is probed and reported as an
optional capability; it is not silently inferred from IPv4 results.

The checks intentionally use public Paqto APIs. Direct standard-library socket
binds are used only to establish environmental IPv4/IPv6 capability before an
adapter check runs. An IPv6 PASS additionally requires a real Paqto TCP
listener, client connection, framed exchange, and clean close over `::1`; a
successful raw socket bind alone is not reported as Paqto IPv6 success.

## Desktop CI matrix

`.github/workflows/ci.yml` defines real hosted jobs for Linux, macOS, and
Windows with Python 3.10, 3.12, and 3.14. Every matrix job runs:

- pytest in Python development mode with warnings treated as errors;
- Ruff;
- mypy;
- compileall;
- pip dependency consistency checks;
- source-distribution and wheel builds;
- the deterministic `ci` conformance profile and JSON artifact upload.

The workflow does not replace a platform with a Linux alias and does not
change asyncio's event-loop policy. A matrix entry becomes tested evidence only
after the hosted job has actually completed; merely adding the workflow does
not retroactively verify it.

## Running on a real Android runtime

Android remains **UNVERIFIED** until this exact suite runs under a supported
Python interpreter actually executing on an Android device or emulator. The
generic procedure is:

1. install or embed a Python 3.10+ interpreter that supports the standard
   library capabilities required by the selected Paqto features;
2. transfer the Paqto source checkout (including `platform_conformance/`) and
   install the local Paqto wheel or source without requiring Internet access;
3. have the host application grant the network permissions required by the
   operating system and make the conformance entry point invokable;
4. run `python -m platform_conformance --profile full --json
   paqto-conformance-android.json` inside that real runtime;
5. preserve the command exit code and JSON report with the device/runtime and
   Paqto revision evidence;
6. update the support matrix only if every claimed required capability passed.

If broadcast is prohibited by sandbox or network policy, the report must show
UNAVAILABLE and exit `2`. That result is useful evidence for a reduced
capability profile, but it is not a full LAN-discovery certification. Paqto
cannot grant permissions or bypass host policy.

Current state: **awaiting real Android execution**. No Android result is
claimed by the present Windows-host run or by desktop CI configuration.

## Two-device interoperability exercise

`tools/two_device_interop.py` runs the same public Paqto protocol between two
independent Python processes or devices. Device A uses `--role server`; Device
B uses `--role client`. The client verifies a READY handshake, technical ACK,
request/reply, explicit disconnect, a fresh TCP/TLS/Paqto connection, and a
second request/reply. The server exits automatically after the default two
requests, so the scenario is scriptable.

Example with an explicitly provisioned endpoint and mTLS:

```console
python tools/two_device_interop.py --role server --peer-id node-b --remote-peer-id node-a --bind-host 0.0.0.0 --advertised-host 192.0.2.20 --local-port 7450 --security mtls --cert node-b.pem --key node-b-key.pem --ca ca.pem --identity-san-uri-prefix urn:example:peer: --require-identity-match --json device-a.json
```

```console
python tools/two_device_interop.py --role client --peer-id node-a --remote-peer-id node-b --bind-host 0.0.0.0 --advertised-host 192.0.2.21 --local-port 7451 --peer-host 192.0.2.20 --peer-port 7450 --security mtls --cert node-a.pem --key node-a-key.pem --ca ca.pem --identity-san-uri-prefix urn:example:peer: --require-identity-match --json device-b.json
```

Replace the documentation-only addresses and certificate paths with reachable
values. The server certificate must cover the exact hostname or IP passed by
the client. The URI identity encoded in each certificate must match its
`--peer-id` when strict identity matching is enabled.

To test discovery, add the same `--discovery`, `--discovery-port`, and
`--discovery-broadcast-host` values on both devices; the client may then omit
`--peer-host` and `--peer-port`. Run separate sessions with `--security plain`,
`tls`, and `mtls` when evidence for all three transport profiles is required.
No library code changes are needed for Windows-to-Windows, Android-to-Windows,
or Android-to-Android runs; only arguments and provisioned credentials differ.

The tool's JSON reports are per-device evidence. Discovery failure is not
silently converted to explicit addressing in the same run: choose the explicit
mode deliberately when broadcast is unavailable.

# PUBLIC TEST-ONLY TLS MATERIAL — DO NOT USE IN PRODUCTION

These deterministic certificates and private keys are intentionally public.
They provide no secrecy and exist only for Paqto offline compatibility tests.
Never deploy them, use them for real identity provisioning, or copy them into
an application credential store.

The CA signs `node-a` and `node-b`. Their URI SANs bind the fixed test peer ids
`urn:test:peer:node-a` and `urn:test:peer:node-b`; the certificates also cover
loopback (`127.0.0.1` and `localhost`) for the solo hostname-validation check.

Pair scenarios must work with arbitrary LAN IP addresses, so they validate the
CA chain, require mTLS, and strictly bind the URI identity to the Paqto peer id,
but do not claim that the dynamic LAN IP is covered by these fixed fixtures.
The solo TLS check separately verifies certificate hostname rejection/success.

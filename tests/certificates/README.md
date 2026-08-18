# TLS test fixtures

These certificates and private keys are public, deterministic test material for
loopback-only automated tests. They provide no secrecy and must never be used
for production, deployment, or identity provisioning.

The trusted node certificates are signed by ca.pem. The untrusted certificate
is signed by a separate CA that is intentionally not included, so verification
failure can be tested without network access.

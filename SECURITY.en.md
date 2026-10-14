# Security policy

[中文](SECURITY.md) | [English](SECURITY.en.md)

## Supported scope

Security maintenance targets the current published version. Published artifacts cannot be replaced in place; security updates use a separate version.

## Reporting vulnerabilities

Report privately through [GitHub Security Advisories](https://github.com/nasa-runtime/nasa-kafka/security/advisories/new).

Include affected versions, impact, reproduction conditions, and suggested protection. Do not open a public issue or disclose directly exploitable details before coordinated disclosure. Maintainers assess impact, prepare an update, and coordinate disclosure. Ordinary feature requests and questions belong in GitHub Issues.

## Runtime security boundaries

Each source independently configures TLS/SASL identity. Credentials are not exposed through logs or business administration handles. `KafkaHandle.admin()` is always read-only. Source-local `admin-write: true` enables a writable `administration()` view only for that source; broker ACLs still apply. Trusted configuration or plans approve topics, output partitions, schema IDs, and durable-task destinations. Message headers do not confer write authority.

`KafkaProxy` and `KafkaClients.require()` are resource-owner entry points with administration and whole-source lifecycle authority, intended for trusted assembly. `KafkaHandle` narrows business APIs, not arbitrary untrusted JVM execution. Isolate untrusted extensions at the process/deployment layer instead of treating Java visibility as a sandbox.

After managed business admission closes, old handles and lanes cannot start new business sends. DLT, Transfer, and DLT replay can finish only with registered task authority bound to the actual source, physical lane, approved purpose, and task lifetime. Matching source/lane names or a retained cooperation method cannot grant it. Timeout and cancelled waits neither retract broker writes nor release actual in-flight quotas.

A publishing Future is a caller-owned wait result. Completing or cancelling it cannot change private delivery evidence, and flush barriers expose no mutable receipts. Use actual delivery and shutdown state; a caller-created success is not broker confirmation.

Declarative consumers validate signatures and actual proxy access during registration. Closed Java modules or missing exported JDK-proxy methods reject registration. The application controls package openness; the runtime does not bypass its proxy/interceptor chain.

Transactions, checkpoints, and digests do not replace authentication or business authorization. External producers must obey the wire contract. Cross-system effects need independent idempotency or fencing. See [broker security](README.en.md#broker-security-and-build-capabilities) and [capability boundaries](README.en.md#capability-boundaries).

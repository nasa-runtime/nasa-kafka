# Contributing

[中文](CONTRIBUTING.md) | [English](CONTRIBUTING.en.md)

Before a substantial change, open a GitHub issue describing the background, compatibility impact, and expected behavior. Small changes with clear boundaries may be submitted directly as pull requests.

## Development environment

- JDK 21 or later
- Maven 3.6.3 or later

```bash
mvn -B -ntp clean verify
```

## Code requirements

- Keep the Kafka core explicitly constructible and independently runnable. Spring integration uses named resources and lifecycle entry points rather than silently creating another consumer in a business call.
- Explain binary and wire compatibility for public API changes.
- Prefer existing components and project facilities when business semantics are equivalent. For execution infrastructure, compare ordering domains, actual completion, capacity, failure isolation, and lifecycle together; interface similarity alone does not establish equivalence.
- Keep interfaces, records, enums, and dedicated exceptions with their functional package. The root contains the directory, source owner, and business handle. Source Javadoc explains responsibility, confirmation points, and failure boundaries.
- Write source comments in Chinese, preserving identifiers, protocol names, and configuration keys.
- Method comments include `业务作用`; meaningful parameters and return descriptions explain boundaries, failures, and side effects.
- Explain protection, acknowledgment revocation, task leases, external effects, and durable-progress publication immediately before the relevant branch. A method header does not replace ordering constraints.
- Public comments retain current design reasons and constraints without tool attribution, generation provenance, or temporary development labels.
- Local quality code is for local use only and does not enter the public repository or product archives. Product code, public documentation, and release configuration do not depend on its entry points.
- Maintain shared Java/Rust wire, identity, defaults, acknowledgment responsibility, and failure categories together; check corresponding entry points when changing either side.
- Keep Chinese/English README files, Javadoc, POM metadata, and final archives consistent about capabilities, initialization, limits, and failure consequences.
- Java visibility does not grant resource authority. Cross-package calls must retain actual-source, task-lifetime, and approved-target checks and must not expose mutable delivery evidence.
- Do not include generated directories, IDE metadata, secrets, tokens, logs, or machine-specific configuration.

## Submissions

Commit messages explain what changes and why. Pull requests include a concise description, compatibility implications, and relevant validation evidence.

Contributions are licensed under Apache-2.0 OR MIT.

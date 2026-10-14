# Compatibility and integration contract

[中文](CHANGELOG.md) | [English](CHANGELOG.en.md)

The [README](README.en.md) and Javadoc describe current capabilities, configuration, and usage. Git history records source changes.

## Dependencies and entry points

Maven coordinates are `io.github.nasa-runtime:nasa-kafka`; the Java root package and automatic module name are `io.github.nasaruntime.kafka`. JDK 21 or later is required. Public types live in functional packages such as producer, consumer, transaction, checkpoint, and replay; see the README for exact paths. The core uses Kafka Java clients directly; Spring Boot supplies configuration and named-resource assembly. Use the component's declared dependencies rather than inferring them from a parent POM.

Business code should use `KafkaHandle`. `KafkaProxy` and `KafkaClients` own source resources and lifecycle. Declare a default source and explicit source identities; a failed explicit lookup never falls back. Single-source and named-source configuration cannot be mixed, and unknown fields reject startup.

## Functional packages and call authority

The root package contains only the top-level entry types `KafkaProxy`, `KafkaClients`, and `KafkaHandle`. Import records from `consumer.KafkaRecord`, delivery results from `producer.Delivery`, and annotations/assembly from `spring`. Old package paths have no binary aliases. Update imports, reflective class names, and explicit `@Import` declarations and recompile. Boot discovers `io.github.nasaruntime.kafka.spring.NasaKafkaConfig`.

Public members documented for framework-package cooperation are not stable business APIs. Internal publication requires an actual source-issued permit bound to task purpose; single-argument `sendBytesInternal` follows ordinary business admission. `ProducerLane.FlushBarrier` is an opaque lane-created object without public construction, sequence, or Future-array accessors. Business callers use `flush()` to await real delivery and receipt responsibility.

`@KafkaConsumer` methods must be public instance methods, while the declaring Bean class may be non-public. Registration validates access through the actual proxy; module access restrictions or a missing exported proxy-interface method produce `CONFIG`. Existing AOP invocation chains remain active.

## Identity, wire format, and recovery

- A group determines assignment and offset ownership. Valid names retain strict UTF-8 bytes. Names outside the shared domain require explicit migration, not trimming, substitution, or automatic offset copying.
- `X-Nasa-Event`, `X-Nasa-Payload-Codec`, `X-Nasa-Payload-Mode`, `X-Nasa-Passthrough`, and DLT origin fields are wire contracts. Events are not normalized; null/empty keys, tombstones, and ordered duplicate headers remain distinct.
- JSON and ProtocolBytes are explicit codec choices. A Java DTO name does not determine the other language's decoding support. Schema, codec, and mode conflicts follow input policy.
- Auto/Manual Ack advances only the current assignment's contiguous safe prefix. Redelivery may repeat external effects, requiring stable business identity and idempotency.
- Kafka transaction recovery reuses stable deployment identity. Finite replay, transfer, and DLT replay additionally retain the plan, physical resource identities, and a valid checkpoint. Kafka transactions exclude databases and other clusters.

## Configuration conversion and rolling deployment

Use [configuration migration](README.en.md#configuration-migration-and-rolling-deployment) for a candidate and explicit decisions; it changes neither configuration files nor broker positions. Establish reliable publication, input capacity, error policy, Admin write grants, and Ready requirements before deployment. Replacing a binary cannot revoke committed business facts. A rollback must still understand existing wire and persistent formats.

# nasa-kafka

[中文](README.md) | [English](README.en.md)

A multi-source Kafka event runtime for **JDK 21 or later**. It coordinates publishing, consumption, historical processing, and resource shutdown across multiple clusters in one application. Each source provides isolated producer lanes, bounded ordered consumption, same-cluster consume/output transactions, durable finite replay, recoverable single-destination transfer, finite DLT replay and quarantine, and observable health and delivery state.

The core uses the native Kafka Java client. Spring Boot supplies named Beans, configuration, and lifecycle integration; explicitly constructed `KafkaProxy` instances work without a Spring container. Java and Rust `nasa::kafka` share event wire formats, consumer-group identities, acknowledgment responsibilities, transaction identities, and durable task formats, allowing event exchange and compatible task recovery.

## Core value and boundaries

| Capability | Application value | Boundary |
| --- | --- | --- |
| Multiple sources and isolated producer lanes | Connect business and audit clusters together; isolate control and bulk producers and quotas within a source | Unknown explicit source names never fall back; memory, CPU, and brokers remain shared physical resources |
| Key ordering and multiple consumers per group | Preserve same-key order while processing other keys or partitions concurrently; distribute polling and execution across owner slots | Commit only the contiguous safe prefix; an invocation retains ordering and capacity until it actually exits |
| Bounded input and failure handling | Limit outstanding input across process, source, group, partition, and key; retain explicit retry and DLT responsibility | Input reservations are not a JVM heap limit; an unknown delivery outcome is not a cancelled write |
| Durable replay and cross-cluster transfer | Freeze history ranges and resource identities; resume Java/Rust tasks from a shared checkpoint | Destination confirmation precedes durable progress; duplicates require stable event IDs and idempotency |
| Kafka transactions | Atomically commit input positions and approved outputs within one cluster | Does not include databases, HTTP, or another Kafka cluster |
| Managed access and observable shutdown | Named Beans, read-only business administration, current Ready evidence, health, metrics, and shutdown reports | Host stop, resource exit, and business success are different facts |

The poll owner exclusively accesses its native consumer and maintains assignment epochs and commits. Decoding and business execution use bounded resources. Producer lanes observe actual delivery, while finite tasks hold checkpoint authority and drain with their source and destination dependencies. Cancelling a caller's wait does not leave an operation without an owner.

Start with [installation](#installation-and-assembly), [multiple sources](#multiple-sources-and-producer-lanes), and [declarative consumption](#typed-and-declarative-consumption). See [functional packages](#functional-packages-and-entry-points) for imports, [group concurrency](#multiple-consumers-per-group) and [configuration](#configuration-values-and-inheritance) for capacity, and [control and shutdown](#control-administration-and-shutdown) and [metrics](#metrics-export) for operations.

## Runtime architecture and confirmation points

```text
Spring configuration / explicit assembly -> KafkaClients named directory -> KafkaProxy source owner
                                                                          |- KafkaHandle business access
                                                                          |- ProducerLane -> native producer -> delivery
                                                                          |- group / owner slots -> poll -> route / bounded execution
                                                                          |                                  -> Ack -> safe prefix -> commit
                                                                          |- TransactionPlan -> dedicated consumer / producer
                                                                          |- Replay / Transfer / DLT replay -> checkpoint
```

Each source freezes its connection and security configuration. Each ordinary consumer slot owns a native consumer. Business completion, Ack, DLT delivery, and broker commit are separate facts; only current-assignment responsibility can enter the commit prefix. After rebalance, an old Ack cannot advance the new owner's position. Old invocations still executing in the same process retain ordering and capacity.

| Path | Confirmed success | Unknown or interrupted outcome |
| --- | --- | --- |
| Publishing | Broker delivery confirmation | `fire` admission is not delivery; cancelling a wait does not retract the send |
| Ordinary consumption | Business confirmation forms a safe prefix; broker commit confirmation releases the corresponding input responsibility | An unsafe prefix blocks progress; external effects may repeat |
| Same-cluster transaction | Confirmed commit of the whole batch's positions and approved outputs | An unknown commit protects the owner; recover using the stable identity |
| Finite task | Confirm processing or delivery, persist checkpoint progress, then expose progress | Exit before persistence may repeat work; no cross-resource atomicity |

Ready uses current assignment and resource evidence, not thread existence or a previous success. Shutdown stops new work, waits for admitted work to exit, then closes dependent resources. Reports distinguish stop, drain, resource exit, and failure; a timeout never marks unfinished work successful.

An assignment epoch governs acknowledgment for that assignment; a checkpoint lease governs durable task progress; a transaction identity is fenced by Kafka's coordinator. None can revoke an external database, HTTP, or cross-cluster effect that has already occurred. Choose ordinary Ack, a same-cluster transaction, or resumable finite processing according to the systems that must confirm business success.

## Shared Java and Rust contracts

| Contract | Java | Rust |
| --- | --- | --- |
| Named sources and business access | `KafkaClients`, `KafkaHandle` | `Application::kafka`, `napp::KafkaHandle` |
| Typed routing and batch acknowledgment | `@KafkaConsumer`, listeners, `AckMode` | `#[kafka_consumer]`, consumer traits, `AckMode` |
| Same-cluster atomic processing | `TransactionPlan`, `TransactionContext` | Corresponding plans and contexts through `nasa::kafka` |
| Finite history and durable recovery | `ReplayPlan`, `TransferPlan`, `CheckpointStore` | Corresponding plans and shared encodings |
| Dead-letter evidence and replay | `DltReplayPlan`, `DltOrigin` | Corresponding plans and origin evidence |

Wire bytes, identities, defaults, and failure responsibility are shared. Configuration spelling, asynchronous types, and native-client options follow each language. Valid UTF-8 group and event names retain their original bytes. Null and empty keys, ordered duplicate headers, and tombstones remain distinct. Cross-language recovery requires the same physical resources, stable task identity, compatible plan, and valid checkpoint; matching topic strings alone are insufficient.

See [nafka](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafka/README.md) for the Rust component. Database Outbox/Inbox adapters, cross-cluster atomic transactions, and exactly-once arbitrary external effects are outside this component.

## Same-cluster consume and output transactions

`KafkaProxy.transact(TransactionPlan, TransactionProcessor)` uses a dedicated subscribe consumer and transactional producer to atomically commit a complete input batch's outputs and group positions. The plan approves every output topic/partition and binds the actual physical cluster ID. Transaction input requires `read_committed`; explicit `read_uncommitted` is rejected. Downstream readers must also use `read_committed` to exclude uncommitted outputs.

Transaction groups follow the ordinary group domain: 1–1024 strict UTF-8 bytes, no NUL, not entirely shared whitespace, and no leading or trailing U+0000–0020. Valid names are preserved. The plan validates at construction and execution revalidates. Invalid groups and native-configuration preflight failures produce `INVALID_CONFIGURATION`; isolation conflicts retain `ISOLATION_MISMATCH`. These rejections precede identity registration, native-resource creation, and business execution.

```java
var plan = new TransactionPlan("primary", actualClusterId,
    new TransactionIdentity("order-service", "replica-0", "settlement"),
    "order-settlement", Set.of("orders"),
    Set.of(new TopicPartition("settled-orders", 0)),
    AckMode.MANUAL, TransactionLimits.DEFAULT);
var transaction = proxy.transact(plan, context -> {
    for (int index = 0; index < context.records().size(); index++) {
        var input = context.records().get(index);
        context.send("settled-orders", 0, null, input.key(), input.payload(), input.headers());
        context.ack(index);
    }
});
transaction.awaitReady(Duration.ofSeconds(30));
```

`TransactionContext.send` buffers this batch's outputs. Sending begins only after the callback returns successfully and Manual mode has acknowledged every input. `deadLetter` preserves the failed input and DLT origin headers through the same transactional producer and acknowledges that input. Missing acknowledgment, business failure, or lost authority aborts the entire batch. Retry from the batch start requires a confirmed abort. Ordinary lanes, other sources, databases, HTTP, and sockets are outside the transaction.

Each buffered output costs 64 bytes plus raw key, payload, and complete ordered header bytes. Null and empty remain distinct. Exceeding capacity, using an unapproved target, or supplying invalid timestamps or headers latches failure for the whole batch, even if business code catches the exception. Only the creating synchronous callback thread may use the context; sealing, close, or lost authority prevents further sends and acknowledgments.

`TransactionIdentity(application, slot, owner)` comes from stable deployment identity. Reuse it after a crash of the same slot; concurrently active slots must differ. Duplicate cross-process identity causes fencing. Do not mix ordinary and transactional consumers in the same cluster/group. The process rejects occupied transaction identities; a source also rejects sharing a group between its ordinary commit coordinator and transaction owner. Both languages derive the same `transactional.id`. Recovery initializes transactions before reading broker-committed group positions.

Defaults: 100 input records/8 MiB, 1000 output records/16 MiB, 10-second callback, 60-second transaction, 10-second native-operation budget, and at most 3 retries of confirmed-aborted batches. Each source permits 16 transaction owners. Each has an independent poll thread maintaining membership and one synchronous business batch at a time. `KEY_ORDERED`, asynchronous handlers, and manual assign are unsupported here. The dedicated producer inherits source producer settings but fixes transactional identity, idempotence, all ACKs, and plan budgets; it never reuses an ordinary lane. Transaction consumer and producer configuration reject `interceptor.classes`.

`TransactionHandle.status()` exposes epoch, assignment, Ready, actual drain, confirmed commit/abort counts, and the last batch's input range. Send acknowledgment alone is not transaction success. `COMMIT_UNKNOWN` and `ABORT_UNKNOWN` stop new work; do not treat timeout as confirmed abort or resend using a random identity. Initialize with the same stable identity and resume from broker positions.

The broker's inter-broker listener must be reachable within the broker network so the coordinator can write transaction markers. Client connectivity alone is insufficient: unresolved markers can keep initialization returning concurrent-transaction or timeout errors, leaving the task unready without new input. `transaction.state.log.replication.factor` and `transaction.state.log.min.isr` must fit available replicas; ordinary messaging does not prove `__transaction_state` is usable.

Cancellation or callback timeout revokes uncommitted-batch authority without proving user code has exited. An already-started commit waits for its actual outcome. `awaitDrained` and source shutdown retain callback and native-resource responsibility until actual exit; only then is identity released. Ready becomes invalid after rebalance, cancellation, or failure.

## Multiple sources and producer lanes

`sourceId` identifies a logical source; `ProducerLane` isolates producers, queues, byte budgets, and receipt processing within it. A source name is not a physical cluster identity. `expected-cluster-id` can constrain the actual connection target.

```yaml
nasa:
  kafka:
    default-source: primary
    export-static: true
    sources:
      primary:
        bootstrap-servers: 127.0.0.1:9092
        group-id: order-service
        producer:
          acks: all
          enable-idempotence: true
          lanes:
            control:
              queue-buffering-max-messages: 1000
              queue-buffering-max-kbytes: 8192
        behavior:
          max-consume-attempts: 3
          dead-letter-topic-suffix: .DLT
          dead-letter-required: true
      audit:
        bootstrap-servers: 127.0.0.1:19092
        group-id: audit-service
```

| Source | Business Bean | Resource-owner Bean | Explicit business lookup |
| --- | --- | --- | --- |
| primary | kafkaHandle | kafkaProxy | `clients.handle("primary")` |
| audit | auditKafkaHandle | auditKafkaProxy | `clients.handle("audit")` |

`default-source` selects type-based injection, `load()`, and listeners without an explicit source. Changing the default does not rename fixed Beans. An unknown explicit source never falls back: `load(name)` returns null, while `require(name)` throws.

Prefer injecting `KafkaHandle`, with `@Qualifier("auditKafkaHandle")` for a named source. It exposes publishing, consumption control, health, replay, transfer, and transactions without returning `KafkaProxy`, native clients, mutable configuration, or whole-source shutdown authority. `handle.admin()` is always read-only; topic creation/deletion, partition expansion, and configuration mutation return `AUTHORIZATION` before connection. `readOnly()` can narrow another Admin view but cannot restore write authority. Explicit source-local `admin-write: true` enables `handle.administration()`; the default is false, grants do not cross sources, and broker ACLs still apply.

`KafkaProxy`, `KafkaClients.require()`, and static `KafkaProxy.require()` are resource-owner entry points with administration and lifecycle authority, intended for assembly, independent tools, and existing callers. Keep them separate from business injection. Closed handles, Admin views, and lanes cannot admit new operations. `handle.awaitClosed(deadline)` may continue waiting only after the host has handed over final cleanup; it cannot close an active source early.

After managed business admission closes, retained lanes cannot start new sends. Registered DLT/Transfer delivery may finish only within the original task's responsibility, actual source, physical lane, approved destination, and resource lifetime. Business handles do not export that authority, and a wait timeout does not grant it again.

Only one active owner may export the JVM static directory. Independent Spring containers use `export-static: false` and their own `KafkaClients`. All routes are registered before consumption starts; another container cannot overwrite an active directory.

Source and lane names match `[a-z][a-z0-9_-]{0,63}`. Named `sources` cannot be mixed with legacy root connection/producer/consumer settings. Unknown configuration fields reject startup. Legacy `nasa.kafka.bootstrap-servers` maps the single source to primary.

## Installation and assembly

```xml
<dependency>
    <groupId>io.github.nasa-runtime</groupId>
    <artifactId>nasa-kafka</artifactId>
    <version>1.0.0</version>
</dependency>
```

Requires JDK 21 or later; builds require Maven 3.6.3 or later. `release=21` retains the Java 21 API and bytecode baseline. Spring Boot discovers `spring.NasaKafkaConfig`; ordinary Spring uses `@Import(NasaKafkaConfig.class)`. This component owns its directory and container lifecycle integration and does not require `nasa-spring-boot-starter`. Missing connection configuration or `nasa.kafka.enabled=false` creates no background resources.

### Explicit named-source assembly

This example needs no Spring container. Supply business and audit bootstrap servers as the two arguments. Create `orders` in the business cluster beforehand and configure the required identities and permissions; the example neither creates topics nor changes broker configuration.

```java
import io.github.nasaruntime.kafka.KafkaClients;
import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.config.NasaKafkaProperties;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public final class NamedSources {
    /**
     * 业务作用：装配两个命名源，在所需集群可达后发布订单事件，并归还全部源资源。
     * 返回：发布与关闭均确认后正常结束；启动、交付或关闭未确认时向调用方报告异常。
     *
     * @param args 业务集群与审计集群的 bootstrap servers，必须依次提供两个参数。
     */
    public static void main(String[] args) {
        if (args.length != 2) throw new IllegalArgumentException("需要业务与审计集群地址");
        var primary = new NasaKafkaProperties();
        primary.setBootstrapServers(args[0]);
        var audit = new NasaKafkaProperties();
        audit.setBootstrapServers(args[1]);
        var clients = new KafkaClients(Map.of(
                "primary", new KafkaProxy("primary", primary),
                "audit", new KafkaProxy("audit", audit)), "primary", false);
        try {
            clients.start(List.of());
            clients.awaitReady(Duration.ofSeconds(10));
            var delivery = clients.handle("primary")
                    .message("orders", Map.of("id", "order-42"))
                    .event("created").key("order-42")
                    .sendAndWait(Duration.ofSeconds(5));
            System.out.println(delivery);
        } finally {
            // 交付等待失败也可能留下在途发送，源所有者继续承担实际退出责任。
            clients.shutdownUntil(System.nanoTime() + Duration.ofSeconds(30).toNanos());
        }
    }
}
```

Explicit construction does not register a static directory. `exportStatic=false` keeps names local to this `KafkaClients`. Publishing-only sources need no `group-id`; consuming sources require a valid configured group or an explicit listener group. For consumption, pass the complete listener collection to `clients.start(listeners)`. For a single independent proxy, register listeners or annotated objects, call `before()`, and let the creator call `shutdownUntil`.

`awaitReady` checks brokers for all `required=true` sources and waits for registered ordinary-group assignments. Publishing-only sources have no consumer owners. Ready does not prove target write permission or delivery. `expected-cluster-id` rejects a wrong cluster during startup; transactions and finite tasks have separate status. Retain the owner after shutdown timeout and continue waiting rather than interpreting an exception as resource exit.

## Functional packages and entry points

Types, interfaces, records, enums, and dedicated exceptions live with their capability. Package names below are relative to `io.github.nasaruntime.kafka`. The root contains only the three top-level entry types `KafkaProxy`, `KafkaClients`, and `KafkaHandle`. Exact signatures and call conditions are in each type's Javadoc.

| Package | Responsibility and entry types |
| --- | --- |
| Root | `KafkaClients`, `KafkaProxy`, `KafkaHandle`: directory, resource ownership, business access |
| `config` | `NasaKafkaProperties`, `KafkaSettings`, `KafkaMigration`: configuration, overrides, conversion |
| `spring` | `NasaKafkaConfig`, `KafkaConsumer`: assembly and consumer annotations |
| `producer` | `ProducerLane`, `PublishBuilder`, `PublishMessage`, `Delivery`: publication and delivery |
| `consumer` | `KafkaRecord`, `ConsumeContext`, `AckMode`, `StartOffset`, `AssignmentHandle`, `GroupHealth`: acknowledgment, control, context, health |
| `consumer.listener` | `KafkaListener` and single, batch, asynchronous, broadcast, and raw listener contracts |
| `admin` | `KafkaAdmin`, `OffsetObservation`, `KafkaPartitionLag`: administration and offsets |
| `transaction` | `TransactionPlan`, `TransactionContext`, `TransactionProcessor`, `TransactionHandle`, `TransactionException` |
| `checkpoint` | `CheckpointStore`, `CheckpointLease`, `ReplayCheckpoint`, File/MySQL stores, `CheckpointException` |
| `replay` | `ReplayPlan`, `ReplayProcessor`, `ReplayIdentityProvider`, `ReplayHandle`, `ReplayException` |
| `transfer` | `TransferPlan`, `TransferRegistries`, `TransferHandle`, `TransferException` |
| `dlt` | `DltOrigin`, `DltEvidence`, `DltReplayPlan`, `DltReplayHandle`, `DltReplayException` |
| `schema` | `SchemaRegistryClient`, `ConfluentSchemaRegistry`, `SchemaRegistryTls`, `ConfluentEnvelope` |
| `observability.metrics` | `MetricsSink`, `MetricsOptions`, `MetricCatalog`, backend adapters |
| `observability.trace` | `KafkaTraceSink`, `KafkaTraceContext`, `KafkaTraceSpan` |
| `protocol` | `KafkaHeaders`, `KafkaWire`: shared wire rules |
| `lifecycle` | `CloseReport`: stop, drain, and resource-exit evidence |
| `support` | `KafkaCapabilities`: native capabilities; other types support internal client coordination |
| `exception` | `KafkaFailure`: base failure categories across capabilities |

```java
import io.github.nasaruntime.kafka.KafkaHandle;
import io.github.nasaruntime.kafka.config.NasaKafkaProperties;
import io.github.nasaruntime.kafka.consumer.AckMode;
import io.github.nasaruntime.kafka.consumer.KafkaRecord;
import io.github.nasaruntime.kafka.consumer.listener.KafkaRecordListener;
import io.github.nasaruntime.kafka.spring.KafkaConsumer;
import io.github.nasaruntime.kafka.spring.NasaKafkaConfig;
```

Use explicit imports from the owning package. Callers using other package paths must update imports and recompile; there are no binary aliases for old paths. Nested types remain under their owner, such as `TransferPlan.Target` and `TransferException.Code`. Boot metadata and explicit `@Import` both use `io.github.nasaruntime.kafka.spring.NasaKafkaConfig`.

Members documented for framework-package cooperation are public to permit cross-package assembly, not stable business entry points. Their visibility does not bypass resource, task-lifetime, or authorization checks.

## Typed and declarative consumption

```java
@Component
public class OrderHandler {
    /** 业务作用：处理订单事件并确认本次业务成功。参数说明：record 为订单记录。返回：正常返回才使本次确认生效。 */
    @KafkaConsumer(topics = {"orders"}, event = "created",
            sources = {"primary"}, ackMode = AckMode.MANUAL)
    public void created(KafkaRecord<Order> record) {
        processOrder(record.value());
        record.ack();
    }
}
```

Annotations accept `T`, `KafkaRecord<T>`, `List<T>`, or `List<KafkaRecord<T>>`, optionally followed by `ConsumeContext`. Return `void` or `CompletionStage<Void>`; responsibility remains until asynchronous completion. Spring resolves placeholders in topic, event, group, and source declarations.

Consumer methods must be public instance methods, but their Bean class may be package-private. Inherited methods and JDK/CGLIB proxies retain the actual proxy invocation chain. A JDK proxy must expose the relevant consumer interface method. If module boundaries prohibit access, registration rejects with `CONFIG`; the application must open the appropriate package.

Interface entry points include `KafkaSingleListener`, `KafkaBatchListener`, `KafkaRecordListener`, `KafkaRecordBatchListener`, `KafkaAsyncListener`, and `KafkaAsyncBatchListener`. Explicit construction uses `proxy.register(listener)` and then `before()` after all registration; `registerAnnotated` accepts annotated instances. Routes freeze at startup.

Actual ordinary group names are **1–1024 strict UTF-8 bytes, contain no NUL, are not all whitespace, and have no leading or trailing U+0000–0020**. The shared whitespace set is U+0009–000D, U+001C–0020, U+1680, U+2000–2006, U+2008–200A, U+2028–2029, U+205F, and U+3000. NEL, NBSP, U+2007, and U+202F are valid identity characters. Native string configuration trims edge ASCII control/space characters, so those names are rejected. Internal spaces and valid non-ASCII edge characters remain unchanged. There is no trimming, substitution, or Unicode normalization; lengths are UTF-8 bytes, including valid names longer than 255 bytes.

Listeners, annotations, default `group-id`, group-concurrency keys, manual assign, management queries, and migration candidates use this domain. Unpaired Java UTF-16 surrogates produce `CONFIG`. Null/empty default groups permit publishing-only sources; null/empty listener groups inherit the default, while other invalid explicit names cannot fall back. Consumption always requires a valid group. Source construction freezes configuration and rejects invalid identities before native requests or owner creation.

The 1024-byte limit keeps group names representable in DLT and recovery evidence. Groups outside the domain require a compatible external client or an explicitly planned migration with offset review. The runtime never truncates names or copies committed offsets automatically. Registration resolves inherited/nested generics and Spring proxies; unresolved types or conflicts with `paramType()` reject registration. Batches contain consecutive records from one partition and route: `A/B/A` never becomes `A/A/B`.

## Multiple consumers per group

Subscription groups can have multiple owner slots, each with a native consumer and the same final group ID. Kafka assigns disjoint partitions. More slots distribute polling, decoding, and execution; each partition's assigned owner alone advances its safe prefix. Fixed `assign` always has one slot.

Place these settings under a source:

```yaml
consumer:
  concurrency: 3
  group-concurrency:
    order-service: 2
behavior:
  max-record-bytes: 1048576
  max-progress-bytes-per-group: 67108864
  max-handler-in-flight: 12
```

`concurrency` defaults to 1, range 1–64. `group-concurrency` matches the resolved actual group exactly, including derived broadcast groups. Slot counts freeze at group creation. Multiple slots append `-slot-N` to consumer `client.id` and explicitly configured `group.instance.id`; static member identities must still be unique across processes.

All slots share group input records, reserved bytes, and actual handler limits; adding slots never multiplies those limits. Any owner may use free capacity, and owner count may exceed handler capacity. Input responsibility survives until confirmed broker commit or explicit return for redelivery; logical timeout does not release an executing invocation.

Ordinary DLT uses a separate source-wide envelope pool and actual-call limit, default 16 through `behavior.dlt-max-in-flight`. Envelope count and bytes cover waiting, delivery, completion, and retry across all ordinary groups. Capacity shortage retains source responsibility without starting another call. Failed batches transfer records sequentially and may exceed the pool's record capacity. Reserve before copying; envelope bytes include the original input, UTF-8 topic/group, and a 4096-byte origin-evidence allowance. A record exceeding an empty pool stops that partition unacknowledged. Dispatch rotates partitions, starting at most one ordering domain per partition in each round.

`consumer.max-assignment-partitions` defaults to 10000 distinct assigned partitions across slots; a local takeover counts once. Fixed ranges validate before native assign, subscriptions before Ready and dispatch; excess protects the owner. `behavior.max-retry-entries` defaults to 100000 input records with retained failure, deferral, or missing-Ack counts. Batch failure reserves all new entries atomically; shortage protects the failed partition without committing it, while healthy partitions can progress. Counts clear on success, confirmed DLT, lost ownership, or explicit positioning and are not durable retry counts. Both limits accept 1–2147483647. Flat configuration keys are `max-assignment-partitions` and `max-retry-entries`.

Local rebalance retains actual invocation ordering: a new owner waits for an old same-key or exclusive-partition call to exit, while old acknowledgment authority expires immediately. Cross-process external effects still require idempotency or destination fencing.

Ordinary typed/raw subscribe and assign share process, source, group, partition, and key input limits. All groups in a proxy share source capacity; proxies in the same component classloader share process capacity. The first valid source freezes process limits; a different later declaration fails with `CONFIG` before native resources exist. Changing them requires restarting that loading environment. Retries reuse reservations, and lost ownership or cancelled waits do not free bytes held by actual work. A record too large for any empty byte pool enters oversized-record protection.

Partition and same-partition key quotas distinguish null and empty keys. Identity includes source, group, and partition. Waiting admission retains a fixed digest; collisions conservatively combine capacity but do not change full-key execution ordering. A key limit retains the record's reread position while already admitted other keys may finish; not-yet-read later records in that partition wait for the boundary record's capacity.

For raw payload, key, and complete ordered headers totaling `R`, reserve **`8 × R`** bytes once and count one record. The reservation covers input, owned delivery, header maps, and complete-key ordering copies until the final actual responsibility exits. `max-record-bytes` and `max-batch-bytes` still measure raw `R`: an 8 MiB input budget admits at most 1 MiB of raw data. A full single-record reservation exceeding any empty pool stops that partition. Record counts bound fixed metadata.

These quotas exclude native fetch buffers, decoded objects, thread overhead, and business-retained copies. Transactions, replay, transfer, producer lanes, and DLT envelopes have separate budgets. Consecutive batches default to 500 records and 16 MiB through `max-poll-records` and `behavior.max-batch-bytes`; one otherwise valid record may occupy a batch larger than its batch-byte limit, subject to single-record and input limits.

`groupSlotsHealth(group)` returns stable-slot snapshots. `groupHealth(group)` aggregates assignment, active calls, reserved bytes, and counters, using the slowest poll timestamp. All slots must have joined without conflicting partition ownership for group Ready; joined idle slots are allowed. Group epoch reflects the complete slot-epoch vector, not a single-slot epoch or cross-process lease.

Pause/resume/seek/position split by current ownership and revalidate on the actual owner; subscription changes reach all slots. Cross-slot operations are not atomic. Aggregate calls report `CONTROL_OUTCOME_UNKNOWN` after partial success or timeout after execution begins; use `controlPartitions` for per-target evidence. Reconcile from actual slot state. Recovery affects only eligible slots. Shutdown notifies all slots before waiting under one deadline; timeout does not free unfinished responsibility.

### Ordered execution inside the JVM

For ordinary consumption, `KafkaPollTask` selects eligible records by actual offset on the poll owner. `KafkaGroupBudget` atomically acquires group execution capacity and a partition or full-key ordering permit shared by all owner slots. JDK virtual threads then perform decoding and business execution. An asynchronous listener's `CompletionStage` must finish and invocation context cleanup must complete before the owner returns the permit. Retry backoff, DLT delivery, and the null-key partition barrier participate in ordering. Rebalance invalidates old Ack authority without releasing an old invocation that is still executing.

The nested `Partition` in `KafkaPollTask` holds Kafka assignment, input, and safe-position state; it is not the `nasa-core.base.Partition` scheduler. The latter provides FIFO and ordered work stealing over fixed hash slots and task types, with execution ending when `Task.exec()` returns. Ordinary consumption uses the Kafka ordering permits and virtual-thread execution path described above: task admission order, asynchronous business completion, failure retry, and broker commit have distinct responsibilities. A generic task's completed state alone cannot authorize subsequent consumption or position advancement.

Shared primitives use `nasa-core` facilities including `AnyHolder`, `RecycleLinkedMap`, `ProtocolBytes`, event interfaces, and related utilities. Reusing a general component must preserve ordering domains, actual completion, capacity, failure isolation, and shutdown semantics together. Background work detached by a business handler from its returned result is outside the invocation responsibility that the framework can await.

## Acknowledgment, ordering, and failure responsibility

Default `PARTITION_ORDERED` serializes each partition while other partitions run concurrently. `KEY_ORDERED` serializes equal keys within one partition and permits different keys concurrently; null keys fall back to partition serialization. Key ordering rejects batch listeners and requires a consistent execution mode across routes in the group. Partition expansion may move a key's new records to a different partition, so there is no cross-partition ordering guarantee.

Auto Ack requires successful callback/asynchronous completion. Manual Ack requires acknowledgment within the callback's authority and successful completion; Ack followed by an exception does not commit. Expired tokens reject acknowledgment. Batches advance only their contiguous confirmed prefix. A timed-out but still-running handler retains its ordering slot.

Typed decoding shares bounded execution capacity with business handling rather than running custom codecs on the poll owner. Blocked decoding does not prevent coordination or control-mailbox maintenance. After stop, timeout, or lost authority, the batch tail is not decoded and late results do not enter business code. Java `handler-timeout-ms` begins at execution admission and includes decoding. A synchronous codec cannot necessarily be interrupted; shutdown can time out until it actually exits.

The native consumer belongs exclusively to its owner. Observed records enter a count/byte-bounded responsibility table; only a safe prefix is committed. Commit failure retains responsibility and retries; auto commit cannot be enabled. Typed consumption commits on count/time thresholds, and commits a safe tail early when all local input is safely complete and actual calls have exited. It never crosses failure, missing Ack, or active work. Unknown commit retains protection and retry behavior. Completion can wake scheduling immediately while polling and coordination remain bounded.

`HANDLER_DEFERRED` means an external prerequisite is unavailable and does not consume the ordinary failure budget, although delivery attempts increase. `HANDLER_DEAD_LETTER` requests DLT. `ConsumeContext` and each record's `context()` expose source, actual group, assignment epoch, and both attempt counters.

| Situation | Default behavior |
| --- | --- |
| No matching event | SKIP, explicitly permit safe progress |
| Invalid headers, decoding failure, forbidden null | DEAD_LETTER |
| Ordinary business failure | Backoff/retry, then DLT after `max-consume-attempts`; zero retries indefinitely |
| DLT success | Permit source commit only after broker confirmation |
| Unavailable DLT | Retain responsibility and block later partition commits |
| Disabled or recursive DLT | Stop that partition's progress |
| Oversized raw input | Retain broker coordinates and protect the partition |
| Consecutive decoding failures reach threshold | Protect the partition rather than continuously dead-lettering the whole topic |

Unmatched/invalid policies support SKIP, DEAD_LETTER, and HALT. Explicit `dead-letter-required=false` favors availability: after failed DLT delivery it may discard the record and advance, reporting `DltUnavailableDiscarded`.

Required DLT has separate exponential backoff: `dlt-retry-initial-ms` defaults to 1000, doubles to the `dlt-retry-max-ms` base cap of 60000, then applies stable offset-derived jitter of about ±10%. `dlt-degraded-after-attempts` defaults to 10; DEGRADED retains input and envelope reservations while healthy partitions can proceed. Confirmation clears that record's degradation. Stop prevents new retry rounds; failure is not safe source completion.

An empty `dead-letter-topic-suffix` forbids DEAD_LETTER for invalid, unmatched, or tombstone policies. For example, use `invalid-record-policy=HALT` to retain invalid input with DLT disabled. Ordinary retry exhaustion may still protect the partition. Consumption is at-least-once: crashes, rebalance, failed commits, and unknown sends may repeat effects. Ack and epoch checks do not fence external databases or HTTP endpoints.

## Publishing and resource limits

```java
Delivery delivery = kafka.message("orders", order)
        .event("created").key(order.id())
        .sendAndWait(Duration.ofSeconds(5));
kafka.producerLane("control").message("commands", command)
        .event("refresh").fire();
kafka.publishRaw("archive", originalBytes).keyBytes(rawKey).send();
kafka.tombstone("state", entityId).send();
```

`send()` returns a delivery Future; cancelling it does not cancel internal receipt responsibility. `fire()` reports local admission only, with final outcomes in lane statistics. Admission failures are immediate; timeout/interruption after a possible send means unknown outcome. Typed, raw, tombstone, and compatibility entry points freeze one absolute deadline before encoding, shared by encoding, queuing, and pre-native waiting. A synchronous codec is not necessarily preemptible; returning after expiry rejects with `PUBLISH` before native admission. User callbacks run on separate bounded receipt threads.

Defaults are `acks=all`, idempotence enabled, and `max.in.flight.requests.per.connection=1`. Reliability still depends on broker ISR, minimum replicas, and retention. Producer idempotence is not business exactly-once. `acks=0` yields an unconfirmed Delivery and is prohibited for DLT.

Each lane defaults to 10000 admitted records, 65536 KiB raw bytes, and 10000 delivery observers; the first exhausted limit rejects admission. Bytes include key, payload, each UTF-8 header name, and every non-null value, counting duplicates separately. Ordinary and internal DLT/Transfer sends share these gates. Quotas remain until actual delivery responsibility ends, not caller timeout/cancellation. Native buffers have additional limits; compression, connections, and object overhead are outside this byte quota.

`max-record-bytes` also limits publication including framework-added headers, rejecting oversized outputs with `PUBLISH` before native admission. Transactions use independent `TransactionLimits`. Ordinary groups default to 65536 records, 64 MiB copy reservations, 2048 records per partition, and 64 actual decoding/handler calls shared across slots. Ordinary DLT uses a separate source pool, default 16 actual query/delivery calls, without using those 64 business slots.

String keys preserve exact UTF-8 without trimming or normalization; unpaired UTF-16 surrogates return `CONFIG`, never replacement bytes that would change identity. `keyBytes` preserves arbitrary bytes, with null distinct from empty. The setters cannot be combined. Events likewise preserve exact nonblank UTF-8, including surrounding spaces on otherwise nonblank values. Empty, all-whitespace, and malformed UTF-16 events reject with `CONFIG`; a rejected setter retains the previous event. Compatibility null event means omitted header/DEFAULT; explicit builder `event(null)` is invalid. Event whitespace uses the same shared set listed for groups, but valid event edges are not trimmed.

Explicit `tombstone(topic, key)` requires a non-null key and accepts an empty key; value is Kafka null. Java missing key produces `CONFIG`, while Rust's string parameter cannot represent it. `publishRaw(topic, null)` corresponds to Rust `publish_raw_nullable(topic, None)` and permits null, empty, or binary keys independently of explicit tombstone constraints. Empty payload stays empty.

Header names must be nonempty strict UTF-8 without NUL; names are checked before native conversion for ordinary, transactional, and internal transfer outputs. Values may be arbitrary bytes or null; order, duplicates, and case remain. External producers must obey this contract: brokers can store NUL-bearing names, but the Rust librdkafka iterator exposes only their NUL prefix, so these inputs are outside exact preservation and strict-routing guarantees. Validate before writing; `strict-headers` does not authenticate producers.

`flush()` captures all lanes before waiting; `lane.flush()` captures one lane. Both wait for actual delivery and receipt handling, including old native generations, without waiting for later admissions. Failures propagate; timeout/cancellation does not retract sends. Lanes have no mutual ordering, and slow callbacks occupy their lane's observer quota. A barrier belongs to its physical lane, has no public constructor or mutable receipt/array accessors, and cannot be used across lanes. Cancelling or completing a business Future cannot alter private delivery evidence or reservations.

Each lane manages native-producer generations independently. A later send can replace a generation after no successful delivery for `stalled-rebuild-after-ms`, enough consecutive terminal failures for `stalled-rebuild-min-failures`, and the `stalled-rebuild-cooldown-ms` interval; defaults are 60000 ms, 2, and 60000 ms, overridable per lane. Local admission rejection is excluded; success resets the current failure window. Recoverable native sequence errors may trigger replacement. Authentication, authorization, fencing, and incompatible protocol cause permanent protection requiring a newly configured source.

Old generations retain their sends without automatic resend of unknown outcomes; late results cannot alter a new generation's recovery window. Java keeps at most a current and an exiting generation; another replacement waits for old resource exit. Native `client.id` adds `-generation-N` to avoid old JMX teardown affecting new resources. Idempotent sequences do not span generations. Use stable event IDs for retries and reconciliation. `lane.recoveryStats()` and `proxy.producerRecoveryStats()` expose generation, reasons, permanent protection, and actual native-resource counts. Repeated close continues waiting for outstanding resources.

`publish`, `publishFuture`, and `publishAsync` use the default lane. Omitted event in `publish(topic, value)` and `partition(topic, key, value)` means `DEFAULT`. Callers needing topic-as-event use `partitionLegacy(topic, key, value)` or explicit `partition(topic, topic, key, value)` and verify matching routes.

## Configuration values and inheritance

The default and every declared lane validate acknowledgment, idempotence, retry ordering, capacity, and timeouts before client creation. Named lanes independently inherit source defaults, not each other. Nested lanes and unknown fields are rejected. `producer.retry-backoff-ms` and its lane override allow zero, disabling native retry delay. `consumer.fetch-min-bytes` and `consumer.fetch-max-wait-ms` also allow zero to avoid waiting for aggregation.

Shared configuration millisecond values are at most 2147483647. API `Duration` limits are defined separately. Counts, observer capacity, and effective waits must be positive. `max-consume-attempts=0` retries indefinitely; `handler-timeout-ms=0` removes the invocation confirmation deadline; `decode-failure-halt-streak=0` disables consecutive-decoding protection. Rust also accepts `null` for the latter two disabled behaviors.

Unset Admin `default-api-timeout-ms` inherits `publish-timeout-ms`; unset `request-timeout-ms` inherits that API budget, both 120000 ms by default. Request budget cannot exceed API budget. Explicit configuration or timeout-bearing APIs can shorten waits; raw properties do not override typed defaults.

The Java column below lists flat keys under a source; Rust paths are within `KafkaConfig`. Equal numbers do not imply equal native-buffer behavior. See the respective input, publishing, and DLT sections for accounting scope.

| Java key | Rust path | Default |
|---|---|---|
| `admin-default-api-timeout-ms` | `admin.default_api_timeout_ms` | `120000` ms |
| `admin-request-timeout-ms` | `admin.request_timeout_ms` | `120000` ms |
| `control-queue-capacity` | `behavior.command_queue_capacity` | `1024` |
| `commit-batch-records` | `behavior.commit_batch_records` | `1000` |
| `commit-interval-ms` | `behavior.commit_interval_ms` | `1000` ms |
| `consume-restart-budget-times` | `behavior.consume_restart_budget_times` | `10` |
| `consume-restart-budget-window-ms` | `behavior.consume_restart_budget_window_ms` | `300000` ms |
| `consume-restart-delay-ms` | `behavior.consume_restart_delay_ms` | `3000` ms |
| `consume-restart-max-delay-ms` | `behavior.consume_restart_max_delay_ms` | `60000` ms |
| `consumer-retry-ms` | `behavior.consume_retry_backoff_ms` | `500` ms |
| `control-timeout-ms` | `behavior.control_timeout_ms` | `30000` ms |
| `dead-letter-required` | `behavior.dead_letter_required` | `true` |
| `dead-letter-topic-suffix` | `behavior.dead_letter_topic_suffix` | `.DLT` |
| `decode-failure-halt-streak` | `behavior.decode_failure_halt_streak` | `1000` |
| `dlt-producer-lane` | `behavior.dlt_producer_lane` | `default` |
| `dlt-queue-capacity` | `behavior.dlt_queue_capacity` | `1024` |
| `dlt-queue-max-bytes` | `behavior.dlt_queue_max_bytes` | `67108864` bytes |
| `dlt-max-in-flight` | `behavior.dlt_max_in_flight` | `16` |
| `dlt-retry-initial-ms` | `behavior.dlt_retry_initial_ms` | `1000` ms |
| `dlt-retry-max-ms` | `behavior.dlt_retry_max_ms` | `60000` ms, exponential base cap |
| `dlt-degraded-after-attempts` | `behavior.dlt_degraded_after_attempts` | `10` |
| `handler-timeout-ms` | `behavior.handler_timeout_ms` | Java `0` / Rust `null` ms |
| `invalid-record-policy` | `behavior.invalid_record_policy` | `dead_letter` |
| `max-poll-records` | `behavior.max_batch_records` | `500` |
| `max-assignment-partitions` | `consumer.max_assignment_partitions` | `10000`, distinct partitions per group |
| `max-retry-entries` | `behavior.max_retry_entries` | `100000`, failure records per group |
| `max-batch-bytes` | `behavior.max_batch_bytes` | `16777216` bytes |
| `max-consume-attempts` | `behavior.max_consume_attempts` | `3` |
| `max-progress-bytes-per-group` | `behavior.max_group_pending_bytes` | `67108864` bytes |
| `max-progress-records-per-process` | `behavior.max_process_pending_records` | `1048576` |
| `max-progress-bytes-per-process` | `behavior.max_process_pending_bytes` | `1073741824` bytes |
| `max-progress-records-per-source` | `behavior.max_source_pending_records` | `262144` |
| `max-progress-bytes-per-source` | `behavior.max_source_pending_bytes` | `268435456` bytes |
| `max-progress-records-per-group` | `behavior.max_group_progress_records` | `65536` |
| `max-handler-in-flight` | `behavior.max_in_flight_handlers` | `64` |
| `max-progress-records-per-partition` | `behavior.max_progress_records` | `2048` |
| `max-progress-bytes-per-partition` | `behavior.max_partition_pending_bytes` | `67108864` bytes |
| `max-progress-records-per-key` | `behavior.max_key_pending_records` | `1024` |
| `max-progress-bytes-per-key` | `behavior.max_key_pending_bytes` | `16777216` bytes |
| `max-record-bytes` | `behavior.max_record_bytes` | `16777216` bytes |
| `poll-timeout-ms` | `behavior.poll_timeout_ms` | `500` ms |
| `publish-timeout-ms` | `behavior.publish_timeout_ms` | `120000` ms |
| `shutdown-timeout-ms` | `behavior.shutdown_timeout_ms` | `30000` ms |
| `start-consume-delay-ms` | `behavior.start_consume_delay_ms` | `1000` ms |
| `strict-headers` | `behavior.strict_headers` | `true` |
| `tombstone-policy` | `behavior.tombstone_policy` | `skip` |
| `unmatched-policy` | `behavior.unmatched_policy` | `skip` |
| `auto-offset-reset` | `consumer.auto_offset_reset` | `latest` |
| `consumer-concurrency` | `consumer.concurrency` | `1` |
| `fetch-min-bytes` | `consumer.fetch_min_bytes` | `1` bytes |
| `fetch-max-wait-ms` | `consumer.fetch_wait_max_ms` | `500` ms |
| `group-concurrency` | `consumer.group_concurrency` | `{}` |
| `heartbeat-interval-ms` | `consumer.heartbeat_interval_ms` | `10000` ms |
| `max-partition-fetch-bytes` | `consumer.max_partition_fetch_bytes` | `1048576` bytes |
| `max-poll-interval-ms` | `consumer.max_poll_interval_ms` | `300000` ms |
| `session-timeout-ms` | `consumer.session_timeout_ms` | `30000` ms |
| `acks` | `producer.acks` | `all` |
| `batch-size` | `producer.batch_size` | `16384` bytes |
| `compression-type` | `producer.compression` | `none` |
| `delivery-timeout-ms` | `producer.delivery_timeout_ms` | `120000` ms |
| `enable-idempotence` | `producer.enable_idempotence` | `true` |
| `fire-observer-capacity` | `producer.fire_observer_capacity` | `10000` |
| `linger-ms` | `producer.linger_ms` | `5` ms |
| `max-inflight-requests-per-connection` | `producer.max_in_flight_requests_per_connection` | `1` |
| `queue-buffering-max-kbytes` | `producer.queue_buffering_max_kbytes` | `65536` KiB |
| `stalled-rebuild-after-ms` | `producer.stalled_rebuild_after_ms` | `60000` ms |
| `stalled-rebuild-min-failures` | `producer.stalled_rebuild_min_failures` | `2` |
| `stalled-rebuild-cooldown-ms` | `producer.stalled_rebuild_cooldown_ms` | `60000` ms |
| `queue-buffering-max-messages` | `producer.queue_buffering_max_messages` | `10000` |
| `request-timeout-ms` | `producer.request_timeout_ms` | `30000` ms |
| `retries` | `producer.retries` | `3` |
| `retry-backoff-ms` | `producer.retry_backoff_ms` | `100` ms |

## Wire format and context

JSON values are the business object itself, without Java class names. `ProtocolBytes` payloads use their declared binary mode. Only `legacy-envelope=true` enables legacy envelope detection; arbitrary-class default typing is forbidden.

Shared modes are `JSON_BYTES`, `VARINT_TLV`, and `BITPACK_TLV`, requiring matching tags, types, JSON names, and field ordering. `BITPACK_TLV` requires a fixed schema. `VARINT_TLV` collection layout cannot be replaced directly with generated `.proto` classes. Rust does not encode/decode `FAST_FIXED`; Java-only use must constrain its consumers. A `ProtocolBytes` target requires the `protocol-bytes` codec declaration even when bytes happen to be valid JSON.

`KafkaHeaders` preserves cross-name order, duplicates, null, and empty bytes. `KafkaRecord.rawKey()` preserves binary keys on all paths. `KafkaRecord.key()` is a convenience view for valid UTF-8; invalid UTF-8 yields null without dead-lettering the message.

| Header | Meaning |
| --- | --- |
| `X-Nasa-Event` | Event route; only absence selects DEFAULT |
| `X-Nasa-Payload-Codec` / `X-Nasa-Payload-Mode` | Binary codec and mode declaration |
| `X-Nasa-Passthrough` | JSON passthrough context |
| `X-Nasa-Delivery-Attempt` | Session delivery attempts overwritten by the owner |
| `X-Nasa-DLT-Origin-Topic` / `Partition` / `Offset` | Actual DLT source coordinates |
| `X-Nasa-Dlt-Origin-Group` | Source consumer group |
| `X-Nasa-DLT-Origin-Source` / `X-Nasa-DLT-Origin-Cluster` | Actual source alias / broker-observed physical cluster |
| `X-Nasa-DLT-Reason` / `X-Nasa-DLT-Delivery-Attempts` | Bounded reason and attempts |

Event, codec, and mode headers are case-sensitive and reject duplicates, null, and malformed UTF-8 by default. `behavior.strict-headers=false` only permits selecting the last duplicate, which must still be valid. An empty or all-whitespace event is invalid, not DEFAULT. Only `protocol-bytes` is accepted as a codec declaration, checked even without a matching event route. JSON routes reject codec/mode; binary routes require codec and a matching declared mode, or the type's mode when absent. Raw routes preserve payload without mode decoding. Passthrough context and trace have independent tolerance rules.

DLT removes forged evidence fields, preserves business-header order, and adds actual origin evidence; reasons are bounded at a UTF-8 boundary to 1024 bytes. Ordinary DLT checks source physical identity and effective target configuration before each send; transactional `deadLetter` checks every DLT target before any batch output is sent. Targets must already exist, use only `cleanup.policy=delete`, and have positive or `-1` `retention.ms` and `retention.bytes`. Compaction, immediate deletion, missing settings, and unreadable configuration are rejected. Describe/DescribeConfigs permissions are required. Failed queries do not authorize automatic topic creation; reliable mode retains input, while explicitly availability-first policy may discard. Queries and delivery do not block the poll owner; shutdown waits for actual exit.

This is a pre-send configuration snapshot, not a lock against later administrator changes or an unlimited-history guarantee. Retained original timestamps can put old inputs near expiry under broker timestamp policy; byte retention also removes old segments. Configure the operational replay window and control changes. Original keys, null/empty payloads, and business headers remain intact; tombstones are not wrapped and target configuration is not rewritten.

For matched typed routes, `behavior.tombstone-policy` defaults to `skip`; alternatives are `deliver`, `dead-letter`, and `halt`. `deliver` also requires listener `nullable()=true`, otherwise invalid-payload policy applies. JSON `null` is controlled by `nullable()`; empty bytes still undergo codec decoding. `record.context().tombstone()` preserves the original null identity; batch-level context describes only the first record, so inspect individual contexts. Raw listeners receive nullable original bytes independently of typed tombstone policy; missing routes and invalid routing headers are still handled first. Tombstones have no encoded payload, so target codec/mode matching is omitted while uniqueness, UTF-8, and known-codec checks remain.

## Synchronous read-only passthrough

`KafkaPassthroughListener.onRecord(RawKafkaRecord)` exposes read-only key, payload, and ordered duplicate headers without DTO construction or copying raw bytes. Descriptors and view objects still allocate; this is not end-to-end zero-copy.

Only the current synchronous callback thread may read the views. Cross-thread or post-callback access returns `LIFECYCLE`; retaining a subview does not extend raw-data lifetime. `Bytes.get/length` support bounded parsing, `copyTo` writes into a business-budgeted buffer, and `copy` creates an independently owned copy whose retention and asynchronous use are the application's responsibility. Null and empty key/value/header bytes remain distinct.

Callbacks perform bounded parsing and nonblocking local admission, not network/lock/Registry waits. Java runs them on bounded workers; timeout or stop revokes acknowledgment but cannot forcibly terminate user code, so capacity remains occupied. Rust `PassthroughConsumer` runs synchronously on the owner's stack under compile-time borrowing; the two memory/execution models are intentionally different.

`PassthroughDisposition.Handled` means all targets entered local bounded queues; `BestEffortDropped` explicitly accepts partial dropping. Both still require `record.ack()` in Manual mode. `Skipped` means no local target, wrong target node, loop, or expired origin and requires no further Manual Ack. `PassthroughFailure` maps to ordinary retry, invalid-input policy, reliable DLT, hard partition pause, or CRASHED. Null is not success. Partial fan-out admission followed by failure may redeliver, so destinations need stable deduplication. Only single-record synchronous partition-ordered execution is supported; general asynchronous processing uses `KafkaRawListener` or typed record listeners.

## Broadcast identity and offset continuity

Listeners declare `broadcastScope()` and optionally `instanceId()`; annotations use `broadcastScope` and `instanceId`. Scope and explicit group are mutually exclusive; instance identity requires scope. Priority is listener, source `behavior.broadcast-instance-id`, system property `nasa.kafka.broadcast-instance-id`, environment `NASA_KAFKA_BROADCAST_INSTANCE_ID`, then a temporary JVM identity. A stable ID resumes the same group's committed offsets; a temporary ID changes at restart and uses `auto-offset-reset`, without offline-continuity guarantees.

Java `KafkaProxy.broadcastGroup(scope, instanceId)` and Rust `broadcast_group` preserve exact UTF-8 without normalization. Each part is 1–4096 bytes, contains no Unicode controls or edge ASCII spaces. A short group is `nasa-kafka-broadcast-b<lowercase scope hex>.<lowercase instance hex>`. Above 255 ASCII bytes it becomes `nasa-kafka-broadcast-h<lowercase sha256>`. Digest input is UTF-8 `nasa.kafka.broadcast`, one zero byte, then each part's four-byte big-endian byte length and bytes. Punctuation is never collapsed; long identities rely on SHA-256 collision resistance.

Different instance IDs each receive a copy; two active processes sharing a stable ID compete within one group. Before startup broadcasts, wait on the derived group using `awaitGroupReady`. Ready is a current observation, and offline cache consistency still needs reconstruction or snapshots. Single-argument `broadcastGroup(scope)` and legacy broadcast listeners without scope retain their previous algorithm. To migrate, explicitly retain the old group or stop old consumers and plan offsets/new-group replay; the runtime does not silently change groups or copy positions.

## Control, administration, and shutdown

`assign` supports single/batch handlers on fixed partitions, with earliest, latest, timestamp, explicit offset, or committed starts. Out-of-retention explicit positions reject; a timestamp without a match uses the end. Assign does not provide a cross-process exclusive partition lease.

`replaceSubscriptions` replaces the set; `addSubscriptions`/`removeSubscriptions` add/remove whole sets; `clearSubscriptions` retains the owner with an empty set; `stop(group)` stops and unregisters it. `subscribeGroup`/`unsubscribeGroup` retain additive/removal semantics, with one validated command per owner. Legacy null/empty collection overloads remain no-ops. Explicit add/remove reject empty, duplicate, null, or blank topics; replacement permits empty. Changes alter fetching, not frozen handler routes; new topics follow unmatched policy.

Subscription changes reject while input responsibilities remain. Native acceptance revokes old Ready and requires a new assignment; an identical set retains Ready. Single-owner changes apply together, but multiple owners/groups have no atomic transaction. Partial failure may be unknown; read actual assignments and reconcile using a complete set after drain. Fixed assign cannot change subscriptions, and an old assignment handle can stop only its own instance.

Seek, subscription, pause, and resume commands use bounded owner mailboxes. A partition with outstanding consumption responsibility cannot seek. Queue cancellation before execution differs from timeout after starting. Removing user pause does not remove framework failure/capacity protection.

`controlPartitions(group, actions)` reports each explicit partition independently:

| Outcome | Meaning |
| --- | --- |
| `APPLIED` | Owner confirmed execution; later rebalance can still change state |
| `NOT_APPLIED` | Not dispatched or rejected/cancelled before effects; retry after resolving the cause |
| `UNKNOWN` | Effects may have occurred; query actual position/pause state before retrying |
| `OWNER_CHANGED` | No unique ownership or frozen epoch changed; the old intent was not executed |

```java
ControlReport report = orders.controlPartitions("order-worker", Map.of(
    new TopicPartition("orders", 0), new PartitionControl(PartitionControl.Kind.PAUSE),
    new TopicPartition("orders", 1), new PartitionControl(PartitionControl.Kind.SEEK, 100)));
```

Targets must be explicit and nonempty. Validate all declarations/ownership before dispatch; missing or duplicate owners reject the entire request before execution. Admission and execution share `control-timeout-ms`. Later timeout cannot overwrite confirmed targets. There is no cross-partition atomicity, ordering, rollback, or automatic rerouting. Unknown outcome prevents remaining actions from starting; the returned report is sealed against late changes. Committed positioning without commit history rejects. Successful seek clears only the target partition's responsibility.

`proxy.admin()` and authorized `handle.administration()` expose cluster identity, topic descriptions, incremental configuration, and structured creation results; `handle.admin()` is query-only. `createTopicIfAbsent(topic, partitions, replicas, settings, timeout)` returns `CREATED` or `ALREADY_EXISTS`; existing topics are not modified or presumed compatible. Mutations complete only on explicit resource confirmation. Permissions produce `AUTHORIZATION`; post-submission timeout, interruption, or missing trustworthy response produces `OUTCOME_UNKNOWN`, requiring readback. Legacy synchronous proxy methods wrap a stable cause chain in `IllegalStateException`; legacy delete is idempotent for missing topics, whereas `KafkaAdmin.deleteTopic` reports `ADMIN`.

`alterTopicConfig` sets multiple fields; null restores broker defaults and unspecified fields remain unchanged. `describeTopic(topic, false, timeout)` needs no DescribeConfigs. `topicExists` returns false only for explicit absence, not hidden listings, permissions, or missing response fields. `committedOffsets` distinguishes null history from valid zero. Composite queries share a total budget; lag is not atomic. Public Admin `Duration` values must be positive and at most `Integer.MAX_VALUE` milliseconds.

Final close prevents new Admin and temporary metadata queries and waits for native connections and close threads. Timeout/interruption retains responsibility for another `shutdownUntil`. An old Admin facade cannot reopen a closed source. `partitionsFor` preserves replica/offline-replica metadata, and its temporary consumer belongs to source shutdown.

`observeOffsets(group, partitions, isolation, timestampMillis, timeout)` accepts at most 10000 explicit unique partitions. `READ_COMMITTED` end is LSO, while `READ_UNCOMMITTED` end is the ordinary log end; open transactions can separate them. `OffsetObservation` carries source, optional group, isolation, overall and per-field times, and independent beginning/end/commit/timestamp/lag values or absence reasons. Valid zero differs from missing data. Reasons include `NOT_REQUESTED`, `NO_COMMITTED_OFFSET`, `NO_TIMESTAMP_MATCH`, `RESOURCE_MISSING`, `AUTHORIZATION`, `TIMEOUT`, `INTERRUPTED`, `UNAVAILABLE`, and `INCONSISTENT`.

Null group/time omits that dimension; no group also omits lag. Lag is `max(0, end - committed)`, an offset distance including holes, control records, and potentially expired prefixes, not a business-record or local-backlog count. One deadline covers all fields, preserving partial evidence; the report seals on return while source close tracks native requests. Queries never join a group, commit, create topics, or change configuration.

`verifyTopic(new KafkaAdmin.TopicContract(topic, partitions, replicationFactor, settings), timeout)` reads and verifies partitions, replicas, and the required configuration subset. Empty settings need no DescribeConfigs. Matching returns the actual description; drift is `TOPIC_CONTRACT`, with query/authorization categories preserved. It never creates or alters resources or locks them against later change.

`before()` starts locally; use `awaitGroupReady` or `KafkaClients.awaitReady` for broker and assignment evidence. `ReadyRequirement` supports `JOINED`, `assigned(count)`, `assignedTopics(topics)`, and `assignedPartitions(partitions)`. Other partitions cannot substitute for required ones. JOINED permits an idle joined member. User pause alone does not revoke readiness, while retry, backpressure, recovery, hard protection, and stop do.

`groupReadiness(group, requirement)` includes source/group health, epoch, observation time, requirement, and stable reasons: starting, joining, authority loss, recovery, retry, commit blocking, DLT/progress backpressure, degradation, crash, stop, or missing assignments. `awaitGroupsReady(requirements, timeout)` uses one total budget and rechecks all bound group instances together rather than latching earlier success. Empty requirements and negative budgets reject; zero observes once. `READY_TIMEOUT` includes unmet groups in `KafkaFailure.readiness()`; protected/terminal states return `LIFECYCLE`. A returned snapshot cannot constrain later rebalance.

Retryable native-session failures use exponential backoff/jitter: initial 3000 ms, maximum 60000 ms, at most 10 automatic restarts per 300000 ms window. Authentication, invalid configuration, other nonretryable errors, or exhausted budget enter CRASHED while retaining query/stop/explicit recovery. Replacement waits for admitted invocations to exit. Empty polls do not refresh broker-reachability evidence; native responses/connections provide it. An all-paused fixed assignment uses bounded read-only checks without moving positions. Metadata availability does not prove coordinator or delivery recovery. Stop takes precedence over backoff and restart.

`restartGroup(group)` accepts only protected abnormal tasks, rejecting healthy or still-executing ones. Admission immediately revokes old Ready; the new session must await assignment. Fixed assignments restore frozen uncommitted starts rather than reevaluating initial latest/timestamp. Out-of-retention recovery stops without choosing a new start. Repetition of unconfirmed business remains possible. Recovery settings are `consume-restart-delay-ms`, `consume-restart-max-delay-ms`, `consume-restart-budget-times`, and `consume-restart-budget-window-ms` under behavior. Health includes starts/restarts, commit attempts/success/failure, last-success times, and pause reasons, scoped to the local instance rather than a durable cross-process ledger.

Shutdown has two stages: `stopConsumersUntil(deadline)` drains consumers while publishing remains available for completion; `shutdownUntil(deadline)` then closes publishing, receipts, and Admin. Deadlines are absolute `System.nanoTime()` nanoseconds. Timeout preserves outstanding responsibility; `isClosed()` is true only after actual resource exit and repeated close continues waiting. `requestConsumerStop()` synchronously publishes permanent source-consumption stop without waiting, and both wait APIs invoke it even with an expired deadline. It is not a reversible pause.

`closeReport()` is connection-free evidence. `shutdownReportUntil(deadline)` advances the same protocol and returns `COMPLETED` or `INCOMPLETE`; timeout/interruption does not undo stop. For original exception categories, call `shutdownUntil` then read the report. A business handle may observe reports after close without active whole-source shutdown authority.

Reports separately show owners/handlers, retained input, input released without confirmed commit during stop, pending and cumulative terminal-unknown lane delivery, DLT envelopes/calls, finite tasks, Admin, temporary queries, and observation threads. At most 64 outstanding group names are listed with full counts separately. `resourcesClosed` requires actual exit of all resources; other fields are independent samples, not an atomic snapshot. Task handles retain Replay/Transfer/transaction business outcomes and checkpoints. `releasedUnverifiedRecords` is not an epoch-deduplicated event count. Local cleanup cannot prove broker records still exist, so ordinary-close `returnedForRedelivery` remains zero; retained ranges cannot prove compacted-record survival or unchanged topic generation. Terminal-unknown counts survive resource exit; a timed-out caller that later gets success is not terminal unknown.

Spring `KafkaClients` binds its close event and Bean destruction to one shared deadline across all named sources. After final cleanup returns, even with outstanding tasks, old proxies, Admin views, and lanes cannot admit new business. Registered internal delivery retains its original quota and dependencies. Timeout does not release static names or mark closed. The owner can continue with `KafkaClients.shutdownUntil(deadline)`; only actual completion removes names, without reopening admission.

Stop immediately revokes in-flight Auto/Manual confirmation, including temporary Ack not yet incorporated into the safe prefix. Owners await actual invocation exit and release uncommitted input for subsequent consumption; external effects cannot be undone. Already-safe prefixes and already-sent DLT later confirmed by the broker may finish committing. Stop starts no new business or DLT retry round. DLT quotas survive actual native delivery and lost assignment; a wait timeout never authorizes overlapping delivery. Source stop waits for old-assignment tasks too, not merely current assignment or an empty completion map.

### Configuration migration and rolling deployment

`KafkaMigration.inspectLegacy(sourceId, flatConfig, routes)` reads flat single-source settings and resolved `Route(topic, event, group)` declarations, returning named-source candidates, routes, default differences, and decisions. It does not scan the application, connect, create topics, or move group positions. Unknown/duplicate field spellings and invalid identities reject.

```java
var report = KafkaMigration.inspectLegacy("orders", Map.of(
    "bootstrap-servers", "localhost:9092",
    "group-id", "order-handler",
    "max-inflight-requests-per-connection", 1
), List.of(new KafkaMigration.Route("orders", "DEFAULT", "order-handler")));
```

Review `locallyValid`, all findings, and the route directory before deployment. The candidate preserves legacy `acks=1`, disabled idempotence, and five requests in flight instead of silently substituting current defaults of all, enabled, and one. The old retry combination violates current non-idempotent ordering constraints and produces `CONFIG_INVALID`; explicitly choose serialized retries or idempotence. Credentials, certificate paths, and all raw-property values become `<redacted>`; `SECRETS_REDACTED` requires rebinding. `locallyValid` checks the unredacted candidate, not rolling compatibility or business equivalence. Source diagnostics expose reliability and routing contracts without credentials.

- Omitted partition event is DEFAULT; preserve topic-as-event through the explicit compatibility entry point or event argument.
- Zero maximum attempts means unlimited retry. Strict routing headers reject duplicates by default; relaxed duplicate selection requires `strict-headers=false`. Unmatched Skip may advance safe progress.
- Deploy support for new events to every group member before producing them. Broadcast continuity requires retaining the actual group or planning migration; stable scope does not copy offsets.
- Callers of `getContext/getProducer/getAdminClient/getPollTasks/getListenerRegistry` or former configuration factories must recompile against host injection, lanes, Admin, health/control, and unified assembly. Business code uses `KafkaHandle`, with management writes through explicitly granted `administration()`.

Before rollback, stop new capability use, drain actual tasks, and preserve checkpoints/database state. Older code without Replay/Transfer/checkpoint cannot adopt those states just because it understands event bytes. Resume only with a compatible durable format and plan fingerprint, or an explicit business replay/deduplication strategy; deleting progress does not establish a successful rollback.

## Schema Registry

`ConfluentEnvelope` and `ConfluentSchemaRegistry` offer independent Confluent-compatible schema access corresponding to Rust `nasa::kafka`. They do not generate Avro, Protobuf, or JSON Schema codecs or automatically envelope ordinary JSON publication. Definitions may omit references or use `references: []`; nonempty or malformed references return `INVALID_RESPONSE` without fetching dependencies. Applications choose subject naming, compatibility, Protobuf message-indexes, schema GUID handling, and reference resolution; these are not assumed to be ordinary four-byte ID envelopes.

```java
Set<Integer> approved = Set.of(42);
ConfluentEnvelope envelope = ConfluentEnvelope.decode(wireBytes, 1024 * 1024, approved);
try (ConfluentSchemaRegistry registry = new ConfluentSchemaRegistry(
        ConfluentRegistryOptions.defaults("https://registry.example.com"))) {
    SchemaRegistryClient.RegisteredSchema schema = registry.schemaById(envelope.schemaId()).join();
    // 使用业务选定的 codec 与已批准 schema 解码 envelope.payload()。
}
```

The allowlist applies before Registry access: an input ID cannot authorize arbitrary lookups. The envelope uses magic byte 0 and a positive four-byte big-endian ID; Java retains an independent payload copy. IDs are Registry-local; transfer requires explicit identity and mapping checks.

Defaults are a 3-second budget, 1 MiB response limit, 256 cache entries, 300-second success TTL, and 5-second negative TTL for 404, with a 16 MiB hard response cap. Positive/negative entries share LRU capacity; other failures are not negatively cached. Same-ID cold lookups coalesce, and waiters compete again after a failed round. Distinct IDs and control calls share HTTP concurrency; excess returns `CAPACITY`. There is no subject scanning or background refresh.

`isCompatible` is an explicit read-only control request. `register` requires `autoRegister=true`. Registration timeout, cancellation, or connection failure does not prove no remote revision was created. Close rejects new calls but cannot retract remote writes. Cancelling one waiter does not cancel a shared lookup or free an executing registration's slot; client-owned requests last until HTTP completion, deadline, or close. Observe them with `metricsSnapshot().inFlight()`. Other waiters retain shared results; cancelled waits count `CANCELLED` once, without later success recounting.

`maxPendingLookups` additionally bounds all lookup callers, including same-ID waiters: default 4096, range 1–65536, excess `CAPACITY`; `pendingLookups()` reads usage. The eight-argument options constructor keeps the default; the nine-argument form customizes it. Cancellation removes the waiter immediately, and completion dispatch remains bounded rather than retaining unlimited cancellation history.

Endpoints are trusted configuration, reject redirects and embedded credentials, and require HTTPS except localhost. Basic/Bearer credentials are independent of Kafka SASL. Diagnostics do not expose endpoints, subjects, schema bodies, or authentication content. Metrics separate data lookup from control calls. Positive hit ratio is `CACHE_HIT / (CACHE_HIT + FETCHED)`, excluding negative hits, failures, and cancellation.

`new ConfluentSchemaRegistry(options, new SchemaRegistryTls(caPem, identityPem))` supports private CA and mTLS. Either material may be null. Declared CA PEM replaces JVM trust roots; otherwise defaults apply. Identity PEM contains the client chain and one unencrypted PKCS#8 key. Each material is at most 1 MiB, with at most 16 certificates, copied at construction. Malformation, duplicate keys, or key/certificate mismatch return `INVALID_CONFIGURATION` before connection. HTTPS, certificate-chain and hostname verification remain required; TLS 1.2/1.3 are supported. TLS failure is sanitized `TRANSPORT`; 401/403 are `REMOTE_STATUS`, not cached absence. Kafka TLS/SASL settings are not inherited.

## Broker security and build capabilities

Each source freezes one protocol, credential, and trust domain shared by its producer, consumers, Admin, and lanes. Configure `security.protocol`, `security.sasl-mechanism`, `security.sasl-jaas-config`, `security.ssl-truststore-location/type/password`, `security.ssl-keystore-location/type/password`, and `security.ssl-key-password`. Flat equivalents remain available for single-source settings. Native authentication properties belong only in source-level `properties`, not role/lane overrides. Registry and checkpoint-database authentication are separate.

Protocols are exactly `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL`; mechanisms are `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, and `GSSAPI`. PLAIN needs a trusted transport; SASL_PLAINTEXT does not encrypt it. SSL verifies chain and hostname; mTLS additionally needs client identity. GSSAPI depends on JVM GSS, Kerberos principal/ticket or keytab, service name, DNS, and clock. Algorithm availability is not successful login. OAuth and arbitrary Java callbacks have no shared cross-language guarantee.

`KafkaCapabilities.supportedCapabilities()` describes local support. `proxy.capabilities()` separates `supported`, `configured`, `laneCompression`, and ordinary-group current `readiness`. The first two have no remote effects; empty readiness is no group evidence. Only a valid join establishes Joined, not TLS configuration or successful construction. Transaction handles provide their own status. Snapshots expose no credentials, paths, or native handles.

Compression is exactly `none/gzip/snappy/lz4/zstd`, configured through `producer.compression` or lane `compression`, with flat `compression-type`. Raw `compression.type/codec` cannot bypass declarations. All declared lanes validate before startup, even before first publication. Missing codec dependencies/native libraries yield `UNSUPPORTED_CAPABILITY` without substitution. Rust also requires appropriate `tls`, `gssapi`, and `zstd` features and actual librdkafka capabilities.

Explicit TLS, SASL, or topic/group authorization failure moves ordinary groups to CRASHED without automatic session restart. Restore permissions before explicit group recovery; changed credentials/trust require stopping the old source, awaiting exit, constructing a new one, and verifying Ready. Configuration mutation does not hot-replace connections. Native construction errors produce sanitized CONFIG without a native cause chain. A generic disconnect alone is not proof of authentication failure; transient recovery remains budgeted, including Rust mTLS failures reported as disconnects.

| Role | Kafka resources and operations |
| --- | --- |
| Ordinary producer | Target topic Write and Describe; delivery does not require topic administration |
| Ordinary consumer | Source topic Read/Describe and actual group Read; committed-position queries also involve Describe |
| Ordinary DLT writer | Target Write/Describe/DescribeConfigs plus source consumption permissions; unavailable target evidence prevents send and source progress |
| Finite Replay | Source Read/Describe; direct partition reads without joining a group or committing its positions |
| Transfer / DLT replay | Source Read/Describe; target/quarantine Write/Describe; resource-generation directory authorized separately |
| Read-only diagnostics | Topic/group Describe and DescribeConfigs when requested, without write or administrative authority |
| Topic administration | Create, Delete, Alter for partition expansion, AlterConfigs for configuration; broker checks each operation |
| Same-cluster transaction | Input Read, output/DLT Write, corresponding Describe, group Read, derived transactional.id Write; transaction DLT also DescribeConfigs |

These operation contracts target Kafka 4.1.0; cluster-wide diagnostics or other deployments need their own authorization review. Ready does not auto-create topics, alter ACLs/configuration, or disable hostname verification. `handle.admin()` is read-only; owner `proxy.admin()` and granted `handle.administration()` permit management writes and should stay out of publish-only modules. Authentication alone cannot prove permission for every request.

Publication and Admin preserve explicit ACL denial as `KafkaFailure.Code.AUTHORIZATION`; Replay, Transfer, DLT replay, and transactions use their own exception's `Code.AUTHORIZATION`. Proxy synchronous administration wraps the stable cause chain in `IllegalStateException`. Transfer source failure appears in `status.replay().failure()`, target failure in outer `failure()`. Rejected new work advances no checkpoint/commit, while confirmed prefixes may remain. Await old task exit before restarting from its plan/progress; a permanently protected producer needs source replacement. ACL revocation cannot retract already-fetched input or in-flight effects.

The shared runtime scope is Kafka 4.1.0 KRaft, Java client 3.8.1/JDK 21, librdkafka 2.12.1/rdkafka 0.39, classic group protocol, macOS arm64 applications, and Linux arm64 brokers. Other brokers/platforms/new group protocols do not automatically inherit equivalent guarantees. Local GSSAPI support alone is not Kerberos authentication.

## Finite replay plans

`proxy.admin().replayObservation(topics, timestampMillis, timeout)` reads complete partitions, actual cluster ID, retention starts, and read-committed LSO under one positive metadata/offset budget of at most five minutes. It never joins groups, creates topics, or commits live positions. Null timestamp omits that query; no match remains explicit absence.

`ReplayPlan.earliest`, `offsets`, and `timestamp` freeze `[start,end)` from retention starts, a complete explicit start map, or time observation. Missing timestamps use `MissingTimestamp.FREEZE_END` or `REJECT`; positions beyond the observed LSO clamp to frozen end. Later writes or transaction completion never expand the plan. Maximum partitions are 4096; task/source/physical identities use bounded ASCII. `ReplayLimits` independently binds record rate, byte rate, and actual in-flight capacity; zero rate means unthrottled.

```java
ReplayObservation observation = proxy.admin().replayObservation(Set.of("orders"), null, Duration.ofSeconds(10));
ReplayPlan plan = ReplayPlan.earliest("orders-rebuild", proxy.sourceId(), observation,
        currentTopicGenerations, pipelineFingerprint, ReplayLimits.defaults());
byte[] frozenPlan = plan.encode();
ReplayPlan restored = ReplayPlan.decode(frozenPlan);
ReplayCheckpoint initial = restored.initialCheckpoint();
```

`topicGenerations` must come from a resource directory that detects deletion/recreation, not topic names or partition counts. Rust's safe client does not expose topic UUID, so shared recovery relies on this explicit external evidence; callers keep it consistent with Kafka. `pipelineFingerprint` is SHA-256 over schema, transformation, filtering, destination routing, and success conditions, and changes with processing meaning. Shared `NSKRPL01` encoding includes integrity checks and is at most 8 MiB. Digests are not authentication; control persistent-directory writes.

Before resume, `validateResume(currentObservation, currentTopicGenerations, checkpoint)` rejects changed physical identity/generation, added/missing partitions, retention beyond pending positions, regressed end, or changed processing contract. It never resets earliest or recomputes latest. `TOPIC_MISSING`, `OBSERVATION_FAILED`, and source `CLOSED` remain distinct. Planning/validation do not invoke handlers. Compacted logs contain only surviving records; offset gaps do not establish complete business history.

## Managed finite replay

`proxy.replay(plan, store, identityProvider, processor)` returns `ReplayHandle` and uses a dedicated consumer to read raw data serially per partition, with parallelism across partitions. It does not join or commit live groups. `ReplayRecord` preserves binary keys, tombstones, timestamps, ordered duplicate headers, and physical coordinates; later messages do not enlarge the range.

```java
ReplayHandle handle = proxy.replay(restored, store, identityProvider, processor);
ReplayStatus snapshot = handle.status();
handle.pause();
handle.resume();
handle.cancel();
boolean drained = handle.awaitDrained(Duration.ofSeconds(10));
```

`ReplayProcessor.fingerprint()` must match the plan. `process(record)` is a synchronous success contract: return only after all actual effects succeed, never immediately after starting background work. The runtime merges partition progress, conditionally persists the checkpoint, then exposes success. Processing failure stops the entire task. Persistence failure protects further writes and separately reports execution/storage failure categories. Restart the same plan from last confirmed progress; a successful effect before unconfirmed checkpoint can repeat and needs stable event-ID or source-coordinate idempotency.

Source identity and external generations are queried before execution and again after each processing admission; metadata and directory queries share a ten-second budget per check. `ReplayIdentityProvider` must supply complete current evidence and honor its budget. A blocked extension remains an actual responsibility despite a cancelled wait. These checks detect observable drift but cannot atomically lock external effects.

Retention/truncation beyond the checkpoint returns `DATA_GAP` with last confirmed progress, never skips to remaining history. Already-admitted work may complete and persist; subsequent admissions recheck. Native positions may cross compacted holes, including a range with no surviving final record, provided identity, retention, and delivered-record success remain valid. Transfer uses the same source checks and cannot recover compacted events or expired tombstones.

Each source allows at most 16 active tasks and 64 partition workers, plus one coordinator per task. Actual parallelism is the lesser of concurrency and partition count; each worker retains at most one record. Its limit is the lesser of source `max-record-bytes` and task bytes divided by worker count. Accounting includes key, payload, UTF-8 topic/headers, and fixed metadata; shortage returns `CAPACITY`. Record/byte rates are shared at task admission with at most one initial burst record. Fetch buffers, decompression, and business copies are excluded, so this is not a process-memory limit.

Pause stops new processing: PAUSING retains admitted responsibility, PAUSED means no processing in flight but may retain a record. Cancel enters STOPPING and does not retract effects. COMPLETED/CANCELLED/FAILED requires actual worker, consumer, and lease exit. Wait timeout does not release a task name or exclusive ownership. Source shutdown drains replay before producer/Admin; repeated shutdown continues outstanding work.

## Recoverable single-destination transfer

`TransferPlan` binds finite source ranges to a destination source, physical cluster ID, topic generation, complete partitions, producer lane, and explicit per-source-partition routing. It supports history movement, archiving, and cross-cluster backfill. Destination acknowledgment precedes durable source progress. `freeze` preserves ranges but replaces the ordinary handler fingerprint with the built-in delivery contract; a handler checkpoint is not automatically transfer progress. Shared encoding is at most 16 MiB.

```java
TransferPlan.Target target = TransferPlan.Target.observed(destination.sourceId(), "archive",
        targetObservation, targetTopicGenerations);
TransferPlan transfer = TransferPlan.freeze(sourceRanges, target, approvedPartitionRoutes,
        TransferPlan.PayloadPolicy.opaque(), 4);
TransferHandle task = source.transfer(transfer, destination, checkpointStore,
        sourceIdentityProvider, targetIdentityProvider, null);
TransferStatus status = task.status();
```

The target lane requires `acks=all` or `-1`. Register the destination dependency before opening the source task. Stop on either side prevents further transfer admission, while native-admitted records retain actual-delivery responsibility. Timeout/pause/cancel cannot prematurely release either source or the lease. A destination permits 16 dependencies; the source shares task/worker capacity with ordinary Replay. Lifecycle methods follow Replay. `TransferStatus.replay` is durable progress; `deliveredRecords` counts this execution's target successes and can lead checkpoint progress. Outer failure describes target identity, schema, capacity, path, or delivery errors.

Opaque transfer preserves binary keys, null/empty payloads, ordered duplicate headers, and fixed partition mapping. Each source partition is serial; merging sources does not establish global ordering. Original timestamps are retained, subject to destination `LogAppendTime`. `x-nasa-transfer` holds original physical coordinates/generation/time and a stable `eventId`, appending the physical topic path; read it through `TransferTrace.read(headers)`. Hops are 1..16, with at most 256 headers/64 KiB. Case variants, duplicates, malformed transfer evidence, and revisiting a physical topic reject. Event ID does not depend on task name or destination offset. Integrity/deduplication digests do not authenticate writers; use controlled topic writes, resource directories, and trusted origins.

Confluent envelopes require explicit `CONFLUENT_ID_MAP` and `TransferRegistries`, approving destination ID, schema type, and UTF-8 definition SHA-256 for each source ID. Both Registries are queried; only matching ID/type/digest permits replacing four ID bytes. The body, including Protobuf message-indexes, remains unchanged. Tombstones skip Registry lookup. There is no auto-registration, evolution, or re-encoding. OPAQUE leaves IDs uninterpreted, so destination compatibility is the application's responsibility. Host-owned Registry clients must outlive actual transfer drain.

Before each send, verify target physical identity, external generation, and complete partitions; observable deletion/recreation, expansion, or wrong-cluster connection reject. End-offset growth does not alter the contract. Schema and target checks share ten seconds. Query and send are not atomic; the external directory must match reality, and applications requiring a no-change window must coordinate infrastructure changes. Confirmed target with unconfirmed checkpoint can repeat; unknown delivery cannot advance. This is single-target at-least-once, even when clusters coincide. It does not migrate group offsets, ACLs, topic configuration, or provide automatic disaster recovery. MySQL supports shared progress; DLT replay and multi-destination delivery ledgers are separate concerns.

## Finite DLT replay and raw quarantine

`source.replayDlt(plan, destination, store, sourceIdentity, destinationIdentity, registries)` returns `DltReplayHandle` with pause/resume/cancel/status/awaitDrained. It reuses frozen Replay ranges, exclusive checkpoint ownership, and actual task lifetime; MySQL allows cross-host recovery without changing live group offsets. Either side stopping prevents the next input admission and retains already-admitted delivery/lease responsibility. Lanes require all/-1 ISR acknowledgment, subject to `min.insync.replicas`, not acknowledgment from every configured replica. Target/schema checks follow Transfer.

`DltReplayPlan` fingerprints approved original-business topic/partition mappings, original groups, target physical identity and partitions, schema policy, invalid-input handling, and maximum replay count. Headers cannot choose arbitrary connections/targets. At most 4096 routes and 256 original groups are allowed; an empty group set selects all groups. Read partitions belong to the DLT, route-source partitions to its reported original business input. Only valid evidence excluded by selection can count as skipped and advance.

`DltEvidence` strictly reads six canonical fields: topic, partition, offset, group, reason, and DLT delivery attempts. Missing/duplicate fields, ASCII-case aliases, null, malformed UTF-8, and noncanonical numbers reject. Attempts are 1..4294967295 and need not match the current `X-Nasa-Delivery-Attempt`. Original group is 1–1024 UTF-8 bytes; reason may be empty and is at most 1024 bytes. Complete headers are at most 256 entries/64 KiB without silent truncation.

Optional `DltOrigin` reads source and physical-cluster fields as an inseparable pair, each 1–1024 UTF-8 bytes without NUL. Missing one, aliases, duplicates, or malformed text reject. New ordinary/transactional DLT writes the pair. Old six-field input remains readable with `origin()==null`, and the six-argument Java constructor remains available. Source alias is not physical identity; cluster/topic/partition/offset cannot distinguish deleted/recreated original topics, and fields do not authenticate writers.

Replay evidence without the extension retains `NSKDRH01`; extended evidence uses `NSKDRH02` and preserves the first origin through repeated failure/replay. Upgrade readers first because old readers cannot parse NSKDRH02. Rust struct literals must set `origin: None` or actual origin; Java's older constructor/accessors remain available.

Invalid input defaults to stopping at its failed position. Explicit `InvalidPolicy.QUARANTINE` sends missing/malformed evidence or replay-limit input to a fixed quarantine partition distinct from all business replay targets. It preserves key, payload, time, and all original invalid headers, adding `x-nasa-dlt-replay-failure`; `DltQuarantineTrace` exposes actual DLT coordinates, event ID, and reason. Existing quarantine markers or header-budget overflow stop rather than replace evidence. Schema mismatch, unavailable directories, changed targets, and failed/unknown delivery are never quarantine success. Only confirmed quarantine advances progress.

Normal replay preserves original payload/tombstone/empty values, ordered duplicate business headers, and DLT evidence, adding `x-nasa-dlt-replay`. `DltReplayTrace` preserves the first actual DLT resource/coordinate, original report, latest approved target, and count. Initial ID comes from actual DLT coordinates; valid Transfer evidence retains its original event ID, is enclosed intact, and loses its active transfer header. Further failure never resets replay identity/count, limited to 1..16. Returning to the original business topic is allowed explicitly; writing directly to the currently read DLT topic in the same cluster is forbidden. Ordinary Transfer loop checks remain unchanged.

Digests detect format/content changes, not writer authenticity; selection, origin, and counts require controlled DLT writers and trusted directories. Old DLT lacks original topic-generation evidence, which is not fabricated. Replay creates a new Kafka offset, not the original log position. Confirmed delivery before checkpoint can repeat and requires deduplication. Forwarded/skipped/quarantined status counters belong to this execution; durable progress is in the replay status.

## Durable task progress

`ReplayCheckpoint`, `CheckpointStore`, and `CheckpointLease` define stable task ID, contract SHA-256, ownership `generation`, conditional-update `revision`, and at most 4096 frozen partition ranges. Ranges are `[startOffset,endOffset)`; `nextOffset` never regresses or exceeds bounds. Completion does not require an actual `endOffset-1` record. Fingerprints bind physical source, topic generation, partitions, schema/transformation, destination, and end range, not only a topic string.

```java
CheckpointStore store = new FileCheckpointStore(Path.of("/var/lib/application/replay"));
ReplayCheckpoint initial = new ReplayCheckpoint("orders-rebuild", contractFingerprint, 0, 0,
        List.of(new ReplayCheckpoint.Position("orders", 0, 100, 200, 100)));
try (CheckpointLease lease = store.claim(initial)) {
    lease.verify();
    ReplayCheckpoint current = lease.checkpoint();
    // 只有目标写入已确认，才把连续安全位置持久化；返回前不公开新的业务进度。
    ReplayCheckpoint persisted = lease.advance(current.revision(),
            List.of(new ReplayCheckpoint.Position("orders", 0, 100, 200, confirmedNextOffset)));
}
```

`claim` acquires exclusivity before persisting a new generation; changed contract is `CONTRACT_MISMATCH`, active ownership `BUSY`. `verify()` checks current ownership and complete durable evidence without advancing revision; failure prevents further lease progress. Custom stores must implement this gate, used before managed Replay/Transfer business calls. Verification and external effects are still non-atomic; `checkpoint()` is historical confirmed state, not execution authority.

`advance` compares revision and the complete existing snapshot, persists, then exposes the new value. Stale revision is `STALE_REVISION`; lost/changed evidence is `FENCED`. Write failure is `OUTCOME_UNKNOWN`, protecting further lease writes. Actual business must exit before lease close; close cannot retract destination effects. Success before unconfirmed progress may repeat.

The file backend supports controlled POSIX local directories and single-host recovery, using shared `NSKCP001` big-endian encoding and SHA-256 integrity. It holds a stable task lock file and writes full-body sync, atomic replacement, then parent-directory sync. OS releases locks on process crash. The component exclusively manages directory/lock files: do not delete, recreate, or independently open locks. Integrity is not authentication. NFS and cross-host takeover are not guaranteed. Custom shared stores need atomic claim/CAS and acknowledgments backed by actual persistence.

### Shared MySQL checkpoints

`MySqlCheckpointStore` supports exclusive Java/Rust task recovery across hosts for finite Replay, single-target Transfer, and DLT replay. Explicitly provide MySQL Connector/J 8 or later; the component only depends on JDBC, so ordinary Kafka usage needs no driver.

```java
Properties sql = new Properties();
sql.setProperty("user", checkpointUser);
sql.setProperty("password", checkpointPassword);
sql.setProperty("sslMode", "VERIFY_IDENTITY");
MySqlCheckpointStore shared = new MySqlCheckpointStore(
        "jdbc:mysql://mysql-primary.example:3306/event_progress", sql,
        "orders", Duration.ofSeconds(5), 64);
shared.initialize(); // 运维显式建表；日常 load/claim 不执行 DDL。
```

Precreate the database. The URL permits one endpoint and an ASCII database name, without credentials/query parameters. Pass credentials/TLS through separately frozen `Properties`. `sslMode` defaults to VERIFY_IDENTITY and only additionally permits explicit DISABLED. Private CA/mTLS uses Connector/J `trustCertificateKeyStore*`/`clientCertificateKeyStore*`; `enabledTLSProtocols` and `serverRSAPublicKeyFile` are supported. Unknown properties, automatic reconnect, replication URLs, multiple statements, and pool proxies reject.

The fixed table is `nasa_kafka_checkpoint`, with binary `(namespace, task_id)` primary key and NSKCP001 body up to 2 MiB. Namespace/task names are 1–128 ASCII letters/digits/dots/hyphens/underscores, starting with a letter/digit. All owners use the same database, table, and namespace; shared SHA-256 lock names length-prefix these identities. Each active lease owns a physical connection and GET_LOCK session lock; contention is immediately BUSY. Initial generation/revision are 1/0; takeover increments both while retaining progress. Writes compare lock owner, original session, generation, revision, and complete old body; only one confirmed updated row publishes progress.

Supported storage is MySQL 8+ with one writable primary, InnoDB, autocommit, and `innodb_flush_log_at_trx_commit=1`. With binlog enabled, require `sync_binlog=1` and session `binlog_format=ROW`. Invalid structure, extra indexes, and visible triggers reject. The table is exclusive to this protocol; control DDL/triggers/direct writes and allow the runtime to inspect structure evidence. `initialize()` additionally needs CREATE; normal operation needs SELECT/INSERT/UPDATE and named-lock access. Do not change durability settings or schema during active tasks.

Explicit connection capacity is 1–1024, shared by connection creation, initialization, loads, and leases; exhaustion is CAPACITY. SQL sequence budgets are 100 ms–60 s, with a separate equal close budget; failure cleanup can add time. Capacity returns only after actual local connection exit. A disconnected server session may still hold the lock, so a new claim remains BUSY until server cleanup; bypassing it is invalid.

Disconnect, SQL timeout, or lost confirmation stops the old lease. Unknown write outcome cannot be retried from old in-memory progress; a new claim reads durable reality. Named locks belong to one MySQL server; failover must externally prevent the old primary from writing. This backend supplies neither multi-primary consensus nor automatic split-brain protection. Lease verification and Kafka effects remain non-atomic, preserving at-least-once behavior.

Store APIs do not read Kafka, execute handlers, forward messages, or decide business success. Run disk operations on independent bounded execution resources.

## Metrics export

Before business admission, call `proxy.metrics(sink)` or `proxy.metrics(sink, new MetricsOptions(capacity, maxBytes))`. Each source uses its own bounded export thread. Defaults are 256 queued events, 4096 UTF-8 bytes per event name plus labels, and at most one active callback. The framework adds `source` and permits at most seven other controlled labels; business keys, offsets, payloads, and exception bodies are excluded. No sink means no export thread.

Queue fullness, admission contention, or oversize events drop observation only. Backend failure does not affect ACK, DLT, commit, or delivery. `metricsExportStats()` reports queued/active/exported/dropped/failed work and actual thread exit without calling the backend. Counters are best-effort observations, not accounting or business ledgers; exported means the local callback returned successfully. After business resource exit, shutdown closes observation admission and drops queued events. A blocked callback must actually return; timeout reports OUTCOME_UNKNOWN and retains source identity for later shutdown.

Optional `MicrometerMetricsSink(MeterRegistry)` and `OpenTelemetryMetricsSink(Meter)` also accept `maxSeries`, default 4096. Applications declare the chosen optional dependency and supply the registry/SDK/exporter; the component does not own backend shutdown.

```java
proxy.metrics(new MicrometerMetricsSink(registry, 4096));
// 或 proxy.metrics(new OpenTelemetryMetricsSink(meter, 4096));
```

`MetricCatalog.definitions()` specifies names, types, units, and labels. Adapters reject unknown names, wrong types, missing source, extra labels, or negative counters. Once capacity is reached, existing series still update; eviction never creates unbounded backend series. `registeredSeries()`/`rejectedSamples()` expose capacity. OpenTelemetry units are ms/us/By/%; Micrometer keeps logical names/base units while exporters may rename physical series. Counters accumulate per backend instance, even when a source name is reused. Stale gauges do not establish current source health.

Java `MetricCatalog` and Rust `METRIC_CATALOG` describe the same 64 logical metrics, with matching types, units, labels, and meanings. All carry source. Metrics appear only when their path runs or explicit sampling occurs; catalog presence is not observed data.

| Metric | Type and unit | Additional labels and meaning |
| --- | --- | --- |
| `publish_admitted_total` | counter, records | lane/topic; native client accepted input |
| `published_total` / `publish_failed_total` | counter, records | lane/topic; native success / not confirmed successful; fire admission is not success |
| `publish_admission_rejected_total` | counter, calls | lane/reason; observer_capacity, byte_capacity, native_rejected |
| `publish_wait_unknown_total` | counter, calls | lane; sendAndWait lacks a definite result; direct Future cancellation is excluded |
| `producer_lane_in_flight` / `producer_lane_queue_utilization` | gauge, records / percent | lane; actual responsibility / observer use |
| `group_ready` / `group_ready_wait_millis` | gauge, 0 or 1 / milliseconds | group; all slots Running and joined / latest successful wait duration |
| `group_ready_total` / `group_ready_timeout_total` | counter, calls | group; successful / timed-out Ready waits, not state transitions |
| `assigned_partitions` / `paused_partitions` | gauge, partitions | group; total assignment / partitions with any pause reason |
| `active_handlers` / `dlt_in_flight` | gauge, calls | group; actual decoding/business / separate DLT responsibility |
| `retained_bytes` | gauge, bytes | group; input/copy reservation, not broker lag |
| `commit_attempted_total` / `commit_succeeded_total` / `commit_failed_total` | counter, calls | group; attempted / broker-confirmed / unconfirmed commits |
| `owner_session_started_total` / `owner_session_restarted_total` | counter, sessions | group; starts / starts after the first |
| `consumed_total` / `manual_acked_total` / `manual_ack_missing_total` / `manual_ack_after_gap_total` | counter, records | group/consumer_id; valid business prefix / Manual prefix / missing-Ack suffix / Ack beyond a gap; not broker commit |
| `handler_deferred_total` / `handler_dead_letter_total` / `handler_failed_total` | counter, calls | group/consumer_id; deferred / requested DLT / ordinary failure; passthrough counted separately |
| `decode_failed_total` | counter, record attempts | group/consumer_id; payload decode failure on a matched route |
| `retried_total` | counter, record attempts | group/topic; scheduled retry including deferral, excluding direct DLT requests |
| `dlt_total` / `dlt_failed_total` | counter, source records | group/topic; valid DLT confirmation / terminal availability-first failure; persistent retries use attempt counters |
| `dlt_attempt_failed_total` | counter, source record attempts | group/topic; actual query/delivery failure, including retained reliable input; post-stop admission cancellation excluded |
| `passthrough_calls_total` / `passthrough_duration_micros_total` | counter, calls / microseconds | group; actually exited read-only callbacks, including exceptions |
| `key_active_handlers` / `key_pending_records` / `key_pending_bytes` | gauge, calls / records / bytes | group; actual keyed calls and retained input, including eightfold copy reservations |
| `retained_records` | gauge, records | group; unconfirmed retained input, not inferred from consumer position |
| `dlt_queue_jobs` / `dlt_queue_bytes` | gauge, records / bytes | none; source-wide DLT admission usage |
| `registry_consumers` / `registry_groups` | gauge, routes / groups | client; frozen routes and managed groups |
| `publish_delivery_unknown_total` | counter, records | lane; local native responsibility ended with unknown remote write outcome |
| `control_target_total` | counter, targets | group/outcome; applied, not_applied, unknown, owner_changed |
| `rebalance_assigned_total` | counter, callbacks | group; assignment callbacks including initial join |
| `trace_invalid_total` | counter, input occurrences | group/reason; malformed or duplicate, excluding raw header content |
| `broker_lag_offsets` / `broker_lag_partitions` / `broker_lag_missing_partitions` / `broker_lag_observed_at_millis` | gauge, offset distance / partitions / partitions / Unix milliseconds | group/isolation; latest explicit offset-query scope and completeness |
| `source_stop_requested` / `source_resources_closed` | gauge, 0 or 1 | none; permanent stop intent / actual full resource exit |
| `close_active_owners` / `close_active_handlers` / `close_retained_records` / `close_released_unverified_records` | gauge, owners / calls / records / occurrences | none; remaining responsibility and unverified release during stop |
| `replay_active_tasks` / `transfer_active_dependencies` / `transaction_active_tasks` / `dlt_active_tasks` / `admin_active_borrows` | gauge, items | none; actual responsibility not yet exited in each domain |
| `metrics_queued` / `metrics_active` | gauge, events / callbacks | none; queued observation and actual execution |

The host calls `sampleMetrics()` for source responsibility, registration, and groups; it performs no broker query and creates no timer. Group metrics aggregate slots once per second. Key metrics include queued/running/retrying/completed evidence and old-authority calls still retaining input. Byte counts are reservations, not JVM/native-memory limits. A closed handle can still return the report, but a closed exporter need not deliver a final gauge; use CloseReport for resource conclusions.

Only actual `admin().observeOffsets(...)` queries update the four broker-lag gauges; there is no hidden network polling. Series cover the latest queried partition set per group/isolation, not automatically every group partition. Any missing valid lag makes `broker_lag_offsets=-1`; interpret it with total/missing counts and observation time, never as zero. Read-committed uses LSO, read-uncommitted ordinary end. Offset distance includes holes/control records. Local retained responsibility remains separate even when fetch positions have advanced.

Ready requires every owner RUNNING; protected owners are not Ready even if assignment remains. `consumer_id` comes from registration-frozen `KafkaListener.metricId()`, default target class or full annotated method signature. Explicit IDs must be stable, nonempty, control-free, and at most 1024 UTF-8 bytes, not random or message-derived. Discarded old results after stop/lost authority do not count as success. Temporary Manual Ack beyond a batch gap is counted separately and never advances its safe prefix.

## Trace propagation and actual execution lifetime

`KafkaTraceContext` accepts one exact-case `traceparent`, version 00, lowercase hexadecimal, and nonzero trace/span IDs. Duplicates, null, invalid bytes, or unsupported versions discard external trace only; valid business input keeps its original processing route. Reasons use bounded labels. Trace identity never grants routing, Ack, or commit authority. Flags propagate unchanged; the lowest bit controls sampling.

Before business admission, `proxy.telemetry(metricsSink, traceSink, sampleTraceRoots, options)` installs at least one non-null sink. Metrics and traces share a bounded source thread/queue and shutdown responsibility. `KafkaTraceSink` receives immutable payload-free `KafkaTraceSpan`; applications map it to their telemetry SDK and own that backend. Metrics export statistics include trace. For Spring-created sources, an application BeanPostProcessor initialization callback may install by `sourceId()` before any publish/start. A sink Bean alone is not automatically wired; repeated installation rejects.

Single-record consumption creates a child of valid inbound context, or optionally a root when missing. `KafkaRecord.traceContext()` is the inbound parent; `ConsumeContext.traceContext()` is the current invocation child. A batch creates an independent root with at most 128 unique parent `traceLinks()`, and `traceLinksOmitted()` counts valid inputs omitted by capacity. Completion spans retain the same links. Raw synchronous passthrough exposes context only within its valid borrowed scope.

Publication on the business invocation thread inherits `ConsumeContext.current()`. Across Executor continuations, explicitly pass `message(...).traceContext(context.traceContext())`; arbitrary Futures do not propagate thread context. Exit restores the outer context. Consumption spans wait for actual handler or CompletionStage completion. Publishing spans follow private delivery observers, unaffected by fire/cancellation or user-completed Futures. DELIVERY_UNKNOWN means local observation ended without known remote outcome; exporting a span does not prove committed consumption.

## Capability boundaries

Shared entry points include typed events, consecutive batches, callback-scoped Ack, keyed concurrency, lanes, DLT, partition control, controlled session recovery, synchronous raw passthrough, and Schema Registry. Both typed decoders and handlers are isolated from the native consumer owner, which continues polling during slow calls. Decoding and business share the admission-time timeout, retaining actual execution responsibility until exit. Owners rotate partitions so healthy partitions and later-admitted cold keys can use free capacity; a full DLT envelope pool pauses only the affected failed partition. Shared budgets still apply, with no real-time deadline or throughput guarantee. Java passthrough uses runtime read-only scopes; Rust uses synchronous borrowing. API names alone cannot establish full state/configuration equivalence.

Managed finite Replay, single-target Transfer, DLT replay/quarantine, POSIX local checkpoints, single-primary MySQL shared checkpoints, and dedicated same-cluster transactions are available. There is no Outbox/Inbox or dynamic hot configuration. A Replay processor defines its own success condition; built-in Transfer requires actual destination confirmation. Neither converts durable progress into cross-resource atomicity.

KafkaProxy getters do not expose native producer/Admin, mutable configuration, or routing tables. Configuration queries return copies; administration and control use managed entry points.

## Building, documentation, and licensing

```bash
mvn -B -ntp clean verify
```

See the [compatibility contract](CHANGELOG.en.md), [contribution guide](CONTRIBUTING.en.md), [security policy](SECURITY.en.md), and [publishing guide](RELEASING.en.md). Chinese and English public documents are included in the main, sources, and Javadoc archives with their license texts.

Licensed under Apache-2.0 OR MIT. See [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT).

# nasa-kafka

[中文](README.md) | [English](README.en.md)

面向 JDK 21 的多数据源 Kafka 事件运行时，统一管理应用连接多个集群时的发布、消费、历史补数与资源退出。每个源提供独立发布通道、有界有序消费、同集群消费与输出事务、持久有限回放、可恢复的单目标转发、有限 DLT 回投与原文隔离，以及可查询的健康与交付状态。

内核直接使用 Kafka 原生 Java client；Spring Boot 负责命名 Bean、配置和生命周期装配。业务也可以显式构造 `KafkaProxy`，不依赖 Spring 容器。与 Rust `nasa::kafka` 共同定义事件线路、消费组身份、确认责任、事务身份和持久任务格式，让两种语言能够交换事件并接续有明确边界的处理任务。

## 核心价值与边界

| 能力 | 业务价值 | 必须保留的边界 |
|---|---|---|
| 多数据源与独立发布通道 | 一个应用同时连接业务、审计等集群；源内控制消息与批量消息使用独立 producer 和额度 | 显式源名不回退；进程内存、CPU 和 broker 仍是共享的物理资源 |
| 按键有序与组内多消费者 | 同 key 保序，不同 key 或分区并行；增加消费槽分担拉取与执行 | 只提交连续安全前缀；慢调用实际退出前继续占用顺序和容量 |
| 有界输入与失败治理 | 进程、源、组、分区、key 共同限制输入责任，重试与 DLT 保留明确责任 | 输入预留不等于整个 JVM 堆上限；未知交付不能当作失败已撤销 |
| 持久回放与跨集群转发 | 冻结历史范围、固定资源身份，Java/Rust 可按共同 checkpoint 接续 | 目标确认先于进度持久化；跨资源可能重复，须使用稳定事件 ID 幂等 |
| Kafka 事务 | 同集群输入、批准输出与消费组位点原子提交 | 不覆盖数据库、HTTP 或另一个 Kafka 集群 |
| 受管接入与可查询关闭 | 命名 Bean、只读业务管理、当前 Ready、健康、指标和关闭责任贯通 | 宿主停止不等于资源已退出，资源退出也不等于全部业务成功 |

普通消费由唯一访问原生 consumer 的 owner 维护 poll、分配代次和提交；解码与业务执行使用有界资源。发布由独立 lane 观察真实交付。有限任务另持有 checkpoint 推进权，并随来源和目标的实际依赖一起排干。这些路径通过明确的确认时刻协作，避免调用方取消等待后留下无人负责的在途操作。

首次接入从[安装与装配](#安装与装配)、[多数据源与发布通道](#多数据源与发布通道)和[类型化与声明式消费](#类型化与声明式消费)开始。API 路径见[功能包与接入入口](#功能包与接入入口)；容量规划见[组内多消费者](#组内多消费者)和[配置数值与继承](#配置数值与继承)；运行判断见[控制、管理与关闭](#控制管理与关闭)及[指标出口](#指标出口)。

## 运行架构与确认时刻

```text
Spring 配置 / 显式构造 → KafkaClients 命名目录 → KafkaProxy 单源资源
                                                  ├─ KafkaHandle 业务权限
                                                  ├─ ProducerLane → 原生 producer → 交付观察
                                                  ├─ 组与 owner 槽 → poll → 路由 / 有界执行
                                                  │                         ↓
                                                  │               Ack → 连续安全水位 → commit
                                                  ├─ TransactionPlan → 专属 consumer / producer
                                                  └─ Replay / Transfer / DLT 回投 → checkpoint
```

每个源冻结连接和安全配置；每个普通消费槽独占原生 consumer。业务完成、Ack、DLT 交付和 broker 提交是不同事实，只有当前 assignment 代次的安全责任能进入提交前缀。发生 rebalance 后，旧 Ack 不再推进新 owner 的水位；同进程仍在执行的旧调用继续占用顺序和容量。

| 路径 | 可以报告成功的时刻 | 未知或中断的处理 |
| --- | --- | --- |
| 普通发布 | broker 交付确认 | `fire` 受理不等于送达；取消等待不撤销在途发送 |
| 普通消费 | 业务确认形成安全前缀，broker 确认提交后释放对应输入责任 | 失败前缀阻止越过；外部副作用可能因重投重复 |
| 同集群事务 | 整批输入位点与批准输出的 commit 获得确认 | commit 结果未知时保护 owner，按稳定身份恢复 |
| 有限任务 | 处理或目标交付确认后持久推进 checkpoint，再发布进度 | 持久进度前退出可能重复，不承诺跨资源原子性 |

Ready 使用当前分配与资源证据，不能由线程存在或过去成功推断。两段停机先停止新工作，再等待已接纳的责任实际退出；关闭报告分别展示停止、排干、资源关闭与失败，超时不会把未完成工作标为成功。

普通消费的 assignment epoch 只约束该次分配下的确认权；checkpoint lease 只约束持久任务的推进权；事务身份由 Kafka 协调器 fencing。三者作用域不同，均不能撤销已经发生的数据库、HTTP 或跨集群副作用。选择处理入口时，应先确定业务成功需要哪些系统确认，再决定使用普通 Ack、同集群事务或可重复接续的有限任务。

## Java 与 Rust 的共同合同

| 业务合同 | Java 入口 | Rust 入口 |
| --- | --- | --- |
| 命名源与业务权限 | `KafkaClients`、`KafkaHandle` | `Application::kafka`、`napp::KafkaHandle` |
| 类型化路由和整批确认 | `@KafkaConsumer`、listener、`AckMode` | `#[kafka_consumer]`、consumer trait、`AckMode` |
| 同集群原子处理 | `TransactionPlan`、`TransactionContext` | 同名计划与上下文，经 `nasa::kafka` 使用 |
| 有限历史与持久接续 | `ReplayPlan`、`TransferPlan`、`CheckpointStore` | 对应计划、共同编码与持久合同 |
| 死信来源与回投 | `DltReplayPlan`、`DltOrigin` | 对应计划与来源证据 |

共享的是线路字节、身份、默认行为和失败责任；配置拼写、异步类型和原生客户端参数遵循各自语言。普通消费组与事务组均保留合法 UTF-8 原名，事件名、null/空 key、有序重复 header 和 tombstone 保持各自含义。跨语言恢复仍要求同一物理资源身份、稳定任务身份、相容计划和有效 checkpoint；仅使用相同 topic 字符串不构成恢复资格。

Rust 对应组件的使用方式见 [nafka](https://github.com/nasa-runtime/nasa-runtime-rust/blob/master/nafka/README.md)。详细使用条件以本页各能力章节为准；数据库 Outbox/Inbox 适配、跨集群原子事务和任意外部副作用的 exactly-once 不属于本组件。

## 同集群消费与输出事务

`KafkaProxy.transact(TransactionPlan, TransactionProcessor)` 使用独立的 subscribe consumer 与专属事务 producer，把完整输入批次的业务输出和消费组位点原子提交。输出目标固定为计划批准的 topic/partition；物理 cluster ID 必须与真实 broker 一致。事务输入强制 `read_committed`，显式配置 `read_uncommitted` 会拒绝；下游也必须使用 `read_committed` 才能排除未提交输出。

事务 `group` 与普通实际组使用同一输入域：1至1024个严格 UTF-8 字节、不含 NUL、非共同纯空白，首尾不含 U+0000–0020；合法名称原样保留。`TransactionPlan` 构造时即校验，执行前仍复验。非法组及构造专属原生配置时的框架预检失败以 `TransactionException.Code.INVALID_CONFIGURATION` 拒绝；隔离冲突保留 `ISOLATION_MISMATCH`。这些预检拒绝发生在事务身份登记、专属原生资源创建和业务处理启动之前。

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

`TransactionContext.send` 只暂存本批输出，回调正常返回且 Manual 全部确认后才发送。`deadLetter` 通过同一个事务 producer 保存失败原文与 DLT 来源头，并确认对应输入。任一输入缺确认、业务失败或失权时中止整批；已确认 abort 才允许从输入起点重试。普通 lane、其它源、数据库、HTTP 和 socket 操作不属于该原子范围。

每条暂存输出按64字节固定开销加原始 key、payload 与完整有序 header 字节计费；null 与空字节保持区别。超额、未批准目标、非法时间或头名等准入拒绝锁存为整批失败，即使业务捕获异常也不能提交先前的部分输出。上下文只允许创建它的同步回调线程使用，封存、关闭或失权后不能继续确认或发送。

`TransactionIdentity(application, slot, owner)` 必须来自稳定部署身份。同一槽崩溃恢复复用身份；同时活动的槽必须不同，跨进程重复身份会导致 fencing。同集群中同一个 group 不应混入普通非事务消费者。本进程拒绝仍被占用的事务身份，同源内也拒绝普通提交协调器与事务 owner 共用组。两种语言按相同编码派生 `transactional.id`，恢复先初始化事务，再读取 broker 的组提交位置。

默认每批输入100条/8MiB、输出1000条/16MiB、回调10秒、事务60秒、原生操作10秒；最多重试3次已确认中止的批次。每源最多16个事务 owner，独立 poll 线程持续维护组成员资格；每个 owner 同时只有一个同步业务批次。此入口不接收 `KEY_ORDERED`、异步 handler 或手动 assign。

专属 producer 继承源的 producer 配置，并固定事务身份、幂等、all ACK 和本计划预算；不会复用普通 lane 实例。事务消费和发布拒绝 `interceptor.classes`，避免原生扩展改写已冻结的输入坐标或输出目标。

`TransactionHandle.status()` 提供 assignment epoch、当前分配、Ready、实际 drained、已确认提交/中止计数以及最后批次输入范围。发送确认不是最终事务成功。`COMMIT_UNKNOWN` 和 `ABORT_UNKNOWN` 会停止该 owner 新工作；不能把提交超时当作已中止，也不能直接换随机身份重发。使用同一稳定身份初始化恢复，并以 broker 提交位点接续。

broker 的 inter-broker listener 必须在 broker 所在网络内可达，包括事务协调器写入提交/中止标记的连接。仅有客户端地址可达不足以完成事务恢复；旧事务标记无法写入时，同一身份初始化可能持续返回并发事务或超时错误，任务不会进入 Ready，也不会读取新业务输入。
broker 的 `transaction.state.log.replication.factor` 与 `transaction.state.log.min.isr` 还必须满足实际可用副本条件；集群无法创建或写入 `__transaction_state` 时，普通消息可用并不能证明事务可用，初始化可能超时并保持未就绪。

取消或回调超时只撤销未提交批次的权威，不能证明用户代码已经退出。已进入 commit 的操作继续等待真实结果；`awaitDrained` 和来源两段停机保留回调及原生资源责任，实际退出后才释放身份。Ready 是当前快照，重平衡、取消或失败后会失效。

## 多数据源与发布通道

`sourceId` 区分逻辑源，`ProducerLane` 在源内隔离生产者、队列、字节预算与回执处理。源名不等于物理集群身份；`expected-cluster-id` 可约束实际连接目标。

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

| 源 | 业务 Bean | 资源所有者 Bean | 显式业务查找 |
|---|---|---|---|
| primary | kafkaHandle | kafkaProxy | `clients.handle("primary")` |
| audit | auditKafkaHandle | auditKafkaProxy | `clients.handle("audit")` |

`default-source` 决定按类型注入、`load()` 和未声明来源的 listener。切换默认源不改变固定 Bean 名；未知显式源绝不回退。`load(name)` 未命中返回 null，`require(name)` 抛错。

业务优先注入 `KafkaHandle`，多源使用 `@Qualifier("auditKafkaHandle")`。它提供发布、消费控制、健康与回放／转发／事务入口，不返回 `KafkaProxy`、原生客户端、可变配置或整源关闭权。`handle.admin()` 始终只读，建删 topic、扩分区和改配置在建连前返回 `AUTHORIZATION`；`readOnly()` 可进一步收窄其它 Admin 视图，不能恢复写权。确需管理写入时，按源显式设置 `admin-write: true`，再调用 `handle.administration()`；默认值为 false，授权不跨源，仍需 broker ACL。

`KafkaProxy`、`KafkaClients.require()` 和静态 `KafkaProxy.require()` 是保留的资源所有者入口，供装配、独立工具和既有调用方使用，拥有管理与生命周期权限，不属于上述业务能力边界。应用在依赖注入和模块装配时应明确区分两类角色。关闭后旧业务句柄、Admin 和 lane 均不能接纳新操作；`handle.awaitClosed(deadline)` 仅在宿主最终清理已交权后续等实际退出，拒绝提前关闭活跃源。

宿主业务准入关闭后，保留的 lane 不能发起新的发送。已登记的 DLT/Transfer 交付按原任务责任继续收尾，受原来源、物理通道、批准目标和实际资源寿命限制；业务句柄不导出这项内部权限，等待超时也不会重新授予准入。

一个 JVM 只允许一个活动静态目录所有者；独立 Spring 容器使用 `export-static: false` 并通过各自的 `KafkaClients` 查找。路由全部登记后才启动消费，已有活动目录不会被另一个容器覆盖。

源名和 lane 名使用 `[a-z][a-z0-9_-]{0,63}`。新 `sources` 与旧根级单源连接/生产/消费字段不能混用，未知配置字段启动时拒绝。旧单源配置仍使用 `nasa.kafka.bootstrap-servers`，映射到 primary。

## 安装与装配

```xml
<dependency>
    <groupId>io.github.nasa-runtime</groupId>
    <artifactId>nasa-kafka</artifactId>
    <version>1.0.0</version>
</dependency>
```

要求 JDK 21 或更高版本，构建要求 Maven 3.6.3 或更高版本。Spring Boot 自动发现 `spring.NasaKafkaConfig`；普通 Spring 使用 `@Import(NasaKafkaConfig.class)`。本组件自行管理源目录与容器生命周期，无需额外引入 `nasa-spring-boot-starter`。未配置连接或设置 `nasa.kafka.enabled=false` 时不创建后台资源。

### 显式装配命名源

以下示例不需要 Spring 容器，两个命令行参数分别提供业务集群与审计集群的 bootstrap servers。预先创建业务集群的 `orders` topic，并配置实际需要的身份与权限；示例不会创建 topic 或修改 broker 配置。

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

显式构造不会登记静态目录；此处的 `exportStatic=false` 使源名仅存在于这个 `KafkaClients` 中。发布源不需要 `group-id`，消费源必须配置有效组名或由 listener 明确指定。需要消费时，把完整 listener 集合传给 `clients.start(listeners)`；只管理一个独立代理时，也可先 `register` / `registerAnnotated`，再 `before()`，最后由创建者调用 `shutdownUntil`。

`awaitReady` 对所有 `required=true` 的源查询 broker，并等待已登记普通组的分配；发布专用源没有消费组，不会创建消费 owner。它不证明目标 topic 的写权限或交付成功。声明 `expected-cluster-id` 可在源启动时拒绝错连；事务与有限任务仍需分别读取自身状态。关闭超时应保留源所有者并再次等待，不能把示例中的异常当作资源已经退出。

## 功能包与接入入口

公共类型按业务功能组织，接口、数据记录、枚举和专用异常与所属功能放在同一包中。下表中的包名均相对于 `io.github.nasaruntime.kafka`，根包只保留 `KafkaProxy`、`KafkaClients`、`KafkaHandle` 三个顶层接入类型。业务通过源目录和业务句柄使用能力；各功能包提供该能力的计划、上下文、结果与扩展接口，具体签名与调用条件见对应类型的 Javadoc。

| 包 | 职责与主要接入类型 |
| --- | --- |
| 根包 | `KafkaClients`、`KafkaProxy`、`KafkaHandle`：命名源、资源所有者与业务权限 |
| `config` | `NasaKafkaProperties`、`KafkaSettings`、`KafkaMigration`：配置、覆盖规则和配置转换 |
| `spring` | `NasaKafkaConfig`、`KafkaConsumer`：Spring 装配与声明式消费注解 |
| `producer` | `ProducerLane`、`PublishBuilder`、`PublishMessage`、`Delivery`：发布与交付 |
| `consumer` | `KafkaRecord`、`ConsumeContext`、`AckMode`、`StartOffset`、`AssignmentHandle`、`GroupHealth`：消费确认、控制、上下文与健康 |
| `consumer.listener` | `KafkaListener` 及单条、批量、异步、广播和原文透传监听接口 |
| `admin` | `KafkaAdmin`、`OffsetObservation`、`KafkaPartitionLag`：管理与位点观测 |
| `transaction` | `TransactionPlan`、`TransactionContext`、`TransactionProcessor`、`TransactionHandle`、`TransactionException`：同集群事务 |
| `checkpoint` | `CheckpointStore`、`CheckpointLease`、`ReplayCheckpoint`、File/MySQL 存储和 `CheckpointException`：持久进度与租约 |
| `replay` | `ReplayPlan`、`ReplayProcessor`、`ReplayIdentityProvider`、`ReplayHandle`、`ReplayException`：有限历史回放 |
| `transfer` | `TransferPlan`、`TransferRegistries`、`TransferHandle`、`TransferException`：跨资源转发 |
| `dlt` | `DltOrigin`、`DltEvidence`、`DltReplayPlan`、`DltReplayHandle`、`DltReplayException`：死信证据、回投和隔离 |
| `schema` | `SchemaRegistryClient`、`ConfluentSchemaRegistry`、`SchemaRegistryTls`、`ConfluentEnvelope`：Schema Registry 与信封 |
| `observability.metrics` | `MetricsSink`、`MetricsOptions`、`MetricCatalog` 与指标后端适配 |
| `observability.trace` | `KafkaTraceSink`、`KafkaTraceContext`、`KafkaTraceSpan`：追踪上下文与导出 |
| `protocol` | `KafkaHeaders`、`KafkaWire`：消息头与共同线路规则 |
| `lifecycle` | `CloseReport`：停止、排干和资源退出证据 |
| `support` | `KafkaCapabilities`：原生客户端能力描述；其它类型用于框架内部客户端协作 |
| `exception` | `KafkaFailure`：跨功能的基础失败分类 |

例如，声明式消费所需的类型从以下位置导入：

```java
import io.github.nasaruntime.kafka.KafkaHandle;
import io.github.nasaruntime.kafka.config.NasaKafkaProperties;
import io.github.nasaruntime.kafka.consumer.AckMode;
import io.github.nasaruntime.kafka.consumer.KafkaRecord;
import io.github.nasaruntime.kafka.consumer.listener.KafkaRecordListener;
import io.github.nasaruntime.kafka.spring.KafkaConsumer;
import io.github.nasaruntime.kafka.spring.NasaKafkaConfig;
```

接入代码应使用对应包的显式 import。使用其它包路径的调用方需要调整 import 并重新编译；不提供原包路径的二进制兼容别名。嵌套类型仍通过所属类型引用，例如 `TransferPlan.Target` 和 `TransferException.Code`。Spring Boot 自动配置资源使用 `io.github.nasaruntime.kafka.spring.NasaKafkaConfig`，显式 `@Import` 使用同一个类。

标明“供框架功能包协作使用”的类型、构造器与方法用于内部装配和资源协调；其 Java 可见性满足跨包调用，不属于稳定的业务接入接口。业务优先使用上述入口、计划和句柄，资源所有者的生命周期与权限边界仍按对应能力说明执行。

## 类型化与声明式消费

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

注解支持 `T`、`KafkaRecord<T>`、`List<T>` 和 `List<KafkaRecord<T>>`，可增加第二个 `ConsumeContext` 参数。返回值为 `void` 或 `CompletionStage<Void>`；异步完成前仍保留消费责任。Spring 中的 topic、event、group 和 source 支持配置占位符。

消费方法必须是 `public` 实例方法，Bean 类可以是包可见类；继承方法与 Spring JDK/CGLIB 代理保留实际代理调用链。JDK 代理必须导出消费方法所属接口；若 Java 模块边界禁止访问该方法，则在注册时以 `CONFIG` 拒绝，需要按应用模块配置开放相应包。

接口接入使用 `KafkaSingleListener`、`KafkaBatchListener`、`KafkaRecordListener`、`KafkaRecordBatchListener`、`KafkaAsyncListener` 或 `KafkaAsyncBatchListener`。显式构造时调用 `proxy.register(listener)`，全部注册后调用 `before()`；注解对象可通过 `registerAnnotated` 接入。启动后路由冻结。

消费组名称决定分区分配和提交位点的归属。普通实际组的共同输入域为：**1–1024 个严格 UTF-8 字节、不含 NUL、不是纯空白，首尾不含 U+0000–0020**。空白集合为 U+0009–000D、U+001C–0020、U+1680、U+2000–2006、U+2008–200A、U+2028–2029、U+205F、U+3000；NEL、NBSP、U+2007、U+202F 是有效身份字符。ASCII 首尾控制字符和空格会被原生字符串配置裁剪，因此直接拒绝；名称内部空格、有效非 ASCII 边缘字符保留。所有合法名称都不替换、不裁剪、不做 Unicode 归一化。长度按 UTF-8 字节计算，允许超过255字节的组在共同上限内原名接续。

显式 listener、注解、默认 `group-id`、组并发覆盖键、手动 `assign`、管理查询和迁移候选遵守相同边界。Java 孤立 UTF-16 代理项也以 `CONFIG` 拒绝。默认配置的 null/空串表示未指定组，允许纯发布源；listener 的 null/空串表示沿用默认组，其它显式名称不能因纯空白而回退。实际消费始终需要有效组名。构造源时冻结配置，后续修改原配置不改变已验证身份；非法组在路由登记、owner 创建或原生请求前拒绝。

1024字节上限保证组名能完整进入现有 DLT 来源证据和恢复合同。已有超过上限、含 NUL 或带 U+0000–0020 首尾字符的组不属于受管消费及管理查询的支持域，不能直接由这两个运行时接续；必须继续使用能够处理原身份的客户端，或显式规划迁移并核对位点。框架不通过截短、改名或复制 committed offset 自动迁移这些组。

泛型在注册时解析，包括继承、嵌套容器与 Spring 代理；无法确定或与显式 `paramType()` 冲突时拒绝注册。批次只包含同一分区、同一路由的连续记录，`A/B/A` 不会聚合成 `A/A/B`。

## 组内多消费者

订阅组可配置多个 owner 槽，每槽独占一个原生 consumer，共享最终 group.id，由 Kafka 分配互不重叠的分区。增加槽数可分散原生 poll、解码和业务执行压力；同一分区仍只由获得分配的 owner 推进安全水位。固定 `assign` 始终只有一个槽，避免本地重复读取显式范围。

下面字段放在具体 source 下：

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

`concurrency` 默认 1，允许 1–64；`group-concurrency` 按解析完成的实际组名精确覆盖，包含广播模式最终生成的组名。配置在组创建时冻结，不支持原地调整槽数。多槽自动为 consumer 的 `client.id` 和显式 `group.instance.id` 增加 `-slot-N` 后缀；静态成员的原始身份仍须在不同进程之间唯一。

责任数量、原文副本预留字节及实际 handler 数由同组全部槽共享。空闲额度可被任一 owner 使用，增加槽数不会放大组上限；允许 owner 数量超过可同时执行的 handler 数。原输入额度直到 broker 提交确认或明确交回重投才释放，逻辑超时不释放实际调用名额。DLT 信封按记录数计费，数量、字节与实际交付由同一源全部普通消费组共享；信封额度覆盖等待、交付、完成和重试，实际交付另受 `behavior.dlt-max-in-flight` 限制，默认 16，与业务调用额度独立。容量不足时保留来源责任等待，不启动新的 DLT 调用。失败批按来源顺序逐条转移，因此业务批可以大于 DLT 池的记录容量。复制信封原文前先保留额度，字节额度采用原输入、topic/group 的 UTF-8 字节数及 4096 字节来源证据上界之和；单条超过空池上限时保留未确认来源并停止该分区。分区轮转派发，每轮每分区至多启动一个顺序域。

`consumer.max-assignment-partitions` 默认 10000，约束全部槽当前分配的不同分区数量，本地接管同一分区只计一次。固定范围在原生 assign 前校验，订阅分配在开放 Ready 与派发前校验，超限 owner 进入保护状态。`behavior.max-retry-entries` 默认 100000，按保留普通失败、延期或缺失确认次数的输入记录计费，全部槽共享。批失败原子接纳全部新增记录；容量不足保护失败分区且不提交失败输入，健康分区仍可继续。次数在成功、DLT 确认、失权或明确定位后清除，不是持久重试次数。两项均须为 1–2147483647，空闲槽不预留份额；原有平铺配置入口分别为 `max-assignment-partitions` 与 `max-retry-entries`。

组内重平衡还保留实际调用的顺序占用：新 owner 必须等旧的同 key 或独占分区调用退出后才能派发；旧确认立即失效。跨进程成员的外部副作用仍需业务幂等或目标系统 fencing。

普通 `subscribe` / `assign` 的 typed 和原始消费输入同时受进程、源、组及分区记录边界约束。同一 `KafkaProxy` 下全部组共享源额度；同一组件类加载器内全部代理共享进程额度。首次有效源构造冻结进程上限，后续源声明不同值会在创建原生资源前返回 `CONFIG`；改变进程上限需要重新启动对应加载环境。重试复用原凭证，失权或取消等待不会释放仍被实际业务持有的原文。单条消息超过任何层级空池的字节边界时进入超大记录保护，不无限等待容量。

分区与同一分区的 key 各自具有记录数和原文副本预留字节上限，null key 与空 key 独立计费。计费身份包含所属源、组和分区；两个分区中的同名 key 互不占用对方额度。等待准入只保存固定长度摘要，摘要碰撞会保守合并额度，不影响完整原始 key 的业务串行判断。达到 key 上限时保留该记录的重读位置，已准入的其它 key 仍可完成；同一分区尚未取入的后续输入需要等待边界记录获得容量。

输入字节额度采用共同保守口径：原始 payload、key、完整有序头的字节和为 `R`，每条预留 `8 × R`，记录数量仍只计一次。预留覆盖原输入、拥有型交付、头映射与完整键顺序域的必要副本，物化前一次取得并保持到最后真实责任退出；共享同一原文的内部引用不另占一份记录。这样原输入占满额度时仍有空间执行业务，不会等待额外副本空间而停滞。`max-record-bytes` 和 `max-batch-bytes` 继续按原始 `R` 计量；例如输入预算 8 MiB 最多接纳合计 1 MiB 原文，若单条的完整预留超过任一空池上限则停止该分区。固定框架元数据受记录数上限约束。

这些数量及副本预留字节额度不包含 native fetch、解码对象的额外堆内存或线程开销；业务自行复制并保留的数据由业务限制。事务、有限回放和转发使用各自计划的独立额度，producer lane 和 DLT 信封也独立计费。连续批同时受 `max-poll-records` 和 `behavior.max-batch-bytes` 限制，默认 500 条、16 MiB；单条合法输入可以独占一批超过批字节额度，但仍受单记录及各层输入额度约束。

`groupSlotsHealth(group)` 按固定槽号返回独立快照，`groupHealth(group)` 汇总 assignment、活动调用、保留字节和累计次数，poll 时刻取最慢槽。全部槽完成 join 且分区归属不冲突时才满足组 Ready；已 join 的空闲槽允许存在。组 epoch 记录完整槽代次向量的变化，不能当作单槽 epoch 或跨进程租约。

分区 pause/resume/seek/position 先按当前归属拆分，再由实际 owner 复验；订阅变更向全部槽发送。跨槽操作没有原子性，既有整体返回接口在部分成功或开始后超时时报告 `CONTROL_OUTCOME_UNKNOWN`；需要逐目标证据时使用 `controlPartitions`。应查询各槽实际状态后收敛。重建只作用于允许恢复的槽。关闭先通知所有槽，再在共同期限内等待实际退出，未完成责任不会因等待超时而释放。

### JVM 进程内保序执行

普通消费由 `KafkaPollTask` 在 poll owner 上按真实 offset 选择可执行记录，`KafkaGroupBudget` 在全部 owner 槽之间原子取得组级调用名额及分区或完整 key 的顺序许可，随后通过 JDK 虚拟线程执行解码与业务。异步 listener 返回的 `CompletionStage` 必须结束，调用上下文也必须清理完成，owner 才归还许可。失败退避、DLT 交付和 null key 分区屏障都参与相同的顺序判断；重平衡使旧 Ack 失效，但不能提前释放仍在执行的旧调用。

`KafkaPollTask` 内部的 `Partition` 是 Kafka assignment、输入和安全水位的状态账本，不是 `nasa-core.base.Partition` 调度器。后者按固定哈希槽和任务类型提供 FIFO 与保序任务窃取，以 `Task.exec()` 返回作为执行结束。普通消费使用前述 Kafka 顺序许可和虚拟线程执行路径：任务入队顺序、异步业务完成、失败重试与 broker 提交分别具有独立责任，不能只凭通用任务完成状态开放后续消费或推进位点。

共享基础能力使用 `nasa-core` 提供的 `AnyHolder`、`RecycleLinkedMap`、`ProtocolBytes`、事件接口及相关工具。通用组件复用须同时保持顺序域、实际完成时刻、容量、故障隔离和关闭语义；业务处理器自行启动并脱离返回结果的后台工作不属于框架可等待的调用责任。

## 确认、顺序与失败责任

默认 `PARTITION_ORDERED` 在每个分区串行执行，不同分区可并行。`KEY_ORDERED` 在同分区同 key 内串行，不同 key 可并行；无 key 记录使用分区串行后备。按键模式拒绝批 listener，同组路由必须使用一致执行模式。扩分区后相同 key 的历史与新记录可能属于不同分区，因此不保证跨分区顺序。

Auto Ack 要求回调或异步结果成功。Manual Ack 要求回调期内显式确认且最终成功，先 ack 后抛异常不会提交；过期 token 拒绝确认。批次只推进连续确认前缀。超时不会让实际仍运行的 handler 释放顺序槽。

typed 解码和业务共用有界执行额度，不在 poll owner 上执行自定义 codec。解码阻塞时仍维护协调及控制邮箱；停止、超时或失权后不再解码批次尾部，也不把迟到解码结果交给业务。Java 的 `handler-timeout-ms` 从执行准入起计算，包含解码；实际 codec 退出前关闭仍可能超时，调用者取消等待不能提前释放责任。

owner 是原生 consumer 的唯一访问线程。已观察记录进入数量和字节有界的责任表，只有安全前缀才能提交；提交失败保留责任并继续尝试。自动提交不能开启。

普通 typed 消费按数量或时间阈值提交；本地输入已全部安全完成且实际调用已退出时，尾批提前提交，不等待凑满阈值。该行为只缩短已完成责任的保留时间，不越过失败、未确认或仍在执行的输入；提交结果未知时仍进入保护与重试流程。业务完成通知可以立即唤醒调度，原生 poll 和协调维护保持有界周期。

`HANDLER_DEFERRED` 表示外部条件尚未满足，不消耗普通失败预算；总投递次数仍增长。`HANDLER_DEAD_LETTER` 请求转入 DLT。`ConsumeContext` 与每条 record 的 `context()` 提供源、实际 group、assignment epoch 和两种尝试次数。

| 情形 | 默认行为 |
|---|---|
| 无匹配事件 | SKIP，明确跳过并允许提交 |
| 无效头、解码失败、不允许的 null | DEAD_LETTER |
| 普通业务失败 | 退避重试，达到 max-consume-attempts 后进入 DLT；零表示持续重试 |
| DLT 成功 | 收到 broker 确认后才允许原记录提交 |
| DLT 不可达 | 保留责任，后续分区提交受阻 |
| DLT 禁用或递归 DLT | 停止该分区推进 |
| 单条原文超过预算 | 保留 broker 坐标并进入保护状态 |
| 连续解码失败达到阈值 | 分区进入保护态，阻止整条主题持续转入 DLT |

无匹配与坏消息策略可选择 SKIP、DEAD_LETTER、HALT。`dead-letter-required=false` 是显式可用性优先选择：DLT 发送失败后允许丢弃该记录并前移，健康原因显示 `DltUnavailableDiscarded`。

必需 DLT 使用独立指数退避：`dlt-retry-initial-ms` 默认 1000，指数基准按倍数增长至 `dlt-retry-max-ms` 默认 60000，再按来源 offset 加入约正负百分之十的稳定抖动。连续失败达到 `dlt-degraded-after-attempts` 默认 10 后显示 DEGRADED，输入和信封额度继续保留，健康分区可继续推进；确认成功后解除该记录的降级原因。停止后不再启动新的重试轮次，失败不等于原记录已安全处理。

将 `dead-letter-topic-suffix` 设为空串时，`invalid-record-policy`、`unmatched-policy`、`tombstone-policy` 均不能选择 DEAD_LETTER；例如关闭死信且保留坏输入时显式配置 `invalid-record-policy=HALT`。普通业务重试耗尽后仍可因 DLT 被禁用而进入分区保护。

消费是 at-least-once；业务应以稳定消息身份实现幂等。进程崩溃、再均衡、提交失败和发送结果未知都可能重复。确认与 epoch 约束不提供数据库或 HTTP 外部副作用 fencing。

## 发送与资源上限

```java
Delivery delivery = kafka.message("orders", order)
        .event("created").key(order.id())
        .sendAndWait(Duration.ofSeconds(5));

kafka.producerLane("control").message("commands", command)
        .event("refresh").fire();

kafka.publishRaw("archive", originalBytes).keyBytes(rawKey).send();
kafka.tombstone("state", entityId).send();
```

`send()` 返回交付 Future；取消等待不会取消内部回执责任。`fire()` 只表示本地准入，最终结果由 lane 统计记录。准入失败立即可感知；消息可能发出后的等待超时或中断是结果未知。类型化、raw、tombstone 及兼容发布入口在编码前冻结绝对期限，编码、排队与 native send 前等待共用预算。同步 codec 不保证可抢占；返回时已超期则以 `PUBLISH` 拒绝，消息不进入 native。用户回调在独立有界回执线程执行。

默认 `acks=all`、幂等开启、`max.in.flight.requests.per.connection=1`。确认可靠性仍依赖 broker 的 ISR、最小副本和保留配置；幂等 producer 不等于业务 exactly-once。`acks=0` 的 Delivery 标记为未确认，DLT 拒绝使用未确认通道。

每个 lane 默认最多 10000 条准入记录、65536 KiB 原文与 10000 个交付观察槽；实际准入按消息数、观察槽、字节预算的最先耗尽者拒绝。原文字节为 key、payload、所有 header 名的 UTF-8 字节与非 null 头值之和，重复头逐项计费；普通发布和内部 DLT/Transfer 共用该门禁。条数、字节和 observer 一直保留到实际交付责任结束，调用方超时或取消不会归还。原生客户端另有自身缓冲限制；压缩、连接及对象开销不包含在框架字节额度内。

`max-record-bytes` 同时限制单条发布原文；包含框架追加头的完整输出超限时，在 native 准入前以 `PUBLISH` 拒绝。事务输出使用 `TransactionLimits` 的独立批次条数与字节合同。普通消费默认每组保留最多 65536 条、64 MiB 副本预留，每分区最多 2048 条；同组全部槽共享最多 64 个实际解码与 handler 调用。普通 DLT 使用源级独立池，默认最多 16 个实际查询或交付调用，不占用这 64 个业务执行名额。

`key(String)` 与兼容字符串 key 精确编码为 UTF-8，不裁剪、不归一化；孤立 UTF-16 代理项以 `CONFIG` 拒绝，不能被替换为 `?` 后改变分区或幂等身份。`keyBytes` 保留任意原始字节，null 与空 key 仍不同。

`event` 同样按精确 UTF-8 身份发布和注册；非法 UTF-16、空串和纯空白事件在准入前以 `CONFIG` 拒绝，builder 拒绝 setter 后保留原事件。兼容发布的 null event 仍表示省略事件头并使用 DEFAULT；builder 的显式 `event(null)` 无效。双端事件空白集合为 U+0009–000D、U+001C–0020、U+1680、U+2000–2006、U+2008–200A、U+2028–2029、U+205F、U+3000；NEL、NBSP、U+2007、U+202F 不在此集合。非空白事件保留首尾空格及 Unicode 原文，不裁剪、不归一化；消费与注册使用同一规则。

显式 `tombstone(topic, key)` 要求非 null key，允许空字符串并精确保留为零长度字节键；value 固定为 Kafka null。Java 缺失键以 `CONFIG` 拒绝，Rust 字符串参数不能表达缺失键。`publishRaw(topic, null)` 对应 Rust `publish_raw_nullable(topic, None)`；raw null value 仍允许 null、空字节及任意二进制 key，与显式删除键约束独立。空字节 value 仍为空字节，不转换为 null。

双端支持的 header 名必须非空、可精确编码为 UTF-8 且不含 NUL。普通发布、事务输出和框架内部转发都在 native 转换前验证；头值允许任意字节和 null，顺序、重复名称与大小写保持。外部 producer 也必须遵守名称合同：Kafka broker 允许存储带 NUL 的头名，而 Rust 当前使用的 librdkafka 头迭代器仅提供 NUL 前缀，不能据此还原或保证识别完整原名。这类外部输入不在精确保留与 strict 路由校验的支持范围；应在写入 Kafka 前约束生产者或验证原始输入。`strict-headers` 不认证写入者，也不能替代 topic 写权限和可信来源边界。

`flush()` 先冻结全部 lane 当前已准入的发送，再等待交付和回执处理结束；`lane.flush()` 只冻结该 lane。屏障覆盖旧原生代次，不等待随后准入的消息。屏障内失败会返回异常，各消息结果保留具体终态；超时或取消等待不撤销发送。不同 lane 不保证相互发送顺序，慢回调会占用所属 lane 的观察额度。

屏障绑定创建它的物理通道，不导出可修改的交付 Future 或数组，也不能由调用方自行构造。业务拿到的发送 Future 是独立等待结果，取消或主动完成它不会改变框架私有交付凭据、屏障中的实际交付证据或在途额度。

每个 lane 独立维护原生 producer 代次。连续无成功交付达到 `stalled-rebuild-after-ms`、连续终态失败达到 `stalled-rebuild-min-failures`，并满足 `stalled-rebuild-cooldown-ms` 后，后续发送可切换到新代次；默认分别为 60000ms、2 次、60000ms，均可在命名 lane 覆盖。本地准入拒绝不计入交付失败窗口，成功交付会清空当前代次窗口。可恢复的原生序列错误可触发代次替换；认证、授权、fencing 和不兼容协议进入永久保护，需关闭旧源并以正确配置创建新源。

旧代次继续承担其交付，不自动重发未知消息，其迟到结果不能改变新代次恢复窗口。Java 同一 lane 最多保留当前代次和一个正在退出的旧代次；旧资源未退出时延迟下一次替换。原生 `client.id` 在 lane 身份后附加 `-generation-N`，避免旧代次退出时注销新代次的 JMX 入口。幂等序列不跨代次延续，重试和对账仍需稳定业务 event ID。`lane.recoveryStats()` 与 `proxy.producerRecoveryStats()` 显示代次、重建原因计数、永久保护和真实原生资源数；关闭期限耗尽时仍保留未退出责任，重复关闭继续等待。

`publish`、`publishFuture`、`publishAsync` 入口使用默认 lane。`publish(topic, value)` 和 `partition(topic, key, value)` 省略事件时均使用 `DEFAULT`。需要以 topic 名作为事件的调用方须显式使用 `partitionLegacy(topic, key, value)` 或 `partition(topic, topic, key, value)`；接入前核对消费路由，不能只替换二进制后沿用隐含的 topic 事件假设。

## 配置数值与继承

默认lane及所有已声明lane在创建客户端前验证确认策略、幂等、重试顺序、容量和超时。lane分别继承源默认值，彼此不继承；嵌套lane和未知配置字段拒绝。`producer.retry-backoff-ms`及lane同名字段允许零，表示原生发送不主动延迟重试。`consumer.fetch-min-bytes`、`consumer.fetch-max-wait-ms`可显式配置，零表示不要求聚合等待。

共同的配置毫秒上限为2147483647。调用API传入的Duration有各自边界，不由该配置上限推导。消息数、观察槽和有效业务等待预算必须为正；`max-consume-attempts=0`表示无限业务重试，`handler-timeout-ms=0`表示无限调用确认窗口，`decode-failure-halt-streak=0`关闭连续解码保护。Rust对应后两者也接受零，并额外接受`null`表达相同的关闭含义。

Admin的`default-api-timeout-ms`未设置时继承`publish-timeout-ms`，`request-timeout-ms`未设置时继承该API预算；默认均为120000ms。请求预算不得超过API预算。调用方如需固定更短管理时限，应显式配置或使用带timeout的管理API。raw配置不覆盖这些强类型默认值。

以下列出共同字段的默认值；Java列为源下扁平配置键，Rust列为`KafkaConfig`内路径。容量字段的计费范围仍以对应消费、发送和DLT合同为准，数值相同不表示原生缓冲区相同。

| Java键 | Rust路径 | 默认值 |
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
| `dlt-queue-max-bytes` | `behavior.dlt_queue_max_bytes` | `67108864` 字节 |
| `dlt-max-in-flight` | `behavior.dlt_max_in_flight` | `16` |
| `dlt-retry-initial-ms` | `behavior.dlt_retry_initial_ms` | `1000` ms |
| `dlt-retry-max-ms` | `behavior.dlt_retry_max_ms` | `60000` ms，指数基准上限 |
| `dlt-degraded-after-attempts` | `behavior.dlt_degraded_after_attempts` | `10` |
| `handler-timeout-ms` | `behavior.handler_timeout_ms` | Java `0` / Rust `null` ms |
| `invalid-record-policy` | `behavior.invalid_record_policy` | `dead_letter` |
| `max-poll-records` | `behavior.max_batch_records` | `500` |
| `max-assignment-partitions` | `consumer.max_assignment_partitions` | `10000`，全组不同分区 |
| `max-retry-entries` | `behavior.max_retry_entries` | `100000`，全组失败记录 |
| `max-batch-bytes` | `behavior.max_batch_bytes` | `16777216` 字节 |
| `max-consume-attempts` | `behavior.max_consume_attempts` | `3` |
| `max-progress-bytes-per-group` | `behavior.max_group_pending_bytes` | `67108864` 字节 |
| `max-progress-records-per-process` | `behavior.max_process_pending_records` | `1048576` |
| `max-progress-bytes-per-process` | `behavior.max_process_pending_bytes` | `1073741824` 字节 |
| `max-progress-records-per-source` | `behavior.max_source_pending_records` | `262144` |
| `max-progress-bytes-per-source` | `behavior.max_source_pending_bytes` | `268435456` 字节 |
| `max-progress-records-per-group` | `behavior.max_group_progress_records` | `65536` |
| `max-handler-in-flight` | `behavior.max_in_flight_handlers` | `64` |
| `max-progress-records-per-partition` | `behavior.max_progress_records` | `2048` |
| `max-progress-bytes-per-partition` | `behavior.max_partition_pending_bytes` | `67108864` 字节 |
| `max-progress-records-per-key` | `behavior.max_key_pending_records` | `1024` |
| `max-progress-bytes-per-key` | `behavior.max_key_pending_bytes` | `16777216` 字节 |
| `max-record-bytes` | `behavior.max_record_bytes` | `16777216` 字节 |
| `poll-timeout-ms` | `behavior.poll_timeout_ms` | `500` ms |
| `publish-timeout-ms` | `behavior.publish_timeout_ms` | `120000` ms |
| `shutdown-timeout-ms` | `behavior.shutdown_timeout_ms` | `30000` ms |
| `start-consume-delay-ms` | `behavior.start_consume_delay_ms` | `1000` ms |
| `strict-headers` | `behavior.strict_headers` | `true` |
| `tombstone-policy` | `behavior.tombstone_policy` | `skip` |
| `unmatched-policy` | `behavior.unmatched_policy` | `skip` |
| `auto-offset-reset` | `consumer.auto_offset_reset` | `latest` |
| `consumer-concurrency` | `consumer.concurrency` | `1` |
| `fetch-min-bytes` | `consumer.fetch_min_bytes` | `1` 字节 |
| `fetch-max-wait-ms` | `consumer.fetch_wait_max_ms` | `500` ms |
| `group-concurrency` | `consumer.group_concurrency` | `{}` |
| `heartbeat-interval-ms` | `consumer.heartbeat_interval_ms` | `10000` ms |
| `max-partition-fetch-bytes` | `consumer.max_partition_fetch_bytes` | `1048576` 字节 |
| `max-poll-interval-ms` | `consumer.max_poll_interval_ms` | `300000` ms |
| `session-timeout-ms` | `consumer.session_timeout_ms` | `30000` ms |
| `acks` | `producer.acks` | `all` |
| `batch-size` | `producer.batch_size` | `16384` 字节 |
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

## 线路与上下文

JSON value 为业务对象本身，不携带 Java 类名。实现 `ProtocolBytes` 的 payload 使用声明的二进制模式。`legacy-envelope=true` 才启用历史信封探测；任意类 default typing 不允许开启。

跨语言共同模式为 `JSON_BYTES`、`VARINT_TLV`、`BITPACK_TLV`。两端须使用相同字段 tag、类型、JSON 名称和字段顺序；`BITPACK_TLV` 要求固定 schema。`VARINT_TLV` 的集合布局属于该协议，不能直接用 `.proto` 生成类替代。Rust 的 `FAST_FIXED` 仍不支持编解码，不能用于 Java/Rust 互通；Java 单端使用该模式须明确消费者范围。没有 `protocol-bytes` codec 声明的消息不能进入 ProtocolBytes 目标类型，即使内容恰好是合法 JSON。

`KafkaHeaders` 保留跨名称顺序、重复项、null 与零长字节。`key(String)` 使用 UTF-8；`keyBytes` 与所有消费路线的 `KafkaRecord.rawKey()` 保留二进制 key。`KafkaRecord.key()` 只提供合法 UTF-8 的便利视图，非 UTF-8 时为 null，不因此把消息转入 DLT。两个 key setter 不可混用。

| 字段 | 语义 |
|---|---|
| X-Nasa-Event | 事件路由，缺失才使用 DEFAULT |
| X-Nasa-Payload-Codec / X-Nasa-Payload-Mode | 二进制编码与模式声明 |
| X-Nasa-Passthrough | JSON 透传上下文 |
| X-Nasa-Delivery-Attempt | owner 覆盖生成的会话内实际投递次数 |
| X-Nasa-DLT-Origin-Topic / Partition / Offset | DLT 的真实来源 |
| X-Nasa-Dlt-Origin-Group | DLT 来源消费组 |
| X-Nasa-DLT-Origin-Source / X-Nasa-DLT-Origin-Cluster | 实际消费源别名 / broker 查询得到的物理集群身份 |
| X-Nasa-DLT-Reason / X-Nasa-DLT-Delivery-Attempts | 安全原因及实际尝试次数 |

`X-Nasa-Event`、`X-Nasa-Payload-Codec`、`X-Nasa-Payload-Mode` 按精确大小写识别，默认拒绝重复、null 和非法 UTF-8。`behavior.strict-headers=false` 只允许重复声明取最后值，仍校验选中值；对应 Rust `behavior.strict_headers`。事件缺失才使用 DEFAULT，空串与纯空白事件属于无效记录。

codec 声明仅接受 `protocol-bytes`，即使事件没有匹配路由也先校验声明。JSON 路由不接受 codec 或 mode；二进制路由要求 codec，mode 缺失时使用类型声明，存在时必须匹配。raw 路由保留 payload 原文，不按 mode 解码；透传上下文和 trace 的容错规则独立于这三个路由头。

DLT 在保留业务头顺序的同时去除伪造证据字段，再写入可信来源；原因按 UTF-8 边界限制为 1024 字节。

普通 DLT 每次发送前查询来源物理集群和目标有效配置，事务 `deadLetter` 在本批任何输出发送前验证全部死信目标。目标必须预先创建，`cleanup.policy` 必须仅为 `delete`，`retention.ms` 和 `retention.bytes` 必须为正或 `-1`；拒绝 `compact`、`compact,delete`、立即删除、缺失或不可读取的配置。所用身份需要目标的 Describe/DescribeConfigs 权限。查询失败不会被解释为允许自动建 topic，默认保留输入等待重投；显式可用性优先策略仍允许丢弃。普通死信查询与交付不阻塞消费 owner 的 poll，关闭等待它们实际完成。

该门禁是发送前的配置快照，不能阻止管理员随后更改策略，也不保证无限历史。时间保留按 broker 的实际时间戳策略计算，保留原始时间的旧输入可能已接近清理期限；容量保留也会淘汰旧 segment。运维应按排障和回投窗口配置保留时间/容量并控制变更权限。框架保留原 key、null payload、零长字节与业务头，既不把 tombstone 包装为其它载荷，也不改写目标配置。

`behavior.tombstone-policy` 专门控制匹配到 typed 路由的 Kafka null value，默认 `skip`；可选
`deliver`、`dead-letter`、`halt`，分别表示交付业务、确认 DLT 后推进、保留位点并暂停分区。
`deliver` 要求 listener 的 `nullable()` 为 true；否则按无效载荷处置。JSON `null` 始终由
`nullable()` 决定是否可接纳，零长字节仍按 codec 解码，三者不能混用。
`record.context().tombstone()` 保留原始 null 身份；批量处理应读取每条记录上下文，调用级上下文只代表首条。
raw listener 仍收到原始可空字节，不受 typed tombstone 策略影响；缺少路由和非法路由头仍由各自策略先裁决。
删除标记没有载荷编码，因此不执行目标 codec/mode 匹配；保留头唯一性、UTF-8 和已知 codec 名称的基础检查仍生效。

## 同步只读透传

`KafkaPassthroughListener.onRecord(RawKafkaRecord)` 提供原始 key、payload 和有序重复头的只读视图，不构造 DTO 或复制原文字节。头描述符和视图对象仍有分配；不承诺端到端零复制。

视图只能在当前同步回调的当前线程读取，跨线程或回调结束后访问返回 `LIFECYCLE`；结束时释放底层记录引用，保留子字节视图也不能延长原文持有。`Bytes.get/length` 用于有界解析，`copyTo` 写入业务已取得额度的缓冲，`copy` 返回业务独立持有的副本。副本的后续保存、异步发送与容量由业务负责。null 与空 key/value/头值保持不同。

回调只允许有界解析和非阻塞本地准入，不等待网络、锁或 Registry。Java 在有界 worker 执行，超时和停止会撤销确认权，但不能强制终止用户代码；实际退出前继续占用名额。Rust 对应 `PassthroughConsumer` 在 owner 栈内执行，使用编译期借用约束；两端的内存与执行模型分别成立。

`PassthroughDisposition.Handled` 表示全部目标已进入本地有界队列；`BestEffortDropped` 表示业务显式接受部分丢弃；两者在 Manual 模式仍须 `record.ack()`。`Skipped` 只能表达无本地目标、目标节点不符、回环或来源过期，安全跳过无需再 Manual Ack。`PassthroughFailure` 按原因唯一映射为普通重试、无效输入策略、可靠 DLT、分区硬暂停或会话 CRASHED；返回 null 不构成成功。

本地入队不等于远端客户端收到。fan-out 部分入队后失败会重投，目的端应使用业务稳定身份识别重复。此入口只支持单条、同步、分区有序，不接受异步或按键执行声明。通用异步消费使用 `KafkaRawListener` 或 typed record listener。

## 广播身份与位点接续

listener 声明 `broadcastScope()`，并可声明 `instanceId()`；注解对应 `broadcastScope`、`instanceId`。
作用域与显式 `group` 互斥，实例身份必须与作用域一起使用。身份优先级为 listener、源的
`behavior.broadcast-instance-id`、系统属性 `nasa.kafka.broadcast-instance-id`、环境变量
`NASA_KAFKA_BROADCAST_INSTANCE_ID`、本 JVM 临时身份。稳定 ID 支持重启接续同一组已提交位点；
临时 ID 重启后改变，起点由 consumer 的 `auto-offset-reset` 决定，不能据此承诺离线消息接续。

`KafkaProxy.broadcastGroup(scope, instanceId)` 与 Rust `broadcast_group` 使用相同算法：
两部分保留精确 UTF-8，不裁剪、不替换、不做 Unicode 归一化；各自要求 1–4096 字节，不含 Unicode
控制字符或首尾 ASCII 空格。短组名为 `nasa-kafka-broadcast-b<scope小写hex>.<instance小写hex>`；
超过 255 ASCII 字节时改为 `nasa-kafka-broadcast-h<sha256小写hex>`。摘要输入依次为 UTF-8
`nasa.kafka.broadcast`、一个零字节、scope 的四字节大端长度及原文、instance 的四字节大端长度及原文。
不同标点不会因字符替换产生同名；长身份的区分依赖 SHA-256 的碰撞抗性。

不同实例 ID 各收一份，同一稳定 ID 在两个活动进程中会形成组内竞争，不会各收一份。发送启动广播前
必须用派生组名等待 `awaitGroupReady`；Ready 只代表当前观测，离线期间缓存一致性仍应依靠重建或快照。

单参数 `broadcastGroup(scope)` 和未声明 `broadcastScope()` 的既有广播 listener 保留原算法。
迁移时可先显式 `group()` 固定旧组；选择新作用域前，应停止旧消费者并决定保留旧组、迁移提交位置，
或明确接受按新组起点重读。框架不会静默切换组名或复制位点。

## 控制、管理与关闭

`assign` 支持固定分区的单条和批处理器；起点可选 earliest、latest、timestamp、显式 offset 和 committed。显式 offset 越过保留范围时拒绝，timestamp 无匹配时取末尾；这些操作不提供跨进程固定分区排他租约。

`replaceSubscriptions` 替换集合，`addSubscriptions` / `removeSubscriptions` 明确整批增删，`clearSubscriptions` 清空集合并保留 owner，`stop(group)` 停止并注销。
`subscribeGroup` / `unsubscribeGroup` 保留增删语义，集合重载在完整校验后以每owner一次命令应用；旧集合重载的null或空集合仍为空操作。
明确增删入口拒绝空、重复、null或空白topic；替换允许空集合。所有入口只改变拉取范围，不扩充已冻结的handler路由，新topic遵循unmatched policy。
有未收口输入时拒绝变更；原生接受新声明后撤销旧Ready，重新分配需要再次等待。相同集合不撤销已有Ready。
单owner内整批应用，多owner或多group不提供跨客户端事务；部分槽失败可能结果未知，应读取实际assignment并在责任排干后用完整集合收敛。
固定assign不允许修改订阅，旧 assignment handle 只能停止自身实例。

seek、订阅变更、pause/resume 经有界 owner 命令执行；仍有消费责任的分区拒绝定位。队列未执行与已开始但超时使用不同错误分类。解除用户 pause 不会解除框架的失败或容量保护。

`controlPartitions(group, actions)` 为 pause、resume 和各类 seek 提供逐分区 `ControlReport`，保留数据源、组和每个请求目标的独立结局：

| 结局 | 已知事实与后续动作 |
| --- | --- |
| `APPLIED` | owner 已确认该目标完成；后续 rebalance 仍可能改变状态 |
| `NOT_APPLIED` | 未派发，或在副作用前拒绝/取消；解除原因后可重新提交 |
| `UNKNOWN` | 已可能产生副作用，须查询位置或暂停状态后决定，不能盲目重发 |
| `OWNER_CHANGED` | 缺少唯一归属或冻结代次已改变，旧意图未执行 |

```java
ControlReport report = orders.controlPartitions("order-worker", Map.of(
    new TopicPartition("orders", 0), new PartitionControl(PartitionControl.Kind.PAUSE),
    new TopicPartition("orders", 1), new PartitionControl(PartitionControl.Kind.SEEK, 100)));
```

请求必须显式指定非空目标。先验证全部声明和归属再派发；发现缺席或重复 owner 时整批不执行。准入等待和执行共用 `control-timeout-ms`，已确认成功的目标不会被后续超时覆盖。报告不保证跨分区原子性、执行顺序、回滚或自动重路由；出现未知结果后不再开启本次后继操作。报告返回时封存，晚到结果不改写原报告，也不能继续派发尚未开始的目标。显式 committed 定位在没有提交历史时拒绝；成功定位只清理目标分区责任，保留其它分区的完成证据。

`proxy.admin()` 和已授权的 `handle.administration()` 提供 cluster ID、topic 描述、增量配置修改与结构化创建结果；`handle.admin()` 只允许查询。超时不等于管理修改未发生。`groupHealth()` 和 `producerStats()` 提供不可变快照。

`createTopicIfAbsent(topic, partitions, replicas, settings, timeout)` 返回 `CREATED` 或 `ALREADY_EXISTS`；已存在不会重写配置，也不代表规格已经匹配。创建、删除、扩分区和增量配置只凭目标资源的明确确认完成。权限拒绝为 `AUTHORIZATION`，提交后的超时、中断或缺少可信响应为 `OUTCOME_UNKNOWN`，应回读资源后决定后续动作。`KafkaProxy` 旧同步入口保留 `IllegalStateException` 外层和稳定原因链；旧 `deleteTopic` 对不存在保持幂等，新 `KafkaAdmin.deleteTopic` 明确报告 `ADMIN`。

`alterTopicConfig` 可一次设置多项，值为 `null` 的键恢复 broker 默认值，未声明项保持原样。`describeTopic(topic, false, timeout)` 只读元数据，不要求 DescribeConfigs；`topicExists` 仅将明确的目标不存在转为 `false`，列表不可见、权限拒绝和响应缺项不能代替不存在。`committedOffsets` 区分无历史的 `null` 与有效零位点。描述加配置、多分区位置及 `committedLag` 的组合步骤共用总预算；lag 是分次读取的观测，并非原子快照。公开 `Duration` 必须为正且不超过 `Integer.MAX_VALUE` 毫秒。

最终关闭会封闭全部 Admin 和临时分区查询的准入，并等待原生连接与关闭线程实际退出。关闭等待超时或中断保留责任，后续 `shutdownUntil` 继续收口；持有旧 Admin 门面不能重新开放已关闭源。`partitionsFor` 保留原生副本与离线副本信息，其短连消费者同样属于源的关闭责任。

`observeOffsets(group, partitions, isolation, timestampMillis, timeout)` 为最多 10000 个明确且唯一的分区提供 `OffsetObservation`。`READ_COMMITTED` 的末尾是实际 LSO，`READ_UNCOMMITTED` 是普通日志末尾；未完成事务可能使两者不同。报告包含 source、可选 group、读取隔离、起止时刻及每个字段各自的观测时刻，不是跨分区或跨字段的原子快照。保留起点、末尾、提交位置、时间定位和 lag 分别保存有效非负值或缺失原因；有效零与无历史不同。

缺失原因区分 `NOT_REQUESTED`、`NO_COMMITTED_OFFSET`、`NO_TIMESTAMP_MATCH`、`RESOURCE_MISSING`、`AUTHORIZATION`、`TIMEOUT`、`INTERRUPTED`、`UNAVAILABLE` 和 `INCONSISTENT`。group 或时间参数为 `null` 表示不请求该维度；未请求 group 时也不计算 lag。lag 为 `max(0, end - committed)` 的 offset 距离，包含日志空洞、事务控制记录及可能已过期的前缀，不等于业务消息数或本地未完成数。全部查询共用总期限，保留已经取得的部分证据；返回后报告封存，原生请求的实际退出仍由 source 关闭追踪。该查询不加入消费组、不提交、不创建 topic，也不更改配置。

`verifyTopic(new KafkaAdmin.TopicContract(topic, partitions, replicationFactor, settings), timeout)` 只读核对分区集合、副本数和要求的配置子集；空配置不要求 DescribeConfigs。匹配时返回实际描述，漂移为 `TOPIC_CONTRACT`，权限和查询失败保留原分类。此入口不会创建或改写 topic，后续资源仍可能发生变化，应由部署权限和运行期保护共同约束。

`before()` 只表示本地启动，应用应使用 `awaitGroupReady` 或 `KafkaClients.awaitReady` 等待 broker 身份和分配。Ready 是当时的观测，rebalance 后可能失效。

`ReadyRequirement` 支持 `JOINED`、`assigned(count)`、`assignedTopics(topics)` 和 `assignedPartitions(partitions)`。
具体分区要求不会由同 topic 的其它分区代替。`JOINED` 允许已加入的空闲成员；用户主动暂停不撤销分配就绪，
重试、背压、恢复、硬保护与停止状态均不满足 Ready。

`groupReadiness(group, requirement)` 返回 `ReadyStatus`，保存原要求、源与组健康、分配代次、观测时刻和稳定原因。
原因区分启动、加入中、失权、恢复、重试、提交阻塞、DLT/进度背压、降级、崩溃、停止，以及数量不足、缺少 topic 或具体分区。
`awaitGroupsReady(requirements, timeout)` 共用一个总预算，每轮复验全部绑定的 group 实例；只有同一轮全部满足才返回，
不会锁存较早成功的组。空要求和负预算拒绝，零预算只观察一次。期限耗尽抛出 `READY_TIMEOUT`，
`KafkaFailure.readiness()` 包含全部未满足组的结构化证据；保护和终止态返回 `LIFECYCLE`。
这仍是时点观测，返回后出现的 rebalance 不受该快照约束。

owner 对明确可重试的原生会话失败执行指数退避与抖动，默认初始 3000 ms、上限 60000 ms，300000 ms 滑动窗口内最多自动重建 10 次。认证、配置及其他非重试错误或预算耗尽进入 `CRASHED`，保留查询、停止和显式恢复入口。已接纳的真实调用退出前不会启动替代会话。

空 poll 本身不刷新 broker 可达证据。owner 结合原生响应计数与连接状态识别持续失联；固定分区全部暂停而没有 fetch 时，通过有界只读查询确认可达性，不移动消费位置。metadata 可用不等于组协调器或业务交付已恢复，预算耗尽后仍需显式恢复。停止优先于自动退避和后续重建。

`restartGroup(group)` 只接受已保护的异常任务，正常消费和仍有实际调用的任务拒绝重建。请求受理立即撤销旧 Ready；新会话须重新等待分配。固定分区恢复使用封存的未提交起点，首次 `latest` 或 `timestamp` 不会重新求值。恢复坐标超出 broker 保留范围时停止恢复，不自动改为新起点。重建可能重复尚未确认提交的业务，仍需幂等。

`consume-restart-delay-ms`、`consume-restart-max-delay-ms`、`consume-restart-budget-times` 和 `consume-restart-budget-window-ms` 可在 `behavior` 中覆盖。`decode-failure-halt-streak` 默认为 1000，零表示显式关闭连续解码失败保护。健康快照包含会话启动/重建次数、提交尝试/成功/失败次数、最近成功时刻，以及暂停原因；组快照汇总本实例所有槽，单槽快照仅包含该槽，不是跨进程持久账本。

关闭分两段：`stopConsumersUntil(deadline)` 收口消费，期间允许业务发送和 DLT；`shutdownUntil(deadline)` 继续关闭发布、回执与 Admin。deadline 使用 `System.nanoTime()` 绝对纳秒。超时保留未完成责任，`isClosed()` 只有资源真正退出才为 true，可以再次调用关闭。

`closeReport()` 提供无需建连的结构化责任快照；`shutdownReportUntil(deadline)` 按同一协议推进关闭，返回 `COMPLETED` 或 `INCOMPLETE`，超时和中断不撤销停止意图。需要原始关闭错误分类时仍使用 `shutdownUntil`，随后读取报告。业务 `KafkaHandle.closeReport()` 可在关闭后读取证据，没有活跃整源关闭权。

报告区分实际 owner/handler、保留输入、停止期间释放但未获提交确认的输入、lane 未决交付与累计终态未知交付、DLT 信封及真实调用、有限任务资源、Admin、短连查询和观测线程。未退出组名最多列出64项，完整数量单独保留。`resourcesClosed` 只有全部资源真正退出才为 true；其它字段是独立采样，不构成跨资源原子快照。Replay/Transfer/事务的业务终态及持久进度继续从原任务句柄查询。

`releasedUnverifiedRecords` 只记停止期间未获提交确认的释放观察，不是跨 epoch 去重的事件数；它和仍存活的其它责任不能简单相加。清理本地输入并不证明 broker 上的具体记录仍存在，保留区间也不能证明压缩记录存活或同名 topic 没有重建。因此普通关闭的 `returnedForRedelivery` 保持零，来源是否可重读需独立核实；不能把这些输入当作业务成功。lane 的 `terminalDeliveryUnknown` 在实际资源关闭和重复等待后保留，调用方等待超时但后来收到成功回执不计入该累计量。

`requestConsumerStop()` 只同步发布不可撤销的整源消费停止意图，不等待资源退出；上述两个等待入口也先执行这一门禁，因此预算已经耗尽仍会停止新派发并拒绝迟到确认。它不同于可取消的单组控制命令，不可用作运行期暂停。

Spring 的 `KafkaClients` 将同次容器关闭事件与 Bean 销毁回调绑定到一个截止时间，所有命名源共用该预算。最终清理返回后，即使存在未退出任务，旧 proxy、Admin 和 lane 也不能接纳新业务；已接纳的内置交付仍保留原额度与收尾依赖。超时不会释放静态源名或把 `isClosed()` 改为 true。资源所有者可调用 `KafkaClients.shutdownUntil(deadline)` 显式继续等待；成功退出后才移除名称，等待不会重新开放业务入口。

停止关闭新消费派发，并立即撤销在途业务的 Auto/Manual 确认权；停止前临时 `ack()` 但尚未纳入 owner 安全水位的调用也不能凭迟到成功推进提交。owner 等待实际业务执行退出，再将未提交输入交回后续消费；外部副作用无法撤销，业务仍需幂等。停止前已形成的安全前缀以及已发送且最终获得 broker 确认的 DLT 可以收口提交，停止后不再开启新的业务或 DLT 重试。

DLT 并发额度随实际原生交付存活，发布等待超时不授权另一轮并发发送。rebalance 撤销旧 DLT 对来源提交的权威，但已发出的请求继续占用原额度；源停止必须等这些旧分配任务实际退出，不能仅凭当前 assignment 或待完成表为空报告成功。

### 配置迁移与滚动部署

`KafkaMigration.inspectLegacy(sourceId, flatConfig, routes)` 只读生成 `nasa.kafka.sources` 候选、完整路由身份、默认值差异和人工决策项。输入是扁平单源配置及调用方解析的 `Route(topic, event, group)`；检查器不扫描应用，也不连接 broker、创建 topic 或调整 group 位点。未知字段、同名字段的重复拼写和无效身份会被拒绝。

```java
var report = KafkaMigration.inspectLegacy("orders", Map.of(
    "bootstrap-servers", "localhost:9092",
    "group-id", "order-handler",
    "max-inflight-requests-per-connection", 1
), List.of(new KafkaMigration.Route("orders", "DEFAULT", "order-handler")));
// 先核对 locallyValid、全部 findings 和路由目录，再将候选交给部署配置。
```

候选保留扁平格式的 `acks=1`、幂等关闭和五个请求在途；当前默认分别为 `all`、开启和一个请求在途。原默认组合含重试，不能通过当前非幂等顺序约束，报告会标明 `CONFIG_INVALID`；业务须显式选择串行重试或幂等配置。检查器不会为了通过校验而擅自改变可靠性。凭据、证书路径和全部原生 properties 值以 `<redacted>` 替代，`SECRETS_REDACTED` 要求部署环境重新绑定；`locallyValid` 校验的是脱敏前候选，不证明滚动兼容或业务等价。源构造诊断记录实际可靠性、严格头策略与重试语义，不输出连接凭据。

迁移时必须同时确认以下行为：

- 省略 event 的 `partition` 使用 `DEFAULT`；沿用 topic 事件须调用 `partitionLegacy` 或显式传入 event。
- `max-consume-attempts=0` 表示无限重试；严格保留头默认拒绝重复路由头，兼容读取须显式设置 `strict-headers=false`。无匹配路由的 Skip 可推进安全提交。
- 所有消费组成员应先支持新增 event，再允许发布该事件。广播沿用旧 group 才能沿用位点；切换稳定 scope 不自动复制提交位置。
- 已编译调用方使用 `getContext/getProducer/getAdminClient/getPollTasks/getListenerRegistry` 或旧 `NasaKafkaConfig` 工厂方法时必须重编译并迁移到宿主注入、lane、Admin、健康/控制和统一装配。业务注入 `KafkaHandle`；默认管理只读，写入使用显式授权的 `administration()`。

回退前先停止新能力并排干实际任务，保留 checkpoint 与数据库状态。没有 Replay/Transfer/checkpoint 能力的旧二进制无法接管这些状态；消息线路可读不等于持久任务可回退。应使用理解相同持久格式与计划指纹的实现恢复，或由业务明确制定重放和去重方案；不要删除进度来伪造回退完成。

## Schema Registry

`ConfluentEnvelope` 与 `ConfluentSchemaRegistry` 提供独立的 Confluent-compatible schema 访问能力，对应 Rust `nasa::kafka` 的同名能力。它们不生成 Avro、Protobuf 或 JSON Schema codec，也不把普通 JSON 发布自动改为 schema envelope。

查询接受不声明引用或 `references: []` 的定义；非空引用和非法引用字段返回 `INVALID_RESPONSE`，不隐式下载依赖。subject 命名、兼容级别、Protobuf message-indexes、schema GUID 和引用解析由业务明确选择，本客户端不把这些格式当作普通四字节 ID envelope 自动处理。

```java
Set<Integer> approved = Set.of(42);
ConfluentEnvelope envelope = ConfluentEnvelope.decode(wireBytes, 1024 * 1024, approved);
try (ConfluentSchemaRegistry registry = new ConfluentSchemaRegistry(
        ConfluentRegistryOptions.defaults("https://registry.example.com"))) {
    SchemaRegistryClient.RegisteredSchema schema = registry.schemaById(envelope.schemaId()).join();
    // 使用业务选定的 codec 与已批准 schema 解码 envelope.payload()。
}
```

白名单在访问 Registry 前执行，消息里的 ID 不能授权任意 schema 访问。envelope 使用 magic byte 0、四字节大端正 ID，Java 载荷保留独立副本。ID 只在所属 Registry 内有意义，跨集群搬运须核对 Registry 身份与 ID 映射。

默认请求预算 3 秒、响应上限 1 MiB、缓存 256 项、成功 TTL 300 秒、404 负缓存 TTL 5 秒；响应硬上限为 16 MiB。正负缓存共用 LRU 容量，其他远端错误不写入负缓存。同 ID 的冷查询合并请求，失败轮次结束后等待者重新竞争；不同 ID 和控制请求也共享 HTTP 并发额度，超限返回 `CAPACITY`。客户端不扫描 subject，也不进行后台刷新。

`isCompatible` 是显式只读控制请求；`register` 默认拒绝，只有 `autoRegister=true` 才开放。注册超时、取消或连接失败不证明远端没有创建修订。客户端关闭后拒绝新调用，已送出的远端写入不能由本地关闭撤销。

取消某个调用的等待不取消共享查询，也不释放仍在执行的注册请求额度；请求由客户端持有，直到 HTTP 完成、总预算耗尽或客户端关闭。`metricsSnapshot().inFlight()` 可以观测这部分责任。同 ID 的其它等待者继续复用原请求，已取消等待只记一次 `CANCELLED`，迟到结果不再改记成功。

`maxPendingLookups` 另行限制包含同 ID 等待者的查询调用总数，默认 4096，范围 1–65536；超额同样返回 `CAPACITY`，`pendingLookups()` 读取当前占用。八参数配置构造器保留默认上限，需要调整时使用九参数构造器。取消等待立即从共享轮次的等待集合移除；等待者唤醒派发也受同一数量上限约束，不把取消历史无限附着在原请求上。

Registry endpoint 来自受信配置，禁止重定向及 URL 内嵌凭据；除 localhost 外必须使用 HTTPS。Basic/Bearer 认证与 Kafka SASL 相互独立。错误和配置诊断不输出 endpoint、subject、schema 或认证正文。`metricsSnapshot()` 将数据面查询与控制面操作分开，正命中率使用 `CACHE_HIT / (CACHE_HIT + FETCHED)`，负缓存、失败和取消不进入该分母。

私有 CA 与 mTLS 通过 `new ConfluentSchemaRegistry(options, new SchemaRegistryTls(caPem, identityPem))` 配置，两个材料均可为 `null`。`caPem` 是 PEM 证书集合，声明时替代 JVM 默认信任锚；未声明时使用 JVM 默认根。`identityPem` 包含客户端证书链和一个未加密 PKCS#8 私钥。每份材料最多 1 MiB、证书最多十六个，构造时复制输入；格式错误、重复私钥或证书与私钥不匹配在连接前返回 `INVALID_CONFIGURATION`。此入口只接受 HTTPS，保持证书链及主机名验证，支持 TLS 1.2/1.3。TLS 失败返回脱敏 `TRANSPORT`，401/403 返回 `REMOTE_STATUS`，均不作为 schema 不存在而缓存。TLS 材料不继承 Kafka SASL/SSL 配置。

## Broker 安全与构建能力

每个 source 冻结自己的协议、凭据和信任材料，producer、consumer、Admin 和命名 lane 使用同一认证域。
使用 `security.protocol`、`security.sasl-mechanism`、`security.sasl-jaas-config` 以及
`security.ssl-truststore-location/type/password`、`security.ssl-keystore-location/type/password`、
`security.ssl-key-password` 配置。扁平同名字段仍可用于单源声明；原生认证属性只允许放在源级 `properties`，
不能放在角色或 lane 的 properties 中改变 principal。认证域独立于 Schema Registry 与 checkpoint 数据库。

协议精确使用 `PLAINTEXT`、`SSL`、`SASL_PLAINTEXT`、`SASL_SSL`；SASL 机制精确使用
`PLAIN`、`SCRAM-SHA-256`、`SCRAM-SHA-512` 或 `GSSAPI`。PLAIN 必须通过可信链路传输；
`SASL_PLAINTEXT` 不提供链路加密。SSL 默认验证证书链和主机名；mTLS 还需客户端证书及私钥。
GSSAPI 依赖 JVM 的 GSS 实现、Kerberos principal、票据或 keytab、服务名和正确的 DNS/时钟；
算法存在不表示这些环境已经就绪。OAuth 和任意 Java callback 的跨语言等价实现不在共同能力中。

`KafkaCapabilities.supportedCapabilities()` 探测本进程实现；`proxy.capabilities()` 分别返回
`supported`、`configured`、`laneCompression` 与普通消费组的实时 `readiness`。前两项没有远端副作用，
空 readiness 表示尚无普通组证据；只有有效 join 才能形成 Joined 就绪，不能由 TLS 配置或成功构造推导 Ready。
事务的状态使用各自 TransactionHandle 查询。快照不返回凭据、路径或原生连接句柄。

压缩精确使用 `none/gzip/snappy/lz4/zstd`，通过 `producer.compression` 或 lane 的 `compression`（扁平配置为 `compression-type`）
配置，禁止 raw `compression.type/codec` 绕过声明。所有声明 lane 在启动前检查，即使尚未首次发布；
缺少可选压缩依赖或原生库时返回 `UNSUPPORTED_CAPABILITY`，不替换算法。Java 运行时需要实际可加载的
Kafka codec 依赖；Rust 对应 `tls`、`gssapi`、`zstd` feature 及真实 librdkafka 构建能力。

明确的 TLS、SASL、topic/group 授权失败会使普通消费组进入 CRASHED 并停止自动会话重建；
恢复权限后显式重建组，变更凭据或信任材料则关闭旧 source、等待实际退出、重新创建并验证 Ready。
配置对象修改不会热替换已冻结的连接。原生客户端构造失败返回脱敏 CONFIG，不暴露原生原因链。
底层只报告断连时不能断言认证失败，瞬态恢复仍受重建预算限制；Rust mTLS 对端拒绝证书可能属于此情况。

| 使用角色 | Kafka 资源与最小操作 |
| --- | --- |
| 普通 producer | 目标 topic 的 Write、Describe；确认交付不要求 topic 管理权限 |
| 普通消费组 | 来源 topic 的 Read、Describe，最终 group 的 Read；查询组位点还涉及 Describe |
| 普通 DLT writer | DLT topic 的 Write、Describe、DescribeConfigs，配合来源消费权限；缺少配置证据不发送、不推进来源 |
| 有限 Replay | 来源 topic 的 Read、Describe；直接读取分区，不加入消费组、不提交组位点 |
| Transfer / DLT 回投 | 来源 topic 的 Read、Describe；目标及隔离 topic 的 Write、Describe；外部代次目录另行授权 |
| 只读诊断 | 被查询 topic/group 的 Describe；配置查询另需 DescribeConfigs；这些权限不授予写入或管理 |
| Topic 管理 | 建 topic 为 Create，删除为 Delete，增分区为 Alter，改配置为 AlterConfigs；逐次由 broker 授权 |
| 同集群事务 | 输入 topic 的 Read、输出/DLT topic 的 Write、对应 Describe、group 的 Read、派生 transactional.id 的 Write；事务 DLT 另需 DescribeConfigs |

表中针对 Kafka 4.1.0 的操作合同；集群级诊断或其它 broker 的授权策略须按部署单独核对。
普通业务不会为了 Ready 自动建 topic、改 ACL/配置或关闭主机名校验。`handle.admin()` 是只读诊断入口；
资源所有者的 `proxy.admin()` 与显式授权的 `handle.administration()` 可执行管理写入，不应交给仅需发布的模块。
是否具备某项权限最终由该次 broker 操作决定，认证成功不能证明全部权限。

发布 builder 与 `admin()` 将明确的 broker ACL 拒绝保留为 `KafkaFailure.Code.AUTHORIZATION`；
Replay、Transfer、DLT 回投分别使用 `ReplayException.Code.AUTHORIZATION`、`TransferException.Code.AUTHORIZATION`、
`DltReplayException.Code.AUTHORIZATION`，事务使用 `TransactionException.Code.AUTHORIZATION`。
`KafkaProxy` 的同步管理便捷方法以 `IllegalStateException` 保留原因链。
转发的来源读取失败位于 `status.replay().failure()`，目标失败位于外层 `failure()`。拒绝的新工作不会推进 checkpoint 或输入提交，
已确认前缀仍可保留。恢复授权后等待旧任务实际退出，再以原计划和持久进度启动新任务；永久保护的 producer 需关闭旧 source 后重建。
撤销 ACL 不会撤回客户端已经取得的输入或取消在途副作用，权限边界以 broker 对具体请求的校验为准。

共同运行范围为 Kafka 4.1.0 KRaft、Java client 3.8.1/JDK 21 与 librdkafka 2.12.1/rdkafka 0.39，
classic group 协议，macOS arm64 应用与 Linux arm64 broker。其它 broker、平台和新 group 协议
不由此自动获得等价性承诺，GSSAPI 的本地实现支持也不等于完成 Kerberos 登录。

## 有限回放计划

`proxy.admin().replayObservation(topics, timestampMillis, timeout)` 只读查询所选 topic 的完整分区集合、真实 cluster ID、当前保留起点和 `read_committed` 的 Last Stable Offset。所有 metadata 与 offset 查询共用一个正预算，最大五分钟；不加入消费组、不创建缺失 topic，也不提交线上位点。`timestampMillis=null` 不查询时间；时间无匹配时保留明确的空结果。

`ReplayPlan.earliest`、`offsets`、`timestamp` 分别按保留起点、完整逐分区起点和时间观测冻结 `[start,end)`。时间查询无结果由 `MissingTimestamp.FREEZE_END` 或 `REJECT` 决定，晚于本轮 LSO 的结果截断到冻结 end。后续写入和事务完成不会移动已有计划边界。最多 4096 个分区，任务、source 和物理身份采用有界 ASCII；`ReplayLimits` 将条数速率、字节速率和实际在途容量分别纳入合同，速率零表示不节流。

```java
ReplayObservation observation = proxy.admin().replayObservation(Set.of("orders"), null, Duration.ofSeconds(10));
ReplayPlan plan = ReplayPlan.earliest("orders-rebuild", proxy.sourceId(), observation,
        currentTopicGenerations, pipelineFingerprint, ReplayLimits.defaults());
byte[] frozenPlan = plan.encode();
ReplayPlan restored = ReplayPlan.decode(frozenPlan);
ReplayCheckpoint initial = restored.initialCheckpoint();
```

`topicGenerations` 必须由能识别删除重建的外部资源目录重新查询，不能填 topic 名或分区数。Rust 安全客户端不提供 topic UUID，因此双端恢复依赖这一显式外部证明；调用方负责它与实际 Kafka 资源的一致性。`pipelineFingerprint` 是 schema、转换、过滤、目的路由和成功条件的 SHA-256，改变处理语义必须更换。完整计划采用共同 `NSKRPL01` 编码和完整性摘要，最大 8 MiB；摘要不承担身份认证，持久目录仍需控制写权限。

恢复前调用 `validateResume(currentObservation, currentTopicGenerations, checkpoint)`。物理身份或代次变化、分区扩容/缺失、保留期越过待处理位置、末尾回退及处理合同变化分别拒绝，不静默重置 earliest 或重新计算 latest。`TOPIC_MISSING` 与 `OBSERVATION_FAILED` 区分确定缺失和证据不可观测，来源最终关闭后为 `CLOSED`。规划和验证入口不执行 handler；显式执行使用下面的受管入口。压缩日志只能提供仍保留的记录，offset 空洞不能证明每次历史业务变更都可恢复。

## 受管有限回放

`proxy.replay(plan, store, identityProvider, processor)` 用冻结计划执行历史补数或索引重建，返回 `ReplayHandle`。专属 consumer 按分区串行读取原文，不加入消费组、不提交线上 offset；`ReplayRecord` 保留二进制 key、tombstone、时间和有序重复 header，并携带物理来源坐标。不同分区可并行，后续新增消息不会扩大原范围。

```java
ReplayHandle handle = proxy.replay(restored, store, identityProvider, processor);
ReplayStatus snapshot = handle.status();
handle.pause();
handle.resume();
handle.cancel();
boolean drained = handle.awaitDrained(Duration.ofSeconds(10));
```

`ReplayProcessor.fingerprint()` 必须与计划一致。`process(record)` 是同步成功合同：只有本条全部实际副作用成功后才能正常返回，不能启动后台工作后提前返回。框架随后合并各分区进度、条件持久化 checkpoint，再发布成功进度。处理失败停止整个任务；持久写入失败保护后续写入并分别公开执行与存储错误类别。使用相同计划再次执行可从最后确认位置恢复，目标成功而 checkpoint 未确认时可能重复，目的端应使用稳定事件 ID 或来源坐标幂等处理。

执行前及每条实际处理准入后重新查询来源和外部代次；元数据与目录查询每轮共用十秒预算。`ReplayIdentityProvider` 必须返回当前完整证据，遵守传入预算；阻塞的扩展不能靠取消等待来释放实际责任。该查询用于发现已可观测变化，不提供与目标写入跨资源原子化的保证。

保留期清理或显式日志截断使当前 beginning 越过 checkpoint 时返回 `DATA_GAP`，保留最后确认进度，不跳到剩余日志继续发送。已经准入的实际处理仍可完成并持久推进；后续准入重新检查。压缩产生的数值空洞由原生消费位置跨越，范围内部或末尾没有存活记录也可完成，前提是来源身份、保留边界和已交付记录的成功条件均成立。Transfer 使用相同来源门禁，只搬运当前仍存活的记录；它不能补回已压缩的事件或已过期的 tombstone。

每源最多十六个活动任务、六十四个分区 worker，每个任务另有一个协调线程。实际并行数为声明 concurrency 与分区数的较小值，每个 worker 至多驻留一条记录；每条额度不超过源 `max-record-bytes` 与任务字节额度除以 worker 数的较小值。计费包括 key、正文、UTF-8 topic/header 和固定元数据；不足返回 `CAPACITY`。条数及字节速率在全任务处理准入处共享，最多一条初始突发。原生 fetch 缓冲、压缩批解压与业务自行复制不属于该驻留字节计数，因此它不是进程内存硬上限。

暂停先关闭新处理，`PAUSING` 表示仍有已接纳责任，`PAUSED` 表示当前没有处理在途；驻留记录可保留。取消进入 `STOPPING`，不能撤销已开始的远端副作用。只有 worker、consumer 和 lease 实际退出后才进入 `COMPLETED`、`CANCELLED` 或 `FAILED`；等待超时不释放同名任务和独占权。来源停机先通知并排干回放，再关闭 producer/Admin；超时保留责任，后续停机调用继续等待。

## 可恢复的单目标转发

`TransferPlan` 将有限回放范围绑定到明确的目标源、物理 cluster ID、topic 代次、完整目标分区集合、producer lane 和逐来源分区映射。适用于历史搬运、归档和跨集群补数；目标确认后才持久推进来源 checkpoint。`freeze` 保留来源范围，使用内置转发成功合同替换原 handler 指纹；旧 handler 的 checkpoint 不能直接当作转发进度。`encode/decode` 使用共同规范格式，计划最大 16 MiB，可与 Rust 交换。

```java
TransferPlan.Target target = TransferPlan.Target.observed(destination.sourceId(), "archive",
        targetObservation, targetTopicGenerations);
TransferPlan transfer = TransferPlan.freeze(sourceRanges, target, approvedPartitionRoutes,
        TransferPlan.PayloadPolicy.opaque(), 4);
TransferHandle task = source.transfer(transfer, destination, checkpointStore,
        sourceIdentityProvider, targetIdentityProvider, null);
TransferStatus status = task.status();
```

目标 lane 必须提供 `acks=all`（或 `-1`）。框架先登记目标依赖，再开放来源任务；任一端停止都会关闭后续转发准入，已入 native queue 的记录继续等待实际交付终态。业务等待超时、暂停或取消不能提前释放双方责任或来源 lease。每个目标最多十六项依赖；来源与普通 Replay 共用任务和 worker 容量。`pause/resume/cancel/awaitDrained` 的实际责任语义与 Replay 一致。`TransferStatus.replay` 报告持久进度，`deliveredRecords` 报告本实例目标成功回执数，后者可能领先于进度；`failure` 单独呈现目标身份、schema、容量、路径或交付失败。

原文模式保留二进制 key、tombstone、空值、有序重复 header 和固定分区映射。每个来源分区串行，多来源分区合流不提供全局顺序。框架保留源 Kafka 时间，但目标配置 `LogAppendTime` 时，目标存储时间可被 broker 改写。`x-nasa-transfer` 保存原始 cluster/topic/generation/partition/offset、原始时间及稳定 `eventId`，追加物理 topic 路径；`TransferTrace.read(headers)` 提供结构化读取。最大跳数为 1..16，消息头总计最多256项、64 KiB；大小写变体、重复或损坏的转发头拒绝，经过的同一物理 topic 不可再次成为目标。事件 ID 不随任务名或目标 offset 改变。摘要用于完整性和幂等识别，不认证写入者；可信性依赖 Kafka 写权限、受控资源目录和消息来源。

Confluent envelope 必须显式选择 `CONFLUENT_ID_MAP` 并提供 `TransferRegistries`：每个来源 ID 对应批准的目标 ID、schema 类型和 UTF-8 schema 正文 SHA-256。运行时查询两个 Registry，只有 ID、类型和正文摘要全部相符才替换四字节 ID，其余正文（包括 Protobuf message-indexes）保持不变；tombstone 不查询 Registry。不自动注册、演进或重新编码 schema。`OPAQUE` 不解释原文中的 Registry ID，业务需保证目的端按原文处理。Registry 客户端由宿主持有，关闭必须晚于实际转发排干。

每条发送前重新查询目标物理身份、外部代次和完整分区集合；删除重建、扩容或错连在可观测时拒绝，目标位点增长不改变合同。schema 与目标证据共用十秒验证预算。资源查询与发送不具有原子性，外部目录必须与 broker 真实资源保持一致；需要禁止变更窗口的业务应在运维控制面协调资源变更。目标已确认而 checkpoint 未确认时可重复，交付未知时不推进进度。当前合同为单目标 at-least-once；偶然同集群不会升级为事务，也不迁移消费组位点、ACL、topic 配置或提供自动灾备切换。共享进度可使用下述 MySQL 后端；DLT 重放和多目标交付账本不属于此入口。

## 有限 DLT 回投与原文隔离

`source.replayDlt(plan, destination, store, sourceIdentity, destinationIdentity, registries)` 返回 `DltReplayHandle`，支持 `pause/resume/cancel/status/awaitDrained`。计划复用冻结的 `ReplayPlan`、独占 checkpoint 和实际任务生命周期；不同主机可使用 MySQL 后端接续，不修改线上消费组位点。来源与目标停机都关闭下一条准入，并等待已经接纳的目标实际结果；超时保留任务及 lease。回投 lane 要求 `acks=all` 或 `-1` 的 ISR 确认合同，并受 broker 的 `min.insync.replicas` 约束；这不等于等待全部已配置副本。目标资源与 schema 复验沿用单目标 Transfer 合同。

`DltReplayPlan` 将原业务 topic/partition 映射、原消费组集合、目标物理身份及完整分区、schema 策略、无效输入策略和最大回投次数全部绑定到持久指纹。路由依据必须是计划明确批准的映射，消息头不能决定连接或任意目标。最多4096条路由、256个原组；空组集合表示全部组。计划中的读取分区属于实际DLT，路由来源分区属于其报告的原业务，两者无需相同。只有证据完整且未选中的输入才计为 skipped 并推进。

`DltEvidence` 严格读取六个规范DLT字段：原topic、partition、offset、group、reason和DLT delivery attempts。缺失部分字段、重复、ASCII大小写别名、null、非法UTF-8、非规范数字均拒绝。DLT attempts范围1..4294967295，当前 `X-Nasa-Delivery-Attempt` 可因再次消费而不同；不要求与原DLT次数相等。原group为1..1024字节UTF-8，reason可空且最多1024字节。完整消息头最多256项、64KiB，超额不会静默裁剪。

可选 `DltOrigin` 将 `X-Nasa-DLT-Origin-Source` 与 `X-Nasa-DLT-Origin-Cluster` 作为完整一对读取，各为1至1024个UTF-8字节且不含NUL；缺少一项、别名、重复或非法文本拒绝。新普通/事务DLT都写入这对字段。旧六字段记录仍可读，`origin()` 为 null；六参数构造入口保留。源别名不等于物理身份，集群/topic/partition/offset仍不能区分同名topic删除重建后的日志代次，字段本身也不认证写入者。

回投证据无此扩展时保持 `NSKDRH01` 原字节，有扩展时使用 `NSKDRH02` 并保存首次来源身份；再次失败与回投不覆盖这份首次证据。滚动升级时应先升级回投证据读取端，旧读取端无法解释 `NSKDRH02`。Rust `DltEvidence` 结构体字面量需提供 `origin: None` 或实际身份；Java 的旧构造方法和已有字段访问方法继续可用。

默认无效输入停止并保留失败坐标。显式 `InvalidPolicy.QUARANTINE` 将缺少证据、证据非法或达到回投上限的原文发送到固定隔离分区；该分区不得与任何业务回投目标分区相同。隔离保留原key、payload、时间和全部原始错误头，追加 `x-nasa-dlt-replay-failure`；`DltQuarantineTrace` 提供实际DLT坐标、事件ID和拒绝原因。已含隔离标记或追加后超出头预算时停止，避免覆盖历史证据。schema不匹配、目录不可观测、目标身份变化、发送失败或结果未知不会转为隔离成功；只有隔离真实确认才推进checkpoint。

正常回投保留原文、tombstone、空值、有序重复业务头和DLT来源字段，并写入 `x-nasa-dlt-replay`。`DltReplayTrace` 保存首次实际DLT物理资源和坐标、原DLT报告、最近批准目标和回投次数。首次ID来自实际DLT坐标；若存在合法Transfer来源，保留其原事件ID，将旧Transfer证据完整封装并移除活动 `x-nasa-transfer` 头。再次业务失败不会清空已有回投ID或计数，次数上限为1..16。允许显式回投原业务topic，但禁止向当前同集群被读取的DLT topic直接输出。普通Transfer的回环保护不因此改变。

这些摘要只检测格式与内容变化，不认证写入者；过滤、原始业务来源与计数依赖受控DLT写权限和可信资源目录。旧DLT不含原业务topic物理代次，框架不会伪造该身份。回投得到新的Kafka offset，不恢复旧日志位置。目标已确认而checkpoint未确认时允许可识别重复，目的端仍需幂等处理。`DltReplayStatus` 的 forwarded/skipped/quarantined 计数只属于本执行实例，持久进度仍以其 replay 字段为准。

## 持久任务进度

`ReplayCheckpoint`、`CheckpointStore` 和 `CheckpointLease` 提供独立的持久进度合同。每份快照包含稳定任务名、合同 SHA-256、所有权 `generation`、条件更新 `revision` 和最多 4096 个冻结分区范围。范围为 `[startOffset, endOffset)`，`nextOffset` 不得回退或越界；完成不要求日志中存在 `endOffset-1`。合同指纹必须绑定来源物理集群及 topic 代次、分区、schema/转换策略、目标和结束范围，不能只对 topic 名做摘要。

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

`claim` 先取得独占权，再持久化新代次；已有合同变化返回 `CONTRACT_MISMATCH`，活动任务返回 `BUSY`。
`verify()` 复验当前所有权和完整持久证据，不增加 revision；失败后该 lease 不得继续推进。自定义后端必须实现此门禁，受管 Replay/Transfer 在每次业务调用前执行它。检查与远端副作用之间仍有非原子窗口，已在途操作可能重复；`checkpoint()` 仅返回最后确认的历史快照，不授予当前执行权。
`advance` 比较序号与现存完整快照，持久化成功后才更新返回值；旧序号是 `STALE_REVISION`，证据改变或失权是 `FENCED`。写入失败按 `OUTCOME_UNKNOWN` 处理并保护当前 lease，后续推进拒绝。关闭前必须先结束实际在途业务；关闭不能撤回目标副作用。目标成功而进度尚未确认的崩溃可能重复执行，目的端仍需要幂等识别。

文件后端仅支持受控 POSIX 本地目录的单机恢复，使用与 Rust 相同的 `NSKCP001` 大端格式和 SHA-256 完整性校验。它持有固定任务锁文件，按完整正文同步、原子替换、父目录同步的顺序写入；进程崩溃后由 OS 释放锁。目录与锁文件必须由组件独占管理，禁止外部删除、重建或另行打开锁文件；摘要用于检测损坏，不是文件认证。该后端不承诺 NFS 或跨主机接管。自定义共享后端必须提供原子 claim/CAS 和成功回执对应的真实持久化。

### MySQL 共享 checkpoint

`MySqlCheckpointStore` 让不同主机上的 Java 与 Rust 任务使用同一份进度互斥接管，适用于有限 Replay、单目标 Transfer 与 DLT 回投。接入方显式提供 MySQL Connector/J 8 或更高版本；组件只依赖 JDK JDBC，普通 Kafka 使用不需要 MySQL 驱动。

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

数据库必须预先创建。URL 只能指定一个端点及 ASCII 数据库名，不能包含凭据或查询参数；凭据与 TLS 配置通过独立 `Properties` 传入并冻结。`sslMode` 默认 `VERIFY_IDENTITY`，只另行接受显式 `DISABLED`；私有 CA/mTLS 使用 Connector/J 的 `trustCertificateKeyStore*`、`clientCertificateKeyStore*`，还可设置 `enabledTLSProtocols` 和 `serverRSAPublicKeyFile`。未知属性、自动重连、复制 URL 与多语句配置拒绝，不接收连接池代理。

固定表为 `nasa_kafka_checkpoint`，二进制主键 `(namespace, task_id)`，正文复用 `NSKCP001`，最大 2 MiB。命名空间与任务名均为 1–128 个 ASCII 字母、数字、点、横线或下划线，首字符必须为字母或数字。所有接管者必须使用同一数据库名、表和命名空间；Java/Rust 的 SHA-256 锁名包含这些身份的长度前缀。每个活动 lease 独占一个物理连接及 `GET_LOCK` 会话锁，竞争立即返回 `BUSY`。首次写入 generation=1/revision=0；接管时两者加一并保留进度。写入同时比较锁所有者、原会话 ID、原代次、原序号及完整旧正文，确认一行成功后才发布新进度。

仅支持 MySQL 8 或更高版本的单一可写主节点、InnoDB、自动提交和 `innodb_flush_log_at_trx_commit=1`；启用 binlog 时要求 `sync_binlog=1` 与会话 `binlog_format=ROW`。后端拒绝错误结构、额外索引及可见触发器。表须专供此协议使用；运维必须控制 DDL、触发器和直接改写权限，并允许运行账号读取相关结构证据。`initialize()` 另需 CREATE 权限；正常任务需要 SELECT/INSERT/UPDATE 和命名锁权限。不得在活动任务期间改变持久设置或表结构。

连接额度明确传入，范围 1–1024，共同覆盖建连、初始化、读取及活动 lease；无额度返回 `CAPACITY`。单次 SQL 序列预算范围为 100ms–60s，关闭使用独立同长预算，失败清理可能增加调用耗时。只有实际本地连接退出才归还额度。失联服务端会话可能继续占锁，后续 claim 返回 `BUSY`，必须等服务器结束原会话，不能绕过锁接管。

连接中断、SQL 超时或确认丢失会停止旧 lease；写入结果未知为 `OUTCOME_UNKNOWN`，不能根据旧内存进度自行重试，新 claim 必须重新读取真实持久状态。关闭不能撤销在途 Kafka 写入。命名锁只属于一个 MySQL 服务，故障切换必须由外部保证旧主失去写权限；本组件不提供多主共识或自动防脑裂。`verify()` 与 Kafka 副作用存在非原子窗口，因此共享存储仍是 at-least-once，目标需要可识别重复。

这些存储入口不读取 Kafka、不执行 handler 或转发消息，也不替调用方判定成功条件；磁盘操作应放在业务独立的有界执行资源中。

## 指标出口

在开放业务前调用 `proxy.metrics(sink)` 或 `proxy.metrics(sink, new MetricsOptions(capacity, maxBytes))`
安装后端无关的 `MetricsSink`；每源独立的导出线程执行回调。默认最多排队 256 个事件，单事件名称与
全部标签最多 4096 UTF-8 字节，另有至多一个活动回调。框架加入固定 `source` 标签，最多允许七个其它
受控标签，不写业务 key、offset、载荷或异常正文。未安装时不创建导出线程。

队列满、准入竞争或事件过大时只丢弃观测；后端异常不影响 ACK、DLT、commit 或发布回执。
`metricsExportStats()` 无需调用后端即可查询排队、活动、导出、丢弃、失败及线程实际退出状态。
counter 是尽力而为的运行观测，不能充当计费或业务账本；exported 只代表本地回调正常返回。
关闭在 Kafka 业务资源退出后停止观测准入并丢弃队列；阻塞的后端回调仍需真实返回，超时报告
`OUTCOME_UNKNOWN` 并保留源身份，后续 `shutdownUntil` 可以继续收口。

提供可选 `MicrometerMetricsSink(MeterRegistry)` 与 `OpenTelemetryMetricsSink(Meter)`，两者也接受
第二个 `maxSeries` 参数，默认最多 4096 条序列。应用自行声明所用的 Micrometer 或 OpenTelemetry
依赖并提供 registry/SDK/exporter；组件不拥有后端的关闭权。安装示例：

```java
proxy.metrics(new MicrometerMetricsSink(registry, 4096));
// 或 proxy.metrics(new OpenTelemetryMetricsSink(meter, 4096));
```

`MetricCatalog.definitions()` 描述固定名称、类型、单位和标签。适配器拒绝未知名称、错误类型、缺失
source、额外标签或负 counter；目录达到上限后仍更新已有序列，不通过驱逐缓存制造无限后端序列。
`registeredSeries()` / `rejectedSamples()` 直接观察适配器容量。OpenTelemetry 单位分别使用 ms/us/By/%；
Micrometer 保留逻辑名称和基础单位，具体导出器可按其规则改写物理名称。counter 随后端实例累计，
同一后端复用相同源名不会自动清零；gauge 在没有后续采样时保留旧值，不能代替源健康判断。

Java `MetricCatalog` 与 Rust `METRIC_CATALOG` 定义同一组 64 个逻辑指标，类型、单位、标签和含义一致，
全部带 `source`。指标只在相应路线实际发生或显式采样时产生；目录声明不代表已有数据。

| 指标 | 类型与单位 | 额外标签与含义 |
| --- | --- | --- |
| `publish_admitted_total` | counter，消息 | lane/topic；native 已接管输入 |
| `published_total` / `publish_failed_total` | counter，消息 | lane/topic；native 回执成功 / 未确认成功，fire 入队不计成功 |
| `publish_admission_rejected_total` | counter，调用 | lane/reason；observer_capacity、byte_capacity 或 native_rejected |
| `publish_wait_unknown_total` | counter，调用 | lane；sendAndWait 未取得确定结果，直接取消 Future 不在此计数 |
| `producer_lane_in_flight` / `producer_lane_queue_utilization` | gauge，条 / 百分比 | lane；真实责任 / observer 容量占用率 |
| `group_ready` / `group_ready_wait_millis` | gauge，0或1 / 毫秒 | group；全槽 Running 且已 join / 最近成功等待耗时 |
| `group_ready_total` / `group_ready_timeout_total` | counter，调用 | group；Ready 等待 API 成功 / 超时，非状态迁移次数 |
| `assigned_partitions` / `paused_partitions` | gauge，分区 | group；全组分配 / 至少一个暂停原因的分区 |
| `active_handlers` / `dlt_in_flight` | gauge，调用 | group；实际解码和业务调用 / DLT 责任，分别计数 |
| `retained_bytes` | gauge，字节 | group；本地原文及框架副本的预留责任，不等同 broker lag |
| `commit_attempted_total` / `commit_succeeded_total` / `commit_failed_total` | counter，调用 | group；尝试 / broker 确认 / 未确认 |
| `owner_session_started_total` / `owner_session_restarted_total` | counter，会话 | group；会话开始 / 首次后的开始 |
| `consumed_total` / `manual_acked_total` / `manual_ack_missing_total` / `manual_ack_after_gap_total` | counter，条 | group/consumer_id；有效业务前缀 / Manual 前缀 / 缺口后缀 / 缺口后已 Ack；未等同 broker 提交 |
| `handler_deferred_total` / `handler_dead_letter_total` / `handler_failed_total` | counter，调用 | group/consumer_id；延期 / 主动 DLT / 普通失败；透传另计 |
| `decode_failed_total` | counter，条次 | group/consumer_id；已匹配路由的 payload 解码失败 |
| `retried_total` | counter，条次 | group/topic；安排重试的业务输入，延期亦计入，直接请求 DLT 不计 |
| `dlt_total` / `dlt_failed_total` | counter，来源记录 | group/topic；有效 DLT 确认 / 可用性优先策略终结的失败，持续重试的每次失败使用独立尝试指标 |
| `dlt_attempt_failed_total` | counter，来源记录 × 尝试 | group/topic；实际查询或交付失败；可靠模式继续保留输入时也计数，停止后的准入取消不计数 |
| `passthrough_calls_total` / `passthrough_duration_micros_total` | counter，调用 / 微秒 | group；实际退出的只读回调，包括异常返回 |
| `key_active_handlers` / `key_pending_records` / `key_pending_bytes` | gauge，调用 / 条 / 字节 | group；按键实际调用与保留输入责任，字节包含八倍副本预留 |
| `retained_records` | gauge，条 | group；原文凭证仍持有的未确认输入，不能由 consumer position 推算 |
| `dlt_queue_jobs` / `dlt_queue_bytes` | gauge，条 / 字节 | 无；源级 DLT dispatcher 的准入占用 |
| `registry_consumers` / `registry_groups` | gauge，路由 / 组 | client；当前冻结路由及受管组数 |
| `publish_delivery_unknown_total` | counter，条 | lane；原生责任已在本地终结，但远端写入结果仍不明 |
| `control_target_total` | counter，目标 | group/outcome；控制报告中的 applied/not_applied/unknown/owner_changed |
| `rebalance_assigned_total` | counter，回调 | group；分配完成，含初次加入 |
| `trace_invalid_total` | counter，输入次 | group/reason；malformed 或 duplicate，不包含原始头内容 |
| `broker_lag_offsets` / `broker_lag_partitions` / `broker_lag_missing_partitions` / `broker_lag_observed_at_millis` | gauge，offset距离 / 分区 / 分区 / Unix毫秒 | group/isolation；最近一次显式位点查询范围及证据完整性 |
| `source_stop_requested` / `source_resources_closed` | gauge，0或1 | 无；不可撤销停止意图 / 全部资源退出 |
| `close_active_owners` / `close_active_handlers` / `close_retained_records` / `close_released_unverified_records` | gauge，个 / 次 / 条 / 次 | 无；关闭报告的实际剩余责任及停止时未核实恢复的释放次数 |
| `replay_active_tasks` / `transfer_active_dependencies` / `transaction_active_tasks` / `dlt_active_tasks` / `admin_active_borrows` | gauge，个 | 无；各执行域尚未退出的实际责任 |
| `metrics_queued` / `metrics_active` | gauge，事件 / 回调 | 无；观测出口的排队与真实执行量 |

宿主按需要调用 `sampleMetrics()` 采样源级责任、注册规模及组量；该入口不查询 broker，也不创建定时线程。
组内量按秒汇总所有 owner。按键量涵盖仍持有原文的排队、运行、重试及完成证据，并包括已失权但实际尚未退出的调用。
字节计数描述框架预留，不是 JVM 或 native 内存硬上限。关闭后可继续从 `KafkaHandle.sampleMetrics()` 取得报告，
已关闭的出口不会保证最后一个 gauge 被送达，最终资源结论以 `CloseReport` 为准。

`admin().observeOffsets(...)` 的实际查询结果才更新 broker lag 四个 gauge；框架不暗中启动网络轮询。
系列对应该组与 isolation 最近一次请求的分区集合，并非自动覆盖整个组。只要有一个请求分区缺少有效 lag，
`broker_lag_offsets` 就为 -1；必须结合总分区数、缺失数和时间戳解释，不能把未知当零。
read_committed 使用 LSO，read_uncommitted 使用普通日志末尾；offset 距离包含日志空洞及控制记录，不能等同业务消息数。
本地 retained 量独立统计，即使拉取位置已超前，也不会隐藏仍在执行的工作。

Ready 还要求所有 owner 处于 RUNNING；保护态即使保留分配代次，也不能满足 Ready。

`consumer_id` 来自注册时冻结的 `KafkaListener.metricId()`，默认是目标类名，注解方法使用完整方法
签名；可显式提供稳定业务名称。身份须非空、无控制字符且不超过 1024 UTF-8 字节，不得使用消息字段
或随机 ID。失权、停止后丢弃的旧结果不计为成功；批次缺口后的临时 Ack 单独统计，不能推进安全前缀。

## 链路传播与实际执行寿命

`KafkaTraceContext` 读取唯一、大小写精确匹配的 `traceparent`，接纳 version 00、小写十六进制、非零 trace/span ID。
重复、null、非法字节或不支持的版本只放弃外来 trace，合法业务消息仍进入原来的处理路线；原因进入有限指标标签。
trace 身份从不承担路由授权、ACK 或提交证据。flags 原样传播，最低位控制采样。

在开放业务前调用 `proxy.telemetry(metricsSink, traceSink, sampleTraceRoots, options)`，两个 sink 至少一个非空。
指标与 trace 共用每源有界观测线程、排队额度和关闭责任；`KafkaTraceSink` 接收不含 payload 的不可变 `KafkaTraceSpan`。
应用负责把它映射到自己的遥测 SDK；组件不拥有应用后端。`MetricsExportStats` 与关闭报告中的 metrics 字段同时包含 trace。
Spring 自动创建的源可在应用 `BeanPostProcessor` 的初始化回调中按 `sourceId()` 安装，必须早于任何发布或消费启动，
仅声明一个 sink Bean 不会自动绑定。重复安装会拒绝。

单条消费以有效入站上下文为父，缺失时按 `sampleTraceRoots` 创建根；`KafkaRecord.traceContext()` 是原始父上下文，
`ConsumeContext.traceContext()` 是当前调用的子上下文。批调用创建独立根，并在 `traceLinks()` 中保留最多128个不同父节点，
`traceLinksOmitted()` 统计因容量不足而未保留的有效输入数。完成 span 保留相同链接，不能把第一条消息当成整批父节点。
同步原文透传也提供 `RawKafkaRecord.traceContext()`，只在合法借用作用域内可读。

业务调用线程内的发布自动继承 `ConsumeContext.current()`；跨 Executor 的 continuation 必须显式传入
`message(...).traceContext(context.traceContext())`。线程上下文不会随任意业务 future 自动传递；调用退出会恢复外层上下文。
消费 span 等待实际 handler 或返回的 `CompletionStage` 结束。生产 span 跟随内部交付观察者，fire、取消或主动完成业务 Future
都不能提前结束原生交付责任。`DELIVERY_UNKNOWN` 表示本地观察已经结束、远端结果仍不明；span 导出不代表消费位点已经提交。

## 能力边界

Java 与 Rust 的共同入口覆盖类型化事件、连续批次、回调期确认、按键并发、发布 lane、DLT、分区控制、受控会话恢复、同步原文透传和 Schema Registry。两端 typed 解码和 handler 都隔离于 native consumer owner，慢调用期间 owner 继续 poll；自定义解码与业务从执行准入时开始共用总超时预算，实际调用退出前保留线程责任。两端在同一 owner 内持续轮转分区，慢调用期间健康分区仍可接纳后续输入并提交；按键模式也允许后来取入的冷 key 使用空闲执行额度。DLT 信封容量不足只暂停对应失败分区。并发仍受共同输入与执行额度限制，不承诺实时调度期限或特定吞吐。Java 透传使用运行时只读访问域，Rust 使用同步借用；运行状态、指标和所有配置的完整对应仍不能仅凭 API 名称推导。

当前提供受管有限 Replay、单目标 Transfer、DLT 回投与原文隔离、POSIX 本地 checkpoint、单主 MySQL 共享 checkpoint及专属同集群事务。尚无Outbox/Inbox或动态热配置。普通回放 processor 的目标成功条件由业务提供，内置 Transfer 要求目标实际确认；两者都不能由持久进度推导跨资源原子保证。

原生 producer、Admin、可变配置和路由表不通过 KafkaProxy getter 暴露。配置查询返回副本，管理与控制应使用受管入口。

## 构建与许可证

```bash
mvn -B -ntp clean verify
```

采用 Apache-2.0 OR MIT 双许可证，详见 [LICENSE-APACHE](LICENSE-APACHE) 和 [LICENSE-MIT](LICENSE-MIT)。

接入兼容性见 [CHANGELOG.md](CHANGELOG.md)，贡献、安全和发布说明分别见 [CONTRIBUTING.md](CONTRIBUTING.md)、[SECURITY.md](SECURITY.md) 与 [RELEASING.md](RELEASING.md)。中英文公开文档与许可证一同进入主 JAR、sources JAR 和 Javadoc JAR。

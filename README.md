# nasa-kafka

基于 spring-kafka 的事件驱动封装。业务只实现一个 typed 监听接口，消息按 **topic + event** 两级路由，payload 的序列化、反序列化目标类型、批量聚合与广播分组都由框架处理。

```xml
<dependency>
    <groupId>io.github.nasa-runtime</groupId>
    <artifactId>nasa-kafka</artifactId>
    <version>1.0.0</version>
</dependency>
```

要求 JDK 21+，Maven 3.6.3+。

## 启用

本模块不注册 Spring Boot 自动配置，需要显式引入配置类：

```java
@Import(NasaKafkaConfig.class)
@SpringBootApplication
public class Application { }
```

引入后仍要求配置 `nasa.kafka.bootstrap-servers`，未配置时整个配置类不生效，容器里不会出现 `KafkaProxy`。
容器内没有任何 listener Bean 时只装配生产者，不建立消费连接。

## 消费

实现对应的监听接口并注册为 Bean 即可，框架按接口类型自动接线。

```java
@Component
public class OrderCreatedListener implements KafkaSingleListener<OrderDTO> {

    @Override public String[] topics() { return new String[]{"order"}; }
    @Override public String event() { return "created"; }

    @Override
    public void onEvent(OrderDTO order) {
        // 每次一个业务对象
    }
}
```

五种监听形态，按需要的粒度选：

| 接口 | 回调入参 | 适用 |
|---|---|---|
| `KafkaSingleListener<T>` | `T` | 逐条处理 |
| `KafkaBatchListener<T>` | `List<T>` | 同一 event 聚合后批量处理 |
| `KafkaRecordListener<T>` | `KafkaRecord<T>` | 需要 topic / partition / offset / timestamp / key / headers |
| `KafkaRecordBatchListener<T>` | `List<KafkaRecord<T>>` | 批量且需要元数据 |
| `KafkaBroadcast*Listener<T>` | 同上各形态 | **每个 JVM 实例独立 group，各消费一份** |

前四种是竞争消费：同一 group 内一条消息只被一个实例处理。`KafkaBroadcast*` 会为每个实例生成独立的 `group.id`，用于本地缓存失效通知这类需要全实例都收到的场景。

### 失败语义

`onEvent` 抛异常即不 ACK，消息留在 pending 中会被重投，**因此业务逻辑必须幂等**。持续失败的消息按配置进入 DLT，原始 topic / partition / offset 与失败原因写在 `X-Nasa-DLT-*` 头里，便于定位与重放。

## 发送

```java
kafkaProxy.publish("order", "created", orderDTO);                       // topic + event
kafkaProxy.publish("order", "created", orderId, orderDTO);              // 指定 key，保证同 key 有序
kafkaProxy.publish("order", "created", orderId, orderDTO, headers);     // 附加自定义头
kafkaProxy.publishToPartition("order", 3, "created", orderDTO);         // 指定分区
kafkaProxy.publishAt("order", "created", key, ts, dto, headers);        // 指定消息时间戳
```

不带 `event` 的重载使用默认事件名 `DEFAULT`。

### `partition(...)` 的事件名

按业务分片键发布还有一组 `partition` 方法，注意其中**不带 `event` 参数的两个重载把事件名设成了 topic 名本身**，不是 `DEFAULT`：

```java
kafkaProxy.partition("order", orderId, dto);        // event = "order"
kafkaProxy.partition("order", "created", orderId, dto);  // event = "created"
```

消费端必须注册 `event()` 返回 topic 名的 listener 才收得到；注册成 `DEFAULT` 会因匹配不到 listener 而被跳过，且**位点不前移**，表现为该分区反复重读同一批消息。

## 消费起点

手动分区消费（`assign`）用 `StartOffset` 指定起点，五种：

| 起点 | 语义 |
|---|---|
| `earliest()` | 分区当前最早可用位点。受 broker 保留策略约束，不是「第一条」 |
| `latest()` | 分区末尾，跳过全部积压（**不可逆**） |
| `timestamp(epochMs)` | 第一条时间戳不早于该时刻的消息；无满足条件的分区退到末尾 |
| `offset(n)` | 显式位点。**这一个值会应用到本次 assign 的所有分区**，不是按分区分别指定 |
| `committed()` | 沿用该 group 已提交位点；无提交记录的分区不做定位，交由 `auto.offset.reset` 决定 |

不传时按 `earliest()` 处理 —— 手动分区消费多用于回放，默认成末尾会静默跳过全部历史。

## 线路约定

框架在消息头里传递路由与编解码信息，跨语言消费方需要一并识别：

| 头 | 含义 |
|---|---|
| `X-Nasa-Event` | 事件名，消费端二级路由依据 |
| `X-Nasa-Payload-Codec` | 值恒为 `protocol-bytes`。**只在 payload 实现 `ProtocolBytes` 时才写**，JSON 编码的消息没有这个头 |
| `X-Nasa-Payload-Mode` | 二进制编码模式，与上一个头同时出现 |
| `X-Nasa-Passthrough` | 透传上下文 |
| `X-Nasa-DLT-*` | 进入死信队列时记录的原始 topic / partition / offset 与原因 |

这些头名一经发布即为线路契约，**修改会导致新旧版本消费方互不兼容**。

## 配置

前缀 `nasa.kafka`，覆盖连接、认证与 SSL：

```yaml
nasa:
  kafka:
    bootstrap-servers: kafka-1:9092,kafka-2:9092
    group-id: order-service
    client-id-prefix: order
    security-protocol: SASL_SSL
    sasl-mechanism: SCRAM-SHA-512
    sasl-jaas-config: "..."
    properties:              # 透传任意原生 Kafka 配置
      max.poll.records: "500"
```

`properties` 里的键值原样传给 Kafka 客户端，用于本类未显式建模的参数。

## 与 nasa-core 的关系

payload 的编码方式**按类型选择**：实现了 `nasa-core` 的 `ProtocolBytes` 的对象走二进制编码，比 JSON 更紧凑；其余类型一律 JSON。二进制消息带 `X-Nasa-Payload-Codec` / `X-Nasa-Payload-Mode` 两个头，JSON 消息不带头——这样在本组件引入二进制编码之前发出的历史消息仍能被解出。

消费侧用池化的 `RecycleLinkedMap` 承载透传上下文，减少每条消息的分配。因此本模块必须与 `nasa-core` 同时使用。

## 构建

```bash
mvn -B -ntp clean verify
```

## 文档与发布

- 贡献方式见 [CONTRIBUTING.md](CONTRIBUTING.md)。
- 安全问题报告方式见 [SECURITY.md](SECURITY.md)。
- Central Portal 和 GitHub 的发布流程见 [RELEASING.md](RELEASING.md)。
- 版本变化见 [CHANGELOG.md](CHANGELOG.md)。

## 许可证

本项目采用 `Apache-2.0 OR MIT` 双许可证，使用方可任选其一。详见 [LICENSE-APACHE](LICENSE-APACHE) 和 [LICENSE-MIT](LICENSE-MIT)。

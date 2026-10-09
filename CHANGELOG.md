# 变更记录

本项目遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## 1.0.0 - 未发布

首个公开版本。从 Nasa Runtime 单体仓库拆分为独立 Maven 项目。

坐标 `io.github.nasa-runtime:nasa-kafka`。

### 核心能力

- **typed 监听接口**：`KafkaSingleListener` / `KafkaBatchListener` / `KafkaRecordListener` /
  `KafkaRecordBatchListener` 四种消费粒度，按接口类型自动接线，消息按 **topic + event** 两级路由。
- **广播消费**：`KafkaBroadcast*Listener` 为每个 JVM 实例生成独立 `group.id`，
  用于本地缓存失效通知这类需要全实例都收到的场景。
- **发送入口 `KafkaProxy`**：支持指定 event、key、分区、消息时间戳与自定义头。
- **消费起点可控**：`EARLIEST` / `LATEST` / `TIMESTAMP` / `EXPLICIT`，用于回放历史或跳过积压。
- **死信记录**：持续失败的消息进入 DLT，原始 topic、partition、offset 与失败原因写入 `X-Nasa-DLT-*` 头。
- payload 默认走 `nasa-core` 的 `ProtocolBytes` 二进制编码，透传上下文用池化的 `RecycleLinkedMap` 承载。

### 运行要求

- JDK 21 或更高版本，Maven 3.6.3 或更高版本。
- 编译产物为 Java 21 字节码，低于 JDK 21 的项目无法加载。
- 依赖 `io.github.nasa-runtime:nasa-core`，日志只依赖 `slf4j-api`。

### 许可

采用 `Apache-2.0 OR MIT` 双许可证，使用方可任选其一。

### 从 Nasa Runtime 内嵌版本迁移

以下变化仅对原本在 Nasa Runtime 单体仓库中使用这些类的调用方有意义：

- **包名**由 `com.nasa.runtime.kafka` 变为 `io.github.nasaruntime.kafka`，`Automatic-Module-Name` 同步为 `io.github.nasaruntime.kafka`。
- **依赖不再经单体父 POM 传递**，全部在本项目显式声明并锁定版本。
- **线路契约**：`X-Nasa-Event`、`X-Nasa-Payload-Codec`、`X-Nasa-Payload-Mode`、`X-Nasa-Passthrough`
  与 `X-Nasa-DLT-*` 一经发布即为跨版本契约，修改会导致新旧消费方互不兼容。
- **失败语义**：`onEvent` 抛异常即不 ACK，消息会被重投，业务逻辑必须幂等。
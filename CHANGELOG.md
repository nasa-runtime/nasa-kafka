# 兼容性与接入合同

[中文](CHANGELOG.md) | [English](CHANGELOG.en.md)

当前能力、配置和使用示例以 [README](README.md) 与 Javadoc 为准；源码沿革由 Git 提交记录承担。

## 依赖与入口

Maven 坐标为 `io.github.nasa-runtime:nasa-kafka`，Java 根包与自动模块名均为 `io.github.nasaruntime.kafka`，要求 JDK 21。公共类型按 producer、consumer、transaction、checkpoint、replay 等功能包组织，具体类型路径见 README。内核直接使用 Kafka Java client，Spring Boot 提供配置与命名资源装配。接入代码须使用对应功能包的类型，不应根据父 POM 推断本组件的依赖。

业务优先使用 `KafkaHandle`；`KafkaProxy` 与 `KafkaClients` 承担源资源和生命周期。多源声明固定默认源和显式来源，不按查找失败自动回退。单源与多源配置不能混用，未知字段拒绝启动。

## 功能包与调用权限

根包仅保留 `KafkaProxy`、`KafkaClients`、`KafkaHandle` 三个顶层接入类型；消费记录从 `consumer.KafkaRecord` 导入，交付结果从 `producer.Delivery` 导入，注解和装配类从 `spring` 导入。原包路径没有二进制兼容别名，调用方须更新 import、反射类名与显式 `@Import` 后重新编译。Spring Boot 自动配置通过 `io.github.nasaruntime.kafka.spring.NasaKafkaConfig` 装配。

供框架功能包协作使用的公开成员不属于稳定业务 API。内部发布仍需实际来源签发并绑定任务用途的凭据；单参数 `sendBytesInternal` 与普通发送遵守相同业务准入。`ProducerLane.FlushBarrier` 是所属 lane 创建的不透明对象，不提供构造器、序号或 Future 数组访问器，调用方使用 `flush()` 等待真实交付与回执责任。

`@KafkaConsumer` 方法须为 public 实例方法，Bean 类可以非 public；框架在注册时验证实际代理调用权限，模块访问限制或缺少代理接口方法时以 `CONFIG` 拒绝。已有 AOP 调用链保持生效。

## 身份、线路与恢复

- group 决定消费责任和位点归属，合法名称使用严格 UTF-8 原文；不满足共同输入域时须显式规划迁移，不能裁剪、替换或自行复制位点。
- `X-Nasa-Event`、`X-Nasa-Payload-Codec`、`X-Nasa-Payload-Mode`、`X-Nasa-Passthrough` 和 DLT 来源头属于线路合同。event 不归一化，null/空 key、tombstone 和有序重复 header 保持区别。
- JSON 与 ProtocolBytes 是明确选择的编码路径，不能仅凭 Java DTO 名称推断对端编解码能力。schema、codec 和 mode 不匹配时按输入策略处理。
- 普通 Auto/Manual 确认只能推进当前代次的连续安全前缀；重投可能重复外部副作用，业务须按稳定身份保证幂等。
- Kafka 事务恢复复用稳定部署身份；有限回放、转发与 DLT 回投还须保留计划、物理资源身份和有效 checkpoint。Kafka 事务不覆盖数据库或其它集群。

## 配置转换与滚动部署

使用 [配置迁移入口](README.md#配置迁移与滚动部署) 取得候选和人工决策项；该操作不改配置文件或 broker 位点。部署前明确可靠发送、输入容量、错误策略、Admin 写授权和 Ready 要求。只替换二进制不会撤销已提交的业务事实，回退仍须能读取已有线路和持久格式。

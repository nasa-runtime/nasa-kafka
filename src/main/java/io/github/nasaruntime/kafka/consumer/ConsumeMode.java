package io.github.nasaruntime.kafka.consumer;


/**
 * 枚举作用 - KafkaPollTask 的消费模式。
 */
public enum ConsumeMode {
    /**
     * consumer group + rebalance 模式, 由 Spring Bean listener 自动订阅 topic。
     */
    SUBSCRIBE,
    /**
     * 手动 assign 固定分区模式, 不参与 Kafka group rebalance。
     */
    ASSIGN
}

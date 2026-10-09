package io.github.nasaruntime.kafka;

/**
 * 枚举作用 - StartOffset 的起始位点类型。
 */
public enum StartOffsetKind {
    /** 分区最早可用位点。 */
    EARLIEST,
    /** 分区末尾, 只消费后续新消息。 */
    LATEST,
    /** 第一条 timestamp 大于等于指定毫秒值的消息。 */
    TIMESTAMP,
    /** 显式指定 offset。 */
    EXPLICIT,
    /** 使用该 group 已提交 offset。 */
    COMMITTED
}

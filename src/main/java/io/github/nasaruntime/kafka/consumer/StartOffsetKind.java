package io.github.nasaruntime.kafka.consumer;


/**
 * 枚举作用 - StartOffset 的起始位点类型。
 */
public enum StartOffsetKind {
    /**
     * 分区最早可用位点。
     */
    EARLIEST,
    /**
     * 首次会话解析时所选隔离级别的末尾；不删除历史日志，也不立即提交位点。
     */
    LATEST,
    /**
     * 第一条 timestamp 大于等于指定毫秒值的消息。
     */
    TIMESTAMP,
    /**
     * 显式指定 offset。
     */
    EXPLICIT,
    /**
     * 使用该 group 已提交 offset。
     */
    COMMITTED
}

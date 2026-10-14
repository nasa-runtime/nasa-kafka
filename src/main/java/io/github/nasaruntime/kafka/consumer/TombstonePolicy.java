package io.github.nasaruntime.kafka.consumer;

/**
 * typed 消费对 Kafka null value 的处置；空字节和 JSON null 仍按载荷 codec 解码。
 */
public enum TombstonePolicy {
    /**
     * 不调用业务并推进连续水位；适用于不处理删除语义的消费组。
     */
    SKIP,
    /**
     * 交给显式 nullable 的 typed listener；非 nullable 路由按无效载荷处置。
     */
    DELIVER,
    /**
     * 原样转入 DLT；默认要求交付确认，显式关闭 deadLetterRequired 时允许按失败丢弃策略推进。
     */
    DEAD_LETTER,
    /**
     * 保留未提交输入并暂停分区，等待显式处置。
     */
    HALT
}

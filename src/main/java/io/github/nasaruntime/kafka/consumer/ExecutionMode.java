package io.github.nasaruntime.kafka.consumer;

/**
 * 有序处理的范围；两种模式的 broker 提交均服从分区安全前缀。
 */
public enum ExecutionMode {
    /**
     * 同一分区只有一个业务调用；前一记录失败时后续记录等待其处置。
     */
    PARTITION_ORDERED,
    /**
     * 同分区相同原始 key 串行，不同 key 可并行；null key 构成分区屏障，不支持批 listener。
     */
    KEY_ORDERED
}

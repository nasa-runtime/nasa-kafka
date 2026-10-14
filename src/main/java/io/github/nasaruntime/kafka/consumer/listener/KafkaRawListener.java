package io.github.nasaruntime.kafka.consumer.listener;

/**
 * 保留原始 payload 与 key 的 record 路线，仍遵守有界执行和确认合同。
 */
public interface KafkaRawListener extends KafkaRecordListener<byte[]> {

    /**
     * 业务作用：保留 raw tombstone 而不进行 DTO 判空。
     * <p>
     * 参数说明：无。
     *
     * @return 允许 null payload。
     */
    @Override
    default boolean nullable() {
        return true;
    }
}

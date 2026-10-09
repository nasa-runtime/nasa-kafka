package io.github.nasaruntime.kafka;

import java.util.List;
import java.util.Map;

/**
 * 记录作用 - KafkaPollTask 内部使用的已反序列化记录。
 *
 * @param value 反序列化后的业务对象
 * @param topic 来源 topic
 * @param partition 来源分区
 * @param offset 记录 offset
 * @param timestamp 记录 timestamp
 * @param key Kafka record key
 * @param headers Kafka record headers
 */
record PolledRecord(Object value, String topic, int partition, long offset, long timestamp, String key,
                    Map<String, List<byte[]>> headers) {

    /**
     * 业务作用：把内部的已反序列化记录转成对外暴露的记录类型，作为框架内部结构与业务可见类型的边界。
     * 内部类型可随实现调整，业务只依赖转换后的类型。
     *
     * <p>参数说明: 无。
     *
     * @return 承载相同 topic、分区、位点、时间戳、key、业务对象与 headers 的对外记录。
     */
    KafkaRecord<Object> toKafkaRecord() {
        return new KafkaRecord<>(topic, partition, offset, timestamp, key, value, headers);
    }
}

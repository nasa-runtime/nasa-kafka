package io.github.nasaruntime.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * 记录作用 - 对外暴露的 Kafka 消费记录, 包含业务对象和 Kafka 元数据。
 *
 * @param topic 来源 topic
 * @param partition 来源分区
 * @param offset 记录偏移量
 * @param timestamp 记录时间戳, 单位毫秒
 * @param key Kafka record key, 可能为 null
 * @param value 反序列化后的业务对象
 * @param headers Kafka record headers, 同名 header 保留多个值, value 为原始 bytes
 * @param <T> 业务对象类型
 */
public record KafkaRecord<T>(String topic, int partition, long offset, long timestamp, String key, T value,
                             Map<String, List<byte[]>> headers) {

    /**
     * 业务作用：构造不带 headers 的消费记录，用于消息本身没有附加头信息的场景。
     * headers 置为不可变空映射而非 null，使消费方读取 header 时无需先判空。
     *
     * @param topic     来源 topic
     * @param partition 来源分区
     * @param offset    记录在分区内的偏移量
     * @param timestamp 记录时间戳，单位毫秒
     * @param key       Kafka record key，可为 null
     * @param value     反序列化后的业务对象
     */
    public KafkaRecord(String topic, int partition, long offset, long timestamp, String key, T value) {
        this(topic, partition, offset, timestamp, key, value, Map.of());
    }

    /**
     * 业务作用：把本记录的来源 topic 与分区号还原成 Kafka 原生分区标识，
     * 便于消费方据此做手动位点操作或按分区聚合。
     *
     * <p>参数说明: 无。
     *
     * @return 由本记录的 topic 与 partition 组成的分区标识。
     */
    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }
}

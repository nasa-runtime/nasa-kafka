package io.github.nasaruntime.kafka;

import org.apache.kafka.common.TopicPartition;

/**
 * 记录作用 - Kafka 分区 committed offset 与 log end offset 的 lag 快照。
 *
 * @param topic topic 名
 * @param partition 分区号
 * @param committedOffset group 已提交 offset; null 表示该 group 在该分区没有提交历史
 * @param endOffset 分区 log end offset
 * @param lag endOffset - committedOffset; committedOffset 为 null 时也为 null
 */
public record KafkaPartitionLag(String topic, int partition, Long committedOffset, long endOffset, Long lag) {

    /**
     * 业务作用：把本快照的 topic 与分区号还原成 Kafka 原生分区标识，便于直接喂给 seek、pause 等原生接口。
     *
     * <p>参数说明: 无。
     *
     * @return 由本记录的 topic 与 partition 组成的分区标识。
     */
    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }
}

package io.github.nasaruntime.kafka.admin;

import org.apache.kafka.common.TopicPartition;

/**
 * 记录作用 - Kafka 分区已提交位点与 READ_UNCOMMITTED 末尾距离的兼容快照，不是待处理业务条数。
 *
 * @param topic           topic 名
 * @param partition       分区号
 * @param committedOffset group 已提交 offset; null 表示该 group 在该分区没有提交历史
 * @param endOffset       READ_UNCOMMITTED 查询得到的分区末尾，不是事务读取的 LSO
 * @param lag             max(0, endOffset - committedOffset)；committedOffset 为 null 时也为 null
 */
public record KafkaPartitionLag(String topic, int partition, Long committedOffset, long endOffset, Long lag) {

    /**
     * 业务作用：把观测的 topic 与分区号转换成原生查询或控制目标。
     * 观测坐标本身不授予分区归属或消费位点提交权。
     * <p>
     * 参数说明：无。
     *
     * @return 由本记录的 topic 与 partition 组成的分区标识。
     */
    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }
}

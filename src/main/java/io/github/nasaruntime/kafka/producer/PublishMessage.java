package io.github.nasaruntime.kafka.producer;

import io.github.nasaruntime.kafka.KafkaProxy;

/**
 * 记录作用 - Kafka 批量发布入参, 表示一条待发布消息的 event、key 和 payload。
 *
 * @param event   事件名, 写入 Kafka header {@link KafkaProxy#HEADER_EVENT}
 * @param key     Kafka record key, 用于分区 hash, 可为 null
 * @param message 业务消息体
 */
public record PublishMessage(String event, String key, Object message) {

    /**
     * 业务作用：构造没有业务 key 的批量发布项，由发送通道的分区策略选择分区。
     * 不保证同一业务实体的消息具有相同分区或相对顺序；有按实体路由要求时应显式提供 key。
     *
     * @param event   事件名，消费端据此做二级路由
     * @param message 业务消息体
     * @return 不带 key 的批量发布项。
     */
    public static PublishMessage of(String event, Object message) {
        return new PublishMessage(event, null, message);
    }

    /**
     * 业务作用：构造带业务 key 的批量发布项，供未显式指定分区的发送路径选择分区。
     * 实际归属取决于分区器及分区数量；仅提供 key 不保证跨生产者、跨 lane 或分区扩容后的总顺序。
     *
     * @param event   事件名，消费端据此做二级路由
     * @param key     Kafka record key，参与分区计算
     * @param message 业务消息体
     * @return 带 key 的批量发布项。
     */
    public static PublishMessage of(String event, String key, Object message) {
        return new PublishMessage(event, key, message);
    }
}

package io.github.nasaruntime.kafka;

/**
 * 记录作用 - Kafka 批量发布入参, 表示一条待发布消息的 event、key 和 payload。
 *
 * @param event 事件名, 写入 Kafka header {@link KafkaProxy#HEADER_EVENT}
 * @param key Kafka record key, 用于分区 hash, 可为 null
 * @param message 业务消息体
 */
public record PublishMessage(String event, String key, Object message) {

    /**
     * 业务作用：构造不指定 key 的批量发布项。
     * 不指定 key 时由 Kafka 默认分区器选择分区，同一业务实体的多条消息可能落到不同分区，
     * 因而<b>不保证相对顺序</b>；需要按实体保序时必须用带 key 的重载。
     *
     * @param event   事件名，消费端据此做二级路由
     * @param message 业务消息体
     * @return 不带 key 的批量发布项。
     */
    public static PublishMessage of(String event, Object message) {
        return new PublishMessage(event, null, message);
    }

    /**
     * 业务作用：构造指定 key 的批量发布项，使同 key 的消息落到同一分区从而保持相对顺序。
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

package io.github.nasaruntime.kafka.producer;

/**
 * broker 发送结果；confirmed=false 时不构成消费或 DLT 安全完成证据。
 *
 * @param sourceId  完成发送的命名源
 * @param lane      源内物理发布通道
 * @param topic     broker 返回的目标 topic
 * @param partition 实际目标分区
 * @param offset    broker 返回的位置；无确认发送可能为 -1
 * @param timestamp broker 回执时间戳，单位 epoch 毫秒；不可用时为 -1
 * @param confirmed 是否使用要求 broker 确认的 acks 模式，不代表业务消费完成
 */
public record Delivery(String sourceId, String lane, String topic, int partition, long offset, long timestamp, boolean confirmed) {
}

package io.github.nasaruntime.kafka.consumer.listener;

import io.github.nasaruntime.kafka.consumer.KafkaRecord;

/**
 * 接口作用 - Kafka 单条 record 消费 listener, 回调入参携带 topic、partition、offset、timestamp、key 和 headers。
 *
 * @param <T> 消息体类型, {@link #paramType()} 返回的也是该业务类型
 */
public interface KafkaRecordListener<T> extends KafkaListener<T, KafkaRecord<T>> {
}

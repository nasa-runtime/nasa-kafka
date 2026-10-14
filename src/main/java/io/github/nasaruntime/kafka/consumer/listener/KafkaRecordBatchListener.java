package io.github.nasaruntime.kafka.consumer.listener;

import io.github.nasaruntime.kafka.consumer.KafkaRecord;

import java.util.List;

/**
 * 接口作用 - Kafka 批量 record 消费 listener, 批量回调时每条消息都携带 Kafka 元数据。
 *
 * @param <T> 消息体类型, {@link #paramType()} 返回的也是该业务类型
 */
public interface KafkaRecordBatchListener<T> extends KafkaListener<T, List<KafkaRecord<T>>> {
}

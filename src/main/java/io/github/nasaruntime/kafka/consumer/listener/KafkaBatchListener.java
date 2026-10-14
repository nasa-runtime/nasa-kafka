package io.github.nasaruntime.kafka.consumer.listener;

import java.util.List;

/**
 * 接口作用 - Kafka 批量消费 listener, 每次 onEvent 接收同一 event 聚合后的业务对象列表。
 *
 * @param <T> 消息体类型, 即 payload 反序列化后的业务类型
 */
public interface KafkaBatchListener<T> extends KafkaListener<T, List<T>> {
}

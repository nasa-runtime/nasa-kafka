package io.github.nasaruntime.kafka.consumer.listener;

/**
 * 接口作用 - Kafka 单条消费 listener, 每次 onEvent 接收一个业务对象。
 *
 * @param <T> 消息体类型, 即 payload 反序列化后的业务类型
 */
public interface KafkaSingleListener<T> extends KafkaListener<T, T> {
}

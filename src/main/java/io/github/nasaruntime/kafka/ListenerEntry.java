package io.github.nasaruntime.kafka;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * 记录作用 - Kafka listener 注册项, 保存反序列化类型、listener 实例和分发类型。
 *
 * @param paramType listener 声明的 payload 反序列化类型
 * @param listener Kafka listener 实例
 * @param kind listener 分发类型
 */
@SuppressWarnings("rawtypes")
record ListenerEntry(TypeReference paramType, KafkaListener listener, ListenerKind kind) {
}

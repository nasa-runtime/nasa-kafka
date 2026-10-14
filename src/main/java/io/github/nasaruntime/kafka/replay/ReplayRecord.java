package io.github.nasaruntime.kafka.replay;

import io.github.nasaruntime.kafka.protocol.KafkaHeaders;

import java.util.Objects;


/**
 * 回放中的独立原文与稳定来源坐标，可用于目的端幂等识别；offset 不因转发而成为目标 offset。
 */
public record ReplayRecord(String clusterId, String topic, int partition, long offset, long timestamp,
                           byte[] key, byte[] payload, KafkaHeaders headers) {

    /**
     * 业务作用：固定实际处理输入，避免原生缓冲释放或外部修改改变正在确认的记录。
     * <p>
     * 返回：拥有独立字节的记录。
     *
     * @param clusterId 来源Kafka集群的物理身份。
     * @param topic     记录原始来源topic。
     * @param partition 记录原始来源分区号。
     * @param offset    记录在原始来源分区内的位置。
     * @param timestamp 原始Kafka毫秒时间，缺失时间保留为负值。
     * @param key       原始key字节，构造时复制，null与空值不同。
     * @param payload   原始正文，null表示tombstone，构造时隔离外部字节修改。
     * @param headers   原始有序消息头，保留重复名称、顺序及null值。
     */
    public ReplayRecord {
        key = key == null ? null : key.clone();
        payload = payload == null ? null : payload.clone();
        Objects.requireNonNull(headers);
    }

    /**
     * 业务作用：交付不会改写框架快照的原始 key。
     * <p>
     * 参数说明：无。
     *
     * @return 独立字节或 null。
     */
    @Override
    public byte[] key() {
        return key == null ? null : key.clone();
    }

    /**
     * 业务作用：交付不会改写框架快照的原始 payload。
     * <p>
     * 参数说明：无。
     *
     * @return 独立字节或 tombstone 的 null。
     */
    @Override
    public byte[] payload() {
        return payload == null ? null : payload.clone();
    }
}

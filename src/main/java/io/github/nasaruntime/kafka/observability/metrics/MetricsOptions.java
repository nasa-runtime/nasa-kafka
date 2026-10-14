package io.github.nasaruntime.kafka.observability.metrics;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

/**
 * 指标与trace共享的观测队列独立于业务和DLT责任额度；计数或字节超限只丢弃观测。
 */
public record MetricsOptions(int queueCapacity, int maxEventBytes) {

    public static final MetricsOptions DEFAULT = new MetricsOptions(256, 4096);

    /**
     * 业务作用：在创建导出线程前限制观测资源。
     * <p>
     * 返回：合法预算；零、负值或过大声明拒绝。
     *
     * @param queueCapacity 排队观测事件上限
     * @param maxEventBytes 单事件标签或trace身份及链接的保守计费字节上限。
     */
    public MetricsOptions {
        if (queueCapacity < 1 || queueCapacity > 65536 || maxEventBytes < 64 || maxEventBytes > 65536)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "指标队列必须为 1..65536，单事件字节为 64..65536");
    }
}

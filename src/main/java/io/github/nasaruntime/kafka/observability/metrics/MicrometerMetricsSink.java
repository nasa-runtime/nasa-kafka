package io.github.nasaruntime.kafka.observability.metrics;


import io.micrometer.core.instrument.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * 可选 Micrometer 接入；registry 由应用持有和关闭，本适配器最多保留给定数量的序列。
 */
public final class MicrometerMetricsSink extends BoundedMetricsSink {

    private final MeterRegistry registry;

    /**
     * 业务作用：用默认 4096 条序列预算连接应用 registry。
     * <p>
     * 返回：可传给 KafkaProxy.metrics 的适配器。
     *
     * @param registry 应用指标后端。
     */
    public MicrometerMetricsSink(MeterRegistry registry) {
        this(registry, 4096);
    }

    /**
     * 业务作用：给共享 registry 接入增加本组件的独立序列上限。
     * <p>
     * 返回：不取得 registry 生命周期所有权的适配器。
     *
     * @param registry  后端
     * @param maxSeries 预算。
     */
    public MicrometerMetricsSink(MeterRegistry registry, int maxSeries) {
        super(maxSeries);
        this.registry = Objects.requireNonNull(registry);
    }

    /**
     * 业务作用：登记具有稳定 source 标签的 counter 或强引用 gauge。
     *
     * @param definition 固定合同
     * @param labels     完整身份。
     * @return 后续仅更新该序列的记录器。
     */
    @Override
    protected LongConsumer create(MetricCatalog.Definition definition, Map<String, String> labels) {
        List<Tag> tags = definition.labels().stream().map(key -> Tag.of(key, labels.get(key))).toList();
        if (definition.kind() == MetricCatalog.Kind.COUNTER) {
            Counter counter = Counter.builder(definition.name()).description(definition.description()).baseUnit(definition.unit()).tags(tags).register(registry);
            return value -> counter.increment(value);
        }
        AtomicLong value = new AtomicLong();
        Gauge.builder(definition.name(), value, AtomicLong::doubleValue).description(definition.description()).baseUnit(definition.unit()).tags(tags).strongReference(true).register(registry);
        return value::set;
    }
}

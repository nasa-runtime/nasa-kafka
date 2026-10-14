package io.github.nasaruntime.kafka.observability.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;

import java.util.*;
import java.util.function.LongConsumer;

/**
 * 可选 OpenTelemetry 接入；SDK/exporter 由应用提供，本组件只声明和记录有界序列。
 */
public final class OpenTelemetryMetricsSink extends BoundedMetricsSink {

    private final Meter meter;

    /**
     * 业务作用：用默认 4096 条序列预算接入应用 Meter。
     * <p>
     * 返回：可安装到多个源的适配器。
     *
     * @param meter OpenTelemetry Meter。
     */
    public OpenTelemetryMetricsSink(Meter meter) {
        this(meter, 4096);
    }

    /**
     * 业务作用：为 OpenTelemetry 标签组合设置硬上限。
     * <p>
     * 返回：不创建线程或 exporter 的适配器。
     *
     * @param meter     后端入口
     * @param maxSeries 本适配器预算。
     */
    public OpenTelemetryMetricsSink(Meter meter, int maxSeries) {
        super(maxSeries);
        this.meter = Objects.requireNonNull(meter);
    }

    /**
     * 业务作用：将共同单位转成 UCUM 并绑定不缺失 source 的固定标签。
     *
     * @param definition 目录合同
     * @param labels     完整身份。
     * @return counter 或同步 gauge 记录器。
     */
    @Override
    protected LongConsumer create(MetricCatalog.Definition definition, Map<String, String> labels) {
        var builder = Attributes.builder();
        definition.labels().forEach(key -> builder.put(key, labels.get(key)));
        var attributes = builder.build();
        String unit = switch (definition.unit()) {
            case "milliseconds" -> "ms";
            case "microseconds" -> "us";
            case "bytes" -> "By";
            case "percent" -> "%";
            default -> "1";
        };
        if (definition.kind() == MetricCatalog.Kind.COUNTER) {
            var counter = meter.counterBuilder(definition.name()).setDescription(definition.description()).setUnit(unit).build();
            return value -> counter.add(value, attributes);
        }
        var gauge = meter.gaugeBuilder(definition.name()).ofLongs().setDescription(definition.description()).setUnit(unit).build();
        return value -> gauge.set(value, attributes);
    }
}

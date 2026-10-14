package io.github.nasaruntime.kafka.observability.metrics;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.LongConsumer;

/**
 * 后端适配器共用的固定目录与序列硬上限；同一实例可被多个源共享。
 */
abstract class BoundedMetricsSink implements MetricsSink {

    private record Series(String name, Map<String, String> labels) {}

    private final int maxSeries;
    private final Map<Series, LongConsumer> series = new HashMap<>();
    private long rejected;

    /**
     * 业务作用：建立适配器级序列预算，避免源或组反复变化制造无限后端序列。
     * <p>
     * 返回：不创建任何后端指标。
     *
     * @param maxSeries 1..65536 的硬上限。
     */
    BoundedMetricsSink(int maxSeries) {
        if (maxSeries < 1 || maxSeries > 65536) throw new IllegalArgumentException("maxSeries 必须为 1..65536");
        this.maxSeries = maxSeries;
    }

    /**
     * 业务作用：观察当前已经登记的后端序列数。
     * <p>
     * 参数说明：无。
     *
     * @return 不超过构造时预算的计数。
     */
    public synchronized int registeredSeries() {
        return series.size();
    }

    /**
     * 业务作用：观察目录、标签、类型或容量不满足合同而丢弃的样本数。
     * <p>
     * 参数说明：无。
     *
     * @return 适配器生命周期内累计拒绝数。
     */
    public synchronized long rejectedSamples() {
        return rejected;
    }

    /**
     * 业务作用：在校验后向后端累加非负增量。
     * <p>
     * 返回：无效或超额样本丢弃；后端异常交由源异步出口隔离。
     *
     * @param name   目录名
     * @param delta  增量
     * @param labels 完整源身份。
     */
    @Override
    public final void counter(String name, long delta, Map<String, String> labels) {
        record(name, delta, MetricCatalog.Kind.COUNTER, labels);
    }

    /**
     * 业务作用：在校验后向后端设置瞬时值。
     * <p>
     * 返回：无效或超额样本丢弃；不改变业务状态。
     *
     * @param name   目录名
     * @param value  当前值
     * @param labels 完整身份。
     */
    @Override
    public final void gauge(String name, long value, Map<String, String> labels) {
        record(name, value, MetricCatalog.Kind.GAUGE, labels);
    }

    /**
     * 业务作用：集中实施目录与容量门禁，禁止缺失 source 时合并不同源。
     * <p>
     * 返回：已登记记录器接收值，非法样本只累计拒绝。
     *
     * @param name   本次样本的稳定指标名称。
     * @param value  本次样本记录的数值。
     * @param kind   样本采用累计计数还是瞬时观测的类别。
     * @param labels 完整标签。
     */
    private void record(String name, long value, MetricCatalog.Kind kind, Map<String, String> labels) {
        LongConsumer recorder;
        synchronized (this) {
            var definition = name == null ? null : MetricCatalog.find(name).orElse(null);
            if (definition == null || definition.kind() != kind || kind == MetricCatalog.Kind.COUNTER && value < 0 || labels == null
                    || labels.size() != definition.labels().size() || !labels.keySet().containsAll(definition.labels())
                    || labels.values().stream().anyMatch(v -> v == null || v.length() > 4096 || v.getBytes(StandardCharsets.UTF_8).length > 4096)) {
                rejected++;
                return;
            }
            var key = new Series(name, Map.copyOf(labels));
            recorder = series.get(key);
            if (recorder == null) {
                // 保留已有序列，即使目录已满仍可更新；不驱逐旧序列制造不可追踪的后端增长。
                if (series.size() >= maxSeries) {
                    rejected++;
                    return;
                }
                recorder = create(definition, key.labels());
                series.put(key, recorder);
            }
        }
        recorder.accept(value);
    }

    /**
     * 业务作用：把固定定义和完整标签绑定到一个后端记录器。
     *
     * @param definition 目录项
     * @param labels     不可变身份。
     * @return 用于该序列的增量或瞬时值接收器。
     */
    protected abstract LongConsumer create(MetricCatalog.Definition definition, Map<String, String> labels);
}

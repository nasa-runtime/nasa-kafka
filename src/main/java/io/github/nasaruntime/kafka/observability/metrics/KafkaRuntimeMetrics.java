package io.github.nasaruntime.kafka.observability.metrics;

import io.github.nasaruntime.kafka.KafkaProxy;

import io.github.nasaruntime.kafka.lifecycle.CloseReport;
import io.github.nasaruntime.kafka.admin.OffsetObservation;

import java.util.Map;
import java.util.Locale;

/**
 * 无网络副作用的源级责任采样与显式 broker 观测出口。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaRuntimeMetrics {

    /**
     * 业务作用：禁止构造无状态采样器。
     * <p>
     * 参数说明：无。
     * 返回：无实例。
     */
    private KafkaRuntimeMetrics() {}

    /**
     * 业务作用：把独立资源责任送入有限指标队列，保持关闭结论与业务完成分离。
     * <p>
     * 返回：尽力输出；源已关闭时仅保留调用方持有的报告。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param source 所属源
     * @param r      只读关闭快照。
     */
    public static void sample(KafkaProxy source, CloseReport r) {
        long[] values = {r.stopRequested() ? 1 : 0, r.resourcesClosed() ? 1 : 0, r.consumers().activeOwners(), r.consumers().activeHandlers(),
                r.consumers().retainedRecords(), r.consumers().releasedUnverifiedRecords(), r.replayTasks(), r.transferDependencies(),
                r.transactionTasks(), r.dltActiveTasks(), r.auxiliaryResourcesActive(), r.metricsQueued(), r.metricsActive()};
        String[] names = {"source_stop_requested", "source_resources_closed", "close_active_owners", "close_active_handlers",
                "close_retained_records", "close_released_unverified_records", "replay_active_tasks", "transfer_active_dependencies",
                "transaction_active_tasks", "dlt_active_tasks", "admin_active_borrows", "metrics_queued", "metrics_active"};
        for (int i = 0; i < names.length; i++) source.metric(names[i], values[i], false, Map.of());
    }

    /**
     * 业务作用：发布显式请求范围内的 broker 提交距离，缺少任一分区证据时不伪装完整积压。
     * <p>
     * 返回：无 group 时不输出；未知 lag 使用 -1 并同时报告覆盖范围与缺项。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param source 所属源
     * @param r      已封存观测。
     */
    public static void offsets(KafkaProxy source, OffsetObservation r) {
        if (r.group() == null) return;
        long sum = 0, missing = 0;
        for (var p : r.partitions()) {
            if (p.lag().value() == null) missing++;
            else sum = sum > Long.MAX_VALUE - p.lag().value() ? Long.MAX_VALUE : sum + p.lag().value();
        }
        var labels = Map.of("group", r.group(), "isolation", r.isolation().name().toLowerCase(Locale.ROOT));
        source.metric("broker_lag_offsets", missing == 0 ? sum : -1, false, labels);
        source.metric("broker_lag_partitions", r.partitions().size(), false, labels);
        source.metric("broker_lag_missing_partitions", missing, false, labels);
        source.metric("broker_lag_observed_at_millis", r.completedAtMillis(), false, labels);
    }
}

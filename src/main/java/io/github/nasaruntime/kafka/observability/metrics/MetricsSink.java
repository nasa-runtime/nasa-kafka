package io.github.nasaruntime.kafka.observability.metrics;

import java.util.Map;

/**
 * 后端无关的指标出口；回调只在源的专属导出线程执行，不占用 poll 或 producer 交付线程。
 */
public interface MetricsSink {

    /**
     * 业务作用：导出非负单调计数增量。
     * <p>
     * 返回：正常返回仅代表本地回调成功；异常被隔离，阻塞会挤满观测队列。
     *
     * @param name   框架固定名
     * @param delta  增量
     * @param labels 包含 source 的受控身份。
     */
    void counter(String name, long delta, Map<String, String> labels);

    /**
     * 业务作用：导出瞬时状态。
     * <p>
     * 返回：正常返回仅代表本地回调成功；不授予任何确认或业务控制权。
     *
     * @param name   固定名
     * @param value  当前值
     * @param labels 包含 source 的受控身份。
     */
    void gauge(String name, long value, Map<String, String> labels);
}

package io.github.nasaruntime.kafka.observability.metrics;

import io.github.nasaruntime.kafka.exception.KafkaFailure;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceSink;

import io.github.nasaruntime.kafka.observability.trace.KafkaTraceSpan;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.time.Duration;

/**
 * 源级有界异步观测桥；用户回调不持有队列、owner 或 producer 的锁。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class MetricsExporter {

    private final String source;
    private final MetricsOptions options;
    private final ReentrantLock gate = new ReentrantLock();
    private final Condition wake = gate.newCondition();
    private final ArrayDeque<Event> queue = new ArrayDeque<>();
    private final AtomicInteger queued = new AtomicInteger(), active = new AtomicInteger();
    private final AtomicLong exported = new AtomicLong(), dropped = new AtomicLong(), failed = new AtomicLong();
    private volatile boolean accepting = true;
    private final Thread worker;
    private final boolean metricEnabled;
    private final KafkaTraceSink traceSink;
    public final boolean sampleTraceRoots;

    private record Event(String name, long value, boolean counter, Map<String, String> labels, KafkaTraceSpan span) {}

    /**
     * 业务作用：创建独立观测执行域。
     * <p>
     * 返回：已启动导出线程，尚无业务观测。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param source           稳定源名
     * @param sink             可选指标接收后端，负责导出固定指标合同。
     * @param traceSink        可选trace接收后端，与指标出口共用有界事件队列。
     * @param sampleTraceRoots 消费根采样决策
     * @param options          共同有界预算。
     */
    public MetricsExporter(String source, MetricsSink sink, KafkaTraceSink traceSink, boolean sampleTraceRoots, MetricsOptions options) {
        this.source = source;
        this.options = Objects.requireNonNull(options);
        this.traceSink = traceSink;
        this.sampleTraceRoots = sampleTraceRoots;
        metricEnabled = sink != null;
        if (sink == null && traceSink == null) throw new IllegalArgumentException("观测出口不能为空");
        worker = Thread.ofPlatform().daemon().name("kafka-metrics").start(() -> run(sink));
    }

    /**
     * 业务作用：非等待地接纳一条受限观测。
     * <p>
     * 返回：入队或累计丢弃，不执行用户代码。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param name    本次导出的稳定指标名称。
     * @param value   本次导出的增量或瞬时数值。
     * @param counter 区分增量或瞬时值
     * @param labels  受控身份。
     */
    public void emit(String name, long value, boolean counter, Map<String, String> labels) {
        if (!metricEnabled) return;
        if (counter && value == 0) return;
        long bytes = name.getBytes(StandardCharsets.UTF_8).length + 6L + source.getBytes(StandardCharsets.UTF_8).length;
        for (var entry : labels.entrySet())
            bytes += entry.getKey().getBytes(StandardCharsets.UTF_8).length + (long) entry.getValue().getBytes(StandardCharsets.UTF_8).length;
        if (labels.size() > 7 || labels.containsKey("source") || bytes > options.maxEventBytes() || !gate.tryLock()) {
            dropped.incrementAndGet();
            return;
        }
        try {
            if (!accepting || queue.size() >= options.queueCapacity()) {
                dropped.incrementAndGet();
                return;
            }
            Map<String, String> owned = new LinkedHashMap<>(labels);
            owned.put("source", source);
            queue.addLast(new Event(name, value, counter, Map.copyOf(owned), null));
            queued.incrementAndGet();
            wake.signal();
        } finally {
            gate.unlock();
        }
    }

    /**
     * 业务作用：将完成的有限 span 送入同一观测预算，绝不在业务线程调用遥测后端。
     * <p>
     * 返回：超限、竞争、拥塞或关闭计入丢弃；实际回调纳入统一关闭责任。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param span 已采样的不可变记录。
     */
    public void trace(KafkaTraceSpan span) {
        if (traceSink == null) return;
        long bytes = 256L + source.getBytes(StandardCharsets.UTF_8).length + span.group().getBytes(StandardCharsets.UTF_8).length + span.topic().getBytes(StandardCharsets.UTF_8).length + 56L * span.links().size();
        if (bytes > options.maxEventBytes() || !gate.tryLock()) {
            dropped.incrementAndGet();
            return;
        }
        try {
            if (!accepting || queue.size() >= options.queueCapacity()) {
                dropped.incrementAndGet();
                return;
            }
            queue.addLast(new Event("", 0, false, Map.of(), span));
            queued.incrementAndGet();
            wake.signal();
        } finally {
            gate.unlock();
        }
    }

    /**
     * 业务作用：独立调用用户后端并隔离其异常。
     * <p>
     * 返回：关闭后退出，阻塞回调仍需等待真实返回。
     *
     * @param sink 应用后端。
     */
    private void run(MetricsSink sink) {
        try {
            while (true) {
                Event event;
                gate.lock();
                try {
                    while (accepting && queue.isEmpty()) wake.await();
                    if (!accepting) return;
                    event = queue.removeFirst();
                    queued.decrementAndGet();
                    active.set(1);
                } finally {
                    gate.unlock();
                }
                // 用户后端在锁外执行；无论阻塞或异常，都不能阻止消费线程放弃当前观测准入。
                try {
                    if (event.span != null) traceSink.export(event.span);
                    else if (event.counter) sink.counter(event.name, event.value, event.labels);
                    else sink.gauge(event.name, event.value, event.labels);
                    exported.incrementAndGet();
                } catch (Throwable ignored) {
                    failed.incrementAndGet();
                } finally {
                    active.set(0);
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            close();
        }
    }

    /**
     * 业务作用：停止新观测并释放未执行事件。
     * <p>
     * 参数说明：无。
     * 返回：排队事件计为丢弃，正在执行的用户回调不被伪装为已取消。
     */
    void close() {
        gate.lock();
        try {
            // 先关闭准入，再清队列；源关闭过程中不能重新激活导出。
            accepting = false;
            dropped.addAndGet(queue.size());
            queue.clear();
            queued.set(0);
            wake.signalAll();
        } finally {
            gate.unlock();
        }
    }

    /**
     * 业务作用：等待观测执行责任实际归还。
     * <p>
     * 返回：线程实际退出才成功；超时或中断保留出口状态供再次关闭。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param deadline 源共用的单调纳秒期限。
     */
    public void closeUntil(long deadline) {
        close();
        try {
            if (worker.isAlive()) {
                long remaining = deadline - System.nanoTime();
                if (remaining > 0) worker.join(Duration.ofNanos(remaining));
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "指标出口关闭等待被中断");
        }
        if (worker.isAlive()) throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "指标出口仍有实际执行责任");
    }

    /**
     * 业务作用：后端不可用时仍能观察导出责任。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 计数和线程实际终态，不调用用户后端。
     */
    public MetricsExportStats stats() {
        return new MetricsExportStats(true, queued.get(), active.get(), exported.get(), dropped.get(), failed.get(), accepting, !worker.isAlive());
    }

    /**
     * 业务作用：让消费owner跳过没有接收端的指标聚合。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 是否配置指标后端，与trace采样相互独立。
     */
    public boolean metricsEnabled() {
        return metricEnabled;
    }
}

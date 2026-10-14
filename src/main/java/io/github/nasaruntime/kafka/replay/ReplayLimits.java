package io.github.nasaruntime.kafka.replay;

/**
 * 回放独立的速率与实际在途责任预算；速率零表示不节流，容量始终为正。
 */
public record ReplayLimits(long recordsPerSecond, long bytesPerSecond, int maxInFlightRecords,
                           long maxInFlightBytes, int concurrency) {

    /**
     * 业务作用：阻止任务无限占用执行资源，并区分速率与驻留容量。
     * <p>
     * 返回：合法且可共同编码的预算；容量不足或并行超额拒绝。
     *
     * @param recordsPerSecond   全任务业务处理准入速率，每秒记录数；零表示不节流，不限制 broker 原生预取速率。
     * @param bytesPerSecond     全任务按记录计费大小计算的处理准入速率；包含 key、payload、头与固定元数据额度，零表示不节流。
     * @param maxInFlightRecords 实际在途记录责任的数量上限，范围1至1000000。
     * @param maxInFlightBytes   同时持有的记录计费字节上限，包含原文与固定元数据额度，不等于 native 缓冲及 JVM 堆占用。
     * @param concurrency        并行处理的来源分区数，范围1至256且不超过记录容量。
     */
    public ReplayLimits {
        if (recordsPerSecond < 0 || bytesPerSecond < 0 || maxInFlightRecords < 1 || maxInFlightRecords > 1_000_000
                || maxInFlightBytes < 1 || concurrency < 1 || concurrency > 256 || concurrency > maxInFlightRecords)
            ReplayObservation.invalid();
    }

    /**
     * 业务作用：提供独立于在线消费的保守回放预算。
     * <p>
     * 参数说明：无。
     *
     * @return 每秒 1000 条及 16 MiB、最多 256 条及 16 MiB 在途、单分区执行。
     */
    public static ReplayLimits defaults() {
        return new ReplayLimits(1000, 16L * 1024 * 1024, 256, 16L * 1024 * 1024, 1);
    }
}

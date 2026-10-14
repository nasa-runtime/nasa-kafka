package io.github.nasaruntime.kafka.consumer;

/**
 * 就绪诊断保留原要求和同次健康观测；快照不能作为后续控制操作的权威。
 */
public record ReadyStatus(ReadyRequirement requirement, GroupHealth health, Reason reason, long observedAtNanos) {

    /**
     * 跨语言对应的就绪原因；状态阻断优先于要求未覆盖，USER 暂停不改变分配事实。
     */
    public enum Reason {
        READY, STARTING, JOINING, NOT_JOINED, RECOVERING, RETRYING, COMMIT_BLOCKED,
        DLT_BACKPRESSURE, PROGRESS_BACKPRESSURE, DEGRADED, CRASHED, STOPPING, STOPPED,
        INSUFFICIENT_PARTITIONS, MISSING_TOPICS, MISSING_PARTITIONS
    }

    /**
     * 业务作用：以同一健康快照计算稳定原因，避免原因与分配来自不同轮查询。
     *
     * @param requirement 业务要求
     * @param health      当前观测。
     * @return 带观测时刻的诊断。
     */
    public static ReadyStatus evaluate(ReadyRequirement requirement, GroupHealth health) {
        return new ReadyStatus(requirement, health, requirement.reason(health), System.nanoTime());
    }

    /**
     * 业务作用：供接流量门禁使用诊断结果。
     * <p>
     * 参数说明：无。
     *
     * @return 仅本次要求已满足时为真。
     */
    public boolean ready() {
        return reason == Reason.READY;
    }
}

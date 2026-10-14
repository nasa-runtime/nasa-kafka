package io.github.nasaruntime.kafka.lifecycle;

import java.util.List;

/**
 * 源关闭的只读责任快照；资源退出、broker 确认和业务成功是独立事实。计数不包含进程崩溃前未持久化的历史。metrics字段共同包含指标与trace出口责任。
 */
public record CloseReport(String sourceId, boolean stopRequested, boolean resourcesClosed,
                          WaitOutcome waitOutcome, boolean deadlineExpired, Consumers consumers,
                          List<String> pendingGroups, int pendingGroupCount, List<Lane> lanes,
                          int replayTasks, int transferDependencies, int transactionTasks,
                          int dltActiveTasks, long dltRecords, long dltBytes,
                          ResourceState admin, int auxiliaryResourcesActive,
                          int metricsQueued, int metricsActive, boolean metricsClosed) {
    /**
     * 本次 API 是否等待关闭；INCOMPLETE 不撤销已经发布的停止意图。
     */
    public enum WaitOutcome {NOT_WAITED, COMPLETED, INCOMPLETE}

    /**
     * 原生管理资源是否构造、开放或仍有关闭责任。
     */
    public enum ResourceState {NOT_CREATED, OPEN, CLOSING, CLOSED}

    /**
     * 普通消费的当前实际执行和停止期间释放的未确认观察数；释放次数不是跨 epoch 去重后的业务事件数。普通关闭不探测具体记录存活，returnedForRedelivery 保持零；各计数描述不同责任，不能相加当作事件总数。
     */
    public record Consumers(int activeOwners, int activeHandlers, long retainedRecords, long retainedBytes,
                            long releasedUnverifiedRecords, long returnedForRedelivery) {}

    /**
     * pendingDelivery 尚无本地终态；terminalDeliveryUnknown 是本源寿命内远端结果不明的累计终态，实际关闭不会将其清零。
     */
    public record Lane(String name, long pendingDelivery, long terminalDeliveryUnknown, int liveNative, boolean closed) {}

    /**
     * 业务作用：冻结关闭证据，防止调用方修改名单后仍沿用同一结论。
     * <p>
     * 返回：独立不可变快照，不授予重试、提交或重建权限。
     *
     * @param sourceId                 本次关闭报告所属的命名数据源。
     * @param stopRequested            是否已经发布停止消费的意图，不表示资源已经退出。
     * @param resourcesClosed          全部受管资源是否已实际完成关闭，与业务成功相互独立。
     * @param waitOutcome              本次API等待关闭的结局，不撤销此前停止意图。
     * @param deadlineExpired          生成报告时，本次关闭等待的期限是否已耗尽。
     * @param consumers                普通消费owner、实际调用和保留输入责任的汇总快照。
     * @param pendingGroups            尚未关闭的消费组名称，源关闭采样最多列64项，构造时复制为不可变列表。
     * @param pendingGroupCount        尚未关闭的消费组完整数量，不受诊断名单长度限制。
     * @param lanes                    各发布通道的待交付、未知结果和原生资源快照，构造时冻结列表。
     * @param replayTasks              尚未退出的有限回放任务数量。
     * @param transferDependencies     尚未释放的跨集群转发依赖登记数量。
     * @param transactionTasks         尚未退出的同集群事务任务数量。
     * @param dltActiveTasks           当前仍占用普通DLT任务额度的实际任务数量。
     * @param dltRecords               普通DLT责任当前保留的记录数量。
     * @param dltBytes                 普通DLT责任当前保留的计费字节数。
     * @param admin                    该源原生Admin资源的构造、开放或关闭状态。
     * @param auxiliaryResourcesActive 尚未退出的临时metadata查询资源数量。
     * @param metricsQueued            指标与trace出口当前排队的事件数量。
     * @param metricsActive            指标与trace后端当前仍在执行的调用数量。
     * @param metricsClosed            指标与trace出口的worker是否已经实际退出。
     */
    public CloseReport {
        pendingGroups = List.copyOf(pendingGroups);
        lanes = List.copyOf(lanes);
    }
}

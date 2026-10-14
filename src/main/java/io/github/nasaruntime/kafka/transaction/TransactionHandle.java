package io.github.nasaruntime.kafka.transaction;


import java.time.Duration;

/**
 * 专属事务消费 owner 的观测与生命周期句柄。
 */
public final class TransactionHandle {

    private final KafkaTransactionTask task;

    /**
     * 业务作用：绑定真实执行实例，不按名称查找可被替换的 owner。
     * <p>
     * 返回：稳定控制句柄。
     *
     * @param task 已登记任务。
     */
    TransactionHandle(KafkaTransactionTask task) {
        this.task = task;
    }

    /**
     * 业务作用：观察事务阶段、当前权威、确认计数和失败输入范围。
     * <p>
     * 参数说明：无。
     *
     * @return 不触发网络访问的快照。
     */
    public TransactionStatus status() {
        return task.status();
    }

    /**
     * 业务作用：关闭新批次准入并撤销尚未进入提交的批次权威。
     * <p>
     * 参数说明：无。
     *
     * @return 即时快照；在途 commit 仍等待真实结果。
     */
    public TransactionStatus cancel() {
        return task.cancel();
    }

    /**
     * 业务作用：等待当前初始化与分配同时满足接流量条件。
     *
     * @param timeout 总等待预算。
     * @return 当次 Ready 快照；超时、取消或失败按实际状态拒绝。
     */
    public TransactionStatus awaitReady(Duration timeout) {
        return task.awaitReady(timeout);
    }

    /**
     * 业务作用：等待回调、事务 producer 和 consumer 实际退出。
     *
     * @param timeout 非负总等待预算。
     * @return 实际退出为 true；超时不丢弃旧 owner 责任。
     */
    public boolean awaitDrained(Duration timeout) {
        return task.awaitDrained(timeout);
    }
}

package io.github.nasaruntime.kafka.replay;


import java.time.Duration;

/**
 * 单次回放执行实例的控制权；旧句柄不能控制同名恢复任务。
 */
public final class ReplayHandle {

    private final KafkaReplayTask task;

    /**
     * 业务作用：绑定本次任务实例，防止同名恢复造成控制串扰。
     * <p>
     * 返回：无独立资源的句柄。
     *
     * @param task 本次实际责任。
     */
    ReplayHandle(KafkaReplayTask task) {
        this.task = task;
    }

    /**
     * 业务作用：关闭后续处理准入，已经开始的处理仍可确认并落盘。
     * <p>
     * 参数说明：无。
     *
     * @return 即时快照，PAUSING 不代表在途处理已经结束。
     */
    public ReplayStatus pause() {
        return task.pause(true);
    }

    /**
     * 业务作用：解除本实例的用户暂停，取消或失败后不得重新开放。
     * <p>
     * 参数说明：无。
     *
     * @return 即时快照；终态保持不变。
     */
    public ReplayStatus resume() {
        return task.pause(false);
    }

    /**
     * 业务作用：永久停止本次执行的新准入，不用中断模拟实际处理退出。
     * <p>
     * 参数说明：无。
     *
     * @return STOPPING 或已排干终态；目标结果已成功的在途处理仍先落盘。
     */
    public ReplayStatus cancel() {
        return task.cancel();
    }

    /**
     * 业务作用：读取持久进度和实际责任数量而不访问 broker。
     * <p>
     * 参数说明：无。
     *
     * @return 不授予提交权的即时快照。
     */
    public ReplayStatus status() {
        return task.status();
    }

    /**
     * 业务作用：在调用方预算内等待全部实际责任、客户端和 lease 收口。
     *
     * @param timeout 非负等待预算。
     * @return 终态时 true；超时为 false 且任务责任继续保留。
     */
    public boolean awaitDrained(Duration timeout) {
        return task.awaitDrained(timeout);
    }
}

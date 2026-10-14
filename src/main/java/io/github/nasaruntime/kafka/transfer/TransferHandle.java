package io.github.nasaruntime.kafka.transfer;

import io.github.nasaruntime.kafka.replay.ReplayHandle;


import java.time.Duration;

/**
 * 绑定一次实际单目标转发；长期保留旧句柄不会控制同名后续任务。
 */
public final class TransferHandle {

    private final ReplayHandle replay;
    private final KafkaTransferProcessor processor;

    /**
     * 业务作用：把来源控制与目标失败/交付证据组合到同一实例。
     * <p>
     * 返回：无独立线程的控制句柄。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param replay    实际来源任务
     * @param processor 目标执行责任。
     */
    public TransferHandle(ReplayHandle replay, KafkaTransferProcessor processor) {
        this.replay = replay;
        this.processor = processor;
    }

    /**
     * 业务作用：停止接纳新记录，已经开始的目标发送继续等待实际结果。
     * <p>
     * 参数说明：无。
     *
     * @return 即时暂停阶段。
     */
    public TransferStatus pause() {
        replay.pause();
        return status();
    }

    /**
     * 业务作用：恢复尚未停止的任务，不重算冻结范围。
     * <p>
     * 参数说明：无。
     *
     * @return 即时阶段。
     */
    public TransferStatus resume() {
        replay.resume();
        return status();
    }

    /**
     * 业务作用：永久停止本实例的新转发，不用取消等待冒充目标交付终结。
     * <p>
     * 参数说明：无。
     *
     * @return 停止阶段或已排干终态。
     */
    public TransferStatus cancel() {
        replay.cancel();
        return status();
    }

    /**
     * 业务作用：无I/O读取持久来源进度和目标实际交付证据。
     * <p>
     * 参数说明：无。
     *
     * @return 当前脱敏快照，目标确认可领先于持久进度。
     */
    public TransferStatus status() {
        return new TransferStatus(replay.status(), processor.failure(), processor.delivered());
    }

    /**
     * 业务作用：等待来源、目标实际交付与lease退出，超时不归还任务责任。
     *
     * @param timeout 非负等待预算。
     * @return 真实排干时true。
     */
    public boolean awaitDrained(Duration timeout) {
        return replay.awaitDrained(timeout);
    }
}

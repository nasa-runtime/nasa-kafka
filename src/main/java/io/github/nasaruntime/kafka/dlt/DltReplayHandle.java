package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.replay.ReplayHandle;


import java.time.Duration;

/**
 * 绑定一次DLT执行实例，保留旧句柄不能控制后续同名任务。
 */
public final class DltReplayHandle {

    private final ReplayHandle replay;
    private final KafkaDltReplayProcessor processor;

    /**
     * 业务作用：组合实际来源控制与DLT处理结果。
     * <p>
     * 返回：不创建独立线程的句柄。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param replay    来源任务
     * @param processor 目标责任。
     */
    public DltReplayHandle(ReplayHandle replay, KafkaDltReplayProcessor processor) {
        this.replay = replay;
        this.processor = processor;
    }

    /**
     * 业务作用：暂停新记录准入，已经接纳的目标交付继续持有责任。
     * <p>
     * 参数说明：无。
     *
     * @return 即时状态。
     */
    public DltReplayStatus pause() {
        replay.pause();
        return status();
    }

    /**
     * 业务作用：恢复尚未停止的执行，不重算来源范围。
     * <p>
     * 参数说明：无。
     *
     * @return 即时状态。
     */
    public DltReplayStatus resume() {
        replay.resume();
        return status();
    }

    /**
     * 业务作用：永久停止本实例的新准入，不取消在途目标结果等待。
     * <p>
     * 参数说明：无。
     *
     * @return 停止中或实际终态。
     */
    public DltReplayStatus cancel() {
        replay.cancel();
        return status();
    }

    /**
     * 业务作用：无需网络读取来源进度和DLT处理结局。
     * <p>
     * 参数说明：无。
     *
     * @return 本实例脱敏快照。
     */
    public DltReplayStatus status() {
        return processor.status(replay.status());
    }

    /**
     * 业务作用：等待实际任务和lease退出，超时不解除后台责任。
     *
     * @param timeout 非负等待预算。
     * @return 实际排干时true。
     */
    public boolean awaitDrained(Duration timeout) {
        return replay.awaitDrained(timeout);
    }
}

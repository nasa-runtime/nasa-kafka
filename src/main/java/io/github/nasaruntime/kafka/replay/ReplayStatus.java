package io.github.nasaruntime.kafka.replay;

import io.github.nasaruntime.kafka.checkpoint.ReplayCheckpoint;

import io.github.nasaruntime.kafka.checkpoint.CheckpointException;

/**
 * 即时任务责任快照；checkpoint 只包含已确认持久化进度。
 */
public record ReplayStatus(String taskId, State state, ReplayCheckpoint checkpoint, ReplayException.Code failure,
                           CheckpointException.Code checkpointFailure, int inFlightRecords, long inFlightBytes,
                           long processedRecords, int workers) {

    public enum State {STARTING, RUNNING, PAUSING, PAUSED, STOPPING, COMPLETED, CANCELLED, FAILED}

    /**
     * 业务作用：区分停止准入与实际责任、客户端和 lease 已经全部退出。
     * <p>
     * 参数说明：无。
     *
     * @return 仅三种终态为 true。
     */
    public boolean drained() {
        return state == State.COMPLETED || state == State.CANCELLED || state == State.FAILED;
    }
}

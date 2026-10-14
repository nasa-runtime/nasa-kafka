package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.replay.ReplayStatus;


/**
 * 三种计数只属于本执行实例；持久进度以replay为准，目标确认允许领先于checkpoint。
 */
public record DltReplayStatus(ReplayStatus replay, DltReplayException.Code failure, long forwardedRecords,
                              long skippedRecords, long quarantinedRecords) {
    /**
     * 业务作用：区分停止请求与实际来源、目标交付和lease全部退出。
     * <p>
     * 参数说明：无。
     *
     * @return 真实排干才为true。
     */
    public boolean drained() {
        return replay.drained();
    }
}

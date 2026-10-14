package io.github.nasaruntime.kafka.transfer;

import io.github.nasaruntime.kafka.replay.ReplayStatus;


/**
 * replay包含持久进度，deliveredRecords只统计目标真实确认，不等同于已持久checkpoint的条数。
 */
public record TransferStatus(ReplayStatus replay, TransferException.Code failure, long deliveredRecords) {

    /**
     * 业务作用：让管理端判断双方依赖与实际任务是否已经退出。
     * <p>
     * 参数说明：无。
     *
     * @return 来源实际任务进入终态时true，停止请求或等待超时不是终态。
     */
    public boolean drained() {
        return replay.drained();
    }
}

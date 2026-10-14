package io.github.nasaruntime.kafka.observability.metrics;

/**
 * 不依赖用户后端的指标与trace共用出口快照，各计数从源构造起累计。
 *
 * @param enabled      是否安装后端
 * @param queued       尚未执行的事件数
 * @param active       尚未实际返回的用户回调数，只可能零或一
 * @param exported     用户回调正常返回的事件数，不证明远端持久保存
 * @param dropped      容量、锁竞争、超限或关闭造成的丢弃数
 * @param failed       用户回调抛出异常的事件数
 * @param accepting    是否仍接受观测
 * @param workerExited 导出线程是否实际退出，未安装后端时为 true
 */
public record MetricsExportStats(boolean enabled, int queued, int active, long exported, long dropped, long failed,
                                 boolean accepting, boolean workerExited) {
    public static final MetricsExportStats DISABLED = new MetricsExportStats(false, 0, 0
            , 0, 0, 0, false, true);
}

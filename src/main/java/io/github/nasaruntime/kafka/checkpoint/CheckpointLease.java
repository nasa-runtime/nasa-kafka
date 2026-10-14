package io.github.nasaruntime.kafka.checkpoint;


import java.util.List;

/**
 * 单任务进度推进权；只有实际在途副作用全部退出后，回放运行时才能关闭此责任。
 */
public interface CheckpointLease extends AutoCloseable {

    /**
     * 业务作用：读取当前 lease 已确认持久化的进度。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变快照；关闭后保留的快照不再代表推进权限。
     */
    ReplayCheckpoint checkpoint();

    /**
     * 业务作用：在新业务副作用前复验当前所有权与最后持久快照，不以历史读取授权继续执行。
     * <p>
     * 参数说明：无。
     * 返回：当前可证明的推进权仍有效时成功；关闭、失权或证据不足拒绝，并禁止本 lease 后续推进；检查与远端副作用不构成原子操作。
     */
    void verify();

    /**
     * 业务作用：在目标成功后以所有权代次和更新序号条件推进连续进度。
     *
     * @param expectedRevision 调用方观察到的序号
     * @param positions        保持原范围的完整分区进度。
     * @return 持久化成功后的新快照；回退、越界、旧序号、失权或写入未知均拒绝继续推进。
     */
    ReplayCheckpoint advance(long expectedRevision, List<ReplayCheckpoint.Position> positions);

    /**
     * 业务作用：归还任务独占责任并永久关闭旧 lease 的推进入口。
     * <p>
     * 参数说明：无。
     * 返回：重复关闭无副作用；调用方必须先证明在途业务已退出，不能以关闭撤销远端写入。
     */
    @Override
    void close();
}

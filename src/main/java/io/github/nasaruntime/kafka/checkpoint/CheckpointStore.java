package io.github.nasaruntime.kafka.checkpoint;


import java.util.Optional;

/**
 * 回放存储合同；共享后端必须提供原子所有权获取、条件更新及成功回执对应的真实持久化。
 */
public interface CheckpointStore {

    /**
     * 业务作用：只读查看已持久化进度，不取得推进权限。
     *
     * @param taskId 稳定任务名。
     * @return 当前完整快照或不存在；读到快照不能授权业务副作用。
     */
    Optional<ReplayCheckpoint> load(String taskId);

    /**
     * 业务作用：取得任务独占责任并持久化新的所有权代次。
     *
     * @param initial generation/revision 均为零且所有进度位于起点的冻结声明。
     * @return 仅持久化成功后交付 lease；任务正在运行、合同改变或证据损坏均拒绝。
     */
    CheckpointLease claim(ReplayCheckpoint initial);
}

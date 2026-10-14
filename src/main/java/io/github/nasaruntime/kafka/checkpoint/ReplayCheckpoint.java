package io.github.nasaruntime.kafka.checkpoint;


import java.util.*;

/**
 * 冻结任务合同与逐分区进度；nextOffset 只表示已满足任务成功条件的连续范围。
 */
public record ReplayCheckpoint(String taskId, String contractFingerprint, long generation, long revision,
                               List<Position> positions) {

    public static final int MAX_PARTITIONS = 4096;

    /**
     * 单分区的不可变读取范围和下一次恢复坐标，范围采用 [startOffset, endOffset)。
     */
    public record Position(String topic, int partition, long startOffset, long endOffset, long nextOffset) {
        /**
         * 业务作用：冻结可恢复范围并阻止进度越过授权结束边界。
         * <p>
         * 返回：合法范围；非法名称、负值或越界进度拒绝。
         *
         * @param topic       冻结读取范围所属的来源topic。
         * @param partition   该范围所属的非负来源分区号。
         * @param startOffset 冻结范围的包含式起点，恢复时不重新计算。
         * @param endOffset   冻结范围的排他终点，不能越过该边界推进进度。
         * @param nextOffset  下一待处理坐标。
         */
        public Position {
            if (topic == null || !topic.matches("[A-Za-z0-9._-]{1,249}") || topic.equals(".") || topic.equals("..")
                    || partition < 0 || startOffset < 0 || endOffset < startOffset || nextOffset < startOffset || nextOffset > endOffset)
                throw new CheckpointException(CheckpointException.Code.INVALID_CONFIGURATION);
        }
    }

    /**
     * 业务作用：固定任务身份、所有权代次和有界分区进度，建立双端一致的排序。
     * <p>
     * 返回：独立不可变快照；重复坐标、非法序号或超额集合拒绝。
     *
     * @param taskId              稳定任务名
     * @param contractFingerprint 合同 SHA-256 小写十六进制
     * @param generation          持久checkpoint所有权代次，用于拒绝旧持有者写入。
     * @param revision            同一checkpoint内已持久化更新的序号。
     * @param positions           完整分区集合。
     */
    public ReplayCheckpoint {
        if (taskId == null || !taskId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                || contractFingerprint == null || !contractFingerprint.matches("[0-9a-f]{64}")
                || generation < 0 || revision < 0 || positions == null || positions.isEmpty() || positions.size() > MAX_PARTITIONS)
            throw new CheckpointException(CheckpointException.Code.INVALID_CONFIGURATION);
        List<Position> ordered = new ArrayList<>(positions);
        if (ordered.contains(null)) throw new CheckpointException(CheckpointException.Code.INVALID_CONFIGURATION);
        ordered.sort(Comparator.comparing(Position::topic).thenComparingInt(Position::partition));
        for (int i = 1; i < ordered.size(); i++) {
            Position previous = ordered.get(i - 1), current = ordered.get(i);
            if (previous.topic().equals(current.topic()) && previous.partition() == current.partition())
                throw new CheckpointException(CheckpointException.Code.INVALID_CONFIGURATION);
        }
        positions = List.copyOf(ordered);
    }

    /**
     * 业务作用：判断冻结范围是否已经全部形成持久进度。
     * <p>
     * 参数说明：无。
     *
     * @return 每个 nextOffset 均到达 endOffset 时为 true；不要求日志实际存在 endOffset-1 的记录。
     */
    public boolean complete() {
        return positions.stream().allMatch(position -> position.nextOffset() == position.endOffset());
    }

    /**
     * 业务作用：验证恢复声明未改变来源范围或任务处理合同。
     *
     * @param other 待比较的冻结声明或已保存快照。
     * @return 只忽略所有权、更新序号和当前进度，任何身份或范围差异均为 false。
     */
    public boolean sameContract(ReplayCheckpoint other) {
        if (other == null || !taskId.equals(other.taskId) || !contractFingerprint.equals(other.contractFingerprint) || positions.size() != other.positions.size())
            return false;
        for (int i = 0; i < positions.size(); i++) {
            Position left = positions.get(i), right = other.positions.get(i);
            if (!left.topic.equals(right.topic) || left.partition != right.partition || left.startOffset != right.startOffset || left.endOffset != right.endOffset)
                return false;
        }
        return true;
    }
}

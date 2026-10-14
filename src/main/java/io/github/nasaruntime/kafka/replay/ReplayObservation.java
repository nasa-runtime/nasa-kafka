package io.github.nasaruntime.kafka.replay;

import io.github.nasaruntime.kafka.checkpoint.ReplayCheckpoint;

import io.github.nasaruntime.kafka.checkpoint.CheckpointException;

import java.util.*;

/**
 * 不加入消费组的只读范围证据；stableEndOffset 必须来自 read_committed 的 ListOffsets。
 */
public record ReplayObservation(String clusterId, Long timestampMillis, List<Partition> partitions) {

    /**
     * 单分区当前保留起点、LSO 和可选时间定位结果。
     */
    public record Partition(String topic, int partition, long beginningOffset, long stableEndOffset,
                            Long timestampOffset) {
        /**
         * 业务作用：拒绝不完整或相互矛盾的 broker 范围证据。
         * <p>
         * 返回：合法证据；时间结果可位于冻结 end 之后，由规划显式截断。
         *
         * @param topic           本次只读范围观测的来源topic。
         * @param partition       本次范围观测的来源分区号。
         * @param beginningOffset 保留起点
         * @param stableEndOffset LSO
         * @param timestampOffset 时间查询结果或无结果。
         */
        public Partition {
            try {
                new ReplayCheckpoint.Position(topic, partition, beginningOffset, stableEndOffset, beginningOffset);
            } catch (CheckpointException failure) {
                throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
            }
            if (timestampOffset != null && timestampOffset < 0)
                throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
        }
    }

    /**
     * 业务作用：冻结一个有界且无重复坐标的只读观测。
     * <p>
     * 返回：排序后的独立不可变证据；非法或重复项拒绝。
     *
     * @param clusterId       真实物理身份
     * @param timestampMillis 用于定位消息的目标 Unix 毫秒时间，null 表示未请求时间定位；不是观测发起时刻。
     * @param partitions      完整所选 topic 分区集合。
     */
    public ReplayObservation {
        token(clusterId, 1024);
        if (timestampMillis != null && timestampMillis < 0 || partitions == null || partitions.isEmpty()
                || partitions.size() > ReplayCheckpoint.MAX_PARTITIONS || partitions.stream().anyMatch(Objects::isNull))
            invalid();
        List<Partition> sorted = new ArrayList<>(partitions);
        sorted.sort(Comparator.comparing(Partition::topic).thenComparingInt(Partition::partition));
        for (int i = 0; i < sorted.size(); i++) {
            Partition current = sorted.get(i);
            if (timestampMillis == null && current.timestampOffset() != null) invalid();
            if (i > 0 && current.topic().equals(sorted.get(i - 1).topic()) && current.partition() == sorted.get(i - 1).partition())
                invalid();
        }
        partitions = List.copyOf(sorted);
    }

    /**
     * 业务作用：约束共同指纹中的不透明身份，避免语言编码差异或无界声明。
     * <p>
     * 返回：非空可见 ASCII 时通过，否则拒绝。
     *
     * @param value   身份文本
     * @param maximum 最大 ASCII 字节数。
     */
    static void token(String value, int maximum) {
        if (value == null || value.isEmpty() || value.length() > maximum || value.chars().anyMatch(ch -> ch < 33 || ch > 126))
            invalid();
    }

    /**
     * 业务作用：统一非法范围声明的稳定失败分类。
     * <p>
     * 参数说明：无。
     * 返回：始终抛出配置拒绝。
     */
    public static void invalid() {
        throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
    }
}

package io.github.nasaruntime.kafka.consumer;

import java.util.Set;
import java.util.Collection;

import org.apache.kafka.common.TopicPartition;

import java.util.stream.Collectors;

/**
 * broker 就绪要求；仅表示查询时满足条件，不构成永久分区权威。
 */
public record ReadyRequirement(Kind kind, int minimumPartitions, Set<String> topics, Set<TopicPartition> partitions) {

    public enum Kind {JOINED, ASSIGNED, ASSIGNED_TOPICS, ASSIGNED_PARTITIONS}

    public static final ReadyRequirement JOINED = new ReadyRequirement(Kind.JOINED, 0, Set.of());
    public static final ReadyRequirement ASSIGNED = assigned(1);

    /**
     * 业务作用：冻结并验证就绪条件。
     * <p>
     * 返回：合法不可变条件。
     *
     * @param kind              要求
     * @param minimumPartitions 最少分区
     * @param topics            覆盖集合
     * @param partitions        精确坐标集合。
     */
    public ReadyRequirement {
        if (kind == null || topics == null || partitions == null)
            throw new IllegalArgumentException("Ready 条件不能为空");
        topics = Set.copyOf(topics);
        partitions = Set.copyOf(partitions);
        if (kind == Kind.ASSIGNED && minimumPartitions <= 0 || kind == Kind.ASSIGNED_TOPICS && topics.isEmpty()
                || topics.stream().anyMatch(String::isBlank) || kind == Kind.ASSIGNED_PARTITIONS && partitions.isEmpty()
                || partitions.stream().anyMatch(tp -> tp.topic() == null || tp.topic().isBlank() || tp.partition() < 0))
            throw new IllegalArgumentException("Ready 分区数和 topic 条件非法");
    }

    /**
     * 业务作用：保留无需具体分区集合的条件构造入口。
     * <p>
     * 返回：冻结条件，具体分区要求须使用四参数入口。
     *
     * @param kind              当前就绪要求的判定类别。
     * @param minimumPartitions 至少需要有效分配的分区数量。
     * @param topics            需要取得有效分配证据的topic集合。
     */
    public ReadyRequirement(Kind kind, int minimumPartitions, Set<String> topics) {
        this(kind, minimumPartitions, topics, Set.of());
    }

    /**
     * 业务作用：要求至少拥有指定数量分区。
     *
     * @param count 正数。
     * @return 可跨语言对应的 Assigned 要求。
     */
    public static ReadyRequirement assigned(int count) {
        return new ReadyRequirement(Kind.ASSIGNED, count, Set.of());
    }

    /**
     * 业务作用：要求分配覆盖每个业务 topic。
     *
     * @param topics 非空集合。
     * @return AssignedTopics 要求。
     */
    public static ReadyRequirement assignedTopics(Collection<String> topics) {
        return new ReadyRequirement(Kind.ASSIGNED_TOPICS, 0, Set.copyOf(topics));
    }

    /**
     * 业务作用：要求当前 owner 集合确实拥有每个指定分区。
     *
     * @param partitions 非空合法坐标。
     * @return 与 topic 覆盖不同的精确分区条件。
     */
    public static ReadyRequirement assignedPartitions(Collection<TopicPartition> partitions) {
        return new ReadyRequirement(Kind.ASSIGNED_PARTITIONS, 0, Set.of(), Set.copyOf(partitions));
    }

    /**
     * 业务作用：将当前健康快照与业务接流量条件匹配。
     *
     * @param health 当前观测。
     * @return 只在运行、已 join 且满足分配要求时为真。
     */
    public boolean satisfiedBy(GroupHealth health) {
        return reason(health) == ReadyStatus.Reason.READY;
    }

    /**
     * 业务作用：给出当前不能接流量的首要稳定原因。
     *
     * @param health 当前观测。
     * @return 保护态优先于分配缺口，USER 暂停不撤销分配就绪。
     */
    public ReadyStatus.Reason reason(GroupHealth health) {
        ReadyStatus.Reason stateReason = switch (health.state()) {
            case STARTING -> ReadyStatus.Reason.STARTING;
            case JOINING -> ReadyStatus.Reason.JOINING;
            case RECOVERING -> ReadyStatus.Reason.RECOVERING;
            case RETRYING -> ReadyStatus.Reason.RETRYING;
            case COMMIT_BLOCKED -> ReadyStatus.Reason.COMMIT_BLOCKED;
            case DLT_BACKPRESSURE -> ReadyStatus.Reason.DLT_BACKPRESSURE;
            case PROGRESS_BACKPRESSURE -> ReadyStatus.Reason.PROGRESS_BACKPRESSURE;
            case DEGRADED, HALTED -> ReadyStatus.Reason.DEGRADED;
            case CRASHED -> ReadyStatus.Reason.CRASHED;
            case CLOSING -> ReadyStatus.Reason.STOPPING;
            case CLOSED -> ReadyStatus.Reason.STOPPED;
            case RUNNING -> ReadyStatus.Reason.READY;
        };
        // 保留 assignment 仅供诊断；保护状态和失权时绝不能用旧坐标开放流量。
        if (stateReason != ReadyStatus.Reason.READY) return stateReason;
        if (!health.joined()) return ReadyStatus.Reason.NOT_JOINED;
        return switch (kind) {
            case JOINED -> ReadyStatus.Reason.READY;
            case ASSIGNED ->
                    health.assignment().size() >= minimumPartitions ? ReadyStatus.Reason.READY : ReadyStatus.Reason.INSUFFICIENT_PARTITIONS;
            case ASSIGNED_TOPICS ->
                    health.assignment().stream().map(TopicPartition::topic).collect(Collectors.toSet()).containsAll(topics)
                            ? ReadyStatus.Reason.READY : ReadyStatus.Reason.MISSING_TOPICS;
            case ASSIGNED_PARTITIONS ->
                    health.assignment().containsAll(partitions) ? ReadyStatus.Reason.READY : ReadyStatus.Reason.MISSING_PARTITIONS;
        };
    }
}

package io.github.nasaruntime.kafka.replay;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.support.KafkaBrokerErrors;

import io.github.nasaruntime.kafka.checkpoint.CheckpointException;
import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.checkpoint.ReplayCheckpoint;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;

import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * 回放只读探测使用独立 ListOffsets 请求，不复用在线 group 或 high watermark 快照。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaReplayInspector {

    /**
     * 业务作用：禁止实例化无状态只读适配器。
     * <p>
     * 参数说明：无。
     * 返回：不创建资源。
     */
    private KafkaReplayInspector() {}

    /**
     * 业务作用：在一个总预算内冻结完整 topic 范围，并前后复核 metadata。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param owner     源
     * @param topics    白名单
     * @param timestamp 可选时间
     * @param timeout   总预算。
     * @return 稳定来源上的保留起点、LSO 和时间定位结果；缺失证据或期间身份变化拒绝。
     */
    public static ReplayObservation observe(KafkaProxy owner, Set<String> topics, Long timestamp, Duration timeout) {
        // 已关闭来源不能通过只读规划旁路重新创建客户端。
        if (owner.isClosed()) throw new ReplayException(ReplayException.Code.CLOSED);
        if (topics == null || topics.isEmpty() || topics.size() > ReplayCheckpoint.MAX_PARTITIONS || timestamp != null && timestamp < 0
                || timeout == null || timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofMinutes(5)) > 0)
            ReplayObservation.invalid();
        for (String topic : topics) {
            try {
                new ReplayCheckpoint.Position(topic, 0, 0, 0, 0);
            } catch (CheckpointException failure) {
                throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
            }
        }
        List<String> selected = new ArrayList<>(topics);
        Collections.sort(selected);
        long deadline = System.nanoTime() + timeout.toNanos();
        remainingMillis(deadline);
        Admin admin;
        try {
            admin = owner.nativeAdmin();
        } catch (KafkaFailure failure) {
            throw new ReplayException(failure.code() == KafkaFailure.Code.LIFECYCLE ? ReplayException.Code.CLOSED : ReplayException.Code.OBSERVATION_FAILED);
        }
        String cluster = await(admin.describeCluster(new DescribeClusterOptions().timeoutMs(remainingMillis(deadline))).clusterId(), deadline);
        String expected = owner.config().getExpectedClusterId();
        if (expected != null && !expected.equals(cluster))
            throw new ReplayException(ReplayException.Code.IDENTITY_CHANGED);
        var before = await(admin.describeTopics(selected, new DescribeTopicsOptions().timeoutMs(remainingMillis(deadline))).allTopicNames(), deadline);
        TreeMap<TopicPartition, OffsetSpec> query = new TreeMap<>(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
        for (String topic : selected) {
            TopicDescription description = before.get(topic);
            if (description == null || description.partitions().isEmpty())
                throw new ReplayException(ReplayException.Code.OBSERVATION_FAILED);
            for (var partition : description.partitions()) {
                if (query.size() >= ReplayCheckpoint.MAX_PARTITIONS) ReplayObservation.invalid();
                query.put(new TopicPartition(topic, partition.partition()), OffsetSpec.earliest());
            }
        }
        var begins = offsets(admin, query, deadline);
        query.replaceAll((tp, spec) -> OffsetSpec.latest());
        // READ_COMMITTED 的 latest 才是 LSO；未完成事务之后的历史尚未稳定，不能冻结到普通高水位。
        var ends = offsets(admin, query, deadline);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> times = Map.of();
        if (timestamp != null) {
            query.replaceAll((tp, spec) -> OffsetSpec.forTimestamp(timestamp));
            times = offsets(admin, query, deadline);
        }
        var after = await(admin.describeTopics(selected, new DescribeTopicsOptions().timeoutMs(remainingMillis(deadline))).allTopicNames(), deadline);
        for (String topic : selected) {
            TopicDescription left = before.get(topic), right = after.get(topic);
            // 同一查询期间发生删除重建或扩容时，不能拼接出看似完整但并不存在的范围快照。
            if (right == null || !left.topicId().equals(right.topicId()))
                throw new ReplayException(ReplayException.Code.IDENTITY_CHANGED);
            if (!left.partitions().stream().map(partition -> partition.partition()).sorted().toList().equals(
                    right.partitions().stream().map(partition -> partition.partition()).sorted().toList()))
                throw new ReplayException(ReplayException.Code.PARTITIONS_CHANGED);
        }
        String finalCluster = await(admin.describeCluster(new DescribeClusterOptions().timeoutMs(remainingMillis(deadline))).clusterId(), deadline);
        if (!Objects.equals(cluster, finalCluster)) throw new ReplayException(ReplayException.Code.IDENTITY_CHANGED);
        List<ReplayObservation.Partition> partitions = new ArrayList<>();
        for (TopicPartition tp : query.keySet()) {
            if (!begins.containsKey(tp) || !ends.containsKey(tp) || timestamp != null && !times.containsKey(tp))
                throw new ReplayException(ReplayException.Code.OBSERVATION_FAILED);
            Long at = timestamp == null || times.get(tp).offset() == -1 ? null : times.get(tp).offset();
            long beginning = begins.get(tp).offset(), end = ends.get(tp).offset();
            // broker 范围证据彼此矛盾是本轮不可观测，不能归咎于调用方配置，更不能据此建立恢复坐标。
            if (beginning < 0 || end < beginning || at != null && at < 0)
                throw new ReplayException(ReplayException.Code.OBSERVATION_FAILED);
            partitions.add(new ReplayObservation.Partition(tp.topic(), tp.partition(), beginning, end, at));
        }
        try {
            return new ReplayObservation(cluster, timestamp, partitions);
        } catch (ReplayException failure) {
            throw new ReplayException(ReplayException.Code.OBSERVATION_FAILED);
        }
    }

    /**
     * 业务作用：强制所有回放范围查询使用已提交隔离和剩余预算。
     *
     * @param admin    源管理客户端
     * @param query    完整请求
     * @param deadline 统一截止时刻。
     * @return 全体查询成功后的结果；不接受部分结果。
     */
    private static Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets(Admin admin, Map<TopicPartition, OffsetSpec> query, long deadline) {
        return await(admin.listOffsets(query, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED).timeoutMs(remainingMillis(deadline))).all(), deadline);
    }

    /**
     * 业务作用：使后续 metadata 和 offset 请求共用剩余预算。
     *
     * @param deadline 单调时钟截止值。
     * @return 向上取整的正毫秒数；预算耗尽不再发起请求。
     */
    private static int remainingMillis(long deadline) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) throw new ReplayException(ReplayException.Code.TIMEOUT);
        return (int) Math.max(1, (nanos + 999_999) / 1_000_000);
    }

    /**
     * 业务作用：把只读查询失败收敛为无敏感正文的分类。
     *
     * @param future   只读请求
     * @param deadline 共同截止时间。
     * @return 确认结果；中断保留线程标志，所有失败不产生消费位点副作用。
     */
    private static <T> T await(KafkaFuture<T> future, long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new ReplayException(ReplayException.Code.TIMEOUT);
        try {
            return future.get(remaining, TimeUnit.NANOSECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new ReplayException(ReplayException.Code.OBSERVATION_FAILED);
        } catch (TimeoutException failure) {
            throw new ReplayException(ReplayException.Code.TIMEOUT);
        } catch (ExecutionException failure) {
            if (KafkaBrokerErrors.authorization(failure)) throw new ReplayException(ReplayException.Code.AUTHORIZATION);
            if (failure.getCause() instanceof UnknownTopicOrPartitionException)
                throw new ReplayException(ReplayException.Code.TOPIC_MISSING);
            throw new ReplayException(failure.getCause() instanceof org.apache.kafka.common.errors.TimeoutException ? ReplayException.Code.TIMEOUT : ReplayException.Code.OBSERVATION_FAILED);
        }
    }
}

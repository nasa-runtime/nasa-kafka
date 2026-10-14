package io.github.nasaruntime.kafka.admin;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.support.KafkaBrokerErrors;


import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import static io.github.nasaruntime.kafka.admin.OffsetObservation.*;

import java.util.concurrent.TimeoutException;

/**
 * 只读位置查询的逐字段证据收集器，全部请求共用调用方期限。
 */
final class KafkaOffsetInspector {

    /**
     * 业务作用：禁止实例化无状态查询适配器。
     * <p>
     * 参数说明：无。
     * 返回：不创建资源。
     */
    private KafkaOffsetInspector() {}

    /**
     * 业务作用：保留每个目标已取得的位置证据，不让其它目标的失败抹去它。
     *
     * @param owner     源
     * @param group     可选组
     * @param targets   已校验目标
     * @param isolation 读取隔离
     * @param timestamp 可选时间
     * @param deadline  总截止时间。
     * @return 封存的逐字段报告，不改变消费组或资源。
     */
    static OffsetObservation observe(KafkaProxy owner, String group, List<TopicPartition> targets,
                                     Isolation isolation, Long timestamp, long deadline) {
        long started = System.currentTimeMillis();
        List<TopicPartition> ordered = targets.stream().sorted(Comparator.comparing(TopicPartition::topic)
                .thenComparingInt(TopicPartition::partition)).toList();
        Admin admin = owner.nativeAdmin();
        Map<TopicPartition, Missing> preflight = preflight(admin, ordered, deadline);
        Map<TopicPartition, Value> begins = offsets(admin, ordered, preflight, OffsetSpec.earliest(), isolation, false, deadline);
        Map<TopicPartition, Value> ends = offsets(admin, ordered, preflight, OffsetSpec.latest(), isolation, false, deadline);
        Map<TopicPartition, Value> committed = group == null ? missing(ordered, Missing.NOT_REQUESTED)
                : committed(admin, group, ordered, preflight, deadline);
        Map<TopicPartition, Value> times = timestamp == null ? missing(ordered, Missing.NOT_REQUESTED)
                : offsets(admin, ordered, preflight, OffsetSpec.forTimestamp(timestamp), isolation, true, deadline);
        List<Partition> partitions = new ArrayList<>(ordered.size());
        for (TopicPartition tp : ordered) {
            Value begin = begins.get(tp), end = ends.get(tp), commit = committed.get(tp);
            // 先拒绝矛盾范围再计算距离，不能把保留期竞态或非法响应当作可恢复证据。
            if (begin.value() != null && end.value() != null && begin.value() > end.value()) {
                begin = absent(Missing.INCONSISTENT);
                end = absent(Missing.INCONSISTENT);
            }
            Value lag = commit.value() == null ? new Value(null, commit.missing(), commit.observedAtMillis())
                    : end.value() == null ? new Value(null, end.missing(), end.observedAtMillis())
                      : new Value(Math.max(0, end.value() - commit.value()), null, Math.max(commit.observedAtMillis(), end.observedAtMillis()));
            partitions.add(new Partition(tp, begin, end, commit, times.get(tp), lag));
        }
        return new OffsetObservation(owner.sourceId(), group, isolation, started, System.currentTimeMillis(), partitions);
    }

    /**
     * 业务作用：用目标元数据区分明确缺失与暂时查询失败，禁止位置查询隐式创建 topic。
     *
     * @param admin    受管客户端
     * @param targets  目标
     * @param deadline 总截止时间。
     * @return 不能继续查询的逐分区原因，未列出的分区有本轮存在证据。
     */
    private static Map<TopicPartition, Missing> preflight(Admin admin, List<TopicPartition> targets, long deadline) {
        Map<TopicPartition, Missing> failures = new HashMap<>();
        Missing gate = gate(deadline);
        if (gate != null) {
            targets.forEach(tp -> failures.put(tp, gate));
            return failures;
        }
        Map<String, List<TopicPartition>> topics = new TreeMap<>();
        targets.forEach(tp -> topics.computeIfAbsent(tp.topic(), ignored -> new ArrayList<>()).add(tp));
        Map<String, KafkaFuture<TopicDescription>> queries;
        try {
            queries = admin.describeTopics(topics.keySet(), new DescribeTopicsOptions().timeoutMs(timeout(deadline))).topicNameValues();
        } catch (RuntimeException failure) {
            targets.forEach(tp -> failures.put(tp, classify(failure)));
            return failures;
        }
        for (String topic : topics.keySet()) {
            try {
                TopicDescription description = await(queries.get(topic), deadline);
                if (description == null || !topic.equals(description.name()))
                    throw new IllegalStateException("缺少目标元数据");
                Set<Integer> partitions = new HashSet<>();
                description.partitions().forEach(partition -> partitions.add(partition.partition()));
                for (TopicPartition tp : topics.get(topic))
                    if (!partitions.contains(tp.partition())) failures.put(tp, Missing.RESOURCE_MISSING);
            } catch (Exception failure) {
                for (TopicPartition tp : topics.get(topic)) failures.put(tp, classify(failure));
            }
        }
        return failures;
    }

    /**
     * 业务作用：按明确隔离级别读取位置，保留无匹配时间与失败的区别。
     *
     * @param admin     实际执行只读位置查询的原生Admin客户端。
     * @param targets   本轮请求的完整topic及分区集合。
     * @param preflight 查询前得到的逐目标资源或权限拒绝证据。
     * @param spec      位置类型
     * @param isolation 读取隔离
     * @param timestamp 表示允许无匹配
     * @param deadline  总期限。
     * @return 每个目标恰有有效非负位置或缺失原因。
     */
    private static Map<TopicPartition, Value> offsets(Admin admin, List<TopicPartition> targets,
                                                      Map<TopicPartition, Missing> preflight, OffsetSpec spec, Isolation isolation, boolean timestamp, long deadline) {
        Map<TopicPartition, Value> result = new LinkedHashMap<>();
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        for (TopicPartition tp : targets) {
            Missing reason = preflight.get(tp);
            if (reason == null) reason = gate(deadline);
            if (reason == null) request.put(tp, spec);
            else result.put(tp, absent(reason));
        }
        if (!request.isEmpty()) {
            try {
                // READ_COMMITTED 必须送入真实 ListOffsets，不能借用普通 high watermark 代替 LSO。
                IsolationLevel nativeIsolation = isolation == Isolation.READ_COMMITTED ? IsolationLevel.READ_COMMITTED : IsolationLevel.READ_UNCOMMITTED;
                ListOffsetsResult query = admin.listOffsets(request, new ListOffsetsOptions(nativeIsolation).timeoutMs(timeout(deadline)));
                for (TopicPartition tp : request.keySet()) {
                    try {
                        var value = await(query.partitionResult(tp), deadline);
                        result.put(tp, value == null ? absent(Missing.UNAVAILABLE)
                                : value.offset() >= 0 ? present(value.offset())
                                  : absent(timestamp && value.offset() == -1 ? Missing.NO_TIMESTAMP_MATCH : Missing.INCONSISTENT));
                    } catch (Exception failure) {
                        result.put(tp, absent(classify(failure)));
                    }
                }
            } catch (RuntimeException failure) {
                request.keySet().forEach(tp -> result.putIfAbsent(tp, absent(classify(failure))));
            }
        }
        return result;
    }

    /**
     * 业务作用：取得真实组提交证据而不加入该组，缺项不能充当无历史。
     *
     * @param admin     实际执行只读位置查询的原生Admin客户端。
     * @param group     组
     * @param targets   本轮请求的完整topic及分区集合。
     * @param preflight 查询前得到的逐目标资源或权限拒绝证据。
     * @param deadline  总期限。
     * @return 每个请求目标的提交位置或稳定缺失原因。
     */
    private static Map<TopicPartition, Value> committed(Admin admin, String group, List<TopicPartition> targets,
                                                        Map<TopicPartition, Missing> preflight, long deadline) {
        Map<TopicPartition, Value> result = new LinkedHashMap<>();
        List<TopicPartition> selected = new ArrayList<>();
        for (TopicPartition tp : targets) {
            Missing reason = preflight.get(tp);
            if (reason == null) reason = gate(deadline);
            if (reason == null) selected.add(tp);
            else result.put(tp, absent(reason));
        }
        if (!selected.isEmpty()) {
            try {
                Map<TopicPartition, OffsetAndMetadata> values = await(admin.listConsumerGroupOffsets(
                        Map.of(group, new ListConsumerGroupOffsetsSpec().topicPartitions(selected)),
                        new ListConsumerGroupOffsetsOptions().timeoutMs(timeout(deadline))).partitionsToOffsetAndMetadata(group), deadline);
                for (TopicPartition tp : selected) {
                    if (values == null || !values.containsKey(tp)) result.put(tp, absent(Missing.UNAVAILABLE));
                    else {
                        OffsetAndMetadata value = values.get(tp);
                        result.put(tp, value == null || value.offset() == -1 ? absent(Missing.NO_COMMITTED_OFFSET)
                                : value.offset() >= 0 ? present(value.offset()) : absent(Missing.INCONSISTENT));
                    }
                }
            } catch (Exception failure) {
                selected.forEach(tp -> result.put(tp, absent(classify(failure))));
            }
        }
        return result;
    }

    /**
     * 业务作用：为未请求或尚未取得的维度生成显式缺失结果。
     *
     * @param targets 目标
     * @param reason  缺失原因。
     * @return 完整目标映射，不补造零位点。
     */
    private static Map<TopicPartition, Value> missing(List<TopicPartition> targets, Missing reason) {
        Map<TopicPartition, Value> result = new LinkedHashMap<>();
        targets.forEach(tp -> result.put(tp, absent(reason)));
        return result;
    }

    /**
     * 业务作用：记录一个 broker 明确返回的位置。
     *
     * @param value 非负位置。
     * @return 带本地观测时刻的值。
     */
    private static Value present(long value) {
        return new Value(value, null, System.currentTimeMillis());
    }

    /**
     * 业务作用：记录无法取得某维度的原因。
     *
     * @param reason 稳定缺失类别。
     * @return 不含伪造位置的观测。
     */
    private static Value absent(Missing reason) {
        return new Value(null, reason, System.currentTimeMillis());
    }

    /**
     * 业务作用：在新请求前检查共同期限与线程中断，禁止预算耗尽后继续扩大查询责任。
     *
     * @param deadline 单调截止时间。
     * @return 允许继续时 null，否则为缺失原因。
     */
    private static Missing gate(long deadline) {
        return Thread.currentThread().isInterrupted() ? Missing.INTERRUPTED : System.nanoTime() >= deadline ? Missing.TIMEOUT : null;
    }

    /**
     * 业务作用：将剩余正预算传给原生请求，不按查询阶段重复取得完整期限。
     *
     * @param deadline 共同截止时间。
     * @return 至少一毫秒的剩余预算。
     */
    private static int timeout(long deadline) {
        return (int) Math.max(1, Math.min(Integer.MAX_VALUE, (deadline - System.nanoTime() + 999_999) / 1_000_000));
    }

    /**
     * 业务作用：等待单项只读证据，期限耗尽不撤销原生客户端的实际责任。
     *
     * @param future   目标结果
     * @param deadline 共同期限。
     * @return 期限内的结果；中断保留标志，缺项或失败交由字段分类。
     */
    private static <T> T await(KafkaFuture<T> future, long deadline) throws Exception {
        if (future == null) throw new IllegalStateException("查询响应缺项");
        Missing gate = gate(deadline);
        if (gate == Missing.INTERRUPTED) throw new InterruptedException();
        if (gate != null) throw new TimeoutException();
        try {
            return future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }

    /**
     * 业务作用：把本轮只读失败转成稳定证据类别，不泄露异常正文。
     *
     * @param failure 查询原因链。
     * @return 明确缺失、权限、期限或不可观测类别，不推导任何写入结果。
     */
    private static Missing classify(Throwable failure) {
        if (KafkaBrokerErrors.authorization(failure)) return Missing.AUTHORIZATION;
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof UnknownTopicOrPartitionException) return Missing.RESOURCE_MISSING;
            if (current instanceof TimeoutException || current instanceof org.apache.kafka.common.errors.TimeoutException)
                return Missing.TIMEOUT;
            if (current instanceof InterruptedException) return Missing.INTERRUPTED;
        }
        return Missing.UNAVAILABLE;
    }
}

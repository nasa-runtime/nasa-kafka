package io.github.nasaruntime.kafka.admin;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.observability.metrics.KafkaRuntimeMetrics;
import io.github.nasaruntime.kafka.protocol.KafkaWire;
import io.github.nasaruntime.kafka.replay.KafkaReplayInspector;
import io.github.nasaruntime.kafka.support.KafkaBrokerErrors;

import io.github.nasaruntime.kafka.exception.KafkaFailure;
import io.github.nasaruntime.kafka.replay.ReplayException;

import io.github.nasaruntime.kafka.replay.ReplayObservation;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.NetworkException;

import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * 单源管理门面，操作结果不泄露原生客户端所有权。
 */
public final class KafkaAdmin {

    private final KafkaProxy owner;
    private final boolean internal;
    private final boolean readOnly;

    /**
     * 业务作用：绑定源的受管 Admin 资源。
     * <p>
     * 返回：无独立线程的门面。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param owner 所属源。
     */
    public KafkaAdmin(KafkaProxy owner) {
        this(owner, false);
    }

    /**
     * 业务作用：区分业务视图与已接纳责任使用的内部依赖。
     * <p>
     * 返回：不创建原生资源的门面，两者都受源实际关闭门约束。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param owner    承担 Admin 原生资源及关闭责任的数据源。
     * @param internal 是否供已接纳的包内任务收尾使用；可越过业务准入门，仍受源资源关闭门约束。
     */
    public KafkaAdmin(KafkaProxy owner, boolean internal) {
        this(owner, internal, false);
    }

    /**
     * 业务作用：冻结管理视图的生命周期与写权限边界。
     * <p>
     * 返回：不创建连接的视图。
     *
     * @param owner    资源所有者
     * @param internal 是否供已接纳的包内任务使用保留依赖，不改变视图的管理写权限。
     * @param readOnly 是否禁止管理写操作，内部责任投影不因此获得额外写授权。
     */
    private KafkaAdmin(KafkaProxy owner, boolean internal, boolean readOnly) {
        this.owner = owner;
        this.internal = internal;
        this.readOnly = readOnly;
    }

    /**
     * 业务作用：收窄当前管理能力为只读，不影响资源所有者的其它视图。
     * <p>
     * 参数说明：无。
     *
     * @return 不能恢复写权限的视图；写操作在提交前以 AUTHORIZATION 拒绝。
     */
    public KafkaAdmin readOnly() {
        return new KafkaAdmin(owner, internal, true);
    }

    /**
     * 业务作用：诊断当前视图是否被收窄为只读。
     * <p>
     * 参数说明：无。
     *
     * @return 本地限制状态；false 不代表 broker 已授权。
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * 业务作用：在管理外部副作用前复验视图权限。
     * <p>
     * 参数说明：无。
     * 返回：开放且允许写的视图通过；宿主已退出为 LIFECYCLE，只读为 AUTHORIZATION，不创建连接。
     */
    private void requireWrite() {
        if (!internal) owner.requireManagedAccess();
        // 本地能力边界独立于 broker ACL，宽泛的连接权限不能提升业务视图。
        if (readOnly) throw new KafkaFailure(KafkaFailure.Code.AUTHORIZATION, "Kafka Admin 视图仅允许只读操作");
    }

    /**
     * 业务作用：按明确隔离级别观测保留范围、组提交与时间位置，保留逐字段缺失原因。
     *
     * @param group           可选且满足共同组身份边界的组
     * @param partitions      至多10000个唯一分区
     * @param isolation       读取隔离
     * @param timestampMillis 可选非负时间
     * @param timeout         全部步骤共享的正预算。
     * @return 不可变部分证据报告，不创建 topic 或改变消费位点；非法声明以 CONFIG 拒绝，已关闭源拒绝。
     */
    public OffsetObservation observeOffsets(String group, Collection<TopicPartition> partitions,
                                            OffsetObservation.Isolation isolation, Long timestampMillis, Duration timeout) {
        if (isolation == null
                || timestampMillis != null && timestampMillis < 0 || partitions == null || partitions.size() > 10000)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "位置观测声明无效");
        if (group != null) KafkaWire.validateGroup(group);
        List<TopicPartition> targets = validatePartitions(partitions);
        var report = KafkaOffsetInspector.observe(owner, group, targets, isolation, timestampMillis, deadline(timeout));
        KafkaRuntimeMetrics.offsets(owner, report);
        return report;
    }

    /**
     * 业务作用：为有限回放查询不加入消费组的完整范围证据。
     *
     * @param topics          完整 topic 白名单
     * @param timestampMillis 可选非负时间
     * @param timeout         所有查询共用的正预算且不超过五分钟。
     * @return 真实 cluster ID、保留起点和 read_committed LSO；超时或不完整证据拒绝，不提交任何位点。
     */
    public ReplayObservation replayObservation(Set<String> topics, Long timestampMillis, Duration timeout) {
        // 停机后新回放规划不能绕过业务门；已接纳任务使用包内视图继续核对原责任。
        try {
            if (!internal) owner.requireManagedAccess();
        } catch (KafkaFailure failure) {
            throw new ReplayException(ReplayException.Code.CLOSED);
        }
        return KafkaReplayInspector.observe(owner, topics, timestampMillis, timeout);
    }

    public enum TopicCreation {CREATED, ALREADY_EXISTS}

    public record Partition(int id, Integer leader, List<Integer> replicas, List<Integer> inSyncReplicas) {}

    public record Setting(String value, boolean sensitive, boolean readOnly, boolean defaultValue) {}

    public record Topic(String name, List<Partition> partitions, Map<String, Setting> settings) {}

    /**
     * 只读核验使用的显式 topic 声明，不授权创建或更改资源。
     */
    public record TopicContract(String topic, int partitions, int replicationFactor, Map<String, String> settings) {
        /**
         * 业务作用：冻结核验声明，避免查询期间调用方修改预期合同。
         * <p>
         * 返回：有效且不可变的声明，空配置不要求 DescribeConfigs。
         *
         * @param topic             本次创建或验证的目标topic名称。
         * @param partitions        要求创建或验证的分区数量。
         * @param replicationFactor 要求目标 topic 每个分区具有的副本数量，不是 ISR 数量。
         * @param settings          必须相等的配置子集。
         */
        public TopicContract {
            validateTopic(topic);
            if (partitions <= 0 || replicationFactor <= 0 || replicationFactor > Short.MAX_VALUE || settings == null
                    || settings.entrySet().stream().anyMatch(entry -> entry.getKey() == null || entry.getKey().isBlank()
                    || entry.getKey().indexOf('\0') >= 0 || entry.getValue() == null || entry.getValue().indexOf('\0') >= 0))
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "topic 核验声明无效");
            settings = Map.copyOf(settings);
        }
    }

    /**
     * 业务作用：只读核对 topic 分区、副本与要求的配置子集，发现漂移拒绝业务初始化。
     *
     * @param contract 冻结规格
     * @param timeout  元数据和配置查询共用预算。
     * @return 实际完整描述；不匹配为 TOPIC_CONTRACT，权限和观测失败保留原分类，不创建或修改资源。
     */
    public Topic verifyTopic(TopicContract contract, Duration timeout) {
        if (contract == null) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "topic 合同不能为空");
        Topic actual = describeTopic(contract.topic(), !contract.settings().isEmpty(), timeout);
        Set<Integer> partitions = new HashSet<>();
        actual.partitions().forEach(partition -> partitions.add(partition.id()));
        boolean complete = partitions.size() == contract.partitions();
        for (int partition = 0; complete && partition < contract.partitions(); partition++)
            complete = partitions.contains(partition);
        if (!complete || actual.partitions().stream().anyMatch(partition -> partition.replicas().size() != contract.replicationFactor()))
            throw new KafkaFailure(KafkaFailure.Code.TOPIC_CONTRACT, "topic 分区或副本不满足声明");
        for (var expected : contract.settings().entrySet()) {
            Setting observed = actual.settings().get(expected.getKey());
            if (observed == null || observed.sensitive() || !expected.getValue().equals(observed.value()))
                throw new KafkaFailure(KafkaFailure.Code.TOPIC_CONTRACT, "topic 配置未取得匹配证据");
        }
        return actual;
    }

    /**
     * 业务作用：读取 broker 的物理集群身份并检查声明的连接目标。
     *
     * @param timeout 总预算。
     * @return 真实 cluster ID；错连拒绝，超时结果未知。
     */
    public String clusterId(Duration timeout) {
        long deadline = deadline(timeout);
        String cluster = await(owner.nativeAdmin().describeCluster(new DescribeClusterOptions().timeoutMs(requestTimeout(deadline))).clusterId(), deadline);
        if (cluster == null || cluster.isBlank())
            throw new KafkaFailure(KafkaFailure.Code.ADMIN, "管理响应缺少物理集群身份");
        String expected = owner.config().getExpectedClusterId();
        if (expected != null && !expected.equals(cluster))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "Kafka 物理集群与 expected-cluster-id 不一致");
        return cluster;
    }

    /**
     * 业务作用：在一个预算内读取 topic 分区与配置快照。
     *
     * @param topic   目标
     * @param timeout 总预算。
     * @return 包含副本和敏感标记的快照，不返回秘密值。
     */
    public Topic describeTopic(String topic, Duration timeout) {
        return describeTopic(topic, true, timeout);
    }

    /**
     * 业务作用：读取指定 topic 的元数据并按需读取配置，避免只读分区查询要求多余权限。
     *
     * @param topic           目标
     * @param includeSettings 决定是否需要 DescribeConfigs
     * @param timeout         全部步骤共用预算。
     * @return 完整快照；未请求配置时 settings 为空，不代表 broker 没有配置。
     */
    public Topic describeTopic(String topic, boolean includeSettings, Duration timeout) {
        validateTopic(topic);
        long deadline = deadline(timeout);
        var description = await(owner.nativeAdmin().describeTopics(List.of(topic), new DescribeTopicsOptions().timeoutMs(requestTimeout(deadline))).allTopicNames(), deadline).get(topic);
        if (description == null || !topic.equals(description.name()) || description.partitions().isEmpty())
            throw new KafkaFailure(KafkaFailure.Code.ADMIN, "topic 元数据缺少完整目标证据");
        List<Partition> partitions = description.partitions().stream().map(partition -> new Partition(partition.partition(),
                partition.leader() == null ? null : partition.leader().id(), partition.replicas().stream().map(node -> node.id()).toList(),
                partition.isr().stream().map(node -> node.id()).toList())).toList();
        Map<String, Setting> settings = new TreeMap<>();
        if (includeSettings) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config config = await(owner.nativeAdmin().describeConfigs(List.of(resource), new DescribeConfigsOptions().timeoutMs(requestTimeout(deadline))).all(), deadline).get(resource);
            if (config == null || config.entries().isEmpty())
                throw new KafkaFailure(KafkaFailure.Code.ADMIN, "topic 配置响应缺少目标证据");
            for (ConfigEntry entry : config.entries())
                settings.put(entry.name(), new Setting(entry.isSensitive() ? null : entry.value(), entry.isSensitive(), entry.isReadOnly(), entry.isDefault()));
        }
        return new Topic(topic, partitions, Collections.unmodifiableMap(settings));
    }

    /**
     * 业务作用：核对明确目标是否存在，不把当前身份看不到的列表条目当成不存在。
     *
     * @param topic   目标
     * @param timeout 请求预算。
     * @return 只把明确 UnknownTopicOrPartition 转为 false；权限与观测失败保留异常。
     */
    public boolean topicExists(String topic, Duration timeout) {
        try {
            describeTopic(topic, false, timeout);
            return true;
        } catch (KafkaFailure error) {
            if (error.getCause() instanceof UnknownTopicOrPartitionException) return false;
            throw error;
        }
    }

    /**
     * 业务作用：只读查询指定分区的组提交位点，区分无历史与零位点。
     *
     * @param group      满足共同输入边界的精确组名（最多1024个严格 UTF-8 字节）
     * @param partitions 非空唯一目标
     * @param timeout    总预算。
     * @return 逐分区位置，null 表示 broker 确认没有历史；非法组名以 CONFIG 拒绝，权限失败不转为空。
     */
    public Map<TopicPartition, Long> committedOffsets(String group, Collection<TopicPartition> partitions, Duration timeout) {
        // 错误身份不能进入原生请求，既不能查询替代组的位点，也不能破坏共享 Admin 的后续责任。
        KafkaWire.validateGroup(group);
        List<TopicPartition> targets = validatePartitions(partitions);
        long deadline = deadline(timeout);
        var values = await(owner.nativeAdmin().listConsumerGroupOffsets(Map.of(group, new ListConsumerGroupOffsetsSpec().topicPartitions(targets)),
                new ListConsumerGroupOffsetsOptions().timeoutMs(requestTimeout(deadline))).partitionsToOffsetAndMetadata(group), deadline);
        Map<TopicPartition, Long> result = new LinkedHashMap<>();
        for (TopicPartition target : targets) {
            // 原生 Admin 会略去带分区错误的 OffsetFetch 条目；缺项不是无历史，必须拒绝并尽可能取得明确权限证据。
            if (!values.containsKey(target)) {
                describeTopic(target.topic(), false, remainingBudget(deadline));
                throw new KafkaFailure(KafkaFailure.Code.ADMIN, "组位点查询缺少目标分区结果");
            }
            var value = values.get(target);
            result.put(target, value == null ? null : value.offset());
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * 业务作用：读取指定分区的保留起点或日志末尾，不改变组位点。
     *
     * @param partitions 非空且无重复的目标分区集合。
     * @param end        true 查询 READ_UNCOMMITTED 隔离下的末尾，false 查询保留起点；需要 LSO 时使用 observeOffsets 并显式选择 READ_COMMITTED。
     * @param timeout    总预算。
     * @return 每个目标的真实位置；缺失或失败结果不补零。
     */
    public Map<TopicPartition, Long> offsets(Collection<TopicPartition> partitions, boolean end, Duration timeout) {
        List<TopicPartition> targets = validatePartitions(partitions);
        long deadline = deadline(timeout);
        Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
        targets.forEach(target -> request.put(target, end ? OffsetSpec.latest() : OffsetSpec.earliest()));
        var values = await(owner.nativeAdmin().listOffsets(request, new ListOffsetsOptions().timeoutMs(requestTimeout(deadline))).all(), deadline);
        Map<TopicPartition, Long> result = new LinkedHashMap<>();
        for (TopicPartition target : targets) {
            var value = values.get(target);
            if (value == null || value.offset() < 0)
                throw new KafkaFailure(KafkaFailure.Code.ADMIN, "分区位置缺少有效证据");
            result.put(target, value.offset());
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * 业务作用：在共同期限内合并组位点与日志末尾，供离线积压观测。
     *
     * @param group      组名
     * @param partitions 非空唯一目标
     * @param timeout    两个步骤的总预算。
     * @return 无提交历史时 committed 和 lag 均为 null；两个读取不是原子快照，位置缺失或查询失败不补零。
     */
    public List<KafkaPartitionLag> committedLag(String group, Collection<TopicPartition> partitions, Duration timeout) {
        long deadline = deadline(timeout);
        List<TopicPartition> targets = validatePartitions(partitions);
        Map<TopicPartition, Long> committed = committedOffsets(group, targets, remainingBudget(deadline));
        Map<TopicPartition, Long> ends = offsets(targets, true, remainingBudget(deadline));
        List<KafkaPartitionLag> result = new ArrayList<>();
        for (TopicPartition target : targets) {
            Long position = committed.get(target);
            long end = ends.get(target);
            result.add(new KafkaPartitionLag(target.topic(), target.partition(), position, end, position == null ? null : Math.max(0, end - position)));
        }
        return List.copyOf(result);
    }

    /**
     * 业务作用：将组合管理查询的剩余期限交给下一步骤，禁止重新取得整份预算。
     *
     * @param deadline 原调用截止时间。
     * @return 剩余正时长；耗尽时拒绝发出下一查询。
     */
    private static Duration remainingBudget(long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "管理查询预算已耗尽");
        return Duration.ofNanos(remaining);
    }

    /**
     * 业务作用：在只读批量查询前冻结唯一资源集合，拒绝隐式去重或无效分区。
     *
     * @param partitions 调用者目标集合。
     * @return 保持输入顺序的冻结列表，空、重复及非法坐标为 CONFIG。
     */
    private static List<TopicPartition> validatePartitions(Collection<TopicPartition> partitions) {
        if (partitions == null || partitions.isEmpty())
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "查询分区不能为空");
        List<TopicPartition> targets = new ArrayList<>(partitions);
        Set<TopicPartition> unique = new HashSet<>();
        for (TopicPartition partition : targets) {
            if (partition == null || partition.partition() < 0 || !unique.add(partition))
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "查询分区非法或重复");
            validateTopic(partition.topic());
        }
        return List.copyOf(targets);
    }

    /**
     * 业务作用：仅增量修改指定 topic 配置，未提供的键保持不变。
     * <p>
     * 返回：broker 确认才成功；超时不能认定未修改。
     *
     * @param topic   本次显式修改配置的目标 topic。
     * @param changes 显式topic配置变更集合，某项值为null表示恢复该配置默认值。
     * @param timeout 总预算。
     */
    public void alterTopicConfig(String topic, Map<String, String> changes, Duration timeout) {
        requireWrite();
        validateTopic(topic);
        long deadline = deadline(timeout);
        if (changes == null || changes.isEmpty())
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "topic 配置变更不能为空");
        List<AlterConfigOp> operations = new ArrayList<>();
        changes.forEach((key, value) -> {
            if (key == null || key.isBlank() || key.indexOf('\0') >= 0 || value != null && value.indexOf('\0') >= 0)
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "topic 配置键值非法");
            operations.add(new AlterConfigOp(new ConfigEntry(key, value), value == null ? AlterConfigOp.OpType.DELETE : AlterConfigOp.OpType.SET));
        });
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        await(owner.nativeAdmin().incrementalAlterConfigs(Map.of(resource, operations), new AlterConfigsOptions().timeoutMs(requestTimeout(deadline))).all(), deadline);
    }

    /**
     * 业务作用：列出当前身份可见的 topic，不能据此推断不可见资源不存在。
     *
     * @param timeout 整个请求的正预算。
     * @return 不可变名称集合；权限或查询失败保留明确分类。
     */
    public Set<String> listTopics(Duration timeout) {
        long deadline = deadline(timeout);
        return Collections.unmodifiableSet(new TreeSet<>(await(owner.nativeAdmin().listTopics(new ListTopicsOptions().timeoutMs(requestTimeout(deadline))).names(), deadline)));
    }

    /**
     * 业务作用：创建指定规格的 topic，已存在属于明确拒绝。
     * <p>
     * 返回：broker 确认后完成；结果未知时须读取实际资源再决定后续操作。
     *
     * @param topic      本次创建或验证的目标topic名称。
     * @param partitions 要求创建或验证的分区数量。
     * @param replicas   目标topic规格要求的副本数量。
     * @param timeout    总预算。
     */
    public void createTopic(String topic, int partitions, short replicas, Duration timeout) {
        createTopic(topic, partitions, replicas, Map.of(), timeout);
    }

    /**
     * 业务作用：一次创建 topic 及其声明的初始配置。
     * <p>
     * 返回：目标资源确认后完成；非法输入在提交前拒绝，超时不推导未创建。
     *
     * @param topic      本次创建或验证的目标topic名称。
     * @param partitions 要求创建或验证的分区数量。
     * @param replicas   目标topic规格要求的副本数量。
     * @param settings   初始配置
     * @param timeout    总预算。
     */
    public void createTopic(String topic, int partitions, short replicas, Map<String, String> settings, Duration timeout) {
        requireWrite();
        validateTopic(topic);
        long deadline = deadline(timeout);
        if (partitions < 1 || replicas < 1) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "topic 规格必须为正");
        if (settings == null || settings.entrySet().stream().anyMatch(entry -> entry.getKey() == null || entry.getKey().isBlank() || entry.getKey().indexOf('\0') >= 0 || entry.getValue() == null || entry.getValue().indexOf('\0') >= 0))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "topic 初始配置的键和值必须有效");
        await(owner.nativeAdmin().createTopics(List.of(new NewTopic(topic, partitions, replicas).configs(new TreeMap<>(settings))),
                new CreateTopicsOptions().timeoutMs(requestTimeout(deadline))).all(), deadline);
    }

    /**
     * 业务作用：删除明确指定的 topic，并保留不存在与权限拒绝的区别。
     * <p>
     * 返回：broker 确认后完成；不存在为 ADMIN，超时结果未知，删除的数据不能恢复。
     *
     * @param topic   目标
     * @param timeout 总预算。
     */
    public void deleteTopic(String topic, Duration timeout) {
        requireWrite();
        validateTopic(topic);
        long deadline = deadline(timeout);
        await(owner.nativeAdmin().deleteTopics(List.of(topic), new DeleteTopicsOptions().timeoutMs(requestTimeout(deadline))).all(), deadline);
    }

    /**
     * 业务作用：把目标 topic 的分区数增加到指定总数。
     * <p>
     * 返回：broker 确认后完成；不支持缩容，扩容可改变 key 到分区的映射。
     *
     * @param topic           目标
     * @param totalPartitions 正总分区数
     * @param timeout         总预算。
     */
    public void increasePartitions(String topic, int totalPartitions, Duration timeout) {
        requireWrite();
        validateTopic(topic);
        long deadline = deadline(timeout);
        if (totalPartitions < 1) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "总分区数必须为正");
        await(owner.nativeAdmin().createPartitions(Map.of(topic, NewPartitions.increaseTo(totalPartitions)),
                new CreatePartitionsOptions().timeoutMs(requestTimeout(deadline))).all(), deadline);
    }

    /**
     * 业务作用：幂等创建 topic，并区分实际创建与已经存在。
     *
     * @param topic      本次创建或验证的目标topic名称。
     * @param partitions 要求创建或验证的分区数量。
     * @param replicas   目标topic规格要求的副本数量。
     * @param timeout    总预算。
     * @return 明确结果；已存在不代表既有规格匹配。
     */
    public TopicCreation createTopicIfAbsent(String topic, int partitions, short replicas, Duration timeout) {
        return createTopicIfAbsent(topic, partitions, replicas, Map.of(), timeout);
    }

    /**
     * 业务作用：在竞争创建中保留初始配置，并明确区分创建与同名资源已存在。
     *
     * @param topic      本次创建或验证的目标topic名称。
     * @param partitions 要求创建或验证的分区数量。
     * @param replicas   目标topic规格要求的副本数量。
     * @param settings   仅创建时应用的配置
     * @param timeout    总预算。
     * @return 明确创建或已存在；已存在不会重写配置，也不验证规格一致。
     */
    public TopicCreation createTopicIfAbsent(String topic, int partitions, short replicas, Map<String, String> settings, Duration timeout) {
        try {
            createTopic(topic, partitions, replicas, settings, timeout);
            return TopicCreation.CREATED;
        } catch (KafkaFailure failure) {
            if (failure.getCause() instanceof TopicExistsException) return TopicCreation.ALREADY_EXISTS;
            throw failure;
        }
    }

    /**
     * 业务作用：复验业务准入并在管理资源构造前冻结总预算。
     *
     * @param timeout 调用方等待上限。
     * @return 单调时钟截止时间；已退出宿主为 LIFECYCLE，非法预算为 CONFIG。
     */
    private long deadline(Duration timeout) {
        if (!internal) owner.requireManagedAccess();
        if (timeout == null || timeout.isZero() || timeout.isNegative() || timeout.compareTo(Duration.ofMillis(Integer.MAX_VALUE)) > 0)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "管理预算必须为正且不超过 i32 毫秒上限");
        return System.nanoTime() + timeout.toNanos();
    }

    /**
     * 业务作用：让后续原生请求共用剩余预算，避免组合查询重新获得完整超时。
     *
     * @param deadline 本次操作的绝对截止时间。
     * @return 向上取整的正毫秒预算；已耗尽时拒绝继续提交。
     */
    private static int requestTimeout(long deadline) {
        long remaining = deadline - System.nanoTime();
        // 期限耗尽后不能继续发出管理副作用；调用方仍须按整次操作的已发生步骤回读状态。
        if (remaining <= 0) throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "管理预算已耗尽");
        return (int) Math.min(Integer.MAX_VALUE, (remaining + 999_999L) / 1_000_000L);
    }

    /**
     * 业务作用：拒绝没有明确资源名的管理意图。
     * <p>
     * 返回：非空名称通过；非法名称不会构造管理资源。
     *
     * @param topic 目标名称。
     */
    private static void validateTopic(String topic) {
        if (topic == null || topic.isBlank() || topic.indexOf('\0') >= 0)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "topic 不能为空或含 NUL");
    }

    /**
     * 业务作用：统一管理操作的中断、未知与明确失败。
     *
     * @param future   broker 操作
     * @param deadline 总截止时间。
     * @return 已确认结果或稳定错误。
     */
    private static <T> T await(KafkaFuture<T> future, long deadline) {
        try {
            return future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "管理等待被中断", error);
        } catch (TimeoutException error) {
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "管理操作超时", error);
        } catch (CancellationException error) {
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "管理等待已取消", error);
        } catch (ExecutionException error) {
            throw new KafkaFailure(failureCode(error.getCause()),
                    "Kafka 管理操作失败", error.getCause());
        }
    }

    /**
     * 业务作用：区分明确资源拒绝与缺少可信响应的管理结果。
     *
     * @param error 原生终态原因。
     * @return 权限拒绝、结果未知或明确管理失败；通用协议异常没有未执行证据，不能当作安全重试依据。
     */
    private static KafkaFailure.Code failureCode(Throwable error) {
        if (KafkaBrokerErrors.authorization(error)) return KafkaFailure.Code.AUTHORIZATION;
        // 通用 ApiException 可表示响应缺少目标资源；仅有异常不等于 broker 未执行修改。
        if (error == null || error.getClass() == ApiException.class
                || error.getClass() == KafkaException.class
                || error instanceof org.apache.kafka.common.errors.TimeoutException
                || error instanceof DisconnectException
                || error instanceof NetworkException
                || error instanceof UnknownServerException
                || error instanceof CancellationException)
            return KafkaFailure.Code.OUTCOME_UNKNOWN;
        return error instanceof ApiException ? KafkaFailure.Code.ADMIN : KafkaFailure.Code.OUTCOME_UNKNOWN;
    }
}

package io.github.nasaruntime.kafka;

import io.github.nasaruntime.kafka.admin.KafkaAdmin;
import io.github.nasaruntime.kafka.checkpoint.CheckpointStore;
import io.github.nasaruntime.kafka.dlt.DltReplayHandle;
import io.github.nasaruntime.kafka.producer.ProducerLane;
import io.github.nasaruntime.kafka.producer.PublishBuilder;
import io.github.nasaruntime.kafka.replay.ReplayHandle;
import io.github.nasaruntime.kafka.replay.ReplayProcessor;
import io.github.nasaruntime.kafka.transaction.TransactionHandle;
import io.github.nasaruntime.kafka.transfer.TransferHandle;

import io.github.nasaruntime.kafka.exception.KafkaFailure;
import io.github.nasaruntime.kafka.replay.ReplayIdentityProvider;
import io.github.nasaruntime.kafka.transaction.TransactionProcessor;

import io.github.nasaruntime.kafka.lifecycle.CloseReport;
import io.github.nasaruntime.kafka.consumer.ControlReport;
import io.github.nasaruntime.kafka.dlt.DltReplayPlan;
import io.github.nasaruntime.kafka.consumer.GroupHealth;
import io.github.nasaruntime.kafka.support.KafkaCapabilities;
import io.github.nasaruntime.kafka.observability.metrics.MetricsExportStats;
import io.github.nasaruntime.kafka.consumer.PartitionControl;
import io.github.nasaruntime.kafka.consumer.ReadyRequirement;
import io.github.nasaruntime.kafka.consumer.ReadyStatus;
import io.github.nasaruntime.kafka.replay.ReplayPlan;
import io.github.nasaruntime.kafka.transaction.TransactionPlan;
import io.github.nasaruntime.kafka.transfer.TransferPlan;
import io.github.nasaruntime.kafka.transfer.TransferRegistries;

import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单一来源的业务能力视图；不公开资源所有者、原生客户端或整源关闭权。
 * <p>
 * 提供发布、消费控制、健康、同集群事务与有限回放、转发和 DLT 回投入口。
 * {@link #admin()} 始终只读，{@link #administration()} 的管理写入需要按源授权且满足 broker ACL。
 * 该边界约束业务 API，不提供隔离不可信 JVM 代码的沙箱。
 * <p>
 * 宿主封闭业务准入后，保留的句柄或 lane 不能开启新发送。已接纳的内置交付仅按原任务责任收尾，
 * 超时或取消等待不能撤销在途副作用、提前归还实际额度或重新开放业务入口。
 * 关闭后仍可读取 {@link #closeReport()}，资源退出与业务成功分别判断。
 */
public final class KafkaHandle {

    /**
     * 业务作用：向业务开放只读关闭诊断，不授予活跃宿主的整源关闭权。
     * <p>
     * 参数说明：无。
     *
     * @return 本源实际资源与未确认责任快照，关闭后仍可查询。
     */
    public CloseReport closeReport() {
        return owner.closeReport();
    }

    /**
     * 业务作用：让宿主采样本源的剩余责任而不取得关闭权。
     * <p>
     * 参数说明：无。
     *
     * @return 当前关闭快照；指标有界异步输出，源停止后仍可读取报告。
     */
    public CloseReport sampleMetrics() {
        return owner.sampleMetrics();
    }

    private final KafkaProxy owner;

    /**
     * 业务作用：绑定宿主持有的源，不接管其资源寿命。
     * <p>
     * 返回：共享同源准入门的轻量视图。
     *
     * @param owner 装配阶段冻结的资源所有者。
     */
    KafkaHandle(KafkaProxy owner) {
        this.owner = owner;
    }

    /**
     * 业务作用：在新业务操作前复验宿主与资源准入。
     * <p>
     * 参数说明：无。
     * 返回：开放时通过，已进入最终关闭为 LIFECYCLE，不创建连接。
     */
    private void requireOpen() {
        owner.requirePublishing();
    }

    /**
     * 业务作用：标识当前句柄绑定的来源，供多源路由归因。
     * <p>
     * 参数说明：无。
     *
     * @return 冻结的规范源名。
     */
    public String sourceId() {
        return owner.sourceId();
    }

    /**
     * 业务作用：诊断所有实际资源是否退出。
     * <p>
     * 参数说明：无。
     *
     * @return 只有资源全部退出才为 true，不把宿主终止等同于退出。
     */
    public boolean isClosed() {
        return owner.isClosed();
    }

    /**
     * 业务作用：在宿主已经交还最终收尾责任后继续等待资源退出。
     * <p>
     * 返回：实际退出才成功；活跃宿主拒绝，超时保留原责任且不恢复业务准入。
     *
     * @param deadline 本次单调纳秒截止值。
     */
    public void awaitClosed(long deadline) {
        owner.awaitManagedClosure(deadline);
    }

    /**
     * 业务作用：查询构建、配置与实际运行能力，不授予操作权限。
     * <p>
     * 参数说明：无。
     *
     * @return 不含凭据的能力快照。
     */
    public KafkaCapabilities capabilities() {
        return owner.capabilities();
    }

    /**
     * 业务作用：读取实际指标出口责任，便于关闭后诊断。
     * <p>
     * 参数说明：无。
     *
     * @return 队列、丢弃与在途采样快照。
     */
    public MetricsExportStats metricsExportStats() {
        return owner.metricsExportStats();
    }

    /**
     * 业务作用：选择本源预先声明的独立发布通道。
     *
     * @param name 通道名，默认使用 default。
     * @return 无关闭权的共享通道；未知或关闭状态拒绝。
     */
    public ProducerLane producerLane(String name) {
        requireOpen();
        return owner.producerLane(name);
    }

    /**
     * 业务作用：在默认通道构造类型化业务事件。
     *
     * @param topic   目标
     * @param payload 业务值。
     * @return 受同源准入与容量约束的 builder。
     */
    public PublishBuilder message(String topic, Object payload) {
        return producerLane("default").message(topic, payload);
    }

    /**
     * 业务作用：在默认通道保留业务原始字节。
     *
     * @param topic   目标
     * @param payload 原文，可为 null。
     * @return 受同源发布边界约束的 builder。
     */
    public PublishBuilder publishRaw(String topic, byte[] payload) {
        return producerLane("default").publishRaw(topic, payload);
    }

    /**
     * 业务作用：构造带显式业务键的 tombstone。
     *
     * @param topic 目标
     * @param key   非 null 删除键，允许空字符串。
     * @return 不会提前发送的 builder；无效文本或缺失键以 CONFIG 拒绝。
     */
    public PublishBuilder tombstone(String topic, String key) {
        return producerLane("default").tombstone(topic, key);
    }

    /**
     * 业务作用：取得只读 metadata、位点和资源合同诊断能力。
     * <p>
     * 参数说明：无。
     *
     * @return 写操作稳定拒绝的 Admin；即使本源另有写授权，此入口仍只读。
     */
    public KafkaAdmin admin() {
        requireOpen();
        return owner.admin().readOnly();
    }

    /**
     * 业务作用：取得宿主按源显式授权的管理写能力，不授予整源关闭权。
     * <p>
     * 参数说明：无。
     *
     * @return 授权时提供管理视图，未授权为 AUTHORIZATION，实际写入仍需 broker ACL。
     */
    public KafkaAdmin administration() {
        requireOpen();
        // 写权限只能来自装配配置，业务不能通过普通 Admin 视图或配置副本提升权限。
        if (!owner.config().isAdminWrite())
            throw new KafkaFailure(KafkaFailure.Code.AUTHORIZATION, "admin-write 未授权");
        return owner.admin();
    }

    /**
     * 业务作用：查询全部消费组的实际健康，不新建消费者。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变快照，空目录不代表就绪。
     */
    public Map<String, GroupHealth> groupHealth() {
        return owner.groupHealth();
    }

    /**
     * 业务作用：查询指定组的分配与未完成责任。
     *
     * @param group 冻结的消费组身份。
     * @return 当前健康快照，未知组拒绝。
     */
    public GroupHealth groupHealth(String group) {
        return owner.groupHealth(group);
    }

    /**
     * 业务作用：区分同一组内多个 owner 的状态。
     *
     * @param group 消费组。
     * @return 按稳定槽号排列的实际快照。
     */
    public List<GroupHealth> groupSlotsHealth(String group) {
        return owner.groupSlotsHealth(group);
    }

    /**
     * 业务作用：核对当前分配是否足以承接流量。
     *
     * @param group       组
     * @param requirement 所需条件。
     * @return 当次就绪证据，不能锁存为永久许可。
     */
    public ReadyStatus groupReadiness(String group, ReadyRequirement requirement) {
        return owner.groupReadiness(group, requirement);
    }

    /**
     * 业务作用：等待一个消费组的当前分配达到业务要求。
     *
     * @param group       本次调用所操作的实际消费组身份。
     * @param requirement 本次等待需满足的组就绪条件。
     * @param timeout     总预算。
     * @return 当次健康证据；关闭或超时拒绝。
     */
    public GroupHealth awaitGroupReady(String group, ReadyRequirement requirement, Duration timeout) {
        requireOpen();
        return owner.awaitGroupReady(group, requirement, timeout);
    }

    /**
     * 业务作用：等待全部指定组在同一轮满足条件。
     *
     * @param requirements 组条件表
     * @param timeout      共同预算。
     * @return 不可变健康表，任何终态或预算耗尽均拒绝。
     */
    public Map<String, GroupHealth> awaitGroupsReady(Map<String, ReadyRequirement> requirements, Duration timeout) {
        requireOpen();
        return owner.awaitGroupsReady(requirements, timeout);
    }

    /**
     * 业务作用：读取已存在发布通道的容量和交付状态。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变快照，不为观测新建通道。
     */
    public Map<String, ProducerLane.Stats> producerStats() {
        return owner.producerStats();
    }

    /**
     * 业务作用：查询发布通道的实际原生代次和恢复责任。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变快照，包含尚未退出的旧代次。
     */
    public Map<String, ProducerLane.RecoveryStats> producerRecoveryStats() {
        return owner.producerRecoveryStats();
    }

    /**
     * 业务作用：登记独立有限回放，由宿主承担实际停止责任。
     *
     * @param plan      冻结读取范围、处理指纹及独立预算的有限回放合同。
     * @param store     持久后端
     * @param identity  来源证据
     * @param processor 处理器。
     * @return 同源任务句柄；关闭后拒绝新登记。
     */
    public ReplayHandle replay(ReplayPlan plan, CheckpointStore store, ReplayIdentityProvider identity, ReplayProcessor processor) {
        requireOpen();
        return owner.replay(plan, store, identity, processor);
    }

    /**
     * 业务作用：在两个显式来源间登记有限转发，保留双方资源责任。
     *
     * @param plan                冻结来源范围、目标身份、分区映射和原文策略的转发合同。
     * @param destination         目标句柄
     * @param store               承担checkpoint读取、独占权和持久进度写入的后端。
     * @param sourceIdentity      按框架已查询的来源物理集群及 topic 名取得当前外部资源代次，不负责查询 broker 范围。
     * @param destinationIdentity 按框架已查询的目标物理集群及 topic 名取得当前外部资源代次；目标分区证据由框架查询。
     * @param registries          可选 Registry 绑定。
     * @return 双方开放才接纳任务，不暴露任一资源所有者。
     */
    public TransferHandle transfer(TransferPlan plan, KafkaHandle destination, CheckpointStore store,
                                   ReplayIdentityProvider sourceIdentity, ReplayIdentityProvider destinationIdentity, TransferRegistries registries) {
        requireOpen();
        destination.requireOpen();
        return owner.transfer(plan, destination.owner, store, sourceIdentity, destinationIdentity, registries);
    }

    /**
     * 业务作用：把经证据核验的死信回投至显式目标来源。
     *
     * @param plan                冻结DLT读取范围、原组过滤、回投路由及错误隔离策略的合同。
     * @param destination         目标
     * @param store               承担checkpoint读取、独占权和持久进度写入的后端。
     * @param sourceIdentity      按框架已查询的来源物理集群及 topic 名取得当前外部资源代次，不负责查询 broker 范围。
     * @param destinationIdentity 按框架已查询的目标物理集群及 topic 名取得当前外部资源代次；目标分区证据由框架查询。
     * @param registries          编解码绑定。
     * @return 受双方准入约束的任务，不改变普通消费组位点。
     */
    public DltReplayHandle replayDlt(DltReplayPlan plan, KafkaHandle destination, CheckpointStore store,
                                     ReplayIdentityProvider sourceIdentity, ReplayIdentityProvider destinationIdentity, TransferRegistries registries) {
        requireOpen();
        destination.requireOpen();
        return owner.replayDlt(plan, destination.owner, store, sourceIdentity, destinationIdentity, registries);
    }

    /**
     * 业务作用：登记同集群消费、输出与位点共同提交的事务任务。
     *
     * @param plan      冻结同集群完整输入、批准输出、消费组及整批预算的事务合同。
     * @param processor 业务处理器。
     * @return 独立事务句柄；已关闭或身份冲突时拒绝。
     */
    public TransactionHandle transact(TransactionPlan plan, TransactionProcessor processor) {
        requireOpen();
        return owner.transact(plan, processor);
    }

    /**
     * 业务作用：读取当前消费组实际分配。
     *
     * @param group 组名。
     * @return 分区集合，查询失败不推断分区未分配。
     */
    public Set<TopicPartition> assignment(String group) {
        requireOpen();
        return owner.assignment(group);
    }

    /**
     * 业务作用：查询 owner 当前读取位置，独立于 broker 提交水位。
     *
     * @param group     本次调用所操作的实际消费组身份。
     * @param topic     本次操作关联的topic名称。
     * @param partition 本次操作关联的明确分区号。
     * @return owner 当前 position，不代表业务完成。
     */
    public long position(String group, String topic, int partition) {
        requireOpen();
        return owner.position(group, topic, partition);
    }

    /**
     * 业务作用：对明确分区执行独立控制并保留逐目标结果。
     *
     * @param group   组
     * @param actions 动作表。
     * @return 生效、拒绝或结果未知的共同报告。
     */
    public ControlReport controlPartitions(String group, Map<TopicPartition, PartitionControl> actions) {
        requireOpen();
        return owner.controlPartitions(group, actions);
    }

    /**
     * 业务作用：在旧责任收口后切换明确分区的读取位置。
     * <p>
     * 返回：owner 应用后完成；超时不表示撤销。
     *
     * @param group   组
     * @param offsets 绝对位置表。
     */
    public void seek(String group, Map<TopicPartition, Long> offsets) {
        requireOpen();
        owner.seek(group, offsets);
    }

    /**
     * 业务作用：把指定分区定位到当前保留起点。
     * <p>
     * 返回：旧责任收口并定位完成，失败不授权推进进度。
     *
     * @param group      组
     * @param partitions 目标集合。
     */
    public void seekToBeginning(String group, Collection<TopicPartition> partitions) {
        requireOpen();
        owner.seekToBeginning(group, partitions);
    }

    /**
     * 业务作用：显式跳至当前可读末尾，不把跳过范围当作已处理。
     * <p>
     * 返回：各目标槽确认定位后返回；失败可能已有部分定位或结果未知，须查询实际状态，不立即提交组位点。
     *
     * @param group      组
     * @param partitions 目标集合。
     */
    public void seekToEnd(String group, Collection<TopicPartition> partitions) {
        requireOpen();
        owner.seekToEnd(group, partitions);
    }

    /**
     * 业务作用：按时间查询并切换指定分区位置。
     * <p>
     * 返回：按时间证据定位，无匹配时取所选隔离下的末尾；多槽失败可能部分生效，超时不能推定未执行。
     *
     * @param group      组
     * @param timestamps 分区到 epoch 毫秒的映射。
     */
    public void seekToTimestamp(String group, Map<TopicPartition, Long> timestamps) {
        requireOpen();
        owner.seekToTimestamp(group, timestamps);
    }

    /**
     * 业务作用：重新定位到 broker 已确认的消费组边界。
     * <p>
     * 返回：收口旧权威并定位后完成，不使用未经确认的本地候选。
     *
     * @param group      组
     * @param partitions 目标集合。
     */
    public void seekToCommitted(String group, Collection<TopicPartition> partitions) {
        requireOpen();
        owner.seekToCommitted(group, partitions);
    }

    /**
     * 业务作用：增加指定分区的人工暂停原因，不撤销其它保护。
     * <p>
     * 返回：owner 应用人工暂停或返回失败。
     *
     * @param group      组
     * @param partitions 目标集合。
     */
    public void pause(String group, Collection<TopicPartition> partitions) {
        requireOpen();
        owner.pause(group, partitions);
    }

    /**
     * 业务作用：仅解除指定分区的人工暂停，不绕过背压或故障保护。
     * <p>
     * 返回：owner 应用解除动作或返回失败。
     *
     * @param group      组
     * @param partitions 目标集合。
     */
    public void resume(String group, Collection<TopicPartition> partitions) {
        requireOpen();
        owner.resume(group, partitions);
    }

    /**
     * 业务作用：人工暂停当前组已分配分区。
     * <p>
     * 返回：当前 owner 应用后完成，不停止整源。
     *
     * @param group 组。
     */
    public void pauseAll(String group) {
        requireOpen();
        owner.pauseAll(group);
    }

    /**
     * 业务作用：清除各 owner 记住的全部 USER 暂停原因，包括暂时不在当前分配中的坐标；重新计算当前分区的暂停状态。
     * 重试、背压和提交保护等其它原因继续有效，不能据此保证立即恢复拉取。
     * <p>
     * 返回：保留其它保护原因，不能强制恢复不安全分区。
     *
     * @param group 组。
     */
    public void resumeAll(String group) {
        requireOpen();
        owner.resumeAll(group);
    }

    /**
     * 业务作用：替换订阅组的完整 topic 集合，保留组身份。
     * <p>
     * 返回：owner 应用后完成；assign 模式拒绝。
     *
     * @param group  组
     * @param topics 新订阅集合。
     */
    public void replaceSubscriptions(String group, Collection<String> topics) {
        requireOpen();
        owner.replaceSubscriptions(group, topics);
    }

    /**
     * 业务作用：清空当前订阅但保留组的控制能力。
     * <p>
     * 返回：owner 应用后完成，不释放整源资源。
     *
     * @param group 组。
     */
    public void clearSubscriptions(String group) {
        requireOpen();
        owner.clearSubscriptions(group);
    }

    /**
     * 业务作用：在允许人工恢复时重建指定故障组。
     * <p>
     * 返回：旧权威收口后接纳重建，新分配仍须单独等待 Ready。
     *
     * @param group 明确组身份。
     */
    public void restartGroup(String group) {
        requireOpen();
        owner.restartGroup(group);
    }
}

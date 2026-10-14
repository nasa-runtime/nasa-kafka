package io.github.nasaruntime.kafka;

import io.github.nasaruntime.kafka.admin.KafkaAdmin;
import io.github.nasaruntime.kafka.checkpoint.CheckpointStore;
import io.github.nasaruntime.kafka.config.NasaKafkaProperties;
import io.github.nasaruntime.kafka.consumer.AssignmentHandle;
import io.github.nasaruntime.kafka.consumer.AssignmentHandleImpl;
import io.github.nasaruntime.kafka.consumer.BroadcastIdentity;
import io.github.nasaruntime.kafka.consumer.KafkaGroup;
import io.github.nasaruntime.kafka.consumer.KafkaProxyHolder;
import io.github.nasaruntime.kafka.consumer.KafkaSourceBudget;
import io.github.nasaruntime.kafka.consumer.PayloadTypeResolver;
import io.github.nasaruntime.kafka.consumer.StartOffset;
import io.github.nasaruntime.kafka.consumer.listener.KafkaAsyncBatchListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaAsyncListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaListener;
import io.github.nasaruntime.kafka.dlt.DltReplayHandle;
import io.github.nasaruntime.kafka.dlt.DltRetention;
import io.github.nasaruntime.kafka.dlt.KafkaDltBudget;
import io.github.nasaruntime.kafka.dlt.KafkaDltReplayProcessor;
import io.github.nasaruntime.kafka.observability.metrics.KafkaRuntimeMetrics;
import io.github.nasaruntime.kafka.observability.metrics.MetricsExporter;
import io.github.nasaruntime.kafka.observability.metrics.MetricsSink;
import io.github.nasaruntime.kafka.observability.trace.KafkaSpan;
import io.github.nasaruntime.kafka.producer.ProducerLane;
import io.github.nasaruntime.kafka.producer.PublishBuilder;
import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.protocol.KafkaWire;
import io.github.nasaruntime.kafka.replay.KafkaReplayManager;
import io.github.nasaruntime.kafka.replay.KafkaReplayTask;
import io.github.nasaruntime.kafka.replay.ReplayHandle;
import io.github.nasaruntime.kafka.replay.ReplayParticipant;
import io.github.nasaruntime.kafka.replay.ReplayProcessor;
import io.github.nasaruntime.kafka.spring.KafkaMethodListeners;
import io.github.nasaruntime.kafka.support.KafkaNative;
import io.github.nasaruntime.kafka.transaction.KafkaTransactionManager;
import io.github.nasaruntime.kafka.transaction.KafkaTransactionTask;
import io.github.nasaruntime.kafka.transaction.TransactionHandle;
import io.github.nasaruntime.kafka.transfer.KafkaTransferDelivery;
import io.github.nasaruntime.kafka.transfer.KafkaTransferProcessor;
import io.github.nasaruntime.kafka.transfer.KafkaTransferRegistry;
import io.github.nasaruntime.kafka.transfer.TransferHandle;

import io.github.nasaruntime.kafka.consumer.AckMode;
import io.github.nasaruntime.kafka.consumer.ExecutionMode;
import io.github.nasaruntime.kafka.consumer.ListenerKind;
import io.github.nasaruntime.kafka.dlt.DltReplayException;
import io.github.nasaruntime.kafka.exception.KafkaFailure;
import io.github.nasaruntime.kafka.replay.ReplayException;
import io.github.nasaruntime.kafka.transaction.TransactionException;
import io.github.nasaruntime.kafka.transfer.TransferException;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceSink;
import io.github.nasaruntime.kafka.replay.ReplayIdentityProvider;
import io.github.nasaruntime.kafka.transaction.TransactionProcessor;

import io.github.nasaruntime.kafka.lifecycle.CloseReport;
import io.github.nasaruntime.kafka.consumer.ControlReport;
import io.github.nasaruntime.kafka.dlt.DltEvidence;
import io.github.nasaruntime.kafka.dlt.DltOrigin;
import io.github.nasaruntime.kafka.dlt.DltReplayPlan;
import io.github.nasaruntime.kafka.consumer.GroupHealth;
import io.github.nasaruntime.kafka.support.KafkaCapabilities;
import io.github.nasaruntime.kafka.admin.KafkaPartitionLag;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceContext;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceSpan;
import io.github.nasaruntime.kafka.consumer.ListenerEntry;
import io.github.nasaruntime.kafka.observability.metrics.MetricsExportStats;
import io.github.nasaruntime.kafka.observability.metrics.MetricsOptions;
import io.github.nasaruntime.kafka.consumer.PartitionControl;
import io.github.nasaruntime.kafka.producer.PublishMessage;
import io.github.nasaruntime.kafka.consumer.ReadyRequirement;
import io.github.nasaruntime.kafka.consumer.ReadyStatus;
import io.github.nasaruntime.kafka.replay.ReplayPlan;
import io.github.nasaruntime.kafka.transaction.TransactionPlan;
import io.github.nasaruntime.kafka.transfer.TransferPlan;
import io.github.nasaruntime.kafka.transfer.TransferRegistries;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.nasaruntime.core.base.AnyHolder;
import io.github.nasaruntime.core.base.Initialization;
import io.github.nasaruntime.core.base.ProtocolBytes;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.github.nasaruntime.core.evt.PooledEvtData;
import io.github.nasaruntime.core.utils.MapUtils;
import io.github.nasaruntime.core.utils.ObjMprUtils;
import io.github.nasaruntime.core.utils.ReflectUtils;
import io.github.nasaruntime.core.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * 类作用：一个命名 Kafka 源的资源所有者，提供独立发布通道、有界有序消费、同集群事务与持久任务接续。
 * 多源应用由 {@link KafkaClients} 组织，业务优先使用 {@link KafkaHandle}；本入口具有管理和整源关闭权限。
 * <p>
 * 每个普通消费槽独占原生 consumer，解码与业务使用有界执行资源，只提交当前分配代次的连续安全前缀。
 * producer lane 独立观察真实交付；取消等待不撤销发送，flush 也不以可被调用方修改的 Future 代替交付证据。
 * <p>
 * 与 Rust {@code nasa::kafka} 共享线路、身份和持久格式。有限 Replay、单目标 Transfer 与 DLT 回投
 * 在实际成功后持久推进 checkpoint，再公开进度；目标确认与进度持久化之间可能重复，目的端仍需幂等。
 * Kafka 事务只覆盖同集群批准输出与输入位点，不包含数据库、HTTP 或其它集群。
 * <p>
 * 关闭先停止新消费，再等待真实任务退出，最后关闭发布、管理和观测资源。超时保留未退出责任，
 * Ready 与关闭状态均以当前证据为准；资源退出不等于所有业务成功。输入额度不等于 JVM 或原生内存总上限。
 */
@Slf4j
public class KafkaProxy implements Initialization {

    public static final String PRIMARY = "primary";
    private final String sourceId;
    private final ConcurrentMap<String, ProducerLane> lanes = new ConcurrentHashMap<>();
    private final Object lifecycleGate = new Object();
    private final ReentrantLock shutdownGate = new ReentrantLock();
    private volatile boolean started;
    private volatile boolean consumersStopped;
    private volatile boolean closing;
    /**
     * 宿主最终清理结束后永久封闭业务入口，与实际资源退出分别记录。
     */
    private volatile boolean managedAccessClosed;
    private volatile boolean closed;
    private volatile String verifiedClusterId;

    /**
     * 来源私有签发的内置交付权威；业务句柄不导出凭据，任务结束后不能继续接纳发送。
     */
    public static final class InternalPublishPermit {
        private final KafkaProxy source;
        private final ProducerLane lane;
        private final BooleanSupplier active;
        private final Predicate<ProducerRecord<byte[], byte[]>> purpose;

        /**
         * 业务作用：将内置发布限定到来源登记的任务、固定通道及批准目标。
         * <p>
         * 返回：不创建资源、不向业务返回的任务凭据。
         *
         * @param source 拥有发布资源的来源实例。
         * @param lane 该来源已创建的物理通道。
         * @param active 任务责任尚未归还的判定。
         * @param purpose 本次 DLT 原文或转发批准目标的判定。
         */
        private InternalPublishPermit(KafkaProxy source, ProducerLane lane, BooleanSupplier active,
                                      Predicate<ProducerRecord<byte[], byte[]>> purpose) {
            this.source = source;
            this.lane = lane;
            this.active = active;
            this.purpose = purpose;
        }

        /**
         * 业务作用：在内置发送准入前核对实际资源身份与仍存续的任务责任。
         * <p>
         * 返回：身份及用途全部成立才通过，否则以 LIFECYCLE 拒绝；不授予新的任务权威。
         *
         * @param lane 实际执行发送的通道。
         * @param source 通道所属来源实例。
         * @param record 准备发送的目标原文。
         */
        public void require(ProducerLane lane, KafkaProxy source, ProducerRecord<byte[], byte[]> record) {
            // 名称相同不代表同一资源，旧任务、其它来源及未批准目标均不能借用内置收尾权。
            if (this.source != source || this.lane != lane || !active.getAsBoolean() || !purpose.test(record))
                throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "内部发布与已登记任务责任不符");
            source.requireInternalPublishing();
        }
    }

    /**
     * 默认 event 名 (publish(stream, message) 不传 event 时用).
     */
    public static final String DEFAULT_EVENT = "DEFAULT";

    /**
     * Kafka header key, 标识业务 event 名。
     */
    public static final String HEADER_EVENT = "X-Nasa-Event";
    /**
     * Kafka header key, 标识 payload 编码类型.
     */
    public static final String HEADER_PAYLOAD_CODEC = "X-Nasa-Payload-Codec";
    /**
     * payload 由 {@link ProtocolBytes#encodeBytes()} 生成.
     */
    public static final String PAYLOAD_CODEC_PROTOCOL_BYTES = "protocol-bytes";
    /**
     * Kafka header key, ProtocolBytes 的 Mode 名: 消费端据此校验 listener mode 一致, 跨版本/跨语言防呆.
     */
    public static final String HEADER_PAYLOAD_MODE = "X-Nasa-Payload-Mode";
    /**
     * Kafka header key, 承载透传快照 (JSON). 放 header 而非包进 payload → ProtocolBytes 二进制 payload 与透传共存.
     */
    public static final String HEADER_PASSTHROUGH = "X-Nasa-Passthrough";
    /**
     * 死信 (DLQ) 记录的来源元数据 header.
     */
    public static final String HEADER_DLT_ORIGIN_TOPIC = "X-Nasa-DLT-Origin-Topic";
    public static final String HEADER_DLT_ORIGIN_PARTITION = "X-Nasa-DLT-Origin-Partition";
    public static final String HEADER_DLT_ORIGIN_OFFSET = "X-Nasa-DLT-Origin-Offset";
    public static final String HEADER_DLT_REASON = "X-Nasa-DLT-Reason";

    /**
     * 当前 JVM 的广播消费实例 ID, 用于生成每个实例独立的广播 group.id。
     */
    private static final String BROADCAST_INSTANCE_ID = resolveBroadcastInstanceId();

    /**
     * 业务作用：解析本 JVM 的广播实例标识，它是广播消费「每实例一个 group」的区分依据。
     * 优先取系统属性，其次取环境变量，都没有则随机生成。
     * <p>
     * 随机生成意味着<b>进程重启后标识改变</b>，旧 group 的已提交位点不再被沿用，
     * 广播消费从当次配置的起点重新开始。需要重启后接续消费的部署，必须显式配置一个稳定标识。
     * <p>
     * 参数说明：无。
     *
     * @return 本 JVM 的广播实例标识。
     */
    private static String resolveBroadcastInstanceId() {
        String cfg = System.getProperty("nasa.kafka.broadcast-instance-id");
        if (StringUtils.isBlank(cfg)) cfg = System.getenv("NASA_KAFKA_BROADCAST_INSTANCE_ID");
        return StringUtils.isBlank(cfg) ? StringUtils.random(16) : cfg.trim();
    }

    /**
     * 业务作用：为共同广播取得精确实例声明，避免旧算法的裁剪改变身份。
     * <p>
     * 参数说明：无。
     *
     * @return 系统属性、环境变量或本 JVM 临时身份；非法显式值由共同编码拒绝。
     */
    private static String commonBroadcastInstanceId() {
        String declared = System.getProperty("nasa.kafka.broadcast-instance-id");
        if (declared == null) declared = System.getenv("NASA_KAFKA_BROADCAST_INSTANCE_ID");
        return declared == null ? BROADCAST_INSTANCE_ID : declared;
    }

    private final NasaKafkaProperties properties;
    /**
     * 每来源独立的有限回放目录，实际责任退出前保持任务登记。
     */
    private final KafkaReplayManager replays = new KafkaReplayManager(this);
    private final KafkaTransferRegistry transfers = new KafkaTransferRegistry();
    private final KafkaTransactionManager transactions = new KafkaTransactionManager(this);
    private volatile MetricsExporter metrics;
    private final ApplicationContext context;

    /**
     * 共享 AdminClient (线程安全, 复用): admin 操作 + offset/committed 查询走它, 免每次建连. after() 关闭.
     */
    private volatile AdminClient adminClient;
    /**
     * 最终关闭只建立一个原生管理关闭线程；调用方等待超时不解除其真实责任。
     */
    private Thread adminCloser;
    /**
     * 关闭线程的异常不能被包装为资源已经退出。
     */
    private volatile Throwable adminCloseFailure;
    /**
     * 临时元数据查询只由调用线程访问；源关闭等待其真实归还，不能把查询超时等同于连接退出。
     */
    private final Set<KafkaConsumer<String, byte[]>> metadataQueries = new LinkedHashSet<>();
    /**
     * 原生临时连接关闭失败时保留证据，禁止源继续报告完全关闭。
     */
    private volatile Throwable metadataCloseFailure;

    /**
     * group.id 到运行中 poll task 的映射, subscribe 和 assign 模式都在这里登记。
     */
    private final ConcurrentMap<String, KafkaGroup> pollTasks = new ConcurrentHashMap<>();
    /**
     * 全部普通消费组共用源额度，并继续受进程输入总量约束。
     */
    private final KafkaSourceBudget inputBudget;
    private final KafkaDltBudget dltBudget;

    /**
     * listener 注册表, 结构为 group -&gt; topic -&gt; event -&gt; listener entry。
     */
    private final ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<String, ListenerEntry>>> listenerRegistry = new ConcurrentHashMap<>();

    /**
     * 业务作用：构造 Kafka 主入口。此时不建立任何连接——生产者与管理客户端都在首次使用时才创建，
     * 使不实际收发消息的应用不会因引入本组件而与 broker 建连。
     * 返回：持有独立配置快照的默认源；配置非法时拒绝构造。
     *
     * @param properties Kafka 组件配置
     * @param context    Spring 上下文，启动时据此扫描容器内的 listener Bean
     */
    public KafkaProxy(NasaKafkaProperties properties, ApplicationContext context) {
        this.sourceId = PRIMARY;
        this.properties = properties.effective(PRIMARY);
        this.inputBudget = new KafkaSourceBudget(this.properties);
        this.dltBudget = new KafkaDltBudget(this.properties);
        this.context = context;
        logConfigurationContract();
    }

    /**
     * 业务作用：创建不依赖 Spring 的命名源，连接按需建立。
     * <p>
     * 返回：未启动且未静态导出的代理。
     *
     * @param sourceId   稳定源名
     * @param properties 源配置。
     */
    public KafkaProxy(String sourceId, NasaKafkaProperties properties) {
        this.sourceId = sourceId;
        this.properties = properties.effective(sourceId);
        this.inputBudget = new KafkaSourceBudget(this.properties);
        this.dltBudget = new KafkaDltBudget(this.properties);
        this.context = null;
        logConfigurationContract();
    }

    /**
     * 业务作用：在资源激活前呈现实际交付与读取合同，使省略配置的行为变化可见。
     * <p>
     * 参数说明：无。
     * 返回：只记录规范源名、已校验标量与固定语义，不记录连接地址、原生属性或凭据。
     */
    private void logConfigurationContract() {
        log.info("[kafka] source={} acks={} idempotence={} maxInFlight={} strictHeaders={} legacyEnvelope={} maxConsumeAttempts={} (0=infinite) defaultPartitionEvent=DEFAULT managedAdminWrite={}",
                sourceId, properties.getAcks(), properties.isEnableIdempotence(), properties.getMaxInflightRequestsPerConnection(),
                properties.isStrictHeaders(), properties.isLegacyEnvelope(), properties.getMaxConsumeAttempts(), properties.isAdminWrite());
    }

    /**
     * 业务作用：读取语言无关的源身份。
     * <p>
     * 参数说明：无。
     *
     * @return 规范源名。
     */
    public String sourceId() {
        return sourceId;
    }

    /**
     * 业务作用：导出无整源关闭权的业务能力，默认管理操作只读。
     * <p>
     * 参数说明：无。
     *
     * @return 与本源实际资源及宿主准入边界绑定的轻量句柄，不建连。
     */
    public KafkaHandle handle() {
        return new KafkaHandle(this);
    }

    /**
     * 业务作用：分别查询本地构建支持、冻结启用项与实际组就绪，避免把配置成功当作认证成功。
     * <p>
     * 参数说明：无。
     *
     * @return 无网络调用且不含凭据的独立快照，普通组为空时不宣称Ready。
     */
    public KafkaCapabilities capabilities() {
        return KafkaCapabilities.snapshot(this, properties);
    }

    /**
     * 业务作用：以专属 producer 和 subscribe owner 执行同集群消费、输出与位点原子提交。
     *
     * @param plan      冻结同集群完整输入、批准输出、消费组及整批预算的事务合同。
     * @param processor 同步有界整批业务。
     * @return 实际任务句柄；不把普通发布或数据库副作用纳入原子范围。
     */
    public synchronized TransactionHandle transact(TransactionPlan plan, TransactionProcessor processor) {
        if (plan == null || processor == null || !sourceId.equals(plan.sourceId()))
            throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
        if (consumersStopped || closing || closed) throw new TransactionException(TransactionException.Code.CLOSED);
        // 同一来源的组不能同时受普通提交协调器和事务 owner 驱动。
        if (pollTasks.containsKey(plan.group())) throw new TransactionException(TransactionException.Code.BUSY);
        return transactions.start(plan, processor);
    }

    /**
     * 业务作用：显式执行已审阅回放计划，并将读取、处理和持久进度纳入来源停止顺序。
     *
     * @param plan      冻结读取范围、处理指纹及独立预算的有限回放合同。
     * @param store     持久后端
     * @param identity  当前资源代次查询
     * @param processor 实际成功合同。
     * @return 绑定本次执行实例的控制句柄；最多16任务与64分区worker，线程/连接按需创建。
     */
    public ReplayHandle replay(ReplayPlan plan, CheckpointStore store, ReplayIdentityProvider identity, ReplayProcessor processor) {
        return replayParticipating(plan, store, identity, processor, null);
    }

    /**
     * 业务作用：让框架转发在真实来源登记中携带目标依赖，同时维持普通回放的同一准入门。
     *
     * @param plan        冻结读取范围、处理指纹及独立预算的有限回放合同。
     * @param store       承担checkpoint读取、独占权和持久进度写入的后端。
     * @param identity    按已观测的物理集群及 topic 名查询外部资源代次；broker 身份与保留边界由框架另行查询。
     * @param processor   有限回放的实际业务处理及成功判定接口。
     * @param participant 可空的框架依赖票据。
     * @return 绑定具体任务的句柄，未激活依赖阻止开始实际处理。
     */
    ReplayHandle replayParticipating(ReplayPlan plan, CheckpointStore store, ReplayIdentityProvider identity, ReplayProcessor processor, ReplayParticipant participant) {
        if (plan == null || store == null || identity == null || processor == null)
            throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
        String fingerprint;
        // 扩展代码不能占用来源装配门；取得声明后还必须在登记临界区复验来源是否已停止。
        try {
            fingerprint = processor.fingerprint();
        } catch (Throwable failure) {
            throw new ReplayException(ReplayException.Code.PROCESSING_FAILED);
        }
        synchronized (this) {
            if (consumersStopped || closing || closed) throw new ReplayException(ReplayException.Code.CLOSED);
            return replays.start(plan, store, identity, processor, fingerprint, participant);
        }
    }

    /**
     * 业务作用：在目标同一生命周期门内预登记转发依赖，防止关闭快照遗漏正在装配的来源任务。
     * <p>
     * 参数说明：无。
     *
     * @return 尚未激活的目标票据；关闭后拒绝。
     */
    synchronized KafkaTransferRegistry.Link reserveTransfer() {
        if (consumersStopped || closing || closed) throw new TransferException(TransferException.Code.CLOSED);
        return transfers.reserve();
    }

    /**
     * 业务作用：显式执行单目标转发，并让来源和目标都持有关闭责任。
     *
     * @param plan                冻结来源范围、目标身份、分区映射和原文策略的转发合同。
     * @param destination         目标代理
     * @param store               承担checkpoint读取、独占权和持久进度写入的后端。
     * @param sourceIdentity      按框架已查询的来源物理集群及 topic 名取得当前外部资源代次，不负责查询 broker 范围。
     * @param destinationIdentity 按框架已查询的目标物理集群及 topic 名取得当前外部资源代次；目标分区证据由框架查询。
     * @param registries          Confluent模式所需绑定且原文模式可为空。
     * @return 绑定本次执行的句柄；目标真实确认后才推进来源进度，不承诺跨集群原子性。
     */
    public TransferHandle transfer(TransferPlan plan, KafkaProxy destination, CheckpointStore store, ReplayIdentityProvider sourceIdentity, ReplayIdentityProvider destinationIdentity, TransferRegistries registries) {
        if (plan == null || destination == null || store == null || sourceIdentity == null || destinationIdentity == null)
            throw new TransferException(TransferException.Code.INVALID_CONFIGURATION);
        if (!sourceId.equals(plan.replay().sourceId()))
            throw new TransferException(TransferException.Code.CONTRACT_MISMATCH);
        ProducerLane lane;
        try {
            lane = destination.producerLane(plan.target().lane());
        } catch (KafkaFailure failure) {
            throw new TransferException(KafkaTransferProcessor.classify(failure));
        }
        // 必须先让目标关闭流程看到待激活责任，再登记来源；任一侧先关闭都不能遗漏本次发送依赖。
        KafkaTransferRegistry.Link link = destination.reserveTransfer();
        try {
            InternalPublishPermit permit = destination.transferPermit(lane, plan.target(), link);
            KafkaTransferProcessor processor = new KafkaTransferProcessor(plan, destination, lane, destinationIdentity, registries, permit);
            ReplayHandle replay = replayParticipating(plan.replay(), store, sourceIdentity, processor, link);
            link.bind(replay);
            return new TransferHandle(replay, processor);
        } catch (ReplayException failure) {
            link.finished();
            throw new TransferException(KafkaTransferProcessor.classify(failure));
        } catch (Throwable failure) {
            link.finished();
            throw failure;
        }
    }

    /**
     * 业务作用：只为目标已登记的转发依赖签发受批准分区限制的收尾权。
     *
     * @param lane 已选定的目标物理通道。
     * @param target 冻结的目标路由合同。
     * @param link 目标持有且尚未归还的来源任务责任。
     * @return 仅供该内置任务持有的凭据，任务未绑定或已结束时不能发送。
     */
    private InternalPublishPermit transferPermit(ProducerLane lane, TransferPlan.Target target, KafkaTransferRegistry.Link link) {
        return new InternalPublishPermit(this, lane, link::deliveryActive,
                record -> record.partition() != null && target.partitions().contains(new TransferPlan.Partition(record.topic(), record.partition())));
    }

    /**
     * 业务作用：按冻结过滤与路由回投有限DLT，隔离和回投都以真实目标确认为成功条件。
     *
     * @param plan                冻结DLT读取范围、原组过滤、回投路由及错误隔离策略的合同。
     * @param destination         目标源
     * @param store               承担checkpoint读取、独占权和持久进度写入的后端。
     * @param sourceIdentity      按框架已查询的来源物理集群及 topic 名取得当前外部资源代次，不负责查询 broker 范围。
     * @param destinationIdentity 按框架已查询的目标物理集群及 topic 名取得当前外部资源代次；目标分区证据由框架查询。
     * @param registries          可空schema绑定。
     * @return 双方生命周期共同管理的句柄，不改变线上group位点。
     */
    public DltReplayHandle replayDlt(DltReplayPlan plan, KafkaProxy destination, CheckpointStore store, ReplayIdentityProvider sourceIdentity, ReplayIdentityProvider destinationIdentity, TransferRegistries registries) {
        if (plan == null || destination == null || store == null || sourceIdentity == null || destinationIdentity == null)
            throw new DltReplayException(DltReplayException.Code.INVALID_CONFIGURATION);
        if (!sourceId.equals(plan.replay().sourceId()))
            throw new DltReplayException(DltReplayException.Code.CONTRACT_MISMATCH);
        KafkaTransferRegistry.Link link = null;
        try {
            ProducerLane lane = destination.producerLane(plan.target().lane());
            // 目标先持有尚未激活的责任，再登记来源；任一端关闭都必须看见本任务。
            link = destination.reserveTransfer();
            var delivery = new KafkaTransferDelivery(plan.target(), plan.payloadPolicy(), destination, lane, destinationIdentity, registries,
                    destination.transferPermit(lane, plan.target(), link));
            var processor = new KafkaDltReplayProcessor(plan, delivery);
            var replay = replayParticipating(plan.replay(), store, sourceIdentity, processor, link);
            link.bind(replay);
            return new DltReplayHandle(replay, processor);
        } catch (Throwable error) {
            if (link != null) link.finished();
            throw new DltReplayException(KafkaDltReplayProcessor.classify(error));
        }
    }

    /**
     * 业务作用：在开放业务前安装源级异步指标出口。
     *
     * @param sink    后端
     * @param options 队列和字节预算。
     * @return 当前代理；重复安装或业务资源已经开放时拒绝。
     */
    public synchronized KafkaProxy metrics(MetricsSink sink, MetricsOptions options) {
        return telemetry(Objects.requireNonNull(sink), null, false, options);
    }

    /**
     * 业务作用：在开放业务前安装应用拥有的指标和trace后端，共享每源有界执行域。
     *
     * @param sink             可空的指标后端，与traceSink至少有一个非null。
     * @param traceSink        可空的trace后端，与sink至少有一个非null。
     * @param sampleTraceRoots 无父消费链路采样策略
     * @param options          共同队列及事件字节预算。
     * @return 当前源；重复安装或资源已开放时拒绝，不拥有应用后端生命周期。
     */
    public synchronized KafkaProxy telemetry(MetricsSink sink, KafkaTraceSink traceSink, boolean sampleTraceRoots, MetricsOptions options) {
        synchronized (lifecycleGate) {
            if (metrics != null || started || closing || closed || !lanes.isEmpty())
                throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "指标出口必须在开放业务前安装一次");
            metrics = new MetricsExporter(sourceId, sink, traceSink, sampleTraceRoots, options);
        }
        return this;
    }

    /**
     * 业务作用：为实际调用创建有限链路责任，不调用用户后端。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param group   本次调用所操作的实际消费组身份。
     * @param topic   本次操作关联的topic名称。
     * @param kind    生产交付或消费执行的 span 类别。
     * @param parent  有效的入站父trace上下文，无父关系时为null。
     * @param links   批调用中其它独立父trace的有限链接集合。
     * @param omitted 超过链接容量而未保留的有效输入链接数。
     * @return 依源冻结采样策略派生的调用span。
     */
    public KafkaSpan span(String group, String topic, KafkaTraceSpan.Kind kind, KafkaTraceContext parent, List<KafkaTraceContext> links, int omitted) {
        var current = metrics;
        return new KafkaSpan(this, group, topic, kind, parent, links, omitted, current != null && current.sampleTraceRoots);
    }

    /**
     * 业务作用：完成后仅尝试有限观测准入，遥测异常不改变确认或交付。
     * <p>
     * 返回：未安装出口则忽略，否则由独立观测线程执行。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param span 有限完成记录。
     */
    public void trace(KafkaTraceSpan span) {
        var current = metrics;
        if (current != null) try {
            current.trace(span);
        } catch (Throwable ignored) {
        }
    }

    /**
     * 业务作用：使用有限默认预算安装后端。
     *
     * @param sink 用户指标接收端。
     * @return 当前代理，生命周期要求与显式预算入口相同。
     */
    public KafkaProxy metrics(MetricsSink sink) {
        return metrics(sink, MetricsOptions.DEFAULT);
    }

    /**
     * 业务作用：直接观察指标出口的排队、丢弃与实际执行责任。
     * <p>
     * 参数说明：无。
     *
     * @return 不依赖用户后端的进程内快照。
     */
    public MetricsExportStats metricsExportStats() {
        MetricsExporter current = metrics;
        return current == null ? MetricsExportStats.DISABLED : current.stats();
    }

    /**
     * 业务作用：由宿主按自身周期采样全部源级剩余责任和注册规模，不建立额外定时线程或执行 broker 查询。
     * <p>
     * 参数说明：无。
     *
     * @return 当前关闭责任；样本有界异步入队，出口拥塞或关闭可丢弃，最终退出以返回报告为准。
     */
    public CloseReport sampleMetrics() {
        CloseReport report = closeReport();
        KafkaRuntimeMetrics.sample(this, report);
        var labels = Map.of("client", properties.getClientIdPrefix() == null ? "nasa-kafka" : properties.getClientIdPrefix());
        metric("registry_consumers", listenerRegistry.values().stream().flatMap(m -> m.values().stream()).mapToLong(Map::size).sum(), false, labels);
        metric("registry_groups", pollTasks.size(), false, labels);
        pollTasks.values().forEach(KafkaGroup::sampleMetrics);
        return report;
    }

    /**
     * 业务作用：对内部观测提供非等待准入。
     * <p>
     * 返回：未安装时无副作用；观测失败不进入业务控制流。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param name    已定义的指标名称，用于选择指标合同。
     * @param value   本次指标的增量或瞬时观测数值。
     * @param counter true 记录累计增量，false 记录当前瞬时观测值。
     * @param labels  低基数身份。
     */
    public void metric(String name, long value, boolean counter, Map<String, String> labels) {
        MetricsExporter current = metrics;
        if (current != null) try {
            current.emit(name, value, counter, labels);
        } catch (Throwable ignored) {
        }
    }

    /**
     * 业务作用：在槽快照发布后采样整个组，避免多个 owner 覆盖同一 gauge。
     * <p>
     * 返回：至多每秒一次的组级观测，不创建消费资源。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param group 最终组名。
     */
    public void sampleGroupMetrics(String group) {
        if (!metricsEnabled()) return;
        KafkaGroup task = pollTasks.get(group);
        if (task != null) task.sampleMetrics();
    }

    /**
     * 业务作用：让owner仅为已安装的指标后端采样附加聚合量。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 指标后端是否存在；不调用应用代码。
     */
    public boolean metricsEnabled() {
        var current = metrics;
        return current != null && current.metricsEnabled();
    }

    /**
     * 业务作用：保持源身份的 Bean 访问形式。
     * <p>
     * 参数说明：无。
     *
     * @return 稳定源名。
     */
    public String getSourceId() {
        return sourceId;
    }

    /**
     * 业务作用：判断全部受管资源是否实际完成关闭。
     * <p>
     * 参数说明：无。
     *
     * @return 未完成或超时均为 false。
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * 业务作用：读取全源实际关闭责任，保留未确认输入与未知交付，不执行网络探测。
     * <p>
     * 参数说明：无。
     *
     * @return 即时采样；只有 resourcesClosed 是全部受管资源退出的整体结论，有限任务业务结果仍从其句柄查询。
     */
    public CloseReport closeReport() {
        return closeReport(CloseReport.WaitOutcome.NOT_WAITED, false);
    }

    /**
     * 业务作用：以原关闭协议推进资源退出，并将本次未完成等待转为结构化责任报告。
     *
     * @param deadline 共同单调纳秒期限。
     * @return 全部退出为 COMPLETED；超时、中断或关闭失败为 INCOMPLETE，停止意图不撤回，中断标志保留；可重复调用续等。
     */
    public CloseReport shutdownReportUntil(long deadline) {
        try {
            shutdownUntil(deadline);
            return closeReport(CloseReport.WaitOutcome.COMPLETED, System.nanoTime() >= deadline);
        } catch (RuntimeException failure) {
            return closeReport(CloseReport.WaitOutcome.INCOMPLETE, System.nanoTime() >= deadline);
        }
    }

    /**
     * 业务作用：汇总独立资源快照并限制诊断名单长度，不把一次等待结局当作业务成功。
     *
     * @param outcome 本次等待结果
     * @param expired 期限是否耗尽。
     * @return 只含受控身份和计数的不可变报告，源关闭后仍可查询。
     */
    private CloseReport closeReport(CloseReport.WaitOutcome outcome, boolean expired) {
        int owners = 0, handlers = 0, pending = 0;
        long records = 0, bytes = 0, unverified = 0;
        List<String> pendingNames = new ArrayList<>();
        for (var entry : new TreeMap<>(pollTasks).entrySet()) {
            var value = entry.getValue().closeReport();
            owners += value.activeOwners();
            handlers += value.activeHandlers();
            records = saturatingAdd(records, value.retainedRecords());
            bytes = saturatingAdd(bytes, value.retainedBytes());
            unverified = saturatingAdd(unverified, value.releasedUnverifiedRecords());
            if (!entry.getValue().resourcesClosed()) {
                pending++;
                if (pendingNames.size() < 64) pendingNames.add(entry.getKey());
            }
        }
        int auxiliary;
        CloseReport.ResourceState adminState;
        synchronized (lifecycleGate) {
            auxiliary = metadataQueries.size();
            adminState = adminClient == null ? CloseReport.ResourceState.NOT_CREATED : adminCloser == null ? CloseReport.ResourceState.OPEN :
                                                                                       adminCloser.isAlive() || adminCloseFailure != null ? CloseReport.ResourceState.CLOSING : CloseReport.ResourceState.CLOSED;
        }
        var dlt = dltBudget.snapshot();
        var metricsState = metricsExportStats();
        return new CloseReport(sourceId, consumersStopped, closed, outcome, expired,
                new CloseReport.Consumers(owners, handlers, records, bytes, unverified, 0), pendingNames, pending,
                new TreeMap<>(lanes).values().stream().map(ProducerLane::closeReport).toList(),
                replays.activeCount(), transfers.activeCount(), transactions.activeCount(), dltBudget.activeTasks(), dlt.records(), dlt.bytes(),
                adminState, auxiliary, metricsState.queued(), metricsState.active(), metricsState.workerExited());
    }

    /**
     * 业务作用：汇总长期诊断计数时保持非负，不让溢出伪装为责任减少。
     *
     * @param left  参与饱和求和的第一个非负计数。
     * @param right 参与饱和求和的第二个非负计数。
     * @return 精确和或 Long.MAX_VALUE。
     */
    private static long saturatingAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    /**
     * 业务作用：向同包 owner 提供路由登记表；路由在启动前装配，消费期间只读使用。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 内部并发映射本体，不是防修改副本；不得向业务导出或绕过注册门禁写入。
     */
    public ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<String, ListenerEntry>>> listenerRegistry() {
        return listenerRegistry;
    }

    /**
     * 业务作用：读取稳定的 Spring 注入名。
     * <p>
     * 参数说明：无。
     *
     * @return 与默认选择无关的 Bean 名。
     */
    public String getQualifier() {
        return PRIMARY.equals(sourceId) ? "kafkaProxy" : sourceId + "KafkaProxy";
    }

    /**
     * 业务作用：取得配置副本避免运行期外部改写。
     * <p>
     * 参数说明：无。
     *
     * @return 独立配置。
     */
    public NasaKafkaProperties getProperties() {
        return properties.snapshot();
    }

    /**
     * 业务作用：供同源运行时读取冻结配置。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 内部只读约定配置。
     */
    public NasaKafkaProperties config() {
        return properties;
    }

    /**
     * 业务作用：为同一源的消费组提供共享输入计费域。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 冻结的源与进程额度，不提供绕过准入的修改入口。
     */
    public KafkaSourceBudget inputBudget() {
        return inputBudget;
    }

    /**
     * 业务作用：使全部普通消费组共享同一源的死信额度。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 源寿命内稳定的 DLT 责任池。
     */
    public KafkaDltBudget dltBudget() {
        return dltBudget;
    }

    /**
     * 业务作用：查找受管默认源。
     * <p>
     * 参数说明：无。
     *
     * @return 未注册时为 null。
     */
    public static KafkaProxy load() {
        return KafkaClients.loadDefault();
    }

    /**
     * 业务作用：按源名或 Bean 别名查找。
     *
     * @param name 显式名称。
     * @return 未知源为 null，不回退。
     */
    public static KafkaProxy load(String name) {
        return KafkaClients.load(name);
    }

    /**
     * 业务作用：要求显式源存在。
     *
     * @param name 源名或别名。
     * @return 源代理；未知时失败。
     */
    public static KafkaProxy require(String name) {
        KafkaProxy proxy = load(name);
        if (proxy == null) throw new KafkaFailure(KafkaFailure.Code.NO_SUCH_SOURCE, "Kafka 源不存在: " + name);
        return proxy;
    }

    /**
     * 业务作用：列举静态导出源。
     * <p>
     * 参数说明：无。
     *
     * @return 排序后的规范名称。
     */
    public static String[] sourceIds() {
        return KafkaClients.sourceIds();
    }

    /**
     * 业务作用：列举已导出的注入名称。
     * <p>
     * 参数说明：无。
     *
     * @return 排序后的 Bean 名。
     */
    public static String[] qualifiers() {
        return Arrays.stream(sourceIds()).map(id -> require(id).getQualifier()).sorted().toArray(String[]::new);
    }

    /**
     * 业务作用：选取已声明的物理通道。
     *
     * @param name lane 名。
     * @return 同源独立通道，未知名称拒绝。
     */
    public ProducerLane producerLane(String name) {
        requireManagedAccess();
        return internalProducerLane(name);
    }

    /**
     * 业务作用：为已接纳的内置收尾取得发布依赖，不重开宿主业务入口。
     *
     * @param name 冻结通道名。
     * @return 同源通道，实际资源已关闭则拒绝。
     */
    private ProducerLane internalProducerLane(String name) {
        synchronized (lifecycleGate) {
            requireInternalPublishing();
            if (!"default".equals(name) && !properties.getProducerLanes().containsKey(name))
                throw new KafkaFailure(KafkaFailure.Code.NO_SUCH_PRODUCER_LANE, "未知 lane: " + name);
            return lanes.computeIfAbsent(name, n -> new ProducerLane(this, n, properties.lane(n)));
        }
    }

    /**
     * 业务作用：在默认通道构造类型化发送。
     *
     * @param topic   目标
     * @param payload 业务值。
     * @return 消息 builder。
     */
    public PublishBuilder message(String topic, Object payload) {
        return producerLane("default").message(topic, payload);
    }

    /**
     * 业务作用：在默认通道构造原始字节发送。
     *
     * @param topic   目标
     * @param payload 原始字节。
     * @return 消息 builder。
     */
    public PublishBuilder publishRaw(String topic, byte[] payload) {
        return producerLane("default").publishRaw(topic, payload);
    }

    /**
     * 业务作用：构造 tombstone 删除事件。
     *
     * @param topic 目标
     * @param key   非 null 删除键，允许空字符串。
     * @return 消息 builder；缺失或不可精确编码的键以 CONFIG 拒绝。
     */
    public PublishBuilder tombstone(String topic, String key) {
        return producerLane("default").tombstone(topic, key);
    }

    /**
     * 业务作用：关闭后阻断所有新发布及资源重建。
     * <p>
     * 参数说明：无。
     * 返回：开放时正常返回，关闭时拒绝。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void requirePublishing() {
        requireManagedAccess();
        requireInternalPublishing();
    }

    /**
     * 业务作用：保留已接纳 DLT 或转发的收尾依赖，同时禁止实际关闭后的发布。
     * <p>
     * 参数说明：无。
     * 返回：内部资源开放时通过，否则拒绝。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void requireInternalPublishing() {
        if (closing || closed)
            throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "Kafka 源已关闭发布准入: " + sourceId);
    }

    /**
     * 业务作用：拒绝已退出宿主的旧句柄开始新操作。
     * <p>
     * 参数说明：无。
     * 返回：业务域开放时通过，最终清理结束或耗尽预算后为 LIFECYCLE。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void requireManagedAccess() {
        if (managedAccessClosed)
            throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "Kafka 宿主业务准入已关闭: " + sourceId);
    }

    /**
     * 业务作用：宿主交还最终清理责任时封闭所有业务视图。
     * <p>
     * 参数说明：无。
     * 返回：单向关闭本地门，不取消已接纳责任，也不宣称资源已退出。
     */
    void sealManagedAccess() {
        managedAccessClosed = true;
    }

    /**
     * 业务作用：只允许业务句柄接续已结束宿主的资源收尾。
     * <p>
     * 返回：实际关闭或原收尾错误；活跃宿主拒绝，不授予提前关闭权。
     *
     * @param deadline 此次绝对纳秒截止时间。
     */
    void awaitManagedClosure(long deadline) {
        // 宿主最终清理释放权威后才能续等，长期持有的业务句柄不能关闭活跃来源。
        if (!managedAccessClosed) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "Kafka 宿主最终清理尚未结束");
        shutdownUntil(deadline);
    }

    /**
     * 业务作用：为发布快照提供当前外部准入结论。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 宿主业务域与源发布域都开放时为真，不表示 broker 可达。
     */
    public boolean acceptsBusinessPublishing() {
        return !managedAccessClosed && !closing && !closed;
    }

    /**
     * 业务作用：查询运行中 group 的真实健康。
     *
     * @param group 显式组名。
     * @return 当前分配与责任快照。
     */
    public GroupHealth groupHealth(String group) {
        return requireTask(group).health();
    }

    /**
     * 业务作用：按稳定槽号查询独立 owner，区分空闲成员和保护中的槽。
     *
     * @param group 消费组。
     * @return 下标即槽号的不可变快照列表。
     */
    public List<GroupHealth> groupSlotsHealth(String group) {
        return requireTask(group).slotsHealth();
    }

    /**
     * 业务作用：要求已保护的 group 收口旧权威并重建会话。
     * <p>
     * 返回：重建请求已接受；新 assignment 通过 awaitGroupReady 独立等待。
     *
     * @param group 当前组。
     */
    public void restartGroup(String group) {
        requireTask(group).restart();
    }

    /**
     * 业务作用：取得资源所有者的管理入口，复用本源 Admin 连接和关闭责任。
     * <p>
     * 参数说明：无。
     *
     * @return 本地允许管理写操作的门面，远端仍受 broker ACL 约束；业务受限视图使用 KafkaHandle.admin。
     */
    public KafkaAdmin admin() {
        requireManagedAccess();
        return new KafkaAdmin(this);
    }

    /**
     * 业务作用：为已接纳的内置收尾保留只在包内可取得的管理依赖。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 受源实际关闭边界约束的门面，不能导出给业务。
     */
    public KafkaAdmin internalAdmin() {
        return new KafkaAdmin(this, true);
    }

    /**
     * 业务作用：获取所有 group 的无阻塞健康快照。
     * <p>
     * 参数说明：无。
     *
     * @return 按组名排序的不可变 Map。
     */
    public Map<String, GroupHealth> groupHealth() {
        Map<String, GroupHealth> values = new TreeMap<>();
        pollTasks.forEach((group, task) -> values.put(group, task.health()));
        return Collections.unmodifiableMap(values);
    }

    /**
     * 业务作用：读取已创建发布通道的容量与交付统计，不为观测创建新连接。
     * <p>
     * 参数说明：无。
     *
     * @return 按通道名排序的快照。
     */
    public Map<String, ProducerLane.Stats> producerStats() {
        Map<String, ProducerLane.Stats> values = new TreeMap<>();
        lanes.forEach((name, lane) -> values.put(name, lane.stats()));
        return Collections.unmodifiableMap(values);
    }

    /**
     * 业务作用：按稳定lane名读取原生代次恢复和退出责任。
     * <p>
     * 参数说明：无。
     *
     * @return 各通道只读快照，包含永久保护及尚存原生资源数。
     */
    public Map<String, ProducerLane.RecoveryStats> producerRecoveryStats() {
        Map<String, ProducerLane.RecoveryStats> values = new TreeMap<>();
        lanes.forEach((name, lane) -> values.put(name, lane.recoveryStats()));
        return Collections.unmodifiableMap(values);
    }

    /**
     * 业务作用：在发送或消费开放前校验显式物理集群约束。
     * <p>
     * 参数说明：无。
     * 返回：未声明约束时不建连，错连时失败。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void verifyClusterIdentity() {
        if (properties.getExpectedClusterId() != null && verifiedClusterId == null)
            verifiedClusterId = internalAdmin().clusterId(Duration.ofMillis(properties.getStartupTimeoutMs()));
    }

    /**
     * 业务作用：在单一截止时间内等待当前分配满足要求。
     *
     * @param group       组
     * @param requirement 条件
     * @param timeout     总预算。
     * @return 当次就绪快照，失权后需重新查询。
     */
    public GroupHealth awaitGroupReady(String group, ReadyRequirement requirement, Duration timeout) {
        return awaitGroupsReady(Map.of(group, requirement), timeout).get(group);
    }

    /**
     * 业务作用：解释指定组当前是否满足接流量要求，不发起 broker 请求。
     *
     * @param group       已启动的组
     * @param requirement 有效条件。
     * @return 同次观测的原因、要求和健康；未知组拒绝。
     */
    public ReadyStatus groupReadiness(String group, ReadyRequirement requirement) {
        return ReadyStatus.evaluate(Objects.requireNonNull(requirement), requireTask(group).health());
    }

    /**
     * 业务作用：以一个总期限等待全部组在同一轮观测中满足各自要求。
     *
     * @param requirements 非空组条件表
     * @param timeout      非负总预算，零只观察一次。
     * @return 不可变健康表；超时包含全部未满足项，终止态立即返回分类失败。
     */
    public Map<String, GroupHealth> awaitGroupsReady(Map<String, ReadyRequirement> requirements, Duration timeout) {
        if (requirements == null || requirements.isEmpty() || timeout == null || timeout.isNegative())
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "Ready 要求须非空且预算不得为负");
        if (requirements.entrySet().stream().anyMatch(entry -> entry.getKey() == null || entry.getKey().isBlank() || entry.getValue() == null))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "Ready 组名与条件不得为空");
        Map<String, ReadyRequirement> frozen = new TreeMap<>(requirements);
        long startedAt = System.nanoTime(), budget;
        try {
            budget = timeout.toNanos();
        } catch (ArithmeticException error) {
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "Ready 预算超出纳秒范围");
        }
        Map<String, KafkaGroup> handles = new TreeMap<>();
        frozen.keySet().forEach(group -> handles.put(group, requireTask(group)));
        while (true) {
            Map<String, ReadyStatus> missing = new TreeMap<>();
            Map<String, GroupHealth> ready = new TreeMap<>();
            boolean terminal = false;
            // 每轮复验全部绑定实例，不锁存首次成功；旧实例退出后也不接管新建同名 group。
            for (var entry : handles.entrySet()) {
                ReadyStatus status = ReadyStatus.evaluate(frozen.get(entry.getKey()), entry.getValue().health());
                if (status.ready()) ready.put(entry.getKey(), status.health());
                else {
                    missing.put(entry.getKey(), status);
                    terminal |= switch (status.reason()) {
                        case DEGRADED, CRASHED, STOPPING, STOPPED -> true;
                        default -> false;
                    };
                }
            }
            long elapsed = System.nanoTime() - startedAt;
            if (missing.isEmpty()) {
                for (String group : handles.keySet()) {
                    metric("group_ready_total", 1, true, Map.of("group", group));
                    metric("group_ready_wait_millis", Math.max(0, elapsed / 1_000_000), false, Map.of("group", group));
                }
                return Collections.unmodifiableMap(ready);
            }
            // 先完整采样再报告失败，使同一 deadline 下其它未满足组也保留诊断。
            if (terminal)
                throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "Ready 组进入保护或停止状态", null, missing);
            if (elapsed >= budget) {
                for (String group : missing.keySet())
                    metric("group_ready_timeout_total", 1, true, Map.of("group", group));
                throw new KafkaFailure(KafkaFailure.Code.READY_TIMEOUT, "group Ready 超时: " + missing.keySet(), null, missing);
            }
            try {
                TimeUnit.NANOSECONDS.sleep(Math.min(budget - elapsed, 20_000_000L));
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "Ready 等待被中断", error, missing);
            }
        }
    }

    /**
     * 业务作用：替换完整订阅集合，与增量订阅入口分开。
     * <p>
     * 返回：全部槽确认订阅修改才正常返回；多槽失败可能已部分应用，重新分配与 Ready 需要另行观察。
     *
     * @param group  组
     * @param topics 新集合。
     */
    public void replaceSubscriptions(String group, Collection<String> topics) {
        requireTask(group).replaceSubscriptions(topics);
    }

    /**
     * 业务作用：清空订阅但保留控制句柄。
     * <p>
     * 返回：不等同于停止 group。
     *
     * @param group 组。
     */
    public void clearSubscriptions(String group) {
        replaceSubscriptions(group, List.of());
    }

    /**
     * 业务作用：向指定组整批追加订阅，在owner内一次读取并应用，保持其它订阅。
     * <p>
     * 返回：全部槽确认才成功；路由保持冻结，多槽部分失败可能结果未知。
     *
     * @param group  运行组
     * @param topics 非空、唯一主题集合。
     */
    public void addSubscriptions(String group, Collection<String> topics) {
        requireTask(group).addSubscriptions(topics);
    }

    /**
     * 业务作用：从指定组整批删除订阅，删空时保留owner以便重新订阅。
     * <p>
     * 返回：每槽一次修改，非法整批不产生副作用；在途输入阻止变更。
     *
     * @param group  运行组
     * @param topics 非空、唯一主题集合。
     */
    public void removeSubscriptions(String group, Collection<String> topics) {
        requireTask(group).removeSubscriptions(topics);
    }

    /**
     * 业务作用：按冻结类型严格解码，raw 路线保留 null 与零长字节。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param bytes 负载
     * @param type  目标
     * @param topic 本次操作关联的topic名称。
     * @param codec 消息头声明的 codec 名称；null 表示未声明 ProtocolBytes。
     * @param mode  消息头声明的 ProtocolBytes 模式名称，可为 null；不是 codec 实例。
     * @param raw   原样入口。
     * @return 目标值或分类解码错误。
     */
    public Object decode(byte[] bytes, TypeReference<?> type, String topic, String codec, String mode, boolean raw) {
        if (raw) return bytes == null ? null : bytes.clone();
        if (bytes == null) return null;
        if (bytes.length == 0) throw new KafkaFailure(KafkaFailure.Code.INVALID_RECORD, "空字节不是 JSON 值");
        try {
            Class<?> target = rawClass(type);
            // ProtocolBytes 类型必须由显式 codec 选择；即使内容恰好能被 Jackson 读取，也不能绕过协议声明。
            if (target != null && ProtocolBytes.class.isAssignableFrom(target) && !PAYLOAD_CODEC_PROTOCOL_BYTES.equals(codec))
                throw new IllegalArgumentException("ProtocolBytes 缺少匹配的 codec 声明");
            if (PAYLOAD_CODEC_PROTOCOL_BYTES.equals(codec)) {
                Object value = deserializeValue(bytes, type, topic, true, mode, null);
                if (value == null) throw new IllegalArgumentException("二进制消息与目标类型不匹配");
                return value;
            }
            if (codec != null || mode != null) throw new IllegalArgumentException("不匹配的 codec/mode 声明");
            if (properties.isLegacyEnvelope())
                return deserializeUnwrap(ObjMprUtils.OBJECT_MAPPER.readValue(bytes, Object.class), type, topic);
            return ObjMprUtils.OBJECT_MAPPER.readValue(bytes, type);
        } catch (Exception failure) {
            throw new KafkaFailure(KafkaFailure.Code.INVALID_RECORD, "消息无法按声明类型解码", failure);
        }
    }

    /**
     * 业务作用：在业务调用前恢复既有透传合同。
     * <p>
     * 返回：无，非法观测数据不改变业务确认。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param value  业务值
     * @param bytes  透传头
     * @param single 是否单条调用。
     */
    public void restorePassthrough(Object value, byte[] bytes, boolean single) {
        Map<String, Object> restored = parsePassthroughHeader(bytes);
        if (restored == null) return;
        if (value != null) KafkaProxyHolder.set(value, restored);
        if (single) restored.forEach(KafkaProxyHolder::passthrough);
    }

    /**
     * 业务作用：验证来源与目标保留策略后保留失败原文，等待真实broker确认。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param record   原文
     * @param group    消费组
     * @param attempts 尝试数
     * @param reason   有界原因
     * @param stopping 当前停止意图。
     * @param task     当前来源目录登记的消费组实例，不接受同名替代实例。
     * @return 返回交付 Future；成功表示取得所选 lane 的 broker 确认，不代表组位点已提交；发送前的证据失败或停止意图会拒绝。
     */
    public CompletableFuture<Void> deadLetter(ConsumerRecord<byte[], byte[]> record, String group, int attempts, String reason, BooleanSupplier stopping, KafkaGroup task) {
        // 只有目录中的实际消费组可请求 DLT 收尾，公开方法与同名自行构造的组不构成任务权威。
        if (task == null || pollTasks.get(group) != task || task.resourcesClosed())
            throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "DLT 缺少当前消费组责任");
        String suffix = properties.getDeadLetterTopicSuffix();
        if (suffix == null || suffix.isBlank() || record.topic().endsWith(suffix))
            throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "DLT 未配置或禁止递归 DLT");
        NasaKafkaProperties laneConfig = properties.lane(properties.getDltProducerLane());
        if ("0".equals(laneConfig.getAcks()))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "DLT 必须使用 broker 确认通道");
        if (stopping.getAsBoolean()) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "DLT 停止准入");
        DltOrigin origin = new DltOrigin(sourceId, internalAdmin().clusterId(Duration.ofMillis(properties.getStartupTimeoutMs())));
        // 压缩会移除同key的旧失败和tombstone；每次发送前读取实际策略，不缓存配置权威。
        DltRetention.validate(internalAdmin().describeTopic(record.topic() + suffix, Duration.ofMillis(properties.getStartupTimeoutMs())).settings());
        KafkaHeaders headers = origin.attach(KafkaWire.deadLetterHeaders(new KafkaHeaders(record.headers()), group,
                record.topic(), record.partition(), record.offset(), reason, attempts));
        // 完整头必须仍能由共同DLT协议读取；容量不足不能通过裁剪业务头伪装成成功留证。
        DltEvidence.read(headers);
        if (stopping.getAsBoolean()) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "DLT 停止准入");
        ProducerRecord<byte[], byte[]> outgoing = new ProducerRecord<>(record.topic() + suffix, null, record.timestamp() < 0 ? null : record.timestamp(), record.key(), record.value(), headers.entries());
        ProducerLane lane = internalProducerLane(properties.getDltProducerLane());
        InternalPublishPermit permit = new InternalPublishPermit(this, lane,
                () -> pollTasks.get(group) == task && !task.resourcesClosed(), candidate -> candidate == outgoing);
        return lane.sendBytesInternal(outgoing, permit).thenApply(metadata -> null);
    }

    /**
     * 业务作用：保留既有单参数广播组算法，继续使用原位点归属。
     * <p>
     * 此入口会把非 ASCII 标识字符替换为下划线，不同原文可能得到相同作用域。
     * 新业务使用显式 scope/instanceId 的双参数入口；旧业务切换前须决定历史位点迁移。
     *
     * @param scope 广播作用域，为空白时按默认作用域处理
     * @return 形如 {@code nasa-kafka-broadcast-<作用域>-<实例标识>} 的 group.id。
     */
    public static String broadcastGroup(String scope) {
        String s = StringUtils.isBlank(scope) ? "default" : scope.replaceAll("[^A-Za-z0-9._-]", "_");
        return "nasa-kafka-broadcast-" + s + "-" + BROADCAST_INSTANCE_ID;
    }

    /**
     * 业务作用：以显式作用域和实例身份生成双端一致的广播组。
     *
     * @param scope      业务范围
     * @param instanceId 稳定或临时实例身份。
     * @return 共同编码算法的组名，不自动继承既有广播组位点。
     */
    public static String broadcastGroup(String scope, String instanceId) {
        return BroadcastIdentity.group(scope, instanceId);
    }

    /**
     * 业务作用：以 listener 类的全限定名作为兼容广播作用域，复用本 JVM 广播实例标识。
     * 同一 JVM 中同类对象使用相同组；不同 JVM 只有实例标识不同时才得到不同组。
     *
     * @param listenerClass 广播 listener 的类型，可为 null
     * @return 该 listener 在本 JVM 上的独享 group.id。
     */
    public static String broadcastGroup(Class<?> listenerClass) {
        return broadcastGroup(listenerClass == null ? null : listenerClass.getName());
    }

    // ==================== producer init / shutdown ====================

    // ==================== publish API ====================

    /**
     * 业务作用：以默认事件名同步发布一条消息，用于该 topic 只有单一消息语义、无需二级路由的场景。
     * 消费端需注册事件名为默认值的 listener 才能收到。
     * 失败时向调用方传播异常；超时或中断保持结果未知，中断标志会保留，不能据此认定消息未发送。
     * <p>
     * 返回： 无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic   目标 topic
     * @param message 业务消息体
     */
    public void publish(String topic, Object message) {
        this.publish(topic, DEFAULT_EVENT, null, message);
    }

    /**
     * 业务作用：以指定事件名同步发布一条消息，事件名是消费端 topic 之下的二级路由依据。
     * 不指定 key 时由默认分区器选分区，同一业务实体的多条消息可能落到不同分区，<b>不保证相对顺序</b>。
     * 失败时向调用方传播异常；超时或中断保持结果未知，中断标志会保留，不能据此认定消息未发送。
     * <p>
     * 返回： 无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param message 业务消息体
     */
    public void publish(String topic, String event, Object message) {
        this.publish(topic, event, null, message);
    }

    /**
     * 业务作用：同步发布并用业务 key 参与分区选择，同时携带当前线程的透传上下文。
     * 同 key 的分区归属依赖分区器及分区数量；不承诺跨 lane、跨生产者或分区扩容后的总顺序。
     * 超时或中断保留结果未知，不能据此认定未发送。
     * <p>
     * 返回：无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键；相同 key 落到相同分区，为 null 时由默认分区器选择
     * @param message 业务消息体
     */
    public void publish(String topic, String event, String key, Object message) {
        // passthrough 走 header: payload 保持纯序列化, ProtocolBytes 二进制与透传共存.
        this.send(topic, key, message, event, KafkaProxyHolder.passthrough());
    }

    /**
     * 业务作用：同步发布并通过兼容 Map 入口附带自定义头。
     * 该入口跳过精确同名的事件、codec 和透传保留头；其它头不自动去重或消除大小写别名。
     * 需要严格保留头门禁及 null 头值时使用 PublishBuilder.header。
     * <p>
     * 返回：无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键
     * @param message 业务消息体
     * @param headers 自定义消息头；值为集合时展开成多个同名头，值为 null 的项跳过
     */
    public void publish(String topic, String event, String key, Object message, Map<String, ?> headers) {
        long deadline = producerLane("default").publishDeadline();
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, null, key, message, event, headers, KafkaProxyHolder.passthrough());
        this.dispatchSend(record, deadline);
    }

    /**
     * 业务作用：同步发布并为 ProducerRecord 声明消息时间戳，用于表达业务事件时间。
     * 目标 topic 使用 CreateTime 时保留该时间；LogAppendTime 可由 broker 改写，不能保证任何配置下都按原业务时间定位。
     * <p>
     * 返回：无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic     目标 topic
     * @param event     事件名
     * @param key       分区键
     * @param timestamp 非负 Unix 毫秒时间戳，最终存储时间还取决于 topic 的时间戳策略。
     * @param message   业务消息体
     * @param headers   自定义消息头
     */
    public void publishAt(String topic, String event, String key, long timestamp, Object message, Map<String, ?> headers) {
        long deadline = producerLane("default").publishDeadline();
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, timestamp, key, message, event, headers, KafkaProxyHolder.passthrough());
        this.dispatchSend(record, deadline);
    }

    /**
     * 业务作用：把消息同步发到指定分区，绕开分区器，用于分区与业务分片一一对应的场景。
     * 指定分区号超出该 topic 现有分区数时发送失败。
     * <p>
     * 返回： 无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic     目标 topic
     * @param partition 目标分区号
     * @param event     事件名
     * @param message   业务消息体
     */
    public void publishToPartition(String topic, int partition, String event, Object message) {
        this.publishToPartition(topic, partition, event, null, message);
    }

    /**
     * 业务作用：把消息同步发到指定分区，同时保留 key 供消费端使用。
     * 分区已被显式指定，此时 <b>key 不参与分区选择</b>，仅作为消息的业务标识随消息传递。
     * <p>
     * 返回： 无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic     目标 topic
     * @param partition 目标分区号
     * @param event     事件名
     * @param key       消息键，此处不影响分区归属
     * @param message   业务消息体
     */
    public void publishToPartition(String topic, int partition, String event, String key, Object message) {
        this.publishToPartition(topic, partition, event, key, message, null);
    }

    /**
     * 业务作用：把消息同步发到指定分区并附带自定义消息头。
     * <p>
     * 返回： 无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic     目标 topic
     * @param partition 目标分区号
     * @param event     事件名
     * @param key       消息键，此处不影响分区归属
     * @param message   业务消息体
     * @param headers   自定义消息头
     */
    public void publishToPartition(String topic, int partition, String event, String key, Object message, Map<String, ?> headers) {
        long deadline = producerLane("default").publishDeadline();
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, partition, null, key, message, event, headers, KafkaProxyHolder.passthrough());
        this.dispatchSend(record, deadline);
    }

    /**
     * 业务作用：按业务分片键发布消息，使同一分片的消息落到同一分区从而保持处理顺序。
     * <p>
     * 未显式提供事件时使用 DEFAULT；需要以 topic 名路由时使用 partitionLegacy 或显式 event 重载。
     * <p>
     * 返回： 无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic        目标 topic
     * @param partitionKey 参与分区选择的原始业务key字符串。
     * @param data         业务消息体
     */
    public void partition(String topic, String partitionKey, Object data) {
        this.publish(topic, DEFAULT_EVENT, partitionKey, data);
    }

    /**
     * 业务作用：按数值型业务分片键发布消息，键按十进制字符串参与分区计算。
     * <p>
     * 未显式提供事件时使用 DEFAULT，消费端需注册相同事件。
     * <p>
     * 数值转字符串后再哈希，因此与直接传同值字符串的重载落到<b>同一分区</b>，两种写法可混用。
     * <p>
     * 返回： 无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic        目标 topic
     * @param partitionKey 以十进制字符串编码后参与分区选择的数值业务key。
     * @param data         业务消息体
     */
    public void partition(String topic, long partitionKey, Object data) {
        this.publish(topic, DEFAULT_EVENT, String.valueOf(partitionKey), data);
    }

    /**
     * 业务作用：显式以 topic 名作为事件路由并按业务键分区。
     * <p>
     * 返回：按配置等待真实交付，不使用 DEFAULT 路由。
     *
     * @param topic        同时用作目标 topic 和事件名，沿用显式 topic 事件路由。
     * @param partitionKey 参与分区选择的原始业务key字符串。
     * @param data         消息值。
     */
    public void partitionLegacy(String topic, String partitionKey, Object data) {
        publish(topic, topic, partitionKey, data);
    }

    /**
     * 业务作用：显式以 topic 事件路由保留数值分片键语义。
     * <p>
     * 返回：等待对应通道交付，键编码与同值字符串一致。
     *
     * @param topic        同时用作目标 topic 和事件名，沿用显式 topic 事件路由。
     * @param partitionKey 以十进制字符串编码后参与分区选择的数值业务key。
     * @param data         业务值。
     */
    public void partitionLegacy(String topic, long partitionKey, Object data) {
        partitionLegacy(topic, String.valueOf(partitionKey), data);
    }

    /**
     * 业务作用：按业务分片键发布消息并显式指定事件名，用于同一 topic 承载多种消息语义且需要按分片保序的场景。
     * <p>
     * 返回： 无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic        目标 topic
     * @param event        事件名
     * @param partitionKey 参与分区选择的原始业务key字符串。
     * @param data         业务消息体
     */
    public void partition(String topic, String event, String partitionKey, Object data) {
        this.publish(topic, event, partitionKey, data);
    }

    /**
     * 业务作用：按数值型业务分片键发布消息并显式指定事件名。
     * <p>
     * 返回： 无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param topic        目标 topic
     * @param event        事件名
     * @param partitionKey 以十进制字符串编码后参与分区选择的数值业务key。
     * @param data         业务消息体
     */
    public void partition(String topic, String event, long partitionKey, Object data) {
        this.publish(topic, event, String.valueOf(partitionKey), data);
    }

    /**
     * 业务作用：按列表顺序逐条调用同步发布；每条消息仍按自身 key 选择分区。
     * 不保证整批落在同一分区，也不提供事务原子性。中途失败不会撤回前序发送，未知结果须按业务身份核对。
     * <p>
     * 返回：null 或空列表不发送；全部调用达到各自配置的交付条件才正常返回，acks=0 不构成 broker 确认。
     *
     * @param topic            目标 topic
     * @param eventAndMessages 待发布的消息列表，为 null 或空时直接返回
     */
    public void publishBatch(String topic, List<PublishMessage> eventAndMessages) {
        if (eventAndMessages == null || eventAndMessages.isEmpty()) return;
        for (PublishMessage em : eventAndMessages) {
            this.publish(topic, em.event(), em.key(), em.message());
        }
    }

    // ---- 异步 publish (fire-and-forget, 不阻塞业务线程) ----

    /**
     * 业务作用：以默认事件名异步发布，不等待 broker 确认，用于不容许发送耗时影响业务响应的路径。
     * 编码与准入失败立即抛出；准入后的交付失败计入 lane 统计，调用方需结果时使用 publishFuture。
     * <p>
     * 返回：编码及本地有界准入后返回，不等待 broker 终态；准入失败立即抛出，交付结果持续计入 lane 统计。
     *
     * @param topic   目标 topic
     * @param message 业务消息体
     */
    public void publishAsync(String topic, Object message) {
        this.publishAsync(topic, DEFAULT_EVENT, null, message);
    }

    /**
     * 业务作用：以指定事件名异步发布，不等待 broker 确认；准入失败抛出，交付结果由 lane 持续观测。
     * <p>
     * 返回：编码及本地有界准入后返回，不等待 broker 终态；准入失败立即抛出，交付结果持续计入 lane 统计。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param message 业务消息体
     */
    public void publishAsync(String topic, String event, Object message) {
        this.publishAsync(topic, event, null, message);
    }

    /**
     * 业务作用：完成编码与有界本地准入后返回，原生发送在 lane 的提交线程执行。
     * 准入失败立即抛出；最终交付持续计入 lane 统计。返回不表示 broker 确认，
     * 需要逐条确认的业务使用同步发布或 publishFuture。
     * <p>
     * 返回：编码及本地有界准入后返回，不等待 broker 终态；准入失败立即抛出，交付结果持续计入 lane 统计。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键
     * @param message 业务消息体
     */
    public void publishAsync(String topic, String event, String key, Object message) {
        long deadline = producerLane("default").publishDeadline();
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, null, key, message, event, null, KafkaProxyHolder.passthrough());
        producerLane("default").send(record, deadline);
    }

    /**
     * 业务作用：编码并完成本地准入后交付独立 Future，供业务等待或组合交付结果。
     * 编码和准入失败可直接抛出；取消 Future 只取消业务等待，不撤回已准入的发送。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键
     * @param message 业务消息体
     * @return 按 lane 的 acks 配置完成的落位结果；acks=0 不证明 broker 已接收，发送失败以异常完成。
     */
    public Future<RecordMetadata> publishFuture(String topic, String event, String key, Object message) {
        return this.publishFuture(topic, event, key, message, null);
    }

    /**
     * 业务作用：通过兼容 Map 头入口构造消息并交付独立 Future。
     * 编码和准入失败直接抛出；取消 Future 不取消原生交付责任，头处理规则与同签名同步发布一致。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键
     * @param message 业务消息体
     * @param headers 自定义消息头
     * @return 按配置交付条件完成的元数据 Future；acks=0 不构成 broker 确认，准入后的失败以异常完成。
     */
    public Future<RecordMetadata> publishFuture(String topic, String event, String key, Object message, Map<String, ?> headers) {
        long deadline = producerLane("default").publishDeadline();
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, null, key, message, event, headers, KafkaProxyHolder.passthrough());
        return producerLane("default").send(record, deadline);
    }

    /**
     * 业务作用：冻结全部lane当前已准入责任并等待交付及回执处理结束，不把随后发送纳入屏障。
     * 屏障包含的任意失败向调用方传播，各消息凭据保留具体终态；等待超时不取消实际发送。
     * <p>
     * 参数说明：无。
     * 返回：所有冻结责任成功时正常返回；失败、超时或中断返回明确异常。
     */
    public void flush() {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(properties.getPublishTimeoutMs());
        // 先冻结所有lane再等待，避免第一个lane变慢时把其它lane后续流量纳入本次屏障。
        Map<ProducerLane, ProducerLane.FlushBarrier> barriers = new LinkedHashMap<>();
        lanes.values().forEach(lane -> barriers.put(lane, lane.captureBarrier()));
        KafkaFailure failure = null;
        for (var entry : barriers.entrySet()) {
            try {
                entry.getKey().flushUntil(entry.getValue(), deadline);
            } catch (KafkaFailure error) {
                if (failure == null || error.code() == KafkaFailure.Code.OUTCOME_UNKNOWN) failure = error;
            }
        }
        if (failure != null) throw failure;
    }

    // ---- 内部 send ----

    /**
     * 业务作用：同步发送的内部收口，组装记录后交给发送流程。
     * <p>
     * 返回：完成记录构造并等待配置交付终态；超时或中断不能证明未发送，acks=0 不构成 broker 确认。
     *
     * @param topic   目标 topic
     * @param key     分区键
     * @param message 业务消息体
     * @param event   事件名
     * @param pt      透传快照，可为 null
     */
    private void send(String topic, String key, Object message, String event, Map<String, Object> pt) {
        long deadline = producerLane("default").publishDeadline();
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, null, key, message, event, null, pt);
        this.dispatchSend(record, deadline);
    }

    /**
     * 业务作用：为兼容发布入口组装记录，先处理 Map 自定义头，再添加事件、ProtocolBytes 声明及透传上下文。
     * 仅三个精确保留头会在 Map 入口被跳过；不能依赖追加顺序消除重复模式头或大小写别名。
     *
     * @param topic     目标 topic
     * @param partition 目标分区号；为 null 时由分区器按 key 选择
     * @param timestamp 可空的非负 Unix 毫秒时间；null 由 producer 补当前时间，broker 可按 topic 策略改写。
     * @param key       分区键
     * @param message   业务消息体
     * @param event     事件名
     * @param headers   自定义消息头，可为 null
     * @param pt        透传快照，可为 null
     * @return 组装完毕、可直接发送的记录。
     */
    private ProducerRecord<String, byte[]> buildRecord(String topic, Integer partition, Long timestamp, String key,
                                                       Object message, String event, Map<String, ?> headers,
                                                       Map<String, Object> pt) {
        byte[] bytes = this.serializePayload(message);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partition, timestamp, key, bytes);
        this.attachCustomHeaders(record, headers);
        this.attachEventHeader(record, event);
        this.attachPayloadCodecHeader(record, message);
        this.attachPassthroughHeader(record, pt);
        return record;
    }

    /**
     * 业务作用：把业务对象编码成消息负载字节。
     * 实现了二进制协议的对象走其自身编码，比 JSON 紧凑得多；其余对象按 JSON 编码。
     * 两条路径的区分结果会被写进负载编码头，消费端据此选择对应的解码方式。
     *
     * @param payload 业务消息体，可为 null
     * @return 负载字节；入参为 null 或 JSON 编码结果为 null 时返回 null，此时消息成为墓碑消息。
     */
    private byte[] serializePayload(Object payload) {
        if (payload == null) return null;
        if (payload instanceof ProtocolBytes pb) return pb.encodeBytes();
        String json = ObjMprUtils.toString(payload);
        return json == null ? null : json.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：把事件名写进消息头，作为消费端 topic 之下的二级路由依据。
     * 事件名为 null 时不写该头，消费端按默认事件名处理。
     * 显式事件必须非空白且可精确编码为 UTF-8，不能通过字符替换改变路由身份。
     * <p>
     * 返回：非 null 事件经严格 UTF-8 校验后追加；null 不写头，空白或不可编码事件拒绝。
     *
     * @param record 待写入的记录
     * @param event  事件名，可为 null
     */
    private void attachEventHeader(ProducerRecord<String, byte[]> record, String event) {
        if (event != null) {
            record.headers().add(new RecordHeader(HEADER_EVENT, KafkaWire.eventBytes(event)));
        }
    }

    /**
     * 业务作用：为 ProtocolBytes 对象追加 codec 与模式头，使消费端能选择并校验二进制解码合同。
     * 其它载荷不写 codec/mode 头；方法不清理调用方此前写入的模式头。
     * <p>
     * 返回：ProtocolBytes 时追加两个声明头；其它类型保持记录头不变。
     *
     * @param record  待写入的记录
     * @param payload 业务消息体，据其类型判定编码方式
     */
    private void attachPayloadCodecHeader(ProducerRecord<String, byte[]> record, Object payload) {
        if (payload instanceof ProtocolBytes pb) {
            record.headers().add(new RecordHeader(HEADER_PAYLOAD_CODEC, PAYLOAD_CODEC_PROTOCOL_BYTES.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader(HEADER_PAYLOAD_MODE, pb.mode().name().getBytes(StandardCharsets.UTF_8)));
        }
    }

    /**
     * 业务作用：把调用方的自定义头写入消息，同时挡掉对框架保留头的直接占用。
     * <p>
     * 该兼容 Map 入口跳过空白键和 null 值；需要保留 null 头时使用 PublishBuilder.header。
     * <p>
     * 值为集合时展开成多个同名头，用于承载天然多值的元数据，与 Kafka 允许同名头重复的语义一致。
     * <p>
     * 返回：追加非空白名称、非 null 值且未命中兼容保留名单的自定义头；集合按元素顺序展开，不执行通用保留头去重。
     *
     * @param record  待写入的记录
     * @param headers 自定义头映射，可为 null 或空
     */
    private void attachCustomHeaders(ProducerRecord<String, byte[]> record, Map<String, ?> headers) {
        if (headers == null || headers.isEmpty()) return;
        for (Map.Entry<String, ?> e : headers.entrySet()) {
            String key = e.getKey();
            if (StringUtils.isBlank(key) || isReservedHeader(key)) continue;
            Object value = e.getValue();
            if (value == null) continue;
            if (value instanceof Collection<?> values) {
                for (Object v : values) this.attachOneCustomHeader(record, key, v);
            } else {
                this.attachOneCustomHeader(record, key, value);
            }
        }
    }

    /**
     * 业务作用：把单个兼容头值写入记录；byte[] 保留原字节，其它值使用字符串的 UTF-8 编码。
     * 二进制头值的解释由业务合同决定，不能假定全部头值都是 UTF-8 文本。
     * <p>
     * 返回：非 null 值追加一项消息头，null 值不产生写入。
     *
     * @param record 待写入的记录
     * @param key    头名
     * @param value  头值，为 null 时跳过
     */
    private void attachOneCustomHeader(ProducerRecord<String, byte[]> record, String key, Object value) {
        if (value == null) return;
        byte[] bytes = value instanceof byte[] b ? b : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        record.headers().add(new RecordHeader(key, bytes));
    }

    /**
     * 业务作用：判定头名是否为框架保留，用于拦住自定义头对路由与解码信息的直接占用。
     *
     * @param key 待判定的头名
     * @return 属于事件名、负载编码或透传头时返回 true。
     */
    private static boolean isReservedHeader(String key) {
        return HEADER_EVENT.equals(key) || HEADER_PAYLOAD_CODEC.equals(key) || HEADER_PASSTHROUGH.equals(key);
    }

    /**
     * 业务作用：把当前线程的透传快照写进消息头，使链路标识与业务透传项随消息传到下游。
     * <p>
     * 放在头而非包进负载，是为了让透传与二进制负载共存——包进负载会强制所有消息都套一层信封，
     * 二进制编码的紧凑性就被抵消了，也无法兼容不认识该信封的历史消费方。
     * <p>
     * finally 中归还池化的快照容器：快照由池借出，无论写入成功、结果为空还是编码失败都必须归还，
     * 漏还会让池逐渐借空而退化成每次新建。
     * <p>
     * 返回：非空且编码成功的快照写入透传头；无论是否写入，池化快照都在 finally 中归还。
     *
     * @param record 待写入的记录
     * @param pt     透传快照，可为 null；为池化容器时在本方法内归还
     */
    private void attachPassthroughHeader(ProducerRecord<String, byte[]> record, Map<String, Object> pt) {
        if (pt == null) return;
        try {
            if (!pt.isEmpty()) {
                String json = ObjMprUtils.toString(pt);
                if (json != null) {
                    record.headers().add(new RecordHeader(HEADER_PASSTHROUGH, json.getBytes(StandardCharsets.UTF_8)));
                }
            }
        } finally {
            if (pt instanceof RecycleLinkedMap<?, ?> rm) rm.recycle();
        }
    }

    /**
     * 业务作用：在编码前冻结的期限内等待默认 lane 的配置交付终态，统一兼容同步发布的失败出口。
     * 异常不撤销原生发送；超时或中断可能已有交付，调用方不能仅凭异常推导补偿或重发安全。
     * <p>
     * 返回：无返回值；按配置等待交付终态，acks=0 不代表 broker 确认。
     *
     * @param record   待发送的记录
     * @param deadline 编码前冻结的总截止时间
     * @throws RuntimeException 发送失败或等待确认超时
     */
    private void dispatchSend(ProducerRecord<String, byte[]> record, long deadline) {
        ProducerLane.await(producerLane("default").send(record, deadline), deadline);
    }

    // ==================== consumer 动态订阅 ====================

    /**
     * 业务作用：给<b>所有</b>运行中的消费任务追加订阅一个 topic，用于运行期动态扩展消费范围。
     * 手动分配模式或尚有未收口输入的任务拒绝该请求；多个组之间不承诺原子性。
     * 需要只作用于某个 group 时应改用按 group 指定的重载。
     * <p>
     * 返回：全部已登记目标槽确认订阅变更后返回；无任务时不操作，跨组或跨槽失败可能部分生效，实际分配及 Ready 另行观察。
     *
     * @param topic 要追加订阅的 topic
     */
    public void subscribe(String topic) {
        for (KafkaGroup task : pollTasks.values()) {
            task.subscribe(topic);
        }
    }

    /**
     * 业务作用：给所有运行中的消费任务批量追加订阅。
     * <p>
     * 返回：null 或空数组不操作；按 topic 顺序逐项变更，后项失败不撤回此前结果，实际分配及 Ready 另行观察。
     *
     * @param topics 要追加订阅的 topic 数组，为 null 时直接返回
     */
    public void subscribe(String... topics) {
        if (topics == null) return;
        for (String topic : topics) this.subscribe(topic);
    }

    /**
     * 业务作用：只给指定 group 的消费任务追加订阅，避免波及其它 group。
     * <p>
     * 返回：指定组全部目标槽确认本次变更后返回；未知组拒绝，多槽失败可能部分生效，实际分配及 Ready 另行观察。
     *
     * @param group 目标 group.id
     * @param topic 要追加订阅的 topic
     */
    public void subscribeGroup(String group, String topic) {
        requireTask(group).subscribe(topic);
    }

    /**
     * 业务作用：只给指定 group 的消费任务批量追加订阅。
     * <p>
     * 返回：null 或空集合不操作；非空时先校验完整集合再派发，未知组拒绝，多槽失败可能部分生效，Ready 另行观察。
     *
     * @param group  目标 group.id
     * @param topics 要追加订阅的 topic 集合，为 null 时直接返回
     */
    public void subscribeGroup(String group, Collection<String> topics) {
        if (topics == null) return;
        if (topics.isEmpty()) return;
        addSubscriptions(group, topics);
    }

    /**
     * 业务作用：从所有运行中的消费任务中移除一个 topic 的订阅。
     * 已拉取的输入必须先实际处理并收口；尚有责任时拒绝，不撤回正在执行的业务。
     * <p>
     * 返回：全部已登记目标槽确认订阅变更后返回；无任务时不操作，跨组或跨槽失败可能部分生效，实际分配及 Ready 另行观察。
     *
     * @param topic 要取消订阅的 topic
     */
    public void unsubscribe(String topic) {
        for (KafkaGroup task : pollTasks.values()) {
            task.unsubscribe(topic);
        }
    }

    /**
     * 业务作用：从所有运行中的消费任务中批量移除订阅。
     * <p>
     * 返回：null 或空数组不操作；按 topic 顺序逐项变更，后项失败不撤回此前结果，实际分配及 Ready 另行观察。
     *
     * @param topics 要取消订阅的 topic 数组，为 null 时直接返回
     */
    public void unsubscribe(String... topics) {
        if (topics == null) return;
        for (String topic : topics) this.unsubscribe(topic);
    }

    /**
     * 业务作用：只从指定 group 的消费任务中移除一个 topic 的订阅。
     * <p>
     * 返回：指定组全部目标槽确认本次变更后返回；未知组拒绝，多槽失败可能部分生效，实际分配及 Ready 另行观察。
     *
     * @param group 目标 group.id
     * @param topic 要取消订阅的 topic
     */
    public void unsubscribeGroup(String group, String topic) {
        requireTask(group).unsubscribe(topic);
    }

    /**
     * 业务作用：只从指定 group 的消费任务中批量移除订阅。
     * <p>
     * 返回：null 或空集合不操作；非空时先校验完整集合再派发，未知组拒绝，多槽失败可能部分生效，Ready 另行观察。
     *
     * @param group  目标 group.id
     * @param topics 要取消订阅的 topic 集合，为 null 时直接返回
     */
    public void unsubscribeGroup(String group, Collection<String> topics) {
        if (topics == null) return;
        if (topics.isEmpty()) return;
        removeSubscriptions(group, topics);
    }

    /**
     * 业务作用：先向指定组发布停止意图，再等待 owner、调用和控制资源实际退出，成功后注销该实例及其路由。
     * 等待失败时保留组名和责任，后续可继续等待；不能在旧任务仍活动时复用名称。
     * <p>
     * 返回：组资源实际退出并完成注销后正常返回；等待超时或失败向调用方传播，尚未注销的任务继续保留。
     *
     * @param group 当前源的目标组名；未登记时直接返回，不修改其它组。
     */
    public void stop(String group) {
        KafkaGroup task;
        synchronized (lifecycleGate) {
            task = pollTasks.get(group);
            if (task == null) return;
            task.requestStop();
        }
        // 等待期间不能持有发布门禁，旧业务可能仍需通过同源 producer 完成 DLT 或业务收尾。
        task.shutdown();
        synchronized (lifecycleGate) {
            pollTasks.remove(group, task);
            if (!pollTasks.containsKey(group)) listenerRegistry.remove(group);
        }
    }

    // ==================== 分区消费 API ====================

    /**
     * 业务作用：为单个 topic 的若干分区建立手动分配消费，用于定点回放或分区与业务分片一一对应的场景。
     *
     * @param group      本次消费使用的 group.id，由调用方指定且不得与已有消费冲突
     * @param topic      目标 topic
     * @param partitions 要消费的分区号集合
     * @param start      起始位点策略，为 null 时按最早位点处理
     * @param listener   接收消息的 listener
     * @return 用于查询与停止本次消费的控制句柄。
     * @throws KafkaFailure             group 不满足共同组身份边界（非纯空白、无 NUL、无 ASCII 首尾控制字符或空格、严格 UTF-8 且最多1024字节）
     * @throws IllegalArgumentException 分区集合为空
     * @throws IllegalStateException    该 group 已被占用
     */
    @SuppressWarnings("rawtypes")
    public AssignmentHandle assign(String group, String topic, Collection<Integer> partitions, StartOffset start, KafkaListener listener) {
        return this.assign(group, Map.of(topic, partitions), start, listener);
    }

    /**
     * 业务作用：为跨 topic 的一组分区建立手动分配消费，不参与消费组再均衡。
     * <p>
     * 入参在建任务之前一次性校验完毕：group 与分区一旦建出就会启动线程并占用 broker 连接，
     * 参数错误若拖到运行期才暴露，需要额外的清理逻辑。
     * <p>
     * <b>group 已被占用时直接拒绝</b>而非复用：同一 group 上并存订阅式与手动分配式消费，
     * 两者的分区归属会互相冲突，表现为消息时有时无，事后极难定位。
     *
     * @param group           本次消费使用的 group.id
     * @param topicPartitions topic 到分区号集合的映射
     * @param start           起始位点策略，为 null 时按最早位点处理
     * @param listener        接收消息的 listener
     * @return 用于查询与停止本次消费的控制句柄。
     * @throws KafkaFailure             group 不满足共同组身份边界（非纯空白、无 NUL、无 ASCII 首尾控制字符或空格、严格 UTF-8 且最多1024字节）
     * @throws IllegalArgumentException 映射为空、topic 为空或某 topic 的分区集合为空
     * @throws IllegalStateException    该 group 已被订阅式或手动分配式消费占用
     */
    @SuppressWarnings("rawtypes")
    public synchronized AssignmentHandle assign(String group, Map<String, Collection<Integer>> topicPartitions, StartOffset start, KafkaListener listener) {
        if (consumersStopped || closing || closed)
            throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "consumer 准入已关闭");
        // 身份必须先于集群查询、路由登记和 owner 创建确定，避免连接启动后才发现组名被替换。
        KafkaWire.validateGroup(group);
        if (topicPartitions == null || topicPartitions.isEmpty())
            throw new IllegalArgumentException("assign topicPartitions 不能为空");
        if (transactions.contains(group)) throw new TransactionException(TransactionException.Code.BUSY);
        if (pollTasks.containsKey(group)) {
            throw new IllegalStateException("assign group 已被占用 (与已有 subscribe/assign 冲突): " + group);
        }
        Set<TopicPartition> tps = new LinkedHashSet<>();
        List<String> topics = new ArrayList<>();
        topicPartitions.forEach((topic, partitions) -> {
            if (StringUtils.isBlank(topic)) throw new IllegalArgumentException("assign topic 不能为空");
            if (partitions == null || partitions.isEmpty())
                throw new IllegalArgumentException("assign partitions 不能为空 topic=" + topic);
            topics.add(topic);
            for (Integer p : partitions) {
                if (p == null || p < 0 || !tps.add(new TopicPartition(topic, p)))
                    throw new IllegalArgumentException("分区必须非负且无重复");
            }
        });
        // 固定分区入口也必须在创建 owner 前验证物理集群，不能绕过订阅启动的连接约束。
        // 范围超限在任何原生连接和注册副作用前拒绝，调用方可调整声明后重新创建。
        if (tps.size() > properties.getMaxAssignmentPartitions()) throw new KafkaFailure(KafkaFailure.Code.CONFIG,
                "AssignmentTooLarge: group partitions=" + tps.size() + " max=" + properties.getMaxAssignmentPartitions());
        verifyClusterIdentity();
        registerEntries(group, listener, topics.toArray(String[]::new));
        KafkaGroup task = new KafkaGroup(this, group, tps, start == null ? StartOffset.earliest() : start);
        pollTasks.put(group, task);
        task.start(properties.getStartConsumeDelayMs());
        log.info("[kafka] assign group={} partitions={} start={} listener={}",
                group, tps, start, listener.getClass().getName());
        return new AssignmentHandleImpl(this, group, task);
    }

    /**
     * 业务作用：停止一条手动分配消费，由控制句柄调用。幂等，重复调用无副作用。
     * <p>
     * 返回：沿用组停止协议；不存在时返回，实际退出后才释放组名，超时保留责任并抛出异常。
     *
     * @param group 目标 group.id
     */
    void stopAssignment(String group) {
        this.stop(group);
    }

    /**
     * 业务作用：按创建实例复验停止权威。
     * <p>
     * 返回：旧句柄不停止同名新任务。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param group    组名
     * @param expected 句柄创建时任务。
     */
    public void stopAssignment(String group, KafkaGroup expected) {
        synchronized (lifecycleGate) {
            if (pollTasks.get(group) != expected) return;
            expected.requestStop();
        }
        expected.shutdown();
        synchronized (lifecycleGate) {
            if (pollTasks.remove(group, expected)) listenerRegistry.remove(group);
        }
    }

    // ---- seek / pause / resume: 作用于运行中的 group 或 assign consumer (命令入队, poll 线程执行) ----

    /**
     * 业务作用：在共同预算内执行显式分区控制，保留部分成功与归属变化证据。
     *
     * @param group   最终组名
     * @param actions 非空目标映射。
     * @return 独立逐目标报告；非法声明或组不存在时在派发前拒绝。
     */
    public ControlReport controlPartitions(String group, Map<TopicPartition, PartitionControl> actions) {
        Map<TopicPartition, PartitionControl> copy = Map.copyOf(actions);
        if (copy.isEmpty() || copy.keySet().stream().anyMatch(tp -> tp.topic().isBlank() || tp.partition() < 0))
            throw new IllegalArgumentException("分区控制目标不能为空且必须有效");
        var report = requireTask(group).controlPartitions(copy);
        for (var outcome : report.outcomes().values())
            metric("control_target_total", 1, true, Map.of("group", group, "outcome", outcome.name().toLowerCase(Locale.ROOT)));
        return report;
    }

    /**
     * 业务作用：把运行中消费者在某分区的读取位置移到指定偏移量，用于重放或跳过。
     * 命令由 owner 串行执行并等待结果；超时区分尚未执行与结果未知。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group     目标 group.id
     * @param topic     目标 topic
     * @param partition 分区号
     * @param offset    目标偏移量
     */
    public void seek(String group, String topic, int partition, long offset) {
        requireTask(group).seek(new TopicPartition(topic, partition), offset);
    }

    /**
     * 业务作用：批量移动多个分区的读取位置。
     * 命令由 owner 串行执行并等待结果；超时区分尚未执行与结果未知。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group   目标 group.id
     * @param offsets 分区到目标偏移量的映射，为 null 或空时直接返回
     */
    public void seek(String group, Map<TopicPartition, Long> offsets) {
        if (offsets == null || offsets.isEmpty()) return;
        requireTask(group).seek(offsets);
    }

    /**
     * 业务作用：把指定 topic 的若干分区移到最早可用位置，用于重放尚未被清理的历史。
     * 实际起点受 broker 保留策略约束。命令由 owner 串行执行并等待结果；超时区分尚未执行与结果未知。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号，不传表示不操作任何分区
     */
    public void seekToBeginning(String group, String topic, int... partitions) {
        requireTask(group).seekToBeginning(toTps(topic, partitions));
    }

    /**
     * 业务作用：跨 topic 把若干分区移到最早可用位置。命令由 owner 串行执行并等待结果；超时区分尚未执行与结果未知。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     */
    public void seekToBeginning(String group, Collection<TopicPartition> partitions) {
        requireTask(group).seekToBeginning(partitions);
    }

    /**
     * 业务作用：把当前受管分区的读取位置移到查询到的末尾，跳过本次会话中的历史范围。
     * 操作不删除日志，也不立即提交组位点；数据仍在保留范围内时可以再次 seek 回读。
     * owner 在定位前复验分区权威与在途责任，多槽操作不承诺原子性。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号
     */
    public void seekToEnd(String group, String topic, int... partitions) {
        requireTask(group).seekToEnd(toTps(topic, partitions));
    }

    /**
     * 业务作用：把当前受管分区的读取位置移到查询到的末尾，跳过本次会话中的历史范围。
     * 操作不删除日志，也不立即提交组位点；数据仍在保留范围内时可以再次 seek 回读。
     * owner 在定位前复验分区权威与在途责任，多槽操作不承诺原子性。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     */
    public void seekToEnd(String group, Collection<TopicPartition> partitions) {
        requireTask(group).seekToEnd(partitions);
    }

    /**
     * 业务作用：把某分区移到指定时刻之后的第一条消息，用于按时间窗口回放。
     * 该分区没有满足条件的消息时移到末尾，即只消费此后的新消息。命令由 owner 串行执行并等待结果；超时区分尚未执行与结果未知。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group     目标 group.id
     * @param topic     目标 topic
     * @param partition 分区号
     * @param epochMs   起始时刻，epoch 毫秒
     */
    public void seekToTimestamp(String group, String topic, int partition, long epochMs) {
        requireTask(group).seekToTimestamp(new TopicPartition(topic, partition), epochMs);
    }

    /**
     * 业务作用：批量按各自时刻定位多个分区。命令由 owner 串行执行并等待结果；超时区分尚未执行与结果未知。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param timestamps 分区到起始时刻（epoch 毫秒）的映射
     */
    public void seekToTimestamp(String group, Map<TopicPartition, Long> timestamps) {
        if (timestamps == null || timestamps.isEmpty()) return;
        requireTask(group).seekToTimestamp(timestamps);
    }

    /**
     * 业务作用：让各目标分区从 broker 已提交的组位点重新读取。
     * 每个 owner 在本槽定位前核对提交记录和在途责任，缺少历史时拒绝该槽；其它槽可能已应用。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号
     */
    public void seekToCommitted(String group, String topic, int... partitions) {
        requireTask(group).seekToCommitted(toTps(topic, partitions));
    }

    /**
     * 业务作用：让各目标分区从 broker 已提交的组位点重新读取。
     * 每个 owner 在本槽定位前核对提交记录和在途责任，缺少历史时拒绝该槽；其它槽可能已应用。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     */
    public void seekToCommitted(String group, Collection<TopicPartition> partitions) {
        requireTask(group).seekToCommitted(partitions);
    }

    /**
     * 业务作用：为目标分区增加 USER 暂停原因，暂停后续拉取而不撤回已开始的处理。
     * poll 继续维护消费会话；暂停本身不触发再均衡，但不会阻止其它原因导致的再均衡。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号
     */
    public void pause(String group, String topic, int... partitions) {
        requireTask(group).pause(toTps(topic, partitions));
    }

    /**
     * 业务作用：为目标分区增加 USER 暂停原因，暂停后续拉取而不撤回已开始的处理。
     * poll 继续维护消费会话；暂停本身不触发再均衡，但不会阻止其它原因导致的再均衡。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     */
    public void pause(String group, Collection<TopicPartition> partitions) {
        requireTask(group).pause(partitions);
    }

    /**
     * 业务作用：对各 owner 执行命令时持有的分区增加 USER 暂停原因。
     * 本次未纳入目标的新分区不会自动暂停；已记住的相同坐标再次分配时仍受 USER 原因约束，跨槽失败可能部分生效。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group 目标 group.id
     */
    public void pauseAll(String group) {
        requireTask(group).pauseAll();
    }

    /**
     * 业务作用：解除目标分区的 USER 暂停原因；重试、容量和提交保护等其它暂停原因继续有效。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号
     */
    public void resume(String group, String topic, int... partitions) {
        requireTask(group).resume(toTps(topic, partitions));
    }

    /**
     * 业务作用：解除目标分区的 USER 暂停原因；重试、容量和提交保护等其它暂停原因继续有效。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     */
    public void resume(String group, Collection<TopicPartition> partitions) {
        requireTask(group).resume(partitions);
    }

    /**
     * 业务作用：清除各 owner 记住的全部 USER 暂停原因，包括暂时不在当前分配中的坐标；重新计算当前分区的暂停状态。
     * 重试、背压和提交保护等其它原因继续有效，不能据此保证立即恢复拉取。
     * <p>
     * 返回：全部目标完成控制才正常返回；未执行与结果未知分别报告，跨 owner 不承诺原子性，失败后须查询实际状态。
     *
     * @param group 目标 group.id
     */
    public void resumeAll(String group) {
        requireTask(group).resumeAll();
    }

    /**
     * 业务作用：解析分区查询类操作的等待上限。
     * 这类查询要排队到 poll 线程执行，耗时特征与发送不同，因此提供独立配置；
     * 未单独配置时回退到发布超时，避免多出一个必须配置的项。
     * <p>
     * 参数说明：无。
     *
     * @return 等待上限毫秒数。
     */
    private long partitionQueryTimeoutMs() {
        Integer t = properties.getPartitionQueryTimeoutMs();
        return t != null ? t : properties.getPublishTimeoutMs();
    }

    /**
     * 业务作用：查询运行中消费者在某分区的下一条待读位置，用于观测消费进度。
     * 读的是本进程消费者的内存位置，可能领先于已提交位点；计算滞后应改用已提交位点的查询。
     *
     * @param group     目标 group.id
     * @param topic     目标 topic
     * @param partition 分区号
     * @return 下一条待读的偏移量。
     * @throws IllegalStateException 该 group 没有运行中的消费任务，或查询超时
     */
    public long position(String group, String topic, int partition) {
        try {
            return requireTask(group).position(new TopicPartition(topic, partition))
                    .get(partitionQueryTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("query position failed group=" + group + " " + topic + "-" + partition, e);
        }
    }

    /**
     * 业务作用：查询运行中消费者当前实际持有的分区，用于确认再均衡后的分配结果。
     *
     * @param group 目标 group.id
     * @return 分区集合快照。
     * @throws IllegalStateException 该 group 没有运行中的消费任务，或查询超时
     */
    public Set<TopicPartition> assignment(String group) {
        try {
            return requireTask(group).assignmentSnapshot()
                    .get(partitionQueryTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("query assignment failed group=" + group, e);
        }
    }

    /**
     * 业务作用：将已提交位点查询送到运行中组的 owner，由原生 consumer 向 broker 查询。
     * 这不是读取本地处理进度缓存，也不证明业务已经完成；仅要求组在当前源有受管实例。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     * @return 分区到已提交位点的映射；无提交记录的分区值为 null。
     * @throws IllegalStateException 该 group 没有运行中的消费任务，或查询超时
     */
    public Map<TopicPartition, Long> committedOffsetsOfRunningConsumer(String group, Collection<TopicPartition> partitions) {
        try {
            return requireTask(group).committedOffsets(partitions)
                    .get(partitionQueryTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("query running committed offsets failed group=" + group + " partitions=" + partitions, e);
        }
    }

    // 分区副本元数据使用受管短连 consumer；offset 查询复用 Admin。

    /**
     * 业务作用：查询 topic 的分区元数据，含各分区的主副本与副本分布，用于容量规划与故障排查。
     * 每次调用新建受管短连消费者，保留 offlineReplicas 等原生返回字段。
     * 查询与消费 owner 分离；最终关闭拒绝新查询，并等待已准入查询及其连接真实退出。
     *
     * @param topic 目标 topic
     * @return 各分区的元数据列表；topic 不存在时由 Kafka 客户端按其配置处置。
     */
    public List<PartitionInfo> partitionsFor(String topic) {
        if (topic == null || topic.isBlank()) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "topic 不能为空");
        KafkaConsumer<String, byte[]> consumer = metadataConsumer();
        try {
            return consumer.partitionsFor(topic, Duration.ofMillis(adminTimeoutMs()));
        } finally {
            closeMetadataConsumer(consumer);
        }
    }

    /**
     * 业务作用：取 topic 的分区号列表，供需要遍历全部分区的调用方使用。
     *
     * @param topic 目标 topic
     * @return 分区号列表；无元数据时为空列表。
     */
    public List<Integer> partitionNumbers(String topic) {
        List<PartitionInfo> infos = partitionsFor(topic);
        List<Integer> out = new ArrayList<>();
        if (infos != null) for (PartitionInfo pi : infos) out.add(pi.partition());
        return out;
    }

    /**
     * 业务作用：查询指定 topic 各分区当前最早可用位点，用于判断历史数据的可回放范围。
     * 早于 broker 保留期的数据已被清理，因此该值会随时间前移。
     *
     * @param topic      目标 topic
     * @param partitions 分区号集合
     * @return 分区号到最早位点的映射。
     * @throws IllegalStateException 查询失败或超时
     */
    public Map<Integer, Long> beginningOffsets(String topic, Collection<Integer> partitions) {
        return toPartitionOffsetMap(offsetsQuery(toTps(topic, partitions), false));
    }

    /**
     * 业务作用：按 READ_UNCOMMITTED 隔离查询目标分区的末尾，供日志范围与积压观测使用。
     * 该值不等同于 read_committed 的 LSO；需要事务可读边界时使用 Admin 的显式隔离观测。
     *
     * @param topic      目标 topic
     * @param partitions 分区号集合
     * @return 分区号到末尾位点的映射。
     * @throws IllegalStateException 查询失败或超时
     */
    public Map<Integer, Long> endOffsets(String topic, Collection<Integer> partitions) {
        return toPartitionOffsetMap(offsetsQuery(toTps(topic, partitions), true));
    }

    /**
     * 业务作用：跨 topic 批量查询最早可用位点，一次请求覆盖多个 topic 的分区。
     *
     * @param partitions 目标分区集合
     * @return 分区到最早位点的映射；入参为空时返回空映射。
     * @throws IllegalStateException 查询失败或超时
     */
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return offsetsQuery(partitions, false);
    }

    /**
     * 业务作用：按 READ_UNCOMMITTED 隔离查询目标分区的末尾，供日志范围与积压观测使用。
     * 该值不等同于 read_committed 的 LSO；需要事务可读边界时使用 Admin 的显式隔离观测。
     *
     * @param partitions 目标分区集合
     * @return 分区到末尾位点的映射；入参为空时返回空映射。
     * @throws IllegalStateException 查询失败或超时
     */
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return offsetsQuery(partitions, true);
    }

    /**
     * 业务作用：查询某 group 在指定 topic 各分区的已提交位点。
     * 经由管理客户端查询，因此<b>不要求该 group 正在本进程消费</b>，可用于运维侧观测任意消费组。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号集合
     * @return 分区号到已提交位点的映射；无提交记录的分区值为 null。
     * @throws IllegalStateException 查询失败或超时
     */
    public Map<Integer, Long> committedOffsets(String group, String topic, Collection<Integer> partitions) {
        return toPartitionOffsetMap(committedOffsets(group, toTps(topic, partitions)));
    }

    /**
     * 业务作用：跨 topic 查询某 group 的已提交位点。
     * 结果按入参分区逐一填充，因此<b>入参中该 group 从未提交过的分区也会出现在结果里且值为 null</b>，
     * 与「已提交且位点为 0」区分开——两者对滞后计算的含义完全不同。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     * @return 分区到已提交位点的映射；无提交记录的分区值为 null。
     * @throws IllegalStateException 查询失败或超时
     */
    public Map<TopicPartition, Long> committedOffsets(String group, Collection<TopicPartition> partitions) {
        try {
            if (partitions != null && partitions.isEmpty()) return new LinkedHashMap<>();
            return new LinkedHashMap<>(admin().committedOffsets(group, partitions, Duration.ofMillis(adminTimeoutMs())));
        } catch (KafkaFailure e) {
            throw new IllegalStateException("query committed offsets failed group=" + group + " partitions=" + partitions, e);
        }
    }

    /**
     * 业务作用：查询某 group 在指定 topic 各分区的消费滞后，用于告警与容量评估。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号集合
     * @return 各分区的滞后快照。
     * @throws IllegalStateException 查询失败或超时
     */
    public List<KafkaPartitionLag> committedLag(String group, String topic, Collection<Integer> partitions) {
        return committedLag(group, toTps(topic, partitions));
    }

    /**
     * 业务作用：跨 topic 计算某 group 的消费滞后，即末尾位点与已提交位点之差。
     * <p>
     * 滞后取零下界：已提交位点在极端情形下可能超过查询到的末尾位点（两次查询之间存在时间差），
     * 负值对告警毫无意义且会掩盖真实积压。
     * <p>
     * 无提交记录的分区滞后为 null 而非末尾位点，因为「从未消费」与「消费落后了这么多」是两种情形，
     * 按后者计入总量会虚增积压。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     * @return 各分区的滞后快照；无提交记录的分区其已提交位点与滞后均为 null。
     * @throws IllegalStateException 查询失败或超时
     */
    public List<KafkaPartitionLag> committedLag(String group, Collection<TopicPartition> partitions) {
        if (partitions != null && partitions.isEmpty()) return new ArrayList<>();
        try {
            return new ArrayList<>(admin().committedLag(group, partitions, Duration.ofMillis(adminTimeoutMs())));
        } catch (KafkaFailure error) {
            throw new IllegalStateException("query committed lag failed group=" + group, error);
        }
    }

    /**
     * 业务作用：位点查询的内部收口，按需要的是首位点还是末位点向管理客户端一次性发起批量请求。
     * 批量而非逐分区查询，避免分区多时产生大量往返。
     *
     * @param partitions 目标分区集合，为 null 或空时直接返回空映射
     * @param end        true 查末尾位点，false 查最早可用位点
     * @return 分区到位点的映射。
     * @throws IllegalStateException 查询失败或超时
     */
    private Map<TopicPartition, Long> offsetsQuery(Collection<TopicPartition> partitions, boolean end) {
        if (partitions == null || partitions.isEmpty()) return new LinkedHashMap<>();
        try {
            return new LinkedHashMap<>(admin().offsets(partitions, end, Duration.ofMillis(adminTimeoutMs())));
        } catch (KafkaFailure e) {
            throw new IllegalStateException("query offsets failed end=" + end + " partitions=" + partitions, e);
        }
    }

    /**
     * 业务作用：为一次元数据查询创建独立 consumer，并登记到源的关闭责任中。
     * 随机组名用于隔离客户端身份；此连接不 subscribe、不加入消费组，也不提交位点。
     * <p>
     * 参数说明：无。
     *
     * @return 已登记的消费者，调用方必须通过 closeMetadataConsumer 归还；最终关闭后拒绝创建。
     */
    private KafkaConsumer<String, byte[]> metadataConsumer() {
        synchronized (lifecycleGate) {
            requireManagedAccess();
            // 与最终关闭共享门禁，确保关闭快照不会遗漏并发创建的原生连接。
            if (closing || closed) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "管理资源已关闭准入");
            KafkaConsumer<String, byte[]> consumer = KafkaNative.construct(() -> new KafkaConsumer<>(properties.buildConsumerProperties("nasa-kafka-meta-" + UUID.randomUUID())));
            metadataQueries.add(consumer);
            return consumer;
        }
    }

    /**
     * 业务作用：由查询所属线程释放临时连接，连接实际退出后才解除源级关闭责任。
     * <p>
     * 返回：关闭成功后移除登记；失败保留责任并报告未知，中断标志始终保留。
     *
     * @param consumer 本源登记且不再查询的连接。
     */
    private void closeMetadataConsumer(KafkaConsumer<String, byte[]> consumer) {
        // 查询等待的中断不应让资源释放立即短路；先完成原生关闭，再恢复调用者的取消信号。
        boolean interrupted = Thread.interrupted();
        try {
            consumer.close(Duration.ofMillis(Integer.MAX_VALUE));
            synchronized (lifecycleGate) {
                metadataQueries.remove(consumer);
                lifecycleGate.notifyAll();
            }
        } catch (Throwable failure) {
            metadataCloseFailure = failure;
            synchronized (lifecycleGate) {
                lifecycleGate.notifyAll();
            }
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "临时元数据资源关闭未确认", failure);
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /**
     * 业务作用：等待已准入的元数据调用及其原生连接全部退出，避免源注销后仍有活动连接。
     * <p>
     * 返回：登记清空才成功；超时、中断或原生关闭异常保留责任供后续观察。
     *
     * @param deadline 共同停机截止时间。
     */
    private void awaitMetadataClosed(long deadline) {
        synchronized (lifecycleGate) {
            while (!metadataQueries.isEmpty()) {
                if (metadataCloseFailure != null)
                    throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "临时元数据连接仍有未确认责任", metadataCloseFailure);
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "临时元数据查询尚未退出");
                try {
                    TimeUnit.NANOSECONDS.timedWait(lifecycleGate, remaining);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "临时元数据关闭等待被中断", failure);
                }
            }
        }
    }

    /**
     * 业务作用：把带 topic 的分区位点映射降维成按分区号索引，供单 topic 查询接口返回更简洁的结果。
     * <b>仅适用于单 topic 的查询结果</b>：跨 topic 使用会因分区号相同而相互覆盖。
     *
     * @param offsets 分区到位点的映射
     * @return 分区号到位点的映射，保持原有顺序。
     */
    private static Map<Integer, Long> toPartitionOffsetMap(Map<TopicPartition, Long> offsets) {
        Map<Integer, Long> out = new LinkedHashMap<>();
        offsets.forEach((tp, off) -> out.put(tp.partition(), off));
        return out;
    }

    // ---- AdminClient 基础能力 ----

    /**
     * 业务作用：列出当前连接身份可见的非内部 topic 名，用于运维盘点；不能据列表缺项推断目标不存在。
     * <p>
     * 参数说明：无。
     *
     * @return 可见 topic 名称集合；查询失败或超时抛出 IllegalStateException。
     */
    public Set<String> listTopics() {
        try {
            return admin().listTopics(Duration.ofMillis(adminTimeoutMs()));
        } catch (KafkaFailure e) {
            throw new IllegalStateException("list kafka topics failed", e);
        }
    }

    /**
     * 业务作用：判断 topic 是否存在，用于发送前的前置检查。
     * 查询明确目标，只有 broker 明确报告不存在才返回 false；授权失败不能据此触发自动创建。
     *
     * @param topic 目标 topic
     * @return 存在返回 true。
     * @throws IllegalStateException 查询失败或超时
     */
    public boolean topicExists(String topic) {
        try {
            return admin().topicExists(topic, Duration.ofMillis(adminTimeoutMs()));
        } catch (KafkaFailure error) {
            throw new IllegalStateException("query topic existence failed topic=" + topic, error);
        }
    }

    /**
     * 业务作用：创建 topic 并等待创建完成。
     * topic 已存在时按失败处理并抛出异常；需要「有则跳过」语义应改用幂等的重载。
     * <p>
     * 返回：broker 确认创建后正常返回；已有资源或明确失败抛出异常，超时不能证明创建未发生。
     *
     * @param topic             目标 topic
     * @param partitions        分区数，创建后只能增加不能减少
     * @param replicationFactor 副本数，不得超过集群 broker 数
     * @throws IllegalStateException 创建失败、超时，或 topic 已存在
     */
    public void createTopic(String topic, int partitions, short replicationFactor) {
        try {
            admin().createTopic(topic, partitions, replicationFactor, Duration.ofMillis(adminTimeoutMs()));
        } catch (KafkaFailure e) {
            throw new IllegalStateException("create kafka topic failed topic=" + topic, e);
        }
    }

    /**
     * 业务作用：幂等地创建 topic，供服务启动时自建所需 topic。
     * <p>
     * 只把「已存在」这一种失败转成返回 false，其余失败仍然抛出：
     * 副本数超过 broker 数、无权限、集群不可达都是需要人介入的问题，
     * 统统吞成 false 会让服务带着一个并不存在的 topic 继续启动。
     * <p>
     * 返回 false 时<b>不校验既有 topic 的分区数与副本数</b>是否与入参一致，
     * 既有 topic 的配置与预期不符不会被发现。
     *
     * @param topic             目标 topic
     * @param partitions        分区数
     * @param replicationFactor 副本数
     * @return 本次创建成功返回 true；topic 已存在返回 false。
     * @throws IllegalStateException 除「已存在」之外的任何创建失败
     */
    public boolean createTopicIfAbsent(String topic, int partitions, short replicationFactor) {
        try {
            return admin().createTopicIfAbsent(topic, partitions, replicationFactor, Duration.ofMillis(adminTimeoutMs())) == KafkaAdmin.TopicCreation.CREATED;
        } catch (KafkaFailure e) {
            throw new IllegalStateException("create kafka topic failed topic=" + topic, e);
        }
    }

    /**
     * 业务作用：请求删除 topic 并等待 broker 操作回执；明确不存在时视为目标已达成。
     * 删除会破坏该 topic 历史数据的恢复依据；正常返回不保证磁盘清理和消费组位点清理同步完成，未知结果应回读资源状态。
     * <p>
     * 返回：broker 确认删除或明确目标不存在时正常返回；其它失败传播，超时不能证明资源仍存在。
     *
     * @param topic 目标 topic
     * @throws IllegalStateException 除「topic 不存在」之外的任何删除失败
     */
    public void deleteTopic(String topic) {
        try {
            admin().deleteTopic(topic, Duration.ofMillis(adminTimeoutMs()));
        } catch (KafkaFailure e) {
            // 仅明确的不存在等价于删除目标已达成；权限拒绝或结果未知仍保留在异常原因链中。
            if (e.getCause() instanceof UnknownTopicOrPartitionException) return;
            throw new IllegalStateException("delete kafka topic failed topic=" + topic, e);
        }
    }

    /**
     * 业务作用：把 topic 的分区数扩到指定总数，用于消费能力不足时提升并行度。
     * <p>
     * Kafka <b>不支持减少分区</b>，因此扩容不可撤销；给定总数小于等于现有分区数时失败。
     * <p>
     * 扩容会改变 key 到分区的映射：同一 key 在扩容后可能落到不同分区，
     * <b>该 key 的历史消息与新消息不再保序</b>。依赖按 key 保序的业务需要评估这一影响。
     * <p>
     * 返回：broker 确认扩容后正常返回；失败传播，超时后须回读实际分区数量。
     *
     * @param topic           目标 topic
     * @param totalPartitions 扩容后的总分区数
     * @throws IllegalStateException 扩容失败或超时
     */
    public void increasePartitions(String topic, int totalPartitions) {
        try {
            admin().increasePartitions(topic, totalPartitions, Duration.ofMillis(adminTimeoutMs()));
        } catch (KafkaFailure e) {
            throw new IllegalStateException("increase kafka topic partitions failed topic=" + topic
                    + " totalPartitions=" + totalPartitions, e);
        }
    }

    /**
     * 业务作用：在同一源的生命周期锁内按需创建并复用 Admin 客户端，供该源的管理与位点查询使用。
     * 不同源各持有自己的实例；关闭准入与资源创建共用锁，防止停机期间重建连接。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 当前源共享的 Admin 实例；进入资源关闭阶段后以 LIFECYCLE 拒绝。
     */
    public AdminClient nativeAdmin() {
        synchronized (lifecycleGate) {
            if (closing || closed) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "管理资源已关闭准入");
            if (adminClient != null) return adminClient;
            adminClient = KafkaNative.construct(() -> AdminClient.create(properties.buildAdminProperties()));
            log.info("[kafka] admin client initialized");
            return adminClient;
        }
    }

    /**
     * 业务作用：解析管理类操作的等待上限。
     * 按「接口级超时 → 请求级超时 → 发布超时」依次回退，使调用方最少可以一项都不配。
     * <p>
     * 参数说明：无。
     *
     * @return 等待上限毫秒数。
     */
    private long adminTimeoutMs() {
        if (properties.getAdminDefaultApiTimeoutMs() != null) return properties.getAdminDefaultApiTimeoutMs();
        if (properties.getAdminRequestTimeoutMs() != null) return properties.getAdminRequestTimeoutMs();
        return properties.getPublishTimeoutMs();
    }

    /**
     * 业务作用：查找当前源已登记的普通消费组实例，供观测与控制入口继续检查其实际状态。
     *
     * @param group 目标 group.id
     * @return 已登记实例，可能尚未 Ready 或正在停止；缺失时抛出 NO_SUCH_GROUP。
     */
    private KafkaGroup requireTask(String group) {
        KafkaGroup task = pollTasks.get(group);
        if (task == null)
            throw new KafkaFailure(KafkaFailure.Code.NO_SUCH_GROUP, "no running consumer for group: " + group);
        return task;
    }

    /**
     * 业务作用：把 topic 与一组分区号组装成分区标识列表，供变长参数形式的接口使用。
     *
     * @param topic      目标 topic
     * @param partitions 分区号，为 null 时返回空列表
     * @return 分区标识列表。
     */
    private static List<TopicPartition> toTps(String topic, int... partitions) {
        List<TopicPartition> tps = new ArrayList<>();
        if (partitions != null) for (int p : partitions) tps.add(new TopicPartition(topic, p));
        return tps;
    }

    /**
     * 业务作用：把 topic 与一组分区号组装成分区标识列表，供集合形式的接口使用。
     *
     * @param topic      目标 topic
     * @param partitions 分区号集合，为 null 时返回空列表
     * @return 分区标识列表。
     */
    private static List<TopicPartition> toTps(String topic, Collection<Integer> partitions) {
        List<TopicPartition> tps = new ArrayList<>();
        if (partitions != null) for (int p : partitions) tps.add(new TopicPartition(topic, p));
        return tps;
    }

    // ==================== 生命周期 ====================

    /**
     * 业务作用：完成源身份核验与 listener 路由装配后启动已登记的普通消费组。
     * 没有组时不创建消费 owner；声明 expected-cluster-id 时仍会建立 Admin 连接核验。
     * 首次消费延迟只延后启动，不代表业务 Ready，调用方仍须等待所需分配条件。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：已发起本地 owner 启动；配置或身份门禁失败时抛出异常，broker Ready 另行等待。
     */
    @Override
    public synchronized void before() {
        verifyClusterIdentity();
        if (started) return;
        if (consumersStopped || closing || closed) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "源已停止消费");
        if (context != null) for (KafkaListener listener : context.getBeansOfType(KafkaListener.class).values())
            registerListener(listener);
        started = true;
        for (KafkaGroup task : pollTasks.values()) task.start(properties.getStartConsumeDelayMs());
    }

    /**
     * 业务作用：按共同关闭期限先等待消费、事务、回放和转发责任退出，再关闭发布、管理和观测资源。
     * 先保留依赖，确保已接纳调用有机会完成；超时保留未退出资源和名称占用，重复调用继续收尾。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：全部受管资源退出后正常返回；超时、中断或关闭失败抛出异常，不把等待失败当作已关闭。
     */
    @Override
    public void after() {
        shutdownUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(properties.getShutdownTimeoutMs()));
    }

    /**
     * 业务作用：同步封闭消费登记并向全部已接纳任务发布永久停止意图，保留其收尾依赖。
     * <p>
     * 参数说明：无。
     * 返回：不等待退出、不释放名称或原文；后续 stopConsumersUntil 或 shutdownUntil 取得实际退出结论。
     */
    public synchronized void requestConsumerStop() {
        // 停止权威与等待预算分开；即使后续期限已经耗尽，也不能继续派发或接受迟到确认。
        consumersStopped = true;
        for (KafkaGroup task : pollTasks.values()) task.requestStop();
        transactions.stop();
        transfers.stop();
        replays.stop();
    }

    /**
     * 业务作用：关闭消费准入并等待所有 owner 退出，保留 producer 供业务收尾。
     * <p>
     * 返回：消费停止或明确超时。
     *
     * @param deadline 单调纳秒截止时间。
     */
    public void stopConsumersUntil(long deadline) {
        List<KafkaGroup> tasks;
        List<KafkaReplayTask> replayTasks;
        List<KafkaTransferRegistry.Link> transferTasks;
        List<KafkaTransactionTask> transactionTasks;
        synchronized (this) {
            // 先关闭消费准入再冻结停止集合，禁止快照之外并发出现新的 owner。
            requestConsumerStop();
            transactionTasks = transactions.stop();
            transferTasks = transfers.stop();
            replayTasks = replays.stop();
            tasks = List.copyOf(pollTasks.values());
            for (KafkaGroup task : tasks) task.requestStop();
        }
        // owner 与 handler 收尾可能需要查询本代理，等待期间不能占用代理的装配监视器。
        for (KafkaGroup task : tasks) task.shutdownUntil(deadline);
        for (KafkaTransactionTask task : transactionTasks)
            if (!task.awaitDrained(Duration.ofNanos(Math.max(0, deadline - System.nanoTime()))))
                throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "事务任务仍有实际责任");
        for (KafkaReplayTask task : replayTasks)
            if (!task.awaitDrained(Duration.ofNanos(Math.max(0, deadline - System.nanoTime()))))
                throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "回放任务仍有实际责任");
        for (KafkaTransferRegistry.Link task : transferTasks)
            if (!task.awaitDrained(deadline))
                throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "转发目标仍有实际责任");
    }

    /**
     * 业务作用：按消费、发送、管理资源顺序在共同期限内关闭。
     * <p>
     * 返回：全部资源实际退出才报告关闭。
     *
     * @param deadline 单调纳秒截止时间。
     */
    public void shutdownUntil(long deadline) {
        requestConsumerStop();
        try {
            if (!shutdownGate.tryLock(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "另一次关闭仍在推进");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "关闭等待被中断");
        }
        try {
            if (closed) return;
            // 消费收口仍可发送 DLT，只有所有 owner 退出后才关闭发布准入。
            stopConsumersUntil(deadline);
            synchronized (lifecycleGate) {
                closing = true;
            }
            for (ProducerLane lane : List.copyOf(lanes.values())) lane.closeUntil(deadline);
            awaitMetadataClosed(deadline);
            closeAdminUntil(deadline);
            // 业务资源退出之后才关闭观测；排队事件可丢弃，用户回调仍需真实归还线程。
            if (metrics != null) metrics.closeUntil(deadline);
            // 所有原生资源实际退出后才释放静态身份，新实例不能接管仍有活动责任的名称。
            closed = true;
            KafkaClients.remove(this);
        } finally {
            shutdownGate.unlock();
        }
    }

    /**
     * 业务作用：在共同停机期限内等待同一个 Admin 关闭任务真正退出。
     * <p>
     * 返回：原生关闭与关闭线程全部结束才成功；超时保留任务供再次收口，不提前注销 source。
     *
     * @param deadline 绝对截止时间；调用者持有 shutdownGate 且已关闭资源准入。
     */
    private void closeAdminUntil(long deadline) {
        if (adminClient == null) return;
        if (adminCloser == null) {
            AdminClient client = adminClient;
            // 原生 close 可能超过调用者剩余预算；独立持有真实责任，避免本地等待返回后误报整个源已关闭。
            Thread closer = Thread.ofPlatform().daemon(true).name("nasa-kafka-admin-close-" + sourceId).unstarted(() -> {
                try {
                    client.close(Duration.ofMillis(Integer.MAX_VALUE));
                } catch (Throwable failure) {
                    adminCloseFailure = failure;
                }
            });
            closer.start();
            adminCloser = closer;
        }
        try {
            long remaining = Math.max(0, deadline - System.nanoTime());
            if (remaining > 0) TimeUnit.NANOSECONDS.timedJoin(adminCloser, remaining);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "管理资源关闭等待被中断", error);
        }
        if (adminCloser.isAlive()) throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "管理原生资源仍在关闭");
        if (adminCloseFailure != null)
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "管理原生资源关闭未确认", adminCloseFailure);
    }

    /**
     * 业务作用：在开始消费前显式登记 listener。
     *
     * @param listener 完整路由声明。
     * @return 登记成功返回当前代理；同一路由重复登记同一 listener 实例允许，冲突实例或路由冻结后拒绝。
     */
    public synchronized KafkaProxy register(KafkaListener<?, ?> listener) {
        if (started || consumersStopped || closing || closed)
            throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "路由已经冻结");
        registerListener(listener);
        return this;
    }

    /**
     * 业务作用：显式接入带消费注解的对象，使用共同路由与生命周期。
     *
     * @param bean 实例。
     * @return 当前代理，非法签名或路由在启动前失败。
     */
    public KafkaProxy registerAnnotated(Object bean) {
        for (KafkaListener<?, ?> listener : KafkaMethodListeners.from(bean)) register(listener);
        return this;
    }

    /**
     * 业务作用：把一个 listener 接进路由表，并确保其所属 group 的消费任务存在。
     * <p>
     * 未声明任何 topic 的 listener 只记警告并跳过：它不可能收到消息，
     * 但也不应因此让整个应用启动失败。
     * <p>
     * 消费任务按 group 复用：同一 group 的多个 listener 共用配置的 owner 槽集合，
     * 每个 listener 各建一条会让同一 group 的成员数虚增，触发无谓的再均衡。
     * <p>
     * 建出任务后再逐个追加订阅，覆盖「任务已存在、本 listener 带来新 topic」的情形——
     * 只在建任务时取一次订阅集，后注册的 listener 的 topic 就会被漏掉。
     * <p>
     * 返回：有 topic 时登记完整路由并建立或更新组的订阅声明；无 topic 时记录警告并跳过，非法路由抛出异常。
     *
     * @param listener 待注册的 listener
     */
    private void registerListener(KafkaListener listener) {
        String[] topics = listener.topics();
        if (topics == null || topics.length == 0) {
            log.warn("[kafka] listener {} has no topics, skip", listener.getClass().getName());
            return;
        }
        String group = resolveGroup(listener);
        if (transactions.contains(group)) throw new TransactionException(TransactionException.Code.BUSY);
        String event = registerEntries(group, listener, topics);
        // 创建/复用 group 的 SUBSCRIBE poll task
        ConcurrentMap<String, ConcurrentMap<String, ListenerEntry>> byTopic = listenerRegistry.get(group);
        pollTasks.computeIfAbsent(group, g -> new KafkaGroup(this, group, byTopic.keySet()));
        // 后续 listener 添加新 topic 时, 同步告诉已有 task
        KafkaGroup task = pollTasks.get(group);
        for (String topic : topics) {
            if (!StringUtils.isBlank(topic)) task.subscribe(topic);
        }
        log.info("[kafka] register listener group={} topics={} event={} kind={} listener={}",
                group, Set.of(topics), event, ListenerKind.of(listener), listener.getClass().getName());
    }

    /**
     * 业务作用：确定当前 listener 的精确消费组身份。
     * 声明 broadcastScope 时按共同广播身份编码，显式 group 与其互斥；否则仅在 group 为 null 或空串时使用源默认组。
     * 空白但非空的显式组不会回退，非法身份在建立消费资源前拒绝。
     *
     * @param listener 待解析的 listener
     * @return 已验证且保持原文的 group.id，后续注册与原生配置沿用这一快照。
     * @throws KafkaFailure listener 与全局配置均未提供组，或组名不满足共同组身份边界
     */
    @SuppressWarnings("rawtypes")
    private String resolveGroup(KafkaListener listener) {
        String group = listener.group();
        String scope = listener.broadcastScope(), instance = listener.instanceId();
        if (scope != null) {
            // 广播身份决定位点归属，不能同时接受另一份显式 group 并猜测调用方意图。
            if (group != null && !group.isEmpty())
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "broadcastScope 与显式 group 互斥");
            if (instance == null) instance = properties.getBroadcastInstanceId();
            return BroadcastIdentity.group(scope, instance == null ? commonBroadcastInstanceId() : instance);
        }
        if (instance != null) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "instanceId 要求声明 broadcastScope");
        if (group == null || group.isEmpty()) group = properties.getGroupId();
        KafkaWire.validateGroup(group);
        return group;
    }

    /**
     * 业务作用：把 listener 按 group、topic、事件三级注册进路由表。
     * <p>
     * 事件名为空时失败：事件名是 topic 之下的二级路由键，为空会让该 listener 永远匹配不上任何消息。
     * <p>
     * 同一组 (group, topic, 事件) 上已有<b>不同</b>的 listener 时启动期失败，而不是静默覆盖：
     * 一条消息只会路由给一个 listener，静默覆盖会让另一个 listener 悄无声息地收不到消息，
     * 事后极难定位。需要多个 listener 都收到同一消息应改用不同的 group。
     * <p>
     * 本方法只登记路由，不创建消费任务，使手动分配模式能自行决定任务的建立方式。
     *
     * @param group    目标 group.id
     * @param listener 待注册的 listener
     * @param topics   该 listener 声明的 topic，空白或重复项拒绝
     * @return 该 listener 声明的事件名。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private String registerEntries(String group, KafkaListener listener, String[] topics) {
        // 所有注册形态在发布路由表之前复用同一身份门禁，不允许逻辑责任在 broker 上合并。
        KafkaWire.validateGroup(group);
        String event = listener.event();
        // 注册与发布共用身份门禁，避免保留永远无法被合法线路字节匹配的路由。
        KafkaWire.eventBytes(event);
        TypeReference<?> payloadType = PayloadTypeResolver.resolve(listener);
        Class<?> raw = rawClass(payloadType);
        if (raw != null && ProtocolBytes.class.isAssignableFrom(raw) && (raw.isInterface() || Modifier.isAbstract(raw.getModifiers())))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "ProtocolBytes 需要可实例化类型");
        ListenerKind kind = ListenerKind.of(listener);
        // 没有 record 的回调无法持有受管 Ack；按键批次也没有已定义的独立失败前缀，必须在消费前拒绝。
        if (listener.ackMode() == AckMode.MANUAL && kind != ListenerKind.RECORD_SINGLE && kind != ListenerKind.RECORD_BATCH && kind != ListenerKind.PASSTHROUGH)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "Manual 需要 record listener");
        if (listener.executionMode() == ExecutionMode.KEY_ORDERED && (kind == ListenerKind.BATCH || kind == ListenerKind.RECORD_BATCH))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "KEY_ORDERED 暂不支持批 listener");
        // 同步透传只声明分区有序合同；拒绝以未定义的按键并发改变本地 fan-out 的失败与重投边界。
        if (kind == ListenerKind.PASSTHROUGH && (listener.executionMode() != ExecutionMode.PARTITION_ORDERED
                || listener instanceof KafkaAsyncListener || listener instanceof KafkaAsyncBatchListener))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "透传 listener 仅支持同步分区有序执行");
        String metricId = listener.metricId();
        if (metricId == null || metricId.isBlank() || metricId.getBytes(StandardCharsets.UTF_8).length > 1024
                || metricId.codePoints().anyMatch(Character::isISOControl))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "listener metricId 必须是最多 1024 UTF-8 字节的稳定身份");
        ListenerEntry entry = new ListenerEntry(payloadType, listener, kind, listener.ackMode(), listener.executionMode(), listener.nullable(), metricId);
        if (topics == null || topics.length == 0 || Arrays.stream(topics).anyMatch(StringUtils::isBlank)
                || new LinkedHashSet<>(List.of(topics)).size() != topics.length)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "topics 必须非空且没有重复项");
        ConcurrentMap<String, ConcurrentMap<String, ListenerEntry>> byTopic = listenerRegistry.get(group);
        // 先验证整个声明，避免最后一个 topic 冲突时留下前面已发布的半份路由。
        if (byTopic != null) {
            for (var routes : byTopic.values())
                for (ListenerEntry existing : routes.values())
                    if (existing.executionMode() != entry.executionMode())
                        throw new KafkaFailure(KafkaFailure.Code.CONFIG, "同组必须使用一致 execution-mode");
            for (String topic : topics) {
                var routes = byTopic.get(topic);
                ListenerEntry existing = routes == null ? null : routes.get(event);
                if (existing != null && existing.listener() != listener)
                    throw new KafkaFailure(KafkaFailure.Code.CONFIG,
                            "同一 group/topic/event 已有处理器: " + group + "/" + topic + "/" + event);
            }
        }
        byTopic = listenerRegistry.computeIfAbsent(group, g -> new ConcurrentHashMap<>());
        for (String topic : topics) byTopic.computeIfAbsent(topic, t -> new ConcurrentHashMap<>()).put(event, entry);
        return event;
    }

    // ==================== 显式旧信封兼容解码 ====================

    /**
     * 业务作用：在显式启用的兼容解码路径中识别 PooledEvtData 或 topic 字段匹配的 Map 信封，再还原业务值。
     * Map 判定是启发式规则，业务 Map 恰好满足条件也会被拆包；普通 typed 路线应由 legacyEnvelope 开关决定是否进入。
     * 捕获的类型转换失败返回 null，后续由记录策略判定，不在此发送死信或推进位点。
     *
     * @param raw       已按通用类型反序列化出的原始对象，可为 null
     * @param reference 目标业务类型引用，可为 null
     * @param topic     当前 topic，用于判定是否为信封
     * @return 业务对象；入参为 null 或反序列化失败时返回 null。
     */
    @SuppressWarnings("unchecked")
    Object deserializeUnwrap(Object raw, TypeReference reference, String topic) {
        if (raw == null) return null;
        // 段 1: typing=true → PooledEvtData 实例
        if (raw instanceof PooledEvtData pm) {
            Object data = pm.getData();
            Map<String, Object> pt = pm.getPassthrough();
            pm.recycle();
            if (data != null && pt != null) KafkaProxyHolder.set(data, pt);
            return data;
        }
        // 段 2: typing=false → Map 且 FIELD_TOPIC == 当前 topic 名 → 认定 wrap, 必拆包返回 data
        if (raw instanceof Map<?, ?> rawMap
                && topic.equals(rawMap.get(PooledEvtData.FIELD_TOPIC))) {
            Object dataRaw = rawMap.get(PooledEvtData.FIELD_DATA);
            Map<String, Object> pt = MapUtils.getObject(rawMap, PooledEvtData.FIELD_PASSTHROUGH);
            Object data;
            if (reference != null && !properties.isActivateDefaultTyping() && dataRaw != null) {
                try {
                    data = ObjMprUtils.deserialize(ObjMprUtils.toString(dataRaw), reference);
                } catch (Throwable t) {
                    String tid = MapUtils.getString(pt, AnyHolder.TRACE_ID);
                    if (tid != null) AnyHolder.set(AnyHolder.TRACE_ID, tid);
                    log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
                    return null;
                }
            } else {
                data = dataRaw;
            }
            if (data != null && pt != null) KafkaProxyHolder.set(data, pt);
            return data;
        }
        // 段 3: 不是 wrap, 走原路径 (按 reference 反序列化)
        if (reference != null && !properties.isActivateDefaultTyping()) {
            try {
                return ObjMprUtils.deserialize(ObjMprUtils.toString(raw), reference);
            } catch (Throwable t) {
                log.error("{} {}", AnyHolder.getTraceId(), t.getMessage(), t);
                return null;
            }
        }
        return raw;
    }

    /**
     * 业务作用：在兼容解码路径中按明确的 ProtocolBytes 标志选择二进制解码，否则进行通用 JSON/信封还原。
     * 二进制路径要求可实例化的 ProtocolBytes 类型，声明的模式与本地模式不一致时拒绝；模式错误不能视作业务数据。
     * 解码得到业务值后再尝试附加透传上下文，透传解析失败不会清除已有业务值。
     *
     * @param bytes                负载字节，可为 null
     * @param reference            listener 声明的目标类型引用
     * @param topic                当前 topic
     * @param protocolBytesPayload 负载是否为二进制编码
     * @param protocolBytesMode    消息头中的模式标识，为 null 时跳过一致性校验
     * @param passthroughHeader    透传头原始字节，可为 null
     * @return 解码值；已捕获的类型或二进制解码失败返回 null，透传头损坏只忽略该上下文。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    Object deserializeValue(byte[] bytes, TypeReference reference, String topic, boolean protocolBytesPayload,
                            String protocolBytesMode, byte[] passthroughHeader) {
        Object data;
        if (protocolBytesPayload) {
            Class<?> rawClass = rawClass(reference);
            if (rawClass == null || !ProtocolBytes.class.isAssignableFrom(rawClass)) {
                log.error("{} [kafka-poll] ProtocolBytes payload requires listener.paramType() to be ProtocolBytes class, topic={}",
                        AnyHolder.getTraceId(), topic);
                return null;
            }
            try {
                ProtocolBytes inst = (ProtocolBytes) ReflectUtils.newInstance(rawClass);
                // header 带 mode 时, 校验与 listener class 声明的 mode 一致 (不一致必然解码错乱, fail-fast).
                if (protocolBytesMode != null && !protocolBytesMode.equalsIgnoreCase(inst.mode().name())) {
                    log.error("{} [kafka-poll] ProtocolBytes mode 不一致 header={} listener={} topic={} type={}",
                            AnyHolder.getTraceId(), protocolBytesMode, inst.mode(), topic, rawClass.getName());
                    return null;
                }
                data = inst.decodeBytes(bytes);
            } catch (Throwable t) {
                log.error("{} [kafka-poll] ProtocolBytes decode failed topic={} type={}",
                        AnyHolder.getTraceId(), topic, rawClass.getName(), t);
                return null;
            }
        } else {
            Object raw = this.deserializeRaw(bytes);
            data = this.deserializeUnwrap(raw, reference, topic);
        }
        // header 承载的透传 → 关联到解码后的业务对象 (与 ProtocolBytes 二进制 payload 共存;
        // 旧 PooledEvtData 信封消息的透传已在 deserializeUnwrap 内关联, 此处只补 header 路径)。
        if (data != null && passthroughHeader != null) {
            Map<String, Object> pt = this.parsePassthroughHeader(passthroughHeader);
            if (pt != null) KafkaProxyHolder.set(data, pt);
        }
        return data;
    }

    /**
     * 业务作用：把透传头的原始字节解析成键值映射。
     * 解析失败只记警告并返回 null，不影响消息本身的投递：透传是附加上下文，
     * 因为它损坏而丢弃一条本可正常处理的业务消息并不划算。
     *
     * @param json 透传头的原始字节
     * @return 透传映射；解析失败时为 null。
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parsePassthroughHeader(byte[] json) {
        try {
            return ObjMprUtils.OBJECT_MAPPER.readValue(json, LinkedHashMap.class);
        } catch (Throwable t) {
            log.warn("{} [kafka-poll] passthrough header 解析失败", AnyHolder.getTraceId(), t);
            return null;
        }
    }

    /**
     * 业务作用：为兼容信封解码先尝试 JSON 通用解析，失败时保留 UTF-8 字符串表示。
     * 此回退仅属于兼容路径，不能推导普通 typed JSON 消费会接受未加引号的文本。
     *
     * @param bytes 负载字节，可为 null 或空
     * @return 解析出的通用对象；负载为空时返回 null；非 JSON 时返回其 UTF-8 字符串形式。
     */
    private Object deserializeRaw(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        try {
            return ObjMprUtils.OBJECT_MAPPER.readValue(bytes, Object.class);
        } catch (Throwable ignored) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    /**
     * 业务作用：从类型引用中取出可用于反射实例化与类型判定的原始类。
     * 泛型类型要剥掉类型参数才能拿到原始类，否则无法做实例化与继承关系判断。
     *
     * @param reference 类型引用，可为 null
     * @return 原始类；入参为 null 或类型形态无法归约到原始类时返回 null。
     */
    private static Class<?> rawClass(TypeReference reference) {
        if (reference == null) return null;
        Type type = reference.getType();
        if (type instanceof Class<?> c) return c;
        if (type instanceof ParameterizedType pt && pt.getRawType() instanceof Class<?> c) return c;
        return null;
    }

}

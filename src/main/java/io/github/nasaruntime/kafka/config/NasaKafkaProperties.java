package io.github.nasaruntime.kafka.config;

import io.github.nasaruntime.kafka.consumer.BroadcastIdentity;
import io.github.nasaruntime.kafka.protocol.KafkaWire;

import io.github.nasaruntime.kafka.consumer.RecordPolicy;
import io.github.nasaruntime.kafka.consumer.TombstonePolicy;
import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.support.KafkaCapabilities;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 类作用 - Kafka 组件配置项, 负责承载 producer、consumer、admin 和框架行为配置。
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "nasa.kafka")
public class NasaKafkaProperties {

    /**
     * 每个订阅组的 owner 槽数，默认一；固定 assign 始终单槽。
     */
    private int consumerConcurrency = 1;
    /**
     * 按最终 group.id 覆盖并发槽数，容量仍受同一组配置总额约束。
     */
    private Map<String, Integer> groupConcurrency = new LinkedHashMap<>();

    /**
     * 是否启用本组件；关闭时自动装配不建立源及后台资源。
     */
    private boolean enabled = true;
    /**
     * 省略源声明时采用的规范源名；不影响显式未知源的拒绝语义。
     */
    private String defaultSource = "primary";
    /**
     * 是否占有进程静态源目录；独立容器可以关闭导出以隔离同名源。
     */
    private boolean exportStatic = true;
    /**
     * 是否授权业务句柄显式取得管理写视图；默认 admin 视图始终只读，实际操作仍须 broker ACL。
     */
    private boolean adminWrite = false;
    /**
     * 按规范源名声明相互独立的集群；与根级单源参数互斥。
     */
    private Map<String, NasaKafkaProperties> sources = new LinkedHashMap<>();
    /**
     * 源内默认发布配置的显式覆盖，null 字段保留对应默认值。
     */
    private KafkaSettings.Producer producer;
    /**
     * 原生 consumer 参数的显式覆盖，自动提交始终禁止。
     */
    private KafkaSettings.Consumer consumer;
    /**
     * 提交、重试、容量与生命周期参数的显式覆盖。
     */
    private KafkaSettings.Behavior behavior;
    /**
     * 源的统一认证与 TLS 参数；客户端专属 raw 参数不能改写此认证域。
     */
    private KafkaSettings.Security security;
    /**
     * 管理客户端参数；与 consumer、producer 共用源身份。
     */
    private KafkaSettings.Admin admin;
    /**
     * 预先声明的独立发布通道；default 为保留通道，不允许重复声明。
     */
    private Map<String, KafkaSettings.Producer> producerLanes = new LinkedHashMap<>();
    /**
     * 整个发送责任周期允许保留的消息数，覆盖准入、原生发送与回执处理。
     */
    private int queueBufferingMaxMessages = 10000;
    /**
     * 发送责任的字节预算，单位 KiB，计入原始 key、value 与 header。
     */
    private long queueBufferingMaxKbytes = 65536;
    /**
     * 发送回执的预留数量上限；用户回调阻塞期间仍占用该额度。
     */
    private int fireObserverCapacity = 10000;
    /**
     * 当前发布代次连续无成功交付达到该毫秒数后，才允许按失败阈值切换。
     */
    private long stalledRebuildAfterMs = 60000;
    /**
     * 无进展切换至少需要的连续原生终态失败数；本地准入拒绝不计入。
     */
    private int stalledRebuildMinFailures = 2;
    /**
     * 两次无进展切换之间的最短毫秒间隔，限制故障期间的资源重建频率。
     */
    private long stalledRebuildCooldownMs = 60000;
    /**
     * 每个 owner 控制邮箱的容量；满时返回未执行，不等待无限排队。
     */
    private int controlQueueCapacity = 1024;
    /**
     * 控制命令从排队开始计算的总预算，单位毫秒；执行中超时不代表撤销。
     */
    private long controlTimeoutMs = 30000;
    /**
     * 两段关闭共用的总预算，单位毫秒；超时保留实际活动责任。
     */
    private long shutdownTimeoutMs = 30000;
    /**
     * 业务调用有效确认窗口，单位毫秒；零表示无限。超时不释放仍执行的顺序槽。
     */
    private long handlerTimeoutMs = 0;
    /**
     * 失败责任再次派发前的退避毫秒；不以 sleep 阻塞其他分区处理。
     */
    private long consumerRetryMs = 500;
    /**
     * 正常提交的时间触发间隔，单位毫秒；仅提交连续安全前缀。
     */
    private long commitIntervalMs = 1000;
    /**
     * 正常提交的安全记录数量触发阈值，不授权越过前方未完成记录。
     */
    private int commitBatchRecords = 1000;
    /**
     * 每分区最多保留的已观察记录数，broker 提交确认后释放。
     */
    private int maxProgressRecordsPerPartition = 2048;
    /**
     * 同一分区跨 owner 的输入原文及副本预留字节上限，失权但仍存活的调用继续计费。
     */
    private long maxProgressBytesPerPartition = 67108864;
    /**
     * 同一分区同一原始 key 的输入责任数量；null key 使用本分区独立后备域。
     */
    private int maxProgressRecordsPerKey = 1024;
    /**
     * 同一分区同一原始 key 的输入原文及副本预留字节上限。
     */
    private long maxProgressBytesPerKey = 16777216;
    /**
     * 一个 group 跨分区保留的责任数量上限。
     */
    private int maxProgressRecordsPerGroup = 65536;
    /**
     * 一个 group 的原文及框架副本预留总上限，按原始 key、value 和有序 header 字节和的八倍计费。
     */
    private long maxProgressBytesPerGroup = 67108864;
    /**
     * 同一组件加载环境内普通消费输入的记录总上限；所有源必须声明同值，运行期不改变。
     */
    private int maxProgressRecordsPerProcess = 1048576;
    /**
     * 普通消费输入的进程共享原文及副本预留字节上限；不包含 native 预取和解码对象堆内存。
     */
    private long maxProgressBytesPerProcess = 1073741824;
    /**
     * 同一源全部普通消费组的输入记录总上限。
     */
    private int maxProgressRecordsPerSource = 262144;
    /**
     * 同一源全部普通消费组的输入原文及副本预留字节总上限。
     */
    private long maxProgressBytesPerSource = 268435456;
    /**
     * 单条输入或发布的原始字节上限，包含 key、value 与 header。
     */
    private int maxRecordBytes = 16777216;
    /**
     * 单次连续批调用的原文字节上限；首条合法记录可单独超过该批预算，但仍受单记录和进度上限约束。
     */
    private long maxBatchBytes = 16777216;
    /**
     * 每个 group 同时运行的真实业务调用上限；虚拟线程不绕过此额度。
     */
    private int maxHandlerInFlight = 64;
    /**
     * 同一消费组全部 owner 当前分配的不同分区数量上限，空闲槽不预留份额。
     */
    private int maxAssignmentPartitions = 10000;
    /**
     * 同一消费组保留普通失败或延期次数的输入记录上限；整批新增失败原子准入。
     */
    private int maxRetryEntries = 100000;
    /**
     * 死信发送使用的已声明通道；独立通道可隔离普通业务流量。
     */
    private String dltProducerLane = "default";
    /**
     * 同一源全部普通消费组的 DLT 信封数量上限，覆盖等待、交付、完成及退避重试。
     */
    private int dltQueueCapacity = 1024;
    /**
     * 同一源全部普通消费组的 DLT 信封字节上限；原输入另计入 progress，重试不提前归还信封额度。
     */
    private long dltQueueMaxBytes = 67108864;
    /**
     * 同一源全部普通消费组的 DLT 实际交付并发上限，与业务 handler 的额度独立。
     */
    private int dltMaxInFlight = 16;
    /**
     * 必需 DLT 交付首次失败后的退避基准毫秒，与普通业务重试独立。
     */
    private long dltRetryInitialMs = 1000;
    /**
     * DLT 指数退避基准的最大毫秒，实际等待另含约正负百分之十的稳定抖动。
     */
    private long dltRetryMaxMs = 60000;
    /**
     * 同一 DLT 责任连续失败达到此次数后显示 DEGRADED，仍保留责任并按预算重试。
     */
    private int dltDegradedAfterAttempts = 10;
    /**
     * 没有匹配 topic/event 路由的输入处置方式，默认显式跳过。
     */
    private RecordPolicy unmatchedPolicy = RecordPolicy.SKIP;
    /**
     * 确定无效的头或载荷的处置方式；系统性解码失败另受连续失败保护。
     */
    private RecordPolicy invalidRecordPolicy = RecordPolicy.DEAD_LETTER;
    /**
     * typed 路由对 Kafka null value 的处置；raw 路由始终保留原始可空字节。
     */
    private TombstonePolicy tombstonePolicy = TombstonePolicy.SKIP;
    /**
     * 显式共同广播的稳定实例身份；缺省使用进程身份，重启后不保证接续旧组。
     */
    private String broadcastInstanceId;
    /**
     * 是否按已知历史信封解码；关闭时直接按冻结的 DTO 类型读取原始 JSON。
     */
    private boolean legacyEnvelope = false;
    /**
     * 路由与 codec 的 singleton 头是否拒绝重复；关闭时使用最后一个值。
     */
    private boolean strictHeaders = true;
    /**
     * 该源是否参与 KafkaClients 的整体 Ready 等待；可选源仍独立提供健康快照。
     */
    private boolean required = true;
    /**
     * 显式物理集群身份验证的等待预算，单位毫秒。
     */
    private long startupTimeoutMs = 30000;
    /**
     * 允许连接的 broker 物理集群 ID；未设置时不执行身份约束。
     */
    private String expectedClusterId;

    /**
     * 集群地址, 多个用逗号分隔. 必填.
     */
    private String bootstrapServers;

    /**
     * listener 未声明组时使用的 group.id；显式名称须满足共同组身份边界，null 或空串表示未指定。
     */
    private String groupId;

    /**
     * client.id 前缀. producer 默认追加 "-producer", consumer 默认追加 "-consumer-${groupId}".
     */
    private String clientIdPrefix;

    /**
     * 同时透传给 producer / consumer / admin 的 Kafka 原生配置.
     */
    private Map<String, String> properties = new LinkedHashMap<>();

    // ==================== common client ====================

    /**
     * DNS 解析策略, 例如 {@code use_all_dns_ips}.
     */
    private String clientDnsLookup;

    /**
     * metadata 刷新间隔 ms.
     */
    private Long metadataMaxAgeMs;

    /**
     * 空闲连接关闭时间 ms.
     */
    private Long connectionsMaxIdleMs;

    /**
     * 重连基础退避 ms.
     */
    private Long reconnectBackoffMs;

    /**
     * 重连最大退避 ms.
     */
    private Long reconnectBackoffMaxMs;

    // ==================== security ====================

    /**
     * 安全协议, 例如 {@code PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL}.
     */
    private String securityProtocol;

    /**
     * SASL 机制, 例如 {@code PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512}.
     */
    private String saslMechanism;

    /**
     * SASL JAAS 配置.
     */
    private String saslJaasConfig;

    /**
     * SASL callback handler class.
     */
    private String saslClientCallbackHandlerClass;

    /**
     * SSL truststore 路径.
     */
    private String sslTruststoreLocation;

    /**
     * SSL truststore 密码.
     */
    private String sslTruststorePassword;

    /**
     * SSL truststore 类型, 例如 {@code JKS / PKCS12}.
     */
    private String sslTruststoreType;

    /**
     * SSL keystore 路径.
     */
    private String sslKeystoreLocation;

    /**
     * SSL keystore 密码.
     */
    private String sslKeystorePassword;

    /**
     * SSL keystore 类型, 例如 {@code JKS / PKCS12}.
     */
    private String sslKeystoreType;

    /**
     * SSL key 密码.
     */
    private String sslKeyPassword;

    /**
     * hostname 校验算法. 需要关闭时显式配置为空字符串。
     */
    private String sslEndpointIdentificationAlgorithm;

    // ==================== producer ====================

    /**
     * producer client.id. 为空时用 {@code clientIdPrefix + "-producer"}.
     */
    private String producerClientId;

    /**
     * producer acks 策略, 例如 {@code 0 / 1 / all}.
     */
    private String acks = "all";

    /**
     * 失败重试次数.
     */
    private int retries = 3;

    /**
     * producer 重试间隔 ms.
     */
    private long retryBackoffMs = 100;

    /**
     * 原生 producer 每分区批次的目标字节大小；不替代框架单条发布上限，超大首条记录可能单独成批。
     */
    private int batchSize = 16384;

    /**
     * batch 凑齐前等待 ms (大于 0 攒批提升吞吐, 增加 latency).
     */
    private long lingerMs = 5;

    /**
     * producer 总缓冲 byte. 写满阻塞或抛 BufferExhaustedException.
     */
    private long bufferMemory = 33554432L;

    /**
     * 压缩算法. {@code none / gzip / snappy / lz4 / zstd}.
     */
    private String compressionType = "none";

    /**
     * producer 单请求超时 ms.
     */
    private int requestTimeoutMs = 30000;

    /**
     * producer 投递最大耗时 ms (含重试).
     */
    private int deliveryTimeoutMs = 120000;

    /**
     * 发布从编码、本地准入到调用方等待交付的总预算，单位毫秒；等待超时不撤销实际发送。
     */
    private long publishTimeoutMs = 120000;

    /**
     * producer 获取 metadata / buffer 的最大阻塞时间 ms.
     */
    private Long maxBlockMs;

    /**
     * 单连接最大未 ack 请求数 (开启幂等需要 &lt;= 5).
     */
    private int maxInflightRequestsPerConnection = 1;

    /**
     * 是否启用原生幂等发送；启用时必须配置 acks=all 或 -1、retries 大于零且单连接在途请求为 1 至 5。
     */
    private boolean enableIdempotence = true;

    /**
     * 自定义 partitioner class.
     */
    private String partitionerClass;

    /**
     * producer socket send buffer byte.
     */
    private Integer producerSendBufferBytes;

    /**
     * producer socket receive buffer byte.
     */
    private Integer producerReceiveBufferBytes;

    /**
     * 只透传给 producer 的 Kafka 原生配置.
     */
    private Map<String, String> producerProperties = new LinkedHashMap<>();

    // ==================== consumer ====================

    /**
     * consumer client.id 前缀. 为空时用 {@code clientIdPrefix + "-consumer"}.
     */
    private String consumerClientIdPrefix;

    /**
     * 单次 poll 最大消息数.
     */
    private int maxPollRecords = 500;

    /**
     * poll 阻塞超时 ms (无消息时返回间隔).
     */
    private long pollTimeoutMs = 500;

    /**
     * consumer session 超时 ms (kicked out 阈值).
     */
    private int sessionTimeoutMs = 30000;

    /**
     * consumer 心跳间隔 ms (一般 sessionTimeout / 3).
     */
    private int heartbeatIntervalMs = 10000;

    /**
     * 两次 native poll 最大间隔 ms；业务调用隔离执行，不以此限制业务总时长。
     */
    private int maxPollIntervalMs = 300000;

    /**
     * auto.offset.reset 策略. {@code latest / earliest / none}.
     */
    private String autoOffsetReset = "latest";

    /**
     * 受管消费必须为 false；显式开启会在连接前拒绝，位点只由 owner 按安全前缀提交。
     */
    private boolean enableAutoCommit = false;

    /**
     * 兼容保留的原生自动提交间隔，单位毫秒；受管消费强制关闭自动提交，此值不控制框架提交节奏。
     */
    private int autoCommitIntervalMs = 5000;

    /**
     * consumer 单 fetch 最小 byte.
     */
    private int fetchMinBytes = 1;

    /**
     * consumer 单 fetch 最大等 ms (与 fetchMinBytes 配合).
     */
    private int fetchMaxWaitMs = 500;

    /**
     * consumer 单 partition 单次 fetch 最大 byte.
     */
    private int maxPartitionFetchBytes = 1048576;

    /**
     * consumer 单次 fetch 最大总 byte.
     */
    private Integer fetchMaxBytes;

    /**
     * consumer request timeout ms.
     */
    private Integer consumerRequestTimeoutMs;

    /**
     * consumer default api timeout ms, commitSync 等 API 会使用.
     */
    private Integer consumerDefaultApiTimeoutMs;

    /**
     * consumer 重试间隔 ms.
     */
    private Long consumerRetryBackoffMs;

    /**
     * 事务隔离级别: {@code read_uncommitted / read_committed}.
     */
    private String isolationLevel;

    /**
     * 分区分配策略 class, 例如 CooperativeStickyAssignor.
     */
    private String partitionAssignmentStrategy;

    /**
     * 是否允许消费端触发自动创建 topic.
     */
    private Boolean allowAutoCreateTopics;

    /**
     * 是否排除内部 topic.
     */
    private Boolean excludeInternalTopics;

    /**
     * 是否校验 record CRC.
     */
    private Boolean checkCrcs;

    /**
     * consumer socket send buffer byte.
     */
    private Integer consumerSendBufferBytes;

    /**
     * consumer socket receive buffer byte.
     */
    private Integer consumerReceiveBufferBytes;

    /**
     * 只透传给 consumer 的 Kafka 原生配置.
     */
    private Map<String, String> consumerProperties = new LinkedHashMap<>();

    // ==================== admin ====================

    /**
     * admin client.id. 为空时用 {@code clientIdPrefix + "-admin"}.
     */
    private String adminClientId;

    /**
     * Admin单请求毫秒；未配置时依次回退管理API预算、发布预算。
     */
    private Integer adminRequestTimeoutMs;

    /**
     * Admin总API毫秒；未配置时回退发布预算。
     */
    private Integer adminDefaultApiTimeoutMs;

    /**
     * 只透传给 admin 的 Kafka 原生配置.
     */
    private Map<String, String> adminProperties = new LinkedHashMap<>();

    // ==================== Kafka 客户端原生行为参数 ====================

    /**
     * 兼容配置字段；受管 codec 必须为 false，启用时在连接前拒绝。
     */
    private boolean activateDefaultTyping = false;

    /**
     * 框架启动后多久开始 consume (ms), 留出业务 bean 完全 init 的时间.
     */
    private long startConsumeDelayMs = 1000;

    /**
     * consume loop 异常退出后重启间隔 ms (防 busy loop).
     */
    private long consumeRestartDelayMs = 3000;
    /**
     * 自动重建采用有上限的退避与滑动窗口预算，耗尽后等待显式恢复。
     */
    private long consumeRestartMaxDelayMs = 60000;
    /**
     * 滑动窗口内允许的自动重建次数，耗尽后等待显式 restartGroup。
     */
    private int consumeRestartBudgetTimes = 10;
    /**
     * 自动重建预算的滑动窗口长度，单位毫秒，必须为正。
     */
    private long consumeRestartBudgetWindowMs = 300000;
    /**
     * 连续解码失败的分区保护阈值；零表示显式关闭该保护。
     */
    private long decodeFailureHaltStreak = 1000;

    /**
     * 每条输入的普通业务尝试次数上限，包含首次；达到上限后转入 DLT 处置，零表示无限重试，延期不消耗此预算。
     */
    private int maxConsumeAttempts = 3;

    /**
     * 死信 topic 后缀, 为空表示禁用死信转发。
     */
    private String deadLetterTopicSuffix = ".DLT";

    /**
     * 死信转发是否为强要求, true 时 DLT 不可达则不前移 offset。
     */
    private boolean deadLetterRequired = true;

    /**
     * 分区查询 API 的等待超时 ms, 为空时回退 publishTimeoutMs。
     */
    private Integer partitionQueryTimeoutMs;

    /**
     * 业务作用：把本类的配置项翻译成 KafkaProducer 认识的原生配置。
     * <p>
     * 写入顺序决定优先级：先铺通用项与 {@code producerProperties} 原样透传项，
     * 再写本类显式建模的项，因此<b>显式建模的配置项会覆盖同名的原样透传项</b>。
     * 需要覆盖某个已建模项时应改用对应的属性，而不是在透传映射里写同名键。
     * <p>
     * key 与 value 的序列化器被固定为字符串与字节数组，不接受外部覆盖——
     * payload 的编码由本组件自行完成并以 header 标注，换掉序列化器会让消费端按 header 解码时错乱。
     * <p>
     * 参数说明：无。
     *
     * @return 可直接交给 KafkaProducer 构造的原生配置。
     */
    public Properties buildProducerProperties() {
        validateProducerSettings();
        Properties cfg = new Properties();
        applyCommonProperties(cfg);
        putAll(cfg, producerProperties);
        applyIdentity(cfg);
        putIfText(cfg, CommonClientConfigs.CLIENT_ID_CONFIG, producerClientId());
        putIfText(cfg, ProducerConfig.ACKS_CONFIG, acks);
        cfg.put(ProducerConfig.RETRIES_CONFIG, retries);
        cfg.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        cfg.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        cfg.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        cfg.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        putIfText(cfg, ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        putIfNotNull(cfg, ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInflightRequestsPerConnection);
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        putIfText(cfg, ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass);
        putIfNotNull(cfg, CommonClientConfigs.SEND_BUFFER_CONFIG, producerSendBufferBytes);
        putIfNotNull(cfg, CommonClientConfigs.RECEIVE_BUFFER_CONFIG, producerReceiveBufferBytes);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return cfg;
    }

    /**
     * 业务作用：在建出 producer 之前拦截一组自相矛盾的超时配置。
     * 投递超时若小于请求超时加批次等待，消息还没来得及发出就已被判定超时，
     * 发送会稳定失败且原因不直观——放到启动期以配置错误的形式暴露，比运行期逐条失败容易定位得多。
     * <p>
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：预算覆盖请求超时与 linger 之和时通过；否则抛出 CONFIG，阻止原生 producer 创建。
     *
     * @throws KafkaFailure 投递超时小于请求超时与批次等待之和
     */
    private void validateProducerTimeouts() {
        long minDeliveryTimeoutMs = requestTimeoutMs + lingerMs;
        if (deliveryTimeoutMs < minDeliveryTimeoutMs) {
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "delivery-timeout-ms必须覆盖request-timeout-ms与linger-ms之和");
        }
    }

    /**
     * 业务作用：在默认或命名lane创建前统一验证可靠发送与有界等待合同。
     * <p>
     * 参数说明：无。
     * 返回：合法时无副作用；非法数值、确认策略或顺序组合返回CONFIG，不创建原生客户端。
     */
    private void validateProducerSettings() {
        if (acks == null || !Set.of("all", "-1", "0", "1").contains(acks)
                || retries < 0 || batchSize <= 0 || requestTimeoutMs <= 0 || deliveryTimeoutMs <= 0
                || maxInflightRequestsPerConnection <= 0 || bufferMemory <= 0
                || queueBufferingMaxMessages <= 0 || queueBufferingMaxKbytes <= 0 || queueBufferingMaxKbytes > Integer.MAX_VALUE
                || fireObserverCapacity <= 0 || stalledRebuildAfterMs <= 0 || stalledRebuildMinFailures <= 0 || stalledRebuildCooldownMs <= 0)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "producer确认策略、请求时限或队列容量非法");
        for (long millis : new long[]{retryBackoffMs, lingerMs, stalledRebuildAfterMs, stalledRebuildCooldownMs})
            validateMillis(millis);
        if (maxBlockMs != null) validateMillis(maxBlockMs);
        // 未确认发送不能混入依赖broker确认的幂等合同；非幂等重试只允许一条请求保持顺序。
        if (enableIdempotence && (!("all".equals(acks) || "-1".equals(acks)) || retries == 0 || maxInflightRequestsPerConnection > 5)
                || !enableIdempotence && retries > 0 && maxInflightRequestsPerConnection != 1)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "producer幂等、重试和请求并发不满足顺序合同");
        validateProducerTimeouts();
    }

    /**
     * 业务作用：统一配置毫秒的可表示范围，避免两端转换为原生整数或单调时钟时产生不同结果。
     * <p>
     * 返回：0到有符号32位最大值通过，其余返回CONFIG。
     *
     * @param millis 毫秒数，是否允许零由具体业务字段决定。
     */
    private static void validateMillis(long millis) {
        if (millis < 0 || millis > Integer.MAX_VALUE)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "毫秒配置超出共同有符号32位范围");
    }

    /**
     * 业务作用：把本类的配置项翻译成 KafkaConsumer 认识的原生配置。
     * 显式建模项同样覆盖 {@code consumerProperties} 中的同名原样透传项。
     * <p>
     * 反序列化器固定为字符串与字节数组：本组件按 header 标注的编码自行解码 payload，
     * 换掉反序列化器会与解码流程冲突。
     *
     * @param group 本 consumer 使用的 group.id，同时参与客户端标识的生成
     * @return 可直接交给 KafkaConsumer 构造的原生配置；组不满足非纯空白、无 NUL、无 ASCII 首尾控制字符或空格、严格 UTF-8 且最多1024字节的共同边界时以 CONFIG 拒绝。
     */
    public Properties buildConsumerProperties(String group) {
        // 原生配置也可被独立构造，必须在派生客户端标识前验证实际使用的组身份。
        KafkaWire.validateGroup(group);
        if (enableAutoCommit || "true".equalsIgnoreCase(properties.get("enable.auto.commit"))
                || "true".equalsIgnoreCase(consumerProperties.get("enable.auto.commit"))) {
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "受管消费禁止 enable.auto.commit=true");
        }
        Properties cfg = new Properties();
        applyCommonProperties(cfg);
        putAll(cfg, consumerProperties);
        applyIdentity(cfg);
        putIfText(cfg, CommonClientConfigs.CLIENT_ID_CONFIG, consumerClientId(group));
        putIfText(cfg, ConsumerConfig.GROUP_ID_CONFIG, group);
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        cfg.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        cfg.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        cfg.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        putIfText(cfg, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
        cfg.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        cfg.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        cfg.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        putIfNotNull(cfg, ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        putIfNotNull(cfg, CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, consumerRequestTimeoutMs);
        putIfNotNull(cfg, CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, consumerDefaultApiTimeoutMs);
        putIfNotNull(cfg, CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, consumerRetryBackoffMs);
        putIfText(cfg, ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        putIfText(cfg, ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy);
        putIfNotNull(cfg, ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, allowAutoCreateTopics);
        putIfNotNull(cfg, ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, excludeInternalTopics);
        putIfNotNull(cfg, ConsumerConfig.CHECK_CRCS_CONFIG, checkCrcs);
        putIfNotNull(cfg, CommonClientConfigs.SEND_BUFFER_CONFIG, consumerSendBufferBytes);
        putIfNotNull(cfg, CommonClientConfigs.RECEIVE_BUFFER_CONFIG, consumerReceiveBufferBytes);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return cfg;
    }

    /**
     * 业务作用：合并通用及 Admin 专属透传配置，再写入固定源身份、Admin 客户端标识与超时，供资源管理和位点查询使用。
     * <p>
     * 参数说明：无。
     *
     * @return 可直接交给 AdminClient 构造的原生配置。
     */
    public Properties buildAdminProperties() {
        Properties cfg = new Properties();
        applyCommonProperties(cfg);
        putAll(cfg, adminProperties);
        applyIdentity(cfg);
        putIfText(cfg, CommonClientConfigs.CLIENT_ID_CONFIG, adminClientId());
        long apiTimeout = adminDefaultApiTimeoutMs == null ? publishTimeoutMs : adminDefaultApiTimeoutMs;
        long requestTimeout = adminRequestTimeoutMs == null ? apiTimeout : adminRequestTimeoutMs;
        cfg.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(requestTimeout));
        cfg.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, Math.toIntExact(apiTimeout));
        return cfg;
    }

    /**
     * 业务作用：取得独立配置副本，防止外部修改运行中资源身份。
     * <p>
     * 参数说明：无。
     *
     * @return 深拷贝。
     */
    public NasaKafkaProperties snapshot() {
        return new ObjectMapper().convertValue(this, NasaKafkaProperties.class);
    }

    /**
     * 业务作用：冻结源的嵌套配置、身份与不变量。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param sourceId 逻辑源名。
     * @return 可供创建资源的独立有效配置。
     */
    public NasaKafkaProperties effective(String sourceId) {
        if (sourceId == null || !sourceId.matches("[a-z][a-z0-9_-]{0,63}"))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "非法 Kafka sourceId");
        NasaKafkaProperties copy = snapshot();
        KafkaSettings.apply(copy.producer, copy, "producer");
        KafkaSettings.apply(copy.consumer, copy, "consumer");
        KafkaSettings.apply(copy.behavior, copy, "behavior");
        KafkaSettings.apply(copy.security, copy, "security");
        KafkaSettings.apply(copy.admin, copy, "admin");
        copy.producer = null;
        copy.consumer = null;
        copy.behavior = null;
        copy.security = null;
        copy.admin = null;
        if (!copy.sources.isEmpty()) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "source 内不能再声明 sources");
        copy.clientIdPrefix = (hasText(copy.clientIdPrefix) ? copy.clientIdPrefix + "-" : "nasa-") + sourceId;
        copy.validate();
        return copy;
    }

    /**
     * 业务作用：继承源配置形成独立发布通道。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param name 声明的 lane。
     * @return 源身份不变的通道配置；未知 lane 拒绝。
     */
    public NasaKafkaProperties lane(String name) {
        NasaKafkaProperties copy = snapshot();
        if (!"default".equals(name)) {
            KafkaSettings.Producer override = producerLanes.get(name);
            if (override == null)
                throw new KafkaFailure(KafkaFailure.Code.NO_SUCH_PRODUCER_LANE, "未知 producer lane: " + name);
            KafkaSettings.apply(override, copy, "producer");
        }
        copy.producerClientId = clientIdPrefix + "-producer-" + name;
        // 其它lane已相对源默认值验证，不能再让它们继承当前lane的覆盖值。
        copy.validate(false);
        return copy;
    }

    /**
     * 业务作用：在连接前拒绝不安全或无界配置。
     * <p>
     * 参数说明：无。
     * 返回：合法时正常返回，非法时抛出配置错误。
     */
    public void validate() {
        validate(true);
    }

    /**
     * 业务作用：验证当前有效源或lane配置，按源默认值检查已声明的其它lane。
     * <p>
     * 返回：合法无副作用，非法返回CONFIG或缺少能力错误。
     *
     * @param validateNamedLanes true时当前实例是继承根；false时只验证已经合并的当前lane。
     */
    private void validate(boolean validateNamedLanes) {
        if (producerLanes == null || properties == null || producerProperties == null || consumerProperties == null || adminProperties == null)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "配置集合不能为null");
        if (broadcastInstanceId != null) BroadcastIdentity.component(broadcastInstanceId);
        // 纯发布源允许省略或使用空串表示未指定默认组；其它显式值在配置快照开放资源前验证。
        if (groupId != null && !groupId.isEmpty()) KafkaWire.validateGroup(groupId);
        if (unmatchedPolicy == null || invalidRecordPolicy == null || tombstonePolicy == null)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "记录处置策略不能为空");
        if (tombstonePolicy == TombstonePolicy.DEAD_LETTER && !hasText(deadLetterTopicSuffix))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "tombstone-policy=dead-letter 要求非空 DLT 后缀");
        if ((invalidRecordPolicy == RecordPolicy.DEAD_LETTER || unmatchedPolicy == RecordPolicy.DEAD_LETTER) && !hasText(deadLetterTopicSuffix))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "dead-letter处置策略要求非空DLT后缀");
        if (consumerConcurrency < 1 || consumerConcurrency > 64 || groupConcurrency == null
                || groupConcurrency.entrySet().stream().anyMatch(entry -> entry.getKey() == null || entry.getKey().isBlank()
                || entry.getValue() == null || entry.getValue() < 1 || entry.getValue() > 64))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "consumer concurrency 必须为 1..64 且组名不能为空");
        groupConcurrency.keySet().forEach(KafkaWire::validateGroup);
        if (!hasText(bootstrapServers)) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "bootstrap-servers 不能为空");
        if (activateDefaultTyping)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "受管 codec 不允许任意类 default typing");
        if (enableAutoCommit) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "受管消费不允许自动提交");
        if (maxConsumeAttempts < 0 || handlerTimeoutMs < 0 || retries < 0 || lingerMs < 0)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "重试和时限不得为负数");
        if (consumeRestartDelayMs < 0 || consumeRestartMaxDelayMs < consumeRestartDelayMs
                || consumeRestartBudgetTimes <= 0 || consumeRestartBudgetWindowMs <= 0 || decodeFailureHaltStreak < 0)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "重建退避、预算与解码保护阈值非法");
        for (long millis : new long[]{consumeRestartMaxDelayMs, consumeRestartBudgetWindowMs, controlTimeoutMs,
                shutdownTimeoutMs, handlerTimeoutMs, consumerRetryMs, commitIntervalMs, startupTimeoutMs, startConsumeDelayMs,
                publishTimeoutMs, pollTimeoutMs, dltRetryInitialMs, dltRetryMaxMs})
            validateMillis(millis);
        if (dltRetryInitialMs <= 0 || dltRetryMaxMs < dltRetryInitialMs || dltDegradedAfterAttempts <= 0)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "DLT退避基准、上限与降级阈值非法");
        if (sessionTimeoutMs <= 0 || heartbeatIntervalMs <= 0 || heartbeatIntervalMs >= sessionTimeoutMs
                || maxPollIntervalMs <= 0 || commitIntervalMs >= maxPollIntervalMs || fetchMinBytes < 0
                || fetchMaxWaitMs < 0 || maxPartitionFetchBytes <= 0 || fetchMaxBytes != null && fetchMaxBytes <= 0
                || autoOffsetReset == null || !Set.of("earliest", "latest", "none").contains(autoOffsetReset))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "consumer会话、心跳、fetch或起点策略非法");
        long adminApi = adminDefaultApiTimeoutMs == null ? publishTimeoutMs : adminDefaultApiTimeoutMs;
        long adminRequest = adminRequestTimeoutMs == null ? adminApi : adminRequestTimeoutMs;
        if (adminRequest <= 0 || adminApi <= 0 || adminRequest > adminApi)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "Admin请求时限必须为正且不超过API总预算");
        if (queueBufferingMaxMessages <= 0 || queueBufferingMaxKbytes <= 0 || queueBufferingMaxKbytes > Long.MAX_VALUE / 1024
                || fireObserverCapacity <= 0 || controlQueueCapacity <= 0 || controlTimeoutMs <= 0 || shutdownTimeoutMs <= 0
                || maxPollRecords <= 0 || pollTimeoutMs <= 0 || consumerRetryMs <= 0 || commitIntervalMs <= 0 || commitBatchRecords <= 0
                || maxProgressRecordsPerPartition <= 0 || maxProgressBytesPerPartition <= 0 || maxProgressRecordsPerKey <= 0 || maxProgressBytesPerKey <= 0
                || maxProgressRecordsPerGroup < maxPollRecords || maxProgressBytesPerGroup <= 0
                || maxProgressRecordsPerProcess <= 0 || maxProgressBytesPerProcess <= 0 || maxProgressRecordsPerSource <= 0 || maxProgressBytesPerSource <= 0
                || maxRecordBytes <= 0 || maxBatchBytes <= 0 || maxHandlerInFlight <= 0 || maxAssignmentPartitions <= 0 || maxRetryEntries <= 0 || publishTimeoutMs <= 0 || startupTimeoutMs <= 0
                || dltQueueCapacity <= 0 || dltQueueMaxBytes <= 0 || dltMaxInFlight <= 0)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "容量和有效时限必须为正且覆盖 poll 批次");
        if (enableIdempotence && (!("all".equals(acks) || "-1".equals(acks)) || retries == 0
                || maxInflightRequestsPerConnection < 1 || maxInflightRequestsPerConnection > 5))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "幂等要求 acks=all、retries>0、in-flight 在 1..5");
        if (!enableIdempotence && retries > 0 && maxInflightRequestsPerConnection != 1)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "非幂等重试要求 in-flight=1 以保持顺序");
        if (producerLanes.containsKey("default"))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "default lane 不允许重复声明");
        if (producerLanes.values().stream().anyMatch(Objects::isNull))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "lane 配置不能为空");
        if (!"default".equals(dltProducerLane) && !producerLanes.containsKey(dltProducerLane))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "DLT lane 未声明");
        for (String name : producerLanes.keySet())
            if (!name.matches("[a-z][a-z0-9_-]{0,63}"))
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "非法 lane 名");
        var rawLayers = new ArrayList<>(List.of(properties, producerProperties, consumerProperties, adminProperties));
        producerLanes.values().forEach(lane -> {
            if (lane.getProperties() != null) rawLayers.add(lane.getProperties());
        });
        for (Map<String, String> raw : rawLayers) {
            if (raw.containsKey("bootstrap.servers") && !bootstrapServers.equals(raw.get("bootstrap.servers")))
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "raw 配置不能改写源地址");
            if ("true".equalsIgnoreCase(raw.get("enable.auto.commit")))
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "raw 配置不能开启自动提交");
            for (String key : raw.keySet()) {
                if (key == null || key.isBlank() || raw.get(key) == null)
                    throw new KafkaFailure(KafkaFailure.Code.CONFIG, "raw 键值不能为空");
                if (key.endsWith(".serializer") || key.endsWith(".deserializer") || key.equals("transactional.id") || key.equals("group.id"))
                    throw new KafkaFailure(KafkaFailure.Code.CONFIG, "raw 配置不能覆盖框架身份或 codec: " + key);
                if (raw != properties && (key.startsWith("sasl.") || key.startsWith("ssl.") || key.equals("security.protocol")))
                    throw new KafkaFailure(KafkaFailure.Code.CONFIG, "客户端专属配置不能改写源认证");
                if (raw != consumerProperties && raw != adminProperties && (key.equals("compression.type") || key.equals("compression.codec")))
                    throw new KafkaFailure(KafkaFailure.Code.CONFIG, "压缩算法必须通过 producer 或 lane 强类型配置声明");
            }
        }
        validateProducerSettings();
        // 预先声明的lane同样属于启动合同；未被首次使用不能成为延迟暴露非法配置的理由。
        if (validateNamedLanes) for (KafkaSettings.Producer override : producerLanes.values()) {
            if (override.getLanes() != null) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "lane内部不能嵌套lanes");
            NasaKafkaProperties lane = snapshot();
            KafkaSettings.apply(override, lane, "producer");
            lane.validateProducerSettings();
        }
        KafkaCapabilities.validate(this);
    }

    /**
     * 业务作用：在角色透传之后复验并写回连接身份。
     * <p>
     * 返回：校验已有地址与当前源一致后写回 bootstrap.servers；地址冲突以 CONFIG 拒绝。
     *
     * @param cfg 原生配置。
     */
    private void applyIdentity(Properties cfg) {
        Object address = cfg.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        if (address != null && !bootstrapServers.equals(address.toString()))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "角色配置不能切换 Kafka 源");
        cfg.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    /**
     * 业务作用：铺设三类客户端共用的连接、认证与传输层安全配置。
     * 源认证由共同配置与强类型声明固定，启动校验拒绝客户端专属属性改写认证域。
     * <p>
     * 主机名校验算法用显式判空而非「有文本才写」：该项的空字符串是有意义的取值，
     * 表示<b>关闭</b>服务端主机名校验，按空白跳过会让这个关闭意图失效而校验仍然生效。
     * <p>
     * 返回：先合并共同透传项，再覆盖已声明的连接、认证和 TLS 配置；直接更新 cfg。
     *
     * @param cfg 待填充的原生配置
     */
    private void applyCommonProperties(Properties cfg) {
        putAll(cfg, properties);
        putIfText(cfg, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        putIfText(cfg, CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG, clientDnsLookup);
        putIfNotNull(cfg, CommonClientConfigs.METADATA_MAX_AGE_CONFIG, metadataMaxAgeMs);
        putIfNotNull(cfg, CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs);
        putIfNotNull(cfg, CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMs);
        putIfNotNull(cfg, CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, reconnectBackoffMaxMs);
        putIfText(cfg, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        putIfText(cfg, SaslConfigs.SASL_MECHANISM, saslMechanism);
        putIfText(cfg, SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        putIfText(cfg, SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, saslClientCallbackHandlerClass);
        putIfText(cfg, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
        putIfText(cfg, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
        putIfText(cfg, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslTruststoreType);
        putIfText(cfg, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
        putIfText(cfg, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
        putIfText(cfg, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslKeystoreType);
        putIfText(cfg, SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        if (sslEndpointIdentificationAlgorithm != null) {
            cfg.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm);
        }
    }

    /**
     * 业务作用：解析 producer 的客户端标识，供 broker 侧按客户端归类监控与限流。
     * 优先用显式配置，其次由通用前缀派生，都没有则不设置——不设置时由 Kafka 客户端自行生成，
     * 监控上表现为无法按服务区分来源。
     * <p>
     * <p>
     * 参数说明：无。
     *
     * @return 客户端标识；未配置任何来源时为 null，表示不设置该项。
     */
    private String producerClientId() {
        if (hasText(producerClientId)) return producerClientId;
        if (hasText(clientIdPrefix)) return clientIdPrefix + "-producer";
        return null;
    }

    /**
     * 业务作用：解析 consumer 的客户端标识，并把 group 拼进去，使同一服务的不同 group 在 broker 侧可区分。
     *
     * @param group 本 consumer 的 group.id，为空白时标识中不含 group 段
     * @return 客户端标识；未配置任何前缀来源时为 null，表示不设置该项。
     */
    private String consumerClientId(String group) {
        String prefix = null;
        if (hasText(consumerClientIdPrefix)) prefix = consumerClientIdPrefix;
        else if (hasText(clientIdPrefix)) prefix = clientIdPrefix + "-consumer";
        if (!hasText(prefix)) return null;
        return hasText(group) ? prefix + "-" + group : prefix;
    }

    /**
     * 业务作用：解析 AdminClient 的客户端标识，使管理操作在 broker 审计日志中可归因到具体服务。
     * <p>
     * 参数说明：无。
     *
     * @return 客户端标识；未配置任何来源时为 null，表示不设置该项。
     */
    private String adminClientId() {
        if (hasText(adminClientId)) return adminClientId;
        if (hasText(clientIdPrefix)) return clientIdPrefix + "-admin";
        return null;
    }

    /**
     * 业务作用：把非空白键且值非 null 的透传配置合并到原生配置中。
     * 此辅助方法会跳过无效条目；受管源的 validate 先行拒绝这些声明，不能依赖此处过滤让非法源配置通过。
     * <p>
     * 返回：有效条目写入 cfg；null 或空映射不改变 cfg。
     *
     * @param cfg    待填充的原生配置
     * @param values 原样透传的键值对，可为 null 或空
     */
    private static void putAll(Properties cfg, Map<String, String> values) {
        if (values == null || values.isEmpty()) return;
        for (Map.Entry<String, String> e : values.entrySet()) {
            if (hasText(e.getKey()) && e.getValue() != null) {
                cfg.put(e.getKey(), e.getValue());
            }
        }
    }

    /**
     * 业务作用：仅对非 null 且 trim 后非空的可选文本配置执行覆盖；未提供文本时保留 cfg 原有值。
     * 有意义的空字符串配置须由对应调用点单独处理。
     * <p>
     * 返回：有文本时按原文写入，不裁剪配置值；否则不改变 cfg。
     *
     * @param cfg   待填充的原生配置
     * @param key   Kafka 配置键
     * @param value 配置值，为 null 或纯空白时跳过
     */
    private static void putIfText(Properties cfg, String key, String value) {
        if (hasText(value)) cfg.put(key, value);
    }

    /**
     * 业务作用：仅在可选配置非 null 时覆盖原生配置；未提供值时保留已有透传值或让原生客户端采用默认值。
     * <p>
     * 返回：非 null 值写入 cfg；null 不改变 cfg，避免向 Properties 写入不支持的 null 值。
     *
     * @param cfg   待填充的原生配置
     * @param key   Kafka 配置键
     * @param value 配置值，为 null 时跳过
     */
    private static void putIfNotNull(Properties cfg, String key, Object value) {
        if (value != null) cfg.put(key, value);
    }

    /**
     * 业务作用：判定配置值是否携带有效内容，作为「跳过还是写入」的统一判据。
     *
     * @param value 待判定的配置值
     * @return 非 null 且去除首尾空白后非空返回 true。
     */
    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}

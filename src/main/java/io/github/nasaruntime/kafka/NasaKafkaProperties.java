package io.github.nasaruntime.kafka;

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

/**
 * 类作用 - Kafka 组件配置项, 负责承载 producer、consumer、admin 和框架行为配置。
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "nasa.kafka")
public class NasaKafkaProperties {

    /** 集群地址, 多个用逗号分隔. 必填. */
    private String bootstrapServers;

    /** 默认 consumer group_id. listener 未声明 group() 时回退此值. */
    private String groupId;

    /** client.id 前缀. producer 默认追加 "-producer", consumer 默认追加 "-consumer-${groupId}". */
    private String clientIdPrefix;

    /** 同时透传给 producer / consumer / admin 的 Kafka 原生配置. */
    private Map<String, String> properties = new LinkedHashMap<>();

    // ==================== common client ====================

    /** DNS 解析策略, 例如 {@code use_all_dns_ips}. */
    private String clientDnsLookup;

    /** metadata 刷新间隔 ms. */
    private Long metadataMaxAgeMs;

    /** 空闲连接关闭时间 ms. */
    private Long connectionsMaxIdleMs;

    /** 重连基础退避 ms. */
    private Long reconnectBackoffMs;

    /** 重连最大退避 ms. */
    private Long reconnectBackoffMaxMs;

    // ==================== security ====================

    /** 安全协议, 例如 {@code PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL}. */
    private String securityProtocol;

    /** SASL 机制, 例如 {@code PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512}. */
    private String saslMechanism;

    /** SASL JAAS 配置. */
    private String saslJaasConfig;

    /** SASL callback handler class. */
    private String saslClientCallbackHandlerClass;

    /** SSL truststore 路径. */
    private String sslTruststoreLocation;

    /** SSL truststore 密码. */
    private String sslTruststorePassword;

    /** SSL truststore 类型, 例如 {@code JKS / PKCS12}. */
    private String sslTruststoreType;

    /** SSL keystore 路径. */
    private String sslKeystoreLocation;

    /** SSL keystore 密码. */
    private String sslKeystorePassword;

    /** SSL keystore 类型, 例如 {@code JKS / PKCS12}. */
    private String sslKeystoreType;

    /** SSL key 密码. */
    private String sslKeyPassword;

    /** hostname 校验算法. 需要关闭时显式配置为空字符串。 */
    private String sslEndpointIdentificationAlgorithm;

    // ==================== producer ====================

    /** producer client.id. 为空时用 {@code clientIdPrefix + "-producer"}. */
    private String producerClientId;

    /** producer acks 策略, 例如 {@code 0 / 1 / all}. */
    private String acks = "1";

    /** 失败重试次数. */
    private int retries = 3;

    /** producer 重试间隔 ms. */
    private long retryBackoffMs = 100;

    /** 单次 batch 上限 byte (达到即发, 不等 lingerMs). */
    private int batchSize = 16384;

    /** batch 凑齐前等待 ms (大于 0 攒批提升吞吐, 增加 latency). */
    private long lingerMs = 5;

    /** producer 总缓冲 byte. 写满阻塞或抛 BufferExhaustedException. */
    private long bufferMemory = 33554432L;

    /** 压缩算法. {@code none / gzip / snappy / lz4 / zstd}. */
    private String compressionType = "none";

    /** producer 单请求超时 ms. */
    private int requestTimeoutMs = 30000;

    /** producer 投递最大耗时 ms (含重试). */
    private int deliveryTimeoutMs = 120000;

    /** 同步 publish 等待 send future 的最大时间 ms. */
    private long publishTimeoutMs = 120000;

    /** producer 获取 metadata / buffer 的最大阻塞时间 ms. */
    private Long maxBlockMs;

    /** 单连接最大未 ack 请求数 (开启幂等需要 &lt;= 5). */
    private int maxInflightRequestsPerConnection = 5;

    /** 是否开启 producer 幂等 (Kafka 0.11+). 开启后建议 acks=all. */
    private boolean enableIdempotence = false;

    /** 自定义 partitioner class. */
    private String partitionerClass;

    /** producer socket send buffer byte. */
    private Integer producerSendBufferBytes;

    /** producer socket receive buffer byte. */
    private Integer producerReceiveBufferBytes;

    /** 只透传给 producer 的 Kafka 原生配置. */
    private Map<String, String> producerProperties = new LinkedHashMap<>();

    // ==================== consumer ====================

    /** consumer client.id 前缀. 为空时用 {@code clientIdPrefix + "-consumer"}. */
    private String consumerClientIdPrefix;

    /** 单次 poll 最大消息数. */
    private int maxPollRecords = 500;

    /** poll 阻塞超时 ms (无消息时返回间隔). */
    private long pollTimeoutMs = 500;

    /** consumer session 超时 ms (kicked out 阈值). */
    private int sessionTimeoutMs = 30000;

    /** consumer 心跳间隔 ms (一般 sessionTimeout / 3). */
    private int heartbeatIntervalMs = 10000;

    /** 两次 poll 最大间隔 ms (业务处理慢时调大, 否则被 group rebalance kicked out). */
    private int maxPollIntervalMs = 300000;

    /** auto.offset.reset 策略. {@code latest / earliest / none}. */
    private String autoOffsetReset = "latest";

    /** 是否自动 commit offset. 框架默认手动 commit (false), 业务成功才 commit. */
    private boolean enableAutoCommit = false;

    /** 自动 commit 间隔 ms (enableAutoCommit=true 时). */
    private int autoCommitIntervalMs = 5000;

    /** consumer 单 fetch 最小 byte. */
    private int fetchMinBytes = 1;

    /** consumer 单 fetch 最大等 ms (与 fetchMinBytes 配合). */
    private int fetchMaxWaitMs = 500;

    /** consumer 单 partition 单次 fetch 最大 byte. */
    private int maxPartitionFetchBytes = 1048576;

    /** consumer 单次 fetch 最大总 byte. */
    private Integer fetchMaxBytes;

    /** consumer request timeout ms. */
    private Integer consumerRequestTimeoutMs;

    /** consumer default api timeout ms, commitSync 等 API 会使用. */
    private Integer consumerDefaultApiTimeoutMs;

    /** consumer 重试间隔 ms. */
    private Long consumerRetryBackoffMs;

    /** 事务隔离级别: {@code read_uncommitted / read_committed}. */
    private String isolationLevel;

    /** 分区分配策略 class, 例如 CooperativeStickyAssignor. */
    private String partitionAssignmentStrategy;

    /** 是否允许消费端触发自动创建 topic. */
    private Boolean allowAutoCreateTopics;

    /** 是否排除内部 topic. */
    private Boolean excludeInternalTopics;

    /** 是否校验 record CRC. */
    private Boolean checkCrcs;

    /** consumer socket send buffer byte. */
    private Integer consumerSendBufferBytes;

    /** consumer socket receive buffer byte. */
    private Integer consumerReceiveBufferBytes;

    /** 只透传给 consumer 的 Kafka 原生配置. */
    private Map<String, String> consumerProperties = new LinkedHashMap<>();

    // ==================== admin ====================

    /** admin client.id. 为空时用 {@code clientIdPrefix + "-admin"}. */
    private String adminClientId;

    /** admin request timeout ms. */
    private Integer adminRequestTimeoutMs;

    /** admin default api timeout ms. */
    private Integer adminDefaultApiTimeoutMs;

    /** 只透传给 admin 的 Kafka 原生配置. */
    private Map<String, String> adminProperties = new LinkedHashMap<>();

    // ==================== Kafka 客户端原生行为参数 ====================

    /** 是否启用 Jackson default typing, 用于兼容旧的类型包装消息。 */
    private boolean activateDefaultTyping = false;

    /** 框架启动后多久开始 consume (ms), 留出业务 bean 完全 init 的时间. */
    private long startConsumeDelayMs = 1000;

    /** consume loop 异常退出后重启间隔 ms (防 busy loop). */
    private long consumeRestartDelayMs = 3000;

    /** 单批消息消费失败后的最大重试次数, 超过后按死信策略处理。 */
    private int maxConsumeAttempts = 3;

    /** 死信 topic 后缀, 为空表示禁用死信转发。 */
    private String deadLetterTopicSuffix = ".DLT";

    /** 死信转发是否为强要求, true 时 DLT 不可达则不前移 offset。 */
    private boolean deadLetterRequired = true;

    /** 分区查询 API 的等待超时 ms, 为空时回退 publishTimeoutMs。 */
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
     *
     * <p>参数说明: 无。
     *
     * @return 可直接交给 KafkaProducer 构造的原生配置。
     * @throws IllegalArgumentException 投递超时小于请求超时与批次等待之和
     */
    public Properties buildProducerProperties() {
        validateProducerTimeouts();
        Properties cfg = new Properties();
        applyCommonProperties(cfg);
        putAll(cfg, producerProperties);
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
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值；配置不合法时抛出异常阻断 producer 创建。
     * @throws IllegalArgumentException 投递超时小于请求超时与批次等待之和
     */
    private void validateProducerTimeouts() {
        long minDeliveryTimeoutMs = requestTimeoutMs + lingerMs;
        if (deliveryTimeoutMs < minDeliveryTimeoutMs) {
            throw new IllegalArgumentException("nasa.kafka.delivery-timeout-ms must be >= "
                    + "nasa.kafka.request-timeout-ms + nasa.kafka.linger-ms, actual deliveryTimeoutMs="
                    + deliveryTimeoutMs + ", requestTimeoutMs=" + requestTimeoutMs + ", lingerMs=" + lingerMs);
        }
    }

    /**
     * 业务作用：把本类的配置项翻译成 KafkaConsumer 认识的原生配置。
     * 显式建模项同样覆盖 {@code consumerProperties} 中的同名原样透传项。
     * <p>
     * 反序列化器固定为字符串与字节数组：本组件按 header 标注的编码自行解码 payload，
     * 换掉反序列化器会与解码流程冲突。
     *
     * @param group 本 consumer 使用的 group.id，同时参与客户端标识的生成
     * @return 可直接交给 KafkaConsumer 构造的原生配置。
     */
    public Properties buildConsumerProperties(String group) {
        Properties cfg = new Properties();
        applyCommonProperties(cfg);
        putAll(cfg, consumerProperties);
        putIfText(cfg, CommonClientConfigs.CLIENT_ID_CONFIG, consumerClientId(group));
        putIfText(cfg, ConsumerConfig.GROUP_ID_CONFIG, group);
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        cfg.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        cfg.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        cfg.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        putIfText(cfg, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
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
     * 业务作用：把本类的配置项翻译成 AdminClient 认识的原生配置，供 topic 管理与位点查询使用。
     * 只铺通用项与 admin 专有超时，不涉及生产消费参数。
     *
     * <p>参数说明: 无。
     *
     * @return 可直接交给 AdminClient 构造的原生配置。
     */
    public Properties buildAdminProperties() {
        Properties cfg = new Properties();
        applyCommonProperties(cfg);
        putAll(cfg, adminProperties);
        putIfText(cfg, CommonClientConfigs.CLIENT_ID_CONFIG, adminClientId());
        putIfNotNull(cfg, CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, adminRequestTimeoutMs);
        putIfNotNull(cfg, CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, adminDefaultApiTimeoutMs);
        return cfg;
    }

    /**
     * 业务作用：铺设三类客户端共用的连接、认证与传输层安全配置。
     * 最先执行且最先写入，因此其中任何一项都可被后续的客户端专有配置覆盖。
     * <p>
     * 主机名校验算法用显式判空而非「有文本才写」：该项的空字符串是有意义的取值，
     * 表示<b>关闭</b>服务端主机名校验，按空白跳过会让这个关闭意图失效而校验仍然生效。
     *
     * @param cfg 待填充的原生配置
     * 返回: 无返回值，直接写入入参。
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
     *
     * <p>参数说明: 无。
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
     *
     * <p>参数说明: 无。
     *
     * @return 客户端标识；未配置任何来源时为 null，表示不设置该项。
     */
    private String adminClientId() {
        if (hasText(adminClientId)) return adminClientId;
        if (hasText(clientIdPrefix)) return clientIdPrefix + "-admin";
        return null;
    }

    /**
     * 业务作用：把用户自定义的原样透传配置铺进目标配置，用于承载本类未显式建模的 Kafka 参数。
     * 键空白或值为 null 的条目跳过：Kafka 客户端遇到空键或空值会直接抛配置异常，
     * 在此过滤掉可避免一条无效配置阻断整个客户端的创建。
     *
     * @param cfg    待填充的原生配置
     * @param values 原样透传的键值对，可为 null 或空
     * 返回: 无返回值，直接写入入参。
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
     * 业务作用：仅在值有实际文本时写入，避免把空串当作有效配置传给 Kafka。
     * 空串对多数 Kafka 字符串配置是非法值，写进去会在客户端创建时报错。
     *
     * @param cfg   待填充的原生配置
     * @param key   Kafka 配置键
     * @param value 配置值，为 null 或纯空白时跳过
     * 返回: 无返回值。
     */
    private static void putIfText(Properties cfg, String key, String value) {
        if (hasText(value)) cfg.put(key, value);
    }

    /**
     * 业务作用：仅在值非空时写入，用于「不配置就沿用 Kafka 客户端默认值」的可选项。
     * 写入 null 会让 Kafka 认为该项被显式设为空而报错，因此不能无条件写。
     *
     * @param cfg   待填充的原生配置
     * @param key   Kafka 配置键
     * @param value 配置值，为 null 时跳过
     * 返回: 无返回值。
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

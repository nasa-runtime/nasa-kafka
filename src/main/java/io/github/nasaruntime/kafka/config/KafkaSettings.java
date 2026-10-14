package io.github.nasaruntime.kafka.config;

import io.github.nasaruntime.kafka.consumer.RecordPolicy;
import io.github.nasaruntime.kafka.consumer.TombstonePolicy;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import java.beans.PropertyDescriptor;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 嵌套配置只记录显式覆盖；缺省项继承源的有效值。
 */
public final class KafkaSettings {

    /**
     * 发布通道的可选覆盖项；未设置的值继承源的有效配置。
     */
    @Getter
    @Setter
    public static class Producer {
        /**
         * broker 确认范围，all 表示等待全部 ISR；0 不可作为 DLT 完成证据。
         */
        private String acks;
        /**
         * 原生 producer 发送重试次数；启用幂等时必须大于零。
         */
        private Integer retries;
        /**
         * 原生发送失败的重试退避毫秒；零表示不主动延迟重试。
         */
        private Long retryBackoffMs;
        /**
         * 是否启用原生幂等发送；不能替代业务事件幂等。
         */
        private Boolean enableIdempotence;
        /**
         * 单连接请求并发上限，默认一以维持重试顺序。
         */
        private Integer maxInFlightRequestsPerConnection;
        /**
         * 发送聚批等待毫秒，与请求超时共同受交付总期限约束。
         */
        private Long lingerMs;
        /**
         * 原生 producer 每分区批次大小，单位字节。
         */
        private Integer batchSize;
        /**
         * 原生压缩算法，不改变框架 payload codec。
         */
        private String compression;
        /**
         * 一次 broker 请求等待毫秒，不等于整体业务完成时限。
         */
        private Integer requestTimeoutMs;
        /**
         * 原生发送重试与排队的总交付时限，单位毫秒。
         */
        private Integer deliveryTimeoutMs;
        /**
         * 原生 send 等待元数据或缓冲区的上限，单位毫秒。
         */
        private Long maxBlockMs;
        /**
         * 整个发送责任周期允许保留的消息数，覆盖准入、原生发送与回执处理。
         */
        private Integer queueBufferingMaxMessages;
        /**
         * 发送责任的字节预算，单位 KiB，计入原始 key、value 与 header。
         */
        private Long queueBufferingMaxKbytes;
        /**
         * 发送回执的预留数量上限；用户回调阻塞期间仍占用该额度。
         */
        private Integer fireObserverCapacity;
        /**
         * 当前代次连续无成功交付达到该毫秒数后允许按失败阈值切换。
         */
        private Long stalledRebuildAfterMs;
        /**
         * 无进展切换需要的连续原生终态失败数。
         */
        private Integer stalledRebuildMinFailures;
        /**
         * 两次无进展切换之间的最短毫秒间隔。
         */
        private Long stalledRebuildCooldownMs;
        /**
         * 源内命名发布通道的配置；每个通道拥有独立原生 producer。
         */
        private Map<String, Producer> lanes;
        /**
         * 该角色的原生参数补充；不得覆盖源地址、认证域或框架固定的提交与 codec 合同。
         */
        private Map<String, String> properties;
    }

    /**
     * 原生消费组会话参数；不授予独立提交权限。
     */
    @Getter
    @Setter
    public static class Consumer {
        /**
         * 同组全部 owner 当前分配的不同分区数量上限。
         */
        private Integer maxAssignmentPartitions;
        /**
         * 每个订阅组的 owner 数量；未指定时为一。
         */
        private Integer concurrency;
        /**
         * 按最终消费组名覆盖槽数，固定 assign 不使用该配置。
         */
        private Map<String, Integer> groupConcurrency;
        /**
         * 一次 poll 最多交付的记录数，还受分区责任额度限制。
         */
        private Integer maxPollRecords;
        /**
         * broker 判定消费成员失联的毫秒时限。
         */
        private Integer sessionTimeoutMs;
        /**
         * 原生消费组心跳间隔，单位毫秒。
         */
        private Integer heartbeatIntervalMs;
        /**
         * 两次 poll 的最大允许间隔，单位毫秒，超限会失去组成员权威。
         */
        private Integer maxPollIntervalMs;
        /**
         * 首次无历史提交时的起点策略；会话恢复不能用 latest 跳过已接纳责任。
         */
        private String autoOffsetReset;
        /**
         * 读取事务数据的可见性级别，read_committed 只返回已提交事务。
         */
        private String isolationLevel;
        /**
         * 受管消费必须为 false，提交只由 owner 的安全前缀驱动。
         */
        private Boolean enableAutoCommit;
        /**
         * 是否允许原生元数据请求触发 broker 自动建 topic。
         */
        private Boolean allowAutoCreateTopics;
        /**
         * 一次 fetch 响应的原生字节预算，不替代应用保留责任预算。
         */
        private Integer fetchMaxBytes;
        /**
         * 原生单分区 fetch 字节预算，仍受 broker 对首条超大记录的处理语义影响。
         */
        private Integer maxPartitionFetchBytes;
        /**
         * 单次fetch希望取得的最小字节数；零允许broker立即返回当前可用数据。
         */
        private Integer fetchMinBytes;
        /**
         * broker聚合fetch响应的最大等待毫秒；零表示不等待聚合。
         */
        private Integer fetchMaxWaitMs;
        /**
         * 该角色的原生参数补充；不得覆盖源地址、认证域或框架固定的提交与 codec 合同。
         */
        private Map<String, String> properties;
    }

    /**
     * owner 对可靠性、顺序、容量及生命周期的控制合同。
     */
    @Getter
    @Setter
    public static class Behavior {
        /**
         * 同组全部 owner 持有失败次数的输入记录上限，批失败原子接纳。
         */
        private Integer maxRetryEntries;
        /**
         * 原生 poll 等待上限，单位毫秒，owner 可缩短等待以响应控制。
         */
        private Long pollTimeoutMs;
        /**
         * 一次发布从编码到等待回执的总预算，单位毫秒。
         */
        private Long publishTimeoutMs;
        /**
         * 首次消费前预留给业务初始化的毫秒延迟；不检查依赖是否就绪，可由停止请求打断。
         */
        private Long startConsumeDelayMs;
        /**
         * 自动重建的初始退避毫秒，可为零；最终受退避上限约束。
         */
        private Long consumeRestartDelayMs;
        /**
         * 自动重建退避的毫秒上限，必须不小于初始退避。
         */
        private Long consumeRestartMaxDelayMs;
        /**
         * 滑动窗口内允许的自动重建次数，耗尽后等待显式 restartGroup。
         */
        private Integer consumeRestartBudgetTimes;
        /**
         * 自动重建预算的滑动窗口长度，单位毫秒，必须为正。
         */
        private Long consumeRestartBudgetWindowMs;
        /**
         * 连续解码失败触发分区保护的次数；零表示显式关闭。
         */
        private Long decodeFailureHaltStreak;
        /**
         * 普通业务失败的最大尝试次数，零表示无限重试；延期不计入。
         */
        private Integer maxConsumeAttempts;
        /**
         * 失败责任再次派发前的退避毫秒；不以 sleep 阻塞其他分区处理。
         */
        private Long consumerRetryMs;
        /**
         * 业务调用有效确认窗口，单位毫秒；零表示无限。超时不释放仍执行的顺序槽。
         */
        private Long handlerTimeoutMs;
        /**
         * DLT 目标的追加后缀；空值关闭 DLT，失败输入因此进入分区保护。
         */
        private String deadLetterTopicSuffix;
        /**
         * 是否必须取得 DLT broker 确认；false 明确允许丢弃未交付死信。
         */
        private Boolean deadLetterRequired;
        /**
         * 死信发送使用的已声明通道；独立通道可隔离普通业务流量。
         */
        private String dltProducerLane;
        /**
         * 同一源全部普通消费组的 DLT 信封数量上限，覆盖等待、交付、完成及退避重试。
         */
        private Integer dltQueueCapacity;
        /**
         * 同一源全部普通消费组的 DLT 信封的字节上限；原输入另计入 progress，重试不提前归还信封额度。
         */
        private Long dltQueueMaxBytes;
        /**
         * 同一源全部普通消费组的 DLT 实际交付并发上限，与业务调用额度独立。
         */
        private Integer dltMaxInFlight;
        /**
         * 必需 DLT 交付首次失败后的退避基准毫秒。
         */
        private Long dltRetryInitialMs;
        /**
         * DLT 指数退避基准上限；实际等待带约正负百分之十的稳定抖动。
         */
        private Long dltRetryMaxMs;
        /**
         * 同一 DLT 责任持续失败达到此次数后显示降级，仍保留交付责任。
         */
        private Integer dltDegradedAfterAttempts;
        /**
         * 没有匹配 topic/event 路由的输入处置方式，默认显式跳过。
         */
        private RecordPolicy unmatchedPolicy;
        /**
         * 确定无效的头或载荷的处置方式；系统性解码失败另受连续失败保护。
         */
        private RecordPolicy invalidRecordPolicy;
        /**
         * typed 路由对 Kafka null value 的处置；不影响 JSON null 和空字节。
         */
        private TombstonePolicy tombstonePolicy;
        /**
         * 显式共同广播的稳定实例身份；缺省采用进程身份。
         */
        private String broadcastInstanceId;
        /**
         * 每分区最多保留的已观察记录数，broker 提交确认后释放。
         */
        private Integer maxProgressRecordsPerPartition;
        /**
         * 同一分区跨 owner 的输入原文及副本预留字节上限。
         */
        private Long maxProgressBytesPerPartition;
        /**
         * 同一分区同一原始 key 的输入责任数量。
         */
        private Integer maxProgressRecordsPerKey;
        /**
         * 同一分区同一原始 key 的输入原文及副本预留字节上限。
         */
        private Long maxProgressBytesPerKey;
        /**
         * 一个 group 跨分区保留的责任数量上限。
         */
        private Integer maxProgressRecordsPerGroup;
        /**
         * 一个 group 的输入原文及框架副本预留上限，按原始 key、value 与有序 header 字节和的八倍计费。
         */
        private Long maxProgressBytesPerGroup;
        /**
         * 普通消费输入的进程共享记录上限，所有源声明必须一致。
         */
        private Integer maxProgressRecordsPerProcess;
        /**
         * 普通消费输入的进程共享原文及副本预留字节上限。
         */
        private Long maxProgressBytesPerProcess;
        /**
         * 同一源全部普通消费组的输入记录上限。
         */
        private Integer maxProgressRecordsPerSource;
        /**
         * 同一源全部普通消费组的输入原文及副本预留字节上限。
         */
        private Long maxProgressBytesPerSource;
        /**
         * 单条输入或发布的原始字节上限，包含 key、value 与 header。
         */
        private Integer maxRecordBytes;
        /**
         * 连续批业务调用的原文字节上限；首条合法记录允许单独交付，数量同时受 maxPollRecords 限制。
         */
        private Long maxBatchBytes;
        /**
         * 每个 group 同时运行的真实业务调用上限；虚拟线程不绕过此额度。
         */
        private Integer maxHandlerInFlight;
        /**
         * 正常提交的时间触发间隔，单位毫秒；仅提交连续安全前缀。
         */
        private Long commitIntervalMs;
        /**
         * 正常提交的安全记录数量触发阈值，不授权越过前方未完成记录。
         */
        private Integer commitBatchRecords;
        /**
         * 每个 owner 控制邮箱的容量；满时返回未执行，不等待无限排队。
         */
        private Integer controlQueueCapacity;
        /**
         * 控制命令从排队开始计算的总预算，单位毫秒；执行中超时不代表撤销。
         */
        private Long controlTimeoutMs;
        /**
         * 两段关闭共用的总预算，单位毫秒；超时保留实际活动责任。
         */
        private Long shutdownTimeoutMs;
        /**
         * 是否按已知历史信封解码；关闭时直接按冻结的 DTO 类型读取原始 JSON。
         */
        private Boolean legacyEnvelope;
        /**
         * 路由与 codec 的 singleton 头是否拒绝重复；关闭时使用最后一个值。
         */
        private Boolean strictHeaders;
    }

    /**
     * 同一源的认证域，producer、consumer 与 Admin 共用。
     */
    @Getter
    @Setter
    public static class Security {
        /**
         * 源共用的传输与认证协议，例如 PLAINTEXT、SASL_SSL。
         */
        private String protocol;
        /**
         * 源认证采用的 SASL mechanism。
         */
        private String saslMechanism;
        /**
         * 原生 JAAS 凭据配置，不得出现在日志、健康原因或公开配置快照。
         */
        private String saslJaasConfig;
        /**
         * 用于验证服务端证书的 truststore 路径。
         */
        private String sslTruststoreLocation;
        /**
         * 读取 truststore 的凭据，不能导出到诊断文本。
         */
        private String sslTruststorePassword;
        /**
         * truststore 的编码类型，例如 JKS 或 PKCS12，不根据文件后缀猜测。
         */
        private String sslTruststoreType;
        /**
         * mTLS 客户端身份的 keystore 路径。
         */
        private String sslKeystoreLocation;
        /**
         * 读取客户端 keystore 的凭据，不能导出到诊断文本。
         */
        private String sslKeystorePassword;
        /**
         * 客户端身份容器编码类型，与实际材料格式保持一致。
         */
        private String sslKeystoreType;
        /**
         * 客户端私钥的解密凭据，不能导出到诊断文本。
         */
        private String sslKeyPassword;
        /**
         * 服务端主机名验证算法；只有显式空串才关闭主机名校验。
         */
        private String sslEndpointIdentificationAlgorithm;
    }

    /**
     * 管理请求的超时与原生参数；不隐式执行修改操作。
     */
    @Getter
    @Setter
    public static class Admin {
        /**
         * 一次 broker 请求等待毫秒，不等于整体业务完成时限。
         */
        private Integer requestTimeoutMs;
        /**
         * 管理操作等待的默认总预算，单位毫秒。
         */
        private Integer defaultApiTimeoutMs;
        /**
         * 该角色的原生参数补充；不得覆盖源地址、认证域或框架固定的提交与 codec 合同。
         */
        private Map<String, String> properties;
    }

    /**
     * 业务作用：将显式嵌套字段合并到已含默认值的源配置。
     * <p>
     * 返回：将非 null 覆盖项写入目标副本，properties 与已有映射合并；身份和安全约束由后续有效配置校验执行。
     *
     * @param settings 覆盖项
     * @param target   独立副本
     * @param role     角色。
     */
    static void apply(Object settings, NasaKafkaProperties target, String role) {
        if (settings == null) return;
        BeanWrapper from = new BeanWrapperImpl(settings);
        BeanWrapper to = new BeanWrapperImpl(target);
        for (PropertyDescriptor field : from.getPropertyDescriptors()) {
            String name = field.getName();
            if ("class".equals(name) || field.getReadMethod() == null) continue;
            Object value = from.getPropertyValue(name);
            if (value == null) continue;
            String destination = destination(name, role);
            if (value instanceof Map<?, ?> map && "properties".equals(name)) {
                Map<Object, Object> merged = new LinkedHashMap<>((Map<?, ?>) to.getPropertyValue(destination));
                merged.putAll(map);
                value = merged;
            }
            to.setPropertyValue(destination, value);
        }
    }

    /**
     * 业务作用：使嵌套装配与迁移候选使用同一字段映射，避免角色参数被移至另一个客户端。
     *
     * @param name 嵌套属性名
     * @param role producer、consumer、admin、behavior 或 security。
     * @return 对应的源配置属性名，不读取配置值。
     */
    static String destination(String name, String role) {
        return switch (name) {
            case "maxInFlightRequestsPerConnection" -> "maxInflightRequestsPerConnection";
            case "concurrency" -> "consumerConcurrency";
            case "compression" -> "compressionType";
            case "lanes" -> "producerLanes";
            case "protocol" -> "securityProtocol";
            case "properties" -> role + "Properties";
            case "requestTimeoutMs" -> "admin".equals(role) ? "adminRequestTimeoutMs" : name;
            case "defaultApiTimeoutMs" -> "adminDefaultApiTimeoutMs";
            default -> name;
        };
    }
}

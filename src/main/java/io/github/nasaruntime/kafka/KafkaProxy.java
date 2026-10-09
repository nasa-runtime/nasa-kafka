package io.github.nasaruntime.kafka;

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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 类作用 - Kafka 组件主入口, 提供消息发布、群组消费、广播消费、分区消费、offset 查询和 topic 管理能力。
 */
@Slf4j
@Getter
public class KafkaProxy implements Initialization {

    /** 默认 event 名 (publish(stream, message) 不传 event 时用). */
    public static final String DEFAULT_EVENT = "DEFAULT";

    /** Kafka header key, 标识业务 event 名。 */
    public static final String HEADER_EVENT = "X-Nasa-Event";
    /** Kafka header key, 标识 payload 编码类型. */
    public static final String HEADER_PAYLOAD_CODEC = "X-Nasa-Payload-Codec";
    /** payload 由 {@link ProtocolBytes#encodeBytes()} 生成. */
    public static final String PAYLOAD_CODEC_PROTOCOL_BYTES = "protocol-bytes";
    /** Kafka header key, ProtocolBytes 的 Mode 名: 消费端据此校验 listener mode 一致, 跨版本/跨语言防呆. */
    public static final String HEADER_PAYLOAD_MODE = "X-Nasa-Payload-Mode";
    /** Kafka header key, 承载透传快照 (JSON). 放 header 而非包进 payload → ProtocolBytes 二进制 payload 与透传共存. */
    public static final String HEADER_PASSTHROUGH = "X-Nasa-Passthrough";
    /** 死信 (DLQ) 记录的来源元数据 header. */
    public static final String HEADER_DLT_ORIGIN_TOPIC = "X-Nasa-DLT-Origin-Topic";
    public static final String HEADER_DLT_ORIGIN_PARTITION = "X-Nasa-DLT-Origin-Partition";
    public static final String HEADER_DLT_ORIGIN_OFFSET = "X-Nasa-DLT-Origin-Offset";
    public static final String HEADER_DLT_REASON = "X-Nasa-DLT-Reason";

    /** 当前 JVM 的广播消费实例 ID, 用于生成每个实例独立的广播 group.id。 */
    private static final String BROADCAST_INSTANCE_ID = resolveBroadcastInstanceId();

    /**
     * 业务作用：解析本 JVM 的广播实例标识，它是广播消费「每实例一个 group」的区分依据。
     * 优先取系统属性，其次取环境变量，都没有则随机生成。
     * <p>
     * 随机生成意味着<b>进程重启后标识改变</b>，旧 group 的已提交位点不再被沿用，
     * 广播消费从当次配置的起点重新开始。需要重启后接续消费的部署，必须显式配置一个稳定标识。
     *
     * <p>参数说明: 无。
     *
     * @return 本 JVM 的广播实例标识。
     */
    private static String resolveBroadcastInstanceId() {
        String cfg = System.getProperty("nasa.kafka.broadcast-instance-id");
        if (StringUtils.isBlank(cfg)) cfg = System.getenv("NASA_KAFKA_BROADCAST_INSTANCE_ID");
        return StringUtils.isBlank(cfg) ? StringUtils.random(16) : cfg.trim();
    }

    private final NasaKafkaProperties properties;
    private final ApplicationContext context;

    /** 原生 KafkaProducer (key=String, value=byte[] JSON). 线程安全, 全局单例. */
    private volatile KafkaProducer<String, byte[]> producer;

    /** 共享 AdminClient (线程安全, 复用): admin 操作 + offset/committed 查询走它, 免每次建连. after() 关闭. */
    private volatile AdminClient adminClient;

    /** group.id 到运行中 poll task 的映射, subscribe 和 assign 模式都在这里登记。 */
    private final ConcurrentMap<String, KafkaPollTask> pollTasks = new ConcurrentHashMap<>();

    /** listener 注册表, 结构为 group -&gt; topic -&gt; event -&gt; listener entry。 */
    private final ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<String, ListenerEntry>>> listenerRegistry = new ConcurrentHashMap<>();

    /**
     * 业务作用：构造 Kafka 主入口。此时不建立任何连接——生产者与管理客户端都在首次使用时才创建，
     * 使不实际收发消息的应用不会因引入本组件而与 broker 建连。
     *
     * @param properties Kafka 组件配置
     * @param context    Spring 上下文，启动时据此扫描容器内的 listener Bean
     */
    public KafkaProxy(NasaKafkaProperties properties, ApplicationContext context) {
        this.properties = properties;
        this.context = context;
    }

    /**
     * 业务作用：按作用域生成本 JVM 独享的广播 group.id。
     * <p>
     * 把不属于 Kafka group.id 合法字符集的字符统一替换掉：类名中的符号若直接进入 group.id，
     * broker 会拒绝该消费者加入，表现为消费者反复重连而无任何消息。
     *
     * @param scope 广播作用域，为空白时按默认作用域处理
     * @return 形如 {@code nasa-kafka-broadcast-<作用域>-<实例标识>} 的 group.id。
     */
    public static String broadcastGroup(String scope) {
        String s = StringUtils.isBlank(scope) ? "default" : scope.replaceAll("[^A-Za-z0-9._-]", "_");
        return "nasa-kafka-broadcast-" + s + "-" + BROADCAST_INSTANCE_ID;
    }

    /**
     * 业务作用：按 listener 类生成本 JVM 独享的广播 group.id，使不同广播 listener 之间互不干扰。
     * 用类的全限定名作为作用域，因此同一个类在不同实例上得到不同 group，不同类之间也不会共用 group。
     *
     * @param listenerClass 广播 listener 的类型，可为 null
     * @return 该 listener 在本 JVM 上的独享 group.id。
     */
    public static String broadcastGroup(Class<?> listenerClass) {
        return broadcastGroup(listenerClass == null ? null : listenerClass.getName());
    }

    // ==================== producer init / shutdown ====================

    /**
     * 业务作用：按需建出并复用全局唯一的生产者。
     * 生产者是线程安全的重量级对象，内部持有连接池与发送缓冲；每次发送新建会让连接数随并发线性增长，
     * 且失去批量聚合的效果。
     * <p>
     * 用双重检查在锁外快速返回，使稳态下的发送路径不承担同步开销。
     *
     * <p>参数说明: 无。
     *
     * @return 全局唯一的生产者实例。
     */
    private KafkaProducer<String, byte[]> producer() {
        KafkaProducer<String, byte[]> p = producer;
        if (p != null) return p;
        synchronized (this) {
            if (producer != null) return producer;
            producer = new KafkaProducer<>(properties.buildProducerProperties());
            log.info("[kafka] producer initialized bootstrapServers={}", properties.getBootstrapServers());
            return producer;
        }
    }

    // ==================== publish API ====================

    /**
     * 业务作用：以默认事件名同步发布一条消息，用于该 topic 只有单一消息语义、无需二级路由的场景。
     * 消费端需注册事件名为默认值的 listener 才能收到。
     * 失败时抛出运行时异常，调用方可据此回滚业务；但线程被中断的情形只记日志<b>不抛出</b>，
     * 此时消息是否送达无法确定。
     *
     * @param topic   目标 topic
     * @param message 业务消息体
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void publish(String topic, Object message) {
        this.publish(topic, DEFAULT_EVENT, null, message);
    }

    /**
     * 业务作用：以指定事件名同步发布一条消息，事件名是消费端 topic 之下的二级路由依据。
     * 不指定 key 时由默认分区器选分区，同一业务实体的多条消息可能落到不同分区，<b>不保证相对顺序</b>。
     * 失败时抛出运行时异常，调用方可据此回滚业务；但线程被中断的情形只记日志<b>不抛出</b>，
     * 此时消息是否送达无法确定。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param message 业务消息体
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void publish(String topic, String event, Object message) {
        this.publish(topic, event, null, message);
    }

    /**
     * 业务作用：同步发布一条消息，并以 key 决定分区，使同 key 的消息严格保序。
     * 当前线程登记的透传项与链路标识会自动随消息带到下游。
     * 失败时抛出运行时异常，调用方可据此回滚业务；但线程被中断的情形只记日志<b>不抛出</b>，
     * 此时消息是否送达无法确定。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键；相同 key 落到相同分区，为 null 时由默认分区器选择
     * @param message 业务消息体
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void publish(String topic, String event, String key, Object message) {
        // passthrough 走 header: payload 保持纯序列化, ProtocolBytes 二进制与透传共存.
        this.send(topic, key, message, event, KafkaProxyHolder.passthrough());
    }

    /**
     * 业务作用：同步发布一条消息并附带自定义消息头，用于传递框架未建模的业务元数据。
     * <p>
     * 框架保留的头会覆盖同名自定义头：框架头在自定义头之后写入，而消费端取同名头的最后一个值，
     * 因此自定义头无法篡改事件名、负载编码与透传内容。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键
     * @param message 业务消息体
     * @param headers 自定义消息头；值为集合时展开成多个同名头，值为 null 的项跳过
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void publish(String topic, String event, String key, Object message, Map<String, ?> headers) {
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, null, key, message, event, headers, KafkaProxyHolder.passthrough());
        this.dispatchSend(record, topic);
    }

    /**
     * 业务作用：同步发布一条并指定消息时间戳，用于数据补录时保持消息的业务时间而非写入时间。
     * 按时间定位位点的消费方会依据这个时间戳，因此补录的历史数据能被正确地按原始时间检索到。
     *
     * @param topic     目标 topic
     * @param event     事件名
     * @param key       分区键
     * @param timestamp 消息时间戳，epoch 毫秒
     * @param message   业务消息体
     * @param headers   自定义消息头
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void publishAt(String topic, String event, String key, long timestamp, Object message, Map<String, ?> headers) {
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, timestamp, key, message, event, headers, KafkaProxyHolder.passthrough());
        this.dispatchSend(record, topic);
    }

    /**
     * 业务作用：把消息同步发到指定分区，绕开分区器，用于分区与业务分片一一对应的场景。
     * 指定分区号超出该 topic 现有分区数时发送失败。
     *
     * @param topic     目标 topic
     * @param partition 目标分区号
     * @param event     事件名
     * @param message   业务消息体
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void publishToPartition(String topic, int partition, String event, Object message) {
        this.publishToPartition(topic, partition, event, null, message);
    }

    /**
     * 业务作用：把消息同步发到指定分区，同时保留 key 供消费端使用。
     * 分区已被显式指定，此时 <b>key 不参与分区选择</b>，仅作为消息的业务标识随消息传递。
     *
     * @param topic     目标 topic
     * @param partition 目标分区号
     * @param event     事件名
     * @param key       消息键，此处不影响分区归属
     * @param message   业务消息体
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void publishToPartition(String topic, int partition, String event, String key, Object message) {
        this.publishToPartition(topic, partition, event, key, message, null);
    }

    /**
     * 业务作用：把消息同步发到指定分区并附带自定义消息头。
     *
     * @param topic     目标 topic
     * @param partition 目标分区号
     * @param event     事件名
     * @param key       消息键，此处不影响分区归属
     * @param message   业务消息体
     * @param headers   自定义消息头
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void publishToPartition(String topic, int partition, String event, String key, Object message, Map<String, ?> headers) {
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, partition, null, key, message, event, headers, KafkaProxyHolder.passthrough());
        this.dispatchSend(record, topic);
    }

    /**
     * 业务作用：按业务分片键发布消息，使同一分片的消息落到同一分区从而保持处理顺序。
     * <p>
     * <b>事件名被设成 topic 名本身</b>，而不是默认事件名。消费端必须注册事件名等于 topic 名的 listener，
     * 否则消息会因匹配不到 listener 而被跳过且不推进位点。
     *
     * @param topic        目标 topic，同时被用作事件名
     * @param partitionKey 业务分片键
     * @param data         业务消息体
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void partition(String topic, String partitionKey, Object data) {
        this.publish(topic, topic, partitionKey, data);
    }

    /**
     * 业务作用：按数值型业务分片键发布消息，键按十进制字符串参与分区计算。
     * <p>
     * 事件名同样被设成 topic 名本身，消费端需照此注册 listener。
     * <p>
     * 数值转字符串后再哈希，因此与直接传同值字符串的重载落到<b>同一分区</b>，两种写法可混用。
     *
     * @param topic        目标 topic，同时被用作事件名
     * @param partitionKey 数值型业务分片键
     * @param data         业务消息体
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void partition(String topic, long partitionKey, Object data) {
        this.publish(topic, topic, String.valueOf(partitionKey), data);
    }

    /**
     * 业务作用：按业务分片键发布消息并显式指定事件名，用于同一 topic 承载多种消息语义且需要按分片保序的场景。
     *
     * @param topic        目标 topic
     * @param event        事件名
     * @param partitionKey 业务分片键
     * @param data         业务消息体
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void partition(String topic, String event, String partitionKey, Object data) {
        this.publish(topic, event, partitionKey, data);
    }

    /**
     * 业务作用：按数值型业务分片键发布消息并显式指定事件名。
     *
     * @param topic        目标 topic
     * @param event        事件名
     * @param partitionKey 数值型业务分片键
     * @param data         业务消息体
     * 返回: 无返回值；broker 确认后才返回。
     */
    public void partition(String topic, String event, long partitionKey, Object data) {
        this.publish(topic, event, String.valueOf(partitionKey), data);
    }

    /**
     * 业务作用：按列表顺序逐条同步发布，用于一组消息需要按给定次序进入同一分区的场景。
     * <p>
     * 逐条同步而非批量提交，因此中途失败时<b>此前各条已经送达且不会回滚</b>，
     * 异常只终止其后的发送。需要整组原子性的场景不能依赖本方法。
     *
     * @param topic            目标 topic
     * @param eventAndMessages 待发布的消息列表，为 null 或空时直接返回
     * 返回: 无返回值。
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
     * <b>失败只记日志，调用方无从感知</b>，因此不可用于必达消息。
     *
     * @param topic   目标 topic
     * @param message 业务消息体
     * 返回: 无返回值，立即返回。
     */
    public void publishAsync(String topic, Object message) {
        this.publishAsync(topic, DEFAULT_EVENT, null, message);
    }

    /**
     * 业务作用：以指定事件名异步发布，不等待 broker 确认。失败只记日志，调用方无从感知。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param message 业务消息体
     * 返回: 无返回值，立即返回。
     */
    public void publishAsync(String topic, String event, Object message) {
        this.publishAsync(topic, event, null, message);
    }

    /**
     * 业务作用：异步发布并立即返回，把发送耗时与失败完全隔离在业务线程之外。
     * <p>
     * 两处都只记日志不外抛：回调里的失败发生在生产者的 IO 线程上，抛出会打断该线程的后续发送；
     * 提交阶段的失败（如发送缓冲已满而阻塞超时）若外抛，就违背了「不影响业务线程」的本意。
     * <p>
     * 代价是<b>消息可能丢失且调用方完全无感</b>。需要确认送达的场景应改用同步发布或取回异步结果自行处理。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键
     * @param message 业务消息体
     * 返回: 无返回值，立即返回。
     */
    public void publishAsync(String topic, String event, String key, Object message) {
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, null, key, message, event, null, KafkaProxyHolder.passthrough());
        try {
            producer().send(record, (md, ex) -> {
                if (ex != null) {
                    log.error("{} [kafka-publish-async] failed topic={} event={}", AnyHolder.getTraceId(), topic, event, ex);
                }
            });
        } catch (Throwable e) {
            log.error("{} [kafka-publish-async] submit failed topic={} event={}", AnyHolder.getTraceId(), topic, event, e);
        }
    }

    /**
     * 业务作用：异步发送并把结果交回调用方，使调用方能自行决定等待、串接回调还是忽略。
     * 与只记日志的异步发布相比，失败在此是<b>可感知</b>的。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键
     * @param message 业务消息体
     * @return 承载消息落位元数据的异步结果；发送失败时以异常完成。
     */
    public Future<RecordMetadata> publishFuture(String topic, String event, String key, Object message) {
        return this.publishFuture(topic, event, key, message, null);
    }

    /**
     * 业务作用：异步发送并附带自定义消息头，把结果交回调用方处理。
     * <p>
     * 提交阶段的异常在此<b>不被捕获</b>，会直接抛给调用方——与只记日志的异步发布不同，
     * 本方法的语义是「结果归调用方」，吞掉提交失败会让调用方既拿不到结果也收不到异常。
     *
     * @param topic   目标 topic
     * @param event   事件名
     * @param key     分区键
     * @param message 业务消息体
     * @param headers 自定义消息头
     * @return 承载消息落位元数据的异步结果。
     */
    public Future<RecordMetadata> publishFuture(String topic, String event, String key, Object message, Map<String, ?> headers) {
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, null, key, message, event, headers, KafkaProxyHolder.passthrough());
        return producer().send(record);
    }

    /**
     * 业务作用：阻塞等待已提交给生产者的消息全部发送完毕，用于停机前或关键节点确保异步发送的消息不滞留在缓冲里。
     * 不保证全部成功，只保证不再有滞留：失败的消息其结果由各自的回调反映。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    public void flush() {
        producer().flush();
    }

    // ---- 死信 (DLQ): 毒消息超过重试上限后转存 ----

    /**
     * 业务作用：把某分区本批的全部消息转投死信，供上层在连续消费失败达到上限后处置毒批。
     * <p>
     * 未配置死信 topic 后缀视为关闭死信，此时返回「可前移」——坏消息被跳过而不保留，
     * 这是关闭死信功能的应有语义。
     * <p>
     * 转发失败时的处置由死信强制开关决定：默认为强制，返回「不可前移」使消息回退重投，
     * 在死信通道恢复前一条不丢；关闭强制则返回「可前移」并跳过，<b>坏消息被丢弃</b>，
     * 换取消费不被一个不可达的死信通道卡住。
     *
     * @param records 本轮拉取到的全部记录，从中筛出目标分区的部分
     * @param tp      失败的分区
     * @param reason  转投原因，写进死信消息的头供排查
     * @return 调用方是否可以前移位点越过这批消息。
     */
    boolean forwardPartitionToDeadLetter(ConsumerRecords<String, byte[]> records, TopicPartition tp, String reason) {
        String suffix = properties.getDeadLetterTopicSuffix();
        if (StringUtils.isBlank(suffix)) return true; // DLQ 禁用 → 视为可前移 (跳过, 不保留)
        boolean allOk = true;
        for (ConsumerRecord<String, byte[]> r : records.records(tp)) {
            if (!this.deadLetterOne(suffix.trim(), r, reason)) allOk = false;
        }
        if (allOk) return true;
        // 有转发失败: durability-first(默认)不前移、回退重投; availability-first(dead-letter-required=false)跳过并提交(丢消息)。
        if (!properties.isDeadLetterRequired()) {
            log.warn("{} [kafka] DLT 转发失败但 dead-letter-required=false, 跳过并前移 (坏消息丢弃) tp={}", AnyHolder.getTraceId(), tp);
            return true;
        }
        return false;
    }

    /**
     * 业务作用：把单条无法解码的消息转投死信，供解码阶段处置坏消息。
     * <p>
     * 未配置死信 topic 后缀视为关闭死信，此时返回「可前移」——坏消息被跳过而不保留，
     * 这是关闭死信功能的应有语义。
     * <p>
     * 转发失败时的处置由死信强制开关决定：默认为强制，返回「不可前移」使消息回退重投，
     * 在死信通道恢复前一条不丢；关闭强制则返回「可前移」并跳过，<b>坏消息被丢弃</b>，
     * 换取消费不被一个不可达的死信通道卡住。
     *
     * @param record 待转投的原始记录
     * @param reason 转投原因，写进死信消息的头供排查
     * @return 调用方是否可以前移位点越过这条消息。
     */
    boolean forwardRecordToDeadLetter(ConsumerRecord<String, byte[]> record, String reason) {
        String suffix = properties.getDeadLetterTopicSuffix();
        if (StringUtils.isBlank(suffix)) return true; // DLQ 禁用 → 跳过 (可前移), 不保留
        if (this.deadLetterOne(suffix.trim(), record, reason)) return true;
        // 转发失败: durability-first(默认)不前移重投; availability-first(dead-letter-required=false)跳过并提交(丢消息)。
        if (!properties.isDeadLetterRequired()) {
            log.warn("{} [kafka] DLT 转发失败但 dead-letter-required=false, 跳过并前移 (坏消息丢弃) topic={} p={} offset={}",
                    AnyHolder.getTraceId(), record.topic(), record.partition(), record.offset());
            return true;
        }
        return false;
    }

    /**
     * 业务作用：转投一条消息到死信并把成败转成布尔结果，使上层无需处理异常即可决定是否前移位点。
     * 不向上抛异常是有意为之：转投失败是可恢复情形（死信 topic 暂时不可写），
     * 应当由上层按策略选择重投或跳过，而不是让整个消费循环崩溃重建。
     *
     * @param suffix 死信 topic 后缀
     * @param record 待转投的原始记录
     * @param reason 转投原因
     * @return 转投成功返回 true；失败记录日志后返回 false。
     */
    private boolean deadLetterOne(String suffix, ConsumerRecord<String, byte[]> record, String reason) {
        try {
            this.sendToDeadLetter(suffix, record, reason);
            return true;
        } catch (Throwable t) {
            log.error("{} [kafka] 死信转发失败 (将不前移 offset, 下轮重试) topic={} p={} offset={}",
                    AnyHolder.getTraceId(), record.topic(), record.partition(), record.offset(), t);
            return false;
        }
    }

    /**
     * 业务作用：把一条消息完整搬到死信 topic，并附上足以定位与重放的来源信息。
     * <p>
     * 原始的 key、负载与全部消息头都原样保留，使死信消息可以被直接重新投回原 topic 完成重放；
     * 在此之上追加来源 topic、分区、位点与原因四个头，供人工排查定位到确切的那一条。
     * <p>
     * <b>同步等待 broker 确认后才返回</b>：只有确认落盘，上层前移位点越过原消息才是安全的，
     * 异步发送会让「已转投」变成一个未经证实的假设。
     *
     * @param suffix 死信 topic 后缀，与原 topic 名拼接成目标 topic
     * @param record 待转投的原始记录
     * @param reason 转投原因，为 null 时不写原因头
     * 返回: 无返回值。
     * @throws Exception 发送失败、超时或被中断
     */
    private void sendToDeadLetter(String suffix, ConsumerRecord<String, byte[]> record, String reason) throws Exception {
        String dltTopic = record.topic() + suffix;
        ProducerRecord<String, byte[]> dlt = new ProducerRecord<>(dltTopic, null, record.key(), record.value());
        record.headers().forEach(h -> dlt.headers().add(h.key(), h.value()));
        dlt.headers().add(new RecordHeader(HEADER_DLT_ORIGIN_TOPIC, record.topic().getBytes(StandardCharsets.UTF_8)));
        dlt.headers().add(new RecordHeader(HEADER_DLT_ORIGIN_PARTITION, String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8)));
        dlt.headers().add(new RecordHeader(HEADER_DLT_ORIGIN_OFFSET, String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8)));
        if (reason != null) dlt.headers().add(new RecordHeader(HEADER_DLT_REASON, reason.getBytes(StandardCharsets.UTF_8)));
        producer().send(dlt).get(properties.getPublishTimeoutMs(), TimeUnit.MILLISECONDS);
        log.warn("[kafka] 毒消息转死信 {} <- {}-{}@{} reason={}", dltTopic, record.topic(), record.partition(), record.offset(), reason);
    }

    // ---- 内部 send ----

    /**
     * 业务作用：同步发送的内部收口，组装记录后交给发送流程。
     *
     * @param topic   目标 topic
     * @param key     分区键
     * @param message 业务消息体
     * @param event   事件名
     * @param pt      透传快照，可为 null
     * 返回: 无返回值。
     */
    private void send(String topic, String key, Object message, String event, Map<String, Object> pt) {
        ProducerRecord<String, byte[]> record = this.buildRecord(topic, null, null, key, message, event, null, pt);
        this.dispatchSend(record, topic);
    }

    /**
     * 业务作用：把业务参数组装成待发送记录，是所有发布路径的统一组装点。
     * <p>
     * 消息头的写入次序有意为之：先写自定义头，再依次写事件、负载编码与透传头。
     * 消费端取同名头的最后一个值，因此<b>框架头总是覆盖同名的自定义头</b>，
     * 自定义头无法篡改路由与解码所依据的信息。
     *
     * @param topic     目标 topic
     * @param partition 目标分区号；为 null 时由分区器按 key 选择
     * @param timestamp 消息时间戳；为 null 时由 broker 或生产者按配置填入
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
     *
     * @param record 待写入的记录
     * @param event  事件名，可为 null
     * 返回: 无返回值。
     */
    private void attachEventHeader(ProducerRecord<String, byte[]> record, String event) {
        if (event != null) {
            record.headers().add(new RecordHeader(HEADER_EVENT, event.getBytes(StandardCharsets.UTF_8)));
        }
    }

    /**
     * 业务作用：标注负载采用的编码方式，使消费端能选对解码路径。
     * <p>
     * 二进制编码额外写入模式标识，供消费端校验收发双方模式一致——模式不符时解码结果必然错乱，
     * 且错乱往往表现为字段取值离谱而非直接失败，靠这个头才能在解码前拦住。
     * <p>
     * JSON 编码<b>不写任何头</b>，从而与本组件启用二进制编码之前发出的历史消息保持兼容。
     *
     * @param record  待写入的记录
     * @param payload 业务消息体，据其类型判定编码方式
     * 返回: 无返回值。
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
     * 键空白或值为 null 的项跳过：空键在 broker 侧无法检索，空值与「头不存在」无法区分。
     * <p>
     * 值为集合时展开成多个同名头，用于承载天然多值的元数据，与 Kafka 允许同名头重复的语义一致。
     *
     * @param record  待写入的记录
     * @param headers 自定义头映射，可为 null 或空
     * 返回: 无返回值。
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
     * 业务作用：写入单个自定义头，并统一非字节类型的编码方式。
     * 字节数组原样写入以支持二进制元数据；其余类型按其字符串形式的 UTF-8 编码写入，
     * 因此消费端一律按 UTF-8 解读即可，无需为不同类型分别处理。
     *
     * @param record 待写入的记录
     * @param key    头名
     * @param value  头值，为 null 时跳过
     * 返回: 无返回值。
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
     *
     * @param record 待写入的记录
     * @param pt     透传快照，可为 null；为池化容器时在本方法内归还
     * 返回: 无返回值。
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
     * 业务作用：同步发送并等待 broker 确认，是所有同步发布路径的最终出口。
     * <p>
     * 发送失败与超时包装成运行时异常抛出，使调用方能感知并回滚业务。
     * <p>
     * <b>线程中断的情形只重新置位中断标志并记日志，不抛出异常</b>：此时消息是否已送达无法确定，
     * 调用方从返回上看不出异常。在会被中断的线程上做必达发送时，需要另行核对结果。
     *
     * @param record 待发送的记录
     * @param topic  目标 topic，仅用于日志定位
     * 返回: 无返回值；broker 确认后才返回。
     * @throws RuntimeException 发送失败或等待确认超时
     */
    private void dispatchSend(ProducerRecord<String, byte[]> record, String topic) {
        try {
            Future<RecordMetadata> future = producer().send(record);
            RecordMetadata md = future.get(properties.getPublishTimeoutMs(), TimeUnit.MILLISECONDS);
            if (log.isDebugEnabled()) {
                log.debug("{} [kafka-publish] topic={} partition={} offset={} keySize={} valueSize={}",
                        AnyHolder.getTraceId(), md.topic(), md.partition(), md.offset(),
                        record.key() == null ? 0 : record.key().length(),
                        record.value() == null ? 0 : record.value().length);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("{} [kafka-publish] interrupted topic={}", AnyHolder.getTraceId(), topic, e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("{} [kafka-publish] failed topic={}", AnyHolder.getTraceId(), topic, e);
            throw new RuntimeException(e);
        }
    }

    // ==================== consumer 动态订阅 ====================

    /**
     * 业务作用：给<b>所有</b>运行中的消费任务追加订阅一个 topic，用于运行期动态扩展消费范围。
     * 手动分配模式的任务会忽略该请求并记警告，因为其分区绑定在创建时已固定。
     * 需要只作用于某个 group 时应改用按 group 指定的重载。
     *
     * @param topic 要追加订阅的 topic
     * 返回: 无返回值，下一轮循环重新订阅时生效。
     */
    public void subscribe(String topic) {
        for (KafkaPollTask task : pollTasks.values()) {
            task.subscribe(topic);
        }
    }

    /**
     * 业务作用：给所有运行中的消费任务批量追加订阅。
     *
     * @param topics 要追加订阅的 topic 数组，为 null 时直接返回
     * 返回: 无返回值。
     */
    public void subscribe(String... topics) {
        if (topics == null) return;
        for (String topic : topics) this.subscribe(topic);
    }

    /**
     * 业务作用：只给指定 group 的消费任务追加订阅，避免波及其它 group。
     *
     * @param group 目标 group.id
     * @param topic 要追加订阅的 topic
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void subscribeGroup(String group, String topic) {
        requireTask(group).subscribe(topic);
    }

    /**
     * 业务作用：只给指定 group 的消费任务批量追加订阅。
     *
     * @param group  目标 group.id
     * @param topics 要追加订阅的 topic 集合，为 null 时直接返回
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void subscribeGroup(String group, Collection<String> topics) {
        if (topics == null) return;
        KafkaPollTask task = requireTask(group);
        for (String topic : topics) task.subscribe(topic);
    }

    /**
     * 业务作用：从所有运行中的消费任务中移除一个 topic 的订阅。
     * 已拉取但尚未处理完的消息仍会被处理完，取消订阅不会中断进行中的这一批。
     *
     * @param topic 要取消订阅的 topic
     * 返回: 无返回值，下一轮循环重新订阅时生效。
     */
    public void unsubscribe(String topic) {
        for (KafkaPollTask task : pollTasks.values()) {
            task.unsubscribe(topic);
        }
    }

    /**
     * 业务作用：从所有运行中的消费任务中批量移除订阅。
     *
     * @param topics 要取消订阅的 topic 数组，为 null 时直接返回
     * 返回: 无返回值。
     */
    public void unsubscribe(String... topics) {
        if (topics == null) return;
        for (String topic : topics) this.unsubscribe(topic);
    }

    /**
     * 业务作用：只从指定 group 的消费任务中移除一个 topic 的订阅。
     *
     * @param group 目标 group.id
     * @param topic 要取消订阅的 topic
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void unsubscribeGroup(String group, String topic) {
        requireTask(group).unsubscribe(topic);
    }

    /**
     * 业务作用：只从指定 group 的消费任务中批量移除订阅。
     *
     * @param group  目标 group.id
     * @param topics 要取消订阅的 topic 集合，为 null 时直接返回
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void unsubscribeGroup(String group, Collection<String> topics) {
        if (topics == null) return;
        KafkaPollTask task = requireTask(group);
        for (String topic : topics) task.unsubscribe(topic);
    }

    /**
     * 业务作用：停止某个 group 的消费并注销其 listener 注册表，订阅与手动分配两种模式都适用。
     * <p>
     * 先从任务表移除再停线程，使停止过程中新来的同名操作立刻失败而不是作用在正在关闭的任务上。
     * 停线程的异常只记日志，不阻断注册表的注销——注册表若不清掉，同名 group 将无法重新建立。
     * <p>
     * group 名在停止后可被重新占用，这也是手动分配模式重建消费的前提。
     *
     * @param group 目标 group.id；不存在时只清注册表，不报错
     * 返回: 无返回值。
     */
    public void stop(String group) {
        KafkaPollTask task = pollTasks.remove(group);
        if (task != null) {
            try {
                task.shutdown();
            } catch (Throwable t) {
                log.error("[kafka] stop group failed group={}", group, t);
            }
        }
        listenerRegistry.remove(group);
        log.info("[kafka] group stopped group={}", group);
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
     * @throws IllegalArgumentException group 为空、分区集合为空
     * @throws IllegalStateException 该 group 已被占用
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
     * @throws IllegalArgumentException group 为空、映射为空、topic 为空或某 topic 的分区集合为空
     * @throws IllegalStateException 该 group 已被订阅式或手动分配式消费占用
     */
    @SuppressWarnings("rawtypes")
    public AssignmentHandle assign(String group, Map<String, Collection<Integer>> topicPartitions, StartOffset start, KafkaListener listener) {
        if (StringUtils.isBlank(group)) throw new IllegalArgumentException("assign group 不能为空");
        if (topicPartitions == null || topicPartitions.isEmpty()) throw new IllegalArgumentException("assign topicPartitions 不能为空");
        if (pollTasks.containsKey(group)) {
            throw new IllegalStateException("assign group 已被占用 (与已有 subscribe/assign 冲突): " + group);
        }
        Set<TopicPartition> tps = new LinkedHashSet<>();
        List<String> topics = new ArrayList<>();
        topicPartitions.forEach((topic, partitions) -> {
            if (StringUtils.isBlank(topic)) throw new IllegalArgumentException("assign topic 不能为空");
            if (partitions == null || partitions.isEmpty()) throw new IllegalArgumentException("assign partitions 不能为空 topic=" + topic);
            topics.add(topic);
            for (Integer p : partitions) tps.add(new TopicPartition(topic, p));
        });
        registerEntries(group, listener, topics.toArray(String[]::new));
        KafkaPollTask task = new KafkaPollTask(this, group, tps, start == null ? StartOffset.earliest() : start);
        pollTasks.put(group, task);
        task.start(properties.getStartConsumeDelayMs());
        log.info("[kafka] assign group={} partitions={} start={} listener={}",
                group, tps, start, listener.getClass().getName());
        return new AssignmentHandleImpl(this, group, task);
    }

    /**
     * 业务作用：停止一条手动分配消费，由控制句柄调用。幂等，重复调用无副作用。
     *
     * @param group 目标 group.id
     * 返回: 无返回值。
     */
    void stopAssignment(String group) {
        this.stop(group);
    }

    // ---- seek / pause / resume: 作用于运行中的 group 或 assign consumer (命令入队, poll 线程执行) ----

    /**
     * 业务作用：把运行中消费者在某分区的读取位置移到指定偏移量，用于重放或跳过。
     * 命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group     目标 group.id
     * @param topic     目标 topic
     * @param partition 分区号
     * @param offset    目标偏移量
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seek(String group, String topic, int partition, long offset) {
        requireTask(group).seek(new TopicPartition(topic, partition), offset);
    }

    /**
     * 业务作用：批量移动多个分区的读取位置。
     * 命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group   目标 group.id
     * @param offsets 分区到目标偏移量的映射，为 null 或空时直接返回
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seek(String group, Map<TopicPartition, Long> offsets) {
        if (offsets == null || offsets.isEmpty()) return;
        requireTask(group).seek(offsets);
    }

    /**
     * 业务作用：把指定 topic 的若干分区移到最早可用位置，用于重放尚未被清理的历史。
     * 实际起点受 broker 保留策略约束。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号，不传表示不操作任何分区
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seekToBeginning(String group, String topic, int... partitions) {
        requireTask(group).seekToBeginning(toTps(topic, partitions));
    }

    /**
     * 业务作用：跨 topic 把若干分区移到最早可用位置。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seekToBeginning(String group, Collection<TopicPartition> partitions) {
        requireTask(group).seekToBeginning(partitions);
    }

    /**
     * 业务作用：把指定 topic 的若干分区移到末尾，用于丢弃积压。
     * 被跳过的消息<b>不会再被消费</b>，是不可逆操作。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seekToEnd(String group, String topic, int... partitions) {
        requireTask(group).seekToEnd(toTps(topic, partitions));
    }

    /**
     * 业务作用：跨 topic 把若干分区移到末尾，丢弃这些分区的积压。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seekToEnd(String group, Collection<TopicPartition> partitions) {
        requireTask(group).seekToEnd(partitions);
    }

    /**
     * 业务作用：把某分区移到指定时刻之后的第一条消息，用于按时间窗口回放。
     * 该分区没有满足条件的消息时移到末尾，即只消费此后的新消息。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group     目标 group.id
     * @param topic     目标 topic
     * @param partition 分区号
     * @param epochMs   起始时刻，epoch 毫秒
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seekToTimestamp(String group, String topic, int partition, long epochMs) {
        requireTask(group).seekToTimestamp(new TopicPartition(topic, partition), epochMs);
    }

    /**
     * 业务作用：批量按各自时刻定位多个分区。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param timestamps 分区到起始时刻（epoch 毫秒）的映射
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seekToTimestamp(String group, Map<TopicPartition, Long> timestamps) {
        if (timestamps == null || timestamps.isEmpty()) return;
        requireTask(group).seekToTimestamp(timestamps);
    }

    /**
     * 业务作用：把指定 topic 的若干分区退回该 group 已提交的位点，用于放弃本次会话内的进度重新消费。
     * 没有提交记录的分区保持不动。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seekToCommitted(String group, String topic, int... partitions) {
        requireTask(group).seekToCommitted(toTps(topic, partitions));
    }

    /**
     * 业务作用：跨 topic 把若干分区退回该 group 已提交的位点。没有提交记录的分区保持不动。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void seekToCommitted(String group, Collection<TopicPartition> partitions) {
        requireTask(group).seekToCommitted(partitions);
    }

    /**
     * 业务作用：暂停指定 topic 若干分区的拉取，用于下游承压时局部降速。
     * 暂停期间消费者仍在组内保持心跳，<b>不会触发再均衡</b>，因此不影响其它成员。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void pause(String group, String topic, int... partitions) {
        requireTask(group).pause(toTps(topic, partitions));
    }

    /**
     * 业务作用：跨 topic 暂停若干分区的拉取。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void pause(String group, Collection<TopicPartition> partitions) {
        requireTask(group).pause(partitions);
    }

    /**
     * 业务作用：暂停该消费者当前实际持有的全部分区，用于整体限流或维护窗口。
     * 以执行时刻的实际持有为准，因此再均衡后新分到的分区也会被涵盖。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group 目标 group.id
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void pauseAll(String group) {
        requireTask(group).pauseAll();
    }

    /**
     * 业务作用：恢复指定 topic 若干分区的拉取。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param topic      目标 topic
     * @param partitions 分区号
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void resume(String group, String topic, int... partitions) {
        requireTask(group).resume(toTps(topic, partitions));
    }

    /**
     * 业务作用：跨 topic 恢复若干分区的拉取。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group      目标 group.id
     * @param partitions 目标分区集合
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void resume(String group, Collection<TopicPartition> partitions) {
        requireTask(group).resume(partitions);
    }

    /**
     * 业务作用：恢复全部处于暂停状态的分区，不会误碰本就在正常拉取的分区。命令排入 poll 线程队列异步生效，最迟在一个拉取超时之后执行完毕。
     *
     * @param group 目标 group.id
     * 返回: 无返回值。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    public void resumeAll(String group) {
        requireTask(group).resumeAll();
    }

    /**
     * 业务作用：解析分区查询类操作的等待上限。
     * 这类查询要排队到 poll 线程执行，耗时特征与发送不同，因此提供独立配置；
     * 未单独配置时回退到发布超时，避免多出一个必须配置的项。
     *
     * <p>参数说明: 无。
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
     * 业务作用：从运行中消费者视角查询已提交位点。
     * 与经由管理客户端的查询相比，本方法走消费者自身的缓存，反映的是<b>该消费者所知</b>的提交状态，
     * 在提交刚发生而尚未被管理侧看到时更及时；但只能查询本进程正在消费的 group。
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

    // ---- 分区 / offset 元数据: 短连 consumer 查询, 与运行中的 consumer 无关 ----

    /**
     * 业务作用：查询 topic 的分区元数据，含各分区的主副本与副本分布，用于容量规划与故障排查。
     * 每次调用新建一个短连消费者并在返回前关闭，不复用运行中的消费者——
     * 后者的元数据请求会与消费共用同一条线程，查询会被拉取阻塞拖慢。
     *
     * @param topic 目标 topic
     * @return 各分区的元数据列表；topic 不存在时由 Kafka 客户端按其配置处置。
     */
    public List<PartitionInfo> partitionsFor(String topic) {
        try (KafkaConsumer<String, byte[]> c = metadataConsumer()) {
            return c.partitionsFor(topic);
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
     * 业务作用：查询指定 topic 各分区的末尾位点，即下一条待写入的位置，用于计算积压量。
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
     * 业务作用：跨 topic 批量查询末尾位点。
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
            Map<TopicPartition, OffsetAndMetadata> r = admin().listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata().get(adminTimeoutMs(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, Long> out = new LinkedHashMap<>();
            for (TopicPartition tp : partitions) {
                OffsetAndMetadata om = r.get(tp);
                out.put(tp, om == null ? null : om.offset());
            }
            return out;
        } catch (Exception e) {
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
        Map<TopicPartition, Long> committed = committedOffsets(group, partitions);
        Map<TopicPartition, Long> end = endOffsets(partitions);
        List<KafkaPartitionLag> out = new ArrayList<>();
        for (TopicPartition tp : partitions) {
            Long committedOffset = committed.get(tp);
            long endOffset = end.getOrDefault(tp, 0L);
            Long lag = committedOffset == null ? null : Math.max(0L, endOffset - committedOffset);
            out.add(new KafkaPartitionLag(tp.topic(), tp.partition(), committedOffset, endOffset, lag));
        }
        return out;
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
        OffsetSpec spec = end ? OffsetSpec.latest() : OffsetSpec.earliest();
        Map<TopicPartition, OffsetSpec> req = new LinkedHashMap<>();
        for (TopicPartition tp : partitions) req.put(tp, spec);
        try {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> r =
                    admin().listOffsets(req).all().get(adminTimeoutMs(), TimeUnit.MILLISECONDS);
            Map<TopicPartition, Long> out = new LinkedHashMap<>();
            r.forEach((tp, info) -> out.put(tp, info.offset()));
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("query offsets failed end=" + end + " partitions=" + partitions, e);
        }
    }

    /**
     * 业务作用：建出一次性的元数据查询消费者。
     * group 名带随机后缀且用完即关：该消费者不订阅也不加入消费组，仅用于取元数据，
     * 借用运行中消费者的 group 会干扰其组成员身份。
     *
     * <p>参数说明: 无。
     *
     * @return 新建的消费者，调用方负责关闭。
     */
    private KafkaConsumer<String, byte[]> metadataConsumer() {
        return new KafkaConsumer<>(properties.buildConsumerProperties("nasa-kafka-meta-" + UUID.randomUUID()));
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
     * 业务作用：列出集群中全部 topic 名，供存在性判断与运维盘点使用。
     *
     * <p>参数说明: 无。
     *
     * @return topic 名集合。
     * @throws IllegalStateException 查询失败或超时
     */
    public Set<String> listTopics() {
        try {
            return admin().listTopics().names().get(adminTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("list kafka topics failed", e);
        }
    }

    /**
     * 业务作用：判断 topic 是否存在，用于发送前的前置检查。
     * 实现上要拉取集群全部 topic 名再判断，<b>不适合放在高频路径上</b>；
     * 频繁调用会给 broker 带来可观的元数据压力。
     *
     * @param topic 目标 topic
     * @return 存在返回 true。
     * @throws IllegalStateException 查询失败或超时
     */
    public boolean topicExists(String topic) {
        return listTopics().contains(topic);
    }

    /**
     * 业务作用：创建 topic 并等待创建完成。
     * topic 已存在时按失败处理并抛出异常；需要「有则跳过」语义应改用幂等的重载。
     *
     * @param topic             目标 topic
     * @param partitions        分区数，创建后只能增加不能减少
     * @param replicationFactor 副本数，不得超过集群 broker 数
     * @throws IllegalStateException 创建失败、超时，或 topic 已存在
     * 返回: 无返回值。
     */
    public void createTopic(String topic, int partitions, short replicationFactor) {
        try {
            admin().createTopics(List.of(new NewTopic(topic, partitions, replicationFactor)))
                    .all().get(adminTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
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
            admin().createTopics(List.of(new NewTopic(topic, partitions, replicationFactor)))
                    .all().get(adminTimeoutMs(), TimeUnit.MILLISECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) return false;
            throw new IllegalStateException("create kafka topic failed topic=" + topic, e);
        } catch (Exception e) {
            throw new IllegalStateException("create kafka topic failed topic=" + topic, e);
        }
    }

    /**
     * 业务作用：删除 topic 并等待删除完成；topic 不存在视为已达成目标，直接返回。
     * <p>
     * 删除是<b>不可逆</b>的，该 topic 的全部数据与各消费组在其上的提交位点都会丢失。
     * broker 未开启删除功能时本方法会失败。
     *
     * @param topic 目标 topic
     * @throws IllegalStateException 除「topic 不存在」之外的任何删除失败
     * 返回: 无返回值。
     */
    public void deleteTopic(String topic) {
        try {
            admin().deleteTopics(List.of(topic)).all().get(adminTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) return;
            throw new IllegalStateException("delete kafka topic failed topic=" + topic, e);
        } catch (Exception e) {
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
     *
     * @param topic           目标 topic
     * @param totalPartitions 扩容后的总分区数
     * @throws IllegalStateException 扩容失败或超时
     * 返回: 无返回值。
     */
    public void increasePartitions(String topic, int totalPartitions) {
        try {
            admin().createPartitions(Map.of(topic, NewPartitions.increaseTo(totalPartitions)))
                    .all().get(adminTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("increase kafka topic partitions failed topic=" + topic
                    + " totalPartitions=" + totalPartitions, e);
        }
    }

    /**
     * 业务作用：按需建出并复用全局唯一的管理客户端，供 topic 管理与位点查询使用。
     * 管理客户端线程安全且持有到集群的长连接，复用可免去每次操作的建连开销；
     * 用双重检查使稳态路径不承担同步开销。实例在组件停机时统一关闭。
     *
     * <p>参数说明: 无。
     *
     * @return 全局唯一的管理客户端。
     */
    private AdminClient admin() {
        AdminClient a = adminClient;
        if (a != null) return a;
        synchronized (this) {
            if (adminClient != null) return adminClient;
            adminClient = AdminClient.create(properties.buildAdminProperties());
            log.info("[kafka] admin client initialized");
            return adminClient;
        }
    }

    /**
     * 业务作用：解析管理类操作的等待上限。
     * 按「接口级超时 → 请求级超时 → 发布超时」依次回退，使调用方最少可以一项都不配。
     *
     * <p>参数说明: 无。
     *
     * @return 等待上限毫秒数。
     */
    private long adminTimeoutMs() {
        if (properties.getAdminDefaultApiTimeoutMs() != null) return properties.getAdminDefaultApiTimeoutMs();
        if (properties.getAdminRequestTimeoutMs() != null) return properties.getAdminRequestTimeoutMs();
        return properties.getPublishTimeoutMs();
    }

    /**
     * 业务作用：取出指定 group 的运行中消费任务，取不到即失败。
     * 所有按 group 操作的接口都经此收口：group 打错或消费尚未启动时若静默返回，
     * 调用方会误以为位点调整、暂停等操作已经生效。
     *
     * @param group 目标 group.id
     * @return 该 group 的运行中消费任务。
     * @throws IllegalStateException 该 group 没有运行中的消费任务
     */
    private KafkaPollTask requireTask(String group) {
        KafkaPollTask task = pollTasks.get(group);
        if (task == null) throw new IllegalStateException("no running consumer for group: " + group);
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
     * 业务作用：扫描容器内的 listener 并建立路由，然后启动各 group 的消费。
     * <p>
     * 先把全部 listener 注册完再统一启动消费：边注册边启动会让先启动的 group 在后续 listener
     * 尚未注册时就拉到消息，这些消息匹配不到 listener 会被跳过。
     * <p>
     * 容器内没有任何 listener 时直接跳过消费启动，使只做发布的服务不会平白建立消费连接。
     * <p>
     * 各 group 启动时统一施加一个延迟，错开首次组协调的时刻——同时发起会让 broker 侧集中处理，
     * 延长所有组的首次可用时间。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void before() {
        Map<String, KafkaListener> beans = context.getBeansOfType(KafkaListener.class);
        if (beans.isEmpty()) {
            log.info("[kafka] no KafkaListener bean discovered, skip consumer startup");
            return;
        }
        for (KafkaListener listener : beans.values()) {
            this.registerListener(listener);
        }
        // 启动各 group 的 poll task
        long startDelay = properties.getStartConsumeDelayMs();
        for (KafkaPollTask task : pollTasks.values()) {
            task.start(startDelay);
        }
        log.info("[kafka] consumer started groups={} listeners={}", pollTasks.keySet(), beans.size());
    }

    /**
     * 业务作用：停机时依次关闭全部消费任务、生产者与管理客户端，释放线程与连接。
     * <p>
     * 关闭顺序是先消费后生产：消费任务在关闭过程中可能需要转投死信，那要用到生产者，
     * 先关生产者会让这些消息丢失。
     * <p>
     * 每一步的异常都被就地捕获，确保一个组件关闭失败不阻断其余组件的释放——
     * 停机路径上抛出异常会留下未关闭的连接与线程。
     * <p>
     * 本方法可能被容器销毁与关闭事件各触发一次，因此设计为幂等。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void after() {
        for (KafkaPollTask task : pollTasks.values()) {
            try {
                task.shutdown();
            } catch (Throwable t) {
                log.error("{} [kafka] poll task shutdown failed group={}", AnyHolder.getTraceId(), task.getGroupId(), t);
            }
        }
        if (producer != null) {
            try {
                producer.close();
            } catch (Throwable t) {
                log.error("{} [kafka] producer close failed", AnyHolder.getTraceId(), t);
            }
        }
        if (adminClient != null) {
            try {
                adminClient.close();
            } catch (Throwable t) {
                log.error("{} [kafka] admin client close failed", AnyHolder.getTraceId(), t);
            }
        }
        log.info("[kafka] shutdown complete");
    }

    /**
     * 业务作用：把一个 listener 接进路由表，并确保其所属 group 的消费任务存在。
     * <p>
     * 未声明任何 topic 的 listener 只记警告并跳过：它不可能收到消息，
     * 但也不应因此让整个应用启动失败。
     * <p>
     * 消费任务按 group 复用：同一 group 的多个 listener 共用一条消费线程与一个消费者，
     * 每个 listener 各建一条会让同一 group 的成员数虚增，触发无谓的再均衡。
     * <p>
     * 建出任务后再逐个追加订阅，覆盖「任务已存在、本 listener 带来新 topic」的情形——
     * 只在建任务时取一次订阅集，后注册的 listener 的 topic 就会被漏掉。
     *
     * @param listener 待注册的 listener
     * 返回: 无返回值。
     */
    private void registerListener(KafkaListener listener) {
        String[] topics = listener.topics();
        if (topics == null || topics.length == 0) {
            log.warn("[kafka] listener {} has no topics, skip", listener.getClass().getName());
            return;
        }
        String group = resolveGroup(listener);
        String event = registerEntries(group, listener, topics);
        // 创建/复用 group 的 SUBSCRIBE poll task
        ConcurrentMap<String, ConcurrentMap<String, ListenerEntry>> byTopic = listenerRegistry.get(group);
        pollTasks.computeIfAbsent(group, g -> new KafkaPollTask(this, group, byTopic.keySet()));
        // 后续 listener 添加新 topic 时, 同步告诉已有 task
        KafkaPollTask task = pollTasks.get(group);
        for (String topic : topics) {
            if (!StringUtils.isBlank(topic)) task.subscribe(topic);
        }
        log.info("[kafka] register listener group={} topics={} event={} kind={} listener={}",
                group, Set.of(topics), event, ListenerKind.of(listener), listener.getClass().getName());
    }

    /**
     * 业务作用：确定 listener 归属的消费组，决定它与同类实例之间是竞争消费还是各自消费。
     * listener 自己声明的优先，未声明则用全局配置。
     * <p>
     * 两者都为空时<b>直接失败</b>而不使用某个默认值：group 决定消费的分配与位点归属，
     * 猜一个默认值会让不同服务意外共用同一个组，表现为消息被别的服务消费掉。
     *
     * @param listener 待解析的 listener
     * @return 该 listener 的 group.id。
     * @throws NullPointerException listener 未声明 group 且全局配置也为空
     */
    @SuppressWarnings("rawtypes")
    private String resolveGroup(KafkaListener listener) {
        String group = listener.group();
        if (StringUtils.isBlank(group)) group = properties.getGroupId();
        if (StringUtils.isBlank(group)) {
            throw new NullPointerException("KafkaListener.group() and nasa.kafka.group-id are both blank: " + listener.getClass().getName());
        }
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
     * @param topics   该 listener 声明的 topic，空白项跳过
     * @return 该 listener 声明的事件名。
     * @throws NullPointerException listener 未声明事件名
     * @throws IllegalStateException 同一 (group, topic, 事件) 已被另一个 listener 占用
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private String registerEntries(String group, KafkaListener listener, String[] topics) {
        String event = listener.event();
        if (StringUtils.isBlank(event)) {
            throw new NullPointerException("KafkaListener.event() must not be null/empty: " + listener.getClass().getName());
        }
        this.validateProtocolBytesParamType(listener);
        ListenerEntry entry = new ListenerEntry(listener.paramType(), listener, ListenerKind.of(listener));
        ConcurrentMap<String, ConcurrentMap<String, ListenerEntry>> byTopic = listenerRegistry.computeIfAbsent(group, g -> new ConcurrentHashMap<>());
        for (String topic : topics) {
            if (StringUtils.isBlank(topic)) continue;
            ConcurrentMap<String, ListenerEntry> byEvent = byTopic.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
            // 同一 (group,topic,event) 只允许一个 listener, 重复注册不同 listener 启动期 fail-fast (原为静默覆盖).
            ListenerEntry existing = byEvent.get(event);
            if (existing != null && existing.listener() != listener) {
                throw new IllegalStateException("Kafka listener 重复注册同一 (group,topic,event)=(" + group + "," + topic + "," + event
                        + "): " + existing.listener().getClass().getName() + " 与 " + listener.getClass().getName()
                        + " 冲突 (一条消息只路由一个 listener; 需要多播请用不同 group)");
            }
            byEvent.put(event, entry);
        }
        return event;
    }

    /**
     * 业务作用：在启动期拦住无法实例化的二进制协议类型声明。
     * 二进制解码要先按声明的类型建出实例再填充字段，接口与抽象类无法实例化，
     * 运行期必然逐条解码失败——放到启动期以配置错误的形式暴露，比上线后每条消息都进死信容易定位得多。
     *
     * @param listener 待校验的 listener
     * 返回: 无返回值；类型不合法时抛出异常阻断启动。
     * @throws IllegalStateException 声明的二进制协议类型是接口或抽象类
     */
    @SuppressWarnings("rawtypes")
    private void validateProtocolBytesParamType(KafkaListener listener) {
        Class<?> pc = rawClass(listener.paramType());
        if (pc != null && ProtocolBytes.class.isAssignableFrom(pc)
                && (pc.isInterface() || Modifier.isAbstract(pc.getModifiers()))) {
            throw new IllegalStateException("ProtocolBytes listener 的 paramType() 必须是具体可实例化类, 实际="
                    + pc.getName() + " (接口/抽象类无法解码), listener=" + listener.getClass().getName());
        }
    }

    // ==================== 消费端: 探测式拆包 (跟 RedisProxy.deserializeUnwrap 完全一致) ====================

    /**
     * 业务作用：识别并拆掉历史版本的消息信封，还原出真实的业务对象，使新老消息能被同一套 listener 消费。
     * <p>
     * 按三种情形依次判定：
     * 带类型信息时反序列化结果直接就是信封对象，取出其中的业务数据并归还该池化信封；
     * 不带类型信息时信封退化成普通映射，用其中的 topic 字段与当前 topic 相等来认定它是信封——
     * 业务对象恰好含有同名字段且取值等于 topic 名的可能性极低，这是可接受的启发式判定；
     * 两者都不匹配则认为消息没有信封，按目标类型直接反序列化。
     * <p>
     * 信封中携带的透传内容在拆包时关联到业务对象上，使消费方能取到上游传来的上下文。
     * <p>
     * 反序列化失败返回 null 而非抛出：调用方据此把消息判为不可解码并转投死信，
     * 抛出会让整批消费失败并重投，而重投对一条格式本身就错的消息毫无意义。
     * 失败前先恢复信封里的链路标识，使错误日志能关联到上游的调用链。
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
     * 业务作用：把消息负载解码成业务对象，是消费路径上二进制与 JSON 两条解码通道的分派点。
     * <p>
     * 二进制通道要求 listener 声明的类型确实实现了二进制协议，否则无法解码，记录错误后判为不可解码。
     * 消息头带有模式标识时先校验其与本地声明一致：模式不符时解码不会报错，
     * 而是产出字段取值离谱的对象，只有在解码前拦住才能避免这种错误数据流入业务。
     * <p>
     * JSON 通道先按通用类型解析，再交由信封识别流程还原业务对象。
     * <p>
     * 所有失败路径一律返回 null 而非抛出，由调用方按不可解码处置（转投死信或跳过）。
     * <p>
     * 最后把消息头承载的透传内容关联到业务对象上。这一步独立于信封拆包：
     * 二进制负载没有信封可以承载透传，其透传只能来自消息头。
     *
     * @param bytes                负载字节，可为 null
     * @param reference            listener 声明的目标类型引用
     * @param topic                当前 topic
     * @param protocolBytesPayload 负载是否为二进制编码
     * @param protocolBytesMode    消息头中的模式标识，为 null 时跳过一致性校验
     * @param passthroughHeader    透传头原始字节，可为 null
     * @return 解码后的业务对象；负载为空或任一环节失败时返回 null。
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
     * 业务作用：把负载字节按 JSON 解析成通用对象，供后续的信封识别与类型转换使用。
     * <p>
     * 解析失败时回退为按 UTF-8 当作纯字符串处理，而不是判为失败：
     * 以纯文本形式发送的消息本身是合法用法，直接判失败会把它们全部推进死信。
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

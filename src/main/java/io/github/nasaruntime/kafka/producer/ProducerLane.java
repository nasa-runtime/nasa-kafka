package io.github.nasaruntime.kafka.producer;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.config.NasaKafkaProperties;
import io.github.nasaruntime.kafka.observability.trace.KafkaSpan;
import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.protocol.KafkaWire;
import io.github.nasaruntime.kafka.support.KafkaBrokerErrors;
import io.github.nasaruntime.kafka.support.KafkaNative;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.lifecycle.CloseReport;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceContext;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceSpan;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.UnsupportedVersionException;

/**
 * 源内独立的物理发布通道；额度一直保留到交付结果发布与其同步回调处理完成。
 */
public final class ProducerLane {

    private final KafkaProxy owner;
    private final String name;
    private final NasaKafkaProperties config;
    private final ThreadPoolExecutor submitter;
    private final ThreadPoolExecutor observer;
    private final ThreadPoolExecutor closer;
    private final NavigableMap<Long, CompletableFuture<RecordMetadata>> pending = new TreeMap<>();
    private final Set<Long> completing = new HashSet<>();
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong terminalDeliveryUnknown = new AtomicLong();
    private NativeGeneration current;
    private NativeGeneration retiring;
    private long nativeGeneration;
    private long nativeRestarted;
    private long nativeStalledRestarted;
    private int liveNative;
    private boolean terminalFatal;
    private boolean recoveryQueued;
    private boolean rebuildRequested;
    private boolean stalledRebuild;
    private int consecutiveFailures;
    private long lastSuccessAt;
    private long lastRebuildAt;
    private boolean accepting = true;
    private boolean closed;
    private long bytes;
    private long sequence;

    /**
     * 一次原生客户端寿命；旧代次的回执仍按原责任完成，不能更新新代次恢复窗口。
     */
    private static final class NativeGeneration {
        final long id;
        final KafkaProducer<byte[], byte[]> producer;
        boolean closing;
        boolean closed;

        /**
         * 业务作用：绑定不可变原生身份，供发送与退出共同追踪。
         * <p>
         * 返回：尚未关闭的资源责任。
         *
         * @param id       代次
         * @param producer 该次资源。
         */
        NativeGeneration(long id, KafkaProducer<byte[], byte[]> producer) {
            this.id = id;
            this.producer = producer;
        }
    }

    /**
     * 业务作用：冻结通道身份及额度，不立即连接 broker。
     * <p>
     * 返回：未建连通道。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param owner  所属源
     * @param name   声明通道
     * @param config 独立有效配置。
     */
    public ProducerLane(KafkaProxy owner, String name, NasaKafkaProperties config) {
        this.owner = owner;
        this.name = name;
        this.config = config;
        submitter = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.getQueueBufferingMaxMessages()),
                Thread.ofPlatform().daemon().name("kafka-submit-" + owner.sourceId() + "-" + name).factory(), new ThreadPoolExecutor.AbortPolicy());
        observer = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.getFireObserverCapacity()),
                Thread.ofPlatform().daemon().name("kafka-observe-" + owner.sourceId() + "-" + name).factory(), new ThreadPoolExecutor.AbortPolicy());
        closer = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(2),
                Thread.ofPlatform().daemon().name("kafka-producer-close-" + owner.sourceId() + "-" + name).factory(), new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * 业务作用：为有上游链路的真实发布派生发送身份，回执结束后再导出。
     *
     * @param topic  目标
     * @param parent 上游上下文。
     * @return 不持有原文的有限span。
     */
    KafkaSpan span(String topic, KafkaTraceContext parent) {
        return owner.span("", topic, KafkaTraceSpan.Kind.PRODUCER, parent, List.of(), 0);
    }

    /**
     * 业务作用：读取稳定通道名。
     * <p>
     * 参数说明：无。
     *
     * @return 源内唯一名称。
     */
    public String name() {
        return name;
    }

    /**
     * 业务作用：让依赖持久目标确认的内置转发拒绝较弱确认模式。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 只有acks=all或-1满足该成功条件，不改变通道配置。
     */
    public boolean confirmsAllReplicas() {
        return "all".equals(config.getAcks()) || "-1".equals(config.getAcks());
    }

    /**
     * 业务作用：取得通道容量及终态计数。
     * <p>
     * 参数说明：无。
     *
     * @return 不含业务 key 或 payload 的快照。
     */
    public synchronized Stats stats() {
        return new Stats(name, pending.size(), bytes, delivered.get(), failed.get(), accepting && !terminalFatal && owner.acceptsBusinessPublishing(), closed);
    }

    public record Stats(String lane, int inFlight, long retainedBytes, long delivered, long failed, boolean accepting,
                        boolean closed) {}

    /**
     * 业务作用：区分尚未完成的本地交付与已退出但远端结果未知的交付。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 不发起 flush 的关闭证据，未知累计量在关闭后保留。
     */
    public synchronized CloseReport.Lane closeReport() {
        return new CloseReport.Lane(name, pending.size(), terminalDeliveryUnknown.get(), liveNative, closed);
    }

    /**
     * 业务作用：读取发布恢复和原生资源寿命，不改变原有Stats构造合同。
     * <p>
     * 参数说明：无。
     *
     * @return 累计代次、重建数、永久保护与当前原生资源数。
     */
    public synchronized RecoveryStats recoveryStats() {
        return new RecoveryStats(nativeGeneration, nativeRestarted, nativeStalledRestarted, terminalFatal, liveNative, consecutiveFailures);
    }

    /**
     * 原生恢复快照；liveNative包含正在退出的旧代次，零表示所有原生资源已退出。
     */
    public record RecoveryStats(long nativeGeneration, long nativeRestarted, long nativeStalledRestarted,
                                boolean terminalFatal, int liveNative, int consecutiveFailures) {}

    /**
     * 一次 flush 的不透明责任集合；外部不能构造、替换或完成其中的交付凭据。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public static final class FlushBarrier {
        private final ProducerLane lane;
        private final long sequence;
        private final CompletableFuture<?>[] futures;

        /**
         * 业务作用：绑定截取屏障的通道与私有交付凭据，保持等待期间的责任集合不变。
         * <p>
         * 返回：仅由所属通道创建且不导出可变内容的屏障。
         *
         * @param lane 所属物理通道。
         * @param sequence 截取时最后一个准入序号。
         * @param futures 通道在锁内创建的私有回执数组。
         */
        private FlushBarrier(ProducerLane lane, long sequence, CompletableFuture<?>[] futures) {
            this.lane = lane;
            this.sequence = sequence;
            this.futures = futures;
        }
    }

    /**
     * 业务作用：在该物理通道构造类型化消息。
     *
     * @param topic   目标
     * @param payload 业务值。
     * @return 尚未发送的 builder。
     */
    public PublishBuilder message(String topic, Object payload) {
        return new PublishBuilder(this, topic, payload, false);
    }

    /**
     * 业务作用：以原始字节发送，不额外包裹业务信封。
     *
     * @param topic   目标
     * @param payload 原始字节。
     * @return 尚未发送的 builder。
     */
    public PublishBuilder publishRaw(String topic, byte[] payload) {
        return new PublishBuilder(this, topic, payload, true);
    }

    /**
     * 业务作用：构造具有 null value 的删除事件。
     *
     * @param topic 目标
     * @param key   非 null 业务键，允许空字符串。
     * @return tombstone builder；缺失或不可精确编码的键以 CONFIG 拒绝。
     */
    public PublishBuilder tombstone(String topic, String key) {
        return new PublishBuilder(this, topic, null, true, true).key(key);
    }

    /**
     * 业务作用：兼容旧字符串 key 的发送并转入共同准入。
     *
     * @param record 旧格式。
     * @return 独立的交付等待结果。
     */
    CompletableFuture<RecordMetadata> send(ProducerRecord<String, byte[]> record) {
        return send(record, publishDeadline());
    }

    /**
     * 业务作用：让兼容字符串入口继承编码前的预算并保持精确键身份。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param record   旧格式
     * @param deadline 编码前冻结的期限。
     * @return 独立交付等待；非法文本或准入超期不发送。
     */
    public CompletableFuture<RecordMetadata> send(ProducerRecord<String, byte[]> record, long deadline) {
        byte[] key = record.key() == null ? null : KafkaWire.utf8(record.key());
        return sendBytes(new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), key, record.value(), record.headers()), deadline);
    }

    /**
     * 业务作用：接管发送责任，先预留容量再异步调用 native producer。
     *
     * @param record 输入。
     * @return 结果等待副本，取消等待不取消内部观测。
     */
    CompletableFuture<RecordMetadata> sendBytes(ProducerRecord<byte[], byte[]> record) {
        return sendBytes(record, publishDeadline());
    }

    /**
     * 业务作用：从编码开始统一计算一次发布预算。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 单调时钟截止时间。
     */
    public long publishDeadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getPublishTimeoutMs());
    }

    /**
     * 业务作用：在调用方总预算内进行有界准入。
     *
     * @param record   消息
     * @param deadline 包含编码的截止时间。
     * @return 独立交付等待；准入前超期不会发送。
     */
    CompletableFuture<RecordMetadata> sendBytes(ProducerRecord<byte[], byte[]> record, long deadline) {
        return sendBytes(record, deadline, false, null);
    }

    /**
     * 业务作用：把发送span绑定到内部实际交付，而非可被调用方取消的等待副本。
     *
     * @param record   消息
     * @param deadline 总预算
     * @param trace    有限链路责任。
     * @return 独立等待副本，取消或调用方修改不能提前结束span。
     */
    CompletableFuture<RecordMetadata> sendBytes(ProducerRecord<byte[], byte[]> record, long deadline, KafkaSpan trace) {
        return sendBytes(record, deadline, false, trace);
    }

    /**
     * 业务作用：发送原文字节，始终复验宿主业务准入，不授予内置收尾权。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param record 原文输入。
     * @return 独立交付等待；宿主停止后以 LIFECYCLE 拒绝新发送。
     */
    public CompletableFuture<RecordMetadata> sendBytesInternal(ProducerRecord<byte[], byte[]> record) {
        return sendBytes(record, publishDeadline(), false, null);
    }

    /**
     * 业务作用：凭来源登记的任务权威接续内置交付，业务句柄不能自行取得该权限。
     *
     * @param record 已登记 DLT 或转发责任的目标原文。
     * @param permit 仅来源可签发的通道、生命周期与用途凭据。
     * @return 独立交付等待；凭据不符或责任已结束时拒绝，不释放真实在途额度。
     */
    public CompletableFuture<RecordMetadata> sendBytesInternal(ProducerRecord<byte[], byte[]> record, KafkaProxy.InternalPublishPermit permit) {
        // 公开通道不代表内置发布权；必须先核对来源、物理通道与仍存续的任务责任。
        if (permit == null) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "内部发布缺少任务凭据");
        permit.require(this, owner, record);
        return sendBytes(record, publishDeadline(), true, null);
    }

    /**
     * 业务作用：在同一资源域内分别复验业务或内部准入并登记实际交付。
     *
     * @param record   输入
     * @param deadline 本次发布统一的 System.nanoTime 纳秒截止值，编码和排队不重新取得预算。
     * @param internal 是否属于已接纳的内置收尾责任，仅这种责任可继续使用保留的源依赖。
     * @param trace    可空的实际交付观测责任。
     * @return 独立等待副本，取消不释放真实责任。
     */
    private CompletableFuture<RecordMetadata> sendBytes(ProducerRecord<byte[], byte[]> record, long deadline, boolean internal, KafkaSpan trace) {
        // 宿主终止不能接纳新业务；内部责任继续受同一容量、期限和实际关闭边界限制。
        if (internal) owner.requireInternalPublishing();
        else owner.requirePublishing();
        if (System.nanoTime() >= deadline)
            throw new KafkaFailure(KafkaFailure.Code.PUBLISH, "编码或准入前已超过发布期限");
        if (record.topic() == null || record.topic().isBlank())
            throw new KafkaFailure(KafkaFailure.Code.PUBLISH, "topic 不能为空");
        KafkaHeaders headers = new KafkaHeaders(record.headers());
        // 内部 DLT/Transfer 和兼容入口也必须在发送前排除不能精确保留的头名。
        headers.entries().forEach(header -> KafkaWire.validateHeaderName(header.key()));
        byte[] key = record.key() == null ? null : record.key().clone();
        byte[] value = record.value() == null ? null : record.value().clone();
        long retained = (key == null ? 0L : key.length) + (value == null ? 0L : value.length) + headers.byteSize();
        if (retained > config.getMaxRecordBytes())
            throw new KafkaFailure(KafkaFailure.Code.PUBLISH, "消息超过 max-record-bytes");
        ProducerRecord<byte[], byte[]> snapshot = new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), key, value, headers.entries());
        CompletableFuture<RecordMetadata> responsibility = new CompletableFuture<>();
        // 观测责任附着在私有回执上；业务等待副本的cancel/complete只结束自身等待。
        if (trace != null) responsibility.whenComplete((metadata, error) -> trace.finishDelivery(error));
        long id;
        synchronized (this) {
            if (!accepting) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "producer lane 已关闭准入");
            if (terminalFatal) throw terminalFailure();
            // observer 与字节额度在 native send 之前取得，避免消息发出后没有人承担回执。
            if (pending.size() >= Math.min(config.getQueueBufferingMaxMessages(), config.getFireObserverCapacity())
                    || retained > config.getQueueBufferingMaxKbytes() * 1024 - bytes) {
                owner.metric("publish_admission_rejected_total", 1, true, Map.of("lane", name, "reason",
                        retained > config.getQueueBufferingMaxKbytes() * 1024 - bytes ? "byte_capacity" : "observer_capacity"));
                throw new KafkaFailure(KafkaFailure.Code.PRODUCER_QUEUE_FULL, "producer lane 容量已满: " + name);
            }
            id = ++sequence;
            bytes += retained;
            pending.put(id, responsibility);
        }
        pressure();
        try {
            submitter.execute(() -> {
                if (System.nanoTime() >= deadline) {
                    complete(id, retained, responsibility, snapshot.topic(), false, null, new KafkaFailure(KafkaFailure.Code.PUBLISH, "消息在 native send 前超过截止时间"));
                    return;
                }
                NativeGeneration generation = null;
                try {
                    generation = nativeProducer();
                    if (System.nanoTime() >= deadline)
                        throw new KafkaFailure(KafkaFailure.Code.PUBLISH, "native send 前已超过发布期限");
                    NativeGeneration used = generation;
                    used.producer.send(snapshot, (metadata, failure) -> {
                        noteNativeResult(used, failure, true);
                        complete(id, retained, responsibility, snapshot.topic(), true, metadata, failure);
                    });
                    owner.metric("publish_admitted_total", 1, true, Map.of("lane", name, "topic", snapshot.topic()));
                } catch (Throwable failure) {
                    if (generation != null) noteNativeResult(generation, failure, false);
                    complete(id, retained, responsibility, snapshot.topic(), false, null, failure);
                }
            });
        } catch (RejectedExecutionException rejected) {
            KafkaFailure failure = new KafkaFailure(KafkaFailure.Code.PRODUCER_QUEUE_FULL, "lane 未接受本次发送", rejected);
            // 屏障可能已捕获此准入凭据；提交线程拒绝也必须发布终态，不能留下永远未完成的Future。
            responsibility.completeExceptionally(failure);
            synchronized (this) {
                pending.remove(id);
                bytes -= retained;
                notifyAll();
            }
            pressure();
            throw failure;
        }
        return responsibility.copy();
    }

    /**
     * 业务作用：将原生终态移交调用方并释放该消息所有额度。
     * <p>
     * 返回：无，重复回执不重复释放；准入失败不冒充 broker 交付失败。
     *
     * @param id        责任序号
     * @param retained  字节
     * @param future    责任
     * @param topic     目标
     * @param submitted 表示来自 native 回执
     * @param metadata  原生成功回调的落位信息，可为空；acks=0 的成功不构成 broker 确认。
     * @param error     准入、原生发送或交付阶段的异常；null 表示该路径成功完成。
     */
    private void complete(long id, long retained, CompletableFuture<RecordMetadata> future, String topic, boolean submitted, RecordMetadata metadata, Throwable error) {
        synchronized (this) {
            if (!pending.containsKey(id) || !completing.add(id)) return;
        }
        // 准入时已为每条记录预留 observer 额度；用户 Future 回调不得占用 native I/O 线程。
        observer.execute(() -> {
            try {
                if (error == null) {
                    delivered.incrementAndGet();
                    owner.metric("published_total", 1, true, Map.of("lane", name, "topic", topic));
                    future.complete(metadata);
                } else {
                    failed.incrementAndGet();
                    if (submitted) owner.metric("publish_failed_total", 1, true, Map.of("lane", name, "topic", topic));
                    else
                        owner.metric("publish_admission_rejected_total", 1, true, Map.of("lane", name, "reason", "native_rejected"));
                    // 原生责任结束不等于远端未写入；超时、断连和追加后ISR不足必须保留重复可能性。
                    boolean unknown = error instanceof org.apache.kafka.common.errors.TimeoutException
                            || error instanceof NetworkException
                            || error instanceof NotEnoughReplicasAfterAppendException;
                    KafkaFailure failure = error instanceof KafkaFailure k ? k : new KafkaFailure(
                            unknown ? KafkaFailure.Code.OUTCOME_UNKNOWN : KafkaBrokerErrors.authorization(error) ? KafkaFailure.Code.AUTHORIZATION : KafkaFailure.Code.PUBLISH,
                            "Kafka 发布未成功 source=" + owner.sourceId() + " lane=" + name, error);
                    if (submitted && failure.code() == KafkaFailure.Code.OUTCOME_UNKNOWN) {
                        terminalDeliveryUnknown.incrementAndGet();
                        owner.metric("publish_delivery_unknown_total", 1, true, Map.of("lane", name));
                    }
                    future.completeExceptionally(failure);
                }
            } finally {
                // 完成结果发布后才释放额度，避免 flush 在结果尚不可见时越过该责任。
                synchronized (this) {
                    pending.remove(id);
                    completing.remove(id);
                    bytes -= retained;
                    notifyAll();
                }
                pressure();
            }
        });
    }

    /**
     * 业务作用：观察发布通道责任占用，不以未完成数冒充交付成功。
     * <p>
     * 参数说明：无。
     * 返回：向独立观测队列提供当前占用和 observer 百分比。
     */
    private void pressure() {
        Stats current = stats();
        Map<String, String> labels = Map.of("lane", name);
        owner.metric("producer_lane_in_flight", current.inFlight(), false, labels);
        owner.metric("producer_lane_queue_utilization", Math.min(100, 100L * current.inFlight() / config.getFireObserverCapacity()), false, labels);
    }

    /**
     * 业务作用：将调用方结束等待与最终交付结果分开计数。
     * <p>
     * 参数说明：无。
     * 返回：记录一次未知等待，不释放或取消任何交付责任。
     */
    void waitUnknown() {
        owner.metric("publish_wait_unknown_total", 1, true, Map.of("lane", name));
    }

    /**
     * 业务作用：由单一提交线程选择或切换发布代次。
     * <p>
     * 参数说明：无。
     *
     * @return 当前可用的原生代次；停止准入后不轮换已有代次，但已接纳任务仍可首次建立 producer，资源关闭后拒绝。
     */
    private synchronized NativeGeneration nativeProducer() {
        if (closed) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "lane 已关闭");
        if (terminalFatal) throw terminalFailure();
        if (current == null) current = createNative();
        if (rebuildRequested && accepting && retiring == null) {
            // 先成功构造再发布代次；最多一个旧代次退出，防止网络故障持续堆积客户端和线程。
            NativeGeneration replacement = createNative();
            retiring = current;
            current = replacement;
            nativeRestarted++;
            if (stalledRebuild) nativeStalledRestarted++;
            rebuildRequested = false;
            stalledRebuild = false;
            closeNative(retiring);
        }
        return current;
    }

    /**
     * 业务作用：以冻结配置创建原生资源并开始新的恢复窗口。
     * <p>
     * 参数说明：无。
     *
     * @return 构造成功的下一代次；失败不发布半完成代次。
     */
    private NativeGeneration createNative() {
        owner.verifyClusterIdentity();
        Properties props = config.buildProducerProperties();
        // 旧代次可能仍在退出；独立client.id避免旧资源注销JMX时移除新代次的观测入口。
        props.put(ProducerConfig.CLIENT_ID_CONFIG, props.getProperty(ProducerConfig.CLIENT_ID_CONFIG) + "-generation-" + (nativeGeneration + 1));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getQueueBufferingMaxKbytes() * 1024);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Math.min(config.getPublishTimeoutMs(), config.getMaxBlockMs() == null ? 1000L : config.getMaxBlockMs()));
        KafkaProducer<byte[], byte[]> producer = KafkaNative.construct(() -> new KafkaProducer<>(props));
        nativeGeneration++;
        liveNative++;
        consecutiveFailures = 0;
        lastSuccessAt = lastRebuildAt = System.nanoTime();
        return new NativeGeneration(nativeGeneration, producer);
    }

    /**
     * 业务作用：把当前代次原生证据纳入恢复窗口，保护凭据或权威错误。
     * <p>
     * 返回：只登记或排队恢复，不重发输入，不在I/O回调关闭资源。
     *
     * @param generation 实际发送代次
     * @param failure    当前原生代次的交付或同步发送异常；null 表示达到该 lane 配置的完成条件。
     * @param delivery   true 表示原生交付回调，false 表示同步发送阶段；后者不重置成功交付观测。
     */
    private synchronized void noteNativeResult(NativeGeneration generation, Throwable failure, boolean delivery) {
        // 旧代次只完成自己的交付责任，不能重置新代次失败窗口或触发新一轮切换。
        if (current != generation || closed) return;
        if (failure == null) {
            consecutiveFailures = 0;
            lastSuccessAt = System.nanoTime();
            if (stalledRebuild) {
                rebuildRequested = false;
                stalledRebuild = false;
            }
            return;
        }
        Throwable cause = failure;
        for (int depth = 0; depth < 8 && cause != null; depth++, cause = cause.getCause()) {
            if (cause instanceof AuthorizationException
                    || cause instanceof AuthenticationException
                    || cause instanceof ProducerFencedException
                    || cause instanceof UnsupportedVersionException) {
                // 明确认证、授权、fencing或协议不兼容不能通过创建新身份绕过；恢复需重建源配置。
                terminalFatal = true;
                rebuildRequested = false;
                return;
            }
            if (cause instanceof OutOfOrderSequenceException) {
                rebuildRequested = true;
                stalledRebuild = false;
                queueRecovery();
                return;
            }
        }
        if (!delivery) return;
        if (consecutiveFailures < Integer.MAX_VALUE) consecutiveFailures++;
        long now = System.nanoTime();
        if (consecutiveFailures >= config.getStalledRebuildMinFailures()
                && now - lastSuccessAt >= TimeUnit.MILLISECONDS.toNanos(config.getStalledRebuildAfterMs())
                && now - lastRebuildAt >= TimeUnit.MILLISECONDS.toNanos(config.getStalledRebuildCooldownMs())) {
            rebuildRequested = true;
            stalledRebuild = true;
            queueRecovery();
        }
    }

    /**
     * 业务作用：合并恢复请求并与send调用串行，避免替换发生在取代次与入队之间。
     * <p>
     * 参数说明：无。
     * 返回：至多一个排队动作；满载时保留恢复标志供后续send复验。
     */
    private synchronized void queueRecovery() {
        if (!accepting || terminalFatal || recoveryQueued || !rebuildRequested || retiring != null) return;
        recoveryQueued = true;
        try {
            submitter.execute(() -> {
                synchronized (ProducerLane.this) {
                    recoveryQueued = false;
                    if (!accepting || terminalFatal || !rebuildRequested) return;
                    try {
                        nativeProducer();
                    } catch (KafkaFailure unavailable) {
                        // 构造失败保留旧代次与恢复请求，由下一次业务发送重新尝试，不在后台无限循环。
                    }
                }
            });
        } catch (RejectedExecutionException full) {
            recoveryQueued = false;
        }
    }

    /**
     * 业务作用：把原生关闭移至独立有界线程，旧交付仍产生正常终态。
     * <p>
     * 返回：排队关闭；只有close实际返回才解除资源责任。
     *
     * @param generation 需要退出的资源。
     */
    private synchronized void closeNative(NativeGeneration generation) {
        if (generation == null || generation.closing || generation.closed) return;
        generation.closing = true;
        closer.execute(() -> {
            boolean completed = false;
            try {
                generation.producer.close();
                completed = true;
            } finally {
                synchronized (ProducerLane.this) {
                    generation.closing = false;
                    if (completed) {
                        generation.closed = true;
                        liveNative--;
                        if (retiring == generation) retiring = null;
                        queueRecovery();
                    }
                    ProducerLane.this.notifyAll();
                }
            }
        });
    }

    /**
     * 业务作用：为永久保护态提供固定脱敏错误，不重复暴露原生凭据诊断。
     * <p>
     * 参数说明：无。
     *
     * @return 不能由自动重建绕过的发布失败。
     */
    private KafkaFailure terminalFailure() {
        return new KafkaFailure(KafkaFailure.Code.PUBLISH, "producer lane 已进入永久保护: " + name);
    }

    /**
     * 业务作用：等待调用时已经准入的所有消息终态。
     * <p>
     * 参数说明：无。
     * 返回：屏障之前无未决消息，任意失败向调用方传播。
     */
    public void flush() {
        flushUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getPublishTimeoutMs()));
    }

    /**
     * 业务作用：在共享总期限内完成发送屏障。
     * <p>
     * 返回：屏障成功或结果未知异常。
     *
     * @param deadline 单调时钟纳秒。
     */
    void flushUntil(long deadline) {
        flushUntil(captureBarrier(), deadline);
    }

    /**
     * 业务作用：在等待之前冻结该lane当前责任，使源级flush可统一截取所有lane。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 绑定本通道的不透明屏障，不含之后准入，也不导出真实回执。
     */
    public synchronized FlushBarrier captureBarrier() {
        return new FlushBarrier(this, sequence, pending.values().toArray(CompletableFuture[]::new));
    }

    /**
     * 业务作用：等待指定屏障包含的交付与回执责任释放。
     * <p>
     * 返回：全部责任完成后成功或传播失败，超时不取消交付；null 屏障或其它通道的屏障以 CONFIG 拒绝。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param captured 已冻结责任
     * @param deadline 源级总期限。
     */
    public void flushUntil(FlushBarrier captured, long deadline) {
        // 序号只在所属通道内有效，不能借用其它通道的空屏障跳过本通道责任。
        if (captured == null || captured.lane != this)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "flush 屏障不属于当前通道");
        synchronized (this) {
            long barrier = captured.sequence;
            // Future终态可早于同步用户回调返回；屏障须等责任真正释放，且不追逐屏障后的新发送。
            while (!pending.isEmpty() && pending.firstKey() <= barrier) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0)
                    throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "lane 屏障之前仍有交付或回执责任");
                try {
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "lane 屏障等待被中断");
                }
            }
        }
        KafkaFailure failure = null;
        for (CompletableFuture<?> future : captured.futures) {
            try {
                await(future, deadline);
            } catch (KafkaFailure error) {
                // 任一交付未知都不能被另一个确定失败遮蔽，否则聚合调用方可能误判为安全重发。
                if (failure == null || error.code() == KafkaFailure.Code.OUTCOME_UNKNOWN) failure = error;
            }
        }
        if (failure != null) throw failure;
    }

    /**
     * 业务作用：关闭准入并收口原生资源。
     * <p>
     * 返回：全部资源退出才成功，重复调用继续收口。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param deadline 共享截止时间。
     */
    public void closeUntil(long deadline) {
        // 先封闭外部准入再排空提交线程；已预留的发送仍需产生可观察的终态。
        synchronized (this) {
            accepting = false;
            if (closed) return;
        }
        submitter.shutdown();
        try {
            if (!submitter.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "lane 提交线程尚未退出");
            synchronized (this) {
                closeNative(retiring);
                closeNative(current);
                // 总期限只结束调用方等待，不能强制撤销旧代次交付或假称原生线程已经退出。
                while (liveNative != 0) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0)
                        throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "lane 原生代次尚未退出");
                    TimeUnit.NANOSECONDS.timedWait(this, remaining);
                }
            }
            closer.shutdown();
            if (!closer.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "lane 原生关闭任务尚未退出");
            observer.shutdown();
            if (!observer.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "lane 回执处理尚未退出");
            synchronized (this) {
                // 原生关闭不等于用户回执已收口；未决责任存在时不能释放该通道身份。
                if (!pending.isEmpty()) throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "lane 仍有未决交付");
                closed = true;
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "lane 关闭等待被中断");
        }
    }

    /**
     * 业务作用：统一同步等待的失败与中断语义。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param future   交付结果
     * @param deadline 总期限。
     * @return 最终结果；超时不表示消息未发送。
     */
    public static <T> T await(CompletableFuture<T> future, long deadline) {
        try {
            return future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "等待被中断，执行结果未知");
        } catch (TimeoutException error) {
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "等待超时，执行结果未知");
        } catch (ExecutionException error) {
            if (error.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new KafkaFailure(KafkaFailure.Code.PUBLISH, "发布失败", error.getCause());
        }
    }

    /**
     * 业务作用：转换 native 回执为语言无关交付信息。
     *
     * @param metadata native 结果。
     * @return 包含确认模式的结果。
     */
    Delivery delivery(RecordMetadata metadata) {
        return new Delivery(owner.sourceId(), name, metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(), !"0".equals(config.getAcks()));
    }
}

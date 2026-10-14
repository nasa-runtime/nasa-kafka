package io.github.nasaruntime.kafka.producer;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.observability.trace.KafkaSpan;
import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.protocol.KafkaWire;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.consumer.ConsumeContext;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceContext;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceSpan;

import io.github.nasaruntime.core.base.ProtocolBytes;
import io.github.nasaruntime.core.utils.ObjMprUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * 一次消息的线路元数据，发送时取得独立快照。
 */
public final class PublishBuilder {

    private final ProducerLane lane;
    private final String topic;
    private final Object payload;
    private final boolean raw;
    private final boolean requireKey;
    private final RecordHeaders headers = new RecordHeaders();
    private byte[] event = KafkaWire.eventBytes(KafkaProxy.DEFAULT_EVENT);
    private byte[] key;
    private int keyKind;
    private Integer partition;
    private Long timestamp;
    private KafkaTraceContext traceParent;
    private boolean explicitTrace;

    /**
     * 业务作用：保存一次发送的目标与负载。
     * <p>
     * 返回：未准入消息。
     *
     * @param lane    通道
     * @param topic   目标
     * @param payload 负载
     * @param raw     表示原样字节。
     */
    PublishBuilder(ProducerLane lane, String topic, Object payload, boolean raw) {
        this(lane, topic, payload, raw, false);
    }

    /**
     * 业务作用：区分显式删除事件与任意 raw null value 的键约束。
     * <p>
     * 返回：未准入消息，空字节键仍有效。
     *
     * @param lane       实际接收发布责任并执行交付的producer通道。
     * @param topic      本次消息的目标topic。
     * @param payload    待发布载荷；raw 为 true 时必须为 byte[] 或 null，否则由类型化路径编码。
     * @param raw        true 保留 payload 原始字节及 null tombstone；false 使用 ProtocolBytes 或 JSON 编码。
     * @param requireKey 表示必须有非 null 键。
     */
    PublishBuilder(ProducerLane lane, String topic, Object payload, boolean raw, boolean requireKey) {
        this.lane = lane;
        this.topic = Objects.requireNonNull(topic);
        this.payload = payload;
        this.raw = raw;
        this.requireKey = requireKey;
    }

    /**
     * 业务作用：以精确 UTF-8 身份选择业务路由。
     *
     * @param event 非空白事件名。
     * @return 当前 builder；无效输入以 CONFIG 拒绝并保留之前的事件。
     */
    public PublishBuilder event(String event) {
        byte[] bytes = KafkaWire.eventBytes(event);
        this.event = bytes;
        return this;
    }

    /**
     * 业务作用：按精确 UTF-8 业务键选择分区。
     *
     * @param key 可空键，显式 tombstone 要求非 null。
     * @return 当前 builder；非法 UTF-16 或违反删除键约束时在改变键之前以 CONFIG 拒绝，不替换或归一化。
     */
    public PublishBuilder key(String key) {
        if (keyKind == 2) throw new IllegalArgumentException("key 与 keyBytes 不能混用");
        if (requireKey && key == null)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "显式 tombstone key 不能为 null");
        byte[] bytes = key == null ? null : KafkaWire.utf8(key);
        keyKind = 1;
        this.key = bytes;
        return this;
    }

    /**
     * 业务作用：保留原始二进制 key。
     *
     * @param key 可空原始键，显式 tombstone 要求非 null。
     * @return 当前 builder，保存独立副本；违反删除键约束时以 CONFIG 拒绝。
     */
    public PublishBuilder keyBytes(byte[] key) {
        if (keyKind == 1) throw new IllegalArgumentException("key 与 keyBytes 不能混用");
        if (requireKey && key == null)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "显式 tombstone key 不能为 null");
        keyKind = 2;
        this.key = key == null ? null : key.clone();
        return this;
    }

    /**
     * 业务作用：显式指定目标分区。
     *
     * @param partition 非负分区。
     * @return 当前 builder。
     */
    public PublishBuilder partition(int partition) {
        if (partition < 0) throw new IllegalArgumentException("partition 不能为负");
        this.partition = partition;
        return this;
    }

    /**
     * 业务作用：指定记录业务时间。
     *
     * @param epochMs epoch 毫秒。
     * @return 当前 builder。
     */
    public PublishBuilder timestamp(long epochMs) {
        if (epochMs < 0) throw new IllegalArgumentException("timestamp 不能为负");
        timestamp = epochMs;
        return this;
    }

    /**
     * 业务作用：跨执行器显式传递父链路，发送时派生独立下一跳身份。
     *
     * @param parent 可信调用方选择的父上下文，null 显式关闭继承。
     * @return 当前 builder，不改变消息路由或确认语义。
     */
    public PublishBuilder traceContext(KafkaTraceContext parent) {
        traceParent = parent;
        explicitTrace = true;
        return this;
    }

    /**
     * 业务作用：追加保留 null 值的业务头。
     *
     * @param name  名称
     * @param value 可空内容。
     * @return 当前 builder；框架路由头拒绝覆盖。
     */
    public PublishBuilder header(String name, byte[] value) {
        KafkaWire.validateHeaderName(name);
        if (KafkaWire.reserved(name)) throw new IllegalArgumentException("不能覆盖框架保留头");
        headers.add(name, value == null ? null : value.clone());
        return this;
    }

    /**
     * 业务作用：编码消息并交给有界 lane。
     * <p>
     * 参数说明：无。
     *
     * @return 最终交付结果，编码或准入失败可感知。
     */
    public CompletableFuture<Delivery> send() {
        return sendUntil(lane.publishDeadline());
    }

    /**
     * 业务作用：把序列化耗时计入发送总预算。
     *
     * @param deadline 总截止时间。
     * @return 准入后的最终交付结果。
     */
    private CompletableFuture<Delivery> sendUntil(long deadline) {
        // 删除标记必须指向一个确定键；零长度字节键有效，缺失键不能表达该删除责任。
        if (requireKey && key == null)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "显式 tombstone key 不能为 null");
        ConsumeContext current = ConsumeContext.current();
        KafkaTraceContext parent = explicitTrace ? traceParent : current == null ? null : current.traceContext();
        KafkaSpan trace = parent == null ? null : lane.span(topic, parent);
        try {
            byte[] bytes = raw ? (byte[]) payload : payload instanceof ProtocolBytes pb ? pb.encodeBytes() : ObjMprUtils.OBJECT_MAPPER.writeValueAsBytes(payload);
            RecordHeaders outgoing = new RecordHeaders(new KafkaHeaders(headers).entries());
            if (trace != null)
                outgoing.add(KafkaWire.TRACEPARENT, trace.context.traceparent().getBytes(StandardCharsets.US_ASCII));
            outgoing.add(KafkaProxy.HEADER_EVENT, event.clone());
            if (!raw && payload instanceof ProtocolBytes pb) {
                outgoing.add(KafkaProxy.HEADER_PAYLOAD_CODEC, KafkaProxy.PAYLOAD_CODEC_PROTOCOL_BYTES.getBytes(StandardCharsets.UTF_8));
                outgoing.add(KafkaProxy.HEADER_PAYLOAD_MODE, pb.mode().name().getBytes(StandardCharsets.UTF_8));
            }
            var result = lane.sendBytes(new ProducerRecord<>(topic, partition, timestamp, key, bytes, outgoing), deadline, trace).thenApply(lane::delivery);
            return result;
        } catch (RuntimeException failure) {
            if (trace != null) trace.finish(KafkaTraceSpan.Outcome.FAILED);
            throw failure;
        } catch (Exception failure) {
            if (trace != null) trace.finish(KafkaTraceSpan.Outcome.FAILED);
            throw new KafkaFailure(KafkaFailure.Code.PUBLISH, "消息编码失败", failure);
        }
    }

    /**
     * 业务作用：只等待本地准入，最终交付仍由 lane 统计承担。
     * <p>
     * 参数说明：无。
     * 返回：准入失败抛出异常，返回不代表 broker 确认。
     */
    public void fire() {
        send();
    }

    /**
     * 业务作用：在调用方期限内等待交付。
     *
     * @param timeout 等待预算。
     * @return 交付结果；超时和中断分类为未知。
     */
    public Delivery sendAndWait(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative())
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "发布等待预算必须为正");
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            return ProducerLane.await(sendUntil(Math.min(deadline, lane.publishDeadline())), deadline);
        } catch (KafkaFailure failure) {
            if (failure.code() == KafkaFailure.Code.OUTCOME_UNKNOWN) lane.waitUnknown();
            throw failure;
        }
    }
}

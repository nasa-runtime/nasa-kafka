package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.observability.trace.KafkaTraceContext;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.Objects;
import java.util.Optional;

/**
 * 同步回调内的原始只读记录。运行时拒绝跨线程及回调结束后的访问。
 * 不暴露底层 byte[]/ByteBuffer；显式复制后的字节由调用方持有并承担后续容量责任。
 */
public final class RawKafkaRecord {

    private final Thread owner = Thread.currentThread();
    private ConsumerRecord<byte[], byte[]> record;
    private Header[] headers;
    private AckToken ack;
    private ConsumeContext context;

    /**
     * 业务作用：为一次同步调用建立只读访问域。
     * <p>
     * 返回：未复制 key/value/头值的视图。
     *
     * @param record  框架持有原文
     * @param ack     本次确认权
     * @param context 消费身份。
     */
    RawKafkaRecord(ConsumerRecord<byte[], byte[]> record, AckToken ack, ConsumeContext context) {
        this.record = Objects.requireNonNull(record);
        this.headers = record.headers().toArray();
        this.ack = ack;
        this.context = context;
    }

    /**
     * 业务作用：禁止原生缓冲逃逸到其它线程或已结束调用。
     * <p>
     * 参数说明：无。
     * 返回：有效时继续，否则拒绝读取。
     */
    private void check() {
        // 先拒绝其它线程，避免其与当前回调的释放动作竞争底层引用。
        if (Thread.currentThread() != owner || record == null)
            throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "原始视图只在当前同步回调内有效");
    }

    /**
     * 业务作用：读取来源主题。
     * <p>
     * 参数说明：无。
     *
     * @return 当前有效记录的主题。
     */
    public String topic() {
        check();
        return record.topic();
    }

    /**
     * 业务作用：读取来源分区。
     * <p>
     * 参数说明：无。
     *
     * @return 当前有效记录的分区号。
     */
    public int partition() {
        check();
        return record.partition();
    }

    /**
     * 业务作用：读取来源位点以构造业务幂等身份。
     * <p>
     * 参数说明：无。
     *
     * @return 当前记录的 offset。
     */
    public long offset() {
        check();
        return record.offset();
    }

    /**
     * 业务作用：读取 Kafka 消息时间。
     * <p>
     * 参数说明：无。
     *
     * @return 毫秒时间戳，无时间戳时保留原生值。
     */
    public long timestamp() {
        check();
        return record.timestamp();
    }

    /**
     * 业务作用：取得来源和实际尝试身份。
     * <p>
     * 参数说明：无。
     *
     * @return 本次调用上下文；不授予其它记录确认权。
     */
    public ConsumeContext context() {
        check();
        return context;
    }

    /**
     * 业务作用：在有效借用域内解析入站父trace，固定身份可以显式跨执行器携带。
     * <p>
     * 参数说明：无。
     *
     * @return 有效上下文或空；非法观测不改变业务消息结论。
     */
    public Optional<KafkaTraceContext> traceContext() {
        check();
        return Optional.ofNullable(KafkaTraceContext.extract(record.headers()).context());
    }

    /**
     * 业务作用：只读访问原始 key。
     * <p>
     * 参数说明：无。
     *
     * @return null key 返回 null，空 key 返回长度零视图。
     */
    public Bytes key() {
        check();
        return record.key() == null ? null : new Bytes(-1);
    }

    /**
     * 业务作用：只读访问原始载荷。
     * <p>
     * 参数说明：无。
     *
     * @return tombstone 返回 null，空消息返回长度零视图。
     */
    public Bytes payload() {
        check();
        return record.value() == null ? null : new Bytes(-2);
    }

    /**
     * 业务作用：获取按 wire 顺序的头数量。
     * <p>
     * 参数说明：无。
     *
     * @return 包含重复和 null 值条目的总数。
     */
    public int headerCount() {
        check();
        return headers.length;
    }

    /**
     * 业务作用：读取指定位置的精确头名。
     *
     * @param index wire 顺序下标。
     * @return 区分大小写的头名；越界拒绝。
     */
    public String headerName(int index) {
        check();
        return headers[index].key();
    }

    /**
     * 业务作用：只读访问指定头值。
     *
     * @param index wire 顺序下标。
     * @return 保留 null 与空字节区别的视图；越界拒绝。
     */
    public Bytes headerValue(int index) {
        check();
        return headers[index].value() == null ? null : new Bytes(index);
    }

    /**
     * 业务作用：按 Manual 合同临时确认本条输入。
     * <p>
     * 参数说明：无。
     * 返回：有效时记录确认；超时、停止、失权或非 Manual 拒绝。
     */
    public void ack() {
        check();
        ack.ack();
    }

    /**
     * 业务作用：关闭访问域并解除对原文的持有。
     * <p>
     * 参数说明：无。
     * 返回：任何保留的记录或字节视图均不能继续读取。
     */
    void invalidate() {
        check();
        record = null;
        headers = null;
        ack = null;
        context = null;
    }

    /**
     * 不持有独立原文字节；每次读取都复验所属回调，因此无法通过保留子视图绕过生命周期。
     */
    public final class Bytes {
        private final int selector;

        /**
         * 业务作用：保存原文字段定位而不延长缓冲所有权。
         * <p>
         * 返回：依赖父记录访问域的字节视图。
         *
         * @param selector key/payload 或头下标。
         */
        private Bytes(int selector) {
            this.selector = selector;
        }

        /**
         * 业务作用：仅向内部读取步骤提供当前原文。
         * <p>
         * 参数说明：无。
         *
         * @return 通过访问域检查的底层值，绝不公开返回给业务。
         */
        private byte[] bytes() {
            check();
            return selector == -1 ? record.key() : selector == -2 ? record.value() : headers[selector].value();
        }

        /**
         * 业务作用：读取原文字节数以执行有界准入。
         * <p>
         * 参数说明：无。
         *
         * @return 有效视图的长度。
         */
        public int length() {
            return bytes().length;
        }

        /**
         * 业务作用：进行不复制原文的有界解析。
         *
         * @param index 字节下标。
         * @return 原始字节，越界或视图过期拒绝。
         */
        public byte get(int index) {
            return bytes()[index];
        }

        /**
         * 业务作用：显式取得可跨回调持有的独立内容。
         * <p>
         * 参数说明：无。
         *
         * @return 业务拥有的副本，其后续保存与出站额度由调用方管理。
         */
        public byte[] copy() {
            return bytes().clone();
        }

        /**
         * 业务作用：把指定片段复制到业务已取得额度的缓冲。
         * <p>
         * 返回：完成复制，非法范围或过期视图拒绝。
         *
         * @param sourceOffset 原文起点
         * @param target       业务缓冲
         * @param targetOffset 目标起点
         * @param length       长度。
         */
        public void copyTo(int sourceOffset, byte[] target, int targetOffset, int length) {
            System.arraycopy(bytes(), sourceOffset, target, targetOffset, length);
        }
    }
}

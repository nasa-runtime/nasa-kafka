package io.github.nasaruntime.kafka.protocol;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.util.*;
import java.nio.charset.StandardCharsets;

/**
 * 保留全局顺序、重复名称及 null 值的不可变消息头快照。
 */
public final class KafkaHeaders {

    private final List<Header> entries;

    /**
     * 业务作用：取得独立头快照，防止异步期间被调用方修改。
     * <p>
     * 返回：拥有独立字节的快照。
     *
     * @param headers 原始顺序。
     */
    public KafkaHeaders(Iterable<Header> headers) {
        List<Header> copy = new ArrayList<>();
        if (headers != null) for (Header header : headers) copy.add(new OwnedHeader(header.key(), header.value()));
        entries = List.copyOf(copy);
    }

    /**
     * 业务作用：派生可信框架字段并复用已经隔离的不可变消息头。
     * <p>
     * 返回：原快照不变的新顺序集合。
     *
     * @param original 原快照
     * @param name     准备写入或验证的消息头名称。
     * @param value    该头对应的原始字节值，可保留null。
     */
    private KafkaHeaders(KafkaHeaders original, String name, byte[] value) {
        List<Header> updated = new ArrayList<>();
        for (Header entry : original.entries)
            if (!KafkaWire.asciiEqualsIgnoreCase(entry.key(), name)) updated.add(entry);
        updated.add(new OwnedHeader(name, value));
        entries = List.copyOf(updated);
    }

    /**
     * 业务作用：发布唯一可信框架字段并共享其它不可变消息头。
     *
     * @param name  框架字段
     * @param value 可信值。
     * @return 删除大小写等价旧字段的新集合，业务原文不重复复制。
     */
    KafkaHeaders withFrameworkValue(String name, byte[] value) {
        return new KafkaHeaders(this, name, value);
    }

    /**
     * 业务作用：读取保留重复项的完整顺序。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变序列，各值读取均取得副本。
     */
    public List<Header> entries() {
        return entries;
    }

    /**
     * 业务作用：生成独立原生头容器供一次发送使用。
     * <p>
     * 参数说明：无。
     *
     * @return 调用方拥有的原生容器。
     */
    public Headers toNative() {
        RecordHeaders out = new RecordHeaders();
        entries.forEach(h -> out.add(h.key(), h.value()));
        return out;
    }

    /**
     * 业务作用：提供旧 API 的按名称聚合视图。
     * <p>
     * 参数说明：无。
     *
     * @return 不丢 null 值且值字节与原快照隔离的只读映射；跨名称全局顺序请使用 entries。
     */
    public Map<String, List<byte[]>> asMap() {
        if (entries.isEmpty()) return Map.of();
        if (entries.size() == 1) {
            Header entry = entries.getFirst();
            return Map.of(entry.key(), Collections.singletonList(entry.value()));
        }
        Map<String, List<byte[]>> out = new LinkedHashMap<>();
        for (Header entry : entries) out.computeIfAbsent(entry.key(), ignored -> new ArrayList<>()).add(entry.value());
        out.replaceAll((key, values) -> Collections.unmodifiableList(values));
        return Collections.unmodifiableMap(out);
    }

    /**
     * 业务作用：计算准入应承担的头字节。
     * <p>
     * 参数说明：无。
     *
     * @return 名称 UTF-8 与非空值的总长度。
     */
    public long byteSize() {
        return entries.stream().mapToLong(h -> (long) h.key().getBytes(StandardCharsets.UTF_8).length + (((OwnedHeader) h).value == null ? 0 : ((OwnedHeader) h).value.length)).sum();
    }

    private static final class OwnedHeader implements Header {
        private final String key;
        private final byte[] value;

        /**
         * 业务作用：持有头的独立内容。
         * <p>
         * 返回：独立头。
         *
         * @param key   非 null 的消息头名称，此内部快照保留入站原名，不执行输出头名校验。
         * @param value 可为 null 的原始头值；非 null 时复制字节，隔离外部修改。
         */
        OwnedHeader(String key, byte[] value) {
            this.key = Objects.requireNonNull(key);
            this.value = value == null ? null : value.clone();
        }

        /**
         * 业务作用：返回精确头名。
         * <p>
         * 参数说明：无。
         *
         * @return 区分大小写的名称。
         */
        @Override
        public String key() {
            return key;
        }

        /**
         * 业务作用：防止外部改写发送快照。
         * <p>
         * 参数说明：无。
         *
         * @return 值副本或 null。
         */
        @Override
        public byte[] value() {
            return value == null ? null : value.clone();
        }
    }
}

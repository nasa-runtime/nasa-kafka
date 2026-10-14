package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.observability.trace.KafkaTraceContext;

import io.github.nasaruntime.kafka.exception.KafkaFailure;
import io.github.nasaruntime.kafka.protocol.KafkaHeaders;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * 记录作用 - 对外暴露的 Kafka 消费记录, 包含业务对象和 Kafka 元数据。
 *
 * @param topic          来源 topic
 * @param partition      来源分区
 * @param offset         记录偏移量
 * @param timestamp      记录时间戳, 单位毫秒
 * @param key            原始 key 的合法 UTF-8 便利视图，null 或非 UTF-8 时为 null，精确身份使用 rawKey
 * @param value          反序列化后的业务对象
 * @param headers        Kafka record headers, 同名 header 保留多个值, value 为原始 bytes
 * @param ackToken       当前调用的临时确认令牌，null 表示没有受管确认权限
 * @param orderedHeaders 保留跨名称顺序和重复项的头快照；提供时优先于 headers
 * @param rawKey         原始 key 字节快照，null 与空数组不同；构造时不核对其与 key 文本是否一致
 * @param context        可空的显式消费上下文，不授予其它记录的确认权
 * @param <T>            业务对象类型
 */
public record KafkaRecord<T>(String topic, int partition, long offset, long timestamp, String key, T value,
                             Map<String, List<byte[]>> headers, AckToken ackToken, KafkaHeaders orderedHeaders,
                             byte[] rawKey, ConsumeContext context) {

    /**
     * 业务作用：兼容不携带显式上下文的记录构造。
     * <p>
     * 返回：上下文为空的记录，不能推测其它源。
     *
     * @param topic          实际来源topic名称。
     * @param partition      记录所属的来源分区号。
     * @param offset         该记录在来源分区内的offset，不是下一条提交位置。
     * @param timestamp      原始Kafka记录时间戳，单位毫秒。
     * @param key            原始key的合法UTF-8便利视图，精确字节身份使用rawKey。
     * @param value          反序列化后的业务对象，不包含框架确认权限。
     * @param headers        兼容消息头Map；未提供orderedHeaders时据此建立有序头快照。
     * @param ackToken       框架为当前调用授予的确认令牌；null表示无受管确认权限。
     * @param orderedHeaders 保留跨名称顺序、重复项和null值的规范消息头；null时由headers构造。
     * @param rawKey         原始key字节，null与空数组不同，构造时复制以隔离外部修改。
     */
    public KafkaRecord(String topic, int partition, long offset, long timestamp, String key, T value,
                       Map<String, List<byte[]>> headers, AckToken ackToken, KafkaHeaders orderedHeaders, byte[] rawKey) {
        this(topic, partition, offset, timestamp, key, value, headers, ackToken, orderedHeaders, rawKey, null);
    }

    /**
     * 业务作用：兼容无确认权威的外部记录构造。
     * <p>
     * 返回：不能用于任意位点确认的记录。
     *
     * @param topic     实际来源topic名称。
     * @param partition 记录所属的来源分区号。
     * @param offset    该记录在来源分区内的offset，不是下一条提交位置。
     * @param timestamp 原始Kafka记录时间戳，单位毫秒。
     * @param key       调用方提供的文本 key；非 null 时按 UTF-8 派生 rawKey，此重载无法表达非 UTF-8 原始 key。
     * @param value     反序列化后的业务对象，不包含框架确认权限。
     * @param headers   兼容消息头 Map，构造时建立有序快照；不同头名之间只保留输入 Map 的遍历顺序。
     */
    public KafkaRecord(String topic, int partition, long offset, long timestamp, String key, T value, Map<String, List<byte[]>> headers) {
        this(topic, partition, offset, timestamp, key, value, headers, null, null, key == null ? null : key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 业务作用：复制原始 key 并固定有序头，隔离调用方对输入字节与头集合的后续修改。
     * 业务对象 value 不做深拷贝；文本 key 与 rawKey 分别保存，不在构造时验证两者一致。
     * <p>
     * 返回：key 字节及消息头有独立快照的记录；value 和上下文保持调用方提供的引用。
     *
     * @param topic          实际来源topic名称。
     * @param partition      记录所属的来源分区号。
     * @param offset         该记录在来源分区内的offset，不是下一条提交位置。
     * @param timestamp      原始Kafka记录时间戳，单位毫秒。
     * @param key            原始key的合法UTF-8便利视图，精确字节身份使用rawKey。
     * @param value          反序列化后的业务对象，不包含框架确认权限。
     * @param headers        兼容消息头Map；未提供orderedHeaders时据此建立有序头快照。
     * @param ackToken       框架为当前调用授予的确认令牌；null表示无受管确认权限。
     * @param orderedHeaders 保留跨名称顺序、重复项和null值的规范消息头；null时由headers构造。
     * @param rawKey         原始key字节，null与空数组不同，构造时复制以隔离外部修改。
     * @param context        当前记录的显式消费身份；可为null，不据此猜测其它数据源。
     */
    public KafkaRecord {
        rawKey = rawKey == null ? null : rawKey.clone();
        if (orderedHeaders == null) {
            RecordHeaders nativeHeaders = new RecordHeaders();
            if (headers != null)
                headers.forEach((name, values) -> values.forEach(bytes -> nativeHeaders.add(name, bytes)));
            orderedHeaders = new KafkaHeaders(nativeHeaders);
        }
        headers = orderedHeaders.asMap();
    }

    /**
     * 业务作用：提供兼容 Map 视图并隔离调用方对字节的修改。
     * <p>
     * 参数说明：无。
     *
     * @return 独立字节副本，精确跨名顺序使用 orderedHeaders。
     */
    @Override
    public Map<String, List<byte[]>> headers() {
        return orderedHeaders.asMap();
    }

    /**
     * 业务作用：读取本记录唯一规范的入站父trace，批处理的调用根与链接另由ConsumeContext提供。
     * <p>
     * 参数说明：无。
     *
     * @return 有效父上下文或空；不赋予确认权限。
     */
    public Optional<KafkaTraceContext> traceContext() {
        return Optional.ofNullable(KafkaTraceContext.extract(orderedHeaders.entries()).context());
    }

    /**
     * 业务作用：读取可能不是 UTF-8 的 key。
     * <p>
     * 参数说明：无。
     *
     * @return 独立字节副本，null 与空数组不同。
     */
    @Override
    public byte[] rawKey() {
        return rawKey == null ? null : rawKey.clone();
    }

    /**
     * 业务作用：在当前 Manual 调用内临时确认本记录。
     * <p>
     * 参数说明：无。
     * 返回：有效时幂等，非框架记录或过期确认失败。
     */
    public void ack() {
        if (ackToken == null) throw new KafkaFailure(KafkaFailure.Code.ACK_EXPIRED, "记录没有确认权限");
        ackToken.ack();
    }

    /**
     * 业务作用：构造不带 headers 的消费记录，用于消息本身没有附加头信息的场景。
     * headers 置为不可变空映射而非 null，使消费方读取 header 时无需先判空。
     * 返回：无头信息且不携带受管确认权威的记录。
     *
     * @param topic     来源 topic
     * @param partition 来源分区
     * @param offset    记录在分区内的偏移量
     * @param timestamp 记录时间戳，单位毫秒
     * @param key       Kafka record key，可为 null
     * @param value     反序列化后的业务对象
     */
    public KafkaRecord(String topic, int partition, long offset, long timestamp, String key, T value) {
        this(topic, partition, offset, timestamp, key, value, Map.of());
    }

    /**
     * 业务作用：从本记录的来源坐标构造原生分区标识，供聚合、查询或受管控制选定目标。
     * 该坐标不授予提交权威，不能绕过 owner 直接提交消费位点。
     * <p>
     * 参数说明：无。
     *
     * @return 由本记录的 topic 与 partition 组成的分区标识。
     */
    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }
}

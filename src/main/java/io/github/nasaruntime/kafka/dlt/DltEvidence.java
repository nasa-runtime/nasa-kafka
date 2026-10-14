package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.protocol.KafkaWire;

import java.nio.*;
import java.nio.charset.*;
import java.util.*;

import org.apache.kafka.common.header.Header;

/**
 * 消息自带的 DLT 来源描述。格式验证不授予访问权，真实投递来源还须绑定读取计划与部署 ACL。
 * originOffset 是原输入坐标；回投产生的新 offset 与它不同，证据也不包含原 topic 的物理代次。
 */
public record DltEvidence(String originTopic, int originPartition, long originOffset,
                          String originGroup, String reason, long deliveryAttempts, DltOrigin origin) {

    public static final int MAX_HEADER_BYTES = 65536;
    public static final int MAX_HEADERS = 256;
    private static final List<String> FIELDS = List.of(KafkaProxy.HEADER_DLT_ORIGIN_TOPIC,
            KafkaProxy.HEADER_DLT_ORIGIN_PARTITION, KafkaProxy.HEADER_DLT_ORIGIN_OFFSET,
            KafkaWire.DLT_GROUP, KafkaProxy.HEADER_DLT_REASON, KafkaWire.DLT_ATTEMPTS);

    /**
     * 业务作用：保留没有源和集群扩展的历史证据构造入口。
     * <p>
     * 返回：origin为空的合法证据，不推测缺失身份。
     *
     * @param originTopic      死信报告的原业务topic，不直接作为可信回投路由。
     * @param originPartition  原业务记录的非负来源分区号。
     * @param originOffset     原业务记录的非负来源offset。
     * @param originGroup      产生该死信的原消费组身份。
     * @param reason           安全失败原因，UTF-8字节长度最多1024。
     * @param deliveryAttempts 原失败流程中的投递次数，范围1至4294967295。
     */
    public DltEvidence(String originTopic, int originPartition, long originOffset, String originGroup, String reason, long deliveryAttempts) {
        this(originTopic, originPartition, originOffset, originGroup, reason, deliveryAttempts, null);
    }

    /**
     * 业务作用：建立有界且不混淆当前消费次数的 DLT 来源描述。
     * <p>
     * 返回：合法不可变描述；不查询 Kafka，也不认证来源字段。
     *
     * @param originTopic      死信报告的原业务topic，不直接作为可信回投路由。
     * @param originPartition  原业务记录的非负来源分区号。
     * @param originOffset     原业务记录的非负来源offset。
     * @param originGroup      产生该死信的原消费组身份。
     * @param reason           安全失败原因，UTF-8字节长度最多1024。
     * @param deliveryAttempts 原失败流程中的投递次数，范围1至4294967295。
     * @param origin           可空的原命名源及物理集群扩展证据。
     */
    public DltEvidence {
        if (originTopic == null || !originTopic.matches("[A-Za-z0-9._-]{1,249}") || originTopic.equals(".") || originTopic.equals("..")
                || originPartition < 0 || originOffset < 0 || deliveryAttempts < 1 || deliveryAttempts > 0xffff_ffffL)
            invalid();
        boundedText(originGroup, 1024, false);
        boundedText(reason, 1024, true);
    }

    /**
     * 业务作用：在重放前拒绝缺项、重复、别名和非规范数字，不能将半份证据当作普通记录跳过。
     *
     * @param headers 完整有序消息头。
     * @return 完全没有 DLT 字段为空；六字段完整合法才返回来源，任何不可信结构均为 HEADER_INVALID。
     */
    public static Optional<DltEvidence> read(KafkaHeaders headers) {
        if (headers == null || headers.entries().size() > MAX_HEADERS || headers.byteSize() > MAX_HEADER_BYTES)
            malformed();
        Map<String, byte[]> values = new HashMap<>();
        for (Header header : headers.entries())
            for (String field : FIELDS) {
                if (!KafkaWire.asciiEqualsIgnoreCase(header.key(), field)) continue;
                // 别名或重复项不能通过“取最后一项”改变来源，必须在路由选择前拒绝整个证据。
                if (!header.key().equals(field) || header.value() == null || values.containsKey(field)) malformed();
                values.put(field, header.value());
            }
        Optional<DltOrigin> origin = DltOrigin.read(headers);
        if (values.isEmpty()) {
            if (origin.isPresent()) malformed();
            return Optional.empty();
        }
        if (values.size() != FIELDS.size()) malformed();
        boolean currentAttempt = false;
        for (Header header : headers.entries())
            if (KafkaWire.asciiEqualsIgnoreCase(header.key(), KafkaWire.DELIVERY_ATTEMPT)) {
                if (currentAttempt || !header.key().equals(KafkaWire.DELIVERY_ATTEMPT) || header.value() == null)
                    malformed();
                decimal(header.value(), 0xffff_ffffL, false);
                currentAttempt = true;
            }
        try {
            // 当前 owner 会重写 DELIVERY_ATTEMPT；它不参与原 DLT 次数的相等判断。
            return Optional.of(new DltEvidence(text(values.get(FIELDS.get(0)), 249, false),
                    (int) decimal(values.get(FIELDS.get(1)), Integer.MAX_VALUE, true),
                    decimal(values.get(FIELDS.get(2)), Long.MAX_VALUE, true),
                    text(values.get(FIELDS.get(3)), 1024, false), text(values.get(FIELDS.get(4)), 1024, true),
                    decimal(values.get(FIELDS.get(5)), 0xffff_ffffL, false), origin.orElse(null)));
        } catch (DltReplayException rejected) {
            throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
        }
    }

    /**
     * 业务作用：验证配置文本确实具有唯一 UTF-8 表达，不允许孤立 surrogate 被静默替换。
     * <p>
     * 返回：合法文本通过；编码歧义或越界按配置拒绝。
     *
     * @param value 文本
     * @param max   UTF-8 字节上限
     * @param empty true允许空文本，false要求至少一个UTF-8字节；两者都拒绝null。
     */
    public static void boundedText(String value, int max, boolean empty) {
        if (value == null || !empty && value.isEmpty()) invalid();
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
            if (encoded.remaining() > max) invalid();
        } catch (CharacterCodingException rejected) {
            invalid();
        }
    }

    /**
     * 业务作用：对线上的有界证据使用严格 UTF-8，不用替代字符构造新身份。
     *
     * @param bytes 待解码的原始线路字段，不能为null。
     * @param max   原始字段的最大字节数。
     * @param empty true允许零字节字段，false要求至少一个字节。
     * @return 唯一文本；截断或非法 UTF-8 拒绝。
     */
    private static String text(byte[] bytes, int max, boolean empty) {
        if (bytes == null || bytes.length > max || !empty && bytes.length == 0) malformed();
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException rejected) {
            throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
        }
    }

    /**
     * 业务作用：避免带符号、前导零、非 ASCII 数字或溢出改变来源坐标。
     *
     * @param bytes 不含符号、前导零和空白的ASCII十进制字段。
     * @param max   该业务数值允许的最大值。
     * @param zero  true允许零，false要求严格正数。
     * @return 规范非负十进制值，不接受空白或隐式截断。
     */
    private static long decimal(byte[] bytes, long max, boolean zero) {
        if (bytes == null || bytes.length == 0 || bytes.length > 19 || bytes.length > 1 && bytes[0] == '0') malformed();
        long value = 0;
        for (byte digit : bytes) {
            if (digit < '0' || digit > '9' || value > (max - (digit - '0')) / 10) malformed();
            value = value * 10 + digit - '0';
        }
        if (!zero && value == 0) malformed();
        return value;
    }

    /**
     * 业务作用：拒绝业务构造的不完整来源声明。
     * <p>
     * 参数说明：无。
     * 返回：固定配置异常。
     */
    private static void invalid() {
        throw new DltReplayException(DltReplayException.Code.INVALID_CONFIGURATION);
    }

    /**
     * 业务作用：拒绝不能作为回投依据的线路证据。
     * <p>
     * 参数说明：无。
     * 返回：固定脱敏字段异常。
     */
    private static void malformed() {
        throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
    }
}

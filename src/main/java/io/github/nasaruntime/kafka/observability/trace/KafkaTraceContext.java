package io.github.nasaruntime.kafka.observability.trace;

import io.github.nasaruntime.kafka.protocol.KafkaWire;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.common.header.Header;

/**
 * 不依赖监控 SDK 的不可变 W3C traceparent；当前仅接纳 version 00，不承载授权。
 */
public record KafkaTraceContext(String traceId, String spanId, int flags) {

    /**
     * 业务作用：拒绝非规范或全零身份，避免传播无法关联的 trace。
     * <p>
     * 返回：有效上下文；非法声明立即拒绝。
     *
     * @param traceId 32位小写十六进制trace身份，不能全零。
     * @param spanId  16位小写十六进制span身份，不能全零。
     * @param flags   无符号字节。
     */
    public KafkaTraceContext {
        if (!hex(traceId, 32) || !hex(spanId, 16) || flags < 0 || flags > 255)
            throw new IllegalArgumentException("trace context 无效");
    }

    /**
     * 业务作用：校验固定长度非零十六进制身份。
     *
     * @param value  输入
     * @param length 协议长度。
     * @return 是否符合规范。
     */
    private static boolean hex(String value, int length) {
        if (value == null || value.length() != length) return false;
        boolean nonzero = false;
        for (int index = 0; index < length; index++) {
            int digit = hexDigit(value.charAt(index));
            if (digit < 0) return false;
            nonzero |= digit != 0;
        }
        return nonzero;
    }

    /**
     * 业务作用：将线路允许的小写十六进制字符还原为数值，拒绝大写及其它 Unicode 数字。
     *
     * @param value 单个协议字符。
     * @return 0至15，非法字符返回 -1。
     */
    private static int hexDigit(char value) {
        return value >= '0' && value <= '9' ? value - '0' : value >= 'a' && value <= 'f' ? value - 'a' + 10 : -1;
    }

    /**
     * 业务作用：读取已采样标志而不把其它位解释为授权或业务策略。
     * <p>
     * 参数说明：无。
     *
     * @return 最低位是否置位。
     */
    public boolean sampled() {
        return (flags & 1) != 0;
    }

    /**
     * 业务作用：输出唯一规范的线路形式。
     * <p>
     * 参数说明：无。
     *
     * @return version 00 的固定长度 traceparent。
     */
    public String traceparent() {
        return "00-" + traceId + "-" + spanId + "-" + HexFormat.of().toHexDigits((byte) flags);
    }

    /**
     * 业务作用：保守读取受支持的外来 trace，非法观测不改变业务消息有效性。
     *
     * @param value 线路字符串。
     * @return 合法上下文或空，不抛出输入格式错误。
     */
    public static Optional<KafkaTraceContext> parse(String value) {
        if (value == null || value.length() != 55 || !value.startsWith("00-") || value.charAt(35) != '-' || value.charAt(52) != '-')
            return Optional.empty();
        int high = hexDigit(value.charAt(53)), low = hexDigit(value.charAt(54));
        if (high < 0 || low < 0) return Optional.empty();
        try {
            return Optional.of(new KafkaTraceContext(value.substring(3, 35), value.substring(36, 52), high * 16 + low));
        } catch (IllegalArgumentException invalid) {
            return Optional.empty();
        }
    }

    /**
     * 有限解析原因，禁止把原始 trace 值放入指标标签。
     */
    public enum Invalid {DUPLICATE, MALFORMED}

    /**
     * 缺失时两个字段均为空；有效上下文与非法原因不会同时出现。
     */
    public record Extraction(KafkaTraceContext context, Invalid invalid) {}

    /**
     * 业务作用：拒绝重复或非法观测头，防止在两个外来身份中任意选一个。
     *
     * @param headers 有序原始 Kafka 头，精确匹配 traceparent。
     * @return 上下文、有限失败原因或缺失；业务路由不受影响。
     */
    public static Extraction extract(Iterable<Header> headers) {
        byte[] value = null;
        int count = 0;
        for (Header h : headers)
            if (h.key().equals(KafkaWire.TRACEPARENT)) {
                count++;
                if (count > 1) return new Extraction(null, Invalid.DUPLICATE);
                value = h.value();
            }
        if (count == 0) return new Extraction(null, null);
        KafkaTraceContext context = value == null ? null : parse(new String(value, StandardCharsets.US_ASCII)).orElse(null);
        return new Extraction(context, context == null ? Invalid.MALFORMED : null);
    }

    /**
     * 业务作用：创建独立 trace 根身份，批处理不把首条消息当作整批父节点。
     *
     * @param sampled 本地采样决策。
     * @return 非零随机根上下文。
     */
    public static KafkaTraceContext root(boolean sampled) {
        return new KafkaTraceContext(randomId(16), randomSpan(), sampled ? 1 : 0);
    }

    /**
     * 业务作用：在同一 trace 下派生下一跳身份，保持上游采样位。
     * <p>
     * 参数说明：无。
     *
     * @return 独立非零 span ID 的子上下文。
     */
    public KafkaTraceContext child() {
        return new KafkaTraceContext(traceId, randomSpan(), flags);
    }

    /**
     * 业务作用：生成传播身份，不用业务 key、offset 或用户标识构造 trace。
     * <p>
     * 参数说明：无。
     *
     * @return 非零随机十六进制 span ID。
     */
    private static String randomSpan() {
        return randomId(8);
    }

    /**
     * 业务作用：按协议长度生成独立的观测身份，不携带业务字段或 UUID 格式约束。
     *
     * @param bytes 身份的字节数，内部使用16字节 trace ID 或8字节 span ID。
     * @return 非零小写十六进制随机身份，不用于认证凭证或安全随机数。
     */
    private static String randomId(int bytes) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long low;
        do {
            low = random.nextLong();
        } while (low == 0);
        String suffix = HexFormat.of().toHexDigits(low);
        return bytes == 16 ? HexFormat.of().toHexDigits(random.nextLong()) + suffix : suffix;
    }
}

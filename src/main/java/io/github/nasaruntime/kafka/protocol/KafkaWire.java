package io.github.nasaruntime.kafka.protocol;

import io.github.nasaruntime.kafka.KafkaProxy;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.dlt.DltOrigin;
import io.github.nasaruntime.kafka.dlt.DltQuarantineTrace;
import io.github.nasaruntime.kafka.dlt.DltReplayTrace;
import io.github.nasaruntime.kafka.transfer.TransferTrace;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;

/**
 * Java 与 Rust 共用的线路字段及受管证据构造规则。
 */
public final class KafkaWire {
    public static final String DELIVERY_ATTEMPT = "X-Nasa-Delivery-Attempt";
    public static final String DLT_ATTEMPTS = "X-Nasa-DLT-Delivery-Attempts";
    public static final String DLT_GROUP = "X-Nasa-Dlt-Origin-Group";
    public static final String DLT_SOURCE = "X-Nasa-DLT-Origin-Source";
    public static final String DLT_CLUSTER = "X-Nasa-DLT-Origin-Cluster";
    public static final String TRACEPARENT = "traceparent";
    private static final Set<String> EVIDENCE = Set.of(DELIVERY_ATTEMPT, DLT_ATTEMPTS, DLT_GROUP, DLT_SOURCE, DLT_CLUSTER,
            KafkaProxy.HEADER_DLT_ORIGIN_TOPIC, KafkaProxy.HEADER_DLT_ORIGIN_PARTITION,
            KafkaProxy.HEADER_DLT_ORIGIN_OFFSET, KafkaProxy.HEADER_DLT_REASON);

    /**
     * 业务作用：禁止构造无状态协议工具实例。
     * <p>
     * 参数说明：无。
     * 返回：仅用于类内构造，不建立连接或改变协议状态。
     */
    private KafkaWire() {}

    /**
     * 业务作用：把文本精确编码为线路字节，避免非法 UTF-16 被替换后改变消息或消费组身份。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param value 非 null 文本。
     * @return 严格 UTF-8；孤立代理项在外部副作用前以 CONFIG 拒绝。
     */
    public static byte[] utf8(String value) {
        try {
            var encoded = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException invalid) {
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "线路文本含不可编码字符");
        }
    }

    /**
     * 普通消费组的共同字节上限，与 DLT 来源证据的组身份容量一致。
     */
    static final int MAX_GROUP_BYTES = 1024;

    /**
     * 业务作用：在路由登记及原生请求前验证消费责任的精确组身份。
     * <p>
     * 返回：非纯空白、无 NUL、首尾不含 U+0000–0020 且可严格编码为1至1024个 UTF-8 字节时通过，否则以 CONFIG 拒绝；不裁剪或归一化。
     *
     * @param group 已解析的实际 group.id。
     */
    public static void validateGroup(String group) {
        // 空白集合与事件文本相同；NEL 和不换行空格是可区分的身份，不采用 Unicode trim 改名。
        if (blankEvent(group)) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "group 不能为空或纯空白");
        // NUL 无法穿过 native C 字符串边界；有损编码或超出来源证据容量都会破坏组位点接续。
        // Kafka 字符串配置会裁剪首尾 U+0000–0020，必须拒绝这些原文，不能把位点归属交给裁剪后的名称。
        if (group.indexOf('\0') >= 0 || group.charAt(0) <= 0x20 || group.charAt(group.length() - 1) <= 0x20
                || utf8(group).length > MAX_GROUP_BYTES)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "group 必须不含 NUL、首尾不含 U+0000–0020 且最多1024个 UTF-8 字节");
    }

    /**
     * 业务作用：统一发布、注册和消费使用的事件空白边界。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param event 事件原文，可为 null。
     * @return 缺失、空串或仅含 Character.isWhitespace 字符时为 true；不换行空格和 NEL 保留为可区分的事件身份。
     */
    public static boolean blankEvent(String event) {
        return event == null || event.isBlank();
    }

    /**
     * 业务作用：在路由身份进入发布或注册状态之前冻结精确线路字节。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param event 显式事件名。
     * @return 不裁剪、不归一化的 UTF-8；空白或非法 UTF-16 以 CONFIG 拒绝。
     */
    public static byte[] eventBytes(String event) {
        // 空白事件无法形成有效路由；有损编码则会把非法输入变成另一个有效身份，两者均不能交给 broker。
        if (blankEvent(event)) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "event 不能为空或纯空白");
        return utf8(event);
    }

    /**
     * 业务作用：限定双端能够精确保留的输出头名。
     * <p>
     * 返回：非空、无 NUL 且可精确编码时通过，否则在 native 转换前拒绝。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param name 大小写敏感名称。
     */
    public static void validateHeaderName(String name) {
        // librdkafka 以 C 字符串读取头名，内嵌 NUL 会形成另一个名称，不能留给 native 隐式处理。
        if (name == null || name.isEmpty() || name.indexOf('\0') >= 0)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "header 名必须非空且不含 NUL");
        utf8(name);
    }

    /**
     * 业务作用：阻止普通业务头覆盖路由、追踪和恢复责任字段。
     *
     * @param name 精确名称。
     * @return 是否为框架保留头。
     */
    public static boolean reserved(String name) {
        return DltOrigin.isHeader(name) || TransferTrace.isHeader(name) || DltReplayTrace.isHeader(name) || DltQuarantineTrace.isHeader(name) || EVIDENCE.contains(name) || Set.of(KafkaProxy.HEADER_EVENT, KafkaProxy.HEADER_PAYLOAD_CODEC,
                KafkaProxy.HEADER_PAYLOAD_MODE, KafkaProxy.HEADER_PASSTHROUGH, TRACEPARENT).contains(name);
    }

    /**
     * 业务作用：按协议 ASCII 大小写识别投递证据，保留 Unicode 类似字母组成的独立业务头。
     *
     * @param name 输入名。
     * @return 是否必须被可信 owner 重写。
     */
    public static boolean evidence(String name) {
        return EVIDENCE.stream().anyMatch(value -> asciiEqualsIgnoreCase(value, name));
    }

    /**
     * 业务作用：保持 Java/Rust 对保留头的共同字符边界，避免 Unicode 大小写折叠改变业务头归属。
     *
     * @param left  参与ASCII大小写无关比较的第一个名称。
     * @param right 参与ASCII大小写无关比较的第二个名称。
     * @return 仅 ASCII 字母忽略大小写，其它字符必须精确相同。
     */
    public static boolean asciiEqualsIgnoreCase(String left, String right) {
        if (left == null || right == null || left.length() != right.length()) return false;
        for (int i = 0; i < left.length(); i++) {
            char a = left.charAt(i), b = right.charAt(i);
            if (a >= 'A' && a <= 'Z') a += 'a' - 'A';
            if (b >= 'A' && b <= 'Z') b += 'a' - 'A';
            if (a != b) return false;
        }
        return true;
    }

    /**
     * 业务作用：把来源自报次数替换为 owner 的可信投递序号。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param original 原始头
     * @param attempt  当前投递次数。
     * @return 保留其它头顺序的独立集合。
     */
    public static KafkaHeaders deliveryHeaders(KafkaHeaders original, int attempt) {
        return original.withFrameworkValue(DELIVERY_ATTEMPT, Integer.toString(Math.max(1, attempt)).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 业务作用：构造无伪造重复字段的 DLT 来源记录。
     *
     * @param original  原始头
     * @param group     产生原失败记录的实际消费组。
     * @param topic     原业务记录的来源topic。
     * @param partition 原业务记录的来源分区号。
     * @param offset    原业务记录在来源分区内的offset。
     * @param reason    安全原因
     * @param attempts  实际次数。
     * @return 保持业务头顺序的新集合。
     */
    public static KafkaHeaders deadLetterHeaders(KafkaHeaders original, String group, String topic, int partition, long offset, String reason, int attempts) {
        RecordHeaders headers = new RecordHeaders();
        for (Header header : original.entries()) if (!evidence(header.key())) headers.add(header);
        add(headers, DELIVERY_ATTEMPT, Integer.toString(Math.max(1, attempts)));
        add(headers, KafkaProxy.HEADER_DLT_ORIGIN_TOPIC, topic);
        add(headers, KafkaProxy.HEADER_DLT_ORIGIN_PARTITION, Integer.toString(partition));
        add(headers, KafkaProxy.HEADER_DLT_ORIGIN_OFFSET, Long.toString(offset));
        byte[] bytes = reason.getBytes(StandardCharsets.UTF_8);
        int end = Math.min(1024, bytes.length);
        while (end < bytes.length && end > 0 && (bytes[end] & 0xc0) == 0x80) end--;
        headers.add(KafkaProxy.HEADER_DLT_REASON, Arrays.copyOf(bytes, end));
        add(headers, DLT_ATTEMPTS, Integer.toString(Math.max(1, attempts)));
        add(headers, DLT_GROUP, group);
        return new KafkaHeaders(headers);
    }

    /**
     * 业务作用：按唯一 UTF-8 形式写入受管字段。
     * <p>
     * 返回：追加一个 UTF-8 编码的受管头；不去重，调用方负责先移除旧证据。
     *
     * @param headers 接收受管字段的目标消息头集合。
     * @param name    框架确定的消息头名称。
     * @param value   非 null 的字段文本，按 UTF-8 编码为头值。
     */
    private static void add(RecordHeaders headers, String name, String value) {
        headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}

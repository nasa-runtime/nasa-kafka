package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.protocol.KafkaWire;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.util.*;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * DLT 的逻辑源和物理集群来源；头字段不授予访问权，也不证明 topic 的物理代次。
 */
public record DltOrigin(String sourceId, String clusterId) {

    /**
     * 业务作用：建立不可混淆且有界的来源身份。
     * <p>
     * 返回：合法不可变身份，非法文本拒绝。
     *
     * @param sourceId  实际消费源别名
     * @param clusterId broker 查询得到的物理身份，各为1至1024个UTF-8字节且不含NUL。
     */
    public DltOrigin {
        DltEvidence.boundedText(sourceId, 1024, false);
        DltEvidence.boundedText(clusterId, 1024, false);
        if (sourceId.indexOf('\0') >= 0 || clusterId.indexOf('\0') >= 0)
            throw new DltReplayException(DltReplayException.Code.INVALID_CONFIGURATION);
    }

    /**
     * 业务作用：替换来源自报的源和集群字段，保留其他有序业务头。
     *
     * @param headers 已构造的完整死信头。
     * @return 唯一规范来源字段的新集合；不认证传入身份。
     */
    public KafkaHeaders attach(KafkaHeaders headers) {
        List<Header> result = new ArrayList<>();
        for (Header header : headers.entries())
            if (!isHeader(header.key())) result.add(header);
        result.add(new RecordHeader(KafkaWire.DLT_SOURCE, sourceId.getBytes(StandardCharsets.UTF_8)));
        result.add(new RecordHeader(KafkaWire.DLT_CLUSTER, clusterId.getBytes(StandardCharsets.UTF_8)));
        return new KafkaHeaders(result);
    }

    /**
     * 业务作用：读取可选来源扩展，拒绝半份身份和重复别名。
     *
     * @param headers 完整记录头。
     * @return 旧记录为空；完整扩展返回身份；歧义或越界为HEADER_INVALID。
     */
    public static Optional<DltOrigin> read(KafkaHeaders headers) {
        if (headers == null || headers.entries().size() > DltEvidence.MAX_HEADERS || headers.byteSize() > DltEvidence.MAX_HEADER_BYTES)
            malformed();
        Map<String, String> fields = new HashMap<>();
        for (Header header : headers.entries()) {
            if (!isHeader(header.key())) continue;
            if ((!header.key().equals(KafkaWire.DLT_SOURCE) && !header.key().equals(KafkaWire.DLT_CLUSTER))
                    || fields.containsKey(header.key()) || header.value() == null || header.value().length > 1024)
                malformed();
            try {
                fields.put(header.key(), StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(header.value())).toString());
            } catch (CharacterCodingException invalid) {
                malformed();
            }
        }
        if (fields.isEmpty()) return Optional.empty();
        if (fields.size() != 2) malformed();
        try {
            return Optional.of(new DltOrigin(fields.get(KafkaWire.DLT_SOURCE), fields.get(KafkaWire.DLT_CLUSTER)));
        } catch (DltReplayException invalid) {
            throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
        }
    }

    /**
     * 业务作用：按共同ASCII规则识别需要重写的来源字段。
     *
     * @param name 头名。
     * @return 规范名称及其ASCII别名均为true。
     */
    public static boolean isHeader(String name) {
        return KafkaWire.asciiEqualsIgnoreCase(name, KafkaWire.DLT_SOURCE) || KafkaWire.asciiEqualsIgnoreCase(name, KafkaWire.DLT_CLUSTER);
    }

    /**
     * 业务作用：阻止不完整来源进入回投判定。
     * <p>
     * 参数说明：无。
     * 返回：固定脱敏字段异常。
     */
    private static void malformed() {
        throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
    }
}

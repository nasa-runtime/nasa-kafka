package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.replay.ReplayRecord;
import io.github.nasaruntime.kafka.transfer.TransferTrace;

import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.protocol.KafkaWire;
import io.github.nasaruntime.kafka.transfer.TransferException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * 隔离证据只声明实际读取坐标与拒绝原因，原始无效头保留供业务处置。
 */
public record DltQuarantineTrace(TransferTrace.Resource origin, int partition, long offset, long timestamp,
                                 DltReplayException.Code reason, String eventId) {

    public static final String HEADER = "x-nasa-dlt-replay-failure";

    /**
     * 业务作用：固定隔离来源，不能用无效消息自报身份替代实际坐标。
     * <p>
     * 返回：合法不可变证据，非输入错误不得标记为隔离原因。
     *
     * @param origin    本次被隔离记录实际所在的 DLT 集群、topic 及资源代次，不使用无效头中自报的原业务来源。
     * @param partition 本次实际读取的 DLT 分区号。
     * @param offset    本次实际读取的 DLT 记录 offset。
     * @param timestamp 本次 DLT 记录的 Kafka 毫秒时间戳，-1 表示没有时间戳。
     * @param reason    确定的输入拒绝类别
     * @param eventId   实际位置摘要。
     */
    public DltQuarantineTrace {
        if (origin == null || partition < 0 || offset < 0 || timestamp < -1 || !eligible(reason)
                || !TransferTrace.identity(origin, partition, offset).equals(eventId)) invalid();
    }

    /**
     * 业务作用：读取隔离处置所需的可信坐标格式，不认证原始消息。
     *
     * @param headers 完整有序头。
     * @return 无证据时为空；重复、别名和损坏拒绝。
     */
    public static Optional<DltQuarantineTrace> read(KafkaHeaders headers) {
        DltReplayTrace.checkHeaders(headers);
        byte[] value = null;
        boolean found = false;
        for (Header header : headers.entries())
            if (isHeader(header.key())) {
                if (found || !HEADER.equals(header.key()) || header.value() == null) invalid();
                found = true;
                value = header.value();
            }
        return found ? Optional.of(decode(value)) : Optional.empty();
    }

    /**
     * 业务作用：明确哪些失败可以被批准的原文隔离承接。
     *
     * @param reason 处理失败类别。
     * @return 仅确定的输入证据失败为true，交付未知或基础设施失败为false。
     */
    public static boolean eligible(DltReplayException.Code reason) {
        return reason == DltReplayException.Code.MISSING_EVIDENCE || reason == DltReplayException.Code.HEADER_INVALID || reason == DltReplayException.Code.REPLAY_LIMIT;
    }

    /**
     * 业务作用：保留全部原始输入并追加一次隔离事实，不丢弃无法解析的证据。
     *
     * @param plan   冻结来源
     * @param record 实际输入
     * @param reason 确定拒绝。
     * @return 新增唯一隔离头；已有隔离标记或容量不足时停止。
     */
    public static KafkaHeaders append(DltReplayPlan plan, ReplayRecord record, DltReplayException.Code reason) {
        DltReplayTrace.checkHeaders(record.headers());
        // 已有标记不能被覆盖，否则重复隔离会掩盖先前失败并无限增长消息头。
        if (record.headers().entries().stream().anyMatch(h -> isHeader(h.key()))) invalid();
        var origin = new TransferTrace.Resource(record.clusterId(), record.topic(), plan.replay().topicGenerations().get(record.topic()));
        var trace = new DltQuarantineTrace(origin, record.partition(), record.offset(), record.timestamp(), reason, TransferTrace.identity(origin, record.partition(), record.offset()));
        List<Header> headers = new ArrayList<>(record.headers().entries());
        headers.add(new RecordHeader(HEADER, trace.encode()));
        KafkaHeaders result = new KafkaHeaders(headers);
        DltReplayTrace.checkHeaders(result);
        return result;
    }

    /**
     * 业务作用：将隔离证据保留为框架字段。
     *
     * @param name 头名。
     * @return 规范隔离头名称或其 ASCII 大小写变体为 true，其它名称及 null 为 false。
     */
    public static boolean isHeader(String name) {
        return KafkaWire.asciiEqualsIgnoreCase(name, HEADER);
    }

    /**
     * 业务作用：生成共同隔离格式，摘要只验证完整性。
     * <p>
     * 参数说明：无。
     *
     * @return NSKDQH01正文及SHA-256。
     */
    byte[] encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.write("NSKDQH01".getBytes(StandardCharsets.US_ASCII));
            DltReplayTrace.resource(out, origin);
            out.writeInt(partition);
            out.writeLong(offset);
            out.writeLong(timestamp);
            out.writeByte(switch (reason) {
                case MISSING_EVIDENCE -> 0;
                case HEADER_INVALID -> 1;
                case REPLAY_LIMIT -> 2;
                default -> throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
            });
            out.write(HexFormat.of().parseHex(eventId));
            out.write(DltReplayTrace.digest(bytes.toByteArray()));
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("无法编码DLT隔离证据");
        }
    }

    /**
     * 业务作用：验证整个有界隔离证据，拒绝截断或非规范表达。
     *
     * @param encoded 单头值。
     * @return 合法物理坐标与拒绝类别。
     */
    private static DltQuarantineTrace decode(byte[] encoded) {
        if (encoded.length < 80 || encoded.length > DltEvidence.MAX_HEADER_BYTES) invalid();
        byte[] body = Arrays.copyOf(encoded, encoded.length - 32);
        if (!MessageDigest.isEqual(DltReplayTrace.digest(body), Arrays.copyOfRange(encoded, body.length, encoded.length)))
            invalid();
        try {
            var in = new DataInputStream(new ByteArrayInputStream(body));
            if (!Arrays.equals(in.readNBytes(8), "NSKDQH01".getBytes(StandardCharsets.US_ASCII))) invalid();
            var origin = DltReplayTrace.resource(in);
            int partition = in.readInt();
            long offset = in.readLong(), timestamp = in.readLong();
            var reason = switch (in.readUnsignedByte()) {
                case 0 -> DltReplayException.Code.MISSING_EVIDENCE;
                case 1 -> DltReplayException.Code.HEADER_INVALID;
                case 2 -> DltReplayException.Code.REPLAY_LIMIT;
                default -> throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
            };
            var trace = new DltQuarantineTrace(origin, partition, offset, timestamp, reason, HexFormat.of().formatHex(in.readNBytes(32)));
            if (in.available() != 0 || !Arrays.equals(encoded, trace.encode())) invalid();
            return trace;
        } catch (IOException | IllegalArgumentException | TransferException rejected) {
            throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
        }
    }

    /**
     * 业务作用：阻止不完整隔离证据进入业务。
     * <p>
     * 参数说明：无。
     * 返回：固定字段错误。
     */
    private static void invalid() {
        throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
    }
}

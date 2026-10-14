package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.replay.ReplayRecord;
import io.github.nasaruntime.kafka.transfer.TransferPlan;
import io.github.nasaruntime.kafka.transfer.TransferTrace;

import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.protocol.KafkaWire;
import io.github.nasaruntime.kafka.transfer.TransferException;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.security.*;
import java.util.*;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * 有界的 DLT 回投来源。完整性摘要不是认证，业务自报身份不能改变计划中的目标权限。
 */
public record DltReplayTrace(TransferTrace.Resource origin, int partition, long offset, long timestamp,
                             DltEvidence originalEvidence, TransferTrace originalTransfer, String eventId,
                             int replayCount, TransferTrace.Resource destination) {

    public static final String HEADER = "x-nasa-dlt-replay";

    /**
     * 业务作用：固定首次 DLT 读取坐标、原证据和重复身份，不把新目标 offset 当作原位置。
     * <p>
     * 返回：不可变有界证据，ID不一致或非法坐标拒绝。
     *
     * @param origin           首次回投时实际读取的 DLT 集群、topic 及资源代次，再次回投时沿用。
     * @param partition        首次回投输入所在的 DLT 分区号，不是原业务分区号。
     * @param offset           首次回投输入在 DLT 分区中的 offset，不是原业务 offset。
     * @param timestamp        首次回投输入的 Kafka 毫秒时间戳，-1 表示没有时间戳。
     * @param originalEvidence 该记录的来源报告
     * @param originalTransfer 可空的既有转发身份
     * @param eventId          原转发ID或首次DLT坐标摘要
     * @param replayCount      1至16
     * @param destination      最近批准目标。
     */
    public DltReplayTrace {
        if (origin == null || destination == null || originalEvidence == null || partition < 0 || offset < 0 || timestamp < -1
                || replayCount < 1 || replayCount > 16) invalid();
        String expected = originalTransfer == null ? TransferTrace.identity(origin, partition, offset) : originalTransfer.eventId();
        if (!expected.equals(eventId)) invalid();
    }

    /**
     * 业务作用：从回投记录提取稳定来源和回投计数，不以字段授予路由或写权限。
     *
     * @param headers 完整有序消息头。
     * @return 普通消息为空；重复、ASCII别名、损坏及超额证据拒绝。
     */
    public static Optional<DltReplayTrace> read(KafkaHeaders headers) {
        checkHeaders(headers);
        byte[] bytes = null;
        boolean found = false;
        for (Header header : headers.entries())
            if (isHeader(header.key())) {
                if (found || !header.key().equals(HEADER) || header.value() == null) invalid();
                found = true;
                bytes = header.value();
            }
        return found ? Optional.of(decode(bytes)) : Optional.empty();
    }

    /**
     * 业务作用：保留原事件ID并限制再次失败后的回投次数，不松动普通 Transfer 的回环合同。
     *
     * @param plan     批准计划
     * @param record   实际DLT输入
     * @param evidence 当前 DLT 头中通过格式校验的原业务来源报告；不是实际 DLT 存储坐标或写入者认证。
     * @param target   显式映射分区。
     * @return 去除旧活动转发头后附加唯一回投证据的完整业务头，旧转发身份保存在证据内。
     */
    public static KafkaHeaders forward(DltReplayPlan plan, ReplayRecord record, DltEvidence evidence, TransferPlan.Partition target) {
        checkHeaders(record.headers());
        Optional<DltReplayTrace> previous = read(record.headers());
        TransferTrace transferred;
        try {
            transferred = TransferTrace.read(record.headers()).orElse(null);
        } catch (TransferException rejected) {
            throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
        }
        TransferTrace.Resource destination = new TransferTrace.Resource(plan.target().clusterId(), target.topic(), plan.target().topicGenerations().get(target.topic()));
        DltReplayTrace next;
        if (previous.isPresent()) {
            DltReplayTrace old = previous.get();
            // 回投原业务 topic 是显式动作，以次数门禁约束反复失败；不能通过新任务名清零计数。
            if (old.replayCount() >= plan.maxReplays())
                throw new DltReplayException(DltReplayException.Code.REPLAY_LIMIT);
            next = new DltReplayTrace(old.origin(), old.partition(), old.offset(), old.timestamp(), old.originalEvidence(), old.originalTransfer(), old.eventId(), old.replayCount() + 1, destination);
        } else {
            TransferTrace.Resource origin = new TransferTrace.Resource(record.clusterId(), record.topic(), plan.replay().topicGenerations().get(record.topic()));
            String eventId = transferred == null ? TransferTrace.identity(origin, record.partition(), record.offset()) : transferred.eventId();
            next = new DltReplayTrace(origin, record.partition(), record.offset(), record.timestamp(), evidence, transferred, eventId, 1, destination);
        }
        // DLT 回投会离开原 Transfer 路径，旧头不能继续伪装成当前位置；其稳定身份已封装到回投证据。
        List<Header> outgoing = new ArrayList<>();
        for (Header header : record.headers().entries())
            if (!isHeader(header.key()) && !TransferTrace.isHeader(header.key())) outgoing.add(header);
        outgoing.add(new RecordHeader(HEADER, next.encode()));
        KafkaHeaders result = new KafkaHeaders(outgoing);
        checkHeaders(result);
        return result;
    }

    /**
     * 业务作用：为框架保留回投证据字段，保持两端共同的ASCII边界。
     *
     * @param name 头名。
     * @return 规范名称或其ASCII大小写变体为true。
     */
    public static boolean isHeader(String name) {
        return KafkaWire.asciiEqualsIgnoreCase(name, HEADER);
    }

    /**
     * 业务作用：约束证据及业务头的总驻留规模。
     * <p>
     * 返回：最多256项、合计64KiB，否则拒绝生成回投输出。
     *
     * @param headers 完整头集合。
     */
    static void checkHeaders(KafkaHeaders headers) {
        if (headers == null || headers.entries().size() > DltEvidence.MAX_HEADERS || headers.byteSize() > DltEvidence.MAX_HEADER_BYTES)
            invalid();
    }

    /**
     * 业务作用：以确定字节序保存稳定来源和回投次数，不包含当前目标offset。
     * <p>
     * 参数说明：无。
     *
     * @return 无来源扩展时使用NSKDRH01，有扩展时使用NSKDRH02；正文附加SHA-256，单头最大64KiB。
     */
    byte[] encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.write((originalEvidence.origin() == null ? "NSKDRH01" : "NSKDRH02").getBytes(StandardCharsets.US_ASCII));
            resource(out, origin);
            out.writeInt(partition);
            out.writeLong(offset);
            out.writeLong(timestamp);
            text(out, originalEvidence.originTopic());
            out.writeInt(originalEvidence.originPartition());
            out.writeLong(originalEvidence.originOffset());
            text(out, originalEvidence.originGroup());
            text(out, originalEvidence.reason());
            out.writeInt((int) originalEvidence.deliveryAttempts());
            if (originalEvidence.origin() != null) {
                text(out, originalEvidence.origin().sourceId());
                text(out, originalEvidence.origin().clusterId());
            }
            byte[] transfer = originalTransfer == null ? new byte[0] : originalTransfer.encode();
            out.writeInt(transfer.length);
            out.write(transfer);
            out.write(HexFormat.of().parseHex(eventId));
            out.writeByte(replayCount);
            resource(out, destination);
            byte[] payload = bytes.toByteArray();
            out.write(digest(payload));
            if (bytes.size() > DltEvidence.MAX_HEADER_BYTES) invalid();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("无法编码 DLT 回投证据");
        }
    }

    /**
     * 业务作用：在使用历史回投次数和事件身份前验证整个有界正文。
     *
     * @param encoded 单一头值。
     * @return 规范、完整的来源证据，任何歧义或损坏均拒绝。
     */
    private static DltReplayTrace decode(byte[] encoded) {
        if (encoded == null || encoded.length < 80 || encoded.length > DltEvidence.MAX_HEADER_BYTES) invalid();
        byte[] body = Arrays.copyOf(encoded, encoded.length - 32);
        if (!MessageDigest.isEqual(digest(body), Arrays.copyOfRange(encoded, body.length, encoded.length))) invalid();
        try {
            var input = new DataInputStream(new ByteArrayInputStream(body));
            byte[] magic = input.readNBytes(8);
            boolean extended = Arrays.equals(magic, "NSKDRH02".getBytes(StandardCharsets.US_ASCII));
            if (!extended && !Arrays.equals(magic, "NSKDRH01".getBytes(StandardCharsets.US_ASCII))) invalid();
            TransferTrace.Resource origin = resource(input);
            int partition = input.readInt();
            long offset = input.readLong(), timestamp = input.readLong();
            DltEvidence evidence = new DltEvidence(text(input, 249, false), input.readInt(), input.readLong(), text(input, 1024, false), text(input, 1024, true), Integer.toUnsignedLong(input.readInt()));
            if (extended)
                evidence = new DltEvidence(evidence.originTopic(), evidence.originPartition(), evidence.originOffset(), evidence.originGroup(), evidence.reason(), evidence.deliveryAttempts(), new DltOrigin(text(input, 1024, false), text(input, 1024, false)));
            int length = input.readInt();
            if (length < 0 || length > DltEvidence.MAX_HEADER_BYTES || length > input.available()) invalid();
            TransferTrace transferred = length == 0 ? null : TransferTrace.decode(input.readNBytes(length));
            byte[] id = input.readNBytes(32);
            if (id.length != 32) invalid();
            DltReplayTrace trace = new DltReplayTrace(origin, partition, offset, timestamp, evidence, transferred,
                    HexFormat.of().formatHex(id), input.readUnsignedByte(), resource(input));
            if (input.available() != 0 || !Arrays.equals(encoded, trace.encode())) invalid();
            return trace;
        } catch (IOException | IllegalArgumentException | DltReplayException | TransferException rejected) {
            throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
        }
    }

    /**
     * 业务作用：编码物理资源身份，不以逻辑源别名代替集群。
     * <p>
     * 返回：三个长度前缀身份。
     *
     * @param out      正文
     * @param resource 合法资源。
     */
    static void resource(DataOutputStream out, TransferTrace.Resource resource) throws IOException {
        text(out, resource.clusterId());
        text(out, resource.topic());
        text(out, resource.generation());
    }

    /**
     * 业务作用：恢复并复验物理资源的有界身份。
     *
     * @param input 正文。
     * @return 合法资源。
     */
    static TransferTrace.Resource resource(DataInputStream input) throws IOException {
        return new TransferTrace.Resource(text(input, 1024, false), text(input, 249, false), text(input, 1024, false));
    }

    /**
     * 业务作用：写入已验证的UTF-8来源文本，保留Unicode原始证据。
     * <p>
     * 返回：u16大端长度和字节。
     *
     * @param out   正文
     * @param value 有界文本。
     */
    private static void text(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    /**
     * 业务作用：在分配身份前限制长度并拒绝无效UTF-8。
     *
     * @param input 正文
     * @param max   字节上限
     * @param empty 决定是否允许空原因。
     * @return 完整唯一文本。
     */
    private static String text(DataInputStream input, int max, boolean empty) throws IOException {
        int length = input.readUnsignedShort();
        if (length > max || !empty && length == 0) invalid();
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) invalid();
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
    }

    /**
     * 业务作用：为共同证据计算完整性摘要。
     *
     * @param bytes 规范正文。
     * @return SHA-256，不能证明写入者身份。
     */
    static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("无法摘要 DLT 回投证据");
        }
    }

    /**
     * 业务作用：在缺少完整证据时停止回投。
     * <p>
     * 参数说明：无。
     * 返回：固定字段错误。
     */
    private static void invalid() {
        throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
    }
}

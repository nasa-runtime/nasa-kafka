package io.github.nasaruntime.kafka.transfer;

import io.github.nasaruntime.kafka.replay.ReplayRecord;

import io.github.nasaruntime.kafka.protocol.KafkaHeaders;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * 有界转发来源证据；完整性摘要不是身份认证，可信性仍依赖 Kafka 写权限和资源目录。
 */
public record TransferTrace(Resource origin, int partition, long offset, long timestamp, String eventId, List<Resource> path) {

    public static final String HEADER = "x-nasa-transfer";
    public static final int MAX_HEADER_BYTES = 65536;

    /**
     * 路径以物理 topic 资源标识节点，同集群内不同 topic 不会被当作相同目的地。
     */
    public record Resource(String clusterId, String topic, String generation) {
        /**
         * 业务作用：固定能区分删除重建的物理 topic 身份。
         * <p>
         * 返回：有界合法节点。
         *
         * @param clusterId  物理集群
         * @param topic      合法名称
         * @param generation 外部资源代次。
         */
        public Resource {
            token(clusterId, 1024);
            token(generation, 1024);
            try {
                new TransferPlan.Partition(topic, 0);
            } catch (TransferException error) {
                invalid();
            }
        }
    }

    /**
     * 业务作用：校验原始事件坐标与实际经过路径的完整结构。
     * <p>
     * 返回：不可变证据；重复路径、非法坐标或摘要不符拒绝。
     *
     * @param origin    记录首次来源的稳定资源身份，不以本次目标覆盖历史来源。
     * @param partition 首次来源记录的原始分区号。
     * @param offset    首次来源记录在原分区内的offset。
     * @param timestamp 原始记录的Kafka毫秒时间戳。
     * @param eventId   共同坐标摘要
     * @param path      包含起点和当前目的地的有序节点。
     */
    public TransferTrace {
        if (origin == null || partition < 0 || offset < 0 || timestamp < -1 || path == null || path.size() < 2 || path.size() > 17 || path.stream().anyMatch(Objects::isNull))
            invalid();
        path = List.copyOf(path);
        if (!path.getFirst().equals(origin) || new HashSet<>(path).size() != path.size() || !identity(origin, partition, offset).equals(eventId))
            invalid();
    }

    /**
     * 业务作用：从目标记录提取可用于幂等识别的结构化原始坐标。
     *
     * @param headers 完整有序消息头。
     * @return 未转发记录为空；重复、大小写变体、损坏或超额证据拒绝，不把字段内容当作访问授权。
     */
    public static Optional<TransferTrace> read(KafkaHeaders headers) {
        if (headers == null) invalid();
        byte[] encoded = null;
        boolean found = false;
        for (Header header : headers.entries())
            if (isHeader(header.key())) {
                if (found || !header.key().equals(HEADER) || header.value() == null) invalid();
                found = true;
                encoded = header.value();
            }
        return found ? Optional.of(decode(encoded)) : Optional.empty();
    }

    /**
     * 业务作用：保留全部业务头并追加当前目标节点，阻止回环和超额跳数。
     *
     * @param plan        固定合同
     * @param record      来源原文
     * @param destination 固定目标分区。
     * @return 新消息头；旧来源坐标和事件ID不随Kafka新offset变化。
     */
    public static KafkaHeaders forward(TransferPlan plan, ReplayRecord record, TransferPlan.Partition destination) {
        if (record.headers().entries().size() > 256 || record.headers().byteSize() > MAX_HEADER_BYTES) invalid();
        Resource current = new Resource(record.clusterId(), record.topic(), plan.replay().topicGenerations().get(record.topic()));
        Resource target = new Resource(plan.target().clusterId(), destination.topic(), plan.target().topicGenerations().get(destination.topic()));
        Optional<TransferTrace> previous = read(record.headers());
        Resource origin;
        int partition;
        long offset, timestamp;
        String id;
        List<Resource> path;
        if (previous.isPresent()) {
            TransferTrace trace = previous.get();
            // 跨topic复制旧证据不能被当作完整转发路径；来源目录必须与最后一跳的物理资源一致。
            if (!trace.path().getLast().equals(current)) invalid();
            origin = trace.origin();
            partition = trace.partition();
            offset = trace.offset();
            timestamp = trace.timestamp();
            id = trace.eventId();
            path = new ArrayList<>(trace.path());
        } else {
            origin = current;
            partition = record.partition();
            offset = record.offset();
            timestamp = record.timestamp();
            id = identity(origin, partition, offset);
            path = new ArrayList<>(List.of(current));
        }
        if (path.contains(target)) throw new TransferException(TransferException.Code.LOOP_DETECTED);
        if (path.size() > plan.maxHops()) throw new TransferException(TransferException.Code.HOP_LIMIT);
        path.add(target);
        byte[] encoded = new TransferTrace(origin, partition, offset, timestamp, id, path).encode();
        List<Header> outgoing = new ArrayList<>();
        for (Header header : record.headers().entries()) if (!isHeader(header.key())) outgoing.add(header);
        outgoing.add(new RecordHeader(HEADER, encoded));
        KafkaHeaders result = new KafkaHeaders(outgoing);
        if (result.entries().size() > 256 || result.byteSize() > MAX_HEADER_BYTES) invalid();
        return result;
    }

    /**
     * 业务作用：为来源坐标生成跨语言、跨任务稳定的事件身份。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param origin    记录首次来源的稳定资源身份，不以本次目标覆盖历史来源。
     * @param partition 首次来源记录的原始分区号。
     * @param offset    首次来源记录在原分区内的offset。
     * @return SHA-256小写十六进制，不包含目标offset或处理任务名。
     */
    public static String identity(Resource origin, int partition, long offset) {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.write("NSKTID01".getBytes(StandardCharsets.US_ASCII));
            resource(out, origin);
            out.writeInt(partition);
            out.writeLong(offset);
            return HexFormat.of().formatHex(digest(bytes.toByteArray()));
        } catch (IOException error) {
            throw new IllegalStateException("无法编码来源身份");
        }
    }

    /**
     * 业务作用：按协议ASCII大小写识别框架字段，避免Unicode字母折叠改变业务头归属。
     *
     * @param name 原始头名。
     * @return 仅规范名称及ASCII大小写变体为true。
     */
    public static boolean isHeader(String name) {
        if (name == null || name.length() != HEADER.length()) return false;
        for (int i = 0; i < name.length(); i++) {
            char current = name.charAt(i);
            if (current >= 'A' && current <= 'Z') current = (char) (current + ('a' - 'A'));
            if (current != HEADER.charAt(i)) return false;
        }
        return true;
    }

    /**
     * 业务作用：生成原始身份与有序路径的唯一共同表达。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 有界正文及完整性摘要，不暴露任意路由配置入口。
     */
    public byte[] encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.write("NSKTRH01".getBytes(StandardCharsets.US_ASCII));
            resource(out, origin);
            out.writeInt(partition);
            out.writeLong(offset);
            out.writeLong(timestamp);
            out.write(HexFormat.of().parseHex(eventId));
            out.writeByte(path.size());
            for (Resource node : path) resource(out, node);
            byte[] payload = bytes.toByteArray();
            out.write(digest(payload));
            byte[] encoded = bytes.toByteArray();
            if (encoded.length > MAX_HEADER_BYTES) invalid();
            return encoded;
        } catch (IOException error) {
            throw new IllegalStateException("无法编码转发路径");
        }
    }

    /**
     * 业务作用：在任何转发副作用前拒绝伪造结构、损坏和非规范路径。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param encoded 单个原始证据头。
     * @return 完整结构化证据；摘要只检测内容变化，不证明写入者身份。
     */
    public static TransferTrace decode(byte[] encoded) {
        if (encoded == null || encoded.length < 82 || encoded.length > MAX_HEADER_BYTES) invalid();
        byte[] payload = Arrays.copyOf(encoded, encoded.length - 32);
        if (!MessageDigest.isEqual(digest(payload), Arrays.copyOfRange(encoded, payload.length, encoded.length)))
            invalid();
        try {
            var input = new DataInputStream(new ByteArrayInputStream(payload));
            if (!Arrays.equals(input.readNBytes(8), "NSKTRH01".getBytes(StandardCharsets.US_ASCII))) invalid();
            Resource origin = resource(input);
            int partition = input.readInt();
            long offset = input.readLong(), timestamp = input.readLong();
            byte[] id = input.readNBytes(32);
            int count = input.readUnsignedByte();
            if (id.length != 32 || count < 2 || count > 17) invalid();
            List<Resource> path = new ArrayList<>(count);
            for (int i = 0; i < count; i++) path.add(resource(input));
            TransferTrace trace = new TransferTrace(origin, partition, offset, timestamp, HexFormat.of().formatHex(id), path);
            if (input.available() != 0 || !Arrays.equals(trace.encode(), encoded)) invalid();
            return trace;
        } catch (IOException | IllegalArgumentException error) {
            throw new TransferException(TransferException.Code.HEADER_INVALID);
        }
    }

    /**
     * 业务作用：规范编码一个物理topic身份。
     * <p>
     * 返回：三个有界长度前缀文本。
     *
     * @param out  内存编码器
     * @param node 合法节点。
     */
    private static void resource(DataOutputStream out, Resource node) throws IOException {
        text(out, node.clusterId());
        text(out, node.topic());
        text(out, node.generation());
    }

    /**
     * 业务作用：解析并复验单个物理topic身份。
     *
     * @param input 有界正文。
     * @return 合法节点。
     */
    private static Resource resource(DataInputStream input) throws IOException {
        return new Resource(text(input, 1024), text(input, 249), text(input, 1024));
    }

    /**
     * 业务作用：编码经过身份校验的ASCII文本。
     * <p>
     * 返回：两字节长度与原字节。
     *
     * @param out   内存编码器
     * @param value 身份。
     */
    private static void text(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    /**
     * 业务作用：在分配内容前限制路径身份长度。
     *
     * @param input 有界正文
     * @param max   最大长度。
     * @return 完整可见ASCII文本。
     */
    private static String text(DataInputStream input, int max) throws IOException {
        int size = input.readUnsignedShort();
        if (size < 1 || size > max) invalid();
        byte[] bytes = input.readNBytes(size);
        if (bytes.length != size) invalid();
        String value = new String(bytes, StandardCharsets.US_ASCII);
        token(value, max);
        return value;
    }

    /**
     * 业务作用：拒绝路径中的空身份和非共同字符。
     * <p>
     * 返回：合法可见ASCII。
     *
     * @param value 文本
     * @param max   最大长度。
     */
    private static void token(String value, int max) {
        if (value == null || value.isEmpty() || value.length() > max || value.chars().anyMatch(c -> c < 33 || c > 126))
            invalid();
    }

    /**
     * 业务作用：计算共同事件ID或证据完整性。
     *
     * @param bytes 规范正文。
     * @return SHA-256，不承担认证。
     */
    private static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("无法摘要转发证据");
        }
    }

    /**
     * 业务作用：在没有完整可信结构时停止转发。
     * <p>
     * 参数说明：无。
     * 返回：稳定证据错误，不发送或推进进度。
     */
    private static void invalid() {
        throw new TransferException(TransferException.Code.HEADER_INVALID);
    }
}

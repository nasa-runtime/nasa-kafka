package io.github.nasaruntime.kafka.checkpoint;



import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/**
 * 有界、排序固定且带 SHA-256 完整性校验的双端 checkpoint 格式。
 */
final class CheckpointWire {

    static final int MAX_BYTES = 2 * 1024 * 1024;
    private static final byte[] MAGIC = "NSKCP001".getBytes(StandardCharsets.US_ASCII);

    /**
     * 业务作用：将已验证快照编码为可原子替换的完整文件内容。
     *
     * @param checkpoint 不可变进度。
     * @return 正文与 SHA-256，长度超过存储硬上限时拒绝。
     */
    static byte[] encode(ReplayCheckpoint checkpoint) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(buffer);
            output.write(MAGIC);
            text(output, checkpoint.taskId());
            output.write(HexFormat.of().parseHex(checkpoint.contractFingerprint()));
            output.writeLong(checkpoint.generation());
            output.writeLong(checkpoint.revision());
            output.writeInt(checkpoint.positions().size());
            for (ReplayCheckpoint.Position position : checkpoint.positions()) {
                text(output, position.topic());
                output.writeInt(position.partition());
                output.writeLong(position.startOffset());
                output.writeLong(position.endOffset());
                output.writeLong(position.nextOffset());
            }
            byte[] payload = buffer.toByteArray();
            output.write(digest(payload));
            if (buffer.size() > MAX_BYTES) throw new CheckpointException(CheckpointException.Code.CAPACITY);
            return buffer.toByteArray();
        } catch (IOException impossible) {
            throw new CheckpointException(CheckpointException.Code.IO);
        }
    }

    /**
     * 业务作用：在恢复前验证整个文件完整性、格式和范围，损坏证据不能成为消费起点。
     *
     * @param bytes 有界完整文件。
     * @return 规范化快照；截断、尾随内容、错序或不合法进度均为 CORRUPT。
     */
    static ReplayCheckpoint decode(byte[] bytes) {
        if (bytes.length > MAX_BYTES || bytes.length < 8 + 2 + 1 + 32 + 16 + 4 + 32) throw corrupt();
        byte[] payload = Arrays.copyOf(bytes, bytes.length - 32);
        if (!MessageDigest.isEqual(digest(payload), Arrays.copyOfRange(bytes, payload.length, bytes.length)))
            throw corrupt();
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
            if (!Arrays.equals(input.readNBytes(MAGIC.length), MAGIC)) throw corrupt();
            String task = text(input, 128);
            String fingerprint = HexFormat.of().formatHex(input.readNBytes(32));
            long generation = input.readLong(), revision = input.readLong();
            int count = input.readInt();
            if (count <= 0 || count > ReplayCheckpoint.MAX_PARTITIONS) throw corrupt();
            List<ReplayCheckpoint.Position> positions = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                positions.add(new ReplayCheckpoint.Position(text(input, 249), input.readInt(), input.readLong(), input.readLong(), input.readLong()));
            ReplayCheckpoint checkpoint = new ReplayCheckpoint(task, fingerprint, generation, revision, positions);
            if (input.available() != 0 || !checkpoint.positions().equals(positions)) throw corrupt();
            return checkpoint;
        } catch (IOException | IllegalArgumentException | CheckpointException rejected) {
            throw corrupt();
        }
    }

    /**
     * 业务作用：以无路径含义的固定摘要映射稳定任务名。
     *
     * @param taskId 经过公开任务名规则验证的身份。
     * @return 小写 SHA-256 文件前缀，调用前拒绝路径片段或空白身份。
     */
    static String key(String taskId) {
        if (taskId == null || !taskId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))
            throw new CheckpointException(CheckpointException.Code.INVALID_CONFIGURATION);
        return HexFormat.of().formatHex(digest(taskId.getBytes(StandardCharsets.US_ASCII)));
    }

    /**
     * 业务作用：为文件正文和任务命名计算共同完整性摘要。
     *
     * @param bytes 确定字节序列。
     * @return 32 字节 SHA-256，不输出正文。
     */
    private static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException unavailable) {
            throw new CheckpointException(CheckpointException.Code.IO);
        }
    }

    /**
     * 业务作用：以固定大端长度编码受限 ASCII 身份。
     * <p>
     * 返回：写入长度与原文字节，不使用平台字符集。
     *
     * @param output 正文
     * @param value  已校验任务名或 topic。
     */
    private static void text(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        output.writeShort(bytes.length);
        output.write(bytes);
    }

    /**
     * 业务作用：在分配字符串前校验身份长度和字符集。
     *
     * @param input 有界正文
     * @param limit 对应身份上限。
     * @return 完整 ASCII 文本；截断和非 ASCII 拒绝。
     */
    private static String text(DataInputStream input, int limit) throws IOException {
        int length = input.readUnsignedShort();
        if (length == 0 || length > limit) throw corrupt();
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) throw corrupt();
        for (byte value : bytes) if (value < 0) throw corrupt();
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    /**
     * 业务作用：统一无法信任的持久证据分类。
     * <p>
     * 参数说明：无。
     *
     * @return 不泄露文件内容的损坏异常。
     */
    private static CheckpointException corrupt() {
        return new CheckpointException(CheckpointException.Code.CORRUPT);
    }
}

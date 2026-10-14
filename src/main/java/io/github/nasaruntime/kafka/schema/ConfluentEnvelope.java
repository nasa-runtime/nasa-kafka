package io.github.nasaruntime.kafka.schema;


import java.nio.ByteBuffer;
import java.util.Set;
import java.util.Arrays;

/**
 * Confluent 的五字节 envelope；持有独立载荷，不提供 Avro、Protobuf 或 JSON Schema codec。
 *
 * @param schemaId 正的有符号 32 位 Registry ID
 * @param payload  不含 envelope 的原始 codec 字节
 */
public record ConfluentEnvelope(int schemaId, byte[] payload) {

    public static final int HEADER_LENGTH = 5;

    /**
     * 业务作用：冻结 schema ID 与消息体，避免异步使用期间被修改。
     * <p>
     * 返回：拥有独立载荷的 envelope，非法 ID 或 null 载荷拒绝。
     *
     * @param schemaId 正 ID
     * @param payload  codec 字节。
     */
    public ConfluentEnvelope {
        if (schemaId <= 0) throw new SchemaRegistryException(SchemaRegistryException.Code.INVALID_SCHEMA_ID);
        if (payload == null) throw new SchemaRegistryException(SchemaRegistryException.Code.INVALID_ENVELOPE);
        payload = payload.clone();
    }

    /**
     * 业务作用：隔离读取方对已冻结字节的修改。
     * <p>
     * 参数说明：无。
     *
     * @return 独立载荷副本。
     */
    @Override
    public byte[] payload() {
        return payload.clone();
    }

    /**
     * 业务作用：在任何 Registry 请求之前执行线路与角色白名单校验。
     *
     * @param wire            完整消息
     * @param maxPayloadBytes 业务字节上限
     * @param approved        受信配置的 ID 集合。
     * @return 合法独立 envelope；未批准 ID 不会产生网络访问。
     */
    public static ConfluentEnvelope decode(byte[] wire, int maxPayloadBytes, Set<Integer> approved) {
        if (wire == null || wire.length < HEADER_LENGTH)
            throw new SchemaRegistryException(SchemaRegistryException.Code.INVALID_ENVELOPE);
        if (wire[0] != 0) throw new SchemaRegistryException(SchemaRegistryException.Code.UNSUPPORTED_MAGIC);
        int id = ByteBuffer.wrap(wire, 1, 4).getInt();
        if (id <= 0) throw new SchemaRegistryException(SchemaRegistryException.Code.INVALID_SCHEMA_ID);
        if (maxPayloadBytes < 0 || wire.length - HEADER_LENGTH > maxPayloadBytes)
            throw new SchemaRegistryException(SchemaRegistryException.Code.PAYLOAD_TOO_LARGE);
        // 消息自报 ID 不能授权读取任意 schema，先执行受信控制面批准的白名单。
        if (approved == null || !approved.contains(id))
            throw new SchemaRegistryException(SchemaRegistryException.Code.UNAPPROVED_SCHEMA_ID);
        return new ConfluentEnvelope(id, Arrays.copyOfRange(wire, HEADER_LENGTH, wire.length));
    }

    /**
     * 业务作用：输出与 Rust 相同的大端 schema ID 及原始载荷。
     *
     * @param maxPayloadBytes 不含 envelope 的字节上限。
     * @return 独立线路字节，超限拒绝编码。
     */
    public byte[] encode(int maxPayloadBytes) {
        if (payload.length > maxPayloadBytes || payload.length > Integer.MAX_VALUE - HEADER_LENGTH)
            throw new SchemaRegistryException(SchemaRegistryException.Code.PAYLOAD_TOO_LARGE);
        return ByteBuffer.allocate(HEADER_LENGTH + payload.length).put((byte) 0).putInt(schemaId).put(payload).array();
    }
}

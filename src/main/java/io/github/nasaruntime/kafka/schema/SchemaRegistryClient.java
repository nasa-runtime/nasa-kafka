package io.github.nasaruntime.kafka.schema;

import java.util.concurrent.CompletableFuture;

/**
 * 独立于 Kafka 认证域的 Schema Registry 能力；不会自动为普通 JSON 消息生成或注册 schema。
 */
public interface SchemaRegistryClient extends AutoCloseable {

    /**
     * Confluent HTTP 接口使用的标准 schema 类型。
     */
    enum SchemaType {AVRO, PROTOBUF, JSON}

    /**
     * Registry 已返回的 schema，不自动等同于当前消费角色批准使用。
     *
     * @param id     正的 Registry 本地 ID，不是跨 Registry 的全局身份
     * @param type   声明的 schema 协议
     * @param schema 完整 schema 文本，业务日志不应输出该内容
     */
    record RegisteredSchema(int id, SchemaType type, String schema) {
        /**
         * 业务作用：避免默认 record 文本泄露业务 schema。
         * <p>
         * 参数说明：无。
         *
         * @return 只包含 ID 和类型的诊断。
         */
        @Override
        public String toString() {
            return "RegisteredSchema[id=" + id + ", type=" + type + ", schema=<redacted>]";
        }
    }

    /**
     * 业务作用：按正 ID 查询 Registry 或有界缓存。
     *
     * @param id 已验证的正 ID。
     * @return 异步 schema，缺失或传输失败按稳定分类返回。
     */
    CompletableFuture<RegisteredSchema> schemaById(int id);

    /**
     * 业务作用：只读检查候选 schema 对某修订的兼容性。
     *
     * @param subject 控制配置
     * @param version 正整数或 latest
     * @param type    候选或目标schema的共同类型，例如AVRO、JSON或PROTOBUF。
     * @param schema  候选schema的完整正文，不由消息内容推断或自动替换。
     * @return 兼容结论或分类错误，不注册 schema。
     */
    CompletableFuture<Boolean> isCompatible(String subject, String version, SchemaType type, String schema);

    /**
     * 业务作用：通过显式控制入口注册候选 schema。
     *
     * @param subject 目标
     * @param type    候选或目标schema的共同类型，例如AVRO、JSON或PROTOBUF。
     * @param schema  候选schema的完整正文，不由消息内容推断或自动替换。
     * @return 远端分配 ID；默认关闭，超时不能推定没有注册。
     */
    CompletableFuture<Integer> register(String subject, SchemaType type, String schema);

    /**
     * 业务作用：关闭新准入并终止本客户端的 HTTP 等待。
     * <p>
     * 参数说明：无。
     * 返回：本地请求不再接纳；已到达远端的写入仍可能生效。
     */
    @Override
    void close();
}

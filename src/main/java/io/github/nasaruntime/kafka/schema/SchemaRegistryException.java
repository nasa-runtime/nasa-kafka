package io.github.nasaruntime.kafka.schema;

/**
 * Registry 的封闭失败分类；异常文本和原因链不携带 endpoint、subject、schema 或凭据。
 */
public final class SchemaRegistryException extends RuntimeException {

    public enum Code {
        /**
         * 客户端已关闭新调用准入。
         */
        CLOSED,
        /**
         * endpoint、认证或容量时限不满足安全约束。
         */
        INVALID_CONFIGURATION,
        /**
         * schema ID 不属于正的有符号 32 位范围。
         */
        INVALID_SCHEMA_ID,
        /**
         * 线路内容不足五字节。
         */
        INVALID_ENVELOPE,
        /**
         * magic byte 不属于支持的 Confluent envelope。
         */
        UNSUPPORTED_MAGIC,
        /**
         * schema ID 未经本消费角色显式批准，不允许访问 Registry。
         */
        UNAPPROVED_SCHEMA_ID,
        /**
         * 不含 envelope 的业务字节超过声明上限。
         */
        PAYLOAD_TOO_LARGE,
        /**
         * Registry 返回或负缓存记录了不存在的 ID。
         */
        SCHEMA_NOT_FOUND,
        /**
         * 网络或总期限失败；写操作可能已经到达远端。
         */
        TRANSPORT,
        /**
         * 远端返回非成功状态，不能将认证失败缓存为不存在。
         */
        REMOTE_STATUS,
        /**
         * 响应体超过配置的硬字节边界。
         */
        RESPONSE_TOO_LARGE,
        /**
         * 候选 schema 超过配置的 UTF-8 字节边界。
         */
        SCHEMA_TOO_LARGE,
        /**
         * 返回 JSON 或必需字段不满足协议合同。
         */
        INVALID_RESPONSE,
        /**
         * subject 或 version 不能安全形成控制面路径。
         */
        INVALID_SUBJECT,
        /**
         * 运行期注册未显式启用。
         */
        AUTO_REGISTER_DISABLED,
        /**
         * 不同在途 ID 或控制请求达到客户端容量边界。
         */
        CAPACITY
    }

    private final Code code;
    private final int status;

    /**
     * 业务作用：产生仅包含稳定类别的 Registry 失败。
     * <p>
     * 返回：不附加原生敏感原因的异常。
     *
     * @param code 封闭分类。
     */
    public SchemaRegistryException(Code code) {
        this(code, 0);
    }

    /**
     * 业务作用：保留远端状态而隔离正文及认证材料。
     * <p>
     * 返回：可安全记录的异常。
     *
     * @param code   分类
     * @param status HTTP 状态或零。
     */
    public SchemaRegistryException(Code code, int status) {
        super(code.name() + (status == 0 ? "" : ":" + status));
        this.code = code;
        this.status = status;
    }

    /**
     * 业务作用：供调用方选择重试、隔离或人工处置。
     * <p>
     * 参数说明：无。
     *
     * @return 封闭失败分类。
     */
    public Code code() {
        return code;
    }

    /**
     * 业务作用：区分远端拒绝状态。
     * <p>
     * 参数说明：无。
     *
     * @return HTTP 状态，无远端应答时为零。
     */
    public int status() {
        return status;
    }
}

package io.github.nasaruntime.kafka.schema;


import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Set;

/**
 * Registry 独立的配置与认证合同；不会继承 Kafka SASL 或 TLS 凭据。
 *
 * @param endpoint          受信控制配置的 HTTP(S) 基址，明文 HTTP 只允许本机
 * @param authorization     显式 Basic/Bearer 头或 null；不从 endpoint 或消息内容读取凭据
 * @param requestTimeout    单次 HTTP 请求总期限
 * @param maxResponseBytes  响应与候选 schema 的最大 UTF-8 字节数，硬上限 16 MiB
 * @param cacheCapacity     正负缓存共用的最大条目数，同时限制并行 HTTP 请求数
 * @param cacheTtl          成功 schema 缓存时长
 * @param negativeCacheTtl  仅 404 缺失结果的缓存时长
 * @param autoRegister      是否开放显式 register 调用，默认 false
 * @param maxPendingLookups 同时等待 schema 查询结果的业务调用数上限，包含同 ID 的等待者
 */
public record ConfluentRegistryOptions(URI endpoint, String authorization, Duration requestTimeout,
                                       int maxResponseBytes, int cacheCapacity, Duration cacheTtl,
                                       Duration negativeCacheTtl, boolean autoRegister, int maxPendingLookups) {

    public static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
    private static final Duration MAX_DURATION = Duration.ofDays(365);

    /**
     * 业务作用：保留基础配置入口并为共享查询等待者设置独立上限。
     * <p>
     * 返回：最多 4096 个同时等待的查询，HTTP 与缓存容量仍由 cacheCapacity 限制。
     *
     * @param endpoint         受信HTTP(S)基址，不接受URI内嵌凭据；明文HTTP只允许本机。
     * @param authorization    显式Basic或Bearer认证头，可为null，不继承Kafka认证材料。
     * @param requestTimeout   单次HTTP请求总期限，必须为正且不超过365天。
     * @param maxResponseBytes 响应及候选schema的最大UTF-8字节数，必须为正且不超过16MiB。
     * @param cacheCapacity    正负缓存共用的条目上限，同时限制并行HTTP请求数，必须为正。
     * @param cacheTtl         成功schema的缓存时长，必须为正且不超过365天。
     * @param negativeCacheTtl 仅404缺失结果的缓存时长，必须为正且不超过365天。
     * @param autoRegister     是否允许显式register调用，不会自动替业务数据注册schema。
     */
    public ConfluentRegistryOptions(URI endpoint, String authorization, Duration requestTimeout,
                                    int maxResponseBytes, int cacheCapacity, Duration cacheTtl,
                                    Duration negativeCacheTtl, boolean autoRegister) {
        this(endpoint, authorization, requestTimeout, maxResponseBytes, cacheCapacity, cacheTtl, negativeCacheTtl, autoRegister, 4096);
    }

    /**
     * 业务作用：在建连前固定 endpoint、认证与资源边界。
     * <p>
     * 返回：不可变合法配置；非法声明不会建立 HTTP 客户端。
     *
     * @param endpoint          受信HTTP(S)基址，不接受URI内嵌凭据；明文HTTP只允许本机。
     * @param authorization     显式Basic或Bearer认证头，可为null，不继承Kafka认证材料。
     * @param requestTimeout    单次HTTP请求总期限，必须为正且不超过365天。
     * @param maxResponseBytes  响应及候选schema的最大UTF-8字节数，必须为正且不超过16MiB。
     * @param cacheCapacity     正负缓存共用的条目上限，同时限制并行HTTP请求数，必须为正。
     * @param cacheTtl          成功schema的缓存时长，必须为正且不超过365天。
     * @param negativeCacheTtl  仅404缺失结果的缓存时长，必须为正且不超过365天。
     * @param autoRegister      是否允许显式register调用，不会自动替业务数据注册schema。
     * @param maxPendingLookups 同时等待查询结果的业务调用数上限，包含同ID等待者，范围1至65536。
     */
    public ConfluentRegistryOptions {
        if (endpoint == null || endpoint.getHost() == null || endpoint.getUserInfo() != null
                || endpoint.getRawQuery() != null || endpoint.getRawFragment() != null
                || !("https".equals(endpoint.getScheme()) || "http".equals(endpoint.getScheme())))
            throw invalid();
        // 非本机明文连接会暴露 Registry 认证与 schema，必须显式使用 TLS。
        if ("http".equals(endpoint.getScheme()) && !Set.of("localhost", "127.0.0.1", "[::1]", "::1").contains(endpoint.getHost()))
            throw invalid();
        if (maxResponseBytes <= 0 || maxResponseBytes > MAX_RESPONSE_BYTES || cacheCapacity <= 0
                || maxPendingLookups <= 0 || maxPendingLookups > 65536) throw invalid();
        for (Duration value : new Duration[]{requestTimeout, cacheTtl, negativeCacheTtl})
            if (value == null || value.isNegative() || value.isZero() || value.compareTo(MAX_DURATION) > 0)
                throw invalid();
        if (authorization != null) validateAuthorization(authorization);
    }

    /**
     * 业务作用：建立与 Rust 对应的保守 Registry 默认值。
     *
     * @param endpoint 受信基址。
     * @return 3 秒请求、256 项缓存、300 秒正缓存和 5 秒负缓存，注册关闭。
     */
    public static ConfluentRegistryOptions defaults(String endpoint) {
        try {
            return new ConfluentRegistryOptions(URI.create(endpoint), null, Duration.ofSeconds(3), 1024 * 1024, 256, Duration.ofSeconds(300), Duration.ofSeconds(5), false);
        } catch (IllegalArgumentException error) {
            throw invalid();
        }
    }

    /**
     * 业务作用：生成独立 Registry 域的 Basic 凭据。
     *
     * @param username Basic认证用户名，不能为空、含冒号或控制字符，UTF-8长度最多255字节。
     * @param password Basic认证密码，1至4096个可打印ASCII字符，不包含空格或控制字符。
     * @return HTTP 认证头，非法用户名或控制字符拒绝。
     */
    public static String basic(String username, String password) {
        if (username == null || username.isEmpty() || username.getBytes(StandardCharsets.UTF_8).length > 255 || username.contains(":")
                || username.chars().anyMatch(Character::isISOControl) || password == null || password.isEmpty() || password.length() > 4096
                || password.chars().anyMatch(c -> c < 33 || c > 126)) throw invalid();
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 业务作用：限制认证材料长度和字符集并核对 Basic 的明确用户名边界。
     * <p>
     * 返回：合法认证正常返回，否则在建连前拒绝。
     *
     * @param value 完整认证头。
     */
    private static void validateAuthorization(String value) {
        if (value.startsWith("Bearer ")) {
            String token = value.substring(7);
            if (token.isEmpty() || token.length() > 4096 || token.chars().anyMatch(c -> c < 33 || c > 126))
                throw invalid();
        } else if (value.startsWith("Basic ")) {
            if (value.length() > 6000) throw invalid();
            try {
                String decoded = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(Base64.getDecoder().decode(value.substring(6)))).toString();
                int separator = decoded.indexOf(':');
                if (separator < 0 || !basic(decoded.substring(0, separator), decoded.substring(separator + 1)).equals(value))
                    throw invalid();
            } catch (IllegalArgumentException | CharacterCodingException invalid) {
                throw invalid();
            }
        } else throw invalid();
    }

    /**
     * 业务作用：禁止默认配置文本泄露认证或 endpoint。
     * <p>
     * 参数说明：无。
     *
     * @return 只包含容量与注册开关的诊断。
     */
    @Override
    public String toString() {
        return "ConfluentRegistryOptions[endpoint=<redacted>, authorization=<redacted>, cacheCapacity=" + cacheCapacity + ", autoRegister=" + autoRegister + "]";
    }

    /**
     * 业务作用：统一配置拒绝文本，隔离原生 URI 异常中的地址和凭据。
     * <p>
     * 参数说明：无。
     *
     * @return 稳定配置错误。
     */
    private static SchemaRegistryException invalid() {
        return new SchemaRegistryException(SchemaRegistryException.Code.INVALID_CONFIGURATION);
    }
}

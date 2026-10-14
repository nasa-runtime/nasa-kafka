package io.github.nasaruntime.kafka.support;

import io.github.nasaruntime.kafka.consumer.ReadyRequirement;
import io.github.nasaruntime.kafka.consumer.ReadyStatus;

import io.github.nasaruntime.kafka.exception.KafkaFailure;
import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.config.NasaKafkaProperties;

import java.util.*;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.BufferSupplier;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.net.ssl.SSLContext;
import javax.security.sasl.Sasl;

import org.apache.kafka.common.utils.AppInfoParser;

/**
 * 构建支持、冻结启用项与实际组就绪分别呈现；支持认证算法不证明凭据有效或具备资源权限。
 */
public record KafkaCapabilities(String sourceId, String clientVersion, Set<String> supported,
                                Set<String> configured, Map<String, String> laneCompression,
                                Map<String, ReadyStatus> readiness) {

    /**
     * 业务作用：冻结不包含凭据的能力与运行证据。
     * <p>
     * 返回：独立只读快照；空就绪表表示尚无普通消费组证据。
     *
     * @param sourceId        能力报告所属的实际命名数据源。
     * @param clientVersion   当前进程使用的Kafka原生Java客户端版本。
     * @param supported       本进程可用实现
     * @param configured      该源启用项
     * @param laneCompression 声明通道算法
     * @param readiness       当前普通组Joined要求的观测。
     */
    public KafkaCapabilities {
        supported = Collections.unmodifiableSet(new TreeSet<>(supported));
        configured = Collections.unmodifiableSet(new TreeSet<>(configured));
        laneCompression = Collections.unmodifiableMap(new TreeMap<>(laneCompression));
        readiness = Collections.unmodifiableMap(new TreeMap<>(readiness));
    }

    /**
     * 业务作用：报告实际可用的安全和压缩实现，不创建Kafka客户端或连接。
     * <p>
     * 参数说明：无。
     *
     * @return 类加载器范围内的固定能力集；可选依赖或原生实现不可用时不宣称支持。
     */
    public static Set<String> supportedCapabilities() {
        return Available.detect();
    }

    /**
     * 业务作用：汇总所属源配置和当次健康，避免将构建能力误报为Ready。
     *
     * @param source 实际源
     * @param config 该源冻结的有效配置。
     * @return 不建连、不含认证字段或路径的能力快照。
     */
    public static KafkaCapabilities snapshot(KafkaProxy source, NasaKafkaProperties config) {
        Map<String, ReadyStatus> ready = new TreeMap<>();
        source.groupHealth().forEach((group, health) -> ready.put(group, ReadyStatus.evaluate(ReadyRequirement.JOINED, health)));
        return new KafkaCapabilities(source.sourceId(), AppInfoParser.getVersion(),
                supportedCapabilities(), configured(config), compressions(config), ready);
    }

    /**
     * 业务作用：在资源创建前拒绝未知协议、未实现机制及缺失压缩依赖，不回退到其它算法。
     * <p>
     * 返回：全部启用项可用才通过；错误仅含固定能力名，不输出配置值。
     *
     * @param config 已合并的源配置。
     */
    public static void validate(NasaKafkaProperties config) {
        for (String capability : configured(config))
            if (!Available.supported(capability))
                throw new KafkaFailure(KafkaFailure.Code.UNSUPPORTED_CAPABILITY, "当前构建不支持 " + capability);
    }

    /**
     * 业务作用：从有效源配置得到已启用能力，不把配置文本当作远端成功证据。
     *
     * @param config 合并后的源配置。
     * @return 规范能力名集合，未知声明拒绝。
     */
    private static Set<String> configured(NasaKafkaProperties config) {
        Properties nativeConfig = config.buildProducerProperties();
        String protocol = nativeConfig.getProperty("security.protocol", "PLAINTEXT");
        if (!Set.of("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL").contains(protocol))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "security.protocol 非法");
        if (!protocol.endsWith("SSL") && nativeConfig.stringPropertyNames().stream().anyMatch(key -> key.startsWith("ssl.")))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "SSL 字段要求源协议启用 SSL");
        if (!protocol.startsWith("SASL_") && nativeConfig.stringPropertyNames().stream().anyMatch(key -> key.startsWith("sasl.")))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "SASL 字段要求源协议启用 SASL");
        Set<String> values = new TreeSet<>();
        if (protocol.endsWith("SSL")) {
            values.add("tls");
            if (nativeConfig.containsKey("ssl.keystore.location") || nativeConfig.containsKey("ssl.keystore.key"))
                values.add("mtls");
        }
        if (protocol.startsWith("SASL_")) {
            String mechanism = nativeConfig.getProperty("sasl.mechanism", "GSSAPI");
            values.add(switch (mechanism) {
                case "PLAIN" -> "sasl-plain";
                case "SCRAM-SHA-256" -> "sasl-scram-sha-256";
                case "SCRAM-SHA-512" -> "sasl-scram-sha-512";
                case "GSSAPI" -> "sasl-gssapi";
                default -> throw new KafkaFailure(KafkaFailure.Code.UNSUPPORTED_CAPABILITY, "未支持的 SASL 机制");
            });
        }
        for (String compression : compressions(config).values()) {
            if (!Set.of("none", "gzip", "snappy", "lz4", "zstd").contains(compression))
                throw new KafkaFailure(KafkaFailure.Code.UNSUPPORTED_CAPABILITY, "未支持的压缩算法");
            values.add("compression-" + compression);
        }
        return values;
    }

    /**
     * 业务作用：列出全部声明通道的有效压缩算法，检查尚未创建的lane。
     *
     * @param config 冻结源配置。
     * @return default及命名lane的规范算法，不初始化producer。
     */
    private static Map<String, String> compressions(NasaKafkaProperties config) {
        Map<String, String> values = new TreeMap<>();
        String base = config.buildProducerProperties().getProperty("compression.type", "none");
        values.put("default", base);
        config.getProducerLanes().forEach((name, lane) -> {
            String effective = lane.getCompression();
            if (effective == null) effective = config.getCompressionType();
            if (effective == null || effective.isBlank())
                effective = lane.getProperties() != null && lane.getProperties().containsKey("compression.type") ? lane.getProperties().get("compression.type") : base;
            values.put(name, effective);
        });
        return values;
    }

    /**
     * 类加载器内只探测一次本地实现，不涉及broker或凭据。
     */
    private static final class Available {
        private static final ConcurrentMap<String, Boolean> VALUES = new ConcurrentHashMap<>();

        /**
         * 业务作用：禁止实例化进程内的能力缓存。
         * <p>
         * 参数说明：无。
         * 返回：无。
         */
        private Available() {}

        /**
         * 业务作用：只探测实际请求的本地能力，普通无压缩源不会因此加载其它原生压缩库。
         *
         * @param capability 固定规范能力名。
         * @return 类加载器内缓存的支持事实，不创建Kafka客户端。
         */
        private static boolean supported(String capability) {
            return VALUES.computeIfAbsent(capability, value -> {
                if (value.startsWith("compression-"))
                    return compressionAvailable(value.substring("compression-".length()));
                if (value.equals("tls") || value.equals("mtls")) {
                    try {
                        SSLContext.getInstance("TLS");
                        return true;
                    } catch (NoSuchAlgorithmException unavailable) {
                        return false;
                    }
                }
                if (value.equals("sasl-gssapi")) {
                    var factories = Sasl.getSaslClientFactories();
                    while (factories.hasMoreElements())
                        if (Arrays.asList(factories.nextElement().getMechanismNames(Map.of())).contains("GSSAPI"))
                            return true;
                    return false;
                }
                return Set.of("sasl-plain", "sasl-scram-sha-256", "sasl-scram-sha-512").contains(value);
            });
        }

        /**
         * 业务作用：通过实际压缩和解压验证可选实现可加载，安全算法只声明本地实现存在。
         * <p>
         * 参数说明：无。
         *
         * @return 不可变支持集，不表示Kerberos环境或远端身份已验证。
         */
        private static Set<String> detect() {
            Set<String> values = new TreeSet<>();
            for (String capability : List.of("tls", "mtls", "sasl-plain", "sasl-scram-sha-256", "sasl-scram-sha-512", "sasl-gssapi", "compression-none", "compression-gzip", "compression-snappy", "compression-lz4", "compression-zstd"))
                if (supported(capability)) values.add(capability);
            return Collections.unmodifiableSet(values);
        }

        /**
         * 业务作用：避免依赖存在但原生库无法加载时仍开放压缩通道。
         *
         * @param codec 固定算法名。
         * @return 本地完整往返成功才支持；不包含任何业务数据。
         */
        private static boolean compressionAvailable(String codec) {
            try {
                Compression compression = Compression.of(codec).build();
                ByteBufferOutputStream buffer = new ByteBufferOutputStream(256);
                try (var output = compression.wrapForOutput(buffer, (byte) 2)) {
                    output.write(new byte[]{1, 2, 3});
                }
                var encoded = buffer.buffer().duplicate();
                encoded.flip();
                try (var input = compression.wrapForInput(encoded, (byte) 2, BufferSupplier.NO_CACHING)) {
                    return Arrays.equals(input.readAllBytes(), new byte[]{1, 2, 3});
                }
            } catch (Exception | LinkageError unavailable) {
                return false;
            }
        }
    }
}

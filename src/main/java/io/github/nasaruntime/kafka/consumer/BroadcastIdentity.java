package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * 根据明确业务作用域与实例身份生成双端一致的广播消费组，不依赖代码类名。
 */
public final class BroadcastIdentity {

    private static final String PREFIX = "nasa-kafka-broadcast-";

    /**
     * 业务作用：禁止构造无状态身份工具。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：仅限制实例构造，不建立连接、启动线程或创建业务状态。
     */
    private BroadcastIdentity() {}

    /**
     * 业务作用：派生稳定的广播位点归属。
     *
     * @param scope      业务作用域
     * @param instanceId 部署实例身份，均为不含控制字符的非空 UTF-8、最多 4096 字节且首尾不含 ASCII 空格。
     * @return 最多 255 ASCII 字节的 group；短值无损编码，长值使用 SHA-256 摘要。
     */
    public static String group(String scope, String instanceId) {
        byte[] scopeBytes = component(scope), instanceBytes = component(instanceId);
        HexFormat hex = HexFormat.of();
        String encoded = PREFIX + "b" + hex.formatHex(scopeBytes) + "." + hex.formatHex(instanceBytes);
        if (encoded.length() <= 255) return encoded;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update("nasa.kafka.broadcast\0".getBytes(StandardCharsets.UTF_8));
            // 长度前缀保留两个身份分量的边界，不能让不同 scope/instance 拼接成相同摘要输入。
            digest.update(ByteBuffer.allocate(4).putInt(scopeBytes.length).array());
            digest.update(scopeBytes);
            digest.update(ByteBuffer.allocate(4).putInt(instanceBytes.length).array());
            digest.update(instanceBytes);
            return PREFIX + "h" + hex.formatHex(digest.digest());
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("JVM 缺少 SHA-256", unavailable);
        }
    }

    /**
     * 业务作用：拒绝含糊或不可跨语言复现的身份声明。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param value 作用域或实例身份。
     * @return 精确 UTF-8，不执行替换、裁剪或 Unicode 归一化；非法声明在建组前拒绝。
     */
    public static byte[] component(String value) {
        if (value == null || value.isEmpty() || value.length() > 4096 || value.startsWith(" ") || value.endsWith(" ")
                || value.codePoints().anyMatch(c -> Character.getType(c) == Character.CONTROL))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "广播身份必须非空、无控制字符及首尾 ASCII 空格，且最多 4096 UTF-8 字节");
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(value));
            if (encoded.remaining() > 4096)
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "广播身份超过 4096 UTF-8 字节");
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException invalid) {
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "广播身份含不可编码字符");
        }
    }
}

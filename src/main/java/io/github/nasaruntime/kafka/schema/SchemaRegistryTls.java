package io.github.nasaruntime.kafka.schema;


import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;

/**
 * Registry 专用 TLS 材料；不继承 Kafka 认证域，不提供关闭证书或主机名验证的选项。
 */
public final class SchemaRegistryTls {

    private static final int MAX_MATERIAL_BYTES = 1024 * 1024;
    private final byte[] caPem;
    private final byte[] identityPem;

    /**
     * 业务作用：冻结独立的信任锚与客户端身份。
     * <p>
     * 返回：保存独立副本；任一非 null 材料为空数组或超过一 MiB 时拒绝，两项均 null 表示使用默认 TLS 材料。
     *
     * @param caPem       可空 PEM 证书集合，提供时替代 JVM 默认信任锚
     * @param identityPem 可空证书链与一个未加密 PKCS#8 私钥。
     */
    public SchemaRegistryTls(byte[] caPem, byte[] identityPem) {
        for (byte[] value : new byte[][]{caPem, identityPem})
            if (value != null && (value.length == 0 || value.length > MAX_MATERIAL_BYTES)) throw invalid();
        this.caPem = caPem == null ? null : caPem.clone();
        this.identityPem = identityPem == null ? null : identityPem.clone();
    }

    /**
     * 业务作用：在连接前解析身份并建立受限 TLS 上下文。
     * <p>
     * 参数说明：无。
     *
     * @return 完成信任锚和客户端身份装配的 SSLContext；主机名验证由 HTTP 客户端的 SSLParameters 启用，非法材料以固定配置错误拒绝。
     */
    SSLContext context() {
        try {
            TrustManager[] trust = null;
            if (caPem != null) {
                KeyStore anchors = KeyStore.getInstance("PKCS12");
                anchors.load(null, null);
                int index = 0;
                for (Certificate certificate : certificates(caPem))
                    anchors.setCertificateEntry("anchor-" + index++, certificate);
                TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                // 显式私有信任域替代默认根，不能意外让其它系统 CA 授权此 Registry。
                factory.init(anchors);
                trust = factory.getTrustManagers();
            }
            KeyManager[] keys = null;
            if (identityPem != null) {
                Certificate[] chain = certificates(identityPem);
                byte[] encoded = privateKey(identityPem);
                try {
                    String algorithm = chain[0].getPublicKey().getAlgorithm();
                    PrivateKey key = KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(encoded));
                    String signature = switch (algorithm) {
                        case "RSA" -> "SHA256withRSA";
                        case "EC" -> "SHA256withECDSA";
                        case "EdDSA", "Ed25519" -> "Ed25519";
                        default -> throw invalid();
                    };
                    Signature proof = Signature.getInstance(signature);
                    byte[] challenge = new byte[32];
                    new SecureRandom().nextBytes(challenge);
                    proof.initSign(key);
                    proof.update(challenge);
                    byte[] signed = proof.sign();
                    proof.initVerify(chain[0]);
                    proof.update(challenge);
                    if (!proof.verify(signed)) throw invalid();
                    KeyStore identity = KeyStore.getInstance("PKCS12");
                    identity.load(null, null);
                    char[] password = new char[0];
                    identity.setKeyEntry("registry-client", key, password, chain);
                    KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    factory.init(identity, password);
                    keys = factory.getKeyManagers();
                } finally {
                    Arrays.fill(encoded, (byte) 0);
                }
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys, trust, null);
            return context;
        } catch (Exception rejected) {
            // 原生解析异常可能带材料或路径，只向业务暴露固定配置分类。
            throw invalid();
        }
    }

    /**
     * 业务作用：只从 PEM 中读取有限证书链，不把私钥段交给证书解析器。
     *
     * @param pem 有界材料。
     * @return 一至十六个 X.509 证书，缺失或格式非法拒绝。
     */
    private static Certificate[] certificates(byte[] pem) throws Exception {
        List<Certificate> result = new ArrayList<>();
        int cursor = 0;
        byte[] begin = "-----BEGIN CERTIFICATE-----".getBytes(StandardCharsets.US_ASCII);
        byte[] end = "-----END CERTIFICATE-----".getBytes(StandardCharsets.US_ASCII);
        if (occurrences(pem, begin) != occurrences(pem, end)) throw invalid();
        while (true) {
            int first = indexOf(pem, begin, cursor);
            if (first < 0) break;
            int last = indexOf(pem, end, first + begin.length);
            if (last < 0 || result.size() == 16) throw invalid();
            Certificate certificate = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(pem, first, last + end.length - first));
            if (!(certificate instanceof X509Certificate)) throw invalid();
            result.add(certificate);
            cursor = last + end.length;
        }
        if (result.isEmpty()) throw invalid();
        return result.toArray(Certificate[]::new);
    }

    /**
     * 业务作用：限制私钥输入为一个未加密 PKCS#8 段，避免兼容路径静默选择错误身份。
     *
     * @param pem 含证书链的身份材料。
     * @return 待解析 DER 副本，由调用方清零；缺失或重复拒绝。
     */
    private static byte[] privateKey(byte[] pem) {
        byte[] begin = "-----BEGIN PRIVATE KEY-----".getBytes(StandardCharsets.US_ASCII);
        byte[] end = "-----END PRIVATE KEY-----".getBytes(StandardCharsets.US_ASCII);
        int first = indexOf(pem, begin, 0), last = indexOf(pem, end, Math.max(0, first) + begin.length);
        if (first < 0 || last < 0 || occurrences(pem, begin) != 1 || occurrences(pem, end) != 1
                || indexOf(pem, "-----BEGIN RSA PRIVATE KEY-----".getBytes(StandardCharsets.US_ASCII), 0) >= 0
                || indexOf(pem, "-----BEGIN EC PRIVATE KEY-----".getBytes(StandardCharsets.US_ASCII), 0) >= 0
                || indexOf(pem, "-----BEGIN ENCRYPTED PRIVATE KEY-----".getBytes(StandardCharsets.US_ASCII), 0) >= 0)
            throw invalid();
        byte[] body = Arrays.copyOfRange(pem, first + begin.length, last);
        try {
            return Base64.getMimeDecoder().decode(body);
        } finally {
            Arrays.fill(body, (byte) 0);
        }
    }

    /**
     * 业务作用：在有界材料中定位 PEM 边界，避免把私钥转成不可清零的 String。
     *
     * @param value  材料
     * @param marker 固定边界
     * @param start  扫描起点。
     * @return 边界起点或未找到的负值。
     */
    private static int indexOf(byte[] value, byte[] marker, int start) {
        outer:
        for (int i = start; i <= value.length - marker.length; i++) {
            for (int j = 0; j < marker.length; j++) if (value[i + j] != marker[j]) continue outer;
            return i;
        }
        return -1;
    }

    /**
     * 业务作用：校验证书和私钥边界成对出现，避免静默忽略多余或不完整身份。
     *
     * @param value  有界材料
     * @param marker 固定边界。
     * @return 精确匹配数量。
     */
    private static int occurrences(byte[] value, byte[] marker) {
        int count = 0, cursor = 0;
        while ((cursor = indexOf(value, marker, cursor)) >= 0) {
            count++;
            cursor += marker.length;
        }
        return count;
    }

    /**
     * 业务作用：对材料解析失败提供不含认证内容的稳定分类。
     * <p>
     * 参数说明：无。
     *
     * @return 未附带原生解析文本的配置异常。
     */
    private static SchemaRegistryException invalid() {
        return new SchemaRegistryException(SchemaRegistryException.Code.INVALID_CONFIGURATION);
    }

    /**
     * 业务作用：阻止诊断输出泄露 TLS 身份材料。
     * <p>
     * 参数说明：无。
     *
     * @return 固定脱敏表示。
     */
    @Override
    public String toString() {
        return "SchemaRegistryTls[ca=<redacted>, identity=<redacted>]";
    }
}

package io.github.nasaruntime.kafka.transaction;

import io.github.nasaruntime.kafka.dlt.DltEvidence;

import io.github.nasaruntime.kafka.dlt.DltReplayException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * 稳定部署身份；同槽恢复必须复用，所有同时活动的槽必须互异。
 */
public record TransactionIdentity(String application, String slot, String owner) {

    /**
     * 业务作用：拒绝不能跨重启精确复现的空身份或歧义编码。
     * <p>
     * 返回：不裁剪、不规范化文本的身份。
     *
     * @param application 稳定应用身份，非空、无 NUL、严格 UTF-8 且最多 256 字节。
     * @param slot        稳定部署槽身份，非空、无 NUL、严格 UTF-8 且最多 256 字节。
     * @param owner       槽内唯一事务工作单元身份，非空、无 NUL、严格 UTF-8 且最多 256 字节。
     */
    public TransactionIdentity {
        text(application, 256);
        text(slot, 256);
        text(owner, 256);
    }

    /**
     * 业务作用：让 Java 和 Rust 用相同部署身份接管未完成事务。
     * <p>
     * 参数说明：无。
     *
     * @return NSKTXI01 加三个大端 u16 长度前缀 UTF-8 字段的 SHA-256，前缀为 nasa-tx-；不包含随机启动标识。
     */
    public String transactionalId() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.write("NSKTXI01".getBytes(StandardCharsets.US_ASCII));
            for (String value : new String[]{application, slot, owner}) {
                byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
                out.writeShort(encoded.length);
                out.write(encoded);
            }
            return "nasa-tx-" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (IOException | NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    /**
     * 业务作用：验证部署身份与源、集群字段的有界唯一编码。
     * <p>
     * 返回：空值、NUL、超长或非法 UTF-8 表达以事务 INVALID_CONFIGURATION 拒绝；实际消费组另受共同组名边界约束。
     *
     * @param value 精确身份字段
     * @param max   字节上限。
     */
    static void text(String value, int max) {
        try {
            DltEvidence.boundedText(value, max, false);
        } catch (DltReplayException invalid) {
            throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
        }
        if (value.indexOf('\0') >= 0) throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
    }
}

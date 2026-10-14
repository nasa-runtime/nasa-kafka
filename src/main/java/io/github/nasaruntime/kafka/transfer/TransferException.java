package io.github.nasaruntime.kafka.transfer;

import java.util.Objects;


/**
 * 单目标转发的稳定失败合同，不包含消息正文、schema 正文或连接凭据。
 */
public final class TransferException extends RuntimeException {

    public enum Code {
        INVALID_CONFIGURATION, CORRUPT_PLAN, CONTRACT_MISMATCH, TARGET_IDENTITY_CHANGED,
        TARGET_PARTITIONS_CHANGED, OBSERVATION_FAILED, AUTHORIZATION, TIMEOUT, UNCONFIRMED_LANE,
        SCHEMA_UNAPPROVED, SCHEMA_MISMATCH, HEADER_INVALID, LOOP_DETECTED, HOP_LIMIT,
        DELIVERY_FAILED, OUTCOME_UNKNOWN, CLOSED, CAPACITY
    }

    private final Code code;

    /**
     * 业务作用：把转发失败固定为不依赖底层客户端的类别。
     * <p>
     * 返回：脱敏异常，不携带原始远端响应。
     *
     * @param code 失败原因。
     */
    public TransferException(Code code) {
        super("Kafka transfer: " + code);
        this.code = Objects.requireNonNull(code);
    }

    /**
     * 业务作用：区分身份、schema、链路证据和目标交付失败。
     * <p>
     * 参数说明：无。
     *
     * @return 稳定失败类别。
     */
    public Code code() {
        return code;
    }
}

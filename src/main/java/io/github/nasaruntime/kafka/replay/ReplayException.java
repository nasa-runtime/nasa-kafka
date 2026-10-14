package io.github.nasaruntime.kafka.replay;

import java.util.Objects;


/**
 * 有限回放的封闭失败类别，不携带连接凭据或消息正文。
 */
public final class ReplayException extends RuntimeException {

    public enum Code {
        INVALID_CONFIGURATION, OBSERVATION_FAILED, AUTHORIZATION, TIMEOUT, IDENTITY_CHANGED,
        PARTITIONS_CHANGED, DATA_GAP, END_REGRESSED, CONTRACT_MISMATCH, TIMESTAMP_MISSING, CORRUPT_PLAN, TOPIC_MISSING, CLOSED,
        BUSY, CAPACITY, PROCESSING_FAILED, CHECKPOINT_FAILED
    }

    private final Code code;

    /**
     * 业务作用：把规划、恢复和实际执行失败固定为可操作类别。
     * <p>
     * 返回：不附带底层敏感诊断的异常。
     *
     * @param code 失败原因。
     */
    public ReplayException(Code code) {
        super("Kafka replay: " + code);
        this.code = Objects.requireNonNull(code);
    }

    /**
     * 业务作用：供管理者区分证据变化、数据缺口和暂时不可观测。
     * <p>
     * 参数说明：无。
     *
     * @return 稳定失败类别。
     */
    public Code code() {
        return code;
    }
}

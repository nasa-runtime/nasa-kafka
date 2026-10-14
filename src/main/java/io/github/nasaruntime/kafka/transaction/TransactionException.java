package io.github.nasaruntime.kafka.transaction;

import java.util.Objects;


/**
 * 事务专属失败分类，不把事务内发送确认解释为最终交付。
 */
public final class TransactionException extends RuntimeException {

    public enum Code {
        INVALID_CONFIGURATION, CLOSED, BUSY, CAPACITY, CLUSTER_MISMATCH, ISOLATION_MISMATCH,
        AUTHORIZATION, FENCED, REBALANCED, HANDLER_FAILED, HANDLER_TIMEOUT, TRANSACTION_TIMEOUT,
        MISSING_ACK, CONTEXT_CLOSED, CANCELLED, COMMIT_UNKNOWN, ABORT_UNKNOWN, BROKER_ERROR
    }

    private final Code code;

    /**
     * 业务作用：保留稳定且不含连接凭据的失败原因。
     * <p>
     * 返回：仅携带固定分类的异常。
     *
     * @param code 事务阶段分类。
     */
    public TransactionException(Code code) {
        super(Objects.requireNonNull(code).name());
        this.code = code;
    }

    /**
     * 业务作用：供调用方区分重建身份、重新分配与业务失败。
     * <p>
     * 参数说明：无。
     *
     * @return 固定分类，不推断 broker 最终结果。
     */
    public Code code() {
        return code;
    }
}

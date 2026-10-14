package io.github.nasaruntime.kafka.dlt;

import java.util.Objects;

/**
 * DLT 重放的稳定失败分类，不附带消息正文、原始 SQL 或连接凭据。
 */
public final class DltReplayException extends RuntimeException {

    public enum Code {
        /**
         * 声明不完整、越界或有歧义。
         */
        INVALID_CONFIGURATION,
        /**
         * 持久计划损坏、截断或非规范。
         */
        CORRUPT_PLAN,
        /**
         * 冻结处理合同与实际入口不一致。
         */
        CONTRACT_MISMATCH,
        /**
         * 输入没有 DLT 来源证据，不能作为死信回投。
         */
        MISSING_EVIDENCE,
        /**
         * 来源证据不完整、重复、损坏或非法。
         */
        HEADER_INVALID,
        /**
         * 已达到显式批准的回投次数。
         */
        REPLAY_LIMIT,
        /**
         * 目标物理身份或资源代次改变。
         */
        TARGET_IDENTITY_CHANGED,
        /**
         * 目标完整分区集合改变。
         */
        TARGET_PARTITIONS_CHANGED,
        /**
         * 当前资源或 schema 证据不可观测。
         */
        OBSERVATION_FAILED,
        /**
         * broker 明确拒绝来源或目标资源权限。
         */
        AUTHORIZATION,
        /**
         * 操作预算耗尽。
         */
        TIMEOUT,
        /**
         * lane 未配置 acks=all 或 -1 所要求的 ISR 确认合同。
         */
        UNCONFIRMED_LANE,
        /**
         * 来源 schema 不在批准集合中。
         */
        SCHEMA_UNAPPROVED,
        /**
         * 双端 schema 正文或类别不符。
         */
        SCHEMA_MISMATCH,
        /**
         * 目标交付明确未成功。
         */
        DELIVERY_FAILED,
        /**
         * 目标实际副作用无法确定。
         */
        OUTCOME_UNKNOWN,
        /**
         * 来源或目标已经关闭新任务准入。
         */
        CLOSED,
        /**
         * 任务或证据容量耗尽。
         */
        CAPACITY
    }

    private final Code code;

    /**
     * 业务作用：让调用方区分证据拒绝、交付失败与结果未知，不泄露底层输入。
     * <p>
     * 返回：没有底层异常原因链的领域异常。
     *
     * @param code 封闭失败类别。
     */
    public DltReplayException(Code code) {
        super("Kafka DLT replay: " + Objects.requireNonNull(code));
        this.code = code;
    }

    /**
     * 业务作用：提供可用于保护任务和诊断的稳定类别。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变分类，OUTCOME_UNKNOWN 不能解释为未写入。
     */
    public Code code() {
        return code;
    }
}

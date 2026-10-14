package io.github.nasaruntime.kafka.checkpoint;

import java.util.Objects;


/**
 * 持久回放进度的稳定失败分类，不暴露本地路径、任务正文或底层存储异常。
 */
public final class CheckpointException extends RuntimeException {

    public enum Code {
        /**
         * 身份、序号、范围或资源声明非法，尚未据此推进。
         */
        INVALID_CONFIGURATION,
        /**
         * 任务已有活动所有者。
         */
        BUSY,
        /**
         * 冻结合同、分区集合或读取范围发生变化。
         */
        CONTRACT_MISMATCH,
        /**
         * 条件更新序号不匹配，未写入新进度。
         */
        STALE_REVISION,
        /**
         * lease 失权或无法复验既有持久证据。
         */
        FENCED,
        /**
         * 文件完整性、格式或范围不可信，不能据此恢复。
         */
        CORRUPT,
        /**
         * 存储读取、锁或资源关闭失败。
         */
        IO,
        /**
         * 持久化结果未知，不能以旧内存状态继续推进。
         */
        OUTCOME_UNKNOWN,
        /**
         * lease 已永久关闭。
         */
        CLOSED,
        /**
         * 活动任务数、文件容量或序号耗尽。
         */
        CAPACITY
    }

    private final Code code;

    /**
     * 业务作用：以稳定分类报告进度准入、所有权或持久化失败。
     * <p>
     * 返回：不包含底层敏感原因链的异常。
     *
     * @param code 封闭分类。
     */
    public CheckpointException(Code code) {
        super("Kafka checkpoint: " + code);
        this.code = Objects.requireNonNull(code);
    }

    /**
     * 业务作用：让回放控制器区分拒绝、失权与持久化未知。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变失败分类，OUTCOME_UNKNOWN 不能当作未写入。
     */
    public Code code() {
        return code;
    }
}

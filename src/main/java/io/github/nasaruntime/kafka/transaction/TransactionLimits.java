package io.github.nasaruntime.kafka.transaction;


/**
 * 输入、暂存输出和执行时间的硬性准入预算；超时不证明用户回调已经退出。
 */
public record TransactionLimits(int maxBatchRecords, long maxBatchBytes, int maxOutputRecords,
                                long maxOutputBytes, int handlerTimeoutMs, int transactionTimeoutMs,
                                int operationTimeoutMs, int maxAbortedRetries, int retryBackoffMs) {

    public static final TransactionLimits DEFAULT = new TransactionLimits(100, 8L * 1024 * 1024,
            1000, 16L * 1024 * 1024, 10000, 60000, 10000, 3, 250);

    /**
     * 业务作用：固定单批事务可占用的内存、时间和已确认中止后的重试次数。
     * <p>
     * 返回：合法预算，未知提交结果不消耗该重试额度且不能自动重发。
     *
     * @param maxBatchRecords      单批输入记录数上限，范围1至4096。
     * @param maxBatchBytes        单批输入责任字节上限，范围1至64MiB。
     * @param maxOutputRecords     单批暂存输出记录数上限，范围1至4096。
     * @param maxOutputBytes       单批暂存输出责任字节上限，范围1至64MiB。
     * @param handlerTimeoutMs     业务回调的毫秒期限，与operationTimeoutMs之和必须小于事务期限。
     * @param transactionTimeoutMs 事务总毫秒期限，范围1000至900000，必须容纳回调及后续原生操作。
     * @param operationTimeoutMs   单次原生事务操作的毫秒预算，必须为正并与回调预算一起受总期限约束。
     * @param maxAbortedRetries    仅在确认abort后允许的重试次数，范围0至100；未知提交结果不自动重发。
     * @param retryBackoffMs       确认abort后的重试退避毫秒数，范围10至60000。
     */
    public TransactionLimits {
        if (maxBatchRecords < 1 || maxBatchRecords > 4096 || maxOutputRecords < 1 || maxOutputRecords > 4096
                || maxBatchBytes < 1 || maxBatchBytes > 64L * 1024 * 1024 || maxOutputBytes < 1 || maxOutputBytes > 64L * 1024 * 1024
                || transactionTimeoutMs < 1000 || transactionTimeoutMs > 900000 || handlerTimeoutMs < 1 || operationTimeoutMs < 1
                || (long) handlerTimeoutMs + operationTimeoutMs >= transactionTimeoutMs
                || maxAbortedRetries < 0 || maxAbortedRetries > 100 || retryBackoffMs < 10 || retryBackoffMs > 60000)
            throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
    }
}

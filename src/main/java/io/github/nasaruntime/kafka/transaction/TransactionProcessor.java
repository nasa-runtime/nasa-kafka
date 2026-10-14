package io.github.nasaruntime.kafka.transaction;


/**
 * 单批同步业务处理；数据库、HTTP 和普通 lane 发布不属于本次 Kafka 原子范围。
 */
@FunctionalInterface
public interface TransactionProcessor {

    /**
     * 业务作用：在有界批次内确认输入并暂存事务输出，正常返回才允许框架准备提交。
     * <p>
     * 返回：无直接交付回执；异常或缺少 Manual 确认使整批中止，可能以相同输入再次调用。
     *
     * @param context 仅当前回调线程可操作的事务上下文。
     */
    void process(TransactionContext context) throws Exception;
}

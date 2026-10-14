package io.github.nasaruntime.kafka.consumer.listener;

import io.github.nasaruntime.kafka.consumer.PassthroughDisposition;
import io.github.nasaruntime.kafka.consumer.RawKafkaRecord;


/**
 * 同步只读透传入口。只执行有界解析与非阻塞本地准入，通用异步业务应使用 owned record 入口。
 * 回调在受限 worker 内运行，仍不能强制中断；实际退出前持续占用执行责任。
 */
public interface KafkaPassthroughListener extends KafkaListener<byte[], RawKafkaRecord> {

    /**
     * 业务作用：按原文字节作本地准入决定。
     *
     * @param record 本线程本次回调有效的只读视图。
     * @return 明确成功条件；失败抛出 PassthroughFailure，null 返回不构成成功。
     */
    PassthroughDisposition onRecord(RawKafkaRecord record);

    /**
     * 业务作用：防止通用事件分发丢失透传结果语义。
     * <p>
     * 返回：始终拒绝，框架必须使用 onRecord。
     *
     * @param record 只读视图。
     */
    @Override
    default void onEvent(RawKafkaRecord record) {
        throw new IllegalStateException("透传 listener 必须使用受管结果入口");
    }

    /**
     * 业务作用：声明透传保留 tombstone 的原始状态。
     * <p>
     * 参数说明：无。
     *
     * @return 始终允许 null payload，是否消费由业务决定。
     */
    @Override
    default boolean nullable() {
        return true;
    }
}

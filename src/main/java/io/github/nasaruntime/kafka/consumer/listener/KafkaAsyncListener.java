package io.github.nasaruntime.kafka.consumer.listener;

import io.github.nasaruntime.kafka.consumer.ConsumeContext;
import io.github.nasaruntime.kafka.consumer.KafkaRecord;

import java.util.concurrent.CompletionStage;

/**
 * 异步 record 消费入口，异步结果正常结束后才结束本次确认调用。
 */
public interface KafkaAsyncListener<T> extends KafkaRecordListener<T> {

    /**
     * 业务作用：处理记录并显式携带来源上下文。
     *
     * @param record  记录
     * @param context 不可变调用上下文。
     * @return 整个业务处理的最终结果。
     */
    CompletionStage<Void> onRecord(KafkaRecord<T> record, ConsumeContext context);

    /**
     * 业务作用：阻止异步 listener 被同步路径提前确认。
     * <p>
     * 返回：总是拒绝错误调用路径。
     *
     * @param record 输入记录。
     */
    @Override
    default void onEvent(KafkaRecord<T> record) {
        throw new IllegalStateException("请通过异步 record 分发入口调用");
    }
}

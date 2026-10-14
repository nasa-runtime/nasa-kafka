package io.github.nasaruntime.kafka.consumer.listener;


import io.github.nasaruntime.kafka.consumer.ConsumeContext;
import io.github.nasaruntime.kafka.consumer.KafkaRecord;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * 连续分区批次的异步消费入口，future 终态决定本次确认是否生效。
 */
public interface KafkaAsyncBatchListener<T> extends KafkaRecordBatchListener<T> {

    /**
     * 业务作用：处理同分区同路由的连续记录。
     *
     * @param records 批次
     * @param context 本次消费的不可变来源及代次描述，本身不授予确认权；手动确认使用各记录的 AckToken。
     * @return CompletionStage 正常完成且调用仍有效时按确认模式结算；MANUAL 还要求记录显式确认，失败或超时不推进未确认责任。
     */
    CompletionStage<Void> onRecords(List<KafkaRecord<T>> records, ConsumeContext context);

    /**
     * 业务作用：防止异步处理被当成已完成同步调用。
     * <p>
     * 返回：总是拒绝，运行时使用 onRecords。
     *
     * @param records 批次。
     */
    @Override
    default void onEvent(List<KafkaRecord<T>> records) {
        throw new UnsupportedOperationException("使用 onRecords");
    }
}

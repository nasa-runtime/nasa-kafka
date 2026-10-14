package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.consumer.listener.KafkaBatchListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaPassthroughListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaRecordBatchListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaRecordListener;


/**
 * 枚举作用 - Kafka listener 分发类型, 用于区分单条/批量以及是否携带 KafkaRecord 元数据。
 */
public enum ListenerKind {

    /**
     * 单条业务对象消费。
     */
    SINGLE,
    /**
     * 批量业务对象消费。
     */
    BATCH,
    /**
     * 单条 KafkaRecord 消费。
     */
    RECORD_SINGLE,
    /**
     * 批量 KafkaRecord 消费。
     */
    RECORD_BATCH,
    /**
     * 同步只读原文与显式结果消费。
     */
    PASSTHROUGH;

    /**
     * 业务作用：按受管 listener 接口选择借用原文、record 批量、普通批量、record 单条或普通单条分发。
     * 先判断需要专用参数的接口；自定义对象实现多个接口时按此顺序选择，不能根据 payload 类型猜测路线。
     *
     * @param listener 待判定的 listener 实例
     * @return 该 listener 的分发形态；未匹配任何专用接口时按单条消费处理。
     */
    @SuppressWarnings("rawtypes")
    public static ListenerKind of(KafkaListener listener) {
        if (listener instanceof KafkaPassthroughListener) return PASSTHROUGH;
        if (listener instanceof KafkaRecordBatchListener) return RECORD_BATCH;
        if (listener instanceof KafkaBatchListener) return BATCH;
        if (listener instanceof KafkaRecordListener) return RECORD_SINGLE;
        return SINGLE;
    }
}

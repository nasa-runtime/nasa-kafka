package io.github.nasaruntime.kafka;

/**
 * 枚举作用 - Kafka listener 分发类型, 用于区分单条/批量以及是否携带 KafkaRecord 元数据。
 */
public enum ListenerKind {

    /** 单条业务对象消费。 */
    SINGLE,
    /** 批量业务对象消费。 */
    BATCH,
    /** 单条 KafkaRecord 消费。 */
    RECORD_SINGLE,
    /** 批量 KafkaRecord 消费。 */
    RECORD_BATCH;

    /**
     * 业务作用：按 listener 实现的接口判定分发形态，决定框架是逐条回调、批量聚合后回调，
     * 还是把 Kafka 元数据一并包成 record 传入。
     * <p>
     * 判定顺序从最具体到最宽泛：批量 record、批量、单条 record、单条。
     * 顺序不能调整——批量 record 接口同时满足单条判定条件，先判宽泛类型会把它错分成单条消费。
     *
     * @param listener 待判定的 listener 实例
     * @return 该 listener 的分发形态；未匹配任何专用接口时按单条消费处理。
     */
    @SuppressWarnings("rawtypes")
    static ListenerKind of(KafkaListener listener) {
        if (listener instanceof KafkaRecordBatchListener) return RECORD_BATCH;
        if (listener instanceof KafkaBatchListener) return BATCH;
        if (listener instanceof KafkaRecordListener) return RECORD_SINGLE;
        return SINGLE;
    }
}

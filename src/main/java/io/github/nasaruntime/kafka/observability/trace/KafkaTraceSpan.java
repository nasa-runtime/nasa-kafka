package io.github.nasaruntime.kafka.observability.trace;

import java.util.List;

/**
 * 完成后交付后端的有限链路数据；不持有 payload、Kafka 客户端或确认能力。
 */
public record KafkaTraceSpan(String sourceId, String group, String topic, Kind kind, KafkaTraceContext context,
                             KafkaTraceContext parent, List<KafkaTraceContext> links, int omittedLinks,
                             long startedAtNanos, long durationNanos, Outcome outcome) {

    public enum Kind {PRODUCER, CONSUMER}

    /**
     * 描述实际执行或交付观察结果，不替代消费提交和业务幂等结论。
     */
    public enum Outcome {RETURNED, FAILED, AUTHORITY_LOST, DELIVERY_UNKNOWN}

    /**
     * 业务作用：冻结完成 span，禁止后端持有可变批链接或接收无界元数据。
     * <p>
     * 返回：不可变有限记录，非法内部声明拒绝。
     *
     * @param sourceId       实际执行或交付所归属的命名数据源。
     * @param group          消费调用所属的组身份。
     * @param topic          本次执行或交付关联的topic。
     * @param kind           本次span表示生产交付还是消费执行。
     * @param context        本次span自身的trace和span身份。
     * @param parent         有效入站父trace上下文，无父关系时为null。
     * @param links          批处理中其它独立父上下文的链接快照，最多128项。
     * @param omittedLinks   超出链接容量而未保留的有效链接数量，必须非负。
     * @param startedAtNanos span开始的Unix纳秒时刻，用于后端时间定位。
     * @param durationNanos  由单调时钟测得的执行耗时纳秒数，必须非负。
     * @param outcome        本地执行或交付观察到的结局，不替代消费提交或业务幂等判断。
     */
    public KafkaTraceSpan {
        links = List.copyOf(links);
        if (links.size() > 128 || omittedLinks < 0 || durationNanos < 0)
            throw new IllegalArgumentException("trace span 边界无效");
    }
}

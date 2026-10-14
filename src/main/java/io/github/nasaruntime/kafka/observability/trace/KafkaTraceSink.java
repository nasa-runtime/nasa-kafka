package io.github.nasaruntime.kafka.observability.trace;


/**
 * 应用拥有的 trace 出口；与指标共享每源有界观测线程及关闭责任。
 */
@FunctionalInterface
public interface KafkaTraceSink {

    /**
     * 业务作用：把已采样、实际结束的调用交给应用遥测后端。
     * <p>
     * 返回：回调正常返回只代表本地交付；
     * 异常计入出口失败，阻塞保留实际关闭责任，不影响业务确认。
     *
     * @param span 只含身份、链接、时间和结果的不可变记录。
     */
    void export(KafkaTraceSpan span);
}

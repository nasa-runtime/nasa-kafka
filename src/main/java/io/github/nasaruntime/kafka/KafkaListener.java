package io.github.nasaruntime.kafka;

import io.github.nasaruntime.core.evt.EventListener;

/**
 * 接口作用 - Kafka listener 根接口, 在通用 {@link EventListener} 基础上补充 topic 和 group 语义。
 *
 * @param <T> 消息体类型, 即 payload 反序列化后的业务类型
 * @param <TS> 回调入参类型, 单条消费为 T, 批量消费为 List&lt;T&gt;, record 消费为 KafkaRecord 包装类型
 */
public interface KafkaListener<T, TS> extends EventListener<T, TS> {

    /**
     * 业务作用：声明本 listener 订阅哪些 topic，是框架启动时建立「topic → listener」路由的依据。
     * 返回空数组等同于不订阅任何 topic，该 listener 不会被接线。
     *
     * <p>参数说明: 无。
     *
     * @return 订阅的 topic 名数组。
     */
    String[] topics();

    /**
     * 业务作用：声明本 listener 使用的 consumer group，决定它与同类实例之间是竞争消费还是各自消费。
     * 返回 null 表示沿用全局配置的默认 group，此时同一服务的多个实例竞争消费同一份消息。
     * 广播语义由子接口重写本方法为「每实例独立 group」实现。
     *
     * <p>参数说明: 无。
     *
     * @return group.id；为 null 时回退到配置的默认 group。
     */
    default String group() {
        return null;
    }
}

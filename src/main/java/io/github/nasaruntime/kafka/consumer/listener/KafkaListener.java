package io.github.nasaruntime.kafka.consumer.listener;


import io.github.nasaruntime.kafka.consumer.AckMode;
import io.github.nasaruntime.kafka.consumer.ExecutionMode;


import io.github.nasaruntime.core.evt.EventListener;
import org.springframework.aop.support.AopUtils;

/**
 * 接口作用 - Kafka listener 根接口, 在通用 {@link EventListener} 基础上补充 topic 和 group 语义。
 *
 * @param <T>  消息体类型, 即 payload 反序列化后的业务类型
 * @param <TS> 回调入参类型, 单条消费为 T, 批量消费为 List&lt;T&gt;, record 消费为 KafkaRecord 包装类型
 */
public interface KafkaListener<T, TS> extends EventListener<T, TS> {

    /**
     * 业务作用：声明本 listener 订阅哪些 topic，是框架启动时建立「topic → listener」路由的依据。
     * 返回空数组等同于不订阅任何 topic，该 listener 不会被接线。
     * <p>
     * 参数说明：无。
     *
     * @return 订阅的 topic 名数组。
     */
    String[] topics();

    /**
     * 业务作用：声明稳定、低基数的处理器观测身份，在注册时冻结。
     * <p>
     * 参数说明：无。
     *
     * @return 默认目标类名；不得包含事件 ID、业务键或运行时随机值。
     */
    default String metricId() {
        return AopUtils.getTargetClass(this).getName();
    }

    /**
     * 业务作用：声明当前 listener 的消费组身份，决定同组实例间的分区分担范围。
     * 未显式声明时沿用绑定源的默认组；声明 broadcastScope 时由源按广播身份规则派生组，不能再指定显式 group。
     * <p>
     * 参数说明：无。
     *
     * @return group.id 原文；为 null 或空串时回退配置默认组；其它显式值及解析后的组须非纯空白、无 NUL、无 ASCII 首尾控制字符或空格、严格 UTF-8 且最多1024字节，否则注册以 CONFIG 拒绝。
     */
    default String group() {
        return null;
    }

    /**
     * 业务作用：显式选择共同算法的广播作用域。
     * <p>
     * 参数说明：无。
     *
     * @return null 沿用普通或既有广播组声明，非空值与显式 group 互斥。
     */
    default String broadcastScope() {
        return null;
    }

    /**
     * 业务作用：覆盖广播实例身份以支持稳定接续。
     * <p>
     * 参数说明：无。
     *
     * @return null 使用源配置或进程身份；只允许与 broadcastScope 同时声明。
     */
    default String instanceId() {
        return null;
    }

    /**
     * 业务作用：声明消费来源，避免新增源导致 listener 自动多消费。
     * <p>
     * 参数说明：无。
     *
     * @return 显式源名集合；null 使用受管默认源。
     */
    default String[] qualifiers() {
        return null;
    }

    /**
     * 业务作用：选择回调成功或回调期手动确认。
     * <p>
     * 参数说明：无。
     *
     * @return 确认模式。
     */
    default AckMode ackMode() {
        return AckMode.AUTO;
    }

    /**
     * 业务作用：定义有序处理范围。
     * <p>
     * 参数说明：无。
     *
     * @return 默认同分区串行，可显式选择按键串行。
     */
    default ExecutionMode executionMode() {
        return ExecutionMode.PARTITION_ORDERED;
    }

    /**
     * 业务作用：声明本路由是否接收 tombstone 和 JSON null。
     * <p>
     * 参数说明：无。
     *
     * @return 默认拒绝空业务对象。
     */
    default boolean nullable() {
        return false;
    }
}

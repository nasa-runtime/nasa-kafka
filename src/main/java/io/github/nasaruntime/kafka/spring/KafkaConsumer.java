package io.github.nasaruntime.kafka.spring;

import io.github.nasaruntime.kafka.KafkaProxy;

import io.github.nasaruntime.kafka.consumer.AckMode;
import io.github.nasaruntime.kafka.consumer.ExecutionMode;

import java.lang.annotation.*;

/**
 * 在 Spring Bean 或显式注册对象的方法上声明冻结的消费路由。
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface KafkaConsumer {

    /**
     * 业务作用：声明路由 topic。
     * <p>
     * 参数说明：无。
     *
     * @return 至少一个确定 topic。
     */
    String[] topics();

    /**
     * 业务作用：声明业务事件。
     * <p>
     * 参数说明：无。
     *
     * @return 默认事件或显式路由。
     */
    String event() default KafkaProxy.DEFAULT_EVENT;

    /**
     * 业务作用：选择消费组。
     * <p>
     * 参数说明：无。
     *
     * @return 空值使用源默认组。
     */
    String group() default "";

    /**
     * 业务作用：声明共同算法的广播作用域。
     * <p>
     * 参数说明：无。
     *
     * @return 空值不开启，与 group 互斥。
     */
    String broadcastScope() default "";

    /**
     * 业务作用：声明稳定广播实例身份。
     * <p>
     * 参数说明：无。
     *
     * @return 空值沿用源或进程身份，需要非空 broadcastScope。
     */
    String instanceId() default "";

    /**
     * 业务作用：选择来源。
     * <p>
     * 参数说明：无。
     *
     * @return 空数组使用受管默认源。
     */
    String[] sources() default {};

    /**
     * 业务作用：选择确认边界。
     * <p>
     * 参数说明：无。
     *
     * @return 回调成功或 record 显式确认。
     */
    AckMode ackMode() default AckMode.AUTO;

    /**
     * 业务作用：选择顺序作用域。
     * <p>
     * 参数说明：无。
     *
     * @return 分区或业务键顺序。
     */
    ExecutionMode executionMode() default ExecutionMode.PARTITION_ORDERED;

    /**
     * 业务作用：声明空载荷接受能力。
     * <p>
     * 参数说明：无。
     *
     * @return 是否接收 tombstone/JSON null。
     */
    boolean nullable() default false;
}

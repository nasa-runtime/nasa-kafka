package io.github.nasaruntime.kafka.consumer.listener;

import io.github.nasaruntime.kafka.KafkaProxy;


/**
 * 接口作用 - Kafka 广播批量 record listener, 广播批量消费时每条消息都保留 KafkaRecord 元数据。
 *
 * @param <T> 消息体类型, {@link #paramType()} 返回的也是该业务类型
 */
public interface KafkaBroadcastRecordBatchListener<T> extends KafkaRecordBatchListener<T> {

    /**
     * 业务作用：为广播 listener 选择组身份解析路线。
     * 未声明 broadcastScope 时使用类名与本 JVM 广播实例标识，同 JVM 同类对象共享该组；不同部署实例应使用不同身份才能各自消费。
     * 声明 broadcastScope 时交由源解析作用域和实例身份；稳定身份可接续旧组位点，默认随机身份重启后不保证接续。
     * <p>
     * 参数说明：无。
     *
     * @return 未声明 broadcastScope 时返回既有组名；否则返回 null，由源按共同算法解析实例身份。
     */
    @Override
    default String group() {
        // 显式共同作用域由源解析实例身份；未选择时保留原组名，避免静默改变位点归属。
        return broadcastScope() == null ? KafkaProxy.broadcastGroup(getClass()) : null;
    }
}

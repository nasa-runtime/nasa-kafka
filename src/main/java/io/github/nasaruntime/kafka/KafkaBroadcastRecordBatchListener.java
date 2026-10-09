package io.github.nasaruntime.kafka;

/**
 * 接口作用 - Kafka 广播批量 record listener, 广播批量消费时每条消息都保留 KafkaRecord 元数据。
 *
 * @param <T> 消息体类型, {@link #paramType()} 返回的也是该业务类型
 */
public interface KafkaBroadcastRecordBatchListener<T> extends KafkaRecordBatchListener<T> {

    /**
     * 业务作用：为本 批量 record 广播 listener 生成当前 JVM 独享的 group.id，使每个实例各自消费一份全量消息。
     * 广播语义正是靠「每实例不同 group」实现的：同 group 内 Kafka 只把一条消息投给一个消费者，
     * 若各实例共用 group，本地缓存失效这类需要全实例都收到的通知就会只有一个实例收到。
     * <p>
     * 实例标识默认随机生成，因此进程重启后 group 名改变，旧 group 的已提交位点不再被使用——
     * 广播消费天然从当前配置的起点重新开始，不接管重启前的未消费消息。
     *
     * <p>参数说明: 无。
     *
     * @return 形如 {@code nasa-kafka-broadcast-<listener 类名>-<实例标识>} 的 group.id。
     */
    @Override
    default String group() {
        return KafkaProxy.broadcastGroup(getClass());
    }
}

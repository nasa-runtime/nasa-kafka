package io.github.nasaruntime.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

/**
 * 接口作用 - 手动分区消费 assign 模式的控制句柄。
 */
public interface AssignmentHandle {

    /**
     * 业务作用：暴露本次 assign 消费所用的 group.id，供调用方据此调用 KafkaProxy 上以 group 为键的
     * seek、pause、offset 查询等操作——assign 模式的 group 由调用方在 assign 时指定，句柄是唯一的回查途径。
     *
     * <p>参数说明: 无。
     *
     * @return 本次 assign 消费的 group.id。
     */
    String group();

    /**
     * 业务作用：读取该 consumer 当前实际绑定的分区，用于确认 assign 是否按预期生效。
     * assign 模式不参与 rebalance，绑定关系在 assign 时确定且不会自行变化。
     *
     * <p>参数说明: 无。
     *
     * @return 分区集合的快照；对返回值的修改不影响实际绑定。
     */
    Set<TopicPartition> partitions();

    /**
     * 业务作用：判断 poll 线程是否仍在工作，用于区分「消费正常但暂时无消息」与「消费已停止」。
     * poll 线程因致命错误退出后本方法转为 false，是外部健康检查的判据。
     *
     * <p>参数说明: 无。
     *
     * @return poll 线程仍在运行返回 true。
     */
    boolean running();

    /**
     * 业务作用：停止本次 assign 消费并注销其 group，释放 poll 线程与 consumer 连接。
     * 停止后该 group 不再登记在 KafkaProxy 中，此前基于本句柄取得的 group 名再调 seek、pause 等操作会失败。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    void stop();
}

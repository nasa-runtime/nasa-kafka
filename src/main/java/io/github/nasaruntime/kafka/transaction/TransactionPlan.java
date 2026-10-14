package io.github.nasaruntime.kafka.transaction;

import io.github.nasaruntime.kafka.consumer.AckMode;
import io.github.nasaruntime.kafka.exception.KafkaFailure;
import io.github.nasaruntime.kafka.protocol.KafkaWire;

import java.util.Set;

import org.apache.kafka.common.TopicPartition;

/**
 * 单一物理集群内的受限 subscribe 事务合同；输出只能写入显式批准的分区。
 */
public record TransactionPlan(String sourceId, String clusterId, TransactionIdentity identity, String group,
                              Set<String> inputTopics, Set<TopicPartition> outputs, AckMode ackMode,
                              TransactionLimits limits) {

    /**
     * 业务作用：冻结原子输入组、输出白名单及稳定 producer 身份。
     * <p>
     * 返回：不可变计划，组名原样保留；组名或合同非法时抛出事务 INVALID_CONFIGURATION，不建立资源。不支持手动 assign 或异步按键执行。
     *
     * @param sourceId    事务运行时绑定的命名数据源。
     * @param clusterId   事务输入和输出共同归属的实际Kafka集群身份。
     * @param identity    用于producer初始化和恢复的稳定部署事务身份。
     * @param group       满足共同组输入域的专属消费组，不自动裁剪或迁移位点。
     * @param inputTopics 完整事务输入订阅名单，数量1至256。
     * @param outputs     批准发送的事务输出topic和分区集合，数量1至4096。
     * @param ackMode     回调成功自动确认或要求逐条Manual确认的整批策略。
     * @param limits      输入、暂存输出、执行期限及确认abort后重试的固定预算。
     */
    public TransactionPlan {
        TransactionIdentity.text(sourceId, 256);
        TransactionIdentity.text(clusterId, 256);
        // 事务组也承载原生消费与提交身份，必须在计划开放执行前使用共同门禁，不能等待 native 构造才拒绝。
        validateGroup(group);
        if (identity == null || ackMode == null || limits == null || inputTopics == null || inputTopics.isEmpty()
                || inputTopics.size() > 256 || outputs == null || outputs.isEmpty() || outputs.size() > 4096) invalid();
        for (String topic : inputTopics) topic(topic);
        for (TopicPartition output : outputs) {
            if (output == null || output.partition() < 0) invalid();
            topic(output.topic());
        }
        inputTopics = Set.copyOf(inputTopics);
        outputs = Set.copyOf(outputs);
    }

    /**
     * 业务作用：以事务专属失败合同校验实际消费组，供计划冻结与执行复验共用。
     * <p>
     * 返回：满足共同组名边界时通过，非法时抛出事务 INVALID_CONFIGURATION，不裁剪或改名。
     *
     * @param group 原文组名，不接受缺省组选择符。
     */
    public static void validateGroup(String group) {
        try {
            KafkaWire.validateGroup(group);
        } catch (KafkaFailure invalid) {
            throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
        }
    }

    /**
     * 业务作用：拒绝原生客户端可能解释为不同目标的非法 topic。
     * <p>
     * 返回：合法 Kafka topic 名。
     *
     * @param value 明确订阅或发布目标。
     */
    static void topic(String value) {
        if (value == null || !value.matches("[A-Za-z0-9._-]{1,249}") || value.equals(".") || value.equals(".."))
            invalid();
    }

    /**
     * 业务作用：以稳定分类拒绝不完整原子合同。
     * <p>
     * 参数说明：无。
     * 返回：配置异常。
     */
    private static void invalid() {
        throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
    }
}

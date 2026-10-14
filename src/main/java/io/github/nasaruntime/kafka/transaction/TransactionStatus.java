package io.github.nasaruntime.kafka.transaction;


import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;

/**
 * 原子批次与实际线程责任的快照；终止资源与确定事务结果是两个不同维度。
 */
public record TransactionStatus(String transactionalId, String group, State state, long assignmentEpoch,
                                Set<TopicPartition> assignment, boolean ready, boolean drained,
                                TransactionException.Code failure, long committedBatches, long committedInputs,
                                long committedOutputs, long abortedBatches, Map<TopicPartition, Long> inputStarts,
                                Map<TopicPartition, Long> inputEnds) {

    public enum State {STARTING, IDLE, PROCESSING, SENDING, PREPARING, COMMITTING, ABORTING, BACKOFF, STOPPING, STOPPED, FAILED}

    /**
     * 业务作用：保留失败批次的输入证据，防止外部改写观测影响恢复判断。
     * <p>
     * 返回：独立不可变快照。
     *
     * @param transactionalId  当前producer使用的稳定Kafka事务身份。
     * @param group            事务输入位点所属的实际消费组。
     * @param state            当前事务执行或资源停止阶段。
     * @param assignmentEpoch  当前owner的本地分配代次，不授予跨进程租约。
     * @param assignment       当前owner拥有的分区集合，构造时冻结副本。
     * @param ready            事务初始化和当前消费分配是否满足就绪条件。
     * @param drained          事务执行与原生资源是否已实际退出，不表示未知结果已确定。
     * @param failure          当前稳定失败类别，无失败时为null。
     * @param committedBatches 取得原生commit确认的累计批次数。
     * @param committedInputs  取得原生commit确认的累计输入记录数。
     * @param committedOutputs 取得原生commit确认的累计输出记录数。
     * @param abortedBatches   取得原生abort确认的累计批次数。
     * @param inputStarts      当前或最后批次各分区的输入起点，供恢复判断使用。
     * @param inputEnds        当前或最后批次各分区的排他输入终点，即计划提交的next offset。
     */
    public TransactionStatus {
        assignment = Set.copyOf(assignment);
        inputStarts = Map.copyOf(inputStarts);
        inputEnds = Map.copyOf(inputEnds);
    }
}

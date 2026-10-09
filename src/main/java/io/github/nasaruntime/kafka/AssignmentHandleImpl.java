package io.github.nasaruntime.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 类作用 - {@link AssignmentHandle} 默认实现, 委托运行中的 {@link KafkaPollTask} 和 {@link KafkaProxy} 完成查询/停止。
 */
final class AssignmentHandleImpl implements AssignmentHandle {

    private final KafkaProxy proxy;
    private final String group;
    private final KafkaPollTask task;

    /**
     * 业务作用：把 group 名与其 poll 任务、所属 proxy 绑定成一个句柄，使调用方无需自行保管三者的对应关系。
     *
     * @param proxy 所属 KafkaProxy，停止时经由它注销 group
     * @param group 本次 assign 消费使用的 group.id
     * @param task  承载该 assign 消费的 poll 任务
     */
    AssignmentHandleImpl(KafkaProxy proxy, String group, KafkaPollTask task) {
        this.proxy = proxy;
        this.group = group;
        this.task = task;
    }

    /**
     * 业务作用：返回本次 assign 消费的 group.id。
     *
     * <p>参数说明: 无。
     *
     * @return 构造时传入的 group.id。
     */
    @Override
    public String group() {
        return group;
    }

    /**
     * 业务作用：读取 poll 任务当前绑定的分区。
     * 返回的是拷贝而非内部集合，避免调用方遍历期间 poll 线程改动绑定导致并发修改异常。
     *
     * <p>参数说明: 无。
     *
     * @return 分区集合的快照。
     */
    @Override
    public Set<TopicPartition> partitions() {
        return new LinkedHashSet<>(task.getAssignments());
    }

    /**
     * 业务作用：透传 poll 任务的运行状态。
     *
     * <p>参数说明: 无。
     *
     * @return poll 线程仍在运行返回 true。
     */
    @Override
    public boolean running() {
        return task.isRunning();
    }

    /**
     * 业务作用：经由 proxy 停止本 group 的消费。
     * 不直接操作 poll 任务，而是走 proxy：注销登记与停线程必须一起发生，
     * 只停线程会在 proxy 里留下一个指向已死任务的 group 条目，后续同名 assign 将被拒绝。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    @Override
    public void stop() {
        proxy.stopAssignment(group);
    }
}

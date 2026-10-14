package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.KafkaProxy;

import org.apache.kafka.common.TopicPartition;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 类作用 - {@link AssignmentHandle} 默认实现, 委托运行中的 {@link KafkaGroup} 和 {@link KafkaProxy} 完成查询/停止。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class AssignmentHandleImpl implements AssignmentHandle {

    private final KafkaProxy proxy;
    private final String group;
    private final KafkaGroup task;

    /**
     * 业务作用：把 group 名与其 poll 任务、所属 proxy 绑定成一个句柄，使调用方无需自行保管三者的对应关系。
     * 返回：绑定本次 owner 实例的句柄，旧句柄不会停止后续同名任务。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param proxy 所属 KafkaProxy，停止时经由它注销 group
     * @param group 本次 assign 消费使用的 group.id
     * @param task  承载该 assign 消费的 poll 任务
     */
    public AssignmentHandleImpl(KafkaProxy proxy, String group, KafkaGroup task) {
        this.proxy = proxy;
        this.group = group;
        this.task = task;
    }

    /**
     * 业务作用：返回本次 assign 消费的 group.id。
     * <p>
     * 参数说明：无。
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
     * <p>
     * 参数说明：无。
     *
     * @return 分区集合的快照。
     */
    @Override
    public Set<TopicPartition> partitions() {
        return new LinkedHashSet<>(task.getAssignments());
    }

    /**
     * 业务作用：透传 poll 任务的运行状态。
     * <p>
     * 参数说明：无。
     *
     * @return 本次任务的聚合运行标志；不是 Ready，也不是所有资源已退出的证明。
     */
    @Override
    public boolean running() {
        return task.isRunning();
    }

    /**
     * 业务作用：请求停止句柄绑定的 assign 实例，等待实际责任收尾后才注销匹配实例。
     * 等待失败保留任务及名称占用；旧句柄不停止后续同名任务。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：实例退出并完成注销后正常返回；超时或中断抛出异常，调用方可继续等待。
     */
    @Override
    public void stop() {
        proxy.stopAssignment(group, task);
    }
}

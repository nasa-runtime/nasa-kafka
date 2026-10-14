package io.github.nasaruntime.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

/**
 * 接口作用 - 手动分区消费 assign 模式的控制句柄。
 */
public interface AssignmentHandle {

    /**
     * 业务作用：读取本次 assign 使用的组名，供当前任务的位点查询与控制调用使用。
     * 组名只是查找身份；需要绑定本次实例停止时应使用本句柄的 stop，避免同名重建后控制到新任务。
     * <p>
     * 参数说明：无。
     *
     * @return 本次 assign 消费的 group.id。
     */
    String group();

    /**
     * 业务作用：读取本次 assign 任务当前公布的分区集合。
     * assign 不参加组分配；启动、恢复和保护期间的快照可能为空或保留诊断坐标，不能单凭集合授予操作权。
     * <p>
     * 参数说明：无。
     *
     * @return 分区集合的快照；对返回值的修改不影响实际绑定。
     */
    Set<TopicPartition> partitions();

    /**
     * 业务作用：读取本次 assign 任务的 owner 运行标志，供生命周期诊断使用。
     * <p>
     * 参数说明：无。
     *
     * @return 任一槽保留运行标志时为 true；保护态也可为 true，不等同于 Ready 或资源退出证明。
     */
    boolean running();

    /**
     * 业务作用：请求停止句柄绑定的 assign 实例，等待实际责任收尾后才注销匹配实例。
     * 等待失败保留任务及名称占用；旧句柄不停止后续同名任务。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：实例退出并完成注销后正常返回；超时或中断抛出异常，调用方可继续等待。
     */
    void stop();
}

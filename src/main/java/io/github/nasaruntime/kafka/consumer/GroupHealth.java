package io.github.nasaruntime.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * 单槽或全组的健康快照；组快照汇总全部槽，保护态优先，不把线程存在等同于业务 Ready。
 *
 * @param sourceId              命名源身份
 * @param group                 实际消费组
 * @param state                 当前会话或保护状态
 * @param epoch                 单槽分配代次或全组代次向量的观测编号；不能互相比较或作为跨进程租约
 * @param joined                当前槽有效 join，或组内全部槽有效 join 且无重复归属
 * @param assignment            当前公布的分配坐标；保护态可能保留旧坐标，不授予提交权威
 * @param pauses                每分区叠加的保护原因，用户 resume 只解除 USER
 * @param inFlight              尚未结束的业务与 DLT 调用数
 * @param retainedBytes         普通输入责任的八倍原文副本预留计费字节，不等于 JVM 堆占用
 * @param lastPollNanos         最近 poll 返回的本进程单调时刻；组快照取最慢槽，零表示尚未观察
 * @param reason                最近脱敏原因分类，不含载荷或认证材料
 * @param observedAtNanos       生成快照的本进程单调时刻
 * @param lastSuccessNanos      最近一次业务安全完成的单调时刻，零表示尚无成功
 * @param ownerSessionStarted   单槽或全组累计尝试创建原生会话的次数，包含各槽首次
 * @param ownerSessionRestarted 首次之后的会话创建尝试次数
 * @param commitAttempted       实际发起的 broker 提交次数
 * @param commitSucceeded       broker 确认的提交次数
 * @param commitFailed          失败或结果未知的提交次数
 */
public record GroupHealth(String sourceId, String group, State state, long epoch, boolean joined,
                          Set<TopicPartition> assignment, Map<TopicPartition, Set<String>> pauses,
                          int inFlight, long retainedBytes, long lastPollNanos, String reason,
                          long observedAtNanos, long lastSuccessNanos, long ownerSessionStarted,
                          long ownerSessionRestarted, long commitAttempted, long commitSucceeded, long commitFailed) {

    public enum State {
        /**
         * 尚未构造会话或仍处于首次启动延迟。
         */
        STARTING,
        /**
         * 原生 consumer 已构造，正在等待有效分配。
         */
        JOINING,
        /**
         * 当前会话可推进业务；是否满足指定分配由 ReadyRequirement 判断。
         */
        RUNNING,
        /**
         * 安全前缀提交未成功，责任与字节额度继续保留。
         */
        COMMIT_BLOCKED,
        /**
         * 存在失败记录处于退避，健康顺序域仍可推进。
         */
        RETRYING,
        /**
         * DLT 调用额度不足，等待已有死信责任退出。
         */
        DLT_BACKPRESSURE,
        /**
         * 已观察责任达到上限，等待提交裁剪后才继续拉取。
         */
        PROGRESS_BACKPRESSURE,
        /**
         * 至少一个分区处于保护态或 DLT 持续失败；健康分区仍可工作，DLT 按冻结策略继续重试。
         */
        DEGRADED,
        /**
         * 旧会话已失权，正在等待真实调用收口或自动重建退避。
         */
        RECOVERING,
        /**
         * 永久错误或重建预算耗尽，只接受显式恢复或停止。
         */
        CRASHED,
        /**
         * 分区硬暂停的兼容表示；新快照使用 DEGRADED。
         */
        HALTED,
        /**
         * 已关闭新增消费准入，等待实际调用与原生资源退出。
         */
        CLOSING,
        /**
         * owner 已完成本地收尾且原生客户端关闭成功；不代表全部业务处理成功。
         */
        CLOSED
    }

    /**
     * 业务作用：兼容只提供基础观测的调用方。
     * <p>
     * 返回：扩展计数尚未采集的快照。
     *
     * @param sourceId      健康快照所属的命名数据源。
     * @param group         被观测的实际消费组。
     * @param state         当前原生会话或保护状态，不能仅凭线程存在认定Ready。
     * @param epoch         单槽分配代次或全组代次向量的观测编号，不可互比或充当跨进程租约。
     * @param joined        当前槽有效join，或组内全部槽有效join且分区归属不重复。
     * @param assignment    当前观测的分配集合；保护态可能保留旧坐标，提交权威仍须由 owner 复验。
     * @param pauses        各分区叠加的保护原因，用户resume只解除USER。
     * @param inFlight      尚未结束的业务与DLT调用数量。
     * @param retainedBytes 普通消费输入责任的计费字节，包含原文的八倍副本预留；不等于 JVM 实际堆占用。
     * @param lastPollNanos 最近poll返回的本进程单调时刻，组快照取最慢槽，零表示未观察。
     * @param reason        最近的脱敏原因分类，不包含载荷或认证材料。
     */
    public GroupHealth(String sourceId, String group, State state, long epoch, boolean joined,
                       Set<TopicPartition> assignment, Map<TopicPartition, Set<String>> pauses,
                       int inFlight, long retainedBytes, long lastPollNanos, String reason) {
        this(sourceId, group, state, epoch, joined, assignment, pauses, inFlight, retainedBytes,
                lastPollNanos, reason, System.nanoTime(), 0, 0, 0, 0, 0, 0);
    }

    /**
     * 业务作用：限制人工重建到已保护的异常状态。
     * <p>
     * 参数说明：无。
     *
     * @return 正常消费与停机中的任务拒绝重建。
     */
    public boolean permitsRestart() {
        return switch (state) {
            case CRASHED, DEGRADED, HALTED, COMMIT_BLOCKED, DLT_BACKPRESSURE, PROGRESS_BACKPRESSURE, RECOVERING -> true;
            default -> false;
        };
    }
}

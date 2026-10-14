package io.github.nasaruntime.kafka.replay;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.checkpoint.CheckpointStore;



import java.util.*;

/**
 * 每来源的回放登记与固定线程额度；在线 consumer 和 producer 配额不被挪作回放线程。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaReplayManager {

    private final KafkaProxy source;
    private final Map<String, KafkaReplayTask> tasks = new HashMap<>();
    private int workers;
    private boolean closed;

    /**
     * 业务作用：建立惰性的来源任务目录，未执行时不创建线程或连接。
     * <p>
     * 返回：最多16个活动任务、64个分区worker的管理器。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param source 实际来源。
     */
    public KafkaReplayManager(KafkaProxy source) {
        this.source = source;
    }

    /**
     * 业务作用：在线性化准入门内登记独立执行实例与完整线程责任。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param plan        冻结合同
     * @param store       持久后端
     * @param identity    当前资源证据
     * @param processor   实际成功合同
     * @param fingerprint 在装配门外取得的处理声明
     * @param participant 可空的框架多源依赖。
     * @return 执行句柄；关闭、同名占用、合同不符或资源超额立即拒绝。
     */
    public synchronized ReplayHandle start(ReplayPlan plan, CheckpointStore store, ReplayIdentityProvider identity, ReplayProcessor processor, String fingerprint, ReplayParticipant participant) {
        if (closed) throw new ReplayException(ReplayException.Code.CLOSED);
        Objects.requireNonNull(plan);
        Objects.requireNonNull(store);
        Objects.requireNonNull(identity);
        Objects.requireNonNull(processor);
        if (!source.sourceId().equals(plan.sourceId()) || !plan.pipelineFingerprint().equals(fingerprint))
            throw new ReplayException(ReplayException.Code.CONTRACT_MISMATCH);
        if (tasks.containsKey(plan.taskId())) throw new ReplayException(ReplayException.Code.BUSY);
        int count = Math.min(plan.limits().concurrency(), plan.ranges().size());
        if (tasks.size() >= 16 || workers + count > 64) throw new ReplayException(ReplayException.Code.CAPACITY);
        KafkaReplayTask task = new KafkaReplayTask(this, source, plan, store, identity, processor, count, participant);
        // 线程可能立即开始接管；必须先固定目录与额度，关闭快照才能覆盖本次实际责任。
        tasks.put(plan.taskId(), task);
        workers += count;
        try {
            task.start();
        } catch (Throwable failure) {
            tasks.remove(plan.taskId(), task);
            workers -= count;
            throw failure;
        }
        return new ReplayHandle(task);
    }

    /**
     * 业务作用：永久关闭任务准入并通知全部实例，不等待单个实例而拖延其它停止通知。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 本次关闭需要排干的完整责任集合。
     */
    public synchronized List<KafkaReplayTask> stop() {
        closed = true;
        List<KafkaReplayTask> current = List.copyOf(tasks.values());
        current.forEach(KafkaReplayTask::cancel);
        return current;
    }

    /**
     * 业务作用：读取仍有实际资源的回放数，查询不发布停止意图。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 尚未归还目录的任务数，业务终态需从 ReplayHandle 查询。
     */
    public synchronized int activeCount() {
        return tasks.size();
    }

    /**
     * 业务作用：在真实任务资源全部退出后归还目录和线程额度。
     * <p>
     * 返回：只删除匹配实例，不影响同名后续执行。
     *
     * @param task  本次实例
     * @param count 预留worker数量。
     */
    synchronized void finished(KafkaReplayTask task, int count) {
        if (tasks.remove(task.plan().taskId(), task)) workers -= count;
    }
}

package io.github.nasaruntime.kafka.transaction;

import io.github.nasaruntime.kafka.KafkaProxy;



import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 每来源最多16个事务 owner，每个 owner 最多同时持有一个业务批次。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaTransactionManager {

    private static final Set<String> IDENTITIES = ConcurrentHashMap.newKeySet();
    private final Map<String, KafkaTransactionTask> tasks = new HashMap<>();
    private final KafkaProxy source;
    private boolean closed;

    /**
     * 业务作用：创建惰性事务目录，实际连接由任务初始化取得。
     * <p>
     * 返回：未开放任何事务的管理器。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param source 所属命名源。
     */
    public KafkaTransactionManager(KafkaProxy source) {
        this.source = source;
    }

    /**
     * 业务作用：在实际任务启动前占用稳定事务身份和专属组，阻止本进程同身份互相 fencing。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param plan      冻结合同
     * @param processor 有界同步处理入口。
     * @return 已登记句柄；关闭、重复和超额拒绝。
     */
    public synchronized TransactionHandle start(TransactionPlan plan, TransactionProcessor processor) {
        if (closed) throw new TransactionException(TransactionException.Code.CLOSED);
        if (tasks.containsKey(plan.group())) throw new TransactionException(TransactionException.Code.BUSY);
        if (tasks.size() >= 16) throw new TransactionException(TransactionException.Code.CAPACITY);
        KafkaTransactionTask task = new KafkaTransactionTask(this, source, plan, processor);
        String identity = plan.clusterId() + "/" + plan.identity().transactionalId();
        // 先固定身份和任务目录再开放线程；停机快照不能遗漏即将建立的原生 producer。
        if (!IDENTITIES.add(identity)) throw new TransactionException(TransactionException.Code.BUSY);
        tasks.put(plan.group(), task);
        try {
            task.start();
        } catch (Throwable failure) {
            tasks.remove(plan.group());
            IDENTITIES.remove(identity);
            throw failure;
        }
        return new TransactionHandle(task);
    }

    /**
     * 业务作用：禁止普通消费协调器与事务 owner 共用本源组。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param group 拟登记组。
     * @return 该组是否仍承担事务责任。
     */
    public synchronized boolean contains(String group) {
        return tasks.containsKey(group);
    }

    /**
     * 业务作用：读取尚未归还资源的事务 owner 数，避免以输入为空判断实际退出。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 当前责任数，未知事务结局仍由 TransactionHandle 保留。
     */
    public synchronized int activeCount() {
        return tasks.size();
    }

    /**
     * 业务作用：封闭准入并一次通知全部事务 owner。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 必须实际排干的完整任务集合。
     */
    public synchronized List<KafkaTransactionTask> stop() {
        closed = true;
        List<KafkaTransactionTask> active = List.copyOf(tasks.values());
        active.forEach(KafkaTransactionTask::cancel);
        return active;
    }

    /**
     * 业务作用：只有原生资源与回调实际退出后才归还组与稳定身份。
     * <p>
     * 返回：后续同身份恢复可以重新 initTransactions。
     *
     * @param task 真实退出实例。
     */
    synchronized void finished(KafkaTransactionTask task) {
        if (tasks.remove(task.plan().group(), task))
            IDENTITIES.remove(task.plan().clusterId() + "/" + task.plan().identity().transactionalId());
    }
}

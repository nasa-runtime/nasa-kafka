package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.config.NasaKafkaProperties;
import io.github.nasaruntime.kafka.dlt.KafkaDltBudget;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.lifecycle.CloseReport;

import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 同一消费组的 owner 集合；聚合观测不赋予跨 consumer 的原子操作能力。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaGroup {

    private final KafkaProxy proxy;
    private final String group;
    private final AtomicLong nextMetrics = new AtomicLong();
    private final List<KafkaPollTask> owners;
    private final NasaKafkaProperties config;
    private final CountDownLatch startup = new CountDownLatch(1);
    private final ExecutorService controls = Executors.newVirtualThreadPerTaskExecutor();
    private final ReentrantLock controlGate = new ReentrantLock();
    private CompletableFuture<Void> pendingControls = CompletableFuture.completedFuture(null);
    private List<SlotEpoch> epochSignature = List.of();
    private long epoch;

    /**
     * 空 assignment 槽也会失去 join，组代次必须同时观察身份和准入状态。
     */
    private record SlotEpoch(long epoch, boolean joined) {}

    /**
     * 业务作用：构造订阅组的稳定槽集合，不立即开放消费。
     * <p>
     * 返回：全部槽配置有效的组。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param proxy  源
     * @param group  最终组名
     * @param topics 初始订阅。
     */
    public KafkaGroup(KafkaProxy proxy, String group, Collection<String> topics) {
        this.proxy = proxy;
        this.group = group;
        config = proxy.config();
        int count = config.getGroupConcurrency().getOrDefault(group, config.getConsumerConcurrency());
        KafkaGroupBudget budget = new KafkaGroupBudget(config, proxy.inputBudget(), proxy.dltBudget());
        List<KafkaPollTask> created = new ArrayList<>();
        try {
            for (int slot = 0; slot < count; slot++)
                created.add(new KafkaPollTask(proxy, group, topics, slot, count, startup, budget, this));
        } catch (RuntimeException failure) {
            created.forEach(KafkaPollTask::requestStop);
            startup.countDown();
            created.forEach(KafkaPollTask::shutdown);
            controls.shutdown();
            throw failure;
        }
        owners = List.copyOf(created);
    }

    /**
     * 业务作用：固定分区保持单 owner，避免本地并发槽重复读取相同显式范围。
     * <p>
     * 返回：单槽组。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param proxy      提供该消费组数据源能力及生命周期管理的代理。
     * @param group      本组实际注册和提交位点使用的身份。
     * @param partitions 范围
     * @param start      首次起点。
     */
    public KafkaGroup(KafkaProxy proxy, String group, Collection<TopicPartition> partitions, StartOffset start) {
        this.proxy = proxy;
        this.group = group;
        config = proxy.config();
        owners = List.of(new KafkaPollTask(proxy, group, partitions, start, startup, new KafkaGroupBudget(config, proxy.inputBudget(), proxy.dltBudget()), this));
    }

    /**
     * 业务作用：全部槽线程建立后统一开放消费。
     * <p>
     * 返回：本地启动成功；分配仍由 Ready 验证。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param delayMs 首次延迟。
     */
    public void start(long delayMs) {
        try {
            for (KafkaPollTask owner : owners) owner.start(delayMs);
        } catch (RuntimeException | Error failure) {
            requestStop();
            throw failure;
        } finally {
            startup.countDown();
        }
    }

    /**
     * 业务作用：向全部槽关闭新增业务准入。
     * <p>
     * 参数说明：无。
     * 返回：实际在途责任仍由原槽收口。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void requestStop() {
        owners.forEach(KafkaPollTask::requestStop);
        startup.countDown();
    }

    /**
     * 业务作用：使用源关闭预算收口整个组。
     * <p>
     * 参数说明：无。
     * 返回：全部槽退出或结果未知。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void shutdown() {
        shutdownUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getShutdownTimeoutMs()));
    }

    /**
     * 业务作用：在同一绝对期限内等待全部槽和控制资源退出。
     * <p>
     * 返回：未退出时保留组身份并允许再次关闭。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param deadline 单调纳秒期限。
     */
    public void shutdownUntil(long deadline) {
        requestStop();
        RuntimeException failure = null;
        for (KafkaPollTask owner : owners)
            try {
                owner.shutdownUntil(deadline);
            } catch (RuntimeException error) {
                if (failure == null) failure = error;
                else failure.addSuppressed(error);
            }
        if (failure != null) throw failure;
        controls.shutdown();
        try {
            if (!controls.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "组控制任务尚未退出");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "关闭等待被中断");
        }
    }

    /**
     * 业务作用：聚合各槽的运行标志，供固定分配句柄诊断其绑定任务的生命周期。
     * <p>
     * 参数说明：无。
     *
     * @return 任一槽仍保留运行标志时为 true；不等同于 Ready，也不能仅凭 false 判断所有资源已退出。
     */
    boolean isRunning() {
        return owners.stream().anyMatch(KafkaPollTask::isRunning);
    }

    /**
     * 业务作用：汇总当前组的实际消费执行和停止释放责任，保留零输入但线程未退的区别。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 普通消费汇总，不查询 broker 或调用用户业务。
     */
    public CloseReport.Consumers closeReport() {
        int active = 0, handlers = 0;
        long records = 0, bytes = 0, unverified = 0;
        for (KafkaPollTask owner : owners) {
            var value = owner.closeReport();
            active += value.activeOwners();
            handlers += value.activeHandlers();
            records = plus(records, value.retainedRecords());
            bytes = plus(bytes, value.retainedBytes());
            unverified = plus(unverified, value.releasedUnverifiedRecords());
        }
        return new CloseReport.Consumers(active, handlers, records, bytes, unverified, 0);
    }

    /**
     * 业务作用：同时核对 owner 与组控制执行器，避免只看消费线程判断资源退出。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 所有实际线程和控制任务退出才为 true。
     */
    public boolean resourcesClosed() {
        return closeReport().activeOwners() == 0 && controls.isTerminated();
    }

    /**
     * 业务作用：返回组内一致的分配模式。
     * <p>
     * 参数说明：无。
     *
     * @return 订阅或固定分配。
     */
    ConsumeMode getMode() {
        return owners.getFirst().getMode();
    }

    /**
     * 业务作用：按稳定槽号提供独立观测。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 下标即槽号的不可变快照列表。
     */
    public List<GroupHealth> slotsHealth() {
        return owners.stream().map(KafkaPollTask::health).toList();
    }

    /**
     * 业务作用：发布组聚合状态，避免 owner 数量改变指标含义。
     * <p>
     * 参数说明：无。
     * 返回：采样入队或丢弃，不调用用户后端；指标非跨槽原子证据。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void sampleMetrics() {
        long now = System.nanoTime(), next = nextMetrics.get();
        if (now < next || !nextMetrics.compareAndSet(next, now + TimeUnit.SECONDS.toNanos(1))) return;
        GroupHealth current = health();
        Map<String, String> labels = Map.of("group", group);
        proxy.metric("group_ready", ReadyRequirement.JOINED.satisfiedBy(current) ? 1 : 0, false, labels);
        proxy.metric("assigned_partitions", current.assignment().size(), false, labels);
        proxy.metric("paused_partitions", current.pauses().size(), false, labels);
        int handlers = owners.stream().mapToInt(KafkaPollTask::activeHandlers).sum();
        proxy.metric("active_handlers", handlers, false, labels);
        proxy.metric("dlt_in_flight", Math.max(0, current.inFlight() - handlers), false, labels);
        proxy.metric("retained_bytes", current.retainedBytes(), false, labels);
        proxy.metric("retained_records", closeReport().retainedRecords(), false, labels);
        long[] keys = new long[3];
        for (var owner : owners) {
            long[] sample = owner.keyMetrics();
            for (int i = 0; i < 3; i++) keys[i] = plus(keys[i], sample[i]);
        }
        proxy.metric("key_active_handlers", keys[0], false, labels);
        proxy.metric("key_pending_records", keys[1], false, labels);
        proxy.metric("key_pending_bytes", keys[2], false, labels);
        KafkaDltBudget.Snapshot dlt = proxy.dltBudget().snapshot();
        proxy.metric("dlt_queue_jobs", dlt.records(), false, Map.of());
        proxy.metric("dlt_queue_bytes", dlt.bytes(), false, Map.of());
    }

    /**
     * 业务作用：为句柄查询提供组内分区并集。
     * <p>
     * 参数说明：无。
     *
     * @return 只读观测，不作为控制租约。
     */
    Set<TopicPartition> getAssignments() {
        return health().assignment();
    }

    /**
     * 业务作用：聚合所有槽的容量与保护状态，空闲且已 join 的槽不阻止 Ready。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 任一槽失权或归属冲突时 joined 为 false。
     */
    public synchronized GroupHealth health() {
        List<GroupHealth> slots = slotsHealth();
        if (slots.size() == 1) return slots.getFirst();
        GroupHealth first = slots.getFirst();
        Set<TopicPartition> assigned = new LinkedHashSet<>();
        Map<TopicPartition, Set<String>> pauses = new LinkedHashMap<>();
        boolean joined = true;
        int inFlight = 0;
        long bytes = 0, poll = Long.MAX_VALUE, success = 0, started = 0, restarted = 0, attempted = 0, committed = 0, failed = 0;
        GroupHealth.State state = GroupHealth.State.RUNNING;
        String reason = null;
        for (GroupHealth slot : slots) {
            joined &= slot.joined();
            for (TopicPartition tp : slot.assignment()) joined &= assigned.add(tp);
            pauses.putAll(slot.pauses());
            inFlight += slot.inFlight();
            bytes += slot.retainedBytes();
            poll = Math.min(poll, slot.lastPollNanos());
            success = Math.max(success, slot.lastSuccessNanos());
            started = plus(started, slot.ownerSessionStarted());
            restarted = plus(restarted, slot.ownerSessionRestarted());
            attempted = plus(attempted, slot.commitAttempted());
            committed = plus(committed, slot.commitSucceeded());
            failed = plus(failed, slot.commitFailed());
            if (rank(slot.state()) > rank(state)) {
                state = slot.state();
                reason = slot.reason();
            }
        }
        List<SlotEpoch> signature = slots.stream().map(slot -> new SlotEpoch(slot.epoch(), slot.joined())).toList();
        // 组代次随完整槽向量变化，不能让其它槽更大的 epoch 掩盖单槽重建。
        if (!signature.equals(epochSignature)) {
            epochSignature = signature;
            epoch++;
        }
        return new GroupHealth(first.sourceId(), first.group(), state, epoch, joined, Set.copyOf(assigned), Map.copyOf(pauses),
                inFlight, bytes, poll, reason, System.nanoTime(), success, started, restarted, attempted, committed, failed);
    }

    /**
     * 业务作用：聚合长期累计指标时保持非负单调饱和值。
     *
     * @param left  参与饱和汇总的第一个非负数量。
     * @param right 参与饱和汇总的第二个非负数量。
     * @return 总和或 Long.MAX_VALUE。
     */
    private static long plus(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    /**
     * 业务作用：让组内保护与关闭状态优先于健康槽。
     *
     * @param state 单槽状态。
     * @return 越大越优先的等级。
     */
    private static int rank(GroupHealth.State state) {
        return switch (state) {
            case RUNNING -> 0;
            case RETRYING -> 1;
            case PROGRESS_BACKPRESSURE -> 2;
            case DLT_BACKPRESSURE -> 3;
            case COMMIT_BLOCKED -> 4;
            case DEGRADED, HALTED -> 5;
            case STARTING -> 6;
            case JOINING -> 7;
            case RECOVERING -> 8;
            case CLOSED -> 9;
            case CLOSING -> 10;
            case CRASHED -> 11;
        };
    }

    /**
     * 业务作用：完整解析当前分区归属后才派发控制，避免缺席目标产生部分副作用。
     *
     * @param targets 目标集合。
     * @return 每槽分区列表；缺席或重复归属时确定未执行。
     */
    private Map<KafkaPollTask, List<TopicPartition>> route(Collection<TopicPartition> targets) {
        Map<KafkaPollTask, List<TopicPartition>> routed = new LinkedHashMap<>();
        List<GroupHealth> slots = slotsHealth();
        // 全部目标都必须有唯一权威；先完成这一门禁，才允许任何槽产生控制副作用。
        for (TopicPartition tp : Set.copyOf(targets)) {
            int found = -1;
            for (int i = 0; i < slots.size(); i++)
                if (slots.get(i).joined() && slots.get(i).assignment().contains(tp)) {
                    if (found >= 0)
                        throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "assignment 正在转移");
                    found = i;
                }
            if (found < 0) throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "分区尚未分配");
            routed.computeIfAbsent(owners.get(found), ignored -> new ArrayList<>()).add(tp);
        }
        return routed;
    }

    /**
     * 业务作用：并行送达有限个槽命令，保持组级等待预算并显式报告部分失败。
     * <p>
     * 返回：全部成功才成功；超时或多槽失败为结果未知。
     *
     * @param actions 各槽操作。
     */
    private void fanout(List<Runnable> actions) {
        if (actions.isEmpty()) return;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getControlTimeoutMs());
        try {
            if (!controlGate.tryLock(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "组控制准入超时");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "组控制准入中断");
        }
        try {
            // 上次结果未知仍可能有命令执行，不继续累积控制 worker 或交错修改订阅集合。
            if (!pendingControls.isDone())
                throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "前次组控制尚未收口");
            if (owners.size() == 1) {
                actions.forEach(Runnable::run);
                return;
            }
            pendingControls = CompletableFuture.allOf(actions.stream().map(action -> CompletableFuture.runAsync(action, controls)).toArray(CompletableFuture[]::new));
            try {
                pendingControls.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new KafkaFailure(KafkaFailure.Code.CONTROL_OUTCOME_UNKNOWN, "组控制等待被中断");
            } catch (ExecutionException | TimeoutException error) {
                throw new KafkaFailure(KafkaFailure.Code.CONTROL_OUTCOME_UNKNOWN, "部分槽可能已执行，需重新查询实际状态", error);
            }
        } finally {
            controlGate.unlock();
        }
    }

    /**
     * 业务作用：向全部槽发送组级控制。
     * <p>
     * 返回：遵循 fanout 的整体或部分结果语义。
     *
     * @param operation 单槽动作。
     */
    private void all(Consumer<KafkaPollTask> operation) {
        fanout(owners.stream().<Runnable>map(owner -> () -> operation.accept(owner)).toList());
    }

    /**
     * 业务作用：把分区操作交给当前 owner，随后由槽内命令复验。
     * <p>
     * 返回：全部目标的执行结局。
     *
     * @param targets   范围
     * @param operation 单槽操作。
     */
    private void each(Collection<TopicPartition> targets, BiConsumer<KafkaPollTask, List<TopicPartition>> operation) {
        fanout(route(targets).entrySet().stream().<Runnable>map(entry -> () -> operation.accept(entry.getKey(), entry.getValue())).toList());
    }

    /**
     * 业务作用：冻结全部目标归属，在组预算内逐槽执行并保留每个目标的结局。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param actions 已验证意图。
     * @return 独立快照；缺席目标使整批不派发，结果未知后不继续开启其它槽。
     */
    public ControlReport controlPartitions(Map<TopicPartition, PartitionControl> actions) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getControlTimeoutMs());
        KafkaControlProgress results = new KafkaControlProgress(actions.keySet());
        boolean locked = false;
        try {
            locked = controlGate.tryLock(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            if (!locked || !pendingControls.isDone()) return results.freeze(proxy.sourceId(), group);
            List<GroupHealth> slots = slotsHealth();
            Map<Integer, Map<TopicPartition, PartitionControl>> routed = new TreeMap<>();
            List<TopicPartition> ordered = actions.keySet().stream().sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition)).toList();
            for (TopicPartition tp : ordered) {
                int found = -1;
                for (int slot = 0; slot < slots.size(); slot++)
                    if (slots.get(slot).joined() && slots.get(slot).assignment().contains(tp)) {
                        if (found >= 0) {
                            found = -2;
                            break;
                        }
                        found = slot;
                    }
                if (found < 0) results.put(tp, ControlReport.Outcome.OWNER_CHANGED);
                else routed.computeIfAbsent(found, ignored -> new LinkedHashMap<>()).put(tp, actions.get(tp));
            }
            // 全部目标都必须有唯一归属，不能因后续目标缺席而留下前序副作用。
            if (results.containsValue(ControlReport.Outcome.OWNER_CHANGED))
                return results.freeze(proxy.sourceId(), group);
            for (var entry : routed.entrySet()) {
                if (System.nanoTime() >= deadline) break;
                owners.get(entry.getKey()).controlPartitions(entry.getValue(), slots.get(entry.getKey()).epoch(), deadline, results);
                // 已开始但未收口的命令可能仍持有原生资源，不继续扩大本次操作范围。
                if (results.containsValue(ControlReport.Outcome.UNKNOWN)) break;
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            if (locked) controlGate.unlock();
        }
        return results.freeze(proxy.sourceId(), group);
    }

    /**
     * 业务作用：增量订阅对所有槽保持同一声明。
     * <p>
     * 返回：全部槽确认或部分结果未知。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param topic 新增主题。
     */
    public void subscribe(String topic) {
        addSubscriptions(Collections.singleton(topic));
    }

    /**
     * 业务作用：从所有槽删除一个主题。
     * <p>
     * 返回：全部槽确认或部分结果未知。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param topic 待移除主题。
     */
    public void unsubscribe(String topic) {
        removeSubscriptions(Collections.singleton(topic));
    }

    /**
     * 业务作用：在派发任何槽之前冻结并校验整批新增主题。
     * <p>
     * 返回：每槽一次变更；跨槽失败保留既有部分未知合同。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param topics 非空、唯一主题集合。
     */
    public void addSubscriptions(Collection<String> topics) {
        Set<String> copy = subscriptionTopics(topics, false);
        all(owner -> owner.changeSubscriptions(copy, true));
    }

    /**
     * 业务作用：把整批退订交给各owner一次应用，避免集合中途被后继非法参数截断。
     * <p>
     * 返回：其它订阅不变，跨槽不承诺原子性。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param topics 非空、唯一主题集合。
     */
    public void removeSubscriptions(Collection<String> topics) {
        Set<String> copy = subscriptionTopics(topics, false);
        all(owner -> owner.changeSubscriptions(copy, false));
    }

    /**
     * 业务作用：在控制副作用前拒绝非法订阅集合。
     *
     * @param topics     调用方给出的完整本次订阅声明，名称必须非空白且无重复。
     * @param allowEmpty 是否允许空集合；完整替换可清空，增量添加和移除要求非空。
     * @return 自有不可变集合；空白、null或重复主题为配置错误。
     */
    private static Set<String> subscriptionTopics(Collection<String> topics, boolean allowEmpty) {
        if (topics == null || !allowEmpty && topics.isEmpty())
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "订阅修改集合不能为空");
        Set<String> copy = new LinkedHashSet<>();
        for (String topic : topics)
            if (topic == null || topic.isBlank() || !copy.add(topic))
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "订阅主题不能为空或重复");
        return Set.copyOf(copy);
    }

    /**
     * 业务作用：对所有槽替换订阅集合。
     * <p>
     * 返回：路由表不变，部分失败需重新查询并收敛。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param topics 完整新声明。
     */
    public void replaceSubscriptions(Collection<String> topics) {
        Set<String> copy = subscriptionTopics(topics, true);
        all(owner -> owner.replaceSubscriptions(copy));
    }

    /**
     * 业务作用：仅重建处于保护态的槽，保留其它槽的连续处理。
     * <p>
     * 参数说明：无。
     * 返回：没有可恢复槽时拒绝。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void restart() {
        List<Runnable> actions = owners.stream().filter(owner -> owner.health().permitsRestart()).<Runnable>map(owner -> owner::restart).toList();
        if (actions.isEmpty()) throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "没有允许重建的槽");
        fanout(actions);
    }

    /**
     * 业务作用：定位单个当前分配分区。
     * <p>
     * 返回：owner 确认或拒绝。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param tp     分区
     * @param offset 绝对位置。
     */
    public void seek(TopicPartition tp, long offset) {
        seek(Map.of(tp, offset));
    }

    /**
     * 业务作用：按当前槽分配定位多个分区。
     * <p>
     * 返回：不承诺跨槽原子性。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param offsets 非负目标。
     */
    public void seek(Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, Long> copy = Map.copyOf(offsets);
        if (copy.values().stream().anyMatch(value -> value < 0)) throw new IllegalArgumentException("offset 不能为负");
        each(copy.keySet(), (owner, tps) -> owner.seek(subset(copy, tps)));
    }

    /**
     * 业务作用：定位当前拥有分区的保留起点。
     * <p>
     * 返回：全部槽的控制结果。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param targets 目标。
     */
    public void seekToBeginning(Collection<TopicPartition> targets) {
        each(targets, KafkaPollTask::seekToBeginning);
    }

    /**
     * 业务作用：定位当前拥有分区的末尾。
     * <p>
     * 返回：全部槽的控制结果。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param targets 目标。
     */
    public void seekToEnd(Collection<TopicPartition> targets) {
        each(targets, KafkaPollTask::seekToEnd);
    }

    /**
     * 业务作用：按时间定位单个分区。
     * <p>
     * 返回：owner 控制结果。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param tp   分区
     * @param time epoch 毫秒。
     */
    public void seekToTimestamp(TopicPartition tp, long time) {
        seekToTimestamp(Map.of(tp, time));
    }

    /**
     * 业务作用：把时间定位拆分给实际 owner。
     * <p>
     * 返回：全部槽确认或部分结果未知。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param times 目标时间。
     */
    public void seekToTimestamp(Map<TopicPartition, Long> times) {
        Map<TopicPartition, Long> copy = Map.copyOf(times);
        each(copy.keySet(), (owner, tps) -> owner.seekToTimestamp(subset(copy, tps)));
    }

    /**
     * 业务作用：定位 broker 已确认的消费位置。
     * <p>
     * 返回：各 owner 查询并核验本槽历史后定位；缺少历史或归属变化拒绝该槽，其它槽可能已应用。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param targets 范围。
     */
    public void seekToCommitted(Collection<TopicPartition> targets) {
        each(targets, KafkaPollTask::seekToCommitted);
    }

    /**
     * 业务作用：只提取一个槽负责的定位条件。
     *
     * @param values  原条件
     * @param targets 本槽范围。
     * @return 独立目标 map。
     */
    private static Map<TopicPartition, Long> subset(Map<TopicPartition, Long> values, List<TopicPartition> targets) {
        Map<TopicPartition, Long> result = new LinkedHashMap<>();
        targets.forEach(tp -> result.put(tp, values.get(tp)));
        return result;
    }

    /**
     * 业务作用：增加当前目标分区的用户暂停原因。
     * <p>
     * 返回：不改变其它保护原因。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param targets 范围。
     */
    public void pause(Collection<TopicPartition> targets) {
        each(targets, KafkaPollTask::pause);
    }

    /**
     * 业务作用：解除目标分区的用户暂停原因。
     * <p>
     * 返回：内部保护继续有效。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param targets 范围。
     */
    public void resume(Collection<TopicPartition> targets) {
        each(targets, KafkaPollTask::resume);
    }

    /**
     * 业务作用：暂停全部槽的当前分区。
     * <p>
     * 参数说明：无。
     * 返回：各槽确认或部分结果未知。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void pauseAll() {
        all(KafkaPollTask::pauseAll);
    }

    /**
     * 业务作用：解除全部槽的用户暂停。
     * <p>
     * 参数说明：无。
     * 返回：内部门禁继续有效。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public void resumeAll() {
        all(KafkaPollTask::resumeAll);
    }

    /**
     * 业务作用：从当前 owner 查询本地读取位置。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param tp 目标分区。
     * @return 异步位置，不代表业务完成。
     */
    public CompletableFuture<Long> position(TopicPartition tp) {
        return route(List.of(tp)).keySet().iterator().next().position(tp);
    }

    /**
     * 业务作用：汇总所有 owner 实际查询的 assignment。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 分区并集；转移期间归属重复则拒绝。
     */
    public CompletableFuture<Set<TopicPartition>> assignmentSnapshot() {
        var queries = owners.stream().map(KafkaPollTask::assignmentSnapshot).toList();
        return CompletableFuture.allOf(queries.toArray(CompletableFuture[]::new)).thenApply(ignored -> {
            Set<TopicPartition> result = new LinkedHashSet<>();
            for (var query : queries)
                for (var tp : query.join())
                    if (!result.add(tp))
                        throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "assignment 正在转移");
            return Set.copyOf(result);
        });
    }

    /**
     * 业务作用：查询 broker 的组提交记录，该查询不需要目标归属本地槽。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param targets 分区范围。
     * @return 无提交历史保留 null。
     */
    public CompletableFuture<Map<TopicPartition, Long>> committedOffsets(Collection<TopicPartition> targets) {
        return owners.getFirst().committedOffsets(targets);
    }
}

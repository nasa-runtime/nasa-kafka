package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.config.NasaKafkaProperties;
import io.github.nasaruntime.kafka.consumer.listener.KafkaAsyncBatchListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaAsyncListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaPassthroughListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaRawListener;
import io.github.nasaruntime.kafka.dlt.DltRetryPolicy;
import io.github.nasaruntime.kafka.observability.trace.KafkaSpan;
import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.protocol.KafkaWire;
import io.github.nasaruntime.kafka.support.KafkaNative;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.lifecycle.CloseReport;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceContext;
import io.github.nasaruntime.kafka.observability.trace.KafkaTraceSpan;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.locks.LockSupport;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.errors.RetriableException;

import java.util.concurrent.TimeoutException;

/**
 * consumer 的唯一 owner；业务结果不能绕过已观察责任账本直接提交 offset。
 */
final class KafkaPollTask {
    private final KafkaGroup group;

    private final KafkaProxy proxy;
    private final NasaKafkaProperties config;
    private final KafkaGroupBudget budget;
    private final String groupId;
    private final int slot;
    private final int slotCount;
    private final CountDownLatch startupGate;
    private final ConsumeMode mode;
    private final Set<String> subscribedTopics = new TreeSet<>();
    private final Set<TopicPartition> fixedPartitions;
    /**
     * 只在首次会话使用的起点声明；恢复不得重新求值 latest 或 timestamp。
     */
    private final StartOffset startOffset;
    /**
     * 仅 owner 修改的当前权威账本；旧调用继续持有已失权的 Partition 实例。
     */
    private final Map<TopicPartition, Partition> partitions = new LinkedHashMap<>();
    /**
     * 实际尚未结束的调用，超时和失权均不提前移除其顺序占用。
     */
    private final List<Invocation> invocations = new ArrayList<>();
    private volatile int observedBusinessHandlers;
    private volatile long[] keyMetrics = new long[3];
    private long keyMetricsAt;

    /**
     * 业务作用：交付 owner 发布的按键责任快照，避免其它线程遍历可变账本。
     * <p>
     * 参数说明：无。
     *
     * @return 调用数、记录数和计费字节数的独立数组。
     */
    long[] keyMetrics() {
        return keyMetrics.clone();
    }

    /**
     * 业务作用：为组级观测区分业务调用和死信交付。
     * <p>
     * 参数说明：无。
     *
     * @return 最近快照时尚未实际退出的业务调用数，批调用计一个。
     */
    int activeHandlers() {
        return observedBusinessHandlers;
    }

    /**
     * 用户暂停与内部保护分开保存，resume 不得取消可靠性门禁。
     */
    private final Set<TopicPartition> userPaused = new HashSet<>();
    private final BlockingQueue<Command<?>> commands;
    private final ExecutorService workers;
    private volatile KafkaConsumer<byte[], byte[]> consumer;
    private volatile Thread runner;
    private volatile boolean running;
    private volatile boolean stopRequested;
    private volatile GroupHealth health;
    private long nextEpoch;
    private long retainedBytes;
    private volatile int retainedRecords;
    /**
     * 停止期间实际释放但未获提交确认的观察次数；只保存标量，不继续持有输入原文。
     */
    private volatile long releasedUnverifiedRecords;

    /**
     * 业务作用：报告 owner 的实际退出与未确认责任，不能把释放内存等同于成功或必然可重投。
     * <p>
     * 参数说明：无。
     *
     * @return 非原子采样；终态时计数固定，不执行 broker 查询。
     */
    CloseReport.Consumers closeReport() {
        Thread thread = runner;
        GroupHealth current = health();
        return new CloseReport.Consumers(thread != null && thread.isAlive() ? 1 : 0, activeHandlers(), retainedRecords,
                current.retainedBytes(), releasedUnverifiedRecords, 0);
    }

    private long lastCommit;
    private long lastPoll;
    /**
     * 原生完整响应才刷新可达证据，空 poll 和发送心跳不能延长 broker 存活窗口。
     */
    private long lastBrokerResponse;
    private double brokerResponseCount;
    private Metric brokerResponses;
    private Metric brokerConnections;
    private int safeSinceCommit;
    private String reason;
    private boolean joined;
    private GroupHealth.State sessionState = GroupHealth.State.STARTING;
    private boolean restartRequested;
    private boolean commitBlocked;
    /**
     * 固定分区恢复边界随确认提交前移；未确认记录始终把边界压回首条责任。
     */
    private final Map<TopicPartition, Long> recoveryOffsets = new HashMap<>();
    /**
     * 只记录自动重建，显式 operator 恢复在收口后重新开启预算。
     */
    private final Deque<Long> restartHistory = new ArrayDeque<>();
    private long ownerSessionStarted;
    private long commitAttempted;
    private long commitSucceeded;
    private long commitFailed;
    private long lastSuccess;
    private int partitionCursor;

    /**
     * 业务作用：建立订阅消费 owner，不立即连接。
     * <p>
     * 返回：未启动任务。
     *
     * @param proxy   源
     * @param groupId 组
     * @param topics  初始订阅
     * @param slot    当前owner的稳定槽号
     * @param count   组内owner总数
     * @param gate    组启动门
     * @param budget  全组共享额度。
     * @param group   来源登记的消费组实例，供 DLT 复验责任归属。
     */
    KafkaPollTask(KafkaProxy proxy, String groupId, Collection<String> topics, int slot, int count, CountDownLatch gate, KafkaGroupBudget budget, KafkaGroup group) {
        this(proxy, groupId, ConsumeMode.SUBSCRIBE, Set.of(), null, slot, count, gate, budget, group);
        if (topics != null) subscribedTopics.addAll(topics);
    }

    /**
     * 业务作用：建立固定分区 owner。
     * <p>
     * 返回：未启动任务。
     *
     * @param proxy      源
     * @param groupId    独立组
     * @param partitions 范围
     * @param start      初次位置
     * @param gate       组启动门
     * @param budget     组共享额度。
     * @param group      来源登记的消费组实例，供 DLT 复验责任归属。
     */
    KafkaPollTask(KafkaProxy proxy, String groupId, Collection<TopicPartition> partitions, StartOffset start, CountDownLatch gate, KafkaGroupBudget budget, KafkaGroup group) {
        this(proxy, groupId, ConsumeMode.ASSIGN, Set.copyOf(partitions), start == null ? StartOffset.earliest() : start, 0, 1, gate, budget, group);
    }

    /**
     * 业务作用：固定 owner 身份并绑定全组责任额度。
     * <p>
     * 返回：尚未取得分区权威的任务。
     *
     * @param proxy   该消费owner实际绑定的数据源代理。
     * @param groupId 本owner用于订阅和提交的实际消费组身份。
     * @param mode    订阅消费或显式分区分配的运行模式。
     * @param fixed   固定范围
     * @param start   起点
     * @param slot    当前owner的稳定槽号
     * @param count   组内owner总数
     * @param gate    控制全组准入
     * @param budget  共享账本。
     * @param group   来源登记的消费组实例。
     */
    private KafkaPollTask(KafkaProxy proxy, String groupId, ConsumeMode mode, Set<TopicPartition> fixed, StartOffset start, int slot, int count, CountDownLatch gate, KafkaGroupBudget budget, KafkaGroup group) {
        this.proxy = proxy;
        this.group = group;
        this.config = proxy.config().snapshot();
        this.groupId = groupId;
        this.mode = mode;
        this.budget = budget;
        this.slot = slot;
        slotCount = count;
        startupGate = gate;
        fixedPartitions = fixed;
        startOffset = start;
        commands = new ArrayBlockingQueue<>(config.getControlQueueCapacity());
        workers = Executors.newVirtualThreadPerTaskExecutor();
        health = new GroupHealth(proxy.sourceId(), groupId, GroupHealth.State.STARTING, 0, false, Set.of(), Map.of(), 0, 0, 0, null);
    }

    /**
     * 业务作用：读取稳定消费组身份。
     * <p>
     * 参数说明：无。
     *
     * @return 组名。
     */
    String getGroupId() {
        return groupId;
    }

    /**
     * 业务作用：区分动态订阅与固定分区控制。
     * <p>
     * 参数说明：无。
     *
     * @return 消费模式。
     */
    ConsumeMode getMode() {
        return mode;
    }

    /**
     * 业务作用：读取 owner 的运行标志，供任务目录和句柄诊断生命周期阶段。
     * <p>
     * 参数说明：无。
     *
     * @return owner 已启动且尚未清除运行标志时为 true；保护态也可为 true，false 不单独证明线程及资源已完全退出。
     */
    boolean isRunning() {
        return running;
    }

    /**
     * 业务作用：取得当前分配快照。
     * <p>
     * 参数说明：无。
     *
     * @return 只读分区集合。
     */
    Set<TopicPartition> getAssignments() {
        return health.assignment();
    }

    /**
     * 业务作用：读取无需访问原生 consumer 的健康快照。
     * <p>
     * 参数说明：无。
     *
     * @return 带观测代次的快照。
     */
    GroupHealth health() {
        return health;
    }

    /**
     * 业务作用：幂等启动 owner。
     * <p>
     * 返回：后台启动，Ready 另行等待。
     *
     * @param delayMs 初始化延迟。
     */
    synchronized void start(long delayMs) {
        if (running) return;
        if (stopRequested) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "已停止任务不能重新启动");
        running = true;
        runner = Thread.ofPlatform().daemon().name("kafka-owner-" + proxy.sourceId() + "-" + groupId + "-" + slot).start(() -> run(delayMs));
    }

    /**
     * 业务作用：关闭新增消费工作及在途业务确认的准入。
     * <p>
     * 参数说明：无。
     * 返回：旧调用立即失去确认权，实际执行与已发送 DLT 继续收口。
     */
    void requestStop() {
        stopRequested = true;
        signalOwner();
    }

    /**
     * 业务作用：通知owner已有实际完成或控制意图，避免业务吞吐被原生空poll期限限制。
     * <p>
     * 参数说明：无。
     * 返回：只发布一个可合并唤醒许可，不调用native consumer、不提前释放输入责任。
     */
    private void signalOwner() {
        LockSupport.unpark(runner);
    }

    /**
     * 业务作用：使用配置的关闭总预算等待实际退出。
     * <p>
     * 参数说明：无。
     * 返回：退出成功或明确超时。
     */
    void shutdown() {
        shutdownUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getShutdownTimeoutMs()));
    }

    /**
     * 业务作用：在共享 deadline 内等待 owner 与业务调用退出。
     * <p>
     * 返回：仍活动时拒绝报告关闭。
     *
     * @param deadline 单调纳秒。
     */
    void shutdownUntil(long deadline) {
        requestStop();
        Thread thread = runner;
        if (thread == null) {
            workers.shutdown();
            return;
        }
        if (thread == Thread.currentThread())
            throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "owner 不能同步等待自身退出");
        try {
            long remaining = deadline - System.nanoTime();
            if (remaining > 0) TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "consumer 关闭等待被中断");
        }
        if (thread.isAlive())
            throw new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "consumer 仍有活动责任: " + groupId);
    }

    /**
     * 业务作用：监管原生会话，旧调用退出后才允许取得新消费权威。
     * <p>
     * 返回：停止请求、不可继续的失败或中断结束消费循环；退出前执行本地会话收尾并发布终态。
     *
     * @param delayMs 首次延迟。
     */
    private void run(long delayMs) {
        try {
            // 全部槽的线程都已构造后才开放业务；构造失败时先标记停止，再统一解除此门。
            startupGate.await();
            awaitSupervisorDelay(delayMs, false);
            while (!stopRequested) {
                Throwable failure = null;
                restartRequested = false;
                sessionState = GroupHealth.State.STARTING;
                ownerSessionStarted++;
                proxy.metric("owner_session_started_total", 1, true, Map.of("group", groupId));
                if (ownerSessionStarted > 1)
                    proxy.metric("owner_session_restarted_total", 1, true, Map.of("group", groupId));
                snapshot();
                try {
                    runSession();
                } catch (Throwable error) {
                    failure = error;
                    reason = error.getClass().getSimpleName();
                } finally {
                    closeSession();
                }
                if (stopRequested) break;
                if (restartRequested) {
                    restartHistory.clear();
                    continue;
                }
                // 只有明确可重试的原生错误使用自动预算；认证、配置及框架不变量失败保持保护态。
                if (failure instanceof RetriableException && reserveRestart()) {
                    sessionState = GroupHealth.State.RECOVERING;
                    snapshot();
                    awaitSupervisorDelay(restartDelay(), true);
                } else {
                    sessionState = GroupHealth.State.CRASHED;
                    if (failure instanceof RetriableException) reason = "RestartBudgetExhausted";
                    snapshot();
                    while (!stopRequested && !restartRequested) {
                        drainCommands();
                        Thread.sleep(20);
                    }
                }
                if (restartRequested) restartHistory.clear();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            reason = "OwnerInterrupted";
        } finally {
            stopRequested = true;
            closeSession();
            workers.shutdown();
            Command<?> command;
            while ((command = commands.poll()) != null)
                command.result.completeExceptionally(new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "owner 已退出"));
            running = false;
            joined = false;
            snapshot();
        }
    }

    /**
     * 业务作用：运行一代 consumer，所有控制与提交在同一线程线性化。
     * <p>
     * 参数说明：无。
     * 返回：停止、显式重建或原生失败时交回会话。
     */
    private void runSession() throws InterruptedException {
        Properties properties = config.buildConsumerProperties(groupId);
        if (slotCount > 1) {
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) + "-slot-" + slot);
            Object instance = properties.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
            if (instance != null) properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instance + "-slot-" + slot);
        }
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.min(config.getMaxPollRecords(), config.getMaxProgressRecordsPerPartition()));
        // 恢复期间缺失的提交坐标不能被 latest 自动重置吞掉；固定分区使用封存的恢复坐标。
        if (ownerSessionStarted > 1) properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        consumer = KafkaNative.construct(() -> new KafkaConsumer<>(properties));
        bindBrokerEvidence();
        sessionState = GroupHealth.State.JOINING;
        if (mode == ConsumeMode.ASSIGN) {
            // 先取得完整组分配容量，再开放原生取入；固定范围超限不产生业务调用。
            acquire(fixedPartitions);
            consumer.assign(fixedPartitions);
            if (recoveryOffsets.isEmpty()) resolveStart();
            else restorePositions(fixedPartitions);
            for (TopicPartition tp : fixedPartitions) recoveryOffsets.putIfAbsent(tp, consumer.position(tp));
            // 本地 assign 不能替代 broker 取入证据，首次有效 poll 后才开放 Ready。
            joined = false;
        } else consumer.subscribe(List.copyOf(subscribedTopics), rebalanceListener());
        while ((!stopRequested || !invocations.isEmpty()) && !restartRequested) {
            finishInvocations();
            commitSafe(false);
            drainCommands();
            if (restartRequested) break;
            // broker 未确认安全水位时先保留现有责任，不再扩大尚未提交的业务副作用。
            if (!stopRequested && !commitBlocked) schedule();
            // 本地输入已全部安全完成时立即归还尾批责任；提交失败仍遵守原有重试间隔，不能空转重发。
            if (!commitBlocked && safeSinceCommit > 0 && invocations.isEmpty()
                    && partitions.values().stream().allMatch(partition -> partition.records.values().stream().allMatch(record -> record.state == Status.SAFE)))
                commitSafe(true);
            updatePauses();
            snapshot();
            if (mode == ConsumeMode.SUBSCRIBE && subscribedTopics.isEmpty()) {
                Thread.sleep(20);
                continue;
            }
            try {
                // 业务完成通知与原生维护各自推进；本地连续完成不能让每条消息重复执行一次网络状态维护。
                boolean localWork = retainedRecords > 0 || !invocations.isEmpty() || !commands.isEmpty();
                long pollMillis = Math.min(100, config.getPollTimeoutMs());
                long pollNanos = TimeUnit.MILLISECONDS.toNanos(pollMillis);
                ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
                if (!localWork || System.nanoTime() - lastPoll >= pollNanos) {
                    records = consumer.poll(localWork ? Duration.ZERO : Duration.ofMillis(pollMillis));
                    lastPoll = System.nanoTime();
                    verifyBrokerEvidence();
                    if (mode == ConsumeMode.ASSIGN && !stopRequested) joined = true;
                    // member.id 在再均衡期间仍可保留；只有 Assigned 回调可以重新授予 join 观测。
                    // 所有返回记录先进入责任账本，再允许任何业务任务启动。
                    observe(records);
                }
                if (localWork && records.isEmpty() && commands.isEmpty() && invocations.stream().noneMatch(i -> i.exited)) {
                    // native唤醒会中断commit等外部操作；本地permit只缩短调度等待，定时上限仍保证心跳、重试和预算持续推进。
                    LockSupport.parkNanos(this, Math.max(1, pollNanos - (System.nanoTime() - lastPoll)));
                }
            } catch (WakeupException wakeup) {
                if (!stopRequested) throw wakeup;
            }
        }
        commitSafe(true);
    }

    /**
     * 业务作用：绑定当前原生会话的响应与连接证据，不把历史会话指标带入恢复后的权威。
     * <p>
     * 参数说明：无。
     * 返回：保存固定指标引用；缺少原生支持时拒绝开放消费。
     */
    private void bindBrokerEvidence() {
        brokerResponses = null;
        brokerConnections = null;
        for (var entry : consumer.metrics().entrySet()) {
            if (!entry.getKey().group().equals("consumer-metrics")) continue;
            if (entry.getKey().name().equals("response-total")) brokerResponses = entry.getValue();
            if (entry.getKey().name().equals("connection-count")) brokerConnections = entry.getValue();
        }
        if (brokerResponses == null || brokerConnections == null)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "原生 consumer 缺少 broker 可达证据");
        brokerResponseCount = 0;
        lastBrokerResponse = System.nanoTime();
    }

    /**
     * 业务作用：在取入结果交给业务前复验 broker 可达性，使长期断连撤销 Ready 并进入有界恢复。
     * <p>
     * 参数说明：无。
     * 返回：近期有完整响应或只读探测成功；断连及探测超时交给 supervisor，停止期间不新建恢复责任。
     */
    private void verifyBrokerEvidence() {
        if (stopRequested) return;
        long now = System.nanoTime();
        double received = ((Number) brokerResponses.metricValue()).doubleValue();
        if (received > brokerResponseCount) {
            brokerResponseCount = received;
            lastBrokerResponse = now;
            return;
        }
        long silence = now - lastBrokerResponse;
        long probeAfter = TimeUnit.MILLISECONDS.toNanos(Math.max(1, config.getSessionTimeoutMs() / 2));
        // 全部连接失去后不能凭持续返回的空 poll 保持 Ready；短暂连接轮换保留一秒容忍。
        if (((Number) brokerConnections.metricValue()).doubleValue() == 0
                && silence >= Math.min(probeAfter, TimeUnit.SECONDS.toNanos(1)))
            throw new org.apache.kafka.common.errors.TimeoutException("BrokerConnectionUnavailable");
        if (silence < probeAfter) return;
        TopicPartition target = partitions.keySet().stream().findFirst().orElse(null);
        if (target == null && !subscribedTopics.isEmpty())
            target = new TopicPartition(subscribedTopics.iterator().next(), 0);
        if (target == null) return;
        // 固定分区全部暂停时可能没有 fetch 或心跳；有界只读查询补充证据，不改变读取位置或提交水位。
        long budgetMs = Math.max(1, Math.min(250, config.getMaxPollIntervalMs() / 4));
        consumer.endOffsets(List.of(target), Duration.ofMillis(budgetMs));
        lastBrokerResponse = System.nanoTime();
        brokerResponseCount = ((Number) brokerResponses.metricValue()).doubleValue();
    }

    /**
     * 业务作用：撤销旧会话权威并封存未提交的精确恢复边界。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：真实调用退出且原生 consumer 关闭成功后才清理会话引用；关闭失败保留客户端并退避重试，不开放新会话或报告退出。
     */
    private void closeSession() {
        joined = false;
        budget.assign(slot, List.of());
        nextEpoch++;
        // 先使旧 Ack 失效，再等待真实调用；超时不能授权新 owner 与旧副作用同时运行。
        partitions.values().forEach(partition -> {
            partition.active = false;
            partition.records.values().forEach(record -> {
                if (record.retryLease != null) record.retryLease.close();
            });
            if (!partition.records.isEmpty()) recoveryOffsets.put(partition.tp, partition.records.firstKey());
        });
        sessionState = GroupHealth.State.RECOVERING;
        snapshot();
        while (!invocations.isEmpty()) {
            finishInvocations();
            snapshot();
            if (!invocations.isEmpty()) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ignored) {
                }
            }
        }
        for (Partition partition : List.copyOf(partitions.values())) releasePartition(partition);
        partitions.clear();
        boolean interrupted = false;
        while (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(2));
                consumer = null;
            } catch (RuntimeException closeFailure) {
                // 关闭失败不能解除原生资源责任；旧客户端退出前禁止重建，也不能把本地清空引用当作已关闭。
                reason = "ConsumerCloseFailed";
                snapshot();
                interrupted |= Thread.interrupted();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException retained) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
        commitBlocked = false;
        safeSinceCommit = 0;
        snapshot();
    }

    /**
     * 业务作用：固定已接纳输入的恢复坐标，拒绝保留期造成的静默跳跃。
     * <p>
     * 返回：恢复到未提交起点，缺口则进入保护态。
     *
     * @param targets 重新分配范围。
     */
    private void restorePositions(Collection<TopicPartition> targets) {
        Map<TopicPartition, Long> beginning = consumer.beginningOffsets(targets);
        Map<TopicPartition, Long> end = consumer.endOffsets(targets);
        Map<TopicPartition, OffsetAndMetadata> committed = mode == ConsumeMode.SUBSCRIBE ? consumer.committed(Set.copyOf(targets)) : Map.of();
        for (TopicPartition tp : targets) {
            Long offset = mode == ConsumeMode.SUBSCRIBE && committed.get(tp) != null ? committed.get(tp).offset() : recoveryOffsets.get(tp);
            if (offset == null) {
                // 从未取得过该分区的恢复坐标时才使用初始策略；已有责任不能重新解释成 latest。
                if ("earliest".equals(config.getAutoOffsetReset())) consumer.seekToBeginning(List.of(tp));
                else if ("latest".equals(config.getAutoOffsetReset())) consumer.seekToEnd(List.of(tp));
                else throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "RecoveryOffsetMissing");
                recoveryOffsets.put(tp, consumer.position(tp));
                continue;
            }
            // 历史输入已经超出 broker 保留范围时不能重新解释成 earliest 或 latest。
            if (offset < beginning.get(tp) || offset > end.get(tp))
                throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "RecoveryOffsetOutOfRange");
            consumer.seek(tp, offset);
        }
    }

    /**
     * 业务作用：从滑动时间窗领取一次自动重建额度。
     * <p>
     * 参数说明：无。
     *
     * @return 预算足够时登记本次尝试。
     */
    private boolean reserveRestart() {
        long now = System.nanoTime();
        long window = TimeUnit.MILLISECONDS.toNanos(config.getConsumeRestartBudgetWindowMs());
        while (!restartHistory.isEmpty() && now - restartHistory.peekFirst() >= window) restartHistory.removeFirst();
        if (restartHistory.size() >= config.getConsumeRestartBudgetTimes()) return false;
        restartHistory.addLast(now);
        return true;
    }

    /**
     * 业务作用：限制故障重连频率并打散多个消费组同时恢复。
     * <p>
     * 参数说明：无。
     *
     * @return 不超过上限的毫秒退避。
     */
    private long restartDelay() {
        long max = config.getConsumeRestartMaxDelayMs();
        long delay = config.getConsumeRestartDelayMs();
        for (int n = 1; n < restartHistory.size() && delay < max; n++) delay = delay > max / 2 ? max : delay * 2;
        return delay == 0 ? 0 : ThreadLocalRandom.current().nextLong(delay / 2, delay + 1);
    }

    /**
     * 业务作用：退避时仍响应停止和显式重建，不占住控制邮箱。
     * <p>
     * 返回：期限或停止到达即结束。
     *
     * @param millis   最长等待
     * @param controls 表示允许处理恢复命令。
     */
    private void awaitSupervisorDelay(long millis, boolean controls) throws InterruptedException {
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, millis));
        while (!stopRequested && !restartRequested && System.nanoTime() < until) {
            if (controls) drainCommands();
            Thread.sleep(10);
        }
    }

    /**
     * 业务作用：在 rebalance 回调中交回或取得分区权威。
     * <p>
     * 参数说明：无。
     *
     * @return 只在 owner 上执行的监听器。
     */
    private ConsumerRebalanceListener rebalanceListener() {
        return new ConsumerRebalanceListener() {
            /**
             * 业务作用：撤销前只提交已完成前缀。
             *
             * 返回：先收集已结束调用并尝试提交安全前缀，再撤销目标分区权威；剩余旧调用不能凭晚到结果推进新代次。
             *
             * @param revoked 撤销集合。
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
                finishInvocations();
                commitSafe(true);
                revoke(revoked);
            }

            /**
             * 业务作用：失权后禁止任何旧身份提交。
             *
             * 返回：直接撤销，不尝试提交。
             *
             * @param lost 已丢失集合。
             */
            @Override
            public void onPartitionsLost(Collection<TopicPartition> lost) {
                revoke(lost);
            }

            /**
             * 业务作用：为新分配生成独立确认代次。
             *
             * 返回：已有保留分区不重置。
             *
             * @param assigned 当前分配。
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
                proxy.metric("rebalance_assigned_total", 1, true, Map.of("group", groupId));
                acquire(assigned);
                if (ownerSessionStarted > 1) restorePositions(assigned);
                // 即使暂未收到消息，也冻结首次位置；重建不能因空 poll 而重新移动 latest 起点。
                for (TopicPartition tp : assigned) recoveryOffsets.putIfAbsent(tp, consumer.position(tp));
                joined = true;
            }
        };
    }

    /**
     * 业务作用：先校验共享组容量再登记新分区权威。
     * <p>
     * 返回：新分区拥有独立 epoch；超限拒绝且不开放新业务。
     *
     * @param assigned 本次新增或保留范围。
     */
    private void acquire(Collection<TopicPartition> assigned) {
        Set<TopicPartition> complete = new HashSet<>(partitions.keySet());
        complete.addAll(assigned);
        budget.assign(slot, complete);
        for (TopicPartition tp : assigned) partitions.computeIfAbsent(tp, key -> new Partition(key, ++nextEpoch));
    }

    /**
     * 业务作用：封存旧分区且保留仍在执行的责任。
     * <p>
     * 返回：旧结果不能再更新新进度。
     *
     * @param revoked 撤销集合。
     */
    private void revoke(Collection<TopicPartition> revoked) {
        nextEpoch++;
        for (TopicPartition tp : revoked) {
            Partition old = partitions.remove(tp);
            if (old == null) continue;
            old.active = false;
            // 失权后次数不能授权新代次；实际输入与调用额度仍等待原有责任退出。
            old.records.values().forEach(record -> {
                if (record.retryLease != null) record.retryLease.close();
            });
            // 旧调用仍引用分区；先移除不再用于派发的索引，避免已归还额度的原文随慢调用继续存活。
            old.keyQueues.clear();
            old.keyHeads.clear();
            old.records.values().removeIf(record -> {
                if (record.state == Status.RUNNING) return false;
                releaseUnconfirmed(record);
                return true;
            });
        }
        budget.assign(slot, partitions.keySet());
        joined = false;
        snapshot();
    }

    /**
     * 业务作用：解析固定分区的初始起点。
     * <p>
     * 参数说明：无。
     * 返回：明确位置，显式越界拒绝。
     */
    private void resolveStart() {
        switch (startOffset.kind()) {
            case EARLIEST -> consumer.seekToBeginning(fixedPartitions);
            case LATEST -> consumer.seekToEnd(fixedPartitions);
            case TIMESTAMP -> {
                Map<TopicPartition, Long> times = new LinkedHashMap<>();
                fixedPartitions.forEach(tp -> times.put(tp, startOffset.value()));
                applyTimestamps(times);
            }
            case EXPLICIT -> {
                Map<TopicPartition, Long> beginning = consumer.beginningOffsets(fixedPartitions);
                Map<TopicPartition, Long> end = consumer.endOffsets(fixedPartitions);
                for (TopicPartition tp : fixedPartitions) {
                    long offset = startOffset.value();
                    if (offset < beginning.get(tp) || offset > end.get(tp))
                        throw new KafkaFailure(KafkaFailure.Code.CONFIG, "显式起点超出保留范围");
                    consumer.seek(tp, offset);
                }
            }
            case COMMITTED -> {
                Map<TopicPartition, OffsetAndMetadata> offsets = consumer.committed(fixedPartitions);
                offsets.forEach((tp, offset) -> {
                    if (offset != null) consumer.seek(tp, offset.offset());
                });
            }
        }
    }

    /**
     * 业务作用：为 poll 返回记录建立完整责任与路由。
     * <p>
     * 返回：尚未调用业务的账本。
     *
     * @param records 本轮输入。
     */
    private void observe(ConsumerRecords<byte[], byte[]> records) {
        for (TopicPartition tp : records.partitions()) {
            Partition partition = partitions.get(tp);
            if (partition == null) continue;
            for (ConsumerRecord<byte[], byte[]> record : records.records(tp)) {
                Pending pending = new Pending(record);
                if (pending.bytes > Math.min(config.getMaxRecordBytes(), budget.maximumRecordBytes())) {
                    // 超大输入保持 broker 恢复坐标，不为了停机或 DLT 继续持有超预算的原文。
                    consumer.seek(tp, record.offset());
                    partition.halted = true;
                    reason = "OversizedRecord";
                    break;
                }
                if (partition.records.size() >= config.getMaxProgressRecordsPerPartition()
                        || (pending.lease = budget.retain(tp, pending.inputKey, pending.reservedBytes)) == null) {
                    // 本轮尚未派发任何业务，明确保留重拉坐标；此前账本不含更高 offset，提交不能越过此记录。
                    consumer.seek(tp, record.offset());
                    partition.admissionBytes = pending.reservedBytes;
                    partition.admissionKey = pending.inputKey;
                    break;
                }
                try {
                    if (!partition.offsets.observe(record.offset())) {
                        pending.lease.close();
                        continue;
                    }
                } catch (RuntimeException invalid) {
                    // 顺序校验失败时输入尚未进入分区账本；先归还本次预留，不能让会话重建遗留隐形占用。
                    pending.lease.close();
                    throw invalid;
                }
                recoveryOffsets.putIfAbsent(tp, record.offset());
                partition.records.put(record.offset(), pending);
                partition.admissionBytes = 0;
                partition.admissionKey = null;
                retainedBytes += pending.reservedBytes;
                retainedRecords++;
                try {
                    String event = header(record, KafkaProxy.HEADER_EVENT);
                    if (event == null) event = KafkaProxy.DEFAULT_EVENT;
                    if (KafkaWire.blankEvent(event)) throw new IllegalArgumentException("event 为空");
                    var routes = proxy.listenerRegistry().get(groupId);
                    pending.entry = routes == null || routes.get(tp.topic()) == null ? null : routes.get(tp.topic()).get(event);
                    if (pending.entry == null) pending.policy = config.getUnmatchedPolicy();
                    String codec = header(record, KafkaProxy.HEADER_PAYLOAD_CODEC);
                    if (codec != null && !KafkaProxy.PAYLOAD_CODEC_PROTOCOL_BYTES.equals(codec))
                        throw new IllegalArgumentException("未知 codec");
                    header(record, KafkaProxy.HEADER_PAYLOAD_MODE);
                    if (record.value() == null && pending.entry != null && !(pending.entry.listener() instanceof KafkaRawListener)
                            && !(pending.entry.listener() instanceof KafkaPassthroughListener)) {
                        // 删除标记不等同于畸形 JSON；只有匹配的 typed 路由采用明确的源策略。
                        pending.policy = switch (config.getTombstonePolicy()) {
                            case SKIP -> RecordPolicy.SKIP;
                            case DEAD_LETTER -> RecordPolicy.DEAD_LETTER;
                            case HALT -> RecordPolicy.HALT;
                            case DELIVER -> null;
                        };
                        pending.failure = "Tombstone";
                    }
                } catch (RuntimeException invalid) {
                    pending.entry = null;
                    pending.policy = config.getInvalidRecordPolicy();
                    pending.failure = "InvalidHeaders";
                }
                if (partition.keyCandidatesOnly) {
                    if (pending.entry != null && pending.entry.executionMode() == ExecutionMode.KEY_ORDERED) {
                        var queue = partition.keyQueues.computeIfAbsent(keyDomain(pending), unused -> new ArrayDeque<>());
                        if (queue.isEmpty()) partition.keyHeads.put(record.offset(), pending);
                        queue.addLast(pending);
                    } else {
                        // 混合顺序屏障采用完整账本裁决，不能让键索引跳过非按键路由或畸形输入。
                        partition.keyCandidatesOnly = false;
                        partition.keyQueues.clear();
                        partition.keyHeads.clear();
                    }
                }
            }
        }
    }

    /**
     * 业务作用：按声明的顺序域派发有界调用。
     * <p>
     * 参数说明：无。
     * 返回：已派发记录保留独占调用责任。
     */
    private void schedule() {
        List<Partition> order = new ArrayList<>(partitions.values());
        if (!order.isEmpty()) {
            // 起点逐轮移动，前面的热分区不能持续抢走刚归还的全部调用名额。
            Collections.rotate(order, -(partitionCursor % order.size()));
            partitionCursor = (partitionCursor + 1) % order.size();
        }
        int before;
        do {
            before = invocations.size();
            for (Partition partition : order) schedulePartition(partition);
        } while (!stopRequested && invocations.size() > before);
    }

    /**
     * 业务作用：为一个分区派发至多一个可用顺序域，让轮转中的其它分区获得同等准入机会。
     * <p>
     * 返回：没有可用名额或顺序被占用时保持原责任等待。
     *
     * @param partition 当前分区责任。
     */
    private void schedulePartition(Partition partition) {
        if (partition.halted || userPaused.contains(partition.tp)) return;
        Set<String> blocked = new HashSet<>();
        boolean partitionRunning = false;
        // 本地旧代次调用尚未物理退出时，新的同分区同 key 调用继续等待，不能仅凭失权释放顺序槽。
        for (Invocation invocation : invocations)
            if (invocation.partition.tp.equals(partition.tp)) {
                partitionRunning = true;
                for (Pending pending : invocation.records) {
                    if (pending.record.key() == null) return;
                    blocked.add(keyDomain(pending));
                }
            }
        // 调度只读取当前账本并改变记录状态，不复制整个未提交窗口；提交裁剪仍由 owner 下一阶段完成。
        Iterator<Pending> candidates = schedulingCandidates(partition);
        while (candidates.hasNext()) {
            Pending first = candidates.next();
            if (first.state == Status.SAFE) continue;
            String key = keyDomain(first);
            boolean ordered = first.record.key() == null || first.entry == null || first.entry.executionMode() == ExecutionMode.PARTITION_ORDERED;
            if (first.state == Status.RUNNING || first.retryAt > System.nanoTime()) {
                blocked.add(key);
                if (ordered) break;
                else continue;
            }
            if (blocked.contains(key)) continue;
            if (ordered && partitionRunning) break;
            blocked.add(key);
            if (first.policy != null) {
                if (first.policy == RecordPolicy.HALT) {
                    partition.halted = true;
                    reason = first.failure == null ? "UnmatchedRecord" : first.failure;
                    break;
                }
                if (first.policy == RecordPolicy.SKIP) {
                    safe(partition, first);
                    if (partition.keyCandidatesOnly) candidates = schedulingCandidates(partition);
                    continue;
                }
                startDeadLetter(partition, first);
            } else {
                List<Pending> run = new ArrayList<>();
                run.add(first);
                boolean batch = first.entry.kind() == ListenerKind.BATCH || first.entry.kind() == ListenerKind.RECORD_BATCH;
                if (batch) {
                    long batchBytes = first.bytes;
                    while (candidates.hasNext()) {
                        Pending next = candidates.next();
                        if (next.state != Status.WAITING || next.entry != first.entry || next.policy != null || next.retryAt > System.nanoTime())
                            break;
                        // 积累输入不能在单次业务批中突破数量或字节边界；失败后逐条取得 DLT 信封额度。
                        if (run.size() >= config.getMaxPollRecords() || next.bytes > config.getMaxBatchBytes() - batchBytes)
                            break;
                        run.add(next);
                        batchBytes += next.bytes;
                    }
                }
                startHandler(partition, run);
            }
            partitionRunning |= first.state == Status.RUNNING;
            // 每轮每分区至多派发一个顺序域，其它分区先有机会使用剩余额度。
            if (ordered || first.state == Status.RUNNING) break;
        }
    }

    /**
     * 业务作用：只检查每个按键顺序域的最早未完成输入，避免热键尾部反复参与准入判断。
     *
     * @param partition 当前分区责任。
     * @return 按真实 offset 排列的候选，null key 保持同一顺序中的分区屏障；混合路由使用完整账本。
     */
    private Iterator<Pending> schedulingCandidates(Partition partition) {
        return partition.keyCandidatesOnly ? new ArrayList<>(partition.keyHeads.values()).iterator() : partition.records.values().iterator();
    }

    /**
     * 业务作用：为已接纳业务键建立并复用分区内顺序域。
     *
     * @param pending 已持有输入额度的记录。
     * @return 完整原始 key 的稳定身份，null key 使用独立串行后备域。
     */
    private String keyDomain(Pending pending) {
        // 只有输入额度准入后才物化完整键身份；热 key 反复等待不能每轮复制同一原文。
        if (pending.keyDomain == null)
            pending.keyDomain = pending.record.key() == null ? "null" : "bytes:" + Base64.getEncoder().encodeToString(pending.record.key());
        return pending.keyDomain;
    }

    /**
     * 业务作用：在 worker 中完成解码与整个业务调用。
     * <p>
     * 返回：结果仅由 owner 处理。
     *
     * @param partition 当前权威
     * @param records   连续 run。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void startHandler(Partition partition, List<Pending> records) {
        // 停止可能发生在本轮调度中途；未启动的输入保留给下一次消费，不再增加外部副作用。
        if (stopRequested) return;
        Pending first = records.getFirst();
        boolean exclusive = first.record.key() == null || first.entry.executionMode() == ExecutionMode.PARTITION_ORDERED;
        KafkaGroupBudget.Lease call = budget.invoke(partition.tp, keyDomain(first), exclusive, false);
        if (call == null) return;
        Invocation invocation = new Invocation(partition, records, false, call);
        records.forEach(p -> {
            p.state = Status.RUNNING;
            p.attempts++;
            p.deliveries++;
        });
        invocations.add(invocation);
        try {
            workers.execute(() -> {
                List<KafkaTraceContext.Extraction> traces = new ArrayList<>();
                List<KafkaTraceContext> links = new ArrayList<>();
                int omitted = 0;
                for (Pending pending : records) {
                    var extracted = KafkaTraceContext.extract(pending.record.headers());
                    traces.add(extracted);
                    if (extracted.invalid() != null)
                        proxy.metric("trace_invalid_total", 1, true, Map.of("group", groupId, "reason", extracted.invalid().name().toLowerCase(Locale.ROOT)));
                    if (records.size() > 1 && extracted.context() != null && !links.contains(extracted.context())) {
                        if (links.size() < 128) links.add(extracted.context());
                        else omitted++;
                    }
                }
                // 批处理建立独立根并保留逐输入链接；不能把首条消息的父身份冒充整批因果关系。
                KafkaSpan trace = proxy.span(groupId, records.getFirst().record.topic(), KafkaTraceSpan.Kind.CONSUMER, records.size() == 1 ? traces.getFirst().context() : null, links, omitted);
                ConsumeContext old = KafkaConsumeContexts.enter(new ConsumeContext(proxy.sourceId(), groupId, proxy, partition.epoch, records.getFirst().deliveries, records.getFirst().attempts, records.getFirst().record.value() == null, trace.context, links, omitted));
                boolean traceReturned = false;
                List<AckToken> tokens = new ArrayList<>();
                try {
                    KafkaProxyHolder.setKafkaProxy(proxy);
                    KafkaProxyHolder.setTopic(records.getFirst().record.topic());
                    KafkaProxyHolder.setPartition(records.getFirst().record.partition());
                    KafkaProxyHolder.setOffset(records.getFirst().record.offset());
                    ListenerEntry entry = records.getFirst().entry;
                    if (entry.listener() instanceof KafkaPassthroughListener passthrough) {
                        // 透传不建立拥有型头和值快照；视图在 finally 中失效，保留子视图也不能延长原文持有。
                        AckToken token = new AckToken(entry.ackMode(), () -> partition.active && invocation.valid());
                        tokens.add(token);
                        RawKafkaRecord view = new RawKafkaRecord(records.getFirst().record, token, ConsumeContext.current());
                        PassthroughDisposition disposition;
                        long callbackStarted = 0;
                        try {
                            if (!invocation.valid() || !partition.active)
                                throw new KafkaFailure(KafkaFailure.Code.ACK_EXPIRED, "透传确认权已结束");
                            callbackStarted = System.nanoTime();
                            disposition = Objects.requireNonNull(passthrough.onRecord(view), "透传必须返回明确结果");
                        } finally {
                            view.invalidate();
                            if (callbackStarted != 0) {
                                Map<String, String> labels = Map.of("group", groupId);
                                proxy.metric("passthrough_calls_total", 1, true, labels);
                                proxy.metric("passthrough_duration_micros_total", Math.max(0, (System.nanoTime() - callbackStarted) / 1000), true, labels);
                            }
                        }
                        boolean succeeded = invocation.valid() && partition.active;
                        // 安全跳过是独立完成事实；其余成功结果仍须满足 Manual 的临时确认。
                        boolean confirmed = disposition instanceof PassthroughDisposition.Skipped
                                ? succeeded : token.finish(succeeded);
                        invocation.result.complete(new Outcome(List.of(confirmed), null, Map.of()));
                        traceReturned = true;
                        return;
                    }
                    KafkaProxyHolder.setHeaders(new KafkaHeaders(records.getFirst().record.headers()).asMap());
                    List<Object> values = new ArrayList<>();
                    List<KafkaRecord<Object>> wrapped = new ArrayList<>();
                    Map<Long, String> invalid = new HashMap<>();
                    for (Pending pending : records) {
                        // 停止、超时或失权后不再开启本批后续解码；正在执行的 codec 仍保留真实退出责任。
                        if (!invocation.valid() || !partition.active)
                            throw new KafkaFailure(KafkaFailure.Code.ACK_EXPIRED, "解码权威已结束");
                        ConsumerRecord<byte[], byte[]> record = pending.record;
                        try {
                            Object data = proxy.decode(record.value(), entry.paramType(), record.topic(),
                                    header(record, KafkaProxy.HEADER_PAYLOAD_CODEC), header(record, KafkaProxy.HEADER_PAYLOAD_MODE),
                                    entry.listener() instanceof KafkaRawListener);
                            if (data == null && !entry.nullable())
                                throw new IllegalArgumentException("目标类型不允许 null");
                            String key = record.key() == null ? null : lenientKey(record.key());
                            AckToken token = new AckToken(entry.ackMode(), () -> partition.active && invocation.valid());
                            tokens.add(token);
                            KafkaHeaders headers = KafkaWire.deliveryHeaders(new KafkaHeaders(record.headers()), pending.deliveries);
                            Header passthrough = record.headers().lastHeader(KafkaProxy.HEADER_PASSTHROUGH);
                            if (passthrough != null && passthrough.value() != null)
                                proxy.restorePassthrough(data, passthrough.value(), records.size() == 1);
                            wrapped.add(new KafkaRecord<>(record.topic(), record.partition(), record.offset(), record.timestamp(), key, data,
                                    Map.of(), token, headers, record.key(), new ConsumeContext(proxy.sourceId(), groupId, proxy, partition.epoch, pending.deliveries, pending.attempts, record.value() == null, KafkaTraceContext.extract(record.headers()).context(), List.of())));
                            values.add(data);
                        } catch (RuntimeException invalidRecord) {
                            invalid.put(record.offset(), "PayloadDecode");
                        }
                    }
                    if (!invalid.isEmpty()) {
                        invocation.result.complete(new Outcome(null, null, invalid));
                        return;
                    }
                    // 解码可能耗时；调用业务前再次核对停机、超时和分区权威，不能凭派发时的许可进入业务。
                    if (!invocation.valid() || !partition.active)
                        throw new KafkaFailure(KafkaFailure.Code.ACK_EXPIRED, "handler 确认权已结束");
                    if (entry.listener() instanceof KafkaAsyncBatchListener async) {
                        CompletionStage<Void> stage = async.onRecords(wrapped, ConsumeContext.current());
                        if (stage == null) throw new IllegalStateException("异步 listener 必须返回结果");
                        stage.toCompletableFuture().get();
                    } else if (entry.listener() instanceof KafkaAsyncListener async) {
                        CompletionStage<Void> stage = async.onRecord(wrapped.getFirst(), ConsumeContext.current());
                        if (stage == null) throw new IllegalStateException("异步 listener 必须返回结果");
                        stage.toCompletableFuture().get();
                    } else {
                        Object argument = switch (entry.kind()) {
                            case SINGLE -> values.getFirst();
                            case BATCH -> values;
                            case RECORD_SINGLE -> wrapped.getFirst();
                            case RECORD_BATCH -> wrapped;
                            case PASSTHROUGH -> throw new IllegalStateException("透传必须由只读视图入口分发");
                        };
                        entry.listener().onEvent(argument);
                    }
                    boolean succeeded = invocation.valid() && partition.active;
                    List<Boolean> confirmed = new ArrayList<>();
                    boolean prefix = true;
                    int ackedAfterGap = 0;
                    for (AckToken token : tokens) {
                        boolean acked = token.finish(succeeded);
                        if (acked && !prefix) ackedAfterGap++;
                        prefix = acked && prefix;
                        confirmed.add(prefix);
                    }
                    invocation.result.complete(new Outcome(confirmed, succeeded ? null : new KafkaFailure(KafkaFailure.Code.OUTCOME_UNKNOWN, "handler 已停止、超时或失权"), Map.of(), ackedAfterGap));
                    traceReturned = true;
                } catch (Throwable failure) {
                    tokens.forEach(token -> token.finish(false));
                    invocation.result.complete(new Outcome(null, unwrap(failure), Map.of()));
                } finally {
                    tokens.forEach(token -> token.finish(false));
                    KafkaProxyHolder.clear();
                    KafkaConsumeContexts.restore(old);
                    trace.finish(!invocation.valid() || !partition.active ? KafkaTraceSpan.Outcome.AUTHORITY_LOST : traceReturned ? KafkaTraceSpan.Outcome.RETURNED : KafkaTraceSpan.Outcome.FAILED);
                    // 业务终态与执行责任释放分开；上下文归还之前不得开放下一次同序调用或报告关闭。
                    invocation.exited = true;
                    signalOwner();
                }
            });
        } catch (RuntimeException rejected) {
            invocation.result.complete(new Outcome(null, rejected, Map.of()));
            invocation.exited = true;
        }
    }

    /**
     * 业务作用：为所有消费路线提供可选文本视图，顺序域和死信证据始终使用原始 key。
     *
     * @param bytes 原始键。
     * @return 合法 UTF-8 或 null；非 UTF-8 本身不构成坏消息。
     */
    private static String lenientKey(byte[] bytes) {
        try {
            return utf8(bytes);
        } catch (RuntimeException invalid) {
            return null;
        }
    }

    /**
     * 业务作用：将 DLT 交付纳入同一责任生命周期。
     * <p>
     * 返回：broker 回执前不会安全确认。
     *
     * @param partition 来源权威
     * @param record    失败消息。
     */
    private void startDeadLetter(Partition partition, Pending record) {
        // 停止只等待已发出的交付，不再启动新的 DLT 轮次。
        if (stopRequested) return;
        String suffix = config.getDeadLetterTopicSuffix();
        if (suffix == null || suffix.isBlank() || record.record.topic().endsWith(suffix)) {
            partition.halted = true;
            reason = suffix == null || suffix.isBlank() ? "DltDisabled" : "RecursiveDlt";
            return;
        }
        if (record.dltReason == null) record.dltReason = record.failure == null ? "UnmatchedRecord" : record.failure;
        partition.mayHaveDlt = true;
        long retained = dltRetainedBytes(record);
        if (retained > config.getDltQueueMaxBytes()) {
            partition.halted = true;
            reason = "DLTRecordTooLarge";
            return;
        }
        if (record.dltLease == null && (record.dltLease = budget.retainDlt(retained)) == null) return;
        boolean exclusive = record.record.key() == null || record.entry == null || record.entry.executionMode() == ExecutionMode.PARTITION_ORDERED;
        KafkaGroupBudget.Lease call = budget.invoke(partition.tp, keyDomain(record), exclusive, true);
        if (call == null) return;
        Invocation invocation = new Invocation(partition, List.of(record), true, call);
        record.state = Status.RUNNING;
        invocations.add(invocation);
        try {
            // 目标配置查询和真实交付都属于此调用责任；放在有界任务中，owner继续poll并保持组协调。
            workers.execute(() -> {
                try {
                    proxy.deadLetter(record.record, groupId, record.deliveries, record.dltReason, () -> stopRequested, group).join();
                    invocation.result.complete(new Outcome(List.of(true), null, Map.of()));
                } catch (Throwable failure) {
                    invocation.result.complete(new Outcome(null, failure, Map.of()));
                } finally {
                    invocation.exited = true;
                    signalOwner();
                }
            });
        } catch (Throwable failure) {
            invocation.result.complete(new Outcome(null, failure, Map.of()));
            invocation.exited = true;
        }
    }

    /**
     * 业务作用：复制死信原文之前保守计算完整信封责任，避免测量本身创建未计费副本。
     *
     * @param record 原输入。
     * @return 原文字节、group/topic UTF-8 长度加4096证据余量，覆盖有界原因、源与集群身份、字段名和数字。
     */
    private long dltRetainedBytes(Pending record) {
        if (record.dltBytes == 0) record.dltBytes = record.bytes
                + groupId.getBytes(StandardCharsets.UTF_8).length
                + record.record.topic().getBytes(StandardCharsets.UTF_8).length + 4096;
        return record.dltBytes;
    }

    /**
     * 业务作用：消费实际退出通知，保持失败和延期的独立预算。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：只结算已经实际退出的调用；停止后的普通业务结果不推进水位，DLT 确认仅在原分区仍有效时可推进，失权结果只释放未确认责任。
     */
    private void finishInvocations() {
        Iterator<Invocation> iterator = invocations.iterator();
        while (iterator.hasNext()) {
            Invocation invocation = iterator.next();
            Outcome outcome = invocation.result.getNow(null);
            if (outcome == null || !invocation.exited) continue;
            iterator.remove();
            invocation.call.close();
            if (invocation.partition.active && !stopRequested && !invocation.deadLetter)
                observeInvocation(invocation, outcome);
            boolean retryAdmitted = admitRetries(invocation, outcome);
            for (int index = 0; index < invocation.records.size(); index++) {
                Pending record = invocation.records.get(index);
                if (!invocation.partition.active) {
                    releaseUnconfirmed(record);
                    // 同分区其它旧调用可能尚未退出，已退出输入不能继续由旧账本保留原文。
                    invocation.partition.records.remove(record.record.offset());
                    continue;
                }
                record.state = Status.WAITING;
                // 停止后不能用旧业务结果完成输入；保留账本直到实际执行退出并交回 broker 重投。
                if (stopRequested && !invocation.deadLetter) continue;
                if (!outcome.invalid.isEmpty()) {
                    record.attempts--;
                    if (outcome.invalid.containsKey(record.record.offset())) {
                        record.policy = config.getInvalidRecordPolicy();
                        record.failure = outcome.invalid.get(record.record.offset());
                        invocation.partition.decodeFailures++;
                        // 连续解码失败可能来自整个 codec 合同不匹配，不能把整条主题持续排入 DLT。
                        if (config.getDecodeFailureHaltStreak() > 0 && invocation.partition.decodeFailures >= config.getDecodeFailureHaltStreak()) {
                            invocation.partition.halted = true;
                            reason = "DecodeFailureStreak";
                        }
                    } else {
                        invocation.partition.decodeFailures = 0;
                    }
                    continue;
                }
                if (!invocation.deadLetter) invocation.partition.decodeFailures = 0;
                if (outcome.safe != null && outcome.safe.get(index)) {
                    if (invocation.deadLetter)
                        proxy.metric("dlt_total", 1, true, Map.of("group", groupId, "topic", record.record.topic()));
                    safe(invocation.partition, record);
                    continue;
                }
                // 停止仅收口确定交付成功的 DLT；失败或未发送责任交回重投，不因关闭触发可用性优先丢弃。
                if (stopRequested) continue;
                // 整批新增失败元数据不能部分准入；已成功前缀仍可提交，失败范围保持保护态。
                if (!retryAdmitted) {
                    record.state = Status.HALTED;
                    continue;
                }
                Throwable error = unwrap(outcome.failure);
                record.failure = error == null ? "ManualAckMissing" : error.getClass().getSimpleName();
                // 尝试失败与责任终结分别计数，可靠模式持续重试时仍能观测权限或目标配置拒绝。
                if (invocation.deadLetter)
                    proxy.metric("dlt_attempt_failed_total", 1, true, Map.of("group", groupId, "topic", record.record.topic()));
                if (invocation.deadLetter && !config.isDeadLetterRequired()) {
                    proxy.metric("dlt_failed_total", 1, true, Map.of("group", groupId, "topic", record.record.topic()));
                    // 只有显式选择可用性优先时才允许丢弃未投递成功的 DLT；默认路径始终保留恢复责任。
                    reason = "DltUnavailableDiscarded";
                    safe(invocation.partition, record);
                    continue;
                }
                // 后续快照仅在本账本实际出现过退避时扫描记录；先发布标记，再设置任何重试时刻。
                invocation.partition.mayHaveRetry = true;
                if (!invocation.deadLetter) {
                    if (error instanceof PassthroughFailure failure) {
                        record.failure = failure.reason().name();
                        // 硬暂停与致命不变量失败必须先关闭业务，不得误落入普通重试或耗尽后跳过输入。
                        switch (failure.action()) {
                            case INVALID_RECORD -> record.policy = config.getInvalidRecordPolicy();
                            case DEAD_LETTER -> record.policy = RecordPolicy.DEAD_LETTER;
                            case HALT -> {
                                invocation.partition.halted = true;
                                reason = record.failure;
                            }
                            case FATAL -> throw failure;
                            case RETRY -> {
                                if (config.getMaxConsumeAttempts() > 0 && record.attempts >= config.getMaxConsumeAttempts())
                                    record.policy = RecordPolicy.DEAD_LETTER;
                            }
                        }
                        record.retryAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getConsumerRetryMs());
                        continue;
                    }
                    if (error instanceof KafkaFailure failure && failure.code() == KafkaFailure.Code.HANDLER_DEFERRED)
                        record.attempts--;
                    else if (error instanceof KafkaFailure failure && failure.code() == KafkaFailure.Code.HANDLER_DEAD_LETTER
                            || config.getMaxConsumeAttempts() > 0 && record.attempts >= config.getMaxConsumeAttempts())
                        record.policy = RecordPolicy.DEAD_LETTER;
                    if (!(error instanceof KafkaFailure failure && failure.code() == KafkaFailure.Code.HANDLER_DEAD_LETTER))
                        proxy.metric("retried_total", 1, true, Map.of("group", groupId, "topic", record.record.topic()));
                }
                if (invocation.deadLetter) {
                    // DLT 失败不能释放原输入或信封额度；只移动下次准入时间，普通业务重试预算保持独立。
                    record.dltFailures = Math.min(0xffff_ffffL, record.dltFailures + 1);
                    long delay = DltRetryPolicy.delayMillis(config.getDltRetryInitialMs(), config.getDltRetryMaxMs(), record.dltFailures, record.record.offset());
                    record.retryAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay);
                } else {
                    record.retryAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getConsumerRetryMs());
                }
            }
        }
    }

    /**
     * 业务作用：将普通失败、延期与缺失确认按记录纳入组共享次数容量。
     *
     * @param invocation 实际退出调用
     * @param outcome    冻结结果。
     * @return 无需登记或整批登记成功时为 true；超限保护分区且不登记部分新记录。
     */
    private boolean admitRetries(Invocation invocation, Outcome outcome) {
        if (!invocation.partition.active || stopRequested || invocation.deadLetter || !outcome.invalid.isEmpty())
            return true;
        Throwable error = unwrap(outcome.failure);
        if (error instanceof PassthroughFailure failure && failure.action() != PassthroughFailure.Action.RETRY)
            return true;
        if (error instanceof KafkaFailure failure && failure.code() == KafkaFailure.Code.HANDLER_DEAD_LETTER)
            return true;
        List<Pending> missing = new ArrayList<>();
        for (int index = 0; index < invocation.records.size(); index++) {
            Pending record = invocation.records.get(index);
            if ((outcome.safe == null || !outcome.safe.get(index)) && record.retryLease == null) missing.add(record);
        }
        List<KafkaGroupBudget.Lease> leases = budget.retainRetries(missing.size());
        if (leases == null) {
            // 次数证据不足时不能继续毒消息决策或跳过输入，只关闭失败分区等待明确控制。
            invocation.partition.halted = true;
            reason = "RetryStateCapacity";
            return false;
        }
        for (int index = 0; index < missing.size(); index++) missing.get(index).retryLease = leases.get(index);
        return true;
    }

    /**
     * 业务作用：只记录当前权威实际接管的业务结果，区分临时确认和 broker 提交。
     * <p>
     * 返回：向有界出口提交一次调用或记录增量，不改变水位。
     *
     * @param invocation 已退出调用
     * @param outcome    最终裁决。
     */
    private void observeInvocation(Invocation invocation, Outcome outcome) {
        if (!proxy.metricsEnabled()) return;
        ListenerEntry entry = invocation.records.getFirst().entry;
        if (entry == null || entry.kind() == ListenerKind.PASSTHROUGH) return;
        Map<String, String> labels = Map.of("group", groupId, "consumer_id", entry.metricId());
        if (!outcome.invalid.isEmpty()) {
            proxy.metric("decode_failed_total", outcome.invalid.size(), true, labels);
            return;
        }
        if (outcome.failure != null) {
            Throwable error = unwrap(outcome.failure);
            String name = error instanceof KafkaFailure failure ? switch (failure.code()) {
                case HANDLER_DEFERRED -> "handler_deferred_total";
                case HANDLER_DEAD_LETTER -> "handler_dead_letter_total";
                default -> "handler_failed_total";
            } : "handler_failed_total";
            proxy.metric(name, 1, true, labels);
            return;
        }
        if (outcome.safe == null) return;
        long safe = outcome.safe.stream().filter(Boolean::booleanValue).count();
        proxy.metric("consumed_total", safe, true, labels);
        if (entry.ackMode() == AckMode.MANUAL) {
            proxy.metric("manual_acked_total", safe, true, labels);
            proxy.metric("manual_ack_missing_total", invocation.records.size() - safe, true, labels);
            proxy.metric("manual_ack_after_gap_total", outcome.ackedAfterGap, true, labels);
        }
    }

    /**
     * 业务作用：标记已完成记录并推进可提交候选。
     * <p>
     * 返回：尚未等同于 broker commit。
     *
     * @param partition 责任账本
     * @param record    已登记记录。
     */
    private void safe(Partition partition, Pending record) {
        record.state = Status.SAFE;
        partition.offsets.markSafe(record.record.offset());
        if (partition.keyCandidatesOnly) {
            String key = keyDomain(record);
            var queue = partition.keyQueues.get(key);
            if (queue != null) {
                partition.keyHeads.remove(queue.getFirst().record.offset());
                queue.remove(record);
                if (queue.isEmpty()) partition.keyQueues.remove(key);
                else partition.keyHeads.put(queue.getFirst().record.offset(), queue.getFirst());
            }
        }
        // DLT 已明确完成后不再保留其独立信封；原输入额度仍等 broker commit 后才归还。
        if (record.dltLease != null) record.dltLease.close();
        if (record.retryLease != null) record.retryLease.close();
        safeSinceCommit++;
        lastSuccess = System.nanoTime();
    }

    /**
     * 业务作用：同步提交最新安全前缀，成功后才释放责任。
     * <p>
     * 返回：失败仍保留账本供空 poll 重试。
     *
     * @param force 尾批排空、关闭或撤销时立即尝试。
     */
    private void commitSafe(boolean force) {
        if (consumer == null) return;
        long now = System.nanoTime();
        if (!force && safeSinceCommit < config.getCommitBatchRecords() && now - lastCommit < TimeUnit.MILLISECONDS.toNanos(config.getCommitIntervalMs()))
            return;
        Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
        for (Partition partition : partitions.values())
            if (partition.active)
                partition.offsets.committable().ifPresent(offset -> offsets.put(partition.tp, new OffsetAndMetadata(offset)));
        if (offsets.isEmpty()) return;
        lastCommit = now;
        commitAttempted++;
        proxy.metric("commit_attempted_total", 1, true, Map.of("group", groupId));
        try {
            consumer.commitSync(offsets, Duration.ofMillis(Math.min(1000, config.getControlTimeoutMs())));
            commitSucceeded++;
            proxy.metric("commit_succeeded_total", 1, true, Map.of("group", groupId));
            commitBlocked = false;
            for (var entry : offsets.entrySet()) {
                Partition partition = partitions.get(entry.getKey());
                if (partition == null || !partition.active) continue;
                long next = entry.getValue().offset();
                // broker 已确认后才能推进恢复边界并释放责任；超时不能走此路径。
                recoveryOffsets.put(partition.tp, next);
                partition.offsets.committed(next);
                Iterator<Pending> records = partition.records.values().iterator();
                while (records.hasNext()) {
                    Pending record = records.next();
                    if (record.record.offset() >= next) break;
                    release(record);
                    records.remove();
                }
                if (partition.records.isEmpty()) {
                    partition.mayHaveRetry = false;
                    partition.mayHaveDlt = false;
                    partition.keyCandidatesOnly = true;
                    partition.keyQueues.clear();
                    partition.keyHeads.clear();
                }
            }
            safeSinceCommit = 0;
        } catch (RuntimeException failure) {
            commitFailed++;
            proxy.metric("commit_failed_total", 1, true, Map.of("group", groupId));
            commitBlocked = true;
            reason = "CommitPending:" + failure.getClass().getSimpleName();
        }
    }

    /**
     * 业务作用：叠加用户、责任及资源压力，维持 poll 协调而不继续拉取。
     * <p>
     * 参数说明：无。
     * 返回：只有全部阻断原因解除才 resume。
     */
    private void updatePauses() {
        if (consumer == null) return;
        Set<TopicPartition> pause = new HashSet<>();
        Set<TopicPartition> resume = new HashSet<>();
        Set<TopicPartition> current = consumer.paused();
        boolean pressure = budget.full();
        for (TopicPartition tp : consumer.assignment()) {
            Partition partition = partitions.get(tp);
            // 已有慢调用不阻挡其它 key 的后续取入；只在真实责任额度或保护门禁耗尽时暂停。
            boolean blocked = stopRequested || commitBlocked || pressure || userPaused.contains(tp) || partition != null &&
                    (partition.halted || retryPauses(partition) || partition.records.size() >= config.getMaxProgressRecordsPerPartition()
                            || partition.admissionKey != null && !budget.canRetain(partition.tp, partition.admissionKey, partition.admissionBytes));
            // 每次依据原生当前状态复验，涵盖再均衡清除暂停；连续业务完成只发布真正发生变化的路由门禁。
            if (blocked && !current.contains(tp)) pause.add(tp);
            else if (!blocked && current.contains(tp)) resume.add(tp);
        }
        if (!pause.isEmpty()) consumer.pause(pause);
        if (!resume.isEmpty()) consumer.resume(resume);
    }

    /**
     * 业务作用：只让分区级顺序屏障的退避暂停原生拉取，按键退避仍允许其它 key 在额度内取入。
     *
     * @param partition 当前责任账本。
     * @return 最早未完成记录在独占分区的退避期内时为 true。
     */
    private boolean retryPauses(Partition partition) {
        if (!partition.mayHaveRetry) return false;
        for (Pending record : partition.records.values()) {
            if (record.state == Status.SAFE) continue;
            return record.retryAt > System.nanoTime() && (record.record.key() == null || record.entry == null
                    || record.entry.executionMode() == ExecutionMode.PARTITION_ORDERED);
        }
        return false;
    }

    /**
     * 业务作用：发布不包含 mutable consumer 的健康快照。
     * <p>
     * 参数说明：无。
     * 返回：查询线程无需触碰原生 consumer。
     */
    private void snapshot() {
        int businessHandlers = 0;
        for (Invocation invocation : invocations) if (!invocation.deadLetter) businessHandlers++;
        observedBusinessHandlers = businessHandlers;
        if (proxy.metricsEnabled() && System.nanoTime() - keyMetricsAt >= TimeUnit.SECONDS.toNanos(1)) {
            // 附加聚合与出口采用相同秒级频率；旧代次持有的原文仍计入，但账本和调用不能重复统计。
            Set<Pending> keyed = Collections.newSetFromMap(new IdentityHashMap<>());
            partitions.values().forEach(p -> p.records.values().stream().filter(KafkaPollTask::isKeyed).forEach(keyed::add));
            invocations.forEach(i -> i.records.stream().filter(KafkaPollTask::isKeyed).filter(p -> !p.released).forEach(keyed::add));
            keyMetrics = new long[]{invocations.stream().filter(i -> !i.deadLetter && isKeyed(i.records.getFirst())).count(),
                    keyed.size(), keyed.stream().mapToLong(p -> p.reservedBytes).sum()};
            keyMetricsAt = System.nanoTime();
        }
        Map<TopicPartition, Set<String>> pauses = new LinkedHashMap<>();
        boolean dltDegraded = false, halted = false, retrying = false, inputPressure = false, dltPressure = false;
        boolean sharedPressure = budget.full();
        for (Partition partition : partitions.values()) {
            Set<String> reasons = new LinkedHashSet<>();
            if (userPaused.contains(partition.tp)) reasons.add("USER");
            if (partition.halted) {
                reasons.add("HALTED");
                halted = true;
            }
            if (commitBlocked) reasons.add("COMMIT_BLOCKED");
            if (partition.mayHaveRetry || partition.mayHaveDlt) for (Pending record : partition.records.values()) {
                long now = System.nanoTime();
                if (partition.mayHaveRetry && record.retryAt > now) retrying = true;
                if (partition.mayHaveDlt) {
                    if (record.state != Status.SAFE && record.dltFailures >= config.getDltDegradedAfterAttempts())
                        dltDegraded = true;
                    if (record.dltReason != null && record.state == Status.WAITING && record.retryAt <= now) {
                        reasons.add("DLT_BACKPRESSURE");
                        dltPressure = true;
                    }
                }
            }
            if (retryPauses(partition)) reasons.add("RETRY_BACKOFF");
            if (sharedPressure || partition.records.size() >= config.getMaxProgressRecordsPerPartition()
                    || partition.admissionKey != null && !budget.canRetain(partition.tp, partition.admissionKey, partition.admissionBytes)) {
                reasons.add("PROGRESS_BACKPRESSURE");
                inputPressure = true;
            }
            if (stopRequested) reasons.add("CLOSING");
            if (!reasons.isEmpty()) pauses.put(partition.tp, Set.copyOf(reasons));
        }
        GroupHealth.State state;
        if (!running) state = GroupHealth.State.CLOSED;
        else if (stopRequested) state = GroupHealth.State.CLOSING;
        else if (consumer == null || sessionState == GroupHealth.State.RECOVERING) state = sessionState;
        else if (dltDegraded || halted) state = GroupHealth.State.DEGRADED;
        else if (commitBlocked) state = GroupHealth.State.COMMIT_BLOCKED;
        else if (inputPressure) state = GroupHealth.State.PROGRESS_BACKPRESSURE;
        else if (dltPressure) state = GroupHealth.State.DLT_BACKPRESSURE;
        else if (retrying) state = GroupHealth.State.RETRYING;
        else state = joined ? GroupHealth.State.RUNNING : sessionState;
        health = new GroupHealth(proxy.sourceId(), groupId, state, nextEpoch, joined, Set.copyOf(partitions.keySet()),
                Map.copyOf(pauses), invocations.size(), retainedBytes, lastPoll, dltDegraded && reason == null ? "DltDeliveryRetryThreshold" : reason, System.nanoTime(), lastSuccess,
                ownerSessionStarted, Math.max(0, ownerSessionStarted - 1), commitAttempted, commitSucceeded, commitFailed);
        proxy.sampleGroupMetrics(groupId);
    }

    /**
     * 业务作用：按实际路由执行模式识别按键责任，null key 仍属于该路线的串行后备域。
     *
     * @param pending 已路由输入。
     * @return 是否使用按键执行模式。
     */
    private static boolean isKeyed(Pending pending) {
        return pending.entry != null && pending.entry.executionMode() == ExecutionMode.KEY_ORDERED;
    }

    /**
     * 业务作用：释放已明确交回或已提交的记录额度。
     * <p>
     * 返回：重复释放无副作用。
     *
     * @param record 责任。
     */
    private void release(Pending record) {
        if (!record.released) {
            record.released = true;
            retainedBytes -= record.reservedBytes;
            retainedRecords--;
            record.lease.close();
            if (record.dltLease != null) record.dltLease.close();
            if (record.retryLease != null) record.retryLease.close();
        }
    }

    /**
     * 业务作用：为停止期间因失权或退出而释放的输入保留未确认计数。
     * <p>
     * 返回：一次性计数并释放本地额度，不保证来源仍可恢复。
     *
     * @param record 没有提交确认的责任。
     */
    private void releaseUnconfirmed(Pending record) {
        // 失权后迟到调用也走此路径；broker 确认裁剪直接 release，不能混入未确认交回。
        if (stopRequested && !record.released && releasedUnverifiedRecords < Long.MAX_VALUE)
            releasedUnverifiedRecords++;
        release(record);
    }

    /**
     * 业务作用：关闭分区时交回未提交责任。
     * <p>
     * 返回：不将待重投记录标记成功。
     *
     * @param partition 已退出分区。
     */
    private void releasePartition(Partition partition) {
        partition.records.values().forEach(this::releaseUnconfirmed);
    }

    /**
     * 业务作用：读取 singleton 线路声明，防止歧义值改变路由。
     *
     * @param record 消息
     * @param name   精确名称。
     * @return 无头时为 null；严格模式拒绝重复，兼容模式选择末项；被选中的值必须非 null 且为合法 UTF-8。
     */
    private String header(ConsumerRecord<byte[], byte[]> record, String name) {
        Header selected = null;
        for (Header header : record.headers().headers(name)) {
            if (selected != null && config.isStrictHeaders()) throw new IllegalArgumentException("重复保留头");
            selected = header;
        }
        if (selected == null) return null;
        if (selected.value() == null) throw new IllegalArgumentException("保留头不能为 null");
        return utf8(selected.value());
    }

    /**
     * 业务作用：避免有损字符替换改变路由或业务键。
     *
     * @param bytes 线路内容。
     * @return 合法 UTF-8，非法时失败。
     */
    private static String utf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException("非法 UTF-8");
        }
    }

    /**
     * 业务作用：还原异步失败的实际业务原因。
     *
     * @param error 可空异步异常。
     * @return 逐层去除有 cause 的 ExecutionException 与 CompletionException；保留其它异常自身，null 输入仍为 null。
     */
    private static Throwable unwrap(Throwable error) {
        while ((error instanceof ExecutionException || error instanceof CompletionException) && error.getCause() != null)
            error = error.getCause();
        return error;
    }

    /**
     * 业务作用：把控制操作交给 owner 并预留命令容量。
     *
     * @param operation 原生操作。
     * @return 可观察结果。
     */
    private <T> Command<T> command(Function<KafkaConsumer<byte[], byte[]>, T> operation) {
        return command(operation, false);
    }

    /**
     * 业务作用：让恢复命令可进入无 consumer 的保护邮箱，其余命令仍绑定当前分配。
     *
     * @param operation 操作
     * @param recovery  表示仅用于恢复。
     * @return 已预留容量的命令，失权时未执行。
     */
    private <T> Command<T> command(Function<KafkaConsumer<byte[], byte[]>, T> operation, boolean recovery) {
        return command(operation, recovery, health.epoch(), System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getControlTimeoutMs()));
    }

    /**
     * 业务作用：携带冻结代次和剩余预算进入 owner 邮箱，等待期间不能获得新 assignment 权威。
     *
     * @param operation 操作
     * @param recovery  恢复例外
     * @param epoch     调用方代次
     * @param deadline  总期限。
     * @return 有界命令，未取得准入时确定未执行。
     */
    private <T> Command<T> command(Function<KafkaConsumer<byte[], byte[]>, T> operation, boolean recovery, long epoch, long deadline) {
        Command<T> command = new Command<>(nativeConsumer -> {
            // 排队期间若分配已改变，拒绝把旧操作意图应用于新权威。
            if (!recovery && (nextEpoch != epoch || nativeConsumer == null || sessionState == GroupHealth.State.RECOVERING))
                throw new KafkaFailure(KafkaFailure.Code.CONTROL_OWNER_CHANGED, "原生会话不可用或排队期间 assignment 已改变");
            // 先发布动作后的归属和代次，再交付命令回执；紧接的分区控制不能读到 seek 的中间撤销态。
            try {
                return operation.apply(nativeConsumer);
            } finally {
                snapshot();
            }
        }, deadline);
        if (!running || stopRequested || !commands.offer(command))
            throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "owner 不可用或命令队列已满");
        signalOwner();
        return command;
    }

    /**
     * 业务作用：在同一 owner 内逐目标记录执行证据，已经完成的目标不被后续超时覆盖。
     * <p>
     * 返回：等待结束；真实在途命令可继续收口，报告只捕获返回时证据。
     *
     * @param actions  本槽目标
     * @param epoch    冻结代次
     * @param deadline 共同期限
     * @param results  线程安全的调用证据。
     */
    void controlPartitions(Map<TopicPartition, PartitionControl> actions, long epoch, long deadline, KafkaControlProgress results) {
        try {
            awaitCommand(command(c -> {
                for (var entry : actions.entrySet()) {
                    if (System.nanoTime() >= deadline) break;
                    TopicPartition tp = entry.getKey();
                    try {
                        requireAssigned(List.of(tp));
                        PartitionControl action = entry.getValue();
                        if (action.kind() != PartitionControl.Kind.PAUSE && action.kind() != PartitionControl.Kind.RESUME)
                            requireQuiescent(List.of(tp));
                        // 先登记可能产生外部副作用，再调用 native；等待超时不能把在途目标当作未执行。
                        if (!results.begin(tp, deadline)) break;
                        switch (action.kind()) {
                            case PAUSE -> {
                                userPaused.add(tp);
                                updatePauses();
                            }
                            case RESUME -> {
                                userPaused.remove(tp);
                                updatePauses();
                            }
                            case SEEK -> c.seek(tp, action.value());
                            case BEGINNING -> c.seekToBeginning(List.of(tp));
                            case END -> c.seekToEnd(List.of(tp));
                            case TIMESTAMP -> applyTimestamps(Map.of(tp, action.value()));
                            case COMMITTED -> {
                                OffsetAndMetadata offset = c.committed(Set.of(tp)).get(tp);
                                if (offset == null)
                                    throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "目标没有提交历史");
                                c.seek(tp, offset.offset());
                            }
                        }
                        if (action.kind() != PartitionControl.Kind.PAUSE && action.kind() != PartitionControl.Kind.RESUME)
                            relocate(List.of(tp));
                        // 本地权威先发布再交付成功证据，后续查询不观察定位过程中的中间态。
                        snapshot();
                        results.put(tp, ControlReport.Outcome.APPLIED);
                    } catch (RuntimeException failure) {
                        results.put(tp, controlOutcome(failure));
                        if (results.get(tp) == ControlReport.Outcome.UNKNOWN) break;
                    }
                }
                return null;
            }, false, epoch, deadline));
        } catch (RuntimeException failure) {
            ControlReport.Outcome outcome = controlOutcome(failure);
            // 只有执行前的归属拒绝作用于本槽全部目标；在途超时已有逐目标标记，不覆盖已确认结果。
            if (outcome == ControlReport.Outcome.OWNER_CHANGED)
                actions.keySet().forEach(tp -> results.reject(tp, outcome));
        }
    }

    /**
     * 业务作用：把控制失败映射为不含原生诊断或敏感配置的稳定证据。
     *
     * @param failure 本地操作失败。
     * @return 只有有明确执行前证据时才判定未执行。
     */
    private static ControlReport.Outcome controlOutcome(RuntimeException failure) {
        if (failure instanceof KafkaFailure known) {
            if (known.code() == KafkaFailure.Code.CONTROL_OWNER_CHANGED) return ControlReport.Outcome.OWNER_CHANGED;
            if (known.code() == KafkaFailure.Code.CONTROL_NOT_APPLIED) return ControlReport.Outcome.NOT_APPLIED;
        }
        return ControlReport.Outcome.UNKNOWN;
    }

    /**
     * 业务作用：在同一时限内等待命令，区分尚未执行与结果未知。
     *
     * @param operation owner 操作。
     * @return 实际结果。
     */
    private <T> T control(Function<KafkaConsumer<byte[], byte[]>, T> operation) {
        return awaitCommand(command(operation));
    }

    /**
     * 业务作用：等待同一命令的终态并在线性化门禁内撤销尚未开始的操作。
     *
     * @param command 已入队命令。
     * @return 结果、未执行或结果未知。
     */
    private <T> T awaitCommand(Command<T> command) {
        try {
            return command.result.get(Math.max(0, command.deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw command.timeout();
        } catch (TimeoutException error) {
            throw command.timeout();
        } catch (ExecutionException error) {
            if (error.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException(error.getCause());
        }
    }

    /**
     * 业务作用：接受受保护任务的显式重建请求，不让正常消费或未结束调用被重复启动。
     * <p>
     * 参数说明：无。
     * 返回：旧会话已进入重建路径；Ready 需重新等待。
     */
    void restart() {
        awaitCommand(command(c -> {
            // 控制线程必须复验当前状态与真实调用，不能用调用方的旧快照授权重建。
            snapshot();
            if (!health.permitsRestart() || !invocations.isEmpty())
                throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "当前状态不允许重建或仍有实际调用");
            restartRequested = true;
            joined = false;
            // 请求受理时立即撤销 Ready，防止调用方继续使用上一代分配。
            sessionState = GroupHealth.State.RECOVERING;
            snapshot();
            return null;
        }, true));
    }

    /**
     * 业务作用：公平处理一轮有界控制命令。
     * <p>
     * 参数说明：无。
     * 返回：超期未开始的命令绝不执行。
     */
    private void drainCommands() {
        for (int count = 0; count < config.getControlQueueCapacity(); count++) {
            Command<?> command = commands.poll();
            if (command == null) break;
            command.apply(consumer);
        }
    }

    /**
     * 业务作用：定位前拒绝仍承担业务责任的分区。
     * <p>
     * 返回：无在途责任时正常，否则未执行。
     *
     * @param targets 操作范围。
     */
    private void requireQuiescent(Collection<TopicPartition> targets) {
        for (TopicPartition tp : targets) {
            Partition partition = partitions.get(tp);
            if (partition == null || !partition.records.isEmpty())
                throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "分区未分配或仍有消费责任");
        }
    }

    /**
     * 业务作用：生成新的定位代次，隔离旧完成。
     * <p>
     * 返回：空的新账本。
     *
     * @param targets 已收口分区。
     */
    private void relocate(Collection<TopicPartition> targets) {
        // 明确 seek 是新的业务恢复边界；后续会话不能回退到更早的初始化声明。
        for (TopicPartition tp : targets) recoveryOffsets.put(tp, consumer.position(tp));
        revoke(targets);
        acquire(targets);
        joined = true;
    }

    /**
     * 业务作用：启动装配期间增量添加单个订阅，运行后交给owner一次应用。
     * <p>
     * 返回：已接受声明；停止后的owner不再接受修改。
     *
     * @param topic 目标。
     */
    void subscribe(String topic) {
        if (topic == null || topic.isBlank() || mode != ConsumeMode.SUBSCRIBE)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "订阅目标或模式非法");
        changeSubscriptions(Set.of(topic), true);
    }

    /**
     * 业务作用：在同一owner命令中读取并修改完整集合，阻止并发读改写覆盖彼此。
     * <p>
     * 返回：原生已应用或明确控制错误；在途输入未排干时不变更。
     *
     * @param topics 上层已冻结校验的主题
     * @param add    区分添加与删除。
     */
    synchronized void changeSubscriptions(Set<String> topics, boolean add) {
        if (mode != ConsumeMode.SUBSCRIBE)
            throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "assign 不支持订阅变更");
        if (runner == null && !stopRequested) {
            if (add) subscribedTopics.addAll(topics);
            else subscribedTopics.removeAll(topics);
            return;
        }
        control(c -> {
            // 冻结路线不会随订阅变化；先保留所有实际输入责任，只有全分区静止时才改变拉取范围。
            requireQuiescent(partitions.keySet());
            Set<String> desired = new TreeSet<>(subscribedTopics);
            if (add) desired.addAll(topics);
            else desired.removeAll(topics);
            applySubscription(desired);
            return null;
        });
    }

    /**
     * 业务作用：替换完整订阅集合以对应 Rust 入口。
     * <p>
     * 返回：owner 已应用新声明。
     *
     * @param topics 新集合。
     */
    void replaceSubscriptions(Collection<String> topics) {
        Set<String> copy = Set.copyOf(topics);
        control(c -> {
            requireQuiescent(partitions.keySet());
            if (mode != ConsumeMode.SUBSCRIBE || copy.stream().anyMatch(String::isBlank))
                throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "订阅模式或 topic 非法");
            applySubscription(copy);
            return null;
        });
    }

    /**
     * 业务作用：原生接受新订阅后更新恢复声明并撤销旧 Ready。
     * <p>
     * 返回：相同集合无状态变化；空集合保留 owner 但取消分配，新集合等待后续 join。
     *
     * @param desired 完整目标集合。
     */
    private void applySubscription(Set<String> desired) {
        // 同集合不会产生新的分配回调，不能先撤销 Ready 再等待不存在的 join。
        if (subscribedTopics.equals(desired)) return;
        if (desired.isEmpty()) {
            consumer.unsubscribe();
            revoke(List.copyOf(partitions.keySet()));
        } else consumer.subscribe(List.copyOf(desired), rebalanceListener());
        // 原生接受意图后才发布恢复声明与失权事实，控制回执不得早于这一状态发布。
        subscribedTopics.clear();
        subscribedTopics.addAll(desired);
        joined = false;
        sessionState = GroupHealth.State.JOINING;
    }

    /**
     * 业务作用：移动一个分区的读取位置。
     * <p>
     * 返回：owner 已执行或明确拒绝。
     *
     * @param tp     分区
     * @param offset 目标。
     */
    void seek(TopicPartition tp, long offset) {
        seek(Map.of(tp, offset));
    }

    /**
     * 业务作用：按分区 map 重定位。
     * <p>
     * 返回：完成的新定位代次。
     *
     * @param offsets 目标位置。
     */
    void seek(Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, Long> copy = Map.copyOf(offsets);
        control(c -> {
            requireQuiescent(copy.keySet());
            if (copy.values().stream().anyMatch(o -> o < 0)) throw new IllegalArgumentException("offset 不能为负");
            copy.forEach(c::seek);
            relocate(copy.keySet());
            return null;
        });
    }

    /**
     * 业务作用：定位最早保留边界。
     * <p>
     * 返回：owner 已接受定位。
     *
     * @param targets 分区集合。
     */
    void seekToBeginning(Collection<TopicPartition> targets) {
        Set<TopicPartition> copy = Set.copyOf(targets);
        control(c -> {
            requireQuiescent(copy);
            c.seekToBeginning(copy);
            relocate(copy);
            return null;
        });
    }

    /**
     * 业务作用：定位当前可读末尾。
     * <p>
     * 返回：owner 已接受定位。
     *
     * @param targets 分区集合。
     */
    void seekToEnd(Collection<TopicPartition> targets) {
        Set<TopicPartition> copy = Set.copyOf(targets);
        control(c -> {
            requireQuiescent(copy);
            c.seekToEnd(copy);
            relocate(copy);
            return null;
        });
    }

    /**
     * 业务作用：按时间定位一个分区。
     * <p>
     * 返回：完成定位。
     *
     * @param tp   分区
     * @param time epoch 毫秒。
     */
    void seekToTimestamp(TopicPartition tp, long time) {
        seekToTimestamp(Map.of(tp, time));
    }

    /**
     * 业务作用：按时间 map 定位。
     * <p>
     * 返回：无匹配时选择可读末尾。
     *
     * @param times 每分区时间。
     */
    void seekToTimestamp(Map<TopicPartition, Long> times) {
        Map<TopicPartition, Long> copy = Map.copyOf(times);
        control(c -> {
            requireQuiescent(copy.keySet());
            applyTimestamps(copy);
            relocate(copy.keySet());
            return null;
        });
    }

    /**
     * 业务作用：应用 broker 返回的时间位置。
     * <p>
     * 返回：全部目标已定位。
     *
     * @param times 定位条件。
     */
    private void applyTimestamps(Map<TopicPartition, Long> times) {
        consumer.offsetsForTimes(times).forEach((tp, offset) -> {
            if (offset == null) consumer.seekToEnd(List.of(tp));
            else consumer.seek(tp, offset.offset());
        });
    }

    /**
     * 业务作用：定位 broker 已提交位置。
     * <p>
     * 返回：无历史提交明确拒绝。
     *
     * @param targets 分区集合。
     */
    void seekToCommitted(Collection<TopicPartition> targets) {
        Set<TopicPartition> copy = Set.copyOf(targets);
        control(c -> {
            requireQuiescent(copy);
            Map<TopicPartition, OffsetAndMetadata> found = c.committed(copy);
            if (copy.stream().anyMatch(tp -> found.get(tp) == null))
                throw new KafkaFailure(KafkaFailure.Code.CONTROL_NOT_APPLIED, "存在无提交历史的分区");
            found.forEach((tp, offset) -> c.seek(tp, offset.offset()));
            relocate(copy);
            return null;
        });
    }

    /**
     * 业务作用：增加用户暂停原因。
     * <p>
     * 返回：不改变其他保护原因。
     *
     * @param targets 分区。
     */
    void pause(Collection<TopicPartition> targets) {
        Set<TopicPartition> copy = Set.copyOf(targets);
        control(c -> {
            requireAssigned(copy);
            userPaused.addAll(copy);
            updatePauses();
            return null;
        });
    }

    /**
     * 业务作用：只解除用户暂停原因。
     * <p>
     * 返回：其他保护仍然生效。
     *
     * @param targets 分区。
     */
    void resume(Collection<TopicPartition> targets) {
        Set<TopicPartition> copy = Set.copyOf(targets);
        control(c -> {
            requireAssigned(copy);
            userPaused.removeAll(copy);
            updatePauses();
            return null;
        });
    }

    /**
     * 业务作用：在 native 副作用前复验组级路由选择的目标归属。
     * <p>
     * 返回：全部仍属于本槽才通过，转移时确定未执行。
     *
     * @param targets 当前命令分区。
     */
    private void requireAssigned(Collection<TopicPartition> targets) {
        if (!consumer.assignment().containsAll(targets) || !partitions.keySet().containsAll(targets))
            throw new KafkaFailure(KafkaFailure.Code.CONTROL_OWNER_CHANGED, "分区归属已经改变");
    }

    /**
     * 业务作用：暂停当前全部分区。
     * <p>
     * 参数说明：无。
     * 返回：已增加用户暂停原因。
     */
    void pauseAll() {
        control(c -> {
            userPaused.addAll(c.assignment());
            updatePauses();
            return null;
        });
    }

    /**
     * 业务作用：解除当前全部用户暂停。
     * <p>
     * 参数说明：无。
     * 返回：内部保护不被解除。
     */
    void resumeAll() {
        control(c -> {
            userPaused.clear();
            updatePauses();
            return null;
        });
    }

    /**
     * 业务作用：读取当前 consumer position。
     *
     * @param tp 已分配分区。
     * @return 异步查询结果，不代表业务提交。
     */
    CompletableFuture<Long> position(TopicPartition tp) {
        return command(c -> c.position(tp)).result.copy();
    }

    /**
     * 业务作用：查询 owner 当前 assignment。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变快照。
     */
    CompletableFuture<Set<TopicPartition>> assignmentSnapshot() {
        return command(c -> Set.copyOf(c.assignment())).result.copy();
    }

    /**
     * 业务作用：查询 broker 已提交位置。
     *
     * @param targets 分区。
     * @return 无提交历史保留 null。
     */
    CompletableFuture<Map<TopicPartition, Long>> committedOffsets(Collection<TopicPartition> targets) {
        Set<TopicPartition> copy = Set.copyOf(targets);
        return command(c -> {
            Map<TopicPartition, Long> result = new LinkedHashMap<>();
            c.committed(copy).forEach((tp, offset) -> result.put(tp, offset == null ? null : offset.offset()));
            return result;
        }).result.copy();
    }

    private enum Status {WAITING, RUNNING, SAFE, HALTED}

    private static final class Partition {
        final TopicPartition tp;
        final long epoch;
        volatile boolean active = true;
        boolean halted;
        /**
         * 仅用于跳过确定为空的诊断扫描；一旦出现对应责任，保持扫描直到该账本完整提交排空。
         */
        boolean mayHaveRetry, mayHaveDlt;
        /**
         * 上次因共享容量交回的原文大小；余量恢复前不反复拉取同一记录。
         */
        long admissionBytes;
        /**
         * 固定长度计费身份也标识零字节输入的容量等待，不保留尚未计费的原始 key。
         */
        String admissionKey;
        /**
         * 按 owner 收到的解码结果累计，任意有效解码会打断系统性失败序列。
         */
        long decodeFailures;
        final PartitionOffsetState offsets = new PartitionOffsetState();
        final NavigableMap<Long, Pending> records = new TreeMap<>();
        /**
         * 纯按键账本只派发各域队首；其余记录仍保留提交责任，null key 队首按 offset 参与屏障裁决。
         */
        boolean keyCandidatesOnly = true;
        final Map<String, ArrayDeque<Pending>> keyQueues = new HashMap<>();
        final NavigableMap<Long, Pending> keyHeads = new TreeMap<>();

        /**
         * 业务作用：建立分区独立权威。
         * <p>
         * 返回：空责任账本。
         *
         * @param tp    身份
         * @param epoch 代次。
         */
        Partition(TopicPartition tp, long epoch) {
            this.tp = tp;
            this.epoch = epoch;
        }
    }

    private static final class Pending {
        final ConsumerRecord<byte[], byte[]> record;
        final String inputKey;
        /**
         * 已计费输入的顺序身份，由 owner 首次调度时建立并保留到原责任释放。
         */
        String keyDomain;
        final long bytes;
        /**
         * 原文与框架副本预留到最后真实责任退出，DLT 使用独立信封配额。
         */
        final long reservedBytes;
        ListenerEntry entry;
        RecordPolicy policy;
        Status state = Status.WAITING;
        int attempts;
        int deliveries;
        /**
         * 同一信封的失败累计，退避重投不产生新输入责任；确认后不再参与降级判定。
         */
        long dltFailures;
        long retryAt;
        String failure;
        String dltReason;
        long dltBytes;
        boolean released;
        KafkaGroupBudget.Lease lease;
        KafkaGroupBudget.Lease dltLease;
        /**
         * 次数元数据在业务明确完成或失权后独立释放，不代替输入提交责任。
         */
        KafkaGroupBudget.Lease retryLease;

        /**
         * 业务作用：为原始记录保留待处理责任。
         * <p>
         * 返回：包含原始字节预算的责任。
         *
         * @param record 输入。
         */
        Pending(ConsumerRecord<byte[], byte[]> record) {
            this.record = record;
            inputKey = KafkaGroupBudget.inputKey(record.key());
            // 准入前只读取 native 已持有的原文长度，不复制尚未取得额度的 header 值。
            long retained = (record.key() == null ? 0L : record.key().length) + (record.value() == null ? 0L : record.value().length);
            for (var header : record.headers())
                retained += (long) header.key().getBytes(StandardCharsets.UTF_8).length + (header.value() == null ? 0L : header.value().length);
            bytes = retained;
            reservedBytes = KafkaGroupBudget.inputReservationBytes(retained);
        }
    }

    private final class Invocation {
        final Partition partition;
        final List<Pending> records;
        final boolean deadLetter;
        final KafkaGroupBudget.Lease call;
        final long deadline;
        final CompletableFuture<Outcome> result = new CompletableFuture<>();
        volatile boolean exited;

        /**
         * 业务作用：绑定实际调用与分区权威。
         * <p>
         * 返回：未完成调用。
         *
         * @param partition  权威
         * @param records    责任
         * @param deadLetter 交付调用
         * @param call       实际执行额度。
         */
        Invocation(Partition partition, List<Pending> records, boolean deadLetter, KafkaGroupBudget.Lease call) {
            this.partition = partition;
            this.records = List.copyOf(records);
            this.deadLetter = deadLetter;
            this.call = call;
            deadline = config.getHandlerTimeoutMs() == 0 ? Long.MAX_VALUE : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.getHandlerTimeoutMs());
        }

        /**
         * 业务作用：复验回调期确认权威和时限。
         * <p>
         * 参数说明：无。
         *
         * @return 停止或超时后不能继续临时确认。
         */
        boolean valid() {
            return !stopRequested && System.nanoTime() < deadline;
        }
    }

    private record Outcome(List<Boolean> safe, Throwable failure, Map<Long, String> invalid, int ackedAfterGap) {
        /**
         * 业务作用：为非 Manual 前缀场景构造统一结果。
         * <p>
         * 返回：缺口后确认数为零的业务裁决。
         *
         * @param safe    安全前缀
         * @param failure 调用失败
         * @param invalid 解码拒绝坐标。
         */
        Outcome(List<Boolean> safe, Throwable failure, Map<Long, String> invalid) {
            this(safe, failure, invalid, 0);
        }
    }

    private static final class Command<T> {
        final Function<KafkaConsumer<byte[], byte[]>, T> operation;
        final CompletableFuture<T> result = new CompletableFuture<>();
        final AtomicInteger state = new AtomicInteger();
        final long deadline;

        /**
         * 业务作用：保留命令的执行前取消权威。
         * <p>
         * 返回：排队状态命令。
         *
         * @param operation 操作
         * @param deadline  时限。
         */
        Command(Function<KafkaConsumer<byte[], byte[]>, T> operation, long deadline) {
            this.operation = operation;
            this.deadline = deadline;
        }

        /**
         * 业务作用：在线性化门禁内判定能否执行。
         * <p>
         * 返回：终态通过 future 返回。
         *
         * @param consumer owner 原生对象。
         */
        void apply(KafkaConsumer<byte[], byte[]> consumer) {
            if (System.nanoTime() >= deadline) {
                result.completeExceptionally(timeout());
                return;
            }
            if (!state.compareAndSet(0, 1)) return;
            try {
                result.complete(operation.apply(consumer));
            } catch (Throwable failure) {
                result.completeExceptionally(failure);
            }
        }

        /**
         * 业务作用：区分命令尚未开始与结果未知。
         * <p>
         * 参数说明：无。
         *
         * @return 稳定分类，并封住尚未开始的命令。
         */
        KafkaFailure timeout() {
            return new KafkaFailure(state.compareAndSet(0, 2) || state.get() == 2 ? KafkaFailure.Code.CONTROL_NOT_APPLIED : KafkaFailure.Code.CONTROL_OUTCOME_UNKNOWN, "控制命令超过期限");
        }
    }
}

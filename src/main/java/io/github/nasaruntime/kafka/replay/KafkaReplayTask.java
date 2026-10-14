package io.github.nasaruntime.kafka.replay;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.checkpoint.CheckpointLease;
import io.github.nasaruntime.kafka.checkpoint.CheckpointStore;
import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.support.KafkaBrokerErrors;
import io.github.nasaruntime.kafka.support.KafkaNative;

import io.github.nasaruntime.kafka.checkpoint.CheckpointException;

import io.github.nasaruntime.kafka.checkpoint.ReplayCheckpoint;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * 单次实际任务责任；控制线程不持有 consumer，分区 worker 不得直接替换完整进度快照。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaReplayTask {

    private static final Duration OPERATION_BUDGET = Duration.ofSeconds(10);
    private final KafkaReplayManager manager;
    private final KafkaProxy source;
    private final ReplayPlan plan;
    private final CheckpointStore store;
    private final ReplayIdentityProvider identity;
    private final ReplayProcessor processor;
    private final ReplayParticipant participant;
    private final int workerLimit;
    private final long recordLimit;
    private final Set<String> topics;
    private final Object control = new Object(), persistence = new Object();
    private final CountDownLatch completed = new CountDownLatch(1);
    private final AtomicInteger partitionCursor = new AtomicInteger();
    private volatile ReplayCheckpoint published;
    private CheckpointLease lease;
    private boolean checkpointBlocked;
    private boolean initialized, paused, cancelled, done;
    private ReplayException.Code failure;
    private CheckpointException.Code checkpointFailure;
    private int inFlight, activeHandlers, workers;
    private long bytes, processed, nextRecord = System.nanoTime(), nextBytes = nextRecord;

    /**
     * 业务作用：冻结一次执行的真实依赖和额度，不提前接管存储或 broker。
     * <p>
     * 返回：尚未开始的任务实例。
     *
     * @param manager     登记回放任务并协调停止的来源任务管理器。
     * @param source      实际提供读取能力及生命周期责任的来源代理。
     * @param plan        已审阅合同
     * @param store       执行checkpoint独占与持久进度写入的后端。
     * @param identity    按已观测的物理集群和 topic 名查询外部资源代次；物理身份及保留范围由框架 Admin 查询。
     * @param processor   定义该回放任务业务成功条件的同步处理器。
     * @param workerLimit 已预留线程数
     * @param participant 可空的框架多源依赖。
     */
    KafkaReplayTask(KafkaReplayManager manager, KafkaProxy source, ReplayPlan plan, CheckpointStore store,
                    ReplayIdentityProvider identity, ReplayProcessor processor, int workerLimit, ReplayParticipant participant) {
        this.manager = manager;
        this.source = source;
        this.plan = plan;
        this.store = store;
        this.identity = identity;
        this.processor = processor;
        this.workerLimit = workerLimit;
        this.participant = participant;
        this.recordLimit = Math.min(source.config().getMaxRecordBytes(), plan.limits().maxInFlightBytes() / workerLimit);
        if (recordLimit < 1) throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
        this.topics = Set.copyOf(plan.topicGenerations().keySet());
    }

    /**
     * 业务作用：向实例目录提供固定任务身份。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变计划，不代表当前执行权。
     */
    ReplayPlan plan() {
        return plan;
    }

    /**
     * 业务作用：启动一个有界协调线程，存储与元数据准备不阻塞来源装配锁。
     * <p>
     * 参数说明：无。
     * 返回：协调线程已提交，实际失败通过状态发布。
     */
    void start() {
        Thread.ofPlatform().daemon(true).name("nasa-replay-coordinator").start(this::run);
    }

    /**
     * 业务作用：以同一控制锁线性化暂停与新处理准入。
     *
     * @param value 暂停或恢复。
     * @return 即时状态；取消、失败或终态不能重新开放。
     */
    ReplayStatus pause(boolean value) {
        synchronized (control) {
            if (!cancelled && failure == null && !done) paused = value;
            control.notifyAll();
            return snapshot();
        }
    }

    /**
     * 业务作用：停止接纳新的实际处理而不打断已经开始的责任。
     * <p>
     * 参数说明：无。
     *
     * @return 本实例的停止阶段，不表示已排干。
     */
    ReplayStatus cancel() {
        synchronized (control) {
            if (!done) cancelled = true;
            control.notifyAll();
            return snapshot();
        }
    }

    /**
     * 业务作用：读取不会触发存储或网络访问的进度快照。
     * <p>
     * 参数说明：无。
     *
     * @return 最近确认持久进度和实际工作数量。
     */
    ReplayStatus status() {
        synchronized (control) {
            return snapshot();
        }
    }

    /**
     * 业务作用：在控制锁内区分关闭准入、暂停在途与最终退出。
     * <p>
     * 参数说明：无。
     *
     * @return 与本轮责任计数一致的即时状态。
     */
    private ReplayStatus snapshot() {
        ReplayStatus.State state = done ? failure != null ? ReplayStatus.State.FAILED : published != null && published.complete()
                                                                                        ? ReplayStatus.State.COMPLETED : ReplayStatus.State.CANCELLED
                : cancelled || failure != null ? ReplayStatus.State.STOPPING : !initialized ? ReplayStatus.State.STARTING
                                                                               : paused ? activeHandlers == 0 ? ReplayStatus.State.PAUSED : ReplayStatus.State.PAUSING : ReplayStatus.State.RUNNING;
        return new ReplayStatus(plan.taskId(), state, published, failure, checkpointFailure, inFlight, bytes, processed, workers);
    }

    /**
     * 业务作用：让调用方等待实际退出而不取消底层责任。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param timeout 非负等待预算。
     * @return 排干为 true；超时或中断为 false，保留中断标志。
     */
    public boolean awaitDrained(Duration timeout) {
        if (timeout == null || timeout.isNegative())
            throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
        try {
            return completed.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ArithmeticException oversized) {
            throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
        }
    }

    /**
     * 业务作用：在任务目录登记之后取得独占权，排干全部实际 worker 后才释放 lease 与目录。
     * <p>
     * 参数说明：无。
     * 返回：仅真实资源退出后发布终态；关闭失败保留责任并重试。
     */
    private void run() {
        List<Thread> children = new ArrayList<>();
        try {
            // 目标依赖尚未登记完整时不能先接管并发送；来源取消同样撤销尚未开放的执行。
            if (participant != null && !participant.activate(this::stopping)) {
                cancel();
                return;
            }
            ReplayCheckpoint initial = plan.initialCheckpoint();
            ReplayCheckpoint previous = store.load(plan.taskId()).orElse(initial);
            verify(previous);
            if (!stopping()) {
                // 验证计划不授予写权；只有 claim 成功持久化新代次，才允许启动读取和业务处理。
                lease = store.claim(initial);
                published = lease.checkpoint();
                verify(published);
                synchronized (control) {
                    initialized = true;
                }
                if (!published.complete() && !stopping()) for (int index = 0; index < workerLimit; index++) {
                    synchronized (control) {
                        workers++;
                    }
                    try {
                        children.add(Thread.ofPlatform().daemon(true).name("nasa-replay-partition").start(this::worker));
                    } catch (Throwable error) {
                        synchronized (control) {
                            workers--;
                        }
                        throw error;
                    }
                }
            }
        } catch (Throwable error) {
            fail(error);
        } finally {
            boolean interrupted = false;
            for (Thread child : children)
                while (child.isAlive()) {
                    try {
                        child.join();
                    } catch (InterruptedException error) {
                        interrupted = true;
                        fail(new ReplayException(ReplayException.Code.PROCESSING_FAILED));
                    }
                }
            // 任何 worker 尚未真实返回时都不能归还独占权；存储关闭失败同样不能伪装成完成。
            if (lease != null) while (true) {
                try {
                    lease.close();
                    break;
                } catch (Throwable error) {
                    fail(error);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException error) {
                    interrupted = true;
                }
            }
            if (participant != null) participant.finished();
            manager.finished(this, workerLimit);
            synchronized (control) {
                done = true;
                control.notifyAll();
            }
            completed.countDown();
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /**
     * 业务作用：让有限数量的 worker 领取互不重叠的分区，每个分区保持串行。
     * <p>
     * 参数说明：无。
     * 返回：未完成分区或停止状态耗尽后退出，任一失败停止新准入。
     */
    private void worker() {
        try {
            while (!stopping()) {
                int index = partitionCursor.getAndIncrement();
                if (index >= plan.ranges().size()) break;
                ReplayCheckpoint.Position range = published.positions().get(index);
                if (range.nextOffset() < range.endOffset()) consume(index, range);
            }
        } catch (Throwable error) {
            fail(error);
        } finally {
            synchronized (control) {
                workers--;
                control.notifyAll();
            }
        }
    }

    /**
     * 业务作用：用专属、无提交的单分区 consumer 执行冻结范围，不影响线上消费组。
     * <p>
     * 返回：到达已证明边界或停止才退出；未成功记录保持原 checkpoint。
     *
     * @param index 完整进度中的位置
     * @param range 本分区持久恢复坐标。
     */
    private void consume(int index, ReplayCheckpoint.Position range) throws Exception {
        Properties properties = source.config().buildConsumerProperties("nasa-replay-" + plan.fingerprint());
        // 回放合同保留任意原始 key；在线消费的文本 key 配置不能参与本次原文解码。
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("enable.auto.commit", "false");
        properties.put("isolation.level", "read_committed");
        properties.put("allow.auto.create.topics", "false");
        properties.put("auto.offset.reset", "none");
        properties.put("max.poll.records", "1");
        properties.remove("group.instance.id");
        properties.put("fetch.max.bytes", Integer.toString((int) Math.min(Integer.MAX_VALUE, recordLimit)));
        properties.put("max.partition.fetch.bytes", Integer.toString((int) Math.min(Integer.MAX_VALUE, recordLimit)));
        TopicPartition tp = new TopicPartition(range.topic(), range.partition());
        KafkaConsumer<byte[], byte[]> consumer = KafkaNative.construct(() -> new KafkaConsumer<>(properties));
        try {
            consumer.assign(List.of(tp));
            consumer.seek(tp, range.nextOffset());
            while (awaitUnpaused()) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (records.count() > 1) throw new ReplayException(ReplayException.Code.OBSERVATION_FAILED);
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.offset() < published.positions().get(index).nextOffset())
                        throw new ReplayException(ReplayException.Code.OBSERVATION_FAILED);
                    if (record.offset() >= range.endOffset()) {
                        verify(published);
                        advance(index, range.endOffset());
                        return;
                    }
                    long size = size(record);
                    if (size > recordLimit) throw new ReplayException(ReplayException.Code.CAPACITY);
                    synchronized (control) {
                        inFlight++;
                        bytes += size;
                    }
                    boolean active = false;
                    try {
                        if (!admit(size)) return;
                        active = true;
                        // 暂停与限速可能长时间等待，来源证据必须在取得处理名额后重新读取。
                        verify(published);
                        if (!plan.pipelineFingerprint().equals(processor.fingerprint()))
                            throw new ReplayException(ReplayException.Code.CONTRACT_MISMATCH);
                        ReplayRecord input = new ReplayRecord(plan.clusterId(), record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value(), new KafkaHeaders(record.headers()));
                        // 取得来源证据与等待速率后再复验 lease；已可知失权时不得开启新的业务副作用。
                        verifyAuthority();
                        try {
                            processor.process(input);
                        } catch (Throwable error) {
                            throw new ReplayException(ReplayException.Code.PROCESSING_FAILED);
                        }
                        // 处理真实返回成功后才写持久进度；取消只停止新准入，不丢弃已经完成的安全证据。
                        advance(index, record.offset() + 1);
                        synchronized (control) {
                            processed++;
                        }
                    } finally {
                        synchronized (control) {
                            if (active) activeHandlers--;
                            inFlight--;
                            bytes -= size;
                            control.notifyAll();
                        }
                    }
                }
                if (stopping()) return;
                // position 可越过事务控制记录和压缩空洞，只有本轮交付给业务的记录全部已确认才可使用此边界。
                long position = consumer.position(tp, OPERATION_BUDGET);
                if (position >= range.endOffset()) {
                    verify(published);
                    advance(index, range.endOffset());
                    return;
                }
            }
        } finally {
            consumer.close(OPERATION_BUDGET);
        }
    }

    /**
     * 业务作用：在副作用准入与无记录边界推进前复验物理来源、保留范围及外部代次。
     * <p>
     * 返回：本轮全部证据成立；不足、变化或预算耗尽则停止执行。
     *
     * @param checkpoint 最后确认进度。
     */
    private void verify(ReplayCheckpoint checkpoint) throws Exception {
        long deadline = System.nanoTime() + OPERATION_BUDGET.toNanos();
        ReplayObservation observation = source.internalAdmin().replayObservation(topics, null, remaining(deadline));
        Map<String, String> generations = identity.generations(observation.clusterId(), topics, remaining(deadline));
        remaining(deadline);
        plan.validateResume(observation, generations, checkpoint);
    }

    /**
     * 业务作用：将后端权威复验与所有分区持久推进串行，发现失权后关闭整个任务的新派发。
     * <p>
     * 参数说明：无。
     * 返回：当前 lease 证据仍成立；失败保持最后公开进度并进入 checkpoint 保护态，不取消已有实际副作用。
     */
    private void verifyAuthority() {
        synchronized (persistence) {
            if (checkpointBlocked) throw new ReplayException(ReplayException.Code.CHECKPOINT_FAILED);
            try {
                lease.verify();
            } catch (Throwable error) {
                // 自定义后端也必须受运行时保护，不能让其它分区在权威不可证明后继续派发或推进。
                checkpointBlocked = true;
                CheckpointException failure = error instanceof CheckpointException known ? known : new CheckpointException(CheckpointException.Code.FENCED);
                fail(failure);
                throw failure;
            }
        }
    }

    /**
     * 业务作用：串行合并不同分区的安全进度，防止旧完整快照覆盖其它 worker 的确认。
     * <p>
     * 返回：持久 CAS 成功后才公开新快照；失败不发布候选进度。
     *
     * @param index 分区位置
     * @param next  已达到成功条件的下一位置。
     */
    private void advance(int index, long next) {
        synchronized (persistence) {
            // 任一次写入失败都保留最后确认进度，不能依赖自定义存储自行封禁后续分区的写入。
            if (checkpointBlocked) throw new ReplayException(ReplayException.Code.CHECKPOINT_FAILED);
            ReplayCheckpoint current = lease.checkpoint();
            if (next <= current.positions().get(index).nextOffset()) return;
            List<ReplayCheckpoint.Position> positions = new ArrayList<>(current.positions());
            var old = positions.get(index);
            positions.set(index, new ReplayCheckpoint.Position(old.topic(), old.partition(), old.startOffset(), old.endOffset(), next));
            // lease 失败会停止其它分区继续推进，已确认快照不能先于存储成功被读取。
            try {
                published = lease.advance(current.revision(), positions);
            } catch (Throwable error) {
                checkpointBlocked = true;
                Throwable failure = error instanceof CheckpointException ? error : new CheckpointException(CheckpointException.Code.OUTCOME_UNKNOWN);
                fail(failure);
                throw failure instanceof RuntimeException runtime ? runtime : new ReplayException(ReplayException.Code.CHECKPOINT_FAILED);
            }
        }
    }

    /**
     * 业务作用：为记录驻留和原文字节设置可解释的框架预算，保留 null 与空记录的区别。
     *
     * @param record 本轮原生输入。
     * @return 正文、key、UTF-8头名及头值加固定元数据额度。
     */
    private static long size(ConsumerRecord<byte[], byte[]> record) {
        long size = 128L + record.topic().getBytes(StandardCharsets.UTF_8).length + (record.key() == null ? 0 : record.key().length) + (record.value() == null ? 0 : record.value().length);
        for (var header : record.headers())
            size += 16L + header.key().getBytes(StandardCharsets.UTF_8).length + (header.value() == null ? 0 : header.value().length);
        return size;
    }

    /**
     * 业务作用：暂停期间不继续拉取输入，取消或失败后不重新开放。
     * <p>
     * 参数说明：无。
     *
     * @return 当前允许继续读取时 true，否则退出。
     */
    private boolean awaitUnpaused() throws InterruptedException {
        synchronized (control) {
            while (paused && !stoppingLocked()) control.wait(100);
            return !stoppingLocked();
        }
    }

    /**
     * 业务作用：在同一控制锁内实行全任务条数/字节速率和处理准入，取消不能穿过该门。
     *
     * @param size 已占用额度的本条原文字节。
     * @return 获得实际处理权时 true，停止时 false；至多允许一个记录大小的初始速率突发。
     */
    private boolean admit(long size) throws InterruptedException {
        synchronized (control) {
            while (!stoppingLocked()) {
                long now = System.nanoTime();
                long wait = Math.max(nextRecord - now, nextBytes - now);
                if (!paused && wait <= 0) {
                    nextRecord = now + delay(1, plan.limits().recordsPerSecond());
                    nextBytes = now + delay(size, plan.limits().bytesPerSecond());
                    activeHandlers++;
                    return true;
                }
                control.wait(paused ? 100 : Math.max(1, Math.min(100, TimeUnit.NANOSECONDS.toMillis(wait))));
            }
            return false;
        }
    }

    /**
     * 业务作用：将速率额度转为向上取整的间隔，不因整数截断超过声明上限。
     *
     * @param amount 条数或字节
     * @param rate   每秒额度，零代表不节流。
     * @return 非负纳秒间隔。
     */
    private static long delay(long amount, long rate) {
        if (rate == 0) return 0;
        long numerator = Math.multiplyExact(amount, 1_000_000_000L);
        return numerator / rate + (numerator % rate == 0 ? 0 : 1);
    }

    /**
     * 业务作用：跨线程查询停止门而不把暂停当作失败。
     * <p>
     * 参数说明：无。
     *
     * @return 取消、失败或终态时 true。
     */
    private boolean stopping() {
        synchronized (control) {
            return stoppingLocked();
        }
    }

    /**
     * 业务作用：供准入临界区复验同一停止状态。
     * <p>
     * 参数说明：无。
     *
     * @return 不再允许新处理时 true。
     */
    private boolean stoppingLocked() {
        return cancelled || failure != null || done;
    }

    /**
     * 业务作用：保留首个失败并关闭新准入，底层异常正文不进入公开状态。
     * <p>
     * 返回：唤醒等待中的 worker，已经开始的责任仍由协调线程排干。
     *
     * @param error 处理、存储或 broker 失败。
     */
    private void fail(Throwable error) {
        synchronized (control) {
            if (error instanceof CheckpointException checkpoint) {
                if (checkpointFailure == null) checkpointFailure = checkpoint.code();
            }
            if (failure == null) failure = error instanceof ReplayException replay ? replay.code()
                    : KafkaBrokerErrors.authorization(error) ? ReplayException.Code.AUTHORIZATION
                      : error instanceof CheckpointException ? ReplayException.Code.CHECKPOINT_FAILED
                        : error instanceof OffsetOutOfRangeException ? ReplayException.Code.DATA_GAP
                          : ReplayException.Code.OBSERVATION_FAILED;
            control.notifyAll();
        }
    }

    /**
     * 业务作用：元数据与外部目录查询共享一次总预算。
     *
     * @param deadline 单调时钟截止时间。
     * @return 正剩余预算，耗尽后拒绝继续操作。
     */
    private static Duration remaining(long deadline) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) throw new ReplayException(ReplayException.Code.TIMEOUT);
        return Duration.ofNanos(nanos);
    }
}

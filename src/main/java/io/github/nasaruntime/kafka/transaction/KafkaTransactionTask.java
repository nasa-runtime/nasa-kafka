package io.github.nasaruntime.kafka.transaction;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.dlt.DltRetention;
import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.support.KafkaBrokerErrors;
import io.github.nasaruntime.kafka.support.KafkaNative;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.dlt.DltEvidence;
import io.github.nasaruntime.kafka.replay.ReplayRecord;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * consumer 只由 poll owner 操作，事务 producer 与同步业务只由单一批次线程操作。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaTransactionTask {

    private final KafkaTransactionManager manager;
    private final KafkaProxy source;
    private final TransactionPlan plan;
    private final TransactionProcessor processor;
    private final Properties consumerProperties, producerProperties;
    private final Object control = new Object();
    private final CountDownLatch completed = new CountDownLatch(1);
    private Set<TopicPartition> assignment = Set.of();
    private TransactionStatus.State state = TransactionStatus.State.STARTING;
    private TransactionException.Code failure;
    private boolean cancelled, initialized, joined, drained;
    private long epoch, committedBatches, committedInputs, committedOutputs, abortedBatches;
    private Map<TopicPartition, Long> inputStarts = Map.of(), inputEnds = Map.of();
    private Batch active;
    private KafkaProducer<byte[], byte[]> producer;
    private KafkaConsumer<byte[], byte[]> consumer;
    private Thread worker;
    private int attempt = 1;
    private long retryAfter;

    /**
     * 业务作用：冻结专属原生配置并在建连前复验组身份与隔离合同。
     * <p>
     * 返回：尚未取得 broker 权威的实例；组或框架配置预检失败为事务 INVALID_CONFIGURATION，隔离冲突为 ISOLATION_MISMATCH，不占用稳定事务身份。
     *
     * @param manager   登记事务任务并协调停止的任务管理器。
     * @param source    事务输入、输出和组位点共同绑定的数据源代理。
     * @param plan      原子合同
     * @param processor 同步回调。
     */
    KafkaTransactionTask(KafkaTransactionManager manager, KafkaProxy source, TransactionPlan plan, TransactionProcessor processor) {
        this.manager = manager;
        this.source = source;
        this.plan = plan;
        this.processor = processor;
        // 执行边界仍复验同一个原文身份，配置失败必须留在事务错误域，且发生在任务登记和 native 资源创建之前。
        TransactionPlan.validateGroup(plan.group());
        try {
            consumerProperties = source.config().buildConsumerProperties(plan.group());
        } catch (KafkaFailure invalid) {
            throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
        }
        Object isolation = consumerProperties.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        if (isolation != null && !"read_committed".equals(isolation.toString()))
            throw new TransactionException(TransactionException.Code.ISOLATION_MISMATCH);
        // 原生反序列化只返回字节，自动提交和普通提交协调器均不参与事务输入。
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, plan.limits().maxBatchRecords());
        consumerProperties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        consumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (int) plan.limits().maxBatchBytes());
        consumerProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (int) plan.limits().maxBatchBytes());
        try {
            producerProperties = source.config().buildProducerProperties();
        } catch (KafkaFailure invalid) {
            throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
        }
        // 原生 interceptor 可以改写消费坐标或发布目标，不能绕过本批冻结的输入责任与输出白名单。
        if (!Objects.toString(consumerProperties.get(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG), "").isBlank()
                || !Objects.toString(producerProperties.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), "").isBlank())
            throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
        // 专属 producer 的原子协议固定幂等和 all ACK，不继承普通发布 lane 的较弱交付模式。
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, plan.identity().transactionalId());
        producerProperties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, plan.limits().transactionTimeoutMs());
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (long) plan.limits().operationTimeoutMs());
        producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, plan.limits().operationTimeoutMs());
        producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, plan.limits().transactionTimeoutMs());
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 0L);
        producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Math.max(16384L, plan.limits().maxOutputBytes()));
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    }

    /**
     * 业务作用：向目录提供固定身份。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变事务计划。
     */
    TransactionPlan plan() {
        return plan;
    }

    /**
     * 业务作用：登记完成后启动唯一 consumer owner。
     * <p>
     * 参数说明：无。
     * 返回：初始化异步进行，实际失败通过状态暴露。
     */
    void start() {
        Thread.ofPlatform().daemon(true).name("nasa-transaction-poll").start(this::run);
    }

    /**
     * 业务作用：在同一控制门内封闭新批次并撤销未提交批次。
     * <p>
     * 参数说明：无。
     *
     * @return 不伪装为实际排干的快照。
     */
    TransactionStatus cancel() {
        synchronized (control) {
            cancelled = true;
            if (active != null && state != TransactionStatus.State.COMMITTING && active.rejection == null)
                active.rejection = TransactionException.Code.CANCELLED;
            control.notifyAll();
            return snapshot();
        }
    }

    /**
     * 业务作用：取得同一控制时刻的阶段、权威与已确认统计。
     * <p>
     * 参数说明：无。
     *
     * @return 不会启动原生客户端的快照。
     */
    TransactionStatus status() {
        synchronized (control) {
            return snapshot();
        }
    }

    /**
     * 业务作用：使 Ready 在取消、失权或失败后立即失效，保留结果未知的输入范围。
     * <p>
     * 参数说明：无。
     *
     * @return 资源终态不覆盖错误原因。
     */
    private TransactionStatus snapshot() {
        return new TransactionStatus(plan.identity().transactionalId(), plan.group(), state, epoch, assignment,
                initialized && joined && !assignment.isEmpty() && !cancelled && failure == null
                        && (active == null || active.rejection == null), drained, failure,
                committedBatches, committedInputs, committedOutputs, abortedBatches, inputStarts, inputEnds);
    }

    /**
     * 业务作用：按单一绝对期限等待当前分配就绪。
     *
     * @param timeout 非负总预算。
     * @return 就绪快照；失败不等待到超时。
     */
    TransactionStatus awaitReady(Duration timeout) {
        long deadline = deadline(timeout);
        synchronized (control) {
            while (true) {
                TransactionStatus current = snapshot();
                if (current.ready()) return current;
                if (failure != null) throw new TransactionException(failure);
                if (cancelled || drained) throw new TransactionException(TransactionException.Code.CLOSED);
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) throw new TransactionException(TransactionException.Code.TRANSACTION_TIMEOUT);
                try {
                    TimeUnit.NANOSECONDS.timedWait(control, remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new TransactionException(TransactionException.Code.CANCELLED);
                }
            }
        }
    }

    /**
     * 业务作用：等待全部实际资源退出，调用超时不取消原生 commit。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param timeout 非负总预算。
     * @return 退出为 true，中断或预算耗尽为 false。
     */
    public boolean awaitDrained(Duration timeout) {
        long deadline = deadline(timeout);
        try {
            return completed.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 业务作用：验证等待预算并转换到安全的单调时间范围。
     *
     * @param timeout 调用方预算。
     * @return 最多一天的绝对纳秒截止时间，超额拒绝。
     */
    private static long deadline(Duration timeout) {
        if (timeout == null || timeout.isNegative() || timeout.compareTo(Duration.ofDays(1)) > 0)
            throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
        return System.nanoTime() + timeout.toNanos();
    }

    /**
     * 业务作用：持续维护组成员资格，暂停数据拉取期间仍接收失权通知，最后等待真实事务线程。
     * <p>
     * 参数说明：无。
     * 返回：仅全部资源关闭后归还目录身份。
     */
    private void run() {
        TransactionException.Code pollFailure = null;
        try {
            // 同源配置不能替代真实物理身份；先核对 broker，再以稳定 ID 结束旧 producer 的残留责任。
            if (!plan.clusterId().equals(source.internalAdmin().clusterId(Duration.ofMillis(plan.limits().operationTimeoutMs()))))
                throw new TransactionException(TransactionException.Code.CLUSTER_MISMATCH);
            if (stopping()) return;
            producer = KafkaNative.construct(() -> new KafkaProducer<>(producerProperties));
            producer.initTransactions();
            if (stopping()) return;
            consumer = KafkaNative.construct(() -> new KafkaConsumer<>(consumerProperties));
            subscribe();
            synchronized (control) {
                initialized = true;
                state = TransactionStatus.State.IDLE;
                control.notifyAll();
            }
            while (!stopping() || active != null) {
                settle();
                expire();
                if (stopping() && active == null) break;
                if (active != null || System.nanoTime() < retryAfter || stopping())
                    consumer.pause(consumer.assignment());
                else consumer.resume(consumer.assignment());
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(50));
                synchronized (control) {
                    joined = consumer.groupMetadata().generationId() >= 0;
                    control.notifyAll();
                }
                if (!records.isEmpty()) {
                    if (active != null || stopping() || System.nanoTime() < retryAfter) rewind(records);
                    else begin(records);
                }
            }
        } catch (Throwable error) {
            pollFailure = classify(error);
            cancel();
        } finally {
            // 超时只撤销权威，不证明业务线程停止；旧回调真实退出前不能关闭 producer 或允许同身份接管。
            joinWorker();
            // 消费观察失败不能覆盖批次实际 commit/abort 的未知结果，先保留原子操作的最终裁决。
            synchronized (control) {
                if (failure == null) failure = pollFailure;
            }
            closeNative();
            synchronized (control) {
                assignment = Set.of();
                joined = false;
                initialized = false;
                drained = true;
                state = failure == null ? TransactionStatus.State.STOPPED : TransactionStatus.State.FAILED;
                control.notifyAll();
            }
            manager.finished(this);
            completed.countDown();
        }
    }

    /**
     * 业务作用：绑定 subscribe 的真实分配通知，任一参与权威变化都撤销未提交整批。
     * <p>
     * 参数说明：无。
     * 返回：新的组订阅；不使用手动 assign。
     */
    private void subscribe() {
        consumer.subscribe(plan.inputTopics(), new ConsumerRebalanceListener() {
            /**
             * 业务作用：撤销分区交付权，阻止旧 metadata 进入提交。
             *
             * 返回：同步使当前批次失效。
             *
             * @param partitions 撤回集合。
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                assignmentChanged(partitions, false);
            }

            /**
             * 业务作用：失去组权威后停止旧批次的新副作用。
             *
             * 返回：与撤回一致地撤销整批。
             *
             * @param partitions 已失去集合。
             */
            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                assignmentChanged(partitions, false);
            }

            /**
             * 业务作用：发布新的定位代次，并暂停已有批次期间的新分区。
             *
             * 返回：Ready 可重新按当次权威计算。
             *
             * @param partitions 新增集合。
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                assignmentChanged(partitions, true);
                if (active != null || stopping()) consumer.pause(consumer.assignment());
            }
        });
    }

    /**
     * 业务作用：线性化组代次变化与提交准入；进入 commit 后只等待原生结果。
     * <p>
     * 返回：旧上下文立即不能新发送。
     *
     * @param partitions 变更集
     * @param add        表示取得或撤销。
     */
    private void assignmentChanged(Collection<TopicPartition> partitions, boolean add) {
        synchronized (control) {
            epoch++;
            Set<TopicPartition> next = new HashSet<>(assignment);
            if (add) next.addAll(partitions);
            else next.removeAll(partitions);
            assignment = Set.copyOf(next);
            joined = false;
            // 不能将一个新 group metadata 拼接到旧输入批次；整批恢复后再重新读取组位置。
            if (active != null && state != TransactionStatus.State.COMMITTING && active.rejection == null)
                active.rejection = TransactionException.Code.REBALANCED;
            control.notifyAll();
        }
    }

    /**
     * 业务作用：把单次 poll 冻结成有界原文批次，保留每分区起点用于已确认中止后的重读。
     * <p>
     * 返回：开始唯一事务线程，超额不提交任何位点。
     *
     * @param records 当前 owner 实际拉取记录。
     */
    private void begin(ConsumerRecords<byte[], byte[]> records) {
        List<ReplayRecord> inputs = new ArrayList<>();
        Map<TopicPartition, Long> starts = new HashMap<>(), ends = new HashMap<>();
        long bytes = 0;
        for (var record : records) {
            var tp = new TopicPartition(record.topic(), record.partition());
            KafkaHeaders headers = new KafkaHeaders(record.headers());
            starts.merge(tp, record.offset(), Math::min);
            ends.merge(tp, Math.addExact(record.offset(), 1), Math::max);
            bytes += 64L + Math.max(0, record.serializedKeySize()) + Math.max(0, record.serializedValueSize()) + headers.byteSize();
            if (inputs.size() >= plan.limits().maxBatchRecords() || bytes > plan.limits().maxBatchBytes()
                    || headers.entries().size() > DltEvidence.MAX_HEADERS || headers.byteSize() > DltEvidence.MAX_HEADER_BYTES) {
                synchronized (control) {
                    inputStarts = Map.copyOf(starts);
                    inputEnds = Map.copyOf(ends);
                }
                throw new TransactionException(TransactionException.Code.CAPACITY);
            }
            inputs.add(new ReplayRecord(plan.clusterId(), record.topic(), record.partition(), record.offset(), record.timestamp(), record.key(), record.value(), headers));
        }
        consumer.pause(consumer.assignment());
        synchronized (control) {
            if (cancelled) {
                rewind(records);
                return;
            }
            if (!assignment.containsAll(starts.keySet()))
                throw new TransactionException(TransactionException.Code.REBALANCED);
            inputStarts = Map.copyOf(starts);
            inputEnds = Map.copyOf(ends);
            active = new Batch(epoch, consumer.groupMetadata(), inputs, inputStarts, inputEnds, attempt, plan.limits());
            state = TransactionStatus.State.PROCESSING;
            Batch batch = active;
            worker = Thread.ofPlatform().daemon(true).name("nasa-transaction-batch").start(() -> execute(batch));
        }
    }

    /**
     * 业务作用：暂停或取消边界上退回尚未登记的 poll 记录，避免消费者内部位置越过未处理输入。
     * <p>
     * 返回：只调整当前仍持有分区的本地位置。
     *
     * @param records 本轮额外返回数据。
     */
    private void rewind(ConsumerRecords<byte[], byte[]> records) {
        for (TopicPartition tp : records.partitions())
            if (consumer.assignment().contains(tp)) consumer.seek(tp, records.records(tp).getFirst().offset());
    }

    /**
     * 业务作用：在控制门内拒绝超时、取消或旧组代次的新副作用。
     * <p>
     * 返回：仍有权推进时通过。
     *
     * @param batch 不可替换的实际批次。
     */
    private void check(Batch batch) {
        synchronized (control) {
            if (batch.rejection != null) throw new TransactionException(batch.rejection);
            if (cancelled) throw new TransactionException(TransactionException.Code.CANCELLED);
            if (epoch != batch.epoch || !assignment.containsAll(batch.starts.keySet()))
                throw new TransactionException(TransactionException.Code.REBALANCED);
            if (System.nanoTime() >= batch.deadline)
                throw new TransactionException(TransactionException.Code.TRANSACTION_TIMEOUT);
            if (state == TransactionStatus.State.PROCESSING && System.nanoTime() >= batch.handlerDeadline)
                throw new TransactionException(TransactionException.Code.HANDLER_TIMEOUT);
        }
    }

    /**
     * 业务作用：持续 poll 时撤销超时回调的权威，不以线程中断代替实际退出。
     * <p>
     * 参数说明：无。
     * 返回：回调即使忽略期限也不能在返回后提交。
     */
    private void expire() {
        synchronized (control) {
            if (active == null || active.rejection != null || state == TransactionStatus.State.COMMITTING || state == TransactionStatus.State.ABORTING)
                return;
            if (System.nanoTime() >= active.deadline) active.rejection = TransactionException.Code.TRANSACTION_TIMEOUT;
            else if (state == TransactionStatus.State.PROCESSING && System.nanoTime() >= active.handlerDeadline)
                active.rejection = TransactionException.Code.HANDLER_TIMEOUT;
        }
    }

    /**
     * 业务作用：串行完成 begin、业务确认、专属输出、组位点与 commit，失败时只在合法阶段中止。
     * <p>
     * 返回：确认提交才增加成功计数，未知结果封闭 owner。
     *
     * @param batch 完整输入责任。
     */
    private void execute(Batch batch) {
        boolean begun = false, committing = false;
        TransactionContext context = null;
        try {
            check(batch);
            producer.beginTransaction();
            begun = true;
            context = new TransactionContext(plan, batch.inputs, () -> check(batch), batch.attempt);
            try {
                processor.process(context);
            } catch (TransactionException declared) {
                throw declared;
            } catch (Throwable error) {
                throw new TransactionException(TransactionException.Code.HANDLER_FAILED);
            }
            var outputs = context.seal();
            synchronized (control) {
                check(batch);
                state = TransactionStatus.State.SENDING;
            }
            for (String topic : context.dltTopics()) {
                check(batch);
                // 事务原子性不能阻止后续日志压缩；所有DLT目标先验证，再发出任何本批输出。
                var settings = source.internalAdmin().describeTopic(topic, Duration.ofNanos(Math.max(1, batch.deadline - System.nanoTime()))).settings();
                try {
                    DltRetention.validate(settings);
                } catch (KafkaFailure invalid) {
                    throw new TransactionException(TransactionException.Code.INVALID_CONFIGURATION);
                }
            }
            for (var output : outputs) {
                check(batch);
                // 发送确认只属于事务内部；即使成功也不能在 commit 确认前发布 Delivered 或输入水位。
                producer.send(output).get(Math.max(1, batch.deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            }
            synchronized (control) {
                check(batch);
                state = TransactionStatus.State.PREPARING;
            }
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            batch.ends.forEach((tp, offset) -> offsets.put(tp, new OffsetAndMetadata(offset)));
            producer.sendOffsetsToTransaction(offsets, batch.metadata);
            synchronized (control) {
                // 提交准入与本地失权通知使用同一门；之后的取消只能等待 commit，不能切换为 abort。
                check(batch);
                state = TransactionStatus.State.COMMITTING;
                committing = true;
            }
            producer.commitTransaction();
            synchronized (control) {
                committedBatches++;
                committedInputs += batch.inputs.size();
                committedOutputs += outputs.size();
                batch.committed = true;
            }
        } catch (Throwable error) {
            TransactionException.Code code = classify(error);
            if (committing) {
                // 原生 commit 的等待失败不证明 broker 未提交；保留身份和输入证据，关闭后由同身份初始化恢复。
                if (code != TransactionException.Code.FENCED && code != TransactionException.Code.AUTHORIZATION)
                    code = TransactionException.Code.COMMIT_UNKNOWN;
                fail(code);
            } else if (begun && code != TransactionException.Code.FENCED && code != TransactionException.Code.AUTHORIZATION) {
                synchronized (control) {
                    state = TransactionStatus.State.ABORTING;
                }
                try {
                    producer.abortTransaction();
                    synchronized (control) {
                        abortedBatches++;
                        batch.aborted = true;
                        batch.reason = code;
                    }
                } catch (Throwable abortFailure) {
                    TransactionException.Code abortCode = classify(abortFailure);
                    fail(abortCode == TransactionException.Code.FENCED || abortCode == TransactionException.Code.AUTHORIZATION
                            ? abortCode : TransactionException.Code.ABORT_UNKNOWN);
                }
            } else if (code != TransactionException.Code.CANCELLED) fail(code);
        } finally {
            if (context != null) context.close();
            batch.done = true;
        }
    }

    /**
     * 业务作用：事务线程真实退出后裁决重读，只有确认中止才能重新调用业务。
     * <p>
     * 参数说明：无。
     * 返回：成功推进下一批；已失权时重新 subscribe 由 broker 恢复位置。
     */
    private void settle() {
        Batch batch = active;
        if (batch == null || !batch.done) return;
        joinWorker();
        synchronized (control) {
            active = null;
        }
        if (batch.committed) attempt = 1;
        else if (batch.aborted && !stopping()) {
            if (attempt > plan.limits().maxAbortedRetries()) {
                fail(batch.reason);
                return;
            }
            attempt++;
            if (batch.epoch == epoch) {
                // 只有组权威未变化时才恢复本地输入起点；否则旧起点可能已被新的组成员完成。
                batch.starts.forEach((tp, offset) -> consumer.seek(tp, offset));
            } else {
                consumer.unsubscribe();
                subscribe();
            }
            retryAfter = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(plan.limits().retryBackoffMs());
        }
        synchronized (control) {
            if (!cancelled)
                state = System.nanoTime() < retryAfter ? TransactionStatus.State.BACKOFF : TransactionStatus.State.IDLE;
            control.notifyAll();
        }
    }

    /**
     * 业务作用：等待当前唯一批次线程归还实际执行权，中断不提前释放身份。
     * <p>
     * 参数说明：无。
     * 返回：线程终止后清理引用。
     */
    private void joinWorker() {
        boolean interrupted = false;
        while (worker != null && worker.isAlive()) try {
            worker.join();
        } catch (InterruptedException ignored) {
            interrupted = true;
        }
        worker = null;
        if (interrupted) Thread.currentThread().interrupt();
    }

    /**
     * 业务作用：在实际批次退出后关闭原生资源，关闭异常不能冒充资源已释放。
     * <p>
     * 参数说明：无。
     * 返回：producer 与 consumer 都确认关闭后才结束。
     */
    private void closeNative() {
        boolean interrupted = Thread.interrupted();
        while (producer != null || consumer != null) {
            try {
                if (producer != null) {
                    producer.close(Duration.ofMillis(plan.limits().operationTimeoutMs()));
                    producer = null;
                }
                if (consumer != null) {
                    consumer.close(Duration.ofMillis(plan.limits().operationTimeoutMs()));
                    consumer = null;
                }
            } catch (Throwable error) {
                synchronized (control) {
                    if (failure == null) failure = classify(error);
                }
                interrupted |= Thread.interrupted();
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    /**
     * 业务作用：封闭失败 owner 的所有后续工作，保持首个确定分类。
     * <p>
     * 返回：Ready 立即失效，终止资源由 owner 完成。
     *
     * @param code 固定失败原因。
     */
    private void fail(TransactionException.Code code) {
        synchronized (control) {
            if (failure == null) failure = code;
            cancelled = true;
            control.notifyAll();
        }
    }

    /**
     * 业务作用：读取永久关闭准入的状态。
     * <p>
     * 参数说明：无。
     *
     * @return 取消或失败均停止新批次。
     */
    private boolean stopping() {
        synchronized (control) {
            return cancelled || failure != null;
        }
    }

    /**
     * 业务作用：将原生和业务异常归类为稳定的恢复决策依据，不暴露任意异常文本。
     *
     * @param error 当前阶段失败。
     * @return fencing、授权、重平衡、超时或通用 broker 分类。
     */
    private static TransactionException.Code classify(Throwable error) {
        while (error instanceof ExecutionException && error.getCause() != null) error = error.getCause();
        if (error instanceof TransactionException declared) return declared.code();
        if (KafkaBrokerErrors.authorization(error)) return TransactionException.Code.AUTHORIZATION;
        if (error instanceof KafkaFailure declared && declared.code() == KafkaFailure.Code.CONFIG)
            return TransactionException.Code.INVALID_CONFIGURATION;
        if (error instanceof ProducerFencedException || error instanceof InvalidProducerEpochException || error instanceof FencedInstanceIdException)
            return TransactionException.Code.FENCED;
        if (error instanceof AuthorizationException || error instanceof AuthenticationException)
            return TransactionException.Code.AUTHORIZATION;
        if (error instanceof CommitFailedException || error instanceof RebalanceInProgressException)
            return TransactionException.Code.REBALANCED;
        if (error instanceof TimeoutException || error instanceof TimeoutException)
            return TransactionException.Code.TRANSACTION_TIMEOUT;
        return TransactionException.Code.BROKER_ERROR;
    }

    /**
     * 一次输入快照；done 只在线程即将退出时发布，资源归还仍须真实 join。
     */
    private static final class Batch {
        final long epoch, deadline, handlerDeadline;
        final ConsumerGroupMetadata metadata;
        final List<ReplayRecord> inputs;
        final Map<TopicPartition, Long> starts, ends;
        final int attempt;
        volatile boolean done;
        boolean committed, aborted;
        TransactionException.Code rejection, reason;

        /**
         * 业务作用：固定同一定位代次内的整批输入、组 metadata 与时间预算。
         * <p>
         * 返回：不随后续分配替换的批次。
         *
         * @param epoch    该批输入取得时的本地分配代次，后续副作用须复验。
         * @param metadata 该批读取时取得的原生消费组代次证据。
         * @param inputs   必须整体处理和确认的独立输入记录快照。
         * @param starts   本批各来源分区的读取起点，确认abort后才可据此重读。
         * @param ends     本批各来源分区的排他末尾，完整成功时才进入事务提交。
         * @param attempt  当前投递次数
         * @param limits   不随重试中的原生调用续期的预算。
         */
        Batch(long epoch, ConsumerGroupMetadata metadata, List<ReplayRecord> inputs, Map<TopicPartition, Long> starts, Map<TopicPartition, Long> ends, int attempt, TransactionLimits limits) {
            this.epoch = epoch;
            this.metadata = metadata;
            this.inputs = List.copyOf(inputs);
            this.starts = starts;
            this.ends = ends;
            this.attempt = attempt;
            long now = System.nanoTime();
            this.deadline = now + TimeUnit.MILLISECONDS.toNanos(limits.transactionTimeoutMs());
            this.handlerDeadline = now + TimeUnit.MILLISECONDS.toNanos(limits.handlerTimeoutMs());
        }
    }
}

package io.github.nasaruntime.kafka;

import io.github.nasaruntime.core.base.AnyHolder;
import io.github.nasaruntime.core.utils.MapUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * 类作用 - Kafka 消费轮询任务, 负责按 group 拉取消息、分发 listener、提交 offset、执行分区命令和处理失败重试。
 */
@Slf4j
@Getter
class KafkaPollTask {

    /** (topic, event) 复合 key 分隔符: 用 Kafka topic 名不可能出现的控制字符, 避免拼接歧义. */
    private static final char TE_SEP = '\u0001';

    private final KafkaProxy proxy;
    private final String groupId;
    private final ConsumeMode mode;

    /** SUBSCRIBE 模式: 订阅的 topic 集合, 运行时可增减. */
    private final Set<String> subscribedTopics = new CopyOnWriteArraySet<>();
    /** 上次 consumer.subscribe 的快照, 与 subscribedTopics 比对决定是否 re-subscribe. */
    private final Set<String> lastSubscribed = ConcurrentHashMap.newKeySet();

    /** ASSIGN 模式: 固定分区 + 起始位点. */
    private final Set<TopicPartition> assignments = new CopyOnWriteArraySet<>();
    private final StartOffset startOffset;

    private volatile Thread runner;
    private volatile boolean running = false;
    private volatile KafkaConsumer<String, byte[]> consumer;

    /** partition → 待提交 offset (poll 完成后 commit). partition → max(offset)+1. */
    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap<>();
    /** 失败 seek 回退计数: key = "tp@batchStartOffset" → 连续失败次数 (仅 poll 线程访问). */
    private final Map<String, Integer> failCounts = new HashMap<>();
    /** 待在 poll 线程执行的分区命令 (seek/pause/resume/position). */
    private final Queue<Consumer<KafkaConsumer<String, byte[]>>> commands = new ConcurrentLinkedQueue<>();

    /**
     * 业务作用：构造订阅模式的消费任务，由 Kafka 负责分区分配并参与消费组再均衡。
     * 该模式下分区归属随组内成员增减而变化，因此不接受起始位点——位点由组的已提交记录决定。
     *
     * @param proxy         所属 Kafka 主入口，用于取配置、listener 注册表与死信转发能力
     * @param groupId       本任务使用的 group.id
     * @param initialTopics 初始订阅的 topic，可为 null；订阅关系在运行期还可增减
     */
    KafkaPollTask(KafkaProxy proxy, String groupId, Collection<String> initialTopics) {
        this.proxy = proxy;
        this.groupId = groupId;
        this.mode = ConsumeMode.SUBSCRIBE;
        this.startOffset = null;
        if (initialTopics != null) this.subscribedTopics.addAll(initialTopics);
    }

    /**
     * 业务作用：构造手动分配模式的消费任务，绑定固定分区且<b>不参与再均衡</b>。
     * 适用于需要精确控制读哪些分区、从哪里开始读的场景，如定点回放。
     * <p>
     * 起始位点为 null 时按最早位点处理：手动分配模式常用于回放，
     * 若默认成末尾位点，调用方不显式指定就会静默跳过全部历史，与该模式的用途相悖。
     *
     * @param proxy       所属 Kafka 主入口
     * @param groupId     本任务使用的 group.id，由调用方指定
     * @param partitions  要绑定的分区，可为 null
     * @param startOffset 起始位点策略；为 null 时按最早位点处理
     */
    KafkaPollTask(KafkaProxy proxy, String groupId, Collection<TopicPartition> partitions, StartOffset startOffset) {
        this.proxy = proxy;
        this.groupId = groupId;
        this.mode = ConsumeMode.ASSIGN;
        this.startOffset = startOffset == null ? StartOffset.earliest() : startOffset;
        if (partitions != null) this.assignments.addAll(partitions);
    }

    // ==================== 动态订阅 (SUBSCRIBE 模式) ====================

    /**
     * 业务作用：在运行期追加一个订阅 topic，下一轮循环检测到订阅集变化后重新订阅生效。
     * 手动分配模式不支持动态订阅，此时只记警告不做改动——该模式的分区绑定在构造时固定，
     * 静默忽略会让调用方以为订阅已生效而实际收不到消息。
     *
     * @param topic 要追加订阅的 topic
     * 返回: 无返回值。
     */
    void subscribe(String topic) {
        if (mode != ConsumeMode.SUBSCRIBE) {
            log.warn("[kafka-poll] group={} ASSIGN 模式不支持 subscribe(topic={})", groupId, topic);
            return;
        }
        if (subscribedTopics.add(topic)) {
            log.info("[kafka-poll] group={} add subscription topic={}", groupId, topic);
        }
    }

    /**
     * 业务作用：在运行期移除一个订阅 topic，下一轮循环重新订阅时生效。
     * 手动分配模式下直接返回。
     *
     * @param topic 要取消订阅的 topic
     * 返回: 无返回值。
     */
    void unsubscribe(String topic) {
        if (mode != ConsumeMode.SUBSCRIBE) return;
        if (subscribedTopics.remove(topic)) {
            log.info("[kafka-poll] group={} remove subscription topic={}", groupId, topic);
        }
    }

    // ==================== 分区 API (命令入队, poll 线程执行) ====================

    /**
     * 业务作用：把分区操作排进队列，交由 poll 线程执行。
     * <b>不能在调用方线程直接操作 consumer</b>：KafkaConsumer 非线程安全，
     * 外部线程直接调用会触发并发访问检查而抛异常，甚至破坏内部状态。
     * <p>
     * 入队后不唤醒阻塞中的 poll：唤醒会打断正在进行的拉取或位点提交，
     * 代价是命令最迟在一个拉取超时之后才生效。
     *
     * @param cmd 待在 poll 线程上执行的分区操作
     * 返回: 无返回值，命令异步生效。
     */
    void enqueue(Consumer<KafkaConsumer<String, byte[]>> cmd) {
        commands.add(cmd);
    }

    /**
     * 业务作用：把指定分区的读取位置移到给定偏移量，用于重放或跳过一段消息。
     *
     * @param tp     目标分区
     * @param offset 目标偏移量
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void seek(TopicPartition tp, long offset) {
        enqueue(c -> c.seek(tp, offset));
    }

    /**
     * 业务作用：批量移动多个分区的读取位置，各分区互不影响。
     *
     * @param offsets 分区到目标偏移量的映射
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void seek(Map<TopicPartition, Long> offsets) {
        enqueue(c -> offsets.forEach(c::seek));
    }

    /**
     * 业务作用：把指定分区移到当前最早可用位置，用于重放尚未被清理的全部历史。
     * 实际起点受 broker 保留策略约束，并非分区创建以来的第一条。
     *
     * @param tps 目标分区集合
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void seekToBeginning(Collection<TopicPartition> tps) {
        enqueue(c -> c.seekToBeginning(tps));
    }

    /**
     * 业务作用：把指定分区移到末尾，用于丢弃积压直接跟进最新消息。
     * 跳过的消息<b>不会再被消费</b>，是不可逆操作。
     *
     * @param tps 目标分区集合
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void seekToEnd(Collection<TopicPartition> tps) {
        enqueue(c -> c.seekToEnd(tps));
    }

    /**
     * 业务作用：把指定分区移到某时刻之后的第一条消息，用于按时间窗口回放。
     * 该分区没有任何满足条件的消息时移到末尾——此时后续只消费新消息，
     * 调用方若把这理解成「定位失败会停在原处」就会误判。
     *
     * @param tp      目标分区
     * @param epochMs 起始时刻，epoch 毫秒
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void seekToTimestamp(TopicPartition tp, long epochMs) {
        enqueue(c -> {
            OffsetAndTimestamp ot = c.offsetsForTimes(Map.of(tp, epochMs)).get(tp);
            if (ot != null) c.seek(tp, ot.offset());
            else c.seekToEnd(List.of(tp));
        });
    }

    /**
     * 业务作用：批量按时刻定位多个分区，每个分区可指定各自的时刻。
     * 一次性向 broker 查询全部分区的位点，避免逐分区往返。
     * 无满足条件消息的分区移到末尾。
     *
     * @param timestamps 分区到起始时刻（epoch 毫秒）的映射
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void seekToTimestamp(Map<TopicPartition, Long> timestamps) {
        enqueue(c -> {
            Map<TopicPartition, OffsetAndTimestamp> r = c.offsetsForTimes(timestamps);
            timestamps.keySet().forEach(tp -> {
                OffsetAndTimestamp ot = r.get(tp);
                if (ot != null) c.seek(tp, ot.offset());
                else c.seekToEnd(List.of(tp));
            });
        });
    }

    /**
     * 业务作用：把指定分区退回该 group 已提交的位点，用于放弃本次会话内的读取进度重新消费。
     * <b>没有提交记录的分区不做任何移动</b>，保持当前位置——不能盲目当作最早位点，
     * 否则一个从未提交过的分区会被整个重放一遍。
     *
     * @param tps 目标分区集合
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void seekToCommitted(Collection<TopicPartition> tps) {
        enqueue(c -> {
            Map<TopicPartition, OffsetAndMetadata> committed = c.committed(new HashSet<>(tps));
            committed.forEach((tp, om) -> {
                if (om != null) c.seek(tp, om.offset());
            });
        });
    }

    /**
     * 业务作用：暂停指定分区的拉取，用于下游承压时局部降速而不断开整个消费。
     * 暂停期间该分区不再返回消息，但消费者仍在组内保持心跳，不会触发再均衡。
     *
     * @param tps 目标分区集合
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void pause(Collection<TopicPartition> tps) {
        enqueue(c -> c.pause(tps));
    }

    /**
     * 业务作用：恢复此前被暂停分区的拉取。
     *
     * @param tps 目标分区集合
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void resume(Collection<TopicPartition> tps) {
        enqueue(c -> c.resume(tps));
    }

    /**
     * 业务作用：暂停当前实际持有的全部分区，用于整体限流或维护窗口。
     * 以执行时刻的实际持有分区为准而非登记的订阅，因此再均衡后新分到的分区也会被涵盖。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void pauseAll() {
        enqueue(c -> c.pause(c.assignment()));
    }

    /**
     * 业务作用：恢复全部处于暂停状态的分区。
     * 只针对当前处于暂停状态的分区，不会误碰本就在正常拉取的分区。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值，操作在 poll 线程上异步生效。
     */
    void resumeAll() {
        enqueue(c -> c.resume(c.paused()));
    }

    /**
     * 业务作用：查询某分区的下一条待读位置，供外部观测消费进度。
     * 查询本身也要在 poll 线程上执行，因此以异步结果返回；调用方等待时应设超时，
     * poll 线程若已停止，该结果将永远不会完成。
     *
     * @param tp 目标分区
     * @return 承载下一条待读偏移量的异步结果；查询在 poll 线程上抛异常时该结果以异常完成。
     */
    CompletableFuture<Long> position(TopicPartition tp) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        enqueue(c -> {
            try {
                f.complete(c.position(tp));
            } catch (Throwable t) {
                f.completeExceptionally(t);
            }
        });
        return f;
    }

    /**
     * 业务作用：查询该消费者当前实际持有的分区，用于确认再均衡后的分配结果。
     * 返回拷贝而非内部集合，避免调用方遍历期间 poll 线程改动分配。
     *
     * <p>参数说明: 无。
     *
     * @return 承载分区集合快照的异步结果。
     */
    CompletableFuture<Set<TopicPartition>> assignmentSnapshot() {
        CompletableFuture<Set<TopicPartition>> f = new CompletableFuture<>();
        enqueue(c -> {
            try {
                f.complete(new HashSet<>(c.assignment()));
            } catch (Throwable t) {
                f.completeExceptionally(t);
            }
        });
        return f;
    }

    /**
     * 业务作用：查询该 group 在指定分区上的已提交位点，用于计算消费滞后。
     * 没有提交记录的分区在结果中保留键但值为 null，与「已提交且位点为 0」区分开——
     * 两者对滞后计算的含义完全不同。
     *
     * @param tps 目标分区集合
     * @return 承载分区到已提交位点映射的异步结果；无提交记录的分区值为 null。
     */
    CompletableFuture<Map<TopicPartition, Long>> committedOffsets(Collection<TopicPartition> tps) {
        CompletableFuture<Map<TopicPartition, Long>> f = new CompletableFuture<>();
        enqueue(c -> {
            try {
                Map<TopicPartition, OffsetAndMetadata> committed = c.committed(new HashSet<>(tps));
                Map<TopicPartition, Long> out = new LinkedHashMap<>();
                committed.forEach((tp, om) -> out.put(tp, om == null ? null : om.offset()));
                f.complete(out);
            } catch (Throwable t) {
                f.completeExceptionally(t);
            }
        });
        return f;
    }

    /**
     * 业务作用：在每轮拉取之前清空命令队列，使外部排入的位点与暂停操作在下次拉取前生效。
     * <p>
     * 单条命令失败只记日志继续执行其余命令：一个分区的位点操作失败不应连累其它分区，
     * 也不应让整个消费循环中断。
     * <p>
     * 唯独唤醒异常必须原样抛出——它是停机信号，在此吞掉会让停机指令丢失，消费线程无法退出。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    private void drainCommands() {
        Consumer<KafkaConsumer<String, byte[]>> cmd;
        while ((cmd = commands.poll()) != null) {
            try {
                cmd.accept(consumer);
            } catch (WakeupException w) {
                throw w;
            } catch (Throwable t) {
                log.error("[kafka-poll] group={} 分区命令执行失败", groupId, t);
            }
        }
    }

    // ==================== 生命周期 ====================

    /**
     * 业务作用：启动消费线程。已在运行时直接返回，使重复启动无副作用。
     * 线程设为守护线程：消费不应阻止 JVM 退出，真正的收尾由停机流程负责。
     * <p>
     * 启动延迟用于错开多个 group 的首次再均衡——同时启动会让 broker 侧集中处理组协调，
     * 延长所有组的首次可用时间。
     *
     * @param startDelayMs 启动前的等待毫秒数，小于等于 0 时立即开始
     * 返回: 无返回值。
     */
    void start(long startDelayMs) {
        if (running) return;
        running = true;
        runner = new Thread(() -> {
            if (startDelayMs > 0) {
                try {
                    Thread.sleep(startDelayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            this.runLoop();
        }, "kafka-poll-" + groupId);
        runner.setDaemon(true);
        runner.start();
    }

    /**
     * 业务作用：停止消费线程并确保阻塞在拉取上的消费者能被唤醒退出。
     * <p>
     * 先置停止标志并短暂等待，给正在处理中的一批消息留出自然结束的机会——
     * 此时退出可以走正常的提交路径，不会造成已处理消息的重复投递。
     * <p>
     * 仅当线程未能自行结束时才唤醒消费者：唤醒会打断进行中的拉取或提交，
     * 是不得已的手段。唤醒后再给一段较长的等待，让线程完成收尾。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。等待超时后本方法仍会返回，此时线程可能尚未完全退出。
     */
    void shutdown() {
        running = false;
        Thread t = runner;
        if (t != null) {
            try {
                t.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!t.isAlive()) return;
        }
        KafkaConsumer<String, byte[]> c = consumer;
        if (c != null) {
            try {
                c.wakeup();
            } catch (Throwable ignored) {
            }
        }
        if (t != null) {
            try {
                t.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ==================== 主循环 ====================

    /**
     * 业务作用：消费线程的最外层循环，负责让消费在任何异常之后都能自愈重建。
     * <p>
     * 捕获全部异常而非让线程死掉：broker 短暂不可用、网络抖动、反序列化崩溃都不应导致
     * 该 group 永久停止消费。每次重建之间等待一段退避时间，避免故障持续时高频重连打爆 broker。
     * <p>
     * 唤醒异常需区分来源：停止标志已置说明是正常停机，直接退出；
     * 否则是运行期被唤醒，重建消费者继续。
     * <p>
     * 无论何种退出路径都在 finally 中关闭消费者，确保连接与组成员身份被释放，
     * 否则 broker 要等到会话超时才把该成员踢出，期间其分区无人消费。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值，直到停止标志被置才退出。
     */
    private void runLoop() {
        long restartDelay = proxy.getProperties().getConsumeRestartDelayMs();
        while (running) {
            try {
                this.initConsumer();
                this.pollLoop();
            } catch (WakeupException e) {
                if (!running) break;
                log.warn("[kafka-poll] group={} wakeup during running, restart loop", groupId);
            } catch (Throwable t) {
                log.error("{} [kafka-poll] group={} loop crashed, restart in {}ms", AnyHolder.getTraceId(), groupId, restartDelay, t);
            } finally {
                this.closeConsumer();
            }
            if (running && restartDelay > 0) {
                try {
                    Thread.sleep(restartDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.info("[kafka-poll] group={} loop exited", groupId);
    }

    /**
     * 业务作用：单次消费者会话内的主循环，按固定次序完成命令执行、订阅同步、拉取、分发与位点推进。
     * <p>
     * 次序不可调整：命令必须在拉取之前执行，否则本轮拉到的消息会先于位点调整被处理，
     * 调整就失去了意义；订阅同步也必须在拉取之前，否则新加的 topic 要多等一轮。
     * <p>
     * 无订阅或无分区时短暂休眠再重试，而不是以零超时空转——空转会把一个 CPU 核占满。
     * <p>
     * 只有整批全部处理成功才提交位点并清除该批的失败计数；
     * 任一 listener 失败都转入失败处理，不提交，从而保证失败的消息会被重新投递。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值，停止标志置位后退出。
     */
    private void pollLoop() {
        Duration pollTimeout = Duration.ofMillis(proxy.getProperties().getPollTimeoutMs());
        while (running) {
            // 1) 先执行入队的分区命令 (seek/pause/resume/position)
            drainCommands();

            // 2) SUBSCRIBE: 检测订阅变化并 re-subscribe; ASSIGN: 无分区则空转
            if (mode == ConsumeMode.SUBSCRIBE) {
                if (!subscribedTopics.equals(lastSubscribed)) {
                    consumer.subscribe(subscribedTopics);
                    lastSubscribed.clear();
                    lastSubscribed.addAll(subscribedTopics);
                    log.info("[kafka-poll] group={} re-subscribed topics={}", groupId, subscribedTopics);
                    if (subscribedTopics.isEmpty()) {
                        if (!sleepQuiet(1000)) break;
                        continue;
                    }
                }
            } else if (assignments.isEmpty()) {
                if (!sleepQuiet(1000)) break;
                continue;
            }

            // 3) poll + dispatch + commit/retry
            ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
            if (records.isEmpty()) continue;
            DispatchResult res = this.dispatch(records);
            if (res.allOk()) {
                this.commitPending();
                res.batchStart().forEach((tp, off) -> failCounts.remove(tp + "@" + off));
            } else {
                this.handleFailure(records, res);
            }
        }
    }

    /**
     * 业务作用：建出消费者并按模式完成订阅或分区绑定，是每次会话重建的起点。
     * <p>
     * 订阅模式下同步记录本次订阅快照，供主循环比对判断订阅是否发生变化；
     * 不记录会导致每轮都重新订阅，频繁触发再均衡。
     * <p>
     * 手动分配模式在绑定后立即应用起始位点，必须在首次拉取之前完成，
     * 否则第一批消息已按默认位点读出，起始位点设置就落空了。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    private void initConsumer() {
        NasaKafkaProperties props = proxy.getProperties();
        consumer = new KafkaConsumer<>(props.buildConsumerProperties(groupId));
        if (mode == ConsumeMode.SUBSCRIBE) {
            if (!subscribedTopics.isEmpty()) {
                consumer.subscribe(subscribedTopics);
                lastSubscribed.clear();
                lastSubscribed.addAll(subscribedTopics);
            }
            log.info("[kafka-poll] group={} SUBSCRIBE consumer initialized topics={}", groupId, subscribedTopics);
        } else {
            consumer.assign(assignments);
            this.applyStartOffset(consumer);
            log.info("[kafka-poll] group={} ASSIGN consumer initialized partitions={} start={}", groupId, assignments, startOffset);
        }
    }

    /**
     * 业务作用：按调用方声明的起始位点策略定位各分区的读取起点。
     * <p>
     * 按时刻定位时，没有满足条件消息的分区移到末尾，即只消费此后的新消息。
     * <p>
     * 沿用已提交位点时，<b>没有提交记录的分区不做移动</b>，其起点交由消费者的越界重置配置决定——
     * 若在此强行按最早位点处理，一个新分区会被整个重放一遍。
     *
     * @param c 本次会话的消费者，此时已完成分区绑定但尚未开始拉取
     * 返回: 无返回值。
     */
    private void applyStartOffset(KafkaConsumer<String, byte[]> c) {
        if (assignments.isEmpty()) return;
        switch (startOffset.kind()) {
            case EARLIEST -> c.seekToBeginning(assignments);
            case LATEST -> c.seekToEnd(assignments);
            case EXPLICIT -> {
                for (TopicPartition tp : assignments) c.seek(tp, startOffset.value());
            }
            case TIMESTAMP -> {
                Map<TopicPartition, Long> q = new HashMap<>();
                for (TopicPartition tp : assignments) q.put(tp, startOffset.value());
                Map<TopicPartition, OffsetAndTimestamp> r = c.offsetsForTimes(q);
                for (TopicPartition tp : assignments) {
                    OffsetAndTimestamp ot = r.get(tp);
                    if (ot != null) c.seek(tp, ot.offset());
                    else c.seekToEnd(List.of(tp));
                }
            }
            case COMMITTED -> {
                // 沿用 group 已提交位点; 无提交的分区不 seek, 首次 poll 由 auto.offset.reset 决定.
                Map<TopicPartition, OffsetAndMetadata> committed = c.committed(assignments);
                committed.forEach((tp, om) -> {
                    if (om != null) c.seek(tp, om.offset());
                });
            }
        }
    }

    /**
     * 业务作用：关闭消费者，并在关闭前把已成功处理但尚未提交的位点同步提交。
     * <b>先提交再关闭</b>：否则这批已经处理完的消息在下次启动时会被重新投递，
     * 业务侧表现为重复处理。
     * <p>
     * 先置空字段再操作，使停机流程中并发到来的唤醒不会作用在正在关闭的实例上。
     * 关闭过程中的异常只记日志：停机路径上抛出会打断后续清理。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    private void closeConsumer() {
        KafkaConsumer<String, byte[]> c = consumer;
        if (c == null) return;
        consumer = null;
        try {
            if (!pendingOffsets.isEmpty()) {
                c.commitSync(new HashMap<>(pendingOffsets));
                pendingOffsets.clear();
            }
            c.close();
        } catch (Throwable t) {
            log.error("{} [kafka-poll] group={} close consumer failed", AnyHolder.getTraceId(), groupId, t);
        }
    }

    // ==================== dispatch ====================

    /**
     * 业务作用：把一批拉取到的记录按 topic 与事件分桶，解码后交给对应 listener，并登记待提交位点。
     * <p>
     * 未注册 listener 的 topic 或事件直接跳过且<b>不推进位点</b>：这类消息可能只是本服务不关心，
     * 但也可能是 listener 尚未注册完成，前移会造成消息被静默丢弃。
     * <p>
     * 解码得空时分两种情形处置：空负载（墓碑消息）没有可投递内容，直接前移越过，
     * 否则该位点会被反复重读；非空但无法解码的属于坏消息，转投死信，
     * 且<b>仅在转投成功或未启用死信时才前移</b>——转投失败仍前移就等于丢消息。
     * <p>
     * 位点按分区取最大值加一累积，因此同一批内乱序到达的记录不会让位点回退。
     * 同时记录每个分区本批的最小位点，供失败时回退重投定位。
     * <p>
     * finally 中清理线程上下文与链路标识：消费线程会被复用，
     * 不清理会让本批的 topic、位点和链路标识串到下一批的处理中。
     *
     * @param records 本轮拉取到的全部记录
     * @return 分发结果：是否整批成功，以及各分区本批的起始位点。
     */
    private DispatchResult dispatch(ConsumerRecords<String, byte[]> records) {
        // (topic \u0001 event) → 该批记录 (带分区/偏移量元数据)
        Map<String, List<PolledRecord>> byTE = new LinkedHashMap<>();
        // 每个分区本批的最小 offset, 供失败 seek 回退
        Map<TopicPartition, Long> batchStart = new HashMap<>();
        boolean allOk = true;

        ConcurrentMap<String, ConcurrentMap<String, ListenerEntry>> byTopic = proxy.getListenerRegistry().get(groupId);
        if (byTopic == null) return new DispatchResult(true, batchStart);

        try {
            for (ConsumerRecord<String, byte[]> record : records) {
                String topic = record.topic();
                String event = this.extractEvent(record);

                ConcurrentMap<String, ListenerEntry> byEvent = byTopic.get(topic);
                if (byEvent == null) continue;
                ListenerEntry entry = byEvent.get(event);
                if (entry == null) continue;

                Object data = proxy.deserializeValue(record.value(), entry.paramType(), topic,
                        this.isProtocolBytesPayload(record), this.payloadMode(record), this.passthroughHeader(record));
                if (data == null) {
                    // 空 payload(tombstone) → 前移越过 (无可投递内容, 免尾部反复重读);
                    // 非空 undecodable(坏 payload/mode 不符/缺 paramType) → 死信, 仅"可前移"(禁用跳过 或 转发成功)时前移;
                    // 转发失败则不前移, 下轮重投重试 (避免丢消息)。
                    TopicPartition tp = new TopicPartition(topic, record.partition());
                    byte[] raw = record.value();
                    boolean advance = (raw == null || raw.length == 0)
                            || proxy.forwardRecordToDeadLetter(record, "undecodable payload");
                    if (advance) {
                        pendingOffsets.merge(tp, new OffsetAndMetadata(record.offset() + 1),
                                (oldM, newM) -> oldM.offset() < newM.offset() ? newM : oldM);
                    }
                    continue;
                }

                TopicPartition tp = new TopicPartition(topic, record.partition());
                batchStart.merge(tp, record.offset(), Math::min);
                pendingOffsets.merge(tp, new OffsetAndMetadata(record.offset() + 1),
                        (oldM, newM) -> oldM.offset() < newM.offset() ? newM : oldM);

                byTE.computeIfAbsent(topic + TE_SEP + event, k -> new ArrayList<>())
                        .add(new PolledRecord(data, topic, record.partition(), record.offset(), record.timestamp(),
                                record.key(), this.headers(record)));
            }

            for (Map.Entry<String, List<PolledRecord>> e : byTE.entrySet()) {
                int sep = e.getKey().indexOf(TE_SEP);
                String topic = e.getKey().substring(0, sep);
                String event = e.getKey().substring(sep + 1);
                ConcurrentMap<String, ListenerEntry> byEvent = byTopic.get(topic);
                if (byEvent == null) continue;
                ListenerEntry entry = byEvent.get(event);
                if (entry == null) continue;
                if (!this.flush(entry, topic, event, e.getValue())) allOk = false;
            }
        } finally {
            KafkaProxyHolder.clear();
            AnyHolder.remove(AnyHolder.TRACE_ID);
        }
        return new DispatchResult(allOk, batchStart);
    }

    /**
     * 业务作用：把一个 (topic, 事件) 桶按 listener 声明的形态投递出去，并把投递成败转成是否可提交位点的判断。
     * <p>
     * 批量形态一次性投递整桶并合并各条的链路标识；单条形态逐条投递，每条前重设上下文，
     * 使业务在回调内取到的分区与位点始终对应当前这条消息。
     * <p>
     * 捕获全部异常并返回失败，而不是让异常上抛：上抛会中断整批其余桶的投递，
     * 其它本可成功的 listener 也会被连累。返回失败使调用方跳过位点提交，该批消息随后被重新投递，
     * 因此<b>业务处理必须幂等</b>。
     *
     * @param entry 命中的 listener 注册项
     * @param topic 本桶的 topic
     * @param event 本桶的事件名
     * @param recs  本桶内已解码的记录
     * @return 全部投递成功返回 true；listener 抛出任何异常返回 false，调用方据此不提交位点。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean flush(ListenerEntry entry, String topic, String event, List<PolledRecord> recs) {
        KafkaListener listener = entry.listener();
        try {
            switch (entry.kind()) {
                case BATCH -> {
                    List<Object> values = new ArrayList<>(recs.size());
                    StringBuilder traceIds = new StringBuilder();
                    for (PolledRecord r : recs) {
                        values.add(r.value());
                        this.appendTrace(traceIds, r.value());
                    }
                    this.applyBatchContext(topic, traceIds);
                    listener.onEvent(values);
                }
                case RECORD_BATCH -> {
                    List<KafkaRecord<Object>> out = new ArrayList<>(recs.size());
                    StringBuilder traceIds = new StringBuilder();
                    for (PolledRecord r : recs) {
                        out.add(r.toKafkaRecord());
                        this.appendTrace(traceIds, r.value());
                    }
                    this.applyBatchContext(topic, traceIds);
                    listener.onEvent(out);
                }
                case RECORD_SINGLE -> {
                    for (PolledRecord r : recs) {
                        this.applySingleContext(r);
                        listener.onEvent(r.toKafkaRecord());
                    }
                }
                default -> {
                    for (PolledRecord r : recs) {
                        this.applySingleContext(r);
                        listener.onEvent(r.value());
                    }
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("{} [kafka-flush] group={} topic={} event={} kind={} count={} listener={}",
                        AnyHolder.getTraceId(), groupId, topic, event, entry.kind(), recs.size(), listener.getClass().getSimpleName());
            }
            return true;
        } catch (Throwable t) {
            log.error("{} [kafka-flush] failed group={} topic={} event={} kind={} count={} listener={}",
                    AnyHolder.getTraceId(), groupId, topic, event, entry.kind(), recs.size(), listener.getClass().getName(), t);
            return false;
        }
    }

    /**
     * 业务作用：单条投递前重设线程上下文，使业务在回调内能取到本条消息的来源元数据。
     * <p>
     * 链路标识按本条消息的透传内容重设；<b>本条没有携带时必须显式清除</b>，
     * 否则会沿用上一条消息的标识，导致两条无关消息的日志被串到同一条链路上。
     *
     * @param r 即将投递的记录
     * 返回: 无返回值。
     */
    private void applySingleContext(PolledRecord r) {
        KafkaProxyHolder.setKafkaProxy(proxy);
        KafkaProxyHolder.setTopic(r.topic());
        KafkaProxyHolder.setPartition(r.partition());
        KafkaProxyHolder.setOffset(r.offset());
        KafkaProxyHolder.setHeaders(r.headers());
        String tid = MapUtils.getString(KafkaProxyHolder.get(r.value()), AnyHolder.TRACE_ID);
        if (tid != null) AnyHolder.set(AnyHolder.TRACE_ID, tid);
        else AnyHolder.remove(AnyHolder.TRACE_ID);
    }

    /**
     * 业务作用：批量投递前重设线程上下文。
     * 只设置 topic 而不设分区与位点——整桶可能来自不同分区，设成其中任意一条都是错的；
     * 需要按条元数据的业务应改用携带记录元数据的 listener 形态。
     * <p>
     * 链路标识为整桶各条标识的合并，使一次批处理的日志能同时关联到上游的多条链路。
     * 桶内无任何标识时显式清除，避免沿用上一批的标识。
     *
     * @param topic    本桶的 topic
     * @param traceIds 本桶内各条消息链路标识的合并结果，可能为空
     * 返回: 无返回值。
     */
    private void applyBatchContext(String topic, StringBuilder traceIds) {
        KafkaProxyHolder.setKafkaProxy(proxy);
        KafkaProxyHolder.setTopic(topic);
        if (traceIds.length() > 0) AnyHolder.set(AnyHolder.TRACE_ID, traceIds.toString());
        else AnyHolder.remove(AnyHolder.TRACE_ID);
    }

    /**
     * 业务作用：从一条消息的透传内容中取出链路标识并追加进合并串，供批量投递整体染色。
     * 无标识的消息跳过，不占位也不产生空段。
     *
     * @param sb    合并中的链路标识串
     * @param value 已解码的业务对象，其关联的透传内容中可能带有链路标识
     * 返回: 无返回值，直接追加到入参。
     */
    private void appendTrace(StringBuilder sb, Object value) {
        String tid = MapUtils.getString(KafkaProxyHolder.get(value), AnyHolder.TRACE_ID);
        if (tid != null) {
            if (sb.length() > 0) sb.append(',');
            sb.append(tid);
        }
    }

    /**
     * 业务作用：从消息头解析事件名，作为 topic 之下的二级路由依据。
     * 取同名头的最后一个值：转发链路上可能有中间环节追加同名头，最后写入的才代表当前语义。
     * <p>
     * 头缺失或为空时回落到默认事件名，使未声明事件的发送方与只注册了默认事件的消费方仍能对接上。
     *
     * @param record 待解析的原始记录
     * @return 事件名；无该头或头值为空时返回默认事件名。
     */
    private String extractEvent(ConsumerRecord<String, byte[]> record) {
        Header h = record.headers().lastHeader(KafkaProxy.HEADER_EVENT);
        if (h == null) return KafkaProxy.DEFAULT_EVENT;
        byte[] v = h.value();
        if (v == null || v.length == 0) return KafkaProxy.DEFAULT_EVENT;
        return new String(v, StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：判定负载是否为本组件的二进制编码，决定走二进制解码还是 JSON 解码。
     * 无编码头视为 JSON，从而兼容本组件启用二进制编码之前发出的历史消息。
     *
     * @param record 待判定的原始记录
     * @return 编码头明确标注为二进制编码时返回 true。
     */
    private boolean isProtocolBytesPayload(ConsumerRecord<String, byte[]> record) {
        Header h = record.headers().lastHeader(KafkaProxy.HEADER_PAYLOAD_CODEC);
        if (h == null || h.value() == null) return false;
        return KafkaProxy.PAYLOAD_CODEC_PROTOCOL_BYTES.equals(new String(h.value(), StandardCharsets.UTF_8));
    }

    /**
     * 业务作用：取出承载透传内容的原始字节，交由解码流程与业务对象关联。
     * 此处只取字节不做解析，是为了把解析与关联放在同一处完成，避免解析结果在两处各存一份而不一致。
     *
     * @param record 待读取的原始记录
     * @return 透传头的原始字节；无该头时为 null。
     */
    private byte[] passthroughHeader(ConsumerRecord<String, byte[]> record) {
        Header h = record.headers().lastHeader(KafkaProxy.HEADER_PASSTHROUGH);
        return h == null ? null : h.value();
    }

    /**
     * 业务作用：取出二进制编码的模式标识，供解码前校验收发双方模式一致。
     * 无该头时返回 null 表示跳过校验，用于兼容未带模式标识的历史消息；
     * 带了却与 listener 声明不符则必然解码错乱，由调用方据此拒绝。
     *
     * @param record 待读取的原始记录
     * @return 模式标识；无该头或头值为空时为 null，表示跳过一致性校验。
     */
    private String payloadMode(ConsumerRecord<String, byte[]> record) {
        Header h = record.headers().lastHeader(KafkaProxy.HEADER_PAYLOAD_MODE);
        return (h == null || h.value() == null) ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：把原始记录的全部消息头整理成业务可读的映射，供 listener 读取框架未建模的自定义头。
     * <p>
     * 同名头保留全部取值而非只留最后一个：转发链路会追加同名头，只留一个会丢失中间环节的信息。
     * <p>
     * 头值做防御性拷贝：原始字节归 Kafka 客户端的接收缓冲所有，会在下一轮拉取时被复用，
     * 直接引用会让业务在异步处理时读到已被覆写的内容。
     *
     * @param record 待整理的原始记录
     * @return 头名到取值列表的映射；保持原始顺序。
     */
    private Map<String, List<byte[]>> headers(ConsumerRecord<String, byte[]> record) {
        Map<String, List<byte[]>> out = new LinkedHashMap<>();
        for (Header h : record.headers()) {
            byte[] v = h.value();
            out.computeIfAbsent(h.key(), k -> new ArrayList<>())
                    .add(v == null ? null : Arrays.copyOf(v, v.length));
        }
        return out;
    }

    // ==================== commit / 失败重投 ====================

    /**
     * 业务作用：把已成功处理的位点同步提交到 broker，使这些消息不会被重新投递。
     * <p>
     * 提交失败时<b>保留待提交内容不清空</b>，下一轮继续尝试：清空会让这批消息在会话中断后被重新投递，
     * 而保留则最多多提交一次，代价小得多。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    private void commitPending() {
        if (pendingOffsets.isEmpty()) return;
        try {
            consumer.commitSync(new HashMap<>(pendingOffsets));
            pendingOffsets.clear();
        } catch (Throwable t) {
            log.error("{} [kafka-poll] commit offset failed group={}", AnyHolder.getTraceId(), groupId, t);
            // 不清 pendingOffsets, 下轮再 retry commit
        }
    }

    /**
     * 业务作用：处理整批中存在失败的情形，按连续失败次数决定回退重投、转投死信还是跳过。
     * <p>
     * 重试上限配置为非正数表示关闭回退重投：此时丢弃待提交位点但不回退读取位置，
     * 消费者在本次会话内已经越过这批消息，等同于会话内跳过；会话重建后会从上次提交处重读。
     * <p>
     * 失败计数按「分区 + 本批起始位点」计，因此位点前移到新的一批时计数自然归零，
     * 不会把不同批次的失败累加成误判。
     * <p>
     * 达到上限判定为毒消息，转投死信。<b>仅在转投成功或未启用死信时才前移越过</b>；
     * 转投失败则回退重投且<b>不清计数</b>，使下一轮仍按上限逻辑直接重试转投，
     * 在死信通道恢复之前一直不丢消息。
     * <p>
     * 执行次序是先提交需要前移的分区，再回退需要重投的分区：
     * 提交发生在回退之前，才能保证前移的位点被固化下来；顺序颠倒会让回退操作影响到本应前移的分区。
     *
     * @param records 本轮拉取到的全部记录，转投死信时需要原始内容
     * @param res     分发结果，其中各分区本批起始位点是回退的目标
     * 返回: 无返回值。
     */
    private void handleFailure(ConsumerRecords<String, byte[]> records, DispatchResult res) {
        int max = proxy.getProperties().getMaxConsumeAttempts();
        if (max <= 0) {
            // 关闭 seek 回退: 丢弃 pending (不 commit), consumer 会话内 position 已前移 = 会话内跳过
            pendingOffsets.clear();
            return;
        }
        Map<TopicPartition, Long> toReplay = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> e : res.batchStart().entrySet()) {
            TopicPartition tp = e.getKey();
            long start = e.getValue();
            String key = tp + "@" + start;
            int n = failCounts.merge(key, 1, Integer::sum);
            if (n >= max) {
                // 毒批: 转死信. 仅"可前移"(禁用跳过 或 全部转发成功)时前移越过 + 清计数;
                // 转发失败则回退重投 (不清计数, 下轮 n 仍 >= max 直接重试 DLT), 避免丢消息。
                if (proxy.forwardPartitionToDeadLetter(records, tp, "consume failed after " + n + " attempts")) {
                    failCounts.remove(key);
                    log.error("[kafka-poll] group={} tp={} 批起始 offset={} 连续失败 {} 次, 跳过该批并前移提交 (疑似毒消息)",
                            groupId, tp, start, n);
                } else {
                    pendingOffsets.remove(tp);
                    toReplay.put(tp, start);
                    log.error("[kafka-poll] group={} tp={} 死信转发失败, 回退重投待 DLT 恢复 (不丢消息) offset={}", groupId, tp, start);
                }
            } else {
                // 回退重投: 丢弃该分区 pending + seek 回起点
                pendingOffsets.remove(tp);
                toReplay.put(tp, start);
                log.warn("[kafka-poll] group={} tp={} 消费失败, seek 回 offset={} 第 {}/{} 次重投", groupId, tp, start, n, max);
            }
        }
        // 先 commit 需前移的毒批分区 (此时 pendingOffsets 只剩它们)
        this.commitPending();
        // 再 seek 回退需重投的分区
        toReplay.forEach((tp, off) -> {
            try {
                consumer.seek(tp, off);
            } catch (Throwable t) {
                log.error("[kafka-poll] group={} seek 回退失败 tp={} offset={}", groupId, tp, off, t);
            }
        });
    }

    /**
     * 业务作用：在空转等待时休眠，并把线程中断转成「应当退出」的信号返回给调用方。
     * 中断后重新置位中断标志，使外层与更上层的等待也能感知到停机意图，不至于把中断吞掉。
     *
     * @param ms 休眠毫秒数
     * @return 正常休眠完毕返回 true；被中断返回 false，调用方应据此退出循环。
     */
    private boolean sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

}

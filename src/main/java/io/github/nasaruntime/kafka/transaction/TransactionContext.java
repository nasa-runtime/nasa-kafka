package io.github.nasaruntime.kafka.transaction;

import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.protocol.KafkaWire;

import io.github.nasaruntime.kafka.consumer.AckMode;
import io.github.nasaruntime.kafka.dlt.DltReplayException;
import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.dlt.DltEvidence;
import io.github.nasaruntime.kafka.dlt.DltOrigin;
import io.github.nasaruntime.kafka.replay.ReplayRecord;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;
import java.util.TreeSet;

/**
 * 只属于一次回调的输入和暂存输出；回调结束后不再接受确认或发送。
 */
public final class TransactionContext {

    private final TransactionPlan plan;
    private final List<ReplayRecord> records;
    private final Runnable authority;
    private final Thread thread = Thread.currentThread();
    private final List<ProducerRecord<byte[], byte[]>> outputs = new ArrayList<>();
    private final BitSet acknowledged = new BitSet();
    private final Set<String> dltTopics = new TreeSet<>();
    private final int attempt;
    private boolean closed;
    private long outputBytes;
    private TransactionException.Code rejection;

    /**
     * 业务作用：将输入、确认和输出绑定同一实际调用。
     * <p>
     * 返回：仅创建线程能修改的上下文。
     *
     * @param plan      冻结合同
     * @param records   独立输入
     * @param authority 实际 owner 权威检查
     * @param attempt   本次批次尝试序号，首次为 1，确认中止后的本实例重投递增。
     */
    TransactionContext(TransactionPlan plan, List<ReplayRecord> records, Runnable authority, int attempt) {
        this.plan = plan;
        this.records = List.copyOf(records);
        this.authority = authority;
        this.attempt = attempt;
    }

    /**
     * 业务作用：读取本批必须整体处理的独立输入。
     * <p>
     * 参数说明：无。
     *
     * @return 保留原 key、tombstone、时间和有序消息头的不可变列表。
     */
    public List<ReplayRecord> records() {
        return records;
    }

    /**
     * 业务作用：让业务识别已确认中止后的重复执行。
     * <p>
     * 参数说明：无。
     *
     * @return 本执行实例中从1开始的批次尝试次数；重启后不提供持久重试计数。
     */
    public int attempt() {
        return attempt;
    }

    /**
     * 业务作用：显式确认本批的一条输入，不能单独推动组位点。
     * <p>
     * 返回：确认暂存；最终仍要求全部输入和回调成功。
     *
     * @param index records 中的下标。
     */
    public void ack(int index) {
        check();
        index(index);
        acknowledged.set(index);
    }

    /**
     * 业务作用：暂存只能通过本次事务 producer 发送的批准输出。
     * <p>
     * 返回：仅接受到本地批次，不表示 broker 已发送或事务已提交。
     *
     * @param topic     本批批准的输出topic，不能超出事务计划白名单。
     * @param partition 本批批准的目标分区号，不能超出事务计划白名单。
     * @param timestamp 可空的非负Kafka毫秒时间，null交由producer生成。
     * @param key       输出原始key字节，null与空数组不同，准入后持有独立快照。
     * @param payload   输出原始正文，null表示tombstone，准入后持有独立快照。
     * @param headers   有界有序消息头。
     */
    public void send(String topic, int partition, Long timestamp, byte[] key, byte[] payload, KafkaHeaders headers) {
        check();
        if (!plan.outputs().contains(new TopicPartition(topic, partition)) || timestamp != null && timestamp < 0 || headers == null)
            reject(TransactionException.Code.INVALID_CONFIGURATION);
        // 头名校验也锁存为整批失败，避免业务捕获异常后仍提交先前暂存的部分输出。
        try {
            headers.entries().forEach(header -> KafkaWire.validateHeaderName(header.key()));
        } catch (KafkaFailure invalid) {
            reject(TransactionException.Code.INVALID_CONFIGURATION);
        }
        long bytes = 64L + (key == null ? 0 : key.length) + (payload == null ? 0 : payload.length) + headers.byteSize();
        if (headers.entries().size() > DltEvidence.MAX_HEADERS || headers.byteSize() > DltEvidence.MAX_HEADER_BYTES
                || outputs.size() >= plan.limits().maxOutputRecords() || bytes > plan.limits().maxOutputBytes() - outputBytes)
            reject(TransactionException.Code.CAPACITY);
        outputs.add(new ProducerRecord<>(topic, partition, timestamp, key == null ? null : key.clone(),
                payload == null ? null : payload.clone(), headers.toNative()));
        outputBytes += bytes;
    }

    /**
     * 业务作用：在当前原子范围内保留失败原文并确认对应输入，避免独立 DLT 发送提前推动位点。
     * <p>
     * 返回：原文与 DLT 来源头暂存；整批提交前对 read_committed 不可见。
     *
     * @param index     本批输入
     * @param topic     本批批准的输出topic，不能超出事务计划白名单。
     * @param partition 本批批准的目标分区号，不能超出事务计划白名单。
     * @param reason    最多1024 UTF-8字节的安全业务原因。
     */
    public void deadLetter(int index, String topic, int partition, String reason) {
        check();
        index(index);
        try {
            DltEvidence.boundedText(reason, 1024, true);
        } catch (DltReplayException invalid) {
            reject(TransactionException.Code.INVALID_CONFIGURATION);
        }
        ReplayRecord record = records.get(index);
        KafkaHeaders headers;
        try {
            headers = new DltOrigin(plan.sourceId(), record.clusterId()).attach(KafkaWire.deadLetterHeaders(record.headers(), plan.group(), record.topic(), record.partition(), record.offset(), reason, attempt));
        } catch (DltReplayException invalid) {
            reject(TransactionException.Code.INVALID_CONFIGURATION);
            return;
        }
        send(topic, partition, record.timestamp() < 0 ? null : record.timestamp(), record.key(), record.payload(), headers);
        dltTopics.add(topic);
        acknowledged.set(index);
    }

    /**
     * 业务作用：向事务执行器交付必须验证历史保留策略的目标，不依赖业务可伪造的头识别DLT。
     * <p>
     * 参数说明：无。
     *
     * @return 本回调通过deadLetter准入的目标快照。
     */
    Set<String> dltTopics() {
        return Set.copyOf(dltTopics);
    }

    /**
     * 业务作用：在回调正常结束后冻结输出并检查整批责任，捕获过的准入错误也不能变成成功。
     * <p>
     * 参数说明：无。
     *
     * @return 全部条件满足时交付私有输出列表，否则拒绝整批。
     */
    List<ProducerRecord<byte[], byte[]>> seal() {
        check();
        closed = true;
        if (rejection != null) throw new TransactionException(rejection);
        if (plan.ackMode() == AckMode.MANUAL && acknowledged.cardinality() != records.size())
            throw new TransactionException(TransactionException.Code.MISSING_ACK);
        return List.copyOf(outputs);
    }

    /**
     * 业务作用：在批次执行结束时封闭上下文写入口，防止保留的引用继续确认或追加输出。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：只封口，不改变 broker 事务状态。
     */
    void close() {
        closed = true;
    }

    /**
     * 业务作用：拒绝超时、失权、取消、异步线程和回调后访问。
     * <p>
     * 参数说明：无。
     * 返回：当前同步调用仍拥有权威时通过。
     */
    private void check() {
        if (Thread.currentThread() != thread || closed)
            throw new TransactionException(TransactionException.Code.CONTEXT_CLOSED);
        authority.run();
        if (rejection != null) throw new TransactionException(rejection);
    }

    /**
     * 业务作用：防止业务确认不属于本批的输入。
     * <p>
     * 返回：合法下标通过，非法值使整批不能提交。
     *
     * @param index 批内序号。
     */
    private void index(int index) {
        if (index < 0 || index >= records.size()) reject(TransactionException.Code.INVALID_CONFIGURATION);
    }

    /**
     * 业务作用：记录不可由业务捕获异常消除的本批准入失败。
     * <p>
     * 返回：抛出固定异常并保留失败。
     *
     * @param code 拒绝原因。
     */
    private void reject(TransactionException.Code code) {
        rejection = code;
        throw new TransactionException(code);
    }
}

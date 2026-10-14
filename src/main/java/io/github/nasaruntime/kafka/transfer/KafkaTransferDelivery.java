package io.github.nasaruntime.kafka.transfer;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.producer.ProducerLane;
import io.github.nasaruntime.kafka.protocol.KafkaHeaders;

import io.github.nasaruntime.kafka.replay.ReplayIdentityProvider;

import io.github.nasaruntime.kafka.replay.ReplayRecord;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.ByteBuffer;

/**
 * 转发与DLT回投共用的目标交付合同，来源持久进度必须等待真实回执。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaTransferDelivery {

    private final TransferPlan.Target target;
    private final TransferPlan.PayloadPolicy policy;
    private final KafkaProxy destination;
    private final ProducerLane lane;
    private final ReplayIdentityProvider identity;
    private final TransferRegistries registries;
    private final KafkaProxy.InternalPublishPermit permit;

    /**
     * 业务作用：绑定目标身份、可靠lane及schema能力，在任务开放前拒绝错误绑定。
     * <p>
     * 返回：无发送副作用的交付能力。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param target      冻结的目标源、集群、通道、topic代次及批准分区合同。
     * @param policy      保留原文字节或使用批准schema ID映射的固定策略。
     * @param destination 提供实际目标发布能力的数据源代理。
     * @param lane        执行目标交付的固定producer通道，不回退到其它lane。
     * @param identity    资源代次目录
     * @param registries  可空的schema绑定。
     * @param permit      目标来源为本任务签发的发布凭据，不能由业务构造。
     */
    public KafkaTransferDelivery(TransferPlan.Target target, TransferPlan.PayloadPolicy policy, KafkaProxy destination, ProducerLane lane, ReplayIdentityProvider identity, TransferRegistries registries, KafkaProxy.InternalPublishPermit permit) {
        this.target = Objects.requireNonNull(target);
        this.policy = Objects.requireNonNull(policy);
        this.destination = Objects.requireNonNull(destination);
        this.lane = Objects.requireNonNull(lane);
        this.identity = Objects.requireNonNull(identity);
        this.registries = registries;
        this.permit = Objects.requireNonNull(permit);
        if (!destination.sourceId().equals(target.sourceId()) || !lane.name().equals(target.lane()))
            throw new TransferException(TransferException.Code.CONTRACT_MISMATCH);
        // 必须使用 acks=all 的 ISR 确认合同，不能以未满足该合同的交付作为来源 checkpoint 的成功条件。
        if (!lane.confirmsAllReplicas()) throw new TransferException(TransferException.Code.UNCONFIRMED_LANE);
        if (policy.kind() == TransferPlan.PayloadKind.CONFLUENT_ID_MAP && (registries == null || !policy.sourceRegistry().equals(registries.sourceName()) || !policy.destinationRegistry().equals(registries.destinationName())))
            throw new TransferException(TransferException.Code.CONTRACT_MISMATCH);
    }

    /**
     * 业务作用：复验目标资源后等待实际交付，隔离原文时不解释schema。
     * <p>
     * 返回：只有真实broker回执才成功，失败不修改checkpoint。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param record     原始输入
     * @param partition  批准目标
     * @param headers    已构造且通过容量检查的有序业务头及来源证据。
     * @param mapPayload true 按计划的正文策略处理，false 保留原文用于隔离，不读取或映射 schema。
     */
    public void send(ReplayRecord record, TransferPlan.Partition partition, KafkaHeaders headers, boolean mapPayload) throws Exception {
        if (!target.partitions().contains(partition))
            throw new TransferException(TransferException.Code.CONTRACT_MISMATCH);
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        byte[] payload = mapPayload ? payload(record.payload(), deadline) : record.payload();
        // 目的身份复验紧邻发送，不能把启动时的旧metadata当作当前路由证据。
        var current = destination.internalAdmin().replayObservation(target.topicGenerations().keySet(), null, remaining(deadline));
        var generations = identity.generations(current.clusterId(), target.topicGenerations().keySet(), remaining(deadline));
        remaining(deadline);
        target.validateCurrent(current, generations);
        var outgoing = new ProducerRecord<>(partition.topic(), partition.partition(), record.timestamp() < 0 ? null : record.timestamp(), record.key(), payload, headers.entries());
        // native接管后的实际回执必须返回，取消等待不能提前释放来源lease。
        awaitActual(lane.sendBytesInternal(outgoing, permit));
    }

    /**
     * 业务作用：仅重写经过双Registry正文校验的批准ID，保留tombstone及完整envelope正文。
     *
     * @param payload  来源值
     * @param deadline 元数据及schema共用预算。
     * @return tombstone 返回 null，OPAQUE 返回传入字节；映射模式返回仅替换 schema ID 的独立副本，未批准或正文不一致时拒绝。
     */
    private byte[] payload(byte[] payload, long deadline) throws Exception {
        if (payload == null || policy.kind() == TransferPlan.PayloadKind.OPAQUE) return payload;
        if (payload.length < 5 || payload[0] != 0)
            throw new TransferException(TransferException.Code.SCHEMA_UNAPPROVED);
        int id = ByteBuffer.wrap(payload, 1, 4).getInt();
        var mapping = policy.schemas().stream().filter(schema -> schema.sourceId() == id).findFirst().orElseThrow(() -> new TransferException(TransferException.Code.SCHEMA_UNAPPROVED));
        var source = registries.source().schemaById(id).get(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS);
        var target = registries.destination().schemaById(mapping.destinationId()).get(remaining(deadline).toNanos(), TimeUnit.NANOSECONDS);
        if (source.id() != id || target.id() != mapping.destinationId() || !source.type().name().equals(mapping.kind().name()) || source.type() != target.type()
                || !mapping.schemaFingerprint().equals(schemaFingerprint(source.schema())) || !mapping.schemaFingerprint().equals(schemaFingerprint(target.schema())))
            throw new TransferException(TransferException.Code.SCHEMA_MISMATCH);
        byte[] output = payload.clone();
        ByteBuffer.wrap(output, 1, 4).putInt(mapping.destinationId());
        return output;
    }

    /**
     * 业务作用：schema ID映射只接受同一UTF-8正文，不把格式相似当作兼容性证明。
     *
     * @param schema Registry返回正文。
     * @return 规范SHA-256十六进制。
     */
    private static String schemaFingerprint(String schema) throws NoSuchAlgorithmException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(schema.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * 业务作用：等待实际交付终态，中断仅改变等待线程状态而不能提前释放任务lease。
     * <p>
     * 返回：原生终态；失败向处理器传播，退出时恢复中断标记。
     *
     * @param future 内部独立回执，不暴露给业务取消。
     */
    private static void awaitActual(CompletableFuture<?> future) throws ExecutionException {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    future.get();
                    return;
                } catch (InterruptedException error) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /**
     * 业务作用：使Registry与目的来源复验共用固定总预算。
     *
     * @param deadline 单调时钟截止时间。
     * @return 正剩余时间；耗尽后拒绝发起后续操作。
     */
    private static Duration remaining(long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) throw new TransferException(TransferException.Code.TIMEOUT);
        return Duration.ofNanos(remaining);
    }
}

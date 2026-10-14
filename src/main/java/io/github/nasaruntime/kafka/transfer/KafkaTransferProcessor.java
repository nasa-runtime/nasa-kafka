package io.github.nasaruntime.kafka.transfer;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.producer.ProducerLane;
import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.replay.ReplayProcessor;
import io.github.nasaruntime.kafka.support.KafkaBrokerErrors;

import io.github.nasaruntime.kafka.exception.KafkaFailure;
import io.github.nasaruntime.kafka.replay.ReplayException;
import io.github.nasaruntime.kafka.schema.SchemaRegistryException;
import io.github.nasaruntime.kafka.replay.ReplayIdentityProvider;

import io.github.nasaruntime.kafka.replay.ReplayRecord;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * 转发的真实同步成功合同；目标所有依赖退出前，来源任务继续持有checkpoint独占权。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaTransferProcessor implements ReplayProcessor {

    private final TransferPlan plan;
    private final KafkaTransferDelivery delivery;
    private final Map<TransferPlan.Partition, TransferPlan.Route> routes;
    private final AtomicReference<TransferException.Code> failure = new AtomicReference<>();
    private final AtomicLong delivered = new AtomicLong();

    /**
     * 业务作用：固定目的能力并在登记任务前拒绝不可靠lane或Registry错绑。
     * <p>
     * 返回：尚未发送的同步处理器。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param plan        冻结合同
     * @param destination 目标来源
     * @param lane        固定物理发布通道
     * @param identity    当前目标目录
     * @param registries  可空的显式schema绑定。
     * @param permit      目标来源为本次登记任务签发的发布凭据。
     */
    public KafkaTransferProcessor(TransferPlan plan, KafkaProxy destination, ProducerLane lane, ReplayIdentityProvider identity, TransferRegistries registries, KafkaProxy.InternalPublishPermit permit) {
        this.plan = Objects.requireNonNull(plan);
        this.delivery = new KafkaTransferDelivery(plan.target(), plan.payloadPolicy(), destination, lane, identity, registries, permit);
        Map<TransferPlan.Partition, TransferPlan.Route> fixed = new HashMap<>();
        plan.routes().forEach(route -> fixed.put(route.source(), route));
        routes = Map.copyOf(fixed);
    }

    /**
     * 业务作用：向Replay提供内置目标成功条件的固定摘要。
     * <p>
     * 参数说明：无。
     *
     * @return 与持久计划完全一致的处理指纹。
     */
    @Override
    public String fingerprint() {
        return plan.replay().pipelineFingerprint();
    }

    /**
     * 业务作用：读取目标首个稳定失败类别。
     * <p>
     * 参数说明：无。
     *
     * @return 没有目标失败时null，不包含原始异常。
     */
    TransferException.Code failure() {
        return failure.get();
    }

    /**
     * 业务作用：读取本实例目标实际确认数。
     * <p>
     * 参数说明：无。
     *
     * @return 真实broker成功回执数量，不能代替来源checkpoint。
     */
    long delivered() {
        return delivered.get();
    }

    /**
     * 业务作用：复验目标资源与schema，构造有界来源证据，等待真实目标交付后才让来源推进。
     * <p>
     * 返回：目标实际确认后正常返回；未知或失败不推进来源checkpoint。
     *
     * @param record 已取得来源独占责任的原文。
     */
    @Override
    public void process(ReplayRecord record) {
        try {
            var route = routes.get(new TransferPlan.Partition(record.topic(), record.partition()));
            if (route == null || !record.clusterId().equals(plan.replay().clusterId()))
                throw new TransferException(TransferException.Code.CONTRACT_MISMATCH);
            KafkaHeaders headers = TransferTrace.forward(plan, record, route.destination());
            delivery.send(record, route.destination(), headers, true);
            delivered.incrementAndGet();
        } catch (Throwable error) {
            TransferException.Code code = classify(error);
            failure.compareAndSet(null, code);
            throw new TransferException(code);
        }
    }

    /**
     * 业务作用：把来源之外的失败转换为脱敏目标类别。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param error 实际处理异常。
     * @return 身份、schema、未知和交付失败保持可区分。
     */
    public static TransferException.Code classify(Throwable error) {
        if (error instanceof ExecutionException && error.getCause() != null) error = error.getCause();
        if (error instanceof TransferException transfer) return transfer.code();
        if (KafkaBrokerErrors.authorization(error)) return TransferException.Code.AUTHORIZATION;
        if (error instanceof TimeoutException) return TransferException.Code.TIMEOUT;
        if (error instanceof KafkaFailure kafka) return switch (kafka.code()) {
            case OUTCOME_UNKNOWN -> TransferException.Code.OUTCOME_UNKNOWN;
            case LIFECYCLE -> TransferException.Code.CLOSED;
            case PRODUCER_QUEUE_FULL, CAPACITY -> TransferException.Code.CAPACITY;
            case NO_SUCH_PRODUCER_LANE -> TransferException.Code.CONTRACT_MISMATCH;
            case CONFIG -> TransferException.Code.INVALID_CONFIGURATION;
            default -> TransferException.Code.DELIVERY_FAILED;
        };
        if (error instanceof ReplayException replay) return switch (replay.code()) {
            case AUTHORIZATION -> TransferException.Code.AUTHORIZATION;
            case TIMEOUT -> TransferException.Code.TIMEOUT;
            case CLOSED -> TransferException.Code.CLOSED;
            case CAPACITY, BUSY -> TransferException.Code.CAPACITY;
            case CONTRACT_MISMATCH -> TransferException.Code.CONTRACT_MISMATCH;
            case INVALID_CONFIGURATION -> TransferException.Code.INVALID_CONFIGURATION;
            default -> TransferException.Code.OBSERVATION_FAILED;
        };
        if (error instanceof SchemaRegistryException registry) return switch (registry.code()) {
            case CLOSED -> TransferException.Code.CLOSED;
            case CAPACITY -> TransferException.Code.CAPACITY;
            case TRANSPORT, REMOTE_STATUS -> TransferException.Code.OBSERVATION_FAILED;
            default -> TransferException.Code.SCHEMA_MISMATCH;
        };
        return TransferException.Code.OBSERVATION_FAILED;
    }
}

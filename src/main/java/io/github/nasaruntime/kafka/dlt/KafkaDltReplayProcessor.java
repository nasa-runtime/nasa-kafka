package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.protocol.KafkaHeaders;
import io.github.nasaruntime.kafka.replay.ReplayProcessor;
import io.github.nasaruntime.kafka.transfer.KafkaTransferDelivery;
import io.github.nasaruntime.kafka.transfer.KafkaTransferProcessor;

import io.github.nasaruntime.kafka.transfer.TransferException;

import io.github.nasaruntime.kafka.replay.ReplayRecord;
import io.github.nasaruntime.kafka.replay.ReplayStatus;
import io.github.nasaruntime.kafka.transfer.TransferPlan;
import io.github.nasaruntime.kafka.transfer.TransferTrace;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * 固定过滤、原文隔离与实际交付共同决定一条DLT输入何时可以推进。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaDltReplayProcessor implements ReplayProcessor {

    private final DltReplayPlan plan;
    private final KafkaTransferDelivery delivery;
    private final Map<TransferPlan.Partition, TransferPlan.Partition> routes;
    private final AtomicReference<DltReplayException.Code> failure = new AtomicReference<>();
    private final AtomicLong forwarded = new AtomicLong(), skipped = new AtomicLong(), quarantined = new AtomicLong();

    /**
     * 业务作用：冻结原业务来源到目标的批准映射，头字段不直接授权目标。
     * <p>
     * 返回：尚未执行的同步处理器。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param plan     共同计划
     * @param delivery 可靠交付能力。
     */
    public KafkaDltReplayProcessor(DltReplayPlan plan, KafkaTransferDelivery delivery) {
        this.plan = plan;
        this.delivery = delivery;
        Map<TransferPlan.Partition, TransferPlan.Partition> fixed = new HashMap<>();
        plan.routes().forEach(route -> fixed.put(route.source(), route.destination()));
        routes = Map.copyOf(fixed);
    }

    /**
     * 业务作用：绑定来源持久进度与完整DLT处理策略。
     * <p>
     * 参数说明：无。
     *
     * @return 冻结计划指纹。
     */
    @Override
    public String fingerprint() {
        return plan.replay().pipelineFingerprint();
    }

    /**
     * 业务作用：处理一条DLT原文，只有明确过滤或真实目标确认允许来源推进。
     * <p>
     * 返回：确定成功才正常返回，失败保留首个稳定类别。
     *
     * @param record 持有来源责任的记录。
     */
    @Override
    public void process(ReplayRecord record) {
        try {
            if (!record.clusterId().equals(plan.replay().clusterId()))
                throw new DltReplayException(DltReplayException.Code.CONTRACT_MISMATCH);
            try {
                forward(record);
            } catch (DltReplayException rejected) {
                // 只承接确定的输入拒绝；交付未知、schema与目录失败不能伪装成已成功隔离。
                if (plan.invalidPolicy() != DltReplayPlan.InvalidPolicy.QUARANTINE || !DltQuarantineTrace.eligible(rejected.code()))
                    throw rejected;
                KafkaHeaders headers = DltQuarantineTrace.append(plan, record, rejected.code());
                delivery.send(record, plan.quarantine(), headers, false);
                quarantined.incrementAndGet();
            }
        } catch (Throwable error) {
            var code = classify(error);
            failure.compareAndSet(null, code);
            throw new DltReplayException(code);
        }
    }

    /**
     * 业务作用：先验证证据再执行显式过滤，选中记录保留事件身份并限制再次回投。
     * <p>
     * 返回：过滤确认或真实目标回执，非法输入不通过过滤隐式丢弃。
     *
     * @param record 实际DLT输入。
     */
    private void forward(ReplayRecord record) throws Exception {
        var evidence = DltEvidence.read(record.headers()).orElseThrow(() -> new DltReplayException(DltReplayException.Code.MISSING_EVIDENCE));
        DltReplayTrace.read(record.headers());
        try {
            TransferTrace.read(record.headers());
        } catch (TransferException rejected) {
            throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
        }
        // 隔离数据只能由业务显式处置，不能自动回流成普通DLT输入。
        if (DltQuarantineTrace.read(record.headers()).isPresent())
            throw new DltReplayException(DltReplayException.Code.HEADER_INVALID);
        var target = routes.get(new TransferPlan.Partition(evidence.originTopic(), evidence.originPartition()));
        if (target == null || !plan.originGroups().isEmpty() && !plan.originGroups().contains(evidence.originGroup())) {
            skipped.incrementAndGet();
            return;
        }
        var headers = DltReplayTrace.forward(plan, record, evidence, target);
        delivery.send(record, target, headers, true);
        forwarded.incrementAndGet();
    }

    /**
     * 业务作用：组合本实例观测与来源持久责任，不将计数当作跨崩溃账本。
     *
     * @param replay 来源即时状态。
     * @return 脱敏DLT执行快照。
     */
    DltReplayStatus status(ReplayStatus replay) {
        return new DltReplayStatus(replay, failure.get(), forwarded.get(), skipped.get(), quarantined.get());
    }

    /**
     * 业务作用：复用目标失败语义并保留DLT专用拒绝。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param error 实际执行错误。
     * @return 稳定脱敏类别。
     */
    public static DltReplayException.Code classify(Throwable error) {
        if (error instanceof DltReplayException dlt) return dlt.code();
        return switch (KafkaTransferProcessor.classify(error)) {
            case LOOP_DETECTED, HOP_LIMIT -> DltReplayException.Code.HEADER_INVALID;
            default -> DltReplayException.Code.valueOf(KafkaTransferProcessor.classify(error).name());
        };
    }
}

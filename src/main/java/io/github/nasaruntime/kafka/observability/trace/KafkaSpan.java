package io.github.nasaruntime.kafka.observability.trace;

import io.github.nasaruntime.kafka.KafkaProxy;

import io.github.nasaruntime.kafka.exception.KafkaFailure;


import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * 仅持有有限观测数据的调用寿命，不持有业务原文或确认 token。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaSpan {

    public final KafkaTraceContext context;
    private final KafkaProxy owner;
    private final String group, topic;
    private final KafkaTraceSpan.Kind kind;
    private final KafkaTraceContext parent;
    private final List<KafkaTraceContext> links;
    private final int omitted;
    private final long start = System.nanoTime(), epoch = Instant.now().toEpochMilli() * 1_000_000;
    private final AtomicBoolean ended = new AtomicBoolean();

    /**
     * 业务作用：在实际执行准入后冻结一个消费或发布 span，批处理根与输入链接分别建模。
     * <p>
     * 返回：尚未完成的有限记录。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param owner      源
     * @param group      消费调用所属的实际组身份。
     * @param topic      该span关联的来源或发布topic。
     * @param kind       路线
     * @param parent     可选父节点
     * @param links      批处理中其它独立父trace的有限链接。
     * @param omitted    未被链接容量保留的有效父关系数量。
     * @param sampleRoot 无父节点时的本地采样决策。
     */
    public KafkaSpan(KafkaProxy owner, String group, String topic, KafkaTraceSpan.Kind kind, KafkaTraceContext parent, List<KafkaTraceContext> links, int omitted, boolean sampleRoot) {
        this.owner = owner;
        this.group = group;
        this.topic = topic;
        this.kind = kind;
        this.parent = parent;
        this.links = List.copyOf(links);
        this.omitted = omitted;
        context = parent == null ? KafkaTraceContext.root(sampleRoot) : parent.child();
    }

    /**
     * 业务作用：只在真实执行或交付回执结束时导出一次，调用方超时不能提前结束在途 span。
     * <p>
     * 返回：已采样记录尝试有界入队；重复完成无副作用。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param outcome 实际观察结局。
     */
    public void finish(KafkaTraceSpan.Outcome outcome) {
        if (ended.compareAndSet(false, true) && context.sampled())
            owner.trace(new KafkaTraceSpan(owner.sourceId(), group, topic, kind, context, parent, links, omitted, epoch, Math.max(0, System.nanoTime() - start), outcome));
    }

    /**
     * 业务作用：从Future包装中保留交付未知类别，防止把原生超时描述为确定未写入。
     * <p>
     * 返回：导出一次本地观察结论。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param failure 发布完成时观察到的异常，null 表示达到 lane 的 acks 完成条件；acks=0 不证明 broker 已接收。
     */
    public void finishDelivery(Throwable failure) {
        while (failure instanceof CompletionException || failure instanceof ExecutionException) {
            if (failure.getCause() == null) break;
            failure = failure.getCause();
        }
        finish(failure == null ? KafkaTraceSpan.Outcome.RETURNED : failure instanceof KafkaFailure f && f.code() == KafkaFailure.Code.OUTCOME_UNKNOWN ? KafkaTraceSpan.Outcome.DELIVERY_UNKNOWN : KafkaTraceSpan.Outcome.FAILED);
    }
}

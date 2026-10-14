package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.consumer.ControlReport.Outcome;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * 一个调用的共享证据；封存与开始副作用使用同一锁，避免返回未执行后迟到派发。
 */
final class KafkaControlProgress {

    private final Map<TopicPartition, Outcome> values = new LinkedHashMap<>();
    private boolean sealed;

    /**
     * 业务作用：为全部请求目标预置未派发证据。
     * <p>
     * 返回：尚未封存的账本。
     *
     * @param targets 完整范围。
     */
    KafkaControlProgress(Set<TopicPartition> targets) {
        targets.forEach(tp -> values.put(tp, Outcome.NOT_APPLIED));
    }

    /**
     * 业务作用：在封存前原子登记可能产生副作用的目标。
     *
     * @param tp       目标
     * @param deadline 本次期限。
     * @return 登记成功才允许操作；期限耗尽或已封存时拒绝。
     */
    synchronized boolean begin(TopicPartition tp, long deadline) {
        if (sealed || System.nanoTime() >= deadline) return false;
        values.put(tp, Outcome.UNKNOWN);
        return true;
    }

    /**
     * 业务作用：记录本次调用取得的目标证据。
     * <p>
     * 返回：未封存时更新，已返回报告保持不变。
     *
     * @param tp      目标
     * @param outcome 终态。
     */
    synchronized void put(TopicPartition tp, Outcome outcome) {
        if (!sealed) values.put(tp, outcome);
    }

    /**
     * 业务作用：只更新尚未开始的目标，避免覆盖已执行证据。
     * <p>
     * 返回：未封存且原状态为 NOT_APPLIED 时更新。
     *
     * @param tp      目标
     * @param outcome 准入结果。
     */
    synchronized void reject(TopicPartition tp, Outcome outcome) {
        if (!sealed) values.replace(tp, Outcome.NOT_APPLIED, outcome);
    }

    /**
     * 业务作用：读取本次调用的单目标证据。
     *
     * @param tp 目标。
     * @return 当前状态。
     */
    synchronized Outcome get(TopicPartition tp) {
        return values.get(tp);
    }

    /**
     * 业务作用：检测需要停止后续派发的证据。
     *
     * @param outcome 目标状态。
     * @return 任一目标匹配时为 true。
     */
    synchronized boolean containsValue(Outcome outcome) {
        return values.containsValue(outcome);
    }

    /**
     * 业务作用：封存返回证据并禁止尚未开始的副作用。
     *
     * @param source 数据源
     * @param group  组。
     * @return 不会被晚到结果改写的独立报告。
     */
    synchronized ControlReport freeze(String source, String group) {
        sealed = true;
        return new ControlReport(source, group, values);
    }
}

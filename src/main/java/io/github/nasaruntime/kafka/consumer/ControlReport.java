package io.github.nasaruntime.kafka.consumer;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicPartition;


/**
 * 截止返回时的逐目标证据；不构成跨 owner 事务或持续分区租约。
 */
public record ControlReport(String sourceId, String group, Map<TopicPartition, Outcome> outcomes) {

    public enum Outcome {
        /**
         * owner 已确认操作完成，之后仍可能发生 rebalance。
         */
        APPLIED,
        /**
         * 未派发，或在副作用前被拒绝/取消。
         */
        NOT_APPLIED,
        /**
         * 已可能生效但未取得终态，必须查询后决定是否重试。
         */
        UNKNOWN,
        /**
         * 缺少唯一归属或冻结代次已失效，操作未执行。
         */
        OWNER_CHANGED
    }

    /**
     * 业务作用：冻结本次调用证据，晚到结果不能改写已经交付的报告。
     * <p>
     * 返回：独立不可变报告。
     *
     * @param sourceId 数据源
     * @param group    组
     * @param outcomes 全部请求目标。
     */
    public ControlReport {
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(group, "group");
        outcomes = Map.copyOf(outcomes);
    }

    /**
     * 业务作用：检查非空请求是否逐目标获得成功证据。
     * <p>
     * 参数说明：无。
     *
     * @return 全部 APPLIED 时为 true，空请求和任一非成功结果为 false。
     */
    public boolean allApplied() {
        return !outcomes.isEmpty() && outcomes.values().stream().allMatch(value -> value == Outcome.APPLIED);
    }

}

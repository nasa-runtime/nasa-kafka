package io.github.nasaruntime.kafka.admin;

import org.apache.kafka.common.TopicPartition;

import java.util.List;

/**
 * 一次有总期限的只读位置观测；分次读取不构成 broker 原子快照。
 *
 * @param sourceId          配置中的源身份
 * @param group             可选提交位点所属组，null 表示未请求
 * @param isolation         末尾与时间查询使用的读取隔离级别
 * @param startedAtMillis   本次调用开始的 Unix 毫秒
 * @param completedAtMillis 报告封存的 Unix 毫秒
 * @param partitions        完整请求结果；框架查询按 topic 和分区排序，直接构造时保留传入顺序
 */
public record OffsetObservation(String sourceId, String group, Isolation isolation,
                                long startedAtMillis, long completedAtMillis, List<Partition> partitions) {

    /**
     * 读取隔离不改变消费组或事务状态。
     */
    public enum Isolation {
        /**
         * 普通日志末尾，可能包含未确认事务之后的 offset。
         */
        READ_UNCOMMITTED,
        /**
         * 已提交读取末尾为 LSO，未完成事务会限制可读范围。
         */
        READ_COMMITTED
    }

    /**
     * 值缺席的稳定原因，不以零位置或空集合掩盖失败。
     */
    public enum Missing {
        /**
         * 调用方没有请求该维度。
         */
        NOT_REQUESTED,
        /**
         * broker 明确没有该组分区的提交历史。
         */
        NO_COMMITTED_OFFSET,
        /**
         * broker 明确没有符合查询时间的记录。
         */
        NO_TIMESTAMP_MATCH,
        /**
         * broker 明确报告 topic 或分区不存在。
         */
        RESOURCE_MISSING,
        /**
         * 当前身份没有查询目标所需的权限。
         */
        AUTHORIZATION,
        /**
         * 共同期限已耗尽或原生请求明确超时。
         */
        TIMEOUT,
        /**
         * 调用线程中断，尚未取得本轮证据。
         */
        INTERRUPTED,
        /**
         * 连接、响应缺项或其它原因使本轮无法取得可信证据。
         */
        UNAVAILABLE,
        /**
         * 同一报告中的边界相互矛盾，不能据此计算恢复位置。
         */
        INCONSISTENT
    }

    /**
     * 一个独立观测值；value 与 missing 恰有一个存在。
     *
     * @param value            非负位置或距离，有效零原样保留
     * @param missing          没有可信值时的原因
     * @param observedAtMillis 本地取得该结果的 Unix 毫秒，不是 broker 提交时间
     */
    public record Value(Long value, Missing missing, long observedAtMillis) {
        /**
         * 业务作用：拒绝同时包含有效值与缺失结论的矛盾观测。
         * <p>
         * 返回：非负且结论唯一的不可变值。
         *
         * @param value            可信的非负位置或距离，缺少可信证据时为null。
         * @param missing          没有可信值的稳定原因，与value恰有一个非null。
         * @param observedAtMillis 本地取得该观测值或缺失结论的Unix毫秒时刻，不是broker提交时间。
         */
        public Value {
            if ((value == null) == (missing == null) || value != null && value < 0)
                throw new IllegalArgumentException("位置观测必须包含非负值或唯一缺失原因");
        }
    }

    /**
     * 单分区位置与提交距离，所有字段保留各自缺失原因。
     *
     * @param partition       原请求分区
     * @param beginning       当前保留起点
     * @param end             所选隔离级别的可读末尾
     * @param committed       组确认提交的 next offset
     * @param timestampOffset 所请求时间的首个匹配位置
     * @param lag             max(0, end - committed)，不是业务条数
     */
    public record Partition(TopicPartition partition, Value beginning, Value end, Value committed,
                            Value timestampOffset, Value lag) {}

    /**
     * 业务作用：封存一次只读报告，防止晚到查询或调用方集合修改已交付证据。
     * <p>
     * 返回：独立不可变结果集合。
     *
     * @param sourceId          位置观测所属的配置源身份。
     * @param group             提交位点所属的可选消费组，null表示未请求该维度。
     * @param isolation         末尾与时间位置查询采用的读取隔离级别。
     * @param startedAtMillis   本次只读观测开始的Unix毫秒时刻。
     * @param completedAtMillis 封存本次只读报告的Unix毫秒时刻。
     * @param partitions        完整请求结果；框架查询按 topic 和分区排序，此构造器仅按传入顺序冻结列表。
     */
    public OffsetObservation {
        partitions = List.copyOf(partitions);
    }
}

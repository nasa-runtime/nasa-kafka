package io.github.nasaruntime.kafka.observability.metrics;

import java.util.*;

/**
 * 固定逻辑指标目录；各语言按实际执行路线发出对应样本，目录本身不代表路线已经发生。
 */
public final class MetricCatalog {

    public enum Kind {COUNTER, GAUGE}

    /**
     * 不可变的类型、单位与完整标签合同。
     */
    public record Definition(String name, Kind kind, String unit, String description, List<String> labels) {
        /**
         * 业务作用：冻结固定指标的标签声明。
         * <p>
         * 返回：不受调用方列表修改影响的定义。
         *
         * @param name        逻辑名
         * @param kind        类型
         * @param unit        单位
         * @param description 语义
         * @param labels      有序标签。
         */
        public Definition {
            labels = List.copyOf(labels);
        }
    }

    private static final Map<String, Definition> DEFINITIONS = create();

    /**
     * 业务作用：禁止实例化静态指标目录。
     * <p>
     * 参数说明：无。
     * 返回：无实例。
     */
    private MetricCatalog() {}

    /**
     * 业务作用：查找固定指标定义。
     *
     * @param name 逻辑名称。
     * @return 未知名称为空，不创建动态 family。
     */
    public static Optional<Definition> find(String name) {
        return Optional.ofNullable(DEFINITIONS.get(name));
    }

    /**
     * 业务作用：为后端和接入方提供不可变目录。
     * <p>
     * 参数说明：无。
     *
     * @return 按名称排序的定义。
     */
    public static Collection<Definition> definitions() {
        return DEFINITIONS.values();
    }

    /**
     * 业务作用：建立固定类型、单位与标签集合，所有序列必须带 source。
     * <p>
     * 参数说明：无。
     *
     * @return 不可修改的目录。
     */
    private static Map<String, Definition> create() {
        var result = new TreeMap<String, Definition>();
        add(result, "group_ready_total", Kind.COUNTER, "", "调用方等待组 Ready 成功的次数", "group");
        add(result, "group_ready_timeout_total", Kind.COUNTER, "", "调用方等待组 Ready 超时的次数", "group");
        add(result, "group_ready", Kind.GAUGE, "", "全部 owner 均处于 Running 且持有分配代次", "group");
        add(result, "group_ready_wait_millis", Kind.GAUGE, "milliseconds", "最近一次成功等待 Ready 的毫秒数", "group");
        add(result, "published_total", Kind.COUNTER, "", "native 回执确认成功的消息数", "lane", "topic");
        add(result, "publish_failed_total", Kind.COUNTER, "", "native 回执失败或观察终态未知的消息数", "lane", "topic");
        add(result, "producer_lane_queue_utilization", Kind.GAUGE, "percent", "lane 准入占用百分比", "lane");
        add(result, "producer_lane_in_flight", Kind.GAUGE, "", "lane 尚未释放的交付与回执责任数", "lane");
        add(result, "assigned_partitions", Kind.GAUGE, "", "全组当前拥有的分区数", "group");
        add(result, "paused_partitions", Kind.GAUGE, "", "全组当前至少有一种暂停原因的分区数", "group");
        add(result, "active_handlers", Kind.GAUGE, "", "实际尚未退出的业务调用数", "group");
        add(result, "key_active_handlers", Kind.GAUGE, "", "按键模式实际尚未退出的业务调用数", "group");
        add(result, "key_pending_records", Kind.GAUGE, "", "按键模式仍保留原文的排队、运行、重试与完成证据记录数", "group");
        add(result, "key_pending_bytes", Kind.GAUGE, "bytes", "按键模式仍保留原文的输入字节及八倍副本预留", "group");
        add(result, "owner_session_started_total", Kind.COUNTER, "", "已创建的原生消费会话数", "group");
        add(result, "owner_session_restarted_total", Kind.COUNTER, "", "初次启动后创建的原生消费会话数", "group");
        add(result, "commit_attempted_total", Kind.COUNTER, "", "提交安全前缀的原生调用次数", "group");
        add(result, "commit_succeeded_total", Kind.COUNTER, "", "broker 已确认的安全前缀提交次数", "group");
        add(result, "commit_failed_total", Kind.COUNTER, "", "未取得成功确认的安全前缀提交次数", "group");
        add(result, "manual_acked_total", Kind.COUNTER, "", "有效 Manual Ack 连续前缀记录数", "group", "consumer_id");
        add(result, "manual_ack_missing_total", Kind.COUNTER, "", "Manual Ack 连续前缀后保留重投的记录数", "group", "consumer_id");
        add(result, "manual_ack_after_gap_total", Kind.COUNTER, "", "存在前缀空洞时收到的后继 Manual Ack 数", "group", "consumer_id");
        add(result, "consumed_total", Kind.COUNTER, "", "有效业务确认连续前缀记录数，尚不代表 broker 已提交", "group", "consumer_id");
        add(result, "handler_dead_letter_total", Kind.COUNTER, "", "业务调用主动请求 DLT 的次数", "group", "consumer_id");
        add(result, "handler_deferred_total", Kind.COUNTER, "", "业务调用请求延后且不消耗毒消息预算的次数", "group", "consumer_id");
        add(result, "handler_failed_total", Kind.COUNTER, "", "业务调用普通失败或致命失败次数", "group", "consumer_id");
        add(result, "decode_failed_total", Kind.COUNTER, "", "路由有效但载荷解码失败的记录次数", "group", "consumer_id");
        add(result, "retried_total", Kind.COUNTER, "", "安排重试的 typed 输入记录次数", "group", "topic");
        add(result, "dlt_queue_jobs", Kind.GAUGE, "", "源级 DLT pipeline 已准入并保留的记录数");
        add(result, "dlt_queue_bytes", Kind.GAUGE, "bytes", "源级 DLT dispatcher 持有的信封字节数");
        add(result, "dlt_total", Kind.COUNTER, "", "取得有效 DLT broker 确认的来源记录数", "group", "topic");
        add(result, "dlt_failed_total", Kind.COUNTER, "", "DLT 责任以失败终结的来源记录数", "group", "topic");
        add(result, "dlt_attempt_failed_total", Kind.COUNTER, "", "DLT 查询或交付失败尝试涉及的来源记录数，重试可重复计数", "group", "topic");
        add(result, "passthrough_calls_total", Kind.COUNTER, "", "已退出的借用回调次数，含错误返回", "group");
        add(result, "passthrough_duration_micros_total", Kind.COUNTER, "microseconds", "借用回调累计实际执行微秒数", "group");
        add(result, "registry_consumers", Kind.GAUGE, "", "冻结路由注册表中的路由数", "client");
        add(result, "registry_groups", Kind.GAUGE, "", "源注册表中的受管消费组数", "client");
        add(result, "publish_admitted_total", Kind.COUNTER, "", "成功进入 native 队列的消息数", "lane", "topic");
        add(result, "publish_admission_rejected_total", Kind.COUNTER, "", "发布容量或 native 准入拒绝次数", "lane", "reason");
        add(result, "publish_wait_unknown_total", Kind.COUNTER, "", "调用方等待结束时交付仍未知的次数", "lane");
        add(result, "dlt_in_flight", Kind.GAUGE, "", "组内实际尚未终结的 DLT 调用数", "group");
        add(result, "retained_bytes", Kind.GAUGE, "bytes", "组内输入原文字节的八倍副本预留", "group");
        add(result, "retained_records", Kind.GAUGE, "", "组内输入凭证持有的未确认记录数", "group");
        add(result, "publish_delivery_unknown_total", Kind.COUNTER, "", "原生交付在本地终结但远端结果仍不明的记录数", "lane");
        add(result, "control_target_total", Kind.COUNTER, "", "已交付控制报告中的逐目标结论数", "group", "outcome");
        add(result, "rebalance_assigned_total", Kind.COUNTER, "", "owner接收到分配完成回调的次数，含首次加入", "group");
        add(result, "source_stop_requested", Kind.GAUGE, "", "是否已永久请求停止消费");
        add(result, "source_resources_closed", Kind.GAUGE, "", "完整源关闭协议是否确认全部资源退出");
        add(result, "close_active_owners", Kind.GAUGE, "", "关闭报告中尚未退出的普通owner数");
        add(result, "close_active_handlers", Kind.GAUGE, "", "关闭报告中尚未退出的业务调用数");
        add(result, "close_retained_records", Kind.GAUGE, "", "关闭报告中尚未释放的输入记录数");
        add(result, "close_released_unverified_records", Kind.GAUGE, "", "停止期间释放但尚未核实可恢复的累计观察次数");
        add(result, "replay_active_tasks", Kind.GAUGE, "", "尚未退出的有限回放任务数");
        add(result, "transfer_active_dependencies", Kind.GAUGE, "", "目标源仍持有的转发依赖数");
        add(result, "transaction_active_tasks", Kind.GAUGE, "", "尚未归还目录的事务owner数");
        add(result, "dlt_active_tasks", Kind.GAUGE, "", "包含旧分配在内的实际DLT任务数");
        add(result, "admin_active_borrows", Kind.GAUGE, "", "Admin额外借用或短连查询责任数");
        add(result, "metrics_queued", Kind.GAUGE, "", "有界指标出口中的排队事件数");
        add(result, "metrics_active", Kind.GAUGE, "", "正在执行的指标用户回调数");
        add(result, "broker_lag_offsets", Kind.GAUGE, "", "显式查询范围的提交距离之和，任一缺项为-1，不是业务条数", "group", "isolation");
        add(result, "broker_lag_partitions", Kind.GAUGE, "", "最近显式lag查询覆盖的分区数", "group", "isolation");
        add(result, "broker_lag_missing_partitions", Kind.GAUGE, "", "最近显式lag查询缺少有效距离的分区数", "group", "isolation");
        add(result, "broker_lag_observed_at_millis", Kind.GAUGE, "milliseconds", "最近显式lag查询报告封存的Unix毫秒", "group", "isolation");
        add(result, "trace_invalid_total", Kind.COUNTER, "", "调用准入时丢弃非法或重复外来trace的记录次数", "group", "reason");
        return Collections.unmodifiableMap(result);
    }

    /**
     * 业务作用：在固定定义中加入不可省略的源标签。
     * <p>
     * 返回：登记一项定义；重复名称拒绝初始化。
     *
     * @param target      目录
     * @param name        指标的公开稳定名称。
     * @param kind        指标采用累计计数还是瞬时观测的类别。
     * @param unit        该指标数值的公开计量单位。
     * @param description 指标业务含义及使用边界的文本说明。
     * @param labels      其它身份。
     */
    private static void add(Map<String, Definition> target, String name, Kind kind, String unit, String description, String... labels) {
        var names = new ArrayList<String>();
        names.add("source");
        names.addAll(List.of(labels));
        if (target.put(name, new Definition(name, kind, unit, description, names)) != null)
            throw new IllegalStateException("重复指标定义: " + name);
    }
}

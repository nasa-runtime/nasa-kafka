package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.observability.trace.KafkaTraceContext;

import io.github.nasaruntime.kafka.KafkaProxy;

import java.util.List;


/**
 * 可跨 executor 显式传递的调用身份；确认仍受 token 的实际生命周期约束。
 *
 * @param sourceId          实际接收消息的命名源，不采信消息自报的源名
 * @param group             当前 owner 使用的消费组
 * @param proxy             所属源的操作入口，生命周期由资源所有者管理
 * @param assignmentEpoch   本地分配代次，跨进程不能作为排他租约
 * @param deliveryAttempt   当前内存责任的实际业务投递次数，包括延期重投
 * @param retryAttempt      当前普通失败预算次数，延期不消耗该预算
 * @param tombstone         该记录原始 value 是否为 Kafka null；批量回调的调用级上下文取首条，逐条身份使用 record.context
 * @param traceContext      调用级派生上下文；逐记录context保存该条入站父上下文，均只用于观测
 * @param traceLinks        批调用的有限独立父链接，不把首条消息作为整批父节点
 * @param traceLinksOmitted 超出链接容量后未保留的有效输入数
 */
public record ConsumeContext(String sourceId, String group, KafkaProxy proxy, long assignmentEpoch,
                             int deliveryAttempt, int retryAttempt, boolean tombstone,
                             KafkaTraceContext traceContext, List<KafkaTraceContext> traceLinks,
                             int traceLinksOmitted) {

    /**
     * 业务作用：冻结可跨 executor 显式传递的链路身份，链接只用于观测而不扩展确认权限。
     * <p>
     * 返回：不可变上下文，最多保留128个链接。
     *
     * @param sourceId          实际接收消息的命名源身份，不采信消息自报的源名。
     * @param group             当前消费owner使用的实际消费组。
     * @param proxy             所属数据源的操作入口，生命周期仍由资源所有者管理。
     * @param assignmentEpoch   本地分配代次，不是跨进程排他租约。
     * @param deliveryAttempt   当前内存责任的业务投递次数，包括延期重投。
     * @param retryAttempt      普通失败预算已使用的尝试序号，延期不消耗该预算。
     * @param tombstone         原始Kafka value是否为null；批调用取首条，逐条状态从记录上下文读取。
     * @param traceContext      调用级派生或逐记录入站trace上下文，仅用于观测。
     * @param traceLinks        批处理的独立父trace链接，最多128项，不扩展确认权限。
     * @param traceLinksOmitted 超过链接容量而未保留的有效输入链接数，必须非负。
     */
    public ConsumeContext {
        traceLinks = List.copyOf(traceLinks);
        if (traceLinks.size() > 128 || traceLinksOmitted < 0)
            throw new IllegalArgumentException("trace links 边界无效");
    }

    /**
     * 业务作用：构造没有截断链接的明确调用上下文。
     * <p>
     * 返回：遗漏数为零的不可变上下文。
     *
     * @param sourceId        实际接收消息的命名源身份，不采信消息自报的源名。
     * @param group           当前消费owner使用的实际消费组。
     * @param proxy           所属数据源的操作入口，生命周期仍由资源所有者管理。
     * @param assignmentEpoch 本地分配代次，不是跨进程排他租约。
     * @param deliveryAttempt 当前内存责任的业务投递次数，包括延期重投。
     * @param retryAttempt    普通失败预算已使用的尝试序号，延期不消耗该预算。
     * @param tombstone       原始Kafka value是否为null；批调用取首条，逐条状态从记录上下文读取。
     * @param traceContext    调用级派生或逐记录入站trace上下文，仅用于观测。
     * @param traceLinks      批处理的独立父trace链接，最多128项，不扩展确认权限。
     */
    public ConsumeContext(String sourceId, String group, KafkaProxy proxy, long assignmentEpoch, int deliveryAttempt, int retryAttempt, boolean tombstone, KafkaTraceContext traceContext, List<KafkaTraceContext> traceLinks) {
        this(sourceId, group, proxy, assignmentEpoch, deliveryAttempt, retryAttempt, tombstone, traceContext, traceLinks, 0);
    }

    /**
     * 业务作用：保留不携带链路信息的已有上下文构造入口。
     * <p>
     * 返回：没有外来 trace 的上下文。
     *
     * @param sourceId        实际接收消息的命名源身份，不采信消息自报的源名。
     * @param group           当前消费owner使用的实际消费组。
     * @param proxy           所属数据源的操作入口，生命周期仍由资源所有者管理。
     * @param assignmentEpoch 本地分配代次，不是跨进程排他租约。
     * @param deliveryAttempt 当前内存责任的业务投递次数，包括延期重投。
     * @param retryAttempt    普通失败预算已使用的尝试序号，延期不消耗该预算。
     * @param tombstone       原始Kafka value是否为null；批调用取首条，逐条状态从记录上下文读取。
     */
    public ConsumeContext(String sourceId, String group, KafkaProxy proxy, long assignmentEpoch, int deliveryAttempt, int retryAttempt, boolean tombstone) {
        this(sourceId, group, proxy, assignmentEpoch, deliveryAttempt, retryAttempt, tombstone, null, List.of());
    }

    /**
     * 业务作用：兼容没有原始空值观测的构造形式。
     * <p>
     * 返回：未声明 tombstone 的上下文，运行时记录使用包含原始身份的构造形式。
     *
     * @param sourceId        实际接收消息的命名源身份，不采信消息自报的源名。
     * @param group           当前消费owner使用的实际消费组。
     * @param proxy           所属数据源的操作入口，生命周期仍由资源所有者管理。
     * @param assignmentEpoch 本地分配代次，不是跨进程排他租约。
     * @param deliveryAttempt 当前内存责任的业务投递次数，包括延期重投。
     * @param retryAttempt    普通失败预算已使用的尝试序号，延期不消耗该预算。
     */
    public ConsumeContext(String sourceId, String group, KafkaProxy proxy, long assignmentEpoch, int deliveryAttempt, int retryAttempt) {
        this(sourceId, group, proxy, assignmentEpoch, deliveryAttempt, retryAttempt, false);
    }

    /**
     * 业务作用：兼容只记录一种尝试序号的构造形式。
     * <p>
     * 返回：两种次数相同的上下文。
     *
     * @param sourceId        实际接收消息的命名源身份，不采信消息自报的源名。
     * @param group           当前消费owner使用的实际消费组。
     * @param proxy           所属数据源的操作入口，生命周期仍由资源所有者管理。
     * @param assignmentEpoch 本地分配代次，不是跨进程排他租约。
     * @param deliveryAttempt 当前内存责任的业务投递次数，包括延期重投。
     */
    public ConsumeContext(String sourceId, String group, KafkaProxy proxy, long assignmentEpoch, int deliveryAttempt) {
        this(sourceId, group, proxy, assignmentEpoch, deliveryAttempt, deliveryAttempt);
    }

    /**
     * 业务作用：读取同步调用栈的来源。
     * <p>
     * 参数说明：无。
     *
     * @return 当前上下文；异步任务应使用显式参数。
     */
    public static ConsumeContext current() {
        return KafkaConsumeContexts.current();
    }

}

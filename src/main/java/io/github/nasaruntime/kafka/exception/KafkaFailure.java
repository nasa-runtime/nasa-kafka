package io.github.nasaruntime.kafka.exception;

import io.github.nasaruntime.kafka.consumer.ReadyStatus;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Kafka 操作的稳定失败分类；原生异常只作为原因，不承担跨语言合同。
 */
public final class KafkaFailure extends RuntimeException {

    public enum Code {
        /**
         * 本地能力视图或 broker ACL 明确拒绝操作；只有所需两层权限都成立才能执行。
         */
        AUTHORIZATION,
        /**
         * 声明不满足配置或跨字段约束，尚未按该声明开放工作。
         */
        CONFIG,
        /**
         * 当前 broker topic 规格与显式业务合同不一致，不自动修改资源。
         */
        TOPIC_CONTRACT,
        /**
         * 当前生命周期或资源权威不允许该操作。
         */
        LIFECYCLE,
        /**
         * 显式源名未登记，不允许回退到默认源。
         */
        NO_SUCH_SOURCE,
        /**
         * 当前源没有对应消费组。
         */
        NO_SUCH_GROUP,
        /**
         * 发布通道未在源配置中声明。
         */
        NO_SUCH_PRODUCER_LANE,
        /**
         * 发送准入没有取得完整数量或字节额度，该次请求未交给原生 producer。
         */
        PRODUCER_QUEUE_FULL,
        /**
         * 编码、准入或底层发送报告失败；上层重发仍须考虑业务幂等。
         */
        PUBLISH,
        /**
         * 等待超时或被中断，不能由此推导外部副作用是否发生。
         */
        OUTCOME_UNKNOWN,
        /**
         * 就绪等待期限耗尽；未产生业务写入，诊断保留本次未满足的要求与健康。
         */
        READY_TIMEOUT,
        /**
         * 控制命令在执行前被拒绝或成功取消。
         */
        CONTROL_NOT_APPLIED,
        /**
         * 分区不再属于冻结的 owner 代次，旧意图确定未执行。
         */
        CONTROL_OWNER_CHANGED,
        /**
         * 控制操作已经开始，调用方期限内未取得终态。
         */
        CONTROL_OUTCOME_UNKNOWN,
        /**
         * 确认调用已结束、超时或分区已失权。
         */
        ACK_EXPIRED,
        /**
         * 非 Manual listener 请求了显式确认。
         */
        ACK_NOT_MANUAL,
        /**
         * 业务外部门禁暂不可用，需要重投但不消耗普通失败预算。
         */
        HANDLER_DEFERRED,
        /**
         * 业务请求转交死信，仍必须服从 DLT 可靠性门禁。
         */
        HANDLER_DEAD_LETTER,
        /**
         * 输入不符合已冻结的路由、类型或线路合同。
         */
        INVALID_RECORD,
        /**
         * 操作超过声明的资源边界。
         */
        CAPACITY,
        /**
         * 当前构建或受管运行模式不具备所请求能力。
         */
        UNSUPPORTED_CAPABILITY,
        /**
         * 管理客户端明确报告操作失败，不可解释为目标不存在。
         */
        ADMIN
    }

    private final Code code;
    private final Map<String, ReadyStatus> readiness;

    /**
     * 业务作用：构造不包含敏感配置的业务失败。
     * <p>
     * 返回：尚未抛出的异常。
     *
     * @param code    稳定分类
     * @param message 公开原因。
     */
    public KafkaFailure(Code code, String message) {
        this(code, message, null);
    }

    /**
     * 业务作用：保留失败类别及本地原因。
     * <p>
     * 返回：异常实例。
     *
     * @param code    分类
     * @param message 公开原因
     * @param cause   原生原因。
     */
    public KafkaFailure(Code code, String message, Throwable cause) {
        this(code, message, cause, Map.of());
    }

    /**
     * 业务作用：携带一次 Ready 等待的未满足项，调用方无需解析错误文本。
     * <p>
     * 返回：冻结诊断的异常。
     *
     * @param code      调用方可判断的稳定失败类别。
     * @param message   不包含凭据或业务载荷的失败说明。
     * @param cause     底层失败原因，可为null，不替代公开错误分类。
     * @param readiness 按组名索引的诊断。
     */
    public KafkaFailure(Code code, String message, Throwable cause, Map<String, ReadyStatus> readiness) {
        super(message, cause);
        this.code = code;
        this.readiness = Collections.unmodifiableMap(new TreeMap<>(readiness));
    }

    /**
     * 业务作用：供调用方按稳定类别选择恢复动作。
     * <p>
     * 参数说明：无。
     *
     * @return 失败分类。
     */
    public Code code() {
        return code;
    }

    /**
     * 业务作用：读取 Ready 失败当次的结构化证据。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变未满足项；其它失败返回空集合。
     */
    public Map<String, ReadyStatus> readiness() {
        return readiness;
    }
}

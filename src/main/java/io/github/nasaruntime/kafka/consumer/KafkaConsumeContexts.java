package io.github.nasaruntime.kafka.consumer;


/**
 * 同步调用栈的来源观测；写入与恢复仅由消费运行时执行。
 */
public final class KafkaConsumeContexts {

    private static final ThreadLocal<ConsumeContext> CURRENT = new ThreadLocal<>();

    /**
     * 业务作用：禁止实例化线程上下文容器。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：仅限制实例构造，不建立连接、启动线程或创建业务状态。
     */
    private KafkaConsumeContexts() {}

    /**
     * 业务作用：读取当前同步消费调用的来源身份。
     * <p>
     * 参数说明：无。
     *
     * @return 当前上下文，未进入调用时为null；不授予确认权限。
     */
    public static ConsumeContext current() {
        return CURRENT.get();
    }

    /**
     * 业务作用：进入调用时保留外层来源，防止嵌套消费串源。
     *
     * @param context 新来源。
     * @return 先前上下文。
     */
    static ConsumeContext enter(ConsumeContext context) {
        ConsumeContext old = CURRENT.get();
        CURRENT.set(context);
        return old;
    }

    /**
     * 业务作用：退出调用时恢复外层来源，避免线程复用残留身份。
     * <p>
     * 返回：空上下文移除线程引用，其余恢复原身份。
     *
     * @param previous 进入前的上下文。
     */
    static void restore(ConsumeContext previous) {
        if (previous == null) CURRENT.remove();
        else CURRENT.set(previous);
    }
}

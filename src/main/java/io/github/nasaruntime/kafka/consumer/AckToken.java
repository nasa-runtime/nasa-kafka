package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import java.util.function.BooleanSupplier;

/**
 * invocation 内的临时确认；业务不能构造任意 offset 的确认权威。
 */
public final class AckToken {

    private final AckMode mode;
    private BooleanSupplier current;
    private boolean open = true;
    private boolean acknowledged;

    /**
     * 业务作用：绑定当前处理调用的确认权限。
     * <p>
     * 返回：未确认 token。
     *
     * @param mode    当前路由的确认模式，只有 MANUAL 允许业务显式确认。
     * @param current 复验本次调用仍持有分区与投递代次权威的判断函数。
     */
    AckToken(AckMode mode, BooleanSupplier current) {
        this.mode = mode;
        this.current = current;
    }

    /**
     * 业务作用：记录本次调用的临时确认。
     * <p>
     * 参数说明：无。
     * 返回：有效重复确认幂等，过期或非 Manual 时抛出分类异常。
     */
    public synchronized void ack() {
        // 调用结束或分区失权后的确认不能影响新的消费者会话。
        if (!open || !current.getAsBoolean())
            throw new KafkaFailure(KafkaFailure.Code.ACK_EXPIRED, "ack invocation 已结束或失权");
        if (mode != AckMode.MANUAL)
            throw new KafkaFailure(KafkaFailure.Code.ACK_NOT_MANUAL, "当前 listener 不是 Manual");
        acknowledged = true;
    }

    /**
     * 业务作用：封存临时确认并与最终业务结果合并，解除对调用及原文账本的引用。
     *
     * @param succeeded 整个调用是否成功。
     * @return 首次有效封存时本条是否安全完成，之后始终返回 false。
     */
    synchronized boolean finish(boolean succeeded) {
        boolean safe = open && current.getAsBoolean() && succeeded && (mode == AckMode.AUTO || acknowledged);
        open = false;
        // 业务可以长期保留 KafkaRecord 或 token；失效权威不能因此继续持有已归还额度的原文。
        current = null;
        return safe;
    }
}

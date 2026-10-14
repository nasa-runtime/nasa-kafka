package io.github.nasaruntime.kafka.consumer;

import java.util.Objects;

/**
 * 单分区控制意图；定位值不能通过空集合隐式扩展为全部分区。
 */
public record PartitionControl(Kind kind, long value) {

    public enum Kind {
        /**
         * 增加用户暂停原因。
         */
        PAUSE,
        /**
         * 仅解除用户暂停原因。
         */
        RESUME,
        /**
         * 定位到绝对 offset。
         */
        SEEK,
        /**
         * 定位到保留起点。
         */
        BEGINNING,
        /**
         * 定位到可读末尾。
         */
        END,
        /**
         * 定位到首条不早于指定 Unix 毫秒的记录，无匹配时取末尾。
         */
        TIMESTAMP,
        /**
         * 定位到 broker 已确认位置，缺少历史时拒绝。
         */
        COMMITTED
    }

    /**
     * 业务作用：在控制派发前拒绝非法定位声明。
     * <p>
     * 返回：不可变意图；非法声明抛出参数异常。
     *
     * @param kind  操作
     * @param value 非负 offset/时间，其它操作必须为零。
     */
    public PartitionControl {
        Objects.requireNonNull(kind, "kind");
        if (value < 0 || (kind != Kind.SEEK && kind != Kind.TIMESTAMP && value != 0))
            throw new IllegalArgumentException("控制定位值必须非负，无参数操作必须为零");
    }

    /**
     * 业务作用：声明不带定位值的控制。
     * <p>
     * 返回：值为零的不可变意图。
     *
     * @param kind 操作。
     */
    public PartitionControl(Kind kind) {
        this(kind, 0);
    }
}

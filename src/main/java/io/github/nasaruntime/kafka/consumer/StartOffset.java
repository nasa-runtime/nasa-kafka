package io.github.nasaruntime.kafka.consumer;


/**
 * 类作用 - 手动分区消费 assign 模式的起始位点策略。
 */
public final class StartOffset {

    private final StartOffsetKind kind;
    private final long value;

    /**
     * 业务作用：收敛起始位点的构造入口，使实例只能经由语义明确的静态工厂产生。
     * 私有化构造是为了避免调用方传出「类型与值不匹配」的组合，
     * 例如声明按时间定位却传了一个 offset 值。
     * 返回：尚未向 broker 求值的起点声明。
     *
     * @param kind  起始位点类型
     * @param value 位点值；仅时间戳与显式位点两种类型会使用，其余类型传 0
     */
    private StartOffset(StartOffsetKind kind, long value) {
        this.kind = kind;
        this.value = value;
    }

    /**
     * 业务作用：声明从分区当前最早可用位点开始消费，用于回放该分区尚未被 Kafka 清理掉的全部历史。
     * 起点受 broker 的保留策略约束，早于保留期的数据已不存在，并非真正的「第一条」。
     * <p>
     * 参数说明：无。
     *
     * @return 最早位点策略。
     */
    public static StartOffset earliest() {
        return new StartOffset(StartOffsetKind.EARLIEST, 0L);
    }

    /**
     * 业务作用：声明在 assign 会话建立时定位到该 consumer 隔离级别下的末尾，跳过当时可读历史。
     * 不删除日志，也不立即提交组位点；保留范围内仍可通过 seek 回读。
     * <p>
     * 参数说明：无。
     *
     * @return 末尾位点策略。
     */
    public static StartOffset latest() {
        return new StartOffset(StartOffsetKind.LATEST, 0L);
    }

    /**
     * 业务作用：声明按 broker 保存的消息时间戳查找 assign 起点。
     * 时间戳可能是生产者声明的 CreateTime，也可能是 broker 改写的 LogAppendTime；不是固定按业务事件时间定位。
     *
     * @param epochMs 起始时刻，epoch 毫秒
     * @return 时间戳位点策略。
     */
    public static StartOffset timestamp(long epochMs) {
        return new StartOffset(StartOffsetKind.TIMESTAMP, epochMs);
    }

    /**
     * 业务作用：声明 assign 消费的明确起点；工厂只保存请求，不查询 broker。
     * owner 建立会话时核验当前保留范围，越界以 CONFIG 拒绝启动，不回退到 auto.offset.reset。
     *
     * @param offset 分区内的偏移量
     * @return 显式位点策略。
     */
    public static StartOffset offset(long offset) {
        return new StartOffset(StartOffsetKind.EXPLICIT, offset);
    }

    /**
     * 业务作用：声明沿用该 group 已提交的位点，即从上次停下的地方接着消费。
     * 该 group 在此分区没有提交历史时，实际起点由 consumer 的越界重置配置决定。
     * <p>
     * 参数说明：无。
     *
     * @return 已提交位点策略。
     */
    public static StartOffset committed() {
        return new StartOffset(StartOffsetKind.COMMITTED, 0L);
    }

    /**
     * 业务作用：暴露位点类型，供 assign 流程分派到对应的定位方式。
     * <p>
     * 参数说明：无。
     *
     * @return 本策略的位点类型。
     */
    public StartOffsetKind kind() {
        return kind;
    }

    /**
     * 业务作用：暴露位点值，供时间戳与显式位点两种定位方式取用。
     * <p>
     * 参数说明：无。
     *
     * @return 位点值；其余位点类型下恒为 0，不具业务含义。
     */
    public long value() {
        return value;
    }

    /**
     * 业务作用：产出便于排查的可读描述，日志中据此确认消费实际从哪个起点开始。
     * 仅时间戳与显式位点两种类型带出数值，其余类型的数值无意义故不显示。
     * <p>
     * 参数说明：无。
     *
     * @return 位点策略的可读描述。
     */
    @Override
    public String toString() {
        return switch (kind) {
            case TIMESTAMP -> "StartOffset(timestamp=" + value + ")";
            case EXPLICIT -> "StartOffset(offset=" + value + ")";
            default -> "StartOffset(" + kind + ")";
        };
    }
}

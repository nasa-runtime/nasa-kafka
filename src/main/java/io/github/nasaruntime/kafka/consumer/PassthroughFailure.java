package io.github.nasaruntime.kafka.consumer;

import java.util.Objects;

/**
 * 透传失败的稳定分类；诊断文本只保留调用方提供的安全内容，不接受消息正文或凭据。
 */
public final class PassthroughFailure extends RuntimeException {

    /**
     * 原因唯一决定动作，业务不能为同一原因任意选择确认语义。
     */
    public enum Reason {
        /**
         * 本地队列暂时不能满足准入要求。
         */
        SOCKET_ENQUEUE,
        /**
         * 临时依赖不可用。
         */
        TEMPORARY_DEPENDENCY,
        /**
         * 路由内容不满足声明。
         */
        MALFORMED_ROUTE,
        /**
         * 当前业务不接受 tombstone。
         */
        MISSING_PAYLOAD,
        /**
         * 消息模式不受目标协议支持。
         */
        UNSUPPORTED_MESSAGE_MODE,
        /**
         * 目标展开超限，业务显式要求转死信。
         */
        DEAD_LETTER_ROUTE_EXPANSION,
        /**
         * 目标展开超限，需要保留输入并停止该分区。
         */
        HALT_ROUTE_EXPANSION,
        /**
         * 最终帧超过目标允许上限。
         */
        FRAME_TOO_LARGE,
        /**
         * adapter 内部不变量不成立，停止当前消费会话。
         */
        INTERNAL_INVARIANT
    }

    public enum Action {
        /**
         * 消耗普通失败预算后退避重试。
         */
        RETRY,
        /**
         * 按源的无效输入策略处理。
         */
        INVALID_RECORD,
        /**
         * 按可靠死信边界处理。
         */
        DEAD_LETTER,
        /**
         * 关闭当前分区业务路由。
         */
        HALT,
        /**
         * 会话进入需显式恢复的保护态。
         */
        FATAL
    }

    private final Reason reason;

    /**
     * 业务作用：构造分类失败并限制诊断大小。
     * <p>
     * 返回：控制字符替换为空格、最多 256 个 Unicode 字符的异常。
     *
     * @param reason     稳定原因
     * @param safeDetail 不含正文或凭据的诊断。
     */
    public PassthroughFailure(Reason reason, String safeDetail) {
        super(sanitize(safeDetail));
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    /**
     * 业务作用：提供强类型业务原因。
     * <p>
     * 参数说明：无。
     *
     * @return 构造时声明的原因。
     */
    public Reason reason() {
        return reason;
    }

    /**
     * 业务作用：将原因映射到唯一消费动作。
     * <p>
     * 参数说明：无。
     *
     * @return 重试、无效输入、死信、暂停或崩溃。
     */
    public Action action() {
        return switch (reason) {
            case SOCKET_ENQUEUE, TEMPORARY_DEPENDENCY -> Action.RETRY;
            case MALFORMED_ROUTE, MISSING_PAYLOAD, UNSUPPORTED_MESSAGE_MODE -> Action.INVALID_RECORD;
            case DEAD_LETTER_ROUTE_EXPANSION -> Action.DEAD_LETTER;
            case HALT_ROUTE_EXPANSION, FRAME_TOO_LARGE -> Action.HALT;
            case INTERNAL_INVARIANT -> Action.FATAL;
        };
    }

    /**
     * 业务作用：限制诊断文本的展示与存储开销。
     *
     * @param text 调用方提供的安全文本。
     * @return 控制字符清洗后的有限文本；不承诺识别任意秘密。
     */
    private static String sanitize(String text) {
        if (text == null) return "";
        StringBuilder out = new StringBuilder();
        text.codePoints().limit(256).forEach(c -> out.appendCodePoint(Character.getType(c) == Character.CONTROL ? ' ' : c));
        return out.toString();
    }
}

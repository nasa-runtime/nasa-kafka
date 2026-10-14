package io.github.nasaruntime.kafka.dlt;

/**
 * 必需死信交付的有界指数退避；只决定再次尝试的时刻，不授权提交来源。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class DltRetryPolicy {

    /**
     * 业务作用：禁止实例化无状态的交付退避计算器。
     * <p>
     * 参数说明：无。
     * 返回：无运行时资源。
     */
    private DltRetryPolicy() {}

    /**
     * 业务作用：以确定性抖动分散同时失败的死信重试，限制持续故障的请求频率。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param initial 已校验的首次DLT退避毫秒数。
     * @param max     DLT退避增长允许达到的最大毫秒数。
     * @param attempt 从一开始的失败次数
     * @param seed    失败单元首 offset。
     * @return 指数基准不超过 max，最终等待在基准约正负百分之十范围内；不随次数溢出。
     */
    public static long delayMillis(long initial, long max, long attempt, long seed) {
        long exponential = initial;
        for (long n = Math.min(Math.max(attempt - 1, 0), 63); n > 0 && exponential < max; n--)
            exponential = exponential > max / 2 ? max : exponential * 2;
        exponential = Math.min(exponential, max);
        long spread = exponential / 5;
        long jitter = spread == 0 ? 0 : Long.remainderUnsigned(Math.abs(seed) + attempt, spread + 1);
        return exponential - spread / 2 + jitter;
    }
}

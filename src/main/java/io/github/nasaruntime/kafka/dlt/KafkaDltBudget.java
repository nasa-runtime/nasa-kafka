package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.config.NasaKafkaProperties;

/**
 * 同一源全部普通消费组共享的死信信封与真实交付额度，独立于原输入额度。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaDltBudget {

    private final int maxRecords;
    private final long maxBytes;
    private final int maxInFlight;
    private int records;
    private long bytes;
    private int inFlight;

    /**
     * 业务作用：冻结源级 DLT 边界，避免增加组数放大死信资源占用。
     * <p>
     * 返回：零占用账本，不创建线程或连接。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param config 有效源配置。
     */
    public KafkaDltBudget(NasaKafkaProperties config) {
        maxRecords = config.getDltQueueCapacity();
        maxBytes = config.getDltQueueMaxBytes();
        maxInFlight = config.getDltMaxInFlight();
    }

    /**
     * 业务作用：为等待、交付、完成与退避重试预留同一份信封额度。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param retained 完整信封计费字节。
     * @return 实际责任结束才可关闭的凭证；不足时原输入继续等待。
     */
    public synchronized Lease retain(long retained) {
        if (retained < 0 || records >= maxRecords || retained > maxBytes - bytes) return null;
        records++;
        bytes += retained;
        return new Lease(retained, false);
    }

    /**
     * 业务作用：跨消费组限制正在执行的 DLT 查询与交付。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 实际调用结束才可归还的凭证；满额不启动调用。
     */
    public synchronized Lease invoke() {
        if (inFlight >= maxInFlight) return null;
        inFlight++;
        return new Lease(0, true);
    }

    /**
     * 业务作用：为源级仪表提供同一时刻的信封占用，避免按组覆盖或相加不同时刻的值。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 保留信封数与完整字节，不代表可用准入权。
     */
    public synchronized Snapshot snapshot() {
        return new Snapshot(records, bytes);
    }

    /**
     * 业务作用：读取真实 DLT 调用占用，失权后存活的调用也保留责任。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 尚未归还调用凭证的数量。
     */
    public synchronized int activeTasks() {
        return inFlight;
    }

    public record Snapshot(int records, long bytes) {}

    public final class Lease implements AutoCloseable {
        private final long retained;
        private final boolean invocation;
        private boolean closed;

        /**
         * 业务作用：绑定一次已成功取得的源级责任。
         * <p>
         * 返回：尚未归还的凭证。
         *
         * @param retained   信封字节
         * @param invocation 表示实际交付调用。
         */
        private Lease(long retained, boolean invocation) {
            this.retained = retained;
            this.invocation = invocation;
        }

        /**
         * 业务作用：在信封或真实调用责任退出后一次性归还源额度。
         * <p>
         * 参数说明：无。
         * 返回：重复关闭不扩大余量，释放不表示来源已提交。
         */
        @Override
        public void close() {
            synchronized (KafkaDltBudget.this) {
                if (closed) return;
                closed = true;
                if (invocation) inFlight--;
                else {
                    records--;
                    bytes -= retained;
                }
            }
        }
    }
}

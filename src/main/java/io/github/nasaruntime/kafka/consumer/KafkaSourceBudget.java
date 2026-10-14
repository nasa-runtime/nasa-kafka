package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.config.NasaKafkaProperties;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

/**
 * 普通消费输入的源与进程共享计费；副本预留字节不等于解码对象或 native 队列的实际堆占用。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaSourceBudget {

    private static ProcessBudget processBudget;
    private final ProcessBudget process;
    private final int maxRecords;
    private final long maxBytes;
    private int records;
    private long bytes;

    /**
     * 业务作用：在任何原生资源构造前冻结源和进程输入边界。
     * <p>
     * 返回：空的源计费域；同一组件类加载器内的进程边界声明不一致时拒绝。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param config 有效源配置。
     */
    public KafkaSourceBudget(NasaKafkaProperties config) {
        process = process(config.getMaxProgressRecordsPerProcess(), config.getMaxProgressBytesPerProcess());
        maxRecords = config.getMaxProgressRecordsPerSource();
        maxBytes = config.getMaxProgressBytesPerSource();
    }

    /**
     * 业务作用：使所有源共享同一个不可运行期放大的进程输入边界。
     *
     * @param records 进程共享输入责任的记录数量上限声明。
     * @param bytes   进程共享输入责任的计费字节上限声明。
     * @return 组件类加载器寿命内相同的计费池；改变声明需要重新启动对应加载环境。
     */
    private static synchronized ProcessBudget process(int records, long bytes) {
        if (processBudget == null) processBudget = new ProcessBudget(records, bytes);
        if (processBudget.maxRecords != records || processBudget.maxBytes != bytes)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "同一进程的消费输入上限必须一致");
        return processBudget;
    }

    /**
     * 业务作用：在同一锁内取得源和进程两份责任额度，防止多个组分别通过却合计超额。
     *
     * @param retained 原文及框架副本的保守预留。
     * @return 必须保留到实际责任退出的凭证；任一层不足时不改变计数。
     */
    Lease retain(long retained) {
        synchronized (process) {
            if (!available(retained)) return null;
            records++;
            bytes += retained;
            process.records++;
            process.bytes += retained;
            return new Lease(retained);
        }
    }

    /**
     * 业务作用：在恢复原生拉取前观察共享源与进程余量。
     *
     * @param retained 等待输入大小。
     * @return 只表示当前可尝试，实际取入必须重新裁决。
     */
    boolean canRetain(long retained) {
        synchronized (process) {
            return available(retained);
        }
    }

    /**
     * 业务作用：在统一计费锁内同时检查数量和字节，拒绝不可表示的负计量。
     *
     * @param retained 输入大小。
     * @return 两层数量与字节都允许时为 true。
     */
    private boolean available(long retained) {
        return retained >= 0 && records < maxRecords && retained <= maxBytes - bytes
                && process.records < process.maxRecords && retained <= process.maxBytes - process.bytes;
    }

    /**
     * 业务作用：识别即使池为空也无法容纳的原文，避免把永久不可准入误当短暂反压。
     * <p>
     * 参数说明：无。
     *
     * @return 源与进程之间更小的字节边界。
     */
    long maximumRecordBytes() {
        return Math.min(maxBytes, process.maxBytes);
    }

    private static final class ProcessBudget {
        final int maxRecords;
        final long maxBytes;
        int records;
        long bytes;

        /**
         * 业务作用：保存进程寿命内固定边界和原子共享占用。
         * <p>
         * 返回：零占用池，不创建线程或连接。
         *
         * @param records 进程共享输入责任的记录数量上限声明。
         * @param bytes   进程共享输入责任的计费字节上限声明。
         */
        ProcessBudget(int records, long bytes) {
            maxRecords = records;
            maxBytes = bytes;
        }
    }

    final class Lease implements AutoCloseable {
        private final long retained;
        private boolean closed;

        /**
         * 业务作用：把已取得的两层额度绑定到一次输入责任。
         * <p>
         * 返回：尚未归还的凭证。
         *
         * @param retained 成功计费的字节。
         */
        private Lease(long retained) {
            this.retained = retained;
        }

        /**
         * 业务作用：实际输入责任退出后一次性归还源与进程额度。
         * <p>
         * 参数说明：无。
         * 返回：重复关闭不增加余量，归还不表示业务成功。
         */
        @Override
        public void close() {
            synchronized (process) {
                if (closed) return;
                closed = true;
                records--;
                bytes -= retained;
                process.records--;
                process.bytes -= retained;
            }
        }
    }
}

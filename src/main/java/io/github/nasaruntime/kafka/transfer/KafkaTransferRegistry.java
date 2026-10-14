package io.github.nasaruntime.kafka.transfer;

import io.github.nasaruntime.kafka.replay.ReplayHandle;
import io.github.nasaruntime.kafka.replay.ReplayParticipant;


import java.time.Duration;
import java.util.*;
import java.util.function.BooleanSupplier;

/**
 * 目标来源持有的转发依赖；尚未绑定来源句柄的登记也属于必须排干的责任。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaTransferRegistry {

    private final Set<Link> links = new HashSet<>();
    private boolean closed;

    /**
     * 业务作用：为目标登记一份尚未开放的转发责任。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 有界待绑定票据；关闭或16项额度耗尽时拒绝。
     */
    public synchronized Link reserve() {
        if (closed) throw new TransferException(TransferException.Code.CLOSED);
        if (links.size() >= 16) throw new TransferException(TransferException.Code.CAPACITY);
        Link link = new Link(this);
        links.add(link);
        return link;
    }

    /**
     * 业务作用：先关闭新依赖准入并通知全部来源任务，之后才由来源关闭流程等待。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 包含尚未绑定票据的完整关闭集合。
     */
    public synchronized List<Link> stop() {
        closed = true;
        List<Link> current = List.copyOf(links);
        current.forEach(Link::stop);
        return current;
    }

    /**
     * 业务作用：把未激活票据和运行中的来源依赖都纳入目标关闭报告。
     * <p>
     * 参数说明：无。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @return 尚未实际归还的依赖数，不改变准入。
     */
    public synchronized int activeCount() {
        return links.size();
    }

    /**
     * 业务作用：在真实任务资源退出后归还目标依赖额度。
     * <p>
     * 返回：幂等移除，不影响其它来源任务。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param link 具体执行实例。
     */
    private synchronized void release(Link link) {
        links.remove(link);
    }

    public static final class Link implements ReplayParticipant {
        private final KafkaTransferRegistry registry;
        private ReplayHandle handle;
        private boolean stopped, finished;

        /**
         * 业务作用：建立待激活依赖，尚不允许来源执行。
         * <p>
         * 返回：无后台线程的票据。
         *
         * @param registry 目标责任目录。
         */
        Link(KafkaTransferRegistry registry) {
            this.registry = registry;
        }

        /**
         * 业务作用：把来源实际任务绑定到已登记的目标责任，目标先停止时立即传递取消。
         * <p>
         * 返回：唤醒协调线程，只有双方登记成立且未停止才激活。
         *
         * <p>
         * 供框架功能包协作使用，不作为稳定的业务接入接口。
         * @param handle 来源实例句柄。
         */
        public synchronized void bind(ReplayHandle handle) {
            if (this.handle != null) throw new IllegalStateException("转发依赖不可重复绑定");
            Objects.requireNonNull(handle);
            if (stopped || finished) handle.cancel();
            if (!finished) this.handle = handle;
            notifyAll();
        }

        /**
         * 业务作用：将目标停止请求传给已绑定来源，未绑定时保留停止意图。
         * <p>
         * 参数说明：无。
         * 返回：不等待实际任务，不释放登记。
         */
        synchronized void stop() {
            stopped = true;
            if (handle != null) handle.cancel();
            notifyAll();
        }

        /**
         * 业务作用：在两个来源登记都可见之前阻止接管进度和业务副作用。
         *
         * @param stopping 来源即时停止门。
         * @return 已绑定且双方未停止时 true，来源或目标先停止时 false。
         */
        @Override
        public synchronized boolean activate(BooleanSupplier stopping) throws InterruptedException {
            while (handle == null && !stopped && !finished && !stopping.getAsBoolean()) wait(100);
            return handle != null && !stopped && !finished && !stopping.getAsBoolean();
        }

        /**
         * 业务作用：复验已绑定来源任务是否仍承担目标交付责任。
         * <p>
         * 参数说明：无。
         *
         * @return 绑定后至实际退出前为 true；停止请求不提前撤销已进入处理器的交付依赖。
         */
        public synchronized boolean deliveryActive() {
            return handle != null && !finished;
        }

        /**
         * 业务作用：实际来源资源退出后发布依赖完成，遵守不持有票据锁再进入目录锁的顺序。
         * <p>
         * 参数说明：无。
         * 返回：目标可继续关闭，重复调用无副作用。
         */
        @Override
        public void finished() {
            synchronized (this) {
                finished = true;
                handle = null;
                notifyAll();
            }
            registry.release(this);
        }

        /**
         * 业务作用：在目标的同一截止时间内等待来源实际退出，未绑定票据不能被跳过。
         *
         * <p>
         * 供框架功能包协作使用，不作为稳定的业务接入接口。
         * @param deadline 调用方关闭流程共用的 System.nanoTime 纳秒截止值；通常来自目标源的关闭预算。
         * @return 实际资源退出时 true，超时或中断时 false。
         */
        public boolean awaitDrained(long deadline) {
            ReplayHandle current;
            synchronized (this) {
                while (handle == null && !finished) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) return false;
                    try {
                        wait(Math.clamp(remaining / 1_000_000, 1, 100));
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
                if (finished) return true;
                current = handle;
            }
            return current.awaitDrained(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        }
    }
}

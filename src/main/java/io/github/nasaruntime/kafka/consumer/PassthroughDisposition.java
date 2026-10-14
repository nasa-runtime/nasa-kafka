package io.github.nasaruntime.kafka.consumer;

import java.util.Objects;

/**
 * 同步透传的成功条件；本地准入不代表远端客户端已经收到消息。
 */
public sealed interface PassthroughDisposition {

    /**
     * 全部目标已经进入本地有界出站队列，仍服从 Manual 确认要求。
     */
    record Handled(long queued) implements PassthroughDisposition {
        /**
         * 业务作用：声明本次成功准入数。
         * <p>
         * 返回：有效成功条件，负数拒绝。
         *
         * @param queued 非负目标数。
         */
        public Handled {
            if (queued < 0) throw new IllegalArgumentException("queued 必须非负");
        }
    }

    /**
     * 安全跳过由明确事实确认，不要求 Manual 再次 Ack。
     */
    record Skipped(SkipReason reason) implements PassthroughDisposition {
        /**
         * 业务作用：声明不需要重投的安全跳过。
         * <p>
         * 返回：非空跳过条件。
         *
         * @param reason 事实分类。
         */
        public Skipped {
            Objects.requireNonNull(reason, "reason");
        }
    }

    /**
     * 显式允许部分丢弃；有成功返回仍须按所选 Ack 模式确认。
     */
    record BestEffortDropped(long queued, long dropped, long closed) implements PassthroughDisposition {
        /**
         * 业务作用：声明可用性优先的部分准入终态。
         * <p>
         * 返回：非负计数，非法声明拒绝。
         *
         * @param queued  入队数
         * @param dropped 满队列丢弃数
         * @param closed  已关闭目标数。
         */
        public BestEffortDropped {
            if (queued < 0 || dropped < 0 || closed < 0) throw new IllegalArgumentException("准入计数必须非负");
        }
    }

    /**
     * 与 Rust PassthroughSkipReason 对应的安全跳过事实。
     */
    enum SkipReason {
        /**
         * 本节点没有在线目标。
         */
        NO_LOCAL_TARGET,
        /**
         * 消息指定了其它节点。
         */
        TARGET_NODE_MISMATCH,
        /**
         * 来源会形成消息回环。
         */
        LOOPBACK,
        /**
         * 来源代次已经过期。
         */
        STALE_SOURCE
    }
}

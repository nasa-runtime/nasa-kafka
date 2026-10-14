package io.github.nasaruntime.kafka.replay;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * 外部资源目录的当前代次证明；目录必须与 Kafka 的创建、删除和重建保持一致。
 */
@FunctionalInterface
public interface ReplayIdentityProvider {

    /**
     * 业务作用：在执行边界重新取得真实资源代次，阻止同名 topic 复用旧计划。
     *
     * @param clusterId 本轮物理集群
     * @param topics    完整白名单
     * @param timeout   本次剩余预算。
     * @return 每个 topic 的当前代次；缺失、不可观测或超时必须抛出异常，不能回退到计划中的旧值。
     */
    Map<String, String> generations(String clusterId, Set<String> topics, Duration timeout) throws Exception;
}

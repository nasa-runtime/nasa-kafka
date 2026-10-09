package io.github.nasaruntime.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * 记录作用 - KafkaPollTask 单批 dispatch 结果。
 *
 * @param allOk 本批所有 listener 是否全部处理成功
 * @param batchStart 每个分区本批最小 offset, 用于失败 seek 回退重投
 */
record DispatchResult(boolean allOk, Map<TopicPartition, Long> batchStart) {
}

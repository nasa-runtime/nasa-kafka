package io.github.nasaruntime.kafka.consumer;

/**
 * 未匹配或不可解码记录的显式处置策略。
 */
public enum RecordPolicy {
    /**
     * 明确选择丢弃该输入并允许其进入安全提交前缀。
     */
    SKIP,
    /**
     * 转交 DLT 责任，默认等待目标 broker 确认后才推进来源位置。
     */
    DEAD_LETTER,
    /**
     * 保留来源恢复坐标并停止该分区派发，等待人工决定后续处置。
     */
    HALT
}

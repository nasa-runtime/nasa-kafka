package io.github.nasaruntime.kafka.consumer;

import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * owner 独占的已观察记录账本，数字空洞不等于未处理记录。
 */
final class PartitionOffsetState {

    private final NavigableMap<Long, Boolean> records = new TreeMap<>();
    private long lastObserved = -1;

    /**
     * 业务作用：在派发前登记记录的恢复责任。
     *
     * @param offset 同一定位代次下单调观察位置。
     * @return 新登记为 true，重复观察为 false。
     */
    boolean observe(long offset) {
        if (records.containsKey(offset)) return false;
        if (offset < 0 || offset <= lastObserved)
            throw new IllegalStateException("观察位置必须单调，新 seek 需要新的状态实例");
        records.put(offset, false);
        lastObserved = offset;
        return true;
    }

    /**
     * 业务作用：将已登记记录标记为业务或 DLT 安全完成。
     * <p>
     * 返回：未知位置拒绝，不允许直接创建高水位。
     *
     * @param offset 已观察位置。
     */
    void markSafe(long offset) {
        if (!records.containsKey(offset)) throw new IllegalStateException("不能确认未观察 offset");
        records.put(offset, true);
    }

    /**
     * 业务作用：计算尚未确认的连续安全前缀。
     * <p>
     * 参数说明：无。
     *
     * @return 可提交的下一个位置，没有安全前缀时为空。
     */
    OptionalLong committable() {
        long candidate = -1;
        for (var record : records.entrySet()) {
            if (!record.getValue()) return candidate < 0 ? OptionalLong.empty() : OptionalLong.of(record.getKey());
            candidate = Math.addExact(record.getKey(), 1);
        }
        return candidate < 0 ? OptionalLong.empty() : OptionalLong.of(candidate);
    }

    /**
     * 业务作用：仅裁剪 broker 已确认覆盖的责任。
     * <p>
     * 返回：无，不能覆盖不安全记录。
     *
     * @param nextOffset broker 确认位置。
     */
    void committed(long nextOffset) {
        if (records.headMap(nextOffset, false).values().stream().anyMatch(safe -> !safe))
            throw new IllegalStateException("提交越过未安全记录");
        records.headMap(nextOffset, false).clear();
    }
}

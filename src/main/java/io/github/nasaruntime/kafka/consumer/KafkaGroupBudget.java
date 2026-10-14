package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.config.NasaKafkaProperties;
import io.github.nasaruntime.kafka.dlt.KafkaDltBudget;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * 同组全部 owner 共用的责任额度；计费锁内只改变本地计数，不执行业务或原生客户端操作。
 */
final class KafkaGroupBudget {

    /**
     * 原输入、拥有型交付、头映射与完整键顺序域共用的保守副本预留，不代表整个 JVM 堆大小。
     */
    private static final int INPUT_COPY_RESERVATIONS = 8;

    /**
     * 业务作用：在取入前覆盖原文及框架必要副本，避免输入占满后再申请副本额度而无法执行。
     *
     * @param rawBytes 原始 payload/key/headers 字节和。
     * @return 覆盖整个输入生命周期的保守预留，溢出按不可准入处理。
     */
    static long inputReservationBytes(long rawBytes) {
        return rawBytes > Long.MAX_VALUE / INPUT_COPY_RESERVATIONS ? Long.MAX_VALUE : rawBytes * INPUT_COPY_RESERVATIONS;
    }

    private final KafkaSourceBudget source;
    private final KafkaDltBudget dlt;
    private final int maxRecords;
    private final long maxBytes;
    private final int maxPartitionRecords;
    private final long maxPartitionBytes;
    private final int maxKeyRecords;
    private final long maxKeyBytes;
    private final Map<TopicPartition, InputUsage> inputs = new HashMap<>();
    private final int maxHandlers;
    private int records;
    private long bytes;
    private int handlers;
    private final int maxAssignment;
    private final int maxRetries;
    private int retries;
    private final Map<Integer, Set<TopicPartition>> assignments = new HashMap<>();
    /**
     * 组内重平衡不能让新 owner 与尚未退出的旧调用重叠使用相同顺序域。
     */
    private final Map<TopicPartition, Domain> domains = new HashMap<>();

    /**
     * 业务作用：冻结整个消费组的共享上限，使闲置 owner 的额度可被其它槽使用。
     * <p>
     * 返回：零占用账本，不按槽数放大额度。
     *
     * @param config 已校验的源配置。
     */
    KafkaGroupBudget(NasaKafkaProperties config) {
        this(config, new KafkaSourceBudget(config), new KafkaDltBudget(config));
    }

    /**
     * 业务作用：让组内全部输入受源与进程总量约束，死信独立共享源额度。
     * <p>
     * 返回：不预占源额度的空组账本。
     *
     * @param config 组边界
     * @param source 该源全部普通消费组共用的输入责任预算池。
     * @param dlt    该源全部消费组共用的独立DLT责任预算池。
     */
    KafkaGroupBudget(NasaKafkaProperties config, KafkaSourceBudget source, KafkaDltBudget dlt) {
        this.source = source;
        this.dlt = dlt;
        maxRecords = config.getMaxProgressRecordsPerGroup();
        maxBytes = config.getMaxProgressBytesPerGroup();
        maxPartitionRecords = config.getMaxProgressRecordsPerPartition();
        maxPartitionBytes = config.getMaxProgressBytesPerPartition();
        maxKeyRecords = config.getMaxProgressRecordsPerKey();
        maxKeyBytes = config.getMaxProgressBytesPerKey();
        maxHandlers = config.getMaxHandlerInFlight();
        maxAssignment = config.getMaxAssignmentPartitions();
        maxRetries = config.getMaxRetryEntries();
    }

    /**
     * 业务作用：在发布本槽分配前原子校验组内分区并集，同一分区接管期间只计一次。
     * <p>
     * 返回：成功替换；超限不改变任何槽登记并抛出配置错误。
     *
     * @param slot       稳定槽号
     * @param partitions 完整新范围。
     */
    synchronized void assign(int slot, Collection<TopicPartition> partitions) {
        Set<TopicPartition> union = new HashSet<>(partitions);
        assignments.forEach((owner, values) -> {
            if (owner != slot) union.addAll(values);
        });
        // 分配容量不授予业务执行权；旧实际调用仍由独立输入和顺序域凭证保护。
        if (union.size() > maxAssignment)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "AssignmentTooLarge: group partitions=" + union.size() + " max=" + maxAssignment);
        if (partitions.isEmpty()) assignments.remove(slot);
        else assignments.put(slot, Set.copyOf(partitions));
    }

    /**
     * 业务作用：一次接纳整批新增失败记录的次数元数据，空闲槽不预留份额。
     *
     * @param count 此前没有失败凭证的记录数。
     * @return 逐记录独立凭证；容量不足返回 null 且不改变计数。
     */
    synchronized List<Lease> retainRetries(int count) {
        if (count < 0 || count > maxRetries - retries) return null;
        List<Lease> result = new ArrayList<>(count);
        for (int n = 0; n < count; n++) result.add(new Lease(Kind.RETRY, 0, null, null, false));
        retries += count;
        return result;
    }

    /**
     * 业务作用：在消息进入责任账本前同时预留全部层级的数量和原文副本字节。
     *
     * @param tp       来源分区
     * @param key      inputKey 产生的固定长度计费摘要；原始 null key 使用字符串哨兵单独计费，不代表业务顺序权限。
     * @param retained 原文及副本的保守预留。
     * @return 责任凭证；容量不足返回 null 且不改变任何计数。
     */
    synchronized Lease retain(TopicPartition tp, String key, long retained) {
        if (!available(tp, key, retained)) return null;
        // 固定按组、源与进程顺序裁决；上层拒绝时不得留下只计入某一层的责任。
        KafkaSourceBudget.Lease parent = source.retain(retained);
        if (parent == null) return null;
        records++;
        bytes += retained;
        InputUsage partition = inputs.computeIfAbsent(tp, ignored -> new InputUsage());
        partition.records++;
        partition.bytes += retained;
        InputUsage keyed = partition.keys.computeIfAbsent(key, ignored -> new InputUsage());
        keyed.records++;
        keyed.bytes += retained;
        Lease lease = new Lease(Kind.RECORD, retained, tp, key, false);
        lease.parent = parent;
        return lease;
    }

    /**
     * 业务作用：为 DLT 的等待、交付、完成和重试保留同一份额度。
     *
     * @param retained 包含来源证据的完整信封字节。
     * @return 责任凭证；耗尽时保留来源记录等待，不发送。
     */
    synchronized Lease retainDlt(long retained) {
        KafkaDltBudget.Lease reserved = dlt.retain(retained);
        if (reserved == null) return null;
        Lease lease = new Lease(Kind.DLT_RECORD, retained, null, null, false);
        lease.dlt = reserved;
        return lease;
    }

    /**
     * 业务作用：在启动真实调用前原子取得全组执行名额与顺序域。
     *
     * @param tp         分区
     * @param key        实际业务调用的key顺序身份，与分区共同限制执行并发。
     * @param exclusive  表示独占分区
     * @param deadLetter 区分业务与 DLT 执行上限。
     * @return 实际退出前必须保留的凭证；容量或旧调用阻挡时返回 null。
     */
    synchronized Lease invoke(TopicPartition tp, String key, boolean exclusive, boolean deadLetter) {
        Domain domain = domains.get(tp);
        // 新 assignment 仅授予提交权限，不能撤销本进程旧调用已经开始的外部副作用。
        if (domain != null && (domain.exclusive || exclusive || domain.keys.contains(key))) return null;
        KafkaDltBudget.Lease reserved = null;
        if (deadLetter) {
            reserved = dlt.invoke();
            if (reserved == null) return null;
        } else {
            if (handlers >= maxHandlers) return null;
            handlers++;
        }
        domain = domains.computeIfAbsent(tp, ignored -> new Domain());
        domain.exclusive = exclusive;
        domain.keys.add(key);
        Lease lease = new Lease(deadLetter ? Kind.DLT_CALL : Kind.HANDLER, 0, tp, key, exclusive);
        lease.dlt = reserved;
        return lease;
    }

    /**
     * 业务作用：在 poll 之前识别组级数量或字节耗尽，所有槽使用同一事实。
     * <p>
     * 参数说明：无。
     *
     * @return 达到任一硬上限时为 true，不作为随后准入的保证。
     */
    synchronized boolean full() {
        return records >= maxRecords || bytes >= maxBytes || !source.canRetain(0);
    }

    /**
     * 业务作用：判断上次因容量交回的记录是否可以再次取入，避免无容量时反复 fetch 和 seek。
     *
     * @param tp       记录或调用归属的来源topic及分区，用于分区层计费。
     * @param key      inputKey 产生的固定长度计费摘要；原始 null key 使用字符串哨兵单独计费，不代表业务顺序权限。
     * @param retained 拒绝记录的完整预留。
     * @return 全部层级有余量时为 true；真正接纳仍须原子 retain。
     */
    synchronized boolean canRetain(TopicPartition tp, String key, long retained) {
        return available(tp, key, retained) && source.canRetain(retained);
    }

    /**
     * 业务作用：在组锁内裁决组、分区与 key 的本地余量，失败不修改任何状态。
     *
     * @param tp       记录或调用归属的来源topic及分区，用于分区层计费。
     * @param key      inputKey 产生的固定长度计费摘要；原始 null key 使用字符串哨兵单独计费，不代表业务顺序权限。
     * @param retained 原始 key、payload、头字节及框架副本的预留计费量，不是单纯正文长度。
     * @return 本地三层均允许时为 true。
     */
    private boolean available(TopicPartition tp, String key, long retained) {
        if (retained < 0 || records >= maxRecords || retained > maxBytes - bytes) return false;
        InputUsage partition = inputs.get(tp);
        if (partition == null) return retained <= Math.min(maxPartitionBytes, maxKeyBytes);
        if (partition.records >= maxPartitionRecords || retained > maxPartitionBytes - partition.bytes) return false;
        InputUsage keyed = partition.keys.get(key);
        return keyed == null ? retained <= maxKeyBytes : keyed.records < maxKeyRecords && retained <= maxKeyBytes - keyed.bytes;
    }

    /**
     * 业务作用：在尚未准入时以固定长度身份计费，避免等待容量本身复制巨大 key。
     *
     * @param raw 原始键，null 单独成域。
     * @return 摘要仅用于保守容量分组，不授予业务顺序权限。
     */
    static String inputKey(byte[] raw) {
        if (raw == null) return "null";
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 unavailable", unavailable);
        }
    }

    /**
     * 业务作用：识别任意层级空池也无法容纳的原文，避免永久容量等待。
     * <p>
     * 参数说明：无。
     *
     * @return 进程、源、组、分区与 key 之间最小预留边界除以副本系数后的原文字节上限。
     */
    long maximumRecordBytes() {
        return Math.min(Math.min(maxPartitionBytes, maxKeyBytes), Math.min(maxBytes, source.maximumRecordBytes())) / INPUT_COPY_RESERVATIONS;
    }

    private static final class InputUsage {
        int records;
        long bytes;
        final Map<String, InputUsage> keys = new HashMap<>();
    }

    private enum Kind {RECORD, DLT_RECORD, HANDLER, DLT_CALL, RETRY}

    private static final class Domain {
        boolean exclusive;
        final Set<String> keys = new HashSet<>();
    }

    /**
     * 唯一责任凭证；取消等待、超时和失权都不能自动释放。
     */
    final class Lease implements AutoCloseable {
        private final Kind kind;
        private final long retained;
        private final TopicPartition tp;
        private final String key;
        private final boolean exclusive;
        private boolean closed;
        private KafkaSourceBudget.Lease parent;
        private KafkaDltBudget.Lease dlt;

        /**
         * 业务作用：把已成功预留的额度与顺序域绑定到唯一责任。
         * <p>
         * 返回：尚未归还的凭证。
         *
         * @param kind      计费域
         * @param retained  字节数
         * @param tp        记录或调用归属的来源topic及分区，用于分区层计费。
         * @param key       输入责任的计费摘要或实际调用的key顺序身份，具体由凭证的计费域决定。
         * @param exclusive 仅用于调用凭证，表示该调用是否独占整个分区顺序域。
         */
        private Lease(Kind kind, long retained, TopicPartition tp, String key, boolean exclusive) {
            this.kind = kind;
            this.retained = retained;
            this.tp = tp;
            this.key = key;
            this.exclusive = exclusive;
        }

        /**
         * 业务作用：仅在责任已提交、明确交回或真实调用退出后归还共享额度。
         * <p>
         * 参数说明：无。
         * 返回：首次归还减少计数；重复归还不改变其它责任。
         */
        @Override
        public void close() {
            synchronized (KafkaGroupBudget.this) {
                if (closed) return;
                closed = true;
                switch (kind) {
                    case RECORD -> {
                        records--;
                        bytes -= retained;
                        InputUsage partition = inputs.get(tp), keyed = partition.keys.get(key);
                        keyed.records--;
                        keyed.bytes -= retained;
                        if (keyed.records == 0) partition.keys.remove(key);
                        partition.records--;
                        partition.bytes -= retained;
                        if (partition.records == 0) inputs.remove(tp);
                        parent.close();
                    }
                    case DLT_RECORD -> dlt.close();
                    case HANDLER -> handlers--;
                    case DLT_CALL -> dlt.close();
                    case RETRY -> retries--;
                }
                if (kind == Kind.HANDLER || kind == Kind.DLT_CALL) {
                    Domain domain = domains.get(tp);
                    domain.keys.remove(key);
                    if (exclusive) domain.exclusive = false;
                    if (domain.keys.isEmpty()) domains.remove(tp);
                }
            }
        }
    }
}

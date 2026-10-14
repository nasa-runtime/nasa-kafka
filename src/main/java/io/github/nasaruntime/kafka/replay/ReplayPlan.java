package io.github.nasaruntime.kafka.replay;

import io.github.nasaruntime.kafka.checkpoint.ReplayCheckpoint;

import io.github.nasaruntime.kafka.checkpoint.CheckpointException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/**
 * 可审阅的有限回放合同，不发起消费或持久写入。
 * topicGenerations 必须来自能识别删除重建的外部资源目录；静态 topic 名或分区数不构成该证据。
 * pipelineFingerprint 由业务覆盖 schema、转换、过滤、成功条件及目的路由；改变任一语义必须更换指纹。
 */
public record ReplayPlan(String taskId, String sourceId, String clusterId, Map<String, String> topicGenerations,
                         String pipelineFingerprint, ReplayLimits limits, List<ReplayCheckpoint.Position> ranges) {

    public static final int MAX_ENCODED_BYTES = 8 * 1024 * 1024;

    public enum MissingTimestamp {FREEZE_END, REJECT}

    /**
     * 业务作用：建立冻结且可跨语言核对的任务合同。
     * <p>
     * 返回：不可变计划；缺失身份、重复范围、非初始进度或 topic 证据不完整均拒绝。
     *
     * @param taskId              有限回放任务的稳定身份，用于持久checkpoint归属。
     * @param sourceId            回放读取所归属的命名数据源。
     * @param clusterId           冻结读取范围所归属的物理Kafka集群身份。
     * @param topicGenerations    来源topic与冻结资源代次的完整映射。
     * @param pipelineFingerprint 冻结处理流水线的SHA-256指纹。
     * @param limits              独立于在线消费的回放速率、容量和并行预算。
     * @param ranges              各分区已冻结的起止范围；初始nextOffset必须等于startOffset。
     */
    public ReplayPlan {
        ReplayObservation.token(sourceId, 128);
        ReplayObservation.token(clusterId, 1024);
        if (pipelineFingerprint == null || !pipelineFingerprint.matches("[0-9a-f]{64}") || limits == null || topicGenerations == null)
            ReplayObservation.invalid();
        try {
            ranges = new ReplayCheckpoint(taskId, pipelineFingerprint, 0, 0, ranges).positions();
        } catch (CheckpointException failure) {
            throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
        }
        Set<String> topics = new TreeSet<>();
        for (ReplayCheckpoint.Position range : ranges) {
            if (range.nextOffset() != range.startOffset()) ReplayObservation.invalid();
            topics.add(range.topic());
        }
        if (!topics.equals(topicGenerations.keySet())) ReplayObservation.invalid();
        TreeMap<String, String> generations = new TreeMap<>();
        topicGenerations.forEach((topic, generation) -> {
            ReplayObservation.token(generation, 1024);
            generations.put(topic, generation);
        });
        topicGenerations = Collections.unmodifiableMap(generations);
    }

    /**
     * 业务作用：把当前仍保留的历史范围冻结为只读计划。
     *
     * @param taskId      有限回放任务的稳定身份，用于持久checkpoint归属。
     * @param sourceId    回放读取所归属的命名数据源。
     * @param observation 本次来源保留起点、LSO及可选时间查询结果，决定有限范围。
     * @param generations 各来源topic的当前资源代次证据，必须覆盖冻结范围。
     * @param pipeline    实际处理合同的固定指纹，恢复时不能替换为另一处理规则。
     * @param limits      独立于在线消费的回放速率、容量和并行预算。
     * @return 每分区从 beginning 到本轮 LSO 的计划，不包含未来写入。
     */
    public static ReplayPlan earliest(String taskId, String sourceId, ReplayObservation observation, Map<String, String> generations,
                                      String pipeline, ReplayLimits limits) {
        Objects.requireNonNull(observation);
        List<ReplayCheckpoint.Position> ranges = observation.partitions().stream().map(partition ->
                new ReplayCheckpoint.Position(partition.topic(), partition.partition(), partition.beginningOffset(), partition.stableEndOffset(), partition.beginningOffset())).toList();
        return new ReplayPlan(taskId, sourceId, observation.clusterId(), generations, pipeline, limits, ranges);
    }

    /**
     * 业务作用：冻结逐分区显式起点，避免把一个 offset 错用到全部分区。
     *
     * @param taskId      有限回放任务的稳定身份，用于持久checkpoint归属。
     * @param sourceId    回放读取所归属的命名数据源。
     * @param observation 本次来源保留起点、LSO及可选时间查询结果，决定有限范围。
     * @param generations 各来源topic的当前资源代次证据，必须覆盖冻结范围。
     * @param pipeline    实际处理合同的固定指纹，恢复时不能替换为另一处理规则。
     * @param limits      独立于在线消费的回放速率、容量和并行预算。
     * @param starts      全部来源分区的显式起点，不将实时latest作为可移动终点。
     * @return 缺项、重复、保留缺口或超出 LSO 均拒绝；成功时结束边界不再移动。
     */
    public static ReplayPlan offsets(String taskId, String sourceId, ReplayObservation observation, Map<String, String> generations,
                                     String pipeline, ReplayLimits limits, List<Start> starts) {
        if (starts == null || starts.size() != observation.partitions().size() || starts.stream().anyMatch(Objects::isNull))
            ReplayObservation.invalid();
        List<Start> sorted = starts.stream().sorted(Comparator.comparing(Start::topic).thenComparingInt(Start::partition)).toList();
        List<ReplayCheckpoint.Position> ranges = new ArrayList<>();
        for (int i = 0; i < sorted.size(); i++) {
            Start start = sorted.get(i);
            ReplayObservation.Partition partition = observation.partitions().get(i);
            if (!partition.topic().equals(start.topic()) || partition.partition() != start.partition())
                ReplayObservation.invalid();
            if (start.offset() < partition.beginningOffset()) throw new ReplayException(ReplayException.Code.DATA_GAP);
            if (start.offset() > partition.stableEndOffset())
                throw new ReplayException(ReplayException.Code.END_REGRESSED);
            ranges.add(new ReplayCheckpoint.Position(start.topic(), start.partition(), start.offset(), partition.stableEndOffset(), start.offset()));
        }
        return new ReplayPlan(taskId, sourceId, observation.clusterId(), generations, pipeline, limits, ranges);
    }

    /**
     * 逐分区显式读取起点。
     */
    public record Start(String topic, int partition, long offset) {
        /**
         * 业务作用：限制显式起点到 Kafka 可表示的坐标。
         * <p>
         * 返回：合法声明或配置拒绝。
         *
         * @param topic     显式起点所属的来源topic。
         * @param partition 显式起点所属的来源分区号。
         * @param offset    非负起点。
         */
        public Start {
            try {
                new ReplayCheckpoint.Position(topic, partition, offset, offset, offset);
            } catch (CheckpointException failure) {
                throw new ReplayException(ReplayException.Code.INVALID_CONFIGURATION);
            }
        }
    }

    /**
     * 业务作用：按一次时间查询固定历史起点，对无结果分区执行声明的策略。
     *
     * @param taskId      有限回放任务的稳定身份，用于持久checkpoint归属。
     * @param sourceId    回放读取所归属的命名数据源。
     * @param observation 本次来源保留起点、LSO及可选时间查询结果，决定有限范围。
     * @param generations 各来源topic的当前资源代次证据，必须覆盖冻结范围。
     * @param pipeline    实际处理合同的固定指纹，恢复时不能替换为另一处理规则。
     * @param limits      独立于在线消费的回放速率、容量和并行预算。
     * @param missing     时间查询无匹配记录时，拒绝任务或选择冻结末尾的策略。
     * @return 起点被限制在本轮冻结范围内；后续恢复不重新计算时间或 latest。
     */
    public static ReplayPlan timestamp(String taskId, String sourceId, ReplayObservation observation, Map<String, String> generations,
                                       String pipeline, ReplayLimits limits, MissingTimestamp missing) {
        if (observation.timestampMillis() == null || missing == null) ReplayObservation.invalid();
        List<Start> starts = new ArrayList<>();
        for (ReplayObservation.Partition partition : observation.partitions()) {
            if (partition.timestampOffset() == null && missing == MissingTimestamp.REJECT)
                throw new ReplayException(ReplayException.Code.TIMESTAMP_MISSING);
            long offset = partition.timestampOffset() == null ? partition.stableEndOffset() : Math.min(partition.timestampOffset(), partition.stableEndOffset());
            starts.add(new Start(partition.topic(), partition.partition(), offset));
        }
        return offsets(taskId, sourceId, observation, generations, pipeline, limits, starts);
    }

    /**
     * 业务作用：为持久 checkpoint 生成双端相同的完整合同摘要。
     * <p>
     * 参数说明：无。
     *
     * @return 采用长度前缀、固定排序和大端整数编码的 SHA-256，小写十六进制；不包含运行进度。
     */
    public String fingerprint() {
        return HexFormat.of().formatHex(digest(payload()));
    }

    /**
     * 业务作用：保存完整冻结计划，恢复时不再依赖重新计算的 latest 或时间范围。
     * <p>
     * 参数说明：无。
     *
     * @return 共同正文及 SHA-256 完整性尾部；摘要不承担存储认证，归档必须由业务控制写权限。
     */
    public byte[] encode() {
        byte[] payload = payload(), bytes = Arrays.copyOf(payload, payload.length + 32);
        System.arraycopy(digest(payload), 0, bytes, payload.length, 32);
        return bytes;
    }

    /**
     * 业务作用：从持久字节还原原冻结合同，拒绝截断、损坏、非规范排序和无界声明。
     *
     * @param encoded 完整计划，最大 8 MiB。
     * @return 合法独立计划；失败为 CORRUPT_PLAN，不授予执行权。
     */
    public static ReplayPlan decode(byte[] encoded) {
        if (encoded == null || encoded.length < 40 || encoded.length > MAX_ENCODED_BYTES)
            throw new ReplayException(ReplayException.Code.CORRUPT_PLAN);
        byte[] bytes = encoded.clone(), payload = Arrays.copyOf(bytes, bytes.length - 32);
        if (!MessageDigest.isEqual(digest(payload), Arrays.copyOfRange(bytes, payload.length, bytes.length)))
            throw new ReplayException(ReplayException.Code.CORRUPT_PLAN);
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
            if (!Arrays.equals(input.readNBytes(8), "NSKRPL01".getBytes(StandardCharsets.US_ASCII)))
                throw new IOException();
            String task = text(input, 128), source = text(input, 128), cluster = text(input, 1024), pipeline = text(input, 64);
            int topicCount = count(input);
            Map<String, String> generations = new TreeMap<>();
            for (int i = 0; i < topicCount; i++)
                if (generations.put(text(input, 249), text(input, 1024)) != null) throw new IOException();
            ReplayLimits limits = new ReplayLimits(input.readLong(), input.readLong(), input.readInt(), input.readLong(), input.readInt());
            int rangeCount = count(input);
            List<ReplayCheckpoint.Position> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++) {
                String topic = text(input, 249);
                int partition = input.readInt();
                long start = input.readLong(), end = input.readLong();
                ranges.add(new ReplayCheckpoint.Position(topic, partition, start, end, start));
            }
            ReplayPlan plan = new ReplayPlan(task, source, cluster, generations, pipeline, limits, ranges);
            // 规范编码比较也拒绝可解析却重排过的集合或额外尾部，确保一个合同只有一种持久表达。
            if (input.available() != 0 || !Arrays.equals(bytes, plan.encode())) throw new IOException();
            return plan;
        } catch (IOException | IllegalArgumentException | CheckpointException | ReplayException error) {
            throw new ReplayException(ReplayException.Code.CORRUPT_PLAN);
        }
    }

    /**
     * 业务作用：生成无进度字段的规范合同正文，供指纹和完整归档共用。
     * <p>
     * 参数说明：无。
     *
     * @return 有界且确定的内存字节，不访问外部存储。
     */
    private byte[] payload() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.write("NSKRPL01".getBytes(StandardCharsets.US_ASCII));
            text(out, taskId);
            text(out, sourceId);
            text(out, clusterId);
            text(out, pipelineFingerprint);
            out.writeInt(topicGenerations.size());
            for (var entry : topicGenerations.entrySet()) {
                text(out, entry.getKey());
                text(out, entry.getValue());
            }
            out.writeLong(limits.recordsPerSecond());
            out.writeLong(limits.bytesPerSecond());
            out.writeInt(limits.maxInFlightRecords());
            out.writeLong(limits.maxInFlightBytes());
            out.writeInt(limits.concurrency());
            out.writeInt(ranges.size());
            for (var range : ranges) {
                text(out, range.topic());
                out.writeInt(range.partition());
                out.writeLong(range.startOffset());
                out.writeLong(range.endOffset());
            }
            return bytes.toByteArray();
        } catch (IOException error) {
            throw new IllegalStateException("无法编码回放合同");
        }
    }

    /**
     * 业务作用：生成尚未取得任务独占权的持久声明。
     * <p>
     * 参数说明：无。
     *
     * @return generation/revision 为零、全部 next 位于 start 的完整 checkpoint。
     */
    public ReplayCheckpoint initialCheckpoint() {
        return new ReplayCheckpoint(taskId, fingerprint(), 0, 0, ranges);
    }

    /**
     * 业务作用：在接管和恢复前拒绝错连、同名重建、范围漂移和保留缺口。
     * <p>
     * 返回：全部证据一致且剩余范围仍可读时通过，不修改原冻结 end 或存储。
     *
     * @param current     本轮只读 broker 证据
     * @param generations 重新查询的外部代次
     * @param checkpoint  待恢复持久进度。
     */
    public void validateResume(ReplayObservation current, Map<String, String> generations, ReplayCheckpoint checkpoint) {
        if (!clusterId.equals(current.clusterId()) || !topicGenerations.equals(generations))
            throw new ReplayException(ReplayException.Code.IDENTITY_CHANGED);
        if (!initialCheckpoint().sameContract(checkpoint))
            throw new ReplayException(ReplayException.Code.CONTRACT_MISMATCH);
        if (current.partitions().size() != ranges.size())
            throw new ReplayException(ReplayException.Code.PARTITIONS_CHANGED);
        for (int i = 0; i < ranges.size(); i++) {
            ReplayCheckpoint.Position saved = checkpoint.positions().get(i);
            ReplayObservation.Partition observed = current.partitions().get(i);
            if (!saved.topic().equals(observed.topic()) || saved.partition() != observed.partition())
                throw new ReplayException(ReplayException.Code.PARTITIONS_CHANGED);
            // 缺口不能通过自动 earliest 重置掩盖；原 end 也不能因重新规划而悄悄后移。
            if (saved.nextOffset() < observed.beginningOffset())
                throw new ReplayException(ReplayException.Code.DATA_GAP);
            if (saved.endOffset() > observed.stableEndOffset())
                throw new ReplayException(ReplayException.Code.END_REGRESSED);
        }
    }

    /**
     * 业务作用：为共同合同编码提供无歧义的 ASCII 长度前缀。
     * <p>
     * 返回：写入两字节长度及原字节，不执行外部 I/O。
     *
     * @param out   内存编码器
     * @param value 已通过边界校验的文本。
     */
    private static void text(DataOutputStream out, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.US_ASCII);
        out.writeShort(encoded.length);
        out.write(encoded);
    }

    /**
     * 业务作用：在分配身份内容前验证长度和完整性。
     *
     * @param input 受限正文
     * @param limit 该字段最大长度。
     * @return 完整 ASCII 身份或格式拒绝。
     */
    private static String text(DataInputStream input, int limit) throws IOException {
        int length = input.readUnsignedShort();
        if (length == 0 || length > limit) throw new IOException();
        byte[] value = input.readNBytes(length);
        if (value.length != length) throw new IOException();
        for (byte item : value) if (item < 33 || item > 126) throw new IOException();
        return new String(value, StandardCharsets.US_ASCII);
    }

    /**
     * 业务作用：在解析集合前限制资源声明，防止持久字节触发无界分配。
     *
     * @param input 正文读取器。
     * @return 1 到 4096 的条目数，否则拒绝。
     */
    private static int count(DataInputStream input) throws IOException {
        int count = input.readInt();
        if (count < 1 || count > ReplayCheckpoint.MAX_PARTITIONS) throw new IOException();
        return count;
    }

    /**
     * 业务作用：为冻结合同提供确定的完整性摘要。
     *
     * @param bytes 规范正文。
     * @return SHA-256；该值不代替可信存储的写权限。
     */
    private static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("无法摘要回放合同");
        }
    }
}

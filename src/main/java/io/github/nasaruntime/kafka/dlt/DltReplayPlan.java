package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.replay.ReplayPlan;
import io.github.nasaruntime.kafka.transfer.TransferPlan;

import io.github.nasaruntime.kafka.replay.ReplayException;
import io.github.nasaruntime.kafka.transfer.TransferException;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.security.*;
import java.util.*;

/**
 * 冻结 DLT 范围、来源选择、目标和失败隔离；规划不发送消息或修改线上 group 位点。
 */
public record DltReplayPlan(ReplayPlan replay, TransferPlan.Target target, List<TransferPlan.Route> routes,
                            List<String> originGroups, TransferPlan.PayloadPolicy payloadPolicy,
                            InvalidPolicy invalidPolicy, TransferPlan.Partition quarantine, int maxReplays) {

    public static final int MAX_ENCODED_BYTES = 16 * 1024 * 1024;
    private static final Comparator<TransferPlan.Route> ROUTES = Comparator.comparing((TransferPlan.Route r) -> r.source().topic())
            .thenComparingInt(r -> r.source().partition());
    private static final Comparator<String> GROUPS = (left, right) -> Arrays.compareUnsigned(utf8(left), utf8(right));

    /**
     * 非法 DLT 证据的显式处理合同，不包含静默丢弃。
     */
    public enum InvalidPolicy {
        /**
         * 停止并保留失败坐标。
         */
        HALT,
        /**
         * 原文发送到批准的隔离分区，真实确认后才推进。
         */
        QUARANTINE
    }

    /**
     * 业务作用：复验来源选择和输出合同已经绑定到 checkpoint，禁止恢复时静默改规则。
     * <p>
     * 返回：排序不可变计划；未批准目标、重复规则或指纹变化拒绝。
     *
     * @param replay        有限DLT读取范围及其持久处理身份。
     * @param target        批准回投的目标源、集群、通道及分区合同。
     * @param routes        DltEvidence 中原业务 topic/partition 至批准回投分区的映射；不按 DLT 存储分区匹配，未匹配记录按过滤成功推进。
     * @param originGroups  允许处理的原消费组名单，空名单不附加组过滤，最多256项。
     * @param payloadPolicy 原文保留或批准schema ID映射策略。
     * @param invalidPolicy 缺少可信来源、非法头或回投超限时的停止或隔离策略。
     * @param quarantine    错误隔离目标分区；HALT时必须为null，启用隔离时不得与业务回投分区重合。
     * @param maxReplays    同一原文允许的DLT回投次数上限，范围1至16。
     */
    public DltReplayPlan {
        if (replay == null || target == null || routes == null || routes.isEmpty() || routes.size() > 4096
                || routes.stream().anyMatch(Objects::isNull) || originGroups == null || originGroups.size() > 256
                || originGroups.stream().anyMatch(Objects::isNull) || payloadPolicy == null || invalidPolicy == null
                || maxReplays < 1 || maxReplays > 16 || (invalidPolicy == InvalidPolicy.HALT) != (quarantine == null))
            invalid();
        routes = routes.stream().sorted(ROUTES).toList();
        originGroups.forEach(group -> DltEvidence.boundedText(group, 1024, false));
        originGroups = originGroups.stream().sorted(GROUPS).toList();
        if (new HashSet<>(originGroups).size() != originGroups.size()) invalid();
        Set<TransferPlan.Partition> allowed = new HashSet<>(target.partitions());
        Set<String> used = new TreeSet<>();
        for (int i = 0; i < routes.size(); i++) {
            var route = routes.get(i);
            if (i > 0 && route.source().equals(routes.get(i - 1).source()) || !allowed.contains(route.destination()))
                invalid();
            output(replay, target, route.destination());
            used.add(route.destination().topic());
        }
        if (quarantine != null) {
            if (!allowed.contains(quarantine)) invalid();
            output(replay, target, quarantine);
            used.add(quarantine.topic());
            // 隔离输出不能混入业务回投分区，避免错误记录被普通业务入口再次处理。
            for (var route : routes) if (route.destination().equals(quarantine)) invalid();
        }
        if (!used.equals(target.topicGenerations().keySet())) invalid();
        if (!replay.pipelineFingerprint().equals(pipeline(target, routes, originGroups, payloadPolicy, invalidPolicy, quarantine, maxReplays)))
            throw new DltReplayException(DltReplayException.Code.CONTRACT_MISMATCH);
    }

    /**
     * 业务作用：保留只读 DLT 范围，用固定过滤、回投和隔离语义重绑持久合同。
     *
     * @param sourceRanges  本次已冻结的DLT来源范围，保留原资源身份及读取预算。
     * @param target        批准回投的目标源、集群、通道及分区合同。
     * @param routes        DltEvidence 中原业务 topic/partition 至批准回投分区的映射；不按 DLT 存储分区匹配，未匹配记录按过滤成功推进。
     * @param originGroups  允许处理的原消费组名单，空名单不附加组过滤，最多256项。
     * @param payloadPolicy 原文保留或批准schema ID映射策略。
     * @param invalidPolicy 缺少可信来源、非法头或回投超限时的停止或隔离策略。
     * @param quarantine    错误隔离目标分区；HALT时必须为null，启用隔离时不得与业务回投分区重合。
     * @param maxReplays    同一原文允许的DLT回投次数上限，范围1至16。
     * @return 新的执行计划，不沿用旧 handler 的 checkpoint 指纹或取得 lease。
     */
    public static DltReplayPlan freeze(ReplayPlan sourceRanges, TransferPlan.Target target, List<TransferPlan.Route> routes,
                                       List<String> originGroups, TransferPlan.PayloadPolicy payloadPolicy,
                                       InvalidPolicy invalidPolicy, TransferPlan.Partition quarantine, int maxReplays) {
        if (sourceRanges == null || target == null || routes == null || routes.isEmpty() || routes.size() > 4096
                || routes.stream().anyMatch(Objects::isNull) || originGroups == null || originGroups.size() > 256
                || originGroups.stream().anyMatch(Objects::isNull) || payloadPolicy == null || invalidPolicy == null)
            invalid();
        var ordered = routes.stream().sorted(ROUTES).toList();
        originGroups.forEach(group -> DltEvidence.boundedText(group, 1024, false));
        var groups = originGroups.stream().sorted(GROUPS).toList();
        String fingerprint = pipeline(target, ordered, groups, payloadPolicy, invalidPolicy, quarantine, maxReplays);
        var replay = new ReplayPlan(sourceRanges.taskId(), sourceRanges.sourceId(), sourceRanges.clusterId(), sourceRanges.topicGenerations(),
                fingerprint, sourceRanges.limits(), sourceRanges.ranges());
        return new DltReplayPlan(replay, target, ordered, groups, payloadPolicy, invalidPolicy, quarantine, maxReplays);
    }

    /**
     * 业务作用：保存范围与全部处理规则，恢复时不重算移动边界或重新解释 header 路由。
     * <p>
     * 参数说明：无。
     *
     * @return NSKDPL01 规范正文与 SHA-256，最大16MiB；摘要不认证来源。
     */
    public byte[] encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.write("NSKDPL01".getBytes(StandardCharsets.US_ASCII));
            byte[] ranges = replay.encode();
            out.writeInt(ranges.length);
            out.write(ranges);
            out.write(spec(target, routes, originGroups, payloadPolicy, invalidPolicy, quarantine, maxReplays));
            byte[] payload = bytes.toByteArray();
            if (payload.length + 32 > MAX_ENCODED_BYTES) invalid();
            out.write(digest(payload));
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("无法编码 DLT 重放计划");
        }
    }

    /**
     * 业务作用：在执行前恢复并验证原冻结合同，拒绝无界和非规范存储字节。
     *
     * @param encoded 完整计划。
     * @return 只读计划；损坏、截断、尾随内容或处理指纹不符均为 CORRUPT_PLAN。
     */
    public static DltReplayPlan decode(byte[] encoded) {
        if (encoded == null || encoded.length < 44 || encoded.length > MAX_ENCODED_BYTES) corrupt();
        byte[] bytes = encoded.clone(), payload = Arrays.copyOf(bytes, bytes.length - 32);
        if (!MessageDigest.isEqual(digest(payload), Arrays.copyOfRange(bytes, payload.length, bytes.length))) corrupt();
        try {
            var input = new DataInputStream(new ByteArrayInputStream(payload));
            magic(input, "NSKDPL01");
            int length = input.readInt();
            if (length < 40 || length > ReplayPlan.MAX_ENCODED_BYTES || length > input.available()) corrupt();
            ReplayPlan replay = ReplayPlan.decode(input.readNBytes(length));
            magic(input, "NSKDSP01");
            length = input.readInt();
            if (length < 8 || length > MAX_ENCODED_BYTES || length > input.available()) corrupt();
            var targetInput = new DataInputStream(new ByteArrayInputStream(input.readNBytes(length)));
            TransferPlan.Spec target = TransferPlan.readSpec(targetInput);
            if (targetInput.available() != 0) corrupt();
            int count = input.readUnsignedShort();
            if (count > 256) corrupt();
            List<String> groups = new ArrayList<>(count);
            for (int i = 0; i < count; i++) groups.add(text(input, 1024));
            int policy = input.readUnsignedByte();
            if (policy > 1) corrupt();
            TransferPlan.Partition quarantine = policy == 0 ? null : new TransferPlan.Partition(text(input, 249), input.readInt());
            var plan = new DltReplayPlan(replay, target.target(), target.routes(), groups, target.payloadPolicy(), InvalidPolicy.values()[policy], quarantine, target.maxHops());
            if (input.available() != 0 || !Arrays.equals(bytes, plan.encode())) corrupt();
            return plan;
        } catch (IOException | IllegalArgumentException | ReplayException | TransferException |
                 DltReplayException rejected) {
            throw new DltReplayException(DltReplayException.Code.CORRUPT_PLAN);
        }
    }

    /**
     * 业务作用：禁止直接向当前被读取的 DLT topic 回投，避免固定范围任务自产新的死信输入。
     * <p>
     * 返回：不形成同集群输入 topic 自环才通过。
     *
     * @param replay    有限DLT读取范围及其持久处理身份。
     * @param target    批准回投的目标源、集群、通道及分区合同。
     * @param partition 当前待验证的批准输出分区，不得形成同集群DLT输入自环。
     */
    private static void output(ReplayPlan replay, TransferPlan.Target target, TransferPlan.Partition partition) {
        if (replay.clusterId().equals(target.clusterId()) && replay.topicGenerations().containsKey(partition.topic()))
            invalid();
    }

    /**
     * 业务作用：将过滤、schema、错误隔离和回投次数全部绑定到处理身份。
     *
     * @param target        批准回投的目标源、集群、通道及分区合同。
     * @param routes        原业务来源坐标至批准回投分区的有序映射，来源坐标取自 DltEvidence。
     * @param groups        编码到处理指纹中的原消费组过滤名单。
     * @param payloadPolicy 原文保留或批准schema ID映射策略。
     * @param invalidPolicy 缺少可信来源、非法头或回投超限时的停止或隔离策略。
     * @param quarantine    错误隔离目标分区；HALT时必须为null，启用隔离时不得与业务回投分区重合。
     * @param maxReplays    同一原文允许的DLT回投次数上限，范围1至16。
     * @return 共同规范正文的 SHA-256 小写文本。
     */
    private static String pipeline(TransferPlan.Target target, List<TransferPlan.Route> routes, List<String> groups,
                                   TransferPlan.PayloadPolicy payloadPolicy, InvalidPolicy invalidPolicy, TransferPlan.Partition quarantine, int maxReplays) {
        return HexFormat.of().formatHex(digest(spec(target, routes, groups, payloadPolicy, invalidPolicy, quarantine, maxReplays)));
    }

    /**
     * 业务作用：生成与任务 ID 无关的唯一处理语义字节，不能遗漏会改变输出的规则。
     *
     * @param target        批准回投的目标源、集群、通道及分区合同。
     * @param routes        原业务来源坐标至批准回投分区的有序映射，来源坐标取自 DltEvidence。
     * @param groups        编码到处理指纹中的原消费组过滤名单。
     * @param payloadPolicy 原文保留或批准schema ID映射策略。
     * @param invalidPolicy 缺少可信来源、非法头或回投超限时的停止或隔离策略。
     * @param quarantine    错误隔离目标分区；HALT时必须为null，启用隔离时不得与业务回投分区重合。
     * @param maxReplays    同一原文允许的DLT回投次数上限，范围1至16。
     * @return NSKDSP01 正文，目标部分复用转发共同格式。
     */
    private static byte[] spec(TransferPlan.Target target, List<TransferPlan.Route> routes, List<String> groups,
                               TransferPlan.PayloadPolicy payloadPolicy, InvalidPolicy invalidPolicy, TransferPlan.Partition quarantine, int maxReplays) {
        if (maxReplays < 1 || maxReplays > 16 || (invalidPolicy == InvalidPolicy.HALT) != (quarantine == null))
            invalid();
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.write("NSKDSP01".getBytes(StandardCharsets.US_ASCII));
            byte[] common = TransferPlan.spec(target, routes, payloadPolicy, maxReplays);
            out.writeInt(common.length);
            out.write(common);
            out.writeShort(groups.size());
            for (String group : groups) text(out, group);
            out.writeByte(invalidPolicy.ordinal());
            if (quarantine != null) {
                text(out, quarantine.topic());
                out.writeInt(quarantine.partition());
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("无法编码 DLT 处理合同");
        } catch (TransferException rejected) {
            throw new DltReplayException(DltReplayException.Code.INVALID_CONFIGURATION);
        }
    }

    /**
     * 业务作用：为文本排序和字节序采用唯一 UTF-8 编码。
     *
     * @param value 已校验文本。
     * @return 独立 UTF-8 字节。
     */
    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 业务作用：写入有界 UTF-8 身份，避免字符串拼接歧义。
     * <p>
     * 返回：大端 u16 长度及原文。
     *
     * @param out   输出流
     * @param value 已验证文本。
     */
    private static void text(DataOutputStream out, String value) throws IOException {
        byte[] bytes = utf8(value);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    /**
     * 业务作用：在分配文本前限制长度并拒绝无效 UTF-8。
     *
     * @param input 正文
     * @param max   字节上限。
     * @return 非空完整文本。
     */
    private static String text(DataInputStream input, int max) throws IOException {
        int length = input.readUnsignedShort();
        if (length == 0 || length > max) corrupt();
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) corrupt();
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
    }

    /**
     * 业务作用：阻止其它任务格式被解释为 DLT 重放计划。
     * <p>
     * 返回：完全一致才继续。
     *
     * @param input    正文
     * @param expected 协议标识。
     */
    private static void magic(DataInputStream input, String expected) throws IOException {
        if (!Arrays.equals(input.readNBytes(8), expected.getBytes(StandardCharsets.US_ASCII))) corrupt();
    }

    /**
     * 业务作用：为共同规范正文计算完整性摘要。
     *
     * @param bytes 正文。
     * @return SHA-256，不承担消息来源认证。
     */
    private static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("无法摘要 DLT 合同");
        }
    }

    /**
     * 业务作用：在任何副作用前拒绝非法声明。
     * <p>
     * 参数说明：无。
     * 返回：固定配置异常。
     */
    private static void invalid() {
        throw new DltReplayException(DltReplayException.Code.INVALID_CONFIGURATION);
    }

    /**
     * 业务作用：拒绝不可信的计划归档。
     * <p>
     * 参数说明：无。
     * 返回：固定计划损坏异常。
     */
    private static void corrupt() {
        throw new DltReplayException(DltReplayException.Code.CORRUPT_PLAN);
    }
}

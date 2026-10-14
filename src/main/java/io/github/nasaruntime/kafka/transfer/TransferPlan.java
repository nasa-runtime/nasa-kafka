package io.github.nasaruntime.kafka.transfer;

import io.github.nasaruntime.kafka.checkpoint.ReplayCheckpoint;
import io.github.nasaruntime.kafka.replay.ReplayObservation;
import io.github.nasaruntime.kafka.replay.ReplayPlan;

import io.github.nasaruntime.kafka.checkpoint.CheckpointException;
import io.github.nasaruntime.kafka.replay.ReplayException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/**
 * 冻结单目标 at-least-once 转发合同；计划本身不发送消息，也不授予任务执行权。
 */
public record TransferPlan(ReplayPlan replay, Target target, List<Route> routes, PayloadPolicy payloadPolicy,
                           int maxHops) {

    public static final int MAX_ENCODED_BYTES = 16 * 1024 * 1024;
    private static final Comparator<Partition> PARTITIONS = Comparator.comparing(Partition::topic).thenComparingInt(Partition::partition);
    private static final Comparator<Route> ROUTES = Comparator.comparing(Route::source, PARTITIONS);

    /**
     * Kafka 资源坐标，明确分区映射，不依赖目标 partitioner 的运行期选择。
     */
    public record Partition(String topic, int partition) {
        /**
         * 业务作用：限制映射坐标到合法 Kafka topic 和非负分区。
         * <p>
         * 返回：合法固定坐标；不验证远端资源是否存在。
         *
         * @param topic     固定路由坐标所属的topic名称。
         * @param partition 固定路由坐标的非负分区号。
         */
        public Partition {
            try {
                new ReplayCheckpoint.Position(topic, partition, 0, 0, 0);
            } catch (CheckpointException failure) {
                invalid();
            }
        }
    }

    /**
     * 一条来源分区到目标分区的固定路由；多条来源汇入目标不产生跨来源总顺序。
     */
    public record Route(Partition source, Partition destination) {
        /**
         * 业务作用：绑定控制面批准的来源与目标坐标，消息头不能更换路由。
         * <p>
         * 返回：完整路由。
         *
         * @param source      计划内坐标
         * @param destination 目标白名单坐标。
         */
        public Route {
            if (source == null || destination == null) invalid();
        }
    }

    /**
     * 目标完整资源身份；offset 不属于目标身份，因为目标日志会持续增长。
     */
    public record Target(String sourceId, String clusterId, String lane, Map<String, String> topicGenerations,
                         List<Partition> partitions) {
        /**
         * 业务作用：固定目标逻辑源、物理源、独立 lane 及 topic 代次/分区集合。
         * <p>
         * 返回：排序且不可变的目标合同；缺项、重复或非法声明拒绝。
         *
         * @param sourceId         目标发布能力所属的命名数据源。
         * @param clusterId        目标Kafka集群的冻结物理身份。
         * @param lane             执行转发所使用的固定目标producer通道名称。
         * @param topicGenerations 目标topic的当前资源代次目录证据。
         * @param partitions       所选目标topic的完整批准分区集合，不能仅列单条路由用到的分区。
         */
        public Target {
            token(sourceId, 128);
            token(clusterId, 1024);
            if (lane == null || !lane.matches("[a-z][a-z0-9_-]{0,63}") || topicGenerations == null || partitions == null
                    || partitions.isEmpty() || partitions.size() > ReplayCheckpoint.MAX_PARTITIONS || partitions.stream().anyMatch(Objects::isNull))
                invalid();
            partitions = partitions.stream().sorted(PARTITIONS).toList();
            if (new HashSet<>(partitions).size() != partitions.size()) invalid();
            Set<String> topics = new TreeSet<>();
            partitions.forEach(partition -> topics.add(partition.topic()));
            if (!topics.equals(topicGenerations.keySet())) invalid();
            TreeMap<String, String> generations = new TreeMap<>();
            topicGenerations.forEach((topic, generation) -> {
                token(generation, 1024);
                generations.put(topic, generation);
            });
            topicGenerations = Collections.unmodifiableMap(generations);
        }

        /**
         * 业务作用：从只读观测中提取目标资源身份，排除不断移动的目标 offset。
         *
         * @param sourceId    目标发布能力所属的命名数据源。
         * @param lane        执行转发所使用的固定目标producer通道名称。
         * @param observation 完整目标观测
         * @param generations 当前外部代次。
         * @return 不发送消息的冻结目标。
         */
        public static Target observed(String sourceId, String lane, ReplayObservation observation, Map<String, String> generations) {
            if (observation == null) invalid();
            return new Target(sourceId, observation.clusterId(), lane, generations,
                    observation.partitions().stream().map(partition -> new Partition(partition.topic(), partition.partition())).toList());
        }

        /**
         * 业务作用：拒绝错连、同名资源重建及目标扩容，不因目标日志增长而改变映射。
         * <p>
         * 返回：物理身份、代次及完整分区集合全部一致时通过。
         *
         * @param current     新观测
         * @param generations 重新取得的外部目录证据。
         */
        public void validateCurrent(ReplayObservation current, Map<String, String> generations) {
            if (current == null || !clusterId.equals(current.clusterId()) || !topicGenerations.equals(generations))
                throw new TransferException(TransferException.Code.TARGET_IDENTITY_CHANGED);
            if (!partitions.equals(current.partitions().stream().map(partition -> new Partition(partition.topic(), partition.partition())).toList()))
                throw new TransferException(TransferException.Code.TARGET_PARTITIONS_CHANGED);
        }
    }

    /**
     * 原文策略必须由控制面显式选择；不能通过猜测首字节决定是否跨 Registry 搬运。
     */
    public enum PayloadKind {OPAQUE, CONFLUENT_ID_MAP}

    /**
     * Confluent schema 类别；只接受正文一致的 ID 映射，不隐式执行 schema 演进。
     */
    public enum SchemaKind {AVRO, PROTOBUF, JSON}

    /**
     * 来源和目标 schema ID 及共同正文证据。
     */
    public record SchemaMapping(int sourceId, int destinationId, SchemaKind kind, String schemaFingerprint) {
        /**
         * 业务作用：固定经批准的跨 Registry ID 对应与原 schema 正文摘要。
         * <p>
         * 返回：合法声明；不自动注册或转换 schema。
         *
         * @param sourceId          来源Registry中批准读取的正整数schema ID。
         * @param destinationId     目标Registry中批准使用的正整数schema ID。
         * @param kind              来源与目标共同声明的schema类别。
         * @param schemaFingerprint 来源和目标共享schema正文UTF-8字节的SHA-256指纹。
         */
        public SchemaMapping {
            if (sourceId <= 0 || destinationId <= 0 || kind == null || !fingerprint(schemaFingerprint)) invalid();
        }
    }

    /**
     * OPAQUE 保留原文；Confluent 模式只更换经过双端 schema 复验的四字节 ID。
     */
    public record PayloadPolicy(PayloadKind kind, String sourceRegistry, String destinationRegistry,
                                List<SchemaMapping> schemas) {
        /**
         * 业务作用：拒绝无明确 Registry 身份或有歧义的 schema 映射。
         * <p>
         * 返回：排序不可变策略；OPAQUE 的两个 Registry 名必须为空字符串且无映射，所有模式均不接受 null 字段。
         *
         * @param kind                原文透传或Confluent schema ID映射的处理类别。
         * @param sourceRegistry      映射模式下的来源 Registry 稳定逻辑身份；OPAQUE 时为 ""。
         * @param destinationRegistry 映射模式下的目标 Registry 稳定逻辑身份；OPAQUE 时为 ""。
         * @param schemas             全部批准的来源至目标schema ID映射。
         */
        public PayloadPolicy {
            if (kind == null || sourceRegistry == null || destinationRegistry == null || schemas == null || schemas.size() > 4096 || schemas.stream().anyMatch(Objects::isNull))
                invalid();
            schemas = schemas.stream().sorted(Comparator.comparingInt(SchemaMapping::sourceId)).toList();
            if (kind == PayloadKind.OPAQUE) {
                if (!sourceRegistry.isEmpty() || !destinationRegistry.isEmpty() || !schemas.isEmpty()) invalid();
            } else {
                token(sourceRegistry, 128);
                token(destinationRegistry, 128);
                if (schemas.isEmpty()) invalid();
                for (int i = 1; i < schemas.size(); i++)
                    if (schemas.get(i - 1).sourceId() == schemas.get(i).sourceId()) invalid();
            }
        }

        /**
         * 业务作用：声明字节的业务含义由调用方承担，框架不把源 Registry ID 自动解释为目标 ID。
         * <p>
         * 参数说明：无。
         *
         * @return 原文保留策略；用于 schema envelope 时调用方必须显式改用映射策略。
         */
        public static PayloadPolicy opaque() {
            return new PayloadPolicy(PayloadKind.OPAQUE, "", "", List.of());
        }
    }

    /**
     * 业务作用：复验持久转发合同与内部 Replay 处理摘要一致。
     * <p>
     * 返回：不可变合同；重复/缺失路由、未批准目标或摘要变化拒绝。
     *
     * @param replay        已绑定转发摘要的范围
     * @param target        目标源、集群、lane、topic 代次及完整目标分区合同。
     * @param routes        覆盖每个冻结来源分区的固定目标映射，目标必须属于批准分区集合。
     * @param payloadPolicy 显式正文策略
     * @param maxHops       1到16的链路上限。
     */
    public TransferPlan {
        if (replay == null || target == null || routes == null || payloadPolicy == null || maxHops < 1 || maxHops > 16
                || routes.size() != replay.ranges().size() || routes.stream().anyMatch(Objects::isNull)) invalid();
        routes = routes.stream().sorted(ROUTES).toList();
        Set<Partition> allowed = new HashSet<>(target.partitions());
        Set<String> usedTopics = new TreeSet<>();
        for (int i = 0; i < routes.size(); i++) {
            Route route = routes.get(i);
            ReplayCheckpoint.Position range = replay.ranges().get(i);
            if (!route.source().equals(new Partition(range.topic(), range.partition())) || !allowed.contains(route.destination()))
                invalid();
            usedTopics.add(route.destination().topic());
        }
        if (!usedTopics.equals(target.topicGenerations().keySet())) invalid();
        if (!replay.pipelineFingerprint().equals(pipeline(target, routes, payloadPolicy, maxHops)))
            throw new TransferException(TransferException.Code.CONTRACT_MISMATCH);
    }

    /**
     * 业务作用：保留只读来源范围，改用内置转发成功条件绑定 checkpoint 合同。
     *
     * @param sourceRanges  已冻结的来源读取范围，将绑定新的目标处理指纹。
     * @param target        包含目标源、物理集群、通道、topic代次和完整分区的冻结合同。
     * @param routes        每个来源分区至批准目标分区的固定映射。
     * @param payloadPolicy 原文透传或Confluent schema ID映射的固定处理策略。
     * @param maxHops       允许的转发跳数上限，范围1至16。
     * @return 新的转发合同；不沿用原范围计划的 handler 指纹，已有不同 checkpoint 将明确冲突。
     */
    public static TransferPlan freeze(ReplayPlan sourceRanges, Target target, List<Route> routes, PayloadPolicy payloadPolicy, int maxHops) {
        if (sourceRanges == null || target == null || routes == null || payloadPolicy == null || routes.size() != sourceRanges.ranges().size() || routes.stream().anyMatch(Objects::isNull))
            invalid();
        List<Route> ordered = routes.stream().sorted(ROUTES).toList();
        String pipeline = pipeline(target, ordered, payloadPolicy, maxHops);
        ReplayPlan rebound = new ReplayPlan(sourceRanges.taskId(), sourceRanges.sourceId(), sourceRanges.clusterId(), sourceRanges.topicGenerations(),
                pipeline, sourceRanges.limits(), sourceRanges.ranges());
        return new TransferPlan(rebound, target, ordered, payloadPolicy, maxHops);
    }

    /**
     * 业务作用：保存完整单目标任务，恢复不重算来源 latest 或目标分区映射。
     * <p>
     * 参数说明：无。
     *
     * @return NSKTPL01 规范正文与 SHA-256 尾部，最大16MiB，摘要不承担认证。
     */
    public byte[] encode() {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.write("NSKTPL01".getBytes(StandardCharsets.US_ASCII));
            byte[] range = replay.encode();
            out.writeInt(range.length);
            out.write(range);
            out.write(spec(target, routes, payloadPolicy, maxHops));
            byte[] payload = bytes.toByteArray();
            if (payload.length + 32 > MAX_ENCODED_BYTES) invalid();
            out.write(digest(payload));
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("无法编码转发计划");
        }
    }

    /**
     * 业务作用：从受控持久字节恢复原合同，拒绝损坏、截断、非规范编码及无限集合。
     *
     * @param encoded 最大16MiB完整编码。
     * @return 只读计划；任何格式或合同错误均为 CORRUPT_PLAN，不取得执行权。
     */
    public static TransferPlan decode(byte[] encoded) {
        if (encoded == null || encoded.length < 44 || encoded.length > MAX_ENCODED_BYTES) corrupt();
        byte[] bytes = encoded.clone(), payload = Arrays.copyOf(bytes, bytes.length - 32);
        if (!MessageDigest.isEqual(digest(payload), Arrays.copyOfRange(bytes, payload.length, bytes.length))) corrupt();
        try {
            var input = new DataInputStream(new ByteArrayInputStream(payload));
            magic(input, "NSKTPL01");
            int length = input.readInt();
            if (length < 40 || length > ReplayPlan.MAX_ENCODED_BYTES || length > input.available()) corrupt();
            ReplayPlan replay = ReplayPlan.decode(input.readNBytes(length));
            Spec spec = readSpec(input);
            TransferPlan plan = new TransferPlan(replay, spec.target(), spec.routes(), spec.payloadPolicy(), spec.maxHops());
            if (input.available() != 0 || !Arrays.equals(bytes, plan.encode())) corrupt();
            return plan;
        } catch (IOException | IllegalArgumentException | ReplayException | TransferException failure) {
            throw new TransferException(TransferException.Code.CORRUPT_PLAN);
        }
    }

    /**
     * 目标合同的内部解析结果，供具有不同来源选择规则的有限任务共用。
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     */
    public record Spec(Target target, List<Route> routes, PayloadPolicy payloadPolicy, int maxHops) {}

    /**
     * 业务作用：共用目标身份、路由和 schema 合同的有界解析，不授予来源执行权限。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param input 已限长的正文流。
     * @return 已校验字段，来源范围绑定及完整规范编码由外层计划继续核对。
     */
    public static Spec readSpec(DataInputStream input) throws IOException {
        magic(input, "NSKTSP01");
        String source = text(input, 128, false), cluster = text(input, 1024, false), lane = text(input, 64, false);
        int hops = input.readUnsignedByte();
        Map<String, String> generations = new TreeMap<>();
        int count = count(input, false);
        for (int i = 0; i < count; i++)
            if (generations.put(text(input, 249, false), text(input, 1024, false)) != null) corrupt();
        count = count(input, false);
        List<Partition> partitions = new ArrayList<>(count);
        for (int i = 0; i < count; i++) partitions.add(partition(input));
        count = count(input, false);
        List<Route> routes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) routes.add(new Route(partition(input), partition(input)));
        int kind = input.readUnsignedByte();
        if (kind > 1) corrupt();
        String sourceRegistry = text(input, 128, true), destinationRegistry = text(input, 128, true);
        count = count(input, true);
        List<SchemaMapping> schemas = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int from = input.readInt(), to = input.readInt(), schemaKind = input.readUnsignedByte();
            byte[] hash = input.readNBytes(32);
            if (schemaKind < 1 || schemaKind > 3 || hash.length != 32) corrupt();
            schemas.add(new SchemaMapping(from, to, SchemaKind.values()[schemaKind - 1], HexFormat.of().formatHex(hash)));
        }
        return new Spec(new Target(source, cluster, lane, generations, partitions), routes,
                new PayloadPolicy(PayloadKind.values()[kind], sourceRegistry, destinationRegistry, schemas), hops);
    }

    /**
     * 业务作用：把目的地与处理策略纳入来源持久进度合同，阻止恢复时静默换目标。
     *
     * @param target 包含目标源、物理集群、通道、topic代次和完整分区的冻结合同。
     * @param routes 每个来源分区至批准目标分区的固定映射。
     * @param policy 编码到处理指纹中的原文及schema映射规则。
     * @param hops   持久处理合同中的转发跳数上限。
     * @return 共同规范正文的 SHA-256 十六进制。
     */
    private static String pipeline(Target target, List<Route> routes, PayloadPolicy policy, int hops) {
        return HexFormat.of().formatHex(digest(spec(target, routes, policy, hops)));
    }

    /**
     * 业务作用：为内置转发成功条件提供独立规范编码。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param target 包含目标源、物理集群、通道、topic代次和完整分区的冻结合同。
     * @param routes 每个来源分区至批准目标分区的固定映射。
     * @param policy 编码到处理指纹中的原文及schema映射规则。
     * @param hops   持久处理合同中的转发跳数上限。
     * @return 有序、大端且有长度边界的语义正文。
     */
    public static byte[] spec(Target target, List<Route> routes, PayloadPolicy policy, int hops) {
        if (hops < 1 || hops > 16 || routes.size() > 4096) invalid();
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            out.write("NSKTSP01".getBytes(StandardCharsets.US_ASCII));
            text(out, target.sourceId());
            text(out, target.clusterId());
            text(out, target.lane());
            out.writeByte(hops);
            out.writeInt(target.topicGenerations().size());
            for (var entry : target.topicGenerations().entrySet()) {
                text(out, entry.getKey());
                text(out, entry.getValue());
            }
            out.writeInt(target.partitions().size());
            for (var partition : target.partitions()) partition(out, partition);
            out.writeInt(routes.size());
            for (var route : routes) {
                partition(out, route.source());
                partition(out, route.destination());
            }
            out.writeByte(policy.kind().ordinal());
            text(out, policy.sourceRegistry());
            text(out, policy.destinationRegistry());
            out.writeInt(policy.schemas().size());
            for (var schema : policy.schemas()) {
                out.writeInt(schema.sourceId());
                out.writeInt(schema.destinationId());
                out.writeByte(schema.kind().ordinal() + 1);
                out.write(HexFormat.of().parseHex(schema.schemaFingerprint()));
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("无法编码转发处理合同");
        }
    }

    /**
     * 业务作用：规范编码显式分区坐标。
     * <p>
     * 返回：长度前缀 topic 与大端分区号。
     *
     * @param out       内存编码器
     * @param partition 合法坐标。
     */
    private static void partition(DataOutputStream out, Partition partition) throws IOException {
        text(out, partition.topic());
        out.writeInt(partition.partition());
    }

    /**
     * 业务作用：在解析路由时立即拒绝非法坐标。
     *
     * @param input 有界正文。
     * @return 完整合法分区。
     */
    private static Partition partition(DataInputStream input) throws IOException {
        return new Partition(text(input, 249, false), input.readInt());
    }

    /**
     * 业务作用：编码已校验的 ASCII 身份；OPAQUE 的 Registry 字段使用空字符串，占零字节正文，不能传入 null。
     * <p>
     * 返回：两字节长度与原文本。
     *
     * @param out   内存编码器
     * @param value 文本。
     */
    private static void text(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    /**
     * 业务作用：阻止外部计划通过无界文本或非法身份改变路由合同。
     *
     * @param input 有界正文
     * @param max   上限
     * @param empty true 允许零长度文本，false 要求至少一个字节；返回值不使用 null。
     * @return 完整 ASCII 文本。
     */
    private static String text(DataInputStream input, int max, boolean empty) throws IOException {
        int size = input.readUnsignedShort();
        if (size > max || !empty && size == 0) corrupt();
        byte[] bytes = input.readNBytes(size);
        if (bytes.length != size) corrupt();
        for (byte value : bytes) if (value < 33 || value > 126) corrupt();
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    /**
     * 业务作用：在分配计划集合前限制数量。
     *
     * @param input 正文
     * @param empty 表示该集合允许零条。
     * @return 不超过4096的条目数。
     */
    private static int count(DataInputStream input, boolean empty) throws IOException {
        int count = input.readInt();
        if (count < (empty ? 0 : 1) || count > 4096) corrupt();
        return count;
    }

    /**
     * 业务作用：拒绝不同协议被当作转发合同解析。
     * <p>
     * 返回：标识一致才继续。
     *
     * @param input    正文
     * @param expected 八字节协议标识。
     */
    private static void magic(DataInputStream input, String expected) throws IOException {
        if (!Arrays.equals(input.readNBytes(8), expected.getBytes(StandardCharsets.US_ASCII))) corrupt();
    }

    /**
     * 业务作用：限制逻辑和物理身份到共同编码。
     * <p>
     * 返回：可见 ASCII 身份有效。
     *
     * @param value 身份
     * @param max   最大长度。
     */
    private static void token(String value, int max) {
        if (value == null || value.isEmpty() || value.length() > max || value.chars().anyMatch(c -> c < 33 || c > 126))
            invalid();
    }

    /**
     * 业务作用：确认指纹是规范 SHA-256 表达。
     *
     * @param value 指纹。
     * @return 64位小写十六进制时为true。
     */
    private static boolean fingerprint(String value) {
        return value != null && value.matches("[0-9a-f]{64}");
    }

    /**
     * 业务作用：对共同处理合同及完整归档计算确定摘要。
     *
     * @param bytes 规范正文。
     * @return SHA-256，不承担内容来源认证。
     */
    private static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("无法摘要转发合同");
        }
    }

    /**
     * 业务作用：在外部副作用前拒绝不完整转发声明。
     * <p>
     * 参数说明：无。
     * 返回：固定配置异常。
     */
    private static void invalid() {
        throw new TransferException(TransferException.Code.INVALID_CONFIGURATION);
    }

    /**
     * 业务作用：拒绝持久合同中不可信或不规范的证据。
     * <p>
     * 参数说明：无。
     * 返回：固定计划损坏异常。
     */
    private static void corrupt() {
        throw new TransferException(TransferException.Code.CORRUPT_PLAN);
    }
}

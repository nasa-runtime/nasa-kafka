package io.github.nasaruntime.kafka.config;

import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.protocol.KafkaWire;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

import com.fasterxml.jackson.databind.DeserializationFeature;

/**
 * 单源配置的只读迁移检查；生成候选与人工决策项，不建立连接或改变 broker 状态。
 */
public final class KafkaMigration {

    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);
    private static final Map<String, Object> FLAT_DEFAULTS = flatDefaults();

    /**
     * 调用方提供的完整业务路由身份；广播应填写实际沿用的 group，而非重新派生。
     */
    public record Route(String topic, String event, String group) {
        /**
         * 业务作用：冻结迁移前的事件与消费位点归属。
         * <p>
         * 返回：有效声明；缺少身份、组不满足共同输入边界或事件不能精确编码为 UTF-8 时拒绝，不能猜测另一条路由。
         *
         * @param topic 旧路由解析后实际订阅的topic。
         * @param event 旧路由实际匹配的事件名。
         * @param group 旧路由实际使用的消费组身份。
         */
        public Route {
            KafkaWire.eventBytes(event);
            KafkaWire.validateGroup(group);
            if (topic == null || topic.isBlank() || topic.indexOf('\0') >= 0 || event.indexOf('\0') >= 0 || group == null || group.isBlank() || group.indexOf('\0') >= 0)
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "迁移路由须提供完整 topic、event 与 group");
        }
    }

    /**
     * 默认差异独立于用户显式配置；候选保留实际配置，不自动启用更强可靠性。
     */
    public record DefaultChange(String field, Object previousDefault, Object currentDefault, Object candidateValue,
                                boolean explicitlyConfigured) {}

    /**
     * 固定语义的人工决策项；blocking 为真时不能把候选当作可用配置。
     */
    public record Finding(String code, boolean blocking, String detail) {}

    /**
     * 脱敏后的候选与路由目录；通过本地校验不证明业务等价、broker 权限或滚动兼容。
     */
    public record Report(String sourceId, Map<String, Object> candidate, List<Route> routes,
                         List<DefaultChange> defaultChanges, List<Finding> findings, boolean locallyValid) {
        /**
         * 业务作用：封存检查结果，避免调用方修改后仍沿用旧校验结论。
         * <p>
         * 返回：深度不可变的报告；输入脱敏由 inspectLegacy 执行。
         *
         * @param sourceId       待检查候选配置归属的命名源。
         * @param candidate      转换得到的候选配置，只做本地检查，不写入broker。
         * @param routes         调用方提供并冻结的旧消费路由清单。
         * @param defaultChanges 旧默认值、当前默认值及显式声明的逐项差异。
         * @param findings       本地检查发现的稳定分类和阻断说明。
         * @param locallyValid   候选配置是否通过本地结构与默认值检查，不表示远端连接可用。
         */
        public Report {
            candidate = immutable(candidate);
            routes = List.copyOf(routes);
            defaultChanges = List.copyOf(defaultChanges);
            findings = List.copyOf(findings);
        }
    }

    /**
     * 业务作用：禁止创建无状态检查器实例，确保入口不承载资源寿命。
     * <p>
     * 参数说明：无。
     * 返回：仅用于阻止实例化。
     */
    private KafkaMigration() {}

    /**
     * 业务作用：把扁平单源配置转为显式多源候选，保留原生参数并列出无法自动等价迁移的业务合同。
     *
     * @param sourceId 目标规范源名
     * @param legacy   nasa.kafka 下的扁平字段，支持 camelCase、kebab-case、snake_case
     * @param routes   调用方提供的完整既有路由。
     * @return 脱敏候选、本地校验、默认差异和必读决策项；未知或重复字段拒绝。候选保留单源默认 acks=1、幂等关闭和五个在途请求，不自动改组位点或创建资源。
     */
    public static Report inspectLegacy(String sourceId, Map<String, ?> legacy, Collection<Route> routes) {
        if (sourceId == null || !sourceId.matches("[a-z][a-z0-9_-]{0,63}") || legacy == null || routes == null)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "迁移来源、配置或路由目录无效");
        Map<String, String> known = new HashMap<>();
        FLAT_DEFAULTS.keySet().forEach(key -> known.put(normalize(key), key));
        Map<String, Object> supplied = new LinkedHashMap<>();
        for (var entry : legacy.entrySet()) {
            String canonical = known.get(normalize(entry.getKey()));
            if (canonical == null || supplied.containsKey(canonical))
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "迁移输入含未知或重复扁平字段");
            supplied.put(canonical, entry.getValue());
        }
        Map<String, Object> source = JSON.convertValue(FLAT_DEFAULTS, new TypeReference<>() {
        });
        source.putAll(JSON.convertValue(supplied, new TypeReference<Map<String, Object>>() {
        }));
        var findings = new ArrayList<Finding>();
        if (!sourceId.equals(KafkaProxy.PRIMARY))
            findings.add(new Finding("SOURCE_ID_CHANGED", false, "目标源名须同步更新显式 listener 来源和业务查找；group 不自动改名。"));
        // 读取策略只保留允许表达的兼容项；严格 UTF-8、非法事件和安全提交边界仍由当前合同裁决。
        source.put("strictHeaders", false);
        source.put("legacyEnvelope", true);
        source.put("adminWrite", false);
        findings.add(new Finding("DEFAULT_PARTITION_EVENT", false, "省略 event 的 partition 使用 DEFAULT；沿用 topic 事件时显式调用 partitionLegacy 或提供 event。"));
        findings.add(new Finding("STRICT_HEADERS", false, "候选显式选择保留头取末值；仍拒绝非法 UTF-8、null 或空事件。全部生产者规范化后再开启 strict-headers。"));
        findings.add(new Finding("LEGACY_ENVELOPE", false, "候选启用 legacy-envelope；包含同形字段的普通 DTO 必须核对真实线路，不能仅凭字段名证明等价。"));
        findings.add(new Finding("BOUNDED_EXECUTION", false, "输入、worker、DLT 和发布责任采用当前有界准入与安全水位；容量及失败时序需按业务负载确认。"));
        findings.add(new Finding("PRODUCER_ORDERING", false, "非幂等重试要求 max-inflight-requests-per-connection=1；保留的单源默认组合可能不满足此约束，须显式选择串行重试或幂等可靠性配置。"));
        findings.add(new Finding("MANAGED_ADMIN_READ_ONLY", false, "业务应注入 KafkaHandle；默认 Admin 只读，管理写入须按源显式授权。"));
        findings.add(new Finding("ROUTE_MEMBERSHIP", false, "保留完整 group 身份；广播切换算法不自动继承位点。所有组成员先具备新增 event 路由，再允许发布该事件。"));
        Object attempts = source.get("maxConsumeAttempts");
        if (attempts instanceof Number number && number.longValue() == 0 || "0".equals(attempts))
            findings.add(new Finding("RETRY_ZERO", false, "max-consume-attempts=0 表示无限业务重试；不保留在同一会话跳过失败输入的行为。"));
        Map<String, Object> current = JSON.convertValue(new NasaKafkaProperties(), new TypeReference<>() {
        });
        var changes = new ArrayList<DefaultChange>();
        for (String field : List.of("acks", "enableIdempotence", "maxInflightRequestsPerConnection"))
            changes.add(new DefaultChange(field, FLAT_DEFAULTS.get(field), current.get(field), source.get(field) instanceof Map<?, ?> || source.get(field) instanceof Collection<?> ? "<invalid>" : source.get(field), supplied.containsKey(field)));
        boolean valid;
        try {
            // 只解析、冻结和执行本地配置约束，不创建 proxy、producer、consumer 或 Admin。
            JSON.convertValue(source, NasaKafkaProperties.class).effective(sourceId);
            valid = true;
        } catch (RuntimeException invalid) {
            valid = false;
            findings.add(new Finding("CONFIG_INVALID", true, "候选不满足当前本地配置约束；单独校验原配置后再部署，报告不回显失败输入。"));
        }
        List<String> redacted = new ArrayList<>();
        Map<String, Object> safe = redact(grouped(source), "", redacted);
        if (!redacted.isEmpty())
            findings.add(new Finding("SECRETS_REDACTED", true, "候选中的 <redacted> 必须由部署环境重新绑定；包括凭据、证书路径和原生 properties 值。"));
        Map<String, Object> candidate = Map.of("nasa", Map.of("kafka", Map.of("default-source", sourceId, "sources", Map.of(sourceId, safe))));
        return new Report(sourceId, candidate, new ArrayList<>(routes), changes, findings, valid);
    }

    /**
     * 业务作用：读取扁平单源配置格式的固定缺省值，避免检查器随当前默认值漂移。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变格式表；产品资源缺失或无法解析时拒绝检查。
     */
    private static Map<String, Object> flatDefaults() {
        try (var input = KafkaMigration.class.getResourceAsStream("flat-source-defaults.json")) {
            if (input == null) throw new IOException("缺少配置格式资源");
            return immutable(JSON.readValue(input, new TypeReference<Map<String, Object>>() {
            }));
        } catch (IOException failure) {
            throw new ExceptionInInitializerError("Kafka 迁移配置格式不可用");
        }
    }

    /**
     * 业务作用：统一配置字段的拼写形式但不隐式选择重复声明。
     *
     * @param name 输入字段名。
     * @return 仅供同名检测的规范比较值，不改变源名或业务值。
     */
    private static String normalize(String name) {
        return name == null ? "" : name.replace("-", "").replace("_", "").toLowerCase(Locale.ROOT);
    }

    /**
     * 业务作用：把可表达的扁平参数移入对应角色，沿用装配器的精确映射。
     *
     * @param source 独立候选。
     * @return 按 producer、consumer、admin、behavior、security 分组的配置，尚无嵌套属性的兼容字段保持显式。
     */
    private static Map<String, Object> grouped(Map<String, Object> source) {
        Map<String, Object> result = new LinkedHashMap<>(source);
        Map<String, Object> roles = new LinkedHashMap<>();
        roles.put("producer", new KafkaSettings.Producer());
        roles.put("consumer", new KafkaSettings.Consumer());
        roles.put("admin", new KafkaSettings.Admin());
        roles.put("behavior", new KafkaSettings.Behavior());
        roles.put("security", new KafkaSettings.Security());
        roles.forEach((role, prototype) -> {
            Map<String, Object> fields = JSON.convertValue(prototype, new TypeReference<>() {
            });
            Map<String, Object> values = new LinkedHashMap<>();
            for (String name : fields.keySet()) {
                String original = KafkaSettings.destination(name, role);
                if (result.containsKey(original)) values.put(name, result.remove(original));
            }
            result.put(role, values);
        });
        return result;
    }

    /**
     * 业务作用：在导出候选前移除可携带凭据的值，禁止把原生扩展字段当作安全公开文本。
     *
     * @param values   候选
     * @param path     当前路径
     * @param redacted 记录脱敏位置。
     * @return 独立且已脱敏的树，不触及输入对象。
     */
    private static Map<String, Object> redact(Map<String, Object> values, String path, List<String> redacted) {
        Map<String, Object> result = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            String at = path + key, normalized = normalize(key);
            boolean secret = normalized.contains("password") || normalized.contains("secret") || normalized.contains("token")
                    || normalized.contains("jaas") || normalized.contains("username") || normalized.endsWith("location");
            if (value != null && (secret || normalized.endsWith("properties") && !(value instanceof Map<?, ?>))) {
                result.put(key, "<redacted>");
                redacted.add(at);
            } else if (value instanceof Map<?, ?> map) {
                Map<String, Object> entries = new LinkedHashMap<>();
                map.forEach((nested, nestedValue) -> entries.put(String.valueOf(nested), nestedValue));
                if (normalized.endsWith("properties")) entries.replaceAll((nested, nestedValue) -> {
                    if (nestedValue == null) return null;
                    redacted.add(at + "." + nested);
                    return "<redacted>";
                });
                result.put(key, redact(entries, at + ".", redacted));
            } else result.put(key, value);
        });
        return result;
    }

    /**
     * 业务作用：封存 JSON 配置树，含 null 缺省值也能保留原意。
     *
     * @param values 配置节点。
     * @return 深度不可变 Map/List，不暴露可改写的报告内部对象。
     */
    private static Map<String, Object> immutable(Map<String, Object> values) {
        Map<String, Object> result = new LinkedHashMap<>();
        values.forEach((key, value) -> result.put(key, immutableValue(value)));
        return Collections.unmodifiableMap(result);
    }

    /**
     * 业务作用：递归封存单一 JSON 值，保持显式 null 与空集合的区别。
     *
     * @param value 候选节点。
     * @return 不可变副本或不可变标量。
     */
    private static Object immutableValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> entries = new LinkedHashMap<>();
            map.forEach((key, item) -> entries.put(String.valueOf(key), item));
            return immutable(entries);
        }
        if (value instanceof List<?> list)
            return Collections.unmodifiableList(list.stream().map(KafkaMigration::immutableValue).toList());
        return value;
    }
}

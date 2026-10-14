package io.github.nasaruntime.kafka.schema;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;
import java.util.function.Supplier;
import javax.net.ssl.SSLParameters;

/**
 * 有界 Confluent-compatible 客户端；正负缓存共用容量，同 ID 冷查询合并请求。
 * 数据面应先使用 ConfluentEnvelope 的白名单校验；本类只读取 schema，不生成业务 codec。
 * 调用方取消等待不会撤回远端请求，register 的失败或取消不能证明远端没有创建修订。
 */
public final class ConfluentSchemaRegistry implements SchemaRegistryClient {

    /**
     * 数据面终态，均为低基数分类。
     */
    public enum LookupOutcome {CACHE_HIT, FETCHED, NEGATIVE_CACHE_HIT, FETCHED_MISSING, FETCH_FAILED, CANCELLED}

    /**
     * 控制面操作与数据面缓存命中率分开统计。
     */
    public enum ControlOperation {COMPATIBILITY, REGISTER}

    /**
     * 控制请求的终态；本地拒绝不记作网络失败。
     */
    public enum ControlOutcome {SUCCESS, REJECTED, TRANSPORT, REMOTE_STATUS, INVALID_RESPONSE, CANCELLED}

    /**
     * 从客户端构造开始累计的固定维度观测，读取不清零。
     *
     * @param lookups       数据面各终态次数
     * @param cachedEntries 正负缓存共用的当前占用，可能包含尚未被查询清除的过期项
     * @param cacheCapacity 缓存容量上限
     * @param controls      每种控制操作的各终态次数
     * @param inFlight      当前实际 HTTP 请求数，不包含同 ID 的等待者
     */
    public record Snapshot(Map<LookupOutcome, Long> lookups, int cachedEntries, int cacheCapacity,
                           Map<ControlOperation, Map<ControlOutcome, Long>> controls, int inFlight) {}

    private final ConfluentRegistryOptions options;
    private final HttpClient client;
    private final Semaphore lookupPermits;
    private final Semaphore dispatchPermits;
    private final ObjectMapper json = new ObjectMapper();
    /**
     * 所有缓存与准入状态受本实例监视器保护；网络等待与用户完成回调不持有该锁。
     */
    private final LinkedHashMap<Integer, CacheEntry> cache = new LinkedHashMap<>(16, 0.75f, true);
    private final Map<Integer, LookupRound> rounds = new HashMap<>();
    private final Set<CompletableFuture<?>> requests = new HashSet<>();
    private final EnumMap<LookupOutcome, AtomicLong> lookupCounts = new EnumMap<>(LookupOutcome.class);
    private final EnumMap<ControlOperation, EnumMap<ControlOutcome, AtomicLong>> controlCounts = new EnumMap<>(ControlOperation.class);
    private boolean closed;

    /**
     * 业务作用：创建独立且不自动注册 schema 的客户端。
     * <p>
     * 返回：尚未请求 Registry 的客户端，重定向始终关闭。
     *
     * @param options 已验证配置。
     */
    public ConfluentSchemaRegistry(ConfluentRegistryOptions options) {
        this(options, null);
    }

    /**
     * 业务作用：使用独立私有信任域或 mTLS 身份创建 Registry 客户端。
     * <p>
     * 返回：尚未连接的客户端；材料非法在请求前拒绝，重定向与主机名绕过均不开放。
     *
     * @param options 请求合同
     * @param tls     可空独立材料，提供时 endpoint 必须为 HTTPS。
     */
    public ConfluentSchemaRegistry(ConfluentRegistryOptions options, SchemaRegistryTls tls) {
        this.options = Objects.requireNonNull(options);
        this.lookupPermits = new Semaphore(options.maxPendingLookups());
        this.dispatchPermits = new Semaphore(options.maxPendingLookups());
        // 凭据只发往受信 endpoint，禁止远端重定向改变认证接收者。
        HttpClient.Builder builder = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).connectTimeout(options.requestTimeout());
        if (tls != null) {
            if (!"https".equals(options.endpoint().getScheme()))
                throw failure(SchemaRegistryException.Code.INVALID_CONFIGURATION);
            SSLParameters parameters = new SSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            parameters.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
            builder.sslContext(tls.context()).sslParameters(parameters);
        }
        client = builder.build();
        for (LookupOutcome outcome : LookupOutcome.values()) lookupCounts.put(outcome, new AtomicLong());
        for (ControlOperation operation : ControlOperation.values()) {
            EnumMap<ControlOutcome, AtomicLong> counts = new EnumMap<>(ControlOutcome.class);
            for (ControlOutcome outcome : ControlOutcome.values()) counts.put(outcome, new AtomicLong());
            controlCounts.put(operation, counts);
        }
    }

    /**
     * 业务作用：读取数据面、控制面及缓存的独立观测。
     * <p>
     * 参数说明：无。
     *
     * @return 不可变快照，不含 schema ID、subject 或 endpoint 标签。
     */
    public synchronized Snapshot metricsSnapshot() {
        Map<LookupOutcome, Long> lookups = new EnumMap<>(LookupOutcome.class);
        lookupCounts.forEach((key, count) -> lookups.put(key, count.get()));
        Map<ControlOperation, Map<ControlOutcome, Long>> controls = new EnumMap<>(ControlOperation.class);
        controlCounts.forEach((operation, counts) -> {
            Map<ControlOutcome, Long> copy = new EnumMap<>(ControlOutcome.class);
            counts.forEach((key, count) -> copy.put(key, count.get()));
            controls.put(operation, Map.copyOf(copy));
        });
        return new Snapshot(Map.copyOf(lookups), cache.size(), options.cacheCapacity(), Map.copyOf(controls), requests.size());
    }

    /**
     * 业务作用：查询 schema 并为本次调用记一个数据面终态。
     *
     * @param id 正的 Registry ID。
     * @return 异步独立等待，取消不取消共享请求。
     */
    @Override
    public CompletableFuture<RegisteredSchema> schemaById(int id) {
        synchronized (this) {
            if (closed) {
                note(LookupOutcome.FETCH_FAILED);
                return CompletableFuture.failedFuture(failure(SchemaRegistryException.Code.CLOSED));
            }
        }
        // 同一 ID 虽只产生一个 HTTP，也可能累积大量等待者；必须在注册完成回调前限制业务等待数。
        if (!lookupPermits.tryAcquire()) {
            note(LookupOutcome.FETCH_FAILED);
            return CompletableFuture.failedFuture(failure(SchemaRegistryException.Code.CAPACITY));
        }
        CompletableFuture<RegisteredSchema> result = new CompletableFuture<>();
        result.whenComplete((value, error) -> {
            synchronized (this) {
                LookupRound current = rounds.get(id);
                if (current != null) current.waiters.remove(result);
            }
            lookupPermits.release();
            if (result.isCancelled()) note(LookupOutcome.CANCELLED);
        });
        lookup(id, result);
        return result;
    }

    /**
     * 业务作用：观测当前仍持有查询等待额度的业务调用。
     * <p>
     * 参数说明：无。
     *
     * @return 包括 leader 与同 ID 等待者，取消等待后归还，与实际 HTTP 责任分开。
     */
    public int pendingLookups() {
        return options.maxPendingLookups() - lookupPermits.availablePermits();
    }

    /**
     * 业务作用：在同 ID 冷请求之间取得唯一访问权，失败轮次结束后等待者可依次接替。
     * <p>
     * 返回：命中、网络结果或等待后重查缓存。
     *
     * @param id     目标
     * @param result 本次调用等待。
     */
    private void lookup(int id, CompletableFuture<RegisteredSchema> result) {
        if (result.isDone()) return;
        LookupRound round = null;
        boolean leader = false;
        CacheEntry cached = null;
        SchemaRegistryException rejected = null;
        synchronized (this) {
            if (result.isDone()) return;
            if (closed || id <= 0) {
                rejected = failure(closed ? SchemaRegistryException.Code.CLOSED : SchemaRegistryException.Code.INVALID_SCHEMA_ID);
            } else {
                cached = cache.get(id);
                if (cached != null && System.nanoTime() - cached.expiresAt >= 0) {
                    cache.remove(id);
                    cached = null;
                }
                if (cached == null) {
                    round = rounds.get(id);
                    leader = round == null;
                    if (leader) {
                        if (rounds.size() >= options.cacheCapacity())
                            rejected = failure(SchemaRegistryException.Code.CAPACITY);
                        else {
                            // 先登记请求轮次再发 HTTP，同 ID 的并发不会越过请求合并门禁。
                            round = new LookupRound();
                            rounds.put(id, round);
                        }
                    } else round.waiters.add(result);
                }
            }
        }
        // Future 完成可能同步执行用户代码，必须离开缓存锁后发布结果。
        if (rejected != null) {
            finishLookup(result, null, rejected, LookupOutcome.FETCH_FAILED);
            return;
        }
        if (cached != null) {
            finishLookup(result, cached.value, cached.value == null ? failure(SchemaRegistryException.Code.SCHEMA_NOT_FOUND) : null,
                    cached.value == null ? LookupOutcome.NEGATIVE_CACHE_HIT : LookupOutcome.CACHE_HIT);
            return;
        }
        if (!leader) return;
        LookupRound ownedRound = round;
        request("GET", new String[]{"schemas", "ids", Integer.toString(id)}, null).whenComplete((response, error) -> {
            RegisteredSchema value = null;
            SchemaRegistryException problem = error == null ? null : classify(error);
            boolean missing = false;
            if (problem == null) {
                try {
                    if (response.statusCode() == 404) {
                        missing = true;
                        throw failure(SchemaRegistryException.Code.SCHEMA_NOT_FOUND);
                    }
                    JsonNode body = successfulJson(response);
                    // 尚未解析引用依赖时不能把依赖外部定义的 schema 当作完整合同交给业务。
                    JsonNode references = body.get("references");
                    if (references != null && (!references.isArray() || !references.isEmpty()))
                        throw failure(SchemaRegistryException.Code.INVALID_RESPONSE);
                    JsonNode schema = body.get("schema");
                    if (schema == null || !schema.isTextual() || schema.textValue().isEmpty())
                        throw failure(SchemaRegistryException.Code.INVALID_RESPONSE);
                    SchemaType type = SchemaType.valueOf(body.has("schemaType") ? body.get("schemaType").textValue() : "AVRO");
                    value = new RegisteredSchema(id, type, schema.textValue());
                } catch (SchemaRegistryException classified) {
                    problem = classified;
                } catch (RuntimeException invalid) {
                    problem = failure(SchemaRegistryException.Code.INVALID_RESPONSE);
                }
            }
            List<CompletableFuture<RegisteredSchema>> waiting;
            synchronized (this) {
                if (closed) problem = failure(SchemaRegistryException.Code.CLOSED);
                if (!closed && (problem == null || missing)) {
                    cache.put(id, new CacheEntry(value, System.nanoTime() + (missing ? options.negativeCacheTtl() : options.cacheTtl()).toNanos()));
                    while (cache.size() > options.cacheCapacity()) cache.remove(cache.keySet().iterator().next());
                }
                // 先发布缓存再移除访问权，等待者醒来后必然看到本轮已完成的事实。
                rounds.remove(id, ownedRound);
                waiting = List.copyOf(ownedRound.waiters);
                ownedRound.waiters.clear();
            }
            // 先结束内部轮次再发布用户结果，某个用户回调阻塞不能占住其他查询的访问权。
            waiting.forEach(waiter -> dispatchLookup(id, waiter));
            finishLookup(result, value, problem, missing ? LookupOutcome.FETCHED_MISSING : problem == null ? LookupOutcome.FETCHED : LookupOutcome.FETCH_FAILED);
        });
    }

    /**
     * 业务作用：有界唤醒共享查询的等待者，取消不能把历史回调无限堆积在公共执行器。
     * <p>
     * 返回：有限队列内重新查询，超额或关闭时拒绝；实际回调退出后归还派发额度。
     *
     * @param id     原查询目标
     * @param result 仍可能等待的调用。
     */
    private void dispatchLookup(int id, CompletableFuture<RegisteredSchema> result) {
        if (result.isDone()) return;
        if (!dispatchPermits.tryAcquire()) {
            SchemaRegistryException.Code reason;
            synchronized (this) {
                reason = closed ? SchemaRegistryException.Code.CLOSED : SchemaRegistryException.Code.CAPACITY;
            }
            finishLookup(result, null, failure(reason), LookupOutcome.FETCH_FAILED);
            return;
        }
        CompletableFuture.runAsync(() -> {
            try {
                lookup(id, result);
            } finally {
                dispatchPermits.release();
            }
        });
    }

    /**
     * 业务作用：发布查询结果且只记一个终态，取消等待不能随后变成成功统计。
     * <p>
     * 返回：实际赢得终态发布时记账。
     *
     * @param result  独立等待
     * @param value   本次 HTTP 查询或缓存命中的 schema；缺失或失败时为 null。
     * @param error   本次查询失败的异常，与成功value互斥。
     * @param outcome 分类。
     */
    private void finishLookup(CompletableFuture<RegisteredSchema> result, RegisteredSchema value, Throwable error, LookupOutcome outcome) {
        if (error == null ? result.complete(value) : result.completeExceptionally(error)) note(outcome);
    }

    /**
     * 业务作用：累计低基数数据面结局。
     * <p>
     * 返回：计数递增，不改变业务结果。
     *
     * @param outcome 封闭分类。
     */
    private void note(LookupOutcome outcome) {
        lookupCounts.get(outcome).incrementAndGet();
    }

    /**
     * 业务作用：执行只读兼容性检查并与数据面命中率分开记账。
     *
     * @param subject 显式Registry控制操作所针对的subject名称。
     * @param version 本次控制操作指定的subject版本。
     * @param type    候选或目标schema的共同类型，例如AVRO、JSON或PROTOBUF。
     * @param schema  候选schema的完整正文，不由消息内容推断或自动替换。
     * @return 协议中的兼容布尔值或封闭错误。
     */
    @Override
    public CompletableFuture<Boolean> isCompatible(String subject, String version, SchemaType type, String schema) {
        return control(ControlOperation.COMPATIBILITY, () -> {
            validateSubject(subject);
            if (version == null || !(version.equals("latest") || version.matches("[1-9][0-9]{0,9}") && Long.parseLong(version) <= 4294967295L))
                throw failure(SchemaRegistryException.Code.INVALID_SUBJECT);
            return request("POST", new String[]{"compatibility", "subjects", subject, "versions", version}, schemaBody(type, schema)).thenApply(response -> {
                JsonNode body = successfulJson(response), compatible = body.get("is_compatible");
                if (compatible == null || !compatible.isBoolean())
                    throw failure(SchemaRegistryException.Code.INVALID_RESPONSE);
                return compatible.booleanValue();
            });
        });
    }

    /**
     * 业务作用：在显式注册门禁之后请求 Registry 创建修订。
     *
     * @param subject 显式Registry控制操作所针对的subject名称。
     * @param type    候选或目标schema的共同类型，例如AVRO、JSON或PROTOBUF。
     * @param schema  候选schema的完整正文，不由消息内容推断或自动替换。
     * @return 正 ID；结果未知时不能推定未创建。
     */
    @Override
    public CompletableFuture<Integer> register(String subject, SchemaType type, String schema) {
        return control(ControlOperation.REGISTER, () -> {
            // 注册是外部写操作，必须在构造任何 HTTP 请求前验证显式授权配置。
            if (!options.autoRegister()) throw failure(SchemaRegistryException.Code.AUTO_REGISTER_DISABLED);
            validateSubject(subject);
            return request("POST", new String[]{"subjects", subject, "versions"}, schemaBody(type, schema)).thenApply(response -> {
                JsonNode id = successfulJson(response).get("id");
                if (id == null || !id.isIntegralNumber() || !id.canConvertToInt() || id.intValue() <= 0)
                    throw failure(SchemaRegistryException.Code.INVALID_RESPONSE);
                return id.intValue();
            });
        });
    }

    /**
     * 业务作用：统一控制面准入及终态记账。
     *
     * @param operation 操作类型
     * @param action    已约束请求。
     * @return 独立等待，本地拒绝、远端失败和取消分别计数。
     */
    private <T> CompletableFuture<T> control(ControlOperation operation, Supplier<CompletableFuture<T>> action) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.whenComplete((value, error) -> {
            if (result.isCancelled()) controlCounts.get(operation).get(ControlOutcome.CANCELLED).incrementAndGet();
        });
        try {
            synchronized (this) {
                if (closed) throw failure(SchemaRegistryException.Code.CLOSED);
            }
            action.get().whenComplete((value, error) -> {
                if (error == null ? result.complete(value) : result.completeExceptionally(classify(error)))
                    controlCounts.get(operation).get(error == null ? ControlOutcome.SUCCESS : controlOutcome(classify(error))).incrementAndGet();
            });
        } catch (RuntimeException rejected) {
            if (result.completeExceptionally(classify(rejected)))
                controlCounts.get(operation).get(ControlOutcome.REJECTED).incrementAndGet();
        }
        return result;
    }

    /**
     * 业务作用：让控制面错误统计与 Rust 的封闭结局一致。
     *
     * @param error 脱敏失败。
     * @return 低基数终态，HTTP 失败与本地拒绝分别统计。
     */
    private static ControlOutcome controlOutcome(SchemaRegistryException error) {
        return switch (error.code()) {
            case TRANSPORT -> ControlOutcome.TRANSPORT;
            case REMOTE_STATUS, SCHEMA_NOT_FOUND -> ControlOutcome.REMOTE_STATUS;
            case RESPONSE_TOO_LARGE, INVALID_RESPONSE, INVALID_SCHEMA_ID -> ControlOutcome.INVALID_RESPONSE;
            default -> ControlOutcome.REJECTED;
        };
    }

    /**
     * 业务作用：在响应下载时执行字节上限并维持独立请求总期限。
     *
     * @param method   HTTP 方法
     * @param segments 受控路径段
     * @param body     可空 JSON。
     * @return 有界应答；不会跟随重定向或输出网络原因。
     */
    private CompletableFuture<HttpResponse<byte[]>> request(String method, String[] segments, byte[] body) {
        synchronized (this) {
            if (closed) return CompletableFuture.failedFuture(failure(SchemaRegistryException.Code.CLOSED));
            if (requests.size() >= options.cacheCapacity())
                return CompletableFuture.failedFuture(failure(SchemaRegistryException.Code.CAPACITY));
            StringBuilder address = new StringBuilder(options.endpoint().toASCIIString().replaceAll("/+$", ""));
            for (String segment : segments)
                address.append('/').append(URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20"));
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(address.toString())).timeout(options.requestTimeout())
                    .header("Accept", "application/vnd.schemaregistry.v1+json");
            if (options.authorization() != null) builder.header("Authorization", options.authorization());
            if (body != null) builder.header("Content-Type", "application/vnd.schemaregistry.v1+json");
            builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body));
            CompletableFuture<HttpResponse<byte[]>> nativeRequest = client.sendAsync(builder.build(), info -> new BoundedBody(options.maxResponseBytes()));
            requests.add(nativeRequest);
            CompletableFuture<HttpResponse<byte[]>> response = new CompletableFuture<>();
            nativeRequest.whenComplete((value, error) -> {
                synchronized (this) {
                    requests.remove(nativeRequest);
                }
                if (error == null) response.complete(value);
                else response.completeExceptionally(classify(error));
            });
            // 期限覆盖整个 body；调用方停止等待后仍主动取消原生读取，不能持续保留响应缓冲。
            response.orTimeout(options.requestTimeout().toNanos(), TimeUnit.NANOSECONDS).whenComplete((value, error) -> {
                if (error != null) nativeRequest.cancel(true);
            });
            return response;
        }
    }

    /**
     * 业务作用：校验状态及 JSON 结构，不向外泄露远端正文。
     *
     * @param response 有界应答。
     * @return JSON 对象或封闭协议错误。
     */
    private JsonNode successfulJson(HttpResponse<byte[]> response) {
        if (response.statusCode() < 200 || response.statusCode() >= 300)
            throw new SchemaRegistryException(SchemaRegistryException.Code.REMOTE_STATUS, response.statusCode());
        try {
            JsonNode value = json.readTree(response.body());
            if (value == null || !value.isObject()) throw failure(SchemaRegistryException.Code.INVALID_RESPONSE);
            return value;
        } catch (IOException invalid) {
            throw failure(SchemaRegistryException.Code.INVALID_RESPONSE);
        }
    }

    /**
     * 业务作用：限制候选 schema 的字节量再形成控制请求。
     *
     * @param type   声明协议
     * @param schema 候选正文。
     * @return JSON 请求字节，非法或超大正文不会访问网络。
     */
    private byte[] schemaBody(SchemaType type, String schema) {
        if (type == null) throw failure(SchemaRegistryException.Code.INVALID_CONFIGURATION);
        if (schema == null || schema.isEmpty()) throw failure(SchemaRegistryException.Code.INVALID_SUBJECT);
        if (schema.getBytes(StandardCharsets.UTF_8).length > options.maxResponseBytes())
            throw failure(SchemaRegistryException.Code.SCHEMA_TOO_LARGE);
        try {
            return json.writeValueAsBytes(Map.of("schemaType", type.name(), "schema", schema));
        } catch (IOException invalid) {
            throw failure(SchemaRegistryException.Code.INVALID_RESPONSE);
        }
    }

    /**
     * 业务作用：阻断空值、控制字符及路径自指片段，避免控制目标被 URL 规范化改写。
     * <p>
     * 返回：合法目标正常返回，否则请求前拒绝。
     *
     * @param subject 受信声明。
     */
    private static void validateSubject(String subject) {
        if (subject == null || subject.isEmpty() || subject.getBytes(StandardCharsets.UTF_8).length > 255 || subject.contains("/")
                || subject.equals(".") || subject.equals("..") || subject.chars().anyMatch(Character::isISOControl))
            throw failure(SchemaRegistryException.Code.INVALID_SUBJECT);
    }

    /**
     * 业务作用：提取封闭错误并隔离原生 HTTP 原因中的敏感地址。
     *
     * @param error 异步原因。
     * @return 已分类错误或无详情的传输失败。
     */
    private static SchemaRegistryException classify(Throwable error) {
        while (error instanceof CompletionException || error instanceof ExecutionException) error = error.getCause();
        return error instanceof SchemaRegistryException known ? known : failure(SchemaRegistryException.Code.TRANSPORT);
    }

    /**
     * 业务作用：构造只含错误类别的拒绝结果。
     *
     * @param code 封闭分类。
     * @return 无敏感原因链的异常。
     */
    private static SchemaRegistryException failure(SchemaRegistryException.Code code) {
        return new SchemaRegistryException(code);
    }

    /**
     * 业务作用：关闭新准入并取消本地在途 HTTP，保留远端注册可能已发生的语义。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：封闭新请求、清空缓存并请求取消已登记 HTTP；共享等待随后结束，返回不保证全部用户回调已经退出，也不撤销远端写入。
     */
    @Override
    public void close() {
        List<CompletableFuture<?>> pending;
        synchronized (this) {
            if (closed) return;
            // 先关闭新请求准入再取消旧请求，防止等待者醒来后启动替代轮次。
            closed = true;
            pending = List.copyOf(requests);
            cache.clear();
        }
        pending.forEach(request -> request.cancel(true));
        client.shutdownNow();
    }

    private record CacheEntry(RegisteredSchema value, long expiresAt) {}

    /**
     * 可撤销的等待集合；调用取消时移除，不能把每次取消的 Future 长期附着在共享请求上。
     */
    private static final class LookupRound {
        final Set<CompletableFuture<RegisteredSchema>> waiters = new LinkedHashSet<>();
    }

    /**
     * 在上游每次交付字节块时检查硬上限，不能先聚合完整响应再判断。
     */
    private static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final int limit;
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private Flow.Subscription subscription;

        /**
         * 业务作用：绑定单次响应的硬字节预算。
         * <p>
         * 返回：尚未订阅的有界接收器。
         *
         * @param limit 正上限。
         */
        BoundedBody(int limit) {
            this.limit = limit;
        }

        /**
         * 业务作用：交付响应终态，未收齐正文时不发布成功。
         * <p>
         * 参数说明：无。
         *
         * @return 受硬字节边界约束的异步正文。
         */
        @Override
        public CompletionStage<byte[]> getBody() {
            return result;
        }

        /**
         * 业务作用：逐块接纳数据以维持背压。
         * <p>
         * 返回：只申请首块，不无限预取。
         *
         * @param value HTTP 上游。
         */
        @Override
        public void onSubscribe(Flow.Subscription value) {
            subscription = value;
            value.request(1);
        }

        /**
         * 业务作用：复制前验证本块不会突破总预算。
         * <p>
         * 返回：超限立即取消读取，否则申请下一块。
         *
         * @param buffers 原生网络块。
         */
        @Override
        public void onNext(List<ByteBuffer> buffers) {
            for (ByteBuffer buffer : buffers) {
                // 容量拒绝必须发生在分配和复制前，恶意响应不能迫使客户端持有超限正文。
                if (buffer.remaining() > limit - bytes.size()) {
                    subscription.cancel();
                    result.completeExceptionally(failure(SchemaRegistryException.Code.RESPONSE_TOO_LARGE));
                    return;
                }
                byte[] part = new byte[buffer.remaining()];
                buffer.get(part);
                bytes.writeBytes(part);
            }
            subscription.request(1);
        }

        /**
         * 业务作用：将网络读取失败转换为不含原生地址的错误。
         * <p>
         * 返回：正文等待失败。
         *
         * @param error 上游原因。
         */
        @Override
        public void onError(Throwable error) {
            result.completeExceptionally(classify(error));
        }

        /**
         * 业务作用：在上游明确结束之后发布正文。
         * <p>
         * 参数说明：无。
         * 返回：独立字节数组，超限后的完成不能覆盖失败。
         */
        @Override
        public void onComplete() {
            result.complete(bytes.toByteArray());
        }
    }
}

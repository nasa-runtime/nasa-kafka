package io.github.nasaruntime.kafka;

import io.github.nasaruntime.core.base.AnyHolder;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.github.nasaruntime.core.utils.MapUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 类作用 - Kafka 线程上下文 holder, 保存当前 KafkaProxy、消费记录元数据和跨服务透传数据。
 */
@SuppressWarnings({"unchecked"})
public abstract class KafkaProxyHolder {

    /**
     * 业务作用：私有化构造，杜绝实例化——本类只提供线程上下文的静态存取，
     * 实例化没有任何意义且会误导调用方以为上下文是按实例隔离的。
     *
     * <p>参数说明: 无。
     */
    private KafkaProxyHolder() {}

    private static final String KEY = "__KafkaHolder";
    private static final String KEY_KP = "kafkaProxy";
    private static final String KEY_TOPIC = "topic";
    private static final String KEY_PARTITION = "partition";
    private static final String KEY_OFFSET = "offset";
    private static final String KEY_HEADERS = "headers";
    private static final String KEY_PASSTHROUGH = "__passthrough";

    /**
     * 业务作用：取出当前线程的 Kafka 上下文根映射，供需要一次性读取全部上下文项的场景使用。
     * 该映射由池化容器承载，其生命周期归线程上下文管理，调用方不得自行归还。
     *
     * <p>参数说明: 无。
     *
     * @param <K> 键类型
     * @param <V> 值类型
     * @return 当前线程的上下文映射；本线程尚未建立上下文时为 null。
     */
    public static <K, V> RecycleLinkedMap<K, V> get() {
        return AnyHolder.get(KEY);
    }

    /**
     * 业务作用：向当前线程的 Kafka 上下文写入一项，上下文不存在时按需建出。
     *
     * @param key 上下文键
     * @param o   上下文值
     * 返回: 无返回值。
     */
    public static void set(Object key, Object o) {
        AnyHolder.putAsMap(KEY, key, o);
    }

    /**
     * 业务作用：读取当前线程 Kafka 上下文中的一项。
     *
     * @param key 上下文键
     * @param <T> 期望的值类型，由调用方保证与写入类型一致
     * @return 对应的值；上下文不存在或该键未写入时为 null。
     */
    public static <T> T get(Object key) {
        return AnyHolder.getAsMap(KEY, key);
    }

    /**
     * 业务作用：在消费线程上标记本次消息由哪个 KafkaProxy 投递，使业务代码在回调内无需注入即可就近取到发送入口。
     * 多套 Kafka 集群共存时，这也是「从哪来就回哪去」的依据——回发时取错 proxy 会把消息发到另一个集群。
     *
     * @param kafkaProxy 本次消费所属的 Kafka 主入口
     * 返回: 无返回值。
     */
    public static void setKafkaProxy(KafkaProxy kafkaProxy) {
        set(KEY_KP, kafkaProxy);
    }

    /**
     * 业务作用：在消费回调内取回本次消息所属的 Kafka 主入口，使业务无需注入即可就近回发消息。
     *
     * <p>参数说明: 无。
     *
     * @return 当前线程关联的主入口；非消费线程上为 null。
     */
    public static KafkaProxy getKafkaProxy() {
        return get(KEY_KP);
    }

    /**
     * 业务作用：记录本次消费记录的来源 topic，使业务不必声明 record 形态的 listener 也能拿到元数据。
     *
     * @param topic 来源 topic
     * 返回: 无返回值。
     */
    public static void setTopic(String topic) {
        set(KEY_TOPIC, topic);
    }

    /**
     * 业务作用：读取本次消费记录的来源 topic。
     *
     * <p>参数说明: 无。
     *
     * @return 来源 topic；非消费线程上为 null。
     */
    public static String getTopic() {
        return get(KEY_TOPIC);
    }

    /**
     * 业务作用：记录本次消费记录的来源分区。
     *
     * @param partition 来源分区号
     * 返回: 无返回值。
     */
    public static void setPartition(int partition) {
        set(KEY_PARTITION, partition);
    }

    /**
     * 业务作用：读取本次消费记录的来源分区。
     * 返回包装类型而非基本类型，使「不在消费线程上」这一情形能与分区 0 区分开。
     *
     * <p>参数说明: 无。
     *
     * @return 来源分区号；非消费线程上为 null。
     */
    public static Integer getPartition() {
        return get(KEY_PARTITION);
    }

    /**
     * 业务作用：记录本次消费记录的偏移量，业务侧据此甄别重复投递与定位问题。
     *
     * @param offset 记录在分区内的偏移量
     * 返回: 无返回值。
     */
    public static void setOffset(long offset) {
        set(KEY_OFFSET, offset);
    }

    /**
     * 业务作用：读取本次消费记录的偏移量。
     * 返回包装类型，使「不在消费线程上」能与偏移量 0 区分开。
     *
     * <p>参数说明: 无。
     *
     * @return 偏移量；非消费线程上为 null。
     */
    public static Long getOffset() {
        return get(KEY_OFFSET);
    }

    /**
     * 业务作用：记录本次消费记录的全部 header，使业务能读取框架未建模的自定义头。
     *
     * @param headers header 映射；同名 header 保留多个值，值为原始字节
     * 返回: 无返回值。
     */
    public static void setHeaders(Map<String, List<byte[]>> headers) {
        set(KEY_HEADERS, headers);
    }

    /**
     * 业务作用：读取本次消费记录的全部 header。
     *
     * <p>参数说明: 无。
     *
     * @return header 映射；非消费线程上为 null。
     */
    public static Map<String, List<byte[]>> getHeaders() {
        return get(KEY_HEADERS);
    }

    /**
     * 业务作用：产出发布时要写进 header 的透传快照，使链路标识与业务透传项自动随消息传到下游。
     * <p>
     * 链路标识单独从通用上下文取并<b>覆盖</b>同名透传项：链路标识由框架统一维护，
     * 业务手工写入的同名项若能覆盖它，整条链的追踪就会在此断开。
     * <p>
     * 无链路标识且无业务透传项时返回 null 而非空映射，让发送侧据此整个跳过透传 header，
     * 不给每条消息附加一个空头。
     *
     * <p>参数说明: 无。
     *
     * @return 待写入 header 的透传快照；无任何可透传内容时为 null。
     */
    public static Map<String, Object> passthrough() {
        Map<String, Object> passthrough = get(KEY_PASSTHROUGH);
        String traceId = AnyHolder.get(AnyHolder.TRACE_ID);
        if (traceId == null && MapUtils.isEmpty(passthrough)) return null;
        RecycleLinkedMap<String, Object> rtn = RecycleLinkedMap.of(passthrough);
        if (traceId != null) rtn.put(AnyHolder.TRACE_ID, traceId);
        return rtn;
    }

    /**
     * 业务作用：登记一个业务透传项，此后本线程发出的消息都会自动带上它。
     * 值为 null 时不登记：透传项要序列化进 header，空值没有可传递的信息。
     *
     * @param k 透传键
     * @param o 透传值，为 null 时忽略本次调用
     * 返回: 无返回值。
     */
    public static void passthrough(Object k, Object o) {
        if (o == null) return;
        Map<Object, Object> passthrough = get(KEY_PASSTHROUGH);
        if (passthrough == null) set(KEY_PASSTHROUGH, passthrough = new LinkedHashMap<>());
        passthrough.put(k, o);
    }

    /**
     * 业务作用：读取一个业务透传项，消费侧据此取回上游随消息带来的上下文。
     *
     * @param key 透传键
     * @param <V> 期望的值类型
     * @return 透传值；无该项时为 null。
     */
    public static <V> V passthrough(Object key) {
        return MapUtils.getObject(get(KEY_PASSTHROUGH), key);
    }

    /**
     * 业务作用：清理当前线程的 Kafka 上下文，交还其中的池化容器。
     * 消费线程会被复用，不清理则上一条消息的 topic、位点与透传项会串到下一条消息的处理中，
     * 表现为日志元数据错乱或透传项莫名多出来。
     *
     * <p>参数说明: 无。
     *
     * 返回: 无返回值。
     */
    public static void clear() {
        AnyHolder.remove(KEY);
    }
}

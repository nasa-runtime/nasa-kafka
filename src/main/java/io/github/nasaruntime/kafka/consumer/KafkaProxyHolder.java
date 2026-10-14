package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.KafkaProxy;

import io.github.nasaruntime.core.base.AnyHolder;
import io.github.nasaruntime.core.base.RecycleLinkedMap;
import io.github.nasaruntime.core.utils.MapUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 类作用 - Kafka 线程上下文 holder, 保存当前 KafkaProxy、消费记录元数据和跨服务透传数据。
 */
public abstract class KafkaProxyHolder {

    /**
     * 业务作用：私有化构造，杜绝实例化——本类只提供线程上下文的静态存取，
     * 实例化没有任何意义且会误导调用方以为上下文是按实例隔离的。
     * <p>
     * 参数说明：无。
     * 返回：仅限制实例构造，不创建线程上下文。
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
     * 业务作用：读取当前线程的 Kafka 根映射，供兼容调用一次取得该作用域的上下文。
     * 映射由线程作用域持有，clear 后归还池；调用方不得自行回收或跨作用域保留该引用。
     * <p>
     * 参数说明：无。
     *
     * @return 当前作用域的可变根映射；未建立时为 null，不创建快照或新的上下文。
     */
    public static <K, V> RecycleLinkedMap<K, V> get() {
        return AnyHolder.get(KEY);
    }

    /**
     * 业务作用：向当前线程的 Kafka 上下文写入一项，上下文不存在时按需建出。
     * <p>
     * 返回：在当前线程建立或更新上下文项；保留值引用，不复制值，也不传播到其它线程。
     *
     * @param key 上下文键
     * @param o   上下文值
     */
    public static void set(Object key, Object o) {
        RecycleLinkedMap<Object, Object> context = get();
        if (context == null) {
            // 根映射与公开读取类型保持一致，且只由本线程作用域负责归还。
            context = RecycleLinkedMap.of();
            AnyHolder.set(KEY, context);
        }
        context.put(key, o);
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
     * 业务作用：把本次消费所属源保存到同步调用线程，便于兼容回调就近发布。
     * 这里只保存资源所有者引用，不转移关闭权，也不自动传播到异步执行器。
     * <p>
     * 返回：更新当前线程的源引用，不创建或连接 Kafka 客户端。
     *
     * @param kafkaProxy 本次消费所属的 Kafka 主入口
     */
    public static void setKafkaProxy(KafkaProxy kafkaProxy) {
        set(KEY_KP, kafkaProxy);
    }

    /**
     * 业务作用：在消费回调内取回本次消息所属的 Kafka 主入口，使业务无需注入即可就近回发消息。
     * <p>
     * 参数说明：无。
     *
     * @return 当前线程显式保存的主入口；未设置时为 null，异步线程不自动继承，跨执行器使用 ConsumeContext。
     */
    public static KafkaProxy getKafkaProxy() {
        return get(KEY_KP);
    }

    /**
     * 业务作用：保存同步消费调用的来源 topic，用于兼容元数据读取。
     * 批量回调由运行时写入首条记录的值；逐记录数据应从 KafkaRecord 读取。
     * <p>
     * 返回：更新当前线程的对应上下文项，不传播到其它线程。
     *
     * @param topic 来源 topic
     */
    public static void setTopic(String topic) {
        set(KEY_TOPIC, topic);
    }

    /**
     * 业务作用：读取本次消费记录的来源 topic。
     * <p>
     * 参数说明：无。
     *
     * @return 当前线程保存的来源 topic；未设置时为 null，批量回调表示首条记录，异步线程不自动继承。
     */
    public static String getTopic() {
        return get(KEY_TOPIC);
    }

    /**
     * 业务作用：保存同步消费调用的来源分区，用于兼容元数据读取。
     * 批量回调由运行时写入首条记录的值；逐记录数据应从 KafkaRecord 读取。
     * <p>
     * 返回：更新当前线程的对应上下文项，不传播到其它线程。
     *
     * @param partition 来源分区号
     */
    public static void setPartition(int partition) {
        set(KEY_PARTITION, partition);
    }

    /**
     * 业务作用：读取本次消费记录的来源分区。
     * 返回包装类型而非基本类型，使「不在消费线程上」这一情形能与分区 0 区分开。
     * <p>
     * 参数说明：无。
     *
     * @return 当前线程保存的来源分区；未设置时为 null，批量回调表示首条记录，异步线程不自动继承。
     */
    public static Integer getPartition() {
        return get(KEY_PARTITION);
    }

    /**
     * 业务作用：保存同步消费调用的来源 offset，用于兼容元数据读取。
     * 批量回调由运行时写入首条记录的值；逐记录数据应从 KafkaRecord 读取。
     * <p>
     * 返回：更新当前线程的对应上下文项，不传播到其它线程。
     *
     * @param offset 记录在分区内的偏移量
     */
    public static void setOffset(long offset) {
        set(KEY_OFFSET, offset);
    }

    /**
     * 业务作用：读取本次消费记录的偏移量。
     * 返回包装类型，使「不在消费线程上」能与偏移量 0 区分开。
     * <p>
     * 参数说明：无。
     *
     * @return 当前线程保存的来源 offset；未设置时为 null，批量回调表示首条记录，异步线程不自动继承。
     */
    public static Long getOffset() {
        return get(KEY_OFFSET);
    }

    /**
     * 业务作用：保存同步消费调用的消息头映射，用于兼容元数据读取。
     * 批量回调由运行时写入首条记录的值；逐记录数据应从 KafkaRecord 读取。
     * <p>
     * 返回：更新当前线程的对应上下文项，不传播到其它线程。
     *
     * @param headers header 映射；同名 header 保留多个值，值为原始字节
     */
    public static void setHeaders(Map<String, List<byte[]>> headers) {
        set(KEY_HEADERS, headers);
    }

    /**
     * 业务作用：读取本次消费记录的全部 header。
     * <p>
     * 参数说明：无。
     *
     * @return 当前线程保存的消息头映射；未设置时为 null，批量回调表示首条记录，异步线程不自动继承。
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
     * <p>
     * 参数说明：无。
     *
     * @return 需要随发布头传递的池化浅拷贝，无内容时为 null；调用方使用完须归还该快照，嵌套业务值不做深拷贝。
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
     * 业务作用：登记当前线程的业务透传项，供调用 passthrough() 的兼容发布入口组装消息头。
     * 不自动传播到其它线程或所有 builder 发布路线；null 值不写入，也不删除已有同名项。
     * <p>
     * 返回：非 null 值按引用保存到透传映射；null 调用不改变当前内容。
     *
     * @param k 透传键
     * @param o 透传值，为 null 时忽略本次调用
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
     * 业务作用：移除当前线程的 Kafka 根上下文并归还其池化映射，防止后续调用读到上一条消息的来源和透传值。
     * 不清理 AnyHolder 中其它上下文，不递归回收业务对象或已交付的独立透传快照。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：根引用先移除，随后清空映射并归还池；无根项时不操作，调用方不得再使用旧根引用。
     */
    public static void clear() {
        RecycleLinkedMap<?, ?> context = AnyHolder.remove(KEY);
        // 先解除线程引用再回收，后续调用不能读取已经归还池的容器。
        if (context != null) context.recycle();
    }
}

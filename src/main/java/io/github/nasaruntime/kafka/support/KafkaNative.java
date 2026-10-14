package io.github.nasaruntime.kafka.support;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import java.util.function.Supplier;


/**
 * 原生资源构造边界；配置解析、证书和认证模块的异常文本可能包含配置原值。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaNative {

    /**
     * 业务作用：禁止实例化无状态的构造边界。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：仅限制实例构造，不建立连接、启动线程或创建业务状态。
     */
    private KafkaNative() {}

    /**
     * 业务作用：防止原生构造失败的原因链回显凭据或证书路径，同时保留明确配置失败类别。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param factory 一次原生客户端构造。
     * @return 成功资源或不含原生原因链的 CONFIG；失败时不发布资源引用。
     */
    public static <T> T construct(Supplier<T> factory) {
        try {
            return factory.get();
        } catch (RuntimeException invalid) {
            // 不复制 message、cause 或 suppressed；原生解析器可能在三处重复携带认证材料。
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "Kafka 客户端构造失败，请检查有效配置及认证材料");
        }
    }
}

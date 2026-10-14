package io.github.nasaruntime.kafka.consumer;

import io.github.nasaruntime.kafka.consumer.listener.KafkaListener;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.ResolvableType;

import java.lang.reflect.Type;

import com.fasterxml.jackson.databind.type.TypeFactory;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;

/**
 * 在连接 broker 之前冻结 listener 的真实业务类型。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class PayloadTypeResolver {

    /**
     * 业务作用：解析继承、嵌套泛型与 Spring 代理后的消息类型。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param listener 注册对象。
     * @return 确定类型，无法解析时拒绝注册。
     */
    public static TypeReference<?> resolve(KafkaListener<?, ?> listener) {
        TypeReference<?> explicit = listener.paramType();
        ResolvableType resolved = ResolvableType.forClass(AopUtils.getTargetClass(listener)).as(KafkaListener.class).getGeneric(0);
        if (explicit != null) {
            ResolvableType declared = ResolvableType.forType(explicit.getType());
            if (declared.resolve() == null || declared.hasUnresolvableGenerics())
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "paramType 含未绑定泛型");
            if (resolved != ResolvableType.NONE && resolved.resolve() != null && !resolved.hasUnresolvableGenerics()) {
                var factory = TypeFactory.defaultInstance();
                if (!factory.constructType(freeze(resolved)).equals(factory.constructType(explicit.getType())))
                    throw new KafkaFailure(KafkaFailure.Code.CONFIG, "paramType 与 listener 泛型声明冲突");
            }
            return explicit;
        }
        if (resolved == ResolvableType.NONE || resolved.resolve() == null || resolved.hasUnresolvableGenerics()) {
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "listener payload 类型不确定，请显式声明 paramType");
        }
        Type frozen = freeze(resolved);
        return new TypeReference<>() {
            /**
             * 业务作用：向 codec 提供冻结的目标类型。
             * <p>
             * 参数说明：无。
             *
             * @return 业务类型。
             */
            @Override
            public Type getType() {
                return frozen;
            }
        };
    }

    /**
     * 业务作用：递归展开继承环境中的类型变量，避免嵌套 DTO 在 codec 中退化为 Map。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param resolved 带继承环境的类型。
     * @return 所有参数已具体化的类型。
     */
    public static Type freeze(ResolvableType resolved) {
        if (resolved.isArray()) {
            Type component = freeze(resolved.getComponentType());
            if (component instanceof Class<?> concrete) return Array.newInstance(concrete, 0).getClass();
            return (GenericArrayType) () -> component;
        }
        Class<?> raw = resolved.resolve();
        if (raw == null) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "payload 存在未绑定类型");
        ResolvableType[] children = resolved.getGenerics();
        if (children.length == 0) return raw;
        ResolvableType[] frozen = new ResolvableType[children.length];
        for (int index = 0; index < children.length; index++)
            frozen[index] = ResolvableType.forType(freeze(children[index]));
        return ResolvableType.forClassWithGenerics(raw, frozen).getType();
    }
}

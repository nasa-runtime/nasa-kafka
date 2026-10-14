package io.github.nasaruntime.kafka.spring;

import io.github.nasaruntime.kafka.consumer.PayloadTypeResolver;
import io.github.nasaruntime.kafka.consumer.listener.KafkaAsyncBatchListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaAsyncListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaBatchListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaRecordBatchListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaRecordListener;
import io.github.nasaruntime.kafka.consumer.listener.KafkaSingleListener;

import io.github.nasaruntime.kafka.consumer.AckMode;
import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.consumer.ConsumeContext;
import io.github.nasaruntime.kafka.consumer.KafkaRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.ResolvableType;
import org.springframework.core.MethodIntrospector;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * 将声明式方法转为相同 registry、Ack 和 owner 路径，避免第二套消费引擎。
 */
public final class KafkaMethodListeners {

    /**
     * 业务作用：禁止构造无状态适配工具。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：仅限制实例构造，不建立连接、启动线程或创建业务状态。
     */
    private KafkaMethodListeners() {}

    /**
     * 业务作用：在启动前收集对象上的消费方法并验证签名。
     *
     * @param bean 实际调用目标，可为 Spring 代理。
     * @return 冻结 listener 列表；非法泛型或返回类型立即失败。
     */
    public static List<KafkaListener<?, ?>> from(Object bean) {
        return from(bean, Function.identity());
    }

    /**
     * 业务作用：按容器配置展开路由占位符后冻结方法声明。
     *
     * @param bean     目标
     * @param resolver 严格占位符解析器。
     * @return 全部方法的冻结 listener。
     */
    public static List<KafkaListener<?, ?>> from(Object bean, Function<String, String> resolver) {
        Class<?> target = AopUtils.getTargetClass(bean);
        Map<Method, KafkaConsumer> methods = MethodIntrospector.selectMethods(target,
                (MethodIntrospector.MetadataLookup<KafkaConsumer>) method -> method.getAnnotation(KafkaConsumer.class));
        List<KafkaListener<?, ?>> result = new ArrayList<>();
        methods.entrySet().stream().sorted(Comparator.comparing(entry -> entry.getKey().toGenericString()))
                .forEach(entry -> result.add(adapt(bean, target, entry.getKey(), entry.getValue(), resolver)));
        return List.copyOf(result);
    }

    /**
     * 业务作用：生成仅暴露冻结声明的 listener。
     *
     * @param bean     实例
     * @param target   实际类
     * @param method   声明
     * @param config   路由
     * @param resolver 配置展开器。
     * @return 同步或异步适配器。
     */
    private static KafkaListener<?, ?> adapt(Object bean, Class<?> target, Method method, KafkaConsumer config, Function<String, String> resolver) {
        String[] topics = Arrays.stream(config.topics()).map(resolver).toArray(String[]::new);
        String event = resolver.apply(config.event()), group = resolver.apply(config.group());
        String broadcastScope = resolver.apply(config.broadcastScope()), instanceId = resolver.apply(config.instanceId());
        String[] sources = Arrays.stream(config.sources()).map(resolver).toArray(String[]::new);
        if (Stream.concat(Arrays.stream(topics), Stream.concat(Arrays.stream(sources), Stream.of(event, group, broadcastScope, instanceId))).anyMatch(value -> value.contains("${")))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "消费路由包含未解析占位符");
        if (!Modifier.isPublic(method.getModifiers()) || Modifier.isStatic(method.getModifiers()) || method.getParameterCount() < 1
                || method.getParameterCount() > 2 || method.getParameterCount() == 2 && method.getParameterTypes()[1] != ConsumeContext.class)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "消费方法要求 public 实例方法、一个消息参数及可选 ConsumeContext");
        boolean async = CompletionStage.class.isAssignableFrom(method.getReturnType());
        if (method.getReturnType() != void.class && (!async || ResolvableType.forMethodReturnType(method, target).as(CompletionStage.class).getGeneric(0).resolve() != Void.class))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "消费方法返回值只能是 void 或 CompletionStage<Void>");
        ResolvableType argument = ResolvableType.forMethodParameter(method, 0, target);
        boolean batch = argument.resolve() == List.class;
        if (batch) argument = argument.getGeneric(0);
        boolean record = argument.resolve() == KafkaRecord.class;
        if (record) argument = argument.getGeneric(0);
        if (argument.resolve() == null || argument.hasUnresolvableGenerics())
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "消费方法需要完整 payload 类型");
        if (config.ackMode() == AckMode.MANUAL && !record)
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "Manual 消费方法必须接收 KafkaRecord");
        Type payload = PayloadTypeResolver.freeze(argument);
        TypeReference<?> reference = new TypeReference<Object>() {
            /**
             * 业务作用：提供方法真实 payload 类型。
             *
             * 参数说明：无。
             *
             * @return 冻结类型。
             */
            @Override
            public Type getType() {
                return payload;
            }
        };
        Class<?> contract = async ? batch ? KafkaAsyncBatchListener.class : KafkaAsyncListener.class
                : batch ? record ? KafkaRecordBatchListener.class : KafkaBatchListener.class : record ? KafkaRecordListener.class : KafkaSingleListener.class;
        Method invocable = accessibleMethod(bean, method);
        return (KafkaListener<?, ?>) Proxy.newProxyInstance(contract.getClassLoader(), new Class<?>[]{contract}, (proxy, called, args) -> {
            return switch (called.getName()) {
                case "topics" -> topics.clone();
                case "event" -> event;
                case "group" -> group.isEmpty() ? null : group;
                case "broadcastScope" -> broadcastScope.isEmpty() ? null : broadcastScope;
                case "instanceId" -> instanceId.isEmpty() ? null : instanceId;
                case "qualifiers" -> sources.length == 0 ? null : sources.clone();
                case "ackMode" -> config.ackMode();
                case "executionMode" -> config.executionMode();
                case "nullable" -> config.nullable();
                case "paramType" -> reference;
                case "metricId" -> method.toGenericString();
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                case "toString" -> method.toGenericString();
                case "onEvent", "onRecord", "onRecords" -> {
                    Object value = args[0];
                    if (async && !record)
                        value = batch ? ((List<?>) value).stream().map(item -> ((KafkaRecord<?>) item).value()).toList() : ((KafkaRecord<?>) value).value();
                    try {
                        yield invocable.invoke(bean, method.getParameterCount() == 1 ? new Object[]{value} : new Object[]{value, ConsumeContext.current()});
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    }
                }
                default -> {
                    if (called.isDefault()) yield InvocationHandler.invokeDefault(proxy, called, args);
                    throw new KafkaFailure(KafkaFailure.Code.CONFIG, "无法适配 listener 方法: " + called.getName());
                }
            };
        });
    }

    /**
     * 业务作用：在登记消费路由前确认代理上的方法可调用，避免把声明访问失败留给消费线程。
     *
     * @param bean 实际调用对象，保留 Spring 代理及其拦截链。
     * @param method 已通过签名校验的消费声明。
     * @return 可在该对象上调用的方法；代理未导出或模块禁止访问时以 CONFIG 拒绝登记。
     */
    private static Method accessibleMethod(Object bean, Method method) {
        try {
            // 在代理上选取入口，不解包目标对象，否则会跳过事务及其它 AOP 拦截器。
            Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
            // public 方法仍可能由非 public 类声明；不能把不可访问的路由发布给消费 owner。
            if (!invocable.canAccess(bean) && !invocable.trySetAccessible())
                throw new IllegalAccessException("消费方法所属模块未开放访问");
            return invocable;
        } catch (RuntimeException | IllegalAccessException failure) {
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "消费方法无法通过 Bean 调用: " + method.toGenericString(), failure);
        }
    }
}

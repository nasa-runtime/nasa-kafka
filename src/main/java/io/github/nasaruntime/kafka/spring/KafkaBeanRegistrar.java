package io.github.nasaruntime.kafka.spring;

import io.github.nasaruntime.kafka.KafkaClients;
import io.github.nasaruntime.kafka.KafkaHandle;
import io.github.nasaruntime.kafka.KafkaProxy;
import io.github.nasaruntime.kafka.config.NasaKafkaProperties;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.*;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.BindContext;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.AbstractBindHandler;
import org.springframework.boot.context.properties.bind.handler.NoUnboundElementsBindHandler;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

import java.util.*;

/**
 * 在 listener 实例化前声明所有命名代理，保证按类型和按名注入使用同一资源。
 */
final class KafkaBeanRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private Environment environment;

    /**
     * 业务作用：接收装配使用的配置环境。
     * <p>
     * 返回：保存装配环境，后续绑定 nasa.kafka 配置时使用；此时不读取或创建 Kafka 资源。
     *
     * @param environment 所属容器环境。
     */
    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    /**
     * 业务作用：验证源声明并注册独立 Bean 定义。
     * <p>
     * 返回：启用且具有源声明时注册代理、业务句柄和统一生命周期 Bean 定义；已有目录或未启用时跳过，不在此建立连接。
     *
     * @param metadata 导入元数据
     * @param registry 所属容器。
     */
    @Override
    public void registerBeanDefinitions(AnnotationMetadata metadata, BeanDefinitionRegistry registry) {
        if (registry.containsBeanDefinition("nasaKafkaClients")) return;
        Set<String> rootFields = new HashSet<>();
        BindHandler handler = new AbstractBindHandler(new NoUnboundElementsBindHandler(BindHandler.DEFAULT)) {
            /**
             * 业务作用：保留显式声明的根字段，阻止新旧配置混用时静默丢弃。
             *
             * @param name 配置路径
             * @param target 目标
             * @param context 绑定环境
             * @param result 绑定结果。
             * @return 原绑定结果。
             */
            @Override
            public Object onSuccess(ConfigurationPropertyName name, Bindable<?> target, BindContext context, Object result) {
                if (name.getNumberOfElements() == 3)
                    rootFields.add(name.getLastElement(ConfigurationPropertyName.Form.DASHED));
                return super.onSuccess(name, target, context, result);
            }
        };
        NasaKafkaProperties root = Binder.get(environment).bind("nasa.kafka", Bindable.of(NasaKafkaProperties.class), handler).orElseGet(NasaKafkaProperties::new);
        if (!root.isEnabled()) return;
        Map<String, NasaKafkaProperties> declared = new TreeMap<>(root.getSources());
        if (declared.isEmpty()) {
            if (root.getBootstrapServers() == null) return;
            declared.put(KafkaProxy.PRIMARY, root);
        } else if (rootFields.stream().anyMatch(field -> !Set.of("sources", "enabled", "default-source", "export-static").contains(field))) {
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "旧单源连接字段与 sources 不能同时声明");
        }
        if (!declared.containsKey(root.getDefaultSource()))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "default-source 必须显式指向已声明源");
        Set<String> names = new HashSet<>();
        for (Map.Entry<String, NasaKafkaProperties> source : declared.entrySet()) {
            source.getValue().effective(source.getKey());
            for (String bean : List.of(beanName(source.getKey()), handleName(source.getKey()))) {
                if (!names.add(bean) || registry.containsBeanDefinition(bean) || registry.isAlias(bean))
                    throw new KafkaFailure(KafkaFailure.Code.CONFIG, "Kafka Bean 名冲突: " + bean);
            }
        }
        ManagedMap<String, Object> references = new ManagedMap<>();
        for (Map.Entry<String, NasaKafkaProperties> source : declared.entrySet()) {
            String bean = beanName(source.getKey());
            RootBeanDefinition definition = new RootBeanDefinition(KafkaProxy.class);
            definition.setInstanceSupplier(() -> new KafkaProxy(source.getKey(), source.getValue()));
            definition.setPrimary(source.getKey().equals(root.getDefaultSource()));
            registry.registerBeanDefinition(bean, definition);
            RootBeanDefinition handle = new RootBeanDefinition(KafkaHandle.class);
            handle.setFactoryBeanName(bean);
            handle.setFactoryMethodName("handle");
            handle.setPrimary(source.getKey().equals(root.getDefaultSource()));
            registry.registerBeanDefinition(handleName(source.getKey()), handle);
            references.put(source.getKey(), new RuntimeBeanReference(bean));
        }
        registry.registerBeanDefinition("nasaKafkaClients", BeanDefinitionBuilder.genericBeanDefinition(KafkaClients.class)
                .addConstructorArgValue(references).addConstructorArgValue(root.getDefaultSource())
                .addConstructorArgValue(root.isExportStatic()).getBeanDefinition());
    }

    /**
     * 业务作用：由规范源名派生固定注入名。
     *
     * @param id 源名。
     * @return 与默认选择无关的 Bean 名。
     */
    private static String beanName(String id) {
        return KafkaProxy.PRIMARY.equals(id) ? "kafkaProxy" : id + "KafkaProxy";
    }

    /**
     * 业务作用：为默认只读管理的业务句柄派生稳定注入名。
     *
     * @param id 规范源名。
     * @return 与资源所有者 Bean 区分的固定名称。
     */
    private static String handleName(String id) {
        return KafkaProxy.PRIMARY.equals(id) ? "kafkaHandle" : id + "KafkaHandle";
    }
}

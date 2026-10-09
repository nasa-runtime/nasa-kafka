package io.github.nasaruntime.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

/**
 * 类作用 - Kafka 组件自动配置类, 在配置了 nasa.kafka.bootstrap-servers 时注册 KafkaProxy 并接入 Spring 生命周期。
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(NasaKafkaProperties.class)
@ConditionalOnProperty(prefix = "nasa.kafka", name = "bootstrap-servers")
@Order(Ordered.HIGHEST_PRECEDENCE + 2)
public class NasaKafkaConfig {

    /**
     * 业务作用：装配 Kafka 组件主入口。
     * 声明 {@code destroyMethod = "after"} 使容器销毁时也会走一次收尾，与下方的关闭监听器构成双重保障——
     * 两条路径都可能触发，收尾逻辑因此必须幂等。
     *
     * @param properties Kafka 组件配置
     * @param context    Spring 上下文，用于扫描容器内的 KafkaListener Bean
     * @return 尚未启动的 KafkaProxy，实际启动由刷新事件监听器触发。
     */
    @Bean(destroyMethod = "after")
    public KafkaProxy kafkaProxy(NasaKafkaProperties properties, ApplicationContext context) {
        return new KafkaProxy(properties, context);
    }

    /**
     * 业务作用：在容器刷新完成后才启动消费，确保 listener Bean 已全部就绪。
     * 若在 Bean 初始化阶段就启动，消息可能在依赖尚未注入完成时被投递进来。
     * <p>
     * 启动异常只记日志不向外抛：Kafka 不可用不应导致整个应用启动失败，
     * 其余不依赖 Kafka 的能力仍应可用。代价是启动失败只能从日志发现，需由监控覆盖。
     *
     * @param kafkaProxy 待启动的 Kafka 主入口
     * @return 注册进容器的刷新事件监听器。
     */
    @Bean
    public ApplicationListener<ContextRefreshedEvent> kafkaProxyStarter(KafkaProxy kafkaProxy) {
        return event -> {
            try {
                kafkaProxy.before();
            } catch (Throwable t) {
                log.error("[kafka] before() failed", t);
            }
        };
    }

    /**
     * 业务作用：在容器关闭时停止消费并释放 producer、consumer 与 admin 连接。
     * 异常只记日志不外抛：停机路径上抛异常会打断后续组件的关闭，导致其它资源泄漏。
     *
     * @param kafkaProxy 待停止的 Kafka 主入口
     * @return 注册进容器的关闭事件监听器。
     */
    @Bean
    public ApplicationListener<ContextClosedEvent> kafkaProxyStopper(KafkaProxy kafkaProxy) {
        return event -> {
            try {
                kafkaProxy.after();
            } catch (Throwable t) {
                log.error("[kafka] after() failed", t);
            }
        };
    }
}

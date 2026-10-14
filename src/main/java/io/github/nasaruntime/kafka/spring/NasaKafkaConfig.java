package io.github.nasaruntime.kafka.spring;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Import;

/**
 * 显式导入与 Boot 自动配置共用命名源装配，未配置连接时不创建后台资源。
 */
@AutoConfiguration
@Import(KafkaBeanRegistrar.class)
public class NasaKafkaConfig {
}

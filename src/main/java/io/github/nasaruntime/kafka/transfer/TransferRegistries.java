package io.github.nasaruntime.kafka.transfer;

import io.github.nasaruntime.kafka.schema.SchemaRegistryClient;

/**
 * 固定名称与实际Registry客户端的绑定；转发任务借用其能力，不取得客户端关闭权。
 */
public record TransferRegistries(String sourceName, SchemaRegistryClient source, String destinationName,
                                 SchemaRegistryClient destination) {

    /**
     * 业务作用：把转发计划批准的Registry身份连接到具体客户端。
     * <p>
     * 返回：完整绑定；关闭顺序仍由应用保证晚于依赖任务排干。
     *
     * @param sourceName      计划中声明的来源Registry逻辑名称。
     * @param source          读取来源schema的真实Registry客户端。
     * @param destinationName 计划中声明的目标Registry逻辑名称。
     * @param destination     验证或读取目标schema的真实Registry客户端。
     */
    public TransferRegistries {
        if (sourceName == null || destinationName == null || source == null || destination == null || sourceName.isBlank() || destinationName.isBlank())
            throw new TransferException(TransferException.Code.INVALID_CONFIGURATION);
    }
}

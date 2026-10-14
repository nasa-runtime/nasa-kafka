package io.github.nasaruntime.kafka.consumer;

/**
 * 业务完成与输入确认的关系；Manual 仍要求整个 invocation 正常结束。
 */
public enum AckMode {
    /**
     * 调用正常结束且分区权威仍有效时才将该输入标记安全。
     */
    AUTO,
    /**
     * 除调用正常结束外，还要求本次调用内显式 ack；提前 ack 不覆盖后续调用失败。
     */
    MANUAL
}

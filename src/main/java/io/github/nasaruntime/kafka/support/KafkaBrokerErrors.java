package io.github.nasaruntime.kafka.support;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import org.apache.kafka.common.errors.AuthorizationException;

/**
 * 依据原生错误类型保留权限拒绝，禁止通过远端诊断文本推断授权结论。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class KafkaBrokerErrors {

    /**
     * 业务作用：禁止实例化无状态错误分类器。
     * <p>
     * 参数说明：无。
     * 返回：不创建资源。
     */
    private KafkaBrokerErrors() {}

    /**
     * 业务作用：识别原生及受管包装中的明确授权拒绝，避免把权限问题交给瞬态重试。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param error 待分类原因链。
     * @return 存在结构化授权证据时 true；未知或过深原因链不推断权限。
     */
    public static boolean authorization(Throwable error) {
        for (int depth = 0; error != null && depth < 32; depth++, error = error.getCause()) {
            if (error instanceof AuthorizationException
                    || error instanceof KafkaFailure failure && failure.code() == KafkaFailure.Code.AUTHORIZATION)
                return true;
        }
        return false;
    }
}

package io.github.nasaruntime.kafka.dlt;

import io.github.nasaruntime.kafka.admin.KafkaAdmin;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import java.util.Map;

/**
 * DLT 目标保留策略门禁；配置快照不能约束随后发生的管理员操作或保证无限保留。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public final class DltRetention {

    /**
     * 业务作用：禁止实例化无状态的保留规则。
     * <p>
     * 参数说明：无。
     * <p>
     * 返回：仅限制实例构造，不建立连接、启动线程或创建业务状态。
     */
    private DltRetention() {}

    /**
     * 业务作用：拒绝会因压缩或立即删除丢失失败历史的目标。
     * <p>
     * 返回：仅delete且时间、容量保留值为正或-1时通过；缺失、隐藏和非法配置拒绝，不修改broker。
     *
     * <p>
     * 供框架功能包协作使用，不作为稳定的业务接入接口。
     * @param settings broker返回的有效topic配置。
     */
    public static void validate(Map<String, KafkaAdmin.Setting> settings) {
        KafkaAdmin.Setting cleanup = settings.get("cleanup.policy");
        if (cleanup == null || cleanup.sensitive() || cleanup.value() == null || !cleanup.value().trim().equals("delete"))
            throw new KafkaFailure(KafkaFailure.Code.CONFIG, "DLT 目标 cleanup.policy 必须为 delete");
        for (String key : new String[]{"retention.ms", "retention.bytes"}) {
            KafkaAdmin.Setting setting = settings.get(key);
            try {
                if (setting == null || setting.sensitive() || setting.value() == null)
                    throw new NumberFormatException();
                long value = Long.parseLong(setting.value());
                if (value != -1 && value <= 0) throw new NumberFormatException();
            } catch (NumberFormatException invalid) {
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "DLT 目标 retention 必须为正或 -1");
            }
        }
    }
}

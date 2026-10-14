package io.github.nasaruntime.kafka.replay;


/**
 * 有限回放的同步成功合同；并行分区可能同时调用，返回必须代表本条实际副作用已经结束。
 */
public interface ReplayProcessor {

    /**
     * 业务作用：将实际处理路线与已审阅计划绑定。
     * <p>
     * 参数说明：无。
     *
     * @return 稳定 SHA-256，覆盖 schema、转换、过滤、目标和成功条件，执行期间不得变化。
     */
    String fingerprint();

    /**
     * 业务作用：在独立有界 worker 内完成一条回放输入的实际处理。
     * <p>
     * 返回：正常返回表示副作用全部成功且结束；抛出异常停止新准入，不推进该记录。不得仅启动后台工作便返回，也不能把等待取消解释为远端责任结束。
     *
     * @param record 拥有独立字节的原始记录，tombstone、key 和重复头均保留。
     */
    void process(ReplayRecord record) throws Exception;
}

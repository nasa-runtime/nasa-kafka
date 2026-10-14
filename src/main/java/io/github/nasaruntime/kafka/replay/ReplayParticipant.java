package io.github.nasaruntime.kafka.replay;

import java.util.function.BooleanSupplier;

/**
 * 框架内部的多资源任务依赖；激活前不得取得 lease 或执行业务副作用。
 * <p>
 * 供框架功能包协作使用，不作为稳定的业务接入接口。
 */
public interface ReplayParticipant {

    /**
     * 业务作用：等待全部依赖完成登记，并在来源停止时放弃尚未激活的任务。
     *
     * @param stopping 本任务即时停止门。
     * @return 允许实际执行时 true，停止时 false。
     */
    boolean activate(BooleanSupplier stopping) throws InterruptedException;

    /**
     * 业务作用：在所有 worker、consumer 和 checkpoint lease 退出后归还其它来源的责任。
     * <p>
     * 参数说明：无。
     * 返回：依赖登记释放，不再阻塞目标关闭。
     */
    void finished();
}

package io.github.nasaruntime.kafka;

import io.github.nasaruntime.kafka.consumer.listener.KafkaListener;
import io.github.nasaruntime.kafka.spring.KafkaMethodListeners;

import io.github.nasaruntime.kafka.exception.KafkaFailure;

import io.github.nasaruntime.kafka.consumer.ReadyRequirement;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.*;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ApplicationContextEvent;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

/**
 * 命名源的统一资源所有者，在同一应用中组织多个独立 Kafka 集群并协调启动与两段停机。
 * <p>
 * 先登记全部路由再启动消费；默认源只用于省略来源的声明，未知显式源不会回退。
 * 每个源冻结自己的连接和认证配置，源内 producer lane 分别限制发送与回执责任。
 * 业务通过 {@link #handle(String)} 取得绑定来源的能力，资源管理使用 {@link #require(String)}。
 * <p>
 * 全局目录只导出显式受管实例；独立容器可关闭静态导出以隔离同名源。
 * 同次容器关闭共用总期限，等待失败仍保留真实资源与名称责任，后续关闭继续收尾。
 */
public final class KafkaClients implements ApplicationContextAware, ApplicationListener<ApplicationContextEvent>, DisposableBean {

    private static final Map<String, KafkaProxy> EXPORTED = new TreeMap<>();
    private static KafkaClients defaultOwner;
    private final Map<String, KafkaProxy> sources;
    private final String defaultSource;
    private final boolean exportStatic;
    private ApplicationContext context;
    private boolean started;
    /**
     * 同一次 Spring 容器关闭的事件与销毁回调共享预算，不能各自重新开始计时。
     */
    private Long contextShutdownDeadline;

    /**
     * 业务作用：冻结一组代理及默认选择。
     * <p>
     * 返回：尚未启动的资源所有者。
     *
     * @param sources       命名资源
     * @param defaultSource 默认
     * @param exportStatic  是否导出静态查找。
     */
    public KafkaClients(Map<String, KafkaProxy> sources, String defaultSource, boolean exportStatic) {
        this.sources = Collections.unmodifiableMap(new TreeMap<>(sources));
        this.defaultSource = defaultSource;
        this.exportStatic = exportStatic;
        if (!sources.containsKey(defaultSource)) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "默认源不存在");
        sources.forEach((id, proxy) -> {
            if (!id.equals(proxy.sourceId()))
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "源目录与代理身份不一致");
        });
    }

    /**
     * 业务作用：取得作用域内显式源。
     *
     * @param id 规范源名。
     * @return 未知源失败，绝不回退。
     */
    public KafkaProxy require(String id) {
        KafkaProxy proxy = sources.get(id);
        if (proxy == null) throw new KafkaFailure(KafkaFailure.Code.NO_SUCH_SOURCE, "未知源: " + id);
        return proxy;
    }

    /**
     * 业务作用：按规范源名取得无整源关闭权的业务句柄。
     *
     * @param id 显式来源。
     * @return 同源受管视图；未知来源拒绝，默认管理只读。
     */
    public KafkaHandle handle(String id) {
        return require(id).handle();
    }

    /**
     * 业务作用：读取当前作用域源目录。
     * <p>
     * 参数说明：无。
     *
     * @return 不可修改目录。
     */
    public Map<String, KafkaProxy> sources() {
        return sources;
    }

    /**
     * 业务作用：等待全部 required 源的 broker 身份及消费分配，生产者专用源也须验证 metadata。
     * <p>
     * 返回：所有必需源 Ready 或失败；可选源仍可通过 health 独立观测。
     *
     * @param timeout 所有源共享的总预算。
     */
    public void awaitReady(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        if (!started) throw new KafkaFailure(KafkaFailure.Code.LIFECYCLE, "源目录尚未启动");
        for (KafkaProxy proxy : sources.values())
            if (proxy.config().isRequired()) {
                proxy.admin().clusterId(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
            }
        Map<KafkaProxy, Map<String, ReadyRequirement>> requirements = new LinkedHashMap<>();
        for (KafkaProxy proxy : sources.values())
            if (proxy.config().isRequired()) {
                Map<String, ReadyRequirement> groups = new TreeMap<>();
                proxy.groupHealth().keySet().forEach(group -> groups.put(group, ReadyRequirement.ASSIGNED));
                if (!groups.isEmpty()) requirements.put(proxy, groups);
            }
        while (true) {
            for (var entry : requirements.entrySet())
                entry.getKey().awaitGroupsReady(entry.getValue(),
                        Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
            // 等后续源期间前面的组可能失权；最终复验全部消费条件，不能锁存早先的成功。
            boolean allReady = requirements.entrySet().stream().allMatch(entry -> entry.getValue().entrySet().stream()
                    .allMatch(group -> entry.getKey().groupReadiness(group.getKey(), group.getValue()).ready()));
            if (allReady) return;
        }
    }

    /**
     * 业务作用：绑定拥有本资源的 Spring 容器。
     * <p>
     * 返回：保存所属容器引用，后续只响应该容器的生命周期事件。
     *
     * @param context 所属容器。
     */
    @Override
    public void setApplicationContext(ApplicationContext context) {
        this.context = context;
    }

    /**
     * 业务作用：只响应所属容器生命周期，避免父子容器重复启动关闭。
     * <p>
     * 返回：所属容器刷新时装配并启动路由，关闭时执行统一收尾；启动或关闭失败向容器传播。
     *
     * @param event 容器事件。
     */
    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event.getApplicationContext() != context) return;
        if (event instanceof ContextRefreshedEvent) {
            List<KafkaListener> listeners = new ArrayList<>(context.getBeansOfType(KafkaListener.class).values());
            context.getBeansOfType(Object.class).values().forEach(bean -> listeners.addAll(KafkaMethodListeners.from(bean, context.getEnvironment()::resolveRequiredPlaceholders)));
            start(listeners);
        }
        if (event instanceof ContextClosedEvent) {
            synchronized (this) {
                // 先冻结容器停机边界；随后的 DisposableBean 回调只能消费剩余预算。
                if (contextShutdownDeadline == null) contextShutdownDeadline = shutdownDeadline();
            }
            destroy();
        }
    }

    /**
     * 业务作用：冻结全部路由后一次性开放消费，并登记静态身份。
     * <p>
     * 返回：全部来源完成启动调用后标记 started，实际分配仍须等待 Ready；失败时尝试关闭本次资源，未退出责任保留并随异常报告。
     *
     * @param listeners 待绑定声明。
     */
    @SuppressWarnings("rawtypes")
    public synchronized void start(Collection<? extends KafkaListener> listeners) {
        if (started) return;
        try {
            for (KafkaListener listener : listeners) {
                String[] requested = listener.qualifiers();
                Set<String> targets = requested == null ? Set.of(defaultSource) : new LinkedHashSet<>(List.of(requested));
                if (targets.isEmpty()) throw new KafkaFailure(KafkaFailure.Code.CONFIG, "listener sources 不能为空");
                for (String id : targets) require(id).register(listener);
            }
            // 全部路由先冻结，避免后续声明失败时已有 owner 开始消费未完整配置的业务。
            if (exportStatic) export();
            for (KafkaProxy proxy : sources.values()) proxy.before();
            started = true;
        } catch (Throwable failure) {
            // 登记或启动失败仍由本资源所有者收口；旧活动容器的静态身份不会被覆盖或注销。
            try {
                destroy();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /**
     * 业务作用：原子占有静态名称，不能覆盖其它活动容器。
     * <p>
     * 参数说明：无。
     * 返回：完整登记或完全拒绝。
     */
    private void export() {
        synchronized (EXPORTED) {
            // 默认指针与源目录必须同时占有，防止不同容器解析出不一致的默认源。
            if (defaultOwner != null && defaultOwner != this)
                throw new KafkaFailure(KafkaFailure.Code.CONFIG, "已有静态 Kafka 作用域；独立容器请关闭 export-static");
            for (String id : sources.keySet())
                if (EXPORTED.containsKey(id) && EXPORTED.get(id) != sources.get(id))
                    throw new KafkaFailure(KafkaFailure.Code.CONFIG, "源名已占用: " + id);
            EXPORTED.putAll(sources);
            defaultOwner = this;
        }
    }

    /**
     * 业务作用：使用同一总期限关闭全部源，同次容器关闭的重复回调不续期。
     * <p>
     * 参数说明：无。
     * 返回：全部完成或聚合失败；超时后可显式 shutdownUntil 继续等待。
     */
    @Override
    public synchronized void destroy() {
        shutdownUntil(contextShutdownDeadline == null ? shutdownDeadline() : contextShutdownDeadline);
    }

    /**
     * 业务作用：从全部源声明中选取一次统一的容器关闭预算。
     * <p>
     * 参数说明：无。
     *
     * @return 最大配置预算对应的单调纳秒截止时间，不按源累加。
     */
    private long shutdownDeadline() {
        long timeout = sources.values().stream().mapToLong(p -> p.config().getShutdownTimeoutMs()).max().orElse(30000);
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }

    /**
     * 业务作用：以调用方的总期限推进所有源实际退出，允许超时后显式继续收尾。
     * <p>
     * 返回：全部资源退出或聚合失败；不重新开放已封闭业务准入。
     *
     * @param deadline 所有来源共用的单调纳秒截止值。
     */
    public synchronized void shutdownUntil(long deadline) {
        RuntimeException failure = null;
        for (KafkaProxy proxy : sources.values()) {
            try {
                proxy.shutdownUntil(deadline);
            } catch (RuntimeException error) {
                if (failure == null) failure = error;
                else failure.addSuppressed(error);
            } finally {
                // 即使 owner 仍未退出，也不能让已经结束的容器继续接纳新业务；原责任与名称占用继续保留。
                proxy.sealManagedAccess();
            }
        }
        synchronized (EXPORTED) {
            // 关闭超时仍保留名称占用，防止新容器与未退出的 owner 同时持有相同逻辑源。
            if (defaultOwner == this && sources.values().stream().allMatch(KafkaProxy::isClosed)) defaultOwner = null;
        }
        if (failure != null) throw failure;
    }

    /**
     * 业务作用：从静态目录查找规范源或 Bean 别名。
     *
     * @param name 显式名称。
     * @return 未知为 null。
     */
    static KafkaProxy load(String name) {
        synchronized (EXPORTED) {
            if (name == null) return null;
            KafkaProxy direct = EXPORTED.get(name);
            if (direct != null) return direct;
            return EXPORTED.values().stream().filter(p -> p.getQualifier().equals(name)).findFirst().orElse(null);
        }
    }

    /**
     * 业务作用：解析拥有静态目录的默认指针。
     * <p>
     * 参数说明：无。
     *
     * @return 未登记为 null。
     */
    static KafkaProxy loadDefault() {
        synchronized (EXPORTED) {
            return defaultOwner == null ? null : EXPORTED.get(defaultOwner.defaultSource);
        }
    }

    /**
     * 业务作用：列出静态目录的规范名称。
     * <p>
     * 参数说明：无。
     *
     * @return 排序快照。
     */
    static String[] sourceIds() {
        synchronized (EXPORTED) {
            return EXPORTED.keySet().toArray(String[]::new);
        }
    }

    /**
     * 业务作用：在调用方已确认源关闭后按实例注销静态名称，旧实例不能移除同名新实例。
     * <p>
     * 返回：移除匹配的静态登记；所属目录的源全部关闭时清除默认指针，不执行资源关闭。
     *
     * @param proxy 退出实例。
     */
    static void remove(KafkaProxy proxy) {
        synchronized (EXPORTED) {
            EXPORTED.remove(proxy.sourceId(), proxy);
            if (defaultOwner != null && defaultOwner.sources.values().stream().allMatch(KafkaProxy::isClosed))
                defaultOwner = null;
        }
    }
}

package io.github.nasaruntime.kafka.checkpoint;



import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * 单一可写 MySQL 主节点上的共享进度。每份 lease 独占物理会话与命名锁，不支持多主写入。
 * 故障切换必须由外部保证旧主失去写权限；SQL 所有权检查不能原子撤销 Kafka 或其它系统中的在途副作用。
 * 接入方显式提供 MySQL Connector/J 8 或更高版本；同步调用应由业务的有界执行资源承担。
 */
public final class MySqlCheckpointStore implements CheckpointStore {

    private static final String TABLE = "nasa_kafka_checkpoint";
    private static final String DDL = "CREATE TABLE IF NOT EXISTS nasa_kafka_checkpoint (namespace VARBINARY(128) NOT NULL, task_id VARBINARY(128) NOT NULL, generation BIGINT NOT NULL, revision BIGINT NOT NULL, body MEDIUMBLOB NOT NULL, PRIMARY KEY(namespace,task_id)) ENGINE=InnoDB";
    private static final Set<String> PROPERTIES = Set.of("user", "password", "sslMode", "trustCertificateKeyStoreUrl",
            "trustCertificateKeyStorePassword", "trustCertificateKeyStoreType", "clientCertificateKeyStoreUrl",
            "clientCertificateKeyStorePassword", "clientCertificateKeyStoreType", "enabledTLSProtocols", "serverRSAPublicKeyFile");
    private final String url;
    private final String database;
    private final String namespace;
    private final Properties properties;
    private final int timeoutMillis;
    private final Semaphore capacity;
    private final Set<Session> retiring = ConcurrentHashMap.newKeySet();

    /**
     * 业务作用：冻结共享进度的端点、安全参数及连接上限，避免旧 lease 自动重连复用权限。
     * <p>
     * 返回：尚未连接的存储；非法配置或缺少 Connector/J 时拒绝。
     *
     * @param jdbcUrl    单个 MySQL 主库的 JDBC URL，包含数据库名，不允许查询参数、用户信息或片段。
     * @param properties 仅含凭据与TLS声明的连接属性；sslMode默认VERIFY_IDENTITY，本机明文需显式DISABLED。
     * @param namespace  稳定隔离名
     * @param timeout    100ms 至 60s 的单次调用预算
     * @param capacity   1 至 1024 个实际会话额度。
     */
    public MySqlCheckpointStore(String jdbcUrl, Properties properties, String namespace, Duration timeout, int capacity) {
        try {
            CheckpointWire.key(namespace);
            if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:mysql://") || properties == null || timeout == null
                    || timeout.compareTo(Duration.ofMillis(100)) < 0 || timeout.compareTo(Duration.ofSeconds(60)) > 0
                    || capacity < 1 || capacity > 1024) throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            URI endpoint = URI.create(jdbcUrl.substring(5));
            if (endpoint.getHost() == null || endpoint.getRawQuery() != null || endpoint.getRawUserInfo() != null
                    || endpoint.getRawFragment() != null || endpoint.getPort() == 0 || endpoint.getPort() > 65535
                    || endpoint.getRawPath() == null || !endpoint.getRawPath().matches("/[A-Za-z0-9_$-]{1,64}"))
                throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            this.database = endpoint.getRawPath().substring(1);
            this.properties = new Properties();
            for (String name : properties.stringPropertyNames()) {
                if (!PROPERTIES.contains(name)) throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
                this.properties.setProperty(name, properties.getProperty(name));
            }
            if (properties.size() != properties.stringPropertyNames().size())
                throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            String ssl = this.properties.getProperty("sslMode", "VERIFY_IDENTITY");
            if (!Set.of("VERIFY_IDENTITY", "DISABLED").contains(ssl))
                throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            this.properties.setProperty("sslMode", ssl);
            this.timeoutMillis = Math.toIntExact(timeout.toMillis());
            this.properties.setProperty("connectTimeout", Integer.toString(timeoutMillis));
            this.properties.setProperty("socketTimeout", Integer.toString(timeoutMillis));
            this.properties.setProperty("autoReconnect", "false");
            this.properties.setProperty("autoReconnectForPools", "false");
            this.properties.setProperty("allowMultiQueries", "false");
            Driver driver = DriverManager.getDriver(jdbcUrl);
            if (!driver.getClass().getName().equals("com.mysql.cj.jdbc.Driver") || driver.getMajorVersion() < 8)
                throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            this.url = jdbcUrl;
            this.namespace = namespace;
            this.capacity = new Semaphore(capacity);
        } catch (IllegalArgumentException | SQLException rejected) {
            throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
        }
    }

    /**
     * 业务作用：显式建立固定 InnoDB 表并验明结构，不让普通任务承担 DDL。
     * <p>
     * 参数说明：无。
     * 返回：表结构及持久设置通过才成功；DDL 响应未知时可再次显式调用确认。
     */
    public void initialize() {
        long deadline = deadline();
        try (Session session = open(deadline)) {
            validateServer(session, deadline);
            try (PreparedStatement statement = prepare(session, DDL, deadline)) {
                statement.executeUpdate();
            } catch (SQLException rejected) {
                throw failure(CheckpointException.Code.OUTCOME_UNKNOWN);
            }
            validateSchema(session, deadline);
        }
    }

    /**
     * 业务作用：只读加载共享的完整进度，不授予命名锁或业务写权限。
     *
     * @param taskId 精确任务名。
     * @return 不存在为空；超限、损坏及行元数据不一致拒绝。
     */
    @Override
    public Optional<ReplayCheckpoint> load(String taskId) {
        CheckpointWire.key(taskId);
        long deadline = deadline();
        try (Session session = open(deadline)) {
            validateServer(session, deadline);
            validateSchema(session, deadline);
            return Optional.ofNullable(read(session, taskId, deadline));
        }
    }

    /**
     * 业务作用：取得专用会话的任务锁，持久化新代次后才开放推进。
     *
     * @param initial 尚未推进的冻结合同。
     * @return 已持久化的唯一 lease；竞争、合同改变或证据不足时拒绝并退出连接。
     */
    @Override
    public CheckpointLease claim(ReplayCheckpoint initial) {
        if (initial == null || initial.generation() != 0 || initial.revision() != 0
                || initial.positions().stream().anyMatch(p -> p.nextOffset() != p.startOffset()))
            throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
        long deadline = deadline();
        Session session = open(deadline);
        try {
            validateServer(session, deadline);
            validateSchema(session, deadline);
            String lock = lockName(initial.taskId());
            long connectionId;
            // 锁竞争不等待其它任务，只有当前物理会话的成功响应才能成为 claim 依据。
            try (PreparedStatement statement = prepare(session, "SELECT GET_LOCK(?,0), CONNECTION_ID()", deadline)) {
                statement.setString(1, lock);
                try (ResultSet rows = statement.executeQuery()) {
                    if (!rows.next()) throw failure(CheckpointException.Code.IO);
                    int acquired = rows.getInt(1);
                    if (rows.wasNull()) throw failure(CheckpointException.Code.IO);
                    if (acquired != 1) throw failure(CheckpointException.Code.BUSY);
                    connectionId = rows.getLong(2);
                }
            } catch (SQLException rejected) {
                throw failure(CheckpointException.Code.IO);
            }
            ReplayCheckpoint previous = read(session, initial.taskId(), deadline);
            if (previous != null && !initial.sameContract(previous))
                throw failure(CheckpointException.Code.CONTRACT_MISMATCH);
            ReplayCheckpoint next = previous == null
                    ? new ReplayCheckpoint(initial.taskId(), initial.contractFingerprint(), 1, 0, initial.positions())
                    : new ReplayCheckpoint(previous.taskId(), previous.contractFingerprint(), increment(previous.generation()), increment(previous.revision()), previous.positions());
            // 完整 CAS 与锁所有者条件在同一 SQL 中成立，确认持久化以后才交付 lease。
            persist(session, lock, connectionId, previous, next, deadline);
            return new Lease(session, lock, connectionId, next);
        } catch (RuntimeException rejected) {
            try {
                session.close();
            } catch (RuntimeException retained) {
            }
            throw rejected;
        }
    }

    /**
     * 业务作用：限制建连、短操作及长期 lease 的实际资源总量。
     *
     * @param deadline 调用总截止点。
     * @return 独占物理连接；不排队，无额度时明确拒绝。
     */
    private Session open(long deadline) {
        for (Session pending : retiring)
            try {
                pending.close();
            } catch (RuntimeException retained) {
            }
        if (!capacity.tryAcquire()) throw failure(CheckpointException.Code.CAPACITY);
        try {
            Properties snapshot = (Properties) properties.clone();
            snapshot.setProperty("connectTimeout", Integer.toString(remaining(deadline)));
            return new Session(DriverManager.getConnection(url, snapshot));
        } catch (SQLException | RuntimeException rejected) {
            capacity.release();
            throw failure(CheckpointException.Code.IO);
        }
    }

    /**
     * 业务作用：拒绝不能持久提交的从节点、非 MySQL 服务或会话配置。
     * <p>
     * 返回：单主自动提交及持久化设置均成立时成功。
     *
     * @param session  专用连接
     * @param deadline 总截止点。
     */
    private void validateServer(Session session, long deadline) {
        try {
            DatabaseMetaData metadata = session.connection.getMetaData();
            if (!metadata.getDatabaseProductName().equals("MySQL") || metadata.getDatabaseMajorVersion() < 8
                    || !database.equals(session.connection.getCatalog()) || !session.connection.getAutoCommit())
                throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            try (PreparedStatement statement = prepare(session, "SELECT @@global.read_only, @@global.super_read_only, @@global.innodb_flush_log_at_trx_commit, @@global.log_bin, @@global.sync_binlog, @@session.binlog_format, @@session.autocommit", deadline);
                 ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || rows.getInt(1) != 0 || rows.getInt(2) != 0 || rows.getInt(3) != 1
                        || rows.getInt(7) != 1 || (rows.getInt(4) != 0 && (rows.getInt(5) != 1 || !"ROW".equals(rows.getString(6)))))
                    throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            }
        } catch (SQLException rejected) {
            throw failure(CheckpointException.Code.IO);
        }
    }

    /**
     * 业务作用：把共享表结构作为持久合同，禁止不同引擎、触发器或不完整主键改变 CAS 语义。
     * <p>
     * 返回：精确结构匹配才允许读写。
     *
     * @param session  专用连接
     * @param deadline 总截止点。
     */
    private void validateSchema(Session session, long deadline) {
        try (PreparedStatement statement = prepare(session, "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", deadline)) {
            statement.setString(1, database);
            statement.setString(2, TABLE);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next() || !"InnoDB".equals(rows.getString(1)) || rows.next())
                    throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            }
            try (PreparedStatement columns = prepare(session, "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION", deadline)) {
                columns.setString(1, database);
                columns.setString(2, TABLE);
                String[][] expected = {{"namespace", "varbinary(128)"}, {"task_id", "varbinary(128)"}, {"generation", "bigint"}, {"revision", "bigint"}, {"body", "mediumblob"}};
                try (ResultSet rows = columns.executeQuery()) {
                    for (String[] column : expected)
                        if (!rows.next() || !column[0].equals(rows.getString(1))
                                || !column[1].equals(rows.getString(2)) || !"NO".equals(rows.getString(3)) || !rows.getString(4).isEmpty())
                            throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
                    if (rows.next()) throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
                }
            }
            try (PreparedStatement keys = prepare(session, "SELECT INDEX_NAME,COLUMN_NAME,SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX", deadline)) {
                keys.setString(1, database);
                keys.setString(2, TABLE);
                try (ResultSet rows = keys.executeQuery()) {
                    for (String name : List.of("namespace", "task_id"))
                        if (!rows.next() || !"PRIMARY".equals(rows.getString(1)) || !name.equals(rows.getString(2)) || rows.getObject(3) != null)
                            throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
                    if (rows.next()) throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
                }
            }
            try (PreparedStatement triggers = prepare(session, "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?", deadline)) {
                triggers.setString(1, database);
                triggers.setString(2, TABLE);
                try (ResultSet rows = triggers.executeQuery()) {
                    if (!rows.next() || rows.getLong(1) != 0)
                        throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
                }
            }
        } catch (SQLException rejected) {
            throw failure(CheckpointException.Code.IO);
        }
    }

    /**
     * 业务作用：在服务端限制正文返回大小并核对全部持久身份，避免巨型或错位行参与恢复。
     *
     * @param session  专用连接
     * @param task     精确任务名
     * @param deadline 总截止点。
     * @return 可信快照或不存在，损坏时拒绝。
     */
    private ReplayCheckpoint read(Session session, String task, long deadline) {
        try (PreparedStatement statement = prepare(session, "SELECT generation,revision,OCTET_LENGTH(body),CASE WHEN OCTET_LENGTH(body)<=2097152 THEN body ELSE NULL END FROM nasa_kafka_checkpoint WHERE namespace=? AND task_id=?", deadline)) {
            statement.setBytes(1, ascii(namespace));
            statement.setBytes(2, ascii(task));
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return null;
                long generation = rows.getLong(1), revision = rows.getLong(2), size = rows.getLong(3);
                if (size <= 0 || size > CheckpointWire.MAX_BYTES) throw failure(CheckpointException.Code.CORRUPT);
                byte[] body = rows.getBytes(4);
                if (body == null || body.length != size) throw failure(CheckpointException.Code.CORRUPT);
                ReplayCheckpoint checkpoint = CheckpointWire.decode(body);
                if (!checkpoint.taskId().equals(task) || generation <= 0 || checkpoint.generation() != generation
                        || checkpoint.revision() != revision || rows.next())
                    throw failure(CheckpointException.Code.CORRUPT);
                return checkpoint;
            }
        } catch (SQLException rejected) {
            throw failure(CheckpointException.Code.IO);
        }
    }

    /**
     * 业务作用：仅当前锁会话可条件替换精确任务的完整进度，响应未知不得猜测为未写入。
     * <p>
     * 返回：恰好一行确认后成功，零行失权，其它执行异常为未知。
     *
     * @param session      实际持有MySQL命名锁的专用数据库会话。
     * @param lock         该任务申请并已取得的MySQL命名锁名称。
     * @param connectionId 授予该锁的MySQL会话ID，用于复验锁仍由当前会话持有。
     * @param previous     预期正文或首次空值
     * @param next         新快照
     * @param deadline     总截止点。
     */
    private void persist(Session session, String lock, long connectionId, ReplayCheckpoint previous, ReplayCheckpoint next, long deadline) {
        byte[] bytes = CheckpointWire.encode(next);
        String sql = previous == null
                ? "INSERT INTO nasa_kafka_checkpoint(namespace,task_id,generation,revision,body) SELECT ?,?,?,?,? WHERE CONNECTION_ID()=? AND IS_USED_LOCK(?)=CONNECTION_ID()"
                : "UPDATE nasa_kafka_checkpoint SET generation=?,revision=?,body=? WHERE namespace=? AND task_id=? AND generation=? AND revision=? AND body=? AND CONNECTION_ID()=? AND IS_USED_LOCK(?)=CONNECTION_ID()";
        try (PreparedStatement statement = prepare(session, sql, deadline)) {
            if (previous == null) {
                statement.setBytes(1, ascii(namespace));
                statement.setBytes(2, ascii(next.taskId()));
                statement.setLong(3, next.generation());
                statement.setLong(4, next.revision());
                statement.setBytes(5, bytes);
                statement.setLong(6, connectionId);
                statement.setString(7, lock);
            } else {
                statement.setLong(1, next.generation());
                statement.setLong(2, next.revision());
                statement.setBytes(3, bytes);
                statement.setBytes(4, ascii(namespace));
                statement.setBytes(5, ascii(next.taskId()));
                statement.setLong(6, previous.generation());
                statement.setLong(7, previous.revision());
                statement.setBytes(8, CheckpointWire.encode(previous));
                statement.setLong(9, connectionId);
                statement.setString(10, lock);
            }
            if (statement.executeUpdate() != 1) throw failure(CheckpointException.Code.FENCED);
        } catch (SQLException rejected) {
            throw failure(CheckpointException.Code.OUTCOME_UNKNOWN);
        }
    }

    /**
     * 业务作用：对每条 SQL 应用剩余预算，避免无限读写等待。
     *
     * @param session  物理连接
     * @param sql      固定语句
     * @param deadline 当前调用截止点。
     * @return 绑定网络与语句超时的 statement。
     */
    private PreparedStatement prepare(Session session, String sql, long deadline) throws SQLException {
        int remaining = remaining(deadline);
        session.connection.setNetworkTimeout(Runnable::run, remaining);
        PreparedStatement statement = session.connection.prepareStatement(sql);
        try {
            statement.setQueryTimeout((remaining + 999) / 1000);
            return statement;
        } catch (SQLException rejected) {
            statement.close();
            throw rejected;
        }
    }

    /**
     * 业务作用：产生无歧义的双端任务锁名，隔离数据库、表及命名空间。
     *
     * @param task 已验证任务名。
     * @return 64 字符 SHA-256，不暴露业务身份。
     */
    private String lockName(String task) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.write(ascii("NSKMYL01"));
            for (String part : List.of(database, TABLE, namespace, task)) {
                byte[] value = ascii(part);
                out.writeShort(value.length);
                out.write(value);
            }
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (IOException | NoSuchAlgorithmException rejected) {
            throw failure(CheckpointException.Code.IO);
        }
    }

    /**
     * 业务作用：按单调时钟限定一次操作，不受系统时间调整影响。
     * <p>
     * 参数说明：无。
     *
     * @return 本次 SQL 序列的截止点。
     */
    private long deadline() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }

    /**
     * 业务作用：已耗尽预算时拒绝发出下一条操作。
     *
     * @param deadline 单调截止点。
     * @return 至少 1ms 的向上取整剩余预算，超时按 IO 拒绝。
     */
    private static int remaining(long deadline) {
        long nanos = deadline - System.nanoTime();
        if (nanos <= 0) throw failure(CheckpointException.Code.IO);
        return (int) Math.min(Integer.MAX_VALUE, (nanos + 999_999) / 1_000_000);
    }

    /**
     * 业务作用：保持二进制身份绑定的一致字符集。
     *
     * @param text 已校验 ASCII 文本。
     * @return 独立身份字节。
     */
    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * 业务作用：阻止权威序号溢出后复用。
     *
     * @param value 已有序号。
     * @return 下一序号，耗尽则拒绝。
     */
    private static long increment(long value) {
        if (value == Long.MAX_VALUE) throw failure(CheckpointException.Code.CAPACITY);
        return value + 1;
    }

    /**
     * 业务作用：避免存储异常泄露连接参数及正文。
     *
     * @param code 稳定领域分类。
     * @return 没有底层敏感原因链的异常。
     */
    private static CheckpointException failure(CheckpointException.Code code) {
        return new CheckpointException(code);
    }

    /**
     * 物理连接拥有额度，实际本地连接退出以前不能归还。
     */
    private final class Session implements AutoCloseable {
        private final Connection connection;
        private boolean released;

        /**
         * 业务作用：承接已取得额度的物理会话。
         * <p>
         * 返回：拥有唯一关闭责任的对象。
         *
         * @param connection 禁止重连的专用连接。
         */
        Session(Connection connection) {
            this.connection = connection;
        }

        /**
         * 业务作用：先关闭真实连接，再归还额度；失败连接保留引用以便再次退出。
         * <p>
         * 参数说明：无。
         * 返回：实际本地资源退出后成功；SQL 服务器上的失联会话仍须由其超时或运维结束。
         */
        @Override
        public synchronized void close() {
            if (released) return;
            try {
                connection.setNetworkTimeout(Runnable::run, timeoutMillis);
                connection.close();
            } catch (SQLException rejected) {
                try {
                    connection.abort(Runnable::run);
                } catch (SQLException retained) {
                }
            }
            try {
                if (!connection.isClosed()) {
                    retiring.add(this);
                    throw failure(CheckpointException.Code.IO);
                }
                released = true;
                retiring.remove(this);
                capacity.release();
            } catch (SQLException rejected) {
                retiring.add(this);
                throw failure(CheckpointException.Code.IO);
            }
        }
    }

    /**
     * lease 将权限复验、持久化与关闭串行；历史快照不能重新开放已关闭的会话。
     */
    private final class Lease implements CheckpointLease {
        private final Session session;
        private final String lock;
        private final long connectionId;
        private ReplayCheckpoint current;
        private boolean closed;
        private boolean fenced;

        /**
         * 业务作用：发布已持久化代次的唯一推进责任。
         * <p>
         * 返回：可复验且尚未关闭的 lease。
         *
         * @param session      实际持有MySQL命名锁的专用数据库会话。
         * @param lock         该任务申请并已取得的MySQL命名锁名称。
         * @param connectionId 授予该锁的MySQL会话ID，用于复验锁仍由当前会话持有。
         * @param current      确认快照。
         */
        Lease(Session session, String lock, long connectionId, ReplayCheckpoint current) {
            this.session = session;
            this.lock = lock;
            this.connectionId = connectionId;
            this.current = current;
        }

        /**
         * 业务作用：读取最后确认进度，不隐式访问 SQL 或重授权限。
         * <p>
         * 参数说明：无。
         *
         * @return 不可变历史快照。
         */
        @Override
        public synchronized ReplayCheckpoint checkpoint() {
            return current;
        }

        /**
         * 业务作用：在业务派发前证明当前会话仍持有锁及完整进度。
         * <p>
         * 参数说明：无。
         * 返回：证据均成立才成功；失联或证据变化永久保护旧 lease。
         */
        @Override
        public synchronized void verify() {
            verifyUntil(deadline());
        }

        /**
         * 业务作用：复验当前权限，复用推进调用的总预算。
         * <p>
         * 返回：所有权及完整快照相同才成功；任何存储失败关闭推进门禁。
         *
         * @param deadline 本次调用截止点。
         */
        private void verifyUntil(long deadline) {
            if (closed) throw failure(CheckpointException.Code.CLOSED);
            if (fenced) throw failure(CheckpointException.Code.FENCED);
            try {
                validateServer(session, deadline);
                try (PreparedStatement statement = prepare(session, "SELECT CONNECTION_ID(),IS_USED_LOCK(?)", deadline)) {
                    statement.setString(1, lock);
                    try (ResultSet rows = statement.executeQuery()) {
                        if (!rows.next() || rows.getLong(1) != connectionId || rows.getLong(2) != connectionId || rows.wasNull())
                            throw failure(CheckpointException.Code.FENCED);
                    }
                }
                // 外部覆盖、删除或新代次都必须拒绝，单独观察锁不能替代完整持久证据。
                if (!current.equals(read(session, current.taskId(), deadline)))
                    throw failure(CheckpointException.Code.FENCED);
            } catch (SQLException rejected) {
                fenced = true;
                throw failure(CheckpointException.Code.IO);
            } catch (RuntimeException rejected) {
                fenced = true;
                throw rejected;
            }
        }

        /**
         * 业务作用：目标确认后条件推进，SQL 响应未知时保留旧快照并禁止重试旧权限。
         *
         * @param expectedRevision 调用方预期序号
         * @param positions        完整新进度。
         * @return 持久确认后才发布新快照，范围回退或旧序号不写库。
         */
        @Override
        public synchronized ReplayCheckpoint advance(long expectedRevision, List<ReplayCheckpoint.Position> positions) {
            if (closed) throw failure(CheckpointException.Code.CLOSED);
            if (fenced) throw failure(CheckpointException.Code.FENCED);
            if (expectedRevision != current.revision()) throw failure(CheckpointException.Code.STALE_REVISION);
            ReplayCheckpoint next = new ReplayCheckpoint(current.taskId(), current.contractFingerprint(), current.generation(), increment(current.revision()), positions);
            if (!current.sameContract(next)) throw failure(CheckpointException.Code.CONTRACT_MISMATCH);
            for (int i = 0; i < current.positions().size(); i++)
                if (next.positions().get(i).nextOffset() < current.positions().get(i).nextOffset())
                    throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            try {
                long deadline = deadline();
                verifyUntil(deadline);
                persist(session, lock, connectionId, current, next, deadline);
                current = next;
                return next;
            } catch (RuntimeException rejected) {
                fenced = true;
                throw rejected;
            }
        }

        /**
         * 业务作用：先永久关闭推进门禁，再结束专用会话及其命名锁。
         * <p>
         * 参数说明：无。
         * 返回：实际本地连接退出后成功，可重复等待关闭；不撤销外部在途写入。
         */
        @Override
        public synchronized void close() {
            closed = true;
            session.close();
        }
    }
}

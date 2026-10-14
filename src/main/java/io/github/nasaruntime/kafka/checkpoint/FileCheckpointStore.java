package io.github.nasaruntime.kafka.checkpoint;



import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;

/**
 * POSIX 本地目录的单机恢复后端。目录须由组件独占管理，不承诺网络文件系统或跨主机接管。
 * 存储调用可能等待磁盘；回放运行时必须在自己的有界执行资源内调用，不能在 consumer owner 上执行。
 */
public final class FileCheckpointStore implements CheckpointStore {

    private static final Set<Path> OWNED = new HashSet<>();
    private static final int MAX_ACTIVE_TASKS = 1024;
    private final Path root;

    /**
     * 业务作用：绑定支持原子替换和目录持久化的受控本地存储根。
     * <p>
     * 返回：规范化路径的存储；非 POSIX 或非法目录在接纳任务前拒绝。
     *
     * @param root 任务目录，缺失时以仅当前用户可访问的权限创建。
     */
    public FileCheckpointStore(Path root) {
        if (root == null) throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
        try {
            Files.createDirectories(root, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            this.root = root.toRealPath();
            if (!Files.isDirectory(this.root) || !Files.getFileStore(this.root).supportsFileAttributeView("posix"))
                throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
        } catch (IOException | UnsupportedOperationException rejected) {
            throw failure(CheckpointException.Code.IO);
        }
    }

    /**
     * 业务作用：只读加载完整已持久化快照，不打开或关闭任务锁文件。
     *
     * @param taskId 稳定任务名。
     * @return 不存在为空；损坏或身份不一致拒绝，读取不授予推进权限。
     */
    @Override
    public Optional<ReplayCheckpoint> load(String taskId) {
        String key = CheckpointWire.key(taskId);
        Path file = root.resolve(key + ".checkpoint");
        try (InputStream input = Files.newInputStream(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            ReplayCheckpoint checkpoint = CheckpointWire.decode(input.readNBytes(CheckpointWire.MAX_BYTES + 1));
            if (!checkpoint.taskId().equals(taskId) || checkpoint.generation() == 0)
                throw failure(CheckpointException.Code.CORRUPT);
            return Optional.of(checkpoint);
        } catch (NoSuchFileException absent) {
            return Optional.empty();
        } catch (IOException rejected) {
            throw failure(CheckpointException.Code.IO);
        }
    }

    /**
     * 业务作用：独占任务锁并在落盘新代次后交付推进权。
     *
     * @param initial 尚未推进的冻结范围。
     * @return 单一活动 lease；同进程重复登记先于打开文件拒绝，进程崩溃后的新 claim 从原进度恢复。
     */
    @Override
    public CheckpointLease claim(ReplayCheckpoint initial) {
        if (initial == null || initial.generation() != 0 || initial.revision() != 0
                || initial.positions().stream().anyMatch(position -> position.nextOffset() != position.startOffset()))
            throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
        Path lockPath = root.resolve(CheckpointWire.key(initial.taskId()) + ".lock");
        synchronized (OWNED) {
            // POSIX 锁属于进程，重复打开再关闭同一 inode 会影响已有锁；必须在打开 descriptor 前拒绝重复任务。
            if (OWNED.contains(lockPath)) throw failure(CheckpointException.Code.BUSY);
            if (OWNED.size() >= MAX_ACTIVE_TASKS) throw failure(CheckpointException.Code.CAPACITY);
            OWNED.add(lockPath);
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(lockPath, Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            FileLock lock = channel.tryLock();
            if (lock == null) throw failure(CheckpointException.Code.BUSY);
            ReplayCheckpoint previous = load(initial.taskId()).orElse(null);
            if (previous != null && !initial.sameContract(previous))
                throw failure(CheckpointException.Code.CONTRACT_MISMATCH);
            ReplayCheckpoint next = previous == null ? new ReplayCheckpoint(initial.taskId(), initial.contractFingerprint(), 1, 0, initial.positions())
                    : new ReplayCheckpoint(previous.taskId(), previous.contractFingerprint(), increment(previous.generation()), increment(previous.revision()), previous.positions());
            // 新代次先持久化，再开放 lease；调用方不会拿到尚未成立的推进权。
            persist(next);
            return new Lease(lockPath, channel, lock, next);
        } catch (IOException | OverlappingFileLockException rejected) {
            releaseFailedClaim(lockPath, channel);
            throw failure(rejected instanceof OverlappingFileLockException ? CheckpointException.Code.BUSY : CheckpointException.Code.IO);
        } catch (RuntimeException rejected) {
            releaseFailedClaim(lockPath, channel);
            throw rejected;
        }
    }

    /**
     * 业务作用：在完整写入和目录同步后确认进度持久化。
     * <p>
     * 返回：成功才允许公开进度；任何写入失败均按未知处理，禁止基于旧内存状态继续推进。
     *
     * @param checkpoint 下一完整快照。
     */
    private void persist(ReplayCheckpoint checkpoint) {
        String key = CheckpointWire.key(checkpoint.taskId());
        Path target = root.resolve(key + ".checkpoint"), temporary = root.resolve(key + ".pending-" + UUID.randomUUID());
        byte[] bytes = CheckpointWire.encode(checkpoint);
        boolean created = false;
        try {
            try (FileChannel output = FileChannel.open(temporary, Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))) {
                created = true;
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) output.write(buffer);
                output.force(true);
            }
            // 先同步正文，再原子替换并同步目录；不允许以普通复制降级原子发布合同。
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel directory = FileChannel.open(root, StandardOpenOption.READ)) {
                directory.force(true);
            }
        } catch (IOException rejected) {
            throw failure(CheckpointException.Code.OUTCOME_UNKNOWN);
        } finally {
            if (created) try {
                Files.deleteIfExists(temporary);
            } catch (IOException retained) { /* 未被引用的暂存文件不能成为恢复依据。 */ }
        }
    }

    /**
     * 业务作用：在失败 claim 后关闭 descriptor，再解除同进程登记。
     * <p>
     * 返回：关闭失败且 descriptor 仍开放时保留保护登记。
     *
     * @param path    本次登记的精确锁路径
     * @param channel 本次打开的可空 descriptor。
     */
    private static void releaseFailedClaim(Path path, FileChannel channel) {
        if (channel != null) try {
            channel.close();
        } catch (IOException retained) {
        }
        if (channel == null || !channel.isOpen()) synchronized (OWNED) {
            OWNED.remove(path);
        }
    }

    /**
     * 业务作用：避免代次或条件更新序号溢出后复用旧权威。
     *
     * @param value 当前非负序号。
     * @return 下一序号；耗尽时拒绝继续写入。
     */
    private static long increment(long value) {
        if (value == Long.MAX_VALUE) throw failure(CheckpointException.Code.CAPACITY);
        return value + 1;
    }

    /**
     * 业务作用：统一不带存储路径的领域失败。
     *
     * @param code 稳定分类。
     * @return 脱敏异常。
     */
    private static CheckpointException failure(CheckpointException.Code code) {
        return new CheckpointException(code);
    }

    /**
     * 所有权覆盖实际任务寿命，同一 lease 的推进和关闭在监视器内串行。
     */
    private final class Lease implements CheckpointLease {
        private final Path lockPath;
        private final FileChannel channel;
        private final FileLock lock;
        private ReplayCheckpoint current;
        private boolean closed;
        private boolean fenced;

        /**
         * 业务作用：接纳已经落盘的独占代次。
         * <p>
         * 返回：尚未归还责任的 lease，不执行额外存储写入。
         *
         * @param lockPath 该任务独占锁文件的完整路径。
         * @param channel  实际持有锁文件的打开通道，释放lease时必须关闭。
         * @param lock     操作系统授予的文件锁，决定当前lease的实际独占责任。
         * @param current  已确认快照。
         */
        Lease(Path lockPath, FileChannel channel, FileLock lock, ReplayCheckpoint current) {
            this.lockPath = lockPath;
            this.channel = channel;
            this.lock = lock;
            this.current = current;
        }

        /**
         * 业务作用：读取本 lease 最后确认的完整快照。
         * <p>
         * 参数说明：无。
         *
         * @return 不可变历史事实，不重新授予关闭后的权威。
         */
        @Override
        public synchronized ReplayCheckpoint checkpoint() {
            return current;
        }

        /**
         * 业务作用：在新业务调用前检查本地锁和完整磁盘证据，失权后永久禁止旧 lease 推进。
         * <p>
         * 参数说明：无。
         * 返回：锁仍有效且磁盘快照完全相同时成功；读取失败或变化会进入保护态，不能据旧内存继续写入。
         */
        @Override
        public synchronized void verify() {
            if (closed) throw failure(CheckpointException.Code.CLOSED);
            if (fenced) throw failure(CheckpointException.Code.FENCED);
            try {
                // 独占目录的历史内存快照不能代替当前证据，外部改写必须在下一次业务调用前拒绝。
                if (!lock.isValid() || !load(current.taskId()).orElseThrow(() -> failure(CheckpointException.Code.FENCED)).equals(current))
                    throw failure(CheckpointException.Code.FENCED);
            } catch (RuntimeException rejected) {
                fenced = true;
                throw rejected;
            }
        }

        /**
         * 业务作用：复验所有权和磁盘证据后条件推进，禁止未落盘进度提前可见。
         *
         * @param expectedRevision 预期序号
         * @param positions        完整新进度。
         * @return 同步持久化后的快照；存储未知后永久保护本 lease。
         */
        @Override
        public synchronized ReplayCheckpoint advance(long expectedRevision, List<ReplayCheckpoint.Position> positions) {
            if (closed) throw failure(CheckpointException.Code.CLOSED);
            if (fenced || !lock.isValid()) throw failure(CheckpointException.Code.FENCED);
            if (expectedRevision != current.revision()) throw failure(CheckpointException.Code.STALE_REVISION);
            ReplayCheckpoint next = new ReplayCheckpoint(current.taskId(), current.contractFingerprint(), current.generation(), increment(current.revision()), positions);
            if (!current.sameContract(next)) throw failure(CheckpointException.Code.CONTRACT_MISMATCH);
            for (int i = 0; i < current.positions().size(); i++)
                if (next.positions().get(i).nextOffset() < current.positions().get(i).nextOffset())
                    throw failure(CheckpointException.Code.INVALID_CONFIGURATION);
            try {
                // 业务调用期间也可能失去权威，推进时再次复验，不能将调用前检查当成持久写权限。
                verify();
                persist(next);
                current = next;
                return next;
            } catch (CheckpointException rejected) {
                fenced = true;
                throw rejected;
            }
        }

        /**
         * 业务作用：先关闭推进，再释放锁及 descriptor，最后允许同进程新 claim。
         * <p>
         * 参数说明：无。
         * 返回：实际资源关闭后解除登记；失败时保留必要保护并允许再次关闭。
         */
        @Override
        public synchronized void close() {
            if (closed && !channel.isOpen()) return;
            closed = true;
            try {
                if (lock.isValid()) lock.release();
                channel.close();
            } catch (IOException rejected) {
                throw failure(CheckpointException.Code.IO);
            } finally {
                if (!channel.isOpen()) synchronized (OWNED) {
                    OWNED.remove(lockPath);
                }
            }
        }
    }
}

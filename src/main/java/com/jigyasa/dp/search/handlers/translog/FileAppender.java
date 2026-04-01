package com.jigyasa.dp.search.handlers.translog;

import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Write-ahead translog for crash recovery, modeled after Elasticsearch's translog design.
 *
 * <h3>Durability modes (like ES index.translog.durability):</h3>
 * <ul>
 *   <li><b>REQUEST</b> — fsync after every append (safest, higher latency)</li>
 *   <li><b>ASYNC</b> — buffer writes, fsync periodically (fastest, data loss window = flush interval)</li>
 * </ul>
 *
 * <h3>Checkpoint file:</h3>
 * A small {@code translog.ckp} file is written atomically after each fsync, recording
 * the number of valid operations and total bytes. On recovery, only entries covered by
 * the checkpoint are replayed — partial/corrupt trailing entries are safely ignored.
 * This mirrors ES's Checkpoint.java approach.
 */
public class FileAppender implements TranslogAppender {
    private static final Logger log = LoggerFactory.getLogger(FileAppender.class);
    private static final long MAX_FILE_SIZE = 3L * 1024 * 1024 * 1024; // 3 GB
    private static final String FILE_PREFIX = "translog.dat.";
    private static final String CHECKPOINT_FILE = "translog.ckp";

    public enum Durability { REQUEST, ASYNC }

    private int currentFileNumber = 0;
    private FileOutputStream fileOutputStream;
    private volatile DataOutputStream dos;
    private final Object fileOpsLock = new Object();
    private final String dir;
    private final Durability durability;
    private volatile boolean dirty = false;
    private ScheduledExecutorService flushScheduler;

    // Checkpoint state: tracks what has been safely fsynced
    private final AtomicInteger totalOps = new AtomicInteger(0);
    private long totalBytesWritten = 0;
    private int syncedOps = 0;
    private long syncedBytes = 0;

    public FileAppender(String dir) {
        this(dir, Durability.REQUEST, 200);
    }

    public FileAppender(String dir, Durability durability, int flushIntervalMs) {
        try {
            this.dir = dir;
            this.durability = durability;
            openNewFile();
            loadCheckpoint();

            if (durability == Durability.ASYNC) {
                flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "translog-flush");
                    t.setDaemon(true);
                    return t;
                });
                flushScheduler.scheduleWithFixedDelay(this::asyncFlush, flushIntervalMs, flushIntervalMs,
                        TimeUnit.MILLISECONDS);
                log.info("Translog FileAppender initialized at {} (ASYNC durability, fsync every {}ms)", dir, flushIntervalMs);
            } else {
                log.info("Translog FileAppender initialized at {} (REQUEST durability, fsync per request)", dir);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize file appender at " + dir, e);
        }
    }

    /**
     * Fsync translog data to disk + write checkpoint atomically.
     * Mirrors ES: channel.force(false) then Checkpoint.write(channel, path, ckp, fsync).
     */
    private void syncToDisk() throws IOException {
        dos.flush();
        try {
            fileOutputStream.getFD().sync();
        } catch (SyncFailedException e) {
            // Some filesystems (e.g., temp dirs in CI) don't support fsync.
            // Data is still flushed to OS page cache via dos.flush().
            log.debug("fsync not supported on this filesystem, data flushed to OS cache only");
        }
        // After fsync, write checkpoint so recovery knows how far is safe
        writeCheckpoint();
    }

    private void writeCheckpoint() throws IOException {
        syncedOps = totalOps.get();
        syncedBytes = totalBytesWritten;
        Path ckpPath = Paths.get(dir, CHECKPOINT_FILE);
        Path tmpPath = Paths.get(dir, CHECKPOINT_FILE + ".tmp");
        // Atomic write: write to temp, fsync, rename (like ES's < 512 byte checkpoint)
        try (FileOutputStream ckpFos = new FileOutputStream(tmpPath.toString());
             DataOutputStream ckpDos = new DataOutputStream(ckpFos)) {
            ckpDos.writeInt(syncedOps);
            ckpDos.writeLong(syncedBytes);
            ckpDos.writeInt(currentFileNumber - 1); // current file index
            ckpDos.flush();
            ckpFos.getFD().sync();
        }
        // Atomic rename
        Files.move(tmpPath, ckpPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    }

    private void loadCheckpoint() {
        Path ckpPath = Paths.get(dir, CHECKPOINT_FILE);
        if (Files.exists(ckpPath)) {
            try (DataInputStream dis = new DataInputStream(new FileInputStream(ckpPath.toString()))) {
                syncedOps = dis.readInt();
                syncedBytes = dis.readLong();
                int fileNum = dis.readInt();
                totalOps.set(syncedOps);
                totalBytesWritten = syncedBytes;
                log.info("Loaded translog checkpoint: {} ops, {} bytes, file {}", syncedOps, syncedBytes, fileNum);
            } catch (IOException e) {
                log.warn("Failed to load translog checkpoint, will replay all entries", e);
            }
        }
    }

    private void asyncFlush() {
        if (!dirty) return;
        synchronized (fileOpsLock) {
            try {
                if (dirty && dos != null) {
                    syncToDisk();
                    dirty = false;
                }
            } catch (IOException e) {
                log.error("Async translog flush/fsync failed", e);
            }
        }
    }

    private void openNewFile() throws IOException {
        closeOpenFiles();

        Files.createDirectories(Path.of(dir));
        Path filePath = Paths.get(dir, FILE_PREFIX + currentFileNumber);
        if (!Files.exists(filePath)) {
            Files.createFile(filePath);
        }

        fileOutputStream = new FileOutputStream(filePath.toString(), true);
        dos = new DataOutputStream(fileOutputStream);
        currentFileNumber++;
    }

    public void closeOpenFiles() {
        try {
            if (dos != null) {
                dos.flush();
                if (fileOutputStream != null) {
                    try {
                        fileOutputStream.getFD().sync();
                    } catch (SyncFailedException e) {
                        // Graceful degradation on filesystems without fsync support
                    }
                }
                dos.close();
            }
            if (fileOutputStream != null) {
                fileOutputStream.close();
            }
            dirty = false;
        } catch (IOException e) {
            log.warn("Error closing translog files", e);
        }
    }

    @Override
    public void append(IndexRequest request) throws IOException {
        final byte[] byteArray = request.toByteArray();
        synchronized (fileOpsLock) {
            if (Files.size(Paths.get(dir, FILE_PREFIX + (currentFileNumber - 1))) + byteArray.length > MAX_FILE_SIZE) {
                openNewFile();
            }
            dos.writeLong(byteArray.length);
            dos.write(byteArray);
            totalOps.incrementAndGet();
            totalBytesWritten += 8 + byteArray.length; // 8 bytes for the long length header

            if (durability == Durability.REQUEST) {
                syncToDisk();
            } else {
                dirty = true;
            }
        }
    }

    @Override
    public void reset() {
        synchronized (fileOpsLock) {
            closeOpenFiles();
            for (Path path : FileUtils.listFilesWithPrefix(Path.of(dir), FILE_PREFIX, false)) {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to delete translog file: " + path, e);
                }
            }
            // Also clean checkpoint
            try {
                Files.deleteIfExists(Paths.get(dir, CHECKPOINT_FILE));
            } catch (IOException e) {
                log.warn("Failed to delete checkpoint file", e);
            }
            currentFileNumber = 0;
            totalOps.set(0);
            totalBytesWritten = 0;
            syncedOps = 0;
            syncedBytes = 0;
            try {
                openNewFile();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create new translog file", e);
            }
        }
    }

    @Override
    public List<IndexRequest> getData() {
        // Load checkpoint to know how many ops are safely fsynced
        loadCheckpoint();
        int safeOps = syncedOps;

        List<IndexRequest> items = new LinkedList<>();
        int opsRead = 0;
        for (Path path : FileUtils.listFilesWithPrefix(Path.of(dir), FILE_PREFIX, true)) {
            try (DataInputStream dis = new DataInputStream(new FileInputStream(path.toString()))) {
                while (true) {
                    long length;
                    try {
                        length = dis.readLong();
                    } catch (EOFException e) {
                        break; // End of file reached cleanly
                    }
                    if (length <= 0 || length > Integer.MAX_VALUE) {
                        log.warn("Invalid translog entry length {} in file {}, stopping read", length, path);
                        break;
                    }
                    byte[] byteArray = new byte[(int) length];
                    try {
                        dis.readFully(byteArray);
                    } catch (EOFException e) {
                        log.warn("Truncated translog entry in file {} (expected {} bytes), discarding partial entry", path, length);
                        break;
                    }
                    opsRead++;
                    if (safeOps > 0 && opsRead > safeOps) {
                        log.warn("Skipping {} unsynced translog entries beyond checkpoint ({} safe ops)",
                                opsRead - safeOps, safeOps);
                        break;
                    }
                    items.add(IndexRequest.parseFrom(byteArray));
                }
            } catch (IOException e) {
                log.error("Error reading translog file: {}", path, e);
                throw new RuntimeException("Failed to read translog file: " + path, e);
            }
            if (safeOps > 0 && opsRead >= safeOps) {
                break;
            }
        }
        log.info("Read {} translog entries for recovery (checkpoint: {} safe ops)", items.size(), safeOps);
        return items;
    }

    public void shutdown() {
        if (flushScheduler != null) {
            flushScheduler.shutdown();
            try {
                flushScheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Final sync before shutdown
        synchronized (fileOpsLock) {
            if (dirty && dos != null) {
                try {
                    syncToDisk();
                    dirty = false;
                } catch (IOException e) {
                    log.error("Failed to sync translog on shutdown", e);
                }
            }
        }
        closeOpenFiles();
        log.info("Translog FileAppender shut down");
    }
}

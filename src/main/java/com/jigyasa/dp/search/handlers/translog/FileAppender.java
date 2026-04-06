package com.jigyasa.dp.search.handlers.translog;

import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
 * <p>Checkpoint persistence is delegated to {@link TranslogCheckpointManager}.
 * Recovery reads are delegated to {@link TranslogReader}.</p>
 */
public class FileAppender implements TranslogAppender {
    private static final Logger log = LoggerFactory.getLogger(FileAppender.class);
    private static final long MAX_FILE_SIZE = 3L * 1024 * 1024 * 1024; // 3 GB
    static final String FILE_PREFIX = "translog.dat.";

    public enum Durability { REQUEST, ASYNC }

    private int currentFileNumber = 0;
    private FileOutputStream fileOutputStream;
    private volatile DataOutputStream dos;
    private final Object fileOpsLock = new Object();
    private final String dir;
    private final Durability durability;
    private volatile boolean dirty = false;
    private ScheduledExecutorService flushScheduler;

    private final AtomicInteger totalOps = new AtomicInteger(0);
    private long totalBytesWritten = 0;

    private final TranslogCheckpointManager checkpointManager;
    private final TranslogReader translogReader;

    public FileAppender(String dir) {
        this(dir, Durability.REQUEST, 200);
    }

    public FileAppender(String dir, Durability durability, int flushIntervalMs) {
        try {
            this.dir = dir;
            this.durability = durability;
            this.checkpointManager = new TranslogCheckpointManager(dir);
            this.translogReader = new TranslogReader(dir, FILE_PREFIX, checkpointManager);
            // Load checkpoint BEFORE opening file — checkpoint tells us which file to continue from.
            // Without this, restart always opens file 0, corrupting append order after rollover.
            Files.createDirectories(Path.of(dir));
            loadCheckpoint();
            openNewFile();

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
            // Clean up any resources opened before the failure (e.g., file streams from openNewFile)
            closeOpenFiles();
            if (flushScheduler != null) {
                flushScheduler.shutdownNow();
            }
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
        checkpointManager.writeCheckpoint(totalOps.get(), totalBytesWritten, currentFileNumber - 1);
    }

    private void loadCheckpoint() {
        int fileNum = checkpointManager.loadCheckpoint();
        if (fileNum >= 0) {
            totalOps.set(checkpointManager.getSyncedOps());
            totalBytesWritten = checkpointManager.getSyncedBytes();
            // Resume from the correct file number after rollover.
            currentFileNumber = fileNum + 1;
            log.info("Resuming from file {}", currentFileNumber);
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
        totalBytesWritten = 0;
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
            if (totalBytesWritten + byteArray.length > MAX_FILE_SIZE) {
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
            checkpointManager.deleteCheckpoint();
            currentFileNumber = 0;
            totalOps.set(0);
            totalBytesWritten = 0;
            checkpointManager.reset();
            try {
                openNewFile();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create new translog file", e);
            }
        }
    }

    @Override
    public List<IndexRequest> getData() {
        return translogReader.getData();
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

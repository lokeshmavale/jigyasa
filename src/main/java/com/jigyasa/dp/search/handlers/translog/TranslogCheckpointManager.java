package com.jigyasa.dp.search.handlers.translog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * Manages translog checkpoint persistence: write and load checkpoint files atomically.
 * The checkpoint records the number of safely fsynced operations and bytes,
 * plus the current file number, enabling correct recovery and file resumption.
 */
public class TranslogCheckpointManager {
    private static final Logger log = LoggerFactory.getLogger(TranslogCheckpointManager.class);
    static final String CHECKPOINT_FILE = "translog.ckp";

    private final String dir;
    private int syncedOps = 0;
    private long syncedBytes = 0;

    public TranslogCheckpointManager(String dir) {
        this.dir = dir;
    }

    public int getSyncedOps() { return syncedOps; }
    public long getSyncedBytes() { return syncedBytes; }

    /**
     * Atomically writes a checkpoint file recording the current safe state.
     * Uses temp-file + fsync + atomic rename (like ES's Checkpoint.write).
     */
    public void writeCheckpoint(int totalOps, long totalBytesWritten, int currentFileNumber) throws IOException {
        syncedOps = totalOps;
        syncedBytes = totalBytesWritten;
        Path ckpPath = Paths.get(dir, CHECKPOINT_FILE);
        Path tmpPath = Paths.get(dir, CHECKPOINT_FILE + ".tmp");
        try (FileOutputStream ckpFos = new FileOutputStream(tmpPath.toString());
             DataOutputStream ckpDos = new DataOutputStream(ckpFos)) {
            ckpDos.writeInt(syncedOps);
            ckpDos.writeLong(syncedBytes);
            ckpDos.writeInt(currentFileNumber);
            ckpDos.flush();
            ckpFos.getFD().sync();
        }
        Files.move(tmpPath, ckpPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Loads checkpoint from disk. Returns the file number to resume from,
     * or -1 if no checkpoint exists. Updates internal syncedOps/syncedBytes.
     */
    public int loadCheckpoint() {
        Path ckpPath = Paths.get(dir, CHECKPOINT_FILE);
        if (Files.exists(ckpPath)) {
            try (DataInputStream dis = new DataInputStream(new FileInputStream(ckpPath.toString()))) {
                syncedOps = dis.readInt();
                syncedBytes = dis.readLong();
                int fileNum = dis.readInt();
                log.info("Loaded translog checkpoint: {} ops, {} bytes, file {}", syncedOps, syncedBytes, fileNum);
                return fileNum;
            } catch (IOException e) {
                log.warn("Failed to load translog checkpoint, will replay all entries", e);
            }
        }
        return -1;
    }

    /** Deletes the checkpoint file. */
    public void deleteCheckpoint() {
        try {
            Files.deleteIfExists(Paths.get(dir, CHECKPOINT_FILE));
        } catch (IOException e) {
            log.warn("Failed to delete checkpoint file", e);
        }
    }

    /** Resets internal state to zero. */
    public void reset() {
        syncedOps = 0;
        syncedBytes = 0;
    }
}

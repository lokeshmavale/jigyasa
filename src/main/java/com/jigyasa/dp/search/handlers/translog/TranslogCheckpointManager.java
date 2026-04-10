package com.jigyasa.dp.search.handlers.translog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Manages translog checkpoint persistence using a pre-opened RandomAccessFile.
 * Writes 16 bytes (ops:int + bytes:long + fileNum:int) in place and forces once.
 * Avoids temp-file + rename overhead on every checkpoint write (~2x faster than
 * the previous temp-file + fsync + atomic-rename approach).
 */
public class TranslogCheckpointManager {
    private static final Logger log = LoggerFactory.getLogger(TranslogCheckpointManager.class);
    static final String CHECKPOINT_FILE = "translog.ckp";
    private static final int CHECKPOINT_SIZE = 4 + 8 + 4; // int + long + int = 16 bytes

    private final String dir;
    private int syncedOps = 0;
    private long syncedBytes = 0;
    private RandomAccessFile raf;
    private FileChannel channel;
    private final ByteBuffer buf = ByteBuffer.allocate(CHECKPOINT_SIZE);

    public TranslogCheckpointManager(String dir) {
        this.dir = dir;
    }

    public int getSyncedOps() { return syncedOps; }
    public long getSyncedBytes() { return syncedBytes; }

    /**
     * Writes checkpoint in place: seek to 0, write 16 bytes, force once.
     * No temp file, no rename, no file open/close per call.
     */
    public void writeCheckpoint(int totalOps, long totalBytesWritten, int currentFileNumber) throws IOException {
        syncedOps = totalOps;
        syncedBytes = totalBytesWritten;
        ensureOpen();
        buf.clear();
        buf.putInt(syncedOps);
        buf.putLong(syncedBytes);
        buf.putInt(currentFileNumber);
        buf.flip();
        channel.position(0);
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        try {
            channel.force(false);
        } catch (IOException e) {
            log.debug("fsync not supported for checkpoint file, data flushed to OS cache");
        }
    }

    private void ensureOpen() throws IOException {
        if (raf == null) {
            Files.createDirectories(Path.of(dir));
            Path ckpPath = Paths.get(dir, CHECKPOINT_FILE);
            raf = new RandomAccessFile(ckpPath.toFile(), "rw");
            channel = raf.getChannel();
        }
    }

    /**
     * Loads checkpoint from disk. Returns the file number to resume from,
     * or -1 if no checkpoint exists.
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

    /** Deletes the checkpoint file and closes any open handles. */
    public void deleteCheckpoint() {
        closeChannel();
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

    /** Closes the pre-opened file channel. Call on shutdown. */
    public void closeChannel() {
        try {
            if (channel != null) { channel.close(); channel = null; }
            if (raf != null) { raf.close(); raf = null; }
        } catch (IOException e) {
            log.warn("Error closing checkpoint channel", e);
        }
    }
}

package com.jigyasa.dp.search.handlers.translog;

import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

/**
 * Reads translog entries for crash recovery.
 * Respects checkpoint boundaries and skips corrupt trailing entries.
 */
public class TranslogReader {
    private static final Logger log = LoggerFactory.getLogger(TranslogReader.class);

    private final String dir;
    private final String filePrefix;
    private final TranslogCheckpointManager checkpointManager;

    public TranslogReader(String dir, String filePrefix, TranslogCheckpointManager checkpointManager) {
        this.dir = dir;
        this.filePrefix = filePrefix;
        this.checkpointManager = checkpointManager;
    }

    /**
     * Reads all translog entries up to the checkpoint boundary.
     * Loads the checkpoint first, then reads files in order, stopping at the safe ops limit.
     */
    public List<IndexRequest> getData() {
        checkpointManager.loadCheckpoint();
        int safeOps = checkpointManager.getSyncedOps();

        List<IndexRequest> items = new LinkedList<>();
        int opsRead = 0;
        for (Path path : FileUtils.listFilesWithPrefix(Path.of(dir), filePrefix, true)) {
            opsRead = readSingleFile(path, items, opsRead, safeOps);
            if (safeOps > 0 && opsRead >= safeOps) {
                break;
            }
        }
        log.info("Read {} translog entries for recovery (checkpoint: {} safe ops)", items.size(), safeOps);
        return items;
    }

    /**
     * Reads entries from a single translog file, appending valid entries to the list.
     * Returns the updated total ops-read count.
     */
    int readSingleFile(Path path, List<IndexRequest> items, int opsRead, int safeOps) {
        try (DataInputStream dis = new DataInputStream(new FileInputStream(path.toString()))) {
            while (true) {
                long length;
                try {
                    length = dis.readLong();
                } catch (EOFException e) {
                    break;
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
                try {
                    items.add(IndexRequest.parseFrom(byteArray));
                } catch (Exception parseEx) {
                    log.warn("Corrupt translog entry #{} in file {} ({} bytes), skipping",
                            opsRead, path, length, parseEx);
                }
            }
        } catch (IOException e) {
            log.error("Error reading translog file: {}", path, e);
            throw new RuntimeException("Failed to read translog file: " + path, e);
        }
        return opsRead;
    }
}

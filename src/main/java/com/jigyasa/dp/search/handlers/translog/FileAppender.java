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

public class FileAppender implements TranslogAppender {
    private static final Logger log = LoggerFactory.getLogger(FileAppender.class);
    private static final long MAX_FILE_SIZE = 3L * 1024 * 1024 * 1024; // 3 GB
    private static final String FILE_PREFIX = "translog.dat.";
    private int currentFileNumber = 0;
    private FileOutputStream fileOutputStream;
    private volatile DataOutputStream dos;
    private final Object fileOpsLock = new Object();
    private final String dir;

    public FileAppender(String dir) {
        try {
            this.dir = dir;
            openNewFile();
            log.info("Translog FileAppender initialized at {}", dir);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize file appender at " + dir, e);
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
                dos.close();
            }
            if (fileOutputStream != null) {
                fileOutputStream.close();
            }
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
            dos.flush();
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
            currentFileNumber = 0;
            try {
                openNewFile();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create new translog file", e);
            }
        }
    }

    @Override
    public List<IndexRequest> getData() {
        List<IndexRequest> items = new LinkedList<>();
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
                    items.add(IndexRequest.parseFrom(byteArray));
                }
            } catch (IOException e) {
                log.error("Error reading translog file: {}", path, e);
                throw new RuntimeException("Failed to read translog file: " + path, e);
            }
        }
        log.info("Read {} translog entries for recovery", items.size());
        return items;
    }
}

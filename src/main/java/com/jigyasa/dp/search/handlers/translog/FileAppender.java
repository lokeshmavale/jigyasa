package com.jigyasa.dp.search.handlers.translog;

import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.utils.FileUtils;
import lombok.SneakyThrows;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class FileAppender implements TranslogAppender {
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
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize file appender", e);
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

    @SneakyThrows
    public void closeOpenFiles() {
        if (dos != null) {
            dos.close();
        }

        if (fileOutputStream != null) {
            fileOutputStream.close();
        }
    }

    @Override
    public void append(IndexRequest request) throws IOException {
        final byte[] byteArray = request.toByteArray();
        synchronized (fileOpsLock) {
            if (Files.size(Paths.get(dir,FILE_PREFIX + (currentFileNumber - 1))) + byteArray.length > MAX_FILE_SIZE) {
                openNewFile();
            }
            dos.writeLong(byteArray.length);
            dos.write(byteArray);
            dos.flush();
        }
    }

    @Override
    public void reset() {
        //Delete translog files and reset counter
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
        //While executing recovery, we should avoid putting data in translog
        List<IndexRequest> items = new LinkedList<>();
        for (Path path : FileUtils.listFilesWithPrefix(Path.of(dir), FILE_PREFIX, true)) {
            try (DataInputStream dis = new DataInputStream(new FileInputStream(path.toString()))) {
                while (dis.available() > 0) {
                    int length = dis.readInt();
                    if(length>0) {
                        byte[] byteArray = new byte[length];
                        dis.readFully(byteArray);
                        items.add(IndexRequest.parseFrom(byteArray));
                    }

                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return items;
    }
}

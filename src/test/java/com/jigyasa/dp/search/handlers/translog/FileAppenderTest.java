package com.jigyasa.dp.search.handlers.translog;

import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileAppenderTest {

    @TempDir
    Path tempDir;

    private FileAppender appender;

    @AfterEach
    void tearDown() {
        appender.closeOpenFiles();
    }

    @BeforeEach
    void setUp() {
        appender = new FileAppender(tempDir.toString());
    }

    @Test
    @DisplayName("Append and read back single entry")
    void appendAndReadSingle() throws Exception {
        IndexRequest req = buildRequest("doc1", "hello world");
        appender.append(req);

        List<IndexRequest> recovered = appender.getData();
        assertThat(recovered).hasSize(1);
        assertThat(recovered.get(0).getItem(0).getDocument()).isEqualTo("hello world");
    }

    @Test
    @DisplayName("Append multiple entries and read all back")
    void appendMultiple() throws Exception {
        for (int i = 0; i < 100; i++) {
            appender.append(buildRequest("doc" + i, "content " + i));
        }

        List<IndexRequest> recovered = appender.getData();
        assertThat(recovered).hasSize(100);
        assertThat(recovered.get(0).getItem(0).getDocument()).isEqualTo("content 0");
        assertThat(recovered.get(99).getItem(0).getDocument()).isEqualTo("content 99");
    }

    @Test
    @DisplayName("Empty translog returns empty list")
    void emptyTranslog() {
        List<IndexRequest> recovered = appender.getData();
        assertThat(recovered).isEmpty();
    }

    @Test
    @DisplayName("Survives truncated last entry (partial write crash simulation)")
    void truncatedLastEntry() throws Exception {
        appender.append(buildRequest("doc1", "valid entry"));
        // Close the appender to release file handles before corruption
        appender.closeOpenFiles();

        Path[] files = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().startsWith("translog.dat"))
                .toArray(Path[]::new);

        // Append a length header but no payload (simulates crash mid-write)
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(files[0].toFile(), true))) {
            dos.writeLong(9999L);
        }

        // Recovery should return the 1 valid entry and silently discard the truncated one
        FileAppender recoveryAppender = new FileAppender(tempDir.toString());
        try {
            List<IndexRequest> recovered = recoveryAppender.getData();
            assertThat(recovered).hasSize(1);
            assertThat(recovered.get(0).getItem(0).getDocument()).isEqualTo("valid entry");
        } finally {
            recoveryAppender.closeOpenFiles();
        }
    }

    @Test
    @DisplayName("Invalid length (negative) stops reading gracefully")
    void invalidLengthStopsRead() throws Exception {
        appender.append(buildRequest("doc1", "valid"));
        appender.closeOpenFiles();

        Path[] files = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().startsWith("translog.dat"))
                .toArray(Path[]::new);

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(files[0].toFile(), true))) {
            dos.writeLong(-1L);
        }

        FileAppender recoveryAppender = new FileAppender(tempDir.toString());
        try {
            List<IndexRequest> recovered = recoveryAppender.getData();
            assertThat(recovered).hasSize(1);
        } finally {
            recoveryAppender.closeOpenFiles();
        }
    }

    @Test
    @DisplayName("Reset deletes translog files and starts fresh")
    void resetDeletesAndRestarts() throws Exception {
        appender.append(buildRequest("doc1", "data"));
        appender.reset();

        // After reset, data should be gone
        List<IndexRequest> recovered = appender.getData();
        assertThat(recovered).isEmpty();
    }

    private IndexRequest buildRequest(String id, String content) {
        return IndexRequest.newBuilder()
                .addItem(IndexItem.newBuilder()
                        .setAction(IndexAction.UPDATE)
                        .setDocument(content)
                        .build())
                .build();
    }
}

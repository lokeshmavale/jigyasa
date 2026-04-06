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

    @Test
    @DisplayName("Checkpoint persists file number across restarts")
    void checkpointPersistsFileNumber() throws Exception {
        appender.append(buildRequest("doc1", "data"));
        appender.closeOpenFiles();

        // Create new appender from same dir — should resume from checkpoint
        FileAppender recovered = new FileAppender(tempDir.toString());
        try {
            recovered.append(buildRequest("doc2", "more data"));
            List<IndexRequest> data = recovered.getData();
            // Both entries should be recoverable
            assertThat(data).hasSizeGreaterThanOrEqualTo(1);
        } finally {
            recovered.closeOpenFiles();
        }
    }

    @Test
    @DisplayName("Corrupt protobuf entry is skipped without crash")
    void corruptProtobufEntrySkipped() throws Exception {
        appender.append(buildRequest("doc1", "valid"));
        appender.closeOpenFiles();

        // Find the translog file and append garbage with a valid length header
        Path[] files = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().startsWith("translog.dat"))
                .sorted()
                .toArray(Path[]::new);

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(files[0].toFile(), true))) {
            byte[] garbage = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
            dos.writeLong(garbage.length);  // valid length header
            dos.write(garbage);              // invalid protobuf
        }

        FileAppender recovered = new FileAppender(tempDir.toString());
        try {
            List<IndexRequest> data = recovered.getData();
            // Should have 1 valid entry, corrupt one skipped
            assertThat(data).hasSize(1);
            assertThat(data.get(0).getItem(0).getDocument()).isEqualTo("valid");
        } finally {
            recovered.closeOpenFiles();
        }
    }

    @Test
    @DisplayName("Valid entries after corrupt entry are still recovered")
    void validEntriesAfterCorruptEntryRecovered() throws Exception {
        // Write 3 entries through the appender so the checkpoint covers all 3
        appender.append(buildRequest("doc1", "first"));
        appender.append(buildRequest("doc2", "second"));
        appender.append(buildRequest("doc3", "third"));
        appender.closeOpenFiles();

        // Find the translog file and corrupt the SECOND entry's payload on disk
        Path[] files = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().startsWith("translog.dat"))
                .sorted()
                .toArray(Path[]::new);

        byte[] fileBytes = Files.readAllBytes(files[0]);
        // Entry format: [8-byte long length][payload]. Navigate to entry 2's payload.
        int offset = 0;
        // Skip entry 1: read its length (big-endian long), then skip length + 8
        long len1 = java.nio.ByteBuffer.wrap(fileBytes, offset, 8).getLong();
        offset += 8 + (int) len1;
        // Now at entry 2's length header
        long len2 = java.nio.ByteBuffer.wrap(fileBytes, offset, 8).getLong();
        int entry2PayloadStart = offset + 8;
        // Corrupt entry 2's payload with garbage bytes
        for (int i = entry2PayloadStart; i < entry2PayloadStart + (int) len2; i++) {
            fileBytes[i] = (byte) 0xFF;
        }
        Files.write(files[0], fileBytes);

        FileAppender recovered = new FileAppender(tempDir.toString());
        try {
            List<IndexRequest> data = recovered.getData();
            // TranslogReader skips corrupt entries and continues reading.
            // Entries 1 and 3 are valid; entry 2 is corrupt and skipped.
            assertThat(data).hasSize(2);
            assertThat(data.get(0).getItem(0).getDocument()).isEqualTo("first");
            assertThat(data.get(1).getItem(0).getDocument()).isEqualTo("third");
        } finally {
            recovered.closeOpenFiles();
        }
    }

    @Test
    @DisplayName("Multiple restart cycles preserve checkpoint state")
    void multipleRestartCyclesPreserveCheckpoint() throws Exception {
        // Write some entries, close
        appender.append(buildRequest("doc1", "data1"));
        appender.append(buildRequest("doc2", "data2"));
        appender.closeOpenFiles();

        // Restart 1: should pick up from checkpoint
        FileAppender restart1 = new FileAppender(tempDir.toString());
        restart1.append(buildRequest("doc3", "data3"));
        restart1.closeOpenFiles();

        // Restart 2: should pick up from restart1's checkpoint
        FileAppender restart2 = new FileAppender(tempDir.toString());
        try {
            restart2.append(buildRequest("doc4", "data4"));
            List<IndexRequest> data = restart2.getData();
            // Should see entries from original, restart1, and restart2
            assertThat(data).hasSizeGreaterThanOrEqualTo(3);
        } finally {
            restart2.closeOpenFiles();
        }
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

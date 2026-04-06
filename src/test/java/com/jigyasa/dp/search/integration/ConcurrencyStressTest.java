package com.jigyasa.dp.search.integration;

import com.jigyasa.dp.search.handlers.IndexRequestHandler;
import com.jigyasa.dp.search.handlers.IndexSearcherManager;
import com.jigyasa.dp.search.handlers.IndexSearcherManagerISCH;
import com.jigyasa.dp.search.handlers.InitializedSchemaISCH;
import com.jigyasa.dp.search.handlers.QueryRequestHandler;
import com.jigyasa.dp.search.models.BM25Config;
import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.utils.DocIdOverlapLock;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Concurrency stress tests that verify thread safety of index and query operations
 * using a real in-memory Lucene index.
 */
class ConcurrencyStressTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;
    private DocIdOverlapLock lock;

    @BeforeEach
    void setUp() throws Exception {
        directory = new ByteBuffersDirectory();
        schema = buildSchema();
        new InitializedSchemaISCH().handle(schema, null);

        IndexWriterConfig config = new IndexWriterConfig(schema.getInitializedSchema().getIndexAnalyzer());
        config.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        writer = new IndexWriter(directory, config);
        lock = new DocIdOverlapLock(5000L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writer != null && writer.isOpen()) writer.close();
        if (directory != null) directory.close();
    }

    @Test
    @DisplayName("Concurrent writers do not corrupt the index or deadlock")
    void concurrentWriters_noCorruptionOrDeadlock() throws Exception {
        int writerThreads = 4;
        int docsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(writerThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < docsPerThread; i++) {
                        String docId = "t" + threadId + "_doc" + i;
                        String json = String.format("{\"id\":\"%s\",\"title\":\"Thread %d Doc %d\"}", docId, threadId, i);
                        IndexRequest req = IndexRequest.newBuilder()
                                .addItem(IndexItem.newBuilder()
                                        .setAction(IndexAction.UPDATE)
                                        .setDocument(json)
                                        .build())
                                .build();
                        IndexRequestHandler.processIndexRequests(req, schema, writer, lock);
                        successCount.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
        }

        // Release all threads simultaneously
        startLatch.countDown();
        executor.shutdown();
        boolean terminated = executor.awaitTermination(30, TimeUnit.SECONDS);

        assertThat(terminated).as("All writer threads should complete within timeout").isTrue();
        assertThat(errors).as("No exceptions during concurrent writes").isEmpty();
        assertThat(successCount.get()).isEqualTo(writerThreads * docsPerThread);

        // Commit and verify all documents are searchable
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(directory);
        assertThat(reader.numDocs()).isEqualTo(writerThreads * docsPerThread);
        reader.close();
    }

    @Test
    @DisplayName("Concurrent readers and writers operate without errors")
    void concurrentReadersAndWriters_noErrors() throws Exception {
        // Seed the index with some initial documents
        for (int i = 0; i < 10; i++) {
            String json = String.format("{\"id\":\"seed%d\",\"title\":\"Seed Doc %d\"}", i, i);
            IndexRequest req = IndexRequest.newBuilder()
                    .addItem(IndexItem.newBuilder()
                            .setAction(IndexAction.UPDATE)
                            .setDocument(json)
                            .build())
                    .build();
            IndexRequestHandler.processIndexRequests(req, schema, writer, lock);
        }
        writer.commit();

        int writerThreads = 2;
        int readerThreads = 4;
        int opsPerThread = 30;
        ExecutorService executor = Executors.newFixedThreadPool(writerThreads + readerThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger writeOps = new AtomicInteger(0);
        AtomicInteger readOps = new AtomicInteger(0);

        // Writer threads
        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        String docId = "w" + threadId + "_d" + i;
                        String json = String.format("{\"id\":\"%s\",\"title\":\"Writer %d Doc %d\"}", docId, threadId, i);
                        IndexRequest req = IndexRequest.newBuilder()
                                .addItem(IndexItem.newBuilder()
                                        .setAction(IndexAction.UPDATE)
                                        .setDocument(json)
                                        .build())
                                .build();
                        IndexRequestHandler.processIndexRequests(req, schema, writer, lock);
                        writeOps.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
        }

        // Reader threads: open DirectoryReader snapshots and search
        for (int t = 0; t < readerThreads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        // Commit periodically so readers see new data
                        if (i % 10 == 0) {
                            try {
                                writer.commit();
                            } catch (Exception ignored) {
                                // Concurrent commit may fail — that's fine
                            }
                        }
                        DirectoryReader reader = DirectoryReader.open(directory);
                        IndexSearcher searcher = new IndexSearcher(reader);
                        int numDocs = reader.numDocs();
                        assertThat(numDocs).as("Reader should see at least seed documents").isGreaterThanOrEqualTo(10);
                        reader.close();
                        readOps.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        boolean terminated = executor.awaitTermination(30, TimeUnit.SECONDS);

        assertThat(terminated).as("All threads should complete within timeout").isTrue();
        assertThat(errors).as("No exceptions during concurrent reads/writes").isEmpty();
        assertThat(writeOps.get()).isEqualTo(writerThreads * opsPerThread);
        assertThat(readOps.get()).isEqualTo(readerThreads * opsPerThread);
    }

    @Test
    @DisplayName("Overlapping document IDs are serialized by DocIdOverlapLock")
    void overlappingDocIds_serializedByLock() throws Exception {
        int threads = 4;
        int updatesPerThread = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

        // All threads update the SAME document ID — DocIdOverlapLock should serialize them
        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < updatesPerThread; i++) {
                        String json = String.format("{\"id\":\"shared-doc\",\"title\":\"Thread %d Update %d\"}", threadId, i);
                        IndexRequest req = IndexRequest.newBuilder()
                                .addItem(IndexItem.newBuilder()
                                        .setAction(IndexAction.UPDATE)
                                        .setDocument(json)
                                        .build())
                                .build();
                        IndexRequestHandler.processIndexRequests(req, schema, writer, lock);
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        boolean terminated = executor.awaitTermination(30, TimeUnit.SECONDS);

        assertThat(terminated).as("All threads should complete within timeout").isTrue();
        assertThat(errors).as("No exceptions with overlapping doc IDs").isEmpty();

        // Should end up with exactly 1 document (all updates to same ID)
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(directory);
        assertThat(reader.numDocs()).isEqualTo(1);
        reader.close();
    }

    private IndexSchema buildSchema() {
        SchemaField id = new SchemaField();
        id.setName("id");
        id.setType(FieldDataType.STRING);
        id.setKey(true);
        id.setFilterable(true);

        SchemaField title = new SchemaField();
        title.setName("title");
        title.setType(FieldDataType.STRING);
        title.setSearchable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, title});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}

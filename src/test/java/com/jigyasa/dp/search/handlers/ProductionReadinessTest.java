package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.handlers.translog.FileAppender;
import com.jigyasa.dp.search.handlers.translog.TranslogAppender;
import com.jigyasa.dp.search.models.BM25Config;
import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.models.InitializedIndexSchema;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.models.mappers.FieldMapperStrategy;
import com.jigyasa.dp.search.protocol.DeleteByQueryRequest;
import com.jigyasa.dp.search.protocol.DeleteByQueryResponse;
import com.jigyasa.dp.search.protocol.FilterClause;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.IndexResponse;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.protocol.TermFilter;
import com.jigyasa.dp.search.utils.DocIdOverlapLock;
import com.jigyasa.dp.search.utils.SourceVisitor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Production readiness tests that prove hardening fixes actually work.
 * Each test targets a specific hardening scenario.
 */
class ProductionReadinessTest {

    // ==================== Test 1: Offset cap prevents OOM ====================

    @Nested
    @DisplayName("Offset cap prevents OOM (QueryRequestHandler)")
    class OffsetCapTests {

        private Directory directory;
        private IndexWriter writer;
        private IndexSchema schema;
        private QueryRequestHandler handler;
        private IndexSearcherManagerISCH searcherManager;

        @BeforeEach
        void setUp() throws Exception {
            directory = new ByteBuffersDirectory();
            schema = buildQuerySchema();
            new InitializedSchemaISCH().handle(schema, null);

            IndexWriterConfig config = new IndexWriterConfig(schema.getInitializedSchema().getIndexAnalyzer());
            config.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
            writer = new IndexWriter(directory, config);

            // Index a single test doc
            String json = "{\"id\":\"doc1\",\"content\":\"test document\"}";
            IndexRequest req = IndexRequest.newBuilder()
                    .addItem(IndexItem.newBuilder()
                            .setAction(IndexAction.UPDATE)
                            .setDocument(json)
                            .build())
                    .build();
            IndexRequestHandler.processIndexRequests(req, schema, writer, null);
            writer.commit();

            IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
            when(schemaManager.getIndexSchema()).thenReturn(schema);

            searcherManager = mock(IndexSearcherManagerISCH.class);
            DirectoryReader reader = DirectoryReader.open(directory);
            IndexSearcher acquiredSearcher = new IndexSearcher(reader);
            acquiredSearcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
            when(searcherManager.acquireSearcher()).thenReturn(acquiredSearcher);
            when(searcherManager.leaseSearcher()).thenReturn(
                    new IndexSearcherManager.SearcherLease(acquiredSearcher, searcherManager));

            HandlerHelpers helpers = mock(HandlerHelpers.class);
            when(helpers.indexSchemaManager()).thenReturn(schemaManager);
            when(helpers.indexSearcherManager()).thenReturn(searcherManager);

            CollectionRegistry registry = mock(CollectionRegistry.class);
            when(registry.resolveHelpers(anyString())).thenReturn(helpers);
            handler = new QueryRequestHandler(registry);
        }

        @AfterEach
        void tearDown() throws Exception {
            if (writer != null && writer.isOpen()) writer.close();
            if (directory != null) directory.close();
        }

        @Test
        @DisplayName("Offset=50000 exceeds MAX_OFFSET and returns INVALID_ARGUMENT")
        void offsetExceedsMax_returnsInvalidArgument() {
            QueryRequest req = QueryRequest.newBuilder()
                    .setOffset(50_000)
                    .setTopK(10)
                    .build();

            @SuppressWarnings("unchecked")
            StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
            handler.internalHandle(req, observer);

            ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
            verify(observer).onError(captor.capture());
            verify(observer, never()).onNext(any());

            assertThat(captor.getValue()).isInstanceOf(StatusRuntimeException.class);
            assertThat(((StatusRuntimeException) captor.getValue()).getStatus().getCode())
                    .isEqualTo(Status.Code.INVALID_ARGUMENT);
            assertThat(captor.getValue().getMessage()).contains("offset");
        }

        @Test
        @DisplayName("Offset=10000 (boundary) is accepted without error")
        void offsetAtBoundary_succeeds() {
            QueryRequest req = QueryRequest.newBuilder()
                    .setOffset(10_000)
                    .setTopK(1)
                    .build();

            AtomicReference<QueryResponse> result = new AtomicReference<>();
            @SuppressWarnings("unchecked")
            StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
            doAnswer(inv -> {
                result.set(inv.getArgument(0));
                return null;
            }).when(observer).onNext(any());

            handler.internalHandle(req, observer);

            verify(observer, never()).onError(any());
            assertThat(result.get()).isNotNull();
        }

        private IndexSchema buildQuerySchema() {
            SchemaField idField = new SchemaField();
            idField.setName("id");
            idField.setType(FieldDataType.STRING);
            idField.setKey(true);
            idField.setFilterable(true);
            idField.setSearchable(false);
            idField.setSortable(false);

            SchemaField contentField = new SchemaField();
            contentField.setName("content");
            contentField.setType(FieldDataType.STRING);
            contentField.setKey(false);
            contentField.setFilterable(false);
            contentField.setSearchable(true);
            contentField.setSortable(false);

            IndexSchema schema = new IndexSchema();
            schema.setFields(new SchemaField[]{idField, contentField});
            schema.setBm25Config(new BM25Config());
            return schema;
        }
    }

    // ==================== Tests 2-3: Delete-by-query commit + translog ====================

    @Nested
    @DisplayName("Delete-by-query hardening (DeleteByQueryRequestHandler)")
    class DeleteByQueryHardeningTests {

        private HandlerHelpers helpers;
        private IndexWriterManagerISCH writerManager;
        private IndexSearcherManagerISCH searcherManager;
        private IndexSchemaManager schemaManager;
        private IndexWriter indexWriter;
        private TranslogAppenderManager translogManager;
        private TranslogAppender translogAppender;
        private DeleteByQueryRequestHandler handler;

        @BeforeEach
        void setUp() throws Exception {
            helpers = mock(HandlerHelpers.class);
            writerManager = mock(IndexWriterManagerISCH.class);
            searcherManager = mock(IndexSearcherManagerISCH.class);
            schemaManager = mock(IndexSchemaManager.class);
            indexWriter = mock(IndexWriter.class);

            when(helpers.indexWriterManager()).thenReturn(writerManager);
            when(helpers.indexSearcherManager()).thenReturn(searcherManager);
            when(helpers.indexSchemaManager()).thenReturn(schemaManager);
            when(writerManager.acquireWriter()).thenReturn(indexWriter);
            when(writerManager.leaseWriter()).thenReturn(
                    new IndexWriterManager.WriterLease(indexWriter, writerManager));
            when(indexWriter.getMaxCompletedSequenceNumber()).thenReturn(42L);

            translogManager = mock(TranslogAppenderManager.class);
            translogAppender = mock(TranslogAppender.class);
            when(helpers.translogAppenderManager()).thenReturn(translogManager);
            when(translogManager.getAppender()).thenReturn(translogAppender);

            IndexSchema indexSchema = mock(IndexSchema.class);
            InitializedIndexSchema initSchema = mock(InitializedIndexSchema.class);
            SchemaField statusField = new SchemaField();
            statusField.setName("status");
            statusField.setType(FieldDataType.STRING);
            statusField.setFilterable(true);
            when(initSchema.getFieldLookupMap()).thenReturn(Map.of("status", statusField));
            when(indexSchema.getInitializedSchema()).thenReturn(initSchema);
            when(schemaManager.getIndexSchema()).thenReturn(indexSchema);

            CollectionRegistry registry = mock(CollectionRegistry.class);
            when(registry.resolveHelpers(anyString())).thenReturn(helpers);
            handler = new DeleteByQueryRequestHandler(registry);
        }

        @Test
        @DisplayName("Delete-by-query forces commit to make deletes durable")
        void deleteByQuery_forcesCommit() throws Exception {
            when(indexWriter.deleteDocuments(any(Query.class))).thenReturn(99L);

            DeleteByQueryRequest req = DeleteByQueryRequest.newBuilder()
                    .addFilters(FilterClause.newBuilder()
                            .setField("status")
                            .setTermFilter(TermFilter.newBuilder().setValue("expired").build()))
                    .build();

            @SuppressWarnings("unchecked")
            StreamObserver<DeleteByQueryResponse> observer = mock(StreamObserver.class);
            handler.internalHandle(req, observer);

            verify(observer, never()).onError(any());
            verify(indexWriter).commit();
            verify(translogAppender).reset();
        }

        @Test
        @DisplayName("Delete-by-query tolerates translog reset failure and still returns success")
        void deleteByQuery_toleratesTranslogResetFailure() throws Exception {
            when(indexWriter.deleteDocuments(any(Query.class))).thenReturn(99L);
            doThrow(new RuntimeException("translog reset failed")).when(translogAppender).reset();

            DeleteByQueryRequest req = DeleteByQueryRequest.newBuilder()
                    .addFilters(FilterClause.newBuilder()
                            .setField("status")
                            .setTermFilter(TermFilter.newBuilder().setValue("expired").build()))
                    .build();

            @SuppressWarnings("unchecked")
            StreamObserver<DeleteByQueryResponse> observer = mock(StreamObserver.class);
            handler.internalHandle(req, observer);

            // Should still succeed — commit is durable, translog reset is best-effort
            ArgumentCaptor<DeleteByQueryResponse> captor = ArgumentCaptor.forClass(DeleteByQueryResponse.class);
            verify(observer).onNext(captor.capture());
            verify(observer).onCompleted();
            verify(observer, never()).onError(any());

            // Commit must still have been called before the failed reset
            verify(indexWriter).commit();
        }
    }

    // ==================== Tests 4-5: IndexRequestHandler hardening ====================

    @Nested
    @DisplayName("IndexRequestHandler hardening")
    class IndexRequestHardeningTests {

        private HandlerHelpers helpers;
        private IndexWriterManagerISCH writerManager;
        private IndexSearcherManagerISCH searcherManager;
        private IndexSchemaManager schemaManager;
        private IndexWriter indexWriter;
        private TranslogAppenderManager translogManager;
        private TranslogAppender translogAppender;
        private IndexRequestHandler handler;

        @BeforeEach
        void setUp() throws Exception {
            helpers = mock(HandlerHelpers.class);
            writerManager = mock(IndexWriterManagerISCH.class);
            searcherManager = mock(IndexSearcherManagerISCH.class);
            schemaManager = mock(IndexSchemaManager.class);
            indexWriter = mock(IndexWriter.class);

            when(helpers.indexWriterManager()).thenReturn(writerManager);
            when(helpers.indexSearcherManager()).thenReturn(searcherManager);
            when(helpers.indexSchemaManager()).thenReturn(schemaManager);
            when(writerManager.acquireWriter()).thenReturn(indexWriter);
            when(writerManager.leaseWriter()).thenReturn(
                    new IndexWriterManager.WriterLease(indexWriter, writerManager));
            when(indexWriter.getMaxCompletedSequenceNumber()).thenReturn(1L);

            translogManager = mock(TranslogAppenderManager.class);
            translogAppender = mock(TranslogAppender.class);
            when(helpers.translogAppenderManager()).thenReturn(translogManager);
            when(translogManager.getAppender()).thenReturn(translogAppender);

            IndexSchema indexSchema = mock(IndexSchema.class);
            when(indexSchema.isTtlEnabled()).thenReturn(false);
            InitializedIndexSchema initSchema = mock(InitializedIndexSchema.class);

            SchemaField idField = new SchemaField();
            idField.setName("id");
            idField.setType(FieldDataType.STRING);
            idField.setKey(true);
            idField.setFilterable(true);

            SchemaField nameField = new SchemaField();
            nameField.setName("name");
            nameField.setType(FieldDataType.STRING);
            nameField.setKey(false);
            nameField.setFilterable(false);
            nameField.setSearchable(true);

            when(initSchema.getFieldLookupMap()).thenReturn(Map.of("id", idField, "name", nameField));
            when(initSchema.getKeyFieldName()).thenReturn("id");

            // Field mapper strategy map not needed — tests trigger validation errors
            // before field mapping is reached

            when(indexSchema.getInitializedSchema()).thenReturn(initSchema);
            when(schemaManager.getIndexSchema()).thenReturn(indexSchema);

            CollectionRegistry registry = mock(CollectionRegistry.class);
            when(registry.resolveHelpers(anyString())).thenReturn(helpers);

            DocIdOverlapLock lock = new DocIdOverlapLock(5000L);
            handler = new IndexRequestHandler(lock, registry);
        }

        @Test
        @DisplayName("Missing key field returns INVALID_ARGUMENT, not 500 INTERNAL")
        void missingKeyField_returnsInvalidArgument() {
            // Document with "name" but missing required "id" key field
            IndexRequest req = IndexRequest.newBuilder()
                    .addItem(IndexItem.newBuilder()
                            .setAction(IndexAction.UPDATE)
                            .setDocument("{\"name\":\"test\"}")
                            .build())
                    .build();

            @SuppressWarnings("unchecked")
            StreamObserver<IndexResponse> observer = mock(StreamObserver.class);
            handler.internalHandle(req, observer);

            ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
            verify(observer).onError(captor.capture());

            assertThat(captor.getValue()).isInstanceOf(StatusRuntimeException.class);
            StatusRuntimeException sre = (StatusRuntimeException) captor.getValue();
            assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        }

        @Test
        @DisplayName("Translog NOT appended when all items are invalid")
        void allItemsInvalid_translogNotAppended() throws IOException {
            // Document missing key field — will fail validation before indexing
            IndexRequest req = IndexRequest.newBuilder()
                    .addItem(IndexItem.newBuilder()
                            .setAction(IndexAction.UPDATE)
                            .setDocument("{\"name\":\"test\"}")
                            .build())
                    .build();

            @SuppressWarnings("unchecked")
            StreamObserver<IndexResponse> observer = mock(StreamObserver.class);
            handler.internalHandle(req, observer);

            // Translog append must never be called when all items fail
            verify(translogAppender, never()).append(any(IndexRequest.class));
        }
    }

    // ==================== Test 6: Corrupt protobuf in translog is skipped ====================

    @Nested
    @DisplayName("Corrupt protobuf entry in translog is skipped (FileAppender)")
    class CorruptTranslogTests {

        @TempDir
        Path tempDir;

        @Test
        @DisplayName("Corrupt protobuf entry is skipped during recovery, valid entries preserved")
        void corruptProtobufEntry_skippedDuringRecovery() throws Exception {
            FileAppender appender = new FileAppender(tempDir.toString());
            try {
                // Write a valid entry
                IndexRequest validReq = IndexRequest.newBuilder()
                        .addItem(IndexItem.newBuilder()
                                .setAction(IndexAction.UPDATE)
                                .setDocument("valid document")
                                .build())
                        .build();
                appender.append(validReq);
                appender.closeOpenFiles();

                // Find the translog data file and append garbage bytes with valid length header
                Path[] files = Files.list(tempDir)
                        .filter(p -> p.getFileName().toString().startsWith("translog.dat"))
                        .toArray(Path[]::new);
                assertThat(files).isNotEmpty();

                try (DataOutputStream dos = new DataOutputStream(
                        new FileOutputStream(files[0].toFile(), true))) {
                    // Write a length header for 20 bytes, then garbage that isn't valid protobuf
                    byte[] garbage = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE,
                            (byte) 0xAB, (byte) 0xCD, 0x00, 0x00, 0x00,
                            0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE,
                            (byte) 0xAB, (byte) 0xCD, 0x00, 0x00, 0x00};
                    dos.writeLong(garbage.length);
                    dos.write(garbage);
                }

                // Recovery should return only the valid entry and skip the corrupt one
                FileAppender recoveryAppender = new FileAppender(tempDir.toString());
                try {
                    List<IndexRequest> recovered = recoveryAppender.getData();
                    assertThat(recovered).hasSize(1);
                    assertThat(recovered.get(0).getItem(0).getDocument()).isEqualTo("valid document");
                } finally {
                    recoveryAppender.closeOpenFiles();
                }
            } finally {
                appender.closeOpenFiles();
            }
        }
    }

    // ==================== Test 7: TranslogAppender interface ====================

    @Nested
    @DisplayName("TranslogAppender interface")
    class TranslogAppenderInterfaceTest {
        @Test
        @DisplayName("TranslogAppender interface has shutdown() default method")
        void translogAppenderHasShutdownMethod() throws Exception {
            TranslogAppender appender = new TranslogAppender() {
                @Override public void append(IndexRequest request) {}
                @Override public void reset() {}
                @Override public List<IndexRequest> getData() { return List.of(); }
            };
            // Should not throw — default method does nothing
            appender.shutdown();
        }
    }

    // ==================== Test 8: SourceVisitor returns defensive copy ====================

    @Nested
    @DisplayName("SourceVisitor returns defensive copy")
    class SourceVisitorDefensiveCopyTests {

        @Test
        @DisplayName("getSrc() returns a clone — modifying returned array does not affect internal state")
        void getSrcReturnsDefensiveCopy() throws Exception {
            SourceVisitor visitor = new SourceVisitor();

            byte[] original = new byte[]{1, 2, 3, 4, 5};

            // Simulate Lucene calling binaryField with source data
            FieldInfo fieldInfo = new FieldInfo(
                    FieldMapperStrategy.SOURCE_FIELD_NAME,
                    0, false, false, false,
                    org.apache.lucene.index.IndexOptions.NONE,
                    org.apache.lucene.index.DocValuesType.NONE,
                    org.apache.lucene.index.DocValuesSkipIndexType.NONE,
                    -1, Map.of(),
                    0, 0, 0, 0,
                    org.apache.lucene.index.VectorEncoding.FLOAT32,
                    org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN,
                    false, false
            );
            visitor.binaryField(fieldInfo, original);

            // Get first copy and mutate it
            byte[] firstCopy = visitor.getSrc();
            assertThat(firstCopy).isEqualTo(new byte[]{1, 2, 3, 4, 5});
            firstCopy[0] = 99;

            // Second call should be unaffected by mutation
            byte[] secondCopy = visitor.getSrc();
            assertThat(secondCopy).isEqualTo(new byte[]{1, 2, 3, 4, 5});
            assertThat(secondCopy).isNotSameAs(firstCopy);
        }

        @Test
        @DisplayName("getSrc() returns null when no binary field was set")
        void getSrcReturnsNullWhenUnset() {
            SourceVisitor visitor = new SourceVisitor();
            assertThat(visitor.getSrc()).isNull();
        }
    }
}

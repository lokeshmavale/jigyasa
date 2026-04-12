package com.jigyasa.dp.search.integration;

import com.jigyasa.dp.search.collections.CollectionRegistry;
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

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Integration tests for query timeout behavior.
 * Uses a real Lucene index with PerRequestSearcher.
 */
class QueryTimeoutIntegrationTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;
    private QueryRequestHandler handler;
    private IndexSearcherManagerISCH searcherManager;

    @BeforeEach
    void setUp() throws Exception {
        directory = new ByteBuffersDirectory();
        schema = buildSchema();
        new InitializedSchemaISCH().handle(schema, null);

        IndexWriterConfig config = new IndexWriterConfig(schema.getInitializedSchema().getIndexAnalyzer());
        config.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        writer = new IndexWriter(directory, config);

        for (int i = 0; i < 100; i++) {
            indexDoc("doc" + i, "machine learning deep learning neural networks artificial intelligence " + i);
        }
        writer.commit();

        IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
        when(schemaManager.getIndexSchema()).thenReturn(schema);

        searcherManager = mock(IndexSearcherManagerISCH.class);
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        when(searcherManager.acquireSearcher()).thenReturn(searcher);
        when(searcherManager.leaseSearcher()).thenReturn(
                new IndexSearcherManager.SearcherLease(searcher, searcherManager));

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
    @DisplayName("Default timeout (0) — query completes, timed_out=false")
    void defaultTimeoutCompletesNormally() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTextQuery("machine learning")
                .setTopK(10)
                .setTimeoutMs(0)
                .build();

        QueryResponse response = executeQuery(req);
        assertThat(response.getTimedOut()).isFalse();
        assertThat(response.getTotalHits()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Generous timeout — query completes within deadline")
    void generousTimeoutCompletes() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTextQuery("machine learning")
                .setTopK(10)
                .setTimeoutMs(30000)
                .build();

        QueryResponse response = executeQuery(req);
        assertThat(response.getTimedOut()).isFalse();
        assertThat(response.getTotalHits()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Negative timeout — no timeout enforced, query completes")
    void negativeTimeoutMeansUnlimited() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTextQuery("machine learning")
                .setTopK(10)
                .setTimeoutMs(-1)
                .build();

        QueryResponse response = executeQuery(req);
        assertThat(response.getTimedOut()).isFalse();
        assertThat(response.getTotalHits()).isGreaterThan(0);
    }

    @Test
    @DisplayName("No timeout_ms field set — uses server default, completes")
    void noTimeoutFieldSet() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTextQuery("neural networks")
                .setTopK(5)
                .build();

        QueryResponse response = executeQuery(req);
        assertThat(response.getTimedOut()).isFalse();
        assertThat(response.getHitsList()).isNotEmpty();
    }

    @Test
    @DisplayName("MatchAll with timeout — completes normally on small index")
    void matchAllWithTimeout() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTopK(10)
                .setTimeoutMs(5000)
                .build();

        QueryResponse response = executeQuery(req);
        assertThat(response.getTimedOut()).isFalse();
        assertThat(response.getTotalHits()).isEqualTo(100);
    }

    @Test
    @DisplayName("Query response has timed_out field present (proto field populated)")
    void timedOutFieldPresent() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTextQuery("machine")
                .setTopK(5)
                .setTimeoutMs(10000)
                .build();

        QueryResponse response = executeQuery(req);
        // timed_out is always populated (default false for proto3)
        assertThat(response.getTimedOut()).isFalse();
    }

    // --- Helpers ---

    private QueryResponse executeQuery(QueryRequest req) {
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
        return result.get();
    }

    private void indexDoc(String id, String content) throws Exception {
        String json = String.format("{\"id\":\"%s\",\"content\":\"%s\"}", id, content);
        IndexRequest req = IndexRequest.newBuilder()
                .addItem(IndexItem.newBuilder()
                        .setAction(IndexAction.UPDATE)
                        .setDocument(json)
                        .build())
                .build();
        IndexRequestHandler.processIndexRequests(req, schema, writer, null);
    }

    private IndexSchema buildSchema() {
        SchemaField idField = new SchemaField();
        idField.setName("id");
        idField.setType(FieldDataType.STRING);
        idField.setKey(true);
        idField.setFilterable(true);
        idField.setSearchable(false);

        SchemaField contentField = new SchemaField();
        contentField.setName("content");
        contentField.setType(FieldDataType.STRING);
        contentField.setKey(false);
        contentField.setSearchable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{idField, contentField});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}

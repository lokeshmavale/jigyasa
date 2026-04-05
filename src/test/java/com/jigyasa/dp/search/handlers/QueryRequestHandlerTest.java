package com.jigyasa.dp.search.handlers;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.models.*;
import com.jigyasa.dp.search.protocol.*;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class QueryRequestHandlerTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;
    private QueryRequestHandler handler;
    private HandlerHelpers helpers;
    private IndexSearcherManagerISCH searcherManager;
    private IndexSearcher acquiredSearcher;

    @BeforeEach
    void setUp() throws Exception {
        directory = new ByteBuffersDirectory();
        schema = buildSchema();
        new InitializedSchemaISCH().handle(schema, null);

        IndexWriterConfig config = new IndexWriterConfig(schema.getInitializedSchema().getIndexAnalyzer());
        config.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        writer = new IndexWriter(directory, config);

        // Index test data
        indexDoc("doc1", "artificial intelligence machine learning");
        indexDoc("doc2", "database systems and indexing");
        indexDoc("doc3", "machine learning deep learning neural networks");
        writer.commit();

        // Set up handler with real searcher
        IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
        when(schemaManager.getIndexSchema()).thenReturn(schema);

        searcherManager = mock(IndexSearcherManagerISCH.class);
        DirectoryReader reader = DirectoryReader.open(directory);
        acquiredSearcher = new IndexSearcher(reader);
        acquiredSearcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        when(searcherManager.acquireSearcher()).thenReturn(acquiredSearcher);
        when(searcherManager.leaseSearcher()).thenReturn(new IndexSearcherManagerISCH.SearcherLease(acquiredSearcher, searcherManager));

        helpers = mock(HandlerHelpers.class);
        when(helpers.getIndexSchemaManager()).thenReturn(schemaManager);
        when(helpers.getIndexSearcherManager()).thenReturn(searcherManager);

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
    @DisplayName("Text query returns matching documents")
    void textQueryReturnsMatches() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTextQuery("machine learning")
                .setTopK(10)
                .setIncludeSource(true)
                .build();

        QueryResponse response = executeQuery(req);

        assertThat(response.getTotalHits()).isGreaterThanOrEqualTo(2);
        assertThat(response.getHitsList()).isNotEmpty();
        // All hits should have source
        response.getHitsList().forEach(hit ->
                assertThat(hit.getSource()).isNotEmpty());
    }

    @Test
    @DisplayName("Text query on specific field")
    void textQueryOnSpecificField() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTextQuery("database")
                .setTextField("content")
                .setTopK(10)
                .build();

        QueryResponse response = executeQuery(req);
        assertThat(response.getTotalHits()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("MatchAll when no text or vector query")
    void matchAllWhenNoQuery() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTopK(10)
                .build();

        QueryResponse response = executeQuery(req);
        assertThat(response.getTotalHits()).isEqualTo(3);
    }

    @Test
    @DisplayName("Pagination with offset and topK")
    void pagination() {
        QueryRequest req = QueryRequest.newBuilder()
                .setTopK(1)
                .setOffset(0)
                .build();

        QueryResponse page1 = executeQuery(req);
        assertThat(page1.getHitsList()).hasSize(1);

        req = QueryRequest.newBuilder()
                .setTopK(1)
                .setOffset(1)
                .build();

        QueryResponse page2 = executeQuery(req);
        assertThat(page2.getHitsList()).hasSize(1);
    }

    @Test
    @DisplayName("Filter on unknown field returns INVALID_ARGUMENT")
    void filterOnUnknownField() {
        QueryRequest req = QueryRequest.newBuilder()
                .addFilters(FilterClause.newBuilder()
                        .setField("nonexistent")
                        .setTermFilter(TermFilter.newBuilder().setValue("x").build())
                        .build())
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
        handler.internalHandle(req, observer);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(captor.capture());
        assertThat(captor.getValue()).isInstanceOf(StatusRuntimeException.class);
        assertThat(((StatusRuntimeException) captor.getValue()).getStatus().getCode())
                .isEqualTo(io.grpc.Status.Code.INVALID_ARGUMENT);
    }

    @Test
    @DisplayName("Searcher is always released even on error")
    void searcherReleasedOnError() {
        // Force an error by using a bad filter
        QueryRequest req = QueryRequest.newBuilder()
                .addFilters(FilterClause.newBuilder()
                        .setField("nonexistent")
                        .setTermFilter(TermFilter.newBuilder().setValue("x").build())
                        .build())
                .build();

        @SuppressWarnings("unchecked")
        StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
        handler.internalHandle(req, observer);

        verify(searcherManager).releaseSearcher(same(acquiredSearcher));
    }

    @Test
    @DisplayName("Default topK is 10 when not specified")
    void defaultTopK() {
        QueryRequest req = QueryRequest.newBuilder().build();
        QueryResponse response = executeQuery(req);
        // We only have 3 docs, so total should be 3
        assertThat(response.getTotalHits()).isEqualTo(3);
    }

    @Test
    @DisplayName("Include source false omits source field")
    void excludeSource() {
        QueryRequest req = QueryRequest.newBuilder()
                .setIncludeSource(false)
                .setTopK(10)
                .build();

        QueryResponse response = executeQuery(req);
        response.getHitsList().forEach(hit ->
                assertThat(hit.getSource()).isEmpty());
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

package com.jigyasa.dp.search.integration;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.entrypoint.IndexManager;
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
import com.jigyasa.dp.search.protocol.CountRequest;
import com.jigyasa.dp.search.protocol.CountResponse;
import com.jigyasa.dp.search.protocol.FilterClause;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.QueryHit;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.QueryResponse;
import com.jigyasa.dp.search.protocol.RangeFilter;
import com.jigyasa.dp.search.protocol.TermFilter;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Integration tests for query_string (Lucene syntax), Count API, and field projection (source_fields).
 */
class QueryStringCountProjectionTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;
    private QueryRequestHandler handler;
    private HandlerHelpers helpers;
    private IndexSearcherManagerISCH searcherManager;
    private IndexSearcher acquiredSearcher;
    private CollectionRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        directory = new ByteBuffersDirectory();
        schema = buildSchema();
        new InitializedSchemaISCH().handle(schema, null);

        IndexWriterConfig config = new IndexWriterConfig(schema.getInitializedSchema().getIndexAnalyzer());
        config.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        writer = new IndexWriter(directory, config);

        indexTestData();
        writer.commit();
        writer.forceMerge(1);
        writer.commit();

        setupHandler();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writer != null && writer.isOpen()) writer.close();
        if (directory != null) directory.close();
    }

    // =====================================================================
    // Feature 1: Query String (Lucene syntax)
    // =====================================================================
    @Nested
    @DisplayName("QueryString")
    class QueryStringTests {

        @Test
        @DisplayName("Simple term search matches documents containing the term")
        void queryString_simpleTermSearch() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setQueryString("machine")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .contains("doc1", "doc4");
        }

        @Test
        @DisplayName("Boolean AND matches only documents with both terms")
        void queryString_booleanAnd() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setQueryString("machine AND learning")
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc4");
        }

        @Test
        @DisplayName("Boolean OR matches documents with any of the terms")
        void queryString_booleanOr() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setQueryString("cooking OR database")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(2);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .contains("doc2", "doc3");
        }

        @Test
        @DisplayName("Field-scoped query searches only specified field")
        void queryString_fieldScoped() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setQueryString("title:machine")
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc4");
        }

        @Test
        @DisplayName("Phrase search matches exact phrase")
        void queryString_phraseSearch() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setQueryString("\"machine learning\"")
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc4");
        }

        @Test
        @DisplayName("Wildcard search matches prefix patterns")
        void queryString_wildcardSearch() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setQueryString("mach*")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .contains("doc1", "doc4");
        }

        @Test
        @DisplayName("NOT operator excludes matching documents")
        void queryString_notOperator() {
            // "learning" appears in doc1, doc4 titles; "machine" also in doc1, doc4
            // "learning NOT basics" should match docs with "learning" but not "basics"
            // doc1 title has both "learning" and "basics", doc4 has "learning" but not "basics"
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setQueryString("learning NOT basics")
                    .setQueryStringDefaultField("title")
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .contains("doc4");
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .doesNotContain("doc1");
        }

        @Test
        @DisplayName("Invalid syntax returns INVALID_ARGUMENT error")
        void queryString_invalidSyntax_throws() {
            StatusRuntimeException ex = executeQueryExpectingError(QueryRequest.newBuilder()
                    .setQueryString("AND OR NOT")
                    .setTopK(10)
                    .build());

            assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        }

        @Test
        @DisplayName("query_string_default_field directs unscoped terms to specified field")
        void queryString_withDefaultField() {
            // "supervised" appears only in doc1's content
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setQueryString("supervised")
                    .setQueryStringDefaultField("content")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .contains("doc1");
        }

        @Test
        @DisplayName("query_string combined with filters narrows results")
        void queryString_withFilters() {
            // Search for "learning" but only in category=food
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setQueryString("learning")
                    .addFilters(termFilter("category", "food"))
                    .setTopK(10)
                    .build());

            // "learning" appears in doc1 (tech), doc4 (tech) titles/content — food has none
            assertThat(resp.getTotalHits()).isEqualTo(0);
        }
    }

    // =====================================================================
    // Feature 2: Count API
    // =====================================================================
    @Nested
    @DisplayName("CountAPI")
    class CountAPITests {

        @Test
        @DisplayName("Count all documents returns total count")
        void count_allDocs() {
            CountResponse resp = executeCount(CountRequest.newBuilder().build());
            assertThat(resp.getCount()).isEqualTo(5);
        }

        @Test
        @DisplayName("Count with term filter returns filtered count")
        void count_withFilter() {
            CountResponse resp = executeCount(CountRequest.newBuilder()
                    .addFilters(termFilter("category", "tech"))
                    .build());
            assertThat(resp.getCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("Count with multiple filters ANDs them")
        void count_withMultipleFilters() {
            CountResponse resp = executeCount(CountRequest.newBuilder()
                    .addFilters(termFilter("category", "tech"))
                    .addFilters(rangeFilter("price", "30.0", "80.0", false, false))
                    .build());
            // tech docs: doc1(29.99), doc2(49.99), doc4(79.99) — price 30-80: doc2, doc4
            assertThat(resp.getCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("Count on empty result set returns 0")
        void count_emptyCollection() {
            CountResponse resp = executeCount(CountRequest.newBuilder()
                    .addFilters(termFilter("category", "nonexistent"))
                    .build());
            assertThat(resp.getCount()).isEqualTo(0);
        }
    }

    // =====================================================================
    // Feature 3: Field Projection (source_fields)
    // =====================================================================
    @Nested
    @DisplayName("FieldProjection")
    class FieldProjectionTests {

        @Test
        @DisplayName("Returns only requested fields in source")
        void projection_returnsOnlyRequestedFields() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addAllSourceFields(List.of("title", "category"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).isNotEmpty();
            for (QueryHit hit : resp.getHitsList()) {
                assertThat(hit.getSource()).isNotEmpty();
                assertThat(hit.getSource()).contains("title");
                assertThat(hit.getSource()).contains("category");
                assertThat(hit.getSource()).doesNotContain("\"content\"");
                assertThat(hit.getSource()).doesNotContain("\"price\"");
                assertThat(hit.getSource()).doesNotContain("\"quantity\"");
            }
        }

        @Test
        @DisplayName("Includes key field if explicitly requested")
        void projection_includesKeyFieldIfRequested() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addAllSourceFields(List.of("id", "title"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).isNotEmpty();
            for (QueryHit hit : resp.getHitsList()) {
                assertThat(hit.getSource()).contains("\"id\"");
                assertThat(hit.getSource()).contains("\"title\"");
                assertThat(hit.getSource()).doesNotContain("\"content\"");
            }
        }

        @Test
        @DisplayName("Empty source_fields with include_source=true returns full source")
        void projection_emptyFieldList_fallsBackToIncludeSource() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setIncludeSource(true)
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).isNotEmpty();
            for (QueryHit hit : resp.getHitsList()) {
                assertThat(hit.getSource()).isNotEmpty();
                // Full source includes all fields
                assertThat(hit.getSource()).contains("title");
                assertThat(hit.getSource()).contains("content");
                assertThat(hit.getSource()).contains("price");
            }
        }

        @Test
        @DisplayName("source_fields overrides include_source=false")
        void projection_overridesIncludeSourceFalse() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addAllSourceFields(List.of("title"))
                    .setIncludeSource(false)
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).isNotEmpty();
            for (QueryHit hit : resp.getHitsList()) {
                assertThat(hit.getSource()).isNotEmpty();
                assertThat(hit.getSource()).contains("title");
            }
        }

        @Test
        @DisplayName("Unknown field in source_fields is silently omitted")
        void projection_unknownField_returnsEmptyForThat() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .addAllSourceFields(List.of("title", "nonexistent"))
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).isNotEmpty();
            for (QueryHit hit : resp.getHitsList()) {
                assertThat(hit.getSource()).contains("title");
                assertThat(hit.getSource()).doesNotContain("nonexistent");
            }
        }
    }

    // =====================================================================
    // Helper methods
    // =====================================================================

    private void indexDoc(String id, String title, String content, String category,
                          double price, int quantity) throws Exception {
        String json = String.format(
                "{\"id\":\"%s\",\"title\":\"%s\",\"content\":\"%s\",\"category\":\"%s\"," +
                        "\"price\":%s,\"quantity\":%d}",
                id, title, content, category, price, quantity);

        IndexItem.Builder itemBuilder = IndexItem.newBuilder()
                .setAction(IndexAction.UPDATE)
                .setDocument(json);
        IndexRequest req = IndexRequest.newBuilder()
                .addItem(itemBuilder.build())
                .build();
        IndexRequestHandler.IndexResult result = IndexRequestHandler.processIndexRequests(req, schema, writer, null);
        result.response().getItemResponseList().forEach(status ->
                assertThat(status.getCode()).as("Index error: " + status.getMessage()).isEqualTo(0));
    }

    private void indexTestData() throws Exception {
        indexDoc("doc1", "Machine Learning Basics",
                "introduction to supervised learning", "tech", 29.99, 100);
        indexDoc("doc2", "Database Internals",
                "B-tree and LSM-tree indexing", "tech", 49.99, 50);
        indexDoc("doc3", "Cooking Made Easy",
                "simple recipes for beginners", "food", 15.99, 200);
        indexDoc("doc4", "Advanced Machine Learning",
                "deep reinforcement learning techniques", "tech", 79.99, 25);
        indexDoc("doc5", "Food Science Explained",
                "chemistry behind cooking", "food", 35.99, 75);
    }

    private void setupHandler() throws Exception {
        IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
        when(schemaManager.getIndexSchema()).thenReturn(schema);

        searcherManager = mock(IndexSearcherManagerISCH.class);
        DirectoryReader reader = DirectoryReader.open(directory);
        acquiredSearcher = new IndexSearcher(reader);
        acquiredSearcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        when(searcherManager.acquireSearcher()).thenReturn(acquiredSearcher);
        when(searcherManager.leaseSearcher()).thenReturn(new IndexSearcherManager.SearcherLease(acquiredSearcher, searcherManager));

        helpers = mock(HandlerHelpers.class);
        when(helpers.indexSchemaManager()).thenReturn(schemaManager);
        when(helpers.indexSearcherManager()).thenReturn(searcherManager);

        registry = mock(CollectionRegistry.class);
        when(registry.resolveHelpers(anyString())).thenReturn(helpers);
        handler = new QueryRequestHandler(registry);
    }

    private static FilterClause termFilter(String field, String value) {
        return FilterClause.newBuilder()
                .setField(field)
                .setTermFilter(TermFilter.newBuilder().setValue(value))
                .build();
    }

    private static FilterClause rangeFilter(String field, String min, String max,
                                             boolean minExclusive, boolean maxExclusive) {
        return FilterClause.newBuilder()
                .setField(field)
                .setRangeFilter(RangeFilter.newBuilder()
                        .setMin(min).setMax(max)
                        .setMinExclusive(minExclusive).setMaxExclusive(maxExclusive))
                .build();
    }

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

    private StatusRuntimeException executeQueryExpectingError(QueryRequest req) {
        @SuppressWarnings("unchecked")
        StreamObserver<QueryResponse> observer = mock(StreamObserver.class);
        handler.internalHandle(req, observer);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(observer).onError(captor.capture());
        assertThat(captor.getValue()).isInstanceOf(StatusRuntimeException.class);
        return (StatusRuntimeException) captor.getValue();
    }

    private CountResponse executeCount(CountRequest req) {
        IndexManager indexManager = new IndexManager(registry);
        AtomicReference<CountResponse> result = new AtomicReference<>();
        @SuppressWarnings("unchecked")
        StreamObserver<CountResponse> observer = mock(StreamObserver.class);
        doAnswer(inv -> {
            result.set(inv.getArgument(0));
            return null;
        }).when(observer).onNext(any());

        indexManager.count(req, observer);
        verify(observer, never()).onError(any());
        assertThat(result.get()).isNotNull();
        return result.get();
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
        title.setFilterable(true);
        title.setSortable(true);

        SchemaField content = new SchemaField();
        content.setName("content");
        content.setType(FieldDataType.STRING);
        content.setSearchable(true);

        SchemaField category = new SchemaField();
        category.setName("category");
        category.setType(FieldDataType.STRING);
        category.setFilterable(true);

        SchemaField price = new SchemaField();
        price.setName("price");
        price.setType(FieldDataType.DOUBLE);
        price.setFilterable(true);
        price.setSortable(true);

        SchemaField quantity = new SchemaField();
        quantity.setName("quantity");
        quantity.setType(FieldDataType.INT32);
        quantity.setFilterable(true);
        quantity.setSortable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, title, content, category, price, quantity});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}

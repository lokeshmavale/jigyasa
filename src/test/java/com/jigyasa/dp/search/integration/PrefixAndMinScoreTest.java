package com.jigyasa.dp.search.integration;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.handlers.IndexRequestHandler;
import com.jigyasa.dp.search.handlers.IndexSearcherManager;
import com.jigyasa.dp.search.handlers.IndexSearcherManagerISCH;
import com.jigyasa.dp.search.handlers.InitializedSchemaISCH;
import com.jigyasa.dp.search.handlers.QueryRequestHandler;
import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.HandlerHelpers;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.IndexSchemaManager;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import com.jigyasa.dp.search.protocol.QueryHit;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
 * Tests for prefix query and min_score threshold features.
 */
class PrefixAndMinScoreTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;
    private QueryRequestHandler handler;

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
    // Prefix Queries
    // =====================================================================
    @Nested
    @DisplayName("PrefixQueries")
    class PrefixQueries {

        @Test
        @DisplayName("Prefix 'depl' matches deployment and deploy")
        void prefixQuery_matchesPrefix() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setPrefixQuery("depl")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(2);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc2");
        }

        @Test
        @DisplayName("Prefix on specific field")
        void prefixQuery_specificField() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setPrefixQuery("depl")
                    .setPrefixField("title")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(2);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                    .containsExactlyInAnyOrder("doc1", "doc2");
        }

        @Test
        @DisplayName("Prefix with no match returns empty")
        void prefixQuery_noMatch() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setPrefixQuery("xyz")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isEqualTo(0);
            assertThat(resp.getHitsList()).isEmpty();
        }

        @Test
        @DisplayName("Prefix with empty field searches all searchable STRING fields")
        void prefixQuery_allSearchableFields() {
            // "etl" appears in content of doc5
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setPrefixQuery("etl")
                    .setTopK(10)
                    .build());

            assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
            assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc5");
        }
    }

    // =====================================================================
    // Min Score Threshold
    // =====================================================================
    @Nested
    @DisplayName("MinScoreThreshold")
    class MinScoreThreshold {

        @Test
        @DisplayName("minScore filters low-score hits")
        void minScore_filtersLowScoreHits() {
            // "kubernetes" only in doc1 content — strong match
            // First query without min_score
            QueryResponse all = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("kubernetes")
                    .setTopK(10)
                    .build());
            assertThat(all.getTotalHits()).isGreaterThanOrEqualTo(1);

            // Now with very high min_score
            QueryResponse filtered = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("kubernetes")
                    .setMinScore(999.0f)
                    .setTopK(10)
                    .build());
            assertThat(filtered.getHitsList()).isEmpty();
        }

        @Test
        @DisplayName("minScore=0 returns all hits (default behavior)")
        void minScore_zeroReturnsAll() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("deployment")
                    .setMinScore(0.0f)
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).isNotEmpty();
        }

        @Test
        @DisplayName("Very high minScore returns no hits")
        void minScore_veryHighReturnsNone() {
            QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                    .setTextQuery("deployment")
                    .setMinScore(999.0f)
                    .setTopK(10)
                    .build());

            assertThat(resp.getHitsList()).isEmpty();
        }

        @Test
        @DisplayName("minScore with MatchAllDocsQuery (score=1.0)")
        void minScore_withMatchAll() {
            // MatchAllDocsQuery scores 1.0 for all docs
            QueryResponse low = executeQuery(QueryRequest.newBuilder()
                    .setMinScore(0.5f)
                    .setTopK(10)
                    .build());
            assertThat(low.getHitsList()).hasSize(5);

            QueryResponse high = executeQuery(QueryRequest.newBuilder()
                    .setMinScore(1.5f)
                    .setTopK(10)
                    .build());
            assertThat(high.getHitsList()).isEmpty();
        }
    }

    // =====================================================================
    // Helpers
    // =====================================================================

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

    private void indexDoc(String id, String title, String content, String category) throws Exception {
        String json = String.format(
                "{\"id\":\"%s\",\"title\":\"%s\",\"content\":\"%s\",\"category\":\"%s\"}",
                id, title, content, category);

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
        indexDoc("doc1", "deployment automation", "kubernetes deployment pipelines", "devops");
        indexDoc("doc2", "deploy scripts", "bash deployment scripts", "devops");
        indexDoc("doc3", "deep learning", "neural network training", "ml");
        indexDoc("doc4", "debugging tips", "python debugging techniques", "dev");
        indexDoc("doc5", "data engineering", "etl pipeline design", "data");
    }

    private void setupHandler() throws Exception {
        IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
        when(schemaManager.getIndexSchema()).thenReturn(schema);

        IndexSearcherManagerISCH searcherManager = mock(IndexSearcherManagerISCH.class);
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher acquiredSearcher = new IndexSearcher(reader);
        acquiredSearcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        when(searcherManager.acquireSearcher()).thenReturn(acquiredSearcher);
        when(searcherManager.leaseSearcher()).thenReturn(new IndexSearcherManager.SearcherLease(acquiredSearcher, searcherManager));

        HandlerHelpers helpers = mock(HandlerHelpers.class);
        when(helpers.indexSchemaManager()).thenReturn(schemaManager);
        when(helpers.indexSearcherManager()).thenReturn(searcherManager);

        CollectionRegistry registry = mock(CollectionRegistry.class);
        when(registry.resolveHelpers(anyString())).thenReturn(helpers);
        handler = new QueryRequestHandler(registry);
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

        IndexSchema s = new IndexSchema();
        s.setFields(new SchemaField[]{id, title, content, category});
        return s;
    }
}

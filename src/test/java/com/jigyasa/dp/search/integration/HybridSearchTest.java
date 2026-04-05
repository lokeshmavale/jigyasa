package com.jigyasa.dp.search.integration;

import com.jigyasa.dp.search.collections.CollectionRegistry;
import com.jigyasa.dp.search.handlers.*;
import com.jigyasa.dp.search.models.*;
import com.jigyasa.dp.search.protocol.*;
import io.grpc.stub.StreamObserver;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration tests for hybrid (BM25 + KNN RRF) search scenarios.
 */
class HybridSearchTest {

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

    @Test
    @DisplayName("Hybrid search combines BM25 and KNN results")
    void hybridSearch_combinesBM25AndKNN() {
        // text_query targets doc1 ("machine learning"), vector close to doc3's [0,0,1]
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setTextQuery("machine learning")
                .setTextField("title")
                .setVectorQuery(VectorQuery.newBuilder()
                        .setField("embedding")
                        .addAllVector(List.of(0.0f, 0.0f, 1.0f))
                        .setK(5))
                .setTopK(5)
                .build());

        assertThat(resp.getHitsList()).isNotEmpty();
        List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
        // Should include text match (doc1) and vector match (doc3 [0,0,1])
        assertThat(ids).contains("doc1");
        assertThat(ids).contains("doc3");
    }

    @Test
    @DisplayName("Hybrid search with filter applies filter to both branches")
    void hybridSearch_withFilter() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setTextQuery("science")
                .setTextField("title")
                .setVectorQuery(VectorQuery.newBuilder()
                        .setField("embedding")
                        .addAllVector(List.of(1.0f, 0.0f, 0.0f))
                        .setK(5))
                .addFilters(termFilter("category", "food"))
                .setTopK(5)
                .build());

        // Only food docs should appear
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                .allMatch(id -> id.equals("doc3") || id.equals("doc4") || id.equals("doc5"));
        // doc1 (tech) and doc2 (tech) should not appear
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                .doesNotContain("doc1", "doc2");
    }

    @Test
    @DisplayName("Hybrid search with tenant isolation")
    void hybridSearch_withTenantIsolation() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setTextQuery("learning")
                .setTextField("title")
                .setVectorQuery(VectorQuery.newBuilder()
                        .setField("embedding")
                        .addAllVector(List.of(0.5f, 0.5f, 0.0f))
                        .setK(5))
                .setTenantId("tenantA")
                .setTopK(5)
                .build());

        // Only tenantA docs: doc1, doc2, doc3
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                .allMatch(id -> id.equals("doc1") || id.equals("doc2") || id.equals("doc3"));
    }

    @Test
    @DisplayName("Phrase query + vector triggers hybrid search")
    void hybridSearch_phraseAndVector() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setPhraseQuery("machine learning")
                .setPhraseField("title")
                .setVectorQuery(VectorQuery.newBuilder()
                        .setField("embedding")
                        .addAllVector(List.of(0.0f, 1.0f, 0.0f))
                        .setK(5))
                .setTopK(5)
                .build());

        assertThat(resp.getHitsList()).isNotEmpty();
        List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
        // Phrase "machine learning" matches doc1; vector [0,1,0] closest to doc2
        assertThat(ids).contains("doc1");
        assertThat(ids).contains("doc2");
    }

    @Test
    @DisplayName("Fuzzy query + vector triggers hybrid search")
    void hybridSearch_fuzzyAndVector() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setFuzzyQuery("machin")
                .setFuzzyField("title")
                .setVectorQuery(VectorQuery.newBuilder()
                        .setField("embedding")
                        .addAllVector(List.of(0.0f, 0.0f, 1.0f))
                        .setK(5))
                .setTopK(5)
                .build());

        assertThat(resp.getHitsList()).isNotEmpty();
        List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
        // Fuzzy "machin" → "machine" matches doc1; vector [0,0,1] closest to doc3
        assertThat(ids).contains("doc3");
    }

    @Test
    @DisplayName("Query string + vector triggers hybrid search")
    void hybridSearch_queryStringAndVector() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setQueryString("machine AND learning")
                .setVectorQuery(VectorQuery.newBuilder()
                        .setField("embedding")
                        .addAllVector(List.of(0.0f, 1.0f, 0.0f))
                        .setK(5))
                .setTopK(5)
                .build());

        assertThat(resp.getHitsList()).isNotEmpty();
        List<String> ids = resp.getHitsList().stream().map(QueryHit::getDocId).toList();
        assertThat(ids).contains("doc1"); // text match
        assertThat(ids).contains("doc2"); // vector [0,1,0] match
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

    private void indexDoc(String id, String title, String category,
                          float[] embedding, String tenantId) throws Exception {
        String json = String.format(
                "{\"id\":\"%s\",\"title\":\"%s\",\"category\":\"%s\",\"embedding\":[%s]}",
                id, title, category, formatEmbedding(embedding));

        IndexItem.Builder itemBuilder = IndexItem.newBuilder()
                .setAction(IndexAction.UPDATE)
                .setDocument(json);
        if (tenantId != null && !tenantId.isEmpty()) {
            itemBuilder.setTenantId(tenantId);
        }
        IndexRequest req = IndexRequest.newBuilder()
                .addItem(itemBuilder.build())
                .build();
        IndexRequestHandler.IndexResult result = IndexRequestHandler.processIndexRequests(req, schema, writer, null);
        result.response().getItemResponseList().forEach(status ->
                assertThat(status.getCode()).as("Index error: " + status.getMessage()).isEqualTo(0));
    }

    private String formatEmbedding(float[] embedding) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < embedding.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(embedding[i]);
        }
        return sb.toString();
    }

    private void indexTestData() throws Exception {
        indexDoc("doc1", "Machine Learning Basics", "tech",
                new float[]{1.0f, 0.0f, 0.0f}, "tenantA");
        indexDoc("doc2", "Database Internals", "tech",
                new float[]{0.0f, 1.0f, 0.0f}, "tenantA");
        indexDoc("doc3", "Cooking Made Easy", "food",
                new float[]{0.0f, 0.0f, 1.0f}, "tenantA");
        indexDoc("doc4", "Food Science", "food",
                new float[]{0.1f, 0.9f, 0.0f}, "tenantB");
        indexDoc("doc5", "Advanced AI Systems", "tech",
                new float[]{0.9f, 0.1f, 0.0f}, "tenantB");
    }

    private void setupHandler() throws Exception {
        IndexSchemaManager schemaManager = mock(IndexSchemaManager.class);
        when(schemaManager.getIndexSchema()).thenReturn(schema);

        IndexSearcherManagerISCH searcherManager = mock(IndexSearcherManagerISCH.class);
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher acquiredSearcher = new IndexSearcher(reader);
        acquiredSearcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        when(searcherManager.acquireSearcher()).thenReturn(acquiredSearcher);
        when(searcherManager.leaseSearcher()).thenReturn(new IndexSearcherManagerISCH.SearcherLease(acquiredSearcher, searcherManager));

        HandlerHelpers helpers = mock(HandlerHelpers.class);
        when(helpers.getIndexSchemaManager()).thenReturn(schemaManager);
        when(helpers.getIndexSearcherManager()).thenReturn(searcherManager);

        CollectionRegistry registry = mock(CollectionRegistry.class);
        when(registry.resolveHelpers(anyString())).thenReturn(helpers);
        handler = new QueryRequestHandler(registry);
    }

    private static FilterClause termFilter(String field, String value) {
        return FilterClause.newBuilder()
                .setField(field)
                .setTermFilter(TermFilter.newBuilder().setValue(value))
                .build();
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

        SchemaField category = new SchemaField();
        category.setName("category");
        category.setType(FieldDataType.STRING);
        category.setFilterable(true);

        SchemaField embedding = new SchemaField();
        embedding.setName("embedding");
        embedding.setType(FieldDataType.VECTOR);
        embedding.setDimension(3);
        embedding.setSimilarityFunction("COSINE");

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, title, category, embedding});
        schema.setBm25Config(new BM25Config());
        schema.setTtlEnabled(true);
        return schema;
    }
}

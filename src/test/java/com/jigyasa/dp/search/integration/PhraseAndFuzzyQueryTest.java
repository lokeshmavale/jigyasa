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

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for phrase query and fuzzy query features.
 */
class PhraseAndFuzzyQueryTest {

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
    // Phrase Query Tests
    // =====================================================================

    @Test
    @DisplayName("phraseQuery_exactMatch: exact phrase 'machine learning' matches doc1 not doc2")
    void phraseQuery_exactMatch() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setPhraseQuery("machine learning")
                .setPhraseField("title")
                .setPhraseSlop(0)
                .setTopK(10)
                .build());

        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc1");
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).doesNotContain("doc2");
    }

    @Test
    @DisplayName("phraseQuery_withSlop: 'learning fundamentals' slop=2 matches doc1")
    void phraseQuery_withSlop() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setPhraseQuery("learning fundamentals")
                .setPhraseField("title")
                .setPhraseSlop(2)
                .setTopK(10)
                .build());

        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc1");
    }

    @Test
    @DisplayName("phraseQuery_noMatch: 'container learning' slop=0 has no match")
    void phraseQuery_noMatch() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setPhraseQuery("container learning")
                .setPhraseField("title")
                .setPhraseSlop(0)
                .setTopK(10)
                .build());

        assertThat(resp.getTotalHits()).isEqualTo(0);
    }

    @Test
    @DisplayName("phraseQuery_allSearchableFields: empty field searches all searchable fields")
    void phraseQuery_allSearchableFields() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setPhraseQuery("machine learning")
                .setPhraseSlop(0)
                .setTopK(10)
                .build());

        assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc1");
    }

    @Test
    @DisplayName("phraseQuery_specificField: phrase on 'title' field only")
    void phraseQuery_specificField() {
        // "neural networks" appears in title of doc3
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setPhraseQuery("neural networks")
                .setPhraseField("title")
                .setPhraseSlop(0)
                .setTopK(10)
                .build());

        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc3");
    }

    // =====================================================================
    // Fuzzy Query Tests
    // =====================================================================

    @Test
    @DisplayName("fuzzyQuery_exactSpelling: 'kubernetes' maxEdits=2 matches doc4")
    void fuzzyQuery_exactSpelling() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setFuzzyQuery("kubernetes")
                .setFuzzyField("title")
                .setMaxEdits(2)
                .setTopK(10)
                .build());

        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc4");
    }

    @Test
    @DisplayName("fuzzyQuery_typoTolerance: 'kubernates' (typo) maxEdits=2 matches doc4 and doc5")
    void fuzzyQuery_typoTolerance() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setFuzzyQuery("kubernates")
                .setFuzzyField("title")
                .setMaxEdits(2)
                .setTopK(10)
                .build());

        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                .containsAnyOf("doc4", "doc5");
    }

    @Test
    @DisplayName("fuzzyQuery_tooManyEdits: 'kuberxxxxx' maxEdits=2 should NOT match")
    void fuzzyQuery_tooManyEdits() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setFuzzyQuery("kuberxxxxx")
                .setFuzzyField("title")
                .setMaxEdits(2)
                .setTopK(10)
                .build());

        assertThat(resp.getTotalHits()).isEqualTo(0);
    }

    @Test
    @DisplayName("fuzzyQuery_multiTerm: 'deep lerning' (typo) maxEdits=1 matches doc3")
    void fuzzyQuery_multiTerm() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setFuzzyQuery("deep lerning")
                .setFuzzyField("title")
                .setMaxEdits(1)
                .setTopK(10)
                .build());

        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc3");
    }

    @Test
    @DisplayName("fuzzyQuery_withPrefixLength: fuzzy with prefixLength=3")
    void fuzzyQuery_withPrefixLength() {
        // "kubernates" with prefix=3 means first 3 chars "kub" must match exactly
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setFuzzyQuery("kubernates")
                .setFuzzyField("title")
                .setMaxEdits(2)
                .setPrefixLength(3)
                .setTopK(10)
                .build());

        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId)
                .containsAnyOf("doc4", "doc5");
    }

    @Test
    @DisplayName("hybridPhraseAndVector: phrase_query + vector_query with RRF fusion")
    void hybridPhraseAndVector() {
        QueryResponse resp = executeQuery(QueryRequest.newBuilder()
                .setPhraseQuery("machine learning")
                .setPhraseField("title")
                .setVectorQuery(VectorQuery.newBuilder()
                        .setField("embedding")
                        .addVector(1.0f).addVector(0.0f).addVector(0.0f)
                        .setK(5))
                .setTopK(5)
                .build());

        assertThat(resp.getTotalHits()).isGreaterThanOrEqualTo(1);
        // doc1 should rank high (matches both phrase and vector)
        assertThat(resp.getHitsList()).extracting(QueryHit::getDocId).contains("doc1");
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

    private void indexDoc(String id, String title, String content, float[] embedding) throws Exception {
        String json = String.format(
                "{\"id\":\"%s\",\"title\":\"%s\",\"content\":\"%s\",\"embedding\":[%s]}",
                id, title, content, formatEmbedding(embedding));

        IndexItem item = IndexItem.newBuilder()
                .setAction(IndexAction.UPDATE)
                .setDocument(json)
                .build();
        IndexRequest req = IndexRequest.newBuilder().addItem(item).build();
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
        indexDoc("doc1", "machine learning fundamentals",
                "introduction to machine learning concepts",
                new float[]{1.0f, 0.0f, 0.0f});
        indexDoc("doc2", "learning about different machines",
                "industrial machines and their applications",
                new float[]{0.0f, 1.0f, 0.0f});
        indexDoc("doc3", "deep learning neural networks",
                "advanced deep learning architectures",
                new float[]{0.0f, 0.0f, 1.0f});
        indexDoc("doc4", "kubernetes container orchestration",
                "deploying applications with kubernetes",
                new float[]{0.5f, 0.5f, 0.0f});
        indexDoc("doc5", "kubernates deployment guide",
                "step by step kubernates deployment",
                new float[]{0.5f, 0.0f, 0.5f});
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

        SchemaField content = new SchemaField();
        content.setName("content");
        content.setType(FieldDataType.STRING);
        content.setSearchable(true);

        SchemaField embedding = new SchemaField();
        embedding.setName("embedding");
        embedding.setType(FieldDataType.VECTOR);
        embedding.setDimension(3);
        embedding.setSimilarityFunction("COSINE");

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, title, content, embedding});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}

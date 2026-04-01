package com.jigyasa.dp.search.query;

import com.jigyasa.dp.search.models.*;
import com.jigyasa.dp.search.handlers.InitializedSchemaISCH;
import com.jigyasa.dp.search.protocol.QueryRequest;
import com.jigyasa.dp.search.protocol.RecencyDecay;
import com.jigyasa.dp.search.utils.SystemFields;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for QueryPipeline and RecencyDecayModifier.
 */
class QueryPipelineTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;

    @BeforeEach
    void setUp() throws Exception {
        directory = new ByteBuffersDirectory();
        schema = buildSchema();
        new InitializedSchemaISCH().handle(schema, null);

        IndexWriterConfig config = new IndexWriterConfig(schema.getInitializedSchema().getIndexAnalyzer());
        config.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        writer = new IndexWriter(directory, config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writer != null && writer.isOpen()) writer.close();
        if (directory != null) directory.close();
    }

    @Test
    @DisplayName("RecencyDecay boosts recent documents higher")
    void recencyDecay_boostsRecentDocuments() throws Exception {
        long now = System.currentTimeMillis();
        indexDocWithTimestamp("doc_old", "test", now - 36_000_000L);
        indexDocWithTimestamp("doc_mid", "test", now - 3_600_000L);
        indexDocWithTimestamp("doc_new", "test", now - 60_000L);
        writer.commit();

        IndexSearcher searcher = newSearcher();
        QueryRequest req = QueryRequest.newBuilder()
                .setTextQuery("test")
                .setTextField("title")
                .setRecencyDecay(RecencyDecay.newBuilder().setHalfLifeSeconds(3600).build())
                .build();

        QueryContext context = new QueryContext(req, schema, schema.getInitializedSchema());
        RecencyDecayModifier modifier = new RecencyDecayModifier();
        assertThat(modifier.applies(context)).isTrue();

        Query baseQuery = new TermQuery(new Term("title", "test"));
        Query decayed = modifier.modify(baseQuery, context);

        TopDocs results = searcher.search(decayed, 10);
        assertThat(results.scoreDocs).hasSizeGreaterThanOrEqualTo(3);

        String topDocId = getDocId(searcher, results.scoreDocs[0].doc);
        assertThat(topDocId).isEqualTo("doc_new");

        String lastDocId = getDocId(searcher, results.scoreDocs[2].doc);
        assertThat(lastDocId).isEqualTo("doc_old");
    }

    @Test
    @DisplayName("RecencyDecay half-life affects ranking order")
    void recencyDecay_halfLifeAffectsRanking() throws Exception {
        long now = System.currentTimeMillis();
        indexDocWithTimestamp("doc_recent", "alpha", now - 1_800_000L);
        indexDocWithTimestamp("doc_older", "alpha", now - 7_200_000L);
        writer.commit();

        IndexSearcher searcher = newSearcher();
        Query base = new TermQuery(new Term("title", "alpha"));

        // Small half-life (10 minutes) — strongly favors recent
        QueryRequest reqSmall = QueryRequest.newBuilder()
                .setTextQuery("alpha")
                .setRecencyDecay(RecencyDecay.newBuilder().setHalfLifeSeconds(600).build())
                .build();
        RecencyDecayModifier modifier = new RecencyDecayModifier();
        Query decayedSmall = modifier.modify(base, new QueryContext(reqSmall, schema, schema.getInitializedSchema()));
        TopDocs smallHalfLife = searcher.search(decayedSmall, 10);
        double ratioSmall = smallHalfLife.scoreDocs[0].score / smallHalfLife.scoreDocs[1].score;

        // Large half-life (24 hours) — less difference
        QueryRequest reqLarge = QueryRequest.newBuilder()
                .setTextQuery("alpha")
                .setRecencyDecay(RecencyDecay.newBuilder().setHalfLifeSeconds(86400).build())
                .build();
        Query decayedLarge = modifier.modify(base, new QueryContext(reqLarge, schema, schema.getInitializedSchema()));
        TopDocs largeHalfLife = searcher.search(decayedLarge, 10);
        double ratioLarge = largeHalfLife.scoreDocs[0].score / largeHalfLife.scoreDocs[1].score;

        assertThat(ratioSmall).isGreaterThan(ratioLarge);
    }

    @Test
    @DisplayName("RecencyDecay not applied when TTL disabled")
    void recencyDecay_notAppliedWhenTtlDisabled() {
        IndexSchema noTtlSchema = buildSchemaWithTtl(false);
        new InitializedSchemaISCH().handle(noTtlSchema, null);

        QueryRequest req = QueryRequest.newBuilder()
                .setRecencyDecay(RecencyDecay.newBuilder().setHalfLifeSeconds(3600).build())
                .build();
        QueryContext context = new QueryContext(req, noTtlSchema, noTtlSchema.getInitializedSchema());

        assertThat(new RecencyDecayModifier().applies(context)).isFalse();
    }

    @Test
    @DisplayName("RecencyDecay not applied when not requested")
    void recencyDecay_notAppliedWhenNotRequested() {
        QueryRequest req = QueryRequest.newBuilder().setTextQuery("test").build();
        QueryContext context = new QueryContext(req, schema, schema.getInitializedSchema());

        assertThat(new RecencyDecayModifier().applies(context)).isFalse();
    }

    @Test
    @DisplayName("Pipeline applies multiple modifiers in order")
    void pipeline_appliesMultipleModifiers() {
        QueryModifier alwaysApply = new QueryModifier() {
            @Override public boolean applies(QueryContext context) { return true; }
            @Override public Query modify(Query query, QueryContext context) {
                return new BoostQuery(query, 2.0f);
            }
        };
        QueryModifier neverApply = new QueryModifier() {
            @Override public boolean applies(QueryContext context) { return false; }
            @Override public Query modify(Query query, QueryContext context) {
                return new BoostQuery(query, 100.0f);
            }
        };
        QueryModifier alsoApply = new QueryModifier() {
            @Override public boolean applies(QueryContext context) { return true; }
            @Override public Query modify(Query query, QueryContext context) {
                return new BoostQuery(query, 3.0f);
            }
        };

        QueryPipeline pipeline = new QueryPipeline(List.of(alwaysApply, neverApply, alsoApply));
        Query base = new TermQuery(new Term("title", "test"));
        QueryContext ctx = new QueryContext(QueryRequest.newBuilder().build(), schema, schema.getInitializedSchema());

        Query result = pipeline.apply(base, ctx);

        assertThat(result).isInstanceOf(BoostQuery.class);
        BoostQuery outer = (BoostQuery) result;
        assertThat(outer.getBoost()).isEqualTo(3.0f);
        assertThat(outer.getQuery()).isInstanceOf(BoostQuery.class);
        BoostQuery inner = (BoostQuery) outer.getQuery();
        assertThat(inner.getBoost()).isEqualTo(2.0f);
        assertThat(inner.getQuery()).isInstanceOf(TermQuery.class);
    }

    // --- Helpers ---

    private void indexDocWithTimestamp(String id, String content, long indexedAtMs) throws Exception {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.YES));

        FieldType ft = new FieldType();
        ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        ft.setTokenized(true);
        ft.setStored(true);
        doc.add(new Field("title", content, ft));

        // Inject _indexed_at manually (mimicking SystemFields)
        doc.add(new LongPoint(SystemFields.INDEXED_AT, indexedAtMs));
        doc.add(new NumericDocValuesField(SystemFields.INDEXED_AT, indexedAtMs));
        doc.add(new SortedNumericDocValuesField(SystemFields.INDEXED_AT + "$o", indexedAtMs));
        doc.add(new StoredField(SystemFields.INDEXED_AT, indexedAtMs));

        writer.addDocument(doc);
    }

    private IndexSearcher newSearcher() throws Exception {
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());
        return searcher;
    }

    private String getDocId(IndexSearcher searcher, int docId) throws Exception {
        return searcher.storedFields().document(docId).get("id");
    }

    private IndexSchema buildSchema() {
        return buildSchemaWithTtl(true);
    }

    private IndexSchema buildSchemaWithTtl(boolean ttlEnabled) {
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
        schema.setTtlEnabled(ttlEnabled);
        return schema;
    }
}

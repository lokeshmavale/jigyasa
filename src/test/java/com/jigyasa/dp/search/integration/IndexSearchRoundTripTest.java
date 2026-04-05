package com.jigyasa.dp.search.integration;

import com.jigyasa.dp.search.handlers.IndexRequestHandler;
import com.jigyasa.dp.search.handlers.InitializedSchemaISCH;
import com.jigyasa.dp.search.models.BM25Config;
import com.jigyasa.dp.search.models.FieldDataType;
import com.jigyasa.dp.search.models.IndexSchema;
import com.jigyasa.dp.search.models.LuceneFieldType;
import com.jigyasa.dp.search.models.SchemaField;
import com.jigyasa.dp.search.protocol.IndexAction;
import com.jigyasa.dp.search.protocol.IndexItem;
import com.jigyasa.dp.search.protocol.IndexRequest;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: index documents, commit, then search/lookup to verify round-trip.
 */
class IndexSearchRoundTripTest {

    private Directory directory;
    private IndexWriter writer;
    private IndexSchema schema;

    @BeforeEach
    void setUp() throws Exception {
        directory = new ByteBuffersDirectory();

        schema = buildTestSchema();
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
    @DisplayName("Index and search text document via BM25")
    void indexAndSearchText() throws Exception {
        indexDoc("doc1", "The quick brown fox jumps over the lazy dog");
        indexDoc("doc2", "A fast red car drives on the highway");
        indexDoc("doc3", "The brown dog sleeps in the garden");
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            searcher.setSimilarity(schema.getInitializedSchema().getBm25Similarity());

            // Search for "brown" — should match doc1 and doc3
            org.apache.lucene.index.Term term = new org.apache.lucene.index.Term("content", "brown");
            TopDocs results = searcher.search(new TermQuery(term), 10);

            assertThat(results.totalHits.value()).isEqualTo(2);
        }
    }

    @Test
    @DisplayName("Index and filter by key field")
    void indexAndFilterByKey() throws Exception {
        indexDoc("doc1", "hello world");
        indexDoc("doc2", "goodbye world");
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);

            // Filter by key — filterable field uses name$f convention
            String filterField = LuceneFieldType.FILTERABLE.toLuceneFieldName("id");
            Query query = new TermQuery(new org.apache.lucene.index.Term(filterField, "doc1"));
            TopDocs results = searcher.search(query, 10);

            assertThat(results.totalHits.value()).isEqualTo(1);

            // Verify source retrieval
            com.jigyasa.dp.search.utils.SourceVisitor visitor = new com.jigyasa.dp.search.utils.SourceVisitor();
            searcher.storedFields().document(results.scoreDocs[0].doc, visitor);
            assertThat(visitor.getSrc()).isNotNull();
            String src = new String(visitor.getSrc(), StandardCharsets.UTF_8);
            assertThat(src).contains("doc1").contains("hello world");
        }
    }

    @Test
    @DisplayName("Update replaces existing document")
    void updateReplacesDocument() throws Exception {
        indexDoc("doc1", "original content");
        writer.commit();
        indexDoc("doc1", "updated content");
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);

            // Only one document should exist for doc1
            String filterField = LuceneFieldType.FILTERABLE.toLuceneFieldName("id");
            TopDocs results = searcher.search(
                    new TermQuery(new org.apache.lucene.index.Term(filterField, "doc1")), 10);
            assertThat(results.totalHits.value()).isEqualTo(1);

            // Source should be the updated version
            com.jigyasa.dp.search.utils.SourceVisitor visitor = new com.jigyasa.dp.search.utils.SourceVisitor();
            searcher.storedFields().document(results.scoreDocs[0].doc, visitor);
            String src = new String(visitor.getSrc(), StandardCharsets.UTF_8);
            assertThat(src).contains("updated content");
        }
    }

    @Test
    @DisplayName("Delete removes document")
    void deleteRemovesDocument() throws Exception {
        indexDoc("doc1", "content to delete");
        indexDoc("doc2", "content to keep");
        writer.commit();

        // Delete doc1
        IndexRequest deleteReq = IndexRequest.newBuilder()
                .addItem(IndexItem.newBuilder()
                        .setAction(IndexAction.DELETE)
                        .setDocument("{\"id\":\"doc1\"}")
                        .build())
                .build();
        IndexRequestHandler.processIndexRequests(deleteReq, schema, writer, null);
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs results = searcher.search(new MatchAllDocsQuery(), 10);
            assertThat(results.totalHits.value()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("Index vector field and perform KNN search")
    void indexAndKnnSearch() throws Exception {
        IndexSchema vectorSchema = buildVectorSchema();
        new InitializedSchemaISCH().handle(vectorSchema, null);

        IndexWriterConfig config = new IndexWriterConfig(vectorSchema.getInitializedSchema().getIndexAnalyzer());
        config.setSimilarity(vectorSchema.getInitializedSchema().getBm25Similarity());

        try (Directory vecDir = new ByteBuffersDirectory();
             IndexWriter vecWriter = new IndexWriter(vecDir, config)) {

            // Index 3 vectors
            indexVectorDoc(vecWriter, vectorSchema, "v1", new float[]{1.0f, 0.0f, 0.0f});
            indexVectorDoc(vecWriter, vectorSchema, "v2", new float[]{0.0f, 1.0f, 0.0f});
            indexVectorDoc(vecWriter, vectorSchema, "v3", new float[]{0.9f, 0.1f, 0.0f});
            vecWriter.commit();

            try (DirectoryReader reader = DirectoryReader.open(vecDir)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                // KNN search for vector close to v1
                float[] queryVec = {1.0f, 0.0f, 0.0f};
                Query knnQuery = new KnnFloatVectorQuery("embedding", queryVec, 2);
                TopDocs results = searcher.search(knnQuery, 2);

                assertThat(results.scoreDocs.length).isEqualTo(2);
                // First result should be v1 (exact match) or v3 (closest)
            }
        }
    }

    @Test
    @DisplayName("Empty key field value is rejected")
    void emptyKeyFieldRejected() throws Exception {
        IndexRequest req = IndexRequest.newBuilder()
                .addItem(IndexItem.newBuilder()
                        .setAction(IndexAction.UPDATE)
                        .setDocument("{\"id\":\"\",\"content\":\"test\"}")
                        .build())
                .build();

        IndexRequestHandler.IndexResult result = IndexRequestHandler.processIndexRequests(req, schema, writer, null);
        // Should return INVALID_ARGUMENT for the item
        assertThat(result.response().getItemResponse(0).getCode())
                .isEqualTo(com.google.rpc.Code.INVALID_ARGUMENT.getNumber());
    }

    // --- Helper methods ---

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

    private void indexVectorDoc(IndexWriter w, IndexSchema s, String id, float[] vec) throws Exception {
        StringBuilder vecJson = new StringBuilder("[");
        for (int i = 0; i < vec.length; i++) {
            if (i > 0) vecJson.append(",");
            vecJson.append(vec[i]);
        }
        vecJson.append("]");

        String json = String.format("{\"id\":\"%s\",\"embedding\":%s}", id, vecJson);
        IndexRequest req = IndexRequest.newBuilder()
                .addItem(IndexItem.newBuilder()
                        .setAction(IndexAction.UPDATE)
                        .setDocument(json)
                        .build())
                .build();
        IndexRequestHandler.processIndexRequests(req, s, w, null);
    }

    private IndexSchema buildTestSchema() {
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

    private IndexSchema buildVectorSchema() {
        SchemaField idField = new SchemaField();
        idField.setName("id");
        idField.setType(FieldDataType.STRING);
        idField.setKey(true);
        idField.setFilterable(true);
        idField.setSearchable(false);
        idField.setSortable(false);

        SchemaField vecField = new SchemaField();
        vecField.setName("embedding");
        vecField.setType(FieldDataType.VECTOR);
        vecField.setDimension(3);
        vecField.setSimilarityFunction("COSINE");
        vecField.setKey(false);
        vecField.setFilterable(false);
        vecField.setSearchable(false);
        vecField.setSortable(false);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{idField, vecField});
        schema.setBm25Config(new BM25Config());
        return schema;
    }
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for SearchAfter cursor-based pagination with various sort field types.
 */
class SearchAfterPaginationTest {

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
    @DisplayName("SearchAfter with INT32 sort paginates 3 pages correctly")
    void searchAfter_withIntSort_multiPage() {
        List<String> allIds = new ArrayList<>();

        // Page 1
        QueryResponse page1 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("quantity").setDescending(false))
                .setTopK(2)
                .build());
        assertThat(page1.getHitsList()).hasSize(2);
        allIds.addAll(page1.getHitsList().stream().map(QueryHit::getDocId).toList());
        assertThat(page1.hasNextSearchAfter()).isTrue();

        // Page 2
        QueryResponse page2 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("quantity").setDescending(false))
                .setTopK(2)
                .setSearchAfter(page1.getNextSearchAfter())
                .build());
        assertThat(page2.getHitsList()).hasSize(2);
        allIds.addAll(page2.getHitsList().stream().map(QueryHit::getDocId).toList());
        assertThat(page2.hasNextSearchAfter()).isTrue();

        // Page 3 (last doc)
        QueryResponse page3 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("quantity").setDescending(false))
                .setTopK(2)
                .setSearchAfter(page2.getNextSearchAfter())
                .build());
        assertThat(page3.getHitsList()).hasSize(1);
        allIds.addAll(page3.getHitsList().stream().map(QueryHit::getDocId).toList());

        // All 5 docs, sorted by quantity ASC: 10, 25, 50, 75, 100
        assertThat(allIds).containsExactly("doc5", "doc4", "doc2", "doc3", "doc1");
        // No duplicates
        assertThat(allIds).doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("SearchAfter with STRING sort works with BytesRef serialization")
    void searchAfter_withStringSort() {
        List<String> allIds = new ArrayList<>();

        // Page 1: sorted by title ASC
        QueryResponse page1 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("title").setDescending(false))
                .setTopK(2)
                .build());
        assertThat(page1.getHitsList()).hasSize(2);
        allIds.addAll(page1.getHitsList().stream().map(QueryHit::getDocId).toList());

        // Page 2
        QueryResponse page2 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("title").setDescending(false))
                .setTopK(2)
                .setSearchAfter(page1.getNextSearchAfter())
                .build());
        assertThat(page2.getHitsList()).hasSize(2);
        allIds.addAll(page2.getHitsList().stream().map(QueryHit::getDocId).toList());

        // Page 3
        QueryResponse page3 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("title").setDescending(false))
                .setTopK(2)
                .setSearchAfter(page2.getNextSearchAfter())
                .build());
        allIds.addAll(page3.getHitsList().stream().map(QueryHit::getDocId).toList());

        assertThat(allIds).hasSize(5);
        assertThat(allIds).doesNotHaveDuplicates();
        // Alphabetical by title: "Advanced AI", "Cooking Easy", "Database Int", "Food Science", "Machine Learn"
        assertThat(allIds).containsExactly("doc4", "doc3", "doc2", "doc5", "doc1");
    }

    @Test
    @DisplayName("SearchAfter with DOUBLE sort")
    void searchAfter_withDoubleSort() {
        List<String> allIds = new ArrayList<>();

        QueryResponse page1 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("price").setDescending(false))
                .setTopK(3)
                .build());
        allIds.addAll(page1.getHitsList().stream().map(QueryHit::getDocId).toList());
        assertThat(page1.hasNextSearchAfter()).isTrue();

        QueryResponse page2 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("price").setDescending(false))
                .setTopK(3)
                .setSearchAfter(page1.getNextSearchAfter())
                .build());
        allIds.addAll(page2.getHitsList().stream().map(QueryHit::getDocId).toList());

        assertThat(allIds).hasSize(5);
        assertThat(allIds).doesNotHaveDuplicates();
        // Price ASC: 15.99, 29.99, 35.99, 49.99, 79.99
        assertThat(allIds).containsExactly("doc3", "doc1", "doc5", "doc2", "doc4");
    }

    @Test
    @DisplayName("SearchAfter with multi-field sort (field1 ASC, field2 DESC)")
    void searchAfter_withMultiFieldSort() {
        List<String> allIds = new ArrayList<>();

        // Sort by category ASC (filterable string not sortable — use title instead), then price DESC
        QueryResponse page1 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("title").setDescending(false))
                .addSort(SortClause.newBuilder().setField("price").setDescending(true))
                .setTopK(2)
                .build());
        allIds.addAll(page1.getHitsList().stream().map(QueryHit::getDocId).toList());

        QueryResponse page2 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("title").setDescending(false))
                .addSort(SortClause.newBuilder().setField("price").setDescending(true))
                .setTopK(2)
                .setSearchAfter(page1.getNextSearchAfter())
                .build());
        allIds.addAll(page2.getHitsList().stream().map(QueryHit::getDocId).toList());

        QueryResponse page3 = executeQuery(QueryRequest.newBuilder()
                .addSort(SortClause.newBuilder().setField("title").setDescending(false))
                .addSort(SortClause.newBuilder().setField("price").setDescending(true))
                .setTopK(2)
                .setSearchAfter(page2.getNextSearchAfter())
                .build());
        allIds.addAll(page3.getHitsList().stream().map(QueryHit::getDocId).toList());

        assertThat(allIds).hasSize(5);
        assertThat(allIds).doesNotHaveDuplicates();
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
                          double price, int quantity, long timestamp) throws Exception {
        String json = String.format(
                "{\"id\":\"%s\",\"title\":\"%s\",\"category\":\"%s\"," +
                        "\"price\":%s,\"quantity\":%d,\"timestamp\":%d}",
                id, title, category, price, quantity, timestamp);

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
        indexDoc("doc1", "Machine Learning Basics", "tech", 29.99, 100, 1000);
        indexDoc("doc2", "Database Internals", "tech", 49.99, 50, 2000);
        indexDoc("doc3", "Cooking Made Easy", "food", 15.99, 75, 3000);
        indexDoc("doc4", "Advanced AI Systems", "tech", 79.99, 25, 4000);
        indexDoc("doc5", "Food Science", "food", 35.99, 10, 5000);
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
        title.setFilterable(true);
        title.setSortable(true);

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

        SchemaField timestamp = new SchemaField();
        timestamp.setName("timestamp");
        timestamp.setType(FieldDataType.INT64);
        timestamp.setFilterable(true);
        timestamp.setSortable(true);

        IndexSchema schema = new IndexSchema();
        schema.setFields(new SchemaField[]{id, title, category, price, quantity, timestamp});
        schema.setBm25Config(new BM25Config());
        schema.setTtlEnabled(true);
        return schema;
    }
}
